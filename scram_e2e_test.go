package mqttv5

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scramBroker is a local in-process MQTT broker configured with SCRAM
// authentication, used as the counterparty for client-side SCRAM e2e tests.
type scramBroker struct {
	addr string
	srv  *Server
	stop func()
}

// startScramBroker starts a broker on 127.0.0.1:0 with the given SCRAM hash
// algorithms enabled and the supplied user → credentials map.
func startScramBroker(t *testing.T, users map[string]*SCRAMCredentials, hashes ...SCRAMHash) *scramBroker {
	t.Helper()
	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, username string) (*SCRAMCredentials, error) {
		if creds, ok := users[username]; ok {
			return creds, nil
		}
		return nil, nil
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := NewServer(
		WithListener(listener),
		WithEnhancedAuth(NewSCRAMAuthenticator(lookup, hashes...)),
	)
	go func() { _ = srv.ListenAndServe() }()

	b := &scramBroker{
		addr: fmt.Sprintf("tcp://%s", listener.Addr().String()),
		srv:  srv,
	}
	b.stop = func() { _ = srv.Close() }
	return b
}

// makeScramCreds constructs SCRAM credentials with a freshly-generated salt.
func makeScramCreds(t *testing.T, hash SCRAMHash, password, namespace string) *SCRAMCredentials {
	t.Helper()
	salt, err := GenerateSalt()
	require.NoError(t, err)
	creds := ComputeSCRAMCredentials(hash, password, salt, 4096)
	creds.Namespace = namespace
	return creds
}

// scramHashName labels subtests with the wire mechanism name.
func scramHashName(h SCRAMHash) string { return h.String() }

// TestE2ESCRAMPubSub verifies a full publish → receive round-trip after a
// successful SCRAM handshake, across SHA-1, SHA-256, and SHA-512.
func TestE2ESCRAMPubSub(t *testing.T) {
	for _, hash := range []SCRAMHash{SCRAMHashSHA1, SCRAMHashSHA256, SCRAMHashSHA512} {
		t.Run(scramHashName(hash), func(t *testing.T) {
			password := fmt.Sprintf("s3cret-%s", hash.String())
			users := map[string]*SCRAMCredentials{
				"alice": makeScramCreds(t, hash, password, "team-alpha"),
			}
			broker := startScramBroker(t, users, hash)
			defer broker.stop()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			client, err := DialContext(ctx,
				WithServers(broker.addr),
				WithClientID(fmt.Sprintf("e2e-scram-%s", hash.String())),
				WithCleanStart(true),
				WithConnectTimeout(3*time.Second),
				WithEnhancedAuthentication(NewClientSCRAMAuthenticator(hash, "alice", password)),
			)
			require.NoError(t, err)
			defer client.Close()
			assert.True(t, client.IsConnected())

			topic := fmt.Sprintf("e2e/scram/%s", hash.String())
			payload := []byte("hello after scram")

			var wg sync.WaitGroup
			wg.Add(1)
			var received *Message
			err = client.Subscribe(topic, QoS1, func(msg *Message) {
				received = msg
				wg.Done()
			})
			require.NoError(t, err)

			err = client.Publish(&Message{Topic: topic, Payload: payload, QoS: QoS1})
			require.NoError(t, err)

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
				require.NotNil(t, received)
				assert.Equal(t, topic, received.Topic)
				assert.Equal(t, payload, received.Payload)
			case <-time.After(3 * time.Second):
				t.Fatal("timeout waiting for SCRAM-authenticated message")
			}
		})
	}
}

// TestE2ESCRAMWrongPassword confirms the broker rejects a client offering the
// wrong password and the client surfaces the failure from DialContext.
func TestE2ESCRAMWrongPassword(t *testing.T) {
	users := map[string]*SCRAMCredentials{
		"alice": makeScramCreds(t, SCRAMHashSHA256, "rightpass", DefaultNamespace),
	}
	broker := startScramBroker(t, users, SCRAMHashSHA256)
	defer broker.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := DialContext(ctx,
		WithServers(broker.addr),
		WithClientID("e2e-scram-wrong-pass"),
		WithCleanStart(true),
		WithConnectTimeout(3*time.Second),
		WithEnhancedAuthentication(NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "wrongpass")),
	)
	require.Error(t, err)
}

// TestE2ESCRAMWrongMechanism confirms the broker refuses a client that
// announces a SCRAM mechanism the broker does not support.
func TestE2ESCRAMWrongMechanism(t *testing.T) {
	users := map[string]*SCRAMCredentials{
		"alice": makeScramCreds(t, SCRAMHashSHA256, "secret", DefaultNamespace),
	}
	// Broker accepts only SHA-256; client will offer SHA-512.
	broker := startScramBroker(t, users, SCRAMHashSHA256)
	defer broker.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := DialContext(ctx,
		WithServers(broker.addr),
		WithClientID("e2e-scram-wrong-mech"),
		WithCleanStart(true),
		WithConnectTimeout(3*time.Second),
		WithEnhancedAuthentication(NewClientSCRAMAuthenticator(SCRAMHashSHA512, "alice", "secret")),
	)
	require.Error(t, err)
}

// TestE2ESCRAMReconnect verifies SCRAM works repeatedly across separate
// connections — i.e., no client-side state leaks between sessions.
func TestE2ESCRAMReconnect(t *testing.T) {
	password := "reconnect-secret"
	users := map[string]*SCRAMCredentials{
		"alice": makeScramCreds(t, SCRAMHashSHA256, password, DefaultNamespace),
	}
	broker := startScramBroker(t, users, SCRAMHashSHA256)
	defer broker.stop()

	connectOnce := func(t *testing.T, label string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := DialContext(ctx,
			WithServers(broker.addr),
			WithClientID(fmt.Sprintf("e2e-scram-reconnect-%s", label)),
			WithCleanStart(true),
			WithConnectTimeout(3*time.Second),
			WithEnhancedAuthentication(NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", password)),
		)
		require.NoError(t, err)
		defer client.Close()
		assert.True(t, client.IsConnected())

		topic := fmt.Sprintf("e2e/scram/reconnect/%s", label)
		var wg sync.WaitGroup
		wg.Add(1)
		var received *Message
		err = client.Subscribe(topic, QoS1, func(msg *Message) {
			received = msg
			wg.Done()
		})
		require.NoError(t, err)

		err = client.Publish(&Message{Topic: topic, Payload: []byte(label), QoS: QoS1})
		require.NoError(t, err)

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			require.NotNil(t, received)
			assert.Equal(t, []byte(label), received.Payload)
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout on %s connect", label)
		}
	}

	connectOnce(t, "first")
	connectOnce(t, "second")
	connectOnce(t, "third")
}
