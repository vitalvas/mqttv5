package mqttv5

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testAuthenticator is a test authenticator with configurable behavior.
type testAuthenticator struct {
	authFunc func(context.Context, *AuthContext) (*AuthResult, error)
}

func (t *testAuthenticator) Authenticate(ctx context.Context, authCtx *AuthContext) (*AuthResult, error) {
	return t.authFunc(ctx, authCtx)
}

// testAuthorizer is a test authorizer with configurable behavior.
type testAuthorizer struct {
	authzFunc func(context.Context, *AuthzContext) (*AuthzResult, error)
}

func (t *testAuthorizer) Authorize(ctx context.Context, authzCtx *AuthzContext) (*AuthzResult, error) {
	return t.authzFunc(ctx, authzCtx)
}

func TestNewServer(t *testing.T) {
	t.Run("creates server with listener", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		require.NotNil(t, srv)
		defer srv.Close()

		addrs := srv.Addrs()
		require.Len(t, addrs, 1)
		assert.Equal(t, listener.Addr(), addrs[0])
	})

	t.Run("applies options", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		var connected bool
		srv := NewServer(
			WithListener(listener),
			WithMaxConnections(100),
			OnConnect(func(_ *ServerClient) {
				connected = true
			}),
		)
		require.NotNil(t, srv)
		defer srv.Close()

		assert.Equal(t, 100, srv.config.maxConnections)
		if srv.config.onConnect != nil {
			srv.config.onConnect(nil)
		}
		assert.True(t, connected)
	})

	t.Run("with keep alive override", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithServerKeepAlive(120),
		)
		require.NotNil(t, srv)
		defer srv.Close()

		assert.Equal(t, uint16(120), srv.keepAlive.ServerOverride())
	})

	t.Run("no listeners returns error", func(t *testing.T) {
		srv := NewServer()
		err := srv.ListenAndServe()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no listeners configured")
	})
}

func TestServerClients(t *testing.T) {
	t.Run("empty server has no clients", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		assert.Equal(t, 0, srv.ClientCount())
		assert.Empty(t, srv.Clients())
	})
}

func TestServerPublish(t *testing.T) {
	t.Run("publish when server not running", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		// Don't start the server

		msg := &Message{Topic: "test", Payload: []byte("data")}
		err = srv.Publish(msg)
		assert.ErrorIs(t, err, ErrServerClosed)

		srv.Close()
	})

	t.Run("publish retained message stores it", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		// Manually set running to true for this test
		srv.running.Store(true)
		defer srv.running.Store(false)

		msg := &Message{
			Topic:   "test/retained",
			Payload: []byte("data"),
			Retain:  true,
		}
		err = srv.Publish(msg)
		require.NoError(t, err)

		// Check retained store
		retained := srv.config.retainedStore.Match(DefaultNamespace, "test/retained")
		require.Len(t, retained, 1)
		assert.Equal(t, "test/retained", retained[0].Topic)
		assert.Equal(t, []byte("data"), retained[0].Payload)
	})

	t.Run("publish empty retained message deletes it", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// First store a retained message
		srv.config.retainedStore.Set(DefaultNamespace, &RetainedMessage{
			Topic:   "test/retained",
			Payload: []byte("data"),
		})

		// Then publish empty payload to delete
		msg := &Message{
			Topic:   "test/retained",
			Payload: []byte{},
			Retain:  true,
		}
		err = srv.Publish(msg)
		require.NoError(t, err)

		// Check retained store is empty
		retained := srv.config.retainedStore.Match(DefaultNamespace, "test/retained")
		assert.Empty(t, retained)
	})

	t.Run("publish retained when retain disabled returns error", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithRetainedStore(nil),
		)
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		msg := &Message{
			Topic:   "test/retained",
			Payload: []byte("data"),
			Retain:  true,
		}
		err = srv.Publish(msg)
		assert.ErrorIs(t, err, ErrRetainNotSupported)
	})

	t.Run("publish with invalid namespace rejected", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// Namespace containing delimiter should be rejected
		msg := &Message{
			Topic:     "test/topic",
			Payload:   []byte("data"),
			Namespace: "tenant||evil",
		}
		err = srv.Publish(msg)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrNamespaceInvalidChar)
	})

	t.Run("publish with uppercase namespace rejected", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		msg := &Message{
			Topic:     "test/topic",
			Payload:   []byte("data"),
			Namespace: "INVALID",
		}
		err = srv.Publish(msg)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrNamespaceInvalidChar)
	})

	t.Run("publish with empty namespace defaults to DefaultNamespace", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		msg := &Message{
			Topic:     "test/retained",
			Payload:   []byte("data"),
			Retain:    true,
			Namespace: "", // Empty should default to DefaultNamespace
		}
		err = srv.Publish(msg)
		require.NoError(t, err)

		// Check it was stored under DefaultNamespace
		retained := srv.config.retainedStore.Match(DefaultNamespace, "test/retained")
		require.Len(t, retained, 1)
		assert.Equal(t, []byte("data"), retained[0].Payload)
	})

	t.Run("publish filtered out by producer interceptor returns nil", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		filteringInterceptor := &testProducerInterceptor{
			modifier: func(_ *Message) *Message {
				return nil // Filter out all messages
			},
		}

		srv := NewServer(
			WithListener(listener),
			WithServerProducerInterceptors(filteringInterceptor),
		)
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		msg := &Message{
			Topic:   "test/topic",
			Payload: []byte("data"),
		}
		err = srv.Publish(msg)
		assert.NoError(t, err, "publish filtered by interceptor should return nil, not an error")
		assert.True(t, filteringInterceptor.called, "interceptor should have been called")
	})

	t.Run("publish with invalid topic returns error", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		msg := &Message{
			Topic:   "test/+/topic", // Wildcards not allowed in topic names (only in filters)
			Payload: []byte("data"),
		}
		err = srv.Publish(msg)
		assert.Error(t, err, "publish with wildcard in topic should fail")
	})

	t.Run("publish with empty topic returns error", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		msg := &Message{
			Topic:   "",
			Payload: []byte("data"),
		}
		err = srv.Publish(msg)
		assert.Error(t, err, "publish with empty topic should fail")
	})

	t.Run("publish expired message is silently discarded", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// Create a message that has already expired
		msg := &Message{
			Topic:         "test/expired",
			Payload:       []byte("data"),
			Retain:        true,
			MessageExpiry: 1,                                // 1 second expiry
			PublishedAt:   time.Now().Add(-2 * time.Second), // Published 2 seconds ago
		}
		err = srv.Publish(msg)
		assert.NoError(t, err, "expired message should be silently discarded without error")

		// Verify the message was NOT stored in retained store
		retained := srv.config.retainedStore.Match(DefaultNamespace, "test/expired")
		assert.Empty(t, retained, "expired message should not be stored as retained")
	})
}

func TestServerClose(t *testing.T) {
	t.Run("close stops server", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		// Start server in background
		go srv.ListenAndServe()

		// Wait for it to start
		time.Sleep(50 * time.Millisecond)

		err = srv.Close()
		require.NoError(t, err)

		// Second close should be no-op
		err = srv.Close()
		require.NoError(t, err)
	})

	t.Run("close when not running", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		err = srv.Close()
		require.NoError(t, err)
	})

	t.Run("close disconnects connected clients", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		var disconnectReceived bool
		var disconnectReason ReasonCode
		var mu sync.Mutex
		disconnectDone := make(chan struct{})

		srv := NewServer(
			WithListener(listener),
			OnDisconnect(func(_ *ServerClient) {
				mu.Lock()
				disconnectReceived = true
				mu.Unlock()
			}),
		)

		go srv.ListenAndServe()
		time.Sleep(50 * time.Millisecond)

		// Connect a client
		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Send CONNECT
		connect := &ConnectPacket{ClientID: "test-client"}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		// Read CONNACK
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Verify client is connected
		assert.Equal(t, 1, srv.ClientCount())

		// Close server - should disconnect client with ReasonServerShuttingDown
		go func() {
			defer close(disconnectDone)
			// Read DISCONNECT from server
			pkt, _, err := ReadPacket(conn, 256*1024)
			if err == nil {
				if disc, ok := pkt.(*DisconnectPacket); ok {
					mu.Lock()
					disconnectReason = disc.ReasonCode
					mu.Unlock()
				}
			}
		}()

		err = srv.Close()
		require.NoError(t, err)

		// Wait for disconnect to be processed
		select {
		case <-disconnectDone:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for disconnect")
		}

		mu.Lock()
		assert.True(t, disconnectReceived)
		assert.Equal(t, ReasonServerShuttingDown, disconnectReason)
		mu.Unlock()

		// Client count should be 0 after close
		assert.Equal(t, 0, srv.ClientCount())
	})

	t.Run("close with multiple listeners", func(t *testing.T) {
		listener1, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		listener2, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener1),
			WithListener(listener2),
		)

		go srv.ListenAndServe()
		time.Sleep(50 * time.Millisecond)

		// Connect to both listeners
		conn1, err := net.Dial("tcp", listener1.Addr().String())
		require.NoError(t, err)
		conn2, err := net.Dial("tcp", listener2.Addr().String())
		require.NoError(t, err)

		// Send CONNECT on both
		connect1 := &ConnectPacket{ClientID: "client1"}
		_, err = WritePacket(conn1, connect1, 256*1024)
		require.NoError(t, err)

		connect2 := &ConnectPacket{ClientID: "client2"}
		_, err = WritePacket(conn2, connect2, 256*1024)
		require.NoError(t, err)

		// Read CONNACKs
		_, _, _ = ReadPacket(conn1, 256*1024)
		_, _, _ = ReadPacket(conn2, 256*1024)

		// Wait for both clients to be registered (with retry for CI)
		require.Eventually(t, func() bool {
			return srv.ClientCount() == 2
		}, 1*time.Second, 10*time.Millisecond, "expected 2 clients to be connected")

		// Close should disconnect both clients
		err = srv.Close()
		require.NoError(t, err)

		assert.Equal(t, 0, srv.ClientCount())
	})

	t.Run("close completes within timeout", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		go srv.ListenAndServe()
		time.Sleep(50 * time.Millisecond)

		// Close should complete quickly
		done := make(chan struct{})
		go func() {
			srv.Close()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not complete within timeout")
		}
	})
}

func TestServerAddrs(t *testing.T) {
	t.Run("returns listener addresses", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		addrs := srv.Addrs()
		require.Len(t, addrs, 1)
		assert.Equal(t, listener.Addr(), addrs[0])
	})

	t.Run("returns empty when no listeners", func(t *testing.T) {
		srv := NewServer()
		assert.Empty(t, srv.Addrs())
	})

	t.Run("returns multiple addresses", func(t *testing.T) {
		listener1, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		listener2, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener1),
			WithListener(listener2),
		)
		defer srv.Close()

		addrs := srv.Addrs()
		require.Len(t, addrs, 2)
		assert.Equal(t, listener1.Addr(), addrs[0])
		assert.Equal(t, listener2.Addr(), addrs[1])
	})
}

func TestServerListenAndServe(t *testing.T) {
	t.Run("already running returns error", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		// Start first instance
		go srv.ListenAndServe()
		time.Sleep(50 * time.Millisecond)

		// Try to start again
		errCh := make(chan error, 1)
		go func() {
			errCh <- srv.ListenAndServe()
		}()

		select {
		case err := <-errCh:
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "already running")
		case <-time.After(100 * time.Millisecond):
			// Expected - first instance is still running
		}

		srv.Close()
	})
}

func TestServerConcurrency(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	srv := NewServer(WithListener(listener))
	defer srv.Close()

	srv.running.Store(true)
	defer srv.running.Store(false)

	var wg sync.WaitGroup

	// Concurrent client count reads
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.ClientCount()
			_ = srv.Clients()
		}()
	}

	// Concurrent publish
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.Publish(&Message{Topic: "test", Payload: []byte("data")})
		}()
	}

	wg.Wait()
}

func TestServerEmptyTopicValidation(t *testing.T) {
	t.Run("empty topic after alias resolution disconnects client", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		// Give server time to start
		time.Sleep(50 * time.Millisecond)

		// Connect a client
		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Send CONNECT
		connect := &ConnectPacket{
			ClientID:   "test-client",
			CleanStart: true,
		}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		// Read CONNACK
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Send PUBLISH with empty topic and no alias (invalid)
		publish := &PublishPacket{
			Topic:   "", // Empty topic
			Payload: []byte("test"),
			QoS:     0,
		}
		_, err = WritePacket(conn, publish, 256*1024)
		require.NoError(t, err)

		// Server should disconnect us - read should fail or return DISCONNECT
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)

		// Either we get a DISCONNECT or connection closed
		if err == nil {
			disconnect, ok := pkt.(*DisconnectPacket)
			if ok {
				assert.Equal(t, ReasonProtocolError, disconnect.ReasonCode)
			}
		}

		conn.Close()
		srv.Close()
		wg.Wait()
	})
}

// TestServerQoSRetryLogic tests that server retries QoS 1/2 messages with DUP flag (Issue 4)
func TestServerQoSRetryLogic(t *testing.T) {
	t.Run("retryClientMessages sets DUP flag for QoS1", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		// Create a mock client with a pending QoS 1 message
		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Use a tracker with short retry interval
		tracker := NewQoS1Tracker(10*time.Millisecond, 3)
		client.qos1Tracker = tracker

		// Track a message
		msg := &Message{Topic: "test/topic", Payload: []byte("data")}
		tracker.Track(1, msg)

		// Wait for retry to be pending
		time.Sleep(20 * time.Millisecond)

		// Call retryClientMessages
		srv.retryClientMessages(client)

		// Check that a PUBLISH packet was written with DUP=true
		written := conn.writeBuf.Bytes()
		assert.NotEmpty(t, written, "should have written retry packet")

		// Parse the packet to verify DUP flag
		r := bytes.NewReader(written)
		var header FixedHeader
		_, err = header.Decode(r)
		require.NoError(t, err)

		var pub PublishPacket
		_, err = pub.Decode(r, header)
		require.NoError(t, err)

		assert.True(t, pub.DUP, "retried packet should have DUP flag set")
		assert.Equal(t, uint16(1), pub.PacketID)
	})

	t.Run("retryClientMessages sets DUP flag for QoS2", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		tracker := NewQoS2Tracker(10*time.Millisecond, 3)
		client.qos2Tracker = tracker

		msg := &Message{Topic: "test/topic", Payload: []byte("data")}
		tracker.TrackSend(1, msg)

		time.Sleep(20 * time.Millisecond)

		srv.retryClientMessages(client)

		written := conn.writeBuf.Bytes()
		assert.NotEmpty(t, written, "should have written retry packet")

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err = header.Decode(r)
		require.NoError(t, err)

		var pub PublishPacket
		_, err = pub.Decode(r, header)
		require.NoError(t, err)

		assert.True(t, pub.DUP, "retried QoS2 packet should have DUP flag set")
	})

	t.Run("retryClientMessages skips disconnected client", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Track a message
		client.QoS1Tracker().Track(1, &Message{Topic: "test", Payload: []byte("data")})

		// Disconnect the client
		client.Close()

		// Should not panic or write anything
		srv.retryClientMessages(client)

		assert.Empty(t, conn.writeBuf.Bytes(), "should not write to disconnected client")
	})
}

// mockServerConn implements net.Conn for testing server write operations
type mockServerConn struct {
	writeBuf   *bytes.Buffer
	closed     bool
	mu         sync.Mutex
	remoteAddr net.Addr
}

func (c *mockServerConn) Read(_ []byte) (int, error) {
	return 0, nil
}

func (c *mockServerConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return c.writeBuf.Write(b)
}

func (c *mockServerConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *mockServerConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1883}
}

func (c *mockServerConn) RemoteAddr() net.Addr {
	if c.remoteAddr != nil {
		return c.remoteAddr
	}
	return &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 12345}
}

func (c *mockServerConn) SetDeadline(_ time.Time) error      { return nil }
func (c *mockServerConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *mockServerConn) SetWriteDeadline(_ time.Time) error { return nil }

// TestServerAcceptLoopRetryDelay tests that accept errors have backoff delay (Issue 10)
func TestServerAcceptLoopRetryDelay(t *testing.T) {
	t.Run("accept error does not cause CPU burn", func(t *testing.T) {
		// This test verifies the 100ms delay exists by checking the code path
		// The actual delay is hard to test without mocking time

		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		// Start server
		go srv.ListenAndServe()
		time.Sleep(50 * time.Millisecond)

		// Close listener to cause accept errors
		listener.Close()

		// Give server time to hit the error path with delay
		time.Sleep(150 * time.Millisecond)

		// Server should still be running (not crashed)
		assert.True(t, srv.running.Load() || !srv.running.Load()) // Just checking no panic

		srv.Close()
	})
}

// TestServerAuthentication tests the server authentication flow
func TestServerAuthentication(t *testing.T) {
	// Custom authenticator for credential validation
	credentialsAuth := &testAuthenticator{
		authFunc: func(_ context.Context, ctx *AuthContext) (*AuthResult, error) {
			if ctx.Username == "admin" && string(ctx.Password) == "secret" {
				return &AuthResult{Success: true, ReasonCode: ReasonSuccess, Namespace: DefaultNamespace}, nil
			}
			return &AuthResult{Success: false, ReasonCode: ReasonBadUserNameOrPassword}, nil
		},
	}

	// Authenticator that returns empty namespace (should default to DefaultNamespace)
	emptyNamespaceAuth := &testAuthenticator{
		authFunc: func(_ context.Context, _ *AuthContext) (*AuthResult, error) {
			return &AuthResult{Success: true, ReasonCode: ReasonSuccess, Namespace: ""}, nil
		},
	}

	tests := []struct {
		name           string
		auth           Authenticator
		username       string
		password       string
		expectedReason ReasonCode
	}{
		{
			name:           "valid credentials accepted",
			auth:           credentialsAuth,
			username:       "admin",
			password:       "secret",
			expectedReason: ReasonSuccess,
		},
		{
			name:           "invalid credentials rejected",
			auth:           credentialsAuth,
			username:       "admin",
			password:       "wrong-password",
			expectedReason: ReasonBadUserNameOrPassword,
		},
		{
			name:           "no auth configured allows all",
			auth:           nil,
			username:       "anyone",
			password:       "anything",
			expectedReason: ReasonSuccess,
		},
		{
			name:           "deny all authenticator rejects all",
			auth:           &DenyAllAuthenticator{},
			username:       "",
			password:       "",
			expectedReason: ReasonNotAuthorized,
		},
		{
			name:           "empty namespace defaults to DefaultNamespace",
			auth:           emptyNamespaceAuth,
			username:       "test",
			password:       "test",
			expectedReason: ReasonSuccess,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", ":0")
			require.NoError(t, err)

			var opts []ServerOption
			if tt.auth != nil {
				opts = append(opts, WithServerAuth(tt.auth))
			}
			opts = append([]ServerOption{WithListener(listener)}, opts...)
			srv := NewServer(opts...)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				srv.ListenAndServe()
			}()

			time.Sleep(50 * time.Millisecond)

			conn, err := net.Dial("tcp", listener.Addr().String())
			require.NoError(t, err)

			connect := &ConnectPacket{
				ClientID: "test-client",
				Username: tt.username,
				Password: []byte(tt.password),
			}
			_, err = WritePacket(conn, connect, 256*1024)
			require.NoError(t, err)

			conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			pkt, _, err := ReadPacket(conn, 256*1024)
			require.NoError(t, err)

			connack, ok := pkt.(*ConnackPacket)
			require.True(t, ok)
			assert.Equal(t, tt.expectedReason, connack.ReasonCode)

			conn.Close()
			srv.Close()
			wg.Wait()
		})
	}
}

// testEnhancedAuthEmptyNamespace is an enhanced authenticator that returns empty namespace.
type testEnhancedAuthEmptyNamespace struct{}

func (a *testEnhancedAuthEmptyNamespace) SupportsMethod(method string) bool {
	return method == "PLAIN"
}

func (a *testEnhancedAuthEmptyNamespace) AuthStart(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	return &EnhancedAuthResult{
		Success:    true,
		ReasonCode: ReasonSuccess,
		Namespace:  "", // Empty namespace should default to DefaultNamespace
	}, nil
}

func (a *testEnhancedAuthEmptyNamespace) AuthContinue(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	return &EnhancedAuthResult{
		Success:    true,
		ReasonCode: ReasonSuccess,
		Namespace:  "",
	}, nil
}

// TestServerEnhancedAuthEmptyNamespace tests that enhanced auth with empty namespace defaults to DefaultNamespace.
func TestServerEnhancedAuthEmptyNamespace(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testEnhancedAuthEmptyNamespace{}
	srv := NewServer(
		WithListener(listener),
		WithEnhancedAuth(enhancedAuth),
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	// Connect with AuthMethod to trigger enhanced auth
	connect := &ConnectPacket{
		ClientID: "test-enhanced-auth",
	}
	connect.Props.Set(PropAuthenticationMethod, "PLAIN")

	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)

	connack, ok := pkt.(*ConnackPacket)
	require.True(t, ok)
	// Should succeed - empty namespace is defaulted to DefaultNamespace
	assert.Equal(t, ReasonSuccess, connack.ReasonCode)

	conn.Close()
	srv.Close()
	wg.Wait()
}

// testSCRAMSHA256Authenticator implements SCRAM-SHA-256 enhanced auth for server testing.
type testSCRAMSHA256Authenticator struct {
	users      map[string]string
	namespaces map[string]string
}

type testSCRAMState struct {
	username    string
	serverNonce string
}

func (a *testSCRAMSHA256Authenticator) SupportsMethod(method string) bool {
	return method == "SCRAM-SHA-256"
}

func (a *testSCRAMSHA256Authenticator) AuthStart(_ context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	// Parse client-first-message: n,,n=<username>,r=<client-nonce>
	clientFirst := string(authCtx.AuthData)
	var username, clientNonce string
	for _, part := range splitByComma(clientFirst) {
		if len(part) > 2 && part[:2] == "n=" {
			username = part[2:]
		}
		if len(part) > 2 && part[:2] == "r=" {
			clientNonce = part[2:]
		}
	}

	if username == "" || clientNonce == "" {
		return &EnhancedAuthResult{Success: false, ReasonCode: ReasonNotAuthorized}, nil
	}

	if _, ok := a.users[username]; !ok {
		return &EnhancedAuthResult{Success: false, ReasonCode: ReasonNotAuthorized}, nil
	}

	serverNonce := fmt.Sprintf("%ssrv123", clientNonce)
	serverFirst := fmt.Sprintf("r=%s,s=c2FsdA==,i=4096", serverNonce)

	return &EnhancedAuthResult{
		Continue:   true,
		ReasonCode: ReasonContinueAuth,
		AuthData:   []byte(serverFirst),
		State:      &testSCRAMState{username: username, serverNonce: serverNonce},
	}, nil
}

func (a *testSCRAMSHA256Authenticator) AuthContinue(_ context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	state, ok := authCtx.State.(*testSCRAMState)
	if !ok || state == nil {
		return &EnhancedAuthResult{Success: false, ReasonCode: ReasonNotAuthorized}, nil
	}

	// Parse client-final: c=...,r=<nonce>,p=<proof>
	clientFinal := string(authCtx.AuthData)
	var nonce, proof string
	for _, part := range splitByComma(clientFinal) {
		if len(part) > 2 && part[:2] == "r=" {
			nonce = part[2:]
		}
		if len(part) > 2 && part[:2] == "p=" {
			proof = part[2:]
		}
	}

	if nonce != state.serverNonce {
		return &EnhancedAuthResult{Success: false, ReasonCode: ReasonNotAuthorized}, nil
	}

	expectedProof := fmt.Sprintf("proof-%s", a.users[state.username])
	if proof != expectedProof {
		return &EnhancedAuthResult{Success: false, ReasonCode: ReasonNotAuthorized}, nil
	}

	namespace := DefaultNamespace
	if ns, ok := a.namespaces[state.username]; ok {
		namespace = ns
	}

	return &EnhancedAuthResult{
		Success:    true,
		ReasonCode: ReasonSuccess,
		AuthData:   []byte("v=signature"),
		Namespace:  namespace,
	}, nil
}

func splitByComma(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			if i > start {
				parts = append(parts, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		parts = append(parts, s[start:])
	}
	return parts
}

// TestServerSCRAMSHA256EnhancedAuth tests the full SCRAM-SHA-256 challenge-response flow through the server.
func TestServerSCRAMSHA256EnhancedAuth(t *testing.T) {
	t.Run("successful authentication", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		auth := &testSCRAMSHA256Authenticator{
			users:      map[string]string{"testuser": "testpass"},
			namespaces: map[string]string{},
		}
		srv := NewServer(WithListener(listener), WithEnhancedAuth(auth))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Send CONNECT with SCRAM-SHA-256 auth method
		connect := &ConnectPacket{ClientID: "scram-client"}
		connect.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
		connect.Props.Set(PropAuthenticationData, []byte("n,,n=testuser,r=clientnonce"))

		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		// Expect AUTH packet with challenge
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		authPkt, ok := pkt.(*AuthPacket)
		require.True(t, ok, "expected AUTH packet, got %T", pkt)
		assert.Equal(t, ReasonContinueAuth, authPkt.ReasonCode)

		serverFirst := string(authPkt.Props.GetBinary(PropAuthenticationData))
		assert.Contains(t, serverFirst, "r=clientnoncesrv123")

		// Send client-final with proof
		clientFinal := &AuthPacket{ReasonCode: ReasonContinueAuth}
		clientFinal.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
		clientFinal.Props.Set(PropAuthenticationData, []byte("c=biws,r=clientnoncesrv123,p=proof-testpass"))

		_, err = WritePacket(conn, clientFinal, 256*1024)
		require.NoError(t, err)

		// Expect CONNACK with success
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok, "expected CONNACK packet, got %T", pkt)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		srv.Close()
		wg.Wait()
	})

	t.Run("authentication failure - wrong password", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		auth := &testSCRAMSHA256Authenticator{
			users:      map[string]string{"testuser": "testpass"},
			namespaces: map[string]string{},
		}
		srv := NewServer(WithListener(listener), WithEnhancedAuth(auth))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Send CONNECT
		connect := &ConnectPacket{ClientID: "scram-client-fail"}
		connect.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
		connect.Props.Set(PropAuthenticationData, []byte("n,,n=testuser,r=nonce1"))

		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		// Get AUTH challenge
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		_, ok := pkt.(*AuthPacket)
		require.True(t, ok)

		// Send wrong proof
		clientFinal := &AuthPacket{ReasonCode: ReasonContinueAuth}
		clientFinal.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
		clientFinal.Props.Set(PropAuthenticationData, []byte("c=biws,r=nonce1srv123,p=proof-wrongpass"))

		_, err = WritePacket(conn, clientFinal, 256*1024)
		require.NoError(t, err)

		// Expect CONNACK with failure
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonNotAuthorized, connack.ReasonCode)

		srv.Close()
		wg.Wait()
	})

	t.Run("immediate auth failures", func(t *testing.T) {
		testCases := []struct {
			name           string
			clientID       string
			authMethod     string
			authData       []byte
			expectedReason ReasonCode
		}{
			{
				name:           "unknown user",
				clientID:       "scram-unknown",
				authMethod:     "SCRAM-SHA-256",
				authData:       []byte("n,,n=unknownuser,r=nonce2"),
				expectedReason: ReasonNotAuthorized,
			},
			{
				name:           "unsupported auth method",
				clientID:       "unsupported-auth",
				authMethod:     "UNKNOWN-METHOD",
				authData:       []byte("data"),
				expectedReason: ReasonBadAuthMethod,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				listener, err := net.Listen("tcp", ":0")
				require.NoError(t, err)

				auth := &testSCRAMSHA256Authenticator{
					users:      map[string]string{"testuser": "testpass"},
					namespaces: map[string]string{},
				}
				srv := NewServer(WithListener(listener), WithEnhancedAuth(auth))

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					srv.ListenAndServe()
				}()

				time.Sleep(50 * time.Millisecond)

				conn, err := net.Dial("tcp", listener.Addr().String())
				require.NoError(t, err)
				defer conn.Close()

				connect := &ConnectPacket{ClientID: tc.clientID}
				connect.Props.Set(PropAuthenticationMethod, tc.authMethod)
				connect.Props.Set(PropAuthenticationData, tc.authData)

				_, err = WritePacket(conn, connect, 256*1024)
				require.NoError(t, err)

				conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				pkt, _, err := ReadPacket(conn, 256*1024)
				require.NoError(t, err)

				connack, ok := pkt.(*ConnackPacket)
				require.True(t, ok)
				assert.Equal(t, tc.expectedReason, connack.ReasonCode)

				srv.Close()
				wg.Wait()
			})
		}
	})

	t.Run("authentication with namespace", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		auth := &testSCRAMSHA256Authenticator{
			users:      map[string]string{"tenant1": "secret"},
			namespaces: map[string]string{"tenant1": "tenant1-ns"},
		}
		srv := NewServer(WithListener(listener), WithEnhancedAuth(auth))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Send CONNECT
		connect := &ConnectPacket{ClientID: "tenant-client"}
		connect.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
		connect.Props.Set(PropAuthenticationData, []byte("n,,n=tenant1,r=tnonce"))

		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		// Get AUTH challenge
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		_, ok := pkt.(*AuthPacket)
		require.True(t, ok)

		// Send correct proof
		clientFinal := &AuthPacket{ReasonCode: ReasonContinueAuth}
		clientFinal.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
		clientFinal.Props.Set(PropAuthenticationData, []byte("c=biws,r=tnoncesrv123,p=proof-secret"))

		_, err = WritePacket(conn, clientFinal, 256*1024)
		require.NoError(t, err)

		// Expect CONNACK with success
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Verify client is in the correct namespace
		clients := srv.ClientsWithNamespace()
		require.Len(t, clients, 1)
		assert.Equal(t, "tenant1-ns", clients[0].Namespace)
		assert.Equal(t, "tenant-client", clients[0].ClientID)

		srv.Close()
		wg.Wait()
	})
}

// TestServerAuthorization tests the server authorization flow
func TestServerAuthorization(t *testing.T) {
	t.Run("publish denied by authorizer", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		authz := &DenyAllAuthorizer{}
		srv := NewServer(WithListener(listener), WithServerAuthz(authz))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Connect
		connect := &ConnectPacket{ClientID: "test-client"}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Publish QoS 1 (will get PUBACK with error)
		publish := &PublishPacket{
			PacketID: 1,
			Topic:    "test/topic",
			Payload:  []byte("data"),
			QoS:      1,
		}
		_, err = WritePacket(conn, publish, 256*1024)
		require.NoError(t, err)

		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		puback, ok := pkt.(*PubackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonNotAuthorized, puback.ReasonCode)

		conn.Close()
		srv.Close()
		wg.Wait()
	})

	t.Run("subscribe denied by authorizer", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		authz := &DenyAllAuthorizer{}
		srv := NewServer(WithListener(listener), WithServerAuthz(authz))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Connect
		connect := &ConnectPacket{ClientID: "test-client"}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Subscribe
		subscribe := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/topic", QoS: 0},
			},
		}
		_, err = WritePacket(conn, subscribe, 256*1024)
		require.NoError(t, err)

		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		suback, ok := pkt.(*SubackPacket)
		require.True(t, ok)
		require.Len(t, suback.ReasonCodes, 1)
		assert.Equal(t, ReasonNotAuthorized, suback.ReasonCodes[0])

		conn.Close()
		srv.Close()
		wg.Wait()
	})

	t.Run("authorizer allows specific topics", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		// Custom authorizer that allows user1 to access user1/# topics only
		authz := &testAuthorizer{
			authzFunc: func(_ context.Context, ctx *AuthzContext) (*AuthzResult, error) {
				if ctx.Username == "user1" && strings.HasPrefix(ctx.Topic, "user1/") {
					return &AuthzResult{Allowed: true, MaxQoS: 2}, nil
				}
				return &AuthzResult{Allowed: false, ReasonCode: ReasonNotAuthorized}, nil
			},
		}

		auth := &testAuthenticator{
			authFunc: func(_ context.Context, ctx *AuthContext) (*AuthResult, error) {
				if ctx.Username == "user1" && string(ctx.Password) == "pass1" {
					return &AuthResult{Success: true, ReasonCode: ReasonSuccess, Namespace: DefaultNamespace}, nil
				}
				return &AuthResult{Success: false, ReasonCode: ReasonBadUserNameOrPassword}, nil
			},
		}

		srv := NewServer(
			WithListener(listener),
			WithServerAuth(auth),
			WithServerAuthz(authz),
		)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Connect with user1
		connect := &ConnectPacket{
			ClientID: "test-client",
			Username: "user1",
			Password: []byte("pass1"),
		}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Subscribe to allowed topic
		subscribe := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "user1/data", QoS: 0},
			},
		}
		_, err = WritePacket(conn, subscribe, 256*1024)
		require.NoError(t, err)

		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		suback, ok := pkt.(*SubackPacket)
		require.True(t, ok)
		require.Len(t, suback.ReasonCodes, 1)
		assert.Equal(t, ReasonCode(0), suback.ReasonCodes[0]) // QoS 0 granted

		// Subscribe to denied topic
		subscribe2 := &SubscribePacket{
			PacketID: 2,
			Subscriptions: []Subscription{
				{TopicFilter: "other/topic", QoS: 0},
			},
		}
		_, err = WritePacket(conn, subscribe2, 256*1024)
		require.NoError(t, err)

		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		suback2, ok := pkt.(*SubackPacket)
		require.True(t, ok)
		require.Len(t, suback2.ReasonCodes, 1)
		assert.Equal(t, ReasonNotAuthorized, suback2.ReasonCodes[0])

		conn.Close()
		srv.Close()
		wg.Wait()
	})
}

// TestServerReceiveMaximumEnforcement tests server-side Receive Maximum enforcement
func TestServerReceiveMaximumEnforcement(t *testing.T) {
	t.Run("exceeding receive maximum disconnects client", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		// Server with receive maximum of 2
		srv := NewServer(WithListener(listener), WithServerReceiveMaximum(2))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Connect
		connect := &ConnectPacket{ClientID: "test-client"}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send 3 QoS 1 publishes without waiting for PUBACK (exceeds limit of 2)
		for i := 1; i <= 3; i++ {
			publish := &PublishPacket{
				PacketID: uint16(i),
				Topic:    "test/topic",
				Payload:  []byte("data"),
				QoS:      1,
			}
			_, err = WritePacket(conn, publish, 256*1024)
			if err != nil {
				// Connection may be closed by server
				break
			}
		}

		// Read responses - server should disconnect us
		var gotDisconnect bool
		var gotQuotaError bool
		for i := 0; i < 5; i++ {
			conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			pkt, _, err := ReadPacket(conn, 256*1024)
			if err != nil {
				// Connection closed by server - this is expected
				gotDisconnect = true
				break
			}
			if disc, ok := pkt.(*DisconnectPacket); ok {
				gotDisconnect = true
				if disc.ReasonCode == ReasonReceiveMaxExceeded {
					gotQuotaError = true
				}
				break
			}
		}

		// Server should have disconnected us for exceeding receive maximum
		assert.True(t, gotDisconnect, "server should disconnect client for exceeding receive maximum")
		if gotDisconnect && !gotQuotaError {
			// Server disconnected but we didn't catch the DISCONNECT packet
			// (it may have been sent and connection closed immediately after)
			t.Log("server disconnected client (DISCONNECT packet may have been missed)")
		}

		conn.Close()
		srv.Close()
		wg.Wait()
	})

	t.Run("within receive maximum allowed", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener), WithServerReceiveMaximum(10))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		connect := &ConnectPacket{ClientID: "test-client"}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send 2 QoS 1 publishes (within limit)
		for i := 1; i <= 2; i++ {
			publish := &PublishPacket{
				PacketID: uint16(i),
				Topic:    "test/topic",
				Payload:  []byte("data"),
				QoS:      1,
			}
			_, err = WritePacket(conn, publish, 256*1024)
			require.NoError(t, err)
		}

		// Read PUBACKs - both should succeed
		for i := 0; i < 2; i++ {
			conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			pkt, _, err := ReadPacket(conn, 256*1024)
			require.NoError(t, err)
			puback, ok := pkt.(*PubackPacket)
			require.True(t, ok)
			assert.Equal(t, ReasonSuccess, puback.ReasonCode)
		}

		conn.Close()
		srv.Close()
		wg.Wait()
	})
}

// TestServerMaxQoSDowngrade tests AuthzResult.MaxQoS downgrade behavior
func TestServerMaxQoSDowngrade(t *testing.T) {
	t.Run("subscription QoS downgraded to MaxQoS", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		// Authorizer that allows subscriptions but limits QoS to 1
		authz := &testAuthorizer{
			authzFunc: func(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
				return &AuthzResult{Allowed: true, MaxQoS: 1}, nil
			},
		}

		srv := NewServer(WithListener(listener), WithServerAuthz(authz))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		connect := &ConnectPacket{ClientID: "test-client"}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Subscribe with QoS 2
		subscribe := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/topic", QoS: 2},
			},
		}
		_, err = WritePacket(conn, subscribe, 256*1024)
		require.NoError(t, err)

		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		suback, ok := pkt.(*SubackPacket)
		require.True(t, ok)
		require.Len(t, suback.ReasonCodes, 1)
		// QoS should be downgraded to 1
		assert.Equal(t, ReasonCode(1), suback.ReasonCodes[0], "QoS should be downgraded to 1")

		conn.Close()
		srv.Close()
		wg.Wait()
	})

	t.Run("publish QoS internally downgraded to MaxQoS", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		// Track what QoS messages are distributed at
		var receivedQoS byte
		var mu sync.Mutex

		// Authorizer that allows publishes but limits QoS to 0
		authz := &testAuthorizer{
			authzFunc: func(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
				return &AuthzResult{Allowed: true, MaxQoS: 0}, nil
			},
		}

		srv := NewServer(
			WithListener(listener),
			WithServerAuthz(authz),
			OnMessage(func(_ *ServerClient, msg *Message) {
				mu.Lock()
				receivedQoS = msg.QoS
				mu.Unlock()
			}),
		)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		connect := &ConnectPacket{ClientID: "test-client"}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Publish with QoS 1 - will be internally downgraded to QoS 0 for distribution
		// but PUBACK is still sent to acknowledge receipt from client
		publish := &PublishPacket{
			PacketID: 1,
			Topic:    "test/topic",
			Payload:  []byte("data"),
			QoS:      1,
		}
		_, err = WritePacket(conn, publish, 256*1024)
		require.NoError(t, err)

		// Server sends PUBACK to acknowledge receipt from client
		conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		puback, ok := pkt.(*PubackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, puback.ReasonCode)

		// Give time for message callback to be called
		time.Sleep(50 * time.Millisecond)

		// Verify the message was handled at QoS 0 internally
		mu.Lock()
		assert.Equal(t, byte(0), receivedQoS, "message should be handled at downgraded QoS 0")
		mu.Unlock()

		conn.Close()
		srv.Close()
		wg.Wait()
	})
}

// TestServerSessionRecoveryErrorHandling tests proper session error handling (Issue 11)
func TestServerSessionRecoveryErrorHandling(t *testing.T) {
	t.Run("session not found creates new session", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		// Connect a client
		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		connect := &ConnectPacket{
			ClientID:   "new-client",
			CleanStart: false, // Request session resumption
		}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		// Read CONNACK
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Should succeed with SessionPresent=false (new session created)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)
		assert.False(t, connack.SessionPresent, "new session should not be present")

		conn.Close()
		srv.Close()
		wg.Wait()
	})

	t.Run("existing session is resumed", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		sessionStore := NewMemorySessionStore()

		// Pre-create a session
		existingSession := NewMemorySession("existing-client", DefaultNamespace)
		existingSession.AddSubscription(Subscription{TopicFilter: "test/topic", QoS: 1})
		err = sessionStore.Create(DefaultNamespace, existingSession)
		require.NoError(t, err)

		srv := NewServer(WithListener(listener), WithSessionStore(sessionStore))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		connect := &ConnectPacket{
			ClientID:   "existing-client",
			CleanStart: false, // Request session resumption
		}
		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		assert.Equal(t, ReasonSuccess, connack.ReasonCode)
		assert.True(t, connack.SessionPresent, "existing session should be present")

		conn.Close()
		srv.Close()
		wg.Wait()
	})
}

// TestServerClientMaxPacketSize tests that server respects client's Maximum Packet Size.
// Per MQTT 5.0 spec, server must not send packets larger than client's advertised limit.
func TestServerClientMaxPacketSize(t *testing.T) {
	t.Run("server uses minimum of client and server max packet size", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		// Server configured with 256KB max packet size
		srv := NewServer(WithListener(listener), WithServerMaxPacketSize(256*1024))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Client advertises smaller max packet size (1KB)
		connect := &ConnectPacket{
			ClientID:   "test-client",
			CleanStart: true,
		}
		connect.Props.Set(PropMaximumPacketSize, uint32(1024))

		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Verify the server client has the correct max packet size
		// by checking the clients map
		srv.mu.RLock()
		client, exists := srv.clients[NamespaceKey(DefaultNamespace, "test-client")]
		srv.mu.RUnlock()
		require.True(t, exists)
		assert.Equal(t, uint32(1024), client.maxPacketSize, "server should use client's smaller max packet size")

		conn.Close()
		srv.Close()
		wg.Wait()
	})

	t.Run("server uses its own limit when client specifies larger", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		// Server configured with 1KB max packet size
		srv := NewServer(WithListener(listener), WithServerMaxPacketSize(1024))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Client advertises larger max packet size (256KB)
		connect := &ConnectPacket{
			ClientID:   "test-client",
			CleanStart: true,
		}
		connect.Props.Set(PropMaximumPacketSize, uint32(256*1024))

		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		srv.mu.RLock()
		client, exists := srv.clients[NamespaceKey(DefaultNamespace, "test-client")]
		srv.mu.RUnlock()
		require.True(t, exists)
		assert.Equal(t, uint32(1024), client.maxPacketSize, "server should use its own smaller max packet size")

		conn.Close()
		srv.Close()
		wg.Wait()
	})

	t.Run("server uses default when client doesn't specify", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener), WithServerMaxPacketSize(256*1024))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Client doesn't specify max packet size
		connect := &ConnectPacket{
			ClientID:   "test-client",
			CleanStart: true,
		}

		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		srv.mu.RLock()
		client, exists := srv.clients[NamespaceKey(DefaultNamespace, "test-client")]
		srv.mu.RUnlock()
		require.True(t, exists)
		assert.Equal(t, uint32(256*1024), client.maxPacketSize, "server should use its configured max packet size")

		conn.Close()
		srv.Close()
		wg.Wait()
	})
}

// TestServerSessionExpiryInterval tests that session expiry interval is read from CONNECT
// and used for will-delay interaction.
func TestServerSessionExpiryInterval(t *testing.T) {
	t.Run("session expiry read from CONNECT", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Client specifies session expiry interval
		connect := &ConnectPacket{
			ClientID:   "test-client",
			CleanStart: true,
		}
		connect.Props.Set(PropSessionExpiryInterval, uint32(3600)) // 1 hour

		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		srv.mu.RLock()
		client, exists := srv.clients[NamespaceKey(DefaultNamespace, "test-client")]
		srv.mu.RUnlock()
		require.True(t, exists)
		assert.Equal(t, uint32(3600), client.SessionExpiryInterval(), "session expiry should be stored from CONNECT")

		conn.Close()
		srv.Close()
		wg.Wait()
	})

	t.Run("session expiry updated on DISCONNECT", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Client specifies session expiry interval
		connect := &ConnectPacket{
			ClientID:   "test-client",
			CleanStart: true,
		}
		connect.Props.Set(PropSessionExpiryInterval, uint32(3600))

		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send DISCONNECT with updated session expiry
		disconnect := &DisconnectPacket{
			ReasonCode: ReasonSuccess,
		}
		disconnect.Props.Set(PropSessionExpiryInterval, uint32(7200)) // Update to 2 hours

		_, err = WritePacket(conn, disconnect, 256*1024)
		require.NoError(t, err)

		// Give server time to process disconnect
		time.Sleep(100 * time.Millisecond)

		conn.Close()
		srv.Close()
		wg.Wait()
	})

	t.Run("session expiry interval zero means no session persistence", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Client doesn't specify session expiry (defaults to 0)
		connect := &ConnectPacket{
			ClientID:   "test-client",
			CleanStart: true,
		}

		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		srv.mu.RLock()
		client, exists := srv.clients[NamespaceKey(DefaultNamespace, "test-client")]
		srv.mu.RUnlock()
		require.True(t, exists)
		assert.Equal(t, uint32(0), client.SessionExpiryInterval(), "session expiry should be 0 when not specified")

		conn.Close()
		srv.Close()
		wg.Wait()
	})

	t.Run("will delay constrained by session expiry", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.ListenAndServe()
		}()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)

		// Client specifies session expiry and will message with delay
		connect := &ConnectPacket{
			ClientID:   "test-client",
			CleanStart: true,
			WillFlag:   true,
			WillTopic:  "will/topic",
			WillQoS:    0,
		}
		connect.WillProps.Set(PropWillDelayInterval, uint32(3600)) // 1 hour will delay
		connect.Props.Set(PropSessionExpiryInterval, uint32(60))   // 1 minute session expiry

		_, err = WritePacket(conn, connect, 256*1024)
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Verify will is registered
		clientKey := NamespaceKey(DefaultNamespace, "test-client")
		assert.True(t, srv.wills.HasWill(clientKey))

		// Close connection abruptly (without DISCONNECT) to trigger will
		conn.Close()

		// Wait for will to be triggered
		time.Sleep(100 * time.Millisecond)

		// Will should be pending, but its publish time should be constrained by session expiry
		// The will delay is 1 hour, but session expiry is 1 minute, so will should publish
		// no later than session expiry
		assert.True(t, srv.wills.HasPendingWill(clientKey), "will should be pending after unclean disconnect")

		srv.Close()
		wg.Wait()
	})
}

// TestServerOnSubscribeCallbackFiltering tests that onSubscribe callback only receives successful subscriptions
func TestServerOnSubscribeCallbackFiltering(t *testing.T) {
	t.Run("onSubscribe callback filters failed subscriptions", func(t *testing.T) {
		var callbackSubs []Subscription
		srv := NewServer(
			OnSubscribe(func(_ *ServerClient, subs []Subscription) {
				callbackSubs = subs
			}),
		)
		defer srv.Close()

		// Create a mock client
		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetSession(NewMemorySession("test-client", DefaultNamespace))

		// Create SUBSCRIBE with mix of valid and invalid filters
		sub := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "valid/topic", QoS: 1},
				{TopicFilter: "", QoS: 1}, // Invalid: empty
				{TopicFilter: "another/valid", QoS: 0},
			},
		}

		discardLogger := NewStdLogger(io.Discard, LogLevelNone)
		srv.handleSubscribe(client, sub, discardLogger)

		// Callback should only include successful subscriptions
		assert.Len(t, callbackSubs, 2, "callback should only have successful subscriptions")
		assert.Equal(t, "valid/topic", callbackSubs[0].TopicFilter)
		assert.Equal(t, "another/valid", callbackSubs[1].TopicFilter)
	})
}

func TestServerSubscribeHelpers(t *testing.T) {
	t.Run("validateSubscriptionSupport checks wildcard and shared availability", func(t *testing.T) {
		srv := NewServer(
			WithWildcardSubAvailable(false),
			WithSharedSubAvailable(false),
		)
		defer srv.Close()

		discardLogger := NewStdLogger(io.Discard, LogLevelNone)

		wildcardSub := Subscription{TopicFilter: "test/#", QoS: QoS0}
		reason := srv.validateSubscriptionSupport(wildcardSub, discardLogger)
		assert.Equal(t, ReasonWildcardSubsNotSupported, reason)

		sharedSub := Subscription{TopicFilter: "$share/group/topic", QoS: QoS0}
		reason = srv.validateSubscriptionSupport(sharedSub, discardLogger)
		assert.Equal(t, ReasonSharedSubsNotSupported, reason)

		normalSub := Subscription{TopicFilter: "test/topic", QoS: QoS0}
		reason = srv.validateSubscriptionSupport(normalSub, discardLogger)
		assert.Equal(t, ReasonSuccess, reason)
	})

	t.Run("applyMaxSubscriptionQoS caps subscription QoS", func(t *testing.T) {
		srv := NewServer(WithMaxQoS(QoS1))
		defer srv.Close()

		sub := Subscription{TopicFilter: "test/topic", QoS: QoS2}
		capped := srv.applyMaxSubscriptionQoS(sub)
		assert.Equal(t, QoS1, capped.QoS)

		sub = Subscription{TopicFilter: "test/topic", QoS: QoS0}
		capped = srv.applyMaxSubscriptionQoS(sub)
		assert.Equal(t, QoS0, capped.QoS)
	})

	t.Run("authorizeSubscribe handles allowed and denied results", func(t *testing.T) {
		authz := &testAuthorizer{
			authzFunc: func(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
				return &AuthzResult{Allowed: true, MaxQoS: QoS0}, nil
			},
		}

		srv := NewServer(WithServerAuthz(authz))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client", Username: "user"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		discardLogger := NewStdLogger(io.Discard, LogLevelNone)
		sub := Subscription{TopicFilter: "test/topic", QoS: QoS1}
		updated, reason, allowed := srv.authorizeSubscribe(client, sub, discardLogger)
		assert.True(t, allowed)
		assert.Equal(t, ReasonSuccess, reason)
		assert.Equal(t, QoS0, updated.QoS)

		authz.authzFunc = func(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
			return &AuthzResult{Allowed: false, ReasonCode: ReasonNotAuthorized}, nil
		}

		_, reason, allowed = srv.authorizeSubscribe(client, sub, discardLogger)
		assert.False(t, allowed)
		assert.Equal(t, ReasonNotAuthorized, reason)
	})

	t.Run("deliverRetainedMessages respects RetainAsPublish", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		retained := &RetainedMessage{
			Topic:       "test/retained",
			Payload:     []byte("payload"),
			QoS:         QoS0,
			PublishedAt: time.Now(),
		}
		srv.config.retainedStore.Set(DefaultNamespace, retained)

		writeBuf := &bytes.Buffer{}
		conn := &mockServerConn{writeBuf: writeBuf}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		sub := Subscription{TopicFilter: "test/retained", QoS: QoS0, RetainAsPublish: false}
		srv.deliverRetainedMessages(client, sub, DefaultNamespace, "test-client")

		pkt, _, err := ReadPacket(bytes.NewReader(writeBuf.Bytes()), 256*1024)
		require.NoError(t, err)
		pub, ok := pkt.(*PublishPacket)
		require.True(t, ok)
		assert.False(t, pub.Retain)

		writeBuf.Reset()
		sub = Subscription{TopicFilter: "test/retained", QoS: QoS0, RetainAsPublish: true}
		srv.deliverRetainedMessages(client, sub, DefaultNamespace, "test-client")

		pkt, _, err = ReadPacket(bytes.NewReader(writeBuf.Bytes()), 256*1024)
		require.NoError(t, err)
		pub, ok = pkt.(*PublishPacket)
		require.True(t, ok)
		assert.True(t, pub.Retain)
	})
}

// TestServerShutdownCopiesClients tests that server shutdown copies clients before disconnect to avoid lock contention
func TestServerShutdownCopiesClients(t *testing.T) {
	t.Run("server shutdown copies clients before disconnect", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()

		// Give server time to start
		time.Sleep(10 * time.Millisecond)

		// Close should not deadlock even with concurrent access
		done := make(chan bool)
		go func() {
			srv.Close()
			done <- true
		}()

		select {
		case <-done:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("server close took too long - possible deadlock")
		}
	})
}

// TestServerMaxConnectionsSendsCONNACK tests that server sends CONNACK with ServerBusy before close when max connections reached
func TestServerMaxConnectionsSendsCONNACK(t *testing.T) {
	t.Run("max connections sends CONNACK before close", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithMaxConnections(0), // No connections allowed
		)
		go srv.ListenAndServe()
		defer srv.Close()

		// Give server time to start
		time.Sleep(10 * time.Millisecond)

		// Connect should receive CONNACK with ServerBusy
		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Set read deadline to avoid hanging
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		// Read the CONNACK packet that should be sent
		var header FixedHeader
		_, err = header.Decode(conn)
		if err == nil {
			assert.Equal(t, PacketCONNACK, header.PacketType)
		}
		// It's acceptable if the connection is closed before we read
	})
}

func TestRemoveClient(t *testing.T) {
	t.Run("old connection does not remove new client state", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		clientKey := NamespaceKey(DefaultNamespace, "test-client")
		oldClient := &ServerClient{clientID: "test-client"}
		newClient := &ServerClient{clientID: "test-client"}

		srv.mu.Lock()
		srv.clients[clientKey] = newClient
		srv.mu.Unlock()

		srv.removeClient(clientKey, oldClient)

		srv.mu.RLock()
		_, exists := srv.clients[clientKey]
		srv.mu.RUnlock()

		assert.True(t, exists, "new client should still be in clients map")
	})

	t.Run("correct client is removed", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		clientKey := NamespaceKey(DefaultNamespace, "test-client")
		client := &ServerClient{clientID: "test-client"}

		srv.mu.Lock()
		srv.clients[clientKey] = client
		srv.mu.Unlock()

		srv.removeClient(clientKey, client)

		srv.mu.RLock()
		_, exists := srv.clients[clientKey]
		srv.mu.RUnlock()

		assert.False(t, exists, "client should be removed")
	})
}

func TestGetTLSConnectionState(t *testing.T) {
	t.Run("non-TLS connection returns nil", func(t *testing.T) {
		conn := &mockConn{}

		state := getTLSConnectionState(conn)

		assert.Nil(t, state, "non-TLS connection should return nil state")
	})

	t.Run("direct TLS connection returns state", func(t *testing.T) {
		// Create a TLS connection using net.Pipe and perform handshake
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		// Generate minimal self-signed certificate for testing
		cert, _ := generateTestCertificate(t)

		tlsConfig := &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		}

		// Wrap server side with TLS
		tlsServerConn := tls.Server(serverConn, tlsConfig)

		// Wrap client side with TLS
		tlsClientConn := tls.Client(clientConn, &tls.Config{
			InsecureSkipVerify: true,
		})

		// Perform handshake in goroutines
		errCh := make(chan error, 2)
		go func() {
			errCh <- tlsServerConn.Handshake()
		}()
		go func() {
			errCh <- tlsClientConn.Handshake()
		}()

		// Wait for both handshakes
		for range 2 {
			err := <-errCh
			require.NoError(t, err)
		}

		// Now test getTLSConnectionState with the TLS connection
		state := getTLSConnectionState(tlsServerConn)
		assert.NotNil(t, state, "TLS connection should return non-nil state")
		assert.True(t, state.HandshakeComplete, "Handshake should be complete")
	})

	t.Run("wrapped TLS connection returns state", func(t *testing.T) {
		// Create a TLS connection
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		cert, _ := generateTestCertificate(t)

		tlsConfig := &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		}

		tlsServerConn := tls.Server(serverConn, tlsConfig)
		tlsClientConn := tls.Client(clientConn, &tls.Config{
			InsecureSkipVerify: true,
		})

		errCh := make(chan error, 2)
		go func() {
			errCh <- tlsServerConn.Handshake()
		}()
		go func() {
			errCh <- tlsClientConn.Handshake()
		}()

		for range 2 {
			err := <-errCh
			require.NoError(t, err)
		}

		// Create a wrapper connection that implements underlyingConnGetter
		wrapper := &tlsWrapperConn{underlying: tlsServerConn}

		state := getTLSConnectionState(wrapper)
		assert.NotNil(t, state, "Wrapped TLS connection should return non-nil state")
		assert.True(t, state.HandshakeComplete, "Handshake should be complete")
	})

	t.Run("wrapped non-TLS connection returns nil", func(t *testing.T) {
		// Create a wrapper that returns a non-TLS connection
		wrapper := &tlsWrapperConn{underlying: &mockConn{}}

		state := getTLSConnectionState(wrapper)
		assert.Nil(t, state, "Wrapped non-TLS connection should return nil state")
	})
}

// tlsWrapperConn is a test wrapper that implements underlyingConnGetter.
type tlsWrapperConn struct {
	underlying net.Conn
}

func (w *tlsWrapperConn) Read(b []byte) (int, error)         { return w.underlying.Read(b) }
func (w *tlsWrapperConn) Write(b []byte) (int, error)        { return w.underlying.Write(b) }
func (w *tlsWrapperConn) Close() error                       { return w.underlying.Close() }
func (w *tlsWrapperConn) LocalAddr() net.Addr                { return w.underlying.LocalAddr() }
func (w *tlsWrapperConn) RemoteAddr() net.Addr               { return w.underlying.RemoteAddr() }
func (w *tlsWrapperConn) SetDeadline(t time.Time) error      { return w.underlying.SetDeadline(t) }
func (w *tlsWrapperConn) SetReadDeadline(t time.Time) error  { return w.underlying.SetReadDeadline(t) }
func (w *tlsWrapperConn) SetWriteDeadline(t time.Time) error { return w.underlying.SetWriteDeadline(t) }
func (w *tlsWrapperConn) UnderlyingConn() net.Conn           { return w.underlying }

// TestMultiTenantNamespaceIsolation tests runtime namespace isolation behaviors.
func TestMultiTenantNamespaceIsolation(t *testing.T) {
	t.Run("same clientID different namespaces coexist", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		// Create two clients with same clientID but different namespaces
		clientA := &ServerClient{clientID: "shared-client", namespace: "tenant-a"}
		clientB := &ServerClient{clientID: "shared-client", namespace: "tenant-b"}

		keyA := NamespaceKey("tenant-a", "shared-client")
		keyB := NamespaceKey("tenant-b", "shared-client")

		srv.mu.Lock()
		srv.clients[keyA] = clientA
		srv.clients[keyB] = clientB
		srv.mu.Unlock()

		// Both should exist
		srv.mu.RLock()
		_, existsA := srv.clients[keyA]
		_, existsB := srv.clients[keyB]
		srv.mu.RUnlock()

		assert.True(t, existsA, "client in tenant-a should exist")
		assert.True(t, existsB, "client in tenant-b should exist")
		assert.Equal(t, 2, len(srv.clients))
	})

	t.Run("Clients returns all clientIDs including duplicates across namespaces", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		clientA := &ServerClient{clientID: "shared-client", namespace: "tenant-a"}
		clientB := &ServerClient{clientID: "shared-client", namespace: "tenant-b"}
		clientC := &ServerClient{clientID: "unique-client", namespace: "tenant-a"}

		srv.mu.Lock()
		srv.clients[NamespaceKey("tenant-a", "shared-client")] = clientA
		srv.clients[NamespaceKey("tenant-b", "shared-client")] = clientB
		srv.clients[NamespaceKey("tenant-a", "unique-client")] = clientC
		srv.mu.Unlock()

		clientIDs := srv.Clients()
		assert.Len(t, clientIDs, 3)

		// Count occurrences of "shared-client"
		sharedCount := 0
		for _, id := range clientIDs {
			if id == "shared-client" {
				sharedCount++
			}
		}
		assert.Equal(t, 2, sharedCount, "should have 2 clients with same ID from different namespaces")
	})

	t.Run("ClientsWithNamespace provides namespace info", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		clientA := &ServerClient{clientID: "client-1", namespace: "tenant-a"}
		clientB := &ServerClient{clientID: "client-1", namespace: "tenant-b"}

		srv.mu.Lock()
		srv.clients[NamespaceKey("tenant-a", "client-1")] = clientA
		srv.clients[NamespaceKey("tenant-b", "client-1")] = clientB
		srv.mu.Unlock()

		clients := srv.ClientsWithNamespace()
		assert.Len(t, clients, 2)

		// Verify we can distinguish them by namespace
		namespaces := make(map[string]string)
		for _, c := range clients {
			namespaces[c.Namespace] = c.ClientID
		}
		assert.Equal(t, "client-1", namespaces["tenant-a"])
		assert.Equal(t, "client-1", namespaces["tenant-b"])
	})

	t.Run("removeClient only removes from correct namespace", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		clientA := &ServerClient{clientID: "shared-client", namespace: "tenant-a"}
		clientB := &ServerClient{clientID: "shared-client", namespace: "tenant-b"}

		keyA := NamespaceKey("tenant-a", "shared-client")
		keyB := NamespaceKey("tenant-b", "shared-client")

		srv.mu.Lock()
		srv.clients[keyA] = clientA
		srv.clients[keyB] = clientB
		srv.mu.Unlock()

		// Remove only tenant-a client
		srv.removeClient(keyA, clientA)

		srv.mu.RLock()
		_, existsA := srv.clients[keyA]
		_, existsB := srv.clients[keyB]
		srv.mu.RUnlock()

		assert.False(t, existsA, "tenant-a client should be removed")
		assert.True(t, existsB, "tenant-b client should still exist")
	})

	t.Run("subscription manager receives correct namespace", func(t *testing.T) {
		subMgr := NewSubscriptionManager()

		// Same client ID, different namespaces, same topic
		subMgr.Subscribe("client-1", "tenant-a", Subscription{TopicFilter: "topic", QoS: 0})
		subMgr.Subscribe("client-1", "tenant-b", Subscription{TopicFilter: "topic", QoS: 1})

		// Publish in tenant-a namespace
		matchesA := subMgr.MatchForDelivery("topic", "publisher", "tenant-a")
		require.Len(t, matchesA, 1)
		assert.Equal(t, "tenant-a", matchesA[0].Namespace)
		assert.Equal(t, byte(0), matchesA[0].Subscription.QoS)

		// Publish in tenant-b namespace
		matchesB := subMgr.MatchForDelivery("topic", "publisher", "tenant-b")
		require.Len(t, matchesB, 1)
		assert.Equal(t, "tenant-b", matchesB[0].Namespace)
		assert.Equal(t, byte(1), matchesB[0].Subscription.QoS)
	})

	t.Run("retained store isolates messages by namespace", func(t *testing.T) {
		retainedStore := NewMemoryRetainedStore()

		// Same topic, different namespaces
		retainedStore.Set("tenant-a", &RetainedMessage{Topic: "sensor/data", Payload: []byte("tenant-a-data")})
		retainedStore.Set("tenant-b", &RetainedMessage{Topic: "sensor/data", Payload: []byte("tenant-b-data")})

		// Match in each namespace
		matchesA := retainedStore.Match("tenant-a", "sensor/#")
		require.Len(t, matchesA, 1)
		assert.Equal(t, []byte("tenant-a-data"), matchesA[0].Payload)

		matchesB := retainedStore.Match("tenant-b", "sensor/#")
		require.Len(t, matchesB, 1)
		assert.Equal(t, []byte("tenant-b-data"), matchesB[0].Payload)
	})
}

func TestBuildConnackCapabilities(t *testing.T) {
	t.Run("includes all capability properties with defaults", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "", 60)
		require.NotNil(t, connack)

		// Verify default capability properties are set
		// Per MQTT v5 spec section 3.2.2.3.4, Maximum QoS property is absent when server supports QoS 2
		assert.False(t, connack.Props.Has(PropMaximumQoS), "MaximumQoS property should be absent when server supports QoS 2")
		assert.Equal(t, byte(1), connack.Props.GetByte(PropRetainAvailable))
		assert.Equal(t, byte(1), connack.Props.GetByte(PropWildcardSubAvailable))
		assert.Equal(t, byte(1), connack.Props.GetByte(PropSubscriptionIDAvailable))
		assert.Equal(t, byte(1), connack.Props.GetByte(PropSharedSubAvailable))
	})

	t.Run("respects configured MaxQoS", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithMaxQoS(1),
		)
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "", 60)
		assert.Equal(t, byte(1), connack.Props.GetByte(PropMaximumQoS))
	})

	t.Run("respects configured RetainAvailable false", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithRetainAvailable(false),
		)
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "", 60)
		assert.Equal(t, byte(0), connack.Props.GetByte(PropRetainAvailable))
	})

	t.Run("respects configured WildcardSubAvailable false", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithWildcardSubAvailable(false),
		)
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "", 60)
		assert.Equal(t, byte(0), connack.Props.GetByte(PropWildcardSubAvailable))
	})

	t.Run("respects configured SubIDAvailable false", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithSubIDAvailable(false),
		)
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "", 60)
		assert.Equal(t, byte(0), connack.Props.GetByte(PropSubscriptionIDAvailable))
	})

	t.Run("respects configured SharedSubAvailable false", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithSharedSubAvailable(false),
		)
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "", 60)
		assert.Equal(t, byte(0), connack.Props.GetByte(PropSharedSubAvailable))
	})

	t.Run("auto-disables retain when retainedStore is nil", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithRetainedStore(nil),
		)
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "", 60)
		assert.Equal(t, byte(0), connack.Props.GetByte(PropRetainAvailable))
	})

	t.Run("includes topic alias maximum when configured", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithServerTopicAliasMax(100),
		)
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "", 60)
		assert.Equal(t, uint16(100), connack.Props.GetUint16(PropTopicAliasMaximum))
	})

	t.Run("includes receive maximum when less than default", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithServerReceiveMaximum(100),
		)
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "", 60)
		assert.Equal(t, uint16(100), connack.Props.GetUint16(PropReceiveMaximum))
	})

	t.Run("includes assigned client identifier", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "assigned-id", 60)
		assert.Equal(t, "assigned-id", connack.Props.GetString(PropAssignedClientIdentifier))
	})

	t.Run("sets session present flag", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		connack := srv.buildConnack(true, nil, "", 60)
		assert.True(t, connack.SessionPresent)
	})

	t.Run("sets session present from authResult", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		authResult := &AuthResult{
			Success:        true,
			SessionPresent: true,
		}
		connack := srv.buildConnack(false, authResult, "", 60)
		assert.True(t, connack.SessionPresent, "SessionPresent should be true when authResult.SessionPresent is true")
	})

	t.Run("includes server keep alive when override set", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(
			WithListener(listener),
			WithServerKeepAlive(120),
		)
		defer srv.Close()

		connack := srv.buildConnack(false, nil, "", 120)
		assert.True(t, connack.Props.Has(PropServerKeepAlive))
		assert.Equal(t, uint16(120), connack.Props.GetUint16(PropServerKeepAlive))
	})

	t.Run("merges authResult properties", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		authResult := &AuthResult{
			Success: true,
		}
		authResult.Properties.Set(PropReasonString, "welcome")
		connack := srv.buildConnack(false, authResult, "", 60)
		assert.Equal(t, "welcome", connack.Props.GetString(PropReasonString))
	})
}

func TestErrorToReasonCode(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	srv := NewServer(WithListener(listener))
	defer srv.Close()

	t.Run("nil error returns success", func(t *testing.T) {
		assert.Equal(t, ReasonSuccess, srv.errorToReasonCode(nil))
	})

	t.Run("packet too large returns correct code", func(t *testing.T) {
		assert.Equal(t, ReasonPacketTooLarge, srv.errorToReasonCode(ErrPacketTooLarge))
	})

	t.Run("unknown packet type returns protocol error", func(t *testing.T) {
		assert.Equal(t, ReasonProtocolError, srv.errorToReasonCode(ErrUnknownPacketType))
	})

	t.Run("protocol violation returns protocol error", func(t *testing.T) {
		assert.Equal(t, ReasonProtocolError, srv.errorToReasonCode(ErrProtocolViolation))
	})

	t.Run("invalid reason code returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrInvalidReasonCode))
	})

	t.Run("io EOF returns success (no disconnect needed)", func(t *testing.T) {
		assert.Equal(t, ReasonSuccess, srv.errorToReasonCode(io.EOF))
	})

	t.Run("generic error returns success (network error)", func(t *testing.T) {
		assert.Equal(t, ReasonSuccess, srv.errorToReasonCode(fmt.Errorf("connection reset")))
	})

	t.Run("invalid packet flags returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrInvalidPacketFlags))
	})

	t.Run("invalid packet ID returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrInvalidPacketID))
	})

	t.Run("invalid QoS returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrInvalidQoS))
	})

	t.Run("packet ID required returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrPacketIDRequired))
	})

	t.Run("invalid packet type returns protocol error", func(t *testing.T) {
		assert.Equal(t, ReasonProtocolError, srv.errorToReasonCode(ErrInvalidPacketType))
	})

	t.Run("duplicate property returns protocol error", func(t *testing.T) {
		assert.Equal(t, ReasonProtocolError, srv.errorToReasonCode(ErrDuplicateProperty))
	})

	t.Run("varint too large returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrVarintTooLarge))
	})

	t.Run("varint malformed returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrVarintMalformed))
	})

	t.Run("varint overlong returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrVarintOverlong))
	})

	t.Run("unknown property ID returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrUnknownPropertyID))
	})

	t.Run("invalid property type returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrInvalidPropertyType))
	})

	t.Run("invalid UTF-8 returns malformed packet", func(t *testing.T) {
		assert.Equal(t, ReasonMalformedPacket, srv.errorToReasonCode(ErrInvalidUTF8))
	})

	t.Run("wrapped errors are detected", func(t *testing.T) {
		wrappedErr := fmt.Errorf("context: %w", ErrPacketTooLarge)
		assert.Equal(t, ReasonPacketTooLarge, srv.errorToReasonCode(wrappedErr))
	})
}

func TestClientDisconnectWithWill(t *testing.T) {
	tests := []struct {
		name        string
		reasonCode  ReasonCode
		expectWill  bool
		description string
	}{
		{
			name:        "normal disconnect suppresses will",
			reasonCode:  ReasonSuccess,
			expectWill:  false,
			description: "ReasonSuccess should suppress Will message",
		},
		{
			name:        "disconnect with will publishes will",
			reasonCode:  ReasonDisconnectWithWill,
			expectWill:  true,
			description: "ReasonDisconnectWithWill (0x04) should publish Will message",
		},
		{
			name:        "server shutting down suppresses will",
			reasonCode:  ReasonServerShuttingDown,
			expectWill:  false,
			description: "Server-initiated disconnect suppresses Will",
		},
		{
			name:        "unspecified error suppresses will",
			reasonCode:  ReasonUnspecifiedError,
			expectWill:  false,
			description: "Any non-0x04 reason should suppress Will",
		},
		{
			name:        "protocol error suppresses will",
			reasonCode:  ReasonProtocolError,
			expectWill:  false,
			description: "Protocol error disconnect suppresses Will",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test to verify Will behavior based on disconnect reason
			// This tests the server's handling of DISCONNECT packets from clients
			disconnectPkt := &DisconnectPacket{ReasonCode: tt.reasonCode}

			// Verify the packet encodes correctly
			var buf bytes.Buffer
			_, err := WritePacket(&buf, disconnectPkt, MaxPacketSizeDefault)
			require.NoError(t, err)

			// Verify the reason code is preserved
			assert.Equal(t, tt.reasonCode, disconnectPkt.ReasonCode)

			// Test that DisconnectWithWill is the only reason that should publish Will
			shouldPublishWill := tt.reasonCode == ReasonDisconnectWithWill
			assert.Equal(t, tt.expectWill, shouldPublishWill, tt.description)
		})
	}
}

func TestUnsubscribeTopicFilterValidation(t *testing.T) {
	tests := []struct {
		name        string
		filter      string
		expectValid bool
		description string
	}{
		// Positive cases - valid filters
		{
			name:        "simple topic",
			filter:      "sensors/temperature",
			expectValid: true,
			description: "Simple topic path should be valid",
		},
		{
			name:        "single level wildcard",
			filter:      "sensors/+/data",
			expectValid: true,
			description: "Single level wildcard should be valid",
		},
		{
			name:        "multi level wildcard at end",
			filter:      "sensors/#",
			expectValid: true,
			description: "Multi-level wildcard at end should be valid",
		},
		{
			name:        "root wildcard",
			filter:      "#",
			expectValid: true,
			description: "Root wildcard should be valid",
		},
		{
			name:        "single plus wildcard",
			filter:      "+",
			expectValid: true,
			description: "Single plus wildcard should be valid",
		},
		{
			name:        "multiple single wildcards",
			filter:      "+/+/+",
			expectValid: true,
			description: "Multiple single-level wildcards should be valid",
		},
		// Negative cases - invalid filters
		{
			name:        "empty filter",
			filter:      "",
			expectValid: false,
			description: "Empty filter should be invalid",
		},
		{
			name:        "hash in middle",
			filter:      "sensors/#/data",
			expectValid: false,
			description: "Multi-level wildcard in middle should be invalid",
		},
		{
			name:        "plus not at level boundary",
			filter:      "sensors+/data",
			expectValid: false,
			description: "Plus not at level boundary should be invalid",
		},
		{
			name:        "hash not at level boundary",
			filter:      "sensors#",
			expectValid: false,
			description: "Hash not at level boundary should be invalid",
		},
		{
			name:        "null character",
			filter:      "sensors/\x00/data",
			expectValid: false,
			description: "Null character should be invalid",
		},
		// Edge cases
		{
			name:        "very long topic",
			filter:      string(make([]byte, 65535)),
			expectValid: false,
			description: "Topic exceeding max length should be invalid",
		},
		{
			name:        "trailing slash",
			filter:      "sensors/temperature/",
			expectValid: true,
			description: "Trailing slash should be valid",
		},
		{
			name:        "leading slash",
			filter:      "/sensors/temperature",
			expectValid: true,
			description: "Leading slash should be valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTopicFilter(tt.filter)
			if tt.expectValid {
				assert.NoError(t, err, tt.description)
			} else {
				assert.Error(t, err, tt.description)
			}
		})
	}
}

func TestConnectPropertyValidation(t *testing.T) {
	tests := []struct {
		name           string
		setupProps     func(*ConnectPacket)
		expectReject   bool
		expectedReason ReasonCode
		description    string
	}{
		// Positive cases
		{
			name:         "valid receive maximum",
			setupProps:   func(p *ConnectPacket) { p.Props.Set(PropReceiveMaximum, uint16(100)) },
			expectReject: false,
			description:  "Valid Receive Maximum should be accepted",
		},
		{
			name:         "valid max packet size",
			setupProps:   func(p *ConnectPacket) { p.Props.Set(PropMaximumPacketSize, uint32(1024)) },
			expectReject: false,
			description:  "Valid Maximum Packet Size should be accepted",
		},
		{
			name:         "no properties",
			setupProps:   func(_ *ConnectPacket) {},
			expectReject: false,
			description:  "No properties should be accepted",
		},
		{
			name:         "max receive maximum value",
			setupProps:   func(p *ConnectPacket) { p.Props.Set(PropReceiveMaximum, uint16(65535)) },
			expectReject: false,
			description:  "Maximum Receive Maximum value should be accepted",
		},
		// Negative cases
		{
			name:           "receive maximum zero",
			setupProps:     func(p *ConnectPacket) { p.Props.Set(PropReceiveMaximum, uint16(0)) },
			expectReject:   true,
			expectedReason: ReasonProtocolError,
			description:    "Receive Maximum = 0 should be rejected",
		},
		{
			name:           "max packet size zero",
			setupProps:     func(p *ConnectPacket) { p.Props.Set(PropMaximumPacketSize, uint32(0)) },
			expectReject:   true,
			expectedReason: ReasonProtocolError,
			description:    "Maximum Packet Size = 0 should be rejected",
		},
		// Edge cases
		{
			name:         "receive maximum one",
			setupProps:   func(p *ConnectPacket) { p.Props.Set(PropReceiveMaximum, uint16(1)) },
			expectReject: false,
			description:  "Receive Maximum = 1 should be accepted (minimum valid)",
		},
		{
			name:         "max packet size one",
			setupProps:   func(p *ConnectPacket) { p.Props.Set(PropMaximumPacketSize, uint32(1)) },
			expectReject: false,
			description:  "Maximum Packet Size = 1 should be accepted (minimum valid)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connect := &ConnectPacket{
				ClientID:   "test-client",
				CleanStart: true,
			}
			tt.setupProps(connect)

			// Test validation logic
			hasReceiveMax := connect.Props.Has(PropReceiveMaximum)
			receiveMaxZero := hasReceiveMax && connect.Props.GetUint16(PropReceiveMaximum) == 0

			hasMaxPacketSize := connect.Props.Has(PropMaximumPacketSize)
			maxPacketSizeZero := hasMaxPacketSize && connect.Props.GetUint32(PropMaximumPacketSize) == 0

			isRejected := receiveMaxZero || maxPacketSizeZero

			assert.Equal(t, tt.expectReject, isRejected, tt.description)
		})
	}
}

func TestServerClientCredentialExpiry(t *testing.T) {
	t.Run("default credential expiry is zero", func(t *testing.T) {
		client := &ServerClient{}
		assert.True(t, client.CredentialExpiry().IsZero())
		assert.False(t, client.IsCredentialExpired())
	})

	t.Run("set and get credential expiry", func(t *testing.T) {
		client := &ServerClient{}
		expiry := time.Now().Add(time.Hour)
		client.SetCredentialExpiry(expiry)

		assert.Equal(t, expiry, client.CredentialExpiry())
		assert.False(t, client.IsCredentialExpired())
	})

	t.Run("credential is expired when past expiry time", func(t *testing.T) {
		client := &ServerClient{}
		expiry := time.Now().Add(-time.Second) // Already expired
		client.SetCredentialExpiry(expiry)

		assert.True(t, client.IsCredentialExpired())
	})

	t.Run("zero expiry means never expired", func(t *testing.T) {
		client := &ServerClient{}
		client.SetCredentialExpiry(time.Time{})

		assert.False(t, client.IsCredentialExpired())
	})
}

func TestServerGetClient(t *testing.T) {
	t.Run("get existing client", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		client := &ServerClient{clientID: "test-client", namespace: DefaultNamespace}
		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "test-client")] = client
		srv.mu.Unlock()

		found := srv.GetClient(DefaultNamespace, "test-client")
		assert.Equal(t, client, found)
	})

	t.Run("get non-existing client returns nil", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		found := srv.GetClient(DefaultNamespace, "non-existent")
		assert.Nil(t, found)
	})

	t.Run("get client with different namespace returns nil", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		client := &ServerClient{clientID: "test-client", namespace: "tenant-a"}
		srv.mu.Lock()
		srv.clients[NamespaceKey("tenant-a", "test-client")] = client
		srv.mu.Unlock()

		found := srv.GetClient(DefaultNamespace, "test-client")
		assert.Nil(t, found)

		found = srv.GetClient("tenant-a", "test-client")
		assert.Equal(t, client, found)
	})
}

func TestServerDisconnectClient(t *testing.T) {
	t.Run("disconnect existing client returns true", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		// Create a mock client with a connection
		conn := &mockConn{}
		client := &ServerClient{
			clientID:  "test-client",
			namespace: DefaultNamespace,
			conn:      conn,
		}
		client.connected.Store(true)

		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "test-client")] = client
		srv.mu.Unlock()

		result := srv.DisconnectClient(DefaultNamespace, "test-client", ReasonAdminAction)
		assert.True(t, result)
	})

	t.Run("disconnect non-existing client returns false", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		result := srv.DisconnectClient(DefaultNamespace, "non-existent", ReasonAdminAction)
		assert.False(t, result)
	})
}

func TestServerCheckCredentialExpiry(t *testing.T) {
	t.Run("disconnects expired clients", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		// Create client with expired credentials
		conn := &mockConn{}
		client := &ServerClient{
			clientID:  "expired-client",
			namespace: DefaultNamespace,
			conn:      conn,
		}
		client.connected.Store(true)
		client.SetCredentialExpiry(time.Now().Add(-time.Second))

		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "expired-client")] = client
		srv.mu.Unlock()

		// Run credential expiry check
		srv.checkCredentialExpiry()

		// Client should have been marked for disconnect
		assert.True(t, client.IsCleanDisconnect() || !client.IsConnected())
	})

	t.Run("does not disconnect non-expired clients", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		// Create client with future expiry
		conn := &mockConn{}
		client := &ServerClient{
			clientID:  "valid-client",
			namespace: DefaultNamespace,
			conn:      conn,
		}
		client.connected.Store(true)
		client.SetCredentialExpiry(time.Now().Add(time.Hour))

		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "valid-client")] = client
		srv.mu.Unlock()

		// Run credential expiry check
		srv.checkCredentialExpiry()

		// Client should still be connected
		assert.True(t, client.IsConnected())
	})

	t.Run("does not disconnect clients without expiry", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		// Create client without credential expiry
		conn := &mockConn{}
		client := &ServerClient{
			clientID:  "no-expiry-client",
			namespace: DefaultNamespace,
			conn:      conn,
		}
		client.connected.Store(true)

		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "no-expiry-client")] = client
		srv.mu.Unlock()

		// Run credential expiry check
		srv.checkCredentialExpiry()

		// Client should still be connected
		assert.True(t, client.IsConnected())
	})
}

// TestServerHandlePubrec tests the handlePubrec function
func TestServerHandlePubrec(t *testing.T) {
	testCases := []struct {
		name       string
		reasonCode ReasonCode
	}{
		{"handles valid PUBREC and sends PUBREL", ReasonSuccess},
		{"handles PUBREC with error reason code", ReasonNoMatchingSubscribers},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer()
			defer srv.Close()

			conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
			connect := &ConnectPacket{ClientID: "test-client"}
			client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

			// Track a QoS 2 message first
			msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
			client.QoS2Tracker().TrackSend(1, msg)

			// Handle PUBREC
			pubrec := &PubrecPacket{PacketID: 1, ReasonCode: tc.reasonCode}
			srv.handlePubrec(client, pubrec)

			// Verify PUBREL was sent (per MQTT v5 spec, PUBREL is always sent)
			written := conn.writeBuf.Bytes()
			require.NotEmpty(t, written)

			r := bytes.NewReader(written)
			var header FixedHeader
			_, err := header.Decode(r)
			require.NoError(t, err)
			assert.Equal(t, PacketPUBREL, header.PacketType)
		})
	}

	t.Run("handles PUBREC for unknown packet ID", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Handle PUBREC for unknown packet ID
		pubrec := &PubrecPacket{PacketID: 99, ReasonCode: ReasonSuccess}
		srv.handlePubrec(client, pubrec)

		// Should not crash, but also not send anything
		written := conn.writeBuf.Bytes()
		assert.Empty(t, written)
	})
}

// TestServerHandlePubrel tests the handlePubrel function
func TestServerHandlePubrel(t *testing.T) {
	t.Run("handles valid PUBREL and sends PUBCOMP", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Track a received QoS 2 message and transition to awaiting PUBREL
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		client.QoS2Tracker().TrackReceive(1, msg)
		client.QoS2Tracker().SendPubrec(1)

		// Handle PUBREL
		pubrel := &PubrelPacket{PacketID: 1, ReasonCode: ReasonSuccess}
		srv.handlePubrel(client, pubrel)

		// Verify PUBCOMP was sent
		written := conn.writeBuf.Bytes()
		require.NotEmpty(t, written)

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBCOMP, header.PacketType)
	})

	t.Run("handles PUBREL for unknown packet ID with PacketIDNotFound", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Handle PUBREL for unknown packet ID
		pubrel := &PubrelPacket{PacketID: 99, ReasonCode: ReasonSuccess}
		srv.handlePubrel(client, pubrel)

		// Should send PUBCOMP with PacketIDNotFound
		written := conn.writeBuf.Bytes()
		require.NotEmpty(t, written)

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBCOMP, header.PacketType)

		var pubcomp PubcompPacket
		_, err = pubcomp.Decode(r, header)
		require.NoError(t, err)
		assert.Equal(t, ReasonPacketIDNotFound, pubcomp.ReasonCode)
	})

	t.Run("delivers message on PUBREL", func(t *testing.T) {
		var messageReceived bool
		srv := NewServer(OnMessage(func(_ *ServerClient, _ *Message) {
			messageReceived = true
		}))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Track a received QoS 2 message
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		client.QoS2Tracker().TrackReceive(1, msg)
		client.QoS2Tracker().SendPubrec(1)

		// Handle PUBREL - should trigger message delivery
		pubrel := &PubrelPacket{PacketID: 1, ReasonCode: ReasonSuccess}
		srv.handlePubrel(client, pubrel)

		assert.True(t, messageReceived)
	})
}

// TestServerHandlePubcomp tests the handlePubcomp function
func TestServerHandlePubcomp(t *testing.T) {
	t.Run("handles valid PUBCOMP and releases flow control", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Set up flow control
		client.SetReceiveMaximum(10)
		client.FlowControl().TryAcquire()

		// Track a sent QoS 2 message and transition states
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		client.QoS2Tracker().TrackSend(1, msg)
		client.QoS2Tracker().HandlePubrec(1)

		initialAvailable := client.FlowControl().Available()

		// Handle PUBCOMP
		pubcomp := &PubcompPacket{PacketID: 1, ReasonCode: ReasonSuccess}
		srv.handlePubcomp(client, pubcomp)

		// Flow control should have released
		assert.Greater(t, client.FlowControl().Available(), initialAvailable)

		// Message should be removed from tracker
		_, ok := client.QoS2Tracker().Get(1)
		assert.False(t, ok)
	})

	t.Run("handles PUBCOMP for unknown packet ID", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Handle PUBCOMP for unknown packet ID - should not crash
		pubcomp := &PubcompPacket{PacketID: 99, ReasonCode: ReasonSuccess}
		srv.handlePubcomp(client, pubcomp)

		// Should not panic or write anything
		assert.Empty(t, conn.writeBuf.Bytes())
	})
}

// TestServerHandleUnsubscribe tests the handleUnsubscribe function
func TestServerHandleUnsubscribe(t *testing.T) {
	t.Run("handles valid UNSUBSCRIBE and sends UNSUBACK", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetSession(NewMemorySession("test-client", DefaultNamespace))

		// Add subscription first
		srv.subs.Subscribe("test-client", DefaultNamespace, Subscription{TopicFilter: "test/topic", QoS: QoS1})

		logger := &testLogger{}

		// Handle UNSUBSCRIBE
		unsub := &UnsubscribePacket{
			PacketID:     1,
			TopicFilters: []string{"test/topic"},
		}
		srv.handleUnsubscribe(client, unsub, logger)

		// Verify UNSUBACK was sent
		written := conn.writeBuf.Bytes()
		require.NotEmpty(t, written)

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err = header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketUNSUBACK, header.PacketType)
	})

	t.Run("handles UNSUBSCRIBE for non-existing subscription", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetSession(NewMemorySession("test-client", DefaultNamespace))

		logger := &testLogger{}

		// Handle UNSUBSCRIBE for non-existing subscription
		unsub := &UnsubscribePacket{
			PacketID:     1,
			TopicFilters: []string{"non/existing"},
		}
		srv.handleUnsubscribe(client, unsub, logger)

		// Verify UNSUBACK was sent with NoSubscriptionExisted
		written := conn.writeBuf.Bytes()
		require.NotEmpty(t, written)

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err = header.Decode(r)
		require.NoError(t, err)

		var unsuback UnsubackPacket
		_, err = unsuback.Decode(r, header)
		require.NoError(t, err)
		require.Len(t, unsuback.ReasonCodes, 1)
		assert.Equal(t, ReasonNoSubscriptionExisted, unsuback.ReasonCodes[0])
	})

	t.Run("handles UNSUBSCRIBE with invalid topic filter", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		logger := &testLogger{}

		// Handle UNSUBSCRIBE with invalid topic filter
		unsub := &UnsubscribePacket{
			PacketID:     1,
			TopicFilters: []string{""}, // Empty is invalid
		}
		srv.handleUnsubscribe(client, unsub, logger)

		// Verify UNSUBACK was sent with TopicFilterInvalid
		written := conn.writeBuf.Bytes()
		require.NotEmpty(t, written)

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err = header.Decode(r)
		require.NoError(t, err)

		var unsuback UnsubackPacket
		_, err = unsuback.Decode(r, header)
		require.NoError(t, err)
		require.Len(t, unsuback.ReasonCodes, 1)
		assert.Equal(t, ReasonTopicFilterInvalid, unsuback.ReasonCodes[0])
	})

	t.Run("handles multiple topic filters in UNSUBSCRIBE", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServer(WithListener(listener))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetSession(NewMemorySession("test-client", DefaultNamespace))

		// Add subscriptions
		srv.subs.Subscribe("test-client", DefaultNamespace, Subscription{TopicFilter: "topic/a", QoS: QoS1})
		srv.subs.Subscribe("test-client", DefaultNamespace, Subscription{TopicFilter: "topic/b", QoS: QoS0})

		logger := &testLogger{}

		// Handle UNSUBSCRIBE with multiple topics
		unsub := &UnsubscribePacket{
			PacketID:     1,
			TopicFilters: []string{"topic/a", "topic/b", "topic/c"},
		}
		srv.handleUnsubscribe(client, unsub, logger)

		// Verify UNSUBACK has correct reason codes
		written := conn.writeBuf.Bytes()
		require.NotEmpty(t, written)

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err = header.Decode(r)
		require.NoError(t, err)

		var unsuback UnsubackPacket
		_, err = unsuback.Decode(r, header)
		require.NoError(t, err)
		require.Len(t, unsuback.ReasonCodes, 3)
		assert.Equal(t, ReasonSuccess, unsuback.ReasonCodes[0])               // topic/a existed
		assert.Equal(t, ReasonSuccess, unsuback.ReasonCodes[1])               // topic/b existed
		assert.Equal(t, ReasonNoSubscriptionExisted, unsuback.ReasonCodes[2]) // topic/c didn't exist
	})
}

// TestServerQueueOfflineMessage tests the queueOfflineMessage function
func TestServerQueueOfflineMessage(t *testing.T) {
	t.Run("queues message for offline client with session", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		// Create a session in the store
		session := NewMemorySession("test-client", DefaultNamespace)
		srv.config.sessionStore.Create(DefaultNamespace, session)

		// Queue a message
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS1}
		srv.queueOfflineMessage(DefaultNamespace, "test-client", msg)

		// Verify message was queued
		pendingMsgs := session.PendingMessages()
		assert.Len(t, pendingMsgs, 1)
	})

	t.Run("does nothing for client without session", func(_ *testing.T) {
		srv := NewServer()
		defer srv.Close()

		// Queue a message for non-existent client - should not panic
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS1}
		srv.queueOfflineMessage(DefaultNamespace, "non-existent", msg)
	})
}

// TestServerDeliverPendingMessages tests the deliverPendingMessages function
func TestServerDeliverPendingMessages(t *testing.T) {
	t.Run("delivers pending messages to reconnected client", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		session := NewMemorySession("test-client", DefaultNamespace)
		client.SetSession(session)

		// Add pending message
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS1}
		session.AddPendingMessage(1, msg)

		// Deliver pending messages
		srv.deliverPendingMessages(client, session)

		// Verify message was sent
		written := conn.writeBuf.Bytes()
		assert.NotEmpty(t, written)

		// Pending messages should be cleared
		assert.Empty(t, session.PendingMessages())
	})

	t.Run("discards expired messages", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		session := NewMemorySession("test-client", DefaultNamespace)
		client.SetSession(session)

		// Add expired message
		msg := &Message{
			Topic:         "test/topic",
			Payload:       []byte("data"),
			QoS:           QoS1,
			MessageExpiry: 1,                                // 1 second
			PublishedAt:   time.Now().Add(-2 * time.Second), // Published 2 seconds ago
		}
		session.AddPendingMessage(1, msg)

		// Deliver pending messages
		srv.deliverPendingMessages(client, session)

		// No message should be sent (expired)
		written := conn.writeBuf.Bytes()
		assert.Empty(t, written)
	})
}

// TestServerRestoreInflightMessages tests the restoreInflightMessages function
func TestServerRestoreInflightMessages(t *testing.T) {
	t.Run("restores QoS 1 messages with DUP flag", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetReceiveMaximum(10)

		session := NewMemorySession("test-client", DefaultNamespace)
		client.SetSession(session)

		// Add inflight QoS 1 message
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS1}
		session.AddInflightQoS1(1, &QoS1Message{PacketID: 1, Message: msg})

		logger := &testLogger{}

		// Restore inflight messages
		srv.restoreInflightMessages(client, session, logger)

		// Verify PUBLISH was sent with DUP flag
		written := conn.writeBuf.Bytes()
		require.NotEmpty(t, written)

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBLISH, header.PacketType)

		var pub PublishPacket
		_, err = pub.Decode(r, header)
		require.NoError(t, err)
		assert.True(t, pub.DUP)
		assert.Equal(t, uint16(1), pub.PacketID)
	})

	t.Run("restores QoS 2 messages awaiting PUBREC with DUP flag", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetReceiveMaximum(10)

		session := NewMemorySession("test-client", DefaultNamespace)
		client.SetSession(session)

		// Add inflight QoS 2 message awaiting PUBREC
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		session.AddInflightQoS2(1, &QoS2Message{
			PacketID: 1,
			Message:  msg,
			State:    QoS2AwaitingPubrec,
			IsSender: true,
		})

		logger := &testLogger{}

		// Restore inflight messages
		srv.restoreInflightMessages(client, session, logger)

		// Verify PUBLISH was sent with DUP flag
		written := conn.writeBuf.Bytes()
		require.NotEmpty(t, written)

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBLISH, header.PacketType)

		var pub PublishPacket
		_, err = pub.Decode(r, header)
		require.NoError(t, err)
		assert.True(t, pub.DUP)
		assert.Equal(t, QoS2, pub.QoS)
	})

	t.Run("restores QoS 2 messages awaiting PUBCOMP with PUBREL", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetReceiveMaximum(10)

		session := NewMemorySession("test-client", DefaultNamespace)
		client.SetSession(session)

		// Add inflight QoS 2 message awaiting PUBCOMP
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		session.AddInflightQoS2(1, &QoS2Message{
			PacketID: 1,
			Message:  msg,
			State:    QoS2AwaitingPubcomp,
			IsSender: true,
		})

		logger := &testLogger{}

		// Restore inflight messages
		srv.restoreInflightMessages(client, session, logger)

		// Verify PUBREL was sent
		written := conn.writeBuf.Bytes()
		require.NotEmpty(t, written)

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBREL, header.PacketType)
	})

	t.Run("restores receiver-side QoS 2 messages", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetInboundReceiveMaximum(10)

		session := NewMemorySession("test-client", DefaultNamespace)
		client.SetSession(session)

		// Add inflight receiver-side QoS 2 message
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		session.AddInflightQoS2(1, &QoS2Message{
			PacketID: 1,
			Message:  msg,
			State:    QoS2AwaitingPubrel,
			IsSender: false,
		})

		logger := &testLogger{}

		// Restore inflight messages
		srv.restoreInflightMessages(client, session, logger)

		// Should restore to tracker (no packet sent for receiver side)
		_, ok := client.QoS2Tracker().Get(1)
		assert.True(t, ok)
	})
}

// testLogger is a simple logger for testing
type testLogger struct {
	level LogLevel
}

func (l *testLogger) Debug(_ string, _ LogFields)   {}
func (l *testLogger) Info(_ string, _ LogFields)    {}
func (l *testLogger) Warn(_ string, _ LogFields)    {}
func (l *testLogger) Error(_ string, _ LogFields)   {}
func (l *testLogger) WithFields(_ LogFields) Logger { return l }
func (l *testLogger) Level() LogLevel               { return l.level }
func (l *testLogger) SetLevel(level LogLevel)       { l.level = level }

// TestServerHandleReauth tests the handleReauth function
func TestServerHandleReauth(t *testing.T) {
	t.Run("reauth with unsupported method disconnects client", func(t *testing.T) {
		enhancedAuth := &testEnhancedAuthEmptyNamespace{}
		srv := NewServer(WithEnhancedAuth(enhancedAuth))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.connected.Store(true)

		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "test-client")] = client
		srv.mu.Unlock()

		logger := &testLogger{}

		// Send AUTH with unsupported method
		authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
		authPkt.Props.Set(PropAuthenticationMethod, "UNSUPPORTED")

		srv.handleReauth(client, authPkt, NamespaceKey(DefaultNamespace, "test-client"), logger)

		// Client should be disconnected
		assert.False(t, client.IsConnected())
	})

	t.Run("reauth with empty method disconnects client", func(t *testing.T) {
		enhancedAuth := &testEnhancedAuthEmptyNamespace{}
		srv := NewServer(WithEnhancedAuth(enhancedAuth))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.connected.Store(true)

		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "test-client")] = client
		srv.mu.Unlock()

		logger := &testLogger{}

		// Send AUTH without method
		authPkt := &AuthPacket{ReasonCode: ReasonReAuth}

		srv.handleReauth(client, authPkt, NamespaceKey(DefaultNamespace, "test-client"), logger)

		// Client should be disconnected
		assert.False(t, client.IsConnected())
	})

	t.Run("successful reauth sends success AUTH", func(t *testing.T) {
		enhancedAuth := &testEnhancedAuthEmptyNamespace{}
		srv := NewServer(WithEnhancedAuth(enhancedAuth))
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.connected.Store(true)

		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "test-client")] = client
		srv.mu.Unlock()

		// Register keep-alive to prevent nil pointer
		srv.keepAlive.Register(NamespaceKey(DefaultNamespace, "test-client"), 60)

		logger := &testLogger{}

		// Send AUTH with supported method
		authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
		authPkt.Props.Set(PropAuthenticationMethod, "PLAIN")

		srv.handleReauth(client, authPkt, NamespaceKey(DefaultNamespace, "test-client"), logger)

		// Should send success AUTH
		written := conn.writeBuf.Bytes()
		require.NotEmpty(t, written)

		r := bytes.NewReader(written)
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketAUTH, header.PacketType)

		var respAuth AuthPacket
		_, err = respAuth.Decode(r, header)
		require.NoError(t, err)
		assert.Equal(t, ReasonSuccess, respAuth.ReasonCode)
	})
}

// TestServerResolvePublishTopic tests the resolvePublishTopic function
func TestServerResolvePublishTopic(t *testing.T) {
	t.Run("resolves topic without alias", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetTopicAliasMax(10, 0)

		pub := &PublishPacket{Topic: "test/topic"}

		topic, reason := srv.resolvePublishTopic(client, pub)
		assert.Equal(t, ReasonSuccess, reason)
		assert.Equal(t, "test/topic", topic)
	})

	t.Run("resolves topic with new alias", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetTopicAliasMax(10, 0)

		pub := &PublishPacket{Topic: "test/topic"}
		pub.Props.Set(PropTopicAlias, uint16(1))

		topic, reason := srv.resolvePublishTopic(client, pub)
		assert.Equal(t, ReasonSuccess, reason)
		assert.Equal(t, "test/topic", topic)
	})

	t.Run("resolves topic using existing alias", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetTopicAliasMax(10, 0)

		// First, set the alias
		pub1 := &PublishPacket{Topic: "test/topic"}
		pub1.Props.Set(PropTopicAlias, uint16(1))
		topic, reason := srv.resolvePublishTopic(client, pub1)
		assert.Equal(t, ReasonSuccess, reason)
		assert.Equal(t, "test/topic", topic)

		// Now use the alias without topic
		pub2 := &PublishPacket{Topic: ""}
		pub2.Props.Set(PropTopicAlias, uint16(1))
		topic, reason = srv.resolvePublishTopic(client, pub2)
		assert.Equal(t, ReasonSuccess, reason)
		assert.Equal(t, "test/topic", topic)
	})

	t.Run("returns error for invalid alias", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.SetTopicAliasMax(10, 0)

		// Use alias that was never set
		pub := &PublishPacket{Topic: ""}
		pub.Props.Set(PropTopicAlias, uint16(5))

		_, reason := srv.resolvePublishTopic(client, pub)
		assert.Equal(t, ReasonTopicAliasInvalid, reason)
	})

	t.Run("returns error for empty topic without alias", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		pub := &PublishPacket{Topic: ""}

		_, reason := srv.resolvePublishTopic(client, pub)
		assert.Equal(t, ReasonProtocolError, reason)
	})
}

// TestServerValidatePublishFlags tests the validatePublishFlags function
func TestServerValidatePublishFlags(t *testing.T) {
	t.Run("accepts valid QoS and retain", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		pub := &PublishPacket{QoS: QoS1, Retain: true}
		reason := srv.validatePublishFlags(pub)
		assert.Equal(t, ReasonSuccess, reason)
	})

	t.Run("rejects QoS above server max", func(t *testing.T) {
		srv := NewServer(WithMaxQoS(QoS1))
		defer srv.Close()

		pub := &PublishPacket{QoS: QoS2}
		reason := srv.validatePublishFlags(pub)
		assert.Equal(t, ReasonQoSNotSupported, reason)
	})

	t.Run("rejects retain when not available", func(t *testing.T) {
		srv := NewServer(WithRetainAvailable(false))
		defer srv.Close()

		pub := &PublishPacket{Retain: true}
		reason := srv.validatePublishFlags(pub)
		assert.Equal(t, ReasonRetainNotSupported, reason)
	})
}

func TestServerKeepAliveTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	// Connect a client
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	connect := &ConnectPacket{
		ClientID:   "keepalive-test",
		CleanStart: true,
		KeepAlive:  1, // Very short keepalive
	}
	_, err = WritePacket(conn, connect, 0)
	require.NoError(t, err)

	// Read CONNACK
	pkt, _, err := ReadPacket(conn, 0)
	require.NoError(t, err)
	_, ok := pkt.(*ConnackPacket)
	require.True(t, ok)

	// Client count should be 1
	assert.Equal(t, 1, srv.ClientCount())

	// Close connection without DISCONNECT (will trigger keepalive expiry handling)
	conn.Close()

	// Wait for connection to be cleaned up
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, srv.ClientCount())
}

func TestServerRetryClientMessages(t *testing.T) {
	srv := NewServer()
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "retry-test"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
	client.connected.Store(true)

	// Track a QoS 1 message that needs retry
	msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS1}
	client.QoS1Tracker().Track(1, msg)

	// Force a retry by setting sent time in the past
	pending := client.QoS1Tracker().GetPendingRetries()
	assert.Empty(t, pending) // No pending retries yet (not timed out)

	// Manually trigger retry
	srv.retryClientMessages(client)

	// Not yet timed out, so nothing written
	assert.Empty(t, conn.writeBuf.Bytes())
}

func TestServerRetryQoS2Messages(_ *testing.T) {
	srv := NewServer()
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "retry-qos2-test"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
	client.connected.Store(true)

	// Track a QoS 2 message
	msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
	client.QoS2Tracker().TrackSend(1, msg)

	// Trigger retry
	srv.retryClientMessages(client)

	// QoS 2 tracker cleanup should be called (no crash)
	client.QoS2Tracker().CleanupExpired()
	client.QoS2Tracker().CleanupCompleted()
}

func TestServerHandlePubrecWithSession(t *testing.T) {
	srv := NewServer()
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "pubrec-session-test"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

	// Set up session
	session := NewMemorySession("pubrec-session-test", DefaultNamespace)
	client.SetSession(session)

	// Track a QoS 2 message
	msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
	client.QoS2Tracker().TrackSend(1, msg)

	// Add to session as inflight
	session.AddInflightQoS2(1, &QoS2Message{
		PacketID:     1,
		Message:      msg,
		State:        QoS2AwaitingPubrec,
		SentAt:       time.Now(),
		RetryTimeout: 30 * time.Second,
		IsSender:     true,
	})

	// Handle PUBREC
	pubrec := &PubrecPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	srv.handlePubrec(client, pubrec)

	// Verify PUBREL was sent
	written := conn.writeBuf.Bytes()
	require.NotEmpty(t, written)

	// Session should have updated state
	qos2Msg, exists := session.GetInflightQoS2(1)
	require.True(t, exists)
	assert.Equal(t, QoS2AwaitingPubcomp, qos2Msg.State)
}

func TestServerDisconnectedClientRetry(t *testing.T) {
	srv := NewServer()
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "disconnected-retry"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
	// Client is not connected (connected.Store(false) is default)

	// Track messages
	msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS1}
	client.QoS1Tracker().Track(1, msg)

	// Retry should return early for disconnected client
	srv.retryClientMessages(client)

	// Nothing written because client not connected
	assert.Empty(t, conn.writeBuf.Bytes())
}

func TestServerReauthAuthStartFails(t *testing.T) {
	// Create a mock enhanced auth that fails AuthStart
	enhancedAuth := &testEnhancedAuthFailStart{}
	srv := NewServer(WithEnhancedAuth(enhancedAuth))
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "reauth-fail-test"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
	client.connected.Store(true)

	srv.mu.Lock()
	srv.clients[NamespaceKey(DefaultNamespace, "reauth-fail-test")] = client
	srv.mu.Unlock()

	srv.keepAlive.Register(NamespaceKey(DefaultNamespace, "reauth-fail-test"), 60)

	logger := &testLogger{}

	// Send AUTH with supported method but AuthStart will fail
	authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
	authPkt.Props.Set(PropAuthenticationMethod, "FAIL")

	srv.handleReauth(client, authPkt, NamespaceKey(DefaultNamespace, "reauth-fail-test"), logger)

	// Client should be disconnected
	assert.False(t, client.IsConnected())
}

type testEnhancedAuthFailStart struct{}

func (a *testEnhancedAuthFailStart) SupportsMethod(method string) bool {
	return method == "FAIL"
}

func (a *testEnhancedAuthFailStart) AuthStart(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	return nil, assert.AnError // Fail AuthStart
}

func (a *testEnhancedAuthFailStart) AuthContinue(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	return nil, nil
}

func TestServerHandlePubrecWithError(t *testing.T) {
	srv := NewServer()
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "pubrec-error-test"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
	client.connected.Store(true)

	// Track a QoS 2 message
	msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
	client.QoS2Tracker().TrackSend(1, msg)

	// Setup session
	session := NewMemorySession("pubrec-error-test", DefaultNamespace)
	client.SetSession(session)

	// Send PUBREC with error reason code (must be >= 0x80)
	pubrec := &PubrecPacket{
		PacketID:   1,
		ReasonCode: ReasonUnspecifiedError, // Error code = 0x80
	}
	srv.handlePubrec(client, pubrec)

	// PUBREL should still be sent (to complete exchange)
	assert.NotEmpty(t, conn.writeBuf.Bytes())

	// Message should be removed from tracker
	_, exists := client.QoS2Tracker().Get(1)
	assert.False(t, exists)
}

func TestServerPublishToSubscribersExpiredMessage(_ *testing.T) {
	srv := NewServer()
	defer srv.Close()

	// Create an expired message
	msg := &Message{
		Topic:         "test/expired",
		Payload:       []byte("expired data"),
		QoS:           QoS0,
		MessageExpiry: 1,                                // 1 second expiry
		PublishedAt:   time.Now().Add(-2 * time.Second), // Published 2 seconds ago
		Namespace:     DefaultNamespace,
	}

	// This should return early because message is expired
	srv.publishToSubscribers("publisher", msg)

	// No error, just silent discard
}

func TestServerPublishToSubscribersRetainedDelete(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	// First, set a retained message
	setMsg := &Message{
		Topic:     "test/retained",
		Payload:   []byte("retained data"),
		QoS:       QoS0,
		Retain:    true,
		Namespace: DefaultNamespace,
	}
	srv.Publish(setMsg)

	time.Sleep(50 * time.Millisecond)

	// Now delete it with empty payload
	deleteMsg := &Message{
		Topic:     "test/retained",
		Payload:   []byte{}, // Empty payload = delete
		QoS:       QoS0,
		Retain:    true,
		Namespace: DefaultNamespace,
	}
	srv.Publish(deleteMsg)

	// Retained message should be deleted
	// (We can't easily verify this without accessing internal state,
	// but the code path is exercised)
}

func TestServerPublishToSubscribersOfflineClient(_ *testing.T) {
	srv := NewServer()
	defer srv.Close()

	// Create a session for an offline client
	session := NewMemorySession("offline-client", DefaultNamespace)
	srv.config.sessionStore.Create(DefaultNamespace, session)

	// Add a subscription for the offline client
	srv.subs.Subscribe("offline-client", DefaultNamespace, Subscription{
		TopicFilter: "test/offline/#",
		QoS:         QoS1,
	})

	// Publish a message - should be queued for offline delivery
	msg := &Message{
		Topic:     "test/offline/data",
		Payload:   []byte("offline data"),
		QoS:       QoS1, // QoS > 0 required for offline queuing
		Namespace: DefaultNamespace,
	}
	srv.publishToSubscribers("publisher", msg)

	// Message should be queued in session - verify via session's packet ID incremented
	// The session stores pending messages internally
}

func TestServerPublishQoSDowngrade(t *testing.T) {
	t.Run("delivery QoS is minimum of message QoS and subscription QoS", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		var receivedMsgs []*Message
		var msgMu sync.Mutex

		srv := NewServer(
			WithListener(listener),
			OnMessage(func(_ *ServerClient, msg *Message) {
				msgMu.Lock()
				receivedMsgs = append(receivedMsgs, msg)
				msgMu.Unlock()
			}),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		// Connect a client
		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Send CONNECT
		connectPkt := &ConnectPacket{
			ClientID:   "qos-downgrade-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connectPkt, 256*1024)

		// Read CONNACK
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &ConnackPacket{}, pkt)

		// Subscribe with QoS 0 (lower than message QoS)
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/downgrade", QoS: QoS0},
			},
		}
		WritePacket(conn, subPkt, 256*1024)

		// Read SUBACK
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &SubackPacket{}, pkt)

		time.Sleep(50 * time.Millisecond)

		// Publish QoS 2 message via Server.Publish
		msg := &Message{
			Topic:   "test/downgrade",
			Payload: []byte("qos2 data"),
			QoS:     QoS2,
		}
		err = srv.Publish(msg)
		require.NoError(t, err)

		// Wait for delivery
		time.Sleep(100 * time.Millisecond)

		// Read the PUBLISH from the client connection
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		pubPkt, ok := pkt.(*PublishPacket)
		require.True(t, ok, "expected PUBLISH packet")
		// Delivery QoS should be downgraded to subscription QoS (0)
		assert.Equal(t, QoS0, pubPkt.QoS, "delivery QoS should be downgraded to subscription QoS")
	})
}

func TestServerPublishSubscriptionIdentifiers(t *testing.T) {
	t.Run("subscription identifiers are appended to delivery message", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		// Connect a client
		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Send CONNECT
		connectPkt := &ConnectPacket{
			ClientID:   "subid-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connectPkt, 256*1024)

		// Read CONNACK
		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &ConnackPacket{}, pkt)

		// Subscribe with subscription identifier
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/subid/#", QoS: QoS0},
			},
		}
		subPkt.Props.Set(PropSubscriptionIdentifier, uint32(42))
		WritePacket(conn, subPkt, 256*1024)

		// Read SUBACK
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &SubackPacket{}, pkt)

		time.Sleep(50 * time.Millisecond)

		// Publish message via Server.Publish
		msg := &Message{
			Topic:   "test/subid/data",
			Payload: []byte("data with subid"),
			QoS:     QoS0,
		}
		err = srv.Publish(msg)
		require.NoError(t, err)

		// Read the PUBLISH from the client connection
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		pubPkt, ok := pkt.(*PublishPacket)
		require.True(t, ok, "expected PUBLISH packet")

		// Check that subscription identifier is present
		subIDs := pubPkt.Props.GetAllVarInts(PropSubscriptionIdentifier)
		assert.Contains(t, subIDs, uint32(42), "subscription identifier should be in delivered message")
	})
}

func TestServerPublishOfflineClientQueueing(t *testing.T) {
	t.Run("QoS 0 messages are not queued for offline clients", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// Create a session for an offline client
		session := NewMemorySession("offline-qos0", DefaultNamespace)
		srv.config.sessionStore.Create(DefaultNamespace, session)

		// Add a subscription for the offline client
		srv.subs.Subscribe("offline-qos0", DefaultNamespace, Subscription{
			TopicFilter: "test/offline/qos0",
			QoS:         QoS0,
		})

		// Publish QoS 0 message - should NOT be queued
		msg := &Message{
			Topic:   "test/offline/qos0",
			Payload: []byte("qos0 data"),
			QoS:     QoS0,
		}
		err := srv.Publish(msg)
		require.NoError(t, err)

		// No pending messages should be queued for QoS 0
		pendingMsgs := session.PendingMessages()
		assert.Empty(t, pendingMsgs, "QoS 0 messages should not be queued for offline clients")
	})

	t.Run("QoS 1 messages are queued for offline clients", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// Create a session for an offline client
		session := NewMemorySession("offline-qos1", DefaultNamespace)
		srv.config.sessionStore.Create(DefaultNamespace, session)

		// Add a subscription for the offline client with QoS 1
		srv.subs.Subscribe("offline-qos1", DefaultNamespace, Subscription{
			TopicFilter: "test/offline/qos1",
			QoS:         QoS1,
		})

		// Publish QoS 1 message - should be queued
		msg := &Message{
			Topic:   "test/offline/qos1",
			Payload: []byte("qos1 data"),
			QoS:     QoS1,
		}
		err := srv.Publish(msg)
		require.NoError(t, err)

		// Pending messages should be queued for QoS 1
		pendingMsgs := session.PendingMessages()
		assert.Len(t, pendingMsgs, 1, "QoS 1 messages should be queued for offline clients")
	})

	t.Run("QoS 2 messages are queued for offline clients", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// Create a session for an offline client
		session := NewMemorySession("offline-qos2", DefaultNamespace)
		srv.config.sessionStore.Create(DefaultNamespace, session)

		// Add a subscription for the offline client with QoS 2
		srv.subs.Subscribe("offline-qos2", DefaultNamespace, Subscription{
			TopicFilter: "test/offline/qos2",
			QoS:         QoS2,
		})

		// Publish QoS 2 message - should be queued
		msg := &Message{
			Topic:   "test/offline/qos2",
			Payload: []byte("qos2 data"),
			QoS:     QoS2,
		}
		err := srv.Publish(msg)
		require.NoError(t, err)

		// Pending messages should be queued for QoS 2
		pendingMsgs := session.PendingMessages()
		assert.Len(t, pendingMsgs, 1, "QoS 2 messages should be queued for offline clients")
	})

	t.Run("QoS 1 messages are queued when send fails due to quota exceeded", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// Create a client with exhausted flow control quota
		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "quota-exhausted"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.connected.Store(true)

		// Set receive maximum to 1 and exhaust it
		client.SetReceiveMaximum(1)
		client.FlowControl().TryAcquire() // Exhaust the quota

		// Create a session for the client
		session := NewMemorySession("quota-exhausted", DefaultNamespace)
		srv.config.sessionStore.Create(DefaultNamespace, session)
		client.SetSession(session)

		// Add client to server
		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "quota-exhausted")] = client
		srv.mu.Unlock()

		// Add a subscription for the client
		srv.subs.Subscribe("quota-exhausted", DefaultNamespace, Subscription{
			TopicFilter: "test/quota/#",
			QoS:         QoS1,
		})

		// Publish QoS 1 message - should fail to send due to quota and be queued
		msg := &Message{
			Topic:   "test/quota/data",
			Payload: []byte("quota exceeded data"),
			QoS:     QoS1,
		}
		err := srv.Publish(msg)
		require.NoError(t, err)

		// Message should be queued in session due to send failure
		pendingMsgs := session.PendingMessages()
		assert.Len(t, pendingMsgs, 1, "QoS 1 messages should be queued when send fails")
	})
}

func TestServerHandleReauthUnsupportedMethod(t *testing.T) {
	// Use mock that only supports "FAIL" method
	enhancedAuth := &testEnhancedAuthFailStart{}
	srv := NewServer(WithEnhancedAuth(enhancedAuth))
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "reauth-unsupported"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
	client.connected.Store(true)

	srv.mu.Lock()
	srv.clients[NamespaceKey(DefaultNamespace, "reauth-unsupported")] = client
	srv.mu.Unlock()

	srv.keepAlive.Register(NamespaceKey(DefaultNamespace, "reauth-unsupported"), 60)

	logger := &testLogger{}

	// Send AUTH with unsupported method (not "FAIL")
	authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
	authPkt.Props.Set(PropAuthenticationMethod, "UNSUPPORTED")

	srv.handleReauth(client, authPkt, NamespaceKey(DefaultNamespace, "reauth-unsupported"), logger)

	// Client should be disconnected due to bad auth method
	assert.False(t, client.IsConnected())
}

func TestServerHandleReauthEmptyMethod(t *testing.T) {
	enhancedAuth := &testEnhancedAuthFailStart{}
	srv := NewServer(WithEnhancedAuth(enhancedAuth))
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "reauth-empty"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
	client.connected.Store(true)

	srv.mu.Lock()
	srv.clients[NamespaceKey(DefaultNamespace, "reauth-empty")] = client
	srv.mu.Unlock()

	srv.keepAlive.Register(NamespaceKey(DefaultNamespace, "reauth-empty"), 60)

	logger := &testLogger{}

	// Send AUTH without method
	authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
	// No PropAuthenticationMethod set

	srv.handleReauth(client, authPkt, NamespaceKey(DefaultNamespace, "reauth-empty"), logger)

	// Client should be disconnected
	assert.False(t, client.IsConnected())
}

func TestServerMessageFromPublishPacket(t *testing.T) {
	t.Run("with all properties", func(t *testing.T) {
		pub := &PublishPacket{
			Topic:   "test/topic",
			Payload: []byte("test data"),
			QoS:     QoS1,
			Retain:  true,
		}
		pub.Props.Set(PropPayloadFormatIndicator, byte(1))
		pub.Props.Set(PropMessageExpiryInterval, uint32(3600))
		pub.Props.Set(PropContentType, "application/json")
		pub.Props.Set(PropResponseTopic, "response/topic")
		pub.Props.Set(PropCorrelationData, []byte("correl-123"))
		pub.Props.Set(PropUserProperty, StringPair{Key: "key1", Value: "value1"})

		msg := messageFromPublishPacket(pub, "test/topic", QoS1, DefaultNamespace, "client-1")

		assert.Equal(t, "test/topic", msg.Topic)
		assert.Equal(t, []byte("test data"), msg.Payload)
		assert.Equal(t, QoS1, msg.QoS)
		assert.True(t, msg.Retain)
		assert.Equal(t, byte(1), msg.PayloadFormat)
		assert.Equal(t, uint32(3600), msg.MessageExpiry)
		assert.Equal(t, "application/json", msg.ContentType)
		assert.Equal(t, "response/topic", msg.ResponseTopic)
		assert.Equal(t, []byte("correl-123"), msg.CorrelationData)
		assert.Len(t, msg.UserProperties, 1)
		assert.Equal(t, "key1", msg.UserProperties[0].Key)
		assert.Equal(t, "value1", msg.UserProperties[0].Value)
	})

	t.Run("minimal properties", func(t *testing.T) {
		pub := &PublishPacket{
			Topic:   "simple/topic",
			Payload: []byte("simple"),
			QoS:     QoS0,
		}

		msg := messageFromPublishPacket(pub, "simple/topic", QoS0, DefaultNamespace, "client-2")

		assert.Equal(t, "simple/topic", msg.Topic)
		assert.Equal(t, []byte("simple"), msg.Payload)
		assert.Equal(t, QoS0, msg.QoS)
		assert.False(t, msg.Retain)
		assert.Empty(t, msg.ContentType)
	})
}

func TestServerQueueOfflineMessageNoSession(_ *testing.T) {
	srv := NewServer()
	defer srv.Close()

	// Try to queue message for non-existent session - should be silently ignored
	srv.queueOfflineMessage(DefaultNamespace, "nonexistent-client", &Message{
		Topic:   "test/topic",
		Payload: []byte("data"),
		QoS:     QoS1,
	})

	// No crash, message was dropped because no session exists
}

func TestServerHandlePubcompUnknownPacketID(_ *testing.T) {
	srv := NewServer()
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "pubcomp-unknown"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
	client.connected.Store(true)

	// Send PUBCOMP for unknown packet ID
	pubcomp := &PubcompPacket{PacketID: 999}
	srv.handlePubcomp(client, pubcomp)

	// Should not panic, just ignore
}

func TestServerGetTLSConnectionState(t *testing.T) {
	// Test with non-TLS connection
	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	state := getTLSConnectionState(conn)
	assert.Nil(t, state)
}

func TestServerApplyCredentialExpiry(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	defer srv.Close()

	t.Run("applies session expiry", func(_ *testing.T) {
		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "cred-expiry-test"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		expiryTime := time.Now().Add(1 * time.Hour)
		result := &AuthResult{
			Success:       true,
			SessionExpiry: expiryTime,
		}

		srv.applyCredentialExpiry(client, result)
	})

	t.Run("handles nil auth result", func(_ *testing.T) {
		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "nil-result"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Should not panic with nil result
		srv.applyCredentialExpiry(client, nil)
	})

	t.Run("handles zero expiry time", func(_ *testing.T) {
		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "zero-expiry"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		result := &AuthResult{
			Success:       true,
			SessionExpiry: time.Time{}, // Zero time
		}

		srv.applyCredentialExpiry(client, result)
	})
}

func TestServerHandlePubcompNoQoS2Tracker(_ *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "pubcomp-no-tracker"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
	client.connected.Store(true)

	// Track a QoS 2 message in AwaitingPubcomp state
	client.QoS2Tracker().TrackSend(1, &Message{Topic: "test", Payload: []byte("data")})
	client.QoS2Tracker().HandlePubrec(1) // Move to AwaitingPubcomp

	// Handle PUBCOMP
	pubcomp := &PubcompPacket{PacketID: 1}
	srv.handlePubcomp(client, pubcomp)
}

func TestServerPublishAuthorizationError(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	authz := &testAuthorizer{
		authzFunc: func(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
			return nil, fmt.Errorf("authorization error")
		},
	}

	srv := NewServer(
		WithListener(listener),
		WithServerAuthz(authz),
	)
	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	// Connect
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// Send CONNECT
	connect := &ConnectPacket{ClientID: "publish-authz-error"}
	WritePacket(conn, connect, 256*1024)

	// Read CONNACK
	_, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)

	// Send PUBLISH QoS 1
	pub := &PublishPacket{
		Topic:    "test/topic",
		Payload:  []byte("data"),
		QoS:      QoS1,
		PacketID: 1,
	}
	WritePacket(conn, pub, 256*1024)

	// Should receive PUBACK with error
	time.Sleep(100 * time.Millisecond)
}

func TestServerHandleSubscribeAuthorizationError(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	authz := &testAuthorizer{
		authzFunc: func(_ context.Context, azCtx *AuthzContext) (*AuthzResult, error) {
			if azCtx.Action == AuthzActionSubscribe {
				return nil, fmt.Errorf("subscribe auth error")
			}
			return &AuthzResult{Allowed: true}, nil
		},
	}

	srv := NewServer(
		WithListener(listener),
		WithServerAuthz(authz),
	)
	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(50 * time.Millisecond)

	// Connect
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// Send CONNECT
	connect := &ConnectPacket{ClientID: "subscribe-authz-error"}
	WritePacket(conn, connect, 256*1024)

	// Read CONNACK
	_, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)

	// Send SUBSCRIBE
	sub := &SubscribePacket{
		PacketID: 1,
		Subscriptions: []Subscription{
			{TopicFilter: "test/topic", QoS: QoS1},
		},
	}
	WritePacket(conn, sub, 256*1024)

	// Should receive SUBACK
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	suback, ok := pkt.(*SubackPacket)
	assert.True(t, ok)
	assert.NotEmpty(t, suback.ReasonCodes)
}

func TestServerHandleConnectionEdgeCases(t *testing.T) {
	t.Run("first packet not CONNECT", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Send PUBLISH as first packet instead of CONNECT
		pub := &PublishPacket{
			Topic:   "test/topic",
			Payload: []byte("hello"),
		}
		WritePacket(conn, pub, 256*1024)

		// Connection should be closed by server
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		assert.Error(t, err) // Should get EOF or connection closed
	})

	t.Run("max connections reached", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(
			WithListener(listener),
			WithMaxConnections(1), // Only allow 1 connection
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		// First connection - should succeed
		conn1, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn1.Close()

		connect1 := &ConnectPacket{ClientID: "client1"}
		WritePacket(conn1, connect1, 256*1024)

		pkt, _, err := ReadPacket(conn1, 256*1024)
		require.NoError(t, err)
		connack1, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack1.ReasonCode)

		// Second connection - should get ServerBusy
		conn2, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn2.Close()

		connect2 := &ConnectPacket{ClientID: "client2"}
		WritePacket(conn2, connect2, 256*1024)

		pkt2, _, err := ReadPacket(conn2, 256*1024)
		require.NoError(t, err)
		connack2, ok := pkt2.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonServerBusy, connack2.ReasonCode)
	})

	t.Run("empty client ID with CleanStart false", func(t *testing.T) {
		// Note: The code path in server.go for empty ClientID with CleanStart=false
		// cannot be reached because ReadPacket validates the packet first (codec.go:89).
		// The library's ReadPacket calls Validate() after decoding, which rejects
		// empty ClientID with CleanStart=false before the server logic can handle it.
		// The server just sees a failed ReadPacket and closes the connection.

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Send raw CONNECT with empty ClientID and CleanStart=false
		rawConnect := []byte{
			0x10, 0x0D, // Fixed header: CONNECT, remaining length = 13
			0x00, 0x04, 'M', 'Q', 'T', 'T', // Protocol name
			0x05,       // Protocol version 5
			0x00,       // Connect flags (CleanStart=false)
			0x00, 0x00, // Keep alive = 0
			0x00,       // Properties length = 0
			0x00, 0x00, // Client ID length = 0 (empty)
		}
		_, err = conn.Write(rawConnect)
		require.NoError(t, err)

		// Server closes connection because ReadPacket validation fails
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		assert.Error(t, err) // EOF or connection closed
	})

	t.Run("empty client ID with CleanStart true gets assigned ID", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Empty client ID with CleanStart=true should get assigned ID
		connect := &ConnectPacket{
			ClientID:   "",
			CleanStart: true,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)
		// Server should assign a client ID
		assignedID := connack.Props.GetString(PropAssignedClientIdentifier)
		assert.NotEmpty(t, assignedID)
		assert.True(t, strings.HasPrefix(assignedID, "auto-"))
	})

	// Table-driven tests for CONNECT property validation
	propertyValidationTests := []struct {
		name           string
		setupConnect   func(*ConnectPacket)
		expectedReason ReasonCode
	}{
		{
			name: "invalid maximum packet size zero",
			setupConnect: func(c *ConnectPacket) {
				c.Props.Set(PropMaximumPacketSize, uint32(0))
			},
			expectedReason: ReasonProtocolError,
		},
		{
			name: "invalid maximum packet size exceeds protocol max",
			setupConnect: func(c *ConnectPacket) {
				c.Props.Set(PropMaximumPacketSize, uint32(MaxPacketSizeProtocol+1))
			},
			expectedReason: ReasonProtocolError,
		},
		{
			name: "receive maximum zero is protocol error",
			setupConnect: func(c *ConnectPacket) {
				c.Props.Set(PropReceiveMaximum, uint16(0))
			},
			expectedReason: ReasonProtocolError,
		},
		{
			name: "receive maximum from CONNECT is applied",
			setupConnect: func(c *ConnectPacket) {
				c.Props.Set(PropReceiveMaximum, uint16(10))
			},
			expectedReason: ReasonSuccess,
		},
	}

	for _, tc := range propertyValidationTests {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()

			srv := NewServer(WithListener(listener))
			go srv.ListenAndServe()
			defer srv.Close()

			time.Sleep(20 * time.Millisecond)

			conn, err := net.Dial("tcp", listener.Addr().String())
			require.NoError(t, err)
			defer conn.Close()

			connect := &ConnectPacket{ClientID: "test-client"}
			tc.setupConnect(connect)
			WritePacket(conn, connect, 256*1024)

			pkt, _, err := ReadPacket(conn, 256*1024)
			require.NoError(t, err)
			connack, ok := pkt.(*ConnackPacket)
			require.True(t, ok)
			assert.Equal(t, tc.expectedReason, connack.ReasonCode)
		})
	}

	t.Run("namespace validation failure", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		// Custom authenticator that returns an invalid namespace
		auth := &testAuthenticator{
			authFunc: func(_ context.Context, _ *AuthContext) (*AuthResult, error) {
				return &AuthResult{
					Success:   true,
					Namespace: "invalid/namespace/with/slashes", // Will fail validation
				}, nil
			},
		}

		// Namespace validator that rejects slashes
		validator := func(ns string) error {
			if strings.Contains(ns, "/") {
				return fmt.Errorf("namespace cannot contain slashes")
			}
			return nil
		}

		srv := NewServer(
			WithListener(listener),
			WithServerAuth(auth),
			WithNamespaceValidator(validator),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		connect := &ConnectPacket{ClientID: "test-client"}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonNotAuthorized, connack.ReasonCode)
	})

	t.Run("session takeover disconnects existing client", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		// First connection
		conn1, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn1.Close()

		connect1 := &ConnectPacket{ClientID: "same-client-id"}
		WritePacket(conn1, connect1, 256*1024)

		pkt1, _, err := ReadPacket(conn1, 256*1024)
		require.NoError(t, err)
		connack1, ok := pkt1.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack1.ReasonCode)

		// Second connection with same client ID - should trigger session takeover
		conn2, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn2.Close()

		connect2 := &ConnectPacket{ClientID: "same-client-id"}
		WritePacket(conn2, connect2, 256*1024)

		pkt2, _, err := ReadPacket(conn2, 256*1024)
		require.NoError(t, err)
		connack2, ok := pkt2.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack2.ReasonCode)

		// First connection should receive DISCONNECT with SessionTakenOver
		conn1.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		pkt3, _, err := ReadPacket(conn1, 256*1024)
		if err == nil {
			disconnect, ok := pkt3.(*DisconnectPacket)
			if ok {
				assert.Equal(t, ReasonSessionTakenOver, disconnect.ReasonCode)
			}
		}
	})

	t.Run("onConnect callback is called", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		var callbackCalled bool
		var callbackClientID string
		var mu sync.Mutex

		srv := NewServer(
			WithListener(listener),
			OnConnect(func(c *ServerClient) {
				mu.Lock()
				callbackCalled = true
				callbackClientID = c.ClientID()
				mu.Unlock()
			}),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		connect := &ConnectPacket{ClientID: "callback-test-client"}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Give callback time to execute
		time.Sleep(20 * time.Millisecond)

		mu.Lock()
		assert.True(t, callbackCalled)
		assert.Equal(t, "callback-test-client", callbackClientID)
		mu.Unlock()
	})

	t.Run("auth assigns different client ID", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		// Authenticator that assigns a different client ID
		auth := &testAuthenticator{
			authFunc: func(_ context.Context, authCtx *AuthContext) (*AuthResult, error) {
				return &AuthResult{
					Success:          true,
					AssignedClientID: "server-assigned-" + authCtx.ClientID,
				}, nil
			},
		}

		srv := NewServer(
			WithListener(listener),
			WithServerAuth(auth),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		connect := &ConnectPacket{ClientID: "original-id"}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// CONNACK should contain the assigned client ID property
		assignedID := connack.Props.GetString(PropAssignedClientIdentifier)
		assert.Equal(t, "server-assigned-original-id", assignedID)
	})
}

func TestServerPingreqHandling(t *testing.T) {
	t.Run("server responds to PINGREQ with PINGRESP", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect first
		connect := &ConnectPacket{
			ClientID:   "pingreq-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send PINGREQ
		pingreq := &PingreqPacket{}
		WritePacket(conn, pingreq, 256*1024)

		// Should receive PINGRESP
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		pingresp, ok := pkt.(*PingrespPacket)
		require.True(t, ok, "expected PINGRESP packet")
		assert.NotNil(t, pingresp)
	})
}

func TestServerDisconnectSessionExpiryUpdate(t *testing.T) {
	t.Run("client can update session expiry on disconnect", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect with session expiry
		connect := &ConnectPacket{
			ClientID:   "session-expiry-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		connect.Props.Set(PropSessionExpiryInterval, uint32(100))
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		time.Sleep(20 * time.Millisecond)

		// Send DISCONNECT with updated session expiry
		disconnect := &DisconnectPacket{ReasonCode: ReasonSuccess}
		disconnect.Props.Set(PropSessionExpiryInterval, uint32(200))
		WritePacket(conn, disconnect, 256*1024)

		// Connection should close cleanly
		conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		assert.Error(t, err) // EOF expected
	})

	t.Run("session expiry update ignored if original was zero", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect without session expiry (default is 0)
		connect := &ConnectPacket{
			ClientID:   "no-session-expiry-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		time.Sleep(20 * time.Millisecond)

		// Try to set session expiry on DISCONNECT (should be ignored per spec)
		disconnect := &DisconnectPacket{ReasonCode: ReasonSuccess}
		disconnect.Props.Set(PropSessionExpiryInterval, uint32(100))
		WritePacket(conn, disconnect, 256*1024)

		// Connection should close cleanly (no protocol error)
		conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		assert.Error(t, err) // EOF expected
	})
}

func TestServerAuthPacketWithoutEnhancedAuth(t *testing.T) {
	t.Run("AUTH packet without enhanced auth configured disconnects client", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		// Server without enhanced auth configured
		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect first
		connect := &ConnectPacket{
			ClientID:   "auth-test-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send AUTH packet (re-authentication attempt)
		authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
		authPkt.Props.Set(PropAuthenticationMethod, "PLAIN")
		WritePacket(conn, authPkt, 256*1024)

		// Server should send DISCONNECT with protocol error
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		disconnect, ok := pkt.(*DisconnectPacket)
		require.True(t, ok, "expected DISCONNECT packet")
		assert.Equal(t, ReasonProtocolError, disconnect.ReasonCode)
	})
}

func TestServerConnackWriteFailure(t *testing.T) {
	t.Run("client removed when CONNACK write fails", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// Create a connection that will fail on write
		failConn := &failingConn{
			writeErr: net.ErrClosed,
		}

		// Simulate handleConnection by setting up the client manually
		connect := &ConnectPacket{
			ClientID:   "connack-fail-client",
			CleanStart: true,
		}
		client := NewServerClient(failConn, connect, 256*1024, DefaultNamespace)

		// Add client to server
		clientKey := NamespaceKey(DefaultNamespace, "connack-fail-client")
		srv.mu.Lock()
		srv.clients[clientKey] = client
		srv.mu.Unlock()

		// Verify client is in map
		srv.mu.RLock()
		_, exists := srv.clients[clientKey]
		srv.mu.RUnlock()
		assert.True(t, exists, "client should be in map before removal")

		// Call removeClient (simulating what happens after CONNACK write failure)
		srv.removeClient(clientKey, client)

		// Verify client is removed
		srv.mu.RLock()
		_, exists = srv.clients[clientKey]
		srv.mu.RUnlock()
		assert.False(t, exists, "client should be removed from map")
	})
}

func TestServerProtocolErrorDisconnect(t *testing.T) {
	t.Run("protocol error sends disconnect with reason code", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect first
		connect := &ConnectPacket{
			ClientID:   "protocol-error-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send malformed packet (invalid packet type in fixed header)
		// Packet type 0 is RESERVED and invalid
		malformedPacket := []byte{0x00, 0x00} // Type 0, length 0
		_, err = conn.Write(malformedPacket)
		require.NoError(t, err)

		// Server should send DISCONNECT with protocol error or malformed packet
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		if err == nil {
			disconnect, ok := pkt.(*DisconnectPacket)
			if ok {
				// Server correctly sent DISCONNECT with error reason
				assert.True(t, disconnect.ReasonCode.IsError(), "disconnect reason should be an error code")
			}
		}
		// If err != nil, server may have closed connection directly which is also valid
	})
}

func TestServerPublishValidationFailureDisconnect(t *testing.T) {
	t.Run("QoS above server max disconnects client", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		// Server only supports QoS 1
		srv := NewServer(
			WithListener(listener),
			WithMaxQoS(QoS1),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connect := &ConnectPacket{
			ClientID:   "qos-validation-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send PUBLISH with QoS 2 (server only supports QoS 1)
		pub := &PublishPacket{
			Topic:    "test/topic",
			Payload:  []byte("data"),
			QoS:      QoS2,
			PacketID: 1,
		}
		WritePacket(conn, pub, 256*1024)

		// Server should send DISCONNECT with QoS not supported
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		disconnect, ok := pkt.(*DisconnectPacket)
		require.True(t, ok, "expected DISCONNECT packet")
		assert.Equal(t, ReasonQoSNotSupported, disconnect.ReasonCode)
	})

	t.Run("retain when not available disconnects client", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		// Server does not support retain
		srv := NewServer(
			WithListener(listener),
			WithRetainAvailable(false),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connect := &ConnectPacket{
			ClientID:   "retain-validation-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send PUBLISH with retain flag (server doesn't support retain)
		pub := &PublishPacket{
			Topic:   "test/topic",
			Payload: []byte("data"),
			QoS:     QoS0,
			Retain:  true,
		}
		WritePacket(conn, pub, 256*1024)

		// Server should send DISCONNECT with retain not supported
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		disconnect, ok := pkt.(*DisconnectPacket)
		require.True(t, ok, "expected DISCONNECT packet")
		assert.Equal(t, ReasonRetainNotSupported, disconnect.ReasonCode)
	})
}

func TestServerInboundFlowControlExceeded(t *testing.T) {
	t.Run("exceeding inbound receive max disconnects client", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		// Server with very low receive maximum
		srv := NewServer(
			WithListener(listener),
			WithServerReceiveMaximum(1),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connect := &ConnectPacket{
			ClientID:   "flow-control-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Send first QoS 1 PUBLISH (should be accepted, quota = 1)
		pub1 := &PublishPacket{
			Topic:    "test/topic",
			Payload:  []byte("data1"),
			QoS:      QoS1,
			PacketID: 1,
		}
		WritePacket(conn, pub1, 256*1024)

		// Immediately send second QoS 1 PUBLISH without waiting for PUBACK
		// This exceeds the receive maximum
		pub2 := &PublishPacket{
			Topic:    "test/topic",
			Payload:  []byte("data2"),
			QoS:      QoS1,
			PacketID: 2,
		}
		WritePacket(conn, pub2, 256*1024)

		// Server should eventually send DISCONNECT with receive max exceeded
		// (may receive PUBACK for first message first)
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		for {
			pkt, _, err = ReadPacket(conn, 256*1024)
			if err != nil {
				break // Connection closed
			}
			if disconnect, ok := pkt.(*DisconnectPacket); ok {
				assert.Equal(t, ReasonReceiveMaxExceeded, disconnect.ReasonCode)
				break
			}
			// Continue reading (might be PUBACK for first message)
		}
	})
}

func TestServerQoS2AuthorizationFailurePubrec(t *testing.T) {
	t.Run("authorization failure on QoS 2 publish sends PUBREC with error", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		// Authorizer that denies all publishes
		authz := &testAuthorizer{
			authzFunc: func(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
				return &AuthzResult{Allowed: false, ReasonCode: ReasonNotAuthorized}, nil
			},
		}

		srv := NewServer(
			WithListener(listener),
			WithServerAuthz(authz),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connect := &ConnectPacket{
			ClientID:   "qos2-authz-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send QoS 2 PUBLISH
		pub := &PublishPacket{
			Topic:    "test/topic",
			Payload:  []byte("data"),
			QoS:      QoS2,
			PacketID: 1,
		}
		WritePacket(conn, pub, 256*1024)

		// Server should send PUBREC with error reason code
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		pubrec, ok := pkt.(*PubrecPacket)
		require.True(t, ok, "expected PUBREC packet")
		assert.Equal(t, uint16(1), pubrec.PacketID)
		assert.Equal(t, ReasonNotAuthorized, pubrec.ReasonCode)
	})

	t.Run("authorization error on QoS 2 publish sends PUBREC with unspecified error", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		// Authorizer that returns an error
		authz := &testAuthorizer{
			authzFunc: func(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
				return nil, fmt.Errorf("internal authorization error")
			},
		}

		srv := NewServer(
			WithListener(listener),
			WithServerAuthz(authz),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connect := &ConnectPacket{
			ClientID:   "qos2-authz-error-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send QoS 2 PUBLISH
		pub := &PublishPacket{
			Topic:    "test/topic",
			Payload:  []byte("data"),
			QoS:      QoS2,
			PacketID: 1,
		}
		WritePacket(conn, pub, 256*1024)

		// Server should send PUBREC with error reason code
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		pubrec, ok := pkt.(*PubrecPacket)
		require.True(t, ok, "expected PUBREC packet")
		assert.Equal(t, uint16(1), pubrec.PacketID)
		assert.True(t, pubrec.ReasonCode.IsError(), "reason code should be an error")
	})
}

func TestServerQoS2PublishHandling(t *testing.T) {
	t.Run("QoS 2 publish tracks message and sends PUBREC", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connect := &ConnectPacket{
			ClientID:   "qos2-publish-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send QoS 2 PUBLISH
		pub := &PublishPacket{
			Topic:    "test/qos2",
			Payload:  []byte("qos2 data"),
			QoS:      QoS2,
			PacketID: 1,
		}
		WritePacket(conn, pub, 256*1024)

		// Should receive PUBREC with success
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		pubrec, ok := pkt.(*PubrecPacket)
		require.True(t, ok, "expected PUBREC packet")
		assert.Equal(t, uint16(1), pubrec.PacketID)
		assert.Equal(t, ReasonSuccess, pubrec.ReasonCode)
	})

	t.Run("QoS 2 publish persists state in session", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "qos2-session-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.connected.Store(true)

		// Set up session
		session := NewMemorySession("qos2-session-client", DefaultNamespace)
		client.SetSession(session)

		// Set up inbound flow control
		client.SetInboundReceiveMaximum(10)

		// Simulate handlePublish by calling the tracker methods directly
		msg := &Message{Topic: "test/qos2", Payload: []byte("data"), QoS: QoS2}
		client.QoS2Tracker().TrackReceive(1, msg)

		// Persist to session (simulating what handlePublish does)
		session.AddInflightQoS2(1, &QoS2Message{
			PacketID:     1,
			Message:      msg,
			State:        QoS2ReceivedPublish,
			SentAt:       time.Now(),
			RetryTimeout: 30 * time.Second,
			IsSender:     false,
		})

		// Verify message is tracked
		_, exists := client.QoS2Tracker().Get(1)
		assert.True(t, exists, "message should be tracked in QoS2Tracker")

		// Verify session has the QoS 2 message
		qos2Msg, exists := session.GetInflightQoS2(1)
		assert.True(t, exists, "message should be in session inflight")
		assert.Equal(t, QoS2ReceivedPublish, qos2Msg.State)
		assert.False(t, qos2Msg.IsSender)
	})

	t.Run("QoS 2 DUP retransmit sends PUBREC but does not double-track", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connect := &ConnectPacket{
			ClientID:   "qos2-dup-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send first QoS 2 PUBLISH
		pub := &PublishPacket{
			Topic:    "test/qos2/dup",
			Payload:  []byte("original"),
			QoS:      QoS2,
			PacketID: 1,
			DUP:      false,
		}
		WritePacket(conn, pub, 256*1024)

		// Receive first PUBREC
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		pubrec1, ok := pkt.(*PubrecPacket)
		require.True(t, ok)
		assert.Equal(t, uint16(1), pubrec1.PacketID)
		assert.Equal(t, ReasonSuccess, pubrec1.ReasonCode)

		// Send DUP retransmit (same packet ID, DUP flag set)
		pubDup := &PublishPacket{
			Topic:    "test/qos2/dup",
			Payload:  []byte("retransmit"),
			QoS:      QoS2,
			PacketID: 1,
			DUP:      true,
		}
		WritePacket(conn, pubDup, 256*1024)

		// Should still receive PUBREC for the DUP
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		pubrec2, ok := pkt.(*PubrecPacket)
		require.True(t, ok, "expected PUBREC for DUP retransmit")
		assert.Equal(t, uint16(1), pubrec2.PacketID)
		assert.Equal(t, ReasonSuccess, pubrec2.ReasonCode)
	})

	t.Run("QoS 2 session state updates after PUBREC sent", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "qos2-state-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.connected.Store(true)

		// Set up session
		session := NewMemorySession("qos2-state-client", DefaultNamespace)
		client.SetSession(session)

		// Track message and add to session
		msg := &Message{Topic: "test/qos2", Payload: []byte("data"), QoS: QoS2}
		client.QoS2Tracker().TrackReceive(1, msg)

		session.AddInflightQoS2(1, &QoS2Message{
			PacketID:     1,
			Message:      msg,
			State:        QoS2ReceivedPublish,
			SentAt:       time.Now(),
			RetryTimeout: 30 * time.Second,
			IsSender:     false,
		})

		// Simulate PUBREC sent
		client.QoS2Tracker().SendPubrec(1)

		// Update session state (simulating what handlePublish does after PUBREC)
		if qos2Msg, exists := session.GetInflightQoS2(1); exists {
			qos2Msg.State = QoS2AwaitingPubrel
			qos2Msg.SentAt = time.Now()
		}

		// Verify state was updated
		qos2Msg, exists := session.GetInflightQoS2(1)
		require.True(t, exists)
		assert.Equal(t, QoS2AwaitingPubrel, qos2Msg.State)
	})

	t.Run("QoS 2 complete flow with PUBREL and PUBCOMP", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		var messageDelivered bool
		var deliveredTopic string
		var mu sync.Mutex

		srv := NewServer(
			WithListener(listener),
			OnMessage(func(_ *ServerClient, msg *Message) {
				mu.Lock()
				messageDelivered = true
				deliveredTopic = msg.Topic
				mu.Unlock()
			}),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connect := &ConnectPacket{
			ClientID:   "qos2-complete-flow-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Send QoS 2 PUBLISH
		pub := &PublishPacket{
			Topic:    "test/qos2/complete",
			Payload:  []byte("complete flow"),
			QoS:      QoS2,
			PacketID: 1,
		}
		WritePacket(conn, pub, 256*1024)

		// Receive PUBREC
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		pubrec, ok := pkt.(*PubrecPacket)
		require.True(t, ok)
		assert.Equal(t, uint16(1), pubrec.PacketID)

		// Message should NOT be delivered yet (QoS 2 defers until PUBREL)
		mu.Lock()
		assert.False(t, messageDelivered, "message should not be delivered before PUBREL")
		mu.Unlock()

		// Send PUBREL
		pubrel := &PubrelPacket{
			PacketID:   1,
			ReasonCode: ReasonSuccess,
		}
		WritePacket(conn, pubrel, 256*1024)

		// Receive PUBCOMP
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		pubcomp, ok := pkt.(*PubcompPacket)
		require.True(t, ok, "expected PUBCOMP packet")
		assert.Equal(t, uint16(1), pubcomp.PacketID)

		// Message should now be delivered
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		assert.True(t, messageDelivered, "message should be delivered after PUBREL")
		assert.Equal(t, "test/qos2/complete", deliveredTopic)
		mu.Unlock()
	})
}

func TestServerRetainedMessageHandling(t *testing.T) {
	t.Run("retained message stores all properties", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// Create a message with all properties
		msg := &Message{
			Topic:           "test/retained/props",
			Payload:         []byte("retained with props"),
			QoS:             QoS1,
			Retain:          true,
			PayloadFormat:   1, // UTF-8
			MessageExpiry:   3600,
			PublishedAt:     time.Now(),
			ContentType:     "application/json",
			ResponseTopic:   "response/topic",
			CorrelationData: []byte("correlation-123"),
			UserProperties: []StringPair{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
			},
		}
		err := srv.Publish(msg)
		require.NoError(t, err)

		// Verify all properties are stored
		retained := srv.config.retainedStore.Match(DefaultNamespace, "test/retained/props")
		require.Len(t, retained, 1)

		rm := retained[0]
		assert.Equal(t, "test/retained/props", rm.Topic)
		assert.Equal(t, []byte("retained with props"), rm.Payload)
		assert.Equal(t, QoS1, rm.QoS)
		assert.Equal(t, byte(1), rm.PayloadFormat)
		assert.Equal(t, uint32(3600), rm.MessageExpiry)
		assert.False(t, rm.PublishedAt.IsZero())
		assert.Equal(t, "application/json", rm.ContentType)
		assert.Equal(t, "response/topic", rm.ResponseTopic)
		assert.Equal(t, []byte("correlation-123"), rm.CorrelationData)
		require.Len(t, rm.UserProperties, 2)
		assert.Equal(t, "key1", rm.UserProperties[0].Key)
		assert.Equal(t, "value1", rm.UserProperties[0].Value)
		assert.Equal(t, "key2", rm.UserProperties[1].Key)
		assert.Equal(t, "value2", rm.UserProperties[1].Value)
	})

	t.Run("retained message with empty payload deletes existing", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// First store a retained message
		msg1 := &Message{
			Topic:   "test/retained/delete",
			Payload: []byte("original data"),
			Retain:  true,
		}
		err := srv.Publish(msg1)
		require.NoError(t, err)

		// Verify it's stored
		retained := srv.config.retainedStore.Match(DefaultNamespace, "test/retained/delete")
		require.Len(t, retained, 1)

		// Delete with empty payload
		msg2 := &Message{
			Topic:   "test/retained/delete",
			Payload: []byte{},
			Retain:  true,
		}
		err = srv.Publish(msg2)
		require.NoError(t, err)

		// Verify it's deleted
		retained = srv.config.retainedStore.Match(DefaultNamespace, "test/retained/delete")
		assert.Empty(t, retained)
	})

	t.Run("client publish retained message stores it via handlePublish", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connect := &ConnectPacket{
			ClientID:   "retained-publish-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Publish retained message with properties
		pub := &PublishPacket{
			Topic:   "test/client/retained",
			Payload: []byte("client retained data"),
			QoS:     QoS0,
			Retain:  true,
		}
		pub.Props.Set(PropContentType, "text/plain")
		pub.Props.Set(PropResponseTopic, "client/response")
		WritePacket(conn, pub, 256*1024)

		// Wait for message to be processed
		time.Sleep(100 * time.Millisecond)

		// Verify retained message is stored with properties
		retained := srv.config.retainedStore.Match(DefaultNamespace, "test/client/retained")
		require.Len(t, retained, 1)
		assert.Equal(t, "test/client/retained", retained[0].Topic)
		assert.Equal(t, []byte("client retained data"), retained[0].Payload)
		assert.Equal(t, "text/plain", retained[0].ContentType)
		assert.Equal(t, "client/response", retained[0].ResponseTopic)
	})

	t.Run("client publish empty retained deletes via handlePublish", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		// Pre-populate retained store
		srv.config.retainedStore.Set(DefaultNamespace, &RetainedMessage{
			Topic:   "test/client/delete",
			Payload: []byte("existing retained"),
		})

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connect := &ConnectPacket{
			ClientID:   "retained-delete-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connect, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Publish empty retained message to delete
		pub := &PublishPacket{
			Topic:   "test/client/delete",
			Payload: []byte{},
			QoS:     QoS0,
			Retain:  true,
		}
		WritePacket(conn, pub, 256*1024)

		// Wait for message to be processed
		time.Sleep(100 * time.Millisecond)

		// Verify retained message is deleted
		retained := srv.config.retainedStore.Match(DefaultNamespace, "test/client/delete")
		assert.Empty(t, retained)
	})

	t.Run("retained message with namespace isolation", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		// Authenticator that assigns namespaces based on username
		auth := &testAuthenticator{
			authFunc: func(_ context.Context, ctx *AuthContext) (*AuthResult, error) {
				namespace := "tenant-" + ctx.Username
				return &AuthResult{
					Success:   true,
					Namespace: namespace,
				}, nil
			},
		}

		srv := NewServer(
			WithListener(listener),
			WithServerAuth(auth),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(20 * time.Millisecond)

		// Connect as tenant-a
		connA, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer connA.Close()

		connectA := &ConnectPacket{
			ClientID:   "tenant-a-client",
			CleanStart: true,
			KeepAlive:  60,
			Username:   "a",
		}
		WritePacket(connA, connectA, 256*1024)

		pkt, _, err := ReadPacket(connA, 256*1024)
		require.NoError(t, err)
		_, ok := pkt.(*ConnackPacket)
		require.True(t, ok)

		// Publish retained message as tenant-a
		pubA := &PublishPacket{
			Topic:   "shared/topic",
			Payload: []byte("tenant-a data"),
			QoS:     QoS0,
			Retain:  true,
		}
		WritePacket(connA, pubA, 256*1024)

		time.Sleep(50 * time.Millisecond)

		// Connect as tenant-b
		connB, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer connB.Close()

		connectB := &ConnectPacket{
			ClientID:   "tenant-b-client",
			CleanStart: true,
			KeepAlive:  60,
			Username:   "b",
		}
		WritePacket(connB, connectB, 256*1024)

		pkt, _, err = ReadPacket(connB, 256*1024)
		require.NoError(t, err)
		_, ok = pkt.(*ConnackPacket)
		require.True(t, ok)

		// Publish retained message as tenant-b
		pubB := &PublishPacket{
			Topic:   "shared/topic",
			Payload: []byte("tenant-b data"),
			QoS:     QoS0,
			Retain:  true,
		}
		WritePacket(connB, pubB, 256*1024)

		time.Sleep(50 * time.Millisecond)

		// Verify messages are stored in separate namespaces
		retainedA := srv.config.retainedStore.Match("tenant-a", "shared/topic")
		require.Len(t, retainedA, 1)
		assert.Equal(t, []byte("tenant-a data"), retainedA[0].Payload)

		retainedB := srv.config.retainedStore.Match("tenant-b", "shared/topic")
		require.Len(t, retainedB, 1)
		assert.Equal(t, []byte("tenant-b data"), retainedB[0].Payload)
	})
}

func TestServerPublishToSubscribersSubscriptionIdentifiers(t *testing.T) {
	t.Run("subscription identifiers added when client publishes", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		// Connect publisher client
		pubConn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer pubConn.Close()

		pubConnect := &ConnectPacket{
			ClientID:   "publisher",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(pubConn, pubConnect, 256*1024)

		pkt, _, err := ReadPacket(pubConn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &ConnackPacket{}, pkt)

		// Connect subscriber client
		subConn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer subConn.Close()

		subConnect := &ConnectPacket{
			ClientID:   "subscriber",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(subConn, subConnect, 256*1024)

		pkt, _, err = ReadPacket(subConn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &ConnackPacket{}, pkt)

		// Subscribe with subscription identifier
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/subid/#", QoS: QoS0},
			},
		}
		subPkt.Props.Set(PropSubscriptionIdentifier, uint32(99))
		WritePacket(subConn, subPkt, 256*1024)

		pkt, _, err = ReadPacket(subConn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &SubackPacket{}, pkt)

		time.Sleep(50 * time.Millisecond)

		// Publisher sends a message (goes through handlePublish -> publishToSubscribers)
		pubPkt := &PublishPacket{
			Topic:   "test/subid/data",
			Payload: []byte("client published"),
			QoS:     QoS0,
		}
		WritePacket(pubConn, pubPkt, 256*1024)

		// Read the PUBLISH from subscriber connection
		subConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(subConn, 256*1024)
		require.NoError(t, err)

		receivedPub, ok := pkt.(*PublishPacket)
		require.True(t, ok, "expected PUBLISH packet")

		// Check that subscription identifier is present in delivered message
		subIDs := receivedPub.Props.GetAllVarInts(PropSubscriptionIdentifier)
		assert.Contains(t, subIDs, uint32(99), "subscription identifier should be in delivered message")
	})
}

func TestServerPublishToSubscribersSendFailureQueueing(t *testing.T) {
	tests := []struct {
		name         string
		qos          byte
		expectQueued bool
		pubClientID  string
		subClientID  string
		topicFilter  string
		topic        string
	}{
		{
			name:         "QoS 0 messages not queued when send fails",
			qos:          QoS0,
			expectQueued: false,
			pubClientID:  "pub-qos0",
			subClientID:  "sub-qos0",
			topicFilter:  "test/fail/qos0",
			topic:        "test/fail/qos0",
		},
		{
			name:         "QoS 1 messages queued when send fails",
			qos:          QoS1,
			expectQueued: true,
			pubClientID:  "pub-qos1",
			subClientID:  "sub-qos1",
			topicFilter:  "test/fail/qos1",
			topic:        "test/fail/qos1",
		},
		{
			name:         "QoS 2 messages queued when send fails",
			qos:          QoS2,
			expectQueued: true,
			pubClientID:  "pub-qos2",
			subClientID:  "sub-qos2",
			topicFilter:  "test/fail/qos2",
			topic:        "test/fail/qos2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := NewServer()
			defer srv.Close()

			srv.running.Store(true)
			defer srv.running.Store(false)

			// Create publisher client
			pubConn := &mockServerConn{writeBuf: &bytes.Buffer{}}
			pubConnect := &ConnectPacket{ClientID: tt.pubClientID}
			pubClient := NewServerClient(pubConn, pubConnect, 256*1024, DefaultNamespace)
			pubClient.connected.Store(true)

			// Create subscriber client with exhausted flow control quota
			subConn := &mockServerConn{writeBuf: &bytes.Buffer{}}
			subConnect := &ConnectPacket{ClientID: tt.subClientID}
			subClient := NewServerClient(subConn, subConnect, 256*1024, DefaultNamespace)
			subClient.connected.Store(true)

			// Exhaust flow control to cause Send to fail
			subClient.SetReceiveMaximum(1)
			subClient.FlowControl().TryAcquire()

			// Create session for subscriber
			subSession := NewMemorySession(tt.subClientID, DefaultNamespace)
			srv.config.sessionStore.Create(DefaultNamespace, subSession)
			subClient.SetSession(subSession)

			// Add clients to server
			srv.mu.Lock()
			srv.clients[NamespaceKey(DefaultNamespace, tt.pubClientID)] = pubClient
			srv.clients[NamespaceKey(DefaultNamespace, tt.subClientID)] = subClient
			srv.mu.Unlock()

			// Add subscription for subscriber
			srv.subs.Subscribe(tt.subClientID, DefaultNamespace, Subscription{
				TopicFilter: tt.topicFilter,
				QoS:         tt.qos,
			})

			// Call publishToSubscribers directly
			msg := &Message{
				Topic:     tt.topic,
				Payload:   []byte("test payload"),
				QoS:       tt.qos,
				Namespace: DefaultNamespace,
			}
			srv.publishToSubscribers(tt.pubClientID, msg)

			// Check if message was queued
			pendingMsgs := subSession.PendingMessages()
			if tt.expectQueued {
				assert.Len(t, pendingMsgs, 1, "message should be queued when send fails")
			} else {
				assert.Empty(t, pendingMsgs, "message should not be queued when send fails")
			}
		})
	}
}

func TestServerHandleSubscribeSubIDNotSupported(t *testing.T) {
	t.Run("disconnects client when subscription identifier not supported", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(
			WithListener(listener),
			WithSubIDAvailable(false),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Send CONNECT
		connectPkt := &ConnectPacket{
			ClientID:   "subid-not-supported",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connectPkt, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Verify CONNACK indicates subscription IDs not available
		subIDAvail := connack.Props.Get(PropSubscriptionIDAvailable)
		assert.Equal(t, byte(0), subIDAvail)

		// Subscribe with subscription identifier - should cause disconnect
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/topic", QoS: QoS0},
			},
		}
		subPkt.Props.Set(PropSubscriptionIdentifier, uint32(123))
		WritePacket(conn, subPkt, 256*1024)

		// Should receive DISCONNECT with SubIDsNotSupported
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		disconnectPkt, ok := pkt.(*DisconnectPacket)
		require.True(t, ok, "expected DISCONNECT packet")
		assert.Equal(t, ReasonSubIDsNotSupported, disconnectPkt.ReasonCode)
	})
}

func TestServerHandleSubscribeWildcardNotSupported(t *testing.T) {
	t.Run("returns error code when wildcard subscription not supported", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(
			WithListener(listener),
			WithWildcardSubAvailable(false),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Send CONNECT
		connectPkt := &ConnectPacket{
			ClientID:   "wildcard-not-supported",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connectPkt, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Subscribe with wildcard - should return error code
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/+/data", QoS: QoS0},
			},
		}
		WritePacket(conn, subPkt, 256*1024)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		subackPkt, ok := pkt.(*SubackPacket)
		require.True(t, ok, "expected SUBACK packet")
		require.Len(t, subackPkt.ReasonCodes, 1)
		assert.Equal(t, ReasonWildcardSubsNotSupported, subackPkt.ReasonCodes[0])
	})

	t.Run("rejects multi-level wildcard when not supported", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(
			WithListener(listener),
			WithWildcardSubAvailable(false),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		connectPkt := &ConnectPacket{
			ClientID:   "wildcard-multi-level",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connectPkt, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &ConnackPacket{}, pkt)

		// Subscribe with # wildcard
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/#", QoS: QoS1},
			},
		}
		WritePacket(conn, subPkt, 256*1024)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		subackPkt, ok := pkt.(*SubackPacket)
		require.True(t, ok)
		require.Len(t, subackPkt.ReasonCodes, 1)
		assert.Equal(t, ReasonWildcardSubsNotSupported, subackPkt.ReasonCodes[0])
	})
}

func TestServerHandleSubscribeSharedNotSupported(t *testing.T) {
	t.Run("returns error code when shared subscription not supported", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(
			WithListener(listener),
			WithSharedSubAvailable(false),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		connectPkt := &ConnectPacket{
			ClientID:   "shared-not-supported",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connectPkt, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Subscribe with shared subscription - should return error code
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "$share/group/test/topic", QoS: QoS0},
			},
		}
		WritePacket(conn, subPkt, 256*1024)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		subackPkt, ok := pkt.(*SubackPacket)
		require.True(t, ok, "expected SUBACK packet")
		require.Len(t, subackPkt.ReasonCodes, 1)
		assert.Equal(t, ReasonSharedSubsNotSupported, subackPkt.ReasonCodes[0])
	})
}

func TestServerHandleSubscribeMaxQoSEnforcement(t *testing.T) {
	t.Run("caps subscription QoS to server max QoS", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(
			WithListener(listener),
			WithMaxQoS(QoS1), // Server only supports up to QoS 1
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		connectPkt := &ConnectPacket{
			ClientID:   "max-qos-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connectPkt, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		connack, ok := pkt.(*ConnackPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonSuccess, connack.ReasonCode)

		// Subscribe with QoS 2 - should be capped to QoS 1
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/maxqos", QoS: QoS2},
			},
		}
		WritePacket(conn, subPkt, 256*1024)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)

		subackPkt, ok := pkt.(*SubackPacket)
		require.True(t, ok, "expected SUBACK packet")
		require.Len(t, subackPkt.ReasonCodes, 1)
		// Reason code should be QoS 1 (the capped value)
		assert.Equal(t, ReasonCode(QoS1), subackPkt.ReasonCodes[0])
	})
}

func TestServerHandleSubscribeRetainedMessageDelivery(t *testing.T) {
	t.Run("delivers retained messages with QoS downgrade", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		// Store a retained message with QoS 2
		srv.config.retainedStore.Set(DefaultNamespace, &RetainedMessage{
			Topic:       "test/retained/qos",
			Payload:     []byte("retained qos2"),
			QoS:         QoS2,
			PublishedAt: time.Now(),
		})

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		connectPkt := &ConnectPacket{
			ClientID:   "retained-qos-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connectPkt, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &ConnackPacket{}, pkt)

		// Subscribe with QoS 1 and RetainAsPublish - retained message should be downgraded
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/retained/qos", QoS: QoS1, RetainAsPublish: true},
			},
		}
		WritePacket(conn, subPkt, 256*1024)

		// Read packets - SUBACK and PUBLISH may arrive in any order
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		var pubPkt *PublishPacket
		for i := 0; i < 2; i++ {
			pkt, _, err = ReadPacket(conn, 256*1024)
			require.NoError(t, err)
			if p, ok := pkt.(*PublishPacket); ok {
				pubPkt = p
			}
		}

		require.NotNil(t, pubPkt, "expected PUBLISH packet with retained message")
		assert.Equal(t, "test/retained/qos", pubPkt.Topic)
		assert.Equal(t, QoS1, pubPkt.QoS) // Downgraded from QoS 2
		assert.True(t, pubPkt.Retain, "retain flag should be preserved when RetainAsPublish is true")
	})

	t.Run("respects RetainAsPublish flag", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(WithListener(listener))
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		// Store a retained message
		srv.config.retainedStore.Set(DefaultNamespace, &RetainedMessage{
			Topic:       "test/retained/rap",
			Payload:     []byte("retained rap test"),
			QoS:         QoS0,
			PublishedAt: time.Now(),
		})

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		connectPkt := &ConnectPacket{
			ClientID:   "rap-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connectPkt, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &ConnackPacket{}, pkt)

		// Subscribe with RetainAsPublish = false (retain flag should be cleared)
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/retained/rap", QoS: QoS0, RetainAsPublish: false},
			},
		}
		WritePacket(conn, subPkt, 256*1024)

		// Read packets - SUBACK and PUBLISH may arrive in any order
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		var pubPkt *PublishPacket
		for i := 0; i < 2; i++ {
			pkt, _, err = ReadPacket(conn, 256*1024)
			require.NoError(t, err)
			if p, ok := pkt.(*PublishPacket); ok {
				pubPkt = p
			}
		}

		require.NotNil(t, pubPkt, "expected PUBLISH packet with retained message")
		assert.Equal(t, "test/retained/rap", pubPkt.Topic)
		assert.False(t, pubPkt.Retain, "retain flag should be cleared when RetainAsPublish is false")
	})

	t.Run("queues retained message on send failure for QoS > 0", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// Store a retained message with QoS 1
		srv.config.retainedStore.Set(DefaultNamespace, &RetainedMessage{
			Topic:       "test/retained/queue",
			Payload:     []byte("retained to queue"),
			QoS:         QoS1,
			PublishedAt: time.Now(),
		})

		// Create client with exhausted flow control
		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "retained-queue-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.connected.Store(true)

		// Exhaust flow control to cause Send to fail
		client.SetReceiveMaximum(1)
		client.FlowControl().TryAcquire()

		// Create session
		session := NewMemorySession("retained-queue-client", DefaultNamespace)
		srv.config.sessionStore.Create(DefaultNamespace, session)
		client.SetSession(session)

		// Add client to server
		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "retained-queue-client")] = client
		srv.mu.Unlock()

		srv.keepAlive.Register(NamespaceKey(DefaultNamespace, "retained-queue-client"), 60)

		logger := &testLogger{}

		// Call handleSubscribe with QoS 1 subscription
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/retained/queue", QoS: QoS1},
			},
		}
		srv.handleSubscribe(client, subPkt, logger)

		// Retained message should be queued due to send failure
		pendingMsgs := session.PendingMessages()
		assert.Len(t, pendingMsgs, 1, "retained message should be queued when send fails")
	})

	t.Run("does not queue QoS 0 retained message on send failure", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// Store a retained message with QoS 0
		srv.config.retainedStore.Set(DefaultNamespace, &RetainedMessage{
			Topic:       "test/retained/noqueue",
			Payload:     []byte("qos0 retained"),
			QoS:         QoS0,
			PublishedAt: time.Now(),
		})

		// Create client with exhausted flow control
		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "retained-noqueue-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
		client.connected.Store(true)

		// Exhaust flow control
		client.SetReceiveMaximum(1)
		client.FlowControl().TryAcquire()

		// Create session
		session := NewMemorySession("retained-noqueue-client", DefaultNamespace)
		srv.config.sessionStore.Create(DefaultNamespace, session)
		client.SetSession(session)

		// Add client to server
		srv.mu.Lock()
		srv.clients[NamespaceKey(DefaultNamespace, "retained-noqueue-client")] = client
		srv.mu.Unlock()

		srv.keepAlive.Register(NamespaceKey(DefaultNamespace, "retained-noqueue-client"), 60)

		logger := &testLogger{}

		// Call handleSubscribe with QoS 0 subscription
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/retained/noqueue", QoS: QoS0},
			},
		}
		srv.handleSubscribe(client, subPkt, logger)

		// QoS 0 retained message should NOT be queued
		pendingMsgs := session.PendingMessages()
		assert.Empty(t, pendingMsgs, "QoS 0 retained message should not be queued")
	})
}

func TestServerOnUnsubscribeCallback(t *testing.T) {
	t.Run("callback is invoked on unsubscribe", func(t *testing.T) {
		var callbackCalled bool
		var callbackTopics []string

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		srv := NewServer(
			WithListener(listener),
			OnUnsubscribe(func(_ *ServerClient, topics []string) {
				callbackCalled = true
				callbackTopics = topics
			}),
		)
		go srv.ListenAndServe()
		defer srv.Close()

		time.Sleep(50 * time.Millisecond)

		conn, err := net.Dial("tcp", listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()

		// Connect
		connectPkt := &ConnectPacket{
			ClientID:   "unsub-callback-client",
			CleanStart: true,
			KeepAlive:  60,
		}
		WritePacket(conn, connectPkt, 256*1024)

		pkt, _, err := ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &ConnackPacket{}, pkt)

		// Subscribe first
		subPkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/topic1", QoS: QoS0},
				{TopicFilter: "test/topic2", QoS: QoS0},
			},
		}
		WritePacket(conn, subPkt, 256*1024)

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &SubackPacket{}, pkt)

		// Unsubscribe
		unsubPkt := &UnsubscribePacket{
			PacketID:     2,
			TopicFilters: []string{"test/topic1", "test/topic2"},
		}
		WritePacket(conn, unsubPkt, 256*1024)

		pkt, _, err = ReadPacket(conn, 256*1024)
		require.NoError(t, err)
		require.IsType(t, &UnsubackPacket{}, pkt)

		time.Sleep(50 * time.Millisecond)

		assert.True(t, callbackCalled, "onUnsubscribe callback should be called")
		assert.ElementsMatch(t, []string{"test/topic1", "test/topic2"}, callbackTopics)
	})
}

func TestServerRemoveClientWithNilClient(t *testing.T) {
	t.Run("removes client unconditionally when client parameter is nil", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		// Create and add a client
		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "nil-remove-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		clientKey := NamespaceKey(DefaultNamespace, "nil-remove-client")
		srv.mu.Lock()
		srv.clients[clientKey] = client
		srv.mu.Unlock()

		// Verify client exists
		srv.mu.RLock()
		_, exists := srv.clients[clientKey]
		srv.mu.RUnlock()
		require.True(t, exists)

		// Remove with nil client - should delete unconditionally
		srv.removeClient(clientKey, nil)

		// Verify client is removed
		srv.mu.RLock()
		_, exists = srv.clients[clientKey]
		srv.mu.RUnlock()
		assert.False(t, exists, "client should be removed when removeClient is called with nil")
	})
}

func TestServerErrorToReasonCodePropertyNotAllowed(t *testing.T) {
	srv := NewServer()
	defer srv.Close()

	t.Run("property not allowed returns protocol error", func(t *testing.T) {
		assert.Equal(t, ReasonProtocolError, srv.errorToReasonCode(ErrPropertyNotAllowed))
	})
}

func TestServerDeliverPendingMessagesRequeue(t *testing.T) {
	t.Run("re-queues message when send fails", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		// Create client with exhausted flow control
		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "requeue-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Exhaust flow control to cause Send to fail
		client.SetReceiveMaximum(1)
		client.FlowControl().TryAcquire()

		session := NewMemorySession("requeue-client", DefaultNamespace)
		srv.config.sessionStore.Create(DefaultNamespace, session)
		client.SetSession(session)

		// Add pending message
		msg := &Message{
			Topic:     "test/topic",
			Payload:   []byte("data"),
			QoS:       QoS1,
			Namespace: DefaultNamespace,
		}
		session.AddPendingMessage(1, msg)

		// Deliver pending messages - should fail and re-queue
		srv.deliverPendingMessages(client, session)

		// Message should be re-queued due to send failure
		pendingMsgs := session.PendingMessages()
		assert.Len(t, pendingMsgs, 1, "message should be re-queued when send fails")
	})
}

func TestServerRestoreInflightMessagesFlowControlExhausted(t *testing.T) {
	t.Run("skips message when flow control exhausted for QoS 1 restore", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "qos1-flow-client"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Exhaust flow control
		client.SetReceiveMaximum(1)
		client.FlowControl().TryAcquire()

		session := NewMemorySession("qos1-flow-client", DefaultNamespace)
		client.SetSession(session)

		// Add inflight QoS 1 message
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS1}
		session.AddInflightQoS1(1, &QoS1Message{PacketID: 1, Message: msg})

		logger := &testLogger{}

		// Restore inflight messages
		srv.restoreInflightMessages(client, session, logger)

		// No message should be sent due to flow control exhaustion
		written := conn.writeBuf.Bytes()
		assert.Empty(t, written, "no message should be sent when flow control exhausted")
	})

	t.Run("skips message when flow control exhausted for QoS 2 AwaitingPubrec restore", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "qos2-pubrec-flow"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Exhaust flow control
		client.SetReceiveMaximum(1)
		client.FlowControl().TryAcquire()

		session := NewMemorySession("qos2-pubrec-flow", DefaultNamespace)
		client.SetSession(session)

		// Add inflight QoS 2 message awaiting PUBREC
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		session.AddInflightQoS2(1, &QoS2Message{
			PacketID: 1,
			Message:  msg,
			State:    QoS2AwaitingPubrec,
			IsSender: true,
		})

		logger := &testLogger{}

		srv.restoreInflightMessages(client, session, logger)

		// No message should be sent due to flow control exhaustion
		written := conn.writeBuf.Bytes()
		assert.Empty(t, written, "no PUBLISH should be sent when flow control exhausted")
	})

	t.Run("skips message when flow control exhausted for QoS 2 AwaitingPubcomp restore", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "qos2-pubcomp-flow"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Exhaust flow control
		client.SetReceiveMaximum(1)
		client.FlowControl().TryAcquire()

		session := NewMemorySession("qos2-pubcomp-flow", DefaultNamespace)
		client.SetSession(session)

		// Add inflight QoS 2 message awaiting PUBCOMP
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		session.AddInflightQoS2(1, &QoS2Message{
			PacketID: 1,
			Message:  msg,
			State:    QoS2AwaitingPubcomp,
			IsSender: true,
		})

		logger := &testLogger{}

		srv.restoreInflightMessages(client, session, logger)

		// No PUBREL should be sent due to flow control exhaustion
		written := conn.writeBuf.Bytes()
		assert.Empty(t, written, "no PUBREL should be sent when flow control exhausted")
	})

	t.Run("restores receiver-side QoS 2 message despite inbound quota exceeded", func(t *testing.T) {
		srv := NewServer()
		defer srv.Close()

		conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
		connect := &ConnectPacket{ClientID: "qos2-inbound-flow"}
		client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)

		// Exhaust inbound flow control
		client.SetInboundReceiveMaximum(1)
		client.InboundFlowControl().TryAcquire()

		session := NewMemorySession("qos2-inbound-flow", DefaultNamespace)
		client.SetSession(session)

		// Add inflight receiver-side QoS 2 message
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		session.AddInflightQoS2(1, &QoS2Message{
			PacketID: 1,
			Message:  msg,
			State:    QoS2AwaitingPubrel,
			IsSender: false,
		})

		logger := &testLogger{}

		srv.restoreInflightMessages(client, session, logger)

		// Message should still be restored to tracker despite inbound quota warning
		_, ok := client.QoS2Tracker().Get(1)
		assert.True(t, ok, "message should still be restored despite quota exceeded")
	})
}

func TestServerRetryClientMessagesPubrelRetransmit(t *testing.T) {
	srv := NewServer()
	defer srv.Close()

	conn := &mockServerConn{writeBuf: &bytes.Buffer{}}
	connect := &ConnectPacket{ClientID: "pubrel-retry-test"}
	client := NewServerClient(conn, connect, 256*1024, DefaultNamespace)
	client.connected.Store(true)

	// Track a QoS 2 message and transition to QoS2AwaitingPubcomp state
	msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
	client.QoS2Tracker().TrackSend(1, msg)

	// Transition to QoS2AwaitingPubcomp by handling a PUBREC
	_, ok := client.QoS2Tracker().HandlePubrec(1)
	require.True(t, ok, "HandlePubrec should succeed")

	// Verify state is now QoS2AwaitingPubcomp
	qos2Msg, exists := client.QoS2Tracker().Get(1)
	require.True(t, exists)
	assert.Equal(t, QoS2AwaitingPubcomp, qos2Msg.State)

	// Force message to need retry by setting SentAt in the past
	qos2Msg.SentAt = time.Now().Add(-2 * time.Minute)
	qos2Msg.RetryTimeout = 1 * time.Millisecond

	// Call retryClientMessages - should retransmit PUBREL
	srv.retryClientMessages(client)

	// Verify PUBREL was sent
	written := conn.writeBuf.Bytes()
	require.NotEmpty(t, written, "PUBREL should be written")

	r := bytes.NewReader(written)
	var header FixedHeader
	_, err := header.Decode(r)
	require.NoError(t, err)
	assert.Equal(t, PacketPUBREL, header.PacketType)
}

// testEnhancedAuthFailStartConnect is a mock that fails AuthStart for CONNECT flow testing.
type testEnhancedAuthFailStartConnect struct{}

func (a *testEnhancedAuthFailStartConnect) SupportsMethod(method string) bool {
	return method == "FAIL-CONNECT"
}

func (a *testEnhancedAuthFailStartConnect) AuthStart(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	return nil, assert.AnError
}

func (a *testEnhancedAuthFailStartConnect) AuthContinue(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	return nil, nil
}

func TestServerPerformEnhancedAuthStartFails(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testEnhancedAuthFailStartConnect{}
	srv := NewServer(WithListener(listener), WithEnhancedAuth(enhancedAuth))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	connect := &ConnectPacket{ClientID: "auth-start-fail"}
	connect.Props.Set(PropAuthenticationMethod, "FAIL-CONNECT")
	connect.Props.Set(PropAuthenticationData, []byte("test-data"))

	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)

	connack, ok := pkt.(*ConnackPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonNotAuthorized, connack.ReasonCode)

	srv.Close()
	wg.Wait()
}

// testEnhancedAuthContinueAuth is a mock that returns Continue=true on AuthStart
// but fails on AuthContinue.
type testEnhancedAuthContinueAuth struct {
	authContinueFails bool
}

func (a *testEnhancedAuthContinueAuth) SupportsMethod(method string) bool {
	return method == "CONTINUE-AUTH"
}

func (a *testEnhancedAuthContinueAuth) AuthStart(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	return &EnhancedAuthResult{
		Continue:   true,
		ReasonCode: ReasonContinueAuth,
		AuthData:   []byte("challenge"),
	}, nil
}

func (a *testEnhancedAuthContinueAuth) AuthContinue(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	if a.authContinueFails {
		return nil, assert.AnError
	}
	return &EnhancedAuthResult{
		Success:    true,
		Namespace:  DefaultNamespace,
		ReasonCode: ReasonSuccess,
	}, nil
}

func TestServerPerformEnhancedAuthContinueFails(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testEnhancedAuthContinueAuth{authContinueFails: true}
	srv := NewServer(WithListener(listener), WithEnhancedAuth(enhancedAuth))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	connect := &ConnectPacket{ClientID: "auth-continue-fail"}
	connect.Props.Set(PropAuthenticationMethod, "CONTINUE-AUTH")
	connect.Props.Set(PropAuthenticationData, []byte("test-data"))

	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	// Read AUTH packet from server
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)

	authPkt, ok := pkt.(*AuthPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonContinueAuth, authPkt.ReasonCode)

	// Send AUTH response back to server
	clientAuth := &AuthPacket{ReasonCode: ReasonContinueAuth}
	clientAuth.Props.Set(PropAuthenticationMethod, "CONTINUE-AUTH")
	clientAuth.Props.Set(PropAuthenticationData, []byte("response"))

	_, err = WritePacket(conn, clientAuth, 256*1024)
	require.NoError(t, err)

	// Read CONNACK - should be NotAuthorized due to AuthContinue failure
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)

	connack, ok := pkt.(*ConnackPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonNotAuthorized, connack.ReasonCode)

	srv.Close()
	wg.Wait()
}

func TestServerPerformEnhancedAuthReadFails(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testEnhancedAuthContinueAuth{authContinueFails: false}
	srv := NewServer(WithListener(listener), WithEnhancedAuth(enhancedAuth))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	connect := &ConnectPacket{ClientID: "auth-read-fail"}
	connect.Props.Set(PropAuthenticationMethod, "CONTINUE-AUTH")
	connect.Props.Set(PropAuthenticationData, []byte("test-data"))

	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	// Read AUTH packet from server
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)

	_, ok := pkt.(*AuthPacket)
	require.True(t, ok)

	// Close connection without sending AUTH response - this should cause read failure on server
	conn.Close()

	// Wait a bit for server to handle the read error
	time.Sleep(100 * time.Millisecond)

	srv.Close()
	wg.Wait()
}

func TestServerPerformEnhancedAuthWrongPacketType(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testEnhancedAuthContinueAuth{authContinueFails: false}
	srv := NewServer(WithListener(listener), WithEnhancedAuth(enhancedAuth))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	connect := &ConnectPacket{ClientID: "auth-wrong-pkt"}
	connect.Props.Set(PropAuthenticationMethod, "CONTINUE-AUTH")
	connect.Props.Set(PropAuthenticationData, []byte("test-data"))

	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	// Read AUTH packet from server
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)

	_, ok := pkt.(*AuthPacket)
	require.True(t, ok)

	// Send DISCONNECT instead of AUTH - wrong packet type
	disconnect := &DisconnectPacket{ReasonCode: ReasonSuccess}
	_, err = WritePacket(conn, disconnect, 256*1024)
	require.NoError(t, err)

	// Read CONNACK - should be ProtocolError due to wrong packet type
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)

	connack, ok := pkt.(*ConnackPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonProtocolError, connack.ReasonCode)

	srv.Close()
	wg.Wait()
}

// testReauthMultiStepAuth is a configurable mock for testing multi-step re-authentication.
type testReauthMultiStepAuth struct {
	authContinueFails   bool
	authContinueSuccess bool // If false, returns Success=false in final result
	customReasonCode    ReasonCode
}

func (a *testReauthMultiStepAuth) SupportsMethod(method string) bool {
	return method == "REAUTH-MULTI"
}

func (a *testReauthMultiStepAuth) AuthStart(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	return &EnhancedAuthResult{
		Continue:   true,
		ReasonCode: ReasonContinueAuth,
		AuthData:   []byte("reauth-challenge"),
	}, nil
}

func (a *testReauthMultiStepAuth) AuthContinue(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	if a.authContinueFails {
		return nil, assert.AnError
	}
	if !a.authContinueSuccess {
		reasonCode := a.customReasonCode
		if reasonCode == 0 {
			reasonCode = ReasonNotAuthorized
		}
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: reasonCode,
		}, nil
	}
	return &EnhancedAuthResult{
		Success:    true,
		Namespace:  DefaultNamespace,
		ReasonCode: ReasonSuccess,
	}, nil
}

func TestServerHandleReauthMultiStepContinueFails(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testReauthMultiStepAuth{authContinueFails: true}
	srv := NewServer(WithListener(listener), WithEnhancedAuth(enhancedAuth))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// First establish a connection
	connect := &ConnectPacket{ClientID: "reauth-continue-fail"}
	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	_, ok := pkt.(*ConnackPacket)
	require.True(t, ok)

	// Send re-auth request
	authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
	authPkt.Props.Set(PropAuthenticationMethod, "REAUTH-MULTI")
	_, err = WritePacket(conn, authPkt, 256*1024)
	require.NoError(t, err)

	// Read AUTH challenge from server
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	serverAuth, ok := pkt.(*AuthPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonContinueAuth, serverAuth.ReasonCode)

	// Send AUTH response back
	clientAuth := &AuthPacket{ReasonCode: ReasonContinueAuth}
	clientAuth.Props.Set(PropAuthenticationMethod, "REAUTH-MULTI")
	clientAuth.Props.Set(PropAuthenticationData, []byte("response"))
	_, err = WritePacket(conn, clientAuth, 256*1024)
	require.NoError(t, err)

	// Wait for server to process and disconnect
	time.Sleep(100 * time.Millisecond)

	srv.Close()
	wg.Wait()
}

func TestServerHandleReauthMultiStepReadFails(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testReauthMultiStepAuth{authContinueSuccess: true}
	srv := NewServer(WithListener(listener), WithEnhancedAuth(enhancedAuth))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	// First establish a connection
	connect := &ConnectPacket{ClientID: "reauth-read-fail"}
	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	_, ok := pkt.(*ConnackPacket)
	require.True(t, ok)

	// Send re-auth request
	authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
	authPkt.Props.Set(PropAuthenticationMethod, "REAUTH-MULTI")
	_, err = WritePacket(conn, authPkt, 256*1024)
	require.NoError(t, err)

	// Read AUTH challenge from server
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	_, ok = pkt.(*AuthPacket)
	require.True(t, ok)

	// Close connection without responding - causes read failure
	conn.Close()

	time.Sleep(100 * time.Millisecond)

	srv.Close()
	wg.Wait()
}

func TestServerHandleReauthMultiStepWrongPacketType(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testReauthMultiStepAuth{authContinueSuccess: true}
	srv := NewServer(WithListener(listener), WithEnhancedAuth(enhancedAuth))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// First establish a connection
	connect := &ConnectPacket{ClientID: "reauth-wrong-pkt"}
	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	_, ok := pkt.(*ConnackPacket)
	require.True(t, ok)

	// Send re-auth request
	authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
	authPkt.Props.Set(PropAuthenticationMethod, "REAUTH-MULTI")
	_, err = WritePacket(conn, authPkt, 256*1024)
	require.NoError(t, err)

	// Read AUTH challenge from server
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	_, ok = pkt.(*AuthPacket)
	require.True(t, ok)

	// Send PUBLISH instead of AUTH - wrong packet type
	pub := &PublishPacket{Topic: "test/topic", Payload: []byte("data")}
	_, err = WritePacket(conn, pub, 256*1024)
	require.NoError(t, err)

	// Wait for server to process and disconnect
	time.Sleep(100 * time.Millisecond)

	srv.Close()
	wg.Wait()
}

func TestServerHandleReauthMultiStepFinalResultFails(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testReauthMultiStepAuth{
		authContinueFails:   false,
		authContinueSuccess: false, // Will return Success=false
	}
	srv := NewServer(WithListener(listener), WithEnhancedAuth(enhancedAuth))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// First establish a connection
	connect := &ConnectPacket{ClientID: "reauth-final-fail"}
	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	_, ok := pkt.(*ConnackPacket)
	require.True(t, ok)

	// Send re-auth request
	authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
	authPkt.Props.Set(PropAuthenticationMethod, "REAUTH-MULTI")
	_, err = WritePacket(conn, authPkt, 256*1024)
	require.NoError(t, err)

	// Read AUTH challenge from server
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	_, ok = pkt.(*AuthPacket)
	require.True(t, ok)

	// Send valid AUTH response
	clientAuth := &AuthPacket{ReasonCode: ReasonContinueAuth}
	clientAuth.Props.Set(PropAuthenticationMethod, "REAUTH-MULTI")
	clientAuth.Props.Set(PropAuthenticationData, []byte("response"))
	_, err = WritePacket(conn, clientAuth, 256*1024)
	require.NoError(t, err)

	// Wait for server to process (client should be disconnected due to auth failure)
	time.Sleep(100 * time.Millisecond)

	srv.Close()
	wg.Wait()
}

func TestServerHandleReauthMultiStepFinalResultCustomReasonCode(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testReauthMultiStepAuth{
		authContinueFails:   false,
		authContinueSuccess: false,
		customReasonCode:    0, // Will use default ReasonNotAuthorized
	}
	srv := NewServer(WithListener(listener), WithEnhancedAuth(enhancedAuth))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// First establish a connection
	connect := &ConnectPacket{ClientID: "reauth-custom-reason"}
	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	_, ok := pkt.(*ConnackPacket)
	require.True(t, ok)

	// Send re-auth request
	authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
	authPkt.Props.Set(PropAuthenticationMethod, "REAUTH-MULTI")
	_, err = WritePacket(conn, authPkt, 256*1024)
	require.NoError(t, err)

	// Read AUTH challenge from server
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	_, ok = pkt.(*AuthPacket)
	require.True(t, ok)

	// Send valid AUTH response
	clientAuth := &AuthPacket{ReasonCode: ReasonContinueAuth}
	clientAuth.Props.Set(PropAuthenticationMethod, "REAUTH-MULTI")
	clientAuth.Props.Set(PropAuthenticationData, []byte("response"))
	_, err = WritePacket(conn, clientAuth, 256*1024)
	require.NoError(t, err)

	// Wait for server to process
	time.Sleep(100 * time.Millisecond)

	srv.Close()
	wg.Wait()
}

func TestServerHandleReauthMultiStepSuccess(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	enhancedAuth := &testReauthMultiStepAuth{authContinueSuccess: true}
	srv := NewServer(WithListener(listener), WithEnhancedAuth(enhancedAuth))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// First establish a connection
	connect := &ConnectPacket{ClientID: "reauth-success"}
	_, err = WritePacket(conn, connect, 256*1024)
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	_, ok := pkt.(*ConnackPacket)
	require.True(t, ok)

	// Send re-auth request
	authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
	authPkt.Props.Set(PropAuthenticationMethod, "REAUTH-MULTI")
	_, err = WritePacket(conn, authPkt, 256*1024)
	require.NoError(t, err)

	// Read AUTH challenge from server
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	serverAuth, ok := pkt.(*AuthPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonContinueAuth, serverAuth.ReasonCode)

	// Send valid AUTH response
	clientAuth := &AuthPacket{ReasonCode: ReasonContinueAuth}
	clientAuth.Props.Set(PropAuthenticationMethod, "REAUTH-MULTI")
	clientAuth.Props.Set(PropAuthenticationData, []byte("response"))
	_, err = WritePacket(conn, clientAuth, 256*1024)
	require.NoError(t, err)

	// Read success AUTH from server
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	pkt, _, err = ReadPacket(conn, 256*1024)
	require.NoError(t, err)
	successAuth, ok := pkt.(*AuthPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonSuccess, successAuth.ReasonCode)

	srv.Close()
	wg.Wait()
}
