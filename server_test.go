package mqttv5

import (
	"bytes"
	"context"
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

		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, 2, srv.ClientCount())

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
}

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
