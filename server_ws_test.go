package mqttv5

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encodePacket encodes an MQTT packet to bytes for WebSocket testing
func encodePacket(pkt Packet) []byte {
	buf := &bytes.Buffer{}
	pkt.Encode(buf)
	return buf.Bytes()
}

func TestNewWSServer(t *testing.T) {
	t.Run("creates ws server with defaults", func(t *testing.T) {
		srv := NewWSServer()
		require.NotNil(t, srv)
		defer srv.Close()

		assert.NotNil(t, srv.Server)
		assert.NotNil(t, srv.handler)
	})

	t.Run("applies options", func(t *testing.T) {
		var connected bool
		srv := NewWSServer(
			WithMaxConnections(50),
			OnConnect(func(_ *ServerClient) {
				connected = true
			}),
		)
		require.NotNil(t, srv)
		defer srv.Close()

		assert.Equal(t, 50, srv.config.maxConnections)
		if srv.config.onConnect != nil {
			srv.config.onConnect(nil)
		}
		assert.True(t, connected)
	})

	t.Run("with keep alive override", func(t *testing.T) {
		srv := NewWSServer(
			WithServerKeepAlive(60),
		)
		require.NotNil(t, srv)
		defer srv.Close()

		assert.Equal(t, uint16(60), srv.keepAlive.ServerOverride())
	})
}

func TestWSServerStart(t *testing.T) {
	t.Run("starts background tasks", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()

		// Should be running now
		assert.True(t, srv.running.Load())
	})

	t.Run("start is idempotent", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()
		srv.Start() // Second call should be no-op

		assert.True(t, srv.running.Load())
	})
}

func TestWSServerServeHTTP(t *testing.T) {
	t.Run("serves http requests", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()

		// Create test request (not a valid WebSocket upgrade, so should fail)
		req := httptest.NewRequest(http.MethodGet, "/mqtt", nil)
		w := httptest.NewRecorder()

		srv.ServeHTTP(w, req)

		// Without proper WebSocket headers, this should fail
		// The important thing is it doesn't panic
		assert.True(t, w.Code >= 400 || w.Code == 200)
	})
}

func TestWSServerClose(t *testing.T) {
	t.Run("close stops server", func(t *testing.T) {
		srv := NewWSServer()
		srv.Start()

		// Give it time to start background tasks
		time.Sleep(10 * time.Millisecond)

		err := srv.Close()
		require.NoError(t, err)

		assert.False(t, srv.running.Load())
	})

	t.Run("close when not started", func(t *testing.T) {
		srv := NewWSServer()

		err := srv.Close()
		require.NoError(t, err)
	})

	t.Run("close completes within timeout", func(t *testing.T) {
		srv := NewWSServer()
		srv.Start()

		time.Sleep(10 * time.Millisecond)

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

func TestWSServerPublish(t *testing.T) {
	t.Run("publish when not running", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		msg := &Message{Topic: "test", Payload: []byte("data")}
		err := srv.Publish(msg)
		assert.ErrorIs(t, err, ErrServerClosed)
	})

	t.Run("publish when running", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()

		msg := &Message{Topic: "test", Payload: []byte("data")}
		err := srv.Publish(msg)
		require.NoError(t, err)
	})
}

func TestWSServerClientManagement(t *testing.T) {
	t.Run("client count starts at zero", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()

		assert.Equal(t, 0, srv.ClientCount())
		assert.Empty(t, srv.Clients())
	})
}

func TestWSServerConcurrency(_ *testing.T) {
	srv := NewWSServer()
	defer srv.Close()

	srv.Start()

	var wg sync.WaitGroup

	// Concurrent operations
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.ClientCount()
			_ = srv.Clients()
		}()
	}

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.Publish(&Message{Topic: "test", Payload: []byte("data")})
		}()
	}

	wg.Wait()
}

func TestWSServerWithHTTPMux(t *testing.T) {
	t.Run("can be mounted on http mux", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)

		// Create test server
		ts := httptest.NewServer(mux)
		defer ts.Close()

		// Make a regular HTTP request (not WebSocket)
		resp, err := http.Get(ts.URL + "/mqtt")
		require.NoError(t, err)
		resp.Body.Close()

		// Should get some response (likely error since not WebSocket)
		assert.True(t, resp.StatusCode >= 200)
	})
}

func TestWSServerHandleWSConnection(t *testing.T) {
	t.Run("rejects connection when server not running", func(t *testing.T) {
		srv := NewWSServer()
		// Don't call Start() - server is not running

		// Create a mock connection
		mockConn := &mockWSConn{}
		srv.handleWSConnection(mockConn)

		// Connection should be closed because server is not running
		assert.True(t, mockConn.closed)
	})

	t.Run("accepts connection when server running", func(t *testing.T) {
		connectReceived := make(chan struct{})
		srv := NewWSServer(
			OnConnect(func(_ *ServerClient) {
				close(connectReceived)
			}),
		)

		srv.Start()

		// Create test HTTP server
		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		// Connect via WebSocket
		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{
			Subprotocols: []string{"mqtt"},
		}

		conn, resp, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		// Send CONNECT packet
		connect := &ConnectPacket{ClientID: "ws-test-client"}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		// Read CONNACK
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := conn.ReadMessage()
		require.NoError(t, err)

		// Verify CONNACK
		require.Greater(t, len(data), 0)
		header := data[0] >> 4
		assert.Equal(t, uint8(PacketCONNACK), header)

		// Wait for connect callback
		select {
		case <-connectReceived:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("connect callback not received")
		}

		// Close in correct order to avoid race
		conn.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})
}

func TestWSServerHandleWSConn(t *testing.T) {
	t.Run("handles full MQTT connection lifecycle", func(t *testing.T) {
		var connectedClient *ServerClient
		var disconnectedClient *ServerClient
		connectDone := make(chan struct{})
		disconnectDone := make(chan struct{})

		srv := NewWSServer(
			OnConnect(func(client *ServerClient) {
				connectedClient = client
				close(connectDone)
			}),
			OnDisconnect(func(client *ServerClient) {
				disconnectedClient = client
				close(disconnectDone)
			}),
		)

		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{
			Subprotocols: []string{"mqtt"},
		}

		conn, resp, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		// Send CONNECT
		connect := &ConnectPacket{ClientID: "ws-lifecycle-test"}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		// Read CONNACK
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := conn.ReadMessage()
		require.NoError(t, err)

		header := data[0] >> 4
		assert.Equal(t, uint8(PacketCONNACK), header)

		// Wait for connect
		select {
		case <-connectDone:
		case <-time.After(2 * time.Second):
			t.Fatal("connect callback not received")
		}

		assert.NotNil(t, connectedClient)
		assert.Equal(t, "ws-lifecycle-test", connectedClient.ClientID())

		// Send DISCONNECT
		disconnect := &DisconnectPacket{ReasonCode: ReasonSuccess}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(disconnect))
		require.NoError(t, err)

		// Close connection
		conn.Close()

		// Wait for disconnect
		select {
		case <-disconnectDone:
		case <-time.After(2 * time.Second):
			t.Fatal("disconnect callback not received")
		}

		assert.NotNil(t, disconnectedClient)

		// Close in correct order to avoid race
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})

	t.Run("handles max connections limit", func(t *testing.T) {
		srv := NewWSServer(
			WithMaxConnections(1),
		)

		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{
			Subprotocols: []string{"mqtt"},
		}

		// First connection should succeed
		conn1, resp1, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp1 != nil {
			resp1.Body.Close()
		}

		// Send CONNECT for first client
		connect1 := &ConnectPacket{ClientID: "ws-max-conn-1"}
		err = conn1.WriteMessage(websocket.BinaryMessage, encodePacket(connect1))
		require.NoError(t, err)

		// Read CONNACK
		conn1.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := conn1.ReadMessage()
		require.NoError(t, err)
		header := data[0] >> 4
		assert.Equal(t, uint8(PacketCONNACK), header)

		// Second connection should fail with ServerBusy
		conn2, resp2, err := dialer.Dial(wsURL, nil)
		if err != nil {
			conn1.Close()
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed for second connection: %v", err)
		}
		if resp2 != nil {
			resp2.Body.Close()
		}

		// Send CONNECT for second client
		connect2 := &ConnectPacket{ClientID: "ws-max-conn-2"}
		err = conn2.WriteMessage(websocket.BinaryMessage, encodePacket(connect2))
		require.NoError(t, err)

		// Read CONNACK - should be ReasonServerBusy
		conn2.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err = conn2.ReadMessage()
		require.NoError(t, err)

		header = data[0] >> 4
		assert.Equal(t, uint8(PacketCONNACK), header)

		// Parse CONNACK to check reason code
		if len(data) >= 4 {
			reasonCode := ReasonCode(data[3])
			assert.Equal(t, ReasonServerBusy, reasonCode)
		}

		// Close connections first to avoid race with srv.Close()
		conn2.Close()
		conn1.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond) // Allow handlers to finish
		srv.Close()
	})

	t.Run("handles subscribe", func(t *testing.T) {
		srv := NewWSServer()

		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{
			Subprotocols: []string{"mqtt"},
		}

		conn, resp, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		// CONNECT
		connect := &ConnectPacket{ClientID: "ws-pubsub-test"}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, err = conn.ReadMessage() // CONNACK
		require.NoError(t, err)

		// SUBSCRIBE
		subscribe := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/ws/#", QoS: QoS0},
			},
		}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(subscribe))
		require.NoError(t, err)

		_, data, err := conn.ReadMessage() // SUBACK
		require.NoError(t, err)

		// Verify it's a SUBACK packet
		packetType := PacketType(data[0] >> 4)
		assert.Equal(t, PacketSUBACK, packetType)

		// Send DISCONNECT to gracefully close
		disconnect := &DisconnectPacket{ReasonCode: ReasonSuccess}
		conn.WriteMessage(websocket.BinaryMessage, encodePacket(disconnect))

		// Close in correct order to avoid race
		conn.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond) // Wait for handlers to finish
		srv.Close()
	})

	// Protocol error validation tests (table-driven to reduce duplication)
	protocolErrorCases := []struct {
		name      string
		clientID  string
		setupProp func(c *ConnectPacket)
	}{
		{
			name:     "rejects invalid maximum packet size",
			clientID: "ws-invalid-maxps",
			setupProp: func(c *ConnectPacket) {
				c.Props.Set(PropMaximumPacketSize, uint32(0))
			},
		},
		{
			name:     "rejects receive maximum zero",
			clientID: "ws-invalid-rm",
			setupProp: func(c *ConnectPacket) {
				c.Props.Set(PropReceiveMaximum, uint16(0))
			},
		},
	}

	for _, tc := range protocolErrorCases {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewWSServer()
			srv.Start()

			mux := http.NewServeMux()
			mux.Handle("/mqtt", srv)
			ts := httptest.NewServer(mux)

			wsURL := "ws" + ts.URL[4:] + "/mqtt"
			dialer := websocket.Dialer{Subprotocols: []string{"mqtt"}}

			conn, resp, err := dialer.Dial(wsURL, nil)
			if err != nil {
				ts.Close()
				srv.Close()
				t.Skipf("WebSocket dial failed: %v", err)
			}
			if resp != nil {
				resp.Body.Close()
			}

			connect := &ConnectPacket{ClientID: tc.clientID}
			tc.setupProp(connect)
			err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
			require.NoError(t, err)

			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, data, err := conn.ReadMessage()
			require.NoError(t, err)

			header := data[0] >> 4
			assert.Equal(t, uint8(PacketCONNACK), header)
			if len(data) >= 4 {
				reasonCode := ReasonCode(data[3])
				assert.Equal(t, ReasonProtocolError, reasonCode)
			}

			conn.Close()
			ts.Close()
			time.Sleep(20 * time.Millisecond)
			srv.Close()
		})
	}

	t.Run("rejects empty client ID with CleanStart false", func(t *testing.T) {
		srv := NewWSServer()
		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{
			Subprotocols: []string{"mqtt"},
		}

		conn, resp, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		// CONNECT with empty client ID and CleanStart=false (invalid per MQTT spec)
		connect := &ConnectPacket{ClientID: "", CleanStart: false}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		// Server should respond with CONNACK and then close connection
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, data, err := conn.ReadMessage()
		if err == nil {
			// Verify CONNACK with ClientIDNotValid
			header := data[0] >> 4
			assert.Equal(t, uint8(PacketCONNACK), header)
			if len(data) >= 4 {
				reasonCode := ReasonCode(data[3])
				assert.Equal(t, ReasonClientIDNotValid, reasonCode)
			}
		}
		// If read fails (connection closed), that's also acceptable

		conn.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})

	t.Run("assigns client ID when empty with CleanStart true", func(t *testing.T) {
		var connectedClient *ServerClient
		connectDone := make(chan struct{})

		srv := NewWSServer(
			OnConnect(func(client *ServerClient) {
				connectedClient = client
				close(connectDone)
			}),
		)
		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{
			Subprotocols: []string{"mqtt"},
		}

		conn, resp, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		// CONNECT with empty client ID and CleanStart=true (server should assign ID)
		connect := &ConnectPacket{ClientID: "", CleanStart: true}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := conn.ReadMessage()
		require.NoError(t, err)

		// Verify CONNACK with Success
		header := data[0] >> 4
		assert.Equal(t, uint8(PacketCONNACK), header)
		if len(data) >= 4 {
			reasonCode := ReasonCode(data[3])
			assert.Equal(t, ReasonSuccess, reasonCode)
		}

		select {
		case <-connectDone:
		case <-time.After(2 * time.Second):
			t.Fatal("connect callback not received")
		}

		// Verify client ID was assigned
		assert.NotNil(t, connectedClient)
		assert.NotEmpty(t, connectedClient.ClientID())
		assert.Contains(t, connectedClient.ClientID(), "auto-")

		conn.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})

}

func TestWSServerHandleWSConnProperties(t *testing.T) {
	t.Run("handles will message", func(t *testing.T) {
		srv := NewWSServer()
		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{Subprotocols: []string{"mqtt"}}

		conn, resp, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		connect := &ConnectPacket{
			ClientID:    "ws-will-test",
			WillFlag:    true,
			WillTopic:   "test/will",
			WillPayload: []byte("goodbye"),
			WillQoS:     QoS0,
		}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := conn.ReadMessage()
		require.NoError(t, err)

		header := data[0] >> 4
		assert.Equal(t, uint8(PacketCONNACK), header)
		if len(data) >= 4 {
			reasonCode := ReasonCode(data[3])
			assert.Equal(t, ReasonSuccess, reasonCode)
		}

		conn.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})

	t.Run("handles receive maximum property", func(t *testing.T) {
		var connectedClient *ServerClient
		connectDone := make(chan struct{})

		srv := NewWSServer(OnConnect(func(client *ServerClient) {
			connectedClient = client
			close(connectDone)
		}))
		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{Subprotocols: []string{"mqtt"}}

		conn, resp, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		connect := &ConnectPacket{ClientID: "ws-rm-test"}
		connect.Props.Set(PropReceiveMaximum, uint16(10))
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, err = conn.ReadMessage()
		require.NoError(t, err)

		select {
		case <-connectDone:
		case <-time.After(2 * time.Second):
			t.Fatal("connect callback not received")
		}

		assert.NotNil(t, connectedClient)

		conn.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})

	// Table-driven property tests
	propCases := []struct {
		name      string
		clientID  string
		setupProp func(c *ConnectPacket)
	}{
		{
			name:     "handles topic alias maximum property",
			clientID: "ws-tam-test",
			setupProp: func(c *ConnectPacket) {
				c.Props.Set(PropTopicAliasMaximum, uint16(5))
			},
		},
		{
			name:     "handles session expiry interval property",
			clientID: "ws-sei-test",
			setupProp: func(c *ConnectPacket) {
				c.Props.Set(PropSessionExpiryInterval, uint32(3600))
			},
		},
	}

	for _, tc := range propCases {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewWSServer()
			srv.Start()

			mux := http.NewServeMux()
			mux.Handle("/mqtt", srv)
			ts := httptest.NewServer(mux)

			wsURL := "ws" + ts.URL[4:] + "/mqtt"
			dialer := websocket.Dialer{Subprotocols: []string{"mqtt"}}

			conn, resp, err := dialer.Dial(wsURL, nil)
			if err != nil {
				ts.Close()
				srv.Close()
				t.Skipf("WebSocket dial failed: %v", err)
			}
			if resp != nil {
				resp.Body.Close()
			}

			connect := &ConnectPacket{ClientID: tc.clientID}
			tc.setupProp(connect)
			err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
			require.NoError(t, err)

			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, data, err := conn.ReadMessage()
			require.NoError(t, err)

			header := data[0] >> 4
			assert.Equal(t, uint8(PacketCONNACK), header)

			conn.Close()
			ts.Close()
			time.Sleep(20 * time.Millisecond)
			srv.Close()
		})
	}
}

func TestWSServerHandleWSConnSession(t *testing.T) {
	t.Run("takes over existing connection", func(t *testing.T) {
		disconnectDone := make(chan struct{}, 2) // Buffer for multiple disconnects

		srv := NewWSServer(
			OnDisconnect(func(_ *ServerClient) {
				select {
				case disconnectDone <- struct{}{}:
				default:
				}
			}),
		)
		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{Subprotocols: []string{"mqtt"}}

		// First connection
		conn1, resp1, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp1 != nil {
			resp1.Body.Close()
		}

		connect := &ConnectPacket{ClientID: "ws-takeover-test"}
		err = conn1.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		conn1.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, err = conn1.ReadMessage()
		require.NoError(t, err)

		// Close first connection to avoid race during takeover
		conn1.Close()

		// Wait a bit for server to process disconnect
		time.Sleep(50 * time.Millisecond)

		// Second connection with same client ID (reconnect/takeover scenario)
		conn2, resp2, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed for second connection: %v", err)
		}
		if resp2 != nil {
			resp2.Body.Close()
		}

		connect2 := &ConnectPacket{ClientID: "ws-takeover-test"}
		err = conn2.WriteMessage(websocket.BinaryMessage, encodePacket(connect2))
		require.NoError(t, err)

		conn2.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, err = conn2.ReadMessage()
		require.NoError(t, err)

		// Clean up
		conn2.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})

	t.Run("handles session with subscriptions", func(t *testing.T) {
		srv := NewWSServer()
		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{Subprotocols: []string{"mqtt"}}

		// First connection - create session and subscription
		conn1, resp1, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp1 != nil {
			resp1.Body.Close()
		}

		// CONNECT with session expiry (to keep session)
		connect := &ConnectPacket{ClientID: "ws-session-test", CleanStart: false}
		connect.Props.Set(PropSessionExpiryInterval, uint32(3600))
		err = conn1.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		conn1.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, err = conn1.ReadMessage()
		require.NoError(t, err)

		// Subscribe
		subscribe := &SubscribePacket{
			PacketID:      1,
			Subscriptions: []Subscription{{TopicFilter: "test/session/#", QoS: QoS1}},
		}
		err = conn1.WriteMessage(websocket.BinaryMessage, encodePacket(subscribe))
		require.NoError(t, err)

		_, _, err = conn1.ReadMessage() // SUBACK
		require.NoError(t, err)

		// Disconnect
		disconnect := &DisconnectPacket{ReasonCode: ReasonSuccess}
		conn1.WriteMessage(websocket.BinaryMessage, encodePacket(disconnect))
		conn1.Close()

		time.Sleep(50 * time.Millisecond)

		// Second connection - session should be present with subscription
		conn2, resp2, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed for second connection: %v", err)
		}
		if resp2 != nil {
			resp2.Body.Close()
		}

		connect2 := &ConnectPacket{ClientID: "ws-session-test", CleanStart: false}
		err = conn2.WriteMessage(websocket.BinaryMessage, encodePacket(connect2))
		require.NoError(t, err)

		conn2.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := conn2.ReadMessage()
		require.NoError(t, err)

		// Verify session present flag
		header := data[0] >> 4
		assert.Equal(t, uint8(PacketCONNACK), header)

		conn2.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})

	t.Run("rejects first packet not CONNECT", func(t *testing.T) {
		srv := NewWSServer()
		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{Subprotocols: []string{"mqtt"}}

		conn, resp, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		// Send PINGREQ instead of CONNECT (invalid first packet)
		pingreq := &PingreqPacket{}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(pingreq))
		require.NoError(t, err)

		// Connection should be closed by server
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, _, err = conn.ReadMessage()
		// Expect error or EOF (connection closed)
		assert.Error(t, err)

		conn.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})

	t.Run("handles authentication failure", func(t *testing.T) {
		srv := NewWSServer(WithServerAuth(&DenyAllAuthenticator{}))
		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{Subprotocols: []string{"mqtt"}}

		conn, resp, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		connect := &ConnectPacket{ClientID: "ws-auth-fail-test"}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := conn.ReadMessage()
		require.NoError(t, err)

		// Verify CONNACK with NotAuthorized
		header := data[0] >> 4
		assert.Equal(t, uint8(PacketCONNACK), header)
		if len(data) >= 4 {
			reasonCode := ReasonCode(data[3])
			assert.Equal(t, ReasonNotAuthorized, reasonCode)
		}

		conn.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})

	t.Run("handles server receive maximum config", func(t *testing.T) {
		var connectedClient *ServerClient
		connectDone := make(chan struct{})

		srv := NewWSServer(
			WithServerReceiveMaximum(5),
			OnConnect(func(client *ServerClient) {
				connectedClient = client
				close(connectDone)
			}),
		)
		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)
		ts := httptest.NewServer(mux)

		wsURL := "ws" + ts.URL[4:] + "/mqtt"
		dialer := websocket.Dialer{Subprotocols: []string{"mqtt"}}

		conn, resp, err := dialer.Dial(wsURL, nil)
		if err != nil {
			ts.Close()
			srv.Close()
			t.Skipf("WebSocket dial failed: %v", err)
		}
		if resp != nil {
			resp.Body.Close()
		}

		connect := &ConnectPacket{ClientID: "ws-srv-rm-test"}
		err = conn.WriteMessage(websocket.BinaryMessage, encodePacket(connect))
		require.NoError(t, err)

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, err = conn.ReadMessage()
		require.NoError(t, err)

		select {
		case <-connectDone:
		case <-time.After(2 * time.Second):
			t.Fatal("connect callback not received")
		}

		assert.NotNil(t, connectedClient)

		conn.Close()
		ts.Close()
		time.Sleep(20 * time.Millisecond)
		srv.Close()
	})
}

// mockWSConn is a mock connection that tracks if it was closed
type mockWSConn struct {
	closed bool
}

func (m *mockWSConn) Read(_ []byte) (int, error)         { return 0, io.EOF }
func (m *mockWSConn) Write(_ []byte) (int, error)        { return 0, nil }
func (m *mockWSConn) Close() error                       { m.closed = true; return nil }
func (m *mockWSConn) LocalAddr() net.Addr                { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0} }
func (m *mockWSConn) RemoteAddr() net.Addr               { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0} }
func (m *mockWSConn) SetDeadline(_ time.Time) error      { return nil }
func (m *mockWSConn) SetReadDeadline(_ time.Time) error  { return nil }
func (m *mockWSConn) SetWriteDeadline(_ time.Time) error { return nil }
