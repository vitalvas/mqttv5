package mqttv5

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWSConnReadWrite(t *testing.T) {
	// Create test server
	var serverConn *WSConn
	serverReady := make(chan struct{})

	handler := NewWSHandler(func(conn Conn) {
		serverConn = conn.(*WSConn)
		close(serverReady)

		// Echo server
		buf := make([]byte, 1024)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			_, _ = conn.Write(buf[:n])
		}
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	// Connect client
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	dialer := NewWSDialer()
	conn, err := dialer.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	defer conn.Close()

	<-serverReady
	assert.NotNil(t, serverConn)

	// Test write and read
	testData := []byte("hello mqtt")
	n, err := conn.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)

	buf := make([]byte, 1024)
	n, err = conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, testData, buf[:n])
}

func TestWSConnAddresses(t *testing.T) {
	handler := NewWSHandler(func(conn Conn) {
		assert.NotNil(t, conn.LocalAddr())
		assert.NotNil(t, conn.RemoteAddr())
		conn.Close()
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	dialer := NewWSDialer()
	conn, err := dialer.Dial(context.Background(), wsURL)
	require.NoError(t, err)

	assert.NotNil(t, conn.LocalAddr())
	assert.NotNil(t, conn.RemoteAddr())
	conn.Close()
}

func TestWSConnDeadlines(t *testing.T) {
	handler := NewWSHandler(func(conn Conn) {
		time.Sleep(100 * time.Millisecond)
		conn.Close()
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	dialer := NewWSDialer()
	conn, err := dialer.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	defer conn.Close()

	// Test SetDeadline
	err = conn.SetDeadline(time.Now().Add(10 * time.Millisecond))
	assert.NoError(t, err)

	// Test SetReadDeadline
	err = conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	assert.NoError(t, err)

	// Test SetWriteDeadline
	err = conn.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
	assert.NoError(t, err)
}

func TestWSDialerWithSubprotocol(t *testing.T) {
	subprotocolCh := make(chan string, 1)

	upgrader := websocket.Upgrader{
		Subprotocols: []string{WebSocketSubprotocol},
		CheckOrigin:  func(_ *http.Request) bool { return true },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		subprotocolCh <- conn.Subprotocol()
		conn.Close()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	dialer := NewWSDialer()
	conn, err := dialer.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	conn.Close()

	select {
	case subprotocol := <-subprotocolCh:
		assert.Equal(t, WebSocketSubprotocol, subprotocol)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subprotocol")
	}
}

func TestWSHandlerMQTTPackets(t *testing.T) {
	handler := NewWSHandler(func(conn Conn) {
		defer conn.Close()

		// Read CONNECT
		packet, _, err := ReadPacket(conn, 0)
		if err != nil {
			return
		}

		if packet.Type() == PacketCONNECT {
			// Send CONNACK
			_, _ = WritePacket(conn, &ConnackPacket{ReasonCode: ReasonSuccess}, 0)
		}
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	dialer := NewWSDialer()
	conn, err := dialer.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	defer conn.Close()

	// Send CONNECT
	connectPacket := &ConnectPacket{
		ClientID:   "test-client",
		CleanStart: true,
		KeepAlive:  60,
	}
	_, err = WritePacket(conn, connectPacket, 0)
	require.NoError(t, err)

	// Read CONNACK
	packet, _, err := ReadPacket(conn, 0)
	require.NoError(t, err)
	assert.Equal(t, PacketCONNACK, packet.Type())

	connack, ok := packet.(*ConnackPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonSuccess, connack.ReasonCode)
}

func BenchmarkWSRoundTrip(b *testing.B) {
	handler := NewWSHandler(func(conn Conn) {
		defer conn.Close()
		for {
			packet, _, err := ReadPacket(conn, 0)
			if err != nil {
				return
			}
			if packet.Type() == PacketPINGREQ {
				_, _ = WritePacket(conn, &PingrespPacket{}, 0)
			}
		}
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	dialer := NewWSDialer()
	conn, err := dialer.Dial(context.Background(), wsURL)
	require.NoError(b, err)
	defer conn.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = WritePacket(conn, &PingreqPacket{}, 0)
		_, _, _ = ReadPacket(conn, 0)
	}
}
