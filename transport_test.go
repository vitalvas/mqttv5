package mqttv5

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTCPConnection(t *testing.T) {
	t.Run("accept and dial", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		// Connect in goroutine
		done := make(chan struct{})
		go func() {
			defer close(done)
			conn, err := net.Dial("tcp", listener.Addr().String())
			require.NoError(t, err)
			conn.Close()
		}()

		// Accept connection
		conn, err := listener.Accept()
		require.NoError(t, err)
		assert.NotNil(t, conn)
		conn.Close()

		<-done
	})

	t.Run("dial timeout", func(t *testing.T) {
		dialer := net.Dialer{Timeout: 10 * time.Millisecond}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := dialer.DialContext(ctx, "tcp", "192.0.2.1:1883") // TEST-NET-1, should timeout
		assert.Error(t, err)
	})

	t.Run("dial context cancel", func(t *testing.T) {
		dialer := net.Dialer{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := dialer.DialContext(ctx, "tcp", "127.0.0.1:1883")
		assert.Error(t, err)
	})
}

func TestTCPRoundTrip(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	// Server goroutine
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read packet
		packet, _, err := ReadPacket(conn, 0)
		if err != nil {
			return
		}

		// Send response
		if packet.Type() == PacketCONNECT {
			response := &ConnackPacket{ReasonCode: ReasonSuccess}
			_, _ = WritePacket(conn, response, 0)
		}
	}()

	// Client
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(context.Background(), "tcp", listener.Addr().String())
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

	<-serverDone
}

func BenchmarkTCPDial(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	defer listener.Close()

	// Accept connections
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	dialer := net.Dialer{}
	addr := listener.Addr().String()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		conn, err := dialer.DialContext(context.Background(), "tcp", addr)
		if err != nil {
			b.Fatal(err)
		}
		conn.Close()
	}
}

func BenchmarkTCPRoundTrip(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	defer listener.Close()

	// Server
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				for {
					packet, _, err := ReadPacket(c, 0)
					if err != nil {
						return
					}
					if packet.Type() == PacketPINGREQ {
						_, _ = WritePacket(c, &PingrespPacket{}, 0)
					}
				}
			}(conn)
		}
	}()

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(context.Background(), "tcp", listener.Addr().String())
	require.NoError(b, err)
	defer conn.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = WritePacket(conn, &PingreqPacket{}, 0)
		_, _, _ = ReadPacket(conn, 0)
	}
}
