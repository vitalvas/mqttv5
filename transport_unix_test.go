package mqttv5

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getShortSocketPath(t testing.TB) string {
	t.Helper()
	return fmt.Sprintf("/tmp/mqtt_test_%d.sock", time.Now().UnixNano())
}

func TestUnixSocketConnection(t *testing.T) {
	t.Run("accept and dial", func(t *testing.T) {
		socketPath := getShortSocketPath(t)
		defer os.Remove(socketPath)

		listener, err := NewUnixListener(socketPath)
		require.NoError(t, err)
		defer listener.Close()

		done := make(chan struct{})
		go func() {
			defer close(done)
			dialer := NewUnixDialer()
			conn, err := dialer.Dial(context.Background(), socketPath)
			require.NoError(t, err)
			conn.Close()
		}()

		conn, err := listener.Accept()
		require.NoError(t, err)
		assert.NotNil(t, conn)
		conn.Close()

		<-done
	})

	t.Run("listener address", func(t *testing.T) {
		socketPath := getShortSocketPath(t)
		defer os.Remove(socketPath)

		listener, err := NewUnixListener(socketPath)
		require.NoError(t, err)
		defer listener.Close()

		assert.NotNil(t, listener.Addr())
		assert.Equal(t, "unix", listener.Addr().Network())
		assert.Equal(t, socketPath, listener.Path())
	})

	t.Run("dial context cancel", func(t *testing.T) {
		dialer := NewUnixDialer()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := dialer.Dial(ctx, "/nonexistent/socket.sock")
		assert.Error(t, err)
	})

	t.Run("dial nonexistent socket", func(t *testing.T) {
		dialer := NewUnixDialer()
		_, err := dialer.Dial(context.Background(), "/nonexistent/socket.sock")
		assert.Error(t, err)
	})

	t.Run("listener on existing file", func(t *testing.T) {
		socketPath := getShortSocketPath(t)
		defer os.Remove(socketPath)

		f, err := os.Create(socketPath)
		require.NoError(t, err)
		f.Close()

		_, err = NewUnixListener(socketPath)
		assert.Error(t, err)
	})
}

func TestUnixSocketRoundTrip(t *testing.T) {
	socketPath := getShortSocketPath(t)
	defer os.Remove(socketPath)

	listener, err := NewUnixListener(socketPath)
	require.NoError(t, err)
	defer listener.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		packet, _, err := ReadPacket(conn, 0)
		if err != nil {
			return
		}

		if packet.Type() == PacketCONNECT {
			response := &ConnackPacket{ReasonCode: ReasonSuccess}
			_, _ = WritePacket(conn, response, 0)
		}
	}()

	dialer := NewUnixDialer()
	conn, err := dialer.Dial(context.Background(), socketPath)
	require.NoError(t, err)
	defer conn.Close()

	connectPacket := &ConnectPacket{
		ClientID:   "test-client",
		CleanStart: true,
		KeepAlive:  60,
	}
	_, err = WritePacket(conn, connectPacket, 0)
	require.NoError(t, err)

	packet, _, err := ReadPacket(conn, 0)
	require.NoError(t, err)
	assert.Equal(t, PacketCONNACK, packet.Type())

	connack, ok := packet.(*ConnackPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonSuccess, connack.ReasonCode)

	<-serverDone
}

func TestUnixListenerNetListenerInterface(t *testing.T) {
	socketPath := getShortSocketPath(t)
	defer os.Remove(socketPath)

	listener, err := NewUnixListener(socketPath)
	require.NoError(t, err)
	defer listener.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		dialer := NewUnixDialer()
		conn, _ := dialer.Dial(context.Background(), socketPath)
		if conn != nil {
			conn.Close()
		}
	}()

	conn, err := listener.Accept()
	require.NoError(t, err)
	conn.Close()

	<-done
}

func BenchmarkUnixSocketDial(b *testing.B) {
	socketPath := getShortSocketPath(b)
	defer os.Remove(socketPath)

	listener, err := NewUnixListener(socketPath)
	require.NoError(b, err)
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	dialer := NewUnixDialer()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		conn, err := dialer.Dial(context.Background(), socketPath)
		if err != nil {
			b.Fatal(err)
		}
		conn.Close()
	}
}

func BenchmarkUnixSocketRoundTrip(b *testing.B) {
	socketPath := getShortSocketPath(b)
	defer os.Remove(socketPath)

	listener, err := NewUnixListener(socketPath)
	require.NoError(b, err)
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c Conn) {
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

	dialer := NewUnixDialer()
	conn, err := dialer.Dial(context.Background(), socketPath)
	require.NoError(b, err)
	defer conn.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = WritePacket(conn, &PingreqPacket{}, 0)
		_, _, _ = ReadPacket(conn, 0)
	}
}
