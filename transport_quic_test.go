//go:build quic

package mqttv5

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQUICConnection(t *testing.T) {
	t.Run("listener address", func(t *testing.T) {
		cert, _ := generateTestCertificate(t)

		serverTLS := &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"mqtt"},
		}

		listener, err := NewQUICListener("127.0.0.1:0", serverTLS, nil)
		require.NoError(t, err)
		defer listener.Close()

		assert.NotNil(t, listener.Addr())
	})

	t.Run("listener requires TLS", func(t *testing.T) {
		_, err := NewQUICListener("127.0.0.1:0", nil, nil)
		assert.ErrorIs(t, err, ErrTLSRequired)
	})

	t.Run("dial context cancel", func(t *testing.T) {
		clientTLS := &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"mqtt"},
		}
		dialer := NewQUICDialer(clientTLS)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := dialer.Dial(ctx, "127.0.0.1:1234")
		assert.Error(t, err)
	})

	t.Run("dial nonexistent server", func(t *testing.T) {
		clientTLS := &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"mqtt"},
		}
		dialer := NewQUICDialer(clientTLS)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := dialer.Dial(ctx, "127.0.0.1:59999")
		assert.Error(t, err)
	})

	t.Run("dialer with nil TLS config uses default", func(t *testing.T) {
		dialer := NewQUICDialer(nil)
		assert.NotNil(t, dialer.TLSConfig)
		assert.Equal(t, uint16(tls.VersionTLS13), dialer.TLSConfig.MinVersion)
		assert.Contains(t, dialer.TLSConfig.NextProtos, "mqtt")
	})
}

func TestQUICRoundTrip(t *testing.T) {
	cert, certPool := generateTestCertificate(t)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"mqtt"},
	}

	listener, err := NewQUICListener("127.0.0.1:0", serverTLS, nil)
	require.NoError(t, err)
	defer listener.Close()

	clientDone := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		conn, acceptErr := listener.Accept(ctx)
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}

		packet, _, readErr := readPacketV5(conn, 0)
		if readErr != nil {
			conn.Close()
			serverDone <- readErr
			return
		}

		if packet.Type() == PacketCONNECT {
			response := &ConnackPacket{ReasonCode: ReasonSuccess}
			_, _ = writePacketRaw(conn, response, 0)
		}

		// Wait for client to finish before closing
		<-clientDone
		conn.Close()
		serverDone <- nil
	}()

	clientTLS := &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: true,
		NextProtos:         []string{"mqtt"},
	}
	dialer := NewQUICDialer(clientTLS)
	conn, err := dialer.Dial(context.Background(), listener.Addr().String())
	require.NoError(t, err)

	// Test connection methods
	assert.NotNil(t, conn.LocalAddr())
	assert.NotNil(t, conn.RemoteAddr())

	// Test deadlines
	err = conn.SetDeadline(time.Now().Add(5 * time.Second))
	assert.NoError(t, err)
	err = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	assert.NoError(t, err)
	err = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	assert.NoError(t, err)

	// Send CONNECT
	connectPacket := &ConnectPacket{
		ClientID:   "test-client",
		CleanStart: true,
		KeepAlive:  60,
	}
	_, err = writePacketRaw(conn, connectPacket, 0)
	require.NoError(t, err)

	// Read CONNACK
	packet, _, err := readPacketV5(conn, 0)
	require.NoError(t, err)
	assert.Equal(t, PacketCONNACK, packet.Type())

	connack, ok := packet.(*ConnackPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonSuccess, connack.ReasonCode)

	// Signal client is done, then close
	close(clientDone)
	conn.Close()

	// Wait for server
	select {
	case serverErr := <-serverDone:
		require.NoError(t, serverErr)
	case <-time.After(10 * time.Second):
		t.Fatal("server timed out")
	}
}

func TestQUICNetListenerAdapter(t *testing.T) {
	cert, certPool := generateTestCertificate(t)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"mqtt"},
	}

	quicListener, err := NewQUICListener("127.0.0.1:0", serverTLS, nil)
	require.NoError(t, err)

	netListener := quicListener.NetListener()
	defer netListener.Close()

	clientDone := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		conn, acceptErr := netListener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}

		packet, _, readErr := readPacketV5(conn, 0)
		if readErr != nil {
			conn.Close()
			serverDone <- readErr
			return
		}

		if packet.Type() == PacketCONNECT {
			response := &ConnackPacket{ReasonCode: ReasonSuccess}
			_, _ = writePacketRaw(conn, response, 0)
		}

		// Wait for client to finish before closing
		<-clientDone
		conn.Close()
		serverDone <- nil
	}()

	clientTLS := &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: true,
		NextProtos:         []string{"mqtt"},
	}
	dialer := NewQUICDialer(clientTLS)
	conn, err := dialer.Dial(context.Background(), netListener.Addr().String())
	require.NoError(t, err)

	// Verify it's a net.Conn
	assert.NotNil(t, conn)

	// Send CONNECT
	connectPacket := &ConnectPacket{
		ClientID:   "test-client",
		CleanStart: true,
		KeepAlive:  60,
	}
	_, err = writePacketRaw(conn, connectPacket, 0)
	require.NoError(t, err)

	// Read CONNACK
	packet, _, err := readPacketV5(conn, 0)
	require.NoError(t, err)
	assert.Equal(t, PacketCONNACK, packet.Type())

	// Signal client is done, then close
	close(clientDone)
	conn.Close()

	// Wait for server
	select {
	case serverErr := <-serverDone:
		require.NoError(t, serverErr)
	case <-time.After(10 * time.Second):
		t.Fatal("server timed out")
	}
}

func TestQUICListenerTLSVersionEnforcement(t *testing.T) {
	cert, _ := generateTestCertificate(t)

	t.Run("upgrades TLS version to 1.3", func(t *testing.T) {
		serverTLS := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}

		listener, err := NewQUICListener("127.0.0.1:0", serverTLS, nil)
		require.NoError(t, err)
		defer listener.Close()
	})

	t.Run("adds ALPN if missing", func(t *testing.T) {
		serverTLS := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
		}

		listener, err := NewQUICListener("127.0.0.1:0", serverTLS, nil)
		require.NoError(t, err)
		defer listener.Close()
	})
}

func TestQUICDialerEmptyALPN(t *testing.T) {
	cert, certPool := generateTestCertificate(t)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"mqtt"},
	}

	listener, err := NewQUICListener("127.0.0.1:0", serverTLS, nil)
	require.NoError(t, err)
	defer listener.Close()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := listener.Accept(ctx)
		if err == nil {
			// Keep connection open briefly then close
			time.Sleep(5 * time.Millisecond)
			conn.Close()
		}
	}()

	// Test dialer with empty NextProtos - should add "mqtt" automatically
	clientTLS := &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: true,
		NextProtos:         []string{}, // Empty ALPN
	}
	dialer := &QUICDialer{TLSConfig: clientTLS}

	conn, err := dialer.Dial(context.Background(), listener.Addr().String())
	require.NoError(t, err, "Dial should succeed with empty ALPN - mqtt should be added automatically")
	assert.NotNil(t, conn)
	if conn != nil {
		conn.Close()
	}
}

func TestQUICListenerAcceptContextCancel(t *testing.T) {
	cert, _ := generateTestCertificate(t)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"mqtt"},
	}

	listener, err := NewQUICListener("127.0.0.1:0", serverTLS, nil)
	require.NoError(t, err)
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = listener.Accept(ctx)
	assert.Error(t, err)
}

func TestQUICListenerInvalidAddress(t *testing.T) {
	cert, _ := generateTestCertificate(t)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"mqtt"},
	}

	_, err := NewQUICListener("invalid-address-not-ip:port", serverTLS, nil)
	assert.Error(t, err)
}
