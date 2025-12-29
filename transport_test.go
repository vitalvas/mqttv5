package mqttv5

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTCPListenerAccept(t *testing.T) {
	listener, err := NewTCPListener("127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	addr := listener.Addr()
	assert.NotNil(t, addr)

	// Connect in goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := net.Dial("tcp", addr.String())
		require.NoError(t, err)
		conn.Close()
	}()

	// Accept connection
	conn, err := listener.Accept()
	require.NoError(t, err)
	assert.NotNil(t, conn)
	conn.Close()

	<-done
}

func TestTCPDialer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	// Accept in goroutine
	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	dialer := &TCPDialer{Timeout: 5 * time.Second}
	conn, err := dialer.Dial(context.Background(), listener.Addr().String())
	require.NoError(t, err)
	assert.NotNil(t, conn)
	conn.Close()
}

func TestTCPDialerTimeout(t *testing.T) {
	// Use an address that won't respond
	dialer := &TCPDialer{Timeout: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := dialer.Dial(ctx, "192.0.2.1:1883") // TEST-NET-1, should timeout
	assert.Error(t, err)
}

func TestTCPDialerContextCancel(t *testing.T) {
	dialer := &TCPDialer{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := dialer.Dial(ctx, "127.0.0.1:1883")
	assert.Error(t, err)
}

func generateTestCert() (tls.Certificate, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	return tls.X509KeyPair(certPEM, keyPEM)
}

func TestTLSListenerAccept(t *testing.T) {
	cert, err := generateTestCert()
	require.NoError(t, err)

	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	listener, err := NewTLSListener("127.0.0.1:0", config)
	require.NoError(t, err)
	defer listener.Close()

	addr := listener.Addr()
	assert.NotNil(t, addr)

	// Connect in goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := tls.Dial("tcp", addr.String(), &tls.Config{InsecureSkipVerify: true})
		if err == nil {
			conn.Close()
		}
	}()

	// Accept connection
	conn, err := listener.Accept()
	require.NoError(t, err)
	assert.NotNil(t, conn)
	conn.Close()

	<-done
}

func TestTLSDialer(t *testing.T) {
	cert, err := generateTestCert()
	require.NoError(t, err)

	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	require.NoError(t, err)
	defer listener.Close()

	// Accept in goroutine
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		// Keep connection open until client is done
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
		conn.Close()
	}()

	dialer := &TLSDialer{
		Config:  &tls.Config{InsecureSkipVerify: true},
		Timeout: 5 * time.Second,
	}
	conn, err := dialer.Dial(context.Background(), listener.Addr().String())
	require.NoError(t, err)
	assert.NotNil(t, conn)
	conn.Close()

	<-serverDone
}

func TestTCPRoundTrip(t *testing.T) {
	listener, err := NewTCPListener("127.0.0.1:0")
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
	dialer := &TCPDialer{Timeout: 5 * time.Second}
	conn, err := dialer.Dial(context.Background(), listener.Addr().String())
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

func BenchmarkTCPDialer(b *testing.B) {
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

	dialer := &TCPDialer{}
	addr := listener.Addr().String()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		conn, err := dialer.Dial(context.Background(), addr)
		if err != nil {
			b.Fatal(err)
		}
		conn.Close()
	}
}

func BenchmarkTCPRoundTrip(b *testing.B) {
	listener, err := NewTCPListener("127.0.0.1:0")
	require.NoError(b, err)
	defer listener.Close()

	// Server
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

	dialer := &TCPDialer{}
	conn, err := dialer.Dial(context.Background(), listener.Addr().String())
	require.NoError(b, err)
	defer conn.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = WritePacket(conn, &PingreqPacket{}, 0)
		_, _, _ = ReadPacket(conn, 0)
	}
}
