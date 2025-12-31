package mqttv5

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// ErrTLSRequired is returned when TLS configuration is required but not provided.
var ErrTLSRequired = errors.New("TLS configuration is required for QUIC")

// QUICConn wraps a QUIC stream to implement net.Conn.
type QUICConn struct {
	conn   *quic.Conn
	stream *quic.Stream
	mu     sync.Mutex
}

// Read reads data from the QUIC stream.
func (c *QUICConn) Read(b []byte) (int, error) {
	return c.stream.Read(b)
}

// Write writes data to the QUIC stream.
func (c *QUICConn) Write(b []byte) (int, error) {
	return c.stream.Write(b)
}

// Close closes the QUIC stream and connection.
func (c *QUICConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.stream.Close(); err != nil {
		return err
	}
	return c.conn.CloseWithError(0, "")
}

// LocalAddr returns the local network address.
func (c *QUICConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *QUICConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines.
func (c *QUICConn) SetDeadline(t time.Time) error {
	if err := c.stream.SetReadDeadline(t); err != nil {
		return err
	}
	return c.stream.SetWriteDeadline(t)
}

// SetReadDeadline sets the read deadline.
func (c *QUICConn) SetReadDeadline(t time.Time) error {
	return c.stream.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline.
func (c *QUICConn) SetWriteDeadline(t time.Time) error {
	return c.stream.SetWriteDeadline(t)
}

// QUICDialer connects to MQTT brokers over QUIC.
type QUICDialer struct {
	// TLSConfig is the TLS configuration for the QUIC connection.
	// QUIC requires TLS 1.3, so this must be configured.
	TLSConfig *tls.Config

	// QUICConfig is the QUIC configuration.
	QUICConfig *quic.Config
}

// Dial connects to the QUIC address.
// The address should be in the format "host:port".
func (d *QUICDialer) Dial(ctx context.Context, address string) (Conn, error) {
	tlsConfig := d.TLSConfig
	if tlsConfig == nil {
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS13,
			NextProtos: []string{"mqtt"},
		}
	}

	// Ensure ALPN is set for MQTT
	if len(tlsConfig.NextProtos) == 0 {
		tlsConfig = tlsConfig.Clone()
		tlsConfig.NextProtos = []string{"mqtt"}
	}

	conn, err := quic.DialAddr(ctx, address, tlsConfig, d.QUICConfig)
	if err != nil {
		return nil, err
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		conn.CloseWithError(0, "failed to open stream")
		return nil, err
	}

	return &QUICConn{
		conn:   conn,
		stream: stream,
	}, nil
}

// NewQUICDialer creates a new QUIC dialer with default configuration.
func NewQUICDialer(tlsConfig *tls.Config) *QUICDialer {
	if tlsConfig == nil {
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS13,
			NextProtos: []string{"mqtt"},
		}
	}
	return &QUICDialer{
		TLSConfig: tlsConfig,
	}
}

// QUICListener listens for MQTT connections over QUIC.
type QUICListener struct {
	listener *quic.Listener
}

// NewQUICListener creates a new QUIC listener.
// TLS configuration is required for QUIC (TLS 1.3).
func NewQUICListener(addr string, tlsConfig *tls.Config, quicConfig *quic.Config) (*QUICListener, error) {
	if tlsConfig == nil {
		return nil, ErrTLSRequired
	}

	// Ensure TLS 1.3 minimum and ALPN
	if tlsConfig.MinVersion < tls.VersionTLS13 {
		tlsConfig = tlsConfig.Clone()
		tlsConfig.MinVersion = tls.VersionTLS13
	}
	if len(tlsConfig.NextProtos) == 0 {
		tlsConfig = tlsConfig.Clone()
		tlsConfig.NextProtos = []string{"mqtt"}
	}

	listener, err := quic.ListenAddr(addr, tlsConfig, quicConfig)
	if err != nil {
		return nil, err
	}

	return &QUICListener{
		listener: listener,
	}, nil
}

// Accept waits for and returns the next QUIC connection.
// It accepts the QUIC connection and opens a bidirectional stream.
func (l *QUICListener) Accept(ctx context.Context) (Conn, error) {
	conn, err := l.listener.Accept(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		conn.CloseWithError(0, "failed to accept stream")
		return nil, err
	}

	return &QUICConn{
		conn:   conn,
		stream: stream,
	}, nil
}

// Close closes the QUIC listener.
func (l *QUICListener) Close() error {
	return l.listener.Close()
}

// Addr returns the listener's network address.
func (l *QUICListener) Addr() net.Addr {
	return l.listener.Addr()
}

// NetListener returns a net.Listener adapter for use with the MQTT server.
// The adapter uses a background context for Accept calls.
func (l *QUICListener) NetListener() net.Listener {
	ctx, cancel := context.WithCancel(context.Background())
	return &quicNetListener{
		quicListener: l,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// quicNetListener adapts QUICListener to the net.Listener interface.
type quicNetListener struct {
	quicListener *QUICListener
	ctx          context.Context
	cancel       context.CancelFunc
}

// Accept waits for and returns the next connection.
func (l *quicNetListener) Accept() (net.Conn, error) {
	conn, err := l.quicListener.Accept(l.ctx)
	if err != nil {
		return nil, err
	}
	return conn.(*QUICConn), nil
}

// Close closes the listener.
func (l *quicNetListener) Close() error {
	l.cancel()
	return l.quicListener.Close()
}

// Addr returns the listener's network address.
func (l *quicNetListener) Addr() net.Addr {
	return l.quicListener.Addr()
}
