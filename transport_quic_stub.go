//go:build !quic

package mqttv5

import (
	"context"
	"crypto/tls"
	"net"
	"time"
)

// QUICConn is a stub for the QUIC connection type. It is non-functional
// when the binary is built without the "quic" build tag.
type QUICConn struct{}

// Read returns ErrQUICNotEnabled.
func (*QUICConn) Read([]byte) (int, error) { return 0, ErrQUICNotEnabled }

// Write returns ErrQUICNotEnabled.
func (*QUICConn) Write([]byte) (int, error) { return 0, ErrQUICNotEnabled }

// Close returns ErrQUICNotEnabled.
func (*QUICConn) Close() error { return ErrQUICNotEnabled }

// LocalAddr returns nil.
func (*QUICConn) LocalAddr() net.Addr { return nil }

// RemoteAddr returns nil.
func (*QUICConn) RemoteAddr() net.Addr { return nil }

// SetDeadline returns ErrQUICNotEnabled.
func (*QUICConn) SetDeadline(time.Time) error { return ErrQUICNotEnabled }

// SetReadDeadline returns ErrQUICNotEnabled.
func (*QUICConn) SetReadDeadline(time.Time) error { return ErrQUICNotEnabled }

// SetWriteDeadline returns ErrQUICNotEnabled.
func (*QUICConn) SetWriteDeadline(time.Time) error { return ErrQUICNotEnabled }

// QUICDialer is a stub dialer. Dial always returns ErrQUICNotEnabled.
type QUICDialer struct {
	TLSConfig *tls.Config
}

// Dial always returns ErrQUICNotEnabled.
func (*QUICDialer) Dial(context.Context, string) (Conn, error) {
	return nil, ErrQUICNotEnabled
}

// NewQUICDialer returns a stub QUIC dialer that always fails with
// ErrQUICNotEnabled. Rebuild with -tags quic to enable QUIC support.
func NewQUICDialer(tlsConfig *tls.Config) *QUICDialer {
	return &QUICDialer{TLSConfig: tlsConfig}
}

// QUICListener is a stub listener type.
type QUICListener struct{}

// NewQUICListener always returns ErrQUICNotEnabled. Rebuild with -tags quic
// to enable QUIC support.
func NewQUICListener(string, *tls.Config, any) (*QUICListener, error) {
	return nil, ErrQUICNotEnabled
}

// Accept returns ErrQUICNotEnabled.
func (*QUICListener) Accept(context.Context) (Conn, error) { return nil, ErrQUICNotEnabled }

// Close returns ErrQUICNotEnabled.
func (*QUICListener) Close() error { return ErrQUICNotEnabled }

// Addr returns nil.
func (*QUICListener) Addr() net.Addr { return nil }

// NetListener returns a stub net.Listener whose Accept always fails.
func (l *QUICListener) NetListener() net.Listener { return &QUICNetListener{} }

// QUICNetListener is a stub net.Listener.
type QUICNetListener struct{}

// Accept returns ErrQUICNotEnabled.
func (*QUICNetListener) Accept() (net.Conn, error) { return nil, ErrQUICNotEnabled }

// Close returns ErrQUICNotEnabled.
func (*QUICNetListener) Close() error { return ErrQUICNotEnabled }

// Addr returns nil.
func (*QUICNetListener) Addr() net.Addr { return nil }
