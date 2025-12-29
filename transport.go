package mqttv5

import (
	"context"
	"crypto/tls"
	"net"
	"time"
)

// Conn represents a network connection for MQTT communication.
// It extends net.Conn with MQTT-specific functionality.
type Conn interface {
	net.Conn
}

// Listener accepts incoming MQTT connections.
type Listener interface {
	// Accept waits for and returns the next connection.
	Accept() (Conn, error)

	// Close closes the listener.
	Close() error

	// Addr returns the listener's network address.
	Addr() net.Addr
}

// Dialer establishes MQTT connections.
type Dialer interface {
	// Dial connects to the address with the given context.
	Dial(ctx context.Context, address string) (Conn, error)
}

// TCPListener wraps net.Listener for TCP connections.
type TCPListener struct {
	listener net.Listener
}

// NewTCPListener creates a new TCP listener on the given address.
func NewTCPListener(address string) (*TCPListener, error) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return &TCPListener{listener: l}, nil
}

// Accept waits for and returns the next connection.
func (l *TCPListener) Accept() (Conn, error) {
	conn, err := l.listener.Accept()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Close closes the listener.
func (l *TCPListener) Close() error {
	return l.listener.Close()
}

// Addr returns the listener's network address.
func (l *TCPListener) Addr() net.Addr {
	return l.listener.Addr()
}

// TCPDialer connects to MQTT brokers over TCP.
type TCPDialer struct {
	// Timeout is the maximum time to wait for a connection.
	// Zero means no timeout.
	Timeout time.Duration
}

// Dial connects to the address.
func (d *TCPDialer) Dial(ctx context.Context, address string) (Conn, error) {
	var dialer net.Dialer
	if d.Timeout > 0 {
		dialer.Timeout = d.Timeout
	}
	return dialer.DialContext(ctx, "tcp", address)
}

// TLSListener wraps net.Listener for TLS connections.
type TLSListener struct {
	listener net.Listener
}

// NewTLSListener creates a new TLS listener on the given address.
func NewTLSListener(address string, config *tls.Config) (*TLSListener, error) {
	l, err := tls.Listen("tcp", address, config)
	if err != nil {
		return nil, err
	}
	return &TLSListener{listener: l}, nil
}

// Accept waits for and returns the next connection.
func (l *TLSListener) Accept() (Conn, error) {
	conn, err := l.listener.Accept()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Close closes the listener.
func (l *TLSListener) Close() error {
	return l.listener.Close()
}

// Addr returns the listener's network address.
func (l *TLSListener) Addr() net.Addr {
	return l.listener.Addr()
}

// TLSDialer connects to MQTT brokers over TLS.
type TLSDialer struct {
	// Config is the TLS configuration.
	Config *tls.Config

	// Timeout is the maximum time to wait for a connection.
	// Zero means no timeout.
	Timeout time.Duration
}

// Dial connects to the address.
func (d *TLSDialer) Dial(ctx context.Context, address string) (Conn, error) {
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{
			Timeout: d.Timeout,
		},
		Config: d.Config,
	}
	return dialer.DialContext(ctx, "tcp", address)
}
