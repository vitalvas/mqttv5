package mqttv5

import (
	"context"
	"net"
)

// UnixDialer connects to MQTT brokers over Unix domain sockets.
type UnixDialer struct{}

// Dial connects to the Unix socket at the given path.
// The address should be the socket file path (e.g., "/var/run/mqtt.sock").
func (d *UnixDialer) Dial(ctx context.Context, address string) (Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// NewUnixDialer creates a new Unix socket dialer.
func NewUnixDialer() *UnixDialer {
	return &UnixDialer{}
}

// UnixListener listens for MQTT connections on a Unix domain socket.
type UnixListener struct {
	listener net.Listener
	path     string
}

// NewUnixListener creates a new Unix socket listener.
// The path is the socket file path (e.g., "/var/run/mqtt.sock").
func NewUnixListener(path string) (*UnixListener, error) {
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	return &UnixListener{
		listener: listener,
		path:     path,
	}, nil
}

// Accept waits for and returns the next connection.
func (l *UnixListener) Accept() (net.Conn, error) {
	return l.listener.Accept()
}

// Close closes the listener and removes the socket file.
func (l *UnixListener) Close() error {
	return l.listener.Close()
}

// Addr returns the listener's network address.
func (l *UnixListener) Addr() net.Addr {
	return l.listener.Addr()
}

// Path returns the socket file path.
func (l *UnixListener) Path() string {
	return l.path
}
