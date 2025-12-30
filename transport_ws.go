package mqttv5

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// WebSocketSubprotocol is the MQTT WebSocket subprotocol.
	WebSocketSubprotocol = "mqtt"
)

// WSConn wraps a WebSocket connection to implement net.Conn.
type WSConn struct {
	conn   *websocket.Conn
	reader *wsReader
}

// wsReader handles reading from WebSocket with message framing.
type wsReader struct {
	conn    *websocket.Conn
	buf     []byte
	readPos int
}

func (r *wsReader) Read(p []byte) (int, error) {
	// If we have buffered data, return it
	if r.readPos < len(r.buf) {
		n := copy(p, r.buf[r.readPos:])
		r.readPos += n
		return n, nil
	}

	// Read next message
	messageType, data, err := r.conn.ReadMessage()
	if err != nil {
		return 0, err
	}

	// MQTT over WebSocket uses binary messages
	if messageType != websocket.BinaryMessage {
		return 0, ErrProtocolViolation
	}

	r.buf = data
	r.readPos = 0

	n := copy(p, r.buf)
	r.readPos = n
	return n, nil
}

// newWSConn creates a new WebSocket connection wrapper.
func newWSConn(conn *websocket.Conn) *WSConn {
	return &WSConn{
		conn:   conn,
		reader: &wsReader{conn: conn},
	}
}

// Read reads data from the connection.
func (c *WSConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

// Write writes data to the connection as a binary message.
func (c *WSConn) Write(b []byte) (int, error) {
	err := c.conn.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

// Close closes the connection.
func (c *WSConn) Close() error {
	return c.conn.Close()
}

// LocalAddr returns the local network address.
func (c *WSConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *WSConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines.
func (c *WSConn) SetDeadline(t time.Time) error {
	if err := c.conn.SetReadDeadline(t); err != nil {
		return err
	}
	return c.conn.SetWriteDeadline(t)
}

// SetReadDeadline sets the read deadline.
func (c *WSConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline.
func (c *WSConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// WSDialer connects to MQTT brokers over WebSocket.
type WSDialer struct {
	// Dialer is the underlying WebSocket dialer.
	Dialer *websocket.Dialer

	// Header is the HTTP header to send with the handshake.
	Header http.Header
}

// Dial connects to the WebSocket address.
func (d *WSDialer) Dial(ctx context.Context, address string) (Conn, error) {
	dialer := d.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}

	header := d.Header
	if header == nil {
		header = http.Header{}
	}

	conn, _, err := dialer.DialContext(ctx, address, header)
	if err != nil {
		return nil, err
	}

	return newWSConn(conn), nil
}

// NewWSDialer creates a new WebSocket dialer with MQTT subprotocol.
func NewWSDialer() *WSDialer {
	return &WSDialer{
		Dialer: &websocket.Dialer{
			Subprotocols:    []string{WebSocketSubprotocol},
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
		},
	}
}

// WSHandler is an HTTP handler that upgrades connections to WebSocket for MQTT.
type WSHandler struct {
	// Upgrader is the WebSocket upgrader.
	Upgrader websocket.Upgrader

	// OnConnect is called when a new WebSocket connection is established.
	// The handler should process MQTT packets on the connection.
	OnConnect func(conn Conn)

	// AllowedOrigins is a list of allowed origins for WebSocket connections.
	// If nil or empty, origin checking is strict (Origin must match Host header).
	// Use "*" to allow all origins (not recommended for production).
	AllowedOrigins []string
}

// NewWSHandler creates a new WebSocket handler for MQTT.
// By default, origin checking is strict (Origin must match Host header).
// Use AllowedOrigins to configure allowed origins.
func NewWSHandler(onConnect func(conn Conn)) *WSHandler {
	h := &WSHandler{
		OnConnect: onConnect,
	}
	h.Upgrader = websocket.Upgrader{
		Subprotocols:    []string{WebSocketSubprotocol},
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     h.checkOrigin,
	}
	return h
}

// checkOrigin validates the Origin header for WebSocket connections.
func (h *WSHandler) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")

	// If no Origin header, allow (non-browser clients)
	if origin == "" {
		return true
	}

	// Check allowed origins list
	if len(h.AllowedOrigins) > 0 {
		for _, allowed := range h.AllowedOrigins {
			if allowed == "*" {
				return true
			}
			if origin == allowed {
				return true
			}
		}
		return false
	}

	// Default: strict check - Origin must match Host header
	// Parse origin to extract host
	host := r.Host
	if host == "" {
		return false
	}

	// Extract host from origin URL
	// Origin format: "scheme://host[:port]"
	originHost := extractHost(origin)
	if originHost == "" {
		return false
	}

	return originHost == host
}

// extractHost extracts the host:port from a URL string.
func extractHost(urlStr string) string {
	// Skip scheme
	var start int
	switch {
	case len(urlStr) > 8 && urlStr[:8] == "https://":
		start = 8
	case len(urlStr) > 7 && urlStr[:7] == "http://":
		start = 7
	case len(urlStr) > 6 && urlStr[:6] == "wss://":
		start = 6
	case len(urlStr) > 5 && urlStr[:5] == "ws://":
		start = 5
	default:
		return ""
	}

	// Find end of host (path starts at /)
	end := len(urlStr)
	for i := start; i < len(urlStr); i++ {
		if urlStr[i] == '/' {
			end = i
			break
		}
	}

	return urlStr[start:end]
}

// ServeHTTP implements http.Handler.
func (h *WSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	wsConn := newWSConn(conn)

	if h.OnConnect != nil {
		h.OnConnect(wsConn)
	}
}
