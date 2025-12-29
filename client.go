package mqttv5

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// MessageHandler handles incoming MQTT messages.
type MessageHandler func(msg *Message)

// Client is an MQTT v5 client.
type Client struct {
	conn    net.Conn
	options *clientOptions
	addr    string

	// Session state
	session     *MemorySession
	packetIDMgr *PacketIDManager
	qos1Tracker *QoS1Tracker
	qos2Tracker *QoS2Tracker

	// Subscriptions with handlers
	subscriptions   map[string]MessageHandler
	subscriptionsMu sync.RWMutex

	// Connection state
	connected    atomic.Bool
	reconnecting atomic.Bool
	closed       atomic.Bool

	// Lifecycle control
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	readDone   chan struct{}
	writeMu    sync.Mutex
	lastPacket time.Time
}

// Dial connects to an MQTT broker and returns a client.
// The address should be in the format "scheme://host:port" where scheme is
// "tcp", "ssl", "tls", "ws", or "wss".
func Dial(addr string, opts ...Option) (*Client, error) {
	return DialContext(context.Background(), addr, opts...)
}

// DialContext connects to an MQTT broker with a context.
func DialContext(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	options := applyOptions(opts...)

	c := &Client{
		addr:          addr,
		options:       options,
		subscriptions: make(map[string]MessageHandler),
		packetIDMgr:   NewPacketIDManager(),
		qos1Tracker:   NewQoS1Tracker(30*time.Second, 3),
		qos2Tracker:   NewQoS2Tracker(30*time.Second, 3),
		done:          make(chan struct{}),
		readDone:      make(chan struct{}),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	// Generate client ID if not provided
	if options.clientID == "" {
		options.clientID = generateClientID()
	}
	c.session = NewMemorySession(options.clientID)

	// Connect with timeout
	connectCtx, connectCancel := context.WithTimeout(ctx, options.connectTimeout)
	defer connectCancel()

	if err := c.connect(connectCtx); err != nil {
		return nil, err
	}

	return c, nil
}

// connect establishes the TCP/TLS connection and performs MQTT handshake.
func (c *Client) connect(ctx context.Context) error {
	conn, err := c.dial(ctx)
	if err != nil {
		return err
	}
	c.conn = conn

	// Build CONNECT packet
	connectPkt := &ConnectPacket{
		ClientID:   c.options.clientID,
		CleanStart: c.options.cleanStart,
		KeepAlive:  c.options.keepAlive,
	}

	// Set credentials if provided
	connectPkt.Username = c.options.username
	connectPkt.Password = c.options.password

	// Set Will message if configured
	if c.options.willTopic != "" {
		connectPkt.WillFlag = true
		connectPkt.WillTopic = c.options.willTopic
		connectPkt.WillPayload = c.options.willPayload
		connectPkt.WillRetain = c.options.willRetain
		connectPkt.WillQoS = c.options.willQoS
		if c.options.willProps != nil {
			connectPkt.WillProps = *c.options.willProps
		}
	}

	// Set CONNECT properties
	if c.options.sessionExpiryInterval > 0 {
		connectPkt.Props.Set(PropSessionExpiryInterval, c.options.sessionExpiryInterval)
	}
	if c.options.receiveMaximum > 0 && c.options.receiveMaximum < 65535 {
		connectPkt.Props.Set(PropReceiveMaximum, c.options.receiveMaximum)
	}
	if c.options.maxPacketSize > 0 {
		connectPkt.Props.Set(PropMaximumPacketSize, c.options.maxPacketSize)
	}
	if c.options.topicAliasMaximum > 0 {
		connectPkt.Props.Set(PropTopicAliasMaximum, c.options.topicAliasMaximum)
	}

	// Send CONNECT
	if err := c.writePacket(connectPkt); err != nil {
		c.conn.Close()
		return fmt.Errorf("failed to send CONNECT: %w", err)
	}

	// Read CONNACK with timeout
	c.conn.SetReadDeadline(time.Now().Add(c.options.connectTimeout))
	pkt, _, err := ReadPacket(c.conn, c.options.maxPacketSize)
	c.conn.SetReadDeadline(time.Time{})

	if err != nil {
		c.conn.Close()
		return fmt.Errorf("failed to read CONNACK: %w", err)
	}

	connack, ok := pkt.(*ConnackPacket)
	if !ok {
		c.conn.Close()
		return fmt.Errorf("expected CONNACK, got %T", pkt)
	}

	// Check reason code
	if connack.ReasonCode != ReasonSuccess {
		c.conn.Close()
		return NewConnectError(connack.ReasonCode, connack.Properties())
	}

	c.connected.Store(true)
	c.lastPacket = time.Now()

	// Start background goroutines
	go c.readLoop()
	go c.keepAliveLoop()

	// Emit connected event
	c.emit(NewConnectedEvent(connack.SessionPresent, connack.Properties()))

	return nil
}

// dial creates the network connection.
func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	u, err := url.Parse(c.addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address: %w", err)
	}

	host := u.Host
	if u.Port() == "" {
		switch u.Scheme {
		case "tcp", "mqtt":
			host = net.JoinHostPort(u.Hostname(), "1883")
		case "ssl", "tls", "mqtts":
			host = net.JoinHostPort(u.Hostname(), "8883")
		case "ws":
			host = net.JoinHostPort(u.Hostname(), "80")
		case "wss":
			host = net.JoinHostPort(u.Hostname(), "443")
		}
	}

	var conn net.Conn
	dialer := &net.Dialer{}

	switch u.Scheme {
	case "tcp", "mqtt":
		conn, err = dialer.DialContext(ctx, "tcp", host)
	case "ssl", "tls", "mqtts":
		tlsConfig := c.options.tlsConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		tlsDialer := &tls.Dialer{NetDialer: dialer, Config: tlsConfig}
		conn, err = tlsDialer.DialContext(ctx, "tcp", host)
	case "ws", "wss":
		wsDialer := NewWSDialer()
		if c.options.tlsConfig != nil && wsDialer.Dialer != nil {
			wsDialer.Dialer.TLSClientConfig = c.options.tlsConfig
		}
		var wsConn Conn
		wsConn, err = wsDialer.Dial(ctx, c.addr)
		if wsConn != nil {
			conn = wsConn.(net.Conn)
		}
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	if err != nil {
		return nil, fmt.Errorf("dial failed: %w", err)
	}

	return conn, nil
}

// Close disconnects from the broker and releases resources.
func (c *Client) Close() error {
	return c.CloseWithCode(ReasonSuccess)
}

// CloseWithCode disconnects with a specific reason code.
func (c *Client) CloseWithCode(code ReasonCode) error {
	if c.closed.Swap(true) {
		return nil // Already closed
	}

	c.cancel()

	if c.connected.Load() {
		// Send DISCONNECT packet
		disconnectPkt := &DisconnectPacket{
			ReasonCode: code,
		}
		c.writePacket(disconnectPkt)
		c.connected.Store(false)
	}

	if c.conn != nil {
		c.conn.Close()
	}

	// Wait for readLoop to finish
	select {
	case <-c.readDone:
	case <-time.After(time.Second):
	}

	close(c.done)

	c.emit(NewDisconnectError(code, nil, false))

	return nil
}

// IsConnected returns true if the client is connected.
func (c *Client) IsConnected() bool {
	return c.connected.Load() && !c.closed.Load()
}

// ClientID returns the client identifier.
func (c *Client) ClientID() string {
	return c.options.clientID
}

// Publish sends a message to the broker.
func (c *Client) Publish(topic string, payload []byte, qos byte, retain bool) error {
	return c.PublishMessage(&Message{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
		Retain:  retain,
	})
}

// PublishMessage sends a message with full control over all fields.
func (c *Client) PublishMessage(msg *Message) error {
	if c.closed.Load() {
		return ErrClientClosed
	}
	if !c.connected.Load() {
		return ErrNotConnected
	}

	if msg.Topic == "" {
		return ErrInvalidTopic
	}

	pkt := &PublishPacket{}
	pkt.FromMessage(msg)

	// Assign packet ID for QoS > 0
	if msg.QoS > 0 {
		packetID, err := c.packetIDMgr.Allocate()
		if err != nil {
			return err
		}
		pkt.PacketID = packetID

		// Track for acknowledgment
		switch msg.QoS {
		case 1:
			c.qos1Tracker.Track(packetID, msg)
		case 2:
			c.qos2Tracker.TrackSend(packetID, msg)
		}
	}

	if err := c.writePacket(pkt); err != nil {
		if msg.QoS > 0 {
			_ = c.packetIDMgr.Release(pkt.PacketID)
		}
		return err
	}

	return nil
}

// Subscribe subscribes to a topic filter with a message handler.
func (c *Client) Subscribe(filter string, qos byte, handler MessageHandler) error {
	return c.SubscribeMultiple(map[string]byte{filter: qos}, handler)
}

// SubscribeMultiple subscribes to multiple topic filters with a single handler.
func (c *Client) SubscribeMultiple(filters map[string]byte, handler MessageHandler) error {
	if c.closed.Load() {
		return ErrClientClosed
	}
	if !c.connected.Load() {
		return ErrNotConnected
	}

	if len(filters) == 0 {
		return ErrInvalidTopic
	}

	// Build subscription list
	subs := make([]Subscription, 0, len(filters))
	for filter, qos := range filters {
		if filter == "" {
			return ErrInvalidTopic
		}
		subs = append(subs, Subscription{
			TopicFilter: filter,
			QoS:         qos,
		})
	}

	packetID, err := c.packetIDMgr.Allocate()
	if err != nil {
		return err
	}

	pkt := &SubscribePacket{
		PacketID:      packetID,
		Subscriptions: subs,
	}

	if err := c.writePacket(pkt); err != nil {
		_ = c.packetIDMgr.Release(packetID)
		return err
	}

	// Register handlers
	c.subscriptionsMu.Lock()
	for filter := range filters {
		c.subscriptions[filter] = handler
	}
	c.subscriptionsMu.Unlock()

	return nil
}

// Unsubscribe unsubscribes from topic filters.
func (c *Client) Unsubscribe(filters ...string) error {
	if c.closed.Load() {
		return ErrClientClosed
	}
	if !c.connected.Load() {
		return ErrNotConnected
	}

	if len(filters) == 0 {
		return ErrInvalidTopic
	}

	packetID, err := c.packetIDMgr.Allocate()
	if err != nil {
		return err
	}

	pkt := &UnsubscribePacket{
		PacketID:     packetID,
		TopicFilters: filters,
	}

	if err := c.writePacket(pkt); err != nil {
		_ = c.packetIDMgr.Release(packetID)
		return err
	}

	// Remove handlers
	c.subscriptionsMu.Lock()
	for _, filter := range filters {
		delete(c.subscriptions, filter)
	}
	c.subscriptionsMu.Unlock()

	return nil
}

// writePacket writes a packet to the connection with proper locking.
func (c *Client) writePacket(pkt Packet) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return ErrNotConnected
	}

	if c.options.writeTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.options.writeTimeout))
		defer c.conn.SetWriteDeadline(time.Time{})
	}

	_, err := WritePacket(c.conn, pkt, c.options.maxPacketSize)
	if err != nil {
		return err
	}

	c.lastPacket = time.Now()
	return nil
}

// emit sends an event to the event handler.
func (c *Client) emit(event error) {
	if c.options.onEvent != nil {
		c.options.onEvent(c, event)
	}
}

// readLoop reads packets from the connection.
func (c *Client) readLoop() {
	defer close(c.readDone)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		if c.options.readTimeout > 0 {
			c.conn.SetReadDeadline(time.Now().Add(c.options.readTimeout + time.Duration(c.options.keepAlive)*time.Second))
		}

		pkt, _, err := ReadPacket(c.conn, c.options.maxPacketSize)
		if err != nil {
			if c.closed.Load() {
				return
			}

			c.connected.Store(false)
			c.emit(NewConnectionLostError(err))

			if c.options.autoReconnect && !c.closed.Load() {
				go c.reconnectLoop()
			}
			return
		}

		c.handlePacket(pkt)
	}
}

// handlePacket processes an incoming packet.
func (c *Client) handlePacket(pkt Packet) {
	switch p := pkt.(type) {
	case *PublishPacket:
		c.handlePublish(p)
	case *PubackPacket:
		c.handlePuback(p)
	case *PubrecPacket:
		c.handlePubrec(p)
	case *PubrelPacket:
		c.handlePubrel(p)
	case *PubcompPacket:
		c.handlePubcomp(p)
	case *SubackPacket:
		c.handleSuback(p)
	case *UnsubackPacket:
		c.handleUnsuback(p)
	case *PingrespPacket:
		// Keep-alive response received
	case *DisconnectPacket:
		c.handleDisconnect(p)
	case *AuthPacket:
		// Enhanced auth not implemented yet
	}
}

// handlePublish processes an incoming PUBLISH packet.
func (c *Client) handlePublish(pkt *PublishPacket) {
	msg := pkt.ToMessage()

	// Handle QoS acknowledgments
	switch pkt.QoS {
	case 1:
		puback := &PubackPacket{
			PacketID:   pkt.PacketID,
			ReasonCode: ReasonSuccess,
		}
		c.writePacket(puback)
	case 2:
		pubrec := &PubrecPacket{
			PacketID:   pkt.PacketID,
			ReasonCode: ReasonSuccess,
		}
		c.writePacket(pubrec)
		c.qos2Tracker.TrackReceive(pkt.PacketID, msg)
	}

	// Deliver to matching handlers
	c.subscriptionsMu.RLock()
	for filter, handler := range c.subscriptions {
		if TopicMatch(filter, pkt.Topic) {
			handler(msg)
		}
	}
	c.subscriptionsMu.RUnlock()
}

// handlePuback processes a PUBACK packet.
func (c *Client) handlePuback(pkt *PubackPacket) {
	c.qos1Tracker.Acknowledge(pkt.PacketID)
	c.packetIDMgr.Release(pkt.PacketID)
}

// handlePubrec processes a PUBREC packet.
func (c *Client) handlePubrec(pkt *PubrecPacket) {
	if _, ok := c.qos2Tracker.HandlePubrec(pkt.PacketID); ok {
		pubrel := &PubrelPacket{
			PacketID:   pkt.PacketID,
			ReasonCode: ReasonSuccess,
		}
		c.writePacket(pubrel)
	}
}

// handlePubrel processes a PUBREL packet.
func (c *Client) handlePubrel(pkt *PubrelPacket) {
	if _, ok := c.qos2Tracker.HandlePubrel(pkt.PacketID); ok {
		pubcomp := &PubcompPacket{
			PacketID:   pkt.PacketID,
			ReasonCode: ReasonSuccess,
		}
		c.writePacket(pubcomp)
	}
}

// handlePubcomp processes a PUBCOMP packet.
func (c *Client) handlePubcomp(pkt *PubcompPacket) {
	c.qos2Tracker.HandlePubcomp(pkt.PacketID)
	c.packetIDMgr.Release(pkt.PacketID)
}

// handleSuback processes a SUBACK packet.
func (c *Client) handleSuback(_ *SubackPacket) {
	// Subscription confirmed - handlers already registered
}

// handleUnsuback processes an UNSUBACK packet.
func (c *Client) handleUnsuback(_ *UnsubackPacket) {
	// Unsubscription confirmed
}

// handleDisconnect processes a DISCONNECT packet from the server.
func (c *Client) handleDisconnect(pkt *DisconnectPacket) {
	c.connected.Store(false)
	c.emit(NewDisconnectError(pkt.ReasonCode, pkt.Properties(), true))

	if c.options.autoReconnect && !c.closed.Load() {
		go c.reconnectLoop()
	}
}

// keepAliveLoop sends PINGREQ packets to keep the connection alive.
func (c *Client) keepAliveLoop() {
	if c.options.keepAlive == 0 {
		return
	}

	interval := time.Duration(c.options.keepAlive) * time.Second / 2
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if !c.connected.Load() {
				continue
			}

			if time.Since(c.lastPacket) >= interval {
				pingreq := &PingreqPacket{}
				if err := c.writePacket(pingreq); err != nil {
					c.emit(NewConnectionLostError(err))
				}
			}
		}
	}
}

// generateClientID generates a random client ID.
func generateClientID() string {
	return fmt.Sprintf("mqttv5-%d", time.Now().UnixNano())
}

// reconnectLoop handles automatic reconnection.
// This is a placeholder that will be expanded in client_events.go.
func (c *Client) reconnectLoop() {
	if !c.options.autoReconnect || c.closed.Load() {
		return
	}

	if !c.reconnecting.CompareAndSwap(false, true) {
		return // Already reconnecting
	}
	defer c.reconnecting.Store(false)

	attempt := 0
	backoff := c.options.reconnectBackoff

	for {
		if c.closed.Load() {
			return
		}

		attempt++
		if c.options.maxReconnects > 0 && attempt > c.options.maxReconnects {
			c.emit(ErrReconnectFailed)
			return
		}

		c.emit(NewReconnectEvent(attempt, c.options.maxReconnects, backoff, c.cancel))

		select {
		case <-c.ctx.Done():
			return
		case <-time.After(backoff):
		}

		// Try to reconnect
		connectCtx, connectCancel := context.WithTimeout(context.Background(), c.options.connectTimeout)
		err := c.connect(connectCtx)
		connectCancel()

		if err == nil {
			return // Successfully reconnected
		}

		// Increase backoff
		backoff *= 2
		if backoff > c.options.maxBackoff {
			backoff = c.options.maxBackoff
		}
	}
}
