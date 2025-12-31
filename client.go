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
	session           Session
	packetIDMgr       *PacketIDManager
	qos1Tracker       *QoS1Tracker
	qos2Tracker       *QoS2Tracker
	serverFlowControl *FlowController // limits outbound QoS 1/2 per server's Receive Maximum

	// Subscriptions with handlers
	subscriptions   map[string]MessageHandler
	subscriptionsMu sync.RWMutex

	// Pending subscription/unsubscription operations awaiting ACK
	pendingSubscribes   map[uint16][]string // packet ID -> topic filters
	pendingUnsubscribes map[uint16][]string // packet ID -> topic filters
	pendingOpsMu        sync.Mutex

	// Packet size limits
	// outboundMaxPacketSize limits packets we send to the server (from CONNACK PropMaximumPacketSize)
	// options.maxPacketSize limits packets we receive from the server (our configured limit)
	outboundMaxPacketSize uint32

	// Connection state
	connected    atomic.Bool
	reconnecting atomic.Bool
	closed       atomic.Bool

	// Lifecycle control
	parentCtx  context.Context // User's context for lifecycle management
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	readDone   chan struct{}
	writeMu    sync.Mutex
	lastPacket atomic.Int64 // Unix nano timestamp for thread-safe access
}

// Dial connects to an MQTT broker and returns a client.
// The address should be in the format "scheme://host:port" where scheme is
// "tcp", "ssl", "tls", "ws", or "wss".
func Dial(addr string, opts ...Option) (*Client, error) {
	return DialContext(context.Background(), addr, opts...)
}

// DialContext connects to an MQTT broker with a context.
// The context controls the client's lifecycle - when canceled, the client will close.
func DialContext(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	options := applyOptions(opts...)

	c := &Client{
		addr:                addr,
		options:             options,
		parentCtx:           ctx, // Store parent context for lifecycle management
		subscriptions:       make(map[string]MessageHandler),
		pendingSubscribes:   make(map[uint16][]string),
		pendingUnsubscribes: make(map[uint16][]string),
		packetIDMgr:         NewPacketIDManager(),
		qos1Tracker:         NewQoS1Tracker(30*time.Second, 3),
		qos2Tracker:         NewQoS2Tracker(30*time.Second, 3),
		serverFlowControl:   NewFlowController(65535), // Default, updated from CONNACK
		done:                make(chan struct{}),
	}

	// Generate client ID if not provided
	if options.clientID == "" {
		options.clientID = generateClientID()
	}
	c.session = options.sessionFactory(options.clientID)

	// Connect with timeout
	connectCtx, connectCancel := context.WithTimeout(ctx, options.connectTimeout)
	defer connectCancel()

	if _, err := c.connect(connectCtx); err != nil {
		return nil, err
	}

	return c, nil
}

// connect establishes the TCP/TLS connection and performs MQTT handshake.
// Returns (sessionPresent, error) where sessionPresent indicates if the server
// resumed an existing session.
func (c *Client) connect(ctx context.Context) (bool, error) {
	// Cancel any existing goroutines from previous connection
	if c.cancel != nil {
		c.cancel()
		// Wait for readLoop to finish if it was running
		select {
		case <-c.readDone:
		case <-time.After(time.Second):
		}
	}

	// Create new context and channels for this connection
	// Derive from parent context to respect user's lifecycle control
	parentCtx := c.parentCtx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	c.ctx, c.cancel = context.WithCancel(parentCtx)
	c.readDone = make(chan struct{})

	conn, err := c.dial(ctx)
	if err != nil {
		c.cancel()
		return false, err
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
	// Add user properties
	for key, value := range c.options.userProperties {
		connectPkt.Props.Add(PropUserProperty, StringPair{Key: key, Value: value})
	}

	// Send CONNECT
	if err := c.writePacket(connectPkt); err != nil {
		c.cancel()
		c.conn.Close()
		return false, fmt.Errorf("failed to send CONNECT: %w", err)
	}

	// Read CONNACK with timeout
	c.conn.SetReadDeadline(time.Now().Add(c.options.connectTimeout))
	pkt, _, err := ReadPacket(c.conn, c.options.maxPacketSize)
	c.conn.SetReadDeadline(time.Time{})

	if err != nil {
		c.cancel()
		c.conn.Close()
		return false, fmt.Errorf("failed to read CONNACK: %w", err)
	}

	connack, ok := pkt.(*ConnackPacket)
	if !ok {
		c.cancel()
		c.conn.Close()
		return false, fmt.Errorf("expected CONNACK, got %T", pkt)
	}

	// Check reason code
	if connack.ReasonCode != ReasonSuccess {
		c.cancel()
		c.conn.Close()
		return false, NewConnectError(connack.ReasonCode, connack.Properties())
	}

	// Initialize outbound packet size limit to our configured max
	c.outboundMaxPacketSize = c.options.maxPacketSize

	// Apply CONNACK properties
	props := connack.Properties()
	if props != nil {
		// Assigned Client Identifier - server assigned us a new client ID
		if assignedID := props.GetString(PropAssignedClientIdentifier); assignedID != "" {
			c.options.clientID = assignedID
			c.session = c.options.sessionFactory(assignedID)
		}
		// Server Keep Alive - server overrides our keep-alive
		if serverKA := props.GetUint16(PropServerKeepAlive); serverKA > 0 {
			c.options.keepAlive = serverKA
		}
		// Maximum Packet Size - limit our OUTBOUND packets only
		// This limits what we can SEND to the server, NOT what we can receive
		// options.maxPacketSize remains unchanged for inbound packet limiting
		if maxPacketSize := props.GetUint32(PropMaximumPacketSize); maxPacketSize > 0 {
			c.outboundMaxPacketSize = maxPacketSize
		}
		// Receive Maximum - limit outbound QoS 1/2 messages in flight
		// Per MQTT v5 spec, client must not exceed server's advertised receive maximum
		if serverRM := props.GetUint16(PropReceiveMaximum); serverRM > 0 {
			c.serverFlowControl.SetReceiveMaximum(serverRM)
		}
		// Topic Alias Maximum - limit topic aliases we can use
		// (stored for future use when sending publishes)
	}

	c.connected.Store(true)
	c.lastPacket.Store(time.Now().UnixNano())

	// Start background goroutines
	go c.readLoop()
	go c.keepAliveLoop()
	go c.qosRetryLoop()

	// Emit connected event
	c.emit(NewConnectedEvent(connack.SessionPresent, connack.Properties()))

	return connack.SessionPresent, nil
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
func (c *Client) Publish(msg *Message) error {
	if c.closed.Load() {
		return ErrClientClosed
	}
	if !c.connected.Load() {
		return ErrNotConnected
	}

	if err := ValidateTopicName(msg.Topic); err != nil {
		return err
	}

	pkt := &PublishPacket{}
	pkt.FromMessage(msg)

	// Assign packet ID for QoS > 0
	if msg.QoS > 0 {
		// Check flow control before sending (per MQTT v5 spec)
		if !c.serverFlowControl.TryAcquire() {
			return ErrQuotaExceeded
		}

		packetID, err := c.packetIDMgr.Allocate()
		if err != nil {
			c.serverFlowControl.Release()
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
			c.serverFlowControl.Release()
			_ = c.packetIDMgr.Release(pkt.PacketID)
			// Use Remove to unconditionally remove tracker entry regardless of state
			switch msg.QoS {
			case 1:
				c.qos1Tracker.Remove(pkt.PacketID)
			case 2:
				c.qos2Tracker.Remove(pkt.PacketID)
			}
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

	// Check subscription limit
	if c.options.maxSubscriptions > 0 {
		c.subscriptionsMu.RLock()
		currentCount := len(c.subscriptions)
		c.subscriptionsMu.RUnlock()

		// Count new subscriptions (filters not already subscribed)
		newCount := 0
		c.subscriptionsMu.RLock()
		for filter := range filters {
			if _, exists := c.subscriptions[filter]; !exists {
				newCount++
			}
		}
		c.subscriptionsMu.RUnlock()

		if currentCount+newCount > c.options.maxSubscriptions {
			return ErrTooManySubscriptions
		}
	}

	// Build subscription list with validation
	// Build both subs and topicFilters in a single loop to preserve order
	// (Go map iteration order is randomized, so we must use the same iteration for both)
	subs := make([]Subscription, 0, len(filters))
	topicFilters := make([]string, 0, len(filters))
	for filter, qos := range filters {
		if filter == "" {
			return ErrInvalidTopic
		}
		// Validate topic filter (wildcards, UTF-8, etc.)
		if err := ValidateTopicFilter(filter); err != nil {
			return err
		}
		subs = append(subs, Subscription{
			TopicFilter: filter,
			QoS:         qos,
		})
		topicFilters = append(topicFilters, filter)
	}

	packetID, err := c.packetIDMgr.Allocate()
	if err != nil {
		return err
	}

	pkt := &SubscribePacket{
		PacketID:      packetID,
		Subscriptions: subs,
	}

	// Register handlers BEFORE sending packet to avoid race with incoming messages
	c.subscriptionsMu.Lock()
	for filter := range filters {
		c.subscriptions[filter] = handler
	}
	c.subscriptionsMu.Unlock()

	// Track pending subscription to check SUBACK reason codes later
	c.pendingOpsMu.Lock()
	c.pendingSubscribes[packetID] = topicFilters
	c.pendingOpsMu.Unlock()

	if err := c.writePacket(pkt); err != nil {
		// Remove handlers on failure
		c.subscriptionsMu.Lock()
		for filter := range filters {
			delete(c.subscriptions, filter)
		}
		c.subscriptionsMu.Unlock()
		// Clean up pending tracking
		c.pendingOpsMu.Lock()
		delete(c.pendingSubscribes, packetID)
		c.pendingOpsMu.Unlock()
		_ = c.packetIDMgr.Release(packetID)
		return err
	}

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

	// Validate topic filters
	for _, filter := range filters {
		if filter == "" {
			return ErrInvalidTopic
		}
		if err := ValidateTopicFilter(filter); err != nil {
			return err
		}
	}

	packetID, err := c.packetIDMgr.Allocate()
	if err != nil {
		return err
	}

	pkt := &UnsubscribePacket{
		PacketID:     packetID,
		TopicFilters: filters,
	}

	// Track pending unsubscribe to check UNSUBACK reason codes later
	c.pendingOpsMu.Lock()
	c.pendingUnsubscribes[packetID] = filters
	c.pendingOpsMu.Unlock()

	if err := c.writePacket(pkt); err != nil {
		c.pendingOpsMu.Lock()
		delete(c.pendingUnsubscribes, packetID)
		c.pendingOpsMu.Unlock()
		_ = c.packetIDMgr.Release(packetID)
		return err
	}

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

	// Use outbound packet size limit for sending (from server's CONNACK PropMaximumPacketSize)
	// If not yet set (before CONNACK), use our configured max
	maxSize := c.outboundMaxPacketSize
	if maxSize == 0 {
		maxSize = c.options.maxPacketSize
	}

	_, err := WritePacket(c.conn, pkt, maxSize)
	if err != nil {
		return err
	}

	c.lastPacket.Store(time.Now().UnixNano())
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

			// Close connection to avoid stale/half-open sockets
			if c.conn != nil {
				c.conn.Close()
			}

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
		// Enhanced authentication is not implemented on the client side.
		// Per MQTT v5.0 spec, if we receive an AUTH packet but don't support
		// enhanced auth, we should disconnect with protocol error.
		c.emit(NewDisconnectError(ReasonProtocolError, nil, true))
		c.CloseWithCode(ReasonProtocolError)
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
		// Track the message and send PUBREC, but don't deliver yet
		// Per MQTT 5.0 spec, message delivery happens after PUBREL is received
		c.qos2Tracker.TrackReceive(pkt.PacketID, msg)
		pubrec := &PubrecPacket{
			PacketID:   pkt.PacketID,
			ReasonCode: ReasonSuccess,
		}
		c.writePacket(pubrec)
		c.qos2Tracker.SendPubrec(pkt.PacketID)
		// QoS 2 message delivery is deferred until PUBREL is received
		return
	}

	// Deliver to matching handlers (QoS 0 and 1 only)
	c.deliverMessage(msg, pkt.Topic)
}

// deliverMessage delivers a message to matching subscription handlers.
// Handlers are copied to avoid holding the lock during callback invocation,
// which would cause deadlock if handlers call Subscribe/Unsubscribe.
func (c *Client) deliverMessage(msg *Message, topic string) {
	c.subscriptionsMu.RLock()
	var handlers []MessageHandler
	for filter, handler := range c.subscriptions {
		if TopicMatch(filter, topic) {
			handlers = append(handlers, handler)
		}
	}
	c.subscriptionsMu.RUnlock()

	for _, handler := range handlers {
		handler(msg)
	}
}

// handlePuback processes a PUBACK packet.
func (c *Client) handlePuback(pkt *PubackPacket) {
	if _, ok := c.qos1Tracker.Acknowledge(pkt.PacketID); ok {
		c.serverFlowControl.Release()
	}
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
	msg, ok := c.qos2Tracker.HandlePubrel(pkt.PacketID)
	if ok {
		pubcomp := &PubcompPacket{
			PacketID:   pkt.PacketID,
			ReasonCode: ReasonSuccess,
		}
		c.writePacket(pubcomp)

		// Deliver the QoS 2 message now that the flow is complete
		if msg != nil && msg.Message != nil {
			c.deliverMessage(msg.Message, msg.Message.Topic)
		}
	}
}

// handlePubcomp processes a PUBCOMP packet.
func (c *Client) handlePubcomp(pkt *PubcompPacket) {
	if _, ok := c.qos2Tracker.HandlePubcomp(pkt.PacketID); ok {
		c.serverFlowControl.Release()
	}
	c.packetIDMgr.Release(pkt.PacketID)
}

// handleSuback processes a SUBACK packet.
func (c *Client) handleSuback(pkt *SubackPacket) {
	// Get pending subscription for this packet ID
	c.pendingOpsMu.Lock()
	filters, ok := c.pendingSubscribes[pkt.PacketID]
	delete(c.pendingSubscribes, pkt.PacketID)
	c.pendingOpsMu.Unlock()

	if !ok {
		return
	}

	// Release packet ID
	_ = c.packetIDMgr.Release(pkt.PacketID)

	// Check reason codes and update handlers/session for subscriptions
	c.subscriptionsMu.Lock()
	for i, code := range pkt.ReasonCodes {
		if i >= len(filters) {
			break
		}
		filter := filters[i]
		// Reason codes >= 0x80 indicate failure
		if code.IsError() {
			delete(c.subscriptions, filter)
		} else if c.session != nil {
			// Persist successful subscription to session with granted QoS
			// The reason code for success is the granted QoS (0, 1, or 2)
			grantedQoS := byte(code)
			c.session.AddSubscription(Subscription{
				TopicFilter: filter,
				QoS:         grantedQoS,
			})
		}
	}
	c.subscriptionsMu.Unlock()
}

// handleUnsuback processes an UNSUBACK packet.
func (c *Client) handleUnsuback(pkt *UnsubackPacket) {
	// Get pending unsubscribe for this packet ID
	c.pendingOpsMu.Lock()
	filters, ok := c.pendingUnsubscribes[pkt.PacketID]
	delete(c.pendingUnsubscribes, pkt.PacketID)
	c.pendingOpsMu.Unlock()

	if !ok {
		return
	}

	// Release packet ID
	_ = c.packetIDMgr.Release(pkt.PacketID)

	// Only remove handlers for successful unsubscribes
	c.subscriptionsMu.Lock()
	for i, code := range pkt.ReasonCodes {
		if i >= len(filters) {
			break
		}
		filter := filters[i]
		// Only remove handler if unsubscribe was successful
		if code.IsSuccess() {
			delete(c.subscriptions, filter)
			// Also remove from session
			if c.session != nil {
				c.session.RemoveSubscription(filter)
			}
		}
	}
	c.subscriptionsMu.Unlock()
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

			lastPacketTime := time.Unix(0, c.lastPacket.Load())
			if time.Since(lastPacketTime) >= interval {
				pingreq := &PingreqPacket{}
				if err := c.writePacket(pingreq); err != nil {
					c.emit(NewConnectionLostError(err))
				}
			}
		}
	}
}

// qosRetryLoop handles retransmission of unacknowledged QoS 1/2 messages.
func (c *Client) qosRetryLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if !c.connected.Load() {
				continue
			}

			// Retry QoS 1 messages with DUP flag, preserving all message properties
			for _, msg := range c.qos1Tracker.GetPendingRetries() {
				pub := &PublishPacket{}
				pub.FromMessage(msg.Message)
				pub.PacketID = msg.PacketID
				pub.QoS = 1 // Ensure QoS 1 for QoS1Tracker messages
				pub.DUP = true
				c.writePacket(pub)
			}

			// Retry QoS 2 messages with DUP flag, preserving all message properties
			for _, msg := range c.qos2Tracker.GetPendingRetries() {
				switch msg.State {
				case QoS2AwaitingPubrec:
					pub := &PublishPacket{}
					pub.FromMessage(msg.Message)
					pub.PacketID = msg.PacketID
					pub.QoS = 2 // Ensure QoS 2 for QoS2Tracker messages
					pub.DUP = true
					c.writePacket(pub)
				case QoS2AwaitingPubcomp:
					pubrel := &PubrelPacket{PacketID: msg.PacketID}
					c.writePacket(pubrel)
				}
			}

			// Cleanup expired messages and completed QoS 2 entries
			c.qos1Tracker.CleanupExpired()
			c.qos2Tracker.CleanupExpired()
			c.qos2Tracker.CleanupCompleted()
		}
	}
}

// generateClientID generates a random client ID.
func generateClientID() string {
	return fmt.Sprintf("mqttv5-%d", time.Now().UnixNano())
}

// reconnectLoop handles automatic reconnection.
// This is a placeholder that will be expanded in client_events.go.
// resendInflightMessages resends all inflight QoS 1/2 messages after reconnect.
// Per MQTT v5 spec, messages are resent with DUP flag set on session resume.
func (c *Client) resendInflightMessages() {
	// Resend QoS 1 messages
	c.qos1Tracker.mu.RLock()
	qos1Messages := make([]*QoS1Message, 0, len(c.qos1Tracker.messages))
	for _, msg := range c.qos1Tracker.messages {
		qos1Messages = append(qos1Messages, msg)
	}
	c.qos1Tracker.mu.RUnlock()

	for _, msg := range qos1Messages {
		pub := &PublishPacket{}
		pub.FromMessage(msg.Message)
		pub.PacketID = msg.PacketID
		pub.QoS = 1 // Ensure QoS 1 for QoS1Tracker messages
		pub.DUP = true
		c.writePacket(pub)
	}

	// Resend QoS 2 messages based on state
	c.qos2Tracker.mu.RLock()
	qos2Messages := make([]*QoS2Message, 0, len(c.qos2Tracker.messages))
	for _, msg := range c.qos2Tracker.messages {
		qos2Messages = append(qos2Messages, msg)
	}
	c.qos2Tracker.mu.RUnlock()

	for _, msg := range qos2Messages {
		switch msg.State {
		case QoS2AwaitingPubrec:
			// Resend PUBLISH with DUP flag
			pub := &PublishPacket{}
			pub.FromMessage(msg.Message)
			pub.PacketID = msg.PacketID
			pub.QoS = 2 // Ensure QoS 2 for QoS2Tracker messages
			pub.DUP = true
			c.writePacket(pub)
		case QoS2AwaitingPubcomp:
			// Resend PUBREL
			pubrel := &PubrelPacket{PacketID: msg.PacketID}
			c.writePacket(pubrel)
		}
	}
}

// restoreSubscriptions re-subscribes to all stored subscriptions after reconnect.
func (c *Client) restoreSubscriptions() {
	c.subscriptionsMu.RLock()
	// Make a copy of subscriptions to avoid holding lock during network calls
	subs := make(map[string]MessageHandler, len(c.subscriptions))
	for filter, handler := range c.subscriptions {
		subs[filter] = handler
	}
	c.subscriptionsMu.RUnlock()

	if len(subs) == 0 {
		return
	}

	// Build subscription list from session if available
	var subOptions []Subscription
	if c.session != nil {
		for filter := range subs {
			if sub, ok := c.session.GetSubscription(filter); ok {
				subOptions = append(subOptions, sub)
			} else {
				// Fallback to QoS 0 if no session info
				subOptions = append(subOptions, Subscription{
					TopicFilter: filter,
					QoS:         0,
				})
			}
		}
	} else {
		// No session, restore with QoS 0
		for filter := range subs {
			subOptions = append(subOptions, Subscription{
				TopicFilter: filter,
				QoS:         0,
			})
		}
	}

	// Resubscribe - errors are silently ignored since this is best-effort
	for _, sub := range subOptions {
		packetID, err := c.packetIDMgr.Allocate()
		if err != nil {
			continue
		}

		subPkt := &SubscribePacket{
			PacketID:      packetID,
			Subscriptions: []Subscription{sub},
		}

		if err := c.writePacket(subPkt); err != nil {
			_ = c.packetIDMgr.Release(packetID)
		}
	}
}

// resetInflightState clears all inflight state when session is not present on reconnect.
// This prevents stale entries from blocking publishes, leaking packet IDs, or causing
// incorrect retransmissions when the server has no record of our previous session.
func (c *Client) resetInflightState() {
	// Reset QoS 1 tracker
	c.qos1Tracker = NewQoS1Tracker(30*time.Second, 3)

	// Reset QoS 2 tracker
	c.qos2Tracker = NewQoS2Tracker(30*time.Second, 3)

	// Reset packet ID manager
	c.packetIDMgr = NewPacketIDManager()

	// Reset server flow control to default (will be updated from next CONNACK)
	c.serverFlowControl = NewFlowController(65535)

	// Clear pending operations
	c.pendingOpsMu.Lock()
	c.pendingSubscribes = make(map[uint16][]string)
	c.pendingUnsubscribes = make(map[uint16][]string)
	c.pendingOpsMu.Unlock()
}

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
		sessionPresent, err := c.connect(connectCtx)
		connectCancel()

		if err == nil {
			// Only restore subscriptions and resend inflight messages if session was NOT
			// present. If session is present, the server has already stored our subscriptions
			// and inflight messages, so resending would cause duplicates.
			if !sessionPresent {
				// Reset inflight state since server has no session for us
				c.resetInflightState()
				c.restoreSubscriptions()
				c.resendInflightMessages()
			}
			return // Successfully reconnected
		}

		// Calculate next backoff duration
		if c.options.backoffStrategy != nil {
			// Use custom backoff strategy if provided
			backoff = c.options.backoffStrategy(attempt, backoff, err)
		} else {
			// Default: exponential backoff (double)
			backoff *= 2
		}
		if backoff > c.options.maxBackoff {
			backoff = c.options.maxBackoff
		}
	}
}
