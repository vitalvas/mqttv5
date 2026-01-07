package mqttv5

import (
	"context"
	"crypto/tls"
	"errors"
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

	// Multi-server support
	serverIndex uint32 // Atomic counter for round-robin server selection

	// Session state
	session            Session
	packetIDMgr        *PacketIDManager
	qos1Tracker        *QoS1Tracker
	qos2Tracker        *QoS2Tracker
	topicAliases       *TopicAliasManager // resolves inbound topic aliases from server
	serverFlowControl  *FlowController    // limits outbound QoS 1/2 per server's Receive Maximum
	inboundFlowControl *FlowController    // limits inbound QoS 1/2 per client's Receive Maximum

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

	// Server capabilities (from CONNACK properties)
	serverMaxQoS             byte // Maximum QoS level supported by server (0, 1, or 2)
	serverRetainAvailable    bool // Whether server supports retained messages
	serverWildcardSubAvail   bool // Whether server supports wildcard subscriptions
	serverSubIDAvailable     bool // Whether server supports subscription identifiers
	serverSharedSubAvailable bool // Whether server supports shared subscriptions

	// Enhanced authentication state
	enhancedAuthState any // Holds authenticator-specific state during auth exchanges

	// Connection state
	connected    atomic.Bool
	reconnecting atomic.Bool
	closed       atomic.Bool

	// Lifecycle control
	parentCtx     context.Context // User's context for lifecycle management
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	readDone      chan struct{}
	reconnectStop chan struct{} // Used to cancel reconnection attempts
	reconnectMu   sync.Mutex    // Protects reconnectStop
	writeMu       sync.Mutex
	lastPacket    atomic.Int64 // Unix nano timestamp for thread-safe access
}

// Dial connects to an MQTT broker and returns a client.
// Use WithServers() or WithServerResolver() to configure server addresses.
func Dial(opts ...Option) (*Client, error) {
	return DialContext(context.Background(), opts...)
}

// DialContext connects to an MQTT broker with a context.
// The context controls the client's lifecycle - when canceled, the client will close.
// Use WithServers() or WithServerResolver() to configure server addresses.
func DialContext(ctx context.Context, opts ...Option) (*Client, error) {
	options := applyOptions(opts...)

	// Validate that servers are configured
	if len(options.servers) == 0 && options.serverResolver == nil {
		return nil, errors.New("no servers configured: use WithServers() or WithServerResolver()")
	}

	c := &Client{
		options:             options,
		parentCtx:           ctx, // Store parent context for lifecycle management
		subscriptions:       make(map[string]MessageHandler),
		pendingSubscribes:   make(map[uint16][]string),
		pendingUnsubscribes: make(map[uint16][]string),
		packetIDMgr:         NewPacketIDManager(),
		qos1Tracker:         NewQoS1Tracker(30*time.Second, 3),
		qos2Tracker:         NewQoS2Tracker(30*time.Second, 3),
		topicAliases:        NewTopicAliasManager(options.topicAliasMaximum, 0), // Inbound from our advertised max, no outbound yet
		serverFlowControl:   NewFlowController(65535),                           // Default, updated from CONNACK
		inboundFlowControl:  NewFlowController(options.receiveMaximum),          // Client's Receive Maximum for inbound QoS 1/2
		done:                make(chan struct{}),
		// Default server capabilities (MQTT v5 spec: if not present, assume supported)
		serverMaxQoS:             QoS2, // QoS 0, 1, 2 all supported
		serverRetainAvailable:    true, // Retained messages supported
		serverWildcardSubAvail:   true, // Wildcard subscriptions supported
		serverSubIDAvailable:     true, // Subscription identifiers supported
		serverSharedSubAvailable: true, // Shared subscriptions supported
	}

	// Generate client ID if not provided
	if options.clientID == "" {
		options.clientID = generateClientID()
	}
	c.session = options.sessionFactory(options.clientID, "") // Client-side sessions don't need namespace

	// Connect with timeout
	connectCtx, connectCancel := context.WithTimeout(ctx, options.connectTimeout)
	defer connectCancel()

	if _, err := c.connect(connectCtx); err != nil {
		return nil, err
	}

	// Watch parent context for cancellation to support graceful shutdown
	// When the parent context is canceled, close the client
	go c.watchParentContext()

	return c, nil
}

// watchParentContext monitors the parent context and closes the client when canceled.
// This ensures that canceling the context passed to DialContext properly shuts down
// the client and stops any reconnection attempts.
func (c *Client) watchParentContext() {
	if c.parentCtx == nil {
		return
	}

	select {
	case <-c.parentCtx.Done():
		// Parent context canceled - close the client
		c.Close()
	case <-c.done:
		// Client already closed
	}
}

// readConnackWithAuth reads the response after CONNECT and handles any enhanced
// authentication exchange. Returns the CONNACK packet on success.
func (c *Client) readConnackWithAuth(ctx context.Context) (*ConnackPacket, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.options.connectTimeout))
	pkt, _, err := ReadPacket(c.conn, c.options.maxPacketSize)
	c.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Handle enhanced authentication exchange
	for {
		authPkt, isAuth := pkt.(*AuthPacket)
		if !isAuth {
			break // Not an AUTH packet, proceed to CONNACK handling
		}

		if c.options.enhancedAuth == nil {
			return nil, fmt.Errorf("received AUTH packet but enhanced auth not configured")
		}

		// Check if authentication succeeded
		if authPkt.ReasonCode == ReasonSuccess {
			c.conn.SetReadDeadline(time.Now().Add(c.options.connectTimeout))
			pkt, _, err = ReadPacket(c.conn, c.options.maxPacketSize)
			c.conn.SetReadDeadline(time.Time{})
			if err != nil {
				return nil, fmt.Errorf("failed to read CONNACK after AUTH success: %w", err)
			}
			break
		}

		if authPkt.ReasonCode != ReasonContinueAuth {
			return nil, fmt.Errorf("enhanced auth failed: %s", authPkt.ReasonCode)
		}

		authCtx := &ClientEnhancedAuthContext{
			AuthMethod: authPkt.Props.GetString(PropAuthenticationMethod),
			AuthData:   authPkt.Props.GetBinary(PropAuthenticationData),
			ReasonCode: authPkt.ReasonCode,
			State:      c.enhancedAuthState,
		}

		authResult, err := c.options.enhancedAuth.AuthContinue(ctx, authCtx)
		if err != nil {
			return nil, fmt.Errorf("enhanced auth continue failed: %w", err)
		}
		c.enhancedAuthState = authResult.State

		respAuth := &AuthPacket{ReasonCode: ReasonContinueAuth}
		respAuth.Props.Set(PropAuthenticationMethod, c.options.enhancedAuth.AuthMethod())
		if len(authResult.AuthData) > 0 {
			respAuth.Props.Set(PropAuthenticationData, authResult.AuthData)
		}

		if err := c.writePacket(respAuth); err != nil {
			return nil, fmt.Errorf("failed to send AUTH: %w", err)
		}

		c.conn.SetReadDeadline(time.Now().Add(c.options.connectTimeout))
		pkt, _, err = ReadPacket(c.conn, c.options.maxPacketSize)
		c.conn.SetReadDeadline(time.Time{})
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}
	}

	connack, ok := pkt.(*ConnackPacket)
	if !ok {
		return nil, fmt.Errorf("expected CONNACK, got %T", pkt)
	}

	if connack.ReasonCode != ReasonSuccess {
		return nil, NewConnectError(connack.ReasonCode, connack.Properties())
	}

	return connack, nil
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

	// Clear topic alias mappings per MQTT v5 spec section 3.3.2.3.4
	// Topic alias mappings are specific to a Network Connection and
	// MUST NOT be reused across connections.
	c.topicAliases.Clear()

	// Reset inbound flow control to prevent stale in-flight count from
	// previous connection causing spurious ReasonReceiveMaxExceeded errors.
	c.inboundFlowControl.Reset()

	// Get server address for this connection attempt
	serverAddr, err := c.nextServer(ctx)
	if err != nil {
		c.cancel()
		return false, fmt.Errorf("failed to get server: %w", err)
	}

	conn, err := c.dial(ctx, serverAddr)
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

	// Add enhanced authentication if configured
	if c.options.enhancedAuth != nil {
		authResult, err := c.options.enhancedAuth.AuthStart(ctx)
		if err != nil {
			c.cancel()
			c.conn.Close()
			return false, fmt.Errorf("enhanced auth start failed: %w", err)
		}
		connectPkt.Props.Set(PropAuthenticationMethod, c.options.enhancedAuth.AuthMethod())
		if len(authResult.AuthData) > 0 {
			connectPkt.Props.Set(PropAuthenticationData, authResult.AuthData)
		}
		c.enhancedAuthState = authResult.State
	}

	// Send CONNECT
	if err := c.writePacket(connectPkt); err != nil {
		c.cancel()
		c.conn.Close()
		return false, fmt.Errorf("failed to send CONNECT: %w", err)
	}

	// Read response and handle any enhanced authentication exchange
	connack, err := c.readConnackWithAuth(ctx)
	if err != nil {
		c.cancel()
		c.conn.Close()
		return false, err
	}

	// Initialize outbound packet size limit to our configured max
	c.outboundMaxPacketSize = c.options.maxPacketSize

	// Apply CONNACK properties
	if err := c.applyConnackProperties(connack); err != nil {
		c.cancel()
		c.conn.Close()
		return false, err
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

// applyConnackProperties applies properties from CONNACK to the client.
// Returns an error if the server sends invalid properties.
func (c *Client) applyConnackProperties(connack *ConnackPacket) error {
	props := connack.Properties()
	if props == nil {
		return nil
	}

	// Assigned Client Identifier - server assigned us a new client ID
	if assignedID := props.GetString(PropAssignedClientIdentifier); assignedID != "" {
		c.options.clientID = assignedID
		c.session = c.options.sessionFactory(assignedID, "") // Client-side sessions don't need namespace
	}

	// Server Keep Alive - server overrides our keep-alive
	if serverKA := props.GetUint16(PropServerKeepAlive); serverKA > 0 {
		c.options.keepAlive = serverKA
	}

	// Maximum Packet Size - limit our OUTBOUND packets only
	// This limits what we can SEND to the server, NOT what we can receive
	// options.maxPacketSize remains unchanged for inbound packet limiting
	// Per MQTT v5 spec section 3.2.2.3.5, value must be > 0 and <= 268435455
	if props.Has(PropMaximumPacketSize) {
		maxPacketSize := props.GetUint32(PropMaximumPacketSize)
		if maxPacketSize == 0 || maxPacketSize > MaxPacketSizeProtocol {
			return fmt.Errorf("server sent invalid Maximum Packet Size: %w", ErrProtocolViolation)
		}
		c.outboundMaxPacketSize = maxPacketSize
	}

	// Receive Maximum - limit outbound QoS 1/2 messages in flight
	// Per MQTT v5 spec, client must not exceed server's advertised receive maximum
	// If server explicitly sends Receive Maximum = 0, it's a protocol error (Section 3.2.2.3.3)
	if props.Has(PropReceiveMaximum) {
		serverRM := props.GetUint16(PropReceiveMaximum)
		if serverRM == 0 {
			return fmt.Errorf("server sent Receive Maximum = 0: %w", ErrProtocolViolation)
		}
		c.serverFlowControl.SetReceiveMaximum(serverRM)
	}

	// Topic Alias Maximum - limit topic aliases we can use for outbound publishes
	if serverTAM := props.GetUint16(PropTopicAliasMaximum); serverTAM > 0 {
		c.topicAliases.SetOutboundMax(serverTAM)
	}

	// Server capability properties (MQTT v5 spec section 3.2.2.3)
	// If present, these override the defaults set in DialContext
	if props.Has(PropMaximumQoS) {
		maxQoS := props.GetByte(PropMaximumQoS)
		// Per MQTT v5 spec section 3.2.2.3.4, Maximum QoS can only be 0 or 1
		// Value of 2 or higher is a protocol error
		if maxQoS > QoS1 {
			return fmt.Errorf("server sent invalid Maximum QoS = %d: %w", maxQoS, ErrProtocolViolation)
		}
		c.serverMaxQoS = maxQoS
	}
	if props.Has(PropRetainAvailable) {
		c.serverRetainAvailable = props.GetByte(PropRetainAvailable) == 1
	}
	if props.Has(PropWildcardSubAvailable) {
		c.serverWildcardSubAvail = props.GetByte(PropWildcardSubAvailable) == 1
	}
	if props.Has(PropSubscriptionIDAvailable) {
		c.serverSubIDAvailable = props.GetByte(PropSubscriptionIDAvailable) == 1
	}
	if props.Has(PropSharedSubAvailable) {
		c.serverSharedSubAvailable = props.GetByte(PropSharedSubAvailable) == 1
	}

	return nil
}

// nextServer returns the next server address to try using round-robin selection.
// It calls the resolver if configured, then falls back to static servers.
func (c *Client) nextServer(ctx context.Context) (string, error) {
	var servers []string

	// Try resolver first if configured
	if c.options.serverResolver != nil {
		resolvedServers, err := c.options.serverResolver(ctx)
		if err == nil && len(resolvedServers) > 0 {
			servers = resolvedServers
		}
		// If resolver fails, fall through to static servers
	}

	// Use static servers if no resolved servers
	if len(servers) == 0 {
		servers = c.options.servers
	}

	// Error if no servers available
	if len(servers) == 0 {
		return "", errors.New("no servers available")
	}

	// Round-robin selection using atomic increment
	index := atomic.AddUint32(&c.serverIndex, 1) - 1
	selectedIndex := index % uint32(len(servers))

	return servers[selectedIndex], nil
}

// dial creates the network connection to the specified address.
func (c *Client) dial(ctx context.Context, addr string) (net.Conn, error) {
	u, err := url.Parse(addr)
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
		case "quic":
			host = net.JoinHostPort(u.Hostname(), "8883")
		}
	}

	// Resolve proxy configuration
	proxyDialer, err := c.resolveProxy(addr)
	if err != nil {
		return nil, fmt.Errorf("proxy configuration error: %w", err)
	}

	var conn net.Conn
	dialer := &net.Dialer{}

	switch u.Scheme {
	case "tcp", "mqtt":
		if proxyDialer != nil {
			conn, err = proxyDialer.DialContext(ctx, "tcp", host)
		} else {
			conn, err = dialer.DialContext(ctx, "tcp", host)
		}
	case "ssl", "tls", "mqtts":
		tlsConfig := c.options.tlsConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		if proxyDialer != nil {
			// Dial through proxy, then wrap with TLS
			conn, err = proxyDialer.DialContext(ctx, "tcp", host)
			if err == nil && conn != nil {
				tlsConn := tls.Client(conn, tlsConfig)
				if err = tlsConn.HandshakeContext(ctx); err != nil {
					conn.Close()
					return nil, fmt.Errorf("TLS handshake failed: %w", err)
				}
				conn = tlsConn
			}
		} else {
			tlsDialer := &tls.Dialer{NetDialer: dialer, Config: tlsConfig}
			conn, err = tlsDialer.DialContext(ctx, "tcp", host)
		}
	case "ws", "wss":
		wsDialer := NewWSDialer()
		if c.options.tlsConfig != nil && wsDialer.Dialer != nil {
			wsDialer.Dialer.TLSClientConfig = c.options.tlsConfig
		}
		// Set proxy for WebSocket if configured
		if proxyDialer != nil || c.options.proxyFromEnv {
			wsDialer.SetProxyFromEnvironment()
		}
		var wsConn Conn
		wsConn, err = wsDialer.Dial(ctx, addr)
		if wsConn != nil {
			conn = wsConn.(net.Conn)
		}
	case "unix":
		// Unix socket: unix:///path/to/socket or unix://localhost/path/to/socket
		// Proxy is not applicable to unix sockets
		socketPath := u.Path
		if socketPath == "" {
			socketPath = u.Host + u.Path
		}
		unixDialer := NewUnixDialer()
		var unixConn Conn
		unixConn, err = unixDialer.Dial(ctx, socketPath)
		if unixConn != nil {
			conn = unixConn.(net.Conn)
		}
	case "quic":
		// QUIC over proxy is not supported (UDP tunneling is complex)
		quicDialer := NewQUICDialer(c.options.tlsConfig)
		var quicConn Conn
		quicConn, err = quicDialer.Dial(ctx, host)
		if quicConn != nil {
			conn = quicConn.(net.Conn)
		}
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	if err != nil {
		return nil, fmt.Errorf("dial failed: %w", err)
	}

	return conn, nil
}

// resolveProxy returns a ProxyDialer based on client configuration.
// Returns nil if no proxy should be used.
func (c *Client) resolveProxy(targetAddr string) (*ProxyDialer, error) {
	// Check explicit proxy configuration first
	if c.options.proxyConfig != nil {
		return NewProxyDialer(
			c.options.proxyConfig.URL,
			c.options.proxyConfig.Username,
			c.options.proxyConfig.Password,
		)
	}

	// Check environment variables if enabled
	if c.options.proxyFromEnv {
		proxyURL, err := ProxyFromEnvironment(targetAddr)
		if err != nil {
			return nil, err
		}
		if proxyURL != nil {
			return NewProxyDialer(proxyURL.String(), "", "")
		}
	}

	return nil, nil
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

	// Check server capability: QoS level
	if msg.QoS > c.serverMaxQoS {
		return ErrQoSNotSupported
	}

	// Check server capability: retained messages
	if msg.Retain && !c.serverRetainAvailable {
		return ErrRetainNotSupported
	}

	// Apply producer interceptors
	msg = applyProducerInterceptors(c.options.producerInterceptors, msg)
	if msg == nil {
		return nil // Message was filtered out by interceptor
	}

	if err := ValidateTopicName(msg.Topic); err != nil {
		return err
	}

	pkt := &PublishPacket{}
	pkt.FromMessage(msg)

	// Assign packet ID for QoS > 0
	if msg.QoS > QoS0 {
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
		case QoS1:
			c.qos1Tracker.Track(packetID, msg)
		case QoS2:
			c.qos2Tracker.TrackSend(packetID, msg)
		}
	}

	if err := c.writePacket(pkt); err != nil {
		if msg.QoS > QoS0 {
			c.serverFlowControl.Release()
			_ = c.packetIDMgr.Release(pkt.PacketID)
			// Use Remove to unconditionally remove tracker entry regardless of state
			switch msg.QoS {
			case QoS1:
				c.qos1Tracker.Remove(pkt.PacketID)
			case QoS2:
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
		// Check server capability: wildcard subscriptions
		if !c.serverWildcardSubAvail && containsWildcard(filter) {
			return ErrWildcardSubNotSupported
		}
		// Check server capability: shared subscriptions
		if !c.serverSharedSubAvailable && isSharedSubscription(filter) {
			return ErrSharedSubNotSupported
		}
		// Enforce server's Maximum QoS - cap subscription QoS to what server supports
		effectiveQoS := min(qos, c.serverMaxQoS)
		subs = append(subs, Subscription{
			TopicFilter: filter,
			QoS:         effectiveQoS,
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

			// Check if this is a protocol/malformed packet error vs network error
			// Per MQTT v5 spec, client should send DISCONNECT with proper reason code
			if reason := clientErrorToReasonCode(err); reason != ReasonSuccess {
				c.sendDisconnect(reason)
			}

			c.connected.Store(false)

			// Cancel context to stop keepAliveLoop and qosRetryLoop goroutines
			// This prevents goroutine leaks when connection is lost
			if c.cancel != nil {
				c.cancel()
			}

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
		c.handleAuth(p)
	}
}

// handlePublish processes an incoming PUBLISH packet.
func (c *Client) handlePublish(pkt *PublishPacket) {
	topic := pkt.Topic

	// Resolve topic alias if present (MQTT v5.0 spec section 3.3.2.3.4)
	if alias := pkt.Props.GetUint16(PropTopicAlias); alias > 0 {
		if topic != "" {
			// Topic present with alias - store the mapping
			if err := c.topicAliases.SetInbound(alias, topic); err != nil {
				// Invalid alias - disconnect per MQTT spec
				c.emit(NewDisconnectError(ReasonTopicAliasInvalid, nil, true))
				c.CloseWithCode(ReasonTopicAliasInvalid)
				return
			}
		} else {
			// No topic - resolve alias from previous mapping
			resolved, err := c.topicAliases.GetInbound(alias)
			if err != nil {
				// Alias not found - disconnect per MQTT spec
				c.emit(NewDisconnectError(ReasonTopicAliasInvalid, nil, true))
				c.CloseWithCode(ReasonTopicAliasInvalid)
				return
			}
			topic = resolved
			pkt.Topic = topic // Update packet for ToMessage()
		}
	}

	// MQTT v5.0 spec: PUBLISH must have either a topic name or a valid topic alias
	if topic == "" {
		c.emit(NewDisconnectError(ReasonProtocolError, nil, true))
		c.CloseWithCode(ReasonProtocolError)
		return
	}

	msg := pkt.ToMessage()

	// Handle QoS acknowledgments
	switch pkt.QoS {
	case QoS1:
		// For QoS 1, DUP retransmits don't consume additional quota since
		// we immediately acknowledge and release. Send PUBACK regardless.
		// Note: QoS 1 has no tracking state, so we can't detect DUP retransmits,
		// but the acquire/release is atomic so no quota leak occurs.
		if c.inboundFlowControl != nil && !pkt.DUP {
			if err := c.inboundFlowControl.Acquire(); err != nil {
				c.emit(NewDisconnectError(ReasonReceiveMaxExceeded, nil, true))
				c.CloseWithCode(ReasonReceiveMaxExceeded)
				return
			}
		}
		puback := &PubackPacket{
			PacketID:   pkt.PacketID,
			ReasonCode: ReasonSuccess,
		}
		c.writePacket(puback)
		// Release inbound quota after sending PUBACK (only if we acquired)
		if c.inboundFlowControl != nil && !pkt.DUP {
			c.inboundFlowControl.Release()
		}
	case QoS2:
		// Check if this is a DUP retransmit by seeing if we're already tracking this packet ID
		_, isRetransmit := c.qos2Tracker.Get(pkt.PacketID)

		// Only acquire quota for new messages, not DUP retransmits
		if !isRetransmit {
			if c.inboundFlowControl != nil {
				if err := c.inboundFlowControl.Acquire(); err != nil {
					c.emit(NewDisconnectError(ReasonReceiveMaxExceeded, nil, true))
					c.CloseWithCode(ReasonReceiveMaxExceeded)
					return
				}
			}
			// Track the message (only for new messages, not retransmits)
			c.qos2Tracker.TrackReceive(pkt.PacketID, msg)
		}

		// Send PUBREC for both new messages and retransmits
		pubrec := &PubrecPacket{
			PacketID:   pkt.PacketID,
			ReasonCode: ReasonSuccess,
		}
		c.writePacket(pubrec)

		if !isRetransmit {
			c.qos2Tracker.SendPubrec(pkt.PacketID)
		}
		// QoS 2 message delivery is deferred until PUBREL is received
		// Inbound quota is released after PUBCOMP is sent
		return
	}

	// Deliver to matching handlers (QoS 0 and 1 only)
	c.deliverMessage(msg, pkt.Topic)
}

// deliverMessage delivers a message to matching subscription handlers.
// Handlers are copied to avoid holding the lock during callback invocation,
// which would cause deadlock if handlers call Subscribe/Unsubscribe.
func (c *Client) deliverMessage(msg *Message, topic string) {
	// Apply consumer interceptors
	msg = applyConsumerInterceptors(c.options.consumerInterceptors, msg)
	if msg == nil {
		return // Message was filtered out by interceptor
	}

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
	msg, ok := c.qos1Tracker.Acknowledge(pkt.PacketID)
	if ok {
		c.serverFlowControl.Release()

		// Check for error reason code (>= 0x80) per MQTT v5 spec
		if pkt.ReasonCode.IsError() {
			topic := ""
			if msg != nil && msg.Message != nil {
				topic = msg.Message.Topic
			}
			c.emit(NewPublishError(topic, pkt.PacketID, pkt.ReasonCode))
		}
	}
	c.packetIDMgr.Release(pkt.PacketID)
}

// handlePubrec processes a PUBREC packet.
func (c *Client) handlePubrec(pkt *PubrecPacket) {
	// Per MQTT v5 spec section 4.3.3: "The Sender MUST send a PUBREL message
	// in response to a PUBREC message". We must always send PUBREL to complete
	// the exchange, even when PUBREC contains an error reason code.
	// This prevents strict peers from retransmitting PUBREC indefinitely.

	// Handle error reason code (>= 0x80)
	if pkt.ReasonCode.IsError() {
		msg, _ := c.qos2Tracker.Get(pkt.PacketID)
		if c.qos2Tracker.Remove(pkt.PacketID) {
			c.serverFlowControl.Release()
			topic := ""
			if msg != nil && msg.Message != nil {
				topic = msg.Message.Topic
			}
			c.emit(NewPublishError(topic, pkt.PacketID, pkt.ReasonCode))
		}
		// Send PUBREL to complete exchange even on error
		pubrel := &PubrelPacket{
			PacketID:   pkt.PacketID,
			ReasonCode: ReasonSuccess,
		}
		c.writePacket(pubrel)
		c.packetIDMgr.Release(pkt.PacketID)
		return
	}

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
	if !ok {
		// Unknown packet ID - respond with PUBCOMP containing PacketIDNotFound
		// per MQTT v5 spec to prevent sender from retrying indefinitely
		pubcomp := &PubcompPacket{
			PacketID:   pkt.PacketID,
			ReasonCode: ReasonPacketIDNotFound,
		}
		c.writePacket(pubcomp)
		return
	}

	pubcomp := &PubcompPacket{
		PacketID:   pkt.PacketID,
		ReasonCode: ReasonSuccess,
	}
	c.writePacket(pubcomp)

	// Release inbound quota after sending PUBCOMP (QoS 2 flow complete)
	if c.inboundFlowControl != nil {
		c.inboundFlowControl.Release()
	}

	// Deliver the QoS 2 message now that the flow is complete
	if msg != nil && msg.Message != nil {
		c.deliverMessage(msg.Message, msg.Message.Topic)
	}
}

// handlePubcomp processes a PUBCOMP packet.
func (c *Client) handlePubcomp(pkt *PubcompPacket) {
	msg, ok := c.qos2Tracker.HandlePubcomp(pkt.PacketID)
	if ok {
		c.serverFlowControl.Release()

		// Check for error reason code (>= 0x80) per MQTT v5 spec
		if pkt.ReasonCode.IsError() {
			topic := ""
			if msg != nil && msg.Message != nil {
				topic = msg.Message.Topic
			}
			c.emit(NewPublishError(topic, pkt.PacketID, pkt.ReasonCode))
		}
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

	// Per MQTT v5 spec section 3.9.3, reason code count must match subscription count
	if len(pkt.ReasonCodes) != len(filters) {
		c.emit(NewDisconnectError(ReasonProtocolError, nil, true))
		c.CloseWithCode(ReasonProtocolError)
		return
	}

	// Check reason codes and update handlers/session for subscriptions
	c.subscriptionsMu.Lock()
	for i, code := range pkt.ReasonCodes {
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

	// Per MQTT v5 spec section 3.11.3, reason code count must match topic filter count
	if len(pkt.ReasonCodes) != len(filters) {
		c.emit(NewDisconnectError(ReasonProtocolError, nil, true))
		c.CloseWithCode(ReasonProtocolError)
		return
	}

	// Only remove handlers for successful unsubscribes
	c.subscriptionsMu.Lock()
	for i, code := range pkt.ReasonCodes {
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

	// Cancel context to stop keepAliveLoop and qosRetryLoop
	if c.cancel != nil {
		c.cancel()
	}

	// Close the network connection per MQTT v5 spec
	// The receiver of DISCONNECT must close the connection
	if c.conn != nil {
		c.conn.Close()
	}

	c.emit(NewDisconnectError(pkt.ReasonCode, pkt.Properties(), true))

	if c.options.autoReconnect && !c.closed.Load() {
		go c.reconnectLoop()
	}
}

// handleAuth processes an AUTH packet from the server (for re-authentication).
func (c *Client) handleAuth(pkt *AuthPacket) {
	// Enhanced authentication not configured - disconnect with protocol error
	if c.options.enhancedAuth == nil {
		c.emit(NewDisconnectError(ReasonProtocolError, nil, true))
		c.CloseWithCode(ReasonProtocolError)
		return
	}

	// Handle re-authentication request from server
	if pkt.ReasonCode == ReasonReAuth {
		// Server is requesting re-authentication
		authResult, err := c.options.enhancedAuth.AuthStart(c.ctx)
		if err != nil {
			c.emit(NewDisconnectError(ReasonNotAuthorized, nil, true))
			c.CloseWithCode(ReasonNotAuthorized)
			return
		}
		c.enhancedAuthState = authResult.State

		respAuth := &AuthPacket{
			ReasonCode: ReasonReAuth,
		}
		respAuth.Props.Set(PropAuthenticationMethod, c.options.enhancedAuth.AuthMethod())
		if len(authResult.AuthData) > 0 {
			respAuth.Props.Set(PropAuthenticationData, authResult.AuthData)
		}
		c.writePacket(respAuth)
		return
	}

	// Handle continue authentication
	if pkt.ReasonCode == ReasonContinueAuth {
		authCtx := &ClientEnhancedAuthContext{
			AuthMethod: pkt.Props.GetString(PropAuthenticationMethod),
			AuthData:   pkt.Props.GetBinary(PropAuthenticationData),
			ReasonCode: pkt.ReasonCode,
			State:      c.enhancedAuthState,
		}

		authResult, err := c.options.enhancedAuth.AuthContinue(c.ctx, authCtx)
		if err != nil {
			c.emit(NewDisconnectError(ReasonNotAuthorized, nil, true))
			c.CloseWithCode(ReasonNotAuthorized)
			return
		}
		c.enhancedAuthState = authResult.State

		respAuth := &AuthPacket{
			ReasonCode: ReasonContinueAuth,
		}
		respAuth.Props.Set(PropAuthenticationMethod, c.options.enhancedAuth.AuthMethod())
		if len(authResult.AuthData) > 0 {
			respAuth.Props.Set(PropAuthenticationData, authResult.AuthData)
		}
		c.writePacket(respAuth)
		return
	}

	// Handle success - re-authentication completed
	if pkt.ReasonCode == ReasonSuccess {
		c.enhancedAuthState = nil // Clear auth state
		return
	}

	// Unknown or error reason code - disconnect
	c.emit(NewDisconnectError(pkt.ReasonCode, pkt.Properties(), true))
	c.CloseWithCode(pkt.ReasonCode)
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
				pub.QoS = QoS1 // Ensure QoS 1 for QoS1Tracker messages
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
					pub.QoS = QoS2 // Ensure QoS 2 for QoS2Tracker messages
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
		pub.QoS = QoS1 // Ensure QoS 1 for QoS1Tracker messages
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
			pub.QoS = QoS2 // Ensure QoS 2 for QoS2Tracker messages
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
					QoS:         QoS0,
				})
			}
		}
	} else {
		// No session, restore with QoS 0
		for filter := range subs {
			subOptions = append(subOptions, Subscription{
				TopicFilter: filter,
				QoS:         QoS0,
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
}

// clearPendingOperations releases packet IDs and clears pending subscribe/unsubscribe
// operations. This must be called on reconnect because ACKs for in-flight operations
// will never arrive after disconnect.
func (c *Client) clearPendingOperations() {
	c.pendingOpsMu.Lock()
	// Release packet IDs for pending subscribes
	for packetID := range c.pendingSubscribes {
		_ = c.packetIDMgr.Release(packetID)
	}
	// Release packet IDs for pending unsubscribes
	for packetID := range c.pendingUnsubscribes {
		_ = c.packetIDMgr.Release(packetID)
	}
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

	// Create a channel to allow canceling reconnection attempts
	c.reconnectMu.Lock()
	c.reconnectStop = make(chan struct{})
	stopCh := c.reconnectStop
	c.reconnectMu.Unlock()

	// Cancel function to stop reconnection
	cancelReconnect := func() {
		c.reconnectMu.Lock()
		defer c.reconnectMu.Unlock()
		if c.reconnectStop != nil {
			select {
			case <-c.reconnectStop:
				// Already closed
			default:
				close(c.reconnectStop)
			}
		}
	}

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

		c.emit(NewReconnectEvent(attempt, c.options.maxReconnects, backoff, cancelReconnect))

		// Wait for backoff duration, checking for close or cancel
		timer := time.NewTimer(backoff)
		select {
		case <-c.done:
			timer.Stop()
			return
		case <-stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}

		// Try to reconnect
		connectCtx, connectCancel := context.WithTimeout(context.Background(), c.options.connectTimeout)
		sessionPresent, err := c.connect(connectCtx)
		connectCancel()

		if err == nil {
			// Always clear pending subscribe/unsubscribe operations on reconnect.
			// These operations were in-flight during disconnect and their ACKs will
			// never arrive. The packet IDs must be released to prevent exhaustion.
			c.clearPendingOperations()

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

// sendDisconnect sends a DISCONNECT packet with the given reason code.
// This is a best-effort send used before closing on protocol errors.
func (c *Client) sendDisconnect(reason ReasonCode) {
	if c.conn == nil {
		return
	}
	pkt := &DisconnectPacket{ReasonCode: reason}
	c.writeMu.Lock()
	WritePacket(c.conn, pkt, c.outboundMaxPacketSize)
	c.writeMu.Unlock()
}

// clientErrorToReasonCode maps read/decode errors to MQTT v5 reason codes.
// Returns ReasonSuccess if the error is a network error (connection closed,
// timeout, etc.) which doesn't require sending DISCONNECT.
func clientErrorToReasonCode(err error) ReasonCode {
	if err == nil {
		return ReasonSuccess
	}

	switch {
	case errors.Is(err, ErrPacketTooLarge):
		return ReasonPacketTooLarge
	case errors.Is(err, ErrUnknownPacketType):
		return ReasonProtocolError
	case errors.Is(err, ErrProtocolViolation):
		return ReasonProtocolError
	case errors.Is(err, ErrInvalidPacketType):
		return ReasonProtocolError
	case errors.Is(err, ErrDuplicateProperty):
		return ReasonProtocolError
	case errors.Is(err, ErrPropertyNotAllowed):
		return ReasonProtocolError
	case errors.Is(err, ErrInvalidReasonCode):
		return ReasonMalformedPacket
	case errors.Is(err, ErrInvalidPacketFlags):
		return ReasonMalformedPacket
	case errors.Is(err, ErrInvalidPacketID):
		return ReasonMalformedPacket
	case errors.Is(err, ErrInvalidQoS):
		return ReasonMalformedPacket
	case errors.Is(err, ErrPacketIDRequired):
		return ReasonMalformedPacket
	case errors.Is(err, ErrVarintTooLarge):
		return ReasonMalformedPacket
	case errors.Is(err, ErrVarintMalformed):
		return ReasonMalformedPacket
	case errors.Is(err, ErrVarintOverlong):
		return ReasonMalformedPacket
	case errors.Is(err, ErrInvalidConnackFlags):
		return ReasonMalformedPacket
	case errors.Is(err, ErrUnknownPropertyID):
		return ReasonMalformedPacket
	case errors.Is(err, ErrInvalidPropertyType):
		return ReasonMalformedPacket
	case errors.Is(err, ErrInvalidUTF8):
		return ReasonMalformedPacket
	}

	// For network errors (io.EOF, connection reset, timeout, etc.)
	// return ReasonSuccess to indicate no DISCONNECT should be sent
	return ReasonSuccess
}
