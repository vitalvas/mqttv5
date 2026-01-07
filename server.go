package mqttv5

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var authCtx = context.Background()

// setupSession handles session creation/retrieval based on CleanStart flag.
// Returns true if an existing session was found.
func (s *Server) setupSession(client *ServerClient, clientID, namespace string, cleanStart bool) bool {
	if cleanStart {
		s.config.sessionStore.Delete(namespace, clientID)
		session := s.config.sessionFactory(clientID, namespace)
		s.config.sessionStore.Create(namespace, session)
		client.SetSession(session)
		return false
	}

	existing, err := s.config.sessionStore.Get(namespace, clientID)
	if err == nil {
		client.SetSession(existing)
		return true
	}

	session := s.config.sessionFactory(clientID, namespace)
	s.config.sessionStore.Create(namespace, session)
	client.SetSession(session)
	return false
}

// authenticateClient performs authentication (standard or enhanced) and returns
// the auth result, any assigned client ID, namespace, and whether authentication succeeded.
func (s *Server) authenticateClient(conn net.Conn, connect *ConnectPacket, clientID string, maxPacketSize uint32, logger Logger) (*AuthResult, string, string, bool) {
	// Check for enhanced authentication (AuthMethod in CONNECT properties)
	authMethod := connect.Props.GetString(PropAuthenticationMethod)
	if authMethod != "" {
		// Client requested enhanced authentication
		if s.config.enhancedAuth == nil || !s.config.enhancedAuth.SupportsMethod(authMethod) {
			logger.Warn("unsupported authentication method", LogFields{
				"authMethod": authMethod,
			})
			connack := &ConnackPacket{
				ReasonCode: ReasonBadAuthMethod,
			}
			WritePacket(conn, connack, maxPacketSize)
			return nil, "", "", false
		}

		// Perform enhanced authentication
		enhancedResult, ok := s.performEnhancedAuth(conn, connect, clientID, authMethod, maxPacketSize, logger)
		if !ok {
			return nil, "", "", false
		}

		// Convert enhanced auth result to standard auth result for CONNACK properties
		if enhancedResult != nil {
			// Default empty namespace to DefaultNamespace
			namespace := enhancedResult.Namespace
			if namespace == "" {
				namespace = DefaultNamespace
			}
			authResult := &AuthResult{
				Success:          enhancedResult.Success,
				ReasonCode:       enhancedResult.ReasonCode,
				AssignedClientID: enhancedResult.AssignedClientID,
				Namespace:        namespace,
			}
			// Merge enhanced auth properties (Auth Data, User Properties, Reason String, etc.)
			authResult.Properties.Merge(&enhancedResult.Properties)
			// Add authentication method and data to response properties
			authResult.Properties.Set(PropAuthenticationMethod, authMethod)
			if len(enhancedResult.AuthData) > 0 {
				authResult.Properties.Set(PropAuthenticationData, enhancedResult.AuthData)
			}
			return authResult, enhancedResult.AssignedClientID, namespace, true
		}
		return nil, "", DefaultNamespace, true
	}

	// Standard authentication
	if s.config.auth != nil {
		actx := buildAuthContext(conn, connect, clientID)
		result, err := s.config.auth.Authenticate(authCtx, actx)
		if err != nil || result == nil || !result.Success {
			reasonCode := ReasonNotAuthorized
			if result != nil {
				reasonCode = result.ReasonCode
			}
			logger.Warn("authentication failed", LogFields{
				LogFieldReasonCode: reasonCode.String(),
			})
			connack := &ConnackPacket{
				ReasonCode: reasonCode,
			}
			WritePacket(conn, connack, maxPacketSize)
			return nil, "", "", false
		}
		logger.Debug("authentication successful", nil)
		// Default empty namespace to DefaultNamespace
		namespace := result.Namespace
		if namespace == "" {
			namespace = DefaultNamespace
		}
		return result, result.AssignedClientID, namespace, true
	}

	// No authentication configured - use default namespace
	return nil, "", DefaultNamespace, true
}

// underlyingConnGetter is an interface for connections that wrap another connection.
// This allows extracting TLS state from WebSocket connections.
type underlyingConnGetter interface {
	UnderlyingConn() net.Conn
}

// buildAuthContext creates an AuthContext from connection and CONNECT packet info.
func buildAuthContext(conn net.Conn, connect *ConnectPacket, clientID string) *AuthContext {
	actx := &AuthContext{
		ClientID:      clientID,
		Username:      connect.Username,
		Password:      connect.Password,
		RemoteAddr:    conn.RemoteAddr(),
		LocalAddr:     conn.LocalAddr(),
		ConnectPacket: connect,
		CleanStart:    connect.CleanStart,
		AuthMethod:    connect.Props.GetString(PropAuthenticationMethod),
		AuthData:      connect.Props.GetBinary(PropAuthenticationData),
	}

	// Extract TLS certificate information if available
	// Check the connection directly first, then check underlying connection
	// (for WebSocket connections that wrap TLS)
	extractTLSInfo(conn, actx)

	return actx
}

// extractTLSInfo extracts TLS certificate information from a connection.
// It handles both direct TLS connections and wrapped connections (e.g., WebSocket over TLS).
func extractTLSInfo(conn net.Conn, actx *AuthContext) {
	// First, try direct TLS connection
	if tlsConn, ok := conn.(*tls.Conn); ok {
		state := tlsConn.ConnectionState()
		if len(state.PeerCertificates) > 0 {
			actx.TLSCommonName = state.PeerCertificates[0].Subject.CommonName
			actx.TLSVerified = len(state.VerifiedChains) > 0
		}
		return
	}

	// Check if connection wraps another connection (e.g., WSConn wrapping tls.Conn)
	if wrapper, ok := conn.(underlyingConnGetter); ok {
		underlying := wrapper.UnderlyingConn()
		if tlsConn, ok := underlying.(*tls.Conn); ok {
			state := tlsConn.ConnectionState()
			if len(state.PeerCertificates) > 0 {
				actx.TLSCommonName = state.PeerCertificates[0].Subject.CommonName
				actx.TLSVerified = len(state.VerifiedChains) > 0
			}
		}
	}
}

// buildConnack creates a CONNACK packet with all required properties.
func (s *Server) buildConnack(sessionPresent bool, authResult *AuthResult, assignedClientID string, effectiveKeepAlive uint16) *ConnackPacket {
	connack := &ConnackPacket{
		SessionPresent: sessionPresent,
		ReasonCode:     ReasonSuccess,
	}

	// Apply AuthResult fields to CONNACK
	if authResult != nil {
		if authResult.SessionPresent {
			connack.SessionPresent = true
		}
		if authResult.Properties.Len() > 0 {
			connack.Props.Merge(&authResult.Properties)
		}
	}

	// Set Assigned Client Identifier if we generated one
	if assignedClientID != "" {
		connack.Props.Set(PropAssignedClientIdentifier, assignedClientID)
	}

	// Set server properties
	if s.config.keepAliveOverride > 0 {
		connack.Props.Set(PropServerKeepAlive, effectiveKeepAlive)
	}
	if s.config.topicAliasMax > 0 {
		connack.Props.Set(PropTopicAliasMaximum, s.config.topicAliasMax)
	}
	if s.config.receiveMaximum < 65535 {
		connack.Props.Set(PropReceiveMaximum, s.config.receiveMaximum)
	}
	if s.config.maxPacketSize > 0 && s.config.maxPacketSize < 268435455 {
		connack.Props.Set(PropMaximumPacketSize, s.config.maxPacketSize)
	}

	// Advertise server capabilities (MQTT v5 spec section 3.2.2.3)
	// Maximum QoS property: only include when maxQoS is 0 or 1
	// Per MQTT v5 spec section 3.2.2.3.4, valid values are 0 or 1 only
	// If server supports QoS 2, the property MUST be absent (client assumes QoS 2 if absent)
	if s.config.maxQoS < QoS2 {
		connack.Props.Set(PropMaximumQoS, s.config.maxQoS)
	}
	connack.Props.Set(PropRetainAvailable, boolToByte(s.config.retainAvailable))
	connack.Props.Set(PropWildcardSubAvailable, boolToByte(s.config.wildcardSubAvail))
	connack.Props.Set(PropSubscriptionIDAvailable, boolToByte(s.config.subIDAvailable))
	connack.Props.Set(PropSharedSubAvailable, boolToByte(s.config.sharedSubAvailable))

	return connack
}

var (
	ErrServerClosed     = errors.New("server closed")
	ErrMaxConnections   = errors.New("maximum connections reached")
	ErrClientIDConflict = errors.New("client ID already connected")
)

// Server is an MQTT v5.0 broker server.
type Server struct {
	mu        sync.RWMutex
	config    *serverConfig
	clients   map[string]*ServerClient
	subs      *SubscriptionManager
	keepAlive *KeepAliveManager
	wills     *WillManager
	running   atomic.Bool
	done      chan struct{}
	wg        sync.WaitGroup
}

// boolToByte converts a boolean to a byte (0 or 1) for MQTT properties.
func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// NewServer creates a new MQTT server.
// Use WithListener to add one or more listeners before calling ListenAndServe.
func NewServer(opts ...ServerOption) *Server {
	config := defaultServerConfig()
	for _, opt := range opts {
		opt(config)
	}

	// Adjust capabilities based on interface availability
	// If retainedStore is nil, disable retain support
	if config.retainedStore == nil {
		config.retainAvailable = false
	}

	ka := NewKeepAliveManager()
	if config.keepAliveOverride > 0 {
		ka.SetServerOverride(config.keepAliveOverride)
	}

	return &Server{
		config:    config,
		clients:   make(map[string]*ServerClient),
		subs:      NewSubscriptionManager(),
		keepAlive: ka,
		wills:     NewWillManager(),
		done:      make(chan struct{}),
	}
}

// ListenAndServe starts the server and blocks until it is closed.
func (s *Server) ListenAndServe() error {
	if !s.running.CompareAndSwap(false, true) {
		return errors.New("server already running")
	}

	if len(s.config.listeners) == 0 {
		s.running.Store(false)
		return errors.New("no listeners configured")
	}

	// Log all listeners
	for _, listener := range s.config.listeners {
		s.config.logger.Info("server started", LogFields{
			LogFieldRemoteAddr: listener.Addr().String(),
		})
	}

	// Start background tasks
	s.wg.Add(4)
	go s.keepAliveLoop()
	go s.willLoop()
	go s.qosRetryLoop()
	go s.sessionExpiryLoop()

	// Start accept loop for each listener (all but last in goroutines)
	for i, listener := range s.config.listeners {
		if i < len(s.config.listeners)-1 {
			s.wg.Add(1)
			go func(l net.Listener) {
				defer s.wg.Done()
				s.acceptLoop(l)
			}(listener)
		}
	}

	// Run last listener in current goroutine (blocking)
	s.acceptLoop(s.config.listeners[len(s.config.listeners)-1])

	s.config.logger.Info("server stopped", nil)
	return ErrServerClosed
}

// acceptLoop accepts connections from a listener.
func (s *Server) acceptLoop(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				s.config.logger.Error("accept error", LogFields{
					LogFieldError: err.Error(),
				})
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// Close stops the server.
func (s *Server) Close() error {
	if !s.running.CompareAndSwap(true, false) {
		return nil
	}

	close(s.done)

	// Close all listeners
	for _, listener := range s.config.listeners {
		listener.Close()
	}

	// Disconnect all clients - copy first to avoid holding lock during I/O
	s.mu.RLock()
	clients := make([]*ServerClient, 0, len(s.clients))
	for _, client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.RUnlock()

	for _, client := range clients {
		client.Disconnect(ReasonServerShuttingDown)
	}

	// Wait for all goroutines
	s.wg.Wait()

	return nil
}

// Metrics returns the server's metrics collector.
func (s *Server) Metrics() MetricsCollector {
	return s.config.metrics
}

// Publish sends a message to all matching subscribers.
// The message's Namespace field determines the target namespace.
func (s *Server) Publish(msg *Message) error {
	if !s.running.Load() {
		return ErrServerClosed
	}

	// Apply producer interceptors
	msg = applyProducerInterceptors(s.config.producerInterceptors, msg)
	if msg == nil {
		return nil // Message was filtered out by interceptor
	}

	if err := ValidateTopicName(msg.Topic); err != nil {
		return err
	}

	namespace := msg.Namespace
	if namespace == "" {
		namespace = DefaultNamespace
	}

	// Validate namespace to prevent key collisions with delimiter
	if err := s.config.namespaceValidator(namespace); err != nil {
		return err
	}

	// Set publish time if not already set
	if msg.PublishedAt.IsZero() {
		msg.PublishedAt = time.Now()
	}

	// Check if message has expired
	if msg.IsExpired() {
		return nil // Silently discard expired message
	}

	// Handle retained messages
	if msg.Retain {
		if len(msg.Payload) == 0 {
			s.config.retainedStore.Delete(namespace, msg.Topic)
		} else {
			s.config.retainedStore.Set(namespace, &RetainedMessage{
				Topic:           msg.Topic,
				Payload:         msg.Payload,
				QoS:             msg.QoS,
				PayloadFormat:   msg.PayloadFormat,
				MessageExpiry:   msg.MessageExpiry,
				PublishedAt:     msg.PublishedAt,
				ContentType:     msg.ContentType,
				ResponseTopic:   msg.ResponseTopic,
				CorrelationData: msg.CorrelationData,
				UserProperties:  msg.UserProperties,
			})
		}
	}

	// Find matching subscribers in the same namespace
	matches := s.subs.MatchForDelivery(msg.Topic, "", namespace)

	for _, entry := range matches {
		clientKey := NamespaceKey(entry.Namespace, entry.ClientID)
		s.mu.RLock()
		client, ok := s.clients[clientKey]
		s.mu.RUnlock()

		// Determine delivery QoS (minimum of message QoS and subscription QoS)
		deliveryQoS := msg.QoS
		if entry.Subscription.QoS < deliveryQoS {
			deliveryQoS = entry.Subscription.QoS
		}

		// Calculate remaining expiry for delivery
		remainingExpiry := msg.RemainingExpiry()

		// Create delivery message
		deliveryMsg := &Message{
			Topic:                   msg.Topic,
			Payload:                 msg.Payload,
			QoS:                     deliveryQoS,
			Retain:                  GetDeliveryRetain(entry.Subscription, msg.Retain),
			PayloadFormat:           msg.PayloadFormat,
			MessageExpiry:           remainingExpiry, // Use remaining expiry, not original
			PublishedAt:             msg.PublishedAt,
			ContentType:             msg.ContentType,
			ResponseTopic:           msg.ResponseTopic,
			CorrelationData:         msg.CorrelationData,
			UserProperties:          msg.UserProperties,
			SubscriptionIdentifiers: msg.SubscriptionIdentifiers,
			Namespace:               entry.Namespace,
		}

		// Add all aggregated subscription identifiers from matching subscriptions
		if len(entry.SubscriptionIDs) > 0 {
			deliveryMsg.SubscriptionIdentifiers = append(
				deliveryMsg.SubscriptionIdentifiers,
				entry.SubscriptionIDs...,
			)
		}

		if !ok {
			// Client is offline - queue QoS > 0 messages to session for later delivery
			if deliveryQoS > QoS0 {
				s.queueOfflineMessage(entry.Namespace, entry.ClientID, deliveryMsg)
			}
			continue
		}

		// Try to send, queue on failure for QoS > 0
		if err := client.Send(deliveryMsg); err != nil && deliveryQoS > QoS0 {
			// Queue for later delivery if quota exceeded or connection lost
			s.queueOfflineMessage(entry.Namespace, entry.ClientID, deliveryMsg)
		}
	}

	return nil
}

// Clients returns a list of connected client IDs.
// Note: In multi-tenant deployments, the same client ID may exist in different namespaces.
// Use ClientsWithNamespace() for namespace-aware client listing.
func (s *Server) Clients() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.clients))
	for key := range s.clients {
		_, clientID := ParseNamespaceKey(key)
		ids = append(ids, clientID)
	}
	return ids
}

// ClientInfo represents a connected client with its namespace.
type ClientInfo struct {
	Namespace string
	ClientID  string
}

// ClientsWithNamespace returns a list of connected clients with their namespaces.
// This is useful for multi-tenant deployments where clients are isolated by namespace.
func (s *Server) ClientsWithNamespace() []ClientInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clients := make([]ClientInfo, 0, len(s.clients))
	for key := range s.clients {
		namespace, clientID := ParseNamespaceKey(key)
		clients = append(clients, ClientInfo{
			Namespace: namespace,
			ClientID:  clientID,
		})
	}
	return clients
}

// ClientCount returns the number of connected clients.
func (s *Server) ClientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}

// Addrs returns all listener network addresses.
func (s *Server) Addrs() []net.Addr {
	addrs := make([]net.Addr, len(s.config.listeners))
	for i, l := range s.config.listeners {
		addrs[i] = l.Addr()
	}
	return addrs
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	logger := s.config.logger.WithFields(LogFields{
		LogFieldRemoteAddr: conn.RemoteAddr().String(),
	})

	// Read CONNECT packet with timeout
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	pkt, n, err := ReadPacket(conn, s.config.maxPacketSize)
	if err != nil {
		logger.Debug("failed to read CONNECT", LogFields{LogFieldError: err.Error()})
		return
	}
	s.config.metrics.BytesReceived(n)
	s.config.metrics.PacketReceived(PacketCONNECT)

	conn.SetReadDeadline(time.Time{})

	connect, ok := pkt.(*ConnectPacket)
	if !ok {
		logger.Warn("first packet not CONNECT", LogFields{
			LogFieldPacketType: pkt.Type().String(),
		})
		return
	}

	// Determine max packet size for outbound messages early (minimum of server and client limits)
	// Per MQTT 5.0 spec, server must not send packets larger than client's advertised limit
	// Computed before any CONNACK responses so all error paths respect client's valid limit
	clientMaxPacketSize := s.config.maxPacketSize
	if clientLimit := connect.Props.GetUint32(PropMaximumPacketSize); clientLimit > 0 && clientLimit <= MaxPacketSizeProtocol && clientLimit < clientMaxPacketSize {
		clientMaxPacketSize = clientLimit
	}

	// Check max connections after reading CONNECT (per MQTT spec, CONNACK must follow CONNECT)
	if s.config.maxConnections > 0 {
		s.mu.RLock()
		count := len(s.clients)
		s.mu.RUnlock()

		if count >= s.config.maxConnections {
			logger.Warn("max connections reached", nil)
			connack := &ConnackPacket{
				ReasonCode: ReasonServerBusy,
			}
			WritePacket(conn, connack, clientMaxPacketSize)
			return
		}
	}

	// Validate and handle client ID
	clientID := connect.ClientID
	var assignedClientID string
	if clientID == "" {
		// Per MQTT 5.0 spec: If CleanStart=false and ClientID is empty, reject
		if !connect.CleanStart {
			logger.Warn("empty client ID with CleanStart=false", nil)
			connack := &ConnackPacket{
				ReasonCode: ReasonClientIDNotValid,
			}
			WritePacket(conn, connack, clientMaxPacketSize)
			return
		}
		// CleanStart=true with empty ClientID: assign one
		clientID = fmt.Sprintf("auto-%d", time.Now().UnixNano())
		assignedClientID = clientID
		connect.ClientID = clientID
	}

	logger = logger.WithFields(LogFields{LogFieldClientID: clientID})

	// Validate CONNECT properties per MQTT v5 spec
	// Maximum Packet Size MUST be > 0 and <= 268435455 (Section 3.1.2.11.4)
	// Validate first so we can use valid client limit for subsequent error responses
	if connect.Props.Has(PropMaximumPacketSize) {
		clientMaxPS := connect.Props.GetUint32(PropMaximumPacketSize)
		if clientMaxPS == 0 || clientMaxPS > MaxPacketSizeProtocol {
			logger.Warn("maximum packet size invalid (protocol error)", nil)
			connack := &ConnackPacket{
				ReasonCode: ReasonProtocolError,
			}
			WritePacket(conn, connack, clientMaxPacketSize)
			return
		}
	}

	// Receive Maximum MUST be > 0 (Section 3.1.2.11.3)
	if connect.Props.Has(PropReceiveMaximum) && connect.Props.GetUint16(PropReceiveMaximum) == 0 {
		logger.Warn("receive maximum is zero (protocol error)", nil)
		connack := &ConnackPacket{
			ReasonCode: ReasonProtocolError,
		}
		WritePacket(conn, connack, clientMaxPacketSize)
		return
	}

	// Perform authentication (standard or enhanced)
	authResult, newClientID, namespace, ok := s.authenticateClient(conn, connect, clientID, clientMaxPacketSize, logger)
	if !ok {
		return // Auth failed, connection closed
	}
	if newClientID != "" && newClientID != clientID {
		clientID = newClientID
		assignedClientID = clientID
		connect.ClientID = clientID
		logger = logger.WithFields(LogFields{LogFieldClientID: clientID})
	}

	// Validate namespace using configured validator
	if err := s.config.namespaceValidator(namespace); err != nil {
		logger.Warn("invalid namespace", LogFields{
			"namespace":   namespace,
			LogFieldError: err.Error(),
		})
		connack := &ConnackPacket{
			ReasonCode: ReasonNotAuthorized,
		}
		WritePacket(conn, connack, clientMaxPacketSize)
		return
	}

	// Create client with the effective max packet size and namespace
	client := NewServerClient(conn, connect, clientMaxPacketSize, namespace)

	// Apply CONNECT properties from client
	// Receive Maximum: how many QoS 1/2 messages server can send to this client concurrently
	if rm := connect.Props.GetUint16(PropReceiveMaximum); rm > 0 {
		client.SetReceiveMaximum(rm)
	}

	// Set server's receive maximum (how many QoS 1/2 messages client can send to server)
	if s.config.receiveMaximum < 65535 {
		client.SetInboundReceiveMaximum(s.config.receiveMaximum)
	}
	// Topic Alias Maximum: how many topic aliases server can use when sending to this client
	clientTopicAliasMax := connect.Props.GetUint16(PropTopicAliasMaximum)

	// Session Expiry Interval: stored for session management and will-delay interaction
	// Set unconditionally (0 is the default meaning no session persistence)
	client.SetSessionExpiryInterval(connect.Props.GetUint32(PropSessionExpiryInterval))

	// Handle session
	sessionPresent := s.setupSession(client, clientID, namespace, connect.CleanStart)

	// Check for existing connection with same client ID in the same namespace
	clientKey := NamespaceKey(namespace, clientID)
	s.mu.Lock()
	if existing, ok := s.clients[clientKey]; ok {
		existing.Disconnect(ReasonSessionTakenOver)
		delete(s.clients, clientKey)
	}
	s.clients[clientKey] = client
	s.mu.Unlock()

	// Register keep-alive (using composite key for namespace isolation)
	effectiveKeepAlive := s.keepAlive.Register(clientKey, connect.KeepAlive)

	// Register will message or cancel any pending will from prior connection
	if connect.WillFlag {
		will := WillMessageFromConnect(connect)
		will.Namespace = namespace // Set namespace on will message
		s.wills.Register(clientKey, will)
	} else {
		// Client reconnected without a Will flag - cancel any pending will
		// from a prior connection to prevent stale wills from being published
		s.wills.CancelPending(clientKey)
	}

	// Build CONNACK with all properties
	connack := s.buildConnack(sessionPresent, authResult, assignedClientID, effectiveKeepAlive)

	// Set topic alias limits: inbound from server config, outbound from client's CONNECT
	client.SetTopicAliasMax(s.config.topicAliasMax, clientTopicAliasMax)

	// Use clientMaxPacketSize for CONNACK to respect client's advertised limit
	if _, err := WritePacket(conn, connack, clientMaxPacketSize); err != nil {
		s.removeClient(clientKey, client)
		return
	}

	// Metrics and logging
	s.config.metrics.ConnectionOpened()
	logger.Info("client connected", nil)

	// Callback
	if s.config.onConnect != nil {
		s.config.onConnect(client)
	}

	// Restore subscriptions from session
	if sessionPresent {
		session := client.Session()
		if session != nil {
			for _, sub := range session.Subscriptions() {
				s.subs.Subscribe(clientID, namespace, sub)
				s.config.metrics.SubscriptionAdded()
			}
		}
	}

	// Restore and resend inflight QoS messages from session
	if sessionPresent {
		session := client.Session()
		if session != nil {
			s.restoreInflightMessages(client, session, logger)
			s.deliverPendingMessages(client, session)
		}
	}

	// Handle packets
	s.clientLoop(client, logger)
}

func (s *Server) clientLoop(client *ServerClient, logger Logger) {
	clientID := client.ClientID()
	namespace := client.Namespace()
	clientKey := NamespaceKey(namespace, clientID)
	conn := client.Conn()

	defer func() {
		// Metrics and logging
		s.config.metrics.ConnectionClosed()
		logger.Info("client disconnected", nil)

		// Disconnect callback
		if s.config.onDisconnect != nil {
			s.config.onDisconnect(client)
		}

		// Trigger will if not clean disconnect
		// Clean disconnect is when DISCONNECT packet was received from client
		if client.IsCleanDisconnect() {
			s.wills.Unregister(clientKey)
		} else {
			// Pass session expiry interval to will manager for will-delay interaction
			sessionExpiry := time.Duration(client.SessionExpiryInterval()) * time.Second
			s.wills.TriggerWill(clientKey, sessionExpiry)
			logger.Debug("will message triggered", nil)
		}

		// Handle session expiry
		if session := client.Session(); session != nil {
			expiryInterval := client.SessionExpiryInterval()
			if expiryInterval > 0 {
				// Set expiry time for cleanup loop
				session.SetExpiryTime(time.Now().Add(time.Duration(expiryInterval) * time.Second))
			} else {
				// Session expiry of 0 means session ends immediately on disconnect
				// Delete session and subscriptions immediately per MQTT v5 spec
				s.config.sessionStore.Delete(namespace, clientID)
			}
		}

		// Pass client pointer to prevent race condition with new connections
		s.removeClient(clientKey, client)
	}()

	for {
		select {
		case <-s.done:
			return
		default:
		}

		// Set read deadline based on keep-alive
		if deadline, ok := s.keepAlive.GetDeadline(clientKey); ok {
			conn.SetReadDeadline(deadline)
		}

		pkt, n, err := ReadPacket(conn, s.config.maxPacketSize)
		if err != nil {
			// Send DISCONNECT with appropriate reason code for protocol errors
			// Network errors (io.EOF, connection reset, etc.) don't need DISCONNECT
			// as the connection is already broken
			reason := s.errorToReasonCode(err)
			if reason != ReasonSuccess {
				client.Disconnect(reason)
			}
			return
		}

		// Metrics
		s.config.metrics.BytesReceived(n)
		s.config.metrics.PacketReceived(pkt.Type())

		// Update keep-alive
		s.keepAlive.UpdateActivity(clientKey)

		switch p := pkt.(type) {
		case *PublishPacket:
			s.handlePublish(client, p, logger)

		case *PubackPacket:
			s.handlePuback(client, p)

		case *PubrecPacket:
			s.handlePubrec(client, p)

		case *PubrelPacket:
			s.handlePubrel(client, p)

		case *PubcompPacket:
			s.handlePubcomp(client, p)

		case *SubscribePacket:
			s.handleSubscribe(client, p, logger)

		case *UnsubscribePacket:
			s.handleUnsubscribe(client, p, logger)

		case *PingreqPacket:
			pingresp := &PingrespPacket{}
			if n, err := WritePacket(conn, pingresp, client.MaxPacketSize()); err == nil {
				s.config.metrics.BytesSent(n)
				s.config.metrics.PacketSent(PacketPINGRESP)
			}

		case *DisconnectPacket:
			// Per MQTT v5 spec: if client sends DISCONNECT with reason 0x04 (DisconnectWithWill),
			// the Will message SHOULD be published. Otherwise, it's a clean disconnect.
			if p.ReasonCode != ReasonDisconnectWithWill {
				client.SetCleanDisconnect()
				logger.Debug("clean disconnect received", nil)
			} else {
				logger.Debug("disconnect with will received - will publish Will message", nil)
			}
			// Handle session expiry interval update from DISCONNECT
			// Per MQTT 5.0 spec: client can update session expiry on DISCONNECT,
			// but cannot set it to non-zero if CONNECT had zero
			if sei := p.Props.GetUint32(PropSessionExpiryInterval); sei > 0 {
				if client.SessionExpiryInterval() > 0 {
					client.SetSessionExpiryInterval(sei)
				}
				// If original was 0, ignore per spec (protocol error, but we gracefully ignore)
			}
			client.Close()
			return

		case *AuthPacket:
			// Handle re-authentication per MQTT v5.0 spec section 4.12
			// If enhanced auth is not configured, disconnect with protocol error
			if s.config.enhancedAuth == nil {
				logger.Warn("AUTH packet received but enhanced auth not configured", nil)
				client.Disconnect(ReasonProtocolError)
				return
			}
			s.handleReauth(client, p, clientKey, logger)
		}
	}
}

func (s *Server) handlePuback(client *ServerClient, p *PubackPacket) {
	if _, ok := client.QoS1Tracker().Acknowledge(p.PacketID); ok {
		client.FlowControl().Release()
		if session := client.Session(); session != nil {
			session.RemoveInflightQoS1(p.PacketID)
		}
	}
}

func (s *Server) handlePubrec(client *ServerClient, p *PubrecPacket) {
	// Per MQTT v5 spec section 4.3.3: "The Sender MUST send a PUBREL message
	// in response to a PUBREC message". We must always send PUBREL to complete
	// the exchange, even when PUBREC contains an error reason code.
	// This prevents strict peers from retransmitting PUBREC indefinitely.

	// Handle error reason code (>= 0x80)
	if p.ReasonCode.IsError() {
		client.QoS2Tracker().Remove(p.PacketID)
		client.FlowControl().Release()
		if session := client.Session(); session != nil {
			session.RemoveInflightQoS2(p.PacketID)
		}
		// Send PUBREL to complete exchange even on error
		pubrel := &PubrelPacket{PacketID: p.PacketID}
		if n, err := WritePacket(client.Conn(), pubrel, client.MaxPacketSize()); err == nil {
			s.config.metrics.BytesSent(n)
			s.config.metrics.PacketSent(PacketPUBREL)
		}
		return
	}

	if _, ok := client.QoS2Tracker().HandlePubrec(p.PacketID); ok {
		pubrel := &PubrelPacket{PacketID: p.PacketID}
		if n, err := WritePacket(client.Conn(), pubrel, client.MaxPacketSize()); err == nil {
			s.config.metrics.BytesSent(n)
			s.config.metrics.PacketSent(PacketPUBREL)
		}
		if session := client.Session(); session != nil {
			if qos2Msg, exists := session.GetInflightQoS2(p.PacketID); exists {
				qos2Msg.State = QoS2AwaitingPubcomp
				qos2Msg.SentAt = time.Now()
			}
		}
	}
}

func (s *Server) handlePubrel(client *ServerClient, p *PubrelPacket) {
	msg, ok := client.QoS2Tracker().HandlePubrel(p.PacketID)
	if !ok {
		// Unknown packet ID - respond with PUBCOMP containing PacketIDNotFound
		// per MQTT v5 spec to prevent sender from retrying indefinitely
		pubcomp := &PubcompPacket{
			PacketID:   p.PacketID,
			ReasonCode: ReasonPacketIDNotFound,
		}
		if n, err := WritePacket(client.Conn(), pubcomp, client.MaxPacketSize()); err == nil {
			s.config.metrics.BytesSent(n)
			s.config.metrics.PacketSent(PacketPUBCOMP)
		}
		return
	}

	pubcomp := &PubcompPacket{PacketID: p.PacketID}
	if n, err := WritePacket(client.Conn(), pubcomp, client.MaxPacketSize()); err == nil {
		s.config.metrics.BytesSent(n)
		s.config.metrics.PacketSent(PacketPUBCOMP)
	}

	client.InboundFlowControl().Release()

	if session := client.Session(); session != nil {
		session.RemoveInflightQoS2(p.PacketID)
	}

	if msg != nil && msg.Message != nil {
		if s.config.onMessage != nil {
			s.config.onMessage(client, msg.Message)
		}
		s.publishToSubscribers(client.ClientID(), msg.Message)
	}
}

func (s *Server) handlePubcomp(client *ServerClient, p *PubcompPacket) {
	if _, ok := client.QoS2Tracker().HandlePubcomp(p.PacketID); ok {
		client.FlowControl().Release()
		if session := client.Session(); session != nil {
			session.RemoveInflightQoS2(p.PacketID)
		}
	}
}

// resolvePublishTopic resolves topic alias and validates the topic.
// Returns the resolved topic and a reason code (ReasonSuccess if valid).
func (s *Server) resolvePublishTopic(client *ServerClient, pub *PublishPacket) (string, ReasonCode) {
	topic := pub.Topic
	if alias := pub.Props.GetUint16(PropTopicAlias); alias > 0 {
		if topic != "" {
			if err := client.TopicAliases().SetInbound(alias, topic); err != nil {
				return "", ReasonTopicAliasInvalid
			}
		} else {
			resolved, err := client.TopicAliases().GetInbound(alias)
			if err != nil {
				return "", ReasonTopicAliasInvalid
			}
			topic = resolved
		}
	}

	if topic == "" {
		return "", ReasonProtocolError
	}

	if err := ValidateTopicName(topic); err != nil {
		return "", ReasonTopicNameInvalid
	}

	return topic, ReasonSuccess
}

// validatePublishFlags validates QoS and retain flags against server config.
// Returns a reason code (ReasonSuccess if valid).
func (s *Server) validatePublishFlags(pub *PublishPacket) ReasonCode {
	if pub.QoS > s.config.maxQoS {
		return ReasonQoSNotSupported
	}
	if pub.Retain && !s.config.retainAvailable {
		return ReasonRetainNotSupported
	}
	return ReasonSuccess
}

func (s *Server) handlePublish(client *ServerClient, pub *PublishPacket, logger Logger) {
	clientID := client.ClientID()
	namespace := client.Namespace()
	startTime := time.Now()

	// Resolve topic alias and validate topic
	topic, reason := s.resolvePublishTopic(client, pub)
	if reason != ReasonSuccess {
		client.Disconnect(reason)
		return
	}

	// Validate QoS and retain flags
	if reason := s.validatePublishFlags(pub); reason != ReasonSuccess {
		logger.Warn("publish validation failed", LogFields{
			LogFieldTopic:      topic,
			LogFieldReasonCode: reason.String(),
		})
		client.Disconnect(reason)
		return
	}

	// Check if this is a DUP retransmit (either DUP flag set, or QoS 2 packet already being tracked)
	_, alreadyTracked := client.QoS2Tracker().Get(pub.PacketID)
	isRetransmit := pub.DUP || (pub.QoS == QoS2 && alreadyTracked)

	// Enforce server's Receive Maximum for inbound QoS 1/2 messages
	// Skip quota acquisition for DUP retransmits to avoid double-counting
	if pub.QoS > QoS0 && !isRetransmit && !client.InboundFlowControl().TryAcquire() {
		client.Disconnect(ReasonReceiveMaxExceeded)
		return
	}

	logger.Debug("publish received", LogFields{
		LogFieldTopic: topic,
		LogFieldQoS:   pub.QoS,
	})

	// Authorization
	effectiveQoS := pub.QoS
	if s.config.authz != nil {
		azCtx := &AuthzContext{
			ClientID:   clientID,
			Namespace:  namespace,
			Username:   client.Username(),
			Topic:      topic,
			Action:     AuthzActionPublish,
			QoS:        pub.QoS,
			Retain:     pub.Retain,
			RemoteAddr: client.Conn().RemoteAddr(),
			LocalAddr:  client.Conn().LocalAddr(),
		}
		result, err := s.config.authz.Authorize(authCtx, azCtx)
		if err != nil || result == nil || !result.Allowed {
			reasonCode := ReasonNotAuthorized
			if result != nil {
				reasonCode = result.ReasonCode
			}
			logger.Warn("publish authorization failed", LogFields{
				LogFieldTopic:      topic,
				LogFieldReasonCode: reasonCode.String(),
			})
			switch pub.QoS {
			case QoS1:
				puback := &PubackPacket{
					PacketID:   pub.PacketID,
					ReasonCode: reasonCode,
				}
				WritePacket(client.Conn(), puback, client.MaxPacketSize())
				client.InboundFlowControl().Release()
			case QoS2:
				// QoS 2 requires PUBREC, not PUBACK
				pubrec := &PubrecPacket{
					PacketID:   pub.PacketID,
					ReasonCode: reasonCode,
				}
				WritePacket(client.Conn(), pubrec, client.MaxPacketSize())
				client.InboundFlowControl().Release()
			}
			return
		}
		// Apply MaxQoS downgrade if authorizer specified a lower QoS
		if result.MaxQoS < effectiveQoS {
			effectiveQoS = result.MaxQoS
		}
	}

	// Metrics
	s.config.metrics.MessageReceived(pub.QoS)

	// Send PUBACK for QoS 1
	if pub.QoS == QoS1 {
		puback := &PubackPacket{
			PacketID:   pub.PacketID,
			ReasonCode: ReasonSuccess,
		}
		if n, err := WritePacket(client.Conn(), puback, client.MaxPacketSize()); err == nil {
			s.config.metrics.BytesSent(n)
			s.config.metrics.PacketSent(PacketPUBACK)
		}
		// Release inbound quota
		client.InboundFlowControl().Release()
	}

	// Track latency
	defer func() {
		s.config.metrics.PublishLatency(time.Since(startTime))
	}()

	// Convert to Message
	msg := messageFromPublishPacket(pub, topic, effectiveQoS, namespace, clientID)

	// Apply consumer interceptors
	msg = applyConsumerInterceptors(s.config.consumerInterceptors, msg)
	if msg == nil {
		return // Message was filtered out by interceptor
	}

	// Send PUBREC for QoS 2 and track the message
	// Per MQTT 5.0 spec, message delivery happens after PUBREL is received
	if pub.QoS == QoS2 {
		// Only track new messages, not DUP retransmits
		if !isRetransmit {
			client.QoS2Tracker().TrackReceive(pub.PacketID, msg)

			// Persist receiver-side QoS 2 state for session resume
			if session := client.Session(); session != nil {
				session.AddInflightQoS2(pub.PacketID, &QoS2Message{
					PacketID:     pub.PacketID,
					Message:      msg,
					State:        QoS2ReceivedPublish,
					SentAt:       time.Now(),
					RetryTimeout: 30 * time.Second,
					IsSender:     false,
				})
			}
		}

		// Send PUBREC for both new messages and retransmits
		pubrec := &PubrecPacket{
			PacketID:   pub.PacketID,
			ReasonCode: ReasonSuccess,
		}
		if n, err := WritePacket(client.Conn(), pubrec, client.MaxPacketSize()); err == nil {
			s.config.metrics.BytesSent(n)
			s.config.metrics.PacketSent(PacketPUBREC)

			if !isRetransmit {
				client.QoS2Tracker().SendPubrec(pub.PacketID)

				// Update session state after PUBREC sent
				if session := client.Session(); session != nil {
					if qos2Msg, exists := session.GetInflightQoS2(pub.PacketID); exists {
						qos2Msg.State = QoS2AwaitingPubrel
						qos2Msg.SentAt = time.Now()
					}
				}
			}
		}
		// QoS 2 message delivery is deferred until PUBREL is received
		return
	}

	// Callback (QoS 0 and 1 only - QoS 2 callback happens on PUBREL)
	if s.config.onMessage != nil {
		s.config.onMessage(client, msg)
	}

	// Publish to subscribers (QoS 0 and 1 only - QoS 2 delivery happens on PUBREL)
	s.publishToSubscribers(clientID, msg)
}

func (s *Server) publishToSubscribers(publisherID string, msg *Message) {
	// Apply producer interceptors
	msg = applyProducerInterceptors(s.config.producerInterceptors, msg)
	if msg == nil {
		return // Message was filtered out by interceptor
	}

	namespace := msg.Namespace

	// Check if message has expired before delivery
	if msg.IsExpired() {
		return // Discard expired message
	}

	// Handle retained messages
	if msg.Retain {
		if len(msg.Payload) == 0 {
			s.config.retainedStore.Delete(namespace, msg.Topic)
		} else {
			s.config.retainedStore.Set(namespace, &RetainedMessage{
				Topic:           msg.Topic,
				Payload:         msg.Payload,
				QoS:             msg.QoS,
				PayloadFormat:   msg.PayloadFormat,
				MessageExpiry:   msg.MessageExpiry,
				PublishedAt:     msg.PublishedAt,
				ContentType:     msg.ContentType,
				ResponseTopic:   msg.ResponseTopic,
				CorrelationData: msg.CorrelationData,
				UserProperties:  msg.UserProperties,
			})
		}
	}

	// Find matching subscribers in the same namespace
	matches := s.subs.MatchForDelivery(msg.Topic, publisherID, namespace)

	for _, entry := range matches {
		clientKey := NamespaceKey(entry.Namespace, entry.ClientID)
		s.mu.RLock()
		client, ok := s.clients[clientKey]
		s.mu.RUnlock()

		// Determine delivery QoS
		deliveryQoS := msg.QoS
		if entry.Subscription.QoS < deliveryQoS {
			deliveryQoS = entry.Subscription.QoS
		}

		// Calculate remaining expiry for delivery
		remainingExpiry := msg.RemainingExpiry()

		// Create delivery message
		// IMPORTANT: When setting a new MessageExpiry, reset PublishedAt to now
		// to prevent double-decrementing when RemainingExpiry() is called again
		deliveryMsg := &Message{
			Topic:           msg.Topic,
			Payload:         msg.Payload,
			QoS:             deliveryQoS,
			Retain:          GetDeliveryRetain(entry.Subscription, msg.Retain),
			PayloadFormat:   msg.PayloadFormat,
			MessageExpiry:   remainingExpiry, // Use remaining expiry, not original
			PublishedAt:     time.Now(),      // Reset to now to match new MessageExpiry
			ContentType:     msg.ContentType,
			ResponseTopic:   msg.ResponseTopic,
			CorrelationData: msg.CorrelationData,
			UserProperties:  msg.UserProperties,
		}

		// Add all aggregated subscription identifiers from matching subscriptions
		if len(entry.SubscriptionIDs) > 0 {
			deliveryMsg.SubscriptionIdentifiers = entry.SubscriptionIDs
		}

		if !ok {
			// Client is offline - queue QoS > 0 messages to session for later delivery
			if deliveryQoS > QoS0 {
				s.queueOfflineMessage(entry.Namespace, entry.ClientID, deliveryMsg)
			}
			continue
		}

		// Try to send, queue on failure for QoS > 0
		if err := client.Send(deliveryMsg); err != nil && deliveryQoS > QoS0 {
			// Queue for later delivery if quota exceeded or connection lost
			s.queueOfflineMessage(entry.Namespace, entry.ClientID, deliveryMsg)
		}
	}
}

// queueOfflineMessage queues a message for later delivery to an offline client.
// Only QoS > 0 messages should be queued (QoS 0 has no delivery guarantee).
func (s *Server) queueOfflineMessage(namespace, clientID string, msg *Message) {
	session, err := s.config.sessionStore.Get(namespace, clientID)
	if err != nil {
		return // No session exists for this client
	}

	// Use a monotonically increasing packet ID for pending messages
	packetID := session.NextPacketID()
	if packetID == 0 {
		// Packet ID exhaustion - all 65535 IDs are in use
		// Drop the message as per MQTT v5 spec when resources are exhausted
		return
	}
	session.AddPendingMessage(packetID, msg)
}

func (s *Server) handleSubscribe(client *ServerClient, sub *SubscribePacket, logger Logger) {
	clientID := client.ClientID()
	namespace := client.Namespace()
	session := client.Session()

	// Check subscription identifier support
	if sub.Props.Has(PropSubscriptionIdentifier) && !s.config.subIDAvailable {
		logger.Warn("subscription identifiers not supported", nil)
		client.Disconnect(ReasonSubIDsNotSupported)
		return
	}

	reasonCodes := make([]ReasonCode, len(sub.Subscriptions))

	for i, subscription := range sub.Subscriptions {
		// Check wildcard subscription support
		if !s.config.wildcardSubAvail && containsWildcard(subscription.TopicFilter) {
			logger.Warn("wildcard subscriptions not supported", LogFields{
				LogFieldTopic: subscription.TopicFilter,
			})
			reasonCodes[i] = ReasonWildcardSubsNotSupported
			continue
		}

		// Check shared subscription support
		if !s.config.sharedSubAvailable && isSharedSubscription(subscription.TopicFilter) {
			logger.Warn("shared subscriptions not supported", LogFields{
				LogFieldTopic: subscription.TopicFilter,
			})
			reasonCodes[i] = ReasonSharedSubsNotSupported
			continue
		}

		// Enforce server's Maximum QoS - cap subscription QoS to what server supports
		if subscription.QoS > s.config.maxQoS {
			subscription.QoS = s.config.maxQoS
			sub.Subscriptions[i] = subscription
		}

		// Authorization
		if s.config.authz != nil {
			azCtx := &AuthzContext{
				ClientID:   clientID,
				Namespace:  namespace,
				Username:   client.Username(),
				Topic:      subscription.TopicFilter,
				Action:     AuthzActionSubscribe,
				QoS:        subscription.QoS,
				RemoteAddr: client.Conn().RemoteAddr(),
				LocalAddr:  client.Conn().LocalAddr(),
			}
			result, err := s.config.authz.Authorize(authCtx, azCtx)
			if err != nil || result == nil || !result.Allowed {
				reasonCode := ReasonNotAuthorized
				if result != nil {
					reasonCode = result.ReasonCode
				}
				logger.Warn("subscribe authorization failed", LogFields{
					LogFieldTopic:      subscription.TopicFilter,
					LogFieldReasonCode: reasonCode.String(),
				})
				reasonCodes[i] = reasonCode
				continue
			}
			// Apply MaxQoS downgrade if authorizer specified a lower QoS
			if result.MaxQoS < subscription.QoS {
				subscription.QoS = result.MaxQoS
				sub.Subscriptions[i] = subscription
			}
		}

		// Check if this is a new subscription (without removing the existing one)
		// Subscribe() handles removal of existing subscription atomically after validation
		isNew := !s.subs.HasSubscription(clientID, namespace, subscription.TopicFilter)

		// Add subscription (this validates first, then removes any existing, then adds)
		if err := s.subs.Subscribe(clientID, namespace, subscription); err != nil {
			logger.Warn("subscription failed", LogFields{
				LogFieldTopic: subscription.TopicFilter,
				LogFieldError: err.Error(),
			})
			reasonCodes[i] = ReasonTopicFilterInvalid
			continue
		}
		s.config.metrics.SubscriptionAdded()

		logger.Debug("subscription added", LogFields{
			LogFieldTopic: subscription.TopicFilter,
			LogFieldQoS:   subscription.QoS,
		})

		// Add to session
		if session != nil {
			session.AddSubscription(subscription)
		}

		reasonCodes[i] = ReasonCode(subscription.QoS)

		// Send retained messages
		if ShouldSendRetained(subscription.RetainHandling, isNew) {
			retained := s.config.retainedStore.Match(namespace, subscription.TopicFilter)
			for _, retMsg := range retained {
				deliveryQoS := retMsg.QoS
				if subscription.QoS < deliveryQoS {
					deliveryQoS = subscription.QoS
				}

				// Per MQTT v5 spec, retain flag respects RetainAsPublish subscription option
				retainFlag := true
				if !subscription.RetainAsPublish {
					retainFlag = false
				}

				// Calculate remaining expiry for delivery
				remainingExpiry := retMsg.RemainingExpiry()

				// IMPORTANT: When setting a new MessageExpiry, reset PublishedAt to now
				// to prevent double-decrementing when RemainingExpiry() is called again
				deliveryMsg := &Message{
					Topic:           retMsg.Topic,
					Payload:         retMsg.Payload,
					QoS:             deliveryQoS,
					Retain:          retainFlag,
					PayloadFormat:   retMsg.PayloadFormat,
					MessageExpiry:   remainingExpiry,
					PublishedAt:     time.Now(), // Reset to now to match new MessageExpiry
					ContentType:     retMsg.ContentType,
					ResponseTopic:   retMsg.ResponseTopic,
					CorrelationData: retMsg.CorrelationData,
					UserProperties:  retMsg.UserProperties,
				}

				// Handle send failures for QoS > 0 retained messages
				if err := client.Send(deliveryMsg); err != nil && deliveryQoS > QoS0 {
					// Queue for later delivery if quota exceeded or connection issue
					s.queueOfflineMessage(namespace, clientID, deliveryMsg)
				}
			}
		}
	}

	// Callback - only include successful subscriptions
	if s.config.onSubscribe != nil {
		var grantedSubs []Subscription
		for i, code := range reasonCodes {
			if !code.IsError() && i < len(sub.Subscriptions) {
				grantedSubs = append(grantedSubs, sub.Subscriptions[i])
			}
		}
		if len(grantedSubs) > 0 {
			s.config.onSubscribe(client, grantedSubs)
		}
	}

	// Send SUBACK
	suback := &SubackPacket{
		PacketID:    sub.PacketID,
		ReasonCodes: reasonCodes,
	}
	if n, err := WritePacket(client.Conn(), suback, client.MaxPacketSize()); err == nil {
		s.config.metrics.BytesSent(n)
		s.config.metrics.PacketSent(PacketSUBACK)
	}
}

func (s *Server) handleUnsubscribe(client *ServerClient, unsub *UnsubscribePacket, logger Logger) {
	clientID := client.ClientID()
	namespace := client.Namespace()
	session := client.Session()

	reasonCodes := make([]ReasonCode, len(unsub.TopicFilters))

	for i, filter := range unsub.TopicFilters {
		// Validate topic filter per MQTT v5 spec
		if err := ValidateTopicFilter(filter); err != nil {
			reasonCodes[i] = ReasonTopicFilterInvalid
			logger.Warn("invalid topic filter in UNSUBSCRIBE", LogFields{
				LogFieldTopic: filter,
			})
			continue
		}

		if s.subs.Unsubscribe(clientID, namespace, filter) {
			reasonCodes[i] = ReasonSuccess
			s.config.metrics.SubscriptionRemoved()
			logger.Debug("subscription removed", LogFields{
				LogFieldTopic: filter,
			})
			if session != nil {
				session.RemoveSubscription(filter)
			}
		} else {
			reasonCodes[i] = ReasonNoSubscriptionExisted
		}
	}

	// Callback
	if s.config.onUnsubscribe != nil {
		s.config.onUnsubscribe(client, unsub.TopicFilters)
	}

	// Send UNSUBACK
	unsuback := &UnsubackPacket{
		PacketID:    unsub.PacketID,
		ReasonCodes: reasonCodes,
	}
	if n, err := WritePacket(client.Conn(), unsuback, client.MaxPacketSize()); err == nil {
		s.config.metrics.BytesSent(n)
		s.config.metrics.PacketSent(PacketUNSUBACK)
	}
}

// removeClient removes a client from the server.
// If client is non-nil, the client is only removed if it matches the current
// entry in the clients map. This prevents a race condition where a new client
// with the same ID could be removed by the old client's deferred cleanup.
// clientKey is the composite key (namespace||clientID).
func (s *Server) removeClient(clientKey string, client *ServerClient) {
	s.mu.Lock()
	removed := false
	if client != nil {
		// Only remove if the map entry still points to this client
		if existing, ok := s.clients[clientKey]; ok && existing == client {
			delete(s.clients, clientKey)
			removed = true
		}
	} else {
		delete(s.clients, clientKey)
		removed = true
	}
	s.mu.Unlock()

	// Only unregister keep-alive and subscriptions if this client was actually removed.
	// This prevents an old connection from unregistering a new client's state
	// after session takeover.
	if removed {
		namespace, clientID := ParseNamespaceKey(clientKey)
		s.keepAlive.Unregister(clientKey)
		s.subs.UnsubscribeAll(clientID, namespace)
	}
}

// errorToReasonCode maps read/decode errors to MQTT v5 reason codes.
// Returns ReasonSuccess if the error is a network error (connection closed,
// timeout, etc.) which doesn't require sending DISCONNECT.
func (s *Server) errorToReasonCode(err error) ReasonCode {
	if err == nil {
		return ReasonSuccess
	}

	// Check for specific protocol errors that require DISCONNECT
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

// deliverPendingMessages delivers queued messages to a reconnected client.
// These are QoS > 0 messages that were queued while the client was offline.
func (s *Server) deliverPendingMessages(client *ServerClient, session Session) {
	pendingMsgs := session.PendingMessages()
	for packetID, msg := range pendingMsgs {
		// Remove from pending before attempting delivery
		session.RemovePendingMessage(packetID)

		// Check if message has expired - discard if so per MQTT v5 spec
		if msg.IsExpired() {
			continue
		}

		// Send the message - if it fails due to quota, it will be re-queued
		if err := client.Send(msg); err != nil {
			// Re-queue if send failed
			s.queueOfflineMessage(client.Namespace(), client.ClientID(), msg)
		}
	}
}

// restoreInflightMessages restores and resends inflight QoS messages from session.
// Per MQTT v5 spec, messages are resent with DUP flag set on session resume.
func (s *Server) restoreInflightMessages(client *ServerClient, session Session, logger Logger) {
	conn := client.Conn()

	// Restore QoS 1 messages
	for packetID, msg := range session.InflightQoS1() {
		// Restore to tracker
		client.QoS1Tracker().Track(packetID, msg.Message)

		// Acquire flow control for the resend
		if !client.FlowControl().TryAcquire() {
			logger.Warn("flow control exhausted during session restore", nil)
			continue
		}

		// Resend with DUP flag, preserving all message properties
		pub := &PublishPacket{}
		pub.FromMessage(msg.Message)
		pub.PacketID = packetID
		pub.QoS = QoS1 // Ensure QoS 1 for restored QoS 1 messages
		pub.DUP = true
		if n, err := WritePacket(conn, pub, client.MaxPacketSize()); err == nil {
			s.config.metrics.BytesSent(n)
			s.config.metrics.PacketSent(PacketPUBLISH)
		}
	}

	// Restore QoS 2 messages
	for packetID, msg := range session.InflightQoS2() {
		if msg.IsSender {
			// Restore sender-side messages
			switch msg.State {
			case QoS2AwaitingPubrec:
				// Restore to tracker and resend PUBLISH with DUP
				client.QoS2Tracker().TrackSend(packetID, msg.Message)

				// Acquire flow control for the resend
				if !client.FlowControl().TryAcquire() {
					logger.Warn("flow control exhausted during session restore", nil)
					continue
				}

				// Resend with DUP flag, preserving all message properties
				pub := &PublishPacket{}
				pub.FromMessage(msg.Message)
				pub.PacketID = packetID
				pub.QoS = QoS2 // Ensure QoS 2 for restored QoS 2 messages
				pub.DUP = true
				if n, err := WritePacket(conn, pub, client.MaxPacketSize()); err == nil {
					s.config.metrics.BytesSent(n)
					s.config.metrics.PacketSent(PacketPUBLISH)
				}

			case QoS2AwaitingPubcomp:
				// Restore to tracker (already past PUBREC) and resend PUBREL
				client.QoS2Tracker().TrackSend(packetID, msg.Message)
				client.QoS2Tracker().HandlePubrec(packetID) // Transition to AwaitingPubcomp state

				// Acquire flow control for the resend
				if !client.FlowControl().TryAcquire() {
					logger.Warn("flow control exhausted during session restore", nil)
					continue
				}

				pubrel := &PubrelPacket{PacketID: packetID}
				if n, err := WritePacket(conn, pubrel, client.MaxPacketSize()); err == nil {
					s.config.metrics.BytesSent(n)
					s.config.metrics.PacketSent(PacketPUBREL)
				}
			}
		} else {
			// Restore receiver-side messages
			// Per MQTT v5 spec, if we received a PUBLISH and sent PUBREC but haven't
			// received PUBREL yet, we need to be ready to respond to PUBREL
			switch msg.State {
			case QoS2ReceivedPublish, QoS2AwaitingPubrel:
				// Acquire inbound flow control quota for the restored message
				// If quota is exceeded (e.g., lower receive maximum on reconnect),
				// we must still restore the message but log the inconsistency
				if !client.InboundFlowControl().TryAcquire() {
					logger.Warn("inbound quota exceeded during QoS 2 session restore", LogFields{
						"packetID": packetID,
					})
					// Continue anyway - we must handle the in-flight message
				}
				// Restore to tracker - client will resend PUBREL which we'll respond to
				client.QoS2Tracker().TrackReceive(packetID, msg.Message)
				if msg.State == QoS2AwaitingPubrel {
					client.QoS2Tracker().SendPubrec(packetID)
				}
			}
		}
	}
}

func (s *Server) keepAliveLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			expired := s.keepAlive.GetExpiredClients()
			for _, clientID := range expired {
				s.mu.RLock()
				client, ok := s.clients[clientID]
				s.mu.RUnlock()

				if ok {
					// Close the client - will is triggered by deferred cleanup in clientLoop
					// since this is an unclean disconnect (no DISCONNECT packet)
					client.Close()
				}
			}
		}
	}
}

func (s *Server) willLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			ready := s.wills.GetReadyWills()
			for _, entry := range ready {
				msg := entry.Will.ToMessage()
				s.Publish(msg)
			}
		}
	}
}

func (s *Server) qosRetryLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.mu.RLock()
			clients := make([]*ServerClient, 0, len(s.clients))
			for _, client := range s.clients {
				clients = append(clients, client)
			}
			s.mu.RUnlock()

			for _, client := range clients {
				s.retryClientMessages(client)
			}
		}
	}
}

func (s *Server) retryClientMessages(client *ServerClient) {
	if !client.IsConnected() {
		return
	}

	conn := client.Conn()

	// Retry QoS 1 messages
	for _, msg := range client.QoS1Tracker().GetPendingRetries() {
		// Retransmit with DUP flag, preserving all message properties
		pub := &PublishPacket{}
		pub.FromMessage(msg.Message)
		pub.PacketID = msg.PacketID
		pub.QoS = QoS1 // Ensure QoS 1 for QoS1Tracker messages
		pub.DUP = true
		WritePacket(conn, pub, client.MaxPacketSize())
	}

	// Retry QoS 2 messages
	for _, msg := range client.QoS2Tracker().GetPendingRetries() {
		switch msg.State {
		case QoS2AwaitingPubrec:
			// Retransmit PUBLISH with DUP flag, preserving all message properties
			pub := &PublishPacket{}
			pub.FromMessage(msg.Message)
			pub.PacketID = msg.PacketID
			pub.QoS = QoS2 // Ensure QoS 2 for QoS2Tracker messages
			pub.DUP = true
			WritePacket(conn, pub, client.MaxPacketSize())
		case QoS2AwaitingPubcomp:
			// Retransmit PUBREL
			pubrel := &PubrelPacket{PacketID: msg.PacketID}
			WritePacket(conn, pubrel, client.MaxPacketSize())
		}
	}

	// Cleanup expired and completed entries to prevent unbounded growth
	client.QoS1Tracker().CleanupExpired()
	client.QoS2Tracker().CleanupExpired()
	client.QoS2Tracker().CleanupCompleted()
}

func (s *Server) sessionExpiryLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.config.sessionStore.Cleanup("") // Cleanup all namespaces
		}
	}
}

// performEnhancedAuth performs enhanced authentication with AUTH packet exchanges.
// Returns the final EnhancedAuthResult and true if successful, or nil and false if failed.
func (s *Server) performEnhancedAuth(conn net.Conn, connect *ConnectPacket, clientID, authMethod string, maxPacketSize uint32, logger Logger) (*EnhancedAuthResult, bool) {
	// Build initial context from CONNECT
	eaCtx := &EnhancedAuthContext{
		ClientID:   clientID,
		AuthMethod: authMethod,
		AuthData:   connect.Props.GetBinary(PropAuthenticationData),
		ReasonCode: ReasonSuccess,
		RemoteAddr: conn.RemoteAddr(),
	}

	// Start enhanced auth
	result, err := s.config.enhancedAuth.AuthStart(authCtx, eaCtx)
	if err != nil || result == nil {
		logger.Warn("enhanced auth start failed", LogFields{
			"authMethod": authMethod,
		})
		connack := &ConnackPacket{
			ReasonCode: ReasonNotAuthorized,
		}
		WritePacket(conn, connack, maxPacketSize)
		return nil, false
	}

	// Loop for multi-step authentication
	for result.Continue {
		// Send AUTH packet to client
		authPkt := &AuthPacket{
			ReasonCode: ReasonContinueAuth,
		}
		authPkt.Props.Set(PropAuthenticationMethod, authMethod)
		if len(result.AuthData) > 0 {
			authPkt.Props.Set(PropAuthenticationData, result.AuthData)
		}
		authPkt.Props.Merge(&result.Properties)

		if _, err := WritePacket(conn, authPkt, maxPacketSize); err != nil {
			logger.Warn("failed to send AUTH packet", LogFields{
				LogFieldError: err.Error(),
			})
			return nil, false
		}

		// Read client's AUTH response
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		pkt, _, err := ReadPacket(conn, s.config.maxPacketSize)
		conn.SetReadDeadline(time.Time{})

		if err != nil {
			logger.Warn("failed to read AUTH response", LogFields{
				LogFieldError: err.Error(),
			})
			return nil, false
		}

		clientAuth, ok := pkt.(*AuthPacket)
		if !ok {
			logger.Warn("expected AUTH packet, got different type", LogFields{
				LogFieldPacketType: pkt.Type().String(),
			})
			connack := &ConnackPacket{
				ReasonCode: ReasonProtocolError,
			}
			WritePacket(conn, connack, maxPacketSize)
			return nil, false
		}

		// Update context for next step
		eaCtx.AuthData = clientAuth.Props.GetBinary(PropAuthenticationData)
		eaCtx.ReasonCode = clientAuth.ReasonCode
		eaCtx.State = result.State

		// Continue authentication
		result, err = s.config.enhancedAuth.AuthContinue(authCtx, eaCtx)
		if err != nil || result == nil {
			logger.Warn("enhanced auth continue failed", LogFields{
				"authMethod": authMethod,
			})
			connack := &ConnackPacket{
				ReasonCode: ReasonNotAuthorized,
			}
			WritePacket(conn, connack, maxPacketSize)
			return nil, false
		}
	}

	// Check final result
	if !result.Success {
		reasonCode := result.ReasonCode
		if reasonCode == 0 {
			reasonCode = ReasonNotAuthorized
		}
		logger.Warn("enhanced authentication failed", LogFields{
			"authMethod":       authMethod,
			LogFieldReasonCode: reasonCode.String(),
		})
		connack := &ConnackPacket{
			ReasonCode: reasonCode,
		}
		WritePacket(conn, connack, maxPacketSize)
		return nil, false
	}

	logger.Debug("enhanced authentication successful", LogFields{
		"authMethod": authMethod,
	})

	return result, true
}

// handleReauth handles re-authentication via AUTH packet after CONNECT.
// Per MQTT v5.0 spec section 4.12, clients may initiate re-authentication.
func (s *Server) handleReauth(client *ServerClient, authPkt *AuthPacket, clientKey string, logger Logger) {
	conn := client.Conn()
	authMethod := authPkt.Props.GetString(PropAuthenticationMethod)

	// Update keep-alive activity at start of re-auth
	s.keepAlive.UpdateActivity(clientKey)

	// Verify auth method is supported
	if authMethod == "" || !s.config.enhancedAuth.SupportsMethod(authMethod) {
		logger.Warn("unsupported re-authentication method", LogFields{
			"authMethod": authMethod,
		})
		client.Disconnect(ReasonBadAuthMethod)
		return
	}

	// Build context for re-authentication
	eaCtx := &EnhancedAuthContext{
		ClientID:   client.ClientID(),
		AuthMethod: authMethod,
		AuthData:   authPkt.Props.GetBinary(PropAuthenticationData),
		ReasonCode: authPkt.ReasonCode,
		RemoteAddr: conn.RemoteAddr(),
	}

	// Start re-authentication
	result, err := s.config.enhancedAuth.AuthStart(authCtx, eaCtx)
	if err != nil || result == nil {
		logger.Warn("re-authentication start failed", LogFields{
			"authMethod": authMethod,
		})
		client.Disconnect(ReasonNotAuthorized)
		return
	}

	// Multi-step re-authentication loop
	for result.Continue {
		// Send AUTH packet to client
		respPkt := &AuthPacket{
			ReasonCode: ReasonContinueAuth,
		}
		respPkt.Props.Set(PropAuthenticationMethod, authMethod)
		if len(result.AuthData) > 0 {
			respPkt.Props.Set(PropAuthenticationData, result.AuthData)
		}
		respPkt.Props.Merge(&result.Properties)

		if _, err := WritePacket(conn, respPkt, client.MaxPacketSize()); err != nil {
			logger.Warn("failed to send AUTH packet during re-auth", LogFields{
				LogFieldError: err.Error(),
			})
			return
		}

		// Read client's AUTH response
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		pkt, _, err := ReadPacket(conn, s.config.maxPacketSize)
		conn.SetReadDeadline(time.Time{})

		if err != nil {
			logger.Warn("failed to read AUTH response during re-auth", LogFields{
				LogFieldError: err.Error(),
			})
			return
		}

		clientAuth, ok := pkt.(*AuthPacket)
		if !ok {
			logger.Warn("expected AUTH packet during re-auth", LogFields{
				LogFieldPacketType: pkt.Type().String(),
			})
			client.Disconnect(ReasonProtocolError)
			return
		}

		// Update keep-alive activity after receiving AUTH response
		s.keepAlive.UpdateActivity(clientKey)

		// Update context for next step
		eaCtx.AuthData = clientAuth.Props.GetBinary(PropAuthenticationData)
		eaCtx.ReasonCode = clientAuth.ReasonCode
		eaCtx.State = result.State

		// Continue authentication
		result, err = s.config.enhancedAuth.AuthContinue(authCtx, eaCtx)
		if err != nil || result == nil {
			logger.Warn("re-authentication continue failed", LogFields{
				"authMethod": authMethod,
			})
			client.Disconnect(ReasonNotAuthorized)
			return
		}
	}

	// Check final result
	if !result.Success {
		reasonCode := result.ReasonCode
		if reasonCode == 0 {
			reasonCode = ReasonNotAuthorized
		}
		logger.Warn("re-authentication failed", LogFields{
			"authMethod":       authMethod,
			LogFieldReasonCode: reasonCode.String(),
		})
		client.Disconnect(reasonCode)
		return
	}

	// Send success AUTH packet
	successPkt := &AuthPacket{
		ReasonCode: ReasonSuccess,
	}
	successPkt.Props.Set(PropAuthenticationMethod, authMethod)
	if len(result.AuthData) > 0 {
		successPkt.Props.Set(PropAuthenticationData, result.AuthData)
	}
	successPkt.Props.Merge(&result.Properties)

	if _, err := WritePacket(conn, successPkt, client.MaxPacketSize()); err != nil {
		logger.Warn("failed to send success AUTH packet", LogFields{
			LogFieldError: err.Error(),
		})
		return
	}

	logger.Debug("re-authentication successful", LogFields{
		"authMethod": authMethod,
	})
}

// messageFromPublishPacket creates a Message from a PublishPacket.
func messageFromPublishPacket(pub *PublishPacket, topic string, effectiveQoS byte, namespace, clientID string) *Message {
	msg := &Message{
		Topic:       topic,
		Payload:     pub.Payload,
		QoS:         effectiveQoS,
		Retain:      pub.Retain,
		Namespace:   namespace,
		ClientID:    clientID,
		PublishedAt: time.Now(),
	}

	// Copy properties
	if v := pub.Props.GetByte(PropPayloadFormatIndicator); v > 0 {
		msg.PayloadFormat = v
	}
	if v := pub.Props.GetUint32(PropMessageExpiryInterval); v > 0 {
		msg.MessageExpiry = v
	}
	if v := pub.Props.GetString(PropContentType); v != "" {
		msg.ContentType = v
	}
	if v := pub.Props.GetString(PropResponseTopic); v != "" {
		msg.ResponseTopic = v
	}
	if v := pub.Props.GetBinary(PropCorrelationData); len(v) > 0 {
		msg.CorrelationData = v
	}
	msg.UserProperties = pub.Props.GetAllStringPairs(PropUserProperty)

	return msg
}
