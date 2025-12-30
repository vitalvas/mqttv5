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
func (s *Server) setupSession(client *ServerClient, clientID string, cleanStart bool) bool {
	if cleanStart {
		s.config.sessionStore.Delete(clientID)
		session := s.config.sessionFactory(clientID)
		s.config.sessionStore.Create(session)
		client.SetSession(session)
		return false
	}

	existing, err := s.config.sessionStore.Get(clientID)
	if err == nil {
		client.SetSession(existing)
		return true
	}

	session := s.config.sessionFactory(clientID)
	s.config.sessionStore.Create(session)
	client.SetSession(session)
	return false
}

// authenticateClient performs authentication (standard or enhanced) and returns
// the auth result, any assigned client ID, and whether authentication succeeded.
func (s *Server) authenticateClient(conn net.Conn, connect *ConnectPacket, clientID string, logger Logger) (*AuthResult, string, bool) {
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
			WritePacket(conn, connack, s.config.maxPacketSize)
			return nil, "", false
		}

		// Perform enhanced authentication
		enhancedResult, ok := s.performEnhancedAuth(conn, connect, clientID, authMethod, logger)
		if !ok {
			return nil, "", false
		}

		// Return assigned client ID if set
		if enhancedResult != nil && enhancedResult.AssignedClientID != "" {
			return nil, enhancedResult.AssignedClientID, true
		}
		return nil, "", true
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
			WritePacket(conn, connack, s.config.maxPacketSize)
			return nil, "", false
		}
		logger.Debug("authentication successful", nil)
		return result, result.AssignedClientID, true
	}

	// No authentication configured
	return nil, "", true
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
	if tlsConn, ok := conn.(*tls.Conn); ok {
		state := tlsConn.ConnectionState()
		if len(state.PeerCertificates) > 0 {
			actx.TLSCommonName = state.PeerCertificates[0].Subject.CommonName
			actx.TLSVerified = len(state.VerifiedChains) > 0
		}
	}

	return actx
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

// NewServer creates a new MQTT server.
// Use WithListener to add one or more listeners before calling ListenAndServe.
func NewServer(opts ...ServerOption) *Server {
	config := defaultServerConfig()
	for _, opt := range opts {
		opt(config)
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

		// Check max connections
		if s.config.maxConnections > 0 {
			s.mu.RLock()
			count := len(s.clients)
			s.mu.RUnlock()

			if count >= s.config.maxConnections {
				s.config.logger.Warn("max connections reached", LogFields{
					LogFieldRemoteAddr: conn.RemoteAddr().String(),
				})
				conn.Close()
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

	// Disconnect all clients
	s.mu.Lock()
	for _, client := range s.clients {
		client.Disconnect(ReasonServerShuttingDown)
	}
	s.mu.Unlock()

	// Wait for all goroutines
	s.wg.Wait()

	return nil
}

// Publish sends a message to all matching subscribers.
func (s *Server) Publish(msg *Message) error {
	if !s.running.Load() {
		return ErrServerClosed
	}

	if err := ValidateTopicName(msg.Topic); err != nil {
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
			s.config.retainedStore.Delete(msg.Topic)
		} else {
			s.config.retainedStore.Set(&RetainedMessage{
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

	// Find matching subscribers
	matches := s.subs.MatchForDelivery(msg.Topic, "")

	for _, entry := range matches {
		s.mu.RLock()
		client, ok := s.clients[entry.ClientID]
		s.mu.RUnlock()

		if !ok {
			continue
		}

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
		}

		// Add all aggregated subscription identifiers from matching subscriptions
		if len(entry.SubscriptionIDs) > 0 {
			deliveryMsg.SubscriptionIdentifiers = append(
				deliveryMsg.SubscriptionIdentifiers,
				entry.SubscriptionIDs...,
			)
		}

		client.Send(deliveryMsg)
	}

	return nil
}

// Clients returns a list of connected client IDs.
func (s *Server) Clients() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.clients))
	for id := range s.clients {
		ids = append(ids, id)
	}
	return ids
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
			WritePacket(conn, connack, s.config.maxPacketSize)
			return
		}
		// CleanStart=true with empty ClientID: assign one
		clientID = fmt.Sprintf("auto-%d", time.Now().UnixNano())
		assignedClientID = clientID
		connect.ClientID = clientID
	}

	logger = logger.WithFields(LogFields{LogFieldClientID: clientID})

	// Perform authentication (standard or enhanced)
	authResult, newClientID, ok := s.authenticateClient(conn, connect, clientID, logger)
	if !ok {
		return // Auth failed, connection closed
	}
	if newClientID != "" && newClientID != clientID {
		clientID = newClientID
		assignedClientID = clientID
		connect.ClientID = clientID
		logger = logger.WithFields(LogFields{LogFieldClientID: clientID})
	}

	// Determine max packet size for outbound messages (minimum of server and client limits)
	// Per MQTT 5.0 spec, server must not send packets larger than client's advertised limit
	clientMaxPacketSize := s.config.maxPacketSize
	if clientLimit := connect.Props.GetUint32(PropMaximumPacketSize); clientLimit > 0 && clientLimit < clientMaxPacketSize {
		clientMaxPacketSize = clientLimit
	}

	// Create client with the effective max packet size
	client := NewServerClient(conn, connect, clientMaxPacketSize)

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
	sessionPresent := s.setupSession(client, clientID, connect.CleanStart)

	// Check for existing connection with same client ID
	s.mu.Lock()
	if existing, ok := s.clients[clientID]; ok {
		existing.Disconnect(ReasonSessionTakenOver)
		delete(s.clients, clientID)
	}
	s.clients[clientID] = client
	s.mu.Unlock()

	// Register keep-alive
	effectiveKeepAlive := s.keepAlive.Register(clientID, connect.KeepAlive)

	// Register will message
	if connect.WillFlag {
		will := WillMessageFromConnect(connect)
		s.wills.Register(clientID, will)
	}

	// Build CONNACK
	connack := &ConnackPacket{
		SessionPresent: sessionPresent,
		ReasonCode:     ReasonSuccess,
	}

	// Apply AuthResult fields to CONNACK
	if authResult != nil {
		// AuthResult.SessionPresent can override session presence
		if authResult.SessionPresent {
			connack.SessionPresent = true
		}
		// Merge AuthResult properties into CONNACK
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
	// Set topic alias limits: inbound from server config, outbound from client's CONNECT
	client.SetTopicAliasMax(s.config.topicAliasMax, clientTopicAliasMax)
	if s.config.receiveMaximum < 65535 {
		connack.Props.Set(PropReceiveMaximum, s.config.receiveMaximum)
	}

	if _, err := WritePacket(conn, connack, s.config.maxPacketSize); err != nil {
		s.removeClient(clientID, client)
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
				s.subs.Subscribe(clientID, sub)
				s.config.metrics.SubscriptionAdded()
			}
		}
	}

	// Restore and resend inflight QoS messages from session
	if sessionPresent {
		session := client.Session()
		if session != nil {
			s.restoreInflightMessages(client, session, logger)
		}
	}

	// Handle packets
	s.clientLoop(client, logger)
}

func (s *Server) clientLoop(client *ServerClient, logger Logger) {
	clientID := client.ClientID()
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
			s.wills.Unregister(clientID)
		} else {
			// Pass session expiry interval to will manager for will-delay interaction
			sessionExpiry := time.Duration(client.SessionExpiryInterval()) * time.Second
			s.wills.TriggerWill(clientID, sessionExpiry)
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
				s.config.sessionStore.Delete(clientID)
			}
		}

		// Pass client pointer to prevent race condition with new connections
		s.removeClient(clientID, client)
	}()

	for {
		select {
		case <-s.done:
			return
		default:
		}

		// Set read deadline based on keep-alive
		if deadline, ok := s.keepAlive.GetDeadline(clientID); ok {
			conn.SetReadDeadline(deadline)
		}

		pkt, n, err := ReadPacket(conn, s.config.maxPacketSize)
		if err != nil {
			return
		}

		// Metrics
		s.config.metrics.BytesReceived(n)
		s.config.metrics.PacketReceived(pkt.Type())

		// Update keep-alive
		s.keepAlive.UpdateActivity(clientID)

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
			if n, err := WritePacket(conn, pingresp, s.config.maxPacketSize); err == nil {
				s.config.metrics.BytesSent(n)
				s.config.metrics.PacketSent(PacketPINGRESP)
			}

		case *DisconnectPacket:
			// Clean disconnect - mark it and close
			logger.Debug("clean disconnect received", nil)
			client.SetCleanDisconnect()
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
	if _, ok := client.QoS2Tracker().HandlePubrec(p.PacketID); ok {
		pubrel := &PubrelPacket{PacketID: p.PacketID}
		if n, err := WritePacket(client.Conn(), pubrel, s.config.maxPacketSize); err == nil {
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
		return
	}

	pubcomp := &PubcompPacket{PacketID: p.PacketID}
	if n, err := WritePacket(client.Conn(), pubcomp, s.config.maxPacketSize); err == nil {
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

func (s *Server) handlePublish(client *ServerClient, pub *PublishPacket, logger Logger) {
	clientID := client.ClientID()
	startTime := time.Now()

	// Resolve topic alias
	topic := pub.Topic
	if alias := pub.Props.GetUint16(PropTopicAlias); alias > 0 {
		if topic != "" {
			if err := client.TopicAliases().SetInbound(alias, topic); err != nil {
				client.Disconnect(ReasonTopicAliasInvalid)
				return
			}
		} else {
			resolved, err := client.TopicAliases().GetInbound(alias)
			if err != nil {
				client.Disconnect(ReasonTopicAliasInvalid)
				return
			}
			topic = resolved
		}
	}

	// Validate topic is not empty after alias resolution
	if topic == "" {
		client.Disconnect(ReasonProtocolError)
		return
	}

	// Validate topic name (no wildcards, valid UTF-8)
	if err := ValidateTopicName(topic); err != nil {
		client.Disconnect(ReasonTopicNameInvalid)
		return
	}

	// Enforce server's Receive Maximum for inbound QoS 1/2 messages
	if pub.QoS > 0 {
		if !client.InboundFlowControl().TryAcquire() {
			// Client has exceeded the receive maximum - protocol error
			client.Disconnect(ReasonReceiveMaxExceeded)
			return
		}
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
			case 1:
				puback := &PubackPacket{
					PacketID:   pub.PacketID,
					ReasonCode: reasonCode,
				}
				WritePacket(client.Conn(), puback, s.config.maxPacketSize)
				client.InboundFlowControl().Release()
			case 2:
				// QoS 2 requires PUBREC, not PUBACK
				pubrec := &PubrecPacket{
					PacketID:   pub.PacketID,
					ReasonCode: reasonCode,
				}
				WritePacket(client.Conn(), pubrec, s.config.maxPacketSize)
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
	if pub.QoS == 1 {
		puback := &PubackPacket{
			PacketID:   pub.PacketID,
			ReasonCode: ReasonSuccess,
		}
		if n, err := WritePacket(client.Conn(), puback, s.config.maxPacketSize); err == nil {
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

	// Convert to Message (use effectiveQoS which may be downgraded by authorizer)
	msg := &Message{
		Topic:       topic,
		Payload:     pub.Payload,
		QoS:         effectiveQoS,
		Retain:      pub.Retain,
		PublishedAt: time.Now(), // Track publish time for message expiry
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

	// Send PUBREC for QoS 2 and track the message
	// Per MQTT 5.0 spec, message delivery happens after PUBREL is received
	if pub.QoS == 2 {
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

		pubrec := &PubrecPacket{
			PacketID:   pub.PacketID,
			ReasonCode: ReasonSuccess,
		}
		if n, err := WritePacket(client.Conn(), pubrec, s.config.maxPacketSize); err == nil {
			s.config.metrics.BytesSent(n)
			s.config.metrics.PacketSent(PacketPUBREC)
			client.QoS2Tracker().SendPubrec(pub.PacketID)

			// Update session state after PUBREC sent
			if session := client.Session(); session != nil {
				if qos2Msg, exists := session.GetInflightQoS2(pub.PacketID); exists {
					qos2Msg.State = QoS2AwaitingPubrel
					qos2Msg.SentAt = time.Now()
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
	// Check if message has expired before delivery
	if msg.IsExpired() {
		return // Discard expired message
	}

	// Handle retained messages
	if msg.Retain {
		if len(msg.Payload) == 0 {
			s.config.retainedStore.Delete(msg.Topic)
		} else {
			s.config.retainedStore.Set(&RetainedMessage{
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

	// Find matching subscribers
	matches := s.subs.MatchForDelivery(msg.Topic, publisherID)

	for _, entry := range matches {
		s.mu.RLock()
		client, ok := s.clients[entry.ClientID]
		s.mu.RUnlock()

		if !ok {
			continue
		}

		// Determine delivery QoS
		deliveryQoS := msg.QoS
		if entry.Subscription.QoS < deliveryQoS {
			deliveryQoS = entry.Subscription.QoS
		}

		// Calculate remaining expiry for delivery
		remainingExpiry := msg.RemainingExpiry()

		// Create delivery message
		deliveryMsg := &Message{
			Topic:           msg.Topic,
			Payload:         msg.Payload,
			QoS:             deliveryQoS,
			Retain:          GetDeliveryRetain(entry.Subscription, msg.Retain),
			PayloadFormat:   msg.PayloadFormat,
			MessageExpiry:   remainingExpiry, // Use remaining expiry, not original
			PublishedAt:     msg.PublishedAt,
			ContentType:     msg.ContentType,
			ResponseTopic:   msg.ResponseTopic,
			CorrelationData: msg.CorrelationData,
			UserProperties:  msg.UserProperties,
		}

		// Add all aggregated subscription identifiers from matching subscriptions
		if len(entry.SubscriptionIDs) > 0 {
			deliveryMsg.SubscriptionIdentifiers = entry.SubscriptionIDs
		}

		client.Send(deliveryMsg)
	}
}

func (s *Server) handleSubscribe(client *ServerClient, sub *SubscribePacket, logger Logger) {
	clientID := client.ClientID()
	session := client.Session()

	reasonCodes := make([]ReasonCode, len(sub.Subscriptions))

	for i, subscription := range sub.Subscriptions {
		// Authorization
		if s.config.authz != nil {
			azCtx := &AuthzContext{
				ClientID:   clientID,
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
		isNew := !s.subs.HasSubscription(clientID, subscription.TopicFilter)

		// Add subscription (this validates first, then removes any existing, then adds)
		if err := s.subs.Subscribe(clientID, subscription); err != nil {
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
			retained := s.config.retainedStore.Match(subscription.TopicFilter)
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

				client.Send(&Message{
					Topic:           retMsg.Topic,
					Payload:         retMsg.Payload,
					QoS:             deliveryQoS,
					Retain:          retainFlag,
					PayloadFormat:   retMsg.PayloadFormat,
					MessageExpiry:   remainingExpiry,
					PublishedAt:     retMsg.PublishedAt,
					ContentType:     retMsg.ContentType,
					ResponseTopic:   retMsg.ResponseTopic,
					CorrelationData: retMsg.CorrelationData,
					UserProperties:  retMsg.UserProperties,
				})
			}
		}
	}

	// Callback
	if s.config.onSubscribe != nil {
		s.config.onSubscribe(client, sub.Subscriptions)
	}

	// Send SUBACK
	suback := &SubackPacket{
		PacketID:    sub.PacketID,
		ReasonCodes: reasonCodes,
	}
	if n, err := WritePacket(client.Conn(), suback, s.config.maxPacketSize); err == nil {
		s.config.metrics.BytesSent(n)
		s.config.metrics.PacketSent(PacketSUBACK)
	}
}

func (s *Server) handleUnsubscribe(client *ServerClient, unsub *UnsubscribePacket, logger Logger) {
	clientID := client.ClientID()
	session := client.Session()

	reasonCodes := make([]ReasonCode, len(unsub.TopicFilters))

	for i, filter := range unsub.TopicFilters {
		if s.subs.Unsubscribe(clientID, filter) {
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
	if n, err := WritePacket(client.Conn(), unsuback, s.config.maxPacketSize); err == nil {
		s.config.metrics.BytesSent(n)
		s.config.metrics.PacketSent(PacketUNSUBACK)
	}
}

// removeClient removes a client from the server.
// If client is non-nil, the client is only removed if it matches the current
// entry in the clients map. This prevents a race condition where a new client
// with the same ID could be removed by the old client's deferred cleanup.
func (s *Server) removeClient(clientID string, client *ServerClient) {
	s.mu.Lock()
	if client != nil {
		// Only remove if the map entry still points to this client
		if existing, ok := s.clients[clientID]; ok && existing == client {
			delete(s.clients, clientID)
		}
	} else {
		delete(s.clients, clientID)
	}
	s.mu.Unlock()

	s.keepAlive.Unregister(clientID)
	s.subs.UnsubscribeAll(clientID)
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

		// Resend with DUP flag
		pub := &PublishPacket{
			PacketID: packetID,
			Topic:    msg.Message.Topic,
			Payload:  msg.Message.Payload,
			QoS:      1,
			Retain:   msg.Message.Retain,
			DUP:      true,
		}
		if n, err := WritePacket(conn, pub, s.config.maxPacketSize); err == nil {
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

				pub := &PublishPacket{
					PacketID: packetID,
					Topic:    msg.Message.Topic,
					Payload:  msg.Message.Payload,
					QoS:      2,
					Retain:   msg.Message.Retain,
					DUP:      true,
				}
				if n, err := WritePacket(conn, pub, s.config.maxPacketSize); err == nil {
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
				if n, err := WritePacket(conn, pubrel, s.config.maxPacketSize); err == nil {
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
				// Restore to tracker - client will resend PUBREL which we'll respond to
				client.QoS2Tracker().TrackReceive(packetID, msg.Message)
				if msg.State == QoS2AwaitingPubrel {
					client.QoS2Tracker().SendPubrec(packetID)
				}
				// Acquire inbound flow control quota for the restored message
				client.InboundFlowControl().TryAcquire()
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
		pub := &PublishPacket{
			PacketID: msg.PacketID,
			Topic:    msg.Message.Topic,
			Payload:  msg.Message.Payload,
			QoS:      1,
			Retain:   msg.Message.Retain,
			DUP:      true, // Set DUP flag for retransmission
		}
		WritePacket(conn, pub, s.config.maxPacketSize)
	}

	// Retry QoS 2 messages
	for _, msg := range client.QoS2Tracker().GetPendingRetries() {
		switch msg.State {
		case QoS2AwaitingPubrec:
			// Retransmit PUBLISH with DUP flag
			pub := &PublishPacket{
				PacketID: msg.PacketID,
				Topic:    msg.Message.Topic,
				Payload:  msg.Message.Payload,
				QoS:      2,
				Retain:   msg.Message.Retain,
				DUP:      true,
			}
			WritePacket(conn, pub, s.config.maxPacketSize)
		case QoS2AwaitingPubcomp:
			// Retransmit PUBREL
			pubrel := &PubrelPacket{PacketID: msg.PacketID}
			WritePacket(conn, pubrel, s.config.maxPacketSize)
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
			s.config.sessionStore.Cleanup()
		}
	}
}

// performEnhancedAuth performs enhanced authentication with AUTH packet exchanges.
// Returns the final EnhancedAuthResult and true if successful, or nil and false if failed.
func (s *Server) performEnhancedAuth(conn net.Conn, connect *ConnectPacket, clientID, authMethod string, logger Logger) (*EnhancedAuthResult, bool) {
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
		WritePacket(conn, connack, s.config.maxPacketSize)
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

		if _, err := WritePacket(conn, authPkt, s.config.maxPacketSize); err != nil {
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
			WritePacket(conn, connack, s.config.maxPacketSize)
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
			WritePacket(conn, connack, s.config.maxPacketSize)
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
		WritePacket(conn, connack, s.config.maxPacketSize)
		return nil, false
	}

	logger.Debug("enhanced authentication successful", LogFields{
		"authMethod": authMethod,
	})

	return result, true
}
