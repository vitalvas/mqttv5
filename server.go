package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var authCtx = context.Background()

var (
	ErrServerClosed     = errors.New("server closed")
	ErrMaxConnections   = errors.New("maximum connections reached")
	ErrClientIDConflict = errors.New("client ID already connected")
)

// Server is an MQTT v5.0 broker server.
type Server struct {
	mu        sync.RWMutex
	config    *serverConfig
	listener  net.Listener
	clients   map[string]*ServerClient
	subs      *SubscriptionManager
	keepAlive *KeepAliveManager
	wills     *WillManager
	running   atomic.Bool
	done      chan struct{}
	wg        sync.WaitGroup
}

// NewServer creates a new MQTT server.
func NewServer(addr string, opts ...ServerOption) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return NewServerWithListener(listener, opts...), nil
}

// NewServerWithListener creates a new MQTT server with a custom listener.
func NewServerWithListener(listener net.Listener, opts ...ServerOption) *Server {
	config := defaultServerConfig()
	for _, opt := range opts {
		opt(config)
	}

	if config.keepAliveOverride > 0 {
		ka := NewKeepAliveManager()
		ka.SetServerOverride(config.keepAliveOverride)
		return &Server{
			config:    config,
			listener:  listener,
			clients:   make(map[string]*ServerClient),
			subs:      NewSubscriptionManager(),
			keepAlive: ka,
			wills:     NewWillManager(),
			done:      make(chan struct{}),
		}
	}

	return &Server{
		config:    config,
		listener:  listener,
		clients:   make(map[string]*ServerClient),
		subs:      NewSubscriptionManager(),
		keepAlive: NewKeepAliveManager(),
		wills:     NewWillManager(),
		done:      make(chan struct{}),
	}
}

// ListenAndServe starts the server and blocks until it is closed.
func (s *Server) ListenAndServe() error {
	if !s.running.CompareAndSwap(false, true) {
		return errors.New("server already running")
	}

	// Start background tasks
	s.wg.Add(3)
	go s.keepAliveLoop()
	go s.willLoop()
	go s.qosRetryLoop()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return ErrServerClosed
			default:
				// Add backoff delay to prevent CPU burn on persistent errors
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

	// Close listener
	if s.listener != nil {
		s.listener.Close()
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

	// Handle retained messages
	if msg.Retain {
		if len(msg.Payload) == 0 {
			s.config.retainedStore.Delete(msg.Topic)
		} else {
			s.config.retainedStore.Set(&RetainedMessage{
				Topic:   msg.Topic,
				Payload: msg.Payload,
				QoS:     msg.QoS,
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

		// Create delivery message
		deliveryMsg := &Message{
			Topic:                   msg.Topic,
			Payload:                 msg.Payload,
			QoS:                     deliveryQoS,
			Retain:                  GetDeliveryRetain(entry.Subscription, msg.Retain),
			PayloadFormat:           msg.PayloadFormat,
			MessageExpiry:           msg.MessageExpiry,
			ContentType:             msg.ContentType,
			ResponseTopic:           msg.ResponseTopic,
			CorrelationData:         msg.CorrelationData,
			UserProperties:          msg.UserProperties,
			SubscriptionIdentifiers: msg.SubscriptionIdentifiers,
		}

		// Add subscription identifier if present
		if entry.Subscription.SubscriptionID > 0 {
			deliveryMsg.SubscriptionIdentifiers = append(
				deliveryMsg.SubscriptionIdentifiers,
				entry.Subscription.SubscriptionID,
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

// Addr returns the server's network address.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	// Read CONNECT packet with timeout
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	pkt, _, err := ReadPacket(conn, s.config.maxPacketSize)
	if err != nil {
		return
	}

	conn.SetReadDeadline(time.Time{})

	connect, ok := pkt.(*ConnectPacket)
	if !ok {
		return
	}

	// Generate client ID if empty
	clientID := connect.ClientID
	if clientID == "" {
		clientID = fmt.Sprintf("auto-%d", time.Now().UnixNano())
		connect.ClientID = clientID
	}

	// Authenticate
	if s.config.auth != nil {
		actx := &AuthContext{
			ClientID:      clientID,
			Username:      connect.Username,
			Password:      connect.Password,
			RemoteAddr:    conn.RemoteAddr(),
			ConnectPacket: connect,
			CleanStart:    connect.CleanStart,
		}

		result, err := s.config.auth.Authenticate(authCtx, actx)
		if err != nil || !result.Success {
			reasonCode := ReasonNotAuthorized
			if result != nil {
				reasonCode = result.ReasonCode
			}
			connack := &ConnackPacket{
				ReasonCode: reasonCode,
			}
			WritePacket(conn, connack, s.config.maxPacketSize)
			return
		}
	}

	// Create client
	client := NewServerClient(conn, connect, s.config.maxPacketSize)

	// Handle session
	var sessionPresent bool
	if connect.CleanStart {
		s.config.sessionStore.Delete(clientID)
		session := NewMemorySession(clientID)
		s.config.sessionStore.Create(session)
		client.SetSession(session)
	} else {
		existing, err := s.config.sessionStore.Get(clientID)
		if err == nil {
			sessionPresent = true
			client.SetSession(existing)
		} else {
			// Session doesn't exist or error - create a new one
			// Only claim sessionPresent if we successfully retrieved an existing session
			session := NewMemorySession(clientID)
			s.config.sessionStore.Create(session)
			client.SetSession(session)
		}
	}

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

	// Set server properties
	if s.config.keepAliveOverride > 0 {
		connack.Props.Set(PropServerKeepAlive, effectiveKeepAlive)
	}
	if s.config.topicAliasMax > 0 {
		connack.Props.Set(PropTopicAliasMaximum, s.config.topicAliasMax)
		client.SetTopicAliasMax(s.config.topicAliasMax, 0)
	}
	if s.config.receiveMaximum < 65535 {
		connack.Props.Set(PropReceiveMaximum, s.config.receiveMaximum)
	}

	if _, err := WritePacket(conn, connack, s.config.maxPacketSize); err != nil {
		s.removeClient(clientID)
		return
	}

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
			}
		}
	}

	// Handle packets
	s.clientLoop(client)
}

func (s *Server) clientLoop(client *ServerClient) {
	clientID := client.ClientID()
	conn := client.Conn()

	defer func() {
		// Disconnect callback
		if s.config.onDisconnect != nil {
			s.config.onDisconnect(client)
		}

		// Trigger will if not clean disconnect
		if client.IsConnected() {
			s.wills.TriggerWill(clientID, 0)
		} else {
			s.wills.Unregister(clientID)
		}

		s.removeClient(clientID)
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

		pkt, _, err := ReadPacket(conn, s.config.maxPacketSize)
		if err != nil {
			return
		}

		// Update keep-alive
		s.keepAlive.UpdateActivity(clientID)

		switch p := pkt.(type) {
		case *PublishPacket:
			s.handlePublish(client, p)

		case *PubackPacket:
			if _, ok := client.QoS1Tracker().Acknowledge(p.PacketID); ok {
				client.FlowControl().Release()
			}

		case *PubrecPacket:
			client.QoS2Tracker().HandlePubrec(p.PacketID)
			pubrel := &PubrelPacket{PacketID: p.PacketID}
			WritePacket(conn, pubrel, s.config.maxPacketSize)

		case *PubrelPacket:
			if _, ok := client.QoS2Tracker().HandlePubrel(p.PacketID); ok {
				pubcomp := &PubcompPacket{PacketID: p.PacketID}
				WritePacket(conn, pubcomp, s.config.maxPacketSize)
			}

		case *PubcompPacket:
			if _, ok := client.QoS2Tracker().HandlePubcomp(p.PacketID); ok {
				client.FlowControl().Release()
			}

		case *SubscribePacket:
			s.handleSubscribe(client, p)

		case *UnsubscribePacket:
			s.handleUnsubscribe(client, p)

		case *PingreqPacket:
			pingresp := &PingrespPacket{}
			WritePacket(conn, pingresp, s.config.maxPacketSize)

		case *DisconnectPacket:
			// Clean disconnect - don't send will
			client.Close()
			return
		}
	}
}

func (s *Server) handlePublish(client *ServerClient, pub *PublishPacket) {
	clientID := client.ClientID()

	// Resolve topic alias
	topic := pub.Topic
	if alias := pub.Props.GetUint16(PropTopicAlias); alias > 0 {
		if topic != "" {
			client.TopicAliases().SetInbound(alias, topic)
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

	// Authorization
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
		if err != nil || !result.Allowed {
			if pub.QoS > 0 {
				reasonCode := ReasonNotAuthorized
				if result != nil {
					reasonCode = result.ReasonCode
				}
				puback := &PubackPacket{
					PacketID:   pub.PacketID,
					ReasonCode: reasonCode,
				}
				WritePacket(client.Conn(), puback, s.config.maxPacketSize)
			}
			return
		}
	}

	// Send PUBACK for QoS 1
	if pub.QoS == 1 {
		puback := &PubackPacket{
			PacketID:   pub.PacketID,
			ReasonCode: ReasonSuccess,
		}
		WritePacket(client.Conn(), puback, s.config.maxPacketSize)
	}

	// Send PUBREC for QoS 2
	if pub.QoS == 2 {
		pubrec := &PubrecPacket{
			PacketID:   pub.PacketID,
			ReasonCode: ReasonSuccess,
		}
		WritePacket(client.Conn(), pubrec, s.config.maxPacketSize)
	}

	// Convert to Message
	msg := &Message{
		Topic:   topic,
		Payload: pub.Payload,
		QoS:     pub.QoS,
		Retain:  pub.Retain,
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

	// Callback
	if s.config.onMessage != nil {
		s.config.onMessage(client, msg)
	}

	// Publish to subscribers
	s.publishToSubscribers(clientID, msg)
}

func (s *Server) publishToSubscribers(publisherID string, msg *Message) {
	// Handle retained messages
	if msg.Retain {
		if len(msg.Payload) == 0 {
			s.config.retainedStore.Delete(msg.Topic)
		} else {
			s.config.retainedStore.Set(&RetainedMessage{
				Topic:   msg.Topic,
				Payload: msg.Payload,
				QoS:     msg.QoS,
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

		// Create delivery message
		deliveryMsg := &Message{
			Topic:           msg.Topic,
			Payload:         msg.Payload,
			QoS:             deliveryQoS,
			Retain:          GetDeliveryRetain(entry.Subscription, msg.Retain),
			PayloadFormat:   msg.PayloadFormat,
			MessageExpiry:   msg.MessageExpiry,
			ContentType:     msg.ContentType,
			ResponseTopic:   msg.ResponseTopic,
			CorrelationData: msg.CorrelationData,
			UserProperties:  msg.UserProperties,
		}

		// Add subscription identifier
		if entry.Subscription.SubscriptionID > 0 {
			deliveryMsg.SubscriptionIdentifiers = []uint32{entry.Subscription.SubscriptionID}
		}

		client.Send(deliveryMsg)
	}
}

func (s *Server) handleSubscribe(client *ServerClient, sub *SubscribePacket) {
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
			if err != nil || !result.Allowed {
				reasonCode := ReasonNotAuthorized
				if result != nil {
					reasonCode = result.ReasonCode
				}
				reasonCodes[i] = reasonCode
				continue
			}
		}

		// Check if this is a new subscription
		isNew := !s.subs.Unsubscribe(clientID, subscription.TopicFilter)

		// Add subscription
		s.subs.Subscribe(clientID, subscription)

		// Add to session
		if session != nil {
			session.AddSubscription(subscription)
		}

		reasonCodes[i] = ReasonCode(subscription.QoS)

		// Send retained messages
		if ShouldSendRetained(subscription.RetainHandling, isNew) {
			retained := s.config.retainedStore.Match(subscription.TopicFilter)
			for _, msg := range retained {
				deliveryQoS := msg.QoS
				if subscription.QoS < deliveryQoS {
					deliveryQoS = subscription.QoS
				}

				client.Send(&Message{
					Topic:   msg.Topic,
					Payload: msg.Payload,
					QoS:     deliveryQoS,
					Retain:  true,
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
	WritePacket(client.Conn(), suback, s.config.maxPacketSize)
}

func (s *Server) handleUnsubscribe(client *ServerClient, unsub *UnsubscribePacket) {
	clientID := client.ClientID()
	session := client.Session()

	reasonCodes := make([]ReasonCode, len(unsub.TopicFilters))

	for i, filter := range unsub.TopicFilters {
		if s.subs.Unsubscribe(clientID, filter) {
			reasonCodes[i] = ReasonSuccess
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
	WritePacket(client.Conn(), unsuback, s.config.maxPacketSize)
}

func (s *Server) removeClient(clientID string) {
	s.mu.Lock()
	delete(s.clients, clientID)
	s.mu.Unlock()

	s.keepAlive.Unregister(clientID)
	s.subs.UnsubscribeAll(clientID)
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
