package mqttv5

import (
	"net/http"
	"time"
)

// WSServer is an MQTT v5.0 broker server over WebSocket.
type WSServer struct {
	*Server
	handler *WSHandler
}

// NewWSServer creates a new WebSocket MQTT server.
func NewWSServer(opts ...ServerOption) *WSServer {
	srv := NewServer(opts...)
	ws := &WSServer{Server: srv}
	ws.handler = NewWSHandler(ws.handleWSConnection)
	return ws
}

// ServeHTTP implements http.Handler for WebSocket connections.
func (s *WSServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// Start starts the background tasks without blocking.
// Call this before using the server with an HTTP handler.
func (s *WSServer) Start() {
	if !s.running.CompareAndSwap(false, true) {
		return
	}

	s.wg.Add(3)
	go s.keepAliveLoop()
	go s.willLoop()
	go s.qosRetryLoop()
}

// handleWSConnection handles a new WebSocket MQTT connection.
func (s *WSServer) handleWSConnection(conn Conn) {
	if !s.running.Load() {
		conn.Close()
		return
	}

	// Check max connections
	if s.config.maxConnections > 0 {
		s.mu.RLock()
		count := len(s.clients)
		s.mu.RUnlock()

		if count >= s.config.maxConnections {
			conn.Close()
			return
		}
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.handleWSConn(conn)
	}()
}

// handleWSConn handles a WebSocket connection using the wrapped Conn interface.
func (s *WSServer) handleWSConn(conn Conn) {
	defer conn.Close()

	logger := s.config.logger.WithFields(LogFields{
		LogFieldRemoteAddr: conn.RemoteAddr().String(),
	})

	// Read CONNECT packet
	pkt, n, err := ReadPacket(conn, s.config.maxPacketSize)
	if err != nil {
		logger.Debug("failed to read CONNECT", LogFields{LogFieldError: err.Error()})
		return
	}
	s.config.metrics.BytesReceived(n)
	s.config.metrics.PacketReceived(PacketCONNECT)

	connect, ok := pkt.(*ConnectPacket)
	if !ok {
		logger.Warn("first packet not CONNECT", LogFields{
			LogFieldPacketType: pkt.Type().String(),
		})
		return
	}

	// Generate client ID if empty
	clientID := connect.ClientID
	if clientID == "" {
		clientID = generateClientID()
		connect.ClientID = clientID
	}

	logger = logger.WithFields(LogFields{LogFieldClientID: clientID})

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
			logger.Warn("authentication failed", LogFields{
				LogFieldReasonCode: reasonCode.String(),
			})
			connack := &ConnackPacket{
				ReasonCode: reasonCode,
			}
			WritePacket(conn, connack, s.config.maxPacketSize)
			return
		}
		logger.Debug("authentication successful", nil)
	}

	// Create client with Conn wrapper
	client := &ServerClient{
		conn:          conn,
		clientID:      clientID,
		username:      connect.Username,
		properties:    connect,
		cleanStart:    connect.CleanStart,
		keepAlive:     connect.KeepAlive,
		maxPacketSize: s.config.maxPacketSize,
		topicAliases:  NewTopicAliasManager(0, 0),
		qos1Tracker:   NewQoS1Tracker(20*time.Second, 3),
		qos2Tracker:   NewQoS2Tracker(20*time.Second, 3),
		flowControl:   NewFlowController(65535),
	}
	client.connected.Store(true)

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

	// Handle packets
	s.clientLoop(client, logger)
}
