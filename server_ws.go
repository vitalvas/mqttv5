package mqttv5

import (
	"fmt"
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

	s.wg.Add(4)
	go s.keepAliveLoop()
	go s.willLoop()
	go s.qosRetryLoop()
	go s.sessionExpiryLoop()
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

	// Read CONNECT packet with timeout (matching TCP behavior)
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

	// Validate and handle client ID (matching TCP behavior)
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

	// Authenticate
	var authResult *AuthResult
	if s.config.auth != nil {
		actx := buildAuthContext(conn, connect, clientID)
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
		authResult = result
		logger.Debug("authentication successful", nil)

		// Apply AuthResult fields
		if result.AssignedClientID != "" {
			clientID = result.AssignedClientID
			assignedClientID = clientID
			connect.ClientID = clientID
			logger = logger.WithFields(LogFields{LogFieldClientID: clientID})
		}
	}

	// Determine max packet size for outbound messages (minimum of server and client limits)
	clientMaxPacketSize := s.config.maxPacketSize
	if clientLimit := connect.Props.GetUint32(PropMaximumPacketSize); clientLimit > 0 && clientLimit < clientMaxPacketSize {
		clientMaxPacketSize = clientLimit
	}

	// Create client using the constructor to ensure all fields are initialized
	client := NewServerClient(conn, connect, clientMaxPacketSize)

	// Apply CONNECT properties from client
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

	// Apply AuthResult fields to CONNACK (matching TCP behavior)
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

	// Restore and resend inflight QoS messages from session (matching TCP behavior)
	if sessionPresent {
		session := client.Session()
		if session != nil {
			s.restoreInflightMessages(client, session, logger)
		}
	}

	// Handle packets
	s.clientLoop(client, logger)
}
