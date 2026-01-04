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
	// Set WebSocket read limit to match MQTT max packet size
	ws.handler.MaxPacketSize = int64(srv.config.maxPacketSize)
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
			WritePacket(conn, connack, clientMaxPacketSize)
			return
		}
		// CleanStart=true with empty ClientID: assign one
		clientID = fmt.Sprintf("auto-%d", time.Now().UnixNano())
		assignedClientID = clientID
		connect.ClientID = clientID
	}

	logger = logger.WithFields(LogFields{LogFieldClientID: clientID})

	// Perform authentication (standard or enhanced) - same as TCP path
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

	// Create client using the constructor to ensure all fields are initialized
	client := NewServerClient(conn, connect, clientMaxPacketSize, namespace)

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

	// Restore and resend inflight QoS messages from session (matching TCP behavior)
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
