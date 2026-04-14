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

	pkt, n, err := readPacketV5(conn, s.config.maxPacketSize)
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

	// Validate protocol version and CONNECT properties
	wsConnCodec := connectCodec(connect)
	clientMaxPacketSize, reason, ok := s.validateConnectVersion(connect)
	if !ok {
		logger.Warn("connect validation failed", LogFields{
			LogFieldReasonCode: reason.String(),
		})
		wsConnCodec.writePacket(conn, &ConnackPacket{ReasonCode: reason}, s.config.maxPacketSize)
		s.fireConnectFailed(&ConnectFailedContext{
			ClientID:           connect.ClientID,
			Username:           connect.Username,
			RemoteAddr:         conn.RemoteAddr(),
			LocalAddr:          conn.LocalAddr(),
			TLSConnectionState: getTLSConnectionState(conn),
			ReasonCode:         reason,
		})
		return
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
			wsConnCodec.writePacket(conn, connack, clientMaxPacketSize)
			s.fireConnectFailed(&ConnectFailedContext{
				ClientID:           connect.ClientID,
				Username:           connect.Username,
				RemoteAddr:         conn.RemoteAddr(),
				LocalAddr:          conn.LocalAddr(),
				TLSConnectionState: getTLSConnectionState(conn),
				ReasonCode:         ReasonServerBusy,
			})
			return
		}
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
			wsConnCodec.writePacket(conn, connack, clientMaxPacketSize)
			s.fireConnectFailed(&ConnectFailedContext{
				Username:           connect.Username,
				RemoteAddr:         conn.RemoteAddr(),
				LocalAddr:          conn.LocalAddr(),
				TLSConnectionState: getTLSConnectionState(conn),
				ReasonCode:         ReasonClientIDNotValid,
			})
			return
		}
		// CleanStart=true with empty ClientID: assign one
		clientID = generateServerClientID()
		assignedClientID = clientID
		connect.ClientID = clientID
	}

	logger = logger.WithFields(LogFields{LogFieldClientID: clientID})

	// Perform authentication (standard or enhanced) - same as TCP path
	authRes := s.authenticateClient(conn, connect, clientID, clientMaxPacketSize, logger)
	if !authRes.ok {
		s.fireConnectFailed(&ConnectFailedContext{
			ClientID:           clientID,
			Username:           connect.Username,
			RemoteAddr:         conn.RemoteAddr(),
			LocalAddr:          conn.LocalAddr(),
			TLSConnectionState: getTLSConnectionState(conn),
			ReasonCode:         authRes.reasonCode,
		})
		return
	}
	authResult := authRes.authResult
	namespace := authRes.namespace
	if authRes.assignedClientID != "" && authRes.assignedClientID != clientID {
		clientID = authRes.assignedClientID
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
		wsConnCodec.writePacket(conn, connack, clientMaxPacketSize)
		s.fireConnectFailed(&ConnectFailedContext{
			ClientID:           clientID,
			Username:           connect.Username,
			RemoteAddr:         conn.RemoteAddr(),
			LocalAddr:          conn.LocalAddr(),
			TLSConnectionState: getTLSConnectionState(conn),
			ReasonCode:         ReasonNotAuthorized,
		})
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

	// Set credential expiry from authentication result (for cert/token-based expiry)
	s.applyCredentialExpiry(client, authResult)

	// Store TLS connection state and identity on client for authorization
	client.SetTLSConnectionState(authRes.tlsConnectionState)
	client.SetTLSIdentity(authRes.tlsIdentity)

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
	if _, err := wsConnCodec.writePacket(conn, connack, clientMaxPacketSize); err != nil {
		s.removeClient(clientKey, client)
		return
	}

	// Metrics and logging
	s.config.metrics.ConnectionOpened()
	logger.Info("client connected", nil)

	// Callbacks
	for _, fn := range s.config.onConnect {
		fn(client)
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
