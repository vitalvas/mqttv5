package mqttv5

import (
	"crypto/tls"
	"sync"
	"sync/atomic"
	"time"
)

// ServerClient represents a connected client on the server.
type ServerClient struct {
	mu                    sync.RWMutex
	writeMu               sync.Mutex // protects concurrent writes to conn
	conn                  Conn
	codec                 *codec
	protocolVersion       ProtocolVersion
	clientID              string
	username              string
	namespace             string
	session               Session
	topicAliases          *TopicAliasManager
	qos1Tracker           *QoS1Tracker
	qos2Tracker           *QoS2Tracker
	flowControl           *FlowController // outbound flow control (server → client)
	inboundFlowControl    *FlowController // inbound flow control (client → server)
	properties            *ConnectPacket  // original connect properties
	connected             atomic.Bool
	cleanDisconnect       atomic.Bool // true if DISCONNECT packet was received
	cleanStart            bool
	keepAlive             uint16
	maxPacketSize         uint32
	sessionExpiryInterval uint32    // session expiry interval in seconds (from CONNECT or DISCONNECT)
	credentialExpiry      time.Time // when credentials (cert/token) expire, zero means no expiry
	tlsConnectionState    *tls.ConnectionState
	tlsIdentity           *TLSIdentity

	// writeTimeout bounds how long a single packet write may take. Zero
	// disables the deadline (writes block until the kernel completes or
	// the connection is closed). Non-zero values protect the server from
	// slow or stuck consumers that would otherwise hold writeMu indefinitely
	// and cascade into the publish/accept paths.
	writeTimeout time.Duration

	// Per-client stats
	connectedAt  time.Time
	messagesIn   atomic.Int64
	messagesOut  atomic.Int64
	bytesIn      atomic.Int64
	bytesOut     atomic.Int64
	lastActivity atomic.Int64 // unix nano timestamp
}

// NewServerClient creates a new server client.
func NewServerClient(conn Conn, connect *ConnectPacket, maxPacketSize uint32, namespace string) *ServerClient {
	version := connect.ProtocolVersion
	if version == 0 {
		version = ProtocolV5
	}

	now := time.Now()
	client := &ServerClient{
		conn:               conn,
		codec:              newCodec(version),
		protocolVersion:    version,
		clientID:           connect.ClientID,
		username:           connect.Username,
		namespace:          namespace,
		properties:         connect,
		cleanStart:         connect.CleanStart,
		keepAlive:          connect.KeepAlive,
		maxPacketSize:      maxPacketSize,
		topicAliases:       NewTopicAliasManager(0, 0),
		qos1Tracker:        NewQoS1Tracker(20*time.Second, 3),
		qos2Tracker:        NewQoS2Tracker(20*time.Second, 3),
		flowControl:        NewFlowController(65535),
		inboundFlowControl: NewFlowController(65535),
		connectedAt:        now,
	}
	client.lastActivity.Store(now.UnixNano())
	client.connected.Store(true)
	return client
}

// Conn returns the underlying connection.
func (c *ServerClient) Conn() Conn {
	return c.conn
}

// ClientID returns the client identifier.
func (c *ServerClient) ClientID() string {
	return c.clientID
}

// Username returns the username if provided during connect.
func (c *ServerClient) Username() string {
	return c.username
}

// Namespace returns the namespace for multi-tenancy isolation.
func (c *ServerClient) Namespace() string {
	return c.namespace
}

// ProtocolVersion returns the MQTT protocol version of this client.
func (c *ServerClient) ProtocolVersion() ProtocolVersion {
	return c.protocolVersion
}

// getCodec returns the client's codec, defaulting to v5 if not set.
func (c *ServerClient) getCodec() *codec {
	if c.codec != nil {
		return c.codec
	}
	return newCodec(ProtocolV5)
}

// ReadPacket reads a packet from the client using the version-aware codec.
func (c *ServerClient) ReadPacket(maxPacketSize uint32) (Packet, int, error) {
	return c.getCodec().readPacket(c.conn, maxPacketSize)
}

// SetWriteTimeout configures the per-packet write deadline applied in
// writePacketWithMetrics. Zero disables the deadline.
func (c *ServerClient) SetWriteTimeout(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeTimeout = d
}

// writePacketWithMetrics sends a packet using the version-aware codec and returns
// the number of bytes written. Used internally by the server for metrics tracking.
//
// When a non-zero write timeout is configured, SetWriteDeadline is applied
// before the write and cleared afterwards so that a stuck consumer cannot
// hold writeMu indefinitely and cascade into the publish/accept paths.
func (c *ServerClient) writePacketWithMetrics(packet Packet) (int, error) {
	c.mu.RLock()
	timeout := c.writeTimeout
	c.mu.RUnlock()

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if timeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(timeout))
		defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()
	}

	return c.getCodec().writePacket(c.conn, packet, c.maxPacketSize)
}

// Session returns the client's session.
func (c *ServerClient) Session() Session {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.session
}

// SetSession sets the client's session.
func (c *ServerClient) SetSession(session Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.session = session
}

// CleanStart returns whether clean start was requested.
func (c *ServerClient) CleanStart() bool {
	return c.cleanStart
}

// KeepAlive returns the keep-alive interval in seconds.
func (c *ServerClient) KeepAlive() uint16 {
	return c.keepAlive
}

// MaxPacketSize returns the negotiated maximum packet size for this client.
func (c *ServerClient) MaxPacketSize() uint32 {
	return c.maxPacketSize
}

// IsConnected returns whether the client is connected.
func (c *ServerClient) IsConnected() bool {
	return c.connected.Load()
}

// SetCleanDisconnect marks this as a clean disconnect (DISCONNECT packet received).
func (c *ServerClient) SetCleanDisconnect() {
	c.cleanDisconnect.Store(true)
}

// SessionExpiryInterval returns the session expiry interval in seconds.
func (c *ServerClient) SessionExpiryInterval() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessionExpiryInterval
}

// SetSessionExpiryInterval sets the session expiry interval in seconds.
func (c *ServerClient) SetSessionExpiryInterval(interval uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionExpiryInterval = interval
}

// CredentialExpiry returns when the client's credentials expire.
// Returns zero time if no credential expiry is set.
func (c *ServerClient) CredentialExpiry() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.credentialExpiry
}

// SetCredentialExpiry sets when the client's credentials expire.
// The server will disconnect the client when this time is reached.
// Use zero time to disable credential expiry.
func (c *ServerClient) SetCredentialExpiry(expiry time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.credentialExpiry = expiry
}

// IsCredentialExpired returns true if the client's credentials have expired.
func (c *ServerClient) IsCredentialExpired() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.credentialExpiry.IsZero() {
		return false
	}
	return time.Now().After(c.credentialExpiry)
}

// TLSConnectionState returns the TLS connection state.
// Returns nil for non-TLS connections.
func (c *ServerClient) TLSConnectionState() *tls.ConnectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tlsConnectionState
}

// SetTLSConnectionState sets the TLS connection state.
func (c *ServerClient) SetTLSConnectionState(state *tls.ConnectionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tlsConnectionState = state
}

// TLSIdentity returns the TLS identity mapped from the certificate.
// Returns nil if no identity mapper is configured or no identity was mapped.
func (c *ServerClient) TLSIdentity() *TLSIdentity {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tlsIdentity
}

// SetTLSIdentity sets the TLS identity.
func (c *ServerClient) SetTLSIdentity(identity *TLSIdentity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tlsIdentity = identity
}

// IsCleanDisconnect returns true if the client sent a DISCONNECT packet.
func (c *ServerClient) IsCleanDisconnect() bool {
	return c.cleanDisconnect.Load()
}

// TopicAliases returns the topic alias manager.
func (c *ServerClient) TopicAliases() *TopicAliasManager {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.topicAliases
}

// SetTopicAliasMax sets the topic alias maximum values.
func (c *ServerClient) SetTopicAliasMax(inbound, outbound uint16) {
	c.topicAliases.SetInboundMax(inbound)
	c.topicAliases.SetOutboundMax(outbound)
}

// QoS1Tracker returns the QoS 1 message tracker.
func (c *ServerClient) QoS1Tracker() *QoS1Tracker {
	return c.qos1Tracker
}

// QoS2Tracker returns the QoS 2 message tracker.
func (c *ServerClient) QoS2Tracker() *QoS2Tracker {
	return c.qos2Tracker
}

// FlowControl returns the flow controller.
func (c *ServerClient) FlowControl() *FlowController {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.flowControl
}

// SetReceiveMaximum sets the receive maximum from the client (outbound flow control).
func (c *ServerClient) SetReceiveMaximum(maxVal uint16) {
	if maxVal == 0 {
		maxVal = 65535
	}
	c.mu.Lock()
	c.flowControl = NewFlowController(maxVal)
	c.mu.Unlock()
}

// SetInboundReceiveMaximum sets the server's receive maximum (inbound flow control).
func (c *ServerClient) SetInboundReceiveMaximum(maxVal uint16) {
	if maxVal == 0 {
		maxVal = 65535
	}
	c.mu.Lock()
	c.inboundFlowControl = NewFlowController(maxVal)
	c.mu.Unlock()
}

// InboundFlowControl returns the inbound flow controller.
func (c *ServerClient) InboundFlowControl() *FlowController {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inboundFlowControl
}

// Send sends a message to the client.
func (c *ServerClient) Send(msg *Message) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}

	// For QoS > 0, check flow control before sending
	if msg.QoS > QoS0 {
		if !c.flowControl.TryAcquire() {
			return ErrQuotaExceeded
		}
	}

	pub := &PublishPacket{
		Topic:   msg.Topic,
		Payload: msg.Payload,
		QoS:     msg.QoS,
		Retain:  msg.Retain,
	}

	// Handle topic alias for outbound
	if alias := c.topicAliases.GetOutbound(msg.Topic); alias > 0 {
		pub.Props.Set(PropTopicAlias, alias)
	}

	// Assign packet ID for QoS > 0
	if msg.QoS > QoS0 {
		if c.session != nil {
			pub.PacketID = c.session.NextPacketID()
			if pub.PacketID == 0 {
				// All packet IDs exhausted - release flow control and return error
				c.flowControl.Release()
				return ErrPacketIDExhausted
			}
		}
		// Track for acknowledgment
		switch msg.QoS {
		case QoS1:
			qos1Msg := &QoS1Message{
				PacketID:     pub.PacketID,
				Message:      msg,
				State:        QoS1AwaitingPuback,
				SentAt:       time.Now(),
				RetryTimeout: c.qos1Tracker.RetryTimeout(),
			}
			c.qos1Tracker.Track(pub.PacketID, msg)
			// Persist to session for recovery on reconnect. If the session
			// queue is full the in-memory tracker still holds the record,
			// so online delivery keeps working; only resume-after-reconnect
			// loses this message.
			if c.session != nil {
				_ = c.session.AddInflightQoS1(pub.PacketID, qos1Msg)
			}
		case QoS2:
			qos2Msg := &QoS2Message{
				PacketID:     pub.PacketID,
				Message:      msg,
				State:        QoS2AwaitingPubrec,
				SentAt:       time.Now(),
				RetryTimeout: c.qos2Tracker.RetryTimeout(),
				IsSender:     true,
			}
			c.qos2Tracker.TrackSend(pub.PacketID, msg)
			// Persist to session for recovery on reconnect. Same policy as
			// QoS 1 above: online delivery is unaffected by a full queue.
			if c.session != nil {
				_ = c.session.AddInflightQoS2(pub.PacketID, qos2Msg)
			}
		}
	}

	// Copy message properties
	if msg.PayloadFormat > 0 {
		pub.Props.Set(PropPayloadFormatIndicator, msg.PayloadFormat)
	}
	if msg.MessageExpiry > 0 {
		pub.Props.Set(PropMessageExpiryInterval, msg.MessageExpiry)
	}
	if msg.ContentType != "" {
		pub.Props.Set(PropContentType, msg.ContentType)
	}
	if msg.ResponseTopic != "" {
		pub.Props.Set(PropResponseTopic, msg.ResponseTopic)
	}
	if len(msg.CorrelationData) > 0 {
		pub.Props.Set(PropCorrelationData, msg.CorrelationData)
	}
	for _, up := range msg.UserProperties {
		pub.Props.Add(PropUserProperty, up)
	}
	for _, subID := range msg.SubscriptionIdentifiers {
		pub.Props.Add(PropSubscriptionIdentifier, subID)
	}

	c.writeMu.Lock()
	n, err := c.getCodec().writePacket(c.conn, pub, c.maxPacketSize)
	c.writeMu.Unlock()

	if err == nil {
		c.recordBytesOut(n)
		c.recordMessageOut()
	}

	if err != nil && msg.QoS > QoS0 {
		// Rollback: release flow control quota and remove tracker entry
		c.flowControl.Release()
		switch msg.QoS {
		case QoS1:
			c.qos1Tracker.Remove(pub.PacketID)
		case QoS2:
			// Use Remove instead of HandlePubcomp since message is in QoS2AwaitingPubrec state
			c.qos2Tracker.Remove(pub.PacketID)
		}
		// Also remove from session persistence
		if c.session != nil {
			c.session.RemoveInflightQoS1(pub.PacketID)
			c.session.RemoveInflightQoS2(pub.PacketID)
		}
	}
	return err
}

// SendPacket sends a raw packet to the client.
func (c *ServerClient) SendPacket(packet Packet) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}

	c.writeMu.Lock()
	_, err := c.getCodec().writePacket(c.conn, packet, c.maxPacketSize)
	c.writeMu.Unlock()

	return err
}

// Close closes the client connection.
func (c *ServerClient) Close() error {
	if !c.connected.CompareAndSwap(true, false) {
		return nil
	}
	return c.conn.Close()
}

// Disconnect sends a DISCONNECT packet and closes the connection.
// When the server sends a DISCONNECT packet (for any reason), the Will message
// is NOT published because it's a controlled termination. The client is being
// properly notified, so Will (meant for unexpected disconnections) doesn't apply.
//
// MQTT 3.1.1 does not define a server-to-client DISCONNECT packet, so for
// v3.1.1 clients the connection is closed without writing a packet.
func (c *ServerClient) Disconnect(reason ReasonCode) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}

	// Mark as clean disconnect - server is explicitly sending DISCONNECT,
	// which is a controlled termination. Will messages are for unexpected
	// disconnections where the client can't notify others.
	c.cleanDisconnect.Store(true)

	if c.protocolVersion != ProtocolV311 {
		disconnect := &DisconnectPacket{
			ReasonCode: reason,
		}

		c.mu.RLock()
		timeout := c.writeTimeout
		c.mu.RUnlock()

		c.writeMu.Lock()
		if timeout > 0 {
			_ = c.conn.SetWriteDeadline(time.Now().Add(timeout))
		}
		_, _ = c.getCodec().writePacket(c.conn, disconnect, c.maxPacketSize)
		if timeout > 0 {
			_ = c.conn.SetWriteDeadline(time.Time{})
		}
		c.writeMu.Unlock()
	}

	return c.Close()
}

// recordBytesIn records bytes received from this client.
func (c *ServerClient) recordBytesIn(n int) {
	c.bytesIn.Add(int64(n))
	c.lastActivity.Store(time.Now().UnixNano())
}

// recordBytesOut records bytes sent to this client.
func (c *ServerClient) recordBytesOut(n int) {
	c.bytesOut.Add(int64(n))
}

// recordMessageIn records a message received from this client.
func (c *ServerClient) recordMessageIn() {
	c.messagesIn.Add(1)
}

// recordMessageOut records a message sent to this client.
func (c *ServerClient) recordMessageOut() {
	c.messagesOut.Add(1)
}

// ConnectedAt returns when the client connected.
func (c *ServerClient) ConnectedAt() time.Time {
	return c.connectedAt
}

// Uptime returns how long the client has been connected.
func (c *ServerClient) Uptime() time.Duration {
	return time.Since(c.connectedAt)
}

// LastActivity returns the time of last activity from this client.
func (c *ServerClient) LastActivity() time.Time {
	return time.Unix(0, c.lastActivity.Load())
}

// IdleDuration returns how long the client has been idle.
func (c *ServerClient) IdleDuration() time.Duration {
	return time.Since(c.LastActivity())
}

// MessagesIn returns the total messages received from this client.
func (c *ServerClient) MessagesIn() int64 {
	return c.messagesIn.Load()
}

// MessagesOut returns the total messages sent to this client.
func (c *ServerClient) MessagesOut() int64 {
	return c.messagesOut.Load()
}

// BytesIn returns the total bytes received from this client.
func (c *ServerClient) BytesIn() int64 {
	return c.bytesIn.Load()
}

// BytesOut returns the total bytes sent to this client.
func (c *ServerClient) BytesOut() int64 {
	return c.bytesOut.Load()
}
