package mqttv5

import (
	"sync"
	"sync/atomic"
	"time"
)

// ServerClient represents a connected client on the server.
type ServerClient struct {
	mu            sync.RWMutex
	conn          Conn
	clientID      string
	username      string
	session       Session
	topicAliases  *TopicAliasManager
	qos1Tracker   *QoS1Tracker
	qos2Tracker   *QoS2Tracker
	flowControl   *FlowController
	properties    *ConnectPacket // original connect properties
	connected     atomic.Bool
	cleanStart    bool
	keepAlive     uint16
	maxPacketSize uint32
}

// NewServerClient creates a new server client.
func NewServerClient(conn Conn, connect *ConnectPacket, maxPacketSize uint32) *ServerClient {
	client := &ServerClient{
		conn:          conn,
		clientID:      connect.ClientID,
		username:      connect.Username,
		properties:    connect,
		cleanStart:    connect.CleanStart,
		keepAlive:     connect.KeepAlive,
		maxPacketSize: maxPacketSize,
		topicAliases:  NewTopicAliasManager(0, 0),
		qos1Tracker:   NewQoS1Tracker(20*time.Second, 3),
		qos2Tracker:   NewQoS2Tracker(20*time.Second, 3),
		flowControl:   NewFlowController(65535),
	}
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

// IsConnected returns whether the client is connected.
func (c *ServerClient) IsConnected() bool {
	return c.connected.Load()
}

// TopicAliases returns the topic alias manager.
func (c *ServerClient) TopicAliases() *TopicAliasManager {
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
	return c.flowControl
}

// SetReceiveMaximum sets the receive maximum from the client.
func (c *ServerClient) SetReceiveMaximum(maxVal uint16) {
	if maxVal == 0 {
		maxVal = 65535
	}
	c.flowControl = NewFlowController(maxVal)
}

// Send sends a message to the client.
func (c *ServerClient) Send(msg *Message) error {
	if !c.connected.Load() {
		return ErrNotConnected
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
	if msg.QoS > 0 {
		if c.session != nil {
			pub.PacketID = c.session.NextPacketID()
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
		pub.Props.Set(PropSubscriptionIdentifier, subID)
	}

	_, err := WritePacket(c.conn, pub, c.maxPacketSize)
	return err
}

// SendPacket sends a raw packet to the client.
func (c *ServerClient) SendPacket(packet Packet) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}

	_, err := WritePacket(c.conn, packet, c.maxPacketSize)
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
func (c *ServerClient) Disconnect(reason ReasonCode) error {
	if !c.connected.Load() {
		return ErrNotConnected
	}

	disconnect := &DisconnectPacket{
		ReasonCode: reason,
	}

	WritePacket(c.conn, disconnect, c.maxPacketSize)
	return c.Close()
}
