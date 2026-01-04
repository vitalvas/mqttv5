package mqttv5

import (
	"io"
	"time"
)

// QoS levels for MQTT message delivery.
// MQTT v5.0 spec: Section 4.3
const (
	// QoS0 (At most once) - Message is delivered at most once.
	// No acknowledgment or retransmission.
	QoS0 byte = 0

	// QoS1 (At least once) - Message is delivered at least once.
	// Acknowledged with PUBACK, may be retransmitted.
	QoS1 byte = 1

	// QoS2 (Exactly once) - Message is delivered exactly once.
	// Uses PUBREC/PUBREL/PUBCOMP handshake.
	QoS2 byte = 2
)

// Packet is the interface that all MQTT control packets implement.
// MQTT v5.0 spec: Section 2.1
type Packet interface {
	// Type returns the packet type.
	// MQTT v5.0 spec: Section 2.1.2
	Type() PacketType

	// Encode writes the packet to the writer.
	// Returns the number of bytes written.
	Encode(w io.Writer) (int, error)

	// Decode reads the packet from the reader.
	// The fixed header should already be decoded.
	// Returns the number of bytes read.
	Decode(r io.Reader, header FixedHeader) (int, error)

	// Validate validates the packet contents.
	Validate() error
}

// PacketWithID is implemented by packets that have a packet identifier.
// MQTT v5.0 spec: Section 2.2.1
type PacketWithID interface {
	Packet

	// PacketID returns the packet identifier.
	PacketID() uint16

	// SetPacketID sets the packet identifier.
	SetPacketID(id uint16)
}

// PacketWithProperties is implemented by packets that have properties.
// MQTT v5.0 spec: Section 2.2.2
type PacketWithProperties interface {
	Packet

	// Properties returns a pointer to the packet's properties.
	Properties() *Properties
}

// Message represents an MQTT application message.
// This is the user-facing struct with public fields for easy access.
type Message struct {
	// Topic is the topic name to publish to or received from.
	Topic string

	// Payload is the application message payload.
	Payload []byte

	// QoS is the Quality of Service level (0, 1, or 2).
	QoS byte

	// Retain indicates if this is a retained message.
	Retain bool

	// PayloadFormat indicates if the payload is UTF-8 encoded text (1) or unspecified bytes (0).
	PayloadFormat byte

	// MessageExpiry is the lifetime of the message in seconds.
	// Zero means no expiry.
	MessageExpiry uint32

	// PublishedAt is when the message was originally published.
	// Used to calculate remaining expiry on delivery.
	PublishedAt time.Time

	// ContentType is the MIME type of the payload.
	ContentType string

	// ResponseTopic is the topic for response messages.
	ResponseTopic string

	// CorrelationData is used to correlate request/response messages.
	CorrelationData []byte

	// UserProperties contains user-defined name-value pairs.
	UserProperties []StringPair

	// SubscriptionIdentifiers contains subscription identifiers from matching subscriptions.
	// Only set when receiving messages.
	SubscriptionIdentifiers []uint32

	// Namespace is the tenant namespace for multi-tenancy isolation.
	Namespace string
}

// Clone creates a deep copy of the message.
func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}

	clone := &Message{
		Topic:         m.Topic,
		QoS:           m.QoS,
		Retain:        m.Retain,
		PayloadFormat: m.PayloadFormat,
		MessageExpiry: m.MessageExpiry,
		PublishedAt:   m.PublishedAt,
		ContentType:   m.ContentType,
		ResponseTopic: m.ResponseTopic,
		Namespace:     m.Namespace,
	}

	if m.Payload != nil {
		clone.Payload = make([]byte, len(m.Payload))
		copy(clone.Payload, m.Payload)
	}

	if m.CorrelationData != nil {
		clone.CorrelationData = make([]byte, len(m.CorrelationData))
		copy(clone.CorrelationData, m.CorrelationData)
	}

	if m.UserProperties != nil {
		clone.UserProperties = make([]StringPair, len(m.UserProperties))
		copy(clone.UserProperties, m.UserProperties)
	}

	if m.SubscriptionIdentifiers != nil {
		clone.SubscriptionIdentifiers = make([]uint32, len(m.SubscriptionIdentifiers))
		copy(clone.SubscriptionIdentifiers, m.SubscriptionIdentifiers)
	}

	return clone
}

// IsExpired returns true if the message has expired based on MessageExpiry and PublishedAt.
func (m *Message) IsExpired() bool {
	if m.MessageExpiry == 0 {
		return false // No expiry set
	}
	if m.PublishedAt.IsZero() {
		return false // No publish time tracked
	}
	elapsed := time.Since(m.PublishedAt)
	return elapsed >= time.Duration(m.MessageExpiry)*time.Second
}

// RemainingExpiry returns the remaining expiry time in seconds.
// Returns 0 if expired or no expiry is set.
func (m *Message) RemainingExpiry() uint32 {
	if m.MessageExpiry == 0 {
		return 0 // No expiry set
	}
	if m.PublishedAt.IsZero() {
		return m.MessageExpiry // No publish time, return original
	}
	elapsed := time.Since(m.PublishedAt)
	expiryDuration := time.Duration(m.MessageExpiry) * time.Second
	if elapsed >= expiryDuration {
		return 0 // Expired
	}
	remaining := expiryDuration - elapsed
	return uint32(remaining.Seconds())
}

// ToProperties converts the message metadata to MQTT properties.
// This is used when encoding a PUBLISH packet.
func (m *Message) ToProperties() Properties {
	var p Properties

	if m.PayloadFormat != 0 {
		p.Set(PropPayloadFormatIndicator, m.PayloadFormat)
	}

	if m.MessageExpiry != 0 {
		p.Set(PropMessageExpiryInterval, m.MessageExpiry)
	}

	if m.ContentType != "" {
		p.Set(PropContentType, m.ContentType)
	}

	if m.ResponseTopic != "" {
		p.Set(PropResponseTopic, m.ResponseTopic)
	}

	if len(m.CorrelationData) > 0 {
		p.Set(PropCorrelationData, m.CorrelationData)
	}

	for _, up := range m.UserProperties {
		p.Add(PropUserProperty, up)
	}

	return p
}

// FromProperties populates the message metadata from MQTT properties.
// This is used when decoding a PUBLISH packet.
func (m *Message) FromProperties(p *Properties) {
	if p == nil {
		return
	}

	m.PayloadFormat = p.GetByte(PropPayloadFormatIndicator)
	m.MessageExpiry = p.GetUint32(PropMessageExpiryInterval)
	m.ContentType = p.GetString(PropContentType)
	m.ResponseTopic = p.GetString(PropResponseTopic)
	m.CorrelationData = p.GetBinary(PropCorrelationData)
	m.UserProperties = p.GetAllStringPairs(PropUserProperty)
	m.SubscriptionIdentifiers = p.GetAllVarInts(PropSubscriptionIdentifier)
}
