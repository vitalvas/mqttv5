package mqttv5

import (
	"bytes"
	"errors"
	"io"
)

var (
	ErrInvalidPacketID             = errors.New("invalid packet identifier")
	ErrProtocolViolation           = errors.New("protocol violation")
	ErrInvalidSubscriptionID       = errors.New("invalid subscription identifier")
	maxSubscriptionIdentifierValue = uint32(268435455) // 0x0FFFFFFF per MQTT v5.0 spec
)

// Subscription represents a topic filter with subscription options.
// MQTT v5.0 spec: Section 3.8.3.1
type Subscription struct {
	TopicFilter     string
	QoS             byte
	NoLocal         bool
	RetainAsPublish bool
	RetainHandling  byte
	SubscriptionID  uint32 // Set from SUBSCRIBE properties, used in session state
}

// SubscribePacket represents an MQTT SUBSCRIBE packet.
// MQTT v5.0 spec: Section 3.8
type SubscribePacket struct {
	PacketID      uint16
	Props         Properties
	Subscriptions []Subscription
}

// Type returns the packet type.
func (p *SubscribePacket) Type() PacketType { return PacketSUBSCRIBE }

// Properties returns a pointer to the packet's properties.
func (p *SubscribePacket) Properties() *Properties { return &p.Props }

// GetPacketID returns the packet identifier.
func (p *SubscribePacket) GetPacketID() uint16 { return p.PacketID }

// SetPacketID sets the packet identifier.
func (p *SubscribePacket) SetPacketID(id uint16) { p.PacketID = id }

// Encode writes the packet to the writer.
func (p *SubscribePacket) Encode(w io.Writer) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}

	var buf bytes.Buffer

	// Packet Identifier
	_, err := buf.Write([]byte{byte(p.PacketID >> 8), byte(p.PacketID)})
	if err != nil {
		return 0, err
	}

	// Properties
	_, err = p.Props.Encode(&buf)
	if err != nil {
		return 0, err
	}

	// Payload: subscriptions
	for _, sub := range p.Subscriptions {
		// Topic Filter
		if _, err := encodeString(&buf, sub.TopicFilter); err != nil {
			return 0, err
		}

		// Subscription Options
		options := sub.QoS & 0x03
		if sub.NoLocal {
			options |= 0x04
		}
		if sub.RetainAsPublish {
			options |= 0x08
		}
		options |= (sub.RetainHandling & 0x03) << 4

		if err := buf.WriteByte(options); err != nil {
			return 0, err
		}
	}

	// Write fixed header
	header := FixedHeader{
		PacketType:      PacketSUBSCRIBE,
		Flags:           0x02, // SUBSCRIBE must have flags 0x02
		RemainingLength: uint32(buf.Len()),
	}

	total, err := header.Encode(w)
	if err != nil {
		return total, err
	}

	n, err := w.Write(buf.Bytes())
	return total + n, err
}

// Decode reads the packet from the reader.
func (p *SubscribePacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketSUBSCRIBE {
		return 0, ErrInvalidPacketType
	}
	if header.Flags != 0x02 {
		return 0, ErrInvalidPacketFlags
	}

	var totalRead int

	// Packet Identifier
	var idBuf [2]byte
	n, err := io.ReadFull(r, idBuf[:])
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	p.PacketID = uint16(idBuf[0])<<8 | uint16(idBuf[1])

	// Properties
	n, err = p.Props.Decode(r)
	totalRead += n
	if err != nil {
		return totalRead, err
	}

	// Validate subscription identifier if present (must be 1-268435455 per MQTT v5.0 spec)
	var subscriptionID uint32
	if p.Props.Has(PropSubscriptionIdentifier) {
		subscriptionID = p.Props.GetUint32(PropSubscriptionIdentifier)
		if subscriptionID == 0 || subscriptionID > maxSubscriptionIdentifierValue {
			return totalRead, ErrInvalidSubscriptionID
		}
	}

	// Payload: subscriptions
	p.Subscriptions = nil
	for totalRead < int(header.RemainingLength) {
		var sub Subscription

		// Topic Filter
		topicFilter, n, err := decodeString(r)
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		sub.TopicFilter = topicFilter

		// Subscription Options
		var optBuf [1]byte
		n, err = io.ReadFull(r, optBuf[:])
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		options := optBuf[0]

		sub.QoS = options & 0x03
		sub.NoLocal = (options & 0x04) != 0
		sub.RetainAsPublish = (options & 0x08) != 0
		sub.RetainHandling = (options >> 4) & 0x03

		// Attach subscription identifier from SUBSCRIBE properties
		sub.SubscriptionID = subscriptionID

		// Check reserved bits
		if (options & 0xC0) != 0 {
			return totalRead, ErrProtocolViolation
		}

		p.Subscriptions = append(p.Subscriptions, sub)
	}

	return totalRead, nil
}

// Validate validates the packet contents.
func (p *SubscribePacket) Validate() error {
	if p.PacketID == 0 {
		return ErrInvalidPacketID
	}
	if len(p.Subscriptions) == 0 {
		return ErrProtocolViolation
	}
	for _, sub := range p.Subscriptions {
		if sub.TopicFilter == "" {
			return ErrProtocolViolation
		}
		if sub.QoS > 2 {
			return ErrInvalidQoS
		}
		if sub.RetainHandling > 2 {
			return ErrProtocolViolation
		}
	}
	return nil
}
