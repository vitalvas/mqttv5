package mqttv5

import (
	"bytes"
	"errors"
	"io"
)

// PUBLISH packet errors.
var (
	ErrTopicNameEmpty   = errors.New("topic name cannot be empty")
	ErrInvalidQoS       = errors.New("invalid QoS level")
	ErrPacketIDRequired = errors.New("packet identifier required for QoS > 0")
)

// PublishPacket represents an MQTT PUBLISH packet.
// MQTT v5.0 spec: Section 3.3
type PublishPacket struct {
	// Topic is the topic name.
	Topic string

	// Payload is the application message.
	Payload []byte

	// QoS is the Quality of Service level (0, 1, or 2).
	QoS byte

	// Retain indicates if the message should be retained.
	Retain bool

	// DUP indicates if this is a retransmission.
	DUP bool

	// PacketID is the packet identifier (only for QoS > 0).
	PacketID uint16

	// Props contains the PUBLISH properties.
	Props Properties
}

// Type returns the packet type.
func (p *PublishPacket) Type() PacketType {
	return PacketPUBLISH
}

// Properties returns a pointer to the packet's properties.
func (p *PublishPacket) Properties() *Properties {
	return &p.Props
}

// GetPacketID returns the packet identifier.
func (p *PublishPacket) GetPacketID() uint16 {
	return p.PacketID
}

// SetPacketID sets the packet identifier.
func (p *PublishPacket) SetPacketID(id uint16) {
	p.PacketID = id
}

// flags returns the fixed header flags.
func (p *PublishPacket) flags() byte {
	var flags byte
	if p.DUP {
		flags |= 0x08
	}
	flags |= (p.QoS & 0x03) << 1
	if p.Retain {
		flags |= 0x01
	}
	return flags
}

// setFlags parses the fixed header flags.
func (p *PublishPacket) setFlags(flags byte) {
	p.DUP = flags&0x08 != 0
	p.QoS = (flags >> 1) & 0x03
	p.Retain = flags&0x01 != 0
}

// Encode writes the packet to the writer.
func (p *PublishPacket) Encode(w io.Writer) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}

	var buf bytes.Buffer

	// Topic Name
	n, err := encodeString(&buf, p.Topic)
	if err != nil {
		return 0, err
	}
	_ = n

	// Packet Identifier (only for QoS > 0)
	if p.QoS > 0 {
		_, err = buf.Write([]byte{byte(p.PacketID >> 8), byte(p.PacketID)})
		if err != nil {
			return 0, err
		}
	}

	// Properties
	_, err = p.Props.Encode(&buf)
	if err != nil {
		return 0, err
	}

	// Payload
	_, err = buf.Write(p.Payload)
	if err != nil {
		return 0, err
	}

	// Write fixed header
	header := FixedHeader{
		PacketType:      PacketPUBLISH,
		Flags:           p.flags(),
		RemainingLength: uint32(buf.Len()),
	}

	total, err := header.Encode(w)
	if err != nil {
		return total, err
	}

	// Write variable header and payload
	n2, err := w.Write(buf.Bytes())
	return total + n2, err
}

// Decode reads the packet from the reader.
func (p *PublishPacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketPUBLISH {
		return 0, ErrInvalidPacketType
	}

	p.setFlags(header.Flags)

	// Validate QoS
	if p.QoS > 2 {
		return 0, ErrInvalidQoS
	}

	var totalRead int

	// Topic Name
	var n int
	var err error
	p.Topic, n, err = decodeString(r)
	totalRead += n
	if err != nil {
		return totalRead, err
	}

	// Packet Identifier (only for QoS > 0)
	if p.QoS > 0 {
		var idBuf [2]byte
		n, err = io.ReadFull(r, idBuf[:])
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		p.PacketID = uint16(idBuf[0])<<8 | uint16(idBuf[1])
	}

	// Properties
	n, err = p.Props.Decode(r)
	totalRead += n
	if err != nil {
		return totalRead, err
	}

	// Payload - read remaining bytes
	payloadLen := int(header.RemainingLength) - totalRead
	if payloadLen > 0 {
		p.Payload = make([]byte, payloadLen)
		n, err = io.ReadFull(r, p.Payload)
		totalRead += n
		if err != nil {
			return totalRead, err
		}
	}

	return totalRead, nil
}

// Validate validates the packet contents.
func (p *PublishPacket) Validate() error {
	// QoS must be 0, 1, or 2
	if p.QoS > 2 {
		return ErrInvalidQoS
	}

	// DUP must be 0 for QoS 0
	if p.QoS == 0 && p.DUP {
		return ErrInvalidPacketFlags
	}

	// Packet ID is required for QoS > 0
	if p.QoS > 0 && p.PacketID == 0 {
		return ErrPacketIDRequired
	}

	return nil
}

// ToMessage converts the PUBLISH packet to a Message.
func (p *PublishPacket) ToMessage() *Message {
	m := &Message{
		Topic:   p.Topic,
		Payload: p.Payload,
		QoS:     p.QoS,
		Retain:  p.Retain,
	}
	m.FromProperties(&p.Props)
	return m
}

// FromMessage populates the PUBLISH packet from a Message.
func (p *PublishPacket) FromMessage(m *Message) {
	p.Topic = m.Topic
	p.Payload = m.Payload
	p.QoS = m.QoS
	p.Retain = m.Retain
	p.Props = m.ToProperties()
}
