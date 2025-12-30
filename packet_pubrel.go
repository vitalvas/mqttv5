package mqttv5

import "io"

// PubrelPacket represents an MQTT PUBREL packet.
// MQTT v5.0 spec: Section 3.6
type PubrelPacket struct {
	PacketID   uint16
	ReasonCode ReasonCode
	Props      Properties
}

// Type returns the packet type.
func (p *PubrelPacket) Type() PacketType { return PacketPUBREL }

// Properties returns a pointer to the packet's properties.
func (p *PubrelPacket) Properties() *Properties { return &p.Props }

// GetPacketID returns the packet identifier.
func (p *PubrelPacket) GetPacketID() uint16 { return p.PacketID }

// SetPacketID sets the packet identifier.
func (p *PubrelPacket) SetPacketID(id uint16) { p.PacketID = id }

// Encode writes the packet to the writer.
func (p *PubrelPacket) Encode(w io.Writer) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	// PUBREL must have flags = 0x02
	return encodeAck(w, PacketPUBREL, 0x02, &ackPacket{
		PacketID:   p.PacketID,
		ReasonCode: p.ReasonCode,
		Props:      p.Props,
	})
}

// Decode reads the packet from the reader.
func (p *PubrelPacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketPUBREL {
		return 0, ErrInvalidPacketType
	}
	// Validate fixed header flags (must be 0x02)
	if header.Flags != 0x02 {
		return 0, ErrInvalidPacketFlags
	}
	var ack ackPacket
	n, err := decodeAck(r, header, &ack)
	p.PacketID = ack.PacketID
	p.ReasonCode = ack.ReasonCode
	p.Props = ack.Props
	return n, err
}

// Validate validates the packet contents.
func (p *PubrelPacket) Validate() error {
	if p.PacketID == 0 {
		return ErrInvalidPacketID
	}
	if !p.ReasonCode.ValidForPUBREL() {
		return ErrInvalidReasonCode
	}
	return nil
}
