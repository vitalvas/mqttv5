//nolint:dupl // MQTT v5.0 requires separate packet types with same structure
package mqttv5

import "io"

// PubrecPacket represents an MQTT PUBREC packet.
// MQTT v5.0 spec: Section 3.5
type PubrecPacket struct {
	PacketID   uint16
	ReasonCode ReasonCode
	Props      Properties
}

// Type returns the packet type.
func (p *PubrecPacket) Type() PacketType { return PacketPUBREC }

// Properties returns a pointer to the packet's properties.
func (p *PubrecPacket) Properties() *Properties { return &p.Props }

// GetPacketID returns the packet identifier.
func (p *PubrecPacket) GetPacketID() uint16 { return p.PacketID }

// SetPacketID sets the packet identifier.
func (p *PubrecPacket) SetPacketID(id uint16) { p.PacketID = id }

// Encode writes the packet to the writer.
func (p *PubrecPacket) Encode(w io.Writer) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	if err := p.Props.ValidateFor(PropCtxPUBREC); err != nil {
		return 0, err
	}
	return encodeAck(w, PacketPUBREC, 0x00, &ackPacket{
		PacketID:   p.PacketID,
		ReasonCode: p.ReasonCode,
		Props:      p.Props,
	})
}

// Decode reads the packet from the reader.
func (p *PubrecPacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketPUBREC {
		return 0, ErrInvalidPacketType
	}
	var ack ackPacket
	n, err := decodeAck(r, header, &ack, PropCtxPUBREC)
	p.PacketID = ack.PacketID
	p.ReasonCode = ack.ReasonCode
	p.Props = ack.Props
	return n, err
}

// Validate validates the packet contents.
func (p *PubrecPacket) Validate() error {
	if p.PacketID == 0 {
		return ErrInvalidPacketID
	}
	if !p.ReasonCode.ValidForPUBREC() {
		return ErrInvalidReasonCode
	}
	return nil
}
