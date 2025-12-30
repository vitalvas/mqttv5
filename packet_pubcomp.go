//nolint:dupl // MQTT v5.0 requires separate packet types with same structure
package mqttv5

import "io"

// PubcompPacket represents an MQTT PUBCOMP packet.
// MQTT v5.0 spec: Section 3.7
type PubcompPacket struct {
	PacketID   uint16
	ReasonCode ReasonCode
	Props      Properties
}

// Type returns the packet type.
func (p *PubcompPacket) Type() PacketType { return PacketPUBCOMP }

// Properties returns a pointer to the packet's properties.
func (p *PubcompPacket) Properties() *Properties { return &p.Props }

// GetPacketID returns the packet identifier.
func (p *PubcompPacket) GetPacketID() uint16 { return p.PacketID }

// SetPacketID sets the packet identifier.
func (p *PubcompPacket) SetPacketID(id uint16) { p.PacketID = id }

// Encode writes the packet to the writer.
func (p *PubcompPacket) Encode(w io.Writer) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	return encodeAck(w, PacketPUBCOMP, 0x00, &ackPacket{
		PacketID:   p.PacketID,
		ReasonCode: p.ReasonCode,
		Props:      p.Props,
	})
}

// Decode reads the packet from the reader.
func (p *PubcompPacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketPUBCOMP {
		return 0, ErrInvalidPacketType
	}
	var ack ackPacket
	n, err := decodeAck(r, header, &ack)
	p.PacketID = ack.PacketID
	p.ReasonCode = ack.ReasonCode
	p.Props = ack.Props
	return n, err
}

// Validate validates the packet contents.
func (p *PubcompPacket) Validate() error {
	if !p.ReasonCode.ValidForPUBCOMP() {
		return ErrInvalidReasonCode
	}
	return nil
}
