//nolint:dupl // MQTT v5.0 requires separate packet types with same structure
package mqttv5

import "io"

// PubackPacket represents an MQTT PUBACK packet.
// MQTT v5.0 spec: Section 3.4
type PubackPacket struct {
	PacketID   uint16
	ReasonCode ReasonCode
	Props      Properties
}

// Type returns the packet type.
func (p *PubackPacket) Type() PacketType { return PacketPUBACK }

// Properties returns a pointer to the packet's properties.
func (p *PubackPacket) Properties() *Properties { return &p.Props }

// GetPacketID returns the packet identifier.
func (p *PubackPacket) GetPacketID() uint16 { return p.PacketID }

// SetPacketID sets the packet identifier.
func (p *PubackPacket) SetPacketID(id uint16) { p.PacketID = id }

// Encode writes the packet to the writer.
func (p *PubackPacket) Encode(w io.Writer) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	return encodeAck(w, PacketPUBACK, 0x00, &ackPacket{
		PacketID:   p.PacketID,
		ReasonCode: p.ReasonCode,
		Props:      p.Props,
	})
}

// Decode reads the packet from the reader.
func (p *PubackPacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketPUBACK {
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
func (p *PubackPacket) Validate() error {
	if !p.ReasonCode.ValidForPUBACK() {
		return ErrInvalidReasonCode
	}
	return nil
}
