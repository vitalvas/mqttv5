//nolint:dupl // MQTT v5.0 requires separate packet types with same structure
package mqttv5

import (
	"bytes"
	"io"
)

// SubackPacket represents an MQTT SUBACK packet.
// MQTT v5.0 spec: Section 3.9
type SubackPacket struct {
	PacketID    uint16
	Props       Properties
	ReasonCodes []ReasonCode
}

// Type returns the packet type.
func (p *SubackPacket) Type() PacketType { return PacketSUBACK }

// Properties returns a pointer to the packet's properties.
func (p *SubackPacket) Properties() *Properties { return &p.Props }

// GetPacketID returns the packet identifier.
func (p *SubackPacket) GetPacketID() uint16 { return p.PacketID }

// SetPacketID sets the packet identifier.
func (p *SubackPacket) SetPacketID(id uint16) { p.PacketID = id }

// Encode writes the packet to the writer.
func (p *SubackPacket) Encode(w io.Writer) (int, error) {
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

	// Payload: reason codes
	for _, rc := range p.ReasonCodes {
		if err := buf.WriteByte(byte(rc)); err != nil {
			return 0, err
		}
	}

	// Write fixed header
	header := FixedHeader{
		PacketType:      PacketSUBACK,
		Flags:           0x00,
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
func (p *SubackPacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketSUBACK {
		return 0, ErrInvalidPacketType
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

	// Payload: reason codes
	p.ReasonCodes = nil
	for totalRead < int(header.RemainingLength) {
		var rcBuf [1]byte
		n, err = io.ReadFull(r, rcBuf[:])
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		p.ReasonCodes = append(p.ReasonCodes, ReasonCode(rcBuf[0]))
	}

	return totalRead, nil
}

// Validate validates the packet contents.
func (p *SubackPacket) Validate() error {
	if p.PacketID == 0 {
		return ErrInvalidPacketID
	}
	if len(p.ReasonCodes) == 0 {
		return ErrProtocolViolation
	}
	for _, rc := range p.ReasonCodes {
		if !rc.ValidForSUBACK() {
			return ErrInvalidReasonCode
		}
	}
	return nil
}
