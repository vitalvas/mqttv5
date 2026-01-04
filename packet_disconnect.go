//nolint:dupl // MQTT v5.0 requires separate packet types with same structure
package mqttv5

import (
	"bytes"
	"io"
)

// DisconnectPacket represents an MQTT DISCONNECT packet.
// MQTT v5.0 spec: Section 3.14
type DisconnectPacket struct {
	ReasonCode ReasonCode
	Props      Properties
}

// Type returns the packet type.
func (p *DisconnectPacket) Type() PacketType { return PacketDISCONNECT }

// Properties returns a pointer to the packet's properties.
func (p *DisconnectPacket) Properties() *Properties { return &p.Props }

// Encode writes the packet to the writer.
func (p *DisconnectPacket) Encode(w io.Writer) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	if err := p.Props.ValidateFor(PropCtxDISCONNECT); err != nil {
		return 0, err
	}

	var buf bytes.Buffer

	// Reason Code and Properties (optional if success with no properties)
	if p.ReasonCode != ReasonSuccess || p.Props.Len() > 0 {
		if err := buf.WriteByte(byte(p.ReasonCode)); err != nil {
			return 0, err
		}

		if p.Props.Len() > 0 {
			if _, err := p.Props.Encode(&buf); err != nil {
				return 0, err
			}
		}
	}

	// Write fixed header
	header := FixedHeader{
		PacketType:      PacketDISCONNECT,
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
func (p *DisconnectPacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketDISCONNECT {
		return 0, ErrInvalidPacketType
	}
	if header.Flags != 0x00 {
		return 0, ErrInvalidPacketFlags
	}

	var totalRead int

	// Reason Code (optional)
	if header.RemainingLength > 0 {
		var reasonBuf [1]byte
		n, err := io.ReadFull(r, reasonBuf[:])
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		p.ReasonCode = ReasonCode(reasonBuf[0])

		// Properties (optional)
		if header.RemainingLength > 1 {
			n, err = p.Props.Decode(r)
			totalRead += n
			if err != nil {
				return totalRead, err
			}
			if err := p.Props.ValidateFor(PropCtxDISCONNECT); err != nil {
				return totalRead, err
			}
		}
	} else {
		p.ReasonCode = ReasonSuccess
	}

	return totalRead, nil
}

// Validate validates the packet contents.
func (p *DisconnectPacket) Validate() error {
	if !p.ReasonCode.ValidForDISCONNECT() {
		return ErrInvalidReasonCode
	}
	return nil
}
