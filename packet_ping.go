package mqttv5

import "io"

// PingreqPacket represents an MQTT PINGREQ packet.
// MQTT v5.0 spec: Section 3.12
type PingreqPacket struct{}

// Type returns the packet type.
func (p *PingreqPacket) Type() PacketType { return PacketPINGREQ }

// Encode writes the packet to the writer.
func (p *PingreqPacket) Encode(w io.Writer) (int, error) {
	header := FixedHeader{
		PacketType:      PacketPINGREQ,
		Flags:           0x00,
		RemainingLength: 0,
	}
	return header.Encode(w)
}

// Decode reads the packet from the reader.
func (p *PingreqPacket) Decode(_ io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketPINGREQ {
		return 0, ErrInvalidPacketType
	}
	if header.Flags != 0x00 {
		return 0, ErrInvalidPacketFlags
	}
	if header.RemainingLength != 0 {
		return 0, ErrProtocolViolation
	}
	return 0, nil
}

// Validate validates the packet contents.
func (p *PingreqPacket) Validate() error {
	return nil
}

// PingrespPacket represents an MQTT PINGRESP packet.
// MQTT v5.0 spec: Section 3.13
type PingrespPacket struct{}

// Type returns the packet type.
func (p *PingrespPacket) Type() PacketType { return PacketPINGRESP }

// Encode writes the packet to the writer.
func (p *PingrespPacket) Encode(w io.Writer) (int, error) {
	header := FixedHeader{
		PacketType:      PacketPINGRESP,
		Flags:           0x00,
		RemainingLength: 0,
	}
	return header.Encode(w)
}

// Decode reads the packet from the reader.
func (p *PingrespPacket) Decode(_ io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketPINGRESP {
		return 0, ErrInvalidPacketType
	}
	if header.Flags != 0x00 {
		return 0, ErrInvalidPacketFlags
	}
	if header.RemainingLength != 0 {
		return 0, ErrProtocolViolation
	}
	return 0, nil
}

// Validate validates the packet contents.
func (p *PingrespPacket) Validate() error {
	return nil
}
