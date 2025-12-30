package mqttv5

import (
	"bytes"
	"errors"
	"io"
)

// CONNACK packet errors.
var (
	ErrInvalidConnackFlags = errors.New("invalid CONNACK flags")
)

// ConnackPacket represents an MQTT CONNACK packet.
// MQTT v5.0 spec: Section 3.2
type ConnackPacket struct {
	// SessionPresent indicates if a session exists from a previous connection.
	SessionPresent bool

	// ReasonCode is the connection result reason code.
	ReasonCode ReasonCode

	// Props contains the CONNACK properties.
	Props Properties
}

// Type returns the packet type.
func (p *ConnackPacket) Type() PacketType {
	return PacketCONNACK
}

// Properties returns a pointer to the packet's properties.
func (p *ConnackPacket) Properties() *Properties {
	return &p.Props
}

// Encode writes the packet to the writer.
func (p *ConnackPacket) Encode(w io.Writer) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}

	var buf bytes.Buffer

	// Connect Acknowledge Flags
	var flags byte
	if p.SessionPresent {
		flags = 0x01
	}
	if err := buf.WriteByte(flags); err != nil {
		return 0, err
	}

	// Reason Code
	if err := buf.WriteByte(byte(p.ReasonCode)); err != nil {
		return 1, err
	}

	// Properties
	n, err := p.Props.Encode(&buf)
	if err != nil {
		return 2, err
	}
	_ = n

	// Write fixed header
	header := FixedHeader{
		PacketType:      PacketCONNACK,
		Flags:           0x00,
		RemainingLength: uint32(buf.Len()),
	}

	total, err := header.Encode(w)
	if err != nil {
		return total, err
	}

	// Write variable header
	n2, err := w.Write(buf.Bytes())
	return total + n2, err
}

// Decode reads the packet from the reader.
func (p *ConnackPacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketCONNACK {
		return 0, ErrInvalidPacketType
	}

	var totalRead int

	// Connect Acknowledge Flags
	var flagsBuf [1]byte
	n, err := io.ReadFull(r, flagsBuf[:])
	totalRead += n
	if err != nil {
		return totalRead, err
	}

	// Reserved bits must be 0
	if flagsBuf[0]&0xFE != 0 {
		return totalRead, ErrInvalidConnackFlags
	}

	p.SessionPresent = flagsBuf[0]&0x01 != 0

	// Reason Code
	var reasonBuf [1]byte
	n, err = io.ReadFull(r, reasonBuf[:])
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	p.ReasonCode = ReasonCode(reasonBuf[0])

	// Properties (if remaining length allows)
	if header.RemainingLength > 2 {
		n, err = p.Props.Decode(r)
		totalRead += n
		if err != nil {
			return totalRead, err
		}
	}

	return totalRead, nil
}

// Validate validates the packet contents.
func (p *ConnackPacket) Validate() error {
	// Validate reason code is valid for CONNACK
	if !p.ReasonCode.ValidForCONNACK() {
		return ErrInvalidReasonCode
	}

	// If reason code is not success, session present must be false
	if p.ReasonCode != ReasonSuccess && p.SessionPresent {
		return ErrInvalidConnackFlags
	}

	return nil
}

// Error variable for invalid reason code
var ErrInvalidReasonCode = errors.New("invalid reason code for packet type")
