package mqttv5

import (
	"bytes"
	"io"
)

// UnsubscribePacket represents an MQTT UNSUBSCRIBE packet.
// MQTT v5.0 spec: Section 3.10
type UnsubscribePacket struct {
	PacketID     uint16
	Props        Properties
	TopicFilters []string
}

// Type returns the packet type.
func (p *UnsubscribePacket) Type() PacketType { return PacketUNSUBSCRIBE }

// Properties returns a pointer to the packet's properties.
func (p *UnsubscribePacket) Properties() *Properties { return &p.Props }

// GetPacketID returns the packet identifier.
func (p *UnsubscribePacket) GetPacketID() uint16 { return p.PacketID }

// SetPacketID sets the packet identifier.
func (p *UnsubscribePacket) SetPacketID(id uint16) { p.PacketID = id }

// Encode writes the packet to the writer.
func (p *UnsubscribePacket) Encode(w io.Writer) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	if err := p.Props.ValidateFor(PropCtxUNSUBSCRIBE); err != nil {
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

	// Payload: topic filters
	for _, tf := range p.TopicFilters {
		if _, err := encodeString(&buf, tf); err != nil {
			return 0, err
		}
	}

	// Write fixed header
	header := FixedHeader{
		PacketType:      PacketUNSUBSCRIBE,
		Flags:           0x02, // UNSUBSCRIBE must have flags 0x02
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
func (p *UnsubscribePacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketUNSUBSCRIBE {
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
	if err := p.Props.ValidateFor(PropCtxUNSUBSCRIBE); err != nil {
		return totalRead, err
	}

	// Payload: topic filters
	p.TopicFilters = nil
	for totalRead < int(header.RemainingLength) {
		topicFilter, n, err := decodeString(r)
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		p.TopicFilters = append(p.TopicFilters, topicFilter)
	}

	return totalRead, nil
}

// Validate validates the packet contents.
func (p *UnsubscribePacket) Validate() error {
	if p.PacketID == 0 {
		return ErrInvalidPacketID
	}
	if len(p.TopicFilters) == 0 {
		return ErrProtocolViolation
	}
	for _, tf := range p.TopicFilters {
		if tf == "" {
			return ErrProtocolViolation
		}
	}
	return nil
}
