package mqttv5

import (
	"bytes"
	"io"
)

// ackPacket is a helper for encoding/decoding simple acknowledgment packets
// (PUBACK, PUBREC, PUBREL, PUBCOMP).
type ackPacket struct {
	PacketID   uint16
	ReasonCode ReasonCode
	Props      Properties
}

// encodeAck encodes an acknowledgment packet with the given packet type and flags.
func encodeAck(w io.Writer, packetType PacketType, flags byte, ack *ackPacket) (int, error) {
	var buf bytes.Buffer

	// Packet Identifier
	_, err := buf.Write([]byte{byte(ack.PacketID >> 8), byte(ack.PacketID)})
	if err != nil {
		return 0, err
	}

	// Reason Code and Properties (optional if success with no properties)
	if ack.ReasonCode != ReasonSuccess || ack.Props.Len() > 0 {
		if err := buf.WriteByte(byte(ack.ReasonCode)); err != nil {
			return 2, err
		}

		if ack.Props.Len() > 0 {
			_, err = ack.Props.Encode(&buf)
			if err != nil {
				return 3, err
			}
		}
	}

	// Write fixed header
	header := FixedHeader{
		PacketType:      packetType,
		Flags:           flags,
		RemainingLength: uint32(buf.Len()),
	}

	total, err := header.Encode(w)
	if err != nil {
		return total, err
	}

	n, err := w.Write(buf.Bytes())
	return total + n, err
}

// decodeAck decodes an acknowledgment packet with property validation.
func decodeAck(r io.Reader, header FixedHeader, ack *ackPacket, propCtx PropertyContext) (int, error) {
	var totalRead int

	// Packet Identifier
	var idBuf [2]byte
	n, err := io.ReadFull(r, idBuf[:])
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	ack.PacketID = uint16(idBuf[0])<<8 | uint16(idBuf[1])

	// Reason Code (optional)
	if header.RemainingLength > 2 {
		var reasonBuf [1]byte
		n, err = io.ReadFull(r, reasonBuf[:])
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		ack.ReasonCode = ReasonCode(reasonBuf[0])

		// Properties (optional)
		if header.RemainingLength > 3 {
			n, err = ack.Props.Decode(r)
			totalRead += n
			if err != nil {
				return totalRead, err
			}
			if err := ack.Props.ValidateFor(propCtx); err != nil {
				return totalRead, err
			}
		}
	} else {
		ack.ReasonCode = ReasonSuccess
	}

	return totalRead, nil
}
