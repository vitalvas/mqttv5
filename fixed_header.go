package mqttv5

import (
	"errors"
	"io"
)

// PacketType represents an MQTT control packet type.
type PacketType byte

// MQTT control packet types as defined in the specification.
const (
	PacketCONNECT     PacketType = 1
	PacketCONNACK     PacketType = 2
	PacketPUBLISH     PacketType = 3
	PacketPUBACK      PacketType = 4
	PacketPUBREC      PacketType = 5
	PacketPUBREL      PacketType = 6
	PacketPUBCOMP     PacketType = 7
	PacketSUBSCRIBE   PacketType = 8
	PacketSUBACK      PacketType = 9
	PacketUNSUBSCRIBE PacketType = 10
	PacketUNSUBACK    PacketType = 11
	PacketPINGREQ     PacketType = 12
	PacketPINGRESP    PacketType = 13
	PacketDISCONNECT  PacketType = 14
	PacketAUTH        PacketType = 15
)

// String returns the string representation of the packet type.
func (p PacketType) String() string {
	switch p {
	case PacketCONNECT:
		return "CONNECT"
	case PacketCONNACK:
		return "CONNACK"
	case PacketPUBLISH:
		return "PUBLISH"
	case PacketPUBACK:
		return "PUBACK"
	case PacketPUBREC:
		return "PUBREC"
	case PacketPUBREL:
		return "PUBREL"
	case PacketPUBCOMP:
		return "PUBCOMP"
	case PacketSUBSCRIBE:
		return "SUBSCRIBE"
	case PacketSUBACK:
		return "SUBACK"
	case PacketUNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case PacketUNSUBACK:
		return "UNSUBACK"
	case PacketPINGREQ:
		return "PINGREQ"
	case PacketPINGRESP:
		return "PINGRESP"
	case PacketDISCONNECT:
		return "DISCONNECT"
	case PacketAUTH:
		return "AUTH"
	default:
		return "UNKNOWN"
	}
}

// Valid returns true if the packet type is valid.
func (p PacketType) Valid() bool {
	return p >= PacketCONNECT && p <= PacketAUTH
}

// Fixed header errors.
var (
	ErrInvalidPacketType       = errors.New("invalid packet type")
	ErrInvalidPacketFlags      = errors.New("invalid packet flags")
	ErrRemainingLengthTooLarge = errors.New("remaining length too large")
)

// FixedHeader represents the fixed header of an MQTT control packet.
type FixedHeader struct {
	PacketType      PacketType
	Flags           byte
	RemainingLength uint32
}

// Encode writes the fixed header to the writer.
// Returns the number of bytes written.
func (h *FixedHeader) Encode(w io.Writer) (int, error) {
	if !h.PacketType.Valid() {
		return 0, ErrInvalidPacketType
	}

	// First byte: packet type (4 bits) | flags (4 bits)
	firstByte := byte(h.PacketType)<<4 | (h.Flags & 0x0F)
	n, err := w.Write([]byte{firstByte})
	if err != nil {
		return n, err
	}

	// Remaining length as variable byte integer
	n2, err := encodeVarint(w, h.RemainingLength)
	return n + n2, err
}

// Decode reads the fixed header from the reader.
// Returns the number of bytes read.
func (h *FixedHeader) Decode(r io.Reader) (int, error) {
	// Read first byte
	var buf [1]byte
	n, err := io.ReadFull(r, buf[:])
	if err != nil {
		return n, err
	}

	h.PacketType = PacketType(buf[0] >> 4)
	h.Flags = buf[0] & 0x0F

	if !h.PacketType.Valid() {
		return n, ErrInvalidPacketType
	}

	// Read remaining length
	length, n2, err := decodeVarint(r)
	n += n2
	if err != nil {
		return n, err
	}

	h.RemainingLength = length
	return n, nil
}

// Size returns the encoded size of the fixed header in bytes.
func (h *FixedHeader) Size() int {
	return 1 + varintSize(h.RemainingLength)
}

// ValidateFlags validates the flags for the packet type.
// Returns nil if valid, ErrInvalidPacketFlags otherwise.
func (h *FixedHeader) ValidateFlags() error {
	switch h.PacketType {
	case PacketPUBLISH:
		// PUBLISH has variable flags: DUP (bit 3), QoS (bits 2-1), RETAIN (bit 0)
		// QoS must be 0, 1, or 2 (not 3)
		qos := (h.Flags >> 1) & 0x03
		if qos > 2 {
			return ErrInvalidPacketFlags
		}
		return nil

	case PacketPUBREL, PacketSUBSCRIBE, PacketUNSUBSCRIBE:
		// These must have flags = 0x02
		if h.Flags != 0x02 {
			return ErrInvalidPacketFlags
		}
		return nil

	case PacketCONNECT, PacketCONNACK, PacketPUBACK, PacketPUBREC,
		PacketPUBCOMP, PacketSUBACK, PacketUNSUBACK, PacketPINGREQ,
		PacketPINGRESP, PacketDISCONNECT, PacketAUTH:
		// These must have flags = 0x00
		if h.Flags != 0x00 {
			return ErrInvalidPacketFlags
		}
		return nil

	default:
		return ErrInvalidPacketType
	}
}

// PUBLISH flag accessors

// DUP returns the DUP flag from PUBLISH packet flags.
func (h *FixedHeader) DUP() bool {
	return h.Flags&0x08 != 0
}

// SetDUP sets the DUP flag for PUBLISH packet.
func (h *FixedHeader) SetDUP(dup bool) {
	if dup {
		h.Flags |= 0x08
	} else {
		h.Flags &^= 0x08
	}
}

// QoS returns the QoS level from PUBLISH packet flags.
func (h *FixedHeader) QoS() byte {
	return (h.Flags >> 1) & 0x03
}

// SetQoS sets the QoS level for PUBLISH packet.
func (h *FixedHeader) SetQoS(qos byte) {
	h.Flags = (h.Flags & 0xF9) | ((qos & 0x03) << 1)
}

// Retain returns the RETAIN flag from PUBLISH packet flags.
func (h *FixedHeader) Retain() bool {
	return h.Flags&0x01 != 0
}

// SetRetain sets the RETAIN flag for PUBLISH packet.
func (h *FixedHeader) SetRetain(retain bool) {
	if retain {
		h.Flags |= 0x01
	} else {
		h.Flags &^= 0x01
	}
}
