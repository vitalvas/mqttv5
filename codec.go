package mqttv5

import (
	"errors"
	"io"
)

var (
	ErrPacketTooLarge    = errors.New("mqttv5: packet exceeds maximum size")
	ErrUnknownPacketType = errors.New("mqttv5: unknown packet type")
)

// ReadPacket reads a complete MQTT packet from the reader.
// If maxSize is greater than 0, packets larger than maxSize will return ErrPacketTooLarge.
func ReadPacket(r io.Reader, maxSize uint32) (Packet, int, error) {
	var header FixedHeader
	n, err := header.Decode(r)
	if err != nil {
		return nil, n, err
	}

	// Check max size
	if maxSize > 0 && header.RemainingLength > maxSize {
		return nil, n, ErrPacketTooLarge
	}

	// Read remaining bytes
	remaining := make([]byte, header.RemainingLength)
	if header.RemainingLength > 0 {
		rn, err := io.ReadFull(r, remaining)
		n += rn
		if err != nil {
			return nil, n, err
		}
	}

	// Create packet based on type
	var packet Packet
	switch header.PacketType {
	case PacketCONNECT:
		packet = &ConnectPacket{}
	case PacketCONNACK:
		packet = &ConnackPacket{}
	case PacketPUBLISH:
		packet = &PublishPacket{}
	case PacketPUBACK:
		packet = &PubackPacket{}
	case PacketPUBREC:
		packet = &PubrecPacket{}
	case PacketPUBREL:
		packet = &PubrelPacket{}
	case PacketPUBCOMP:
		packet = &PubcompPacket{}
	case PacketSUBSCRIBE:
		packet = &SubscribePacket{}
	case PacketSUBACK:
		packet = &SubackPacket{}
	case PacketUNSUBSCRIBE:
		packet = &UnsubscribePacket{}
	case PacketUNSUBACK:
		packet = &UnsubackPacket{}
	case PacketPINGREQ:
		packet = &PingreqPacket{}
	case PacketPINGRESP:
		packet = &PingrespPacket{}
	case PacketDISCONNECT:
		packet = &DisconnectPacket{}
	case PacketAUTH:
		packet = &AuthPacket{}
	default:
		return nil, n, ErrUnknownPacketType
	}

	// Decode packet
	reader := newBytesReader(remaining)
	_, err = packet.Decode(reader, header)
	if err != nil {
		return nil, n, err
	}

	return packet, n, nil
}

// WritePacket writes a complete MQTT packet to the writer.
// If maxSize is greater than 0, packets larger than maxSize will return ErrPacketTooLarge.
func WritePacket(w io.Writer, packet Packet, maxSize uint32) (int, error) {
	if err := packet.Validate(); err != nil {
		return 0, err
	}

	// If max size check is needed, encode to buffer first
	if maxSize > 0 {
		var buf bytesBuffer
		n, err := packet.Encode(&buf)
		if err != nil {
			return 0, err
		}
		if uint32(n) > maxSize {
			return 0, ErrPacketTooLarge
		}
		return w.Write(buf.Bytes())
	}

	return packet.Encode(w)
}

// bytesReader wraps a byte slice for io.Reader interface.
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// bytesBuffer is a simple buffer for encoding.
type bytesBuffer struct {
	data []byte
}

func (b *bytesBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *bytesBuffer) Bytes() []byte {
	return b.data
}
