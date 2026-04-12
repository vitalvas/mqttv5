package mqttv5

import (
	"errors"
	"io"
)

var (
	ErrPacketTooLarge    = errors.New("packet exceeds maximum size")
	ErrUnknownPacketType = errors.New("unknown packet type")
)

// codec provides version-aware MQTT packet reading and writing for an
// individual connection. It is an internal implementation detail shared by
// Client and Server to remember the negotiated protocol version and dispatch
// packet I/O accordingly.
type codec struct {
	ver ProtocolVersion
}

// newCodec creates a new codec for the given protocol version.
func newCodec(version ProtocolVersion) *codec {
	return &codec{ver: version}
}

// readPacket reads a complete MQTT packet using the codec's protocol version.
func (c *codec) readPacket(r io.Reader, maxSize uint32) (Packet, int, error) {
	if c.ver == ProtocolV311 {
		return readPacketV311(r, maxSize)
	}
	return readPacketV5(r, maxSize)
}

// writePacket writes a complete MQTT packet using the codec's protocol version.
func (c *codec) writePacket(w io.Writer, packet Packet, maxSize uint32) (int, error) {
	if c.ver == ProtocolV311 {
		return writePacketV311(w, packet, maxSize)
	}
	return writePacketRaw(w, packet, maxSize)
}

// readPacketV5 reads a complete MQTT packet from the reader.
// If maxSize is greater than 0, packets larger than maxSize return ErrPacketTooLarge.
//
// readPacketV5 auto-detects the MQTT protocol version for CONNECT packets by
// inspecting the protocol level byte in the variable header, and dispatches
// to the v3.1.1 or v5 decoder accordingly. All other packet types are decoded
// as MQTT 5.0.
func readPacketV5(r io.Reader, maxSize uint32) (Packet, int, error) {
	var header FixedHeader
	n, err := header.Decode(r)
	if err != nil {
		return nil, n, err
	}

	// Validate fixed header flags per MQTT spec
	if err := header.ValidateFlags(); err != nil {
		return nil, n, err
	}

	// Check max size - MQTT v5 Maximum Packet Size is total packet (fixed header + remaining)
	totalPacketSize := uint32(n) + header.RemainingLength
	if maxSize > 0 && totalPacketSize > maxSize {
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

	// Auto-detect v3.1.1 CONNECT packets from the protocol level byte.
	// CONNECT variable header layout: 2-byte protocol-name length, N-byte
	// protocol name, 1-byte protocol level. For "MQTT" (length 4) the level
	// byte is at offset 6.
	if header.PacketType == PacketCONNECT && len(remaining) > 6 && remaining[6] == byte(ProtocolV311) {
		reader := getBytesReader(remaining)
		pkt, err := decodePacketV311(reader, header)
		putBytesReader(reader)
		if err != nil {
			return nil, n, err
		}
		return pkt, n, nil
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

	// Decode packet using pooled reader
	reader := getBytesReader(remaining)
	_, err = packet.Decode(reader, header)
	putBytesReader(reader)
	if err != nil {
		return nil, n, err
	}

	// Validate decoded packet contents
	if err := packet.Validate(); err != nil {
		return nil, n, err
	}

	return packet, n, nil
}

// writePacketRaw writes a complete MQTT packet to the writer.
// If maxSize is greater than 0, packets larger than maxSize return ErrPacketTooLarge.
//
// writePacketRaw produces MQTT 5.0 wire format for every packet type except
// CONNECT: a ConnectPacket whose ProtocolVersion is ProtocolV311 is encoded
// as MQTT 3.1.1 through ConnectPacket.Encode.
func writePacketRaw(w io.Writer, packet Packet, maxSize uint32) (int, error) {
	if err := packet.Validate(); err != nil {
		return 0, err
	}

	// If max size check is needed, encode to buffer first
	if maxSize > 0 {
		buf := getBytesBuffer()
		n, err := packet.Encode(buf)
		if err != nil {
			putBytesBuffer(buf)
			return 0, err
		}
		if uint32(n) > maxSize {
			putBytesBuffer(buf)
			return 0, ErrPacketTooLarge
		}
		written, err := w.Write(buf.bytes())
		putBytesBuffer(buf)
		return written, err
	}

	return packet.Encode(w)
}

// bytesReader wraps a byte slice for io.Reader interface.
type bytesReader struct {
	data []byte
	pos  int
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

func (b *bytesBuffer) bytes() []byte {
	return b.data
}
