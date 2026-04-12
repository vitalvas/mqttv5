package mqttv5

import (
	"bytes"
	"io"
)

// readPacketV311 reads a complete MQTT 3.1.1 packet from the reader.
func readPacketV311(r io.Reader, maxSize uint32) (Packet, int, error) {
	var header FixedHeader
	n, err := header.Decode(r)
	if err != nil {
		return nil, n, err
	}

	if err := header.ValidateFlags(); err != nil {
		return nil, n, err
	}

	totalPacketSize := uint32(n) + header.RemainingLength
	if maxSize > 0 && totalPacketSize > maxSize {
		return nil, n, ErrPacketTooLarge
	}

	remaining := make([]byte, header.RemainingLength)
	if header.RemainingLength > 0 {
		rn, err := io.ReadFull(r, remaining)
		n += rn
		if err != nil {
			return nil, n, err
		}
	}

	reader := getBytesReader(remaining)
	defer putBytesReader(reader)

	pkt, err := decodePacketV311(reader, header)
	if err != nil {
		return nil, n, err
	}
	return pkt, n, nil
}

// decodePacketV311 decodes a v3.1.1 packet from already-read remaining bytes.
func decodePacketV311(reader io.Reader, header FixedHeader) (Packet, error) {
	switch header.PacketType {
	case PacketCONNECT:
		return decodeAndValidate(&ConnectPacket{}, reader, header)
	case PacketCONNACK:
		if header.RemainingLength != 2 {
			return nil, ErrProtocolViolation
		}
		pkt := &ConnackPacket{}
		_, err := decodeConnackV311(reader, header, pkt)
		return pkt, err
	case PacketPUBLISH:
		pkt := &PublishPacket{}
		if _, err := decodePublishV311(reader, header, pkt); err != nil {
			return nil, err
		}
		return pkt, pkt.Validate()
	case PacketPUBACK:
		return decodeAckPacketV311[PubackPacket](reader, header)
	case PacketPUBREC:
		return decodeAckPacketV311[PubrecPacket](reader, header)
	case PacketPUBREL:
		return decodeAckPacketV311[PubrelPacket](reader, header)
	case PacketPUBCOMP:
		return decodeAckPacketV311[PubcompPacket](reader, header)
	case PacketSUBSCRIBE:
		pkt := &SubscribePacket{}
		if _, err := decodeSubscribeV311(reader, header, pkt); err != nil {
			return nil, err
		}
		return pkt, pkt.Validate()
	case PacketSUBACK:
		pkt := &SubackPacket{}
		if _, err := decodeSubackV311(reader, header, pkt); err != nil {
			return nil, err
		}
		if pkt.PacketID == 0 {
			return nil, ErrInvalidPacketID
		}
		return pkt, nil
	case PacketUNSUBSCRIBE:
		pkt := &UnsubscribePacket{}
		if _, err := decodeUnsubscribeV311(reader, header, pkt); err != nil {
			return nil, err
		}
		return pkt, pkt.Validate()
	case PacketUNSUBACK:
		// Skip Validate() — v3.1.1 UNSUBACK has no reason codes
		if header.RemainingLength != 2 {
			return nil, ErrProtocolViolation
		}
		pkt := &UnsubackPacket{}
		if _, err := decodeUnsubackV311(reader, header, pkt); err != nil {
			return nil, err
		}
		if pkt.PacketID == 0 {
			return nil, ErrInvalidPacketID
		}
		return pkt, nil
	case PacketPINGREQ:
		if header.RemainingLength != 0 {
			return nil, ErrProtocolViolation
		}
		return &PingreqPacket{}, nil
	case PacketPINGRESP:
		if header.RemainingLength != 0 {
			return nil, ErrProtocolViolation
		}
		return &PingrespPacket{}, nil
	case PacketDISCONNECT:
		if header.RemainingLength != 0 {
			return nil, ErrProtocolViolation
		}
		return &DisconnectPacket{ReasonCode: ReasonSuccess}, nil
	default:
		return nil, ErrUnknownPacketType
	}
}

// decodeAndValidate decodes a packet using its standard Decode method, then validates.
func decodeAndValidate(pkt Packet, r io.Reader, header FixedHeader) (Packet, error) {
	if _, err := pkt.Decode(r, header); err != nil {
		return nil, err
	}
	return pkt, pkt.Validate()
}

// ackPacketSetter is a constraint for ack packet types that have PacketID and ReasonCode fields.
type ackPacketSetter interface {
	PubackPacket | PubrecPacket | PubrelPacket | PubcompPacket
}

// decodeAckPacketV311 decodes a v3.1.1 ack packet (PUBACK, PUBREC, PUBREL, PUBCOMP).
// v3.1.1 ack packets always have remaining length = 2 (just the packet identifier).
func decodeAckPacketV311[T ackPacketSetter](r io.Reader, header FixedHeader) (*T, error) {
	if header.RemainingLength != 2 {
		return nil, ErrProtocolViolation
	}
	var ack ackPacket
	if _, err := decodeAckV311(r, &ack); err != nil {
		return nil, err
	}
	if ack.PacketID == 0 {
		return nil, ErrInvalidPacketID
	}
	pkt := new(T)
	switch p := any(pkt).(type) {
	case *PubackPacket:
		p.PacketID = ack.PacketID
		p.ReasonCode = ack.ReasonCode
	case *PubrecPacket:
		p.PacketID = ack.PacketID
		p.ReasonCode = ack.ReasonCode
	case *PubrelPacket:
		p.PacketID = ack.PacketID
		p.ReasonCode = ack.ReasonCode
	case *PubcompPacket:
		p.PacketID = ack.PacketID
		p.ReasonCode = ack.ReasonCode
	}
	return pkt, nil
}

// writePacketV311 writes a complete MQTT 3.1.1 packet to the writer.
// When maxSize > 0, the packet is encoded to a buffer first and rejected if its
// final byte length exceeds maxSize, matching v5 writePacketRaw semantics.
func writePacketV311(w io.Writer, packet Packet, maxSize uint32) (int, error) {
	if maxSize == 0 {
		return encodePacketV311(w, packet, 0)
	}

	var buf bytes.Buffer
	if _, err := encodePacketV311(&buf, packet, maxSize); err != nil {
		return 0, err
	}
	if uint32(buf.Len()) > maxSize {
		return 0, ErrPacketTooLarge
	}
	return w.Write(buf.Bytes())
}

// encodePacketV311 dispatches to the type-specific v3.1.1 encoder.
func encodePacketV311(w io.Writer, packet Packet, maxSize uint32) (int, error) {
	switch p := packet.(type) {
	case *ConnectPacket:
		return encodeConnectV311(w, p, maxSize)
	case *ConnackPacket:
		if err := p.Validate(); err != nil {
			return 0, err
		}
		return encodeConnackV311(w, p)
	case *PublishPacket:
		return encodePublishV311(w, p, maxSize)
	case *PubackPacket:
		if p.PacketID == 0 {
			return 0, ErrInvalidPacketID
		}
		return encodeAckV311(w, PacketPUBACK, 0x00, &ackPacket{PacketID: p.PacketID})
	case *PubrecPacket:
		if p.PacketID == 0 {
			return 0, ErrInvalidPacketID
		}
		return encodeAckV311(w, PacketPUBREC, 0x00, &ackPacket{PacketID: p.PacketID})
	case *PubrelPacket:
		if p.PacketID == 0 {
			return 0, ErrInvalidPacketID
		}
		return encodeAckV311(w, PacketPUBREL, 0x02, &ackPacket{PacketID: p.PacketID})
	case *PubcompPacket:
		if p.PacketID == 0 {
			return 0, ErrInvalidPacketID
		}
		return encodeAckV311(w, PacketPUBCOMP, 0x00, &ackPacket{PacketID: p.PacketID})
	case *SubscribePacket:
		return encodeSubscribeV311(w, p, maxSize)
	case *SubackPacket:
		if len(p.ReasonCodes) == 0 {
			return 0, ErrProtocolViolation
		}
		return encodeSubackV311(w, p)
	case *UnsubscribePacket:
		return encodeUnsubscribeV311(w, p, maxSize)
	case *UnsubackPacket:
		return encodeUnsubackV311(w, p)
	case *PingreqPacket:
		return w.Write([]byte{byte(PacketPINGREQ) << 4, 0x00})
	case *PingrespPacket:
		return w.Write([]byte{byte(PacketPINGRESP) << 4, 0x00})
	case *DisconnectPacket:
		return w.Write([]byte{byte(PacketDISCONNECT) << 4, 0x00})
	default:
		return 0, ErrUnknownPacketType
	}
}

// --- CONNECT ---

func encodeConnectV311(w io.Writer, p *ConnectPacket, maxSize uint32) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}

	var buf bytes.Buffer

	// Protocol Name
	if _, err := encodeString(&buf, protocolName); err != nil {
		return 0, err
	}

	// Protocol Version (v3.1.1 = 4)
	buf.WriteByte(byte(ProtocolV311))

	// Connect Flags
	buf.WriteByte(p.connectFlags())

	// Keep Alive
	buf.Write([]byte{byte(p.KeepAlive >> 8), byte(p.KeepAlive)})

	// No Properties in v3.1.1

	// Payload: Client ID
	if _, err := encodeString(&buf, p.ClientID); err != nil {
		return 0, err
	}

	// Will Topic and Payload (no Will Properties in v3.1.1)
	if p.WillFlag {
		if _, err := encodeString(&buf, p.WillTopic); err != nil {
			return 0, err
		}
		if _, err := encodeBinary(&buf, p.WillPayload); err != nil {
			return 0, err
		}
	}

	// Username
	if p.Username != "" {
		if _, err := encodeString(&buf, p.Username); err != nil {
			return 0, err
		}
	}

	// Password
	if len(p.Password) > 0 {
		if _, err := encodeBinary(&buf, p.Password); err != nil {
			return 0, err
		}
	}

	header := FixedHeader{
		PacketType:      PacketCONNECT,
		Flags:           0x00,
		RemainingLength: uint32(buf.Len()),
	}

	if maxSize > 0 && uint32(header.Size())+uint32(buf.Len()) > maxSize {
		return 0, ErrPacketTooLarge
	}

	total, err := header.Encode(w)
	if err != nil {
		return total, err
	}
	n, err := w.Write(buf.Bytes())
	return total + n, err
}

// --- CONNACK ---

func decodeConnackV311(r io.Reader, _ FixedHeader, p *ConnackPacket) (int, error) {
	var buf [2]byte
	n, err := io.ReadFull(r, buf[:])
	if err != nil {
		return n, err
	}

	if buf[0]&0xFE != 0 {
		return n, ErrInvalidConnackFlags
	}
	p.SessionPresent = buf[0]&0x01 != 0

	// MQTT 3.1.1 only defines return codes 0x00..0x05.
	if buf[1] > v311ConnNotAuthorized {
		return n, ErrInvalidReasonCode
	}
	p.ReasonCode = v311ConnReturnToReasonCode(buf[1])

	if p.ReasonCode != ReasonSuccess && p.SessionPresent {
		return n, ErrInvalidConnackFlags
	}

	return n, nil
}

func encodeConnackV311(w io.Writer, p *ConnackPacket) (int, error) {
	var buf [4]byte
	buf[0] = byte(PacketCONNACK) << 4
	buf[1] = 0x02 // remaining length
	if p.SessionPresent {
		buf[2] = 0x01
	}
	buf[3] = reasonCodeToV311ConnReturn(p.ReasonCode)
	return w.Write(buf[:])
}

// --- PUBLISH ---

func decodePublishV311(r io.Reader, header FixedHeader, p *PublishPacket) (int, error) {
	p.setFlags(header.Flags)
	if p.QoS > QoS2 {
		return 0, ErrInvalidQoS
	}

	var totalRead int

	topic, n, err := decodeString(r)
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	p.Topic = topic

	if p.QoS > QoS0 {
		var idBuf [2]byte
		n, err := io.ReadFull(r, idBuf[:])
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		p.PacketID = uint16(idBuf[0])<<8 | uint16(idBuf[1])
	}

	// No properties in v3.1.1 — remaining bytes are payload
	payloadLen := int(header.RemainingLength) - totalRead
	if payloadLen > 0 {
		p.Payload = make([]byte, payloadLen)
		n, err := io.ReadFull(r, p.Payload)
		totalRead += n
		if err != nil {
			return totalRead, err
		}
	}

	return totalRead, nil
}

func encodePublishV311(w io.Writer, p *PublishPacket, maxSize uint32) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}

	var buf bytes.Buffer

	// Topic Name
	if _, err := encodeString(&buf, p.Topic); err != nil {
		return 0, err
	}

	// Packet Identifier (QoS > 0)
	if p.QoS > QoS0 {
		buf.Write([]byte{byte(p.PacketID >> 8), byte(p.PacketID)})
	}

	// No properties — payload directly
	buf.Write(p.Payload)

	header := FixedHeader{
		PacketType:      PacketPUBLISH,
		Flags:           p.flags(),
		RemainingLength: uint32(buf.Len()),
	}

	if maxSize > 0 && uint32(header.Size())+uint32(buf.Len()) > maxSize {
		return 0, ErrPacketTooLarge
	}

	total, err := header.Encode(w)
	if err != nil {
		return total, err
	}
	n, err := w.Write(buf.Bytes())
	return total + n, err
}

// --- ACK packets (PUBACK, PUBREC, PUBREL, PUBCOMP) ---

func decodeAckV311(r io.Reader, ack *ackPacket) (int, error) {
	var idBuf [2]byte
	n, err := io.ReadFull(r, idBuf[:])
	if err != nil {
		return n, err
	}
	ack.PacketID = uint16(idBuf[0])<<8 | uint16(idBuf[1])
	ack.ReasonCode = ReasonSuccess
	return n, nil
}

func encodeAckV311(w io.Writer, packetType PacketType, flags byte, ack *ackPacket) (int, error) {
	buf := [4]byte{
		byte(packetType)<<4 | (flags & 0x0F),
		0x02, // remaining length = 2
		byte(ack.PacketID >> 8),
		byte(ack.PacketID),
	}
	return w.Write(buf[:])
}

// --- SUBSCRIBE ---

func decodeSubscribeV311(r io.Reader, header FixedHeader, p *SubscribePacket) (int, error) {
	if header.Flags != 0x02 {
		return 0, ErrInvalidPacketFlags
	}

	var totalRead int

	var idBuf [2]byte
	n, err := io.ReadFull(r, idBuf[:])
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	p.PacketID = uint16(idBuf[0])<<8 | uint16(idBuf[1])

	// No properties in v3.1.1

	p.Subscriptions = nil
	for totalRead < int(header.RemainingLength) {
		var sub Subscription

		sub.TopicFilter, n, err = decodeString(r)
		totalRead += n
		if err != nil {
			return totalRead, err
		}

		var qosBuf [1]byte
		n, err = io.ReadFull(r, qosBuf[:])
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		// MQTT 3.1.1 reserves bits 2-7 of the subscription options byte; they must be zero.
		if qosBuf[0]&0xFC != 0 {
			return totalRead, ErrInvalidPacketFlags
		}
		if qosBuf[0]&0x03 == 0x03 {
			return totalRead, ErrInvalidQoS
		}
		sub.QoS = qosBuf[0] & 0x03

		p.Subscriptions = append(p.Subscriptions, sub)
	}

	return totalRead, nil
}

func encodeSubscribeV311(w io.Writer, p *SubscribePacket, maxSize uint32) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}

	var buf bytes.Buffer

	buf.Write([]byte{byte(p.PacketID >> 8), byte(p.PacketID)})

	// No properties
	for _, sub := range p.Subscriptions {
		if _, err := encodeString(&buf, sub.TopicFilter); err != nil {
			return 0, err
		}
		buf.WriteByte(sub.QoS & 0x03)
	}

	header := FixedHeader{
		PacketType:      PacketSUBSCRIBE,
		Flags:           0x02,
		RemainingLength: uint32(buf.Len()),
	}

	if maxSize > 0 && uint32(header.Size())+uint32(buf.Len()) > maxSize {
		return 0, ErrPacketTooLarge
	}

	total, err := header.Encode(w)
	if err != nil {
		return total, err
	}
	n, err := w.Write(buf.Bytes())
	return total + n, err
}

// --- SUBACK ---

func decodeSubackV311(r io.Reader, header FixedHeader, p *SubackPacket) (int, error) {
	var totalRead int

	var idBuf [2]byte
	n, err := io.ReadFull(r, idBuf[:])
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	p.PacketID = uint16(idBuf[0])<<8 | uint16(idBuf[1])

	// No properties — return codes follow immediately
	p.ReasonCodes = nil
	for totalRead < int(header.RemainingLength) {
		var rcBuf [1]byte
		n, err = io.ReadFull(r, rcBuf[:])
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		// MQTT 3.1.1 only allows 0x00, 0x01, 0x02, and 0x80.
		switch rcBuf[0] {
		case 0x00, 0x01, 0x02, 0x80:
		default:
			return totalRead, ErrInvalidReasonCode
		}
		p.ReasonCodes = append(p.ReasonCodes, v311SubReturnToReasonCode(rcBuf[0]))
	}

	if len(p.ReasonCodes) == 0 {
		return totalRead, ErrProtocolViolation
	}

	return totalRead, nil
}

func encodeSubackV311(w io.Writer, p *SubackPacket) (int, error) {
	if p.PacketID == 0 {
		return 0, ErrInvalidPacketID
	}

	var buf bytes.Buffer
	buf.Write([]byte{byte(p.PacketID >> 8), byte(p.PacketID)})

	// No properties — return codes directly
	for _, rc := range p.ReasonCodes {
		buf.WriteByte(reasonCodeToV311SubReturn(rc))
	}

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

// --- UNSUBSCRIBE ---

func decodeUnsubscribeV311(r io.Reader, header FixedHeader, p *UnsubscribePacket) (int, error) {
	if header.Flags != 0x02 {
		return 0, ErrInvalidPacketFlags
	}

	var totalRead int

	var idBuf [2]byte
	n, err := io.ReadFull(r, idBuf[:])
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	p.PacketID = uint16(idBuf[0])<<8 | uint16(idBuf[1])

	// No properties in v3.1.1

	p.TopicFilters = nil
	for totalRead < int(header.RemainingLength) {
		tf, n, err := decodeString(r)
		totalRead += n
		if err != nil {
			return totalRead, err
		}
		p.TopicFilters = append(p.TopicFilters, tf)
	}

	return totalRead, nil
}

func encodeUnsubscribeV311(w io.Writer, p *UnsubscribePacket, maxSize uint32) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}

	var buf bytes.Buffer

	buf.Write([]byte{byte(p.PacketID >> 8), byte(p.PacketID)})

	// No properties
	for _, tf := range p.TopicFilters {
		if _, err := encodeString(&buf, tf); err != nil {
			return 0, err
		}
	}

	header := FixedHeader{
		PacketType:      PacketUNSUBSCRIBE,
		Flags:           0x02,
		RemainingLength: uint32(buf.Len()),
	}

	if maxSize > 0 && uint32(header.Size())+uint32(buf.Len()) > maxSize {
		return 0, ErrPacketTooLarge
	}

	total, err := header.Encode(w)
	if err != nil {
		return total, err
	}
	n, err := w.Write(buf.Bytes())
	return total + n, err
}

// --- UNSUBACK ---

func decodeUnsubackV311(r io.Reader, _ FixedHeader, p *UnsubackPacket) (int, error) {
	var idBuf [2]byte
	n, err := io.ReadFull(r, idBuf[:])
	if err != nil {
		return n, err
	}
	p.PacketID = uint16(idBuf[0])<<8 | uint16(idBuf[1])
	// v3.1.1 UNSUBACK has no per-topic reason codes
	p.ReasonCodes = nil
	return n, nil
}

func encodeUnsubackV311(w io.Writer, p *UnsubackPacket) (int, error) {
	if p.PacketID == 0 {
		return 0, ErrInvalidPacketID
	}
	buf := [4]byte{
		byte(PacketUNSUBACK) << 4,
		0x02, // remaining length
		byte(p.PacketID >> 8),
		byte(p.PacketID),
	}
	return w.Write(buf[:])
}
