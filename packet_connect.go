package mqttv5

import (
	"bytes"
	"errors"
	"io"
)

// CONNECT packet constants.
const (
	protocolName    = "MQTT"
	protocolVersion = 5
)

// Connect flag bit positions.
const (
	connectFlagCleanStart   = 0x02
	connectFlagWillFlag     = 0x04
	connectFlagWillRetain   = 0x20
	connectFlagPasswordFlag = 0x40
	connectFlagUsernameFlag = 0x80
)

// CONNECT packet errors.
var (
	ErrInvalidProtocolName    = errors.New("invalid protocol name")
	ErrInvalidProtocolVersion = errors.New("unsupported protocol version")
	ErrInvalidConnectFlags    = errors.New("invalid connect flags")
	ErrClientIDTooLong        = errors.New("client ID too long")
	ErrClientIDRequired       = errors.New("client ID required with clean start false")
)

// ConnectPacket represents an MQTT CONNECT packet.
// MQTT v5.0 spec: Section 3.1
type ConnectPacket struct {
	// ClientID is the client identifier.
	ClientID string

	// CleanStart indicates whether the session should start clean.
	CleanStart bool

	// KeepAlive is the keep alive interval in seconds.
	KeepAlive uint16

	// Properties contains the CONNECT properties.
	Props Properties

	// Username for authentication.
	Username string

	// Password for authentication.
	Password []byte

	// Will message configuration.
	WillFlag    bool
	WillRetain  bool
	WillQoS     byte
	WillTopic   string
	WillPayload []byte
	WillProps   Properties
}

// Type returns the packet type.
func (p *ConnectPacket) Type() PacketType {
	return PacketCONNECT
}

// Properties returns a pointer to the packet's properties.
func (p *ConnectPacket) Properties() *Properties {
	return &p.Props
}

// connectFlags returns the connect flags byte.
func (p *ConnectPacket) connectFlags() byte {
	var flags byte

	if p.CleanStart {
		flags |= connectFlagCleanStart
	}

	if p.WillFlag {
		flags |= connectFlagWillFlag
		flags |= (p.WillQoS & 0x03) << 3
		if p.WillRetain {
			flags |= connectFlagWillRetain
		}
	}

	if len(p.Password) > 0 {
		flags |= connectFlagPasswordFlag
	}

	if p.Username != "" {
		flags |= connectFlagUsernameFlag
	}

	return flags
}

// setConnectFlags parses the connect flags byte.
func (p *ConnectPacket) setConnectFlags(flags byte) error {
	// Reserved bit must be 0
	if flags&0x01 != 0 {
		return ErrInvalidConnectFlags
	}

	p.CleanStart = flags&connectFlagCleanStart != 0
	p.WillFlag = flags&connectFlagWillFlag != 0
	p.WillQoS = (flags >> 3) & 0x03
	p.WillRetain = flags&connectFlagWillRetain != 0

	// Will QoS must be 0 if Will Flag is 0
	if !p.WillFlag && p.WillQoS != 0 {
		return ErrInvalidConnectFlags
	}

	// Will Retain must be 0 if Will Flag is 0
	if !p.WillFlag && p.WillRetain {
		return ErrInvalidConnectFlags
	}

	// Will QoS must not be 3
	if p.WillQoS > 2 {
		return ErrInvalidConnectFlags
	}

	return nil
}

// Encode writes the packet to the writer.
func (p *ConnectPacket) Encode(w io.Writer) (int, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}

	// Build variable header and payload
	var buf bytes.Buffer

	// Protocol Name
	n, err := encodeString(&buf, protocolName)
	if err != nil {
		return 0, err
	}

	// Protocol Version
	if err := buf.WriteByte(protocolVersion); err != nil {
		return n, err
	}
	n++

	// Connect Flags
	if err := buf.WriteByte(p.connectFlags()); err != nil {
		return n, err
	}
	n++

	// Keep Alive
	n2, err := buf.Write([]byte{byte(p.KeepAlive >> 8), byte(p.KeepAlive)})
	n += n2
	if err != nil {
		return n, err
	}

	// Properties
	n3, err := p.Props.Encode(&buf)
	n += n3
	if err != nil {
		return n, err
	}

	// Payload

	// Client ID
	n4, err := encodeString(&buf, p.ClientID)
	n += n4
	if err != nil {
		return n, err
	}

	// Will Properties, Topic, Payload
	if p.WillFlag {
		n5, err := p.WillProps.Encode(&buf)
		n += n5
		if err != nil {
			return n, err
		}

		n6, err := encodeString(&buf, p.WillTopic)
		n += n6
		if err != nil {
			return n, err
		}

		n7, err := encodeBinary(&buf, p.WillPayload)
		n += n7
		if err != nil {
			return n, err
		}
	}

	// Username
	if p.Username != "" {
		n8, err := encodeString(&buf, p.Username)
		n += n8
		if err != nil {
			return n, err
		}
	}

	// Password
	if len(p.Password) > 0 {
		n9, err := encodeBinary(&buf, p.Password)
		n += n9
		if err != nil {
			return n, err
		}
	}

	// Write fixed header
	header := FixedHeader{
		PacketType:      PacketCONNECT,
		Flags:           0x00,
		RemainingLength: uint32(buf.Len()),
	}

	total, err := header.Encode(w)
	if err != nil {
		return total, err
	}

	// Write variable header and payload
	n10, err := w.Write(buf.Bytes())
	return total + n10, err
}

// Decode reads the packet from the reader.
func (p *ConnectPacket) Decode(r io.Reader, header FixedHeader) (int, error) {
	if header.PacketType != PacketCONNECT {
		return 0, ErrInvalidPacketType
	}

	var totalRead int

	// Protocol Name
	protoName, n, err := decodeString(r)
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	if protoName != protocolName {
		return totalRead, ErrInvalidProtocolName
	}

	// Protocol Version
	var versionBuf [1]byte
	n, err = io.ReadFull(r, versionBuf[:])
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	if versionBuf[0] != protocolVersion {
		return totalRead, ErrInvalidProtocolVersion
	}

	// Connect Flags
	var flagsBuf [1]byte
	n, err = io.ReadFull(r, flagsBuf[:])
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	if err := p.setConnectFlags(flagsBuf[0]); err != nil {
		return totalRead, err
	}

	usernameFlag := flagsBuf[0]&connectFlagUsernameFlag != 0
	passwordFlag := flagsBuf[0]&connectFlagPasswordFlag != 0

	// Keep Alive
	var keepAliveBuf [2]byte
	n, err = io.ReadFull(r, keepAliveBuf[:])
	totalRead += n
	if err != nil {
		return totalRead, err
	}
	p.KeepAlive = uint16(keepAliveBuf[0])<<8 | uint16(keepAliveBuf[1])

	// Properties
	n, err = p.Props.Decode(r)
	totalRead += n
	if err != nil {
		return totalRead, err
	}

	// Payload

	// Client ID
	p.ClientID, n, err = decodeString(r)
	totalRead += n
	if err != nil {
		return totalRead, err
	}

	// Will Properties, Topic, Payload
	if p.WillFlag {
		n, err = p.WillProps.Decode(r)
		totalRead += n
		if err != nil {
			return totalRead, err
		}

		p.WillTopic, n, err = decodeString(r)
		totalRead += n
		if err != nil {
			return totalRead, err
		}

		p.WillPayload, n, err = decodeBinary(r)
		totalRead += n
		if err != nil {
			return totalRead, err
		}
	}

	// Username
	if usernameFlag {
		p.Username, n, err = decodeString(r)
		totalRead += n
		if err != nil {
			return totalRead, err
		}
	}

	// Password
	if passwordFlag {
		p.Password, n, err = decodeBinary(r)
		totalRead += n
		if err != nil {
			return totalRead, err
		}
	}

	return totalRead, nil
}

// Validate validates the packet contents.
func (p *ConnectPacket) Validate() error {
	// Client ID length check (max 23 characters recommended, but up to 65535 allowed)
	if len(p.ClientID) > 65535 {
		return ErrClientIDTooLong
	}

	// Client ID must be present if CleanStart is false
	if !p.CleanStart && p.ClientID == "" {
		return ErrClientIDRequired
	}

	// Will QoS must be valid
	if p.WillQoS > 2 {
		return ErrInvalidConnectFlags
	}

	// Will Retain and Will QoS should be 0 if Will Flag is not set
	if !p.WillFlag && (p.WillRetain || p.WillQoS != 0) {
		return ErrInvalidConnectFlags
	}

	return nil
}
