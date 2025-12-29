package mqttv5

import (
	"encoding/binary"
	"errors"
	"io"
	"unicode/utf8"
)

// Encoding errors.
var (
	ErrStringTooLong      = errors.New("string exceeds maximum length of 65535 bytes")
	ErrBinaryTooLong      = errors.New("binary data exceeds maximum length of 65535 bytes")
	ErrInvalidUTF8        = errors.New("invalid UTF-8 string")
	ErrStringContainsNull = errors.New("string contains null character")
	ErrVarintTooLarge     = errors.New("variable byte integer exceeds maximum value")
	ErrVarintMalformed    = errors.New("malformed variable byte integer")
	ErrVarintOverlong     = errors.New("variable byte integer uses more bytes than necessary")
)

const (
	maxUint16         = 65535
	maxVarint         = 268435455 // 0x0FFFFFFF
	varintContinueBit = 0x80
	varintValueMask   = 0x7F
)

// encodeString writes a UTF-8 string with 2-byte length prefix to w.
// Returns the number of bytes written.
func encodeString(w io.Writer, s string) (int, error) {
	if len(s) > maxUint16 {
		return 0, ErrStringTooLong
	}

	if !utf8.ValidString(s) {
		return 0, ErrInvalidUTF8
	}

	for i := range len(s) {
		if s[i] == 0 {
			return 0, ErrStringContainsNull
		}
	}

	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(s)))

	n, err := w.Write(lenBuf[:])
	if err != nil {
		return n, err
	}

	n2, err := io.WriteString(w, s)
	return n + n2, err
}

// decodeString reads a UTF-8 string with 2-byte length prefix from r.
func decodeString(r io.Reader) (string, int, error) {
	var lenBuf [2]byte
	n, err := io.ReadFull(r, lenBuf[:])
	if err != nil {
		return "", n, err
	}

	length := binary.BigEndian.Uint16(lenBuf[:])
	if length == 0 {
		return "", n, nil
	}

	buf := make([]byte, length)
	n2, err := io.ReadFull(r, buf)
	n += n2
	if err != nil {
		return "", n, err
	}

	if !utf8.Valid(buf) {
		return "", n, ErrInvalidUTF8
	}

	for i := range len(buf) {
		if buf[i] == 0 {
			return "", n, ErrStringContainsNull
		}
	}

	return string(buf), n, nil
}

// encodeBinary writes binary data with 2-byte length prefix to w.
// Returns the number of bytes written.
func encodeBinary(w io.Writer, data []byte) (int, error) {
	if len(data) > maxUint16 {
		return 0, ErrBinaryTooLong
	}

	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(data)))

	n, err := w.Write(lenBuf[:])
	if err != nil {
		return n, err
	}

	n2, err := w.Write(data)
	return n + n2, err
}

// decodeBinary reads binary data with 2-byte length prefix from r.
func decodeBinary(r io.Reader) ([]byte, int, error) {
	var lenBuf [2]byte
	n, err := io.ReadFull(r, lenBuf[:])
	if err != nil {
		return nil, n, err
	}

	length := binary.BigEndian.Uint16(lenBuf[:])
	if length == 0 {
		return nil, n, nil
	}

	buf := make([]byte, length)
	n2, err := io.ReadFull(r, buf)
	n += n2
	if err != nil {
		return nil, n, err
	}

	return buf, n, nil
}

// StringPair represents a key-value string pair used in MQTT v5.0 properties.
type StringPair struct {
	Key   string
	Value string
}

// encodeStringPair writes a string pair (key, value) to w.
// Returns the number of bytes written.
func encodeStringPair(w io.Writer, pair StringPair) (int, error) {
	n, err := encodeString(w, pair.Key)
	if err != nil {
		return n, err
	}

	n2, err := encodeString(w, pair.Value)
	return n + n2, err
}

// decodeStringPair reads a string pair (key, value) from r.
func decodeStringPair(r io.Reader) (StringPair, int, error) {
	key, n, err := decodeString(r)
	if err != nil {
		return StringPair{}, n, err
	}

	value, n2, err := decodeString(r)
	n += n2
	if err != nil {
		return StringPair{}, n, err
	}

	return StringPair{Key: key, Value: value}, n, nil
}

// encodeVarint writes a variable byte integer to w.
// Returns the number of bytes written.
func encodeVarint(w io.Writer, value uint32) (int, error) {
	if value > maxVarint {
		return 0, ErrVarintTooLarge
	}

	var buf [4]byte
	n := 0

	for {
		encodedByte := byte(value & varintValueMask)
		value >>= 7

		if value > 0 {
			encodedByte |= varintContinueBit
		}

		buf[n] = encodedByte
		n++

		if value == 0 {
			break
		}
	}

	return w.Write(buf[:n])
}

// decodeVarint reads a variable byte integer from r.
// Returns the value, number of bytes read, and any error.
func decodeVarint(r io.Reader) (uint32, int, error) {
	var value uint32
	var multiplier uint32 = 1
	var buf [1]byte
	bytesRead := 0

	for {
		n, err := r.Read(buf[:])
		bytesRead += n
		if err != nil {
			return 0, bytesRead, err
		}

		encodedByte := buf[0]
		value += uint32(encodedByte&varintValueMask) * multiplier

		if value > maxVarint {
			return 0, bytesRead, ErrVarintTooLarge
		}

		if encodedByte&varintContinueBit == 0 {
			break
		}

		multiplier *= 128
		if multiplier > 128*128*128 {
			return 0, bytesRead, ErrVarintMalformed
		}
	}

	return value, bytesRead, nil
}

// varintSize returns the number of bytes needed to encode a variable byte integer.
func varintSize(value uint32) int {
	switch {
	case value < 128:
		return 1
	case value < 16384:
		return 2
	case value < 2097152:
		return 3
	default:
		return 4
	}
}
