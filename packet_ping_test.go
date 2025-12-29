//nolint:dupl // Similar test structure for PINGREQ and PINGRESP
package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPingreqPacketType(t *testing.T) {
	p := &PingreqPacket{}
	assert.Equal(t, PacketPINGREQ, p.Type())
}

func TestPingreqPacketEncodeDecode(t *testing.T) {
	packet := PingreqPacket{}

	var buf bytes.Buffer
	n, err := packet.Encode(&buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n) // Fixed header only

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)
	assert.Equal(t, PacketPINGREQ, header.PacketType)
	assert.Equal(t, byte(0x00), header.Flags)
	assert.Equal(t, uint32(0), header.RemainingLength)

	var decoded PingreqPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)
}

func TestPingreqPacketInvalidType(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPUBLISH,
		Flags:           0x00,
		RemainingLength: 0,
	}

	var p PingreqPacket
	_, err := p.Decode(bytes.NewReader(nil), header)
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestPingreqPacketInvalidFlags(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPINGREQ,
		Flags:           0x01,
		RemainingLength: 0,
	}

	var p PingreqPacket
	_, err := p.Decode(bytes.NewReader(nil), header)
	assert.ErrorIs(t, err, ErrInvalidPacketFlags)
}

func TestPingreqPacketInvalidLength(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPINGREQ,
		Flags:           0x00,
		RemainingLength: 1,
	}

	var p PingreqPacket
	_, err := p.Decode(bytes.NewReader(nil), header)
	assert.ErrorIs(t, err, ErrProtocolViolation)
}

func TestPingreqPacketValidation(t *testing.T) {
	p := PingreqPacket{}
	assert.NoError(t, p.Validate())
}

func TestPingrespPacketType(t *testing.T) {
	p := &PingrespPacket{}
	assert.Equal(t, PacketPINGRESP, p.Type())
}

func TestPingrespPacketEncodeDecode(t *testing.T) {
	packet := PingrespPacket{}

	var buf bytes.Buffer
	n, err := packet.Encode(&buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n) // Fixed header only

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)
	assert.Equal(t, PacketPINGRESP, header.PacketType)
	assert.Equal(t, byte(0x00), header.Flags)
	assert.Equal(t, uint32(0), header.RemainingLength)

	var decoded PingrespPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)
}

func TestPingrespPacketInvalidType(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPUBLISH,
		Flags:           0x00,
		RemainingLength: 0,
	}

	var p PingrespPacket
	_, err := p.Decode(bytes.NewReader(nil), header)
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestPingrespPacketInvalidFlags(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPINGRESP,
		Flags:           0x01,
		RemainingLength: 0,
	}

	var p PingrespPacket
	_, err := p.Decode(bytes.NewReader(nil), header)
	assert.ErrorIs(t, err, ErrInvalidPacketFlags)
}

func TestPingrespPacketInvalidLength(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPINGRESP,
		Flags:           0x00,
		RemainingLength: 1,
	}

	var p PingrespPacket
	_, err := p.Decode(bytes.NewReader(nil), header)
	assert.ErrorIs(t, err, ErrProtocolViolation)
}

func TestPingrespPacketValidation(t *testing.T) {
	p := PingrespPacket{}
	assert.NoError(t, p.Validate())
}

func BenchmarkPingreqPacketEncode(b *testing.B) {
	packet := PingreqPacket{}
	var buf bytes.Buffer
	buf.Grow(4)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

func BenchmarkPingrespPacketEncode(b *testing.B) {
	packet := PingrespPacket{}
	var buf bytes.Buffer
	buf.Grow(4)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

func FuzzPingreqPacketDecode(f *testing.F) {
	packet := PingreqPacket{}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	f.Add([]byte{0xC0, 0x00}) // Valid PINGREQ

	for range 10 {
		size := rand.IntN(8) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(data)
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := bytes.NewReader(data)
		var header FixedHeader
		_, err := header.Decode(r)
		if err != nil || header.PacketType != PacketPINGREQ {
			return
		}

		var p PingreqPacket
		_, _ = p.Decode(r, header)
	})
}

func FuzzPingrespPacketDecode(f *testing.F) {
	packet := PingrespPacket{}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	f.Add([]byte{0xD0, 0x00}) // Valid PINGRESP

	for range 10 {
		size := rand.IntN(8) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(data)
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := bytes.NewReader(data)
		var header FixedHeader
		_, err := header.Decode(r)
		if err != nil || header.PacketType != PacketPINGRESP {
			return
		}

		var p PingrespPacket
		_, _ = p.Decode(r, header)
	})
}
