//nolint:dupl // Test cases for similar packet types have similar structure
package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PUBACK Tests

func TestPubackPacketType(t *testing.T) {
	p := &PubackPacket{}
	assert.Equal(t, PacketPUBACK, p.Type())
}

func TestPubackPacketID(t *testing.T) {
	p := &PubackPacket{}
	p.SetPacketID(12345)
	assert.Equal(t, uint16(12345), p.GetPacketID())
}

func TestPubackPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet PubackPacket
	}{
		{
			name: "success minimal",
			packet: PubackPacket{
				PacketID:   1,
				ReasonCode: ReasonSuccess,
			},
		},
		{
			name: "no matching subscribers",
			packet: PubackPacket{
				PacketID:   100,
				ReasonCode: ReasonNoMatchingSubscribers,
			},
		},
		{
			name: "not authorized",
			packet: PubackPacket{
				PacketID:   65535,
				ReasonCode: ReasonNotAuthorized,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.packet.Encode(&buf)
			require.NoError(t, err)
			assert.Greater(t, n, 0)

			var header FixedHeader
			_, err = header.Decode(&buf)
			require.NoError(t, err)
			assert.Equal(t, PacketPUBACK, header.PacketType)

			var decoded PubackPacket
			_, err = decoded.Decode(&buf, header)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.PacketID, decoded.PacketID)
			assert.Equal(t, tt.packet.ReasonCode, decoded.ReasonCode)
		})
	}
}

func TestPubackPacketWithProperties(t *testing.T) {
	packet := PubackPacket{
		PacketID:   1,
		ReasonCode: ReasonSuccess,
	}
	packet.Props.Set(PropReasonString, "OK")
	packet.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded PubackPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	assert.Equal(t, "OK", decoded.Props.GetString(PropReasonString))
	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	assert.Len(t, ups, 1)
}

func TestPubackPacketValidation(t *testing.T) {
	valid := PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	assert.NoError(t, valid.Validate())

	invalid := PubackPacket{PacketID: 1, ReasonCode: ReasonGrantedQoS1}
	assert.ErrorIs(t, invalid.Validate(), ErrInvalidReasonCode)
}

// PUBREC Tests

func TestPubrecPacketType(t *testing.T) {
	p := &PubrecPacket{}
	assert.Equal(t, PacketPUBREC, p.Type())
}

func TestPubrecPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet PubrecPacket
	}{
		{
			name: "success",
			packet: PubrecPacket{
				PacketID:   1,
				ReasonCode: ReasonSuccess,
			},
		},
		{
			name: "quota exceeded",
			packet: PubrecPacket{
				PacketID:   100,
				ReasonCode: ReasonQuotaExceeded,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.packet.Encode(&buf)
			require.NoError(t, err)
			assert.Greater(t, n, 0)

			var header FixedHeader
			_, err = header.Decode(&buf)
			require.NoError(t, err)
			assert.Equal(t, PacketPUBREC, header.PacketType)

			var decoded PubrecPacket
			_, err = decoded.Decode(&buf, header)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.PacketID, decoded.PacketID)
			assert.Equal(t, tt.packet.ReasonCode, decoded.ReasonCode)
		})
	}
}

func TestPubrecPacketValidation(t *testing.T) {
	valid := PubrecPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	assert.NoError(t, valid.Validate())

	invalid := PubrecPacket{PacketID: 1, ReasonCode: ReasonGrantedQoS1}
	assert.ErrorIs(t, invalid.Validate(), ErrInvalidReasonCode)
}

// PUBREL Tests

func TestPubrelPacketType(t *testing.T) {
	p := &PubrelPacket{}
	assert.Equal(t, PacketPUBREL, p.Type())
}

func TestPubrelPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet PubrelPacket
	}{
		{
			name: "success",
			packet: PubrelPacket{
				PacketID:   1,
				ReasonCode: ReasonSuccess,
			},
		},
		{
			name: "packet ID not found",
			packet: PubrelPacket{
				PacketID:   100,
				ReasonCode: ReasonPacketIDNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.packet.Encode(&buf)
			require.NoError(t, err)
			assert.Greater(t, n, 0)

			var header FixedHeader
			_, err = header.Decode(&buf)
			require.NoError(t, err)
			assert.Equal(t, PacketPUBREL, header.PacketType)
			assert.Equal(t, byte(0x02), header.Flags) // PUBREL must have flags 0x02

			var decoded PubrelPacket
			_, err = decoded.Decode(&buf, header)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.PacketID, decoded.PacketID)
			assert.Equal(t, tt.packet.ReasonCode, decoded.ReasonCode)
		})
	}
}

func TestPubrelPacketInvalidFlags(t *testing.T) {
	// PUBREL with wrong flags
	header := FixedHeader{
		PacketType:      PacketPUBREL,
		Flags:           0x00, // Should be 0x02
		RemainingLength: 2,
	}

	var p PubrelPacket
	_, err := p.Decode(bytes.NewReader([]byte{0x00, 0x01}), header)
	assert.ErrorIs(t, err, ErrInvalidPacketFlags)
}

func TestPubrelPacketValidation(t *testing.T) {
	valid := PubrelPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	assert.NoError(t, valid.Validate())

	invalid := PubrelPacket{PacketID: 1, ReasonCode: ReasonNotAuthorized}
	assert.ErrorIs(t, invalid.Validate(), ErrInvalidReasonCode)
}

// PUBCOMP Tests

func TestPubcompPacketType(t *testing.T) {
	p := &PubcompPacket{}
	assert.Equal(t, PacketPUBCOMP, p.Type())
}

func TestPubcompPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet PubcompPacket
	}{
		{
			name: "success",
			packet: PubcompPacket{
				PacketID:   1,
				ReasonCode: ReasonSuccess,
			},
		},
		{
			name: "packet ID not found",
			packet: PubcompPacket{
				PacketID:   100,
				ReasonCode: ReasonPacketIDNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.packet.Encode(&buf)
			require.NoError(t, err)
			assert.Greater(t, n, 0)

			var header FixedHeader
			_, err = header.Decode(&buf)
			require.NoError(t, err)
			assert.Equal(t, PacketPUBCOMP, header.PacketType)

			var decoded PubcompPacket
			_, err = decoded.Decode(&buf, header)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.PacketID, decoded.PacketID)
			assert.Equal(t, tt.packet.ReasonCode, decoded.ReasonCode)
		})
	}
}

func TestPubcompPacketValidation(t *testing.T) {
	valid := PubcompPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	assert.NoError(t, valid.Validate())

	invalid := PubcompPacket{PacketID: 1, ReasonCode: ReasonNotAuthorized}
	assert.ErrorIs(t, invalid.Validate(), ErrInvalidReasonCode)
}

// Benchmarks

func BenchmarkPubackPacketEncode(b *testing.B) {
	packet := PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	buf.Grow(16)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

func BenchmarkPubackPacketDecode(b *testing.B) {
	packet := PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		r := bytes.NewReader(data)
		var header FixedHeader
		_, _ = header.Decode(r)
		var p PubackPacket
		_, _ = p.Decode(r, header)
	}
}

func BenchmarkPubrecPacketEncode(b *testing.B) {
	packet := PubrecPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	buf.Grow(16)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

func BenchmarkPubrelPacketEncode(b *testing.B) {
	packet := PubrelPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	buf.Grow(16)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

func BenchmarkPubcompPacketEncode(b *testing.B) {
	packet := PubcompPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	buf.Grow(16)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

// Fuzz tests

func FuzzPubackPacketDecode(f *testing.F) {
	packet := PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	f.Add([]byte{0x40, 0x02, 0x00, 0x01})             // Minimal
	f.Add([]byte{0x40, 0x03, 0x00, 0x01, 0x00})       // With reason code
	f.Add([]byte{0x40, 0x04, 0x00, 0x01, 0x00, 0x00}) // With empty properties

	for range 10 {
		size := rand.IntN(32) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(data)
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := bytes.NewReader(data)
		var header FixedHeader
		n, err := header.Decode(r)
		if err != nil || header.PacketType != PacketPUBACK {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p PubackPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}

func FuzzPubrecPacketDecode(f *testing.F) {
	packet := PubrecPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	f.Add([]byte{0x50, 0x02, 0x00, 0x01})

	for range 10 {
		size := rand.IntN(32) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(data)
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := bytes.NewReader(data)
		var header FixedHeader
		n, err := header.Decode(r)
		if err != nil || header.PacketType != PacketPUBREC {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p PubrecPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}

func FuzzPubrelPacketDecode(f *testing.F) {
	packet := PubrelPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	f.Add([]byte{0x62, 0x02, 0x00, 0x01}) // PUBREL with flags 0x02

	for range 10 {
		size := rand.IntN(32) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(data)
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := bytes.NewReader(data)
		var header FixedHeader
		n, err := header.Decode(r)
		if err != nil || header.PacketType != PacketPUBREL {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p PubrelPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}

func FuzzPubcompPacketDecode(f *testing.F) {
	packet := PubcompPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	f.Add([]byte{0x70, 0x02, 0x00, 0x01})

	for range 10 {
		size := rand.IntN(32) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(data)
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := bytes.NewReader(data)
		var header FixedHeader
		n, err := header.Decode(r)
		if err != nil || header.PacketType != PacketPUBCOMP {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p PubcompPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}
