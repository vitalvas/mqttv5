//nolint:dupl // Similar test structure for similar packet types
package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	t.Run("valid packet", func(t *testing.T) {
		valid := PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess}
		assert.NoError(t, valid.Validate())
	})

	t.Run("invalid reason code", func(t *testing.T) {
		invalid := PubackPacket{PacketID: 1, ReasonCode: ReasonGrantedQoS1}
		assert.ErrorIs(t, invalid.Validate(), ErrInvalidReasonCode)
	})

	t.Run("zero packet ID", func(t *testing.T) {
		invalid := PubackPacket{PacketID: 0, ReasonCode: ReasonSuccess}
		assert.ErrorIs(t, invalid.Validate(), ErrInvalidPacketID)
	})
}

func TestPubackPacketEncodeErrors(t *testing.T) {
	t.Run("encode with validation error", func(t *testing.T) {
		// Invalid packet ID triggers validation error in Encode
		invalid := PubackPacket{PacketID: 0, ReasonCode: ReasonSuccess}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrInvalidPacketID)
	})

	t.Run("encode with invalid reason code", func(t *testing.T) {
		invalid := PubackPacket{PacketID: 1, ReasonCode: ReasonGrantedQoS1}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrInvalidReasonCode)
	})

	t.Run("encode with invalid property", func(t *testing.T) {
		// Use a property not valid for PUBACK context
		invalid := PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess}
		invalid.Props.Set(PropServerKeepAlive, uint16(60)) // Not valid for PUBACK
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.Error(t, err)
	})
}

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

func TestPubackPacketProperties(t *testing.T) {
	p := &PubackPacket{}
	p.Props.Set(PropReasonString, "test reason")
	props := p.Properties()
	require.NotNil(t, props)
	assert.Equal(t, "test reason", props.GetString(PropReasonString))
}

func TestPubackPacketDecodeErrors(t *testing.T) {
	t.Run("invalid packet type", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketPUBLISH,
			Flags:           0x00,
			RemainingLength: 2,
		}
		var p PubackPacket
		_, err := p.Decode(bytes.NewReader([]byte{0x00, 0x01}), header)
		assert.ErrorIs(t, err, ErrInvalidPacketType)
	})

	t.Run("decode read error", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketPUBACK,
			Flags:           0x00,
			RemainingLength: 2,
		}
		var p PubackPacket
		_, err := p.Decode(bytes.NewReader([]byte{}), header)
		assert.Error(t, err)
	})
}
