//nolint:dupl // Similar test structure for similar packet types
package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	t.Run("valid packet", func(t *testing.T) {
		valid := PubcompPacket{PacketID: 1, ReasonCode: ReasonSuccess}
		assert.NoError(t, valid.Validate())
	})

	t.Run("invalid reason code", func(t *testing.T) {
		invalid := PubcompPacket{PacketID: 1, ReasonCode: ReasonNotAuthorized}
		assert.ErrorIs(t, invalid.Validate(), ErrInvalidReasonCode)
	})

	t.Run("zero packet ID", func(t *testing.T) {
		invalid := PubcompPacket{PacketID: 0, ReasonCode: ReasonSuccess}
		assert.ErrorIs(t, invalid.Validate(), ErrInvalidPacketID)
	})
}

func TestPubcompPacketEncodeErrors(t *testing.T) {
	t.Run("encode with validation error", func(t *testing.T) {
		invalid := PubcompPacket{PacketID: 0, ReasonCode: ReasonSuccess}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrInvalidPacketID)
	})

	t.Run("encode with invalid reason code", func(t *testing.T) {
		invalid := PubcompPacket{PacketID: 1, ReasonCode: ReasonNotAuthorized}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrInvalidReasonCode)
	})

	t.Run("encode with invalid property", func(t *testing.T) {
		invalid := PubcompPacket{PacketID: 1, ReasonCode: ReasonSuccess}
		invalid.Props.Set(PropServerKeepAlive, uint16(60)) // Not valid for PUBCOMP
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.Error(t, err)
	})
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

func TestPubcompPacketMethods(t *testing.T) {
	t.Run("Properties", func(t *testing.T) {
		p := &PubcompPacket{}
		p.Props.Set(PropReasonString, "test reason")
		props := p.Properties()
		require.NotNil(t, props)
		assert.Equal(t, "test reason", props.GetString(PropReasonString))
	})

	t.Run("GetPacketID", func(t *testing.T) {
		p := &PubcompPacket{PacketID: 12345}
		assert.Equal(t, uint16(12345), p.GetPacketID())
	})

	t.Run("SetPacketID", func(t *testing.T) {
		p := &PubcompPacket{}
		p.SetPacketID(54321)
		assert.Equal(t, uint16(54321), p.PacketID)
	})
}

func TestPubcompPacketDecodeErrors(t *testing.T) {
	t.Run("invalid packet type", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketPUBLISH,
			Flags:           0x00,
			RemainingLength: 2,
		}
		var p PubcompPacket
		_, err := p.Decode(bytes.NewReader([]byte{0x00, 0x01}), header)
		assert.ErrorIs(t, err, ErrInvalidPacketType)
	})

	t.Run("decode read error", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketPUBCOMP,
			Flags:           0x00,
			RemainingLength: 2,
		}
		var p PubcompPacket
		_, err := p.Decode(bytes.NewReader([]byte{}), header)
		assert.Error(t, err)
	})
}
