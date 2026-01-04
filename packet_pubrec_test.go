//nolint:dupl // Similar test structure for similar packet types
package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	t.Run("valid packet", func(t *testing.T) {
		valid := PubrecPacket{PacketID: 1, ReasonCode: ReasonSuccess}
		assert.NoError(t, valid.Validate())
	})

	t.Run("invalid reason code", func(t *testing.T) {
		invalid := PubrecPacket{PacketID: 1, ReasonCode: ReasonGrantedQoS1}
		assert.ErrorIs(t, invalid.Validate(), ErrInvalidReasonCode)
	})

	t.Run("zero packet ID", func(t *testing.T) {
		invalid := PubrecPacket{PacketID: 0, ReasonCode: ReasonSuccess}
		assert.ErrorIs(t, invalid.Validate(), ErrInvalidPacketID)
	})
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

func TestPubrecPacketMethods(t *testing.T) {
	t.Run("Properties", func(t *testing.T) {
		p := &PubrecPacket{}
		p.Props.Set(PropReasonString, "test reason")
		props := p.Properties()
		require.NotNil(t, props)
		assert.Equal(t, "test reason", props.GetString(PropReasonString))
	})

	t.Run("GetPacketID", func(t *testing.T) {
		p := &PubrecPacket{PacketID: 12345}
		assert.Equal(t, uint16(12345), p.GetPacketID())
	})

	t.Run("SetPacketID", func(t *testing.T) {
		p := &PubrecPacket{}
		p.SetPacketID(54321)
		assert.Equal(t, uint16(54321), p.PacketID)
	})
}
