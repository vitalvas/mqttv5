//nolint:dupl // Similar test structure for similar packet types
package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	t.Run("valid packet", func(t *testing.T) {
		valid := PubrelPacket{PacketID: 1, ReasonCode: ReasonSuccess}
		assert.NoError(t, valid.Validate())
	})

	t.Run("invalid reason code", func(t *testing.T) {
		invalid := PubrelPacket{PacketID: 1, ReasonCode: ReasonNotAuthorized}
		assert.ErrorIs(t, invalid.Validate(), ErrInvalidReasonCode)
	})

	t.Run("zero packet ID", func(t *testing.T) {
		invalid := PubrelPacket{PacketID: 0, ReasonCode: ReasonSuccess}
		assert.ErrorIs(t, invalid.Validate(), ErrInvalidPacketID)
	})
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

func TestPubrelPacketMethods(t *testing.T) {
	t.Run("Properties", func(t *testing.T) {
		p := &PubrelPacket{}
		p.Props.Set(PropReasonString, "test reason")
		props := p.Properties()
		require.NotNil(t, props)
		assert.Equal(t, "test reason", props.GetString(PropReasonString))
	})

	t.Run("GetPacketID", func(t *testing.T) {
		p := &PubrelPacket{PacketID: 12345}
		assert.Equal(t, uint16(12345), p.GetPacketID())
	})

	t.Run("SetPacketID", func(t *testing.T) {
		p := &PubrelPacket{}
		p.SetPacketID(54321)
		assert.Equal(t, uint16(54321), p.PacketID)
	})
}
