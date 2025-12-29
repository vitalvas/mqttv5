//nolint:dupl // Similar test structure for similar packet types
package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisconnectPacketType(t *testing.T) {
	p := &DisconnectPacket{}
	assert.Equal(t, PacketDISCONNECT, p.Type())
}

func TestDisconnectPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet DisconnectPacket
	}{
		{
			name:   "normal disconnect",
			packet: DisconnectPacket{ReasonCode: ReasonSuccess},
		},
		{
			name:   "disconnect with will",
			packet: DisconnectPacket{ReasonCode: ReasonDisconnectWithWill},
		},
		{
			name:   "server shutting down",
			packet: DisconnectPacket{ReasonCode: ReasonServerShuttingDown},
		},
		{
			name:   "session taken over",
			packet: DisconnectPacket{ReasonCode: ReasonSessionTakenOver},
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
			assert.Equal(t, PacketDISCONNECT, header.PacketType)
			assert.Equal(t, byte(0x00), header.Flags)

			var decoded DisconnectPacket
			_, err = decoded.Decode(&buf, header)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.ReasonCode, decoded.ReasonCode)
		})
	}
}

func TestDisconnectPacketMinimal(t *testing.T) {
	// Success with no properties encodes to minimal form
	packet := DisconnectPacket{ReasonCode: ReasonSuccess}

	var buf bytes.Buffer
	n, err := packet.Encode(&buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n) // Just fixed header

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), header.RemainingLength)

	var decoded DisconnectPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)
	assert.Equal(t, ReasonSuccess, decoded.ReasonCode)
}

func TestDisconnectPacketWithProperties(t *testing.T) {
	packet := DisconnectPacket{ReasonCode: ReasonSuccess}
	packet.Props.Set(PropSessionExpiryInterval, uint32(3600))
	packet.Props.Set(PropReasonString, "Goodbye")
	packet.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded DisconnectPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	assert.Equal(t, uint32(3600), decoded.Props.GetUint32(PropSessionExpiryInterval))
	assert.Equal(t, "Goodbye", decoded.Props.GetString(PropReasonString))
	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	require.Len(t, ups, 1)
	assert.Equal(t, "key", ups[0].Key)
}

func TestDisconnectPacketInvalidType(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPUBLISH,
		Flags:           0x00,
		RemainingLength: 0,
	}

	var p DisconnectPacket
	_, err := p.Decode(bytes.NewReader(nil), header)
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestDisconnectPacketInvalidFlags(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketDISCONNECT,
		Flags:           0x01,
		RemainingLength: 0,
	}

	var p DisconnectPacket
	_, err := p.Decode(bytes.NewReader(nil), header)
	assert.ErrorIs(t, err, ErrInvalidPacketFlags)
}

func TestDisconnectPacketValidation(t *testing.T) {
	valid := DisconnectPacket{ReasonCode: ReasonSuccess}
	assert.NoError(t, valid.Validate())

	invalid := DisconnectPacket{ReasonCode: ReasonGrantedQoS1}
	assert.ErrorIs(t, invalid.Validate(), ErrInvalidReasonCode)
}

func BenchmarkDisconnectPacketEncode(b *testing.B) {
	packet := DisconnectPacket{ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	buf.Grow(16)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

func BenchmarkDisconnectPacketDecode(b *testing.B) {
	packet := DisconnectPacket{ReasonCode: ReasonServerShuttingDown}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		r := bytes.NewReader(data)
		var header FixedHeader
		_, _ = header.Decode(r)
		var p DisconnectPacket
		_, _ = p.Decode(r, header)
	}
}

func FuzzDisconnectPacketDecode(f *testing.F) {
	packet := DisconnectPacket{ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	f.Add([]byte{0xE0, 0x00})       // Minimal
	f.Add([]byte{0xE0, 0x01, 0x00}) // With reason code

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
		if err != nil || header.PacketType != PacketDISCONNECT {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p DisconnectPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}
