package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAckPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name       string
		packetType PacketType
		flags      byte
		ack        ackPacket
	}{
		{
			name:       "minimal success",
			packetType: PacketPUBACK,
			flags:      0x00,
			ack: ackPacket{
				PacketID:   1,
				ReasonCode: ReasonSuccess,
			},
		},
		{
			name:       "with reason code",
			packetType: PacketPUBREC,
			flags:      0x00,
			ack: ackPacket{
				PacketID:   100,
				ReasonCode: ReasonNoMatchingSubscribers,
			},
		},
		{
			name:       "max packet ID",
			packetType: PacketPUBCOMP,
			flags:      0x00,
			ack: ackPacket{
				PacketID:   65535,
				ReasonCode: ReasonSuccess,
			},
		},
		{
			name:       "PUBREL with flags",
			packetType: PacketPUBREL,
			flags:      0x02,
			ack: ackPacket{
				PacketID:   12345,
				ReasonCode: ReasonPacketIDNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := encodeAck(&buf, tt.packetType, tt.flags, &tt.ack)
			require.NoError(t, err)
			assert.Greater(t, n, 0)

			var header FixedHeader
			_, err = header.Decode(&buf)
			require.NoError(t, err)
			assert.Equal(t, tt.packetType, header.PacketType)
			assert.Equal(t, tt.flags, header.Flags)

			var decoded ackPacket
			_, err = decodeAck(&buf, header, &decoded)
			require.NoError(t, err)

			assert.Equal(t, tt.ack.PacketID, decoded.PacketID)
			assert.Equal(t, tt.ack.ReasonCode, decoded.ReasonCode)
		})
	}
}

func TestAckPacketWithProperties(t *testing.T) {
	ack := ackPacket{
		PacketID:   42,
		ReasonCode: ReasonSuccess,
	}
	ack.Props.Set(PropReasonString, "test reason")
	ack.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	var buf bytes.Buffer
	n, err := encodeAck(&buf, PacketPUBACK, 0x00, &ack)
	require.NoError(t, err)
	assert.Greater(t, n, 0)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded ackPacket
	_, err = decodeAck(&buf, header, &decoded)
	require.NoError(t, err)

	assert.Equal(t, ack.PacketID, decoded.PacketID)
	assert.Equal(t, ack.ReasonCode, decoded.ReasonCode)
	assert.Equal(t, "test reason", decoded.Props.GetString(PropReasonString))
	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	require.Len(t, ups, 1)
	assert.Equal(t, "key", ups[0].Key)
	assert.Equal(t, "value", ups[0].Value)
}

func TestAckPacketDecodeMinimal(t *testing.T) {
	// Minimal packet: just packet ID (2 bytes), no reason code
	data := []byte{0x00, 0x01} // Packet ID = 1
	header := FixedHeader{
		PacketType:      PacketPUBACK,
		Flags:           0x00,
		RemainingLength: 2,
	}

	var ack ackPacket
	n, err := decodeAck(bytes.NewReader(data), header, &ack)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, uint16(1), ack.PacketID)
	assert.Equal(t, ReasonSuccess, ack.ReasonCode)
}

func TestAckPacketDecodeWithReasonCode(t *testing.T) {
	// Packet with reason code but no properties
	data := []byte{0x00, 0x01, 0x10} // Packet ID = 1, ReasonCode = 0x10 (NoMatchingSubscribers)
	header := FixedHeader{
		PacketType:      PacketPUBACK,
		Flags:           0x00,
		RemainingLength: 3,
	}

	var ack ackPacket
	n, err := decodeAck(bytes.NewReader(data), header, &ack)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, uint16(1), ack.PacketID)
	assert.Equal(t, ReasonNoMatchingSubscribers, ack.ReasonCode)
}

func TestAckPacketDecodeWithEmptyProperties(t *testing.T) {
	// Packet with reason code and empty properties
	data := []byte{0x00, 0x01, 0x00, 0x00} // Packet ID = 1, ReasonCode = 0, Properties length = 0
	header := FixedHeader{
		PacketType:      PacketPUBACK,
		Flags:           0x00,
		RemainingLength: 4,
	}

	var ack ackPacket
	n, err := decodeAck(bytes.NewReader(data), header, &ack)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, uint16(1), ack.PacketID)
	assert.Equal(t, ReasonSuccess, ack.ReasonCode)
}

func TestAckPacketDecodeReadError(t *testing.T) {
	// Empty reader - should fail reading packet ID
	header := FixedHeader{
		PacketType:      PacketPUBACK,
		Flags:           0x00,
		RemainingLength: 2,
	}

	var ack ackPacket
	_, err := decodeAck(bytes.NewReader([]byte{}), header, &ack)
	assert.Error(t, err)
}

func TestAckPacketDecodePartialPacketID(t *testing.T) {
	// Only 1 byte - should fail reading packet ID
	header := FixedHeader{
		PacketType:      PacketPUBACK,
		Flags:           0x00,
		RemainingLength: 2,
	}

	var ack ackPacket
	_, err := decodeAck(bytes.NewReader([]byte{0x00}), header, &ack)
	assert.Error(t, err)
}

// Benchmarks

func BenchmarkAckPacketEncode(b *testing.B) {
	ack := ackPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	buf.Grow(16)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = encodeAck(&buf, PacketPUBACK, 0x00, &ack)
	}
}

func BenchmarkAckPacketEncodeWithProperties(b *testing.B) {
	ack := ackPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	ack.Props.Set(PropReasonString, "OK")
	var buf bytes.Buffer
	buf.Grow(32)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = encodeAck(&buf, PacketPUBACK, 0x00, &ack)
	}
}

func BenchmarkAckPacketDecode(b *testing.B) {
	ack := ackPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	_, _ = encodeAck(&buf, PacketPUBACK, 0x00, &ack)
	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		r := bytes.NewReader(data)
		var header FixedHeader
		_, _ = header.Decode(r)
		var p ackPacket
		_, _ = decodeAck(r, header, &p)
	}
}

// Fuzz test

func FuzzAckPacketDecode(f *testing.F) {
	// Add valid seeds
	ack := ackPacket{PacketID: 1, ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	_, _ = encodeAck(&buf, PacketPUBACK, 0x00, &ack)
	f.Add(buf.Bytes())

	f.Add([]byte{0x40, 0x02, 0x00, 0x01})             // Minimal PUBACK
	f.Add([]byte{0x40, 0x03, 0x00, 0x01, 0x00})       // PUBACK with reason code
	f.Add([]byte{0x40, 0x04, 0x00, 0x01, 0x00, 0x00}) // PUBACK with empty properties

	// Add random seeds
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
		if err != nil {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var ack ackPacket
		_, _ = decodeAck(bytes.NewReader(remaining), header, &ack)
	})
}
