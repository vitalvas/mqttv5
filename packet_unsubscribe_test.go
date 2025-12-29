package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnsubscribePacketType(t *testing.T) {
	p := &UnsubscribePacket{}
	assert.Equal(t, PacketUNSUBSCRIBE, p.Type())
}

func TestUnsubscribePacketID(t *testing.T) {
	p := &UnsubscribePacket{}
	p.SetPacketID(12345)
	assert.Equal(t, uint16(12345), p.GetPacketID())
}

func TestUnsubscribePacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet UnsubscribePacket
	}{
		{
			name: "single topic filter",
			packet: UnsubscribePacket{
				PacketID:     1,
				TopicFilters: []string{"test/topic"},
			},
		},
		{
			name: "multiple topic filters",
			packet: UnsubscribePacket{
				PacketID:     100,
				TopicFilters: []string{"topic1", "topic2", "topic3"},
			},
		},
		{
			name: "max packet ID",
			packet: UnsubscribePacket{
				PacketID:     65535,
				TopicFilters: []string{"home/#"},
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
			assert.Equal(t, PacketUNSUBSCRIBE, header.PacketType)
			assert.Equal(t, byte(0x02), header.Flags)

			var decoded UnsubscribePacket
			_, err = decoded.Decode(&buf, header)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.PacketID, decoded.PacketID)
			assert.Equal(t, tt.packet.TopicFilters, decoded.TopicFilters)
		})
	}
}

func TestUnsubscribePacketWithProperties(t *testing.T) {
	packet := UnsubscribePacket{
		PacketID:     1,
		TopicFilters: []string{"test/topic"},
	}
	packet.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded UnsubscribePacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	require.Len(t, ups, 1)
	assert.Equal(t, "key", ups[0].Key)
}

func TestUnsubscribePacketInvalidFlags(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketUNSUBSCRIBE,
		Flags:           0x00, // Should be 0x02
		RemainingLength: 10,
	}

	var p UnsubscribePacket
	_, err := p.Decode(bytes.NewReader(make([]byte, 10)), header)
	assert.ErrorIs(t, err, ErrInvalidPacketFlags)
}

func TestUnsubscribePacketInvalidType(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPUBLISH,
		Flags:           0x02,
		RemainingLength: 10,
	}

	var p UnsubscribePacket
	_, err := p.Decode(bytes.NewReader(make([]byte, 10)), header)
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestUnsubscribePacketValidation(t *testing.T) {
	tests := []struct {
		name    string
		packet  UnsubscribePacket
		wantErr error
	}{
		{
			name: "valid",
			packet: UnsubscribePacket{
				PacketID:     1,
				TopicFilters: []string{"test"},
			},
			wantErr: nil,
		},
		{
			name: "zero packet ID",
			packet: UnsubscribePacket{
				PacketID:     0,
				TopicFilters: []string{"test"},
			},
			wantErr: ErrInvalidPacketID,
		},
		{
			name: "no topic filters",
			packet: UnsubscribePacket{
				PacketID:     1,
				TopicFilters: []string{},
			},
			wantErr: ErrProtocolViolation,
		},
		{
			name: "empty topic filter",
			packet: UnsubscribePacket{
				PacketID:     1,
				TopicFilters: []string{""},
			},
			wantErr: ErrProtocolViolation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.packet.Validate()
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func BenchmarkUnsubscribePacketEncode(b *testing.B) {
	packet := UnsubscribePacket{
		PacketID:     1,
		TopicFilters: []string{"test/topic"},
	}
	var buf bytes.Buffer
	buf.Grow(64)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

func BenchmarkUnsubscribePacketDecode(b *testing.B) {
	packet := UnsubscribePacket{
		PacketID:     1,
		TopicFilters: []string{"test/topic"},
	}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		r := bytes.NewReader(data)
		var header FixedHeader
		_, _ = header.Decode(r)
		var p UnsubscribePacket
		_, _ = p.Decode(r, header)
	}
}

func FuzzUnsubscribePacketDecode(f *testing.F) {
	packet := UnsubscribePacket{
		PacketID:     1,
		TopicFilters: []string{"test/topic"},
	}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	// Multiple topics
	packet2 := UnsubscribePacket{
		PacketID:     100,
		TopicFilters: []string{"a", "b", "c"},
	}
	buf.Reset()
	_, _ = packet2.Encode(&buf)
	f.Add(buf.Bytes())

	for range 10 {
		size := rand.IntN(64) + 1
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
		if err != nil || header.PacketType != PacketUNSUBSCRIBE {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p UnsubscribePacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}
