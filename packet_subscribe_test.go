package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribePacketType(t *testing.T) {
	p := &SubscribePacket{}
	assert.Equal(t, PacketSUBSCRIBE, p.Type())
}

func TestSubscribePacketID(t *testing.T) {
	p := &SubscribePacket{}
	p.SetPacketID(12345)
	assert.Equal(t, uint16(12345), p.GetPacketID())
}

func TestSubscribePacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet SubscribePacket
	}{
		{
			name: "single subscription QoS 0",
			packet: SubscribePacket{
				PacketID: 1,
				Subscriptions: []Subscription{
					{TopicFilter: "test/topic", QoS: 0},
				},
			},
		},
		{
			name: "single subscription QoS 1",
			packet: SubscribePacket{
				PacketID: 100,
				Subscriptions: []Subscription{
					{TopicFilter: "sensor/+/data", QoS: 1},
				},
			},
		},
		{
			name: "single subscription QoS 2",
			packet: SubscribePacket{
				PacketID: 65535,
				Subscriptions: []Subscription{
					{TopicFilter: "home/#", QoS: 2},
				},
			},
		},
		{
			name: "multiple subscriptions",
			packet: SubscribePacket{
				PacketID: 42,
				Subscriptions: []Subscription{
					{TopicFilter: "topic1", QoS: 0},
					{TopicFilter: "topic2", QoS: 1},
					{TopicFilter: "topic3", QoS: 2},
				},
			},
		},
		{
			name: "with all options",
			packet: SubscribePacket{
				PacketID: 1,
				Subscriptions: []Subscription{
					{
						TopicFilter:     "test/topic",
						QoS:             2,
						NoLocal:         true,
						RetainAsPublish: true,
						RetainHandling:  2,
					},
				},
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
			assert.Equal(t, PacketSUBSCRIBE, header.PacketType)
			assert.Equal(t, byte(0x02), header.Flags)

			var decoded SubscribePacket
			_, err = decoded.Decode(&buf, header)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.PacketID, decoded.PacketID)
			require.Len(t, decoded.Subscriptions, len(tt.packet.Subscriptions))
			for i, sub := range tt.packet.Subscriptions {
				assert.Equal(t, sub.TopicFilter, decoded.Subscriptions[i].TopicFilter)
				assert.Equal(t, sub.QoS, decoded.Subscriptions[i].QoS)
				assert.Equal(t, sub.NoLocal, decoded.Subscriptions[i].NoLocal)
				assert.Equal(t, sub.RetainAsPublish, decoded.Subscriptions[i].RetainAsPublish)
				assert.Equal(t, sub.RetainHandling, decoded.Subscriptions[i].RetainHandling)
			}
		})
	}
}

func TestSubscribePacketWithProperties(t *testing.T) {
	packet := SubscribePacket{
		PacketID: 1,
		Subscriptions: []Subscription{
			{TopicFilter: "test/topic", QoS: 1},
		},
	}
	packet.Props.Set(PropSubscriptionIdentifier, uint32(100))
	packet.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded SubscribePacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	assert.Equal(t, uint32(100), decoded.Props.GetUint32(PropSubscriptionIdentifier))
	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	require.Len(t, ups, 1)
	assert.Equal(t, "key", ups[0].Key)
	assert.Equal(t, "value", ups[0].Value)
}

func TestSubscribePacketInvalidFlags(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketSUBSCRIBE,
		Flags:           0x00, // Should be 0x02
		RemainingLength: 10,
	}

	var p SubscribePacket
	_, err := p.Decode(bytes.NewReader(make([]byte, 10)), header)
	assert.ErrorIs(t, err, ErrInvalidPacketFlags)
}

func TestSubscribePacketInvalidType(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPUBLISH,
		Flags:           0x02,
		RemainingLength: 10,
	}

	var p SubscribePacket
	_, err := p.Decode(bytes.NewReader(make([]byte, 10)), header)
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestSubscribePacketValidation(t *testing.T) {
	tests := []struct {
		name    string
		packet  SubscribePacket
		wantErr error
	}{
		{
			name: "valid",
			packet: SubscribePacket{
				PacketID:      1,
				Subscriptions: []Subscription{{TopicFilter: "test", QoS: 0}},
			},
			wantErr: nil,
		},
		{
			name: "zero packet ID",
			packet: SubscribePacket{
				PacketID:      0,
				Subscriptions: []Subscription{{TopicFilter: "test", QoS: 0}},
			},
			wantErr: ErrInvalidPacketID,
		},
		{
			name: "no subscriptions",
			packet: SubscribePacket{
				PacketID:      1,
				Subscriptions: []Subscription{},
			},
			wantErr: ErrProtocolViolation,
		},
		{
			name: "empty topic filter",
			packet: SubscribePacket{
				PacketID:      1,
				Subscriptions: []Subscription{{TopicFilter: "", QoS: 0}},
			},
			wantErr: ErrProtocolViolation,
		},
		{
			name: "invalid QoS",
			packet: SubscribePacket{
				PacketID:      1,
				Subscriptions: []Subscription{{TopicFilter: "test", QoS: 3}},
			},
			wantErr: ErrInvalidQoS,
		},
		{
			name: "invalid retain handling",
			packet: SubscribePacket{
				PacketID:      1,
				Subscriptions: []Subscription{{TopicFilter: "test", QoS: 0, RetainHandling: 3}},
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

func TestSubscribePacketDecodeReservedBits(t *testing.T) {
	// Build a packet with reserved bits set in subscription options
	var buf bytes.Buffer

	// Fixed header
	buf.WriteByte(0x82) // SUBSCRIBE with flags 0x02
	buf.WriteByte(0x08) // Remaining length

	// Packet ID
	buf.Write([]byte{0x00, 0x01})

	// Properties length (0)
	buf.WriteByte(0x00)

	// Topic filter "test"
	buf.Write([]byte{0x00, 0x04, 't', 'e', 's', 't'})

	// Options with reserved bits set (0xC0)
	buf.WriteByte(0xC0)

	r := bytes.NewReader(buf.Bytes())

	var header FixedHeader
	_, err := header.Decode(r)
	require.NoError(t, err)

	var p SubscribePacket
	_, err = p.Decode(r, header)
	assert.ErrorIs(t, err, ErrProtocolViolation)
}

func TestSubscribePacketSubscriptionIdentifierAttachment(t *testing.T) {
	t.Run("subscription identifier attached to all subscriptions", func(t *testing.T) {
		packet := SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "topic1", QoS: 0},
				{TopicFilter: "topic2", QoS: 1},
				{TopicFilter: "topic3", QoS: 2},
			},
		}
		packet.Props.Set(PropSubscriptionIdentifier, uint32(12345))

		var buf bytes.Buffer
		_, err := packet.Encode(&buf)
		require.NoError(t, err)

		r := bytes.NewReader(buf.Bytes())

		var header FixedHeader
		_, err = header.Decode(r)
		require.NoError(t, err)

		var decoded SubscribePacket
		_, err = decoded.Decode(r, header)
		require.NoError(t, err)

		// Verify each subscription has the subscription identifier attached
		require.Len(t, decoded.Subscriptions, 3)
		for i, sub := range decoded.Subscriptions {
			assert.Equal(t, uint32(12345), sub.SubscriptionID, "subscription %d should have SubscriptionID attached", i)
		}
	})

	t.Run("no subscription identifier means zero in subscriptions", func(t *testing.T) {
		packet := SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "topic1", QoS: 0},
			},
		}
		// No subscription identifier property set

		var buf bytes.Buffer
		_, err := packet.Encode(&buf)
		require.NoError(t, err)

		r := bytes.NewReader(buf.Bytes())

		var header FixedHeader
		_, err = header.Decode(r)
		require.NoError(t, err)

		var decoded SubscribePacket
		_, err = decoded.Decode(r, header)
		require.NoError(t, err)

		require.Len(t, decoded.Subscriptions, 1)
		assert.Equal(t, uint32(0), decoded.Subscriptions[0].SubscriptionID)
	})
}

func TestSubscribePacketSubscriptionIdentifierValidation(t *testing.T) {
	t.Run("valid subscription identifier", func(t *testing.T) {
		// Create a valid subscribe packet with subscription identifier
		packet := SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test", QoS: 0},
			},
		}
		packet.Props.Set(PropSubscriptionIdentifier, uint32(100))

		var buf bytes.Buffer
		_, err := packet.Encode(&buf)
		require.NoError(t, err)

		r := bytes.NewReader(buf.Bytes())

		var header FixedHeader
		_, err = header.Decode(r)
		require.NoError(t, err)

		var decoded SubscribePacket
		_, err = decoded.Decode(r, header)
		assert.NoError(t, err)
	})

	t.Run("subscription identifier zero is invalid", func(t *testing.T) {
		// Build packet manually with subscription identifier = 0
		var buf bytes.Buffer

		// Packet ID
		buf.Write([]byte{0x00, 0x01})

		// Properties: length (2) + PropSubscriptionIdentifier (0x0B) + value 0
		buf.WriteByte(0x02) // Properties length
		buf.WriteByte(0x0B) // PropSubscriptionIdentifier
		buf.WriteByte(0x00) // Value = 0 (invalid)

		// Topic filter "test"
		buf.Write([]byte{0x00, 0x04, 't', 'e', 's', 't'})

		// Subscription options
		buf.WriteByte(0x00)

		header := FixedHeader{
			PacketType:      PacketSUBSCRIBE,
			Flags:           0x02,
			RemainingLength: uint32(buf.Len()),
		}

		r := bytes.NewReader(buf.Bytes())

		var p SubscribePacket
		_, err := p.Decode(r, header)
		assert.ErrorIs(t, err, ErrInvalidSubscriptionID)
	})

	t.Run("subscription identifier at maximum is valid", func(t *testing.T) {
		// Test with max valid value: 268435455 (0x0FFFFFFF)
		packet := SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test", QoS: 0},
			},
		}
		packet.Props.Set(PropSubscriptionIdentifier, uint32(268435455))

		var buf bytes.Buffer
		_, err := packet.Encode(&buf)
		require.NoError(t, err)

		r := bytes.NewReader(buf.Bytes())

		var header FixedHeader
		_, err = header.Decode(r)
		require.NoError(t, err)

		var decoded SubscribePacket
		_, err = decoded.Decode(r, header)
		assert.NoError(t, err)
	})
}

func BenchmarkSubscribePacketEncode(b *testing.B) {
	packet := SubscribePacket{
		PacketID: 1,
		Subscriptions: []Subscription{
			{TopicFilter: "test/topic", QoS: 1},
		},
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

func BenchmarkSubscribePacketEncodeMultiple(b *testing.B) {
	packet := SubscribePacket{
		PacketID: 1,
		Subscriptions: []Subscription{
			{TopicFilter: "topic1", QoS: 0},
			{TopicFilter: "topic2", QoS: 1},
			{TopicFilter: "topic3", QoS: 2},
		},
	}
	var buf bytes.Buffer
	buf.Grow(128)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

func BenchmarkSubscribePacketDecode(b *testing.B) {
	packet := SubscribePacket{
		PacketID: 1,
		Subscriptions: []Subscription{
			{TopicFilter: "test/topic", QoS: 1},
		},
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
		var p SubscribePacket
		_, _ = p.Decode(r, header)
	}
}

func FuzzSubscribePacketDecode(f *testing.F) {
	packet := SubscribePacket{
		PacketID: 1,
		Subscriptions: []Subscription{
			{TopicFilter: "test/topic", QoS: 1},
		},
	}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	// Multiple subscriptions
	packet2 := SubscribePacket{
		PacketID: 100,
		Subscriptions: []Subscription{
			{TopicFilter: "a", QoS: 0},
			{TopicFilter: "b", QoS: 1},
		},
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
		if err != nil || header.PacketType != PacketSUBSCRIBE {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p SubscribePacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}

func TestSubscribePacketProperties(t *testing.T) {
	p := &SubscribePacket{}
	p.Props.Set(PropSubscriptionIdentifier, uint32(12345))
	props := p.Properties()
	require.NotNil(t, props)
	assert.Equal(t, uint32(12345), props.GetUint32(PropSubscriptionIdentifier))
}
