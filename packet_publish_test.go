package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishPacketType(t *testing.T) {
	p := &PublishPacket{}
	assert.Equal(t, PacketPUBLISH, p.Type())
}

func TestPublishPacketProperties(t *testing.T) {
	p := &PublishPacket{}
	p.Props.Set(PropPayloadFormatIndicator, byte(1))
	assert.Equal(t, byte(1), p.Properties().GetByte(PropPayloadFormatIndicator))
}

func TestPublishPacketID(t *testing.T) {
	p := &PublishPacket{}
	p.SetPacketID(12345)
	assert.Equal(t, uint16(12345), p.GetPacketID())
}

func TestPublishPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet PublishPacket
	}{
		{
			name: "QoS 0 minimal",
			packet: PublishPacket{
				Topic:   "test/topic",
				Payload: []byte("hello"),
				QoS:     0,
			},
		},
		{
			name: "QoS 1",
			packet: PublishPacket{
				Topic:    "test/topic",
				Payload:  []byte("hello"),
				QoS:      1,
				PacketID: 1,
			},
		},
		{
			name: "QoS 2",
			packet: PublishPacket{
				Topic:    "test/topic",
				Payload:  []byte("hello"),
				QoS:      2,
				PacketID: 2,
			},
		},
		{
			name: "QoS 1 DUP",
			packet: PublishPacket{
				Topic:    "test/topic",
				Payload:  []byte("hello"),
				QoS:      1,
				DUP:      true,
				PacketID: 100,
			},
		},
		{
			name: "QoS 0 RETAIN",
			packet: PublishPacket{
				Topic:   "test/topic",
				Payload: []byte("hello"),
				QoS:     0,
				Retain:  true,
			},
		},
		{
			name: "QoS 2 DUP RETAIN",
			packet: PublishPacket{
				Topic:    "test/topic",
				Payload:  []byte("hello"),
				QoS:      2,
				DUP:      true,
				Retain:   true,
				PacketID: 65535,
			},
		},
		{
			name: "empty payload",
			packet: PublishPacket{
				Topic:   "test/topic",
				Payload: nil,
				QoS:     0,
			},
		},
		{
			name: "large payload",
			packet: PublishPacket{
				Topic:   "test/topic",
				Payload: bytes.Repeat([]byte{0xAB}, 1024),
				QoS:     0,
			},
		},
		{
			name: "UTF-8 topic",
			packet: PublishPacket{
				Topic:   "test/世界/topic",
				Payload: []byte("message"),
				QoS:     0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.packet.Encode(&buf)
			require.NoError(t, err)
			assert.Greater(t, n, 0)

			// Decode fixed header first
			var header FixedHeader
			_, err = header.Decode(&buf)
			require.NoError(t, err)
			assert.Equal(t, PacketPUBLISH, header.PacketType)

			// Decode PUBLISH packet
			var decoded PublishPacket
			n3, err := decoded.Decode(&buf, header)
			require.NoError(t, err)
			assert.Equal(t, int(header.RemainingLength), n3)

			// Verify fields
			assert.Equal(t, tt.packet.Topic, decoded.Topic)
			assert.Equal(t, tt.packet.Payload, decoded.Payload)
			assert.Equal(t, tt.packet.QoS, decoded.QoS)
			assert.Equal(t, tt.packet.Retain, decoded.Retain)
			assert.Equal(t, tt.packet.DUP, decoded.DUP)
			if tt.packet.QoS > 0 {
				assert.Equal(t, tt.packet.PacketID, decoded.PacketID)
			}
		})
	}
}

func TestPublishPacketWithProperties(t *testing.T) {
	packet := PublishPacket{
		Topic:    "test/topic",
		Payload:  []byte("hello"),
		QoS:      1,
		PacketID: 1,
	}
	packet.Props.Set(PropPayloadFormatIndicator, byte(1))
	packet.Props.Set(PropMessageExpiryInterval, uint32(3600))
	packet.Props.Set(PropTopicAlias, uint16(1))
	packet.Props.Set(PropResponseTopic, "response/topic")
	packet.Props.Set(PropCorrelationData, []byte{0x01, 0x02, 0x03})
	packet.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})
	packet.Props.Add(PropSubscriptionIdentifier, uint32(123))
	packet.Props.Set(PropContentType, "text/plain")

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded PublishPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	assert.Equal(t, byte(1), decoded.Props.GetByte(PropPayloadFormatIndicator))
	assert.Equal(t, uint32(3600), decoded.Props.GetUint32(PropMessageExpiryInterval))
	assert.Equal(t, uint16(1), decoded.Props.GetUint16(PropTopicAlias))
	assert.Equal(t, "response/topic", decoded.Props.GetString(PropResponseTopic))
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded.Props.GetBinary(PropCorrelationData))
	assert.Equal(t, "text/plain", decoded.Props.GetString(PropContentType))

	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	assert.Len(t, ups, 1)
	assert.Equal(t, "key", ups[0].Key)

	subs := decoded.Props.GetAllVarInts(PropSubscriptionIdentifier)
	assert.Len(t, subs, 1)
	assert.Equal(t, uint32(123), subs[0])
}

func TestPublishPacketValidation(t *testing.T) {
	tests := []struct {
		name    string
		packet  PublishPacket
		wantErr error
	}{
		{
			name: "valid QoS 0",
			packet: PublishPacket{
				Topic:   "topic",
				Payload: []byte("data"),
				QoS:     0,
			},
			wantErr: nil,
		},
		{
			name: "valid QoS 1",
			packet: PublishPacket{
				Topic:    "topic",
				Payload:  []byte("data"),
				QoS:      1,
				PacketID: 1,
			},
			wantErr: nil,
		},
		{
			name: "valid QoS 2",
			packet: PublishPacket{
				Topic:    "topic",
				Payload:  []byte("data"),
				QoS:      2,
				PacketID: 1,
			},
			wantErr: nil,
		},
		{
			name: "invalid QoS 3",
			packet: PublishPacket{
				Topic: "topic",
				QoS:   3,
			},
			wantErr: ErrInvalidQoS,
		},
		{
			name: "DUP with QoS 0",
			packet: PublishPacket{
				Topic: "topic",
				QoS:   0,
				DUP:   true,
			},
			wantErr: ErrInvalidPacketFlags,
		},
		{
			name: "QoS 1 without packet ID",
			packet: PublishPacket{
				Topic:    "topic",
				QoS:      1,
				PacketID: 0,
			},
			wantErr: ErrPacketIDRequired,
		},
		{
			name: "QoS 2 without packet ID",
			packet: PublishPacket{
				Topic:    "topic",
				QoS:      2,
				PacketID: 0,
			},
			wantErr: ErrPacketIDRequired,
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

func TestPublishPacketDecodeErrors(t *testing.T) {
	t.Run("wrong packet type", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketCONNECT,
			RemainingLength: 10,
		}

		var p PublishPacket
		_, err := p.Decode(bytes.NewReader(make([]byte, 10)), header)
		assert.ErrorIs(t, err, ErrInvalidPacketType)
	})

	t.Run("invalid QoS 3", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketPUBLISH,
			Flags:           0x06, // QoS 3
			RemainingLength: 10,
		}

		var p PublishPacket
		_, err := p.Decode(bytes.NewReader(make([]byte, 10)), header)
		assert.ErrorIs(t, err, ErrInvalidQoS)
	})
}

func TestPublishPacketFlags(t *testing.T) {
	tests := []struct {
		name  string
		flags byte
		dup   bool
		qos   byte
		ret   bool
	}{
		{"all zero", 0x00, false, 0, false},
		{"retain", 0x01, false, 0, true},
		{"qos1", 0x02, false, 1, false},
		{"qos2", 0x04, false, 2, false},
		{"dup", 0x08, true, 0, false},
		{"all set qos1", 0x0B, true, 1, true},
		{"all set qos2", 0x0D, true, 2, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PublishPacket{
				DUP:    tt.dup,
				QoS:    tt.qos,
				Retain: tt.ret,
			}
			assert.Equal(t, tt.flags, p.flags())

			var p2 PublishPacket
			p2.setFlags(tt.flags)
			assert.Equal(t, tt.dup, p2.DUP)
			assert.Equal(t, tt.qos, p2.QoS)
			assert.Equal(t, tt.ret, p2.Retain)
		})
	}
}

func TestPublishPacketToMessage(t *testing.T) {
	packet := PublishPacket{
		Topic:   "test/topic",
		Payload: []byte("hello"),
		QoS:     1,
		Retain:  true,
	}
	packet.Props.Set(PropPayloadFormatIndicator, byte(1))
	packet.Props.Set(PropMessageExpiryInterval, uint32(3600))
	packet.Props.Set(PropContentType, "text/plain")
	packet.Props.Set(PropResponseTopic, "response/topic")
	packet.Props.Set(PropCorrelationData, []byte{0x01, 0x02})
	packet.Props.Add(PropUserProperty, StringPair{Key: "k", Value: "v"})
	packet.Props.Add(PropSubscriptionIdentifier, uint32(123))

	msg := packet.ToMessage()

	assert.Equal(t, "test/topic", msg.Topic)
	assert.Equal(t, []byte("hello"), msg.Payload)
	assert.Equal(t, byte(1), msg.QoS)
	assert.True(t, msg.Retain)
	assert.Equal(t, byte(1), msg.PayloadFormat)
	assert.Equal(t, uint32(3600), msg.MessageExpiry)
	assert.Equal(t, "text/plain", msg.ContentType)
	assert.Equal(t, "response/topic", msg.ResponseTopic)
	assert.Equal(t, []byte{0x01, 0x02}, msg.CorrelationData)
	assert.Len(t, msg.UserProperties, 1)
	assert.Equal(t, "k", msg.UserProperties[0].Key)
	assert.Len(t, msg.SubscriptionIdentifiers, 1)
	assert.Equal(t, uint32(123), msg.SubscriptionIdentifiers[0])
}

func TestPublishPacketFromMessage(t *testing.T) {
	msg := &Message{
		Topic:           "test/topic",
		Payload:         []byte("hello"),
		QoS:             2,
		Retain:          true,
		PayloadFormat:   1,
		MessageExpiry:   7200,
		ContentType:     "application/json",
		ResponseTopic:   "reply/to",
		CorrelationData: []byte{0xAB, 0xCD},
		UserProperties: []StringPair{
			{Key: "key1", Value: "value1"},
		},
	}

	var packet PublishPacket
	packet.FromMessage(msg)

	assert.Equal(t, "test/topic", packet.Topic)
	assert.Equal(t, []byte("hello"), packet.Payload)
	assert.Equal(t, byte(2), packet.QoS)
	assert.True(t, packet.Retain)
	assert.Equal(t, byte(1), packet.Props.GetByte(PropPayloadFormatIndicator))
	assert.Equal(t, uint32(7200), packet.Props.GetUint32(PropMessageExpiryInterval))
	assert.Equal(t, "application/json", packet.Props.GetString(PropContentType))
	assert.Equal(t, "reply/to", packet.Props.GetString(PropResponseTopic))
	assert.Equal(t, []byte{0xAB, 0xCD}, packet.Props.GetBinary(PropCorrelationData))

	ups := packet.Props.GetAllStringPairs(PropUserProperty)
	assert.Len(t, ups, 1)
	assert.Equal(t, "key1", ups[0].Key)
}

// Benchmarks

func BenchmarkPublishPacketEncode(b *testing.B) {
	packets := []struct {
		name   string
		packet PublishPacket
	}{
		{
			name: "minimal",
			packet: PublishPacket{
				Topic:   "t",
				Payload: []byte("x"),
				QoS:     0,
			},
		},
		{
			name: "typical",
			packet: PublishPacket{
				Topic:    "sensors/temperature/living-room",
				Payload:  []byte(`{"value": 23.5, "unit": "celsius"}`),
				QoS:      1,
				PacketID: 1,
			},
		},
		{
			name: "large payload",
			packet: PublishPacket{
				Topic:    "data/bulk",
				Payload:  bytes.Repeat([]byte{0xAB}, 4096),
				QoS:      2,
				PacketID: 100,
			},
		},
	}

	for _, tt := range packets {
		b.Run(tt.name, func(b *testing.B) {
			var buf bytes.Buffer
			buf.Grow(len(tt.packet.Payload) + 100)

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				buf.Reset()
				_, _ = tt.packet.Encode(&buf)
			}
		})
	}
}

func BenchmarkPublishPacketDecode(b *testing.B) {
	packets := []struct {
		name   string
		packet PublishPacket
	}{
		{
			name: "minimal",
			packet: PublishPacket{
				Topic:   "t",
				Payload: []byte("x"),
				QoS:     0,
			},
		},
		{
			name: "typical",
			packet: PublishPacket{
				Topic:    "sensors/temperature",
				Payload:  []byte(`{"value": 23.5}`),
				QoS:      1,
				PacketID: 1,
			},
		},
	}

	for _, tt := range packets {
		var buf bytes.Buffer
		_, _ = tt.packet.Encode(&buf)
		data := buf.Bytes()

		b.Run(tt.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				r := bytes.NewReader(data)
				var header FixedHeader
				_, _ = header.Decode(r)
				var p PublishPacket
				_, _ = p.Decode(r, header)
			}
		})
	}
}

// Fuzz tests

func FuzzPublishPacketDecode(f *testing.F) {
	// Valid PUBLISH packet seeds
	qos0Packet := PublishPacket{
		Topic:   "test/topic",
		Payload: []byte("hello"),
		QoS:     0,
	}
	var buf bytes.Buffer
	_, _ = qos0Packet.Encode(&buf)
	f.Add(buf.Bytes())

	qos1Packet := PublishPacket{
		Topic:    "test/topic",
		Payload:  []byte("hello"),
		QoS:      1,
		PacketID: 1,
	}
	buf.Reset()
	_, _ = qos1Packet.Encode(&buf)
	f.Add(buf.Bytes())

	qos2Packet := PublishPacket{
		Topic:    "test/topic",
		Payload:  []byte("hello"),
		QoS:      2,
		PacketID: 1,
		DUP:      true,
		Retain:   true,
	}
	buf.Reset()
	_, _ = qos2Packet.Encode(&buf)
	f.Add(buf.Bytes())

	// With properties
	propPacket := PublishPacket{
		Topic:    "test",
		Payload:  []byte("data"),
		QoS:      1,
		PacketID: 1,
	}
	propPacket.Props.Set(PropPayloadFormatIndicator, byte(1))
	propPacket.Props.Set(PropMessageExpiryInterval, uint32(3600))
	buf.Reset()
	_, _ = propPacket.Encode(&buf)
	f.Add(buf.Bytes())

	// Edge cases
	f.Add([]byte{0x30, 0x00})                         // Empty
	f.Add([]byte{0x30, 0x03, 0x00, 0x01, 't'})        // Minimal topic, no payload
	f.Add([]byte{0x32, 0x05, 0x00, 0x01, 't', 0x00, 0x01}) // QoS 1 with packet ID

	// Random generated seeds
	for range 10 {
		size := rand.IntN(128) + 1
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
		if header.PacketType != PacketPUBLISH {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p PublishPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}
