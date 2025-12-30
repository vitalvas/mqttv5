package mqttv5

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadWritePacketRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		packet Packet
	}{
		{
			name: "CONNECT",
			packet: &ConnectPacket{
				ClientID:   "test-client",
				CleanStart: true,
				KeepAlive:  60,
			},
		},
		{
			name: "CONNACK",
			packet: &ConnackPacket{
				SessionPresent: true,
				ReasonCode:     ReasonSuccess,
			},
		},
		{
			name: "PUBLISH QoS0",
			packet: &PublishPacket{
				Topic:   "test/topic",
				Payload: []byte("hello"),
				QoS:     0,
			},
		},
		{
			name: "PUBLISH QoS1",
			packet: &PublishPacket{
				Topic:    "test/topic",
				Payload:  []byte("hello"),
				QoS:      1,
				PacketID: 1,
			},
		},
		{
			name:   "PUBACK",
			packet: &PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		},
		{
			name:   "PUBREC",
			packet: &PubrecPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		},
		{
			name:   "PUBREL",
			packet: &PubrelPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		},
		{
			name:   "PUBCOMP",
			packet: &PubcompPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		},
		{
			name: "SUBSCRIBE",
			packet: &SubscribePacket{
				PacketID: 1,
				Subscriptions: []Subscription{
					{TopicFilter: "test/#", QoS: 1},
				},
			},
		},
		{
			name: "SUBACK",
			packet: &SubackPacket{
				PacketID:    1,
				ReasonCodes: []ReasonCode{ReasonGrantedQoS1},
			},
		},
		{
			name: "UNSUBSCRIBE",
			packet: &UnsubscribePacket{
				PacketID:     1,
				TopicFilters: []string{"test/#"},
			},
		},
		{
			name: "UNSUBACK",
			packet: &UnsubackPacket{
				PacketID:    1,
				ReasonCodes: []ReasonCode{ReasonSuccess},
			},
		},
		{
			name:   "PINGREQ",
			packet: &PingreqPacket{},
		},
		{
			name:   "PINGRESP",
			packet: &PingrespPacket{},
		},
		{
			name:   "DISCONNECT",
			packet: &DisconnectPacket{ReasonCode: ReasonSuccess},
		},
		{
			name:   "AUTH",
			packet: &AuthPacket{ReasonCode: ReasonSuccess},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := WritePacket(&buf, tt.packet, 0)
			require.NoError(t, err)
			assert.Greater(t, n, 0)

			decoded, rn, err := ReadPacket(&buf, 0)
			require.NoError(t, err)
			assert.Equal(t, n, rn)
			assert.Equal(t, tt.packet.Type(), decoded.Type())
		})
	}
}

func TestReadPacketMaxSize(t *testing.T) {
	t.Run("exceeds max size", func(t *testing.T) {
		packet := &PublishPacket{
			Topic:   "test/topic",
			Payload: make([]byte, 1000),
			QoS:     0,
		}

		var buf bytes.Buffer
		_, err := WritePacket(&buf, packet, 0)
		require.NoError(t, err)

		_, _, err = ReadPacket(bytes.NewReader(buf.Bytes()), 100)
		assert.ErrorIs(t, err, ErrPacketTooLarge)
	})

	t.Run("exact max size allowed", func(t *testing.T) {
		packet := &PublishPacket{
			Topic:   "t",
			Payload: []byte("hello"),
			QoS:     0,
		}

		var buf bytes.Buffer
		n, err := WritePacket(&buf, packet, 0)
		require.NoError(t, err)

		// Read with exact size limit (remaining length, not total)
		decoded, _, err := ReadPacket(bytes.NewReader(buf.Bytes()), uint32(n))
		require.NoError(t, err)
		assert.Equal(t, PacketPUBLISH, decoded.Type())
	})

	t.Run("zero max size disables check", func(t *testing.T) {
		packet := &PublishPacket{
			Topic:   "test/topic",
			Payload: make([]byte, 10000),
			QoS:     0,
		}

		var buf bytes.Buffer
		_, err := WritePacket(&buf, packet, 0)
		require.NoError(t, err)

		decoded, _, err := ReadPacket(bytes.NewReader(buf.Bytes()), 0)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBLISH, decoded.Type())
	})

	t.Run("various payload sizes", func(t *testing.T) {
		sizes := []int{0, 1, 127, 128, 255, 256, 16383, 16384}
		for _, size := range sizes {
			packet := &PublishPacket{
				Topic:   "t",
				Payload: make([]byte, size),
				QoS:     0,
			}

			var buf bytes.Buffer
			n, err := WritePacket(&buf, packet, 0)
			require.NoError(t, err, "size=%d", size)

			// Should succeed with sufficient max size
			decoded, rn, err := ReadPacket(bytes.NewReader(buf.Bytes()), uint32(n+100))
			require.NoError(t, err, "size=%d", size)
			assert.Equal(t, n, rn, "size=%d", size)
			assert.Equal(t, PacketPUBLISH, decoded.Type(), "size=%d", size)
		}
	})
}

func TestWritePacketMaxSize(t *testing.T) {
	t.Run("exceeds max size", func(t *testing.T) {
		packet := &PublishPacket{
			Topic:   "test/topic",
			Payload: make([]byte, 1000),
			QoS:     0,
		}

		var buf bytes.Buffer
		_, err := WritePacket(&buf, packet, 100)
		assert.ErrorIs(t, err, ErrPacketTooLarge)
	})

	t.Run("exact max size allowed", func(t *testing.T) {
		packet := &PublishPacket{
			Topic:   "t",
			Payload: []byte("hi"),
			QoS:     0,
		}

		// First encode to find exact size
		var sizeBuf bytes.Buffer
		n, err := WritePacket(&sizeBuf, packet, 0)
		require.NoError(t, err)

		// Now write with exact max size
		var buf bytes.Buffer
		written, err := WritePacket(&buf, packet, uint32(n))
		require.NoError(t, err)
		assert.Equal(t, n, written)
	})

	t.Run("one byte over fails", func(t *testing.T) {
		packet := &PublishPacket{
			Topic:   "t",
			Payload: []byte("hi"),
			QoS:     0,
		}

		var sizeBuf bytes.Buffer
		n, err := WritePacket(&sizeBuf, packet, 0)
		require.NoError(t, err)

		var buf bytes.Buffer
		_, err = WritePacket(&buf, packet, uint32(n-1))
		assert.ErrorIs(t, err, ErrPacketTooLarge)
	})

	t.Run("zero max size disables check", func(t *testing.T) {
		packet := &PublishPacket{
			Topic:   "test/topic",
			Payload: make([]byte, 10000),
			QoS:     0,
		}

		var buf bytes.Buffer
		n, err := WritePacket(&buf, packet, 0)
		require.NoError(t, err)
		assert.Greater(t, n, 10000)
	})

	t.Run("all packet types", func(t *testing.T) {
		packets := []Packet{
			&ConnectPacket{ClientID: "test-client-id", CleanStart: true, KeepAlive: 60},
			&ConnackPacket{ReasonCode: ReasonSuccess},
			&PublishPacket{Topic: "test/topic", Payload: []byte("payload"), QoS: 1, PacketID: 1},
			&PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess},
			&SubscribePacket{PacketID: 1, Subscriptions: []Subscription{{TopicFilter: "test/#", QoS: 1}}},
			&SubackPacket{PacketID: 1, ReasonCodes: []ReasonCode{ReasonGrantedQoS1}},
			&DisconnectPacket{ReasonCode: ReasonSuccess},
		}

		for _, pkt := range packets {
			// Get actual size
			var sizeBuf bytes.Buffer
			n, err := WritePacket(&sizeBuf, pkt, 0)
			require.NoError(t, err, "packet=%s", pkt.Type())

			// Should fail with smaller max
			var buf bytes.Buffer
			_, err = WritePacket(&buf, pkt, uint32(n-1))
			assert.ErrorIs(t, err, ErrPacketTooLarge, "packet=%s", pkt.Type())

			// Should succeed with exact max
			buf.Reset()
			_, err = WritePacket(&buf, pkt, uint32(n))
			require.NoError(t, err, "packet=%s", pkt.Type())
		}
	})
}

func TestMaxPacketSizeBoundaries(t *testing.T) {
	t.Run("varint boundary 127 bytes", func(t *testing.T) {
		// 127 is max 1-byte varint
		packet := &PublishPacket{
			Topic:   "t",
			Payload: make([]byte, 120), // ~124 bytes remaining length
			QoS:     0,
		}

		var buf bytes.Buffer
		n, err := WritePacket(&buf, packet, 0)
		require.NoError(t, err)

		decoded, rn, err := ReadPacket(bytes.NewReader(buf.Bytes()), uint32(n))
		require.NoError(t, err)
		assert.Equal(t, n, rn)
		pub := decoded.(*PublishPacket)
		assert.Len(t, pub.Payload, 120)
	})

	t.Run("varint boundary 16383 bytes", func(t *testing.T) {
		// 16383 is max 2-byte varint
		packet := &PublishPacket{
			Topic:   "t",
			Payload: make([]byte, 16375),
			QoS:     0,
		}

		var buf bytes.Buffer
		n, err := WritePacket(&buf, packet, 0)
		require.NoError(t, err)

		decoded, rn, err := ReadPacket(bytes.NewReader(buf.Bytes()), uint32(n))
		require.NoError(t, err)
		assert.Equal(t, n, rn)
		pub := decoded.(*PublishPacket)
		assert.Len(t, pub.Payload, 16375)
	})

	t.Run("large packet 1MB", func(t *testing.T) {
		payload := make([]byte, 1024*1024)
		packet := &PublishPacket{
			Topic:   "t",
			Payload: payload,
			QoS:     0,
		}

		var buf bytes.Buffer
		n, err := WritePacket(&buf, packet, 0)
		require.NoError(t, err)

		// Should fail with 512KB limit
		_, _, err = ReadPacket(bytes.NewReader(buf.Bytes()), 512*1024)
		assert.ErrorIs(t, err, ErrPacketTooLarge)

		// Should succeed with 2MB limit
		decoded, rn, err := ReadPacket(bytes.NewReader(buf.Bytes()), 2*1024*1024)
		require.NoError(t, err)
		assert.Equal(t, n, rn)
		pub := decoded.(*PublishPacket)
		assert.Len(t, pub.Payload, 1024*1024)
	})
}

func TestReadPacketUnknownType(t *testing.T) {
	// Packet type 0 is reserved/invalid - fixed header decoder catches this
	data := []byte{0x00, 0x00}
	_, _, err := ReadPacket(bytes.NewReader(data), 0)
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestReadPacketIncomplete(t *testing.T) {
	// Valid header but incomplete payload
	data := []byte{0x30, 0x10} // PUBLISH with 16 bytes remaining, but no payload
	_, _, err := ReadPacket(bytes.NewReader(data), 0)
	assert.Error(t, err)
}

func TestWritePacketValidationError(t *testing.T) {
	// Invalid packet - SUBSCRIBE with no subscriptions
	packet := &SubscribePacket{
		PacketID:      1,
		Subscriptions: []Subscription{},
	}

	var buf bytes.Buffer
	_, err := WritePacket(&buf, packet, 0)
	assert.ErrorIs(t, err, ErrProtocolViolation)
}

func TestReadPacketAllTypes(t *testing.T) {
	// Test that all packet types can be read
	packetTypes := []struct {
		packetType PacketType
		packet     Packet
	}{
		{PacketCONNECT, &ConnectPacket{ClientID: "c", CleanStart: true}},
		{PacketCONNACK, &ConnackPacket{ReasonCode: ReasonSuccess}},
		{PacketPUBLISH, &PublishPacket{Topic: "t", QoS: 0}},
		{PacketPUBACK, &PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess}},
		{PacketPUBREC, &PubrecPacket{PacketID: 1, ReasonCode: ReasonSuccess}},
		{PacketPUBREL, &PubrelPacket{PacketID: 1, ReasonCode: ReasonSuccess}},
		{PacketPUBCOMP, &PubcompPacket{PacketID: 1, ReasonCode: ReasonSuccess}},
		{PacketSUBSCRIBE, &SubscribePacket{PacketID: 1, Subscriptions: []Subscription{{TopicFilter: "t", QoS: 0}}}},
		{PacketSUBACK, &SubackPacket{PacketID: 1, ReasonCodes: []ReasonCode{ReasonSuccess}}},
		{PacketUNSUBSCRIBE, &UnsubscribePacket{PacketID: 1, TopicFilters: []string{"t"}}},
		{PacketUNSUBACK, &UnsubackPacket{PacketID: 1, ReasonCodes: []ReasonCode{ReasonSuccess}}},
		{PacketPINGREQ, &PingreqPacket{}},
		{PacketPINGRESP, &PingrespPacket{}},
		{PacketDISCONNECT, &DisconnectPacket{ReasonCode: ReasonSuccess}},
		{PacketAUTH, &AuthPacket{ReasonCode: ReasonSuccess}},
	}

	for _, tt := range packetTypes {
		t.Run(tt.packetType.String(), func(t *testing.T) {
			var buf bytes.Buffer
			_, err := WritePacket(&buf, tt.packet, 0)
			require.NoError(t, err)

			decoded, _, err := ReadPacket(&buf, 0)
			require.NoError(t, err)
			assert.Equal(t, tt.packetType, decoded.Type())
		})
	}
}

func BenchmarkReadPacket(b *testing.B) {
	packet := &PublishPacket{
		Topic:    "test/topic",
		Payload:  []byte("hello world"),
		QoS:      1,
		PacketID: 1,
	}
	var buf bytes.Buffer
	_, _ = WritePacket(&buf, packet, 0)
	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _, _ = ReadPacket(bytes.NewReader(data), 0)
	}
}

func BenchmarkWritePacket(b *testing.B) {
	packet := &PublishPacket{
		Topic:    "test/topic",
		Payload:  []byte("hello world"),
		QoS:      1,
		PacketID: 1,
	}
	var buf bytes.Buffer
	buf.Grow(64)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = WritePacket(&buf, packet, 0)
	}
}

func BenchmarkReadWriteRoundTrip(b *testing.B) {
	packet := &PublishPacket{
		Topic:    "test/topic",
		Payload:  []byte("hello world"),
		QoS:      1,
		PacketID: 1,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		var buf bytes.Buffer
		_, _ = WritePacket(&buf, packet, 0)
		_, _, _ = ReadPacket(&buf, 0)
	}
}

func BenchmarkWritePacketWithMaxSize(b *testing.B) {
	packet := &PublishPacket{
		Topic:    "test/topic",
		Payload:  []byte("hello world"),
		QoS:      1,
		PacketID: 1,
	}
	var buf bytes.Buffer
	buf.Grow(64)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = WritePacket(&buf, packet, 1024)
	}
}

func BenchmarkReadPacketWithMaxSize(b *testing.B) {
	packet := &PublishPacket{
		Topic:    "test/topic",
		Payload:  []byte("hello world"),
		QoS:      1,
		PacketID: 1,
	}
	var buf bytes.Buffer
	_, _ = WritePacket(&buf, packet, 0)
	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _, _ = ReadPacket(bytes.NewReader(data), 1024)
	}
}

func BenchmarkLargePacketEncode(b *testing.B) {
	sizes := []int{1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024}

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			packet := &PublishPacket{
				Topic:    "test/topic",
				Payload:  make([]byte, size),
				QoS:      1,
				PacketID: 1,
			}
			var buf bytes.Buffer
			buf.Grow(size + 100)

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				buf.Reset()
				_, _ = WritePacket(&buf, packet, 0)
			}
		})
	}
}

func BenchmarkLargePacketDecode(b *testing.B) {
	sizes := []int{1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024}

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			packet := &PublishPacket{
				Topic:    "test/topic",
				Payload:  make([]byte, size),
				QoS:      1,
				PacketID: 1,
			}
			var buf bytes.Buffer
			_, _ = WritePacket(&buf, packet, 0)
			data := buf.Bytes()

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				_, _, _ = ReadPacket(bytes.NewReader(data), 0)
			}
		})
	}
}

func formatSize(size int) string {
	switch {
	case size >= 1024*1024:
		return fmt.Sprintf("%dMB", size/(1024*1024))
	case size >= 1024:
		return fmt.Sprintf("%dKB", size/1024)
	default:
		return fmt.Sprintf("%dB", size)
	}
}

func FuzzReadPacket(f *testing.F) {
	// Add valid packet seeds
	packets := []Packet{
		&ConnectPacket{ClientID: "test", CleanStart: true},
		&ConnackPacket{ReasonCode: ReasonSuccess},
		&PublishPacket{Topic: "t", QoS: 0},
		&PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		&PubrecPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		&PubrelPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		&PubcompPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		&SubscribePacket{PacketID: 1, Subscriptions: []Subscription{{TopicFilter: "t", QoS: 0}}},
		&SubackPacket{PacketID: 1, ReasonCodes: []ReasonCode{ReasonSuccess}},
		&UnsubscribePacket{PacketID: 1, TopicFilters: []string{"t"}},
		&UnsubackPacket{PacketID: 1, ReasonCodes: []ReasonCode{ReasonSuccess}},
		&PingreqPacket{},
		&PingrespPacket{},
		&DisconnectPacket{ReasonCode: ReasonSuccess},
		&AuthPacket{ReasonCode: ReasonSuccess},
	}

	for _, p := range packets {
		var buf bytes.Buffer
		_, _ = WritePacket(&buf, p, 0)
		f.Add(buf.Bytes())
	}

	// Add random seeds
	for range 10 {
		size := rand.IntN(128) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(data)
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _, _ = ReadPacket(bytes.NewReader(data), 0)
	})
}
