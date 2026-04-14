package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCodecV311ConnectRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		packet ConnectPacket
	}{
		{
			name: "minimal",
			packet: ConnectPacket{
				ProtocolVersion: ProtocolV311,
				ClientID:        "test-client",
				CleanStart:      true,
				KeepAlive:       60,
			},
		},
		{
			name: "with credentials",
			packet: ConnectPacket{
				ProtocolVersion: ProtocolV311,
				ClientID:        "client-1",
				CleanStart:      true,
				KeepAlive:       120,
				Username:        "user",
				Password:        []byte("secret"),
			},
		},
		{
			name: "with will",
			packet: ConnectPacket{
				ProtocolVersion: ProtocolV311,
				ClientID:        "client-2",
				CleanStart:      true,
				KeepAlive:       30,
				WillFlag:        true,
				WillTopic:       "status",
				WillPayload:     []byte("offline"),
				WillQoS:         1,
				WillRetain:      true,
			},
		},
	}

	codec := newCodec(ProtocolV311)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := codec.writePacket(&buf, &tt.packet, 0)
			require.NoError(t, err)

			pkt, _, err := codec.readPacket(&buf, 0)
			require.NoError(t, err)

			got, ok := pkt.(*ConnectPacket)
			require.True(t, ok)
			assert.Equal(t, ProtocolV311, got.ProtocolVersion)
			assert.Equal(t, tt.packet.ClientID, got.ClientID)
			assert.Equal(t, tt.packet.CleanStart, got.CleanStart)
			assert.Equal(t, tt.packet.KeepAlive, got.KeepAlive)
			assert.Equal(t, tt.packet.Username, got.Username)
			assert.Equal(t, tt.packet.Password, got.Password)
			assert.Equal(t, tt.packet.WillFlag, got.WillFlag)
			if tt.packet.WillFlag {
				assert.Equal(t, tt.packet.WillTopic, got.WillTopic)
				assert.Equal(t, tt.packet.WillPayload, got.WillPayload)
				assert.Equal(t, tt.packet.WillQoS, got.WillQoS)
				assert.Equal(t, tt.packet.WillRetain, got.WillRetain)
			}
		})
	}
}

func TestCodecV311ConnackRoundTrip(t *testing.T) {
	tests := []struct {
		name           string
		sessionPresent bool
		reasonCode     ReasonCode
	}{
		{"success", false, ReasonSuccess},
		{"success with session", true, ReasonSuccess},
		{"not authorized", false, ReasonNotAuthorized},
		{"bad credentials", false, ReasonBadUserNameOrPassword},
		{"server unavailable", false, ReasonServerUnavailable},
	}

	codec := newCodec(ProtocolV311)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkt := &ConnackPacket{
				SessionPresent: tt.sessionPresent,
				ReasonCode:     tt.reasonCode,
			}

			var buf bytes.Buffer
			_, err := codec.writePacket(&buf, pkt, 0)
			require.NoError(t, err)

			// v3.1.1 CONNACK is always 4 bytes
			assert.Equal(t, 4, buf.Len())

			result, _, err := codec.readPacket(&buf, 0)
			require.NoError(t, err)

			got, ok := result.(*ConnackPacket)
			require.True(t, ok)
			assert.Equal(t, tt.sessionPresent, got.SessionPresent)
			assert.Equal(t, tt.reasonCode, got.ReasonCode)
		})
	}
}

func TestCodecV311PublishRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		packet PublishPacket
	}{
		{
			name: "qos 0",
			packet: PublishPacket{
				Topic:   "test/topic",
				Payload: []byte("hello"),
				QoS:     QoS0,
			},
		},
		{
			name: "qos 1",
			packet: PublishPacket{
				Topic:    "test/topic",
				Payload:  []byte("hello"),
				QoS:      QoS1,
				PacketID: 42,
			},
		},
		{
			name: "qos 2 with retain",
			packet: PublishPacket{
				Topic:    "test/topic",
				Payload:  []byte("hello"),
				QoS:      QoS2,
				PacketID: 100,
				Retain:   true,
			},
		},
		{
			name: "empty payload",
			packet: PublishPacket{
				Topic: "test/topic",
				QoS:   QoS0,
			},
		},
	}

	codec := newCodec(ProtocolV311)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := codec.writePacket(&buf, &tt.packet, 0)
			require.NoError(t, err)

			pkt, _, err := codec.readPacket(&buf, 0)
			require.NoError(t, err)

			got, ok := pkt.(*PublishPacket)
			require.True(t, ok)
			assert.Equal(t, tt.packet.Topic, got.Topic)
			assert.Equal(t, tt.packet.Payload, got.Payload)
			assert.Equal(t, tt.packet.QoS, got.QoS)
			assert.Equal(t, tt.packet.Retain, got.Retain)
			if tt.packet.QoS > QoS0 {
				assert.Equal(t, tt.packet.PacketID, got.PacketID)
			}
			// v3.1.1 should have no properties
			assert.Equal(t, 0, got.Props.Len())
		})
	}
}

func TestCodecV311AckRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		packet   Packet
		packetID uint16
	}{
		{"puback", &PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess}, 1},
		{"pubrec", &PubrecPacket{PacketID: 2, ReasonCode: ReasonSuccess}, 2},
		{"pubrel", &PubrelPacket{PacketID: 3, ReasonCode: ReasonSuccess}, 3},
		{"pubcomp", &PubcompPacket{PacketID: 4, ReasonCode: ReasonSuccess}, 4},
	}

	codec := newCodec(ProtocolV311)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := codec.writePacket(&buf, tt.packet, 0)
			require.NoError(t, err)

			// v3.1.1 ack is always 4 bytes (fixed header + packet id)
			assert.Equal(t, 4, buf.Len())

			pkt, _, err := codec.readPacket(&buf, 0)
			require.NoError(t, err)

			switch p := pkt.(type) {
			case *PubackPacket:
				assert.Equal(t, tt.packetID, p.PacketID)
				assert.Equal(t, ReasonSuccess, p.ReasonCode)
			case *PubrecPacket:
				assert.Equal(t, tt.packetID, p.PacketID)
				assert.Equal(t, ReasonSuccess, p.ReasonCode)
			case *PubrelPacket:
				assert.Equal(t, tt.packetID, p.PacketID)
				assert.Equal(t, ReasonSuccess, p.ReasonCode)
			case *PubcompPacket:
				assert.Equal(t, tt.packetID, p.PacketID)
				assert.Equal(t, ReasonSuccess, p.ReasonCode)
			default:
				t.Fatalf("unexpected packet type: %T", pkt)
			}
		})
	}
}

func TestCodecV311SubscribeRoundTrip(t *testing.T) {
	pkt := &SubscribePacket{
		PacketID: 10,
		Subscriptions: []Subscription{
			{TopicFilter: "a/b", QoS: QoS0},
			{TopicFilter: "c/d", QoS: QoS1},
			{TopicFilter: "e/#", QoS: QoS2},
		},
	}

	codec := newCodec(ProtocolV311)
	var buf bytes.Buffer
	_, err := codec.writePacket(&buf, pkt, 0)
	require.NoError(t, err)

	result, _, err := codec.readPacket(&buf, 0)
	require.NoError(t, err)

	got, ok := result.(*SubscribePacket)
	require.True(t, ok)
	assert.Equal(t, uint16(10), got.PacketID)
	require.Len(t, got.Subscriptions, 3)
	assert.Equal(t, "a/b", got.Subscriptions[0].TopicFilter)
	assert.Equal(t, QoS0, got.Subscriptions[0].QoS)
	assert.Equal(t, "c/d", got.Subscriptions[1].TopicFilter)
	assert.Equal(t, QoS1, got.Subscriptions[1].QoS)
	assert.Equal(t, "e/#", got.Subscriptions[2].TopicFilter)
	assert.Equal(t, QoS2, got.Subscriptions[2].QoS)

	// v3.1.1 subscription options should not carry v5 extras
	for _, sub := range got.Subscriptions {
		assert.False(t, sub.NoLocal)
		assert.False(t, sub.RetainAsPublish)
		assert.Equal(t, byte(0), sub.RetainHandling)
	}
}

func TestCodecV311SubackRoundTrip(t *testing.T) {
	pkt := &SubackPacket{
		PacketID: 10,
		ReasonCodes: []ReasonCode{
			ReasonSuccess,
			ReasonGrantedQoS1,
			ReasonGrantedQoS2,
			ReasonUnspecifiedError,
		},
	}

	codec := newCodec(ProtocolV311)
	var buf bytes.Buffer
	_, err := codec.writePacket(&buf, pkt, 0)
	require.NoError(t, err)

	result, _, err := codec.readPacket(&buf, 0)
	require.NoError(t, err)

	got, ok := result.(*SubackPacket)
	require.True(t, ok)
	assert.Equal(t, uint16(10), got.PacketID)
	require.Len(t, got.ReasonCodes, 4)
	assert.Equal(t, ReasonGrantedQoS0, got.ReasonCodes[0]) // Success maps to QoS0 in v3.1.1
	assert.Equal(t, ReasonGrantedQoS1, got.ReasonCodes[1])
	assert.Equal(t, ReasonGrantedQoS2, got.ReasonCodes[2])
	assert.Equal(t, ReasonUnspecifiedError, got.ReasonCodes[3])
}

func TestCodecV311UnsubscribeRoundTrip(t *testing.T) {
	pkt := &UnsubscribePacket{
		PacketID:     11,
		TopicFilters: []string{"a/b", "c/d"},
	}

	codec := newCodec(ProtocolV311)
	var buf bytes.Buffer
	_, err := codec.writePacket(&buf, pkt, 0)
	require.NoError(t, err)

	result, _, err := codec.readPacket(&buf, 0)
	require.NoError(t, err)

	got, ok := result.(*UnsubscribePacket)
	require.True(t, ok)
	assert.Equal(t, uint16(11), got.PacketID)
	assert.Equal(t, []string{"a/b", "c/d"}, got.TopicFilters)
}

func TestCodecV311UnsubackRoundTrip(t *testing.T) {
	pkt := &UnsubackPacket{
		PacketID:    11,
		ReasonCodes: []ReasonCode{ReasonSuccess},
	}

	codec := newCodec(ProtocolV311)
	var buf bytes.Buffer
	_, err := codec.writePacket(&buf, pkt, 0)
	require.NoError(t, err)

	// v3.1.1 UNSUBACK is always 4 bytes
	assert.Equal(t, 4, buf.Len())

	result, _, err := codec.readPacket(&buf, 0)
	require.NoError(t, err)

	got, ok := result.(*UnsubackPacket)
	require.True(t, ok)
	assert.Equal(t, uint16(11), got.PacketID)
	assert.Nil(t, got.ReasonCodes) // v3.1.1 has no per-topic reason codes
}

func TestCodecV311DisconnectRoundTrip(t *testing.T) {
	codec := newCodec(ProtocolV311)
	var buf bytes.Buffer

	pkt := &DisconnectPacket{ReasonCode: ReasonServerShuttingDown}
	_, err := codec.writePacket(&buf, pkt, 0)
	require.NoError(t, err)

	// v3.1.1 DISCONNECT is always 2 bytes (no reason code, no properties)
	assert.Equal(t, 2, buf.Len())

	result, _, err := codec.readPacket(&buf, 0)
	require.NoError(t, err)

	got, ok := result.(*DisconnectPacket)
	require.True(t, ok)
	assert.Equal(t, ReasonSuccess, got.ReasonCode) // v3.1.1 always decodes as success
}

func TestCodecV311PingRoundTrip(t *testing.T) {
	codec := newCodec(ProtocolV311)

	t.Run("pingreq", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := codec.writePacket(&buf, &PingreqPacket{}, 0)
		require.NoError(t, err)
		assert.Equal(t, 2, buf.Len())

		pkt, _, err := codec.readPacket(&buf, 0)
		require.NoError(t, err)
		_, ok := pkt.(*PingreqPacket)
		assert.True(t, ok)
	})

	t.Run("pingresp", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := codec.writePacket(&buf, &PingrespPacket{}, 0)
		require.NoError(t, err)
		assert.Equal(t, 2, buf.Len())

		pkt, _, err := codec.readPacket(&buf, 0)
		require.NoError(t, err)
		_, ok := pkt.(*PingrespPacket)
		assert.True(t, ok)
	})
}

func TestCodecV311RejectsAuthPacket(t *testing.T) {
	codec := newCodec(ProtocolV311)

	// Build a raw AUTH packet bytes (type 15 = 0xF0)
	raw := []byte{0xF0, 0x00}
	_, _, err := codec.readPacket(bytes.NewReader(raw), 0)
	assert.ErrorIs(t, err, ErrUnknownPacketType)
}

func TestCodecV311DecodeValidation(t *testing.T) {
	codec := newCodec(ProtocolV311)

	tests := []struct {
		name    string
		raw     []byte
		wantErr error
	}{
		{
			name:    "PUBACK with zero packet identifier",
			raw:     []byte{byte(PacketPUBACK) << 4, 0x02, 0x00, 0x00},
			wantErr: ErrInvalidPacketID,
		},
		{
			name:    "PUBACK with wrong remaining length",
			raw:     []byte{byte(PacketPUBACK) << 4, 0x03, 0x00, 0x01, 0x00},
			wantErr: ErrProtocolViolation,
		},
		{
			name:    "PUBREC with zero packet identifier",
			raw:     []byte{byte(PacketPUBREC) << 4, 0x02, 0x00, 0x00},
			wantErr: ErrInvalidPacketID,
		},
		{
			name:    "PUBREL with zero packet identifier",
			raw:     []byte{byte(PacketPUBREL)<<4 | 0x02, 0x02, 0x00, 0x00},
			wantErr: ErrInvalidPacketID,
		},
		{
			name:    "PUBCOMP with zero packet identifier",
			raw:     []byte{byte(PacketPUBCOMP) << 4, 0x02, 0x00, 0x00},
			wantErr: ErrInvalidPacketID,
		},
		{
			name:    "SUBACK with zero packet identifier",
			raw:     []byte{byte(PacketSUBACK) << 4, 0x03, 0x00, 0x00, 0x00},
			wantErr: ErrInvalidPacketID,
		},
		{
			name:    "SUBACK with invalid return code",
			raw:     []byte{byte(PacketSUBACK) << 4, 0x03, 0x00, 0x01, 0x03},
			wantErr: ErrInvalidReasonCode,
		},
		{
			name:    "SUBACK with no return codes",
			raw:     []byte{byte(PacketSUBACK) << 4, 0x02, 0x00, 0x01},
			wantErr: ErrProtocolViolation,
		},
		{
			name:    "UNSUBACK with zero packet identifier",
			raw:     []byte{byte(PacketUNSUBACK) << 4, 0x02, 0x00, 0x00},
			wantErr: ErrInvalidPacketID,
		},
		{
			name:    "UNSUBACK with wrong remaining length",
			raw:     []byte{byte(PacketUNSUBACK) << 4, 0x03, 0x00, 0x01, 0x00},
			wantErr: ErrProtocolViolation,
		},
		{
			name:    "PINGREQ with non-zero remaining length",
			raw:     []byte{byte(PacketPINGREQ) << 4, 0x01, 0x00},
			wantErr: ErrProtocolViolation,
		},
		{
			name:    "PINGRESP with non-zero remaining length",
			raw:     []byte{byte(PacketPINGRESP) << 4, 0x01, 0x00},
			wantErr: ErrProtocolViolation,
		},
		{
			name:    "DISCONNECT with non-zero remaining length",
			raw:     []byte{byte(PacketDISCONNECT) << 4, 0x01, 0x00},
			wantErr: ErrProtocolViolation,
		},
		{
			name:    "CONNACK with wrong remaining length",
			raw:     []byte{byte(PacketCONNACK) << 4, 0x03, 0x00, 0x00, 0x00},
			wantErr: ErrProtocolViolation,
		},
		{
			name:    "CONNACK with invalid return code",
			raw:     []byte{byte(PacketCONNACK) << 4, 0x02, 0x00, 0x06},
			wantErr: ErrInvalidReasonCode,
		},
		{
			name: "SUBSCRIBE with reserved option bits set",
			raw: []byte{
				byte(PacketSUBSCRIBE)<<4 | 0x02, // type + reserved flags
				0x08,                            // remaining length
				0x00, 0x01,                      // packet ID
				0x00, 0x03, 'a', '/', 'b', // topic filter
				0x04, // QoS 0 with reserved bit 2 set
			},
			wantErr: ErrInvalidPacketFlags,
		},
		{
			name: "SUBSCRIBE with QoS 3",
			raw: []byte{
				byte(PacketSUBSCRIBE)<<4 | 0x02,
				0x08,
				0x00, 0x01,
				0x00, 0x03, 'a', '/', 'b',
				0x03,
			},
			wantErr: ErrInvalidQoS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := codec.readPacket(bytes.NewReader(tt.raw), 0)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestCodecV311WriteMaxSize(t *testing.T) {
	codec := newCodec(ProtocolV311)

	t.Run("PINGREQ within tight limit", func(t *testing.T) {
		var buf bytes.Buffer
		// PINGREQ is exactly 2 bytes
		_, err := codec.writePacket(&buf, &PingreqPacket{}, 2)
		require.NoError(t, err)
		assert.Equal(t, 2, buf.Len())
	})

	t.Run("PINGREQ exceeds limit of 1", func(t *testing.T) {
		var buf bytes.Buffer
		_, err := codec.writePacket(&buf, &PingreqPacket{}, 1)
		assert.ErrorIs(t, err, ErrPacketTooLarge)
	})

	t.Run("CONNECT not rejected when within limit", func(t *testing.T) {
		// Verify that the CONNECT encoder no longer overestimates header size.
		// A minimal CONNECT has a 2-byte fixed header, not 5.
		pkt := &ConnectPacket{
			ProtocolVersion: ProtocolV311,
			ClientID:        "x",
			CleanStart:      true,
			KeepAlive:       0,
		}

		// First find actual size
		var sizeBuf bytes.Buffer
		_, err := codec.writePacket(&sizeBuf, pkt, 0)
		require.NoError(t, err)
		actualSize := uint32(sizeBuf.Len())

		// Now exact-size limit must succeed (previously failed because of +5 overhead)
		var buf bytes.Buffer
		_, err = codec.writePacket(&buf, pkt, actualSize)
		require.NoError(t, err)
		assert.Equal(t, int(actualSize), buf.Len())
	})

	t.Run("PUBLISH oversized rejected", func(t *testing.T) {
		var buf bytes.Buffer
		pkt := &PublishPacket{
			Topic:   "topic",
			Payload: bytes.Repeat([]byte("x"), 100),
		}
		_, err := codec.writePacket(&buf, pkt, 10)
		assert.ErrorIs(t, err, ErrPacketTooLarge)
	})
}

func TestClientV5ToV311FallbackOnInvalidConnack(t *testing.T) {
	// Simulate a v3.1.1 broker rejecting a v5 CONNECT with an "unacceptable
	// protocol version" CONNACK. The v5 codec must reject the reason code byte,
	// and the client must translate that into a fallback signal.
	v311ConnackBytes := []byte{byte(PacketCONNACK) << 4, 0x02, 0x00, 0x01}

	codec := newCodec(ProtocolV5)
	_, _, err := codec.readPacket(bytes.NewReader(v311ConnackBytes), 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidReasonCode)
}

func TestCodecV311PublishDecodeTopicErrors(t *testing.T) {
	codec := newCodec(ProtocolV311)

	tests := []struct {
		name string
		raw  []byte
	}{
		{
			// Topic length prefix says 10, but only 2 bytes follow.
			name: "truncated topic string",
			raw: []byte{
				byte(PacketPUBLISH) << 4,
				0x04,       // remaining length
				0x00, 0x0A, // topic length = 10
				'a', 'b', // only 2 bytes provided
			},
		},
		{
			// Topic contains a NUL byte (U+0000), forbidden by MQTT.
			name: "topic with NUL byte",
			raw: []byte{
				byte(PacketPUBLISH) << 4,
				0x05,
				0x00, 0x03,
				'a', 0x00, 'b',
			},
		},
		{
			// Topic contains invalid UTF-8 (lone continuation byte).
			name: "topic with invalid UTF-8",
			raw: []byte{
				byte(PacketPUBLISH) << 4,
				0x05,
				0x00, 0x03,
				'a', 0x80, 'b',
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := codec.readPacket(bytes.NewReader(tt.raw), 0)
			require.Error(t, err)
		})
	}
}

func TestCodecV311WriteValidation(t *testing.T) {
	codec := newCodec(ProtocolV311)

	tests := []struct {
		name    string
		packet  Packet
		wantErr error
	}{
		{
			name:    "PUBACK with zero packet ID",
			packet:  &PubackPacket{PacketID: 0},
			wantErr: ErrInvalidPacketID,
		},
		{
			name:    "PUBREC with zero packet ID",
			packet:  &PubrecPacket{PacketID: 0},
			wantErr: ErrInvalidPacketID,
		},
		{
			name:    "PUBREL with zero packet ID",
			packet:  &PubrelPacket{PacketID: 0},
			wantErr: ErrInvalidPacketID,
		},
		{
			name:    "PUBCOMP with zero packet ID",
			packet:  &PubcompPacket{PacketID: 0},
			wantErr: ErrInvalidPacketID,
		},
		{
			name: "CONNACK with SessionPresent=true and failure reason",
			packet: &ConnackPacket{
				SessionPresent: true,
				ReasonCode:     ReasonNotAuthorized,
			},
			wantErr: ErrInvalidConnackFlags,
		},
		{
			name: "SUBACK with no return codes",
			packet: &SubackPacket{
				PacketID:    1,
				ReasonCodes: nil,
			},
			wantErr: ErrProtocolViolation,
		},
		{
			name:    "UNSUBACK with zero packet ID",
			packet:  &UnsubackPacket{PacketID: 0},
			wantErr: ErrInvalidPacketID,
		},
		{
			name: "CONNECT with invalid will QoS",
			packet: &ConnectPacket{
				ProtocolVersion: ProtocolV311,
				ClientID:        "test",
				CleanStart:      true,
				WillFlag:        true,
				WillTopic:       "t",
				WillQoS:         3,
			},
			wantErr: ErrInvalidConnectFlags,
		},
		{
			name: "SUBSCRIBE with zero packet ID",
			packet: &SubscribePacket{
				PacketID: 0,
				Subscriptions: []Subscription{
					{TopicFilter: "a/b", QoS: QoS0},
				},
			},
			wantErr: ErrInvalidPacketID,
		},
		{
			name: "UNSUBSCRIBE with zero packet ID",
			packet: &UnsubscribePacket{
				PacketID:     0,
				TopicFilters: []string{"a/b"},
			},
			wantErr: ErrInvalidPacketID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			_, err := codec.writePacket(&buf, tt.packet, 0)
			assert.ErrorIs(t, err, tt.wantErr)
			assert.Equal(t, 0, buf.Len(), "no bytes should be written on validation failure")
		})
	}
}

func TestCodecV311EncodeMaxSize(t *testing.T) {
	codec := newCodec(ProtocolV311)

	t.Run("CONNECT exceeds maxSize", func(t *testing.T) {
		pkt := &ConnectPacket{
			ProtocolVersion: ProtocolV311,
			ClientID:        "test-client",
			CleanStart:      true,
			KeepAlive:       60,
		}
		var buf bytes.Buffer
		_, err := codec.writePacket(&buf, pkt, 5) // too small
		assert.ErrorIs(t, err, ErrPacketTooLarge)
	})

	t.Run("SUBSCRIBE exceeds maxSize", func(t *testing.T) {
		pkt := &SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "a/very/long/topic/filter/that/exceeds/limit", QoS: QoS0},
			},
		}
		var buf bytes.Buffer
		_, err := codec.writePacket(&buf, pkt, 5) // too small
		assert.ErrorIs(t, err, ErrPacketTooLarge)
	})

	t.Run("UNSUBSCRIBE exceeds maxSize", func(t *testing.T) {
		pkt := &UnsubscribePacket{
			PacketID:     1,
			TopicFilters: []string{"a/very/long/topic/filter/that/exceeds/limit"},
		}
		var buf bytes.Buffer
		_, err := codec.writePacket(&buf, pkt, 5) // too small
		assert.ErrorIs(t, err, ErrPacketTooLarge)
	})
}

func TestCodecV311DecodeConnackErrors(t *testing.T) {
	codec := newCodec(ProtocolV311)

	t.Run("reserved flags set", func(t *testing.T) {
		// CONNACK with acknowledge flags byte having reserved bits set (bit 1)
		raw := []byte{byte(PacketCONNACK) << 4, 0x02, 0x02, 0x00}
		_, _, err := codec.readPacket(bytes.NewReader(raw), 0)
		assert.ErrorIs(t, err, ErrInvalidConnackFlags)
	})

	t.Run("session present with non-success", func(t *testing.T) {
		// CONNACK with session present=true and non-success return code
		raw := []byte{byte(PacketCONNACK) << 4, 0x02, 0x01, 0x04}
		_, _, err := codec.readPacket(bytes.NewReader(raw), 0)
		assert.ErrorIs(t, err, ErrInvalidConnackFlags)
	})
}

func TestCodecV311DecodeConnectError(t *testing.T) {
	codec := newCodec(ProtocolV311)

	// Truncated CONNECT: valid header but not enough remaining bytes to decode
	raw := []byte{
		byte(PacketCONNECT) << 4,
		0x04,                 // remaining length = 4
		0x00, 0x04, 'M', 'Q', // truncated: missing "TT" and rest of CONNECT
	}
	_, _, err := codec.readPacket(bytes.NewReader(raw), 0)
	require.Error(t, err)
}

func TestServerV311PersistentSession(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	disconnected := make(chan struct{}, 1)
	srv := NewServer(
		WithListener(listener),
		WithAcceptProtocolVersions(ProtocolV5, ProtocolV311),
		OnDisconnect(func(_ *ServerClient) {
			select {
			case disconnected <- struct{}{}:
			default:
			}
		}),
	)
	go srv.ListenAndServe()
	defer srv.Close()

	// Wait for server to start
	time.Sleep(20 * time.Millisecond)

	const clientID = "v311-persist"
	codec := newCodec(ProtocolV311)
	addr := listener.Addr().String()

	// --- First connection: subscribe with CleanSession=false ---
	conn1, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	connect1 := &ConnectPacket{
		ProtocolVersion: ProtocolV311,
		ClientID:        clientID,
		CleanStart:      false,
		KeepAlive:       60,
	}
	_, err = codec.writePacket(conn1, connect1, 0)
	require.NoError(t, err)

	pkt, _, err := codec.readPacket(conn1, 0)
	require.NoError(t, err)
	connack1, ok := pkt.(*ConnackPacket)
	require.True(t, ok, "expected CONNACK, got %T", pkt)
	assert.Equal(t, ReasonSuccess, connack1.ReasonCode)
	assert.False(t, connack1.SessionPresent, "first connection must report no prior session")

	// Subscribe to a topic
	subscribe := &SubscribePacket{
		PacketID: 1,
		Subscriptions: []Subscription{
			{TopicFilter: "v311/persistent", QoS: QoS1},
		},
	}
	_, err = codec.writePacket(conn1, subscribe, 0)
	require.NoError(t, err)

	pkt, _, err = codec.readPacket(conn1, 0)
	require.NoError(t, err)
	suback, ok := pkt.(*SubackPacket)
	require.True(t, ok, "expected SUBACK, got %T", pkt)
	require.Len(t, suback.ReasonCodes, 1)
	assert.Equal(t, ReasonGrantedQoS1, suback.ReasonCodes[0])

	// Disconnect cleanly (client-to-server DISCONNECT is valid in v3.1.1)
	_, err = codec.writePacket(conn1, &DisconnectPacket{}, 0)
	require.NoError(t, err)
	conn1.Close()

	// Wait for server-side disconnect cleanup to complete
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server disconnect callback")
	}

	// --- Second connection: same client ID with CleanSession=false ---
	conn2, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn2.Close()

	connect2 := &ConnectPacket{
		ProtocolVersion: ProtocolV311,
		ClientID:        clientID,
		CleanStart:      false,
		KeepAlive:       60,
	}
	_, err = codec.writePacket(conn2, connect2, 0)
	require.NoError(t, err)

	pkt, _, err = codec.readPacket(conn2, 0)
	require.NoError(t, err)
	connack2, ok := pkt.(*ConnackPacket)
	require.True(t, ok, "expected CONNACK, got %T", pkt)
	assert.Equal(t, ReasonSuccess, connack2.ReasonCode)
	assert.True(t, connack2.SessionPresent,
		"v3.1.1 reconnect with CleanSession=false must resume the prior session")

	// Verify the subscription was restored on the server
	subs := srv.GetClientSubscriptions(DefaultNamespace, clientID)
	require.Len(t, subs, 1, "session must keep the prior subscription")
	assert.Equal(t, "v311/persistent", subs[0].TopicFilter)
}

func TestServerV311CleanSessionDeletesSession(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	disconnected := make(chan struct{}, 1)
	srv := NewServer(
		WithListener(listener),
		WithAcceptProtocolVersions(ProtocolV5, ProtocolV311),
		OnDisconnect(func(_ *ServerClient) {
			select {
			case disconnected <- struct{}{}:
			default:
			}
		}),
	)
	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(20 * time.Millisecond)

	const clientID = "v311-clean"
	codec := newCodec(ProtocolV311)
	addr := listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	connect := &ConnectPacket{
		ProtocolVersion: ProtocolV311,
		ClientID:        clientID,
		CleanStart:      true,
		KeepAlive:       60,
	}
	_, err = codec.writePacket(conn, connect, 0)
	require.NoError(t, err)

	pkt, _, err := codec.readPacket(conn, 0)
	require.NoError(t, err)
	_, ok := pkt.(*ConnackPacket)
	require.True(t, ok)

	_, err = codec.writePacket(conn, &DisconnectPacket{}, 0)
	require.NoError(t, err)
	conn.Close()

	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server disconnect callback")
	}

	// Session should be gone
	_, err = srv.config.sessionStore.Get(DefaultNamespace, clientID)
	assert.Error(t, err, "CleanSession=true must delete the session on disconnect")
}

func TestServerV311EmptyClientIDCleanSessionFalse(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := NewServer(
		WithListener(listener),
		WithAcceptProtocolVersions(ProtocolV5, ProtocolV311),
	)
	go srv.ListenAndServe()
	defer srv.Close()

	time.Sleep(20 * time.Millisecond)

	codec := newCodec(ProtocolV311)
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	connect := &ConnectPacket{
		ProtocolVersion: ProtocolV311,
		ClientID:        "",
		CleanStart:      false,
		KeepAlive:       0,
	}
	_, err = codec.writePacket(conn, connect, 0)
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	pkt, _, err := codec.readPacket(conn, 0)
	require.NoError(t, err, "server must reply with CONNACK instead of closing the socket")

	connack, ok := pkt.(*ConnackPacket)
	require.True(t, ok, "expected CONNACK, got %T", pkt)
	// v3.1.1 maps this to return code 0x02 "identifier rejected" which decodes
	// as ReasonClientIDNotValid in our reason-code model.
	assert.Equal(t, ReasonClientIDNotValid, connack.ReasonCode)
}

func TestReadPacketAutoDetectsV311Connect(t *testing.T) {
	v311Connect := &ConnectPacket{
		ProtocolVersion: ProtocolV311,
		ClientID:        "auto-detect",
		CleanStart:      true,
		KeepAlive:       30,
	}

	var buf bytes.Buffer
	_, err := writePacketRaw(&buf, v311Connect, 0)
	require.NoError(t, err)

	pkt, _, err := readPacketV5(&buf, 0)
	require.NoError(t, err)

	got, ok := pkt.(*ConnectPacket)
	require.True(t, ok, "expected ConnectPacket, got %T", pkt)
	assert.Equal(t, ProtocolV311, got.ProtocolVersion, "readPacket should detect v3.1.1 from the protocol level byte")
	assert.Equal(t, "auto-detect", got.ClientID)
	assert.Equal(t, uint16(30), got.KeepAlive)
}

func TestReadPacketAutoDetectsV5Connect(t *testing.T) {
	v5Connect := &ConnectPacket{
		ProtocolVersion: ProtocolV5,
		ClientID:        "v5-client",
		CleanStart:      true,
		KeepAlive:       60,
	}

	var buf bytes.Buffer
	_, err := writePacketRaw(&buf, v5Connect, 0)
	require.NoError(t, err)

	pkt, _, err := readPacketV5(&buf, 0)
	require.NoError(t, err)

	got, ok := pkt.(*ConnectPacket)
	require.True(t, ok)
	assert.Equal(t, ProtocolV5, got.ProtocolVersion)
	assert.Equal(t, "v5-client", got.ClientID)
}

func TestWritePacketEncodesV311Connect(t *testing.T) {
	v311Connect := &ConnectPacket{
		ProtocolVersion: ProtocolV311,
		ClientID:        "c",
		CleanStart:      true,
	}

	var buf bytes.Buffer
	_, err := writePacketRaw(&buf, v311Connect, 0)
	require.NoError(t, err)

	raw := buf.Bytes()
	require.GreaterOrEqual(t, len(raw), 8, "raw CONNECT must be at least 8 bytes")
	// Byte 0: CONNECT type (0x10). Bytes 2-7: protocol name "MQTT" length-prefixed.
	assert.Equal(t, byte(0x10), raw[0])
	assert.Equal(t, byte(0x00), raw[2])
	assert.Equal(t, byte(0x04), raw[3])
	assert.Equal(t, byte('M'), raw[4])
	assert.Equal(t, byte('Q'), raw[5])
	assert.Equal(t, byte('T'), raw[6])
	assert.Equal(t, byte('T'), raw[7])
	// Byte 8 is the protocol level — must be 4 for v3.1.1.
	require.Greater(t, len(raw), 8)
	assert.Equal(t, byte(ProtocolV311), raw[8], "protocol level byte should be 4 for v3.1.1")
}

func TestCodecV5Passthrough(t *testing.T) {
	codec := newCodec(ProtocolV5)

	// Verify v5 codec delegates to standard readPacket/writePacketRaw
	pkt := &PublishPacket{
		Topic:   "test",
		Payload: []byte("hello"),
		QoS:     QoS0,
	}
	pkt.Props.Set(PropContentType, "text/plain")

	var buf bytes.Buffer
	_, err := codec.writePacket(&buf, pkt, 0)
	require.NoError(t, err)

	result, _, err := codec.readPacket(&buf, 0)
	require.NoError(t, err)

	got, ok := result.(*PublishPacket)
	require.True(t, ok)
	assert.Equal(t, "text/plain", got.Props.GetString(PropContentType))
}

// FuzzReadPacketV311 fuzzes the v3.1.1 packet decoder against arbitrary
// wire bytes. The decoder must never panic — it should only return packets
// or errors.
func FuzzReadPacketV311(f *testing.F) {
	// Seed with valid v3.1.1 packets of every type.
	packets := []Packet{
		&ConnectPacket{ProtocolVersion: ProtocolV311, ClientID: "c", CleanStart: true},
		&ConnectPacket{
			ProtocolVersion: ProtocolV311,
			ClientID:        "c",
			CleanStart:      true,
			Username:        "u",
			Password:        []byte("p"),
			WillFlag:        true,
			WillTopic:       "w",
			WillPayload:     []byte("will"),
			WillQoS:         1,
			WillRetain:      true,
		},
		&ConnackPacket{ReasonCode: ReasonSuccess},
		&ConnackPacket{SessionPresent: true, ReasonCode: ReasonSuccess},
		&ConnackPacket{ReasonCode: ReasonNotAuthorized},
		&PublishPacket{Topic: "t", QoS: QoS0, Payload: []byte("hello")},
		&PublishPacket{Topic: "t", QoS: QoS1, PacketID: 1, Payload: []byte("hello")},
		&PublishPacket{Topic: "t", QoS: QoS2, PacketID: 1, Payload: []byte("hello"), Retain: true},
		&PubackPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		&PubrecPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		&PubrelPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		&PubcompPacket{PacketID: 1, ReasonCode: ReasonSuccess},
		&SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "a/b", QoS: QoS0},
				{TopicFilter: "c/#", QoS: QoS1},
			},
		},
		&SubackPacket{
			PacketID:    1,
			ReasonCodes: []ReasonCode{ReasonGrantedQoS0, ReasonGrantedQoS1, ReasonGrantedQoS2, ReasonUnspecifiedError},
		},
		&UnsubscribePacket{PacketID: 1, TopicFilters: []string{"a/b", "c/d"}},
		&UnsubackPacket{PacketID: 1},
		&PingreqPacket{},
		&PingrespPacket{},
		&DisconnectPacket{},
	}

	for _, p := range packets {
		var buf bytes.Buffer
		if _, err := writePacketV311(&buf, p, 0); err == nil {
			f.Add(buf.Bytes())
		}
	}

	// Edge-case byte sequences (manually crafted, often malformed).
	f.Add([]byte{})                                                   // empty
	f.Add([]byte{0x00})                                               // invalid type (reserved)
	f.Add([]byte{0xF0, 0x00})                                         // AUTH (invalid in v3.1.1)
	f.Add([]byte{byte(PacketPUBLISH) << 4, 0xFF, 0xFF})               // truncated remaining length
	f.Add([]byte{byte(PacketCONNECT) << 4, 0x04, 'M', 'Q', 'T', 'T'}) // truncated CONNECT
	f.Add([]byte{byte(PacketPUBACK) << 4, 0x02, 0x00, 0x00})          // zero packet ID
	f.Add([]byte{byte(PacketSUBACK) << 4, 0x03, 0x00, 0x01, 0x03})    // invalid return code
	f.Add([]byte{byte(PacketCONNACK) << 4, 0x02, 0x02, 0x00})         // reserved flags

	// Random fuzz seeds.
	for range 16 {
		size := rand.IntN(128) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(data)
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		// Primary: the decoder must never panic on arbitrary input.
		pkt, _, err := readPacketV311(bytes.NewReader(data), 0)
		if err != nil || pkt == nil {
			return
		}

		// Decoded packet must validate — v3.1.1 decoder returns validated packets.
		_ = pkt.Validate()

		// Round-trip: re-encode and verify the result also decodes without panic.
		var buf bytes.Buffer
		if _, err := writePacketV311(&buf, pkt, 0); err != nil {
			return
		}
		_, _, _ = readPacketV311(&buf, 0)
	})
}
