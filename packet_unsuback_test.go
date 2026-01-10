//nolint:dupl // Similar test structure for similar packet types
package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnsubackPacketType(t *testing.T) {
	p := &UnsubackPacket{}
	assert.Equal(t, PacketUNSUBACK, p.Type())
}

func TestUnsubackPacketID(t *testing.T) {
	p := &UnsubackPacket{}
	p.SetPacketID(12345)
	assert.Equal(t, uint16(12345), p.GetPacketID())
}

func TestUnsubackPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet UnsubackPacket
	}{
		{
			name: "single success",
			packet: UnsubackPacket{
				PacketID:    1,
				ReasonCodes: []ReasonCode{ReasonSuccess},
			},
		},
		{
			name: "no subscription existed",
			packet: UnsubackPacket{
				PacketID:    100,
				ReasonCodes: []ReasonCode{ReasonNoSubscriptionExisted},
			},
		},
		{
			name: "multiple reason codes",
			packet: UnsubackPacket{
				PacketID: 42,
				ReasonCodes: []ReasonCode{
					ReasonSuccess,
					ReasonNoSubscriptionExisted,
					ReasonSuccess,
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
			assert.Equal(t, PacketUNSUBACK, header.PacketType)
			assert.Equal(t, byte(0x00), header.Flags)

			var decoded UnsubackPacket
			_, err = decoded.Decode(&buf, header)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.PacketID, decoded.PacketID)
			assert.Equal(t, tt.packet.ReasonCodes, decoded.ReasonCodes)
		})
	}
}

func TestUnsubackPacketWithProperties(t *testing.T) {
	packet := UnsubackPacket{
		PacketID:    1,
		ReasonCodes: []ReasonCode{ReasonSuccess},
	}
	packet.Props.Set(PropReasonString, "Unsubscribed")
	packet.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded UnsubackPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	assert.Equal(t, "Unsubscribed", decoded.Props.GetString(PropReasonString))
	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	require.Len(t, ups, 1)
	assert.Equal(t, "key", ups[0].Key)
}

func TestUnsubackPacketInvalidType(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPUBLISH,
		Flags:           0x00,
		RemainingLength: 10,
	}

	var p UnsubackPacket
	_, err := p.Decode(bytes.NewReader(make([]byte, 10)), header)
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestUnsubackPacketValidation(t *testing.T) {
	tests := []struct {
		name    string
		packet  UnsubackPacket
		wantErr error
	}{
		{
			name: "valid",
			packet: UnsubackPacket{
				PacketID:    1,
				ReasonCodes: []ReasonCode{ReasonSuccess},
			},
			wantErr: nil,
		},
		{
			name: "zero packet ID",
			packet: UnsubackPacket{
				PacketID:    0,
				ReasonCodes: []ReasonCode{ReasonSuccess},
			},
			wantErr: ErrInvalidPacketID,
		},
		{
			name: "no reason codes",
			packet: UnsubackPacket{
				PacketID:    1,
				ReasonCodes: []ReasonCode{},
			},
			wantErr: ErrProtocolViolation,
		},
		{
			name: "invalid reason code",
			packet: UnsubackPacket{
				PacketID:    1,
				ReasonCodes: []ReasonCode{ReasonPacketIDNotFound}, // Not valid for UNSUBACK
			},
			wantErr: ErrInvalidReasonCode,
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

func BenchmarkUnsubackPacketEncode(b *testing.B) {
	packet := UnsubackPacket{
		PacketID:    1,
		ReasonCodes: []ReasonCode{ReasonSuccess},
	}
	var buf bytes.Buffer
	buf.Grow(32)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

func BenchmarkUnsubackPacketDecode(b *testing.B) {
	packet := UnsubackPacket{
		PacketID:    1,
		ReasonCodes: []ReasonCode{ReasonSuccess},
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
		var p UnsubackPacket
		_, _ = p.Decode(r, header)
	}
}

func FuzzUnsubackPacketDecode(f *testing.F) {
	packet := UnsubackPacket{
		PacketID:    1,
		ReasonCodes: []ReasonCode{ReasonSuccess},
	}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	// Multiple reason codes
	packet2 := UnsubackPacket{
		PacketID:    100,
		ReasonCodes: []ReasonCode{ReasonSuccess, ReasonNoSubscriptionExisted},
	}
	buf.Reset()
	_, _ = packet2.Encode(&buf)
	f.Add(buf.Bytes())

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
		if err != nil || header.PacketType != PacketUNSUBACK {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p UnsubackPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}

func TestUnsubackPacketProperties(t *testing.T) {
	p := &UnsubackPacket{}
	p.Props.Set(PropReasonString, "test reason")
	props := p.Properties()
	require.NotNil(t, props)
	assert.Equal(t, "test reason", props.GetString(PropReasonString))
}

func TestUnsubackPacketEncodeErrors(t *testing.T) {
	t.Run("encode with zero packet ID", func(t *testing.T) {
		invalid := UnsubackPacket{PacketID: 0, ReasonCodes: []ReasonCode{ReasonSuccess}}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrInvalidPacketID)
	})

	t.Run("encode with no reason codes", func(t *testing.T) {
		invalid := UnsubackPacket{PacketID: 1, ReasonCodes: []ReasonCode{}}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrProtocolViolation)
	})

	t.Run("encode with invalid reason code", func(t *testing.T) {
		invalid := UnsubackPacket{PacketID: 1, ReasonCodes: []ReasonCode{ReasonPacketIDNotFound}}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrInvalidReasonCode)
	})

	t.Run("encode with invalid property", func(t *testing.T) {
		invalid := UnsubackPacket{PacketID: 1, ReasonCodes: []ReasonCode{ReasonSuccess}}
		invalid.Props.Set(PropServerKeepAlive, uint16(60)) // Not valid for UNSUBACK
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.Error(t, err)
	})
}

func TestUnsubackPacketDecodeErrors(t *testing.T) {
	t.Run("packet ID read error", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketUNSUBACK,
			Flags:           0x00,
			RemainingLength: 10,
		}
		var p UnsubackPacket
		_, err := p.Decode(bytes.NewReader([]byte{}), header)
		assert.Error(t, err)
	})

	t.Run("properties read error", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketUNSUBACK,
			Flags:           0x00,
			RemainingLength: 10,
		}
		// Packet ID but truncated properties
		var p UnsubackPacket
		_, err := p.Decode(bytes.NewReader([]byte{0x00, 0x01, 0xFF}), header)
		assert.Error(t, err)
	})

	t.Run("invalid properties for UNSUBACK", func(t *testing.T) {
		var propBuf bytes.Buffer
		props := Properties{}
		props.Set(PropServerKeepAlive, uint16(60)) // Not valid for UNSUBACK
		_, _ = props.Encode(&propBuf)

		var buf bytes.Buffer
		buf.Write([]byte{0x00, 0x01}) // Packet ID
		buf.Write(propBuf.Bytes())
		buf.WriteByte(0x00) // Reason code

		header := FixedHeader{
			PacketType:      PacketUNSUBACK,
			Flags:           0x00,
			RemainingLength: uint32(buf.Len()),
		}

		var p UnsubackPacket
		_, err := p.Decode(bytes.NewReader(buf.Bytes()), header)
		assert.Error(t, err)
	})

	t.Run("reason code read error", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketUNSUBACK,
			Flags:           0x00,
			RemainingLength: 10, // Expects more data
		}
		// Packet ID + empty properties but no reason codes
		var p UnsubackPacket
		_, err := p.Decode(bytes.NewReader([]byte{0x00, 0x01, 0x00}), header)
		assert.Error(t, err)
	})
}
