//nolint:dupl // Similar test structure for similar packet types
package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubackPacketType(t *testing.T) {
	p := &SubackPacket{}
	assert.Equal(t, PacketSUBACK, p.Type())
}

func TestSubackPacketID(t *testing.T) {
	p := &SubackPacket{}
	p.SetPacketID(12345)
	assert.Equal(t, uint16(12345), p.GetPacketID())
}

func TestSubackPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet SubackPacket
	}{
		{
			name: "single QoS 0 granted",
			packet: SubackPacket{
				PacketID:    1,
				ReasonCodes: []ReasonCode{ReasonGrantedQoS0},
			},
		},
		{
			name: "single QoS 1 granted",
			packet: SubackPacket{
				PacketID:    100,
				ReasonCodes: []ReasonCode{ReasonGrantedQoS1},
			},
		},
		{
			name: "single QoS 2 granted",
			packet: SubackPacket{
				PacketID:    65535,
				ReasonCodes: []ReasonCode{ReasonGrantedQoS2},
			},
		},
		{
			name: "multiple reason codes",
			packet: SubackPacket{
				PacketID: 42,
				ReasonCodes: []ReasonCode{
					ReasonGrantedQoS0,
					ReasonGrantedQoS1,
					ReasonGrantedQoS2,
				},
			},
		},
		{
			name: "with error",
			packet: SubackPacket{
				PacketID: 1,
				ReasonCodes: []ReasonCode{
					ReasonGrantedQoS1,
					ReasonNotAuthorized,
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
			assert.Equal(t, PacketSUBACK, header.PacketType)
			assert.Equal(t, byte(0x00), header.Flags)

			var decoded SubackPacket
			_, err = decoded.Decode(&buf, header)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.PacketID, decoded.PacketID)
			assert.Equal(t, tt.packet.ReasonCodes, decoded.ReasonCodes)
		})
	}
}

func TestSubackPacketWithProperties(t *testing.T) {
	packet := SubackPacket{
		PacketID:    1,
		ReasonCodes: []ReasonCode{ReasonGrantedQoS1},
	}
	packet.Props.Set(PropReasonString, "Subscription accepted")
	packet.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded SubackPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	assert.Equal(t, "Subscription accepted", decoded.Props.GetString(PropReasonString))
	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	require.Len(t, ups, 1)
	assert.Equal(t, "key", ups[0].Key)
}

func TestSubackPacketInvalidType(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPUBLISH,
		Flags:           0x00,
		RemainingLength: 10,
	}

	var p SubackPacket
	_, err := p.Decode(bytes.NewReader(make([]byte, 10)), header)
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestSubackPacketValidation(t *testing.T) {
	tests := []struct {
		name    string
		packet  SubackPacket
		wantErr error
	}{
		{
			name: "valid",
			packet: SubackPacket{
				PacketID:    1,
				ReasonCodes: []ReasonCode{ReasonGrantedQoS0},
			},
			wantErr: nil,
		},
		{
			name: "zero packet ID",
			packet: SubackPacket{
				PacketID:    0,
				ReasonCodes: []ReasonCode{ReasonGrantedQoS0},
			},
			wantErr: ErrInvalidPacketID,
		},
		{
			name: "no reason codes",
			packet: SubackPacket{
				PacketID:    1,
				ReasonCodes: []ReasonCode{},
			},
			wantErr: ErrProtocolViolation,
		},
		{
			name: "invalid reason code",
			packet: SubackPacket{
				PacketID:    1,
				ReasonCodes: []ReasonCode{ReasonPacketIDNotFound}, // Not valid for SUBACK
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

func BenchmarkSubackPacketEncode(b *testing.B) {
	packet := SubackPacket{
		PacketID:    1,
		ReasonCodes: []ReasonCode{ReasonGrantedQoS1},
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

func BenchmarkSubackPacketDecode(b *testing.B) {
	packet := SubackPacket{
		PacketID:    1,
		ReasonCodes: []ReasonCode{ReasonGrantedQoS1},
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
		var p SubackPacket
		_, _ = p.Decode(r, header)
	}
}

func FuzzSubackPacketDecode(f *testing.F) {
	packet := SubackPacket{
		PacketID:    1,
		ReasonCodes: []ReasonCode{ReasonGrantedQoS1},
	}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	// Multiple reason codes
	packet2 := SubackPacket{
		PacketID:    100,
		ReasonCodes: []ReasonCode{ReasonGrantedQoS0, ReasonGrantedQoS1, ReasonGrantedQoS2},
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
		if err != nil || header.PacketType != PacketSUBACK {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p SubackPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}

func TestSubackPacketProperties(t *testing.T) {
	p := &SubackPacket{}
	p.Props.Set(PropReasonString, "test reason")
	props := p.Properties()
	require.NotNil(t, props)
	assert.Equal(t, "test reason", props.GetString(PropReasonString))
}
