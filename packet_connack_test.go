package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnackPacketType(t *testing.T) {
	p := &ConnackPacket{}
	assert.Equal(t, PacketCONNACK, p.Type())
}

func TestConnackPacketProperties(t *testing.T) {
	p := &ConnackPacket{}
	p.Props.Set(PropSessionExpiryInterval, uint32(3600))
	assert.Equal(t, uint32(3600), p.Properties().GetUint32(PropSessionExpiryInterval))
}

func TestConnackPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet ConnackPacket
	}{
		{
			name: "success no session",
			packet: ConnackPacket{
				SessionPresent: false,
				ReasonCode:     ReasonSuccess,
			},
		},
		{
			name: "success with session",
			packet: ConnackPacket{
				SessionPresent: true,
				ReasonCode:     ReasonSuccess,
			},
		},
		{
			name: "not authorized",
			packet: ConnackPacket{
				SessionPresent: false,
				ReasonCode:     ReasonNotAuthorized,
			},
		},
		{
			name: "bad username or password",
			packet: ConnackPacket{
				SessionPresent: false,
				ReasonCode:     ReasonBadUserNameOrPassword,
			},
		},
		{
			name: "server busy",
			packet: ConnackPacket{
				SessionPresent: false,
				ReasonCode:     ReasonServerBusy,
			},
		},
		{
			name: "malformed packet",
			packet: ConnackPacket{
				SessionPresent: false,
				ReasonCode:     ReasonMalformedPacket,
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
			assert.Equal(t, PacketCONNACK, header.PacketType)
			assert.Equal(t, byte(0x00), header.Flags)

			// Decode CONNACK packet
			var decoded ConnackPacket
			n3, err := decoded.Decode(&buf, header)
			require.NoError(t, err)
			assert.Equal(t, int(header.RemainingLength), n3)

			// Verify fields
			assert.Equal(t, tt.packet.SessionPresent, decoded.SessionPresent)
			assert.Equal(t, tt.packet.ReasonCode, decoded.ReasonCode)
		})
	}
}

func TestConnackPacketWithProperties(t *testing.T) {
	packet := ConnackPacket{
		SessionPresent: false,
		ReasonCode:     ReasonSuccess,
	}
	packet.Props.Set(PropSessionExpiryInterval, uint32(3600))
	packet.Props.Set(PropReceiveMaximum, uint16(100))
	packet.Props.Set(PropMaximumQoS, byte(1))
	packet.Props.Set(PropRetainAvailable, byte(1))
	packet.Props.Set(PropMaximumPacketSize, uint32(1048576))
	packet.Props.Set(PropAssignedClientIdentifier, "assigned-id")
	packet.Props.Set(PropTopicAliasMaximum, uint16(10))
	packet.Props.Set(PropReasonString, "Connection accepted")
	packet.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})
	packet.Props.Set(PropWildcardSubAvailable, byte(1))
	packet.Props.Set(PropSubscriptionIDAvailable, byte(1))
	packet.Props.Set(PropSharedSubAvailable, byte(1))
	packet.Props.Set(PropServerKeepAlive, uint16(120))
	packet.Props.Set(PropResponseInformation, "/response/topic")
	packet.Props.Set(PropServerReference, "server.example.com")
	packet.Props.Set(PropAuthenticationMethod, "PLAIN")
	packet.Props.Set(PropAuthenticationData, []byte{0x01, 0x02, 0x03})

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded ConnackPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	assert.Equal(t, uint32(3600), decoded.Props.GetUint32(PropSessionExpiryInterval))
	assert.Equal(t, uint16(100), decoded.Props.GetUint16(PropReceiveMaximum))
	assert.Equal(t, byte(1), decoded.Props.GetByte(PropMaximumQoS))
	assert.Equal(t, byte(1), decoded.Props.GetByte(PropRetainAvailable))
	assert.Equal(t, uint32(1048576), decoded.Props.GetUint32(PropMaximumPacketSize))
	assert.Equal(t, "assigned-id", decoded.Props.GetString(PropAssignedClientIdentifier))
	assert.Equal(t, uint16(10), decoded.Props.GetUint16(PropTopicAliasMaximum))
	assert.Equal(t, "Connection accepted", decoded.Props.GetString(PropReasonString))
	assert.Equal(t, byte(1), decoded.Props.GetByte(PropWildcardSubAvailable))
	assert.Equal(t, byte(1), decoded.Props.GetByte(PropSubscriptionIDAvailable))
	assert.Equal(t, byte(1), decoded.Props.GetByte(PropSharedSubAvailable))
	assert.Equal(t, uint16(120), decoded.Props.GetUint16(PropServerKeepAlive))
	assert.Equal(t, "/response/topic", decoded.Props.GetString(PropResponseInformation))
	assert.Equal(t, "server.example.com", decoded.Props.GetString(PropServerReference))
	assert.Equal(t, "PLAIN", decoded.Props.GetString(PropAuthenticationMethod))
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded.Props.GetBinary(PropAuthenticationData))

	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	assert.Len(t, ups, 1)
	assert.Equal(t, "key", ups[0].Key)
	assert.Equal(t, "value", ups[0].Value)
}

func TestConnackPacketValidation(t *testing.T) {
	tests := []struct {
		name    string
		packet  ConnackPacket
		wantErr error
	}{
		{
			name: "valid success",
			packet: ConnackPacket{
				SessionPresent: false,
				ReasonCode:     ReasonSuccess,
			},
			wantErr: nil,
		},
		{
			name: "valid success with session",
			packet: ConnackPacket{
				SessionPresent: true,
				ReasonCode:     ReasonSuccess,
			},
			wantErr: nil,
		},
		{
			name: "valid error code",
			packet: ConnackPacket{
				SessionPresent: false,
				ReasonCode:     ReasonNotAuthorized,
			},
			wantErr: nil,
		},
		{
			name: "session present with error code",
			packet: ConnackPacket{
				SessionPresent: true,
				ReasonCode:     ReasonNotAuthorized,
			},
			wantErr: ErrInvalidConnackFlags,
		},
		{
			name: "invalid reason code for CONNACK",
			packet: ConnackPacket{
				SessionPresent: false,
				ReasonCode:     ReasonGrantedQoS1,
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

func TestConnackPacketDecodeErrors(t *testing.T) {
	t.Run("invalid flags", func(t *testing.T) {
		// Build a packet with reserved bits set
		data := []byte{
			0x20, 0x03, // Fixed header
			0x02,       // Invalid flags (reserved bit set)
			0x00,       // Reason code
			0x00,       // Empty properties
		}

		r := bytes.NewReader(data)
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)

		var p ConnackPacket
		_, err = p.Decode(r, header)
		assert.ErrorIs(t, err, ErrInvalidConnackFlags)
	})

	t.Run("wrong packet type", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketCONNECT,
			RemainingLength: 3,
		}

		var p ConnackPacket
		_, err := p.Decode(bytes.NewReader([]byte{0x00, 0x00, 0x00}), header)
		assert.ErrorIs(t, err, ErrInvalidPacketType)
	})
}

func TestConnackPacketMinimal(t *testing.T) {
	// Minimal CONNACK without properties
	packet := ConnackPacket{
		SessionPresent: false,
		ReasonCode:     ReasonSuccess,
	}

	var buf bytes.Buffer
	n, err := packet.Encode(&buf)
	require.NoError(t, err)

	// Fixed header (2) + flags (1) + reason code (1) + properties length (1)
	assert.Equal(t, 5, n)
}

// Benchmarks

func BenchmarkConnackPacketEncode(b *testing.B) {
	packets := []struct {
		name   string
		packet ConnackPacket
	}{
		{
			name: "minimal",
			packet: ConnackPacket{
				SessionPresent: false,
				ReasonCode:     ReasonSuccess,
			},
		},
		{
			name: "with properties",
			packet: func() ConnackPacket {
				p := ConnackPacket{
					SessionPresent: true,
					ReasonCode:     ReasonSuccess,
				}
				p.Props.Set(PropSessionExpiryInterval, uint32(3600))
				p.Props.Set(PropReceiveMaximum, uint16(100))
				p.Props.Set(PropMaximumQoS, byte(1))
				p.Props.Set(PropAssignedClientIdentifier, "client-id-123")
				return p
			}(),
		},
	}

	for _, tt := range packets {
		b.Run(tt.name, func(b *testing.B) {
			var buf bytes.Buffer
			buf.Grow(128)

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				buf.Reset()
				_, _ = tt.packet.Encode(&buf)
			}
		})
	}
}

func BenchmarkConnackPacketDecode(b *testing.B) {
	packets := []struct {
		name   string
		packet ConnackPacket
	}{
		{
			name: "minimal",
			packet: ConnackPacket{
				SessionPresent: false,
				ReasonCode:     ReasonSuccess,
			},
		},
		{
			name: "with properties",
			packet: func() ConnackPacket {
				p := ConnackPacket{
					SessionPresent: true,
					ReasonCode:     ReasonSuccess,
				}
				p.Props.Set(PropSessionExpiryInterval, uint32(3600))
				p.Props.Set(PropReceiveMaximum, uint16(100))
				return p
			}(),
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
				var p ConnackPacket
				_, _ = p.Decode(r, header)
			}
		})
	}
}

// Fuzz tests

func FuzzConnackPacketDecode(f *testing.F) {
	// Valid CONNACK packet seeds
	validPacket := ConnackPacket{
		SessionPresent: false,
		ReasonCode:     ReasonSuccess,
	}
	var buf bytes.Buffer
	_, _ = validPacket.Encode(&buf)
	f.Add(buf.Bytes())

	// With session present
	sessionPacket := ConnackPacket{
		SessionPresent: true,
		ReasonCode:     ReasonSuccess,
	}
	buf.Reset()
	_, _ = sessionPacket.Encode(&buf)
	f.Add(buf.Bytes())

	// With properties
	propPacket := ConnackPacket{
		SessionPresent: false,
		ReasonCode:     ReasonSuccess,
	}
	propPacket.Props.Set(PropSessionExpiryInterval, uint32(3600))
	buf.Reset()
	_, _ = propPacket.Encode(&buf)
	f.Add(buf.Bytes())

	// Error codes
	errPacket := ConnackPacket{
		SessionPresent: false,
		ReasonCode:     ReasonNotAuthorized,
	}
	buf.Reset()
	_, _ = errPacket.Encode(&buf)
	f.Add(buf.Bytes())

	// Edge cases
	f.Add([]byte{0x20, 0x02, 0x00, 0x00})       // Minimal without properties length
	f.Add([]byte{0x20, 0x03, 0x00, 0x00, 0x00}) // Minimal with empty properties
	f.Add([]byte{0x20, 0x00})                   // Zero remaining length
	f.Add([]byte{0x20, 0xFF, 0xFF, 0xFF, 0x7F}) // Max remaining length

	// Random generated seeds
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
		if err != nil {
			return
		}
		if header.PacketType != PacketCONNACK {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p ConnackPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}
