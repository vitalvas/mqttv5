//nolint:dupl // Similar test structure for similar packet types
package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthPacketType(t *testing.T) {
	p := &AuthPacket{}
	assert.Equal(t, PacketAUTH, p.Type())
}

func TestAuthPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet AuthPacket
	}{
		{
			name:   "success",
			packet: AuthPacket{ReasonCode: ReasonSuccess},
		},
		{
			name:   "continue authentication",
			packet: AuthPacket{ReasonCode: ReasonContinueAuth},
		},
		{
			name:   "re-authenticate",
			packet: AuthPacket{ReasonCode: ReasonReAuth},
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
			assert.Equal(t, PacketAUTH, header.PacketType)
			assert.Equal(t, byte(0x00), header.Flags)

			var decoded AuthPacket
			_, err = decoded.Decode(&buf, header)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.ReasonCode, decoded.ReasonCode)
		})
	}
}

func TestAuthPacketMinimal(t *testing.T) {
	// Success with no properties encodes to minimal form
	packet := AuthPacket{ReasonCode: ReasonSuccess}

	var buf bytes.Buffer
	n, err := packet.Encode(&buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n) // Just fixed header

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), header.RemainingLength)

	var decoded AuthPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)
	assert.Equal(t, ReasonSuccess, decoded.ReasonCode)
}

func TestAuthPacketWithProperties(t *testing.T) {
	packet := AuthPacket{ReasonCode: ReasonContinueAuth}
	packet.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
	packet.Props.Set(PropAuthenticationData, []byte("client-first-message"))
	packet.Props.Set(PropReasonString, "Continue")
	packet.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded AuthPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	assert.Equal(t, "SCRAM-SHA-256", decoded.Props.GetString(PropAuthenticationMethod))
	assert.Equal(t, []byte("client-first-message"), decoded.Props.GetBinary(PropAuthenticationData))
	assert.Equal(t, "Continue", decoded.Props.GetString(PropReasonString))
	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	require.Len(t, ups, 1)
	assert.Equal(t, "key", ups[0].Key)
}

func TestAuthPacketInvalidType(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketPUBLISH,
		Flags:           0x00,
		RemainingLength: 0,
	}

	var p AuthPacket
	_, err := p.Decode(bytes.NewReader(nil), header)
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestAuthPacketInvalidFlags(t *testing.T) {
	header := FixedHeader{
		PacketType:      PacketAUTH,
		Flags:           0x01,
		RemainingLength: 0,
	}

	var p AuthPacket
	_, err := p.Decode(bytes.NewReader(nil), header)
	assert.ErrorIs(t, err, ErrInvalidPacketFlags)
}

func TestAuthPacketValidation(t *testing.T) {
	tests := []struct {
		name    string
		packet  AuthPacket
		wantErr error
	}{
		{
			name:    "success",
			packet:  AuthPacket{ReasonCode: ReasonSuccess},
			wantErr: nil,
		},
		{
			name:    "continue auth",
			packet:  AuthPacket{ReasonCode: ReasonContinueAuth},
			wantErr: nil,
		},
		{
			name:    "re-auth",
			packet:  AuthPacket{ReasonCode: ReasonReAuth},
			wantErr: nil,
		},
		{
			name:    "invalid reason code",
			packet:  AuthPacket{ReasonCode: ReasonNotAuthorized},
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

func BenchmarkAuthPacketEncode(b *testing.B) {
	packet := AuthPacket{ReasonCode: ReasonContinueAuth}
	packet.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
	var buf bytes.Buffer
	buf.Grow(32)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = packet.Encode(&buf)
	}
}

func BenchmarkAuthPacketDecode(b *testing.B) {
	packet := AuthPacket{ReasonCode: ReasonContinueAuth}
	packet.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		r := bytes.NewReader(data)
		var header FixedHeader
		_, _ = header.Decode(r)
		var p AuthPacket
		_, _ = p.Decode(r, header)
	}
}

func FuzzAuthPacketDecode(f *testing.F) {
	packet := AuthPacket{ReasonCode: ReasonSuccess}
	var buf bytes.Buffer
	_, _ = packet.Encode(&buf)
	f.Add(buf.Bytes())

	f.Add([]byte{0xF0, 0x00})       // Minimal
	f.Add([]byte{0xF0, 0x01, 0x00}) // With reason code

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
		if err != nil || header.PacketType != PacketAUTH {
			return
		}

		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p AuthPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}

func TestAuthPacketProperties(t *testing.T) {
	p := &AuthPacket{}
	p.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
	props := p.Properties()
	require.NotNil(t, props)
	assert.Equal(t, "SCRAM-SHA-256", props.GetString(PropAuthenticationMethod))
}

func TestAuthPacketEncodeErrors(t *testing.T) {
	t.Run("encode with validation error", func(t *testing.T) {
		invalid := AuthPacket{ReasonCode: ReasonNotAuthorized}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrInvalidReasonCode)
	})

	t.Run("encode with invalid property", func(t *testing.T) {
		invalid := AuthPacket{ReasonCode: ReasonSuccess}
		invalid.Props.Set(PropServerKeepAlive, uint16(60)) // Not valid for AUTH
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.Error(t, err)
	})
}

func TestAuthPacketDecodeErrors(t *testing.T) {
	t.Run("reason code read error", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketAUTH,
			Flags:           0x00,
			RemainingLength: 1, // Expects 1 byte but reader is empty
		}
		var p AuthPacket
		_, err := p.Decode(bytes.NewReader([]byte{}), header)
		assert.Error(t, err)
	})

	t.Run("properties read error", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketAUTH,
			Flags:           0x00,
			RemainingLength: 5, // Expects properties but data is truncated
		}
		var p AuthPacket
		_, err := p.Decode(bytes.NewReader([]byte{0x00}), header) // Just reason code
		assert.Error(t, err)
	})

	t.Run("invalid properties for AUTH", func(t *testing.T) {
		var propBuf bytes.Buffer
		props := Properties{}
		props.Set(PropServerKeepAlive, uint16(60)) // Not valid for AUTH
		_, _ = props.Encode(&propBuf)

		var buf bytes.Buffer
		buf.WriteByte(0x00) // Reason code
		buf.Write(propBuf.Bytes())

		header := FixedHeader{
			PacketType:      PacketAUTH,
			Flags:           0x00,
			RemainingLength: uint32(buf.Len()),
		}
		var p AuthPacket
		_, err := p.Decode(bytes.NewReader(buf.Bytes()), header)
		assert.Error(t, err)
	})
}
