package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacketTypeString(t *testing.T) {
	tests := []struct {
		pt   PacketType
		want string
	}{
		{PacketCONNECT, "CONNECT"},
		{PacketCONNACK, "CONNACK"},
		{PacketPUBLISH, "PUBLISH"},
		{PacketPUBACK, "PUBACK"},
		{PacketPUBREC, "PUBREC"},
		{PacketPUBREL, "PUBREL"},
		{PacketPUBCOMP, "PUBCOMP"},
		{PacketSUBSCRIBE, "SUBSCRIBE"},
		{PacketSUBACK, "SUBACK"},
		{PacketUNSUBSCRIBE, "UNSUBSCRIBE"},
		{PacketUNSUBACK, "UNSUBACK"},
		{PacketPINGREQ, "PINGREQ"},
		{PacketPINGRESP, "PINGRESP"},
		{PacketDISCONNECT, "DISCONNECT"},
		{PacketAUTH, "AUTH"},
		{PacketType(0), "UNKNOWN"},
		{PacketType(16), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.pt.String())
		})
	}
}

func TestPacketTypeValid(t *testing.T) {
	tests := []struct {
		pt    PacketType
		valid bool
	}{
		{PacketType(0), false},
		{PacketCONNECT, true},
		{PacketAUTH, true},
		{PacketType(16), false},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.valid, tt.pt.Valid())
	}
}

func TestFixedHeaderEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		header FixedHeader
	}{
		{
			name:   "CONNECT",
			header: FixedHeader{PacketType: PacketCONNECT, Flags: 0x00, RemainingLength: 0},
		},
		{
			name:   "CONNACK with length",
			header: FixedHeader{PacketType: PacketCONNACK, Flags: 0x00, RemainingLength: 3},
		},
		{
			name:   "PUBLISH QoS 0",
			header: FixedHeader{PacketType: PacketPUBLISH, Flags: 0x00, RemainingLength: 10},
		},
		{
			name:   "PUBLISH QoS 1 DUP",
			header: FixedHeader{PacketType: PacketPUBLISH, Flags: 0x0A, RemainingLength: 100},
		},
		{
			name:   "PUBLISH QoS 2 RETAIN",
			header: FixedHeader{PacketType: PacketPUBLISH, Flags: 0x05, RemainingLength: 1000},
		},
		{
			name:   "PUBREL",
			header: FixedHeader{PacketType: PacketPUBREL, Flags: 0x02, RemainingLength: 4},
		},
		{
			name:   "SUBSCRIBE",
			header: FixedHeader{PacketType: PacketSUBSCRIBE, Flags: 0x02, RemainingLength: 50},
		},
		{
			name:   "UNSUBSCRIBE",
			header: FixedHeader{PacketType: PacketUNSUBSCRIBE, Flags: 0x02, RemainingLength: 20},
		},
		{
			name:   "max remaining length",
			header: FixedHeader{PacketType: PacketPUBLISH, Flags: 0x00, RemainingLength: 268435455},
		},
		{
			name:   "2-byte remaining length",
			header: FixedHeader{PacketType: PacketCONNECT, Flags: 0x00, RemainingLength: 128},
		},
		{
			name:   "3-byte remaining length",
			header: FixedHeader{PacketType: PacketCONNECT, Flags: 0x00, RemainingLength: 16384},
		},
		{
			name:   "4-byte remaining length",
			header: FixedHeader{PacketType: PacketCONNECT, Flags: 0x00, RemainingLength: 2097152},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := tt.header.Encode(&buf)
			require.NoError(t, err)
			assert.Equal(t, tt.header.Size(), n)

			var decoded FixedHeader
			n2, err := decoded.Decode(&buf)
			require.NoError(t, err)
			assert.Equal(t, n, n2)
			assert.Equal(t, tt.header.PacketType, decoded.PacketType)
			assert.Equal(t, tt.header.Flags, decoded.Flags)
			assert.Equal(t, tt.header.RemainingLength, decoded.RemainingLength)
		})
	}
}

func TestFixedHeaderEncodeInvalidPacketType(t *testing.T) {
	header := FixedHeader{PacketType: PacketType(0), Flags: 0x00, RemainingLength: 0}
	var buf bytes.Buffer
	_, err := header.Encode(&buf)
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestFixedHeaderDecodeInvalidPacketType(t *testing.T) {
	// Packet type 0 is invalid
	data := []byte{0x00, 0x00}
	var header FixedHeader
	_, err := header.Decode(bytes.NewReader(data))
	assert.ErrorIs(t, err, ErrInvalidPacketType)
}

func TestFixedHeaderSize(t *testing.T) {
	tests := []struct {
		remainingLength uint32
		wantSize        int
	}{
		{0, 2},
		{127, 2},
		{128, 3},
		{16383, 3},
		{16384, 4},
		{2097151, 4},
		{2097152, 5},
		{268435455, 5},
	}

	for _, tt := range tests {
		header := FixedHeader{PacketType: PacketCONNECT, RemainingLength: tt.remainingLength}
		assert.Equal(t, tt.wantSize, header.Size())
	}
}

func TestFixedHeaderValidateFlags(t *testing.T) {
	tests := []struct {
		name    string
		header  FixedHeader
		wantErr error
	}{
		// CONNECT must have flags = 0x00
		{"CONNECT valid", FixedHeader{PacketType: PacketCONNECT, Flags: 0x00}, nil},
		{"CONNECT invalid", FixedHeader{PacketType: PacketCONNECT, Flags: 0x01}, ErrInvalidPacketFlags},

		// CONNACK must have flags = 0x00
		{"CONNACK valid", FixedHeader{PacketType: PacketCONNACK, Flags: 0x00}, nil},
		{"CONNACK invalid", FixedHeader{PacketType: PacketCONNACK, Flags: 0x0F}, ErrInvalidPacketFlags},

		// PUBLISH has variable flags
		{"PUBLISH QoS 0", FixedHeader{PacketType: PacketPUBLISH, Flags: 0x00}, nil},
		{"PUBLISH QoS 1", FixedHeader{PacketType: PacketPUBLISH, Flags: 0x02}, nil},
		{"PUBLISH QoS 2", FixedHeader{PacketType: PacketPUBLISH, Flags: 0x04}, nil},
		{"PUBLISH QoS 3 invalid", FixedHeader{PacketType: PacketPUBLISH, Flags: 0x06}, ErrInvalidPacketFlags},
		{"PUBLISH DUP", FixedHeader{PacketType: PacketPUBLISH, Flags: 0x08}, nil},
		{"PUBLISH RETAIN", FixedHeader{PacketType: PacketPUBLISH, Flags: 0x01}, nil},
		{"PUBLISH all flags", FixedHeader{PacketType: PacketPUBLISH, Flags: 0x0D}, nil},

		// PUBACK must have flags = 0x00
		{"PUBACK valid", FixedHeader{PacketType: PacketPUBACK, Flags: 0x00}, nil},
		{"PUBACK invalid", FixedHeader{PacketType: PacketPUBACK, Flags: 0x01}, ErrInvalidPacketFlags},

		// PUBREC must have flags = 0x00
		{"PUBREC valid", FixedHeader{PacketType: PacketPUBREC, Flags: 0x00}, nil},
		{"PUBREC invalid", FixedHeader{PacketType: PacketPUBREC, Flags: 0x02}, ErrInvalidPacketFlags},

		// PUBREL must have flags = 0x02
		{"PUBREL valid", FixedHeader{PacketType: PacketPUBREL, Flags: 0x02}, nil},
		{"PUBREL invalid 0x00", FixedHeader{PacketType: PacketPUBREL, Flags: 0x00}, ErrInvalidPacketFlags},
		{"PUBREL invalid 0x03", FixedHeader{PacketType: PacketPUBREL, Flags: 0x03}, ErrInvalidPacketFlags},

		// PUBCOMP must have flags = 0x00
		{"PUBCOMP valid", FixedHeader{PacketType: PacketPUBCOMP, Flags: 0x00}, nil},
		{"PUBCOMP invalid", FixedHeader{PacketType: PacketPUBCOMP, Flags: 0x02}, ErrInvalidPacketFlags},

		// SUBSCRIBE must have flags = 0x02
		{"SUBSCRIBE valid", FixedHeader{PacketType: PacketSUBSCRIBE, Flags: 0x02}, nil},
		{"SUBSCRIBE invalid", FixedHeader{PacketType: PacketSUBSCRIBE, Flags: 0x00}, ErrInvalidPacketFlags},

		// SUBACK must have flags = 0x00
		{"SUBACK valid", FixedHeader{PacketType: PacketSUBACK, Flags: 0x00}, nil},
		{"SUBACK invalid", FixedHeader{PacketType: PacketSUBACK, Flags: 0x02}, ErrInvalidPacketFlags},

		// UNSUBSCRIBE must have flags = 0x02
		{"UNSUBSCRIBE valid", FixedHeader{PacketType: PacketUNSUBSCRIBE, Flags: 0x02}, nil},
		{"UNSUBSCRIBE invalid", FixedHeader{PacketType: PacketUNSUBSCRIBE, Flags: 0x00}, ErrInvalidPacketFlags},

		// UNSUBACK must have flags = 0x00
		{"UNSUBACK valid", FixedHeader{PacketType: PacketUNSUBACK, Flags: 0x00}, nil},
		{"UNSUBACK invalid", FixedHeader{PacketType: PacketUNSUBACK, Flags: 0x01}, ErrInvalidPacketFlags},

		// PINGREQ must have flags = 0x00
		{"PINGREQ valid", FixedHeader{PacketType: PacketPINGREQ, Flags: 0x00}, nil},
		{"PINGREQ invalid", FixedHeader{PacketType: PacketPINGREQ, Flags: 0x0F}, ErrInvalidPacketFlags},

		// PINGRESP must have flags = 0x00
		{"PINGRESP valid", FixedHeader{PacketType: PacketPINGRESP, Flags: 0x00}, nil},
		{"PINGRESP invalid", FixedHeader{PacketType: PacketPINGRESP, Flags: 0x01}, ErrInvalidPacketFlags},

		// DISCONNECT must have flags = 0x00
		{"DISCONNECT valid", FixedHeader{PacketType: PacketDISCONNECT, Flags: 0x00}, nil},
		{"DISCONNECT invalid", FixedHeader{PacketType: PacketDISCONNECT, Flags: 0x02}, ErrInvalidPacketFlags},

		// AUTH must have flags = 0x00
		{"AUTH valid", FixedHeader{PacketType: PacketAUTH, Flags: 0x00}, nil},
		{"AUTH invalid", FixedHeader{PacketType: PacketAUTH, Flags: 0x01}, ErrInvalidPacketFlags},

		// Invalid packet type
		{"invalid packet type", FixedHeader{PacketType: PacketType(0), Flags: 0x00}, ErrInvalidPacketType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.header.ValidateFlags()
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFixedHeaderPUBLISHFlags(t *testing.T) {
	t.Run("DUP flag", func(t *testing.T) {
		var h FixedHeader
		assert.False(t, h.DUP())

		h.SetDUP(true)
		assert.True(t, h.DUP())
		assert.Equal(t, byte(0x08), h.Flags)

		h.SetDUP(false)
		assert.False(t, h.DUP())
		assert.Equal(t, byte(0x00), h.Flags)
	})

	t.Run("QoS levels", func(t *testing.T) {
		var h FixedHeader
		assert.Equal(t, byte(0), h.QoS())

		h.SetQoS(1)
		assert.Equal(t, byte(1), h.QoS())
		assert.Equal(t, byte(0x02), h.Flags)

		h.SetQoS(2)
		assert.Equal(t, byte(2), h.QoS())
		assert.Equal(t, byte(0x04), h.Flags)

		h.SetQoS(0)
		assert.Equal(t, byte(0), h.QoS())
		assert.Equal(t, byte(0x00), h.Flags)
	})

	t.Run("RETAIN flag", func(t *testing.T) {
		var h FixedHeader
		assert.False(t, h.Retain())

		h.SetRetain(true)
		assert.True(t, h.Retain())
		assert.Equal(t, byte(0x01), h.Flags)

		h.SetRetain(false)
		assert.False(t, h.Retain())
		assert.Equal(t, byte(0x00), h.Flags)
	})

	t.Run("combined flags", func(t *testing.T) {
		var h FixedHeader
		h.SetDUP(true)
		h.SetQoS(2)
		h.SetRetain(true)

		assert.True(t, h.DUP())
		assert.Equal(t, byte(2), h.QoS())
		assert.True(t, h.Retain())
		assert.Equal(t, byte(0x0D), h.Flags)
	})
}

// Benchmarks

func BenchmarkFixedHeaderEncode(b *testing.B) {
	headers := []FixedHeader{
		{PacketType: PacketCONNECT, Flags: 0x00, RemainingLength: 0},
		{PacketType: PacketPUBLISH, Flags: 0x00, RemainingLength: 127},
		{PacketType: PacketPUBLISH, Flags: 0x02, RemainingLength: 16383},
		{PacketType: PacketPUBLISH, Flags: 0x04, RemainingLength: 268435455},
	}

	for _, h := range headers {
		b.Run("", func(b *testing.B) {
			var buf bytes.Buffer
			buf.Grow(5)

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				buf.Reset()
				_, _ = h.Encode(&buf)
			}
		})
	}
}

func BenchmarkFixedHeaderDecode(b *testing.B) {
	testData := [][]byte{
		{0x10, 0x00},                   // CONNECT, length 0
		{0x30, 0x7F},                   // PUBLISH, length 127
		{0x32, 0xFF, 0x7F},             // PUBLISH, length 16383
		{0x34, 0xFF, 0xFF, 0xFF, 0x7F}, // PUBLISH, length 268435455
	}

	for _, data := range testData {
		b.Run("", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				var h FixedHeader
				_, _ = h.Decode(bytes.NewReader(data))
			}
		})
	}
}

func BenchmarkFixedHeaderValidateFlags(b *testing.B) {
	headers := []FixedHeader{
		{PacketType: PacketCONNECT, Flags: 0x00},
		{PacketType: PacketPUBLISH, Flags: 0x0D},
		{PacketType: PacketSUBSCRIBE, Flags: 0x02},
	}

	for _, h := range headers {
		b.Run("", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				_ = h.ValidateFlags()
			}
		})
	}
}

// Fuzz tests

func FuzzFixedHeaderDecode(f *testing.F) {
	// Add seed corpus - valid packets
	f.Add([]byte{0x10, 0x00})                   // CONNECT
	f.Add([]byte{0x20, 0x02})                   // CONNACK
	f.Add([]byte{0x30, 0x00})                   // PUBLISH QoS 0
	f.Add([]byte{0x3A, 0x05})                   // PUBLISH QoS 1 DUP
	f.Add([]byte{0x62, 0x02})                   // PUBREL
	f.Add([]byte{0x82, 0x0A})                   // SUBSCRIBE
	f.Add([]byte{0xA2, 0x05})                   // UNSUBSCRIBE
	f.Add([]byte{0xC0, 0x00})                   // PINGREQ
	f.Add([]byte{0xD0, 0x00})                   // PINGRESP
	f.Add([]byte{0xE0, 0x00})                   // DISCONNECT
	f.Add([]byte{0xF0, 0x00})                   // AUTH
	f.Add([]byte{0x30, 0xFF, 0xFF, 0xFF, 0x7F}) // max length

	// Edge cases
	f.Add([]byte{0x00, 0x00})                   // invalid packet type 0
	f.Add([]byte{0xFF, 0x00})                   // packet type 15 with invalid flags
	f.Add([]byte{0x80, 0x80, 0x80, 0x80, 0x80}) // too many continuation bytes
	f.Add([]byte{0x10})                         // incomplete
	f.Add([]byte{0x30, 0x80})                   // incomplete varint

	// Random generated seeds
	for range 10 {
		size := rand.IntN(8) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(data)
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		var h FixedHeader
		_, _ = h.Decode(bytes.NewReader(data))
	})
}
