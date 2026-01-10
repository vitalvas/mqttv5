package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectPacketType(t *testing.T) {
	p := &ConnectPacket{}
	assert.Equal(t, PacketCONNECT, p.Type())
}

func TestConnectPacketProperties(t *testing.T) {
	p := &ConnectPacket{}
	p.Props.Set(PropSessionExpiryInterval, uint32(3600))
	assert.Equal(t, uint32(3600), p.Properties().GetUint32(PropSessionExpiryInterval))
}

func TestConnectPacketEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		packet ConnectPacket
	}{
		{
			name: "minimal",
			packet: ConnectPacket{
				ClientID:   "test-client",
				CleanStart: true,
				KeepAlive:  60,
			},
		},
		{
			name: "with username and password",
			packet: ConnectPacket{
				ClientID:   "client-1",
				CleanStart: true,
				KeepAlive:  120,
				Username:   "user",
				Password:   []byte("secret"),
			},
		},
		{
			name: "with will message",
			packet: ConnectPacket{
				ClientID:    "client-2",
				CleanStart:  true,
				KeepAlive:   30,
				WillFlag:    true,
				WillTopic:   "client/status",
				WillPayload: []byte("offline"),
				WillQoS:     1,
				WillRetain:  true,
			},
		},
		{
			name: "with will message and properties",
			packet: ConnectPacket{
				ClientID:    "client-3",
				CleanStart:  true,
				KeepAlive:   60,
				WillFlag:    true,
				WillTopic:   "will/topic",
				WillPayload: []byte("goodbye"),
				WillQoS:     2,
			},
		},
		{
			name: "with session properties",
			packet: ConnectPacket{
				ClientID:   "client-4",
				CleanStart: false,
				KeepAlive:  300,
			},
		},
		{
			name: "zero keep alive",
			packet: ConnectPacket{
				ClientID:   "client-5",
				CleanStart: true,
				KeepAlive:  0,
			},
		},
		{
			name: "max keep alive",
			packet: ConnectPacket{
				ClientID:   "client-6",
				CleanStart: true,
				KeepAlive:  65535,
			},
		},
		{
			name: "empty client ID with clean start",
			packet: ConnectPacket{
				ClientID:   "",
				CleanStart: true,
				KeepAlive:  60,
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
			assert.Equal(t, PacketCONNECT, header.PacketType)
			assert.Equal(t, byte(0x00), header.Flags)

			// Decode CONNECT packet
			var decoded ConnectPacket
			n3, err := decoded.Decode(&buf, header)
			require.NoError(t, err)
			assert.Equal(t, int(header.RemainingLength), n3)

			// Verify fields
			assert.Equal(t, tt.packet.ClientID, decoded.ClientID)
			assert.Equal(t, tt.packet.CleanStart, decoded.CleanStart)
			assert.Equal(t, tt.packet.KeepAlive, decoded.KeepAlive)
			assert.Equal(t, tt.packet.Username, decoded.Username)
			assert.Equal(t, tt.packet.Password, decoded.Password)
			assert.Equal(t, tt.packet.WillFlag, decoded.WillFlag)
			if tt.packet.WillFlag {
				assert.Equal(t, tt.packet.WillTopic, decoded.WillTopic)
				assert.Equal(t, tt.packet.WillPayload, decoded.WillPayload)
				assert.Equal(t, tt.packet.WillQoS, decoded.WillQoS)
				assert.Equal(t, tt.packet.WillRetain, decoded.WillRetain)
			}
		})
	}
}

func TestConnectPacketWithProperties(t *testing.T) {
	packet := ConnectPacket{
		ClientID:   "prop-test",
		CleanStart: true,
		KeepAlive:  60,
	}
	packet.Props.Set(PropSessionExpiryInterval, uint32(3600))
	packet.Props.Set(PropReceiveMaximum, uint16(100))
	packet.Props.Set(PropMaximumPacketSize, uint32(1048576))
	packet.Props.Set(PropTopicAliasMaximum, uint16(10))
	packet.Props.Set(PropRequestResponseInfo, byte(1))
	packet.Props.Set(PropRequestProblemInfo, byte(1))
	packet.Props.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})
	packet.Props.Set(PropAuthenticationMethod, "PLAIN")
	packet.Props.Set(PropAuthenticationData, []byte{0x01, 0x02, 0x03})

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded ConnectPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	assert.Equal(t, uint32(3600), decoded.Props.GetUint32(PropSessionExpiryInterval))
	assert.Equal(t, uint16(100), decoded.Props.GetUint16(PropReceiveMaximum))
	assert.Equal(t, uint32(1048576), decoded.Props.GetUint32(PropMaximumPacketSize))
	assert.Equal(t, uint16(10), decoded.Props.GetUint16(PropTopicAliasMaximum))
	assert.Equal(t, byte(1), decoded.Props.GetByte(PropRequestResponseInfo))
	assert.Equal(t, byte(1), decoded.Props.GetByte(PropRequestProblemInfo))
	assert.Equal(t, "PLAIN", decoded.Props.GetString(PropAuthenticationMethod))
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded.Props.GetBinary(PropAuthenticationData))

	ups := decoded.Props.GetAllStringPairs(PropUserProperty)
	assert.Len(t, ups, 1)
	assert.Equal(t, "key", ups[0].Key)
	assert.Equal(t, "value", ups[0].Value)
}

func TestConnectPacketWithWillProperties(t *testing.T) {
	packet := ConnectPacket{
		ClientID:    "will-prop-test",
		CleanStart:  true,
		KeepAlive:   60,
		WillFlag:    true,
		WillTopic:   "last/will",
		WillPayload: []byte("goodbye"),
		WillQoS:     1,
	}
	packet.WillProps.Set(PropWillDelayInterval, uint32(60))
	packet.WillProps.Set(PropPayloadFormatIndicator, byte(1))
	packet.WillProps.Set(PropMessageExpiryInterval, uint32(3600))
	packet.WillProps.Set(PropContentType, "text/plain")
	packet.WillProps.Set(PropResponseTopic, "response/topic")
	packet.WillProps.Set(PropCorrelationData, []byte{0xAB, 0xCD})

	var buf bytes.Buffer
	_, err := packet.Encode(&buf)
	require.NoError(t, err)

	var header FixedHeader
	_, err = header.Decode(&buf)
	require.NoError(t, err)

	var decoded ConnectPacket
	_, err = decoded.Decode(&buf, header)
	require.NoError(t, err)

	assert.Equal(t, uint32(60), decoded.WillProps.GetUint32(PropWillDelayInterval))
	assert.Equal(t, byte(1), decoded.WillProps.GetByte(PropPayloadFormatIndicator))
	assert.Equal(t, uint32(3600), decoded.WillProps.GetUint32(PropMessageExpiryInterval))
	assert.Equal(t, "text/plain", decoded.WillProps.GetString(PropContentType))
	assert.Equal(t, "response/topic", decoded.WillProps.GetString(PropResponseTopic))
	assert.Equal(t, []byte{0xAB, 0xCD}, decoded.WillProps.GetBinary(PropCorrelationData))
}

func TestConnectPacketValidation(t *testing.T) {
	tests := []struct {
		name    string
		packet  ConnectPacket
		wantErr error
	}{
		{
			name: "valid minimal",
			packet: ConnectPacket{
				ClientID:   "test",
				CleanStart: true,
			},
			wantErr: nil,
		},
		{
			name: "empty client ID with clean start",
			packet: ConnectPacket{
				ClientID:   "",
				CleanStart: true,
			},
			wantErr: nil,
		},
		{
			name: "empty client ID without clean start",
			packet: ConnectPacket{
				ClientID:   "",
				CleanStart: false,
			},
			wantErr: ErrClientIDRequired,
		},
		{
			name: "will QoS without will flag",
			packet: ConnectPacket{
				ClientID:   "test",
				CleanStart: true,
				WillFlag:   false,
				WillQoS:    1,
			},
			wantErr: ErrInvalidConnectFlags,
		},
		{
			name: "will retain without will flag",
			packet: ConnectPacket{
				ClientID:   "test",
				CleanStart: true,
				WillFlag:   false,
				WillRetain: true,
			},
			wantErr: ErrInvalidConnectFlags,
		},
		{
			name: "invalid will QoS",
			packet: ConnectPacket{
				ClientID:   "test",
				CleanStart: true,
				WillFlag:   true,
				WillQoS:    3,
				WillTopic:  "topic",
			},
			wantErr: ErrInvalidConnectFlags,
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

func TestConnectPacketDecodeErrors(t *testing.T) {
	t.Run("invalid protocol name", func(t *testing.T) {
		// Build a packet with wrong protocol name
		var buf bytes.Buffer
		_, _ = encodeString(&buf, "MQTT3")
		buf.WriteByte(5)
		buf.WriteByte(0x02)
		buf.Write([]byte{0x00, 0x3C}) // keep alive
		buf.WriteByte(0x00)           // empty properties
		_, _ = encodeString(&buf, "client")

		var header FixedHeader
		header.PacketType = PacketCONNECT
		header.RemainingLength = uint32(buf.Len())

		var p ConnectPacket
		_, err := p.Decode(&buf, header)
		assert.ErrorIs(t, err, ErrInvalidProtocolName)
	})

	t.Run("invalid protocol version", func(t *testing.T) {
		var buf bytes.Buffer
		_, _ = encodeString(&buf, "MQTT")
		buf.WriteByte(4) // Wrong version
		buf.WriteByte(0x02)
		buf.Write([]byte{0x00, 0x3C})
		buf.WriteByte(0x00)
		_, _ = encodeString(&buf, "client")

		var header FixedHeader
		header.PacketType = PacketCONNECT
		header.RemainingLength = uint32(buf.Len())

		var p ConnectPacket
		_, err := p.Decode(&buf, header)
		assert.ErrorIs(t, err, ErrInvalidProtocolVersion)
	})

	t.Run("reserved bit set", func(t *testing.T) {
		var buf bytes.Buffer
		_, _ = encodeString(&buf, "MQTT")
		buf.WriteByte(5)
		buf.WriteByte(0x01) // Reserved bit set
		buf.Write([]byte{0x00, 0x3C})
		buf.WriteByte(0x00)
		_, _ = encodeString(&buf, "client")

		var header FixedHeader
		header.PacketType = PacketCONNECT
		header.RemainingLength = uint32(buf.Len())

		var p ConnectPacket
		_, err := p.Decode(&buf, header)
		assert.ErrorIs(t, err, ErrInvalidConnectFlags)
	})

	t.Run("will QoS 3 invalid", func(t *testing.T) {
		var buf bytes.Buffer
		_, _ = encodeString(&buf, "MQTT")
		buf.WriteByte(5)
		buf.WriteByte(0x04 | 0x18) // Will flag + QoS 3
		buf.Write([]byte{0x00, 0x3C})
		buf.WriteByte(0x00)
		_, _ = encodeString(&buf, "client")

		var header FixedHeader
		header.PacketType = PacketCONNECT
		header.RemainingLength = uint32(buf.Len())

		var p ConnectPacket
		_, err := p.Decode(&buf, header)
		assert.ErrorIs(t, err, ErrInvalidConnectFlags)
	})
}

func TestConnectPacketEncodeErrors(t *testing.T) {
	t.Run("encode with client ID required error", func(t *testing.T) {
		invalid := ConnectPacket{ClientID: "", CleanStart: false}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrClientIDRequired)
	})

	t.Run("encode with invalid will QoS", func(t *testing.T) {
		invalid := ConnectPacket{
			ClientID:   "test",
			CleanStart: true,
			WillFlag:   true,
			WillQoS:    3,
			WillTopic:  "topic",
		}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrInvalidConnectFlags)
	})

	t.Run("encode with will QoS without will flag", func(t *testing.T) {
		invalid := ConnectPacket{
			ClientID:   "test",
			CleanStart: true,
			WillFlag:   false,
			WillQoS:    1,
		}
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.ErrorIs(t, err, ErrInvalidConnectFlags)
	})

	t.Run("encode with invalid property", func(t *testing.T) {
		invalid := ConnectPacket{ClientID: "test", CleanStart: true}
		invalid.Props.Set(PropServerKeepAlive, uint16(60)) // Not valid for CONNECT
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.Error(t, err)
	})

	t.Run("encode with invalid will property", func(t *testing.T) {
		invalid := ConnectPacket{
			ClientID:   "test",
			CleanStart: true,
			WillFlag:   true,
			WillTopic:  "topic",
		}
		invalid.WillProps.Set(PropServerKeepAlive, uint16(60)) // Not valid for WILL
		var buf bytes.Buffer
		_, err := invalid.Encode(&buf)
		assert.Error(t, err)
	})
}

func TestConnectPacketDecodeMoreErrors(t *testing.T) {
	t.Run("keep alive read error", func(t *testing.T) {
		var buf bytes.Buffer
		_, _ = encodeString(&buf, "MQTT")
		buf.WriteByte(5)    // Version
		buf.WriteByte(0x02) // Flags (clean start)
		buf.WriteByte(0x00) // Only 1 byte of keep alive

		header := FixedHeader{
			PacketType:      PacketCONNECT,
			RemainingLength: uint32(buf.Len()),
		}
		var p ConnectPacket
		_, err := p.Decode(bytes.NewReader(buf.Bytes()), header)
		assert.Error(t, err)
	})

	t.Run("properties read error", func(t *testing.T) {
		var buf bytes.Buffer
		_, _ = encodeString(&buf, "MQTT")
		buf.WriteByte(5)              // Version
		buf.WriteByte(0x02)           // Flags (clean start)
		buf.Write([]byte{0x00, 0x3C}) // Keep alive
		buf.WriteByte(0xFF)           // Invalid property length

		header := FixedHeader{
			PacketType:      PacketCONNECT,
			RemainingLength: uint32(buf.Len()),
		}
		var p ConnectPacket
		_, err := p.Decode(bytes.NewReader(buf.Bytes()), header)
		assert.Error(t, err)
	})

	t.Run("invalid properties for CONNECT", func(t *testing.T) {
		var propBuf bytes.Buffer
		props := Properties{}
		props.Set(PropServerKeepAlive, uint16(60)) // Not valid for CONNECT
		_, _ = props.Encode(&propBuf)

		var buf bytes.Buffer
		_, _ = encodeString(&buf, "MQTT")
		buf.WriteByte(5)              // Version
		buf.WriteByte(0x02)           // Flags (clean start)
		buf.Write([]byte{0x00, 0x3C}) // Keep alive
		buf.Write(propBuf.Bytes())
		_, _ = encodeString(&buf, "client")

		header := FixedHeader{
			PacketType:      PacketCONNECT,
			RemainingLength: uint32(buf.Len()),
		}
		var p ConnectPacket
		_, err := p.Decode(bytes.NewReader(buf.Bytes()), header)
		assert.Error(t, err)
	})

	t.Run("will QoS without will flag decode error", func(t *testing.T) {
		var buf bytes.Buffer
		_, _ = encodeString(&buf, "MQTT")
		buf.WriteByte(5)              // Version
		buf.WriteByte(0x08)           // Flags: will QoS=1 but will flag=0 (invalid)
		buf.Write([]byte{0x00, 0x3C}) // Keep alive
		buf.WriteByte(0x00)           // Empty properties
		_, _ = encodeString(&buf, "client")

		header := FixedHeader{
			PacketType:      PacketCONNECT,
			RemainingLength: uint32(buf.Len()),
		}
		var p ConnectPacket
		_, err := p.Decode(bytes.NewReader(buf.Bytes()), header)
		assert.ErrorIs(t, err, ErrInvalidConnectFlags)
	})

	t.Run("will retain without will flag decode error", func(t *testing.T) {
		var buf bytes.Buffer
		_, _ = encodeString(&buf, "MQTT")
		buf.WriteByte(5)              // Version
		buf.WriteByte(0x20)           // Flags: will retain but will flag=0 (invalid)
		buf.Write([]byte{0x00, 0x3C}) // Keep alive
		buf.WriteByte(0x00)           // Empty properties
		_, _ = encodeString(&buf, "client")

		header := FixedHeader{
			PacketType:      PacketCONNECT,
			RemainingLength: uint32(buf.Len()),
		}
		var p ConnectPacket
		_, err := p.Decode(bytes.NewReader(buf.Bytes()), header)
		assert.ErrorIs(t, err, ErrInvalidConnectFlags)
	})

	t.Run("invalid will properties", func(t *testing.T) {
		var willPropBuf bytes.Buffer
		willProps := Properties{}
		willProps.Set(PropServerKeepAlive, uint16(60)) // Not valid for WILL
		_, _ = willProps.Encode(&willPropBuf)

		var buf bytes.Buffer
		_, _ = encodeString(&buf, "MQTT")
		buf.WriteByte(5)              // Version
		buf.WriteByte(0x06)           // Flags: clean start + will flag
		buf.Write([]byte{0x00, 0x3C}) // Keep alive
		buf.WriteByte(0x00)           // Empty CONNECT properties
		_, _ = encodeString(&buf, "client")
		buf.Write(willPropBuf.Bytes()) // Will properties
		_, _ = encodeString(&buf, "will/topic")
		_, _ = encodeBinary(&buf, []byte("payload"))

		header := FixedHeader{
			PacketType:      PacketCONNECT,
			RemainingLength: uint32(buf.Len()),
		}
		var p ConnectPacket
		_, err := p.Decode(bytes.NewReader(buf.Bytes()), header)
		assert.Error(t, err)
	})
}

func TestConnectFlagsRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		packet   ConnectPacket
		expected byte
	}{
		{
			name:     "clean start only",
			packet:   ConnectPacket{CleanStart: true},
			expected: 0x02,
		},
		{
			name:     "will QoS 0",
			packet:   ConnectPacket{WillFlag: true},
			expected: 0x04,
		},
		{
			name:     "will QoS 1",
			packet:   ConnectPacket{WillFlag: true, WillQoS: 1},
			expected: 0x0C,
		},
		{
			name:     "will QoS 2",
			packet:   ConnectPacket{WillFlag: true, WillQoS: 2},
			expected: 0x14,
		},
		{
			name:     "will retain",
			packet:   ConnectPacket{WillFlag: true, WillRetain: true},
			expected: 0x24,
		},
		{
			name:     "username",
			packet:   ConnectPacket{Username: "user"},
			expected: 0x80,
		},
		{
			name:     "password",
			packet:   ConnectPacket{Password: []byte("pass")},
			expected: 0x40,
		},
		{
			name:     "all flags",
			packet:   ConnectPacket{CleanStart: true, WillFlag: true, WillQoS: 2, WillRetain: true, Username: "u", Password: []byte("p")},
			expected: 0xF6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags := tt.packet.connectFlags()
			assert.Equal(t, tt.expected, flags)

			var p ConnectPacket
			err := p.setConnectFlags(flags)
			require.NoError(t, err)

			assert.Equal(t, tt.packet.CleanStart, p.CleanStart)
			assert.Equal(t, tt.packet.WillFlag, p.WillFlag)
			assert.Equal(t, tt.packet.WillQoS, p.WillQoS)
			assert.Equal(t, tt.packet.WillRetain, p.WillRetain)
		})
	}
}

// Benchmarks

func BenchmarkConnectPacketEncode(b *testing.B) {
	packets := []struct {
		name   string
		packet ConnectPacket
	}{
		{
			name: "minimal",
			packet: ConnectPacket{
				ClientID:   "test-client",
				CleanStart: true,
				KeepAlive:  60,
			},
		},
		{
			name: "with auth",
			packet: ConnectPacket{
				ClientID:   "client-with-auth",
				CleanStart: true,
				KeepAlive:  120,
				Username:   "username",
				Password:   []byte("password123"),
			},
		},
		{
			name: "with will",
			packet: ConnectPacket{
				ClientID:    "client-with-will",
				CleanStart:  true,
				KeepAlive:   60,
				WillFlag:    true,
				WillTopic:   "client/status",
				WillPayload: []byte("offline"),
				WillQoS:     1,
				WillRetain:  true,
			},
		},
	}

	for _, tt := range packets {
		b.Run(tt.name, func(b *testing.B) {
			var buf bytes.Buffer
			buf.Grow(256)

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				buf.Reset()
				_, _ = tt.packet.Encode(&buf)
			}
		})
	}
}

func BenchmarkConnectPacketDecode(b *testing.B) {
	packets := []struct {
		name   string
		packet ConnectPacket
	}{
		{
			name: "minimal",
			packet: ConnectPacket{
				ClientID:   "test-client",
				CleanStart: true,
				KeepAlive:  60,
			},
		},
		{
			name: "with auth",
			packet: ConnectPacket{
				ClientID:   "client-with-auth",
				CleanStart: true,
				KeepAlive:  120,
				Username:   "username",
				Password:   []byte("password123"),
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
				var p ConnectPacket
				_, _ = p.Decode(r, header)
			}
		})
	}
}

// Fuzz tests

func FuzzConnectPacketDecode(f *testing.F) {
	// Valid CONNECT packet seeds
	validPacket := ConnectPacket{
		ClientID:   "test",
		CleanStart: true,
		KeepAlive:  60,
	}
	var buf bytes.Buffer
	_, _ = validPacket.Encode(&buf)
	f.Add(buf.Bytes())

	// With will
	willPacket := ConnectPacket{
		ClientID:    "test",
		CleanStart:  true,
		KeepAlive:   60,
		WillFlag:    true,
		WillTopic:   "topic",
		WillPayload: []byte("payload"),
		WillQoS:     1,
	}
	buf.Reset()
	_, _ = willPacket.Encode(&buf)
	f.Add(buf.Bytes())

	// With auth
	authPacket := ConnectPacket{
		ClientID:   "test",
		CleanStart: true,
		KeepAlive:  60,
		Username:   "user",
		Password:   []byte("pass"),
	}
	buf.Reset()
	_, _ = authPacket.Encode(&buf)
	f.Add(buf.Bytes())

	// Edge cases
	f.Add([]byte{0x10, 0x00})                   // minimal invalid
	f.Add([]byte{0x10, 0x0A, 0x00, 0x04})       // truncated protocol name
	f.Add([]byte{0x10, 0xFF, 0xFF, 0xFF, 0x7F}) // max length

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
		if header.PacketType != PacketCONNECT {
			return
		}

		// Create limited reader for remaining data
		remaining := data[n:]
		if len(remaining) < int(header.RemainingLength) {
			return
		}

		var p ConnectPacket
		_, _ = p.Decode(bytes.NewReader(remaining), header)
	})
}
