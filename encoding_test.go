package mqttv5

import (
	"bytes"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{
			name:  "empty string",
			input: "",
		},
		{
			name:  "simple ASCII",
			input: "hello",
		},
		{
			name:  "UTF-8 characters",
			input: "hello 世界 🌍",
		},
		{
			name:  "max length string",
			input: strings.Repeat("a", 65535),
		},
		{
			name:    "string too long",
			input:   strings.Repeat("a", 65536),
			wantErr: ErrStringTooLong,
		},
		{
			name:    "string with null",
			input:   "hello\x00world",
			wantErr: ErrStringContainsNull,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			n, err := encodeString(&buf, tt.input)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, 2+len(tt.input), n)
			assert.Equal(t, 2+len(tt.input), buf.Len())

			decoded, n2, err := decodeString(&buf)
			require.NoError(t, err)
			assert.Equal(t, 2+len(tt.input), n2)
			assert.Equal(t, tt.input, decoded)
		})
	}
}

func TestDecodeStringInvalidUTF8(t *testing.T) {
	// Create a buffer with invalid UTF-8: length=3, bytes=0xFF 0xFE 0xFD
	buf := bytes.NewBuffer([]byte{0x00, 0x03, 0xFF, 0xFE, 0xFD})

	_, _, err := decodeString(buf)
	assert.ErrorIs(t, err, ErrInvalidUTF8)
}

func TestEncodeStringInvalidUTF8(t *testing.T) {
	// Create string with invalid UTF-8 bytes
	invalidUTF8 := string([]byte{0xFF, 0xFE, 0xFD})
	var buf bytes.Buffer

	_, err := encodeString(&buf, invalidUTF8)
	assert.ErrorIs(t, err, ErrInvalidUTF8)
}

func TestDecodeStringWithNull(t *testing.T) {
	// Create a buffer with null character in the string
	buf := bytes.NewBuffer([]byte{0x00, 0x05, 'h', 'e', 0x00, 'l', 'o'})

	_, _, err := decodeString(buf)
	assert.ErrorIs(t, err, ErrStringContainsNull)
}

func TestEncodeDecodeBinary(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr error
	}{
		{
			name:  "nil data",
			input: nil,
		},
		{
			name:  "empty data",
			input: []byte{},
		},
		{
			name:  "simple data",
			input: []byte{0x01, 0x02, 0x03},
		},
		{
			name:  "binary with null",
			input: []byte{0x00, 0x01, 0x00},
		},
		{
			name:  "max length data",
			input: bytes.Repeat([]byte{0xAB}, 65535),
		},
		{
			name:    "data too long",
			input:   bytes.Repeat([]byte{0xAB}, 65536),
			wantErr: ErrBinaryTooLong,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			n, err := encodeBinary(&buf, tt.input)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			expectedLen := 2 + len(tt.input)
			assert.Equal(t, expectedLen, n)
			assert.Equal(t, expectedLen, buf.Len())

			decoded, n2, err := decodeBinary(&buf)
			require.NoError(t, err)
			assert.Equal(t, expectedLen, n2)

			if len(tt.input) == 0 {
				assert.Nil(t, decoded)
			} else {
				assert.Equal(t, tt.input, decoded)
			}
		})
	}
}

func TestEncodeDecodeStringPair(t *testing.T) {
	tests := []struct {
		name  string
		input StringPair
	}{
		{
			name:  "empty pair",
			input: StringPair{Key: "", Value: ""},
		},
		{
			name:  "simple pair",
			input: StringPair{Key: "key", Value: "value"},
		},
		{
			name:  "UTF-8 pair",
			input: StringPair{Key: "键", Value: "值"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			n, err := encodeStringPair(&buf, tt.input)
			require.NoError(t, err)

			expectedLen := 2 + len(tt.input.Key) + 2 + len(tt.input.Value)
			assert.Equal(t, expectedLen, n)

			decoded, n2, err := decodeStringPair(&buf)
			require.NoError(t, err)
			assert.Equal(t, expectedLen, n2)
			assert.Equal(t, tt.input, decoded)
		})
	}
}

func TestEncodeStringPairErrors(t *testing.T) {
	t.Run("invalid key", func(t *testing.T) {
		var buf bytes.Buffer
		pair := StringPair{Key: string([]byte{0xFF, 0xFE}), Value: "valid"}

		_, err := encodeStringPair(&buf, pair)
		assert.ErrorIs(t, err, ErrInvalidUTF8)
	})

	t.Run("invalid value", func(t *testing.T) {
		var buf bytes.Buffer
		pair := StringPair{Key: "valid", Value: string([]byte{0xFF, 0xFE})}

		_, err := encodeStringPair(&buf, pair)
		assert.ErrorIs(t, err, ErrInvalidUTF8)
	})
}

func TestDecodeStringPairErrors(t *testing.T) {
	t.Run("truncated key", func(t *testing.T) {
		// Only length prefix, no key data
		buf := bytes.NewBuffer([]byte{0x00, 0x05})

		_, _, err := decodeStringPair(buf)
		assert.Error(t, err)
	})

	t.Run("truncated value", func(t *testing.T) {
		// Valid key but truncated value
		buf := bytes.NewBuffer([]byte{0x00, 0x01, 'k', 0x00, 0x05})

		_, _, err := decodeStringPair(buf)
		assert.Error(t, err)
	})
}

func TestEncodeDecodeVarint(t *testing.T) {
	tests := []struct {
		name      string
		input     uint32
		wantBytes int
		wantErr   error
	}{
		{
			name:      "zero",
			input:     0,
			wantBytes: 1,
		},
		{
			name:      "one",
			input:     1,
			wantBytes: 1,
		},
		{
			name:      "max 1 byte (127)",
			input:     127,
			wantBytes: 1,
		},
		{
			name:      "min 2 bytes (128)",
			input:     128,
			wantBytes: 2,
		},
		{
			name:      "max 2 bytes (16383)",
			input:     16383,
			wantBytes: 2,
		},
		{
			name:      "min 3 bytes (16384)",
			input:     16384,
			wantBytes: 3,
		},
		{
			name:      "max 3 bytes (2097151)",
			input:     2097151,
			wantBytes: 3,
		},
		{
			name:      "min 4 bytes (2097152)",
			input:     2097152,
			wantBytes: 4,
		},
		{
			name:      "max value (268435455)",
			input:     268435455,
			wantBytes: 4,
		},
		{
			name:    "exceeds max value",
			input:   268435456,
			wantErr: ErrVarintTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer

			n, err := encodeVarint(&buf, tt.input)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantBytes, n)
			assert.Equal(t, tt.wantBytes, buf.Len())

			decoded, n2, err := decodeVarint(&buf)
			require.NoError(t, err)
			assert.Equal(t, tt.wantBytes, n2)
			assert.Equal(t, tt.input, decoded)
		})
	}
}

func TestVarintSize(t *testing.T) {
	tests := []struct {
		value uint32
		want  int
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
		{2097151, 3},
		{2097152, 4},
		{268435455, 4},
	}

	for _, tt := range tests {
		got := varintSize(tt.value)
		assert.Equal(t, tt.want, got, "varintSize(%d)", tt.value)
	}
}

func TestDecodeVarintMalformed(t *testing.T) {
	// 5 bytes with continuation bits set (malformed)
	buf := bytes.NewBuffer([]byte{0x80, 0x80, 0x80, 0x80, 0x01})

	_, _, err := decodeVarint(buf)
	assert.Error(t, err)
}

func TestDecodeVarintOverlong(t *testing.T) {
	// Overlong encodings use more bytes than necessary to represent a value
	tests := []struct {
		name  string
		input []byte
		value uint32
	}{
		{
			name:  "zero in 2 bytes",
			input: []byte{0x80, 0x00}, // 0 encoded in 2 bytes (should be 0x00)
			value: 0,
		},
		{
			name:  "1 in 2 bytes",
			input: []byte{0x81, 0x00}, // 1 encoded in 2 bytes (should be 0x01)
			value: 1,
		},
		{
			name:  "127 in 2 bytes",
			input: []byte{0xFF, 0x00}, // 127 encoded in 2 bytes (should be 0x7F)
			value: 127,
		},
		{
			name:  "128 in 3 bytes",
			input: []byte{0x80, 0x81, 0x00}, // 128 encoded in 3 bytes (should be 0x80, 0x01)
			value: 128,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(tt.input)
			_, _, err := decodeVarint(buf)
			assert.ErrorIs(t, err, ErrVarintOverlong, "value %d should reject overlong encoding", tt.value)
		})
	}
}

// Benchmarks

func BenchmarkEncodeString(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		s := strings.Repeat("a", size)
		b.Run(strings.ReplaceAll(b.Name(), "/", "_")+"_"+string(rune('0'+size/100)), func(b *testing.B) {
			var buf bytes.Buffer
			buf.Grow(size + 2)

			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				buf.Reset()
				_, _ = encodeString(&buf, s)
			}
		})
	}
}

func BenchmarkDecodeString(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		var encoded bytes.Buffer
		_, _ = encodeString(&encoded, strings.Repeat("a", size))
		data := encoded.Bytes()

		b.Run(strings.ReplaceAll(b.Name(), "/", "_")+"_"+string(rune('0'+size/100)), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				r := bytes.NewReader(data)
				_, _, _ = decodeString(r)
			}
		})
	}
}

func BenchmarkEncodeBinary(b *testing.B) {
	data := bytes.Repeat([]byte{0xAB}, 100)
	var buf bytes.Buffer
	buf.Grow(len(data) + 2)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		buf.Reset()
		_, _ = encodeBinary(&buf, data)
	}
}

func BenchmarkDecodeBinary(b *testing.B) {
	var encoded bytes.Buffer
	_, _ = encodeBinary(&encoded, bytes.Repeat([]byte{0xAB}, 100))
	data := encoded.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		r := bytes.NewReader(data)
		_, _, _ = decodeBinary(r)
	}
}

func BenchmarkEncodeVarint(b *testing.B) {
	values := []uint32{0, 127, 16383, 2097151, 268435455}

	for _, v := range values {
		b.Run("", func(b *testing.B) {
			var buf bytes.Buffer
			buf.Grow(4)

			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				buf.Reset()
				_, _ = encodeVarint(&buf, v)
			}
		})
	}
}

func BenchmarkDecodeVarint(b *testing.B) {
	values := []uint32{0, 127, 16383, 2097151, 268435455}

	for _, v := range values {
		var encoded bytes.Buffer
		_, _ = encodeVarint(&encoded, v)
		data := encoded.Bytes()

		b.Run("", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				r := bytes.NewReader(data)
				_, _, _ = decodeVarint(r)
			}
		})
	}
}

// Fuzz tests

func FuzzDecodeString(f *testing.F) {
	// Add seed corpus - structured data
	f.Add([]byte{0x00, 0x00})                          // empty string
	f.Add([]byte{0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}) // "hello"
	f.Add([]byte{0x00, 0x03, 0xE4, 0xB8, 0x96})        // "世" (UTF-8)
	f.Add([]byte{0xFF, 0xFF})                          // max length prefix
	f.Add([]byte{0x00, 0x10, 0x00, 0x01, 0x02, 0x03})  // truncated

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
		_, _, _ = decodeString(r)
	})
}

func FuzzDecodeBinary(f *testing.F) {
	f.Add([]byte{0x00, 0x00})
	f.Add([]byte{0x00, 0x03, 0x01, 0x02, 0x03})
	f.Add([]byte{0xFF, 0xFF, 0x00})                   // large length, small data
	f.Add([]byte{0x00, 0x05, 0xDE, 0xAD, 0xBE, 0xEF}) // truncated

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
		_, _, _ = decodeBinary(r)
	})
}

func FuzzDecodeVarint(f *testing.F) {
	f.Add([]byte{0x00})
	f.Add([]byte{0x7F})
	f.Add([]byte{0x80, 0x01})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0x7F})
	f.Add([]byte{0x80, 0x80, 0x80, 0x80, 0x80}) // too many continuation bytes
	f.Add([]byte{0x80})                         // incomplete

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
		r := bytes.NewReader(data)
		_, _, _ = decodeVarint(r)
	})
}

func FuzzDecodeStringPair(f *testing.F) {
	f.Add([]byte{0x00, 0x00, 0x00, 0x00}) // empty key, empty value
	f.Add([]byte{0x00, 0x03, 'k', 'e', 'y', 0x00, 0x05, 'v', 'a', 'l', 'u', 'e'})
	f.Add([]byte{0xFF, 0xFF, 0x00, 0x00}) // large key length
	f.Add([]byte{0x00, 0x01, 'x'})        // missing value

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
		_, _, _ = decodeStringPair(r)
	})
}
