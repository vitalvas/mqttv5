package mqttv5

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPropertyType(t *testing.T) {
	tests := []struct {
		id       PropertyID
		expected PropertyType
	}{
		{PropPayloadFormatIndicator, PropTypeByte},
		{PropMessageExpiryInterval, PropTypeFourByteInt},
		{PropContentType, PropTypeString},
		{PropCorrelationData, PropTypeBinary},
		{PropSubscriptionIdentifier, PropTypeVarInt},
		{PropServerKeepAlive, PropTypeTwoByteInt},
		{PropUserProperty, PropTypeStringPair},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.id.PropertyType())
	}
}

func TestPropertiesBasicOperations(t *testing.T) {
	var p Properties

	// Initially empty
	assert.Equal(t, 0, p.Len())
	assert.False(t, p.Has(PropContentType))
	assert.Nil(t, p.Get(PropContentType))

	// Set a property
	p.Set(PropContentType, "application/json")
	assert.Equal(t, 1, p.Len())
	assert.True(t, p.Has(PropContentType))
	assert.Equal(t, "application/json", p.Get(PropContentType))

	// Replace the property
	p.Set(PropContentType, "text/plain")
	assert.Equal(t, 1, p.Len())
	assert.Equal(t, "text/plain", p.Get(PropContentType))

	// Add another property
	p.Set(PropMessageExpiryInterval, uint32(3600))
	assert.Equal(t, 2, p.Len())
	assert.Equal(t, uint32(3600), p.Get(PropMessageExpiryInterval))

	// Delete a property
	p.Delete(PropContentType)
	assert.Equal(t, 1, p.Len())
	assert.False(t, p.Has(PropContentType))
	assert.True(t, p.Has(PropMessageExpiryInterval))
}

func TestPropertiesAddMultiple(t *testing.T) {
	var p Properties

	// Add multiple user properties
	p.Add(PropUserProperty, StringPair{Key: "key1", Value: "value1"})
	p.Add(PropUserProperty, StringPair{Key: "key2", Value: "value2"})
	p.Add(PropUserProperty, StringPair{Key: "key3", Value: "value3"})

	assert.Equal(t, 3, p.Len())

	// Get returns first one
	first := p.Get(PropUserProperty)
	assert.Equal(t, StringPair{Key: "key1", Value: "value1"}, first)

	// GetAll returns all
	all := p.GetAll(PropUserProperty)
	assert.Len(t, all, 3)

	// GetAllStringPairs returns typed slice
	pairs := p.GetAllStringPairs(PropUserProperty)
	assert.Len(t, pairs, 3)
	assert.Equal(t, "key1", pairs[0].Key)
	assert.Equal(t, "key2", pairs[1].Key)
	assert.Equal(t, "key3", pairs[2].Key)
}

func TestPropertiesTypedGetters(t *testing.T) {
	var p Properties

	// Byte
	p.Set(PropPayloadFormatIndicator, byte(1))
	assert.Equal(t, byte(1), p.GetByte(PropPayloadFormatIndicator))
	assert.Equal(t, byte(0), p.GetByte(PropMaximumQoS)) // not set

	// Uint16
	p.Set(PropReceiveMaximum, uint16(100))
	assert.Equal(t, uint16(100), p.GetUint16(PropReceiveMaximum))
	assert.Equal(t, uint16(0), p.GetUint16(PropServerKeepAlive)) // not set

	// Uint32
	p.Set(PropSessionExpiryInterval, uint32(3600))
	assert.Equal(t, uint32(3600), p.GetUint32(PropSessionExpiryInterval))
	assert.Equal(t, uint32(0), p.GetUint32(PropMessageExpiryInterval)) // not set

	// String
	p.Set(PropContentType, "application/json")
	assert.Equal(t, "application/json", p.GetString(PropContentType))
	assert.Equal(t, "", p.GetString(PropResponseTopic)) // not set

	// Binary
	p.Set(PropCorrelationData, []byte{1, 2, 3})
	assert.Equal(t, []byte{1, 2, 3}, p.GetBinary(PropCorrelationData))
	assert.Nil(t, p.GetBinary(PropAuthenticationData)) // not set

	// StringPair
	p.Set(PropUserProperty, StringPair{Key: "k", Value: "v"})
	assert.Equal(t, StringPair{Key: "k", Value: "v"}, p.GetStringPair(PropUserProperty))
	assert.Equal(t, StringPair{}, p.GetStringPair(PropContentType)) // wrong type
}

func TestPropertiesEncodeDecodeEmpty(t *testing.T) {
	var p Properties
	var buf bytes.Buffer

	n, err := p.Encode(&buf)
	require.NoError(t, err)
	assert.Equal(t, 1, n) // just the length (0)
	assert.Equal(t, []byte{0x00}, buf.Bytes())

	var decoded Properties
	n2, err := decoded.Decode(&buf)
	require.NoError(t, err)
	assert.Equal(t, 1, n2)
	assert.Equal(t, 0, decoded.Len())
}

func TestPropertiesEncodeDecodeByte(t *testing.T) {
	var p Properties
	p.Set(PropPayloadFormatIndicator, byte(1))

	var buf bytes.Buffer
	n, err := p.Encode(&buf)
	require.NoError(t, err)
	assert.Equal(t, 3, n) // length(1) + id(1) + value(1)

	var decoded Properties
	n2, err := decoded.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, n, n2)
	assert.Equal(t, byte(1), decoded.GetByte(PropPayloadFormatIndicator))
}

func TestPropertiesEncodeDecodeTwoByteInt(t *testing.T) {
	var p Properties
	p.Set(PropReceiveMaximum, uint16(1000))

	var buf bytes.Buffer
	n, err := p.Encode(&buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n) // length(1) + id(1) + value(2)

	var decoded Properties
	n2, err := decoded.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, n, n2)
	assert.Equal(t, uint16(1000), decoded.GetUint16(PropReceiveMaximum))
}

func TestPropertiesEncodeDecodeFourByteInt(t *testing.T) {
	var p Properties
	p.Set(PropSessionExpiryInterval, uint32(86400))

	var buf bytes.Buffer
	n, err := p.Encode(&buf)
	require.NoError(t, err)
	assert.Equal(t, 6, n) // length(1) + id(1) + value(4)

	var decoded Properties
	n2, err := decoded.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, n, n2)
	assert.Equal(t, uint32(86400), decoded.GetUint32(PropSessionExpiryInterval))
}

func TestPropertiesEncodeDecodeVarInt(t *testing.T) {
	var p Properties
	p.Set(PropSubscriptionIdentifier, uint32(268435455)) // max value

	var buf bytes.Buffer
	n, err := p.Encode(&buf)
	require.NoError(t, err)

	var decoded Properties
	n2, err := decoded.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, n, n2)
	assert.Equal(t, uint32(268435455), decoded.GetUint32(PropSubscriptionIdentifier))
}

func TestPropertiesEncodeDecodeString(t *testing.T) {
	var p Properties
	p.Set(PropContentType, "application/json")

	var buf bytes.Buffer
	n, err := p.Encode(&buf)
	require.NoError(t, err)

	var decoded Properties
	n2, err := decoded.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, n, n2)
	assert.Equal(t, "application/json", decoded.GetString(PropContentType))
}

func TestPropertiesEncodeDecodeBinary(t *testing.T) {
	var p Properties
	p.Set(PropCorrelationData, []byte{0x01, 0x02, 0x03, 0x04})

	var buf bytes.Buffer
	n, err := p.Encode(&buf)
	require.NoError(t, err)

	var decoded Properties
	n2, err := decoded.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, n, n2)
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, decoded.GetBinary(PropCorrelationData))
}

func TestPropertiesEncodeDecodeStringPair(t *testing.T) {
	var p Properties
	p.Add(PropUserProperty, StringPair{Key: "key1", Value: "value1"})
	p.Add(PropUserProperty, StringPair{Key: "key2", Value: "value2"})

	var buf bytes.Buffer
	n, err := p.Encode(&buf)
	require.NoError(t, err)

	var decoded Properties
	n2, err := decoded.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, n, n2)

	pairs := decoded.GetAllStringPairs(PropUserProperty)
	assert.Len(t, pairs, 2)
	assert.Equal(t, StringPair{Key: "key1", Value: "value1"}, pairs[0])
	assert.Equal(t, StringPair{Key: "key2", Value: "value2"}, pairs[1])
}

func TestPropertiesEncodeDecodeMultiple(t *testing.T) {
	var p Properties
	p.Set(PropPayloadFormatIndicator, byte(1))
	p.Set(PropMessageExpiryInterval, uint32(3600))
	p.Set(PropContentType, "text/plain")
	p.Set(PropCorrelationData, []byte{0xAB, 0xCD})
	p.Add(PropUserProperty, StringPair{Key: "foo", Value: "bar"})

	var buf bytes.Buffer
	n, err := p.Encode(&buf)
	require.NoError(t, err)

	var decoded Properties
	n2, err := decoded.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	assert.Equal(t, n, n2)
	assert.Equal(t, 5, decoded.Len())

	assert.Equal(t, byte(1), decoded.GetByte(PropPayloadFormatIndicator))
	assert.Equal(t, uint32(3600), decoded.GetUint32(PropMessageExpiryInterval))
	assert.Equal(t, "text/plain", decoded.GetString(PropContentType))
	assert.Equal(t, []byte{0xAB, 0xCD}, decoded.GetBinary(PropCorrelationData))
	assert.Equal(t, StringPair{Key: "foo", Value: "bar"}, decoded.GetStringPair(PropUserProperty))
}

func TestPropertiesDecodeUnknownPropertyID(t *testing.T) {
	// Property with unknown ID 0xFF
	data := []byte{0x02, 0xFF, 0x00} // length=2, id=0xFF, value=0x00

	var p Properties
	_, err := p.Decode(bytes.NewReader(data))
	assert.ErrorIs(t, err, ErrUnknownPropertyID)
}

func TestPropertiesNilReceiver(t *testing.T) {
	var p *Properties

	assert.Equal(t, 0, p.Len())
	assert.False(t, p.Has(PropContentType))
	assert.Nil(t, p.Get(PropContentType))
	assert.Nil(t, p.GetAll(PropContentType))
	assert.Equal(t, byte(0), p.GetByte(PropPayloadFormatIndicator))
	assert.Equal(t, "", p.GetString(PropContentType))
}

// Benchmarks

func BenchmarkPropertiesEncode(b *testing.B) {
	var p Properties
	p.Set(PropPayloadFormatIndicator, byte(1))
	p.Set(PropMessageExpiryInterval, uint32(3600))
	p.Set(PropContentType, "application/json")
	p.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	var buf bytes.Buffer
	buf.Grow(100)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf.Reset()
		_, _ = p.Encode(&buf)
	}
}

func BenchmarkPropertiesDecode(b *testing.B) {
	var p Properties
	p.Set(PropPayloadFormatIndicator, byte(1))
	p.Set(PropMessageExpiryInterval, uint32(3600))
	p.Set(PropContentType, "application/json")
	p.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	var buf bytes.Buffer
	_, _ = p.Encode(&buf)
	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		var decoded Properties
		_, _ = decoded.Decode(bytes.NewReader(data))
	}
}

func BenchmarkPropertiesGet(b *testing.B) {
	var p Properties
	p.Set(PropPayloadFormatIndicator, byte(1))
	p.Set(PropMessageExpiryInterval, uint32(3600))
	p.Set(PropContentType, "application/json")
	p.Set(PropCorrelationData, []byte{1, 2, 3})
	p.Add(PropUserProperty, StringPair{Key: "key", Value: "value"})

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = p.GetByte(PropPayloadFormatIndicator)
		_ = p.GetUint32(PropMessageExpiryInterval)
		_ = p.GetString(PropContentType)
	}
}

// Fuzz tests

func FuzzPropertiesDecode(f *testing.F) {
	// Add seed corpus - structured data
	f.Add([]byte{0x00}) // empty properties

	// Properties with byte value
	f.Add([]byte{0x02, 0x01, 0x01}) // PayloadFormatIndicator = 1

	// Properties with string
	f.Add([]byte{0x08, 0x03, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}) // ContentType = "hello"

	// Random data seeds
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0x7F})             // max varint length
	f.Add([]byte{0x05, 0xFF, 0x00, 0x00, 0x00, 0x00}) // unknown property ID
	f.Add([]byte{0x10, 0x01, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x04, 't', 'e', 's', 't'})
	f.Add([]byte{0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x9A}) // random

	f.Fuzz(func(_ *testing.T, data []byte) {
		var p Properties
		_, _ = p.Decode(bytes.NewReader(data))
	})
}
