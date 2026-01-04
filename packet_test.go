package mqttv5

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQoSConstants(t *testing.T) {
	t.Run("QoS values are correct per MQTT spec", func(t *testing.T) {
		assert.Equal(t, byte(0), QoS0, "QoS0 should be 0")
		assert.Equal(t, byte(1), QoS1, "QoS1 should be 1")
		assert.Equal(t, byte(2), QoS2, "QoS2 should be 2")
	})

	t.Run("QoS constants can be used in Message", func(t *testing.T) {
		m := &Message{
			Topic: "test",
			QoS:   QoS1,
		}
		assert.Equal(t, "test", m.Topic)
		assert.Equal(t, QoS1, m.QoS)
	})
}

func TestMessageClone(t *testing.T) {
	t.Run("nil message", func(t *testing.T) {
		var m *Message
		assert.Nil(t, m.Clone())
	})

	t.Run("empty message", func(t *testing.T) {
		m := &Message{}
		clone := m.Clone()
		assert.NotNil(t, clone)
		assert.Equal(t, m.Topic, clone.Topic)
		assert.Equal(t, m.QoS, clone.QoS)
	})

	t.Run("full message", func(t *testing.T) {
		m := &Message{
			Topic:           "test/topic",
			Payload:         []byte("hello world"),
			QoS:             1,
			Retain:          true,
			PayloadFormat:   1,
			MessageExpiry:   3600,
			ContentType:     "text/plain",
			ResponseTopic:   "response/topic",
			CorrelationData: []byte{0x01, 0x02, 0x03},
			UserProperties: []StringPair{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
			},
			SubscriptionIdentifiers: []uint32{1, 2, 3},
		}

		clone := m.Clone()
		assert.Equal(t, m.Topic, clone.Topic)
		assert.Equal(t, m.Payload, clone.Payload)
		assert.Equal(t, m.QoS, clone.QoS)
		assert.Equal(t, m.Retain, clone.Retain)
		assert.Equal(t, m.PayloadFormat, clone.PayloadFormat)
		assert.Equal(t, m.MessageExpiry, clone.MessageExpiry)
		assert.Equal(t, m.ContentType, clone.ContentType)
		assert.Equal(t, m.ResponseTopic, clone.ResponseTopic)
		assert.Equal(t, m.CorrelationData, clone.CorrelationData)
		assert.Equal(t, m.UserProperties, clone.UserProperties)
		assert.Equal(t, m.SubscriptionIdentifiers, clone.SubscriptionIdentifiers)

		// Verify deep copy - modifications don't affect original
		clone.Payload[0] = 0xFF
		assert.NotEqual(t, m.Payload[0], clone.Payload[0])

		clone.CorrelationData[0] = 0xFF
		assert.NotEqual(t, m.CorrelationData[0], clone.CorrelationData[0])

		clone.UserProperties[0].Key = "modified"
		assert.NotEqual(t, m.UserProperties[0].Key, clone.UserProperties[0].Key)

		clone.SubscriptionIdentifiers[0] = 999
		assert.NotEqual(t, m.SubscriptionIdentifiers[0], clone.SubscriptionIdentifiers[0])
	})
}

func TestMessageToProperties(t *testing.T) {
	t.Run("empty message", func(t *testing.T) {
		m := &Message{}
		p := m.ToProperties()
		assert.Equal(t, 0, p.Len())
	})

	t.Run("all properties", func(t *testing.T) {
		m := &Message{
			PayloadFormat:   1,
			MessageExpiry:   3600,
			ContentType:     "application/json",
			ResponseTopic:   "response/topic",
			CorrelationData: []byte{0x01, 0x02},
			UserProperties: []StringPair{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
			},
		}

		p := m.ToProperties()
		assert.Equal(t, 7, p.Len())
		assert.Equal(t, byte(1), p.GetByte(PropPayloadFormatIndicator))
		assert.Equal(t, uint32(3600), p.GetUint32(PropMessageExpiryInterval))
		assert.Equal(t, "application/json", p.GetString(PropContentType))
		assert.Equal(t, "response/topic", p.GetString(PropResponseTopic))
		assert.Equal(t, []byte{0x01, 0x02}, p.GetBinary(PropCorrelationData))

		ups := p.GetAllStringPairs(PropUserProperty)
		assert.Len(t, ups, 2)
		assert.Equal(t, "key1", ups[0].Key)
		assert.Equal(t, "key2", ups[1].Key)
	})

	t.Run("partial properties", func(t *testing.T) {
		m := &Message{
			ContentType: "text/plain",
		}

		p := m.ToProperties()
		assert.Equal(t, 1, p.Len())
		assert.Equal(t, "text/plain", p.GetString(PropContentType))
		assert.False(t, p.Has(PropPayloadFormatIndicator))
		assert.False(t, p.Has(PropMessageExpiryInterval))
	})
}

func TestMessageFromProperties(t *testing.T) {
	t.Run("nil properties", func(t *testing.T) {
		m := &Message{}
		m.FromProperties(nil)
		assert.Equal(t, byte(0), m.PayloadFormat)
		assert.Equal(t, uint32(0), m.MessageExpiry)
	})

	t.Run("empty properties", func(t *testing.T) {
		m := &Message{}
		var p Properties
		m.FromProperties(&p)
		assert.Equal(t, byte(0), m.PayloadFormat)
		assert.Equal(t, uint32(0), m.MessageExpiry)
	})

	t.Run("all properties", func(t *testing.T) {
		var p Properties
		p.Set(PropPayloadFormatIndicator, byte(1))
		p.Set(PropMessageExpiryInterval, uint32(7200))
		p.Set(PropContentType, "application/json")
		p.Set(PropResponseTopic, "response/topic")
		p.Set(PropCorrelationData, []byte{0xAB, 0xCD})
		p.Add(PropUserProperty, StringPair{Key: "k1", Value: "v1"})
		p.Add(PropUserProperty, StringPair{Key: "k2", Value: "v2"})
		p.Add(PropSubscriptionIdentifier, uint32(123))
		p.Add(PropSubscriptionIdentifier, uint32(456))

		m := &Message{}
		m.FromProperties(&p)

		assert.Equal(t, byte(1), m.PayloadFormat)
		assert.Equal(t, uint32(7200), m.MessageExpiry)
		assert.Equal(t, "application/json", m.ContentType)
		assert.Equal(t, "response/topic", m.ResponseTopic)
		assert.Equal(t, []byte{0xAB, 0xCD}, m.CorrelationData)
		assert.Len(t, m.UserProperties, 2)
		assert.Equal(t, "k1", m.UserProperties[0].Key)
		assert.Len(t, m.SubscriptionIdentifiers, 2)
		assert.Equal(t, uint32(123), m.SubscriptionIdentifiers[0])
		assert.Equal(t, uint32(456), m.SubscriptionIdentifiers[1])
	})
}

func TestMessageRoundTrip(t *testing.T) {
	original := &Message{
		PayloadFormat:   1,
		MessageExpiry:   1800,
		ContentType:     "text/plain",
		ResponseTopic:   "reply/to",
		CorrelationData: []byte{0x01, 0x02, 0x03},
		UserProperties: []StringPair{
			{Key: "trace-id", Value: "abc123"},
		},
	}

	// Convert to properties and back
	props := original.ToProperties()

	restored := &Message{}
	restored.FromProperties(&props)

	assert.Equal(t, original.PayloadFormat, restored.PayloadFormat)
	assert.Equal(t, original.MessageExpiry, restored.MessageExpiry)
	assert.Equal(t, original.ContentType, restored.ContentType)
	assert.Equal(t, original.ResponseTopic, restored.ResponseTopic)
	assert.Equal(t, original.CorrelationData, restored.CorrelationData)
	assert.Equal(t, original.UserProperties, restored.UserProperties)
}

// Benchmarks

func BenchmarkMessageClone(b *testing.B) {
	m := &Message{
		Topic:           "test/topic/path",
		Payload:         make([]byte, 100),
		QoS:             1,
		Retain:          true,
		PayloadFormat:   1,
		MessageExpiry:   3600,
		ContentType:     "application/json",
		ResponseTopic:   "response/topic",
		CorrelationData: []byte{0x01, 0x02, 0x03, 0x04},
		UserProperties: []StringPair{
			{Key: "key1", Value: "value1"},
			{Key: "key2", Value: "value2"},
		},
		SubscriptionIdentifiers: []uint32{1, 2, 3},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = m.Clone()
	}
}

func BenchmarkMessageToProperties(b *testing.B) {
	m := &Message{
		PayloadFormat:   1,
		MessageExpiry:   3600,
		ContentType:     "application/json",
		ResponseTopic:   "response/topic",
		CorrelationData: []byte{0x01, 0x02, 0x03},
		UserProperties: []StringPair{
			{Key: "key1", Value: "value1"},
			{Key: "key2", Value: "value2"},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = m.ToProperties()
	}
}

func BenchmarkMessageFromProperties(b *testing.B) {
	var p Properties
	p.Set(PropPayloadFormatIndicator, byte(1))
	p.Set(PropMessageExpiryInterval, uint32(3600))
	p.Set(PropContentType, "application/json")
	p.Set(PropResponseTopic, "response/topic")
	p.Set(PropCorrelationData, []byte{0x01, 0x02, 0x03})
	p.Add(PropUserProperty, StringPair{Key: "key1", Value: "value1"})
	p.Add(PropUserProperty, StringPair{Key: "key2", Value: "value2"})
	p.Add(PropSubscriptionIdentifier, uint32(123))

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		var m Message
		m.FromProperties(&p)
	}
}

func TestMessageExpiry(t *testing.T) {
	t.Run("IsExpired returns false when no expiry set", func(t *testing.T) {
		m := &Message{
			Topic:   "test",
			Payload: []byte("hello"),
		}
		assert.False(t, m.IsExpired())
	})

	t.Run("IsExpired returns false when no PublishedAt set", func(t *testing.T) {
		m := &Message{
			Topic:         "test",
			Payload:       []byte("hello"),
			MessageExpiry: 60,
		}
		assert.False(t, m.IsExpired())
	})

	t.Run("IsExpired returns false when not yet expired", func(t *testing.T) {
		m := &Message{
			Topic:         "test",
			Payload:       []byte("hello"),
			MessageExpiry: 60,
			PublishedAt:   time.Now(),
		}
		assert.False(t, m.IsExpired())
	})

	t.Run("IsExpired returns true when expired", func(t *testing.T) {
		m := &Message{
			Topic:         "test",
			Payload:       []byte("hello"),
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		}
		assert.True(t, m.IsExpired())
	})

	t.Run("RemainingExpiry returns 0 when no expiry set", func(t *testing.T) {
		m := &Message{
			Topic:   "test",
			Payload: []byte("hello"),
		}
		assert.Equal(t, uint32(0), m.RemainingExpiry())
	})

	t.Run("RemainingExpiry returns original when no PublishedAt set", func(t *testing.T) {
		m := &Message{
			Topic:         "test",
			Payload:       []byte("hello"),
			MessageExpiry: 60,
		}
		assert.Equal(t, uint32(60), m.RemainingExpiry())
	})

	t.Run("RemainingExpiry returns remaining time", func(t *testing.T) {
		m := &Message{
			Topic:         "test",
			Payload:       []byte("hello"),
			MessageExpiry: 60,
			PublishedAt:   time.Now().Add(-30 * time.Second),
		}
		remaining := m.RemainingExpiry()
		// Allow some tolerance for test execution time
		assert.True(t, remaining >= 28 && remaining <= 31, "expected ~30, got %d", remaining)
	})

	t.Run("RemainingExpiry returns 0 when expired", func(t *testing.T) {
		m := &Message{
			Topic:         "test",
			Payload:       []byte("hello"),
			MessageExpiry: 10,
			PublishedAt:   time.Now().Add(-20 * time.Second),
		}
		assert.Equal(t, uint32(0), m.RemainingExpiry())
	})

	t.Run("Clone preserves PublishedAt", func(t *testing.T) {
		publishedAt := time.Now().Add(-5 * time.Second)
		m := &Message{
			Topic:         "test",
			Payload:       []byte("hello"),
			MessageExpiry: 60,
			PublishedAt:   publishedAt,
		}
		clone := m.Clone()
		assert.Equal(t, publishedAt, clone.PublishedAt)
	})
}
