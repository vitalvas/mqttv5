package mqttv5

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWillMessageFromConnect(t *testing.T) {
	t.Run("no will", func(t *testing.T) {
		pkt := &ConnectPacket{
			ClientID: "test",
			WillFlag: false,
		}

		will := WillMessageFromConnect(pkt)
		assert.Nil(t, will)
	})

	t.Run("basic will", func(t *testing.T) {
		pkt := &ConnectPacket{
			ClientID:    "test",
			WillFlag:    true,
			WillTopic:   "last/will",
			WillPayload: []byte("goodbye"),
			WillQoS:     1,
			WillRetain:  true,
		}

		will := WillMessageFromConnect(pkt)
		require.NotNil(t, will)
		assert.Equal(t, "last/will", will.Topic)
		assert.Equal(t, []byte("goodbye"), will.Payload)
		assert.Equal(t, byte(1), will.QoS)
		assert.True(t, will.Retain)
	})

	t.Run("with properties", func(t *testing.T) {
		willProps := Properties{}
		willProps.Set(PropWillDelayInterval, uint32(60))
		willProps.Set(PropPayloadFormatIndicator, byte(1))
		willProps.Set(PropMessageExpiryInterval, uint32(3600))
		willProps.Set(PropContentType, "application/json")
		willProps.Set(PropResponseTopic, "response/topic")
		willProps.Set(PropCorrelationData, []byte("corr-id"))
		willProps.Add(PropUserProperty, StringPair{Key: "key1", Value: "value1"})

		pkt := &ConnectPacket{
			ClientID:    "test",
			WillFlag:    true,
			WillTopic:   "last/will",
			WillPayload: []byte("data"),
			WillProps:   willProps,
		}

		will := WillMessageFromConnect(pkt)
		require.NotNil(t, will)
		assert.Equal(t, uint32(60), will.DelayInterval)
		assert.Equal(t, byte(1), will.PayloadFormat)
		assert.Equal(t, uint32(3600), will.MessageExpiry)
		assert.Equal(t, "application/json", will.ContentType)
		assert.Equal(t, "response/topic", will.ResponseTopic)
		assert.Equal(t, []byte("corr-id"), will.CorrelationData)
		require.Len(t, will.UserProperties, 1)
		assert.Equal(t, "key1", will.UserProperties[0].Key)
		assert.Equal(t, "value1", will.UserProperties[0].Value)
	})
}

func TestWillMessageToMessage(t *testing.T) {
	will := &WillMessage{
		Topic:           "test/topic",
		Payload:         []byte("data"),
		QoS:             2,
		Retain:          true,
		PayloadFormat:   1,
		MessageExpiry:   3600,
		ContentType:     "text/plain",
		ResponseTopic:   "response",
		CorrelationData: []byte("corr"),
		UserProperties:  []StringPair{{Key: "k", Value: "v"}},
	}

	msg := will.ToMessage()

	assert.Equal(t, will.Topic, msg.Topic)
	assert.Equal(t, will.Payload, msg.Payload)
	assert.Equal(t, will.QoS, msg.QoS)
	assert.Equal(t, will.Retain, msg.Retain)
	assert.Equal(t, will.PayloadFormat, msg.PayloadFormat)
	assert.Equal(t, will.MessageExpiry, msg.MessageExpiry)
	assert.Equal(t, will.ContentType, msg.ContentType)
	assert.Equal(t, will.ResponseTopic, msg.ResponseTopic)
	assert.Equal(t, will.CorrelationData, msg.CorrelationData)
	assert.Equal(t, will.UserProperties, msg.UserProperties)
}

func TestWillMessageToProperties(t *testing.T) {
	t.Run("all properties", func(t *testing.T) {
		will := &WillMessage{
			DelayInterval:   60,
			PayloadFormat:   1,
			MessageExpiry:   3600,
			ContentType:     "text/plain",
			ResponseTopic:   "response",
			CorrelationData: []byte("corr"),
			UserProperties:  []StringPair{{Key: "k", Value: "v"}},
		}

		props := will.ToProperties()

		assert.True(t, props.Has(PropWillDelayInterval))
		assert.Equal(t, uint32(60), props.GetUint32(PropWillDelayInterval))

		assert.True(t, props.Has(PropPayloadFormatIndicator))
		assert.Equal(t, byte(1), props.GetByte(PropPayloadFormatIndicator))

		assert.True(t, props.Has(PropMessageExpiryInterval))
		assert.Equal(t, uint32(3600), props.GetUint32(PropMessageExpiryInterval))

		assert.True(t, props.Has(PropContentType))
		assert.Equal(t, "text/plain", props.GetString(PropContentType))

		assert.True(t, props.Has(PropResponseTopic))
		assert.Equal(t, "response", props.GetString(PropResponseTopic))

		assert.True(t, props.Has(PropCorrelationData))
		assert.Equal(t, []byte("corr"), props.GetBinary(PropCorrelationData))

		ups := props.GetAllStringPairs(PropUserProperty)
		require.Len(t, ups, 1)
	})

	t.Run("empty properties", func(t *testing.T) {
		will := &WillMessage{}

		props := will.ToProperties()

		assert.False(t, props.Has(PropWillDelayInterval))
	})
}

func TestWillMessageValidate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		will := &WillMessage{
			Topic: "valid/topic",
			QoS:   1,
		}

		err := will.Validate()
		assert.NoError(t, err)
	})

	t.Run("empty topic", func(t *testing.T) {
		will := &WillMessage{
			Topic: "",
		}

		err := will.Validate()
		assert.ErrorIs(t, err, ErrEmptyTopic)
	})

	t.Run("invalid topic with wildcard", func(t *testing.T) {
		will := &WillMessage{
			Topic: "test/+/topic",
		}

		err := will.Validate()
		assert.ErrorIs(t, err, ErrInvalidTopicName)
	})

	t.Run("invalid QoS", func(t *testing.T) {
		will := &WillMessage{
			Topic: "valid/topic",
			QoS:   3,
		}

		err := will.Validate()
		assert.ErrorIs(t, err, ErrInvalidQoS)
	})
}

func TestPendingWill(t *testing.T) {
	t.Parallel()

	t.Run("no delay", func(t *testing.T) {
		t.Parallel()
		will := &WillMessage{
			Topic:         "test/topic",
			DelayInterval: 0,
		}

		pending := NewPendingWill("client-1", will)

		assert.True(t, pending.IsReady())
		assert.Equal(t, time.Duration(0), pending.TimeUntilPublish())
	})

	t.Run("with delay", func(t *testing.T) {
		t.Parallel()
		will := &WillMessage{
			Topic:         "test/topic",
			DelayInterval: 1,
		}

		pending := NewPendingWill("client-1", will)

		assert.False(t, pending.IsReady())
		assert.True(t, pending.TimeUntilPublish() > 0)
		assert.True(t, pending.TimeUntilPublish() <= time.Second)

		time.Sleep(1100 * time.Millisecond)
		assert.True(t, pending.IsReady())
	})

	t.Run("client ID", func(t *testing.T) {
		t.Parallel()
		will := &WillMessage{Topic: "test"}
		pending := NewPendingWill("my-client", will)

		assert.Equal(t, "my-client", pending.ClientID)
	})
}

func BenchmarkWillMessageToMessage(b *testing.B) {
	will := &WillMessage{
		Topic:   "test/topic",
		Payload: []byte("data"),
		QoS:     1,
		Retain:  true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = will.ToMessage()
	}
}

func BenchmarkWillMessageToProperties(b *testing.B) {
	will := &WillMessage{
		DelayInterval:   60,
		PayloadFormat:   1,
		MessageExpiry:   3600,
		ContentType:     "text/plain",
		ResponseTopic:   "response",
		CorrelationData: []byte("corr"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = will.ToProperties()
	}
}
