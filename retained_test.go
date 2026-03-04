package mqttv5

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetainedMessageIsExpired(t *testing.T) {
	t.Run("no expiry set", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:       "test/topic",
			Payload:     []byte("hello"),
			PublishedAt: time.Now().Add(-time.Hour),
		}
		assert.False(t, msg.IsExpired())
	})

	t.Run("zero published at", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test/topic",
			Payload:       []byte("hello"),
			MessageExpiry: 60,
		}
		assert.False(t, msg.IsExpired())
	})

	t.Run("not expired", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test/topic",
			Payload:       []byte("hello"),
			MessageExpiry: 3600,
			PublishedAt:   time.Now(),
		}
		assert.False(t, msg.IsExpired())
	})

	t.Run("expired", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test/topic",
			Payload:       []byte("hello"),
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		}
		assert.True(t, msg.IsExpired())
	})
}

func TestRetainedMessageRemainingExpiry(t *testing.T) {
	t.Run("no expiry set", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:       "test/topic",
			Payload:     []byte("hello"),
			PublishedAt: time.Now(),
		}
		assert.Equal(t, uint32(0), msg.RemainingExpiry())
	})

	t.Run("zero published at", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test/topic",
			Payload:       []byte("hello"),
			MessageExpiry: 60,
		}
		assert.Equal(t, uint32(0), msg.RemainingExpiry())
	})

	t.Run("has remaining time", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test/topic",
			Payload:       []byte("hello"),
			MessageExpiry: 3600,
			PublishedAt:   time.Now(),
		}
		remaining := msg.RemainingExpiry()
		assert.Greater(t, remaining, uint32(3590))
		assert.LessOrEqual(t, remaining, uint32(3600))
	})

	t.Run("expired returns zero", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test/topic",
			Payload:       []byte("hello"),
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		}
		assert.Equal(t, uint32(0), msg.RemainingExpiry())
	})
}
