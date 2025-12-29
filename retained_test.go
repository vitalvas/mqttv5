package mqttv5

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryRetainedStore(t *testing.T) {
	t.Run("set and get", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		msg := &RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("data"),
			QoS:     1,
		}

		err := store.Set(msg)
		require.NoError(t, err)

		got, ok := store.Get("test/topic")
		require.True(t, ok)
		assert.Equal(t, msg.Topic, got.Topic)
		assert.Equal(t, msg.Payload, got.Payload)
		assert.Equal(t, msg.QoS, got.QoS)
	})

	t.Run("get not found", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		_, ok := store.Get("nonexistent")
		assert.False(t, ok)
	})

	t.Run("set with empty payload deletes", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(&RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("data"),
		})

		store.Set(&RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte{},
		})

		_, ok := store.Get("test/topic")
		assert.False(t, ok)
	})

	t.Run("set with nil payload deletes", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(&RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("data"),
		})

		store.Set(&RetainedMessage{
			Topic:   "test/topic",
			Payload: nil,
		})

		_, ok := store.Get("test/topic")
		assert.False(t, ok)
	})

	t.Run("set invalid topic", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		err := store.Set(&RetainedMessage{
			Topic:   "",
			Payload: []byte("data"),
		})

		assert.ErrorIs(t, err, ErrEmptyTopic)
	})

	t.Run("set updates existing", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(&RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("old"),
		})

		store.Set(&RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("new"),
		})

		got, ok := store.Get("test/topic")
		require.True(t, ok)
		assert.Equal(t, []byte("new"), got.Payload)
	})

	t.Run("delete", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(&RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("data"),
		})

		ok := store.Delete("test/topic")
		assert.True(t, ok)

		ok = store.Delete("test/topic")
		assert.False(t, ok)

		_, found := store.Get("test/topic")
		assert.False(t, found)
	})

	t.Run("match exact", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(&RetainedMessage{Topic: "a/b/c", Payload: []byte("1")})
		store.Set(&RetainedMessage{Topic: "a/b/d", Payload: []byte("2")})

		matched := store.Match("a/b/c")
		assert.Len(t, matched, 1)
		assert.Equal(t, "a/b/c", matched[0].Topic)
	})

	t.Run("match single level wildcard", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(&RetainedMessage{Topic: "sensor/1/temp", Payload: []byte("1")})
		store.Set(&RetainedMessage{Topic: "sensor/2/temp", Payload: []byte("2")})
		store.Set(&RetainedMessage{Topic: "sensor/1/humidity", Payload: []byte("3")})

		matched := store.Match("sensor/+/temp")
		assert.Len(t, matched, 2)
	})

	t.Run("match multi level wildcard", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(&RetainedMessage{Topic: "a/b/c", Payload: []byte("1")})
		store.Set(&RetainedMessage{Topic: "a/b/d/e", Payload: []byte("2")})
		store.Set(&RetainedMessage{Topic: "a/x", Payload: []byte("3")})

		matched := store.Match("a/b/#")
		assert.Len(t, matched, 2)

		matched = store.Match("#")
		assert.Len(t, matched, 3)
	})

	t.Run("match system topics", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(&RetainedMessage{Topic: "$SYS/broker/uptime", Payload: []byte("1")})
		store.Set(&RetainedMessage{Topic: "normal/topic", Payload: []byte("2")})

		matched := store.Match("#")
		assert.Len(t, matched, 1)

		matched = store.Match("$SYS/#")
		assert.Len(t, matched, 1)
		assert.Equal(t, "$SYS/broker/uptime", matched[0].Topic)
	})

	t.Run("clear", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(&RetainedMessage{Topic: "a", Payload: []byte("1")})
		store.Set(&RetainedMessage{Topic: "b", Payload: []byte("2")})
		store.Set(&RetainedMessage{Topic: "c", Payload: []byte("3")})

		assert.Equal(t, 3, store.Count())

		store.Clear()

		assert.Equal(t, 0, store.Count())
	})

	t.Run("count", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		assert.Equal(t, 0, store.Count())

		store.Set(&RetainedMessage{Topic: "a", Payload: []byte("1")})
		assert.Equal(t, 1, store.Count())

		store.Set(&RetainedMessage{Topic: "b", Payload: []byte("2")})
		assert.Equal(t, 2, store.Count())

		store.Delete("a")
		assert.Equal(t, 1, store.Count())
	})

	t.Run("topics", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(&RetainedMessage{Topic: "a/b", Payload: []byte("1")})
		store.Set(&RetainedMessage{Topic: "c/d", Payload: []byte("2")})
		store.Set(&RetainedMessage{Topic: "e/f", Payload: []byte("3")})

		topics := store.Topics()
		assert.Len(t, topics, 3)
		assert.Contains(t, topics, "a/b")
		assert.Contains(t, topics, "c/d")
		assert.Contains(t, topics, "e/f")
	})
}

func TestMemoryRetainedStoreConcurrency(_ *testing.T) {
	store := NewMemoryRetainedStore()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			topic := "test/" + string(rune(n))
			store.Set(&RetainedMessage{Topic: topic, Payload: []byte("data")})
			store.Get(topic)
			store.Match("test/#")
			store.Delete(topic)
		}(i)
	}

	wg.Wait()
}

func BenchmarkMemoryRetainedStoreSet(b *testing.B) {
	store := NewMemoryRetainedStore()
	msg := &RetainedMessage{
		Topic:   "test/topic",
		Payload: []byte("data"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		store.Set(msg)
	}
}

func BenchmarkMemoryRetainedStoreGet(b *testing.B) {
	store := NewMemoryRetainedStore()
	store.Set(&RetainedMessage{Topic: "test/topic", Payload: []byte("data")})

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		store.Get("test/topic")
	}
}

func BenchmarkMemoryRetainedStoreMatch(b *testing.B) {
	store := NewMemoryRetainedStore()
	for i := 0; i < 100; i++ {
		topic := "sensor/" + string(rune(i)) + "/temperature"
		store.Set(&RetainedMessage{Topic: topic, Payload: []byte("data")})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		store.Match("sensor/+/temperature")
	}
}
