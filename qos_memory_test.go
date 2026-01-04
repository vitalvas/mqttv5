package mqttv5

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryMessageStore(t *testing.T) {
	t.Run("store and get", func(t *testing.T) {
		store := NewMemoryMessageStore()

		msg := &Message{Topic: "test/topic", Payload: []byte("data")}
		err := store.Store("msg-1", msg, 0)
		require.NoError(t, err)

		got, ok := store.Get("msg-1")
		require.True(t, ok)
		assert.Equal(t, msg.Topic, got.Topic)
	})

	t.Run("get not found", func(t *testing.T) {
		store := NewMemoryMessageStore()

		ok := store.Exists("nonexistent")
		assert.False(t, ok)
	})

	t.Run("delete", func(t *testing.T) {
		store := NewMemoryMessageStore()

		store.Store("msg-1", &Message{}, 0)

		ok := store.Delete("msg-1")
		assert.True(t, ok)

		ok = store.Delete("msg-1")
		assert.False(t, ok)
	})

	t.Run("expiry", func(t *testing.T) {
		store := NewMemoryMessageStore()

		store.Store("msg-1", &Message{}, 10*time.Millisecond)

		ok := store.Exists("msg-1")
		assert.True(t, ok)

		time.Sleep(20 * time.Millisecond)

		ok = store.Exists("msg-1")
		assert.False(t, ok)
	})

	t.Run("cleanup", func(t *testing.T) {
		store := NewMemoryMessageStore()

		store.Store("msg-1", &Message{}, 10*time.Millisecond)
		store.Store("msg-2", &Message{}, time.Hour)
		store.Store("msg-3", &Message{}, 0)

		assert.Equal(t, 3, store.Count())

		time.Sleep(20 * time.Millisecond)

		count := store.Cleanup()
		assert.Equal(t, 1, count)
		assert.Equal(t, 2, store.Count())
	})

	t.Run("count", func(t *testing.T) {
		store := NewMemoryMessageStore()

		assert.Equal(t, 0, store.Count())

		store.Store("msg-1", &Message{}, 0)
		store.Store("msg-2", &Message{}, 0)

		assert.Equal(t, 2, store.Count())
	})
}

func TestMemoryMessageStoreConcurrency(_ *testing.T) {
	store := NewMemoryMessageStore()
	var wg sync.WaitGroup

	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := "msg-" + string(rune(n))
			store.Store(id, &Message{}, 0)
			store.Get(id)
			store.Delete(id)
		}(i)
	}

	wg.Wait()
}

func BenchmarkMemoryMessageStoreStore(b *testing.B) {
	store := NewMemoryMessageStore()
	msg := &Message{Topic: "test/topic"}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		store.Store("msg-1", msg, 0)
	}
}

func BenchmarkMemoryMessageStoreGet(b *testing.B) {
	store := NewMemoryMessageStore()
	store.Store("msg-1", &Message{}, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		store.Get("msg-1")
	}
}
