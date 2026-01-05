package mqttv5

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const msgStoreTestNS = "test"

func TestMemoryMessageStore(t *testing.T) {
	t.Run("store and get", func(t *testing.T) {
		store := NewMemoryMessageStore()

		msg := &Message{Topic: "test/topic", Payload: []byte("data")}
		err := store.Store(msgStoreTestNS, "msg-1", msg, 0)
		require.NoError(t, err)

		got, ok := store.Get(msgStoreTestNS, "msg-1")
		require.True(t, ok)
		assert.Equal(t, msg.Topic, got.Topic)
	})

	t.Run("get not found", func(t *testing.T) {
		store := NewMemoryMessageStore()

		ok := store.Exists(msgStoreTestNS, "nonexistent")
		assert.False(t, ok)
	})

	t.Run("delete", func(t *testing.T) {
		store := NewMemoryMessageStore()

		store.Store(msgStoreTestNS, "msg-1", &Message{}, 0)

		ok := store.Delete(msgStoreTestNS, "msg-1")
		assert.True(t, ok)

		ok = store.Delete(msgStoreTestNS, "msg-1")
		assert.False(t, ok)
	})

	t.Run("expiry", func(t *testing.T) {
		store := NewMemoryMessageStore()

		store.Store(msgStoreTestNS, "msg-1", &Message{}, 10*time.Millisecond)

		ok := store.Exists(msgStoreTestNS, "msg-1")
		assert.True(t, ok)

		time.Sleep(20 * time.Millisecond)

		ok = store.Exists(msgStoreTestNS, "msg-1")
		assert.False(t, ok)
	})

	t.Run("cleanup", func(t *testing.T) {
		store := NewMemoryMessageStore()

		store.Store(msgStoreTestNS, "msg-1", &Message{}, 10*time.Millisecond)
		store.Store(msgStoreTestNS, "msg-2", &Message{}, time.Hour)
		store.Store(msgStoreTestNS, "msg-3", &Message{}, 0)

		assert.Equal(t, 3, store.Count(msgStoreTestNS))

		time.Sleep(20 * time.Millisecond)

		count := store.Cleanup(msgStoreTestNS)
		assert.Equal(t, 1, count)
		assert.Equal(t, 2, store.Count(msgStoreTestNS))
	})

	t.Run("count", func(t *testing.T) {
		store := NewMemoryMessageStore()

		assert.Equal(t, 0, store.Count(msgStoreTestNS))

		store.Store(msgStoreTestNS, "msg-1", &Message{}, 0)
		store.Store(msgStoreTestNS, "msg-2", &Message{}, 0)

		assert.Equal(t, 2, store.Count(msgStoreTestNS))
	})

	t.Run("namespace isolation", func(t *testing.T) {
		store := NewMemoryMessageStore()

		// Store messages in different namespaces
		store.Store("tenant-a", "msg-1", &Message{Topic: "a"}, 0)
		store.Store("tenant-b", "msg-1", &Message{Topic: "b"}, 0)

		// Verify namespace isolation
		msgA, ok := store.Get("tenant-a", "msg-1")
		require.True(t, ok)
		assert.Equal(t, "a", msgA.Topic)

		msgB, ok := store.Get("tenant-b", "msg-1")
		require.True(t, ok)
		assert.Equal(t, "b", msgB.Topic)

		// Count per namespace
		assert.Equal(t, 1, store.Count("tenant-a"))
		assert.Equal(t, 1, store.Count("tenant-b"))
		assert.Equal(t, 2, store.Count("")) // Total count

		// Delete from one namespace doesn't affect other
		store.Delete("tenant-a", "msg-1")
		assert.Equal(t, 0, store.Count("tenant-a"))
		assert.Equal(t, 1, store.Count("tenant-b"))

		// Cross-namespace access should fail
		_, ok = store.Get("tenant-a", "msg-1")
		assert.False(t, ok)
		_, ok = store.Get("tenant-b", "msg-1")
		assert.True(t, ok)
	})

	t.Run("cleanup per namespace", func(t *testing.T) {
		store := NewMemoryMessageStore()

		store.Store("tenant-a", "msg-1", &Message{}, 10*time.Millisecond)
		store.Store("tenant-b", "msg-1", &Message{}, 10*time.Millisecond)

		time.Sleep(20 * time.Millisecond)

		// Cleanup only tenant-a
		count := store.Cleanup("tenant-a")
		assert.Equal(t, 1, count)

		// tenant-b should still have expired message (not cleaned up yet)
		assert.Equal(t, 0, store.Count("tenant-a"))
		// Note: Count() will see expired message but Get()/Exists() won't return it
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
			store.Store(msgStoreTestNS, id, &Message{}, 0)
			store.Get(msgStoreTestNS, id)
			store.Delete(msgStoreTestNS, id)
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
		store.Store(msgStoreTestNS, "msg-1", msg, 0)
	}
}

func BenchmarkMemoryMessageStoreGet(b *testing.B) {
	store := NewMemoryMessageStore()
	store.Store(msgStoreTestNS, "msg-1", &Message{}, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		store.Get(msgStoreTestNS, "msg-1")
	}
}
