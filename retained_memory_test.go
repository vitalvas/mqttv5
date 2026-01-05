package mqttv5

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testNS is the default namespace used for tests
const testNS = "test"

func TestMemoryRetainedStore(t *testing.T) {
	t.Run("set and get", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		msg := &RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("data"),
			QoS:     1,
		}

		err := store.Set(testNS, msg)
		require.NoError(t, err)

		got, ok := store.Get(testNS, "test/topic")
		require.True(t, ok)
		assert.Equal(t, msg.Topic, got.Topic)
		assert.Equal(t, msg.Payload, got.Payload)
		assert.Equal(t, msg.QoS, got.QoS)
	})

	t.Run("get not found", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		ok := store.Exists(testNS, "nonexistent")
		assert.False(t, ok)
	})

	t.Run("set with empty payload deletes", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(testNS, &RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("data"),
		})

		store.Set(testNS, &RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte{},
		})

		ok := store.Exists(testNS, "test/topic")
		assert.False(t, ok)
	})

	t.Run("set with nil payload deletes", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(testNS, &RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("data"),
		})

		store.Set(testNS, &RetainedMessage{
			Topic:   "test/topic",
			Payload: nil,
		})

		ok := store.Exists(testNS, "test/topic")
		assert.False(t, ok)
	})

	t.Run("set invalid topic", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		err := store.Set(testNS, &RetainedMessage{
			Topic:   "",
			Payload: []byte("data"),
		})

		assert.ErrorIs(t, err, ErrEmptyTopic)
	})

	t.Run("set updates existing", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(testNS, &RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("old"),
		})

		store.Set(testNS, &RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("new"),
		})

		got, ok := store.Get(testNS, "test/topic")
		require.True(t, ok)
		assert.Equal(t, []byte("new"), got.Payload)
	})

	t.Run("delete", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(testNS, &RetainedMessage{
			Topic:   "test/topic",
			Payload: []byte("data"),
		})

		ok := store.Delete(testNS, "test/topic")
		assert.True(t, ok)

		ok = store.Delete(testNS, "test/topic")
		assert.False(t, ok)

		_, found := store.Get(testNS, "test/topic")
		assert.False(t, found)
	})

	t.Run("match exact", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(testNS, &RetainedMessage{Topic: "a/b/c", Payload: []byte("1")})
		store.Set(testNS, &RetainedMessage{Topic: "a/b/d", Payload: []byte("2")})

		matched := store.Match(testNS, "a/b/c")
		assert.Len(t, matched, 1)
		assert.Equal(t, "a/b/c", matched[0].Topic)
	})

	t.Run("match single level wildcard", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(testNS, &RetainedMessage{Topic: "sensor/1/temp", Payload: []byte("1")})
		store.Set(testNS, &RetainedMessage{Topic: "sensor/2/temp", Payload: []byte("2")})
		store.Set(testNS, &RetainedMessage{Topic: "sensor/1/humidity", Payload: []byte("3")})

		matched := store.Match(testNS, "sensor/+/temp")
		assert.Len(t, matched, 2)
	})

	t.Run("match multi level wildcard", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(testNS, &RetainedMessage{Topic: "a/b/c", Payload: []byte("1")})
		store.Set(testNS, &RetainedMessage{Topic: "a/b/d/e", Payload: []byte("2")})
		store.Set(testNS, &RetainedMessage{Topic: "a/x", Payload: []byte("3")})

		matched := store.Match(testNS, "a/b/#")
		assert.Len(t, matched, 2)

		matched = store.Match(testNS, "#")
		assert.Len(t, matched, 3)
	})

	t.Run("match system topics", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(testNS, &RetainedMessage{Topic: "$SYS/broker/uptime", Payload: []byte("1")})
		store.Set(testNS, &RetainedMessage{Topic: "normal/topic", Payload: []byte("2")})

		matched := store.Match(testNS, "#")
		assert.Len(t, matched, 1)

		matched = store.Match(testNS, "$SYS/#")
		assert.Len(t, matched, 1)
		assert.Equal(t, "$SYS/broker/uptime", matched[0].Topic)
	})

	t.Run("clear", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(testNS, &RetainedMessage{Topic: "a", Payload: []byte("1")})
		store.Set(testNS, &RetainedMessage{Topic: "b", Payload: []byte("2")})
		store.Set(testNS, &RetainedMessage{Topic: "c", Payload: []byte("3")})

		assert.Equal(t, 3, store.Count(testNS))

		store.Clear(testNS)

		assert.Equal(t, 0, store.Count(testNS))
	})

	t.Run("count", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		assert.Equal(t, 0, store.Count(testNS))

		store.Set(testNS, &RetainedMessage{Topic: "a", Payload: []byte("1")})
		assert.Equal(t, 1, store.Count(testNS))

		store.Set(testNS, &RetainedMessage{Topic: "b", Payload: []byte("2")})
		assert.Equal(t, 2, store.Count(testNS))

		store.Delete(testNS, "a")
		assert.Equal(t, 1, store.Count(testNS))
	})

	t.Run("topics returns namespace keys", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set(testNS, &RetainedMessage{Topic: "a/b", Payload: []byte("1")})
		store.Set(testNS, &RetainedMessage{Topic: "c/d", Payload: []byte("2")})
		store.Set("other", &RetainedMessage{Topic: "a/b", Payload: []byte("3")}) // Same topic, different namespace

		keys := store.Topics("")
		assert.Len(t, keys, 3)
		// Keys should be in namespace||topic format
		assert.Contains(t, keys, NamespaceKey(testNS, "a/b"))
		assert.Contains(t, keys, NamespaceKey(testNS, "c/d"))
		assert.Contains(t, keys, NamespaceKey("other", "a/b"))

		// Verify we can parse the keys back
		for _, key := range keys {
			ns, topic := ParseNamespaceKey(key)
			assert.NotEmpty(t, ns)
			assert.NotEmpty(t, topic)
		}
	})

	t.Run("set and get with metadata", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		msg := &RetainedMessage{
			Topic:           "test/topic",
			Payload:         []byte("data"),
			QoS:             1,
			PayloadFormat:   1,
			MessageExpiry:   3600,
			PublishedAt:     time.Now(),
			ContentType:     "application/json",
			ResponseTopic:   "response/topic",
			CorrelationData: []byte("correlation"),
			UserProperties:  []StringPair{{Key: "key1", Value: "value1"}},
		}

		err := store.Set(testNS, msg)
		require.NoError(t, err)

		got, ok := store.Get(testNS, "test/topic")
		require.True(t, ok)
		assert.Equal(t, msg.Topic, got.Topic)
		assert.Equal(t, msg.Payload, got.Payload)
		assert.Equal(t, msg.QoS, got.QoS)
		assert.Equal(t, msg.PayloadFormat, got.PayloadFormat)
		assert.Equal(t, msg.MessageExpiry, got.MessageExpiry)
		assert.Equal(t, msg.ContentType, got.ContentType)
		assert.Equal(t, msg.ResponseTopic, got.ResponseTopic)
		assert.Equal(t, msg.CorrelationData, got.CorrelationData)
		assert.Equal(t, msg.UserProperties, got.UserProperties)
	})

	t.Run("match filters out expired messages", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		// Active message (no expiry)
		store.Set(testNS, &RetainedMessage{
			Topic:   "active/topic",
			Payload: []byte("active"),
		})

		// Already expired message (published 2 seconds ago with 1 second expiry)
		store.Set(testNS, &RetainedMessage{
			Topic:         "expired/topic",
			Payload:       []byte("expired"),
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		})

		matched := store.Match(testNS, "#")
		assert.Len(t, matched, 1)
		assert.Equal(t, "active/topic", matched[0].Topic)
	})

	t.Run("match purges expired messages from storage", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		// Add a message that expires immediately
		msg := &RetainedMessage{
			Topic:         "test/expired",
			Payload:       []byte("data"),
			QoS:           1,
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		}
		store.Set(testNS, msg)

		// Initial count should be 1
		assert.Equal(t, 1, store.Count(testNS))

		// Match should exclude and purge expired messages
		matched := store.Match(testNS, "test/#")
		assert.Empty(t, matched, "expired messages should not be matched")
		assert.Equal(t, 0, store.Count(testNS), "expired messages should be purged")
	})

	t.Run("topics purges expired messages", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		// Add an expired message
		store.Set(testNS, &RetainedMessage{
			Topic:         "test/expired",
			Payload:       []byte("data"),
			QoS:           1,
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		})

		// Topics should exclude and purge expired messages
		topics := store.Topics(testNS)
		assert.Empty(t, topics, "expired messages should not be in topics")
		assert.Equal(t, 0, store.Count(testNS), "expired messages should be purged")
	})

	t.Run("cleanup removes expired messages", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		// Add expired and non-expired messages
		store.Set(testNS, &RetainedMessage{
			Topic:         "test/expired",
			Payload:       []byte("data"),
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		})
		store.Set(testNS, &RetainedMessage{
			Topic:   "test/active",
			Payload: []byte("data"),
		})

		removed := store.Cleanup(testNS)
		assert.Equal(t, 1, removed, "should remove 1 expired message")
		assert.Equal(t, 1, store.Count(testNS), "should have 1 active message")
	})

	t.Run("cleanup per namespace", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		// Add expired messages in different namespaces
		store.Set("tenant-a", &RetainedMessage{
			Topic:         "test/expired",
			Payload:       []byte("data"),
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		})
		store.Set("tenant-b", &RetainedMessage{
			Topic:         "test/expired",
			Payload:       []byte("data"),
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		})

		// Cleanup only tenant-a
		removed := store.Cleanup("tenant-a")
		assert.Equal(t, 1, removed)
		assert.Equal(t, 0, store.Count("tenant-a"))
		assert.Equal(t, 1, store.Count("tenant-b")) // tenant-b still has expired message

		// Cleanup all namespaces
		removed = store.Cleanup("")
		assert.Equal(t, 1, removed)
		assert.Equal(t, 0, store.Count("tenant-b"))
	})
}

func TestRetainedMessageExpiry(t *testing.T) {
	t.Run("IsExpired returns false when no expiry set", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:   "test",
			Payload: []byte("data"),
		}
		assert.False(t, msg.IsExpired())
	})

	t.Run("IsExpired returns false when PublishedAt is zero", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test",
			Payload:       []byte("data"),
			MessageExpiry: 60,
		}
		assert.False(t, msg.IsExpired())
	})

	t.Run("IsExpired returns false for non-expired message", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test",
			Payload:       []byte("data"),
			MessageExpiry: 60,
			PublishedAt:   time.Now(),
		}
		assert.False(t, msg.IsExpired())
	})

	t.Run("IsExpired returns true for expired message", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test",
			Payload:       []byte("data"),
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		}
		assert.True(t, msg.IsExpired())
	})

	t.Run("RemainingExpiry returns 0 when no expiry set", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:   "test",
			Payload: []byte("data"),
		}
		assert.Equal(t, uint32(0), msg.RemainingExpiry())
	})

	t.Run("RemainingExpiry returns remaining seconds", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test",
			Payload:       []byte("data"),
			MessageExpiry: 60,
			PublishedAt:   time.Now(),
		}
		remaining := msg.RemainingExpiry()
		assert.True(t, remaining >= 59 && remaining <= 60)
	})

	t.Run("RemainingExpiry returns 0 for expired message", func(t *testing.T) {
		msg := &RetainedMessage{
			Topic:         "test",
			Payload:       []byte("data"),
			MessageExpiry: 1,
			PublishedAt:   time.Now().Add(-2 * time.Second),
		}
		assert.Equal(t, uint32(0), msg.RemainingExpiry())
	})
}

func TestRetainedStoreNamespaceIsolation(t *testing.T) {
	t.Run("same topic different namespaces are separate messages", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		// Set same topic in different namespaces
		err := store.Set("tenant-a", &RetainedMessage{Topic: "sensor/temp", Payload: []byte("25C")})
		require.NoError(t, err)
		err = store.Set("tenant-b", &RetainedMessage{Topic: "sensor/temp", Payload: []byte("30C")})
		require.NoError(t, err)

		// Each namespace should get its own message
		msgA, ok := store.Get("tenant-a", "sensor/temp")
		require.True(t, ok)
		assert.Equal(t, []byte("25C"), msgA.Payload)

		msgB, ok := store.Get("tenant-b", "sensor/temp")
		require.True(t, ok)
		assert.Equal(t, []byte("30C"), msgB.Payload)
	})

	t.Run("match only returns messages from own namespace", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set("tenant-a", &RetainedMessage{Topic: "sensor/temp", Payload: []byte("a")})
		store.Set("tenant-a", &RetainedMessage{Topic: "sensor/humidity", Payload: []byte("a")})
		store.Set("tenant-b", &RetainedMessage{Topic: "sensor/temp", Payload: []byte("b")})
		store.Set("tenant-b", &RetainedMessage{Topic: "sensor/pressure", Payload: []byte("b")})

		// Match in tenant-a
		matchesA := store.Match("tenant-a", "sensor/#")
		assert.Len(t, matchesA, 2)
		for _, m := range matchesA {
			assert.Equal(t, []byte("a"), m.Payload)
		}

		// Match in tenant-b
		matchesB := store.Match("tenant-b", "sensor/#")
		assert.Len(t, matchesB, 2)
		for _, m := range matchesB {
			assert.Equal(t, []byte("b"), m.Payload)
		}
	})

	t.Run("delete only affects own namespace", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set("tenant-a", &RetainedMessage{Topic: "topic", Payload: []byte("a")})
		store.Set("tenant-b", &RetainedMessage{Topic: "topic", Payload: []byte("b")})

		// Delete from tenant-a
		deleted := store.Delete("tenant-a", "topic")
		assert.True(t, deleted)

		// tenant-a message should be gone
		ok := store.Exists("tenant-a", "topic")
		assert.False(t, ok)

		// tenant-b message should still exist
		msg, ok := store.Get("tenant-b", "topic")
		assert.True(t, ok)
		assert.Equal(t, []byte("b"), msg.Payload)
	})

	t.Run("update overwrites only in own namespace", func(t *testing.T) {
		store := NewMemoryRetainedStore()

		store.Set("tenant-a", &RetainedMessage{Topic: "topic", Payload: []byte("old-a")})
		store.Set("tenant-b", &RetainedMessage{Topic: "topic", Payload: []byte("old-b")})

		// Update tenant-a
		store.Set("tenant-a", &RetainedMessage{Topic: "topic", Payload: []byte("new-a")})

		// tenant-a should have updated value
		msgA, _ := store.Get("tenant-a", "topic")
		assert.Equal(t, []byte("new-a"), msgA.Payload)

		// tenant-b should still have old value
		msgB, _ := store.Get("tenant-b", "topic")
		assert.Equal(t, []byte("old-b"), msgB.Payload)
	})
}

func TestMemoryRetainedStoreConcurrency(_ *testing.T) {
	store := NewMemoryRetainedStore()
	var wg sync.WaitGroup

	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			topic := "test/" + string(rune(n))
			store.Set(testNS, &RetainedMessage{Topic: topic, Payload: []byte("data")})
			store.Get(testNS, topic)
			store.Match(testNS, "test/#")
			store.Delete(testNS, topic)
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
		store.Set(testNS, msg)
	}
}

func BenchmarkMemoryRetainedStoreGet(b *testing.B) {
	store := NewMemoryRetainedStore()
	store.Set(testNS, &RetainedMessage{Topic: "test/topic", Payload: []byte("data")})

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		store.Get(testNS, "test/topic")
	}
}

func BenchmarkMemoryRetainedStoreMatch(b *testing.B) {
	store := NewMemoryRetainedStore()
	for i := range 100 {
		topic := "sensor/" + string(rune(i)) + "/temperature"
		store.Set(testNS, &RetainedMessage{Topic: topic, Payload: []byte("data")})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		store.Match(testNS, "sensor/+/temperature")
	}
}
