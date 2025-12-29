package mqttv5

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemorySession(t *testing.T) {
	t.Run("create session", func(t *testing.T) {
		session := NewMemorySession("client-1")
		assert.Equal(t, "client-1", session.ClientID())
		assert.NotZero(t, session.CreatedAt())
		assert.NotZero(t, session.LastActivity())
	})

	t.Run("subscriptions", func(t *testing.T) {
		session := NewMemorySession("client-1")

		sub := Subscription{
			TopicFilter: "test/topic",
			QoS:         1,
		}
		session.AddSubscription(sub)

		assert.True(t, session.HasSubscription("test/topic"))
		assert.False(t, session.HasSubscription("other/topic"))

		got, ok := session.GetSubscription("test/topic")
		require.True(t, ok)
		assert.Equal(t, sub.TopicFilter, got.TopicFilter)
		assert.Equal(t, sub.QoS, got.QoS)

		subs := session.Subscriptions()
		assert.Len(t, subs, 1)

		removed := session.RemoveSubscription("test/topic")
		assert.True(t, removed)
		assert.False(t, session.HasSubscription("test/topic"))

		removed = session.RemoveSubscription("test/topic")
		assert.False(t, removed)
	})

	t.Run("update subscription", func(t *testing.T) {
		session := NewMemorySession("client-1")

		session.AddSubscription(Subscription{TopicFilter: "test/topic", QoS: 0})
		session.AddSubscription(Subscription{TopicFilter: "test/topic", QoS: 2})

		got, ok := session.GetSubscription("test/topic")
		require.True(t, ok)
		assert.Equal(t, byte(2), got.QoS)
	})

	t.Run("packet ID", func(t *testing.T) {
		session := NewMemorySession("client-1")

		id1 := session.NextPacketID()
		id2 := session.NextPacketID()
		id3 := session.NextPacketID()

		assert.Equal(t, uint16(1), id1)
		assert.Equal(t, uint16(2), id2)
		assert.Equal(t, uint16(3), id3)
	})

	t.Run("packet ID wraparound", func(t *testing.T) {
		session := NewMemorySession("client-1")
		session.packetIDCounter = 65534

		id1 := session.NextPacketID()
		id2 := session.NextPacketID()
		id3 := session.NextPacketID()

		assert.Equal(t, uint16(65535), id1)
		assert.Equal(t, uint16(1), id2) // Wraps around, skipping 0
		assert.Equal(t, uint16(2), id3)
	})

	t.Run("pending messages", func(t *testing.T) {
		session := NewMemorySession("client-1")

		msg := &Message{Topic: "test/topic", Payload: []byte("data")}
		session.AddPendingMessage(1, msg)

		got, ok := session.GetPendingMessage(1)
		require.True(t, ok)
		assert.Equal(t, msg.Topic, got.Topic)

		_, ok = session.GetPendingMessage(2)
		assert.False(t, ok)

		msgs := session.PendingMessages()
		assert.Len(t, msgs, 1)

		removed := session.RemovePendingMessage(1)
		assert.True(t, removed)

		removed = session.RemovePendingMessage(1)
		assert.False(t, removed)
	})

	t.Run("expiry", func(t *testing.T) {
		session := NewMemorySession("client-1")

		assert.False(t, session.IsExpired())
		assert.True(t, session.ExpiryTime().IsZero())

		session.SetExpiryTime(time.Now().Add(-time.Hour))
		assert.True(t, session.IsExpired())

		session.SetExpiryTime(time.Now().Add(time.Hour))
		assert.False(t, session.IsExpired())
	})

	t.Run("last activity", func(t *testing.T) {
		session := NewMemorySession("client-1")
		initial := session.LastActivity()

		time.Sleep(10 * time.Millisecond)
		session.UpdateLastActivity()

		assert.True(t, session.LastActivity().After(initial))
	})

	t.Run("match subscriptions", func(t *testing.T) {
		session := NewMemorySession("client-1")

		session.AddSubscription(Subscription{TopicFilter: "sensor/+/temperature", QoS: 1})
		session.AddSubscription(Subscription{TopicFilter: "sensor/#", QoS: 0})
		session.AddSubscription(Subscription{TopicFilter: "other/topic", QoS: 2})

		matched := session.MatchSubscriptions("sensor/living/temperature")
		assert.Len(t, matched, 2)

		matched = session.MatchSubscriptions("other/topic")
		assert.Len(t, matched, 1)

		matched = session.MatchSubscriptions("no/match")
		assert.Len(t, matched, 0)
	})
}

func TestMemorySessionStore(t *testing.T) {
	t.Run("create and get", func(t *testing.T) {
		store := NewMemorySessionStore()

		session := NewMemorySession("client-1")
		err := store.Create(session)
		require.NoError(t, err)

		got, err := store.Get("client-1")
		require.NoError(t, err)
		assert.Equal(t, "client-1", got.ClientID())
	})

	t.Run("create duplicate", func(t *testing.T) {
		store := NewMemorySessionStore()

		session := NewMemorySession("client-1")
		err := store.Create(session)
		require.NoError(t, err)

		err = store.Create(session)
		assert.ErrorIs(t, err, ErrSessionExists)
	})

	t.Run("get not found", func(t *testing.T) {
		store := NewMemorySessionStore()

		_, err := store.Get("nonexistent")
		assert.ErrorIs(t, err, ErrSessionNotFound)
	})

	t.Run("update", func(t *testing.T) {
		store := NewMemorySessionStore()

		session := NewMemorySession("client-1")
		err := store.Create(session)
		require.NoError(t, err)

		session.AddSubscription(Subscription{TopicFilter: "test/topic", QoS: 1})
		err = store.Update(session)
		require.NoError(t, err)

		got, err := store.Get("client-1")
		require.NoError(t, err)
		assert.True(t, got.HasSubscription("test/topic"))
	})

	t.Run("update not found", func(t *testing.T) {
		store := NewMemorySessionStore()

		session := NewMemorySession("nonexistent")
		err := store.Update(session)
		assert.ErrorIs(t, err, ErrSessionNotFound)
	})

	t.Run("delete", func(t *testing.T) {
		store := NewMemorySessionStore()

		session := NewMemorySession("client-1")
		err := store.Create(session)
		require.NoError(t, err)

		err = store.Delete("client-1")
		require.NoError(t, err)

		_, err = store.Get("client-1")
		assert.ErrorIs(t, err, ErrSessionNotFound)
	})

	t.Run("delete not found", func(t *testing.T) {
		store := NewMemorySessionStore()

		err := store.Delete("nonexistent")
		assert.ErrorIs(t, err, ErrSessionNotFound)
	})

	t.Run("list", func(t *testing.T) {
		store := NewMemorySessionStore()

		_ = store.Create(NewMemorySession("client-1"))
		_ = store.Create(NewMemorySession("client-2"))
		_ = store.Create(NewMemorySession("client-3"))

		sessions := store.List()
		assert.Len(t, sessions, 3)
	})

	t.Run("cleanup expired", func(t *testing.T) {
		store := NewMemorySessionStore()

		s1 := NewMemorySession("client-1")
		s1.SetExpiryTime(time.Now().Add(-time.Hour))

		s2 := NewMemorySession("client-2")
		s2.SetExpiryTime(time.Now().Add(time.Hour))

		s3 := NewMemorySession("client-3")

		_ = store.Create(s1)
		_ = store.Create(s2)
		_ = store.Create(s3)

		count := store.Cleanup()
		assert.Equal(t, 1, count)

		sessions := store.List()
		assert.Len(t, sessions, 2)

		_, err := store.Get("client-1")
		assert.ErrorIs(t, err, ErrSessionNotFound)
	})

	t.Run("expiry handler", func(t *testing.T) {
		store := NewMemorySessionStore()

		var expiredIDs []string
		store.SetExpiryHandler(func(session Session) {
			expiredIDs = append(expiredIDs, session.ClientID())
		})

		s1 := NewMemorySession("client-1")
		s1.SetExpiryTime(time.Now().Add(-time.Hour))

		_ = store.Create(s1)
		_ = store.Cleanup()

		assert.Contains(t, expiredIDs, "client-1")
	})
}

func TestMemorySessionConcurrency(_ *testing.T) {
	session := NewMemorySession("client-1")
	var wg sync.WaitGroup

	// Concurrent subscription operations
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sub := Subscription{TopicFilter: "test/topic", QoS: byte(n % 3)}
			session.AddSubscription(sub)
			session.HasSubscription("test/topic")
			session.GetSubscription("test/topic")
			_ = session.Subscriptions()
		}(i)
	}

	// Concurrent packet ID operations
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = session.NextPacketID()
		}()
	}

	// Concurrent pending message operations
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			msg := &Message{Topic: "test"}
			session.AddPendingMessage(uint16(n), msg)
			session.GetPendingMessage(uint16(n))
			_ = session.PendingMessages()
		}(i)
	}

	wg.Wait()
}

func TestMemorySessionStoreConcurrency(_ *testing.T) {
	store := NewMemorySessionStore()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			session := NewMemorySession("client-" + string(rune(n)))
			_ = store.Create(session)
			_, _ = store.Get(session.ClientID())
			_ = store.Update(session)
			_ = store.List()
			_ = store.Delete(session.ClientID())
		}(i)
	}

	wg.Wait()
}

func BenchmarkMemorySessionNextPacketID(b *testing.B) {
	session := NewMemorySession("client-1")

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = session.NextPacketID()
	}
}

func BenchmarkMemorySessionAddSubscription(b *testing.B) {
	session := NewMemorySession("client-1")
	sub := Subscription{TopicFilter: "test/topic", QoS: 1}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		session.AddSubscription(sub)
	}
}

func BenchmarkMemorySessionMatchSubscriptions(b *testing.B) {
	session := NewMemorySession("client-1")
	session.AddSubscription(Subscription{TopicFilter: "sensor/+/temperature", QoS: 1})
	session.AddSubscription(Subscription{TopicFilter: "sensor/#", QoS: 0})
	session.AddSubscription(Subscription{TopicFilter: "other/topic", QoS: 2})

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = session.MatchSubscriptions("sensor/living/temperature")
	}
}

func BenchmarkMemorySessionStoreCreate(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		store := NewMemorySessionStore()
		session := NewMemorySession("client-1")
		_ = store.Create(session)
	}
}

func BenchmarkMemorySessionStoreGet(b *testing.B) {
	store := NewMemorySessionStore()
	_ = store.Create(NewMemorySession("client-1"))

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = store.Get("client-1")
	}
}
