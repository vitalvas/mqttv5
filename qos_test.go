package mqttv5

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacketIDManager(t *testing.T) {
	t.Run("allocate sequential", func(t *testing.T) {
		m := NewPacketIDManager()

		id1, err := m.Allocate()
		require.NoError(t, err)
		assert.Equal(t, uint16(1), id1)

		id2, err := m.Allocate()
		require.NoError(t, err)
		assert.Equal(t, uint16(2), id2)

		id3, err := m.Allocate()
		require.NoError(t, err)
		assert.Equal(t, uint16(3), id3)
	})

	t.Run("release and reuse", func(t *testing.T) {
		m := NewPacketIDManager()

		id1, _ := m.Allocate()
		id2, _ := m.Allocate()

		err := m.Release(id1)
		require.NoError(t, err)

		assert.False(t, m.IsUsed(id1))
		assert.True(t, m.IsUsed(id2))

		id3, _ := m.Allocate()
		assert.Equal(t, uint16(3), id3)
	})

	t.Run("release not found", func(t *testing.T) {
		m := NewPacketIDManager()

		err := m.Release(999)
		assert.ErrorIs(t, err, ErrPacketIDNotFound)
	})

	t.Run("wraparound", func(t *testing.T) {
		m := NewPacketIDManager()
		m.next = 65534

		id1, _ := m.Allocate()
		id2, _ := m.Allocate()
		id3, _ := m.Allocate()

		assert.Equal(t, uint16(65534), id1)
		assert.Equal(t, uint16(65535), id2)
		assert.Equal(t, uint16(1), id3)
	})

	t.Run("in use count", func(t *testing.T) {
		m := NewPacketIDManager()

		assert.Equal(t, 0, m.InUse())

		m.Allocate()
		m.Allocate()
		assert.Equal(t, 2, m.InUse())

		m.Release(1)
		assert.Equal(t, 1, m.InUse())
	})
}

func TestPacketIDManagerConcurrency(t *testing.T) {
	m := NewPacketIDManager()
	var wg sync.WaitGroup

	allocated := make(chan uint16, 1000)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				id, err := m.Allocate()
				if err == nil {
					allocated <- id
				}
			}
		}()
	}

	wg.Wait()
	close(allocated)

	ids := make(map[uint16]bool)
	for id := range allocated {
		assert.False(t, ids[id], "duplicate ID allocated: %d", id)
		ids[id] = true
	}
}

func TestQoS1Tracker(t *testing.T) {
	t.Run("track and acknowledge", func(t *testing.T) {
		tracker := NewQoS1Tracker(time.Second, 3)

		msg := &Message{Topic: "test/topic", Payload: []byte("data")}
		tracker.Track(1, msg)

		assert.Equal(t, 1, tracker.Count())

		tracked, ok := tracker.Get(1)
		require.True(t, ok)
		assert.Equal(t, QoS1AwaitingPuback, tracked.State)
		assert.Equal(t, msg.Topic, tracked.Message.Topic)

		acked, ok := tracker.Acknowledge(1)
		require.True(t, ok)
		assert.Equal(t, QoS1Complete, acked.State)
		assert.Equal(t, 0, tracker.Count())
	})

	t.Run("acknowledge not found", func(t *testing.T) {
		tracker := NewQoS1Tracker(time.Second, 3)

		_, ok := tracker.Acknowledge(999)
		assert.False(t, ok)
	})

	t.Run("retry logic", func(t *testing.T) {
		tracker := NewQoS1Tracker(10*time.Millisecond, 3)

		msg := &Message{Topic: "test/topic"}
		tracker.Track(1, msg)

		pending := tracker.GetPendingRetries()
		assert.Len(t, pending, 0)

		time.Sleep(20 * time.Millisecond)

		pending = tracker.GetPendingRetries()
		assert.Len(t, pending, 1)
		assert.Equal(t, 1, pending[0].RetryCount)
	})

	t.Run("max retries", func(t *testing.T) {
		tracker := NewQoS1Tracker(10*time.Millisecond, 2)

		msg := &Message{Topic: "test/topic"}
		tracker.Track(1, msg)

		for i := 0; i < 3; i++ {
			time.Sleep(15 * time.Millisecond)
			pending := tracker.GetPendingRetries()
			if i < 2 {
				assert.Len(t, pending, 1)
			} else {
				assert.Len(t, pending, 0)
			}
		}
	})

	t.Run("remove", func(t *testing.T) {
		tracker := NewQoS1Tracker(time.Second, 3)

		tracker.Track(1, &Message{Topic: "test"})
		assert.True(t, tracker.Remove(1))
		assert.False(t, tracker.Remove(1))
		assert.Equal(t, 0, tracker.Count())
	})
}

func TestQoS2Tracker(t *testing.T) {
	t.Run("sender flow", func(t *testing.T) {
		tracker := NewQoS2Tracker(time.Second, 3)

		msg := &Message{Topic: "test/topic"}
		tracker.TrackSend(1, msg)

		assert.Equal(t, 1, tracker.Count())

		tracked, ok := tracker.Get(1)
		require.True(t, ok)
		assert.Equal(t, QoS2AwaitingPubrec, tracked.State)
		assert.True(t, tracked.IsSender)

		pubrec, ok := tracker.HandlePubrec(1)
		require.True(t, ok)
		assert.Equal(t, QoS2AwaitingPubcomp, pubrec.State)

		pubcomp, ok := tracker.HandlePubcomp(1)
		require.True(t, ok)
		assert.Equal(t, QoS2Complete, pubcomp.State)
		assert.Equal(t, 0, tracker.Count())
	})

	t.Run("receiver flow", func(t *testing.T) {
		tracker := NewQoS2Tracker(time.Second, 3)

		msg := &Message{Topic: "test/topic"}
		tracker.TrackReceive(1, msg)

		tracked, ok := tracker.Get(1)
		require.True(t, ok)
		assert.Equal(t, QoS2ReceivedPublish, tracked.State)
		assert.False(t, tracked.IsSender)

		ok = tracker.SendPubrec(1)
		require.True(t, ok)

		tracked, ok = tracker.Get(1)
		require.True(t, ok)
		assert.Equal(t, QoS2AwaitingPubrel, tracked.State)

		pubrel, ok := tracker.HandlePubrel(1)
		require.True(t, ok)
		assert.Equal(t, QoS2Complete, pubrel.State)
		assert.Equal(t, 0, tracker.Count())
	})

	t.Run("handle pubrec invalid state", func(t *testing.T) {
		tracker := NewQoS2Tracker(time.Second, 3)

		_, ok := tracker.HandlePubrec(999)
		assert.False(t, ok)
	})

	t.Run("handle pubcomp invalid state", func(t *testing.T) {
		tracker := NewQoS2Tracker(time.Second, 3)

		tracker.TrackSend(1, &Message{})

		_, ok := tracker.HandlePubcomp(1)
		assert.False(t, ok)
	})

	t.Run("retry logic", func(t *testing.T) {
		tracker := NewQoS2Tracker(10*time.Millisecond, 3)

		tracker.TrackSend(1, &Message{})

		pending := tracker.GetPendingRetries()
		assert.Len(t, pending, 0)

		time.Sleep(20 * time.Millisecond)

		pending = tracker.GetPendingRetries()
		assert.Len(t, pending, 1)
	})

	t.Run("remove", func(t *testing.T) {
		tracker := NewQoS2Tracker(time.Second, 3)

		tracker.TrackSend(1, &Message{})
		assert.True(t, tracker.Remove(1))
		assert.False(t, tracker.Remove(1))
	})
}

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

		_, ok := store.Get("nonexistent")
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

		_, ok := store.Get("msg-1")
		assert.True(t, ok)

		time.Sleep(20 * time.Millisecond)

		_, ok = store.Get("msg-1")
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

func TestMessageStoreConcurrency(_ *testing.T) {
	store := NewMemoryMessageStore()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
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

func BenchmarkPacketIDManagerAllocate(b *testing.B) {
	m := NewPacketIDManager()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		id, _ := m.Allocate()
		m.Release(id)
	}
}

func BenchmarkQoS1TrackerTrack(b *testing.B) {
	tracker := NewQoS1Tracker(time.Second, 3)
	msg := &Message{Topic: "test/topic"}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		tracker.Track(1, msg)
		tracker.Acknowledge(1)
	}
}

func BenchmarkQoS2TrackerSenderFlow(b *testing.B) {
	tracker := NewQoS2Tracker(time.Second, 3)
	msg := &Message{Topic: "test/topic"}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		tracker.TrackSend(1, msg)
		tracker.HandlePubrec(1)
		tracker.HandlePubcomp(1)
	}
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
