package mqttv5

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWillManager(t *testing.T) {
	t.Run("register will", func(t *testing.T) {
		m := NewWillManager()

		will := &WillMessage{
			Topic:   "client/status",
			Payload: []byte("offline"),
		}

		m.Register("client1", will)

		assert.True(t, m.HasWill("client1"))
		assert.Equal(t, 1, m.GetActiveCount())
	})

	t.Run("register nil will is no-op", func(t *testing.T) {
		m := NewWillManager()

		m.Register("client1", nil)

		assert.False(t, m.HasWill("client1"))
		assert.Equal(t, 0, m.GetActiveCount())
	})

	t.Run("unregister will", func(t *testing.T) {
		m := NewWillManager()

		will := &WillMessage{Topic: "test", Payload: []byte("msg")}
		m.Register("client1", will)
		m.Unregister("client1")

		assert.False(t, m.HasWill("client1"))
		assert.Equal(t, 0, m.GetActiveCount())
	})

	t.Run("trigger will without delay", func(t *testing.T) {
		m := NewWillManager()

		will := &WillMessage{
			Topic:         "status",
			Payload:       []byte("offline"),
			DelayInterval: 0,
		}

		m.Register("client1", will)
		entry := m.TriggerWill("client1", 0)

		require.NotNil(t, entry)
		assert.Equal(t, "client1", entry.ClientID)
		assert.Equal(t, will, entry.Will)
		assert.True(t, entry.IsReady()) // No delay, ready immediately

		assert.False(t, m.HasWill("client1"))
		assert.True(t, m.HasPendingWill("client1"))
	})

	t.Run("trigger will with delay", func(t *testing.T) {
		m := NewWillManager()

		will := &WillMessage{
			Topic:         "status",
			Payload:       []byte("offline"),
			DelayInterval: 5, // 5 seconds
		}

		m.Register("client1", will)
		entry := m.TriggerWill("client1", 0)

		require.NotNil(t, entry)
		assert.False(t, entry.IsReady()) // Should not be ready yet
		assert.True(t, entry.PublishAt.After(time.Now()))
	})

	t.Run("trigger will no will registered", func(t *testing.T) {
		m := NewWillManager()

		entry := m.TriggerWill("client1", 0)
		assert.Nil(t, entry)
	})

	t.Run("cancel pending will", func(t *testing.T) {
		m := NewWillManager()

		will := &WillMessage{Topic: "test", Payload: []byte("msg"), DelayInterval: 60}
		m.Register("client1", will)
		m.TriggerWill("client1", 0)

		assert.True(t, m.HasPendingWill("client1"))

		cancelled := m.CancelPending("client1")
		assert.True(t, cancelled)
		assert.False(t, m.HasPendingWill("client1"))
	})

	t.Run("cancel nonexistent pending will", func(t *testing.T) {
		m := NewWillManager()

		cancelled := m.CancelPending("client1")
		assert.False(t, cancelled)
	})

	t.Run("register clears pending will", func(t *testing.T) {
		m := NewWillManager()

		will1 := &WillMessage{Topic: "test", Payload: []byte("msg1")}
		m.Register("client1", will1)
		m.TriggerWill("client1", 0)

		assert.True(t, m.HasPendingWill("client1"))

		will2 := &WillMessage{Topic: "test", Payload: []byte("msg2")}
		m.Register("client1", will2)

		assert.False(t, m.HasPendingWill("client1"))
		assert.True(t, m.HasWill("client1"))
	})

	t.Run("get ready wills", func(t *testing.T) {
		m := NewWillManager()

		will1 := &WillMessage{Topic: "test1", Payload: []byte("msg1"), DelayInterval: 0}
		will2 := &WillMessage{Topic: "test2", Payload: []byte("msg2"), DelayInterval: 60}

		m.Register("client1", will1)
		m.Register("client2", will2)
		m.TriggerWill("client1", 0)
		m.TriggerWill("client2", 0)

		ready := m.GetReadyWills()
		require.Len(t, ready, 1)
		assert.Equal(t, "client1", ready[0].ClientID)

		// client1 removed from pending
		assert.False(t, m.HasPendingWill("client1"))
		// client2 still pending
		assert.True(t, m.HasPendingWill("client2"))
	})

	t.Run("session expiry limits will delay", func(t *testing.T) {
		m := NewWillManager()

		will := &WillMessage{
			Topic:         "status",
			Payload:       []byte("offline"),
			DelayInterval: 3600, // 1 hour delay
		}

		m.Register("client1", will)
		entry := m.TriggerWill("client1", 5*time.Second) // But session expires in 5 seconds

		require.NotNil(t, entry)
		// PublishAt should be limited to session expiry
		assert.True(t, entry.PublishAt.Before(time.Now().Add(10*time.Second)))
	})

	t.Run("session expired triggers will immediately", func(t *testing.T) {
		m := NewWillManager()

		will := &WillMessage{
			Topic:         "status",
			Payload:       []byte("offline"),
			DelayInterval: 60,
		}

		m.Register("client1", will)
		entry := m.TriggerWill("client1", 1*time.Millisecond)

		time.Sleep(10 * time.Millisecond)

		assert.True(t, entry.IsSessionExpired())

		ready := m.GetReadyWills()
		require.Len(t, ready, 1)
	})

	t.Run("get next publish time", func(t *testing.T) {
		m := NewWillManager()

		will1 := &WillMessage{Topic: "t1", Payload: []byte("m1"), DelayInterval: 10}
		will2 := &WillMessage{Topic: "t2", Payload: []byte("m2"), DelayInterval: 5}

		m.Register("client1", will1)
		m.Register("client2", will2)
		m.TriggerWill("client1", 0)
		m.TriggerWill("client2", 0)

		next := m.GetNextPublishTime()
		assert.True(t, next.After(time.Now()))
		assert.True(t, next.Before(time.Now().Add(6*time.Second)))
	})

	t.Run("get next publish time empty", func(t *testing.T) {
		m := NewWillManager()

		next := m.GetNextPublishTime()
		assert.True(t, next.IsZero())
	})
}
