package mqttv5

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKeepAliveManager(t *testing.T) {
	t.Run("register client", func(t *testing.T) {
		m := NewKeepAliveManager()

		effective := m.Register("client1", 60)
		assert.Equal(t, uint16(60), effective)
		assert.Equal(t, 1, m.Count())

		keepAlive, ok := m.GetKeepAlive("client1")
		assert.True(t, ok)
		assert.Equal(t, uint16(60), keepAlive)
	})

	t.Run("server override", func(t *testing.T) {
		m := NewKeepAliveManager()
		m.SetServerOverride(30)

		effective := m.Register("client1", 60)
		assert.Equal(t, uint16(30), effective)

		keepAlive, ok := m.GetKeepAlive("client1")
		assert.True(t, ok)
		assert.Equal(t, uint16(30), keepAlive)
	})

	t.Run("unregister client", func(t *testing.T) {
		m := NewKeepAliveManager()

		m.Register("client1", 60)
		m.Unregister("client1")

		assert.Equal(t, 0, m.Count())

		_, ok := m.GetKeepAlive("client1")
		assert.False(t, ok)
	})

	t.Run("zero keep-alive never expires", func(t *testing.T) {
		m := NewKeepAliveManager()

		m.Register("client1", 0)

		assert.False(t, m.IsExpired("client1"))

		expired := m.GetExpiredClients()
		assert.Len(t, expired, 0)
	})

	t.Run("update activity extends deadline", func(t *testing.T) {
		m := NewKeepAliveManager()
		m.SetGraceFactor(1.0) // No grace for testing

		m.Register("client1", 1) // 1 second keep-alive

		deadline1, _ := m.GetDeadline("client1")

		time.Sleep(100 * time.Millisecond)
		m.UpdateActivity("client1")

		deadline2, _ := m.GetDeadline("client1")

		assert.True(t, deadline2.After(deadline1))
	})

	t.Run("get deadline", func(t *testing.T) {
		m := NewKeepAliveManager()

		m.Register("client1", 60)

		deadline, ok := m.GetDeadline("client1")
		assert.True(t, ok)
		assert.True(t, deadline.After(time.Now()))
	})

	t.Run("get deadline unknown client", func(t *testing.T) {
		m := NewKeepAliveManager()

		_, ok := m.GetDeadline("unknown")
		assert.False(t, ok)
	})

	t.Run("is expired unknown client", func(t *testing.T) {
		m := NewKeepAliveManager()

		assert.False(t, m.IsExpired("unknown"))
	})

	t.Run("set grace factor minimum", func(t *testing.T) {
		m := NewKeepAliveManager()

		m.SetGraceFactor(0.5) // Should be clamped to 1.0

		m.Register("client1", 10)
		deadline, _ := m.GetDeadline("client1")

		// With grace factor 1.0, deadline should be ~10 seconds from now
		expectedMin := time.Now().Add(9 * time.Second)
		expectedMax := time.Now().Add(11 * time.Second)

		assert.True(t, deadline.After(expectedMin))
		assert.True(t, deadline.Before(expectedMax))
	})

	t.Run("server override getter", func(t *testing.T) {
		m := NewKeepAliveManager()

		assert.Equal(t, uint16(0), m.ServerOverride())

		m.SetServerOverride(120)
		assert.Equal(t, uint16(120), m.ServerOverride())
	})

	t.Run("update activity unknown client", func(_ *testing.T) {
		m := NewKeepAliveManager()

		// Should not panic
		m.UpdateActivity("unknown")
	})
}

func TestKeepAliveExpiration(t *testing.T) {
	t.Parallel()

	t.Run("client expires after timeout", func(t *testing.T) {
		t.Parallel()
		m := NewKeepAliveManager()
		m.SetGraceFactor(1.0)

		m.Register("client1", 1) // 1 second

		assert.False(t, m.IsExpired("client1"))

		time.Sleep(1100 * time.Millisecond)

		assert.True(t, m.IsExpired("client1"))
	})

	t.Run("get expired clients", func(t *testing.T) {
		t.Parallel()
		m := NewKeepAliveManager()
		m.SetGraceFactor(1.0)

		m.Register("client1", 1)  // 1 second
		m.Register("client2", 0)  // never expires
		m.Register("client3", 60) // 60 seconds

		time.Sleep(1100 * time.Millisecond)

		expired := m.GetExpiredClients()
		assert.Len(t, expired, 1)
		assert.Contains(t, expired, "client1")
	})
}
