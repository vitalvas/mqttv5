package mqttv5

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicAliasManager(t *testing.T) {
	t.Run("new manager has correct defaults", func(t *testing.T) {
		m := NewTopicAliasManager(10, 20)

		assert.Equal(t, uint16(10), m.InboundMax())
		assert.Equal(t, uint16(20), m.OutboundMax())
		assert.Equal(t, 0, m.InboundCount())
		assert.Equal(t, 0, m.OutboundCount())
	})

	t.Run("set and get inbound alias", func(t *testing.T) {
		m := NewTopicAliasManager(10, 10)

		err := m.SetInbound(1, "sensors/temp")
		require.NoError(t, err)

		topic, err := m.GetInbound(1)
		require.NoError(t, err)
		assert.Equal(t, "sensors/temp", topic)
	})

	t.Run("inbound alias zero is invalid", func(t *testing.T) {
		m := NewTopicAliasManager(10, 10)

		err := m.SetInbound(0, "test")
		assert.ErrorIs(t, err, ErrTopicAliasInvalid)

		_, err = m.GetInbound(0)
		assert.ErrorIs(t, err, ErrTopicAliasInvalid)
	})

	t.Run("inbound alias exceeds maximum", func(t *testing.T) {
		m := NewTopicAliasManager(5, 10)

		err := m.SetInbound(6, "test")
		assert.ErrorIs(t, err, ErrTopicAliasExceeded)

		err = m.SetInbound(5, "test")
		assert.NoError(t, err)
	})

	t.Run("inbound alias not found", func(t *testing.T) {
		m := NewTopicAliasManager(10, 10)

		_, err := m.GetInbound(5)
		assert.ErrorIs(t, err, ErrTopicAliasNotFound)
	})

	t.Run("inbound alias can be updated", func(t *testing.T) {
		m := NewTopicAliasManager(10, 10)

		err := m.SetInbound(1, "topic/a")
		require.NoError(t, err)

		err = m.SetInbound(1, "topic/b")
		require.NoError(t, err)

		topic, err := m.GetInbound(1)
		require.NoError(t, err)
		assert.Equal(t, "topic/b", topic)
	})

	t.Run("get or create outbound alias", func(t *testing.T) {
		m := NewTopicAliasManager(10, 10)

		alias1 := m.GetOrCreateOutbound("sensors/temp")
		assert.Equal(t, uint16(1), alias1)

		alias2 := m.GetOrCreateOutbound("sensors/humidity")
		assert.Equal(t, uint16(2), alias2)

		// Same topic returns same alias
		alias3 := m.GetOrCreateOutbound("sensors/temp")
		assert.Equal(t, uint16(1), alias3)

		assert.Equal(t, 2, m.OutboundCount())
	})

	t.Run("outbound alias disabled when max is zero", func(t *testing.T) {
		m := NewTopicAliasManager(10, 0)

		alias := m.GetOrCreateOutbound("test")
		assert.Equal(t, uint16(0), alias)
	})

	t.Run("outbound alias exhausted", func(t *testing.T) {
		m := NewTopicAliasManager(10, 2)

		alias1 := m.GetOrCreateOutbound("topic/1")
		assert.Equal(t, uint16(1), alias1)

		alias2 := m.GetOrCreateOutbound("topic/2")
		assert.Equal(t, uint16(2), alias2)

		// No more aliases available
		alias3 := m.GetOrCreateOutbound("topic/3")
		assert.Equal(t, uint16(0), alias3)
	})

	t.Run("get outbound alias", func(t *testing.T) {
		m := NewTopicAliasManager(10, 10)

		// No alias yet
		assert.Equal(t, uint16(0), m.GetOutbound("test"))

		m.GetOrCreateOutbound("test")
		assert.Equal(t, uint16(1), m.GetOutbound("test"))
	})

	t.Run("update max values", func(t *testing.T) {
		m := NewTopicAliasManager(10, 10)

		m.SetInboundMax(20)
		m.SetOutboundMax(30)

		assert.Equal(t, uint16(20), m.InboundMax())
		assert.Equal(t, uint16(30), m.OutboundMax())
	})

	t.Run("clear removes all aliases", func(t *testing.T) {
		m := NewTopicAliasManager(10, 10)

		m.SetInbound(1, "topic/a")
		m.SetInbound(2, "topic/b")
		m.GetOrCreateOutbound("topic/c")

		assert.Equal(t, 2, m.InboundCount())
		assert.Equal(t, 1, m.OutboundCount())

		m.Clear()

		assert.Equal(t, 0, m.InboundCount())
		assert.Equal(t, 0, m.OutboundCount())

		// Next outbound starts from 1 again
		alias := m.GetOrCreateOutbound("new/topic")
		assert.Equal(t, uint16(1), alias)
	})

	t.Run("inbound max zero allows any alias", func(t *testing.T) {
		m := NewTopicAliasManager(0, 10)

		err := m.SetInbound(65535, "test")
		assert.NoError(t, err)
	})
}
