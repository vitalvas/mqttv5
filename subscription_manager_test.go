package mqttv5

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionManager(t *testing.T) {
	t.Run("subscribe and match", func(t *testing.T) {
		m := NewSubscriptionManager()

		err := m.Subscribe("client1", Subscription{TopicFilter: "sensors/+/temp", QoS: 1})
		require.NoError(t, err)

		matches := m.Match("sensors/room1/temp")
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)
		assert.Equal(t, byte(1), matches[0].Subscription.QoS)
	})

	t.Run("multiple clients same filter", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", Subscription{TopicFilter: "test/#", QoS: 0})
		m.Subscribe("client2", Subscription{TopicFilter: "test/#", QoS: 1})

		matches := m.Match("test/topic")
		assert.Len(t, matches, 2)
	})

	t.Run("unsubscribe", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", Subscription{TopicFilter: "topic/a", QoS: 0})
		m.Subscribe("client1", Subscription{TopicFilter: "topic/b", QoS: 0})

		removed := m.Unsubscribe("client1", "topic/a")
		assert.True(t, removed)

		matches := m.Match("topic/a")
		assert.Len(t, matches, 0)

		matches = m.Match("topic/b")
		assert.Len(t, matches, 1)
	})

	t.Run("unsubscribe nonexistent", func(t *testing.T) {
		m := NewSubscriptionManager()

		removed := m.Unsubscribe("client1", "topic/a")
		assert.False(t, removed)
	})

	t.Run("unsubscribe all", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", Subscription{TopicFilter: "a", QoS: 0})
		m.Subscribe("client1", Subscription{TopicFilter: "b", QoS: 0})
		m.Subscribe("client2", Subscription{TopicFilter: "a", QoS: 0})

		m.UnsubscribeAll("client1")

		subs := m.GetSubscriptions("client1")
		assert.Len(t, subs, 0)

		subs = m.GetSubscriptions("client2")
		assert.Len(t, subs, 1)
	})

	t.Run("get subscriptions", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", Subscription{TopicFilter: "a", QoS: 0})
		m.Subscribe("client1", Subscription{TopicFilter: "b", QoS: 1})

		subs := m.GetSubscriptions("client1")
		assert.Len(t, subs, 2)
	})

	t.Run("update subscription replaces existing", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", Subscription{TopicFilter: "topic", QoS: 0})
		m.Subscribe("client1", Subscription{TopicFilter: "topic", QoS: 1})

		subs := m.GetSubscriptions("client1")
		require.Len(t, subs, 1)
		assert.Equal(t, byte(1), subs[0].QoS)
	})

	t.Run("invalid filter rejected", func(t *testing.T) {
		m := NewSubscriptionManager()

		err := m.Subscribe("client1", Subscription{TopicFilter: "", QoS: 0})
		assert.Error(t, err)
	})

	t.Run("count", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", Subscription{TopicFilter: "a", QoS: 0})
		m.Subscribe("client1", Subscription{TopicFilter: "b", QoS: 0})
		m.Subscribe("client2", Subscription{TopicFilter: "c", QoS: 0})

		assert.Equal(t, 3, m.Count())
		assert.Equal(t, 2, m.ClientCount())
	})
}

func TestSubscriptionManagerNoLocal(t *testing.T) {
	t.Run("NoLocal filters out publisher", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", Subscription{TopicFilter: "topic", QoS: 1, NoLocal: true})
		m.Subscribe("client2", Subscription{TopicFilter: "topic", QoS: 1, NoLocal: false})

		// client1 publishes - should not receive due to NoLocal
		matches := m.MatchForDelivery("topic", "client1")
		require.Len(t, matches, 1)
		assert.Equal(t, "client2", matches[0].ClientID)
	})

	t.Run("NoLocal false allows self-delivery", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", Subscription{TopicFilter: "topic", QoS: 1, NoLocal: false})

		matches := m.MatchForDelivery("topic", "client1")
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)
	})

	t.Run("different publisher not affected by NoLocal", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", Subscription{TopicFilter: "topic", QoS: 1, NoLocal: true})

		// client2 publishes - client1 should receive
		matches := m.MatchForDelivery("topic", "client2")
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)
	})
}

func TestSubscriptionManagerDeduplication(t *testing.T) {
	t.Run("multiple matching subscriptions deduplicated", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", Subscription{TopicFilter: "a/b", QoS: 0})
		m.Subscribe("client1", Subscription{TopicFilter: "a/+", QoS: 1})
		m.Subscribe("client1", Subscription{TopicFilter: "#", QoS: 2})

		matches := m.MatchForDelivery("a/b", "other")

		// Should only have one entry for client1 with highest QoS
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)
		assert.Equal(t, byte(2), matches[0].Subscription.QoS)
	})
}

func TestShouldSendRetained(t *testing.T) {
	t.Run("retain handling 0 always sends", func(t *testing.T) {
		assert.True(t, ShouldSendRetained(0, true))
		assert.True(t, ShouldSendRetained(0, false))
	})

	t.Run("retain handling 1 sends only if new", func(t *testing.T) {
		assert.True(t, ShouldSendRetained(1, true))
		assert.False(t, ShouldSendRetained(1, false))
	})

	t.Run("retain handling 2 never sends", func(t *testing.T) {
		assert.False(t, ShouldSendRetained(2, true))
		assert.False(t, ShouldSendRetained(2, false))
	})

	t.Run("unknown value defaults to send", func(t *testing.T) {
		assert.True(t, ShouldSendRetained(3, true))
	})
}

func TestGetDeliveryRetain(t *testing.T) {
	t.Run("RetainAsPublish true preserves flag", func(t *testing.T) {
		sub := Subscription{RetainAsPublish: true}

		assert.True(t, GetDeliveryRetain(sub, true))
		assert.False(t, GetDeliveryRetain(sub, false))
	})

	t.Run("RetainAsPublish false clears flag", func(t *testing.T) {
		sub := Subscription{RetainAsPublish: false}

		assert.False(t, GetDeliveryRetain(sub, true))
		assert.False(t, GetDeliveryRetain(sub, false))
	})
}
