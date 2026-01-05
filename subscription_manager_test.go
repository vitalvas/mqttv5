package mqttv5

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionManager(t *testing.T) {
	t.Run("subscribe and match", func(t *testing.T) {
		m := NewSubscriptionManager()

		err := m.Subscribe("client1", testNS, Subscription{TopicFilter: "sensors/+/temp", QoS: 1})
		require.NoError(t, err)

		matches := m.MatchForDelivery("sensors/room1/temp", "", testNS)
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)
		assert.Equal(t, byte(1), matches[0].Subscription.QoS)
	})

	t.Run("multiple clients same filter", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "test/#", QoS: 0})
		m.Subscribe("client2", testNS, Subscription{TopicFilter: "test/#", QoS: 1})

		matches := m.MatchForDelivery("test/topic", "", testNS)
		assert.Len(t, matches, 2)
	})

	t.Run("unsubscribe", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "topic/a", QoS: 0})
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "topic/b", QoS: 0})

		removed := m.Unsubscribe("client1", testNS, "topic/a")
		assert.True(t, removed)

		matches := m.MatchForDelivery("topic/a", "", testNS)
		assert.Len(t, matches, 0)

		matches = m.MatchForDelivery("topic/b", "", testNS)
		assert.Len(t, matches, 1)
	})

	t.Run("unsubscribe nonexistent", func(t *testing.T) {
		m := NewSubscriptionManager()

		removed := m.Unsubscribe("client1", testNS, "topic/a")
		assert.False(t, removed)
	})

	t.Run("unsubscribe all", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "a", QoS: 0})
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "b", QoS: 0})
		m.Subscribe("client2", testNS, Subscription{TopicFilter: "a", QoS: 0})

		m.UnsubscribeAll("client1", testNS)

		subs := m.GetSubscriptions("client1", testNS)
		assert.Len(t, subs, 0)

		subs = m.GetSubscriptions("client2", testNS)
		assert.Len(t, subs, 1)
	})

	t.Run("get subscriptions", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "a", QoS: 0})
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "b", QoS: 1})

		subs := m.GetSubscriptions("client1", testNS)
		assert.Len(t, subs, 2)
	})

	t.Run("update subscription replaces existing", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "topic", QoS: 0})
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "topic", QoS: 1})

		subs := m.GetSubscriptions("client1", testNS)
		require.Len(t, subs, 1)
		assert.Equal(t, byte(1), subs[0].QoS)
	})

	t.Run("invalid filter rejected", func(t *testing.T) {
		m := NewSubscriptionManager()

		err := m.Subscribe("client1", testNS, Subscription{TopicFilter: "", QoS: 0})
		assert.Error(t, err)
	})

	t.Run("count", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "a", QoS: 0})
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "b", QoS: 0})
		m.Subscribe("client2", testNS, Subscription{TopicFilter: "c", QoS: 0})

		assert.Equal(t, 3, m.Count())
		assert.Equal(t, 2, m.ClientCount())
	})
}

func TestSubscriptionManagerNoLocal(t *testing.T) {
	t.Run("NoLocal filters out publisher", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "topic", QoS: 1, NoLocal: true})
		m.Subscribe("client2", testNS, Subscription{TopicFilter: "topic", QoS: 1, NoLocal: false})

		// client1 publishes - should not receive due to NoLocal
		matches := m.MatchForDelivery("topic", "client1", testNS)
		require.Len(t, matches, 1)
		assert.Equal(t, "client2", matches[0].ClientID)
	})

	t.Run("NoLocal false allows self-delivery", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "topic", QoS: 1, NoLocal: false})

		matches := m.MatchForDelivery("topic", "client1", testNS)
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)
	})

	t.Run("different publisher not affected by NoLocal", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "topic", QoS: 1, NoLocal: true})

		// client2 publishes - client1 should receive
		matches := m.MatchForDelivery("topic", "client2", testNS)
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)
	})
}

func TestSubscriptionManagerDeduplication(t *testing.T) {
	t.Run("multiple matching subscriptions deduplicated", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "a/b", QoS: 0})
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "a/+", QoS: 1})
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "#", QoS: 2})

		matches := m.MatchForDelivery("a/b", "other", testNS)

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

func TestSubscriptionManagerSubscriptionIDsAggregation(t *testing.T) {
	t.Run("aggregates subscription IDs from multiple matching subscriptions", func(t *testing.T) {
		m := NewSubscriptionManager()

		// Subscribe with different subscription IDs
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "a/b", QoS: 0, SubscriptionID: 1})
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "a/+", QoS: 1, SubscriptionID: 2})
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "#", QoS: 2, SubscriptionID: 3})

		matches := m.MatchForDelivery("a/b", "other", testNS)

		// Should have one entry with all subscription IDs
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)
		assert.Equal(t, byte(2), matches[0].Subscription.QoS)
		assert.Len(t, matches[0].SubscriptionIDs, 3)
		assert.Contains(t, matches[0].SubscriptionIDs, uint32(1))
		assert.Contains(t, matches[0].SubscriptionIDs, uint32(2))
		assert.Contains(t, matches[0].SubscriptionIDs, uint32(3))
	})

	t.Run("no subscription IDs when not set", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "topic", QoS: 1})

		matches := m.MatchForDelivery("topic", "other", testNS)

		require.Len(t, matches, 1)
		assert.Len(t, matches[0].SubscriptionIDs, 0)
	})

	t.Run("aggregates RetainAsPublish from any matching subscription", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "a/b", QoS: 0, RetainAsPublish: false})
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "a/+", QoS: 1, RetainAsPublish: true})

		matches := m.MatchForDelivery("a/b", "other", testNS)

		require.Len(t, matches, 1)
		assert.True(t, matches[0].Subscription.RetainAsPublish)
	})
}

func TestSharedSubscriptions(t *testing.T) {
	t.Run("shared subscription basic matching", func(t *testing.T) {
		m := NewSubscriptionManager()

		err := m.Subscribe("client1", testNS, Subscription{TopicFilter: "$share/group1/sensors/+", QoS: 1})
		require.NoError(t, err)

		// Messages to the actual topic should match
		matches := m.MatchForDelivery("sensors/room1", "", testNS)
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)
		assert.Equal(t, "group1", matches[0].ShareGroup)
	})

	t.Run("shared subscription load balancing round-robin", func(t *testing.T) {
		m := NewSubscriptionManager()

		// Add multiple subscribers to same share group
		m.Subscribe("client1", testNS, Subscription{TopicFilter: "$share/mygroup/topic", QoS: 1})
		m.Subscribe("client2", testNS, Subscription{TopicFilter: "$share/mygroup/topic", QoS: 1})
		m.Subscribe("client3", testNS, Subscription{TopicFilter: "$share/mygroup/topic", QoS: 1})

		// Track which clients get messages
		clientCounts := make(map[string]int)
		for range 9 {
			matches := m.MatchForDelivery("topic", "publisher", testNS)
			require.Len(t, matches, 1)
			clientCounts[matches[0].ClientID]++
		}

		// All clients should have received some messages (round-robin)
		assert.Equal(t, 3, clientCounts["client1"])
		assert.Equal(t, 3, clientCounts["client2"])
		assert.Equal(t, 3, clientCounts["client3"])
	})

	t.Run("different share groups are independent", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "$share/group1/topic", QoS: 1})
		m.Subscribe("client2", testNS, Subscription{TopicFilter: "$share/group2/topic", QoS: 1})

		// Each group should get one delivery
		matches := m.MatchForDelivery("topic", "publisher", testNS)
		assert.Len(t, matches, 2)
	})

	t.Run("unsubscribe shared subscription", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "$share/group1/topic", QoS: 1})
		m.Subscribe("client2", testNS, Subscription{TopicFilter: "$share/group1/topic", QoS: 1})

		// Unsubscribe one client
		removed := m.Unsubscribe("client1", testNS, "$share/group1/topic")
		assert.True(t, removed)

		// Only client2 should receive
		matches := m.MatchForDelivery("topic", "publisher", testNS)
		require.Len(t, matches, 1)
		assert.Equal(t, "client2", matches[0].ClientID)
	})

	t.Run("regular and shared subscriptions coexist", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", testNS, Subscription{TopicFilter: "$share/group1/topic", QoS: 1})
		m.Subscribe("client2", testNS, Subscription{TopicFilter: "topic", QoS: 1})

		matches := m.MatchForDelivery("topic", "publisher", testNS)
		// Should get one from shared group + one regular
		assert.Len(t, matches, 2)
	})

	t.Run("namespace isolation - different namespaces don't see each other", func(t *testing.T) {
		m := NewSubscriptionManager()

		// Subscribe clients in different namespaces to the same topic
		m.Subscribe("client1", "tenant-a", Subscription{TopicFilter: "sensors/#", QoS: 1})
		m.Subscribe("client2", "tenant-b", Subscription{TopicFilter: "sensors/#", QoS: 1})

		// Publisher in tenant-a should only reach client1
		matches := m.MatchForDelivery("sensors/temp", "publisher", "tenant-a")
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)
		assert.Equal(t, "tenant-a", matches[0].Namespace)

		// Publisher in tenant-b should only reach client2
		matches = m.MatchForDelivery("sensors/temp", "publisher", "tenant-b")
		require.Len(t, matches, 1)
		assert.Equal(t, "client2", matches[0].ClientID)
		assert.Equal(t, "tenant-b", matches[0].Namespace)
	})

	t.Run("namespace isolation - same client ID different namespaces", func(t *testing.T) {
		m := NewSubscriptionManager()

		// Same client ID in different namespaces are treated as separate clients
		m.Subscribe("client1", "tenant-a", Subscription{TopicFilter: "topic", QoS: 0})
		m.Subscribe("client1", "tenant-b", Subscription{TopicFilter: "topic", QoS: 1})

		// Each namespace sees its own subscription
		subsA := m.GetSubscriptions("client1", "tenant-a")
		require.Len(t, subsA, 1)
		assert.Equal(t, byte(0), subsA[0].QoS)

		subsB := m.GetSubscriptions("client1", "tenant-b")
		require.Len(t, subsB, 1)
		assert.Equal(t, byte(1), subsB[0].QoS)

		// Publishing in tenant-a only reaches tenant-a client
		matches := m.MatchForDelivery("topic", "other", "tenant-a")
		require.Len(t, matches, 1)
		assert.Equal(t, "tenant-a", matches[0].Namespace)
	})

	t.Run("namespace isolation - unsubscribe only affects own namespace", func(t *testing.T) {
		m := NewSubscriptionManager()

		m.Subscribe("client1", "tenant-a", Subscription{TopicFilter: "topic", QoS: 1})
		m.Subscribe("client1", "tenant-b", Subscription{TopicFilter: "topic", QoS: 1})

		// Unsubscribe from tenant-a
		m.Unsubscribe("client1", "tenant-a", "topic")

		// tenant-a subscription should be gone
		subsA := m.GetSubscriptions("client1", "tenant-a")
		assert.Len(t, subsA, 0)

		// tenant-b subscription should remain
		subsB := m.GetSubscriptions("client1", "tenant-b")
		assert.Len(t, subsB, 1)
	})

	t.Run("namespace isolation - shared subscriptions are namespace-scoped", func(t *testing.T) {
		m := NewSubscriptionManager()

		// Same share group name in different namespaces are separate
		m.Subscribe("client1", "tenant-a", Subscription{TopicFilter: "$share/group/topic", QoS: 1})
		m.Subscribe("client2", "tenant-b", Subscription{TopicFilter: "$share/group/topic", QoS: 1})

		// Publisher in tenant-a should only see tenant-a subscriber
		matches := m.MatchForDelivery("topic", "publisher", "tenant-a")
		require.Len(t, matches, 1)
		assert.Equal(t, "client1", matches[0].ClientID)

		// Publisher in tenant-b should only see tenant-b subscriber
		matches = m.MatchForDelivery("topic", "publisher", "tenant-b")
		require.Len(t, matches, 1)
		assert.Equal(t, "client2", matches[0].ClientID)
	})
}
