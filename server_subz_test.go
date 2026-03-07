package mqttv5

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionManagerSummary(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		m := NewSubscriptionManager()
		summary := m.Summary("")
		assert.Equal(t, 0, summary.TotalSubscriptions)
		assert.Equal(t, 0, summary.TotalClients)
		assert.Equal(t, 0, summary.SharedGroups)
		assert.Empty(t, summary.Subscriptions)
	})

	t.Run("all namespaces", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", "ns1", Subscription{TopicFilter: "a/b", QoS: QoS0})
		m.Subscribe("c2", "ns2", Subscription{TopicFilter: "c/d", QoS: QoS1})
		m.Subscribe("c1", "ns1", Subscription{TopicFilter: "e/f", QoS: QoS2})

		summary := m.Summary("")
		assert.Equal(t, 3, summary.TotalSubscriptions)
		assert.Equal(t, 2, summary.TotalClients)
		assert.Len(t, summary.Subscriptions, 3)
	})

	t.Run("filtered by namespace", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", "ns1", Subscription{TopicFilter: "a/b", QoS: QoS0})
		m.Subscribe("c2", "ns2", Subscription{TopicFilter: "c/d", QoS: QoS1})
		m.Subscribe("c3", "ns1", Subscription{TopicFilter: "e/f", QoS: QoS2})

		summary := m.Summary("ns1")
		assert.Equal(t, 2, summary.TotalSubscriptions)
		assert.Equal(t, 2, summary.TotalClients)

		summary2 := m.Summary("ns2")
		assert.Equal(t, 1, summary2.TotalSubscriptions)
		assert.Equal(t, 1, summary2.TotalClients)
	})

	t.Run("subscription info fields", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", DefaultNamespace, Subscription{
			TopicFilter:     "sensor/+/temp",
			QoS:             QoS1,
			NoLocal:         true,
			RetainAsPublish: true,
			RetainHandling:  1,
			SubscriptionID:  42,
		})

		summary := m.Summary("")
		require.Len(t, summary.Subscriptions, 1)

		info := summary.Subscriptions[0]
		assert.Equal(t, "c1", info.ClientID)
		assert.Equal(t, DefaultNamespace, info.Namespace)
		assert.Equal(t, "sensor/+/temp", info.TopicFilter)
		assert.Equal(t, QoS1, info.QoS)
		assert.True(t, info.NoLocal)
		assert.True(t, info.RetainAsPublish)
		assert.Equal(t, byte(1), info.RetainHandling)
		assert.Equal(t, uint32(42), info.SubscriptionID)
		assert.False(t, info.Shared)
		assert.Empty(t, info.ShareGroup)
	})

	t.Run("shared subscriptions", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", DefaultNamespace, Subscription{TopicFilter: "$share/group1/sensor/#", QoS: QoS1})
		m.Subscribe("c2", DefaultNamespace, Subscription{TopicFilter: "$share/group1/sensor/#", QoS: QoS0})
		m.Subscribe("c3", DefaultNamespace, Subscription{TopicFilter: "$share/group2/data/#", QoS: QoS2})

		summary := m.Summary("")
		assert.Equal(t, 3, summary.TotalSubscriptions)
		assert.Equal(t, 2, summary.SharedGroups)

		shared := 0
		for _, info := range summary.Subscriptions {
			if info.Shared {
				shared++
				assert.NotEmpty(t, info.ShareGroup)
			}
		}
		assert.Equal(t, 3, shared)
	})

	t.Run("shared groups filtered by namespace", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", "ns1", Subscription{TopicFilter: "$share/g1/topic", QoS: QoS0})
		m.Subscribe("c2", "ns2", Subscription{TopicFilter: "$share/g1/topic", QoS: QoS0})

		summary := m.Summary("ns1")
		assert.Equal(t, 1, summary.SharedGroups)

		summary2 := m.Summary("ns2")
		assert.Equal(t, 1, summary2.SharedGroups)

		summaryAll := m.Summary("")
		assert.Equal(t, 2, summaryAll.SharedGroups)
	})

	t.Run("nonexistent namespace", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", "ns1", Subscription{TopicFilter: "a/b", QoS: QoS0})

		summary := m.Summary("ns999")
		assert.Equal(t, 0, summary.TotalSubscriptions)
		assert.Equal(t, 0, summary.TotalClients)
	})
}

func TestSubscriptionManagerClientSubscriptionInfo(t *testing.T) {
	t.Run("existing client", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", DefaultNamespace, Subscription{TopicFilter: "a/b", QoS: QoS0})
		m.Subscribe("c1", DefaultNamespace, Subscription{TopicFilter: "c/d", QoS: QoS1})

		infos := m.ClientSubscriptionInfo(DefaultNamespace, "c1")
		require.Len(t, infos, 2)

		filters := make(map[string]byte)
		for _, info := range infos {
			filters[info.TopicFilter] = info.QoS
			assert.Equal(t, "c1", info.ClientID)
			assert.Equal(t, DefaultNamespace, info.Namespace)
		}
		assert.Equal(t, QoS0, filters["a/b"])
		assert.Equal(t, QoS1, filters["c/d"])
	})

	t.Run("nonexistent client", func(t *testing.T) {
		m := NewSubscriptionManager()
		infos := m.ClientSubscriptionInfo(DefaultNamespace, "nope")
		assert.Nil(t, infos)
	})

	t.Run("namespace isolation", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", "ns1", Subscription{TopicFilter: "a/b", QoS: QoS0})
		m.Subscribe("c1", "ns2", Subscription{TopicFilter: "c/d", QoS: QoS1})

		infos := m.ClientSubscriptionInfo("ns1", "c1")
		require.Len(t, infos, 1)
		assert.Equal(t, "a/b", infos[0].TopicFilter)

		infos2 := m.ClientSubscriptionInfo("ns2", "c1")
		require.Len(t, infos2, 1)
		assert.Equal(t, "c/d", infos2[0].TopicFilter)
	})

	t.Run("shared subscription info", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", DefaultNamespace, Subscription{TopicFilter: "$share/mygroup/sensor/#", QoS: QoS1})

		infos := m.ClientSubscriptionInfo(DefaultNamespace, "c1")
		require.Len(t, infos, 1)
		assert.True(t, infos[0].Shared)
		assert.Equal(t, "mygroup", infos[0].ShareGroup)
	})
}

func TestServerGetSubscriptionSummary(t *testing.T) {
	t.Run("empty server", func(t *testing.T) {
		srv := NewServer()
		summary := srv.GetSubscriptionSummary("")
		assert.Equal(t, 0, summary.TotalSubscriptions)
		assert.Equal(t, 0, summary.TotalClients)
	})

	t.Run("with subscriptions", func(t *testing.T) {
		srv := NewServer()
		srv.subs.Subscribe("c1", DefaultNamespace, Subscription{TopicFilter: "a/b", QoS: QoS0})
		srv.subs.Subscribe("c2", DefaultNamespace, Subscription{TopicFilter: "c/d", QoS: QoS1})

		summary := srv.GetSubscriptionSummary(DefaultNamespace)
		assert.Equal(t, 2, summary.TotalSubscriptions)
		assert.Equal(t, 2, summary.TotalClients)
		assert.Len(t, summary.Subscriptions, 2)
	})

	t.Run("filtered by namespace", func(t *testing.T) {
		srv := NewServer()
		srv.subs.Subscribe("c1", "ns1", Subscription{TopicFilter: "a/b", QoS: QoS0})
		srv.subs.Subscribe("c2", "ns2", Subscription{TopicFilter: "c/d", QoS: QoS1})

		summary := srv.GetSubscriptionSummary("ns1")
		assert.Equal(t, 1, summary.TotalSubscriptions)
	})
}

func TestServerGetClientSubscriptions(t *testing.T) {
	t.Run("existing client", func(t *testing.T) {
		srv := NewServer()
		srv.subs.Subscribe("c1", DefaultNamespace, Subscription{TopicFilter: "a/b", QoS: QoS0})

		infos := srv.GetClientSubscriptions(DefaultNamespace, "c1")
		require.Len(t, infos, 1)
		assert.Equal(t, "a/b", infos[0].TopicFilter)
	})

	t.Run("nonexistent client", func(t *testing.T) {
		srv := NewServer()
		infos := srv.GetClientSubscriptions(DefaultNamespace, "nope")
		assert.Nil(t, infos)
	})
}
