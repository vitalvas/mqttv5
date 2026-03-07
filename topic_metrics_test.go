package mqttv5

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicMetrics(t *testing.T) {
	t.Run("record and get", func(t *testing.T) {
		m := NewTopicMetrics()
		m.recordMessageIn(DefaultNamespace, "sensor/temp", 100)
		m.recordMessageIn(DefaultNamespace, "sensor/temp", 200)
		m.recordMessageOut(DefaultNamespace, "sensor/temp", 50)

		info := m.Get(DefaultNamespace, "sensor/temp")
		require.NotNil(t, info)
		assert.Equal(t, "sensor/temp", info.Topic)
		assert.Equal(t, DefaultNamespace, info.Namespace)
		assert.Equal(t, int64(2), info.MessagesIn)
		assert.Equal(t, int64(1), info.MessagesOut)
		assert.Equal(t, int64(300), info.BytesIn)
		assert.Equal(t, int64(50), info.BytesOut)
	})

	t.Run("get nonexistent", func(t *testing.T) {
		m := NewTopicMetrics()
		info := m.Get(DefaultNamespace, "no/such/topic")
		assert.Nil(t, info)
	})

	t.Run("namespace isolation", func(t *testing.T) {
		m := NewTopicMetrics()
		m.recordMessageIn("ns1", "topic/a", 10)
		m.recordMessageIn("ns2", "topic/a", 20)

		info1 := m.Get("ns1", "topic/a")
		require.NotNil(t, info1)
		assert.Equal(t, int64(1), info1.MessagesIn)
		assert.Equal(t, int64(10), info1.BytesIn)

		info2 := m.Get("ns2", "topic/a")
		require.NotNil(t, info2)
		assert.Equal(t, int64(1), info2.MessagesIn)
		assert.Equal(t, int64(20), info2.BytesIn)
	})

	t.Run("all topics", func(t *testing.T) {
		m := NewTopicMetrics()
		m.recordMessageIn("ns1", "a/b", 10)
		m.recordMessageIn("ns2", "c/d", 20)
		m.recordMessageIn("ns1", "e/f", 30)

		all := m.All("")
		assert.Len(t, all, 3)

		filtered := m.All("ns1")
		assert.Len(t, filtered, 2)

		filtered2 := m.All("ns2")
		assert.Len(t, filtered2, 1)

		empty := m.All("ns999")
		assert.Empty(t, empty)
	})

	t.Run("topic count", func(t *testing.T) {
		m := NewTopicMetrics()
		m.recordMessageIn("ns1", "a/b", 10)
		m.recordMessageIn("ns1", "c/d", 20)
		m.recordMessageIn("ns2", "e/f", 30)

		assert.Equal(t, 3, m.TopicCount(""))
		assert.Equal(t, 2, m.TopicCount("ns1"))
		assert.Equal(t, 1, m.TopicCount("ns2"))
		assert.Equal(t, 0, m.TopicCount("ns999"))
	})

	t.Run("reset", func(t *testing.T) {
		m := NewTopicMetrics()
		m.recordMessageIn(DefaultNamespace, "a/b", 10)
		assert.Equal(t, 1, m.TopicCount(""))

		m.Reset()
		assert.Equal(t, 0, m.TopicCount(""))
		assert.Nil(t, m.Get(DefaultNamespace, "a/b"))
	})

	t.Run("concurrent access", func(t *testing.T) {
		m := NewTopicMetrics()
		var wg sync.WaitGroup

		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 100 {
					m.recordMessageIn(DefaultNamespace, "concurrent/topic", 10)
					m.recordMessageOut(DefaultNamespace, "concurrent/topic", 5)
				}
			}()
		}

		wg.Wait()

		info := m.Get(DefaultNamespace, "concurrent/topic")
		require.NotNil(t, info)
		assert.Equal(t, int64(1000), info.MessagesIn)
		assert.Equal(t, int64(1000), info.MessagesOut)
		assert.Equal(t, int64(10000), info.BytesIn)
		assert.Equal(t, int64(5000), info.BytesOut)
	})
}

func TestSubscriptionManagerSubscriberCount(t *testing.T) {
	t.Run("exact match", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", DefaultNamespace, Subscription{TopicFilter: "sensor/temp", QoS: QoS0})
		m.Subscribe("c2", DefaultNamespace, Subscription{TopicFilter: "sensor/temp", QoS: QoS1})

		assert.Equal(t, 2, m.SubscriberCount(DefaultNamespace, "sensor/temp"))
	})

	t.Run("wildcard match", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", DefaultNamespace, Subscription{TopicFilter: "sensor/+/temp", QoS: QoS0})
		m.Subscribe("c2", DefaultNamespace, Subscription{TopicFilter: "sensor/#", QoS: QoS1})

		assert.Equal(t, 2, m.SubscriberCount(DefaultNamespace, "sensor/room1/temp"))
		assert.Equal(t, 1, m.SubscriberCount(DefaultNamespace, "sensor/room1/humidity"))
	})

	t.Run("namespace isolation", func(t *testing.T) {
		m := NewSubscriptionManager()
		m.Subscribe("c1", "ns1", Subscription{TopicFilter: "topic/a", QoS: QoS0})
		m.Subscribe("c2", "ns2", Subscription{TopicFilter: "topic/a", QoS: QoS0})

		assert.Equal(t, 1, m.SubscriberCount("ns1", "topic/a"))
		assert.Equal(t, 1, m.SubscriberCount("ns2", "topic/a"))
	})

	t.Run("no subscribers", func(t *testing.T) {
		m := NewSubscriptionManager()
		assert.Equal(t, 0, m.SubscriberCount(DefaultNamespace, "no/subscribers"))
	})
}

func TestServerTopicMetrics(t *testing.T) {
	t.Run("get topic metrics with subscribers", func(t *testing.T) {
		srv := NewServer()
		srv.subs.Subscribe("c1", DefaultNamespace, Subscription{TopicFilter: "sensor/temp", QoS: QoS0})
		srv.subs.Subscribe("c2", DefaultNamespace, Subscription{TopicFilter: "sensor/+", QoS: QoS1})
		srv.topicMetrics.recordMessageIn(DefaultNamespace, "sensor/temp", 100)

		info := srv.GetTopicMetrics(DefaultNamespace, "sensor/temp")
		require.NotNil(t, info)
		assert.Equal(t, int64(1), info.MessagesIn)
		assert.Equal(t, 2, info.Subscribers)
	})

	t.Run("get nonexistent topic", func(t *testing.T) {
		srv := NewServer()
		info := srv.GetTopicMetrics(DefaultNamespace, "no/topic")
		assert.Nil(t, info)
	})

	t.Run("all topic metrics", func(t *testing.T) {
		srv := NewServer()
		srv.subs.Subscribe("c1", DefaultNamespace, Subscription{TopicFilter: "a/b", QoS: QoS0})
		srv.topicMetrics.recordMessageIn(DefaultNamespace, "a/b", 10)
		srv.topicMetrics.recordMessageIn(DefaultNamespace, "c/d", 20)

		infos := srv.AllTopicMetrics(DefaultNamespace)
		assert.Len(t, infos, 2)

		for _, info := range infos {
			if info.Topic == "a/b" {
				assert.Equal(t, 1, info.Subscribers)
			} else {
				assert.Equal(t, 0, info.Subscribers)
			}
		}
	})

	t.Run("topic metrics accessor", func(t *testing.T) {
		srv := NewServer()
		assert.NotNil(t, srv.TopicMetrics())
		assert.Equal(t, 0, srv.TopicMetrics().TopicCount(""))
	})
}
