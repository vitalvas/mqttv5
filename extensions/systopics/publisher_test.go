package systopics

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

type mockBroker struct {
	mu                sync.Mutex
	clientCount       int
	namespaces        []string
	connections       int64
	connectionsTotal  int64
	maxConnections    int64
	subscriptions     int64
	retainedMessages  int64
	bytesReceived     int64
	bytesSent         int64
	messagesRecv      [3]int64
	messagesSent      [3]int64
	packetsRecv       map[mqttv5.PacketType]int64
	packetsSent       map[mqttv5.PacketType]int64
	topicCount        int64
	startedAt         time.Time
	nsClientCounts    map[string]int
	published         []*mqttv5.Message
	publishErr        error
	mockSubscriptions map[string][]string // namespace -> list of topic filter prefixes
}

func newMockBroker() *mockBroker {
	return &mockBroker{
		startedAt:      time.Now().Add(-60 * time.Second),
		nsClientCounts: make(map[string]int),
	}
}

func (m *mockBroker) ClientCount(namespaces ...string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(namespaces) > 0 {
		total := 0
		for _, ns := range namespaces {
			total += m.nsClientCounts[ns]
		}

		return total
	}

	return m.clientCount
}

func (m *mockBroker) Namespaces() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.namespaces
}

func (m *mockBroker) Connections() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.connections
}

func (m *mockBroker) ConnectionsTotal() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.connectionsTotal
}

func (m *mockBroker) MaxConnections() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.maxConnections
}

func (m *mockBroker) Subscriptions() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.subscriptions
}

func (m *mockBroker) RetainedMessages() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.retainedMessages
}

func (m *mockBroker) TotalBytesReceived() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.bytesReceived
}

func (m *mockBroker) TotalBytesSent() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.bytesSent
}

func (m *mockBroker) TotalMessagesReceived(qos byte) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if qos > 2 {
		qos = 2
	}

	return m.messagesRecv[qos]
}

func (m *mockBroker) TotalMessagesSent(qos byte) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if qos > 2 {
		qos = 2
	}

	return m.messagesSent[qos]
}

func (m *mockBroker) PacketsReceived(pt mqttv5.PacketType) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.packetsRecv == nil {
		return 0
	}

	return m.packetsRecv[pt]
}

func (m *mockBroker) PacketsSent(pt mqttv5.PacketType) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.packetsSent == nil {
		return 0
	}

	return m.packetsSent[pt]
}

func (m *mockBroker) TopicCount() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.topicCount
}

func (m *mockBroker) StartedAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.startedAt
}

func (m *mockBroker) HasSubscribersWithPrefix(namespace, prefix string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.mockSubscriptions == nil {
		return true
	}

	for _, p := range m.mockSubscriptions[namespace] {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}

	return false
}

func (m *mockBroker) Publish(msg *mqttv5.Message) error {
	m.mu.Lock()
	m.published = append(m.published, msg)
	err := m.publishErr
	m.mu.Unlock()

	return err
}

func (m *mockBroker) getMessages() []*mqttv5.Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*mqttv5.Message, len(m.published))
	copy(result, m.published)

	return result
}

func (m *mockBroker) findByTopic(topic string) *mqttv5.Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := len(m.published) - 1; i >= 0; i-- {
		if m.published[i].Topic == topic {
			return m.published[i]
		}
	}

	return nil
}

func (m *mockBroker) findByTopicAndNamespace(topic, namespace string) *mqttv5.Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := len(m.published) - 1; i >= 0; i-- {
		if m.published[i].Topic == topic && m.published[i].Namespace == namespace {
			return m.published[i]
		}
	}

	return nil
}

func TestPublisher(t *testing.T) {
	t.Run("no groups enabled publishes nothing", func(t *testing.T) {
		broker := newMockBroker()
		broker.connections = 5

		p := New(broker, WithInterval(50*time.Millisecond))

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		messages := broker.getMessages()
		assert.Empty(t, messages)
	})

	t.Run("all groups publishes all standard topics", func(t *testing.T) {
		broker := newMockBroker()
		broker.connections = 5
		broker.connectionsTotal = 10
		broker.maxConnections = 8
		broker.subscriptions = 3
		broker.retainedMessages = 2
		broker.bytesReceived = 1000
		broker.bytesSent = 2000
		broker.messagesRecv = [3]int64{10, 5, 1}
		broker.messagesSent = [3]int64{20, 10, 2}

		p := New(broker,
			WithVersion("test 1.0"),
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupAll),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		expectedTopics := []string{
			TopicVersion,
			TopicUptime,
			TopicTimestamp,
			TopicClientsConnected,
			TopicClientsTotal,
			TopicClientsMaximum,
			TopicClientsDisconnected,
			TopicMessagesReceived,
			TopicMessagesSent,
			TopicMessagesStored,
			TopicMessagesPublishRecv,
			TopicMessagesPublishSent,
			TopicBytesReceived,
			TopicBytesSent,
			TopicSubscriptionsCount,
			TopicRetainedMessagesCount,
			TopicLoadMessagesRecv1min,
			TopicLoadMessagesRecv5min,
			TopicLoadMessagesRecv15min,
			TopicLoadMessagesSent1min,
			TopicLoadMessagesSent5min,
			TopicLoadMessagesSent15min,
			TopicLoadBytesRecv1min,
			TopicLoadBytesRecv5min,
			TopicLoadBytesRecv15min,
			TopicLoadBytesSent1min,
			TopicLoadBytesSent5min,
			TopicLoadBytesSent15min,
			TopicLoadConnections1min,
			TopicLoadConnections5min,
			TopicLoadConnections15min,
		}

		for _, topic := range expectedTopics {
			msg := broker.findByTopic(topic)
			require.NotNilf(t, msg, "topic %s not published", topic)
			assert.True(t, msg.Retain, "topic %s should be retained", topic)
		}
	})

	t.Run("correct values published", func(t *testing.T) {
		broker := newMockBroker()
		broker.connections = 5
		broker.connectionsTotal = 10
		broker.maxConnections = 8
		broker.subscriptions = 3
		broker.retainedMessages = 2
		broker.bytesReceived = 1000
		broker.bytesSent = 2000
		broker.messagesRecv = [3]int64{10, 5, 1}
		broker.messagesSent = [3]int64{20, 10, 2}

		p := New(broker,
			WithVersion("test 1.0"),
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupAll),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		assertions := map[string]string{
			TopicVersion:               "test 1.0",
			TopicClientsConnected:      "5",
			TopicClientsTotal:          "10",
			TopicClientsMaximum:        "8",
			TopicClientsDisconnected:   "5",
			TopicMessagesReceived:      "16",
			TopicMessagesSent:          "32",
			TopicMessagesStored:        "2",
			TopicBytesReceived:         "1000",
			TopicBytesSent:             "2000",
			TopicSubscriptionsCount:    "3",
			TopicRetainedMessagesCount: "2",
		}

		for topic, expected := range assertions {
			msg := broker.findByTopic(topic)
			require.NotNilf(t, msg, "topic %s not published", topic)
			assert.Equalf(t, expected, string(msg.Payload), "topic %s", topic)
		}
	})

	t.Run("only broker info group", func(t *testing.T) {
		broker := newMockBroker()
		broker.connections = 5

		p := New(broker,
			WithVersion("v1"),
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupBrokerInfo),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		require.NotNil(t, broker.findByTopic(TopicVersion))
		require.NotNil(t, broker.findByTopic(TopicUptime))
		require.NotNil(t, broker.findByTopic(TopicTimestamp))

		assert.Nil(t, broker.findByTopic(TopicClientsConnected))
		assert.Nil(t, broker.findByTopic(TopicMessagesReceived))
		assert.Nil(t, broker.findByTopic(TopicBytesReceived))
		assert.Nil(t, broker.findByTopic(TopicSubscriptionsCount))
		assert.Nil(t, broker.findByTopic(TopicRetainedMessagesCount))
		assert.Nil(t, broker.findByTopic(TopicLoadMessagesRecv1min))
	})

	t.Run("only clients group", func(t *testing.T) {
		broker := newMockBroker()
		broker.connections = 5
		broker.connectionsTotal = 10
		broker.maxConnections = 8

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupClients),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		require.NotNil(t, broker.findByTopic(TopicClientsConnected))
		require.NotNil(t, broker.findByTopic(TopicClientsTotal))
		require.NotNil(t, broker.findByTopic(TopicClientsMaximum))
		require.NotNil(t, broker.findByTopic(TopicClientsDisconnected))

		assert.Nil(t, broker.findByTopic(TopicVersion))
		assert.Nil(t, broker.findByTopic(TopicMessagesReceived))
	})

	t.Run("multiple groups", func(t *testing.T) {
		broker := newMockBroker()
		broker.bytesReceived = 100
		broker.subscriptions = 5

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupBytes, TopicGroupSubscriptions),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		require.NotNil(t, broker.findByTopic(TopicBytesReceived))
		require.NotNil(t, broker.findByTopic(TopicBytesSent))
		require.NotNil(t, broker.findByTopic(TopicSubscriptionsCount))

		assert.Nil(t, broker.findByTopic(TopicVersion))
		assert.Nil(t, broker.findByTopic(TopicClientsConnected))
		assert.Nil(t, broker.findByTopic(TopicMessagesReceived))
		assert.Nil(t, broker.findByTopic(TopicRetainedMessagesCount))
		assert.Nil(t, broker.findByTopic(TopicLoadMessagesRecv1min))
	})

	t.Run("namespace group disabled hides namespace topics", func(t *testing.T) {
		broker := newMockBroker()
		broker.namespaces = []string{"tenant1"}
		broker.nsClientCounts = map[string]int{"tenant1": 3}

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupClients),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		require.NotNil(t, broker.findByTopic(TopicClientsConnected))
		assert.Nil(t, broker.findByTopic("$SYS/broker/clients/namespace/tenant1/connected"))
	})

	t.Run("uptime is reasonable", func(t *testing.T) {
		broker := newMockBroker()
		broker.startedAt = time.Now().Add(-120 * time.Second)

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupBrokerInfo),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		msg := broker.findByTopic(TopicUptime)
		require.NotNil(t, msg)

		uptime, err := strconv.ParseInt(string(msg.Payload), 10, 64)
		require.NoError(t, err)
		assert.InDelta(t, 120, uptime, 2)
	})

	t.Run("global namespace mode", func(t *testing.T) {
		broker := newMockBroker()
		broker.namespaces = []string{"tenant1", "tenant2"}
		broker.nsClientCounts = map[string]int{"tenant1": 3, "tenant2": 7}

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupNamespace),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		msg1 := broker.findByTopic("$SYS/broker/clients/namespace/tenant1/connected")
		require.NotNil(t, msg1)
		assert.Equal(t, "3", string(msg1.Payload))

		msg2 := broker.findByTopic("$SYS/broker/clients/namespace/tenant2/connected")
		require.NotNil(t, msg2)
		assert.Equal(t, "7", string(msg2.Payload))
	})

	t.Run("isolated namespace mode", func(t *testing.T) {
		broker := newMockBroker()
		broker.namespaces = []string{"ns1"}
		broker.nsClientCounts = map[string]int{"ns1": 5}

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithNamespaceMode(NamespaceModeIsolated),
			WithTopicGroups(TopicGroupNamespace),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		msg := broker.findByTopicAndNamespace(TopicClientsConnected, "ns1")
		require.NotNil(t, msg)
		assert.Equal(t, "5", string(msg.Payload))
		assert.Equal(t, "ns1", msg.Namespace)

		globalMsg := broker.findByTopic("$SYS/broker/clients/namespace/ns1/connected")
		assert.Nil(t, globalMsg)

		nsMsg := broker.findByTopic("$SYS/broker/namespace/ns1/clients/connected")
		assert.Nil(t, nsMsg)
	})

	t.Run("isolated mode skips namespaces without sys subscribers", func(t *testing.T) {
		broker := newMockBroker()
		broker.namespaces = []string{"ns1", "ns2"}
		broker.nsClientCounts = map[string]int{"ns1": 5, "ns2": 3}
		broker.mockSubscriptions = map[string][]string{"ns1": {"$SYS/"}}

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithNamespaceMode(NamespaceModeIsolated),
			WithTopicGroups(TopicGroupNamespace),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		msg := broker.findByTopicAndNamespace(TopicClientsConnected, "ns1")
		require.NotNil(t, msg)
		assert.Equal(t, "5", string(msg.Payload))

		assert.Nil(t, broker.findByTopicAndNamespace(TopicClientsConnected, "ns2"))
	})

	t.Run("namespace count skipped for namespaces without sys subscribers", func(t *testing.T) {
		broker := newMockBroker()
		broker.namespaces = []string{"ns1", "ns2"}
		broker.mockSubscriptions = map[string][]string{"ns1": {"$SYS/"}}

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupNamespace),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		msg := broker.findByTopicAndNamespace(TopicNamespacesCount, "ns1")
		require.NotNil(t, msg)
		assert.Equal(t, "2", string(msg.Payload))

		assert.Nil(t, broker.findByTopicAndNamespace(TopicNamespacesCount, "ns2"))
	})

	t.Run("no namespaces produces no namespace topics", func(t *testing.T) {
		broker := newMockBroker()

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupNamespace),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		messages := broker.getMessages()
		for _, msg := range messages {
			assert.NotContains(t, msg.Topic, "namespace/")
		}
	})

	t.Run("publishes on each tick", func(t *testing.T) {
		broker := newMockBroker()

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupBrokerInfo),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		messages := broker.getMessages()

		count := 0
		for _, msg := range messages {
			if msg.Topic == TopicVersion {
				count++
			}
		}

		assert.GreaterOrEqual(t, count, 3)
	})

	t.Run("context cancellation stops publisher", func(t *testing.T) {
		broker := newMockBroker()
		p := New(broker, WithInterval(time.Hour))

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		go func() {
			p.Run(ctx)
			close(done)
		}()

		cancel()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("publisher did not stop after context cancellation")
		}
	})

	t.Run("disconnected clients cannot be negative", func(t *testing.T) {
		broker := newMockBroker()
		broker.connections = 10
		broker.connectionsTotal = 5

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupClients),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		msg := broker.findByTopic(TopicClientsDisconnected)
		require.NotNil(t, msg)
		assert.Equal(t, "0", string(msg.Payload))
	})

	t.Run("load rates update across ticks", func(t *testing.T) {
		broker := newMockBroker()

		p := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupLoad),
		)

		broker.mu.Lock()
		broker.messagesRecv = [3]int64{100, 0, 0}
		broker.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		p.Run(ctx)

		msg := broker.findByTopic(TopicLoadMessagesRecv1min)
		require.NotNil(t, msg)

		val, err := strconv.ParseFloat(string(msg.Payload), 64)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, val, 0.0)
	})
}

func TestIsolatedModeNoDoublePublish(t *testing.T) {
	broker := newMockBroker()
	broker.connections = 10
	broker.connectionsTotal = 15
	broker.maxConnections = 12
	broker.namespaces = []string{"ns1"}
	broker.nsClientCounts["ns1"] = 5

	pub := New(broker,
		WithInterval(50*time.Millisecond),
		WithNamespaceMode(NamespaceModeIsolated),
		WithTopicGroups(TopicGroupClients, TopicGroupNamespace),
	)

	pub.publish()

	msgs := broker.getMessages()

	var connectedCount int
	for _, m := range msgs {
		if m.Topic == TopicClientsConnected {
			connectedCount++
		}
	}

	// Only the isolated namespace publish should emit TopicClientsConnected.
	assert.Equal(t, 1, connectedCount)

	// The single message should be the namespace-scoped one.
	msg := broker.findByTopicAndNamespace(TopicClientsConnected, "ns1")
	require.NotNil(t, msg)
	assert.Equal(t, "5", string(msg.Payload))

	// Other client topics should still be published globally.
	assert.NotNil(t, broker.findByTopic(TopicClientsTotal))
	assert.NotNil(t, broker.findByTopic(TopicClientsMaximum))
	assert.NotNil(t, broker.findByTopic(TopicClientsDisconnected))
}

func TestIsolatedModeClientsWithoutNamespaceGroup(t *testing.T) {
	broker := newMockBroker()
	broker.connections = 10
	broker.connectionsTotal = 15
	broker.maxConnections = 12

	pub := New(broker,
		WithInterval(50*time.Millisecond),
		WithNamespaceMode(NamespaceModeIsolated),
		WithTopicGroups(TopicGroupClients),
	)

	pub.publish()

	// Without TopicGroupNamespace, TopicClientsConnected should be published normally.
	msg := broker.findByTopic(TopicClientsConnected)
	require.NotNil(t, msg)
	assert.Equal(t, "10", string(msg.Payload))
}

func TestPacketsGroup(t *testing.T) {
	t.Run("publishes packet counters", func(t *testing.T) {
		broker := newMockBroker()
		broker.packetsRecv = map[mqttv5.PacketType]int64{
			mqttv5.PacketCONNECT:   100,
			mqttv5.PacketSUBSCRIBE: 50,
		}
		broker.packetsSent = map[mqttv5.PacketType]int64{
			mqttv5.PacketCONNACK: 100,
			mqttv5.PacketSUBACK:  50,
		}

		pub := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupPackets),
		)

		pub.publish()

		msg := broker.findByTopic(fmt.Sprintf("%sconnect", TopicPacketsReceivedPrefix))
		require.NotNil(t, msg)
		assert.Equal(t, "100", string(msg.Payload))

		msg = broker.findByTopic(fmt.Sprintf("%sconnack", TopicPacketsSentPrefix))
		require.NotNil(t, msg)
		assert.Equal(t, "100", string(msg.Payload))

		msg = broker.findByTopic(fmt.Sprintf("%ssubscribe", TopicPacketsReceivedPrefix))
		require.NotNil(t, msg)
		assert.Equal(t, "50", string(msg.Payload))

		msg = broker.findByTopic(fmt.Sprintf("%ssuback", TopicPacketsSentPrefix))
		require.NotNil(t, msg)
		assert.Equal(t, "50", string(msg.Payload))

		// Zero counters still published.
		msg = broker.findByTopic(fmt.Sprintf("%spublish", TopicPacketsReceivedPrefix))
		require.NotNil(t, msg)
		assert.Equal(t, "0", string(msg.Payload))
	})

	t.Run("not published when disabled", func(t *testing.T) {
		broker := newMockBroker()
		broker.packetsRecv = map[mqttv5.PacketType]int64{mqttv5.PacketCONNECT: 10}

		pub := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupBrokerInfo),
		)

		pub.publish()

		assert.Nil(t, broker.findByTopic(fmt.Sprintf("%sconnect", TopicPacketsReceivedPrefix)))
	})
}

func TestTopicsGroup(t *testing.T) {
	t.Run("publishes topic count", func(t *testing.T) {
		broker := newMockBroker()
		broker.topicCount = 42

		pub := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupTopics),
		)

		pub.publish()

		msg := broker.findByTopic(TopicTopicsCount)
		require.NotNil(t, msg)
		assert.Equal(t, "42", string(msg.Payload))
	})

	t.Run("not published when disabled", func(t *testing.T) {
		broker := newMockBroker()
		broker.topicCount = 42

		pub := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupBrokerInfo),
		)

		pub.publish()

		assert.Nil(t, broker.findByTopic(TopicTopicsCount))
	})

	t.Run("excluded from TopicGroupAll", func(t *testing.T) {
		broker := newMockBroker()
		broker.topicCount = 42

		pub := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupAll),
		)

		pub.publish()

		assert.Nil(t, broker.findByTopic(TopicTopicsCount))
	})

	t.Run("explicit enable with TopicGroupAll", func(t *testing.T) {
		broker := newMockBroker()
		broker.topicCount = 42

		pub := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupAll, TopicGroupTopics),
		)

		pub.publish()

		msg := broker.findByTopic(TopicTopicsCount)
		require.NotNil(t, msg)
		assert.Equal(t, "42", string(msg.Payload))
	})
}

func TestTopicGroupAllIncludesPackets(t *testing.T) {
	cfg := config{groups: make(map[TopicGroup]bool)}
	WithTopicGroups(TopicGroupAll)(&cfg)

	assert.True(t, cfg.groups[TopicGroupPackets])
	assert.False(t, cfg.groups[TopicGroupTopics])
}

func TestPublishErrors(t *testing.T) {
	t.Run("callback_receives_errors", func(t *testing.T) {
		broker := newMockBroker()
		publishErr := errors.New("retain not supported")
		broker.publishErr = publishErr

		var topics []string
		var errs []error

		pub := New(broker,
			WithTopicGroups(TopicGroupBrokerInfo),
			WithOnPublishError(func(topic string, err error) {
				topics = append(topics, topic)
				errs = append(errs, err)
			}),
		)

		pub.publish()

		assert.Len(t, topics, 3)
		assert.Contains(t, topics, TopicVersion)
		assert.Contains(t, topics, TopicUptime)
		assert.Contains(t, topics, TopicTimestamp)

		for _, err := range errs {
			assert.Equal(t, publishErr, err)
		}
	})

	t.Run("no_callback_does_not_panic", func(t *testing.T) {
		broker := newMockBroker()
		broker.publishErr = errors.New("fail")

		pub := New(broker, WithTopicGroups(TopicGroupBrokerInfo))

		assert.NotPanics(t, func() {
			pub.publish()
		})
	})

	t.Run("namespace_publish_error_global", func(t *testing.T) {
		broker := newMockBroker()
		broker.namespaces = []string{"ns1"}
		broker.nsClientCounts["ns1"] = 5
		broker.publishErr = errors.New("fail")

		var gotTopic string

		pub := New(broker,
			WithTopicGroups(TopicGroupNamespace),
			WithOnPublishError(func(topic string, _ error) {
				gotTopic = topic
			}),
		)

		pub.publish()

		assert.Equal(t, "$SYS/broker/clients/namespace/ns1/connected", gotTopic)
	})

	t.Run("namespace_publish_error_isolated", func(t *testing.T) {
		broker := newMockBroker()
		broker.namespaces = []string{"ns1"}
		broker.nsClientCounts["ns1"] = 3
		broker.publishErr = errors.New("fail")

		var gotTopic string

		pub := New(broker,
			WithNamespaceMode(NamespaceModeIsolated),
			WithTopicGroups(TopicGroupNamespace),
			WithOnPublishError(func(topic string, _ error) {
				gotTopic = topic
			}),
		)

		pub.publish()

		assert.Equal(t, TopicClientsConnected, gotTopic)
	})
}

func TestGlobalTopicsPublishedToAllNamespaces(t *testing.T) {
	t.Run("topics reach every namespace", func(t *testing.T) {
		broker := newMockBroker()
		broker.namespaces = []string{"ns1", "ns2"}
		broker.connections = 5
		broker.connectionsTotal = 10
		broker.maxConnections = 8

		pub := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupClients),
		)

		pub.publish()

		for _, ns := range broker.namespaces {
			msg := broker.findByTopicAndNamespace(TopicClientsConnected, ns)
			require.NotNilf(t, msg, "TopicClientsConnected not published to namespace %s", ns)
			assert.Equal(t, "5", string(msg.Payload))

			msg = broker.findByTopicAndNamespace(TopicClientsTotal, ns)
			require.NotNilf(t, msg, "TopicClientsTotal not published to namespace %s", ns)
		}
	})

	t.Run("skips namespaces without sys subscribers", func(t *testing.T) {
		broker := newMockBroker()
		broker.namespaces = []string{"ns1", "ns2", "ns3"}
		broker.connections = 5
		broker.connectionsTotal = 10
		broker.maxConnections = 8
		broker.mockSubscriptions = map[string][]string{"ns1": {"$SYS/"}, "ns3": {"$SYS/"}}

		pub := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupClients),
		)

		pub.publish()

		// ns1 and ns3 should receive topics
		require.NotNil(t, broker.findByTopicAndNamespace(TopicClientsConnected, "ns1"))
		require.NotNil(t, broker.findByTopicAndNamespace(TopicClientsConnected, "ns3"))

		// ns2 has no $SYS subscribers, should be skipped
		assert.Nil(t, broker.findByTopicAndNamespace(TopicClientsConnected, "ns2"))
	})

	t.Run("no namespaces publishes with empty namespace", func(t *testing.T) {
		broker := newMockBroker()
		broker.connections = 3

		pub := New(broker,
			WithInterval(50*time.Millisecond),
			WithTopicGroups(TopicGroupClients),
		)

		pub.publish()

		msg := broker.findByTopicAndNamespace(TopicClientsConnected, "")
		require.NotNil(t, msg)
		assert.Equal(t, "3", string(msg.Payload))
	})
}
