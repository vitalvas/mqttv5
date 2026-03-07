package mqttv5

import (
	"sync"
	"sync/atomic"
)

// TopicMetrics tracks per-topic message and subscription statistics.
type TopicMetrics struct {
	mu     sync.RWMutex
	topics map[string]*topicStats // namespaceKey(namespace, topic) -> stats
}

type topicStats struct {
	messagesIn  atomic.Int64
	messagesOut atomic.Int64
	bytesIn     atomic.Int64
	bytesOut    atomic.Int64
}

// TopicMetricsInfo represents statistics for a single topic.
type TopicMetricsInfo struct {
	Topic       string `json:"topic"`
	Namespace   string `json:"namespace"`
	MessagesIn  int64  `json:"messages_in"`
	MessagesOut int64  `json:"messages_out"`
	BytesIn     int64  `json:"bytes_in"`
	BytesOut    int64  `json:"bytes_out"`
	Subscribers int    `json:"subscribers"`
}

// NewTopicMetrics creates a new topic metrics tracker.
func NewTopicMetrics() *TopicMetrics {
	return &TopicMetrics{
		topics: make(map[string]*topicStats),
	}
}

func (m *TopicMetrics) getOrCreate(namespace, topic string) *topicStats {
	key := NamespaceKey(namespace, topic)

	m.mu.RLock()
	stats, ok := m.topics[key]
	m.mu.RUnlock()

	if ok {
		return stats
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock.
	if stats, ok = m.topics[key]; ok {
		return stats
	}

	stats = &topicStats{}
	m.topics[key] = stats
	return stats
}

// recordMessageIn records an incoming message on a topic.
func (m *TopicMetrics) recordMessageIn(namespace, topic string, bytes int) {
	stats := m.getOrCreate(namespace, topic)
	stats.messagesIn.Add(1)
	stats.bytesIn.Add(int64(bytes))
}

// recordMessageOut records an outgoing message delivery on a topic.
func (m *TopicMetrics) recordMessageOut(namespace, topic string, bytes int) {
	stats := m.getOrCreate(namespace, topic)
	stats.messagesOut.Add(1)
	stats.bytesOut.Add(int64(bytes))
}

// Get returns metrics for a specific topic.
func (m *TopicMetrics) Get(namespace, topic string) *TopicMetricsInfo {
	key := NamespaceKey(namespace, topic)

	m.mu.RLock()
	stats, ok := m.topics[key]
	m.mu.RUnlock()

	if !ok {
		return nil
	}

	return &TopicMetricsInfo{
		Topic:       topic,
		Namespace:   namespace,
		MessagesIn:  stats.messagesIn.Load(),
		MessagesOut: stats.messagesOut.Load(),
		BytesIn:     stats.bytesIn.Load(),
		BytesOut:    stats.bytesOut.Load(),
	}
}

// All returns metrics for all tracked topics, optionally filtered by namespace.
// If namespace is empty, returns all topics.
func (m *TopicMetrics) All(namespace string) []TopicMetricsInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]TopicMetricsInfo, 0, len(m.topics))
	for key, stats := range m.topics {
		ns, topic := ParseNamespaceKey(key)
		if namespace != "" && ns != namespace {
			continue
		}
		result = append(result, TopicMetricsInfo{
			Topic:       topic,
			Namespace:   ns,
			MessagesIn:  stats.messagesIn.Load(),
			MessagesOut: stats.messagesOut.Load(),
			BytesIn:     stats.bytesIn.Load(),
			BytesOut:    stats.bytesOut.Load(),
		})
	}
	return result
}

// TopicCount returns the number of tracked topics, optionally filtered by namespace.
func (m *TopicMetrics) TopicCount(namespace string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if namespace == "" {
		return len(m.topics)
	}

	count := 0
	for key := range m.topics {
		ns, _ := ParseNamespaceKey(key)
		if ns == namespace {
			count++
		}
	}
	return count
}

// Reset clears all topic metrics.
func (m *TopicMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topics = make(map[string]*topicStats)
}
