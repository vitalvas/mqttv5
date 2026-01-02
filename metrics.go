package mqttv5

import (
	"expvar"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Standard metric names for MQTT brokers.
const (
	// MetricConnections is the current number of active connections.
	MetricConnections = "mqtt_connections"

	// MetricConnectionsTotal is the total number of connections.
	MetricConnectionsTotal = "mqtt_connections_total"

	// MetricMessagesReceived is the total number of messages received.
	MetricMessagesReceived = "mqtt_messages_received_total"

	// MetricMessagesSent is the total number of messages sent.
	MetricMessagesSent = "mqtt_messages_sent_total"

	// MetricBytesReceived is the total bytes received.
	MetricBytesReceived = "mqtt_bytes_received_total"

	// MetricBytesSent is the total bytes sent.
	MetricBytesSent = "mqtt_bytes_sent_total"

	// MetricSubscriptions is the current number of subscriptions.
	MetricSubscriptions = "mqtt_subscriptions"

	// MetricRetainedMessages is the current number of retained messages.
	MetricRetainedMessages = "mqtt_retained_messages"

	// MetricPacketsSent is the total number of packets sent.
	MetricPacketsSent = "mqtt_packets_sent_total"

	// MetricPacketsReceived is the total number of packets received.
	MetricPacketsReceived = "mqtt_packets_received_total"

	// MetricPublishLatencyCount is the number of publish latency observations.
	MetricPublishLatencyCount = "mqtt_publish_latency_count"

	// MetricPublishLatencySum is the sum of publish latencies in seconds.
	MetricPublishLatencySum = "mqtt_publish_latency_seconds_sum"
)

// Metrics provides broker metrics using expvar.
type Metrics struct {
	connections      *expvar.Int
	connectionsTotal *expvar.Int
	subscriptions    *expvar.Int
	retainedMessages *expvar.Int
	bytesReceived    *expvar.Int
	bytesSent        *expvar.Int
	latencyCount     *expvar.Int
	latencySum       *expvar.Float

	// Maps for labeled metrics
	mu               sync.RWMutex
	messagesReceived map[byte]*expvar.Int
	messagesSent     map[byte]*expvar.Int
	packetsReceived  map[PacketType]*expvar.Int
	packetsSent      map[PacketType]*expvar.Int
}

// NewMetrics creates a new Metrics instance using expvar.
func NewMetrics() *Metrics {
	m := &Metrics{
		connections:      expvar.NewInt(MetricConnections),
		connectionsTotal: expvar.NewInt(MetricConnectionsTotal),
		subscriptions:    expvar.NewInt(MetricSubscriptions),
		retainedMessages: expvar.NewInt(MetricRetainedMessages),
		bytesReceived:    expvar.NewInt(MetricBytesReceived),
		bytesSent:        expvar.NewInt(MetricBytesSent),
		latencyCount:     expvar.NewInt(MetricPublishLatencyCount),
		latencySum:       expvar.NewFloat(MetricPublishLatencySum),
		messagesReceived: make(map[byte]*expvar.Int),
		messagesSent:     make(map[byte]*expvar.Int),
		packetsReceived:  make(map[PacketType]*expvar.Int),
		packetsSent:      make(map[PacketType]*expvar.Int),
	}

	// Pre-initialize QoS counters
	for qos := byte(0); qos <= 2; qos++ {
		m.messagesReceived[qos] = expvar.NewInt(fmt.Sprintf("%s_qos%d", MetricMessagesReceived, qos))
		m.messagesSent[qos] = expvar.NewInt(fmt.Sprintf("%s_qos%d", MetricMessagesSent, qos))
	}

	return m
}

// ConnectionOpened records a new connection.
func (m *Metrics) ConnectionOpened() {
	m.connections.Add(1)
	m.connectionsTotal.Add(1)
}

// ConnectionClosed records a closed connection.
func (m *Metrics) ConnectionClosed() {
	m.connections.Add(-1)
}

// Connections returns the current connection count.
func (m *Metrics) Connections() int64 {
	return m.connections.Value()
}

// ConnectionsTotal returns the total connection count.
func (m *Metrics) ConnectionsTotal() int64 {
	return m.connectionsTotal.Value()
}

// MessageReceived records a received message.
func (m *Metrics) MessageReceived(qos byte) {
	if qos > 2 {
		qos = 2
	}
	m.messagesReceived[qos].Add(1)
}

// MessageSent records a sent message.
func (m *Metrics) MessageSent(qos byte) {
	if qos > 2 {
		qos = 2
	}
	m.messagesSent[qos].Add(1)
}

// BytesReceived records received bytes.
func (m *Metrics) BytesReceived(n int) {
	m.bytesReceived.Add(int64(n))
}

// BytesSent records sent bytes.
func (m *Metrics) BytesSent(n int) {
	m.bytesSent.Add(int64(n))
}

// SubscriptionAdded records a new subscription.
func (m *Metrics) SubscriptionAdded() {
	m.subscriptions.Add(1)
}

// SubscriptionRemoved records a removed subscription.
func (m *Metrics) SubscriptionRemoved() {
	m.subscriptions.Add(-1)
}

// Subscriptions returns the current subscription count.
func (m *Metrics) Subscriptions() int64 {
	return m.subscriptions.Value()
}

// RetainedMessageSet records a retained message being set.
func (m *Metrics) RetainedMessageSet() {
	m.retainedMessages.Add(1)
}

// RetainedMessageRemoved records a retained message being removed.
func (m *Metrics) RetainedMessageRemoved() {
	m.retainedMessages.Add(-1)
}

// RetainedMessages returns the current retained message count.
func (m *Metrics) RetainedMessages() int64 {
	return m.retainedMessages.Value()
}

// PublishLatency records publish processing latency.
func (m *Metrics) PublishLatency(d time.Duration) {
	m.latencyCount.Add(1)
	m.latencySum.Add(d.Seconds())
}

// PacketReceived records a received packet.
func (m *Metrics) PacketReceived(packetType PacketType) {
	m.mu.RLock()
	counter, ok := m.packetsReceived[packetType]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		if counter, ok = m.packetsReceived[packetType]; !ok {
			counter = expvar.NewInt(fmt.Sprintf("%s_%s", MetricPacketsReceived, packetType.String()))
			m.packetsReceived[packetType] = counter
		}
		m.mu.Unlock()
	}

	counter.Add(1)
}

// PacketSent records a sent packet.
func (m *Metrics) PacketSent(packetType PacketType) {
	m.mu.RLock()
	counter, ok := m.packetsSent[packetType]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		if counter, ok = m.packetsSent[packetType]; !ok {
			counter = expvar.NewInt(fmt.Sprintf("%s_%s", MetricPacketsSent, packetType.String()))
			m.packetsSent[packetType] = counter
		}
		m.mu.Unlock()
	}

	counter.Add(1)
}

// NoOpMetrics is a no-op implementation for when metrics are disabled.
type NoOpMetrics struct{}

// ConnectionOpened does nothing.
func (n *NoOpMetrics) ConnectionOpened() {}

// ConnectionClosed does nothing.
func (n *NoOpMetrics) ConnectionClosed() {}

// MessageReceived does nothing.
func (n *NoOpMetrics) MessageReceived(_ byte) {}

// MessageSent does nothing.
func (n *NoOpMetrics) MessageSent(_ byte) {}

// BytesReceived does nothing.
func (n *NoOpMetrics) BytesReceived(_ int) {}

// BytesSent does nothing.
func (n *NoOpMetrics) BytesSent(_ int) {}

// SubscriptionAdded does nothing.
func (n *NoOpMetrics) SubscriptionAdded() {}

// SubscriptionRemoved does nothing.
func (n *NoOpMetrics) SubscriptionRemoved() {}

// RetainedMessageSet does nothing.
func (n *NoOpMetrics) RetainedMessageSet() {}

// RetainedMessageRemoved does nothing.
func (n *NoOpMetrics) RetainedMessageRemoved() {}

// PublishLatency does nothing.
func (n *NoOpMetrics) PublishLatency(_ time.Duration) {}

// PacketReceived does nothing.
func (n *NoOpMetrics) PacketReceived(_ PacketType) {}

// PacketSent does nothing.
func (n *NoOpMetrics) PacketSent(_ PacketType) {}

// MetricsCollector defines the interface for metrics collection.
type MetricsCollector interface {
	ConnectionOpened()
	ConnectionClosed()
	MessageReceived(qos byte)
	MessageSent(qos byte)
	BytesReceived(n int)
	BytesSent(n int)
	SubscriptionAdded()
	SubscriptionRemoved()
	RetainedMessageSet()
	RetainedMessageRemoved()
	PublishLatency(d time.Duration)
	PacketReceived(packetType PacketType)
	PacketSent(packetType PacketType)
}

// MemoryMetrics is an in-memory implementation for testing without expvar side effects.
type MemoryMetrics struct {
	connections      atomic.Int64
	connectionsTotal atomic.Int64
	subscriptions    atomic.Int64
	retainedMessages atomic.Int64
	bytesReceived    atomic.Int64
	bytesSent        atomic.Int64
	latencyCount     atomic.Int64
	latencySum       atomic.Uint64

	mu               sync.RWMutex
	messagesReceived map[byte]*atomic.Int64
	messagesSent     map[byte]*atomic.Int64
	packetsReceived  map[PacketType]*atomic.Int64
	packetsSent      map[PacketType]*atomic.Int64
}

// NewMemoryMetrics creates a new in-memory metrics instance for testing.
func NewMemoryMetrics() *MemoryMetrics {
	m := &MemoryMetrics{
		messagesReceived: make(map[byte]*atomic.Int64),
		messagesSent:     make(map[byte]*atomic.Int64),
		packetsReceived:  make(map[PacketType]*atomic.Int64),
		packetsSent:      make(map[PacketType]*atomic.Int64),
	}

	for qos := byte(0); qos <= 2; qos++ {
		m.messagesReceived[qos] = &atomic.Int64{}
		m.messagesSent[qos] = &atomic.Int64{}
	}

	return m
}

// ConnectionOpened records a new connection.
func (m *MemoryMetrics) ConnectionOpened() {
	m.connections.Add(1)
	m.connectionsTotal.Add(1)
}

// ConnectionClosed records a closed connection.
func (m *MemoryMetrics) ConnectionClosed() {
	m.connections.Add(-1)
}

// Connections returns the current connection count.
func (m *MemoryMetrics) Connections() int64 {
	return m.connections.Load()
}

// ConnectionsTotal returns the total connection count.
func (m *MemoryMetrics) ConnectionsTotal() int64 {
	return m.connectionsTotal.Load()
}

// MessageReceived records a received message.
func (m *MemoryMetrics) MessageReceived(qos byte) {
	if qos > 2 {
		qos = 2
	}
	m.messagesReceived[qos].Add(1)
}

// MessagesReceived returns the message count for a QoS level.
func (m *MemoryMetrics) MessagesReceived(qos byte) int64 {
	if qos > 2 {
		qos = 2
	}
	return m.messagesReceived[qos].Load()
}

// MessageSent records a sent message.
func (m *MemoryMetrics) MessageSent(qos byte) {
	if qos > 2 {
		qos = 2
	}
	m.messagesSent[qos].Add(1)
}

// MessagesSent returns the message count for a QoS level.
func (m *MemoryMetrics) MessagesSent(qos byte) int64 {
	if qos > 2 {
		qos = 2
	}
	return m.messagesSent[qos].Load()
}

// BytesReceived records received bytes.
func (m *MemoryMetrics) BytesReceived(n int) {
	m.bytesReceived.Add(int64(n))
}

// TotalBytesReceived returns total bytes received.
func (m *MemoryMetrics) TotalBytesReceived() int64 {
	return m.bytesReceived.Load()
}

// BytesSent records sent bytes.
func (m *MemoryMetrics) BytesSent(n int) {
	m.bytesSent.Add(int64(n))
}

// TotalBytesSent returns total bytes sent.
func (m *MemoryMetrics) TotalBytesSent() int64 {
	return m.bytesSent.Load()
}

// SubscriptionAdded records a new subscription.
func (m *MemoryMetrics) SubscriptionAdded() {
	m.subscriptions.Add(1)
}

// SubscriptionRemoved records a removed subscription.
func (m *MemoryMetrics) SubscriptionRemoved() {
	m.subscriptions.Add(-1)
}

// Subscriptions returns the current subscription count.
func (m *MemoryMetrics) Subscriptions() int64 {
	return m.subscriptions.Load()
}

// RetainedMessageSet records a retained message being set.
func (m *MemoryMetrics) RetainedMessageSet() {
	m.retainedMessages.Add(1)
}

// RetainedMessageRemoved records a retained message being removed.
func (m *MemoryMetrics) RetainedMessageRemoved() {
	m.retainedMessages.Add(-1)
}

// RetainedMessages returns the current retained message count.
func (m *MemoryMetrics) RetainedMessages() int64 {
	return m.retainedMessages.Load()
}

// PublishLatency records publish processing latency.
func (m *MemoryMetrics) PublishLatency(d time.Duration) {
	m.latencyCount.Add(1)
	for {
		old := m.latencySum.Load()
		newSum := float64FromBits(old) + d.Seconds()
		if m.latencySum.CompareAndSwap(old, float64ToBits(newSum)) {
			break
		}
	}
}

// LatencyCount returns the number of latency observations.
func (m *MemoryMetrics) LatencyCount() int64 {
	return m.latencyCount.Load()
}

// LatencySum returns the sum of latencies in seconds.
func (m *MemoryMetrics) LatencySum() float64 {
	return float64FromBits(m.latencySum.Load())
}

// PacketReceived records a received packet.
func (m *MemoryMetrics) PacketReceived(packetType PacketType) {
	m.mu.RLock()
	counter, ok := m.packetsReceived[packetType]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		if counter, ok = m.packetsReceived[packetType]; !ok {
			counter = &atomic.Int64{}
			m.packetsReceived[packetType] = counter
		}
		m.mu.Unlock()
	}

	counter.Add(1)
}

// PacketsReceived returns the packet count for a type.
func (m *MemoryMetrics) PacketsReceived(packetType PacketType) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if counter, ok := m.packetsReceived[packetType]; ok {
		return counter.Load()
	}
	return 0
}

// PacketSent records a sent packet.
func (m *MemoryMetrics) PacketSent(packetType PacketType) {
	m.mu.RLock()
	counter, ok := m.packetsSent[packetType]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		if counter, ok = m.packetsSent[packetType]; !ok {
			counter = &atomic.Int64{}
			m.packetsSent[packetType] = counter
		}
		m.mu.Unlock()
	}

	counter.Add(1)
}

// PacketsSent returns the packet count for a type.
func (m *MemoryMetrics) PacketsSent(packetType PacketType) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if counter, ok := m.packetsSent[packetType]; ok {
		return counter.Load()
	}
	return 0
}

// float64ToBits converts a float64 to uint64 bits.
func float64ToBits(f float64) uint64 {
	return math.Float64bits(f)
}

// float64FromBits converts uint64 bits to float64.
func float64FromBits(b uint64) float64 {
	return math.Float64frombits(b)
}
