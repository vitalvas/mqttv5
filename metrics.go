package mqttv5

import (
	"time"
)

// MetricType represents the type of metric.
type MetricType int

const (
	// MetricTypeCounter is a monotonically increasing counter.
	MetricTypeCounter MetricType = 0
	// MetricTypeGauge is a value that can go up and down.
	MetricTypeGauge MetricType = 1
	// MetricTypeHistogram tracks distribution of values.
	MetricTypeHistogram MetricType = 2
)

// String returns the string representation of the metric type.
func (t MetricType) String() string {
	switch t {
	case MetricTypeCounter:
		return "counter"
	case MetricTypeGauge:
		return "gauge"
	case MetricTypeHistogram:
		return "histogram"
	default:
		return "unknown"
	}
}

// MetricLabels represents key-value pairs for metric labels.
type MetricLabels map[string]string

// Metrics defines the interface for collecting metrics.
type Metrics interface {
	// Counter returns a counter metric.
	Counter(name string, labels MetricLabels) Counter

	// Gauge returns a gauge metric.
	Gauge(name string, labels MetricLabels) Gauge

	// Histogram returns a histogram metric.
	Histogram(name string, labels MetricLabels) Histogram
}

// Counter is a monotonically increasing counter.
type Counter interface {
	// Inc increments the counter by 1.
	Inc()

	// Add adds the given value to the counter.
	Add(delta float64)

	// Value returns the current value.
	Value() float64
}

// Gauge is a metric that can go up and down.
type Gauge interface {
	// Set sets the gauge to the given value.
	Set(value float64)

	// Inc increments the gauge by 1.
	Inc()

	// Dec decrements the gauge by 1.
	Dec()

	// Add adds the given value to the gauge.
	Add(delta float64)

	// Sub subtracts the given value from the gauge.
	Sub(delta float64)

	// Value returns the current value.
	Value() float64
}

// Histogram tracks the distribution of values.
type Histogram interface {
	// Observe records a value.
	Observe(value float64)

	// ObserveDuration records a duration in seconds.
	ObserveDuration(d time.Duration)

	// Count returns the number of observations.
	Count() uint64

	// Sum returns the sum of all observations.
	Sum() float64
}

// NoOpMetrics is a no-op implementation of Metrics.
type NoOpMetrics struct{}

// Counter returns a no-op counter.
func (n *NoOpMetrics) Counter(_ string, _ MetricLabels) Counter {
	return &noOpCounter{}
}

// Gauge returns a no-op gauge.
func (n *NoOpMetrics) Gauge(_ string, _ MetricLabels) Gauge {
	return &noOpGauge{}
}

// Histogram returns a no-op histogram.
func (n *NoOpMetrics) Histogram(_ string, _ MetricLabels) Histogram {
	return &noOpHistogram{}
}

type noOpCounter struct{}

func (n *noOpCounter) Inc()           {}
func (n *noOpCounter) Add(_ float64)  {}
func (n *noOpCounter) Value() float64 { return 0 }

type noOpGauge struct{}

func (n *noOpGauge) Set(_ float64)  {}
func (n *noOpGauge) Inc()           {}
func (n *noOpGauge) Dec()           {}
func (n *noOpGauge) Add(_ float64)  {}
func (n *noOpGauge) Sub(_ float64)  {}
func (n *noOpGauge) Value() float64 { return 0 }

type noOpHistogram struct{}

func (n *noOpHistogram) Observe(_ float64)            {}
func (n *noOpHistogram) ObserveDuration(_ time.Duration) {}
func (n *noOpHistogram) Count() uint64                { return 0 }
func (n *noOpHistogram) Sum() float64                 { return 0 }

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

	// MetricPublishLatency is the publish message processing latency.
	MetricPublishLatency = "mqtt_publish_latency_seconds"

	// MetricPacketsSent is the total number of packets sent.
	MetricPacketsSent = "mqtt_packets_sent_total"

	// MetricPacketsReceived is the total number of packets received.
	MetricPacketsReceived = "mqtt_packets_received_total"
)

// Standard metric labels.
const (
	// LabelPacketType is the packet type label.
	LabelPacketType = "packet_type"

	// LabelQoS is the QoS level label.
	LabelQoS = "qos"

	// LabelReasonCode is the reason code label.
	LabelReasonCode = "reason_code"

	// LabelClientID is the client ID label.
	LabelClientID = "client_id"

	// LabelTopic is the topic label.
	LabelTopic = "topic"
)

// BrokerMetrics provides convenience methods for common broker metrics.
type BrokerMetrics struct {
	metrics Metrics
}

// NewBrokerMetrics creates a new BrokerMetrics instance.
func NewBrokerMetrics(m Metrics) *BrokerMetrics {
	return &BrokerMetrics{metrics: m}
}

// ConnectionOpened records a new connection.
func (b *BrokerMetrics) ConnectionOpened() {
	b.metrics.Gauge(MetricConnections, nil).Inc()
	b.metrics.Counter(MetricConnectionsTotal, nil).Inc()
}

// ConnectionClosed records a closed connection.
func (b *BrokerMetrics) ConnectionClosed() {
	b.metrics.Gauge(MetricConnections, nil).Dec()
}

// MessageReceived records a received message.
func (b *BrokerMetrics) MessageReceived(qos byte) {
	labels := MetricLabels{LabelQoS: string(rune('0' + qos))}
	b.metrics.Counter(MetricMessagesReceived, labels).Inc()
}

// MessageSent records a sent message.
func (b *BrokerMetrics) MessageSent(qos byte) {
	labels := MetricLabels{LabelQoS: string(rune('0' + qos))}
	b.metrics.Counter(MetricMessagesSent, labels).Inc()
}

// BytesReceived records received bytes.
func (b *BrokerMetrics) BytesReceived(n int) {
	b.metrics.Counter(MetricBytesReceived, nil).Add(float64(n))
}

// BytesSent records sent bytes.
func (b *BrokerMetrics) BytesSent(n int) {
	b.metrics.Counter(MetricBytesSent, nil).Add(float64(n))
}

// SubscriptionAdded records a new subscription.
func (b *BrokerMetrics) SubscriptionAdded() {
	b.metrics.Gauge(MetricSubscriptions, nil).Inc()
}

// SubscriptionRemoved records a removed subscription.
func (b *BrokerMetrics) SubscriptionRemoved() {
	b.metrics.Gauge(MetricSubscriptions, nil).Dec()
}

// RetainedMessageSet records a retained message being set.
func (b *BrokerMetrics) RetainedMessageSet() {
	b.metrics.Gauge(MetricRetainedMessages, nil).Inc()
}

// RetainedMessageRemoved records a retained message being removed.
func (b *BrokerMetrics) RetainedMessageRemoved() {
	b.metrics.Gauge(MetricRetainedMessages, nil).Dec()
}

// PublishLatency records publish processing latency.
func (b *BrokerMetrics) PublishLatency(d time.Duration) {
	b.metrics.Histogram(MetricPublishLatency, nil).ObserveDuration(d)
}

// PacketReceived records a received packet.
func (b *BrokerMetrics) PacketReceived(packetType PacketType) {
	labels := MetricLabels{LabelPacketType: packetType.String()}
	b.metrics.Counter(MetricPacketsReceived, labels).Inc()
}

// PacketSent records a sent packet.
func (b *BrokerMetrics) PacketSent(packetType PacketType) {
	labels := MetricLabels{LabelPacketType: packetType.String()}
	b.metrics.Counter(MetricPacketsSent, labels).Inc()
}
