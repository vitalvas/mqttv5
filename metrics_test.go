package mqttv5

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetricType(t *testing.T) {
	t.Run("string representation", func(t *testing.T) {
		assert.Equal(t, "counter", MetricTypeCounter.String())
		assert.Equal(t, "gauge", MetricTypeGauge.String())
		assert.Equal(t, "histogram", MetricTypeHistogram.String())
		assert.Equal(t, "unknown", MetricType(99).String())
	})
}

func TestNoOpMetrics(t *testing.T) {
	metrics := &NoOpMetrics{}

	t.Run("counter operations", func(t *testing.T) {
		counter := metrics.Counter("test", nil)
		counter.Inc()
		counter.Add(10)
		assert.Equal(t, float64(0), counter.Value())
	})

	t.Run("gauge operations", func(t *testing.T) {
		gauge := metrics.Gauge("test", nil)
		gauge.Set(100)
		gauge.Inc()
		gauge.Dec()
		gauge.Add(10)
		gauge.Sub(5)
		assert.Equal(t, float64(0), gauge.Value())
	})

	t.Run("histogram operations", func(t *testing.T) {
		histogram := metrics.Histogram("test", nil)
		histogram.Observe(1.5)
		histogram.ObserveDuration(time.Second)
		assert.Equal(t, uint64(0), histogram.Count())
		assert.Equal(t, float64(0), histogram.Sum())
	})
}

func TestBrokerMetrics(t *testing.T) {
	t.Run("connection metrics", func(t *testing.T) {
		m := NewMemoryMetrics()
		bm := NewBrokerMetrics(m)

		bm.ConnectionOpened()
		bm.ConnectionOpened()
		bm.ConnectionClosed()

		connections := m.GetGauge(MetricConnections, nil)
		assert.Equal(t, float64(1), connections.Value())

		total := m.GetCounter(MetricConnectionsTotal, nil)
		assert.Equal(t, float64(2), total.Value())
	})

	t.Run("message metrics", func(t *testing.T) {
		m := NewMemoryMetrics()
		bm := NewBrokerMetrics(m)

		bm.MessageReceived(0)
		bm.MessageReceived(1)
		bm.MessageReceived(1)
		bm.MessageSent(2)

		recv0 := m.GetCounter(MetricMessagesReceived, MetricLabels{LabelQoS: "0"})
		recv1 := m.GetCounter(MetricMessagesReceived, MetricLabels{LabelQoS: "1"})
		sent2 := m.GetCounter(MetricMessagesSent, MetricLabels{LabelQoS: "2"})

		assert.Equal(t, float64(1), recv0.Value())
		assert.Equal(t, float64(2), recv1.Value())
		assert.Equal(t, float64(1), sent2.Value())
	})

	t.Run("bytes metrics", func(t *testing.T) {
		m := NewMemoryMetrics()
		bm := NewBrokerMetrics(m)

		bm.BytesReceived(100)
		bm.BytesReceived(200)
		bm.BytesSent(150)

		recv := m.GetCounter(MetricBytesReceived, nil)
		sent := m.GetCounter(MetricBytesSent, nil)

		assert.Equal(t, float64(300), recv.Value())
		assert.Equal(t, float64(150), sent.Value())
	})

	t.Run("subscription metrics", func(t *testing.T) {
		m := NewMemoryMetrics()
		bm := NewBrokerMetrics(m)

		bm.SubscriptionAdded()
		bm.SubscriptionAdded()
		bm.SubscriptionRemoved()

		subs := m.GetGauge(MetricSubscriptions, nil)
		assert.Equal(t, float64(1), subs.Value())
	})

	t.Run("retained message metrics", func(t *testing.T) {
		m := NewMemoryMetrics()
		bm := NewBrokerMetrics(m)

		bm.RetainedMessageSet()
		bm.RetainedMessageSet()
		bm.RetainedMessageRemoved()

		retained := m.GetGauge(MetricRetainedMessages, nil)
		assert.Equal(t, float64(1), retained.Value())
	})

	t.Run("latency metrics", func(t *testing.T) {
		m := NewMemoryMetrics()
		bm := NewBrokerMetrics(m)

		bm.PublishLatency(10 * time.Millisecond)
		bm.PublishLatency(20 * time.Millisecond)

		latency := m.GetHistogram(MetricPublishLatency, nil)
		assert.Equal(t, uint64(2), latency.Count())
		assert.InDelta(t, 0.03, latency.Sum(), 0.001)
	})

	t.Run("packet metrics", func(t *testing.T) {
		m := NewMemoryMetrics()
		bm := NewBrokerMetrics(m)

		bm.PacketReceived(PacketCONNECT)
		bm.PacketReceived(PacketPUBLISH)
		bm.PacketReceived(PacketPUBLISH)
		bm.PacketSent(PacketCONNACK)

		connect := m.GetCounter(MetricPacketsReceived, MetricLabels{LabelPacketType: "CONNECT"})
		publish := m.GetCounter(MetricPacketsReceived, MetricLabels{LabelPacketType: "PUBLISH"})
		connack := m.GetCounter(MetricPacketsSent, MetricLabels{LabelPacketType: "CONNACK"})

		assert.Equal(t, float64(1), connect.Value())
		assert.Equal(t, float64(2), publish.Value())
		assert.Equal(t, float64(1), connack.Value())
	})
}

func BenchmarkNoOpCounter(b *testing.B) {
	metrics := &NoOpMetrics{}
	counter := metrics.Counter("test", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		counter.Inc()
	}
}

func BenchmarkBrokerMetricsConnectionOpened(b *testing.B) {
	m := NewMemoryMetrics()
	bm := NewBrokerMetrics(m)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bm.ConnectionOpened()
	}
}

func BenchmarkBrokerMetricsMessageReceived(b *testing.B) {
	m := NewMemoryMetrics()
	bm := NewBrokerMetrics(m)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bm.MessageReceived(1)
	}
}
