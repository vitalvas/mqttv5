package mqttv5

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNoOpMetrics(t *testing.T) {
	metrics := &NoOpMetrics{}

	t.Run("all operations are no-ops", func(_ *testing.T) {
		metrics.ConnectionOpened()
		metrics.ConnectionClosed()
		metrics.MessageReceived(0)
		metrics.MessageSent(1)
		metrics.BytesReceived(100)
		metrics.BytesSent(200)
		metrics.SubscriptionAdded()
		metrics.SubscriptionRemoved()
		metrics.RetainedMessageSet()
		metrics.RetainedMessageRemoved()
		metrics.PublishLatency(time.Millisecond)
		metrics.PacketReceived(PacketCONNECT)
		metrics.PacketSent(PacketCONNACK)
	})
}

func TestMemoryMetrics(t *testing.T) {
	t.Run("connection metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.ConnectionOpened()
		m.ConnectionOpened()
		m.ConnectionClosed()

		assert.Equal(t, int64(1), m.Connections())
		assert.Equal(t, int64(2), m.ConnectionsTotal())
	})

	t.Run("message metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.MessageReceived(0)
		m.MessageReceived(1)
		m.MessageReceived(1)
		m.MessageSent(2)

		assert.Equal(t, int64(1), m.MessagesReceived(0))
		assert.Equal(t, int64(2), m.MessagesReceived(1))
		assert.Equal(t, int64(1), m.MessagesSent(2))
	})

	t.Run("bytes metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.BytesReceived(100)
		m.BytesReceived(200)
		m.BytesSent(150)

		assert.Equal(t, int64(300), m.TotalBytesReceived())
		assert.Equal(t, int64(150), m.TotalBytesSent())
	})

	t.Run("subscription metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.SubscriptionAdded()
		m.SubscriptionAdded()
		m.SubscriptionRemoved()

		assert.Equal(t, int64(1), m.Subscriptions())
	})

	t.Run("retained message metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.RetainedMessageSet()
		m.RetainedMessageSet()
		m.RetainedMessageRemoved()

		assert.Equal(t, int64(1), m.RetainedMessages())
	})

	t.Run("latency metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.PublishLatency(10 * time.Millisecond)
		m.PublishLatency(20 * time.Millisecond)

		assert.Equal(t, int64(2), m.LatencyCount())
		assert.InDelta(t, 0.03, m.LatencySum(), 0.001)
	})

	t.Run("packet metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.PacketReceived(PacketCONNECT)
		m.PacketReceived(PacketPUBLISH)
		m.PacketReceived(PacketPUBLISH)
		m.PacketSent(PacketCONNACK)

		assert.Equal(t, int64(1), m.PacketsReceived(PacketCONNECT))
		assert.Equal(t, int64(2), m.PacketsReceived(PacketPUBLISH))
		assert.Equal(t, int64(1), m.PacketsSent(PacketCONNACK))
	})

	t.Run("qos bounds", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.MessageReceived(5) // should be capped to 2
		m.MessageSent(10)    // should be capped to 2

		assert.Equal(t, int64(1), m.MessagesReceived(2))
		assert.Equal(t, int64(1), m.MessagesSent(2))
	})
}

func TestMetricsCollectorInterface(t *testing.T) {
	t.Run("NoOpMetrics implements MetricsCollector", func(_ *testing.T) {
		var _ MetricsCollector = &NoOpMetrics{}
	})

	t.Run("MemoryMetrics implements MetricsCollector", func(_ *testing.T) {
		var _ MetricsCollector = NewMemoryMetrics()
	})
}

func BenchmarkNoOpMetrics(b *testing.B) {
	metrics := &NoOpMetrics{}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		metrics.ConnectionOpened()
	}
}

func BenchmarkMemoryMetricsConnectionOpened(b *testing.B) {
	m := NewMemoryMetrics()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		m.ConnectionOpened()
	}
}

func BenchmarkMemoryMetricsMessageReceived(b *testing.B) {
	m := NewMemoryMetrics()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		m.MessageReceived(1)
	}
}
