package mqttv5

import (
	"sync"
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

func TestMemoryMetrics(t *testing.T) {
	t.Run("counter operations", func(t *testing.T) {
		metrics := NewMemoryMetrics()
		counter := metrics.Counter("test_counter", nil)

		counter.Inc()
		assert.Equal(t, float64(1), counter.Value())

		counter.Add(5)
		assert.Equal(t, float64(6), counter.Value())

		counter.Add(0.5)
		assert.Equal(t, float64(6.5), counter.Value())
	})

	t.Run("gauge operations", func(t *testing.T) {
		metrics := NewMemoryMetrics()
		gauge := metrics.Gauge("test_gauge", nil)

		gauge.Set(100)
		assert.Equal(t, float64(100), gauge.Value())

		gauge.Inc()
		assert.Equal(t, float64(101), gauge.Value())

		gauge.Dec()
		assert.Equal(t, float64(100), gauge.Value())

		gauge.Add(50)
		assert.Equal(t, float64(150), gauge.Value())

		gauge.Sub(30)
		assert.Equal(t, float64(120), gauge.Value())
	})

	t.Run("histogram operations", func(t *testing.T) {
		metrics := NewMemoryMetrics()
		histogram := metrics.Histogram("test_histogram", nil)

		histogram.Observe(1.5)
		histogram.Observe(2.5)
		histogram.Observe(3.0)

		assert.Equal(t, uint64(3), histogram.Count())
		assert.Equal(t, float64(7.0), histogram.Sum())
	})

	t.Run("histogram duration", func(t *testing.T) {
		metrics := NewMemoryMetrics()
		histogram := metrics.Histogram("latency", nil)

		histogram.ObserveDuration(100 * time.Millisecond)
		histogram.ObserveDuration(200 * time.Millisecond)

		assert.Equal(t, uint64(2), histogram.Count())
		assert.InDelta(t, 0.3, histogram.Sum(), 0.001)
	})

	t.Run("metrics with labels", func(t *testing.T) {
		metrics := NewMemoryMetrics()

		counter1 := metrics.Counter("requests", MetricLabels{"method": "GET"})
		counter2 := metrics.Counter("requests", MetricLabels{"method": "POST"})

		counter1.Inc()
		counter1.Inc()
		counter2.Inc()

		assert.Equal(t, float64(2), counter1.Value())
		assert.Equal(t, float64(1), counter2.Value())
	})

	t.Run("same metric returns same instance", func(t *testing.T) {
		metrics := NewMemoryMetrics()

		counter1 := metrics.Counter("test", nil)
		counter1.Inc()

		counter2 := metrics.Counter("test", nil)
		assert.Equal(t, float64(1), counter2.Value())

		counter2.Inc()
		assert.Equal(t, float64(2), counter1.Value())
	})

	t.Run("get counter for testing", func(t *testing.T) {
		metrics := NewMemoryMetrics()
		metrics.Counter("test", MetricLabels{"a": "1"}).Inc()

		c := metrics.GetCounter("test", MetricLabels{"a": "1"})
		assert.NotNil(t, c)
		assert.Equal(t, float64(1), c.Value())

		c = metrics.GetCounter("nonexistent", nil)
		assert.Nil(t, c)
	})

	t.Run("get gauge for testing", func(t *testing.T) {
		metrics := NewMemoryMetrics()
		metrics.Gauge("test", nil).Set(42)

		g := metrics.GetGauge("test", nil)
		assert.NotNil(t, g)
		assert.Equal(t, float64(42), g.Value())
	})

	t.Run("get histogram for testing", func(t *testing.T) {
		metrics := NewMemoryMetrics()
		metrics.Histogram("test", nil).Observe(1.0)

		h := metrics.GetHistogram("test", nil)
		assert.NotNil(t, h)
		assert.Equal(t, uint64(1), h.Count())
	})
}

func TestMemoryMetricsConcurrency(t *testing.T) {
	metrics := NewMemoryMetrics()
	counter := metrics.Counter("concurrent", nil)
	gauge := metrics.Gauge("concurrent", nil)
	histogram := metrics.Histogram("concurrent", nil)

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(3)

		go func() {
			defer wg.Done()
			counter.Inc()
		}()

		go func() {
			defer wg.Done()
			gauge.Inc()
		}()

		go func() {
			defer wg.Done()
			histogram.Observe(1.0)
		}()
	}

	wg.Wait()

	assert.Equal(t, float64(100), counter.Value())
	assert.Equal(t, float64(100), gauge.Value())
	assert.Equal(t, uint64(100), histogram.Count())
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

func TestLabelsKey(t *testing.T) {
	t.Run("without labels", func(t *testing.T) {
		key := labelsKey("test", nil)
		assert.Equal(t, "test", key)
	})

	t.Run("with empty labels", func(t *testing.T) {
		key := labelsKey("test", MetricLabels{})
		assert.Equal(t, "test", key)
	})

	t.Run("with labels", func(t *testing.T) {
		key := labelsKey("test", MetricLabels{"a": "1"})
		assert.Contains(t, key, "test")
		assert.Contains(t, key, "a=1")
	})
}

func BenchmarkCounter(b *testing.B) {
	metrics := NewMemoryMetrics()
	counter := metrics.Counter("test", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		counter.Inc()
	}
}

func BenchmarkCounterConcurrent(b *testing.B) {
	metrics := NewMemoryMetrics()
	counter := metrics.Counter("test", nil)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter.Inc()
		}
	})
}

func BenchmarkGauge(b *testing.B) {
	metrics := NewMemoryMetrics()
	gauge := metrics.Gauge("test", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		gauge.Set(float64(b.N))
	}
}

func BenchmarkHistogram(b *testing.B) {
	metrics := NewMemoryMetrics()
	histogram := metrics.Histogram("test", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		histogram.Observe(1.5)
	}
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
