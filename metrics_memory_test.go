package mqttv5

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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

	for range 100 {
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

func BenchmarkMemoryCounter(b *testing.B) {
	metrics := NewMemoryMetrics()
	counter := metrics.Counter("test", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		counter.Inc()
	}
}

func BenchmarkMemoryCounterConcurrent(b *testing.B) {
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

func BenchmarkMemoryGauge(b *testing.B) {
	metrics := NewMemoryMetrics()
	gauge := metrics.Gauge("test", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		gauge.Set(float64(b.N))
	}
}

func BenchmarkMemoryHistogram(b *testing.B) {
	metrics := NewMemoryMetrics()
	histogram := metrics.Histogram("test", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		histogram.Observe(1.5)
	}
}
