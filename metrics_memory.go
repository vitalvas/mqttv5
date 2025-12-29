package mqttv5

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryMetrics is an in-memory implementation of Metrics for testing.
type MemoryMetrics struct {
	mu         sync.RWMutex
	counters   map[string]*memoryCounter
	gauges     map[string]*memoryGauge
	histograms map[string]*memoryHistogram
}

// NewMemoryMetrics creates a new in-memory metrics instance.
func NewMemoryMetrics() *MemoryMetrics {
	return &MemoryMetrics{
		counters:   make(map[string]*memoryCounter),
		gauges:     make(map[string]*memoryGauge),
		histograms: make(map[string]*memoryHistogram),
	}
}

func labelsKey(name string, labels MetricLabels) string {
	if len(labels) == 0 {
		return name
	}

	key := name
	for k, v := range labels {
		key += "|" + k + "=" + v
	}

	return key
}

// Counter returns a counter metric.
func (m *MemoryMetrics) Counter(name string, labels MetricLabels) Counter {
	key := labelsKey(name, labels)

	m.mu.Lock()
	defer m.mu.Unlock()

	if c, ok := m.counters[key]; ok {
		return c
	}

	c := &memoryCounter{name: name, labels: labels}
	m.counters[key] = c

	return c
}

// Gauge returns a gauge metric.
func (m *MemoryMetrics) Gauge(name string, labels MetricLabels) Gauge {
	key := labelsKey(name, labels)

	m.mu.Lock()
	defer m.mu.Unlock()

	if g, ok := m.gauges[key]; ok {
		return g
	}

	g := &memoryGauge{name: name, labels: labels}
	m.gauges[key] = g

	return g
}

// Histogram returns a histogram metric.
func (m *MemoryMetrics) Histogram(name string, labels MetricLabels) Histogram {
	key := labelsKey(name, labels)

	m.mu.Lock()
	defer m.mu.Unlock()

	if h, ok := m.histograms[key]; ok {
		return h
	}

	h := &memoryHistogram{name: name, labels: labels}
	m.histograms[key] = h

	return h
}

// GetCounter returns a counter by key for testing.
func (m *MemoryMetrics) GetCounter(name string, labels MetricLabels) Counter {
	key := labelsKey(name, labels)

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.counters[key]
}

// GetGauge returns a gauge by key for testing.
func (m *MemoryMetrics) GetGauge(name string, labels MetricLabels) Gauge {
	key := labelsKey(name, labels)

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.gauges[key]
}

// GetHistogram returns a histogram by key for testing.
func (m *MemoryMetrics) GetHistogram(name string, labels MetricLabels) Histogram {
	key := labelsKey(name, labels)

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.histograms[key]
}

type memoryCounter struct {
	name   string
	labels MetricLabels
	value  atomic.Uint64
}

func (c *memoryCounter) Inc() {
	c.Add(1)
}

func (c *memoryCounter) Add(delta float64) {
	for {
		old := c.value.Load()
		newVal := float64FromBits(old) + delta
		if c.value.CompareAndSwap(old, float64ToBits(newVal)) {
			break
		}
	}
}

func (c *memoryCounter) Value() float64 {
	return float64FromBits(c.value.Load())
}

type memoryGauge struct {
	name   string
	labels MetricLabels
	value  atomic.Uint64
}

func (g *memoryGauge) Set(value float64) {
	g.value.Store(float64ToBits(value))
}

func (g *memoryGauge) Inc() {
	g.Add(1)
}

func (g *memoryGauge) Dec() {
	g.Add(-1)
}

func (g *memoryGauge) Add(delta float64) {
	for {
		old := g.value.Load()
		newVal := float64FromBits(old) + delta
		if g.value.CompareAndSwap(old, float64ToBits(newVal)) {
			break
		}
	}
}

func (g *memoryGauge) Sub(delta float64) {
	g.Add(-delta)
}

func (g *memoryGauge) Value() float64 {
	return float64FromBits(g.value.Load())
}

type memoryHistogram struct {
	name   string
	labels MetricLabels
	count  atomic.Uint64
	sum    atomic.Uint64
}

func (h *memoryHistogram) Observe(value float64) {
	h.count.Add(1)

	for {
		old := h.sum.Load()
		newSum := float64FromBits(old) + value
		if h.sum.CompareAndSwap(old, float64ToBits(newSum)) {
			break
		}
	}
}

func (h *memoryHistogram) ObserveDuration(d time.Duration) {
	h.Observe(d.Seconds())
}

func (h *memoryHistogram) Count() uint64 {
	return h.count.Load()
}

func (h *memoryHistogram) Sum() float64 {
	return float64FromBits(h.sum.Load())
}

// float64ToBits converts a float64 to uint64 bits.
func float64ToBits(f float64) uint64 {
	return math.Float64bits(f)
}

// float64FromBits converts uint64 bits to float64.
func float64FromBits(b uint64) float64 {
	return math.Float64frombits(b)
}
