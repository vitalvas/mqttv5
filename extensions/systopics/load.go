package systopics

import (
	"math"
	"sync"
	"time"
)

// loadCalculator computes exponential moving average (EMA) rates
// for 1-minute, 5-minute, and 15-minute windows.
type loadCalculator struct {
	mu       sync.Mutex
	interval time.Duration
	alpha1m  float64
	alpha5m  float64
	alpha15m float64
	metrics  map[string]*emaSet
}

type emaSet struct {
	prev  int64
	ema1  float64
	ema5  float64
	ema15 float64
}

// LoadRates holds the EMA rates for all three windows.
type LoadRates struct {
	Min1  float64
	Min5  float64
	Min15 float64
}

func newLoadCalculator(interval time.Duration) *loadCalculator {
	secs := interval.Seconds()

	return &loadCalculator{
		interval: interval,
		alpha1m:  1 - math.Exp(-secs/60),
		alpha5m:  1 - math.Exp(-secs/300),
		alpha15m: 1 - math.Exp(-secs/900),
		metrics:  make(map[string]*emaSet),
	}
}

// update records the current raw counter value for a named metric.
// It computes the delta since last update and updates the EMA values.
func (lc *loadCalculator) update(name string, current int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	es, ok := lc.metrics[name]
	if !ok {
		lc.metrics[name] = &emaSet{prev: current}
		return
	}

	delta := current - es.prev
	es.prev = current

	// Clamp negative deltas to zero (counter reset).
	if delta < 0 {
		delta = 0
	}

	rate := float64(delta) / lc.interval.Seconds()

	es.ema1 += lc.alpha1m * (rate - es.ema1)
	es.ema5 += lc.alpha5m * (rate - es.ema5)
	es.ema15 += lc.alpha15m * (rate - es.ema15)
}

// rates returns the current EMA rates for a named metric.
func (lc *loadCalculator) rates(name string) LoadRates {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	es, ok := lc.metrics[name]
	if !ok {
		return LoadRates{}
	}

	return LoadRates{
		Min1:  es.ema1,
		Min5:  es.ema5,
		Min15: es.ema15,
	}
}
