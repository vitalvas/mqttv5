package systopics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLoadCalculator(t *testing.T) {
	t.Run("first update initializes without rates", func(t *testing.T) {
		lc := newLoadCalculator(10 * time.Second)

		lc.update("test", 100)
		rates := lc.rates("test")

		assert.Equal(t, 0.0, rates.Min1)
		assert.Equal(t, 0.0, rates.Min5)
		assert.Equal(t, 0.0, rates.Min15)
	})

	t.Run("rates for unknown metric are zero", func(t *testing.T) {
		lc := newLoadCalculator(10 * time.Second)

		rates := lc.rates("unknown")

		assert.Equal(t, 0.0, rates.Min1)
		assert.Equal(t, 0.0, rates.Min5)
		assert.Equal(t, 0.0, rates.Min15)
	})

	t.Run("constant rate converges", func(t *testing.T) {
		lc := newLoadCalculator(time.Second)

		// Simulate a constant rate of 10/sec for many ticks.
		// 15min EMA needs ~3x window to converge, so 3000 ticks.
		for i := range 3000 {
			lc.update("test", int64((i+1)*10))
		}

		rates := lc.rates("test")

		// All EMAs should converge near 10.
		assert.InDelta(t, 10.0, rates.Min1, 0.01)
		assert.InDelta(t, 10.0, rates.Min5, 0.1)
		assert.InDelta(t, 10.0, rates.Min15, 0.5)
	})

	t.Run("zero delta produces decaying rates", func(t *testing.T) {
		lc := newLoadCalculator(time.Second)

		// Generate some activity.
		for i := range 60 {
			lc.update("test", int64((i+1)*100))
		}

		ratesBefore := lc.rates("test")
		assert.Greater(t, ratesBefore.Min1, 0.0)

		// Now stop activity.
		lastVal := int64(61 * 100)
		for range 120 {
			lc.update("test", lastVal)
		}

		ratesAfter := lc.rates("test")
		assert.Less(t, ratesAfter.Min1, ratesBefore.Min1)
	})

	t.Run("multiple metrics are independent", func(t *testing.T) {
		lc := newLoadCalculator(time.Second)

		for i := range 60 {
			lc.update("fast", int64((i+1)*100))
			lc.update("slow", int64((i+1)*10))
		}

		fast := lc.rates("fast")
		slow := lc.rates("slow")

		assert.Greater(t, fast.Min1, slow.Min1)
	})

	t.Run("counter reset clamps to zero", func(t *testing.T) {
		lc := newLoadCalculator(time.Second)

		// Build up some rate.
		for i := range 60 {
			lc.update("test", int64((i+1)*100))
		}

		ratesBefore := lc.rates("test")
		assert.Greater(t, ratesBefore.Min1, 0.0)

		// Simulate counter reset (value drops).
		lc.update("test", 0)

		ratesAfter := lc.rates("test")

		// Rates should decay toward zero, never go negative.
		assert.GreaterOrEqual(t, ratesAfter.Min1, 0.0)
		assert.GreaterOrEqual(t, ratesAfter.Min5, 0.0)
		assert.GreaterOrEqual(t, ratesAfter.Min15, 0.0)
	})

	t.Run("shorter windows react faster", func(t *testing.T) {
		lc := newLoadCalculator(time.Second)

		// Start with some baseline.
		for i := range 60 {
			lc.update("test", int64((i+1)*10))
		}

		// Sudden spike.
		base := int64(61 * 10)
		for i := range 10 {
			lc.update("test", base+int64((i+1)*1000))
		}

		rates := lc.rates("test")

		// 1min EMA should react faster than 5min, which reacts faster than 15min.
		assert.Greater(t, rates.Min1, rates.Min5)
		assert.Greater(t, rates.Min5, rates.Min15)
	})
}
