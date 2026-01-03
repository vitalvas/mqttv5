package mqttv5

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlowController(t *testing.T) {
	t.Run("initial state", func(t *testing.T) {
		fc := NewFlowController(10)

		assert.Equal(t, uint16(10), fc.ReceiveMaximum())
		assert.Equal(t, uint16(10), fc.Available())
		assert.Equal(t, uint16(0), fc.InFlight())
		assert.True(t, fc.CanSend())
	})

	t.Run("default receive maximum", func(t *testing.T) {
		fc := NewFlowController(0)

		assert.Equal(t, uint16(65535), fc.ReceiveMaximum())
	})

	t.Run("acquire and release", func(t *testing.T) {
		fc := NewFlowController(3)

		err := fc.Acquire()
		require.NoError(t, err)
		assert.Equal(t, uint16(2), fc.Available())
		assert.Equal(t, uint16(1), fc.InFlight())

		err = fc.Acquire()
		require.NoError(t, err)
		assert.Equal(t, uint16(1), fc.Available())

		err = fc.Acquire()
		require.NoError(t, err)
		assert.Equal(t, uint16(0), fc.Available())
		assert.False(t, fc.CanSend())

		err = fc.Acquire()
		assert.ErrorIs(t, err, ErrQuotaExceeded)

		fc.Release()
		assert.Equal(t, uint16(1), fc.Available())
		assert.True(t, fc.CanSend())
	})

	t.Run("try acquire", func(t *testing.T) {
		fc := NewFlowController(2)

		assert.True(t, fc.TryAcquire())
		assert.True(t, fc.TryAcquire())
		assert.False(t, fc.TryAcquire())

		fc.Release()
		assert.True(t, fc.TryAcquire())
	})

	t.Run("reset", func(t *testing.T) {
		fc := NewFlowController(5)

		fc.Acquire()
		fc.Acquire()
		fc.Acquire()

		assert.Equal(t, uint16(3), fc.InFlight())

		fc.Reset()
		assert.Equal(t, uint16(0), fc.InFlight())
		assert.Equal(t, uint16(5), fc.Available())
	})

	t.Run("set receive maximum", func(t *testing.T) {
		fc := NewFlowController(10)

		fc.Acquire()
		fc.Acquire()

		fc.SetReceiveMaximum(5)
		assert.Equal(t, uint16(5), fc.ReceiveMaximum())
		assert.Equal(t, uint16(3), fc.Available())

		fc.SetReceiveMaximum(0)
		assert.Equal(t, uint16(65535), fc.ReceiveMaximum())
	})

	t.Run("release underflow protection", func(t *testing.T) {
		fc := NewFlowController(5)

		fc.Release()
		assert.Equal(t, uint16(0), fc.InFlight())
	})
}

func TestFlowControllerConcurrency(t *testing.T) {
	fc := NewFlowController(100)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if fc.TryAcquire() {
					fc.Release()
				}
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, uint16(0), fc.InFlight())
}

func BenchmarkFlowControllerAcquireRelease(b *testing.B) {
	fc := NewFlowController(65535)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		fc.Acquire()
		fc.Release()
	}
}

func BenchmarkFlowControllerTryAcquire(b *testing.B) {
	fc := NewFlowController(65535)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		if fc.TryAcquire() {
			fc.Release()
		}
	}
}

func TestFlowControllerServerReceiveMaximum(t *testing.T) {
	t.Run("client flow control respects server receive maximum", func(t *testing.T) {
		fc := NewFlowController(2) // Server advertises receive maximum of 2

		// Should be able to acquire 2
		assert.True(t, fc.TryAcquire())
		assert.True(t, fc.TryAcquire())

		// Third should fail
		assert.False(t, fc.TryAcquire(), "should not exceed receive maximum")

		// After release, should be able to acquire again
		fc.Release()
		assert.True(t, fc.TryAcquire())
	})
}
