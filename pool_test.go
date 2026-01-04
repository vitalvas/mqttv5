package mqttv5

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetBytesReader(t *testing.T) {
	t.Run("returns reader with correct data", func(t *testing.T) {
		data := []byte("test data")
		reader := getBytesReader(data)

		assert.NotNil(t, reader)
		assert.Equal(t, data, reader.data)
		assert.Equal(t, 0, reader.pos)

		putBytesReader(reader)
	})

	t.Run("reader can read data", func(t *testing.T) {
		data := []byte("hello world")
		reader := getBytesReader(data)

		buf := make([]byte, 5)
		n, err := reader.Read(buf)

		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte("hello"), buf)
		assert.Equal(t, 5, reader.pos)

		putBytesReader(reader)
	})

	t.Run("reused reader is reset", func(t *testing.T) {
		data1 := []byte("first")
		reader := getBytesReader(data1)

		// Read some data
		buf := make([]byte, 3)
		reader.Read(buf)
		assert.Equal(t, 3, reader.pos)

		// Return to pool
		putBytesReader(reader)

		// Get another reader
		data2 := []byte("second")
		reader2 := getBytesReader(data2)

		assert.Equal(t, data2, reader2.data)
		assert.Equal(t, 0, reader2.pos)

		putBytesReader(reader2)
	})
}

func TestPutBytesReader(t *testing.T) {
	t.Run("handles nil reader", func(t *testing.T) {
		assert.NotPanics(t, func() {
			putBytesReader(nil)
		})
	})

	t.Run("clears data reference", func(t *testing.T) {
		data := []byte("test")
		reader := getBytesReader(data)

		// We can't directly verify the pool behavior, but we can verify the function doesn't panic
		assert.NotPanics(t, func() {
			putBytesReader(reader)
		})
	})
}

func TestGetBytesBuffer(t *testing.T) {
	t.Run("returns empty buffer", func(t *testing.T) {
		buf := getBytesBuffer()

		assert.NotNil(t, buf)
		assert.Len(t, buf.data, 0)

		putBytesBuffer(buf)
	})

	t.Run("buffer can write data", func(t *testing.T) {
		buf := getBytesBuffer()

		n, err := buf.Write([]byte("hello"))
		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte("hello"), buf.Bytes())

		n, err = buf.Write([]byte(" world"))
		assert.NoError(t, err)
		assert.Equal(t, 6, n)
		assert.Equal(t, []byte("hello world"), buf.Bytes())

		putBytesBuffer(buf)
	})

	t.Run("reused buffer is reset", func(t *testing.T) {
		buf := getBytesBuffer()
		buf.Write([]byte("some data"))
		assert.Greater(t, len(buf.data), 0)

		putBytesBuffer(buf)

		buf2 := getBytesBuffer()
		assert.Len(t, buf2.data, 0)

		putBytesBuffer(buf2)
	})
}

func TestPutBytesBuffer(t *testing.T) {
	t.Run("handles nil buffer", func(t *testing.T) {
		assert.NotPanics(t, func() {
			putBytesBuffer(nil)
		})
	})

	t.Run("pools small buffers", func(t *testing.T) {
		buf := getBytesBuffer()
		buf.Write(make([]byte, 1000))

		assert.NotPanics(t, func() {
			putBytesBuffer(buf)
		})
	})

	t.Run("does not pool very large buffers", func(t *testing.T) {
		buf := getBytesBuffer()
		// Write more than 64KB to exceed pool limit
		buf.Write(make([]byte, 100000))

		assert.NotPanics(t, func() {
			putBytesBuffer(buf)
		})
	})
}

func TestPoolConcurrency(t *testing.T) {
	t.Run("bytesReader pool is thread safe", func(_ *testing.T) {
		var wg sync.WaitGroup
		iterations := 1000

		for range iterations {
			wg.Add(1)
			go func() {
				defer wg.Done()
				data := []byte("concurrent test data")
				reader := getBytesReader(data)

				buf := make([]byte, 5)
				reader.Read(buf)

				putBytesReader(reader)
			}()
		}

		wg.Wait()
	})

	t.Run("bytesBuffer pool is thread safe", func(_ *testing.T) {
		var wg sync.WaitGroup
		iterations := 1000

		for range iterations {
			wg.Add(1)
			go func() {
				defer wg.Done()
				buf := getBytesBuffer()

				buf.Write([]byte("concurrent write"))
				_ = buf.Bytes()

				putBytesBuffer(buf)
			}()
		}

		wg.Wait()
	})
}

func BenchmarkBytesReaderPool(b *testing.B) {
	data := []byte("benchmark test data for reader pool")

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader := getBytesReader(data)
		buf := make([]byte, 10)
		reader.Read(buf)
		putBytesReader(reader)
	}
}

func BenchmarkBytesBufferPool(b *testing.B) {
	writeData := []byte("benchmark test data for buffer pool")

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		buf := getBytesBuffer()
		buf.Write(writeData)
		_ = buf.Bytes()
		putBytesBuffer(buf)
	}
}

func BenchmarkBytesReaderPoolParallel(b *testing.B) {
	data := []byte("parallel benchmark test data")

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reader := getBytesReader(data)
			buf := make([]byte, 10)
			reader.Read(buf)
			putBytesReader(reader)
		}
	})
}

func BenchmarkBytesBufferPoolParallel(b *testing.B) {
	writeData := []byte("parallel benchmark test data")

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := getBytesBuffer()
			buf.Write(writeData)
			_ = buf.Bytes()
			putBytesBuffer(buf)
		}
	})
}
