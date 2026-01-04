package mqttv5

import (
	"sync"
)

// Buffer pools for reducing allocations in hot paths.
var (
	// bytesReaderPool for packet decoding
	bytesReaderPool = sync.Pool{
		New: func() any {
			return &bytesReader{}
		},
	}

	// bytesBufferPool for packet encoding
	bytesBufferPool = sync.Pool{
		New: func() any {
			return &bytesBuffer{}
		},
	}
)

// getBytesReader returns a pooled bytesReader.
func getBytesReader(data []byte) *bytesReader {
	r := bytesReaderPool.Get().(*bytesReader)
	r.data = data
	r.pos = 0
	return r
}

// putBytesReader returns a bytesReader to the pool.
func putBytesReader(r *bytesReader) {
	if r == nil {
		return
	}
	r.data = nil
	r.pos = 0
	bytesReaderPool.Put(r)
}

// getBytesBuffer returns a pooled bytesBuffer.
func getBytesBuffer() *bytesBuffer {
	b := bytesBufferPool.Get().(*bytesBuffer)
	b.data = b.data[:0]
	return b
}

// putBytesBuffer returns a bytesBuffer to the pool.
func putBytesBuffer(b *bytesBuffer) {
	if b == nil {
		return
	}
	// Only pool if capacity is reasonable (64KB)
	if cap(b.data) <= 65536 {
		b.data = b.data[:0]
		bytesBufferPool.Put(b)
	}
}
