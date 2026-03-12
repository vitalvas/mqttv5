package filedelivery

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStore(t *testing.T) {
	key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "fw-update"}
	doc := &StreamDocument{
		Description: "firmware",
		Version:     1,
		Files: []FileInfo{
			{FileID: 0, Size: 10},
		},
	}

	t.Run("GetStream not found", func(t *testing.T) {
		s := NewMemoryStore()
		got, err := s.GetStream(key)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("PutStream and GetStream", func(t *testing.T) {
		s := NewMemoryStore()
		s.PutStream(key, doc)

		got, err := s.GetStream(key)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, doc.Description, got.Description)
		assert.Equal(t, doc.Version, got.Version)
		assert.Equal(t, doc.Files, got.Files)
	})

	t.Run("GetStream returns deep copy", func(t *testing.T) {
		s := NewMemoryStore()
		s.PutStream(key, doc)

		got1, err := s.GetStream(key)
		require.NoError(t, err)
		got1.Description = "modified"
		got1.Files[0].Size = 999

		got2, err := s.GetStream(key)
		require.NoError(t, err)
		assert.Equal(t, "firmware", got2.Description)
		assert.Equal(t, 10, got2.Files[0].Size)
	})

	t.Run("PutStream stores deep copy", func(t *testing.T) {
		s := NewMemoryStore()
		original := &StreamDocument{
			Description: "test",
			Version:     1,
			Files:       []FileInfo{{FileID: 0, Size: 100}},
		}
		s.PutStream(key, original)

		original.Description = "mutated"
		original.Files[0].Size = 999

		got, err := s.GetStream(key)
		require.NoError(t, err)
		assert.Equal(t, "test", got.Description)
		assert.Equal(t, 100, got.Files[0].Size)
	})

	t.Run("ReadBlock not found", func(t *testing.T) {
		s := NewMemoryStore()
		_, err := s.ReadBlock(key, 0, 0, 100)
		require.Error(t, err)
		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("PutFile and ReadBlock", func(t *testing.T) {
		s := NewMemoryStore()
		data := []byte("hello world!")
		s.PutFile(key, 0, data)

		block, err := s.ReadBlock(key, 0, 0, 5)
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), block)
	})

	t.Run("ReadBlock partial at end of file", func(t *testing.T) {
		s := NewMemoryStore()
		data := []byte("hello")
		s.PutFile(key, 0, data)

		block, err := s.ReadBlock(key, 0, 3, 10)
		require.NoError(t, err)
		assert.Equal(t, []byte("lo"), block)
	})

	t.Run("ReadBlock offset out of bounds", func(t *testing.T) {
		s := NewMemoryStore()
		s.PutFile(key, 0, []byte("hello"))

		_, err := s.ReadBlock(key, 0, 100, 10)
		require.Error(t, err)
		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrOffsetOutOfBounds, errResp.Code)
	})

	t.Run("ReadBlock exact size", func(t *testing.T) {
		s := NewMemoryStore()
		data := []byte("abcde")
		s.PutFile(key, 0, data)

		block, err := s.ReadBlock(key, 0, 0, 5)
		require.NoError(t, err)
		assert.Equal(t, data, block)
	})

	t.Run("ReadBlock returns copy", func(t *testing.T) {
		s := NewMemoryStore()
		s.PutFile(key, 0, []byte("hello"))

		block, err := s.ReadBlock(key, 0, 0, 5)
		require.NoError(t, err)
		block[0] = 'X'

		block2, err := s.ReadBlock(key, 0, 0, 5)
		require.NoError(t, err)
		assert.Equal(t, byte('h'), block2[0])
	})

	t.Run("PutFile stores copy", func(t *testing.T) {
		s := NewMemoryStore()
		data := []byte("hello")
		s.PutFile(key, 0, data)

		data[0] = 'X'

		block, err := s.ReadBlock(key, 0, 0, 5)
		require.NoError(t, err)
		assert.Equal(t, byte('h'), block[0])
	})

	t.Run("DeleteStream", func(t *testing.T) {
		s := NewMemoryStore()
		s.PutStream(key, doc)
		s.PutFile(key, 0, []byte("hello"))

		s.DeleteStream(key)

		got, err := s.GetStream(key)
		require.NoError(t, err)
		assert.Nil(t, got)

		_, err = s.ReadBlock(key, 0, 0, 5)
		require.Error(t, err)
	})

	t.Run("DeleteStream nonexistent", func(_ *testing.T) {
		s := NewMemoryStore()
		s.DeleteStream(key) // should not panic
	})

	t.Run("multiple files", func(t *testing.T) {
		s := NewMemoryStore()
		s.PutFile(key, 0, []byte("file0"))
		s.PutFile(key, 1, []byte("file1data"))

		block0, err := s.ReadBlock(key, 0, 0, 5)
		require.NoError(t, err)
		assert.Equal(t, []byte("file0"), block0)

		block1, err := s.ReadBlock(key, 1, 0, 9)
		require.NoError(t, err)
		assert.Equal(t, []byte("file1data"), block1)
	})

	t.Run("namespace isolation", func(t *testing.T) {
		s := NewMemoryStore()
		key1 := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "s1"}
		key2 := StreamKey{Namespace: "ns2", ClientID: "dev1", StreamID: "s1"}

		s.PutStream(key1, doc)
		s.PutFile(key1, 0, []byte("ns1-data"))

		got, err := s.GetStream(key2)
		require.NoError(t, err)
		assert.Nil(t, got)

		_, err = s.ReadBlock(key2, 0, 0, 5)
		require.Error(t, err)
	})

	t.Run("CreateStream success", func(t *testing.T) {
		s := NewMemoryStore()
		err := s.CreateStream(key, doc)
		require.NoError(t, err)

		got, err := s.GetStream(key)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, doc.Description, got.Description)
		assert.Equal(t, doc.Version, got.Version)
	})

	t.Run("CreateStream already exists", func(t *testing.T) {
		s := NewMemoryStore()
		require.NoError(t, s.CreateStream(key, doc))

		err := s.CreateStream(key, doc)
		require.Error(t, err)
		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("CreateStream pre-allocates files", func(t *testing.T) {
		s := NewMemoryStore()
		createDoc := &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 100}},
		}
		require.NoError(t, s.CreateStream(key, createDoc))

		block, err := s.ReadBlock(key, 0, 0, 100)
		require.NoError(t, err)
		assert.Len(t, block, 100)
	})

	t.Run("WriteBlock success", func(t *testing.T) {
		s := NewMemoryStore()
		createDoc := &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10}},
		}
		require.NoError(t, s.CreateStream(key, createDoc))

		err := s.WriteBlock(key, 0, 0, []byte("hello"))
		require.NoError(t, err)

		block, err := s.ReadBlock(key, 0, 0, 5)
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), block)
	})

	t.Run("WriteBlock file not found", func(t *testing.T) {
		s := NewMemoryStore()
		err := s.WriteBlock(key, 0, 0, []byte("data"))
		require.Error(t, err)
		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("WriteBlock offset out of bounds", func(t *testing.T) {
		s := NewMemoryStore()
		createDoc := &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 5}},
		}
		require.NoError(t, s.CreateStream(key, createDoc))

		err := s.WriteBlock(key, 0, 100, []byte("data"))
		require.Error(t, err)
		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrOffsetOutOfBounds, errResp.Code)
	})

	t.Run("WriteBlock exceeds file size", func(t *testing.T) {
		s := NewMemoryStore()
		createDoc := &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 5}},
		}
		require.NoError(t, s.CreateStream(key, createDoc))

		err := s.WriteBlock(key, 0, 0, []byte("too long data"))
		require.Error(t, err)
		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "write exceeds file size")
	})

	t.Run("WriteBlock partial overflow at end", func(t *testing.T) {
		s := NewMemoryStore()
		createDoc := &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10}},
		}
		require.NoError(t, s.CreateStream(key, createDoc))

		err := s.WriteBlock(key, 0, 8, []byte("abc"))
		require.Error(t, err)
		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("concurrent access", func(_ *testing.T) {
		s := NewMemoryStore()
		s.PutStream(key, doc)
		s.PutFile(key, 0, []byte("concurrent data"))

		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				_, _ = s.GetStream(key)
				_, _ = s.ReadBlock(key, 0, 0, 10)
			})
		}
		wg.Wait()
	})
}
