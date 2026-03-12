package filedelivery

import (
	"fmt"
	"sync"
)

// Store defines the read-only interface for stream data access.
// Files and streams are provisioned externally; the handler only reads.
type Store interface {
	GetStream(key StreamKey) (*StreamDocument, error)
	ReadBlock(key StreamKey, fileID int, offset int64, size int) ([]byte, error)
}

// WriteStore defines the write interface for stream data.
// Used by the handler to accept uploaded streams from devices.
type WriteStore interface {
	GetStream(key StreamKey) (*StreamDocument, error)
	CreateStream(key StreamKey, doc *StreamDocument) error
	WriteBlock(key StreamKey, fileID int, offset int64, data []byte) error
}

// MemoryStore is a thread-safe in-memory implementation of Store.
// It provides additional PutStream, PutFile, and DeleteStream methods
// on the concrete type for test setup and external management.
type MemoryStore struct {
	mu      sync.RWMutex
	streams map[string]*StreamDocument
	files   map[string][]byte // key: "namespace/clientID/streamID/fileID"
}

// NewMemoryStore creates a new in-memory file delivery store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		streams: make(map[string]*StreamDocument),
		files:   make(map[string][]byte),
	}
}

func storeKey(key StreamKey) string {
	return key.Namespace + "/" + key.ClientID + "/" + key.StreamID
}

func fileKey(key StreamKey, fileID int) string {
	return fmt.Sprintf("%s/%s/%s/%d", key.Namespace, key.ClientID, key.StreamID, fileID)
}

// GetStream retrieves the stream document for the given key.
// Returns nil, nil if the stream does not exist.
func (m *MemoryStore) GetStream(key StreamKey) (*StreamDocument, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	doc, ok := m.streams[storeKey(key)]
	if !ok {
		return nil, nil
	}

	cp := *doc
	cp.Files = make([]FileInfo, len(doc.Files))
	copy(cp.Files, doc.Files)

	return &cp, nil
}

// ReadBlock reads a block of data from the specified file.
// Returns the block data, which may be shorter than size if at end of file.
func (m *MemoryStore) ReadBlock(key StreamKey, fileID int, offset int64, size int) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.files[fileKey(key, fileID)]
	if !ok {
		return nil, &ErrorResponse{Code: ErrResourceNotFound, Message: "file not found"}
	}

	if offset >= int64(len(data)) {
		return nil, &ErrorResponse{Code: ErrOffsetOutOfBounds, Message: "offset beyond file size"}
	}

	end := min(offset+int64(size), int64(len(data)))

	block := make([]byte, end-offset)
	copy(block, data[offset:end])

	return block, nil
}

// PutStream stores a stream document. This method is not on the Store interface.
func (m *MemoryStore) PutStream(key StreamKey, doc *StreamDocument) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := *doc
	cp.Files = make([]FileInfo, len(doc.Files))
	copy(cp.Files, doc.Files)

	m.streams[storeKey(key)] = &cp
}

// PutFile stores file data for a stream. This method is not on the Store interface.
func (m *MemoryStore) PutFile(key StreamKey, fileID int, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := make([]byte, len(data))
	copy(cp, data)

	m.files[fileKey(key, fileID)] = cp
}

// CreateStream creates a new stream. Returns an error if the stream already exists.
// Implements WriteStore.
func (m *MemoryStore) CreateStream(key StreamKey, doc *StreamDocument) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sk := storeKey(key)
	if _, ok := m.streams[sk]; ok {
		return &ErrorResponse{Code: ErrInvalidRequest, Message: "stream already exists"}
	}

	cp := *doc
	cp.Files = make([]FileInfo, len(doc.Files))
	copy(cp.Files, doc.Files)

	m.streams[sk] = &cp

	// Pre-allocate file entries
	for _, f := range doc.Files {
		m.files[fileKey(key, f.FileID)] = make([]byte, f.Size)
	}

	return nil
}

// WriteBlock writes data to a file at the given offset. Implements WriteStore.
func (m *MemoryStore) WriteBlock(key StreamKey, fileID int, offset int64, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fk := fileKey(key, fileID)
	fileData, ok := m.files[fk]
	if !ok {
		return &ErrorResponse{Code: ErrResourceNotFound, Message: "file not found"}
	}

	if offset < 0 || offset >= int64(len(fileData)) {
		return &ErrorResponse{Code: ErrOffsetOutOfBounds, Message: "offset beyond file size"}
	}

	if offset+int64(len(data)) > int64(len(fileData)) {
		return &ErrorResponse{Code: ErrInvalidRequest, Message: "write exceeds file size"}
	}

	copy(fileData[offset:], data)

	return nil
}

// DeleteStream removes a stream and all its files. This method is not on the Store interface.
func (m *MemoryStore) DeleteStream(key StreamKey) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sk := storeKey(key)
	doc, ok := m.streams[sk]
	if !ok {
		return
	}

	for _, f := range doc.Files {
		delete(m.files, fileKey(key, f.FileID))
	}

	delete(m.streams, sk)
}
