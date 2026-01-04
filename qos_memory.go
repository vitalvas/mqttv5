package mqttv5

import (
	"sync"
	"time"
)

// MemoryMessageStore is an in-memory implementation of MessageStore.
type MemoryMessageStore struct {
	mu       sync.RWMutex
	messages map[string]*StoredMessage
}

// NewMemoryMessageStore creates a new in-memory message store.
func NewMemoryMessageStore() *MemoryMessageStore {
	return &MemoryMessageStore{
		messages: make(map[string]*StoredMessage),
	}
}

// Store stores a message with optional expiry.
func (s *MemoryMessageStore) Store(id string, msg *Message, expiry time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored := &StoredMessage{
		Message:  msg,
		StoredAt: time.Now(),
	}
	if expiry > 0 {
		stored.ExpiresAt = time.Now().Add(expiry)
	}
	s.messages[id] = stored
	return nil
}

// Get retrieves a message by ID.
func (s *MemoryMessageStore) Get(id string) (*Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stored, ok := s.messages[id]
	if !ok {
		return nil, false
	}
	if stored.IsExpired() {
		return nil, false
	}
	return stored.Message, true
}

// Exists checks if a message exists by ID.
func (s *MemoryMessageStore) Exists(id string) bool {
	_, ok := s.Get(id)
	return ok
}

// Delete deletes a message by ID.
func (s *MemoryMessageStore) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.messages[id]; !ok {
		return false
	}
	delete(s.messages, id)
	return true
}

// Cleanup removes expired messages.
func (s *MemoryMessageStore) Cleanup() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []string
	for id, stored := range s.messages {
		if stored.IsExpired() {
			expired = append(expired, id)
		}
	}

	for _, id := range expired {
		delete(s.messages, id)
	}
	return len(expired)
}

// Count returns the number of stored messages.
func (s *MemoryMessageStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.messages)
}
