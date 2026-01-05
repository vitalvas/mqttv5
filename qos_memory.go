package mqttv5

import (
	"sync"
	"time"
)

// MemoryMessageStore is an in-memory implementation of MessageStore with namespace isolation.
type MemoryMessageStore struct {
	mu       sync.RWMutex
	messages map[string]*StoredMessage // key: namespace||id
}

// NewMemoryMessageStore creates a new in-memory message store.
func NewMemoryMessageStore() *MemoryMessageStore {
	return &MemoryMessageStore{
		messages: make(map[string]*StoredMessage),
	}
}

// Store stores a message with optional expiry in the specified namespace.
func (s *MemoryMessageStore) Store(namespace, id string, msg *Message, expiry time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := NamespaceKey(namespace, id)
	stored := &StoredMessage{
		Message:  msg,
		StoredAt: time.Now(),
	}
	if expiry > 0 {
		stored.ExpiresAt = time.Now().Add(expiry)
	}
	s.messages[key] = stored
	return nil
}

// Get retrieves a message by namespace and ID.
func (s *MemoryMessageStore) Get(namespace, id string) (*Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := NamespaceKey(namespace, id)
	stored, ok := s.messages[key]
	if !ok {
		return nil, false
	}
	if stored.IsExpired() {
		return nil, false
	}
	return stored.Message, true
}

// Exists checks if a message exists by namespace and ID.
func (s *MemoryMessageStore) Exists(namespace, id string) bool {
	_, ok := s.Get(namespace, id)
	return ok
}

// Delete deletes a message by namespace and ID.
func (s *MemoryMessageStore) Delete(namespace, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := NamespaceKey(namespace, id)
	if _, ok := s.messages[key]; !ok {
		return false
	}
	delete(s.messages, key)
	return true
}

// Cleanup removes expired messages from the specified namespace.
// If namespace is empty, cleans up all namespaces.
func (s *MemoryMessageStore) Cleanup(namespace string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	prefix := ""
	if namespace != "" {
		prefix = namespace + namespaceDelimiter
	}

	var expired []string
	for key, stored := range s.messages {
		if namespace != "" && !hasPrefix(key, prefix) {
			continue
		}
		if stored.IsExpired() {
			expired = append(expired, key)
		}
	}

	for _, key := range expired {
		delete(s.messages, key)
	}
	return len(expired)
}

// Count returns the number of stored messages in the specified namespace.
// If namespace is empty, returns total count across all namespaces.
func (s *MemoryMessageStore) Count(namespace string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if namespace == "" {
		return len(s.messages)
	}

	prefix := namespace + namespaceDelimiter
	count := 0
	for key := range s.messages {
		if hasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

// hasPrefix is a helper to check string prefix.
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
