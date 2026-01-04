package mqttv5

import (
	"strings"
	"sync"
)

// MemoryRetainedStore is an in-memory implementation of RetainedStore.
type MemoryRetainedStore struct {
	mu       sync.RWMutex
	messages map[string]*RetainedMessage // key: namespace||topic
}

// NewMemoryRetainedStore creates a new in-memory retained store.
func NewMemoryRetainedStore() *MemoryRetainedStore {
	return &MemoryRetainedStore{
		messages: make(map[string]*RetainedMessage),
	}
}

// Set stores or updates a retained message.
func (s *MemoryRetainedStore) Set(namespace string, msg *RetainedMessage) error {
	if err := ValidateTopicName(msg.Topic); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := NamespaceKey(namespace, msg.Topic)

	// Empty payload means delete
	if len(msg.Payload) == 0 {
		delete(s.messages, key)
		return nil
	}

	s.messages[key] = msg
	return nil
}

// Get retrieves a retained message by exact topic.
func (s *MemoryRetainedStore) Get(namespace, topic string) (*RetainedMessage, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := NamespaceKey(namespace, topic)
	msg, ok := s.messages[key]
	return msg, ok
}

// Exists checks if a retained message exists by exact topic.
func (s *MemoryRetainedStore) Exists(namespace, topic string) bool {
	_, ok := s.Get(namespace, topic)
	return ok
}

// Delete removes a retained message by topic.
func (s *MemoryRetainedStore) Delete(namespace, topic string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := NamespaceKey(namespace, topic)
	if _, ok := s.messages[key]; !ok {
		return false
	}
	delete(s.messages, key)
	return true
}

// Match returns all retained messages matching a topic filter.
// Expired messages are excluded from the result and purged from storage.
func (s *MemoryRetainedStore) Match(namespace, filter string) []*RetainedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	prefix := namespace + namespaceDelimiter
	var matched []*RetainedMessage
	var expired []string
	for key, msg := range s.messages {
		// Only match messages in the specified namespace
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		if msg.IsExpired() {
			expired = append(expired, key)
			continue
		}
		if TopicMatch(filter, msg.Topic) {
			matched = append(matched, msg)
		}
	}

	// Purge expired messages
	for _, key := range expired {
		delete(s.messages, key)
	}

	return matched
}

// Clear removes all retained messages.
func (s *MemoryRetainedStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = make(map[string]*RetainedMessage)
}

// Count returns the number of retained messages.
func (s *MemoryRetainedStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.messages)
}

// Topics returns all topics with retained messages as namespace||topic keys.
// Use ParseNamespaceKey to extract namespace and topic from each key.
// Expired messages are excluded and purged.
func (s *MemoryRetainedStore) Topics() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.messages))
	var expired []string
	for key, msg := range s.messages {
		if msg.IsExpired() {
			expired = append(expired, key)
			continue
		}
		// Return the key (namespace||topic) for namespace awareness
		keys = append(keys, key)
	}

	// Purge expired messages
	for _, key := range expired {
		delete(s.messages, key)
	}

	return keys
}

// Cleanup removes all expired retained messages.
// Returns the number of messages removed.
func (s *MemoryRetainedStore) Cleanup() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []string
	for topic, msg := range s.messages {
		if msg.IsExpired() {
			expired = append(expired, topic)
		}
	}

	for _, topic := range expired {
		delete(s.messages, topic)
	}

	return len(expired)
}
