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

// Clear removes all retained messages in the specified namespace.
// If namespace is empty, removes all retained messages across all namespaces.
func (s *MemoryRetainedStore) Clear(namespace string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if namespace == "" {
		s.messages = make(map[string]*RetainedMessage)
		return
	}

	prefix := namespace + namespaceDelimiter
	for key := range s.messages {
		if strings.HasPrefix(key, prefix) {
			delete(s.messages, key)
		}
	}
}

// Count returns the number of retained messages in the specified namespace.
// If namespace is empty, returns total count across all namespaces.
func (s *MemoryRetainedStore) Count(namespace string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if namespace == "" {
		return len(s.messages)
	}

	prefix := namespace + namespaceDelimiter
	count := 0
	for key := range s.messages {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

// Topics returns all topics with retained messages in the specified namespace.
// If namespace is empty, returns topics across all namespaces.
// Returns namespace||topic keys - use ParseNamespaceKey to extract namespace and topic.
// Expired messages are excluded and purged.
func (s *MemoryRetainedStore) Topics(namespace string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	prefix := ""
	if namespace != "" {
		prefix = namespace + namespaceDelimiter
	}

	keys := make([]string, 0, len(s.messages))
	var expired []string
	for key, msg := range s.messages {
		if namespace != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		if msg.IsExpired() {
			expired = append(expired, key)
			continue
		}
		keys = append(keys, key)
	}

	// Purge expired messages
	for _, key := range expired {
		delete(s.messages, key)
	}

	return keys
}

// Cleanup removes expired retained messages from the specified namespace.
// If namespace is empty, cleans up all namespaces.
// Returns the number of messages removed.
func (s *MemoryRetainedStore) Cleanup(namespace string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []string
	for key, msg := range s.messages {
		if namespace != "" {
			ns, _ := ParseNamespaceKey(key)
			if ns != namespace {
				continue
			}
		}
		if msg.IsExpired() {
			expired = append(expired, key)
		}
	}

	for _, key := range expired {
		delete(s.messages, key)
	}

	return len(expired)
}
