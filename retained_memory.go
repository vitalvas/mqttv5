package mqttv5

import (
	"sync"
)

// MemoryRetainedStore is an in-memory implementation of RetainedStore.
type MemoryRetainedStore struct {
	mu       sync.RWMutex
	messages map[string]*RetainedMessage
}

// NewMemoryRetainedStore creates a new in-memory retained store.
func NewMemoryRetainedStore() *MemoryRetainedStore {
	return &MemoryRetainedStore{
		messages: make(map[string]*RetainedMessage),
	}
}

// Set stores or updates a retained message.
func (s *MemoryRetainedStore) Set(msg *RetainedMessage) error {
	if err := ValidateTopicName(msg.Topic); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Empty payload means delete
	if len(msg.Payload) == 0 {
		delete(s.messages, msg.Topic)
		return nil
	}

	s.messages[msg.Topic] = msg
	return nil
}

// Get retrieves a retained message by exact topic.
func (s *MemoryRetainedStore) Get(topic string) (*RetainedMessage, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg, ok := s.messages[topic]
	return msg, ok
}

// Delete removes a retained message by topic.
func (s *MemoryRetainedStore) Delete(topic string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.messages[topic]; !ok {
		return false
	}
	delete(s.messages, topic)
	return true
}

// Match returns all retained messages matching a topic filter.
// Expired messages are excluded from the result and purged from storage.
func (s *MemoryRetainedStore) Match(filter string) []*RetainedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	var matched []*RetainedMessage
	var expired []string
	for topic, msg := range s.messages {
		if msg.IsExpired() {
			expired = append(expired, topic)
			continue
		}
		if TopicMatch(filter, topic) {
			matched = append(matched, msg)
		}
	}

	// Purge expired messages
	for _, topic := range expired {
		delete(s.messages, topic)
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

// Topics returns all topics with retained messages.
// Expired messages are excluded and purged.
func (s *MemoryRetainedStore) Topics() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	topics := make([]string, 0, len(s.messages))
	var expired []string
	for topic, msg := range s.messages {
		if msg.IsExpired() {
			expired = append(expired, topic)
			continue
		}
		topics = append(topics, topic)
	}

	// Purge expired messages
	for _, topic := range expired {
		delete(s.messages, topic)
	}

	return topics
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
