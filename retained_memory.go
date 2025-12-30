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
// Expired messages are excluded from the result.
func (s *MemoryRetainedStore) Match(filter string) []*RetainedMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matched []*RetainedMessage
	for topic, msg := range s.messages {
		if TopicMatch(filter, topic) && !msg.IsExpired() {
			matched = append(matched, msg)
		}
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
func (s *MemoryRetainedStore) Topics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]string, 0, len(s.messages))
	for topic := range s.messages {
		topics = append(topics, topic)
	}
	return topics
}
