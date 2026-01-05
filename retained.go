package mqttv5

import "time"

// RetainedMessage represents a retained message.
type RetainedMessage struct {
	Topic           string
	Payload         []byte
	QoS             byte
	PayloadFormat   byte
	MessageExpiry   uint32
	PublishedAt     time.Time
	ContentType     string
	ResponseTopic   string
	CorrelationData []byte
	UserProperties  []StringPair
	Props           Properties
}

// IsExpired returns true if the message has expired.
func (m *RetainedMessage) IsExpired() bool {
	if m.MessageExpiry == 0 || m.PublishedAt.IsZero() {
		return false
	}
	expiryTime := m.PublishedAt.Add(time.Duration(m.MessageExpiry) * time.Second)
	return time.Now().After(expiryTime)
}

// RemainingExpiry returns the remaining expiry in seconds, or 0 if not set/expired.
func (m *RetainedMessage) RemainingExpiry() uint32 {
	if m.MessageExpiry == 0 || m.PublishedAt.IsZero() {
		return 0
	}
	elapsed := time.Since(m.PublishedAt)
	remaining := time.Duration(m.MessageExpiry)*time.Second - elapsed
	if remaining <= 0 {
		return 0
	}
	return uint32(remaining.Seconds())
}

// RetainedStore defines the interface for retained message storage with namespace isolation.
type RetainedStore interface {
	// Set stores or updates a retained message.
	// If the payload is empty, the retained message is deleted.
	Set(namespace string, msg *RetainedMessage) error

	// Get retrieves a retained message by exact topic.
	Get(namespace, topic string) (*RetainedMessage, bool)

	// Exists checks if a retained message exists by exact topic.
	Exists(namespace, topic string) bool

	// Delete removes a retained message by topic.
	Delete(namespace, topic string) bool

	// Match returns all retained messages matching a topic filter.
	Match(namespace, filter string) []*RetainedMessage

	// Clear removes all retained messages in the specified namespace.
	// If namespace is empty, removes all retained messages across all namespaces.
	Clear(namespace string)

	// Count returns the number of retained messages in the specified namespace.
	// If namespace is empty, returns total count across all namespaces.
	Count(namespace string) int

	// Topics returns all topics with retained messages in the specified namespace.
	// If namespace is empty, returns topics across all namespaces.
	// Returns namespace||topic keys - use ParseNamespaceKey to extract namespace and topic.
	Topics(namespace string) []string

	// Cleanup removes expired retained messages from the specified namespace.
	// If namespace is empty, cleans up all namespaces.
	// Returns the number of messages removed.
	Cleanup(namespace string) int
}
