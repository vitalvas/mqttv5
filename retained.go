package mqttv5

// RetainedMessage represents a retained message.
type RetainedMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
	Props   Properties
}

// RetainedStore defines the interface for retained message storage.
type RetainedStore interface {
	// Set stores or updates a retained message.
	// If the payload is empty, the retained message is deleted.
	Set(msg *RetainedMessage) error

	// Get retrieves a retained message by exact topic.
	Get(topic string) (*RetainedMessage, bool)

	// Delete removes a retained message by topic.
	Delete(topic string) bool

	// Match returns all retained messages matching a topic filter.
	Match(filter string) []*RetainedMessage

	// Clear removes all retained messages.
	Clear()

	// Count returns the number of retained messages.
	Count() int

	// Topics returns all topics with retained messages.
	Topics() []string
}
