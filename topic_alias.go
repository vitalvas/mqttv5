package mqttv5

import (
	"errors"
	"sync"
)

var (
	ErrTopicAliasInvalid  = errors.New("topic alias invalid")
	ErrTopicAliasExceeded = errors.New("topic alias maximum exceeded")
	ErrTopicAliasNotFound = errors.New("topic alias not found")
)

// TopicAliasManager manages bidirectional topic alias mapping for a connection.
// Inbound aliases are set by the remote peer, outbound aliases are set locally.
type TopicAliasManager struct {
	mu           sync.RWMutex
	inbound      map[uint16]string // aliases received from remote
	outbound     map[string]uint16 // aliases we send to remote
	outboundNext uint16
	inboundMax   uint16 // max aliases we accept (our limit)
	outboundMax  uint16 // max aliases remote accepts (their limit)
}

// NewTopicAliasManager creates a new topic alias manager.
// inboundMax is the maximum aliases we accept from remote (sent in our CONNECT/CONNACK).
// outboundMax is the maximum aliases remote accepts (received in their CONNECT/CONNACK).
func NewTopicAliasManager(inboundMax, outboundMax uint16) *TopicAliasManager {
	return &TopicAliasManager{
		inbound:      make(map[uint16]string),
		outbound:     make(map[string]uint16),
		outboundNext: 1,
		inboundMax:   inboundMax,
		outboundMax:  outboundMax,
	}
}

// SetInbound registers an inbound alias (received from remote).
func (m *TopicAliasManager) SetInbound(alias uint16, topic string) error {
	if alias == 0 {
		return ErrTopicAliasInvalid
	}

	if m.inboundMax > 0 && alias > m.inboundMax {
		return ErrTopicAliasExceeded
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.inbound[alias] = topic
	return nil
}

// GetInbound resolves an inbound alias to topic name.
func (m *TopicAliasManager) GetInbound(alias uint16) (string, error) {
	if alias == 0 {
		return "", ErrTopicAliasInvalid
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	topic, ok := m.inbound[alias]
	if !ok {
		return "", ErrTopicAliasNotFound
	}

	return topic, nil
}

// GetOrCreateOutbound returns existing alias or creates new one for outbound topic.
// Returns 0 if no alias available or outbound aliases disabled.
func (m *TopicAliasManager) GetOrCreateOutbound(topic string) uint16 {
	if m.outboundMax == 0 {
		return 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check existing
	if alias, ok := m.outbound[topic]; ok {
		return alias
	}

	// Allocate new if available
	if m.outboundNext <= m.outboundMax {
		alias := m.outboundNext
		m.outbound[topic] = alias
		m.outboundNext++
		return alias
	}

	return 0
}

// GetOutbound returns existing outbound alias for topic.
// Returns 0 if no alias exists.
func (m *TopicAliasManager) GetOutbound(topic string) uint16 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.outbound[topic]
}

// SetOutboundMax updates the outbound maximum (from remote CONNACK).
func (m *TopicAliasManager) SetOutboundMax(maxVal uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.outboundMax = maxVal
}

// SetInboundMax updates the inbound maximum.
func (m *TopicAliasManager) SetInboundMax(maxVal uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.inboundMax = maxVal
}

// InboundMax returns the inbound alias maximum.
func (m *TopicAliasManager) InboundMax() uint16 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.inboundMax
}

// OutboundMax returns the outbound alias maximum.
func (m *TopicAliasManager) OutboundMax() uint16 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.outboundMax
}

// Clear removes all aliases.
func (m *TopicAliasManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.inbound = make(map[uint16]string)
	m.outbound = make(map[string]uint16)
	m.outboundNext = 1
}

// InboundCount returns the number of registered inbound aliases.
func (m *TopicAliasManager) InboundCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.inbound)
}

// OutboundCount returns the number of registered outbound aliases.
func (m *TopicAliasManager) OutboundCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.outbound)
}
