package mqttv5

import (
	"sync"
)

// SubscriptionEntry holds a subscription with its owner.
type SubscriptionEntry struct {
	ClientID     string
	Subscription Subscription
}

// SubscriptionManager manages subscriptions with MQTT v5.0 options enforcement.
type SubscriptionManager struct {
	mu            sync.RWMutex
	matcher       *TopicMatcher
	subscriptions map[string][]SubscriptionEntry // clientID -> subscriptions
}

// NewSubscriptionManager creates a new subscription manager.
func NewSubscriptionManager() *SubscriptionManager {
	return &SubscriptionManager{
		matcher:       NewTopicMatcher(),
		subscriptions: make(map[string][]SubscriptionEntry),
	}
}

// Subscribe adds a subscription for a client.
func (m *SubscriptionManager) Subscribe(clientID string, sub Subscription) error {
	if err := ValidateTopicFilter(sub.TopicFilter); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry := SubscriptionEntry{
		ClientID:     clientID,
		Subscription: sub,
	}

	// Remove existing subscription with same filter
	m.removeSubscriptionLocked(clientID, sub.TopicFilter)

	// Add to matcher
	if err := m.matcher.Subscribe(sub.TopicFilter, entry); err != nil {
		return err
	}

	// Add to client's subscriptions
	m.subscriptions[clientID] = append(m.subscriptions[clientID], entry)

	return nil
}

// Unsubscribe removes a subscription for a client.
func (m *SubscriptionManager) Unsubscribe(clientID string, filter string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.removeSubscriptionLocked(clientID, filter)
}

func (m *SubscriptionManager) removeSubscriptionLocked(clientID string, filter string) bool {
	subs := m.subscriptions[clientID]
	for i, entry := range subs {
		if entry.Subscription.TopicFilter == filter {
			// Remove from matcher
			m.matcher.Unsubscribe(filter, entry)

			// Remove from client's list
			m.subscriptions[clientID] = append(subs[:i], subs[i+1:]...)
			return true
		}
	}
	return false
}

// UnsubscribeAll removes all subscriptions for a client.
func (m *SubscriptionManager) UnsubscribeAll(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	subs := m.subscriptions[clientID]
	for _, entry := range subs {
		m.matcher.Unsubscribe(entry.Subscription.TopicFilter, entry)
	}
	delete(m.subscriptions, clientID)
}

// GetSubscriptions returns all subscriptions for a client.
func (m *SubscriptionManager) GetSubscriptions(clientID string) []Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := m.subscriptions[clientID]
	subs := make([]Subscription, len(entries))
	for i, entry := range entries {
		subs[i] = entry.Subscription
	}
	return subs
}

// Match returns all matching subscriptions for a topic.
func (m *SubscriptionManager) Match(topic string) []SubscriptionEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	matches := m.matcher.Match(topic)
	entries := make([]SubscriptionEntry, 0, len(matches))
	for _, match := range matches {
		if entry, ok := match.(SubscriptionEntry); ok {
			entries = append(entries, entry)
		}
	}
	return entries
}

// MatchForDelivery returns subscriptions that should receive a message.
// It applies NoLocal filtering and deduplication per client.
func (m *SubscriptionManager) MatchForDelivery(topic string, publisherID string) []SubscriptionEntry {
	matches := m.Match(topic)

	// Deduplicate by clientID, keeping highest QoS
	clientBest := make(map[string]SubscriptionEntry)

	for _, entry := range matches {
		// NoLocal: skip if publisher is subscriber and NoLocal is set
		if entry.Subscription.NoLocal && entry.ClientID == publisherID {
			continue
		}

		existing, ok := clientBest[entry.ClientID]
		if !ok || entry.Subscription.QoS > existing.Subscription.QoS {
			clientBest[entry.ClientID] = entry
		}
	}

	result := make([]SubscriptionEntry, 0, len(clientBest))
	for _, entry := range clientBest {
		result = append(result, entry)
	}
	return result
}

// ShouldSendRetained checks if retained messages should be sent based on RetainHandling.
// retainHandling: 0 = send on subscribe, 1 = send if new subscription, 2 = don't send
// isNewSubscription: true if this is a new subscription (not an update)
func ShouldSendRetained(retainHandling byte, isNewSubscription bool) bool {
	switch retainHandling {
	case 0:
		return true
	case 1:
		return isNewSubscription
	case 2:
		return false
	default:
		return true
	}
}

// GetDeliveryRetain determines if the retain flag should be set on delivery.
// If RetainAsPublished is true, preserve the original retain flag.
// Otherwise, set retain to false (default MQTT behavior).
func GetDeliveryRetain(sub Subscription, originalRetain bool) bool {
	if sub.RetainAsPublish {
		return originalRetain
	}
	return false
}

// Count returns the total number of subscriptions.
func (m *SubscriptionManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, subs := range m.subscriptions {
		count += len(subs)
	}
	return count
}

// ClientCount returns the number of clients with subscriptions.
func (m *SubscriptionManager) ClientCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.subscriptions)
}
