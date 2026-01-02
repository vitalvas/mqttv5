package mqttv5

import (
	"sync"
	"sync/atomic"
)

// SubscriptionEntry holds a subscription with its owner.
type SubscriptionEntry struct {
	ClientID        string
	Namespace       string
	Subscription    Subscription
	SubscriptionIDs []uint32 // Aggregated subscription IDs from all matching subscriptions
	ShareGroup      string   // Non-empty for shared subscriptions ($share/{group}/{filter})
}

// MatchSubscriber implements SubscriberMatcher for SubscriptionEntry comparison.
// Two entries match if they have the same ClientID, Namespace, and TopicFilter.
func (e SubscriptionEntry) MatchSubscriber(other any) bool {
	otherEntry, ok := other.(SubscriptionEntry)
	if !ok {
		return false
	}
	return e.ClientID == otherEntry.ClientID &&
		e.Namespace == otherEntry.Namespace &&
		e.Subscription.TopicFilter == otherEntry.Subscription.TopicFilter
}

// SubscriptionManager manages subscriptions with MQTT v5.0 options enforcement.
type SubscriptionManager struct {
	mu               sync.RWMutex
	matcher          *TopicMatcher
	subscriptions    map[string][]SubscriptionEntry // clientID -> subscriptions
	sharedGroupIndex map[string][]SubscriptionEntry // shareGroup -> subscriptions for round-robin
	sharedCounters   map[string]*atomic.Uint64      // shareGroup -> round-robin counter
}

// NewSubscriptionManager creates a new subscription manager.
func NewSubscriptionManager() *SubscriptionManager {
	return &SubscriptionManager{
		matcher:          NewTopicMatcher(),
		subscriptions:    make(map[string][]SubscriptionEntry),
		sharedGroupIndex: make(map[string][]SubscriptionEntry),
		sharedCounters:   make(map[string]*atomic.Uint64),
	}
}

// Subscribe adds a subscription for a client.
func (m *SubscriptionManager) Subscribe(clientID, namespace string, sub Subscription) error {
	if err := ValidateTopicFilter(sub.TopicFilter); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry := SubscriptionEntry{
		ClientID:     clientID,
		Namespace:    namespace,
		Subscription: sub,
	}

	// Check for shared subscription
	sharedSub, _ := ParseSharedSubscription(sub.TopicFilter)
	var matchFilter string
	var shareGroupKey string

	if sharedSub != nil {
		// Shared subscription: use the underlying filter for matching
		matchFilter = sharedSub.TopicFilter
		entry.ShareGroup = sharedSub.ShareName
		// Create a unique key for the share group + filter combination (namespace-scoped)
		shareGroupKey = NamespaceKey(namespace, sharedSub.ShareName+"/"+sharedSub.TopicFilter)
	} else {
		// Regular subscription
		matchFilter = sub.TopicFilter
	}

	// Remove existing subscription with same filter
	m.removeSubscriptionLocked(clientID, namespace, sub.TopicFilter)

	// Add to matcher using the effective filter (underlying filter for shared subscriptions)
	if err := m.matcher.Subscribe(matchFilter, entry); err != nil {
		return err
	}

	// Add to client's subscriptions (namespace-scoped key)
	key := NamespaceKey(namespace, clientID)
	m.subscriptions[key] = append(m.subscriptions[key], entry)

	// Add to shared group index if shared subscription
	if shareGroupKey != "" {
		m.sharedGroupIndex[shareGroupKey] = append(m.sharedGroupIndex[shareGroupKey], entry)
		if _, exists := m.sharedCounters[shareGroupKey]; !exists {
			m.sharedCounters[shareGroupKey] = &atomic.Uint64{}
		}
	}

	return nil
}

// Unsubscribe removes a subscription for a client.
func (m *SubscriptionManager) Unsubscribe(clientID, namespace, filter string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.removeSubscriptionLocked(clientID, namespace, filter)
}

func (m *SubscriptionManager) removeSubscriptionLocked(clientID, namespace, filter string) bool {
	key := NamespaceKey(namespace, clientID)
	subs := m.subscriptions[key]
	for i, entry := range subs {
		if entry.Subscription.TopicFilter == filter {
			// Determine the match filter for removal from matcher
			matchFilter := filter
			var shareGroupKey string
			if sharedSub, _ := ParseSharedSubscription(filter); sharedSub != nil {
				matchFilter = sharedSub.TopicFilter
				shareGroupKey = NamespaceKey(namespace, sharedSub.ShareName+"/"+sharedSub.TopicFilter)
			}

			// Remove from matcher
			m.matcher.Unsubscribe(matchFilter, entry)

			// Remove from client's list
			m.subscriptions[key] = append(subs[:i], subs[i+1:]...)

			// Remove from shared group index if shared subscription
			if shareGroupKey != "" {
				groupSubs := m.sharedGroupIndex[shareGroupKey]
				for j, groupEntry := range groupSubs {
					if groupEntry.ClientID == clientID && groupEntry.Namespace == namespace && groupEntry.Subscription.TopicFilter == filter {
						m.sharedGroupIndex[shareGroupKey] = append(groupSubs[:j], groupSubs[j+1:]...)
						break
					}
				}
				// Clean up empty shared group
				if len(m.sharedGroupIndex[shareGroupKey]) == 0 {
					delete(m.sharedGroupIndex, shareGroupKey)
					delete(m.sharedCounters, shareGroupKey)
				}
			}

			return true
		}
	}
	return false
}

// UnsubscribeAll removes all subscriptions for a client.
func (m *SubscriptionManager) UnsubscribeAll(clientID, namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := NamespaceKey(namespace, clientID)
	subs := m.subscriptions[key]
	for _, entry := range subs {
		// Determine the match filter
		matchFilter := entry.Subscription.TopicFilter
		var shareGroupKey string
		if sharedSub, _ := ParseSharedSubscription(entry.Subscription.TopicFilter); sharedSub != nil {
			matchFilter = sharedSub.TopicFilter
			shareGroupKey = NamespaceKey(namespace, sharedSub.ShareName+"/"+sharedSub.TopicFilter)
		}

		m.matcher.Unsubscribe(matchFilter, entry)

		// Remove from shared group index if shared subscription
		if shareGroupKey != "" {
			groupSubs := m.sharedGroupIndex[shareGroupKey]
			for j, groupEntry := range groupSubs {
				if groupEntry.ClientID == clientID && groupEntry.Namespace == namespace {
					m.sharedGroupIndex[shareGroupKey] = append(groupSubs[:j], groupSubs[j+1:]...)
					break
				}
			}
			if len(m.sharedGroupIndex[shareGroupKey]) == 0 {
				delete(m.sharedGroupIndex, shareGroupKey)
				delete(m.sharedCounters, shareGroupKey)
			}
		}
	}
	delete(m.subscriptions, key)
}

// HasSubscription checks if a client has a subscription to the given filter.
func (m *SubscriptionManager) HasSubscription(clientID, namespace, filter string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := NamespaceKey(namespace, clientID)
	for _, entry := range m.subscriptions[key] {
		if entry.Subscription.TopicFilter == filter {
			return true
		}
	}
	return false
}

// GetSubscriptions returns all subscriptions for a client.
func (m *SubscriptionManager) GetSubscriptions(clientID, namespace string) []Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := NamespaceKey(namespace, clientID)
	entries := m.subscriptions[key]
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

// clientMatchState holds aggregated state for a client's matching subscriptions.
type clientMatchState struct {
	BestEntry       SubscriptionEntry
	SubIDs          []uint32
	RetainAsPublish bool // True if any matching subscription has RetainAsPublish set
}

// sharedGroupState holds state for a shared subscription group.
type sharedGroupState struct {
	Entries []SubscriptionEntry
}

// MatchForDelivery returns subscriptions that should receive a message.
// It applies namespace filtering, NoLocal filtering, deduplication per client, and shared subscription load-balancing.
// Per MQTT v5 spec, when multiple subscriptions match:
// - Deliver once at max QoS
// - Include ALL matching subscription identifiers
// - Apply RetainAsPublished if any matching subscription has it set
// - For shared subscriptions, deliver to only one subscriber per share group (round-robin)
func (m *SubscriptionManager) MatchForDelivery(topic, publisherID, publisherNamespace string) []SubscriptionEntry {
	matches := m.Match(topic)

	// Separate shared and non-shared subscriptions
	var regularMatches []SubscriptionEntry
	sharedGroups := make(map[string]*sharedGroupState) // shareGroupKey -> state

	for _, entry := range matches {
		// Namespace isolation: skip entries from different namespaces
		if entry.Namespace != publisherNamespace {
			continue
		}

		// NoLocal: skip if publisher is subscriber and NoLocal is set
		if entry.Subscription.NoLocal && entry.ClientID == publisherID {
			continue
		}

		if entry.ShareGroup != "" {
			// Shared subscription - group by share group + effective filter (namespace-scoped)
			sharedSub, _ := ParseSharedSubscription(entry.Subscription.TopicFilter)
			if sharedSub != nil {
				shareGroupKey := NamespaceKey(entry.Namespace, sharedSub.ShareName+"/"+sharedSub.TopicFilter)
				if sharedGroups[shareGroupKey] == nil {
					sharedGroups[shareGroupKey] = &sharedGroupState{}
				}
				sharedGroups[shareGroupKey].Entries = append(sharedGroups[shareGroupKey].Entries, entry)
			}
		} else {
			// Regular subscription
			regularMatches = append(regularMatches, entry)
		}
	}

	// Aggregate regular subscriptions per clientID: keep highest QoS but collect all subscription IDs
	clientStates := make(map[string]*clientMatchState)

	for _, entry := range regularMatches {
		state, ok := clientStates[entry.ClientID]
		if !ok {
			state = &clientMatchState{
				BestEntry:       entry,
				RetainAsPublish: entry.Subscription.RetainAsPublish,
			}
			clientStates[entry.ClientID] = state
		} else {
			// Update to higher QoS if this subscription has higher QoS
			if entry.Subscription.QoS > state.BestEntry.Subscription.QoS {
				state.BestEntry = entry
			}
			// Aggregate RetainAsPublish (true if any subscription has it)
			if entry.Subscription.RetainAsPublish {
				state.RetainAsPublish = true
			}
		}

		// Collect subscription identifier if present
		if entry.Subscription.SubscriptionID > 0 {
			state.SubIDs = append(state.SubIDs, entry.Subscription.SubscriptionID)
		}
	}

	// Build result with regular subscriptions
	result := make([]SubscriptionEntry, 0, len(clientStates)+len(sharedGroups))
	for _, state := range clientStates {
		entry := state.BestEntry
		entry.Subscription.RetainAsPublish = state.RetainAsPublish
		entry.SubscriptionIDs = state.SubIDs
		result = append(result, entry)
	}

	// Handle shared subscriptions - select one subscriber per group (round-robin)
	m.mu.RLock()
	for shareGroupKey, group := range sharedGroups {
		if len(group.Entries) == 0 {
			continue
		}

		// Get or create counter for this group
		counter, exists := m.sharedCounters[shareGroupKey]
		if !exists {
			// Counter doesn't exist - just pick first subscriber
			result = append(result, group.Entries[0])
			continue
		}

		// Round-robin selection
		idx := counter.Add(1) - 1
		selectedIdx := int(idx % uint64(len(group.Entries)))
		selectedEntry := group.Entries[selectedIdx]

		// Add subscription ID if present
		if selectedEntry.Subscription.SubscriptionID > 0 {
			selectedEntry.SubscriptionIDs = []uint32{selectedEntry.Subscription.SubscriptionID}
		}

		result = append(result, selectedEntry)
	}
	m.mu.RUnlock()

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
