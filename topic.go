package mqttv5

import (
	"errors"
	"strings"
	"unicode/utf8"
)

var (
	ErrInvalidTopicName   = errors.New("invalid topic name")
	ErrInvalidTopicFilter = errors.New("invalid topic filter")
	ErrEmptyTopic         = errors.New("topic cannot be empty")
)

const (
	topicSeparator      = '/'
	singleLevelWildcard = '+'
	multiLevelWildcard  = '#'
)

// ValidateTopicName validates a topic name according to MQTT v5.0 specification.
// Topic names cannot contain wildcards and must be valid UTF-8.
// MQTT v5.0 spec: Section 4.7.1
func ValidateTopicName(topic string) error {
	if topic == "" {
		return ErrEmptyTopic
	}

	if !utf8.ValidString(topic) {
		return ErrInvalidTopicName
	}

	// Check for null character and wildcards
	for _, r := range topic {
		if r == 0 {
			return ErrInvalidTopicName
		}
		if r == singleLevelWildcard || r == multiLevelWildcard {
			return ErrInvalidTopicName
		}
	}

	return nil
}

// ValidateTopicFilter validates a topic filter according to MQTT v5.0 specification.
// Topic filters can contain wildcards but must follow wildcard rules.
// MQTT v5.0 spec: Section 4.7.1
func ValidateTopicFilter(filter string) error {
	if filter == "" {
		return ErrEmptyTopic
	}

	if !utf8.ValidString(filter) {
		return ErrInvalidTopicFilter
	}

	// Check for null character
	for _, r := range filter {
		if r == 0 {
			return ErrInvalidTopicFilter
		}
	}

	levels := strings.Split(filter, string(topicSeparator))

	for i, level := range levels {
		// Single-level wildcard must occupy entire level
		if strings.Contains(level, string(singleLevelWildcard)) {
			if level != string(singleLevelWildcard) {
				return ErrInvalidTopicFilter
			}
		}

		// Multi-level wildcard must be last level and occupy entire level
		if strings.Contains(level, string(multiLevelWildcard)) {
			if level != string(multiLevelWildcard) {
				return ErrInvalidTopicFilter
			}
			if i != len(levels)-1 {
				return ErrInvalidTopicFilter
			}
		}
	}

	return nil
}

// TopicMatch checks if a topic name matches a topic filter.
// MQTT v5.0 spec: Section 4.7
func TopicMatch(filter, topic string) bool {
	if filter == "" || topic == "" {
		return false
	}

	// System topics ($SYS/) don't match wildcards at root level
	if len(topic) > 0 && topic[0] == '$' {
		if len(filter) > 0 && (filter[0] == singleLevelWildcard || filter[0] == multiLevelWildcard) {
			return false
		}
	}

	filterLevels := strings.Split(filter, string(topicSeparator))
	topicLevels := strings.Split(topic, string(topicSeparator))

	return matchLevels(filterLevels, topicLevels)
}

func matchLevels(filterLevels, topicLevels []string) bool {
	for i, filterLevel := range filterLevels {
		// Multi-level wildcard matches everything remaining
		if filterLevel == string(multiLevelWildcard) {
			return true
		}

		// If we've run out of topic levels, no match
		if i >= len(topicLevels) {
			return false
		}

		// Single-level wildcard matches any single level
		if filterLevel == string(singleLevelWildcard) {
			continue
		}

		// Exact match required
		if filterLevel != topicLevels[i] {
			return false
		}
	}

	// All filter levels matched; topic must also be exhausted
	return len(filterLevels) == len(topicLevels)
}

// IsSystemTopic returns true if the topic is a system topic ($SYS/).
func IsSystemTopic(topic string) bool {
	return strings.HasPrefix(topic, "$SYS/") || topic == "$SYS"
}

// SharedSubscription represents a parsed shared subscription.
// MQTT v5.0 spec: Section 4.8.2
type SharedSubscription struct {
	ShareName   string
	TopicFilter string
}

// ParseSharedSubscription parses a shared subscription filter.
// Shared subscriptions have the format: $share/{ShareName}/{TopicFilter}
// MQTT v5.0 spec: Section 4.8.2
func ParseSharedSubscription(filter string) (*SharedSubscription, error) {
	const prefix = "$share/"

	if !strings.HasPrefix(filter, prefix) {
		return nil, nil // Not a shared subscription
	}

	rest := filter[len(prefix):]
	idx := strings.Index(rest, string(topicSeparator))
	if idx <= 0 {
		return nil, ErrInvalidTopicFilter
	}

	shareName := rest[:idx]
	topicFilter := rest[idx+1:]

	if shareName == "" {
		return nil, ErrInvalidTopicFilter
	}

	if topicFilter == "" {
		return nil, ErrInvalidTopicFilter
	}

	// Validate the topic filter part
	if err := ValidateTopicFilter(topicFilter); err != nil {
		return nil, err
	}

	return &SharedSubscription{
		ShareName:   shareName,
		TopicFilter: topicFilter,
	}, nil
}

// TopicMatcher provides efficient topic matching with multiple subscriptions.
type TopicMatcher struct {
	root *topicNode
}

type topicNode struct {
	children    map[string]*topicNode
	subscribers []any
	hasWildcard bool
	hasMulti    bool
}

// NewTopicMatcher creates a new topic matcher.
func NewTopicMatcher() *TopicMatcher {
	return &TopicMatcher{
		root: &topicNode{
			children: make(map[string]*topicNode),
		},
	}
}

// Subscribe adds a subscriber for the given topic filter.
func (m *TopicMatcher) Subscribe(filter string, subscriber any) error {
	if err := ValidateTopicFilter(filter); err != nil {
		return err
	}

	levels := strings.Split(filter, string(topicSeparator))
	node := m.root

	for _, level := range levels {
		if node.children == nil {
			node.children = make(map[string]*topicNode)
		}

		child, ok := node.children[level]
		if !ok {
			child = &topicNode{
				children: make(map[string]*topicNode),
			}
			node.children[level] = child

			if level == string(singleLevelWildcard) {
				node.hasWildcard = true
			} else if level == string(multiLevelWildcard) {
				node.hasMulti = true
			}
		}
		node = child
	}

	node.subscribers = append(node.subscribers, subscriber)
	return nil
}

// SubscriberMatcher is an interface for comparing subscribers.
// This is needed because some subscriber types (like SubscriptionEntry)
// contain slices which make them incomparable with ==.
type SubscriberMatcher interface {
	MatchSubscriber(other any) bool
}

// Unsubscribe removes a subscriber for the given topic filter.
func (m *TopicMatcher) Unsubscribe(filter string, subscriber any) error {
	if err := ValidateTopicFilter(filter); err != nil {
		return err
	}

	levels := strings.Split(filter, string(topicSeparator))
	node := m.root

	for _, level := range levels {
		child, ok := node.children[level]
		if !ok {
			return nil // Not subscribed
		}
		node = child
	}

	// Remove subscriber using custom matching if available
	matcher, hasMatcher := subscriber.(SubscriberMatcher)
	for i, s := range node.subscribers {
		var match bool
		if hasMatcher {
			match = matcher.MatchSubscriber(s)
		} else {
			// Fallback to reflect.DeepEqual for uncomparable types
			match = subscriberEqual(subscriber, s)
		}
		if match {
			node.subscribers = append(node.subscribers[:i], node.subscribers[i+1:]...)
			break
		}
	}

	return nil
}

// subscriberEqual compares two subscribers, handling uncomparable types.
func subscriberEqual(a, b any) bool {
	defer func() {
		recover() // Ignore panics from comparing uncomparable types
	}()
	return a == b
}

// Match returns all subscribers matching the given topic.
func (m *TopicMatcher) Match(topic string) []any {
	if err := ValidateTopicName(topic); err != nil {
		return nil
	}

	levels := strings.Split(topic, string(topicSeparator))
	isSystemTopic := len(topic) > 0 && topic[0] == '$'

	var subscribers []any
	m.matchNode(m.root, levels, 0, isSystemTopic, &subscribers)
	return subscribers
}

func (m *TopicMatcher) matchNode(node *topicNode, levels []string, idx int, isSystemTopic bool, subscribers *[]any) {
	if node == nil {
		return
	}

	// Multi-level wildcard matches everything remaining
	if !isSystemTopic || idx > 0 {
		if child, ok := node.children[string(multiLevelWildcard)]; ok {
			*subscribers = append(*subscribers, child.subscribers...)
		}
	}

	// All levels matched
	if idx >= len(levels) {
		*subscribers = append(*subscribers, node.subscribers...)
		return
	}

	level := levels[idx]

	// Exact match
	if child, ok := node.children[level]; ok {
		m.matchNode(child, levels, idx+1, isSystemTopic, subscribers)
	}

	// Single-level wildcard (not for system topics at root)
	if !isSystemTopic || idx > 0 {
		if child, ok := node.children[string(singleLevelWildcard)]; ok {
			m.matchNode(child, levels, idx+1, isSystemTopic, subscribers)
		}
	}
}
