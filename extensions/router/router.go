package router

import (
	"regexp"
	"sync"

	"github.com/vitalvas/mqttv5"
)

// Handler processes an MQTT message.
type Handler func(msg *mqttv5.Message)

// userPropertyMatcher holds regexp patterns for matching user properties.
type userPropertyMatcher struct {
	keyPattern   *regexp.Regexp
	valuePattern *regexp.Regexp
}

// Condition defines filtering criteria for message routing.
type Condition struct {
	topicFilter         *string
	namespace           *string
	qos                 *byte
	contentTypeRegexp   *regexp.Regexp
	clientIDRegexp      *regexp.Regexp
	responseTopicRegexp *regexp.Regexp
	userProperties      []userPropertyMatcher
}

// ConditionOption configures a Condition.
type ConditionOption func(*Condition)

// WithTopic sets the topic filter for message matching.
// Supports MQTT wildcards: + (single level) and # (multi level).
func WithTopic(filter string) ConditionOption {
	return func(c *Condition) {
		c.topicFilter = &filter
	}
}

// WithNamespace filters messages by namespace for multi-tenancy.
func WithNamespace(namespace string) ConditionOption {
	return func(c *Condition) {
		c.namespace = &namespace
	}
}

// WithQoS filters messages by QoS level.
func WithQoS(qos byte) ConditionOption {
	return func(c *Condition) {
		c.qos = &qos
	}
}

// WithContentType filters messages by content type regexp pattern.
func WithContentType(pattern *regexp.Regexp) ConditionOption {
	return func(c *Condition) {
		c.contentTypeRegexp = pattern
	}
}

// WithClientID filters messages by client ID regexp pattern.
func WithClientID(pattern *regexp.Regexp) ConditionOption {
	return func(c *Condition) {
		c.clientIDRegexp = pattern
	}
}

// WithResponseTopic filters messages by response topic regexp pattern.
func WithResponseTopic(pattern *regexp.Regexp) ConditionOption {
	return func(c *Condition) {
		c.responseTopicRegexp = pattern
	}
}

// WithUserProperty filters messages by user property key/value regexp patterns.
// Both key and value must match for the condition to pass.
// Can be called multiple times to match multiple properties.
func WithUserProperty(keyPattern, valuePattern *regexp.Regexp) ConditionOption {
	return func(c *Condition) {
		c.userProperties = append(c.userProperties, userPropertyMatcher{
			keyPattern:   keyPattern,
			valuePattern: valuePattern,
		})
	}
}

// registration holds a handler with its conditions.
type registration struct {
	handler   Handler
	condition Condition
}

// Router dispatches messages to handlers based on conditions.
// Supports MQTT wildcards: + (single level) and # (multi level).
type Router struct {
	mu       sync.RWMutex
	handlers []registration
}

// New creates a new Router.
func New() *Router {
	return &Router{
		handlers: make([]registration, 0),
	}
}

// Handle registers a handler with optional conditions.
// Use WithTopic to specify the topic filter with MQTT wildcards (+ and #).
//
// Examples:
//
//	r.Handle(handler, WithTopic("sensors/#"))
//	r.Handle(handler, WithTopic("sensors/#"), WithQoS(1))
//	r.Handle(handler, WithTopic("sensors/#"), WithContentType("application/json"))
//	r.Handle(handler, WithTopic("sensors/#"), WithClientID(regexp.MustCompile(`^sensor-`)))
func (r *Router) Handle(handler Handler, opts ...ConditionOption) {
	var cond Condition
	for _, opt := range opts {
		opt(&cond)
	}

	r.mu.Lock()
	r.handlers = append(r.handlers, registration{
		handler:   handler,
		condition: cond,
	})
	r.mu.Unlock()
}

// matches checks if a condition matches the message.
func (c *Condition) matches(msg *mqttv5.Message) bool {
	if c.topicFilter != nil && !mqttv5.TopicMatch(*c.topicFilter, msg.Topic) {
		return false
	}
	if c.namespace != nil && *c.namespace != msg.Namespace {
		return false
	}
	if c.qos != nil && *c.qos != msg.QoS {
		return false
	}
	if c.contentTypeRegexp != nil && !c.contentTypeRegexp.MatchString(msg.ContentType) {
		return false
	}
	if c.clientIDRegexp != nil && !c.clientIDRegexp.MatchString(msg.ClientID) {
		return false
	}
	if c.responseTopicRegexp != nil && !c.responseTopicRegexp.MatchString(msg.ResponseTopic) {
		return false
	}
	if len(c.userProperties) > 0 && !c.matchUserProperties(msg.UserProperties) {
		return false
	}
	return true
}

// matchUserProperties checks if all user property matchers find a match.
func (c *Condition) matchUserProperties(props []mqttv5.StringPair) bool {
	for _, matcher := range c.userProperties {
		found := false
		for _, prop := range props {
			if matcher.keyPattern.MatchString(prop.Key) && matcher.valuePattern.MatchString(prop.Value) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Route dispatches a message to all matching handlers.
// Multiple handlers may be called if multiple conditions match.
func (r *Router) Route(msg *mqttv5.Message) {
	if msg == nil {
		return
	}

	r.mu.RLock()
	var matched []Handler
	for _, reg := range r.handlers {
		if reg.condition.matches(msg) {
			matched = append(matched, reg.handler)
		}
	}
	r.mu.RUnlock()

	for _, handler := range matched {
		handler(msg)
	}
}

// Filters returns all unique registered topic filters.
func (r *Router) Filters() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, reg := range r.handlers {
		if reg.condition.topicFilter != nil {
			seen[*reg.condition.topicFilter] = struct{}{}
		}
	}

	filters := make([]string, 0, len(seen))
	for filter := range seen {
		filters = append(filters, filter)
	}
	return filters
}

// Len returns the number of registered handlers.
func (r *Router) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}

// Clear removes all handlers.
func (r *Router) Clear() {
	r.mu.Lock()
	r.handlers = r.handlers[:0]
	r.mu.Unlock()
}

// MessageHandler returns a handler function compatible with mqttv5.MessageHandler.
// Use this with client.Subscribe() or as a general message dispatcher.
func (r *Router) MessageHandler() mqttv5.MessageHandler {
	return func(msg *mqttv5.Message) {
		r.Route(msg)
	}
}
