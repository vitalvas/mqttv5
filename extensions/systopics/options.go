package systopics

import "time"

// NamespaceMode controls how namespace-scoped statistics are published.
type NamespaceMode int

const (
	// NamespaceModeGlobal publishes broker-wide stats with per-namespace breakdown.
	NamespaceModeGlobal NamespaceMode = iota

	// NamespaceModeIsolated publishes stats scoped to each namespace separately.
	NamespaceModeIsolated
)

// TopicGroup identifies a group of $SYS topics that can be enabled or disabled.
type TopicGroup int

const (
	// TopicGroupBrokerInfo enables version, uptime, and timestamp topics.
	TopicGroupBrokerInfo TopicGroup = iota

	// TopicGroupClients enables client count topics (connected, total, maximum, disconnected).
	TopicGroupClients

	// TopicGroupMessages enables message count topics (received, sent, stored, publish/*).
	TopicGroupMessages

	// TopicGroupBytes enables byte count topics (received, sent).
	TopicGroupBytes

	// TopicGroupSubscriptions enables subscription count topic.
	TopicGroupSubscriptions

	// TopicGroupRetained enables retained message count topic.
	TopicGroupRetained

	// TopicGroupLoad enables load average topics (1min, 5min, 15min).
	TopicGroupLoad

	// TopicGroupNamespace enables per-namespace topics.
	TopicGroupNamespace

	// TopicGroupPackets enables packet type counters.
	TopicGroupPackets

	topicGroupCount // sentinel for iteration; TopicGroupAll enables groups before this.

	// TopicGroupAll enables all safe topic groups (excludes high-cardinality groups).
	TopicGroupAll TopicGroup = -1

	// TopicGroupTopics enables topic-level metrics (topics/count).
	// This group is excluded from TopicGroupAll because per-topic metrics
	// can produce high cardinality. Enable it explicitly.
	TopicGroupTopics TopicGroup = -2
)

const defaultInterval = 10 * time.Second

type config struct {
	interval       time.Duration
	version        string
	namespaceMode  NamespaceMode
	groups         map[TopicGroup]bool
	onPublishError func(topic string, err error)
}

// Option configures the Publisher.
type Option func(*config)

// WithInterval sets the publishing interval. Default is 10 seconds.
func WithInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.interval = d
		}
	}
}

// WithVersion sets the broker version string published on $SYS/broker/version.
func WithVersion(v string) Option {
	return func(c *config) {
		c.version = v
	}
}

// WithNamespaceMode sets the namespace publishing mode.
func WithNamespaceMode(mode NamespaceMode) Option {
	return func(c *config) {
		c.namespaceMode = mode
	}
}

// WithOnPublishError sets a callback invoked when a publish to a $SYS topic fails.
// This is useful for detecting misconfigurations such as retained messages being disabled.
func WithOnPublishError(fn func(topic string, err error)) Option {
	return func(c *config) {
		c.onPublishError = fn
	}
}

// WithTopicGroups enables the specified topic groups for publishing.
// All groups are disabled by default. Use TopicGroupAll to enable all groups.
func WithTopicGroups(groups ...TopicGroup) Option {
	return func(c *config) {
		for _, g := range groups {
			if g == TopicGroupAll {
				for ag := range topicGroupCount {
					c.groups[ag] = true
				}

				continue
			}

			c.groups[g] = true
		}
	}
}
