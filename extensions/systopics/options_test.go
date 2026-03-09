package systopics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	t.Run("default interval", func(t *testing.T) {
		cfg := config{interval: defaultInterval}
		assert.Equal(t, 10*time.Second, cfg.interval)
	})

	t.Run("WithInterval sets interval", func(t *testing.T) {
		cfg := config{}
		WithInterval(5 * time.Second)(&cfg)
		assert.Equal(t, 5*time.Second, cfg.interval)
	})

	t.Run("WithInterval ignores non-positive", func(t *testing.T) {
		cfg := config{interval: 10 * time.Second}
		WithInterval(0)(&cfg)
		assert.Equal(t, 10*time.Second, cfg.interval)

		WithInterval(-1 * time.Second)(&cfg)
		assert.Equal(t, 10*time.Second, cfg.interval)
	})

	t.Run("WithVersion sets version", func(t *testing.T) {
		cfg := config{}
		WithVersion("test 1.0")(&cfg)
		assert.Equal(t, "test 1.0", cfg.version)
	})

	t.Run("WithNamespaceMode sets mode", func(t *testing.T) {
		cfg := config{}
		WithNamespaceMode(NamespaceModeIsolated)(&cfg)
		assert.Equal(t, NamespaceModeIsolated, cfg.namespaceMode)
	})

	t.Run("WithTopicGroups enables specific groups", func(t *testing.T) {
		cfg := config{groups: make(map[TopicGroup]bool)}
		WithTopicGroups(TopicGroupClients, TopicGroupBytes)(&cfg)

		assert.True(t, cfg.groups[TopicGroupClients])
		assert.True(t, cfg.groups[TopicGroupBytes])
		assert.False(t, cfg.groups[TopicGroupBrokerInfo])
		assert.False(t, cfg.groups[TopicGroupLoad])
	})

	t.Run("TopicGroupAll enables all groups", func(t *testing.T) {
		cfg := config{groups: make(map[TopicGroup]bool)}
		WithTopicGroups(TopicGroupAll)(&cfg)

		assert.True(t, cfg.groups[TopicGroupBrokerInfo])
		assert.True(t, cfg.groups[TopicGroupClients])
		assert.True(t, cfg.groups[TopicGroupMessages])
		assert.True(t, cfg.groups[TopicGroupBytes])
		assert.True(t, cfg.groups[TopicGroupSubscriptions])
		assert.True(t, cfg.groups[TopicGroupRetained])
		assert.True(t, cfg.groups[TopicGroupLoad])
		assert.True(t, cfg.groups[TopicGroupNamespace])
		assert.True(t, cfg.groups[TopicGroupPackets])
		assert.False(t, cfg.groups[TopicGroupTopics], "TopicGroupTopics must not be enabled by TopicGroupAll")
	})

	t.Run("TopicGroupAll with explicit risky group", func(t *testing.T) {
		cfg := config{groups: make(map[TopicGroup]bool)}
		WithTopicGroups(TopicGroupAll, TopicGroupTopics)(&cfg)

		assert.True(t, cfg.groups[TopicGroupBrokerInfo])
		assert.True(t, cfg.groups[TopicGroupTopics])
	})

	t.Run("WithOnPublishError sets callback", func(t *testing.T) {
		cfg := config{}
		fn := func(string, error) {}
		WithOnPublishError(fn)(&cfg)
		assert.NotNil(t, cfg.onPublishError)
	})
}
