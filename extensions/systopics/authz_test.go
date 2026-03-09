package systopics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vitalvas/mqttv5"
)

func TestCheckAccess(t *testing.T) {
	pub := New(newMockBroker(), WithTopicGroups(TopicGroupBrokerInfo))

	t.Run("non_sys_topic_returns_nil", func(t *testing.T) {
		result := pub.CheckAccess(&mqttv5.AuthzContext{
			Topic:  "some/topic",
			Action: mqttv5.AuthzActionSubscribe,
		})
		assert.Nil(t, result)
	})

	t.Run("publish_denied", func(t *testing.T) {
		result := pub.CheckAccess(&mqttv5.AuthzContext{
			Topic:  TopicVersion,
			Action: mqttv5.AuthzActionPublish,
		})
		assert.NotNil(t, result)
		assert.False(t, result.Allowed)
		assert.Equal(t, mqttv5.ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("subscribe_allowed", func(t *testing.T) {
		for _, topic := range []string{
			TopicVersion,
			TopicClientsConnected,
			TopicMessagesReceived,
			TopicBytesReceived,
			TopicSubscriptionsCount,
			TopicRetainedMessagesCount,
			TopicLoadMessagesRecv1min,
			TopicNamespacesCount,
			TopicTopicsCount,
			TopicPacketsReceivedPrefix + "connect",
			"$SYS/broker/clients/namespace/tenant1/connected",
			"$SYS/broker/unknown/topic",
		} {
			result := pub.CheckAccess(&mqttv5.AuthzContext{
				Topic:  topic,
				Action: mqttv5.AuthzActionSubscribe,
			})
			assert.NotNil(t, result, topic)
			assert.True(t, result.Allowed, topic)
			assert.Equal(t, mqttv5.QoS0, result.MaxQoS, topic)
		}
	})

	t.Run("wildcard_subscribe_allowed", func(t *testing.T) {
		for _, filter := range []string{
			"$SYS/#",
			"$SYS/broker/clients/+",
			"$SYS/broker/packets/received/+",
		} {
			result := pub.CheckAccess(&mqttv5.AuthzContext{
				Topic:  filter,
				Action: mqttv5.AuthzActionSubscribe,
			})
			assert.NotNil(t, result, filter)
			assert.True(t, result.Allowed, filter)
		}
	})
}
