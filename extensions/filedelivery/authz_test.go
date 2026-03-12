package filedelivery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

func TestCheckAccess(t *testing.T) {
	t.Run("not a stream topic", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "devices/dev1/telemetry",
			Action:   mqttv5.AuthzActionPublish,
		})
		assert.Nil(t, result)
	})

	t.Run("publish to own describe", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/streams/s1/describe/json",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("publish to own get", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/streams/s1/get/json",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("subscribe to own description", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/streams/s1/description/json",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("subscribe to own data", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/streams/s1/data/json",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("subscribe to own rejected", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/streams/s1/rejected/json",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("publish to own create", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/streams/s1/create/json",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("publish to own data for upload", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/streams/s1/data/json",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("subscribe to own accepted", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/streams/s1/accepted/json",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("different clientID denied", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev2/streams/s1/describe/json",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
		assert.Equal(t, mqttv5.ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("publish to subscribe-only topic denied", func(t *testing.T) {
		h := NewHandler()
		for _, suffix := range []string{suffixDescription, suffixRejected, suffixAccepted} {
			result := h.CheckAccess(&mqttv5.AuthzContext{
				ClientID: "dev1",
				Topic:    buildTopic("dev1", "s1", suffix),
				Action:   mqttv5.AuthzActionPublish,
			})
			require.NotNil(t, result, "suffix: %s", suffix)
			assert.False(t, result.Allowed, "suffix: %s", suffix)
		}
	})

	t.Run("subscribe to publish-only topic denied", func(t *testing.T) {
		h := NewHandler()
		for _, suffix := range []string{suffixDescribe, suffixGet, suffixCreate} {
			result := h.CheckAccess(&mqttv5.AuthzContext{
				ClientID: "dev1",
				Topic:    buildTopic("dev1", "s1", suffix),
				Action:   mqttv5.AuthzActionSubscribe,
			})
			require.NotNil(t, result, "suffix: %s", suffix)
			assert.False(t, result.Allowed, "suffix: %s", suffix)
		}
	})

	t.Run("unknown action denied", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/streams/s1/describe/json",
			Action:   mqttv5.AuthzAction(99),
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("shadow topic returns nil", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		assert.Nil(t, result)
	})
}
