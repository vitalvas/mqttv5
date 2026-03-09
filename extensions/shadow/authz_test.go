package shadow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

func TestCheckAccess(t *testing.T) {
	t.Run("not a shadow topic", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "devices/dev1/telemetry",
			Action:   mqttv5.AuthzActionPublish,
		})
		assert.Nil(t, result)
	})

	t.Run("publish to own update", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("publish to own get", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/get",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("publish to own delete", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/delete",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("subscribe to own update/accepted", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update/accepted",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("subscribe to own update/rejected", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update/rejected",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("subscribe to own update/delta", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update/delta",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("subscribe to own update/documents", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update/documents",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("subscribe to own get/accepted", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/get/accepted",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("subscribe to own get/rejected", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/get/rejected",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("subscribe to own delete/accepted", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/delete/accepted",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("subscribe to own delete/rejected", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/delete/rejected",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("different clientID denied", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev2/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
		assert.Equal(t, mqttv5.ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("publish to subscribe-only topic denied", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update/accepted",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("publish to delta denied", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update/delta",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("subscribe to publish-only topic denied", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("subscribe to get denied", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/get",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("named shadow publish to update", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/name/config/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("named shadow subscribe to delta", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/name/config/update/delta",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("named shadow different clientID denied", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev2/shadow/name/config/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("publish to own list", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/list",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("subscribe to own list/accepted", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/list/accepted",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("subscribe to own list/rejected", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/list/rejected",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("subscribe to list denied", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/list",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("publish to list/accepted denied", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/list/accepted",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("unknown action denied", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update",
			Action:   mqttv5.AuthzAction(99),
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})
}

func TestCheckAccess_Shared(t *testing.T) {
	allowAll := func(_, _ string) bool { return true }

	t.Run("shared publish update allowed", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
		assert.Equal(t, mqttv5.QoS1, result.MaxQoS)
	})

	t.Run("shared publish get allowed", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/get",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("shared publish delete allowed", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/delete",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("shared subscribe delta allowed", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/update/delta",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("shared subscribe documents allowed", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/update/documents",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("shared named publish allowed", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/name/config/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("shared resolver deny", func(t *testing.T) {
		denyAll := func(_, _ string) bool { return false }
		h := NewHandler(WithSharedShadow(denyAll))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
		assert.Equal(t, mqttv5.ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("shared resolver selective access", func(t *testing.T) {
		selective := func(clientID, groupName string) bool {
			return clientID == "dev1" && groupName == "room-101"
		}

		h := NewHandler(WithSharedShadow(selective))

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)

		result = h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev2",
			Topic:    "$things/$shared/room-101/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)

		result = h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-102/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("shared disabled by default", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("shared publish to subscribe-only denied", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/update/accepted",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("shared subscribe to publish-only denied", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/update",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("shared does not require clientID match", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "any-device",
			Topic:    "$things/$shared/room-101/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("nil resolver allows publish get", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(nil))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/get",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("nil resolver denies publish update", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(nil))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("nil resolver denies publish delete", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(nil))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/delete",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("nil resolver denies publish list", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(nil))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/list",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("nil resolver allows subscribe", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(nil))
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/$shared/room-101/shadow/get/accepted",
			Action:   mqttv5.AuthzActionSubscribe,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})
}

func TestCheckAccess_FeatureFlags(t *testing.T) {
	t.Run("classic disabled by default", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("named disabled by default", func(t *testing.T) {
		h := NewHandler()
		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/name/config/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("classic enabled named disabled", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)

		result = h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/name/config/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("named enabled classic disabled", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)

		result = h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/name/config/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("both enabled", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithNamedShadow())

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)

		result = h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/name/config/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("named only allows list on classic topic", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/list",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("named only denies non-list on classic topic", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())

		for _, suffix := range []string{"update", "get", "delete"} {
			result := h.CheckAccess(&mqttv5.AuthzContext{
				ClientID: "dev1",
				Topic:    "$things/dev1/shadow/" + suffix,
				Action:   mqttv5.AuthzActionPublish,
			})
			require.NotNil(t, result, "suffix: %s", suffix)
			assert.False(t, result.Allowed, "suffix: %s", suffix)
		}
	})

	t.Run("neither enabled denies list", func(t *testing.T) {
		h := NewHandler()

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "dev1",
			Topic:    "$things/dev1/shadow/list",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})
}

func TestCheckAccess_ClientIDResolver(t *testing.T) {
	resolver := func(clientID string) string {
		// Strip session suffix: "device-abc-session-1" -> "device-abc"
		if idx := len(clientID) - len("-session-1"); idx > 0 && clientID[idx:] == "-session-1" {
			return clientID[:idx]
		}
		return clientID
	}

	t.Run("resolver allows matching identity", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithClientIDResolver(resolver))

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "device-abc-session-1",
			Topic:    "$things/device-abc/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("resolver denies non-matching identity", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithClientIDResolver(resolver))

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "device-xyz-session-1",
			Topic:    "$things/device-abc/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("no resolver uses raw clientID", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "device-abc-session-1",
			Topic:    "$things/device-abc/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.False(t, result.Allowed)
	})

	t.Run("resolver with named shadow", func(t *testing.T) {
		h := NewHandler(WithNamedShadow(), WithClientIDResolver(resolver))

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "device-abc-session-1",
			Topic:    "$things/device-abc/shadow/name/config/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})

	t.Run("resolver not applied to shared shadows", func(t *testing.T) {
		allowAll := func(_, _ string) bool { return true }
		h := NewHandler(WithSharedShadow(allowAll), WithClientIDResolver(resolver))

		result := h.CheckAccess(&mqttv5.AuthzContext{
			ClientID: "device-abc-session-1",
			Topic:    "$things/$shared/room-101/shadow/update",
			Action:   mqttv5.AuthzActionPublish,
		})
		require.NotNil(t, result)
		assert.True(t, result.Allowed)
	})
}
