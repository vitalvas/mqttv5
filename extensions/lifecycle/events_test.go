package lifecycle

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventSerialization(t *testing.T) {
	t.Run("connected event", func(t *testing.T) {
		event := ConnectedEvent{
			ClientID:              "client-1",
			Timestamp:             time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			EventType:             EventConnected,
			Namespace:             "ns1",
			Username:              "user1",
			RemoteAddr:            "127.0.0.1:1234",
			LocalAddr:             "0.0.0.0:1883",
			CleanStart:            true,
			KeepAlive:             60,
			SessionExpiryInterval: 3600,
		}

		data, err := json.Marshal(event)
		require.NoError(t, err)

		var decoded ConnectedEvent
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, event, decoded)
	})

	t.Run("disconnected event", func(t *testing.T) {
		event := DisconnectedEvent{
			ClientID:        "client-2",
			Timestamp:       time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC),
			EventType:       EventDisconnected,
			RemoteAddr:      "127.0.0.1:1234",
			LocalAddr:       "0.0.0.0:1883",
			CleanDisconnect: true,
			ConnectedAt:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			SessionDuration: 60.0,
		}

		data, err := json.Marshal(event)
		require.NoError(t, err)

		var decoded DisconnectedEvent
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, event, decoded)
	})

	t.Run("subscribed event", func(t *testing.T) {
		event := SubscribedEvent{
			ClientID:  "client-3",
			Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			EventType: EventSubscribed,
			Subscriptions: []SubscriptionEntry{
				{TopicFilter: "a/b", QoS: 1},
			},
		}

		data, err := json.Marshal(event)
		require.NoError(t, err)

		var decoded SubscribedEvent
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, event, decoded)
	})

	t.Run("unsubscribed event", func(t *testing.T) {
		event := UnsubscribedEvent{
			ClientID:     "client-4",
			Timestamp:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			EventType:    EventUnsubscribed,
			TopicFilters: []string{"a/b", "c/d"},
		}

		data, err := json.Marshal(event)
		require.NoError(t, err)

		var decoded UnsubscribedEvent
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, event, decoded)
	})

	t.Run("connect failed event", func(t *testing.T) {
		event := ConnectFailedEvent{
			ClientID:   "client-fail",
			Timestamp:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			EventType:  EventConnectFailed,
			Username:   "baduser",
			RemoteAddr: "10.0.0.1:5555",
			ReasonCode: 135,
			Reason:     "not authorized",
		}

		data, err := json.Marshal(event)
		require.NoError(t, err)

		var decoded ConnectFailedEvent
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, event, decoded)
	})

	t.Run("connect failed clientId omitted when empty", func(t *testing.T) {
		event := ConnectFailedEvent{
			Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			EventType: EventConnectFailed,
		}

		data, err := json.Marshal(event)
		require.NoError(t, err)
		assert.NotContains(t, string(data), "clientId")
	})

	t.Run("namespace omitted when empty", func(t *testing.T) {
		event := ConnectedEvent{
			ClientID:  "client-5",
			Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			EventType: EventConnected,
		}

		data, err := json.Marshal(event)
		require.NoError(t, err)
		assert.NotContains(t, string(data), "namespace")
	})

	t.Run("username omitted when empty", func(t *testing.T) {
		event := ConnectedEvent{
			ClientID:  "client-6",
			Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			EventType: EventConnected,
		}

		data, err := json.Marshal(event)
		require.NoError(t, err)
		assert.NotContains(t, string(data), "username")
	})
}
