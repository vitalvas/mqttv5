package lifecycle

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

type mockPublisher struct {
	mu         sync.Mutex
	messages   []*mqttv5.Message
	publishErr error
}

func (m *mockPublisher) Publish(msg *mqttv5.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messages = append(m.messages, msg)

	return m.publishErr
}

func (m *mockPublisher) getMessages() []*mqttv5.Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*mqttv5.Message, len(m.messages))
	copy(result, m.messages)

	return result
}

func newTestClient(clientID, namespace, username string) *mqttv5.ServerClient {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	connect := &mqttv5.ConnectPacket{
		ClientID: clientID,
		Username: username,
	}

	return mqttv5.NewServerClient(server, connect, mqttv5.MaxPacketSizeDefault, namespace)
}

func TestHandler(t *testing.T) {
	t.Run("on_connect", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("client-1", "ns1", "user1")

		h.OnConnect(client)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/presence/connected/client-1", msgs[0].Topic)
		assert.Equal(t, mqttv5.DefaultNamespace, msgs[0].Namespace)
		assert.False(t, msgs[0].Retain)

		var event ConnectedEvent
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
		assert.Equal(t, "client-1", event.ClientID)
		assert.Equal(t, EventConnected, event.EventType)
		assert.Equal(t, "ns1", event.Namespace)
		assert.Equal(t, "user1", event.Username)
		assert.NotEmpty(t, event.RemoteAddr)
		assert.NotEmpty(t, event.LocalAddr)
		assert.WithinDuration(t, time.Now(), event.Timestamp, 2*time.Second)
	})

	t.Run("on_connect_failed", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)

		h.OnConnectFailed(&mqttv5.ConnectFailedContext{
			ClientID:   "client-fail",
			Username:   "baduser",
			RemoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5555},
			LocalAddr:  &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: 1883},
			ReasonCode: mqttv5.ReasonNotAuthorized,
		})

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/presence/connect_failed/client-fail", msgs[0].Topic)
		assert.Equal(t, mqttv5.DefaultNamespace, msgs[0].Namespace)
		assert.False(t, msgs[0].Retain)

		var event ConnectFailedEvent
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
		assert.Equal(t, "client-fail", event.ClientID)
		assert.Equal(t, EventConnectFailed, event.EventType)
		assert.Equal(t, "baduser", event.Username)
		assert.Equal(t, "10.0.0.1:5555", event.RemoteAddr)
		assert.Equal(t, "0.0.0.0:1883", event.LocalAddr)
		assert.Equal(t, byte(mqttv5.ReasonNotAuthorized), event.ReasonCode)
		assert.NotEmpty(t, event.Reason)
		assert.WithinDuration(t, time.Now(), event.Timestamp, 2*time.Second)
	})

	t.Run("on_connect_failed_empty_client_id", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)

		h.OnConnectFailed(&mqttv5.ConnectFailedContext{
			RemoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5555},
			ReasonCode: mqttv5.ReasonServerBusy,
		})

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/presence/connect_failed/unknown", msgs[0].Topic)
	})

	t.Run("on_disconnect", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("client-2", "", "")

		h.OnDisconnect(client)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/presence/disconnected/client-2", msgs[0].Topic)

		var event DisconnectedEvent
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
		assert.Equal(t, "client-2", event.ClientID)
		assert.Equal(t, EventDisconnected, event.EventType)
		assert.NotEmpty(t, event.RemoteAddr)
		assert.NotEmpty(t, event.LocalAddr)
		assert.False(t, event.CleanDisconnect)
		assert.GreaterOrEqual(t, event.SessionDuration, 0.0)
	})

	t.Run("on_subscribe", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("client-3", "ns1", "")

		subs := []mqttv5.Subscription{
			{TopicFilter: "sensors/+/temp", QoS: 1},
			{TopicFilter: "alerts/#", QoS: 0},
		}

		h.OnSubscribe(client, subs)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/subscriptions/subscribed/client-3", msgs[0].Topic)
		assert.Equal(t, mqttv5.DefaultNamespace, msgs[0].Namespace)

		var event SubscribedEvent
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
		assert.Equal(t, "client-3", event.ClientID)
		assert.Equal(t, EventSubscribed, event.EventType)
		require.Len(t, event.Subscriptions, 2)
		assert.Equal(t, "sensors/+/temp", event.Subscriptions[0].TopicFilter)
		assert.Equal(t, byte(1), event.Subscriptions[0].QoS)
		assert.Equal(t, "alerts/#", event.Subscriptions[1].TopicFilter)
		assert.Equal(t, byte(0), event.Subscriptions[1].QoS)
	})

	t.Run("on_unsubscribe", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("client-4", "", "")

		topics := []string{"sensors/+/temp", "alerts/#"}

		h.OnUnsubscribe(client, topics)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/subscriptions/unsubscribed/client-4", msgs[0].Topic)

		var event UnsubscribedEvent
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
		assert.Equal(t, "client-4", event.ClientID)
		assert.Equal(t, EventUnsubscribed, event.EventType)
		assert.Equal(t, topics, event.TopicFilters)
	})

	t.Run("default_namespace_used", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("client-5", "tenant-a", "")

		h.OnConnect(client)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, mqttv5.DefaultNamespace, msgs[0].Namespace)
	})

	t.Run("client_namespace_with_flag", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub, WithClientNamespace(true))
		client := newTestClient("client-5b", "tenant-a", "")

		h.OnConnect(client)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "tenant-a", msgs[0].Namespace)
	})

	t.Run("client_namespace_empty_falls_back", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub, WithClientNamespace(true))
		client := newTestClient("client-5c", "", "")

		h.OnConnect(client)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, mqttv5.DefaultNamespace, msgs[0].Namespace)
	})

	t.Run("fixed_namespace", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub, WithNamespace("custom-ns"))
		client := newTestClient("client-5d", "tenant-a", "")

		h.OnConnect(client)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "custom-ns", msgs[0].Namespace)
	})

	t.Run("fixed_namespace_overrides_client_namespace", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub, WithNamespace("fixed"), WithClientNamespace(true))
		client := newTestClient("client-5e", "tenant-a", "")

		h.OnConnect(client)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "fixed", msgs[0].Namespace)
	})

	t.Run("no_namespace_uses_default", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("client-6", "", "")

		h.OnConnect(client)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, mqttv5.DefaultNamespace, msgs[0].Namespace)

		var event ConnectedEvent
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
		assert.Empty(t, event.Namespace)
	})

	t.Run("messages_not_retained", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("client-7", "", "")

		h.OnConnect(client)
		h.OnDisconnect(client)

		for _, msg := range pub.getMessages() {
			assert.False(t, msg.Retain, "lifecycle events should not be retained")
		}
	})
}

func TestSanitizeTopicSegment(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"client/id", "client%2Fid"},
		{"client+id", "client%2Bid"},
		{"client#id", "client%23id"},
		{"a/b+c#d", "a%2Fb%2Bc%23d"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, sanitizeTopicSegment(tt.input))
		})
	}
}

func TestClientIDSanitizedInTopics(t *testing.T) {
	t.Run("connect with unsafe clientID", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("dev/1+2#3", "", "")

		h.OnConnect(client)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/presence/connected/dev%2F1%2B2%233", msgs[0].Topic)

		var event ConnectedEvent
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &event))
		assert.Equal(t, "dev/1+2#3", event.ClientID)
	})

	t.Run("disconnect with unsafe clientID", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("dev/1", "", "")

		h.OnDisconnect(client)

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/presence/disconnected/dev%2F1", msgs[0].Topic)
	})

	t.Run("connect_failed with unsafe clientID", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)

		h.OnConnectFailed(&mqttv5.ConnectFailedContext{
			ClientID:   "bad+client",
			ReasonCode: mqttv5.ReasonNotAuthorized,
		})

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/presence/connect_failed/bad%2Bclient", msgs[0].Topic)
	})

	t.Run("subscribe with unsafe clientID", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("dev#1", "", "")

		h.OnSubscribe(client, []mqttv5.Subscription{{TopicFilter: "a/b", QoS: 0}})

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/subscriptions/subscribed/dev%231", msgs[0].Topic)
	})

	t.Run("unsubscribe with unsafe clientID", func(t *testing.T) {
		pub := &mockPublisher{}
		h := New(pub)
		client := newTestClient("dev/x", "", "")

		h.OnUnsubscribe(client, []string{"a/b"})

		msgs := pub.getMessages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "$events/subscriptions/unsubscribed/dev%2Fx", msgs[0].Topic)
	})
}

func TestPublishErrors(t *testing.T) {
	t.Run("callback_receives_publish_error", func(t *testing.T) {
		pub := &mockPublisher{publishErr: errors.New("publish failed")}

		var gotTopic string
		var gotErr error

		h := New(pub, WithOnPublishError(func(topic string, err error) {
			gotTopic = topic
			gotErr = err
		}))

		client := newTestClient("client-err", "", "")
		h.OnConnect(client)

		assert.Equal(t, "$events/presence/connected/client-err", gotTopic)
		assert.EqualError(t, gotErr, "publish failed")
	})

	t.Run("no_callback_does_not_panic", func(t *testing.T) {
		pub := &mockPublisher{publishErr: errors.New("fail")}
		h := New(pub)

		client := newTestClient("client-panic", "", "")

		assert.NotPanics(t, func() {
			h.OnConnect(client)
		})
	})

	t.Run("marshal_error_reported", func(t *testing.T) {
		pub := &mockPublisher{}

		var gotTopic string
		var gotErr error

		h := New(pub, WithOnPublishError(func(topic string, err error) {
			gotTopic = topic
			gotErr = err
		}))

		// Pass a value that json.Marshal cannot serialize.
		h.publish("test/topic", "", func() {})

		assert.Equal(t, "test/topic", gotTopic)
		assert.Error(t, gotErr)
		assert.Empty(t, pub.getMessages())
	})
}
