package shadow

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

var errSubscribeFailed = errors.New("subscribe failed")

// mockClient implements MQTTClient for testing.
type mockClient struct {
	clientID     string
	mu           sync.Mutex
	published    []*mqttv5.Message
	subscribed   map[string]byte
	unsubscribed []string
}

func newMockClient(clientID string) *mockClient {
	return &mockClient{
		clientID:   clientID,
		subscribed: make(map[string]byte),
	}
}

func (m *mockClient) ClientID() string { return m.clientID }

func (m *mockClient) Subscribe(filter string, qos byte, _ mqttv5.MessageHandler) error {
	m.mu.Lock()
	m.subscribed[filter] = qos
	m.mu.Unlock()
	return nil
}

func (m *mockClient) Unsubscribe(filters ...string) error {
	m.mu.Lock()
	m.unsubscribed = append(m.unsubscribed, filters...)
	m.mu.Unlock()
	return nil
}

func (m *mockClient) Publish(msg *mqttv5.Message) error {
	m.mu.Lock()
	m.published = append(m.published, msg)
	m.mu.Unlock()
	return nil
}

func (m *mockClient) lastPublished() *mqttv5.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.published) == 0 {
		return nil
	}
	return m.published[len(m.published)-1]
}

func TestClient(t *testing.T) {
	t.Run("default client ID", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.Get())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get", msg.Topic)
	})

	t.Run("custom client ID", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client, WithClientID("custom-thing"))

		require.NoError(t, sc.Get())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/custom-thing/shadow/get", msg.Topic)
	})

	t.Run("Get", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.Get())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get", msg.Topic)
		assert.Nil(t, msg.Payload)
	})

	t.Run("GetWithToken", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.GetWithToken("myToken"))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get", msg.Topic)
		require.NotNil(t, msg.Payload)

		var req getRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, "myToken", req.ClientToken)
	})

	t.Run("GetWithToken empty token no payload", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.GetWithToken(""))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get", msg.Topic)
		assert.Nil(t, msg.Payload)
	})

	t.Run("ListShadows", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.ListShadows())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/list", msg.Topic)
		assert.Nil(t, msg.Payload)
	})

	t.Run("ListShadowsWithToken", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.ListShadowsWithToken("list-tok"))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/list", msg.Topic)
		require.NotNil(t, msg.Payload)

		var req listRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, "list-tok", req.ClientToken)
	})

	t.Run("Update with desired", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.Update(UpdateState{
			Desired: map[string]any{"temp": 22.0},
		}))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update", msg.Topic)

		var req UpdateRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, 22.0, req.State.Desired["temp"])
	})

	t.Run("Update with reported", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.Update(UpdateState{
			Reported: map[string]any{"temp": 20.0},
		}))

		msg := client.lastPublished()
		require.NotNil(t, msg)

		var req UpdateRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, 20.0, req.State.Reported["temp"])
	})

	t.Run("Delete", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.Delete())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/delete", msg.Topic)
	})

	t.Run("DeleteWithToken", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.DeleteWithToken("del-tok"))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/delete", msg.Topic)
		require.NotNil(t, msg.Payload)

		var req deleteRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, "del-tok", req.ClientToken)
	})

	t.Run("Delete without token has no payload", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.Delete())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Nil(t, msg.Payload)
	})

	t.Run("SubscribeDelta", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.SubscribeDelta(func(_ *mqttv5.Message) {}))

		client.mu.Lock()
		qos, ok := client.subscribed["$things/dev1/shadow/update/delta"]
		client.mu.Unlock()

		assert.True(t, ok)
		assert.Equal(t, byte(1), qos)
	})

	t.Run("SubscribeDocuments", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.SubscribeDocuments(func(_ *mqttv5.Message) {}))

		client.mu.Lock()
		_, ok := client.subscribed["$things/dev1/shadow/update/documents"]
		client.mu.Unlock()

		assert.True(t, ok)
	})

	t.Run("SubscribeAll", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.SubscribeAll(Handlers{
			Accepted:  func(_ *mqttv5.Message) {},
			Rejected:  func(_ *mqttv5.Message) {},
			Delta:     func(_ *mqttv5.Message) {},
			Documents: func(_ *mqttv5.Message) {},
		}))

		client.mu.Lock()
		assert.Len(t, client.subscribed, 4)
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/update/accepted")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/update/rejected")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/update/delta")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/update/documents")
		client.mu.Unlock()
	})

	t.Run("SubscribeAll with get/delete/list handlers", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.SubscribeAll(Handlers{
			GetAccepted:    func(_ *mqttv5.Message) {},
			GetRejected:    func(_ *mqttv5.Message) {},
			DeleteAccepted: func(_ *mqttv5.Message) {},
			DeleteRejected: func(_ *mqttv5.Message) {},
			ListAccepted:   func(_ *mqttv5.Message) {},
			ListRejected:   func(_ *mqttv5.Message) {},
		}))

		client.mu.Lock()
		assert.Len(t, client.subscribed, 6)
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/get/accepted")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/get/rejected")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/delete/accepted")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/delete/rejected")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/list/accepted")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/list/rejected")
		client.mu.Unlock()
	})

	t.Run("SubscribeAll all handlers", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.SubscribeAll(Handlers{
			Accepted:       func(_ *mqttv5.Message) {},
			Rejected:       func(_ *mqttv5.Message) {},
			Delta:          func(_ *mqttv5.Message) {},
			Documents:      func(_ *mqttv5.Message) {},
			GetAccepted:    func(_ *mqttv5.Message) {},
			GetRejected:    func(_ *mqttv5.Message) {},
			DeleteAccepted: func(_ *mqttv5.Message) {},
			DeleteRejected: func(_ *mqttv5.Message) {},
			ListAccepted:   func(_ *mqttv5.Message) {},
			ListRejected:   func(_ *mqttv5.Message) {},
		}))

		client.mu.Lock()
		assert.Len(t, client.subscribed, 10)
		client.mu.Unlock()
	})

	t.Run("SubscribeAll partial", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.SubscribeAll(Handlers{
			Delta: func(_ *mqttv5.Message) {},
		}))

		client.mu.Lock()
		assert.Len(t, client.subscribed, 1)
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/update/delta")
		client.mu.Unlock()
	})

	t.Run("SubscribeAll error on accepted", func(t *testing.T) {
		client := &errorMockClient{failOnFilter: "update/accepted"}
		sc := NewClient(client)

		err := sc.SubscribeAll(Handlers{
			Accepted: func(_ *mqttv5.Message) {},
		})
		require.ErrorIs(t, err, errSubscribeFailed)
	})

	t.Run("SubscribeAll error on rejected", func(t *testing.T) {
		client := &errorMockClient{failOnFilter: "update/rejected"}
		sc := NewClient(client)

		err := sc.SubscribeAll(Handlers{
			Accepted: func(_ *mqttv5.Message) {},
			Rejected: func(_ *mqttv5.Message) {},
		})
		require.ErrorIs(t, err, errSubscribeFailed)
	})

	t.Run("SubscribeAll error on delta", func(t *testing.T) {
		client := &errorMockClient{failOnFilter: "update/delta"}
		sc := NewClient(client)

		err := sc.SubscribeAll(Handlers{
			Delta: func(_ *mqttv5.Message) {},
		})
		require.ErrorIs(t, err, errSubscribeFailed)
	})

	t.Run("SubscribeAll error on documents", func(t *testing.T) {
		client := &errorMockClient{failOnFilter: "update/documents"}
		sc := NewClient(client)

		err := sc.SubscribeAll(Handlers{
			Documents: func(_ *mqttv5.Message) {},
		})
		require.ErrorIs(t, err, errSubscribeFailed)
	})

	t.Run("Unsubscribe", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)

		require.NoError(t, sc.Unsubscribe())

		client.mu.Lock()
		assert.Len(t, client.unsubscribed, 10)
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/update/accepted")
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/update/rejected")
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/update/delta")
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/update/documents")
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/get/accepted")
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/get/rejected")
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/delete/accepted")
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/delete/rejected")
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/list/accepted")
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/list/rejected")
		client.mu.Unlock()
	})
}

func TestNamedClient(t *testing.T) {
	t.Run("Shadow returns NamedClient with correct topics", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)
		named := sc.Shadow("config")

		require.NoError(t, named.Get())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/name/config/get", msg.Topic)
	})

	t.Run("GetWithToken", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewClient(client).Shadow("config")

		require.NoError(t, named.GetWithToken("tok123"))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/name/config/get", msg.Topic)

		var req getRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, "tok123", req.ClientToken)
	})

	t.Run("Update", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewClient(client).Shadow("config")

		require.NoError(t, named.Update(UpdateState{
			Desired: map[string]any{"mode": "auto"},
		}))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/name/config/update", msg.Topic)

		var req UpdateRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, "auto", req.State.Desired["mode"])
	})

	t.Run("Delete", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewClient(client).Shadow("config")

		require.NoError(t, named.Delete())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/name/config/delete", msg.Topic)
		assert.Nil(t, msg.Payload)
	})

	t.Run("DeleteWithToken", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewClient(client).Shadow("config")

		require.NoError(t, named.DeleteWithToken("named-del"))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/name/config/delete", msg.Topic)

		var req deleteRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, "named-del", req.ClientToken)
	})

	t.Run("SubscribeDelta", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewClient(client).Shadow("config")

		require.NoError(t, named.SubscribeDelta(func(_ *mqttv5.Message) {}))

		client.mu.Lock()
		_, ok := client.subscribed["$things/dev1/shadow/name/config/update/delta"]
		client.mu.Unlock()

		assert.True(t, ok)
	})

	t.Run("SubscribeDocuments", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewClient(client).Shadow("config")

		require.NoError(t, named.SubscribeDocuments(func(_ *mqttv5.Message) {}))

		client.mu.Lock()
		_, ok := client.subscribed["$things/dev1/shadow/name/config/update/documents"]
		client.mu.Unlock()

		assert.True(t, ok)
	})

	t.Run("SubscribeAll", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewClient(client).Shadow("config")

		require.NoError(t, named.SubscribeAll(Handlers{
			Accepted:  func(_ *mqttv5.Message) {},
			Rejected:  func(_ *mqttv5.Message) {},
			Delta:     func(_ *mqttv5.Message) {},
			Documents: func(_ *mqttv5.Message) {},
		}))

		client.mu.Lock()
		assert.Len(t, client.subscribed, 4)
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/name/config/update/accepted")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/name/config/update/rejected")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/name/config/update/delta")
		assert.Contains(t, client.subscribed, "$things/dev1/shadow/name/config/update/documents")
		client.mu.Unlock()
	})

	t.Run("Unsubscribe", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewClient(client).Shadow("config")

		require.NoError(t, named.Unsubscribe())

		client.mu.Lock()
		assert.Len(t, client.unsubscribed, 10)
		assert.Contains(t, client.unsubscribed, "$things/dev1/shadow/name/config/update/accepted")
		client.mu.Unlock()
	})

	t.Run("classic and named coexist", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client)
		config := sc.Shadow("config")

		require.NoError(t, sc.Update(UpdateState{
			Reported: map[string]any{"temp": 22.0},
		}))
		require.NoError(t, config.Update(UpdateState{
			Desired: map[string]any{"mode": "auto"},
		}))

		client.mu.Lock()
		require.Len(t, client.published, 2)
		assert.Equal(t, "$things/dev1/shadow/update", client.published[0].Topic)
		assert.Equal(t, "$things/dev1/shadow/name/config/update", client.published[1].Topic)
		client.mu.Unlock()
	})

	t.Run("custom client ID shared", func(t *testing.T) {
		client := newMockClient("dev1")
		sc := NewClient(client, WithClientID("my-device"))
		named := sc.Shadow("firmware")

		require.NoError(t, sc.Get())
		require.NoError(t, named.Get())

		client.mu.Lock()
		require.Len(t, client.published, 2)
		assert.Equal(t, "$things/my-device/shadow/get", client.published[0].Topic)
		assert.Equal(t, "$things/my-device/shadow/name/firmware/get", client.published[1].Topic)
		client.mu.Unlock()
	})
}

func TestGroupClient(t *testing.T) {
	t.Run("Get", func(t *testing.T) {
		client := newMockClient("dev1")
		gc := NewGroupClient(client, "room-101")

		require.NoError(t, gc.Get())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/get", msg.Topic)
	})

	t.Run("GetWithToken", func(t *testing.T) {
		client := newMockClient("dev1")
		gc := NewGroupClient(client, "room-101")

		require.NoError(t, gc.GetWithToken("groupTok"))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/get", msg.Topic)

		var req getRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, "groupTok", req.ClientToken)
	})

	t.Run("Update", func(t *testing.T) {
		client := newMockClient("dev1")
		gc := NewGroupClient(client, "room-101")

		require.NoError(t, gc.Update(UpdateState{
			Desired: map[string]any{"temp": 22.0},
		}))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/update", msg.Topic)

		var req UpdateRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, 22.0, req.State.Desired["temp"])
	})

	t.Run("Delete", func(t *testing.T) {
		client := newMockClient("dev1")
		gc := NewGroupClient(client, "room-101")

		require.NoError(t, gc.Delete())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/delete", msg.Topic)
	})

	t.Run("DeleteWithToken", func(t *testing.T) {
		client := newMockClient("dev1")
		gc := NewGroupClient(client, "room-101")

		require.NoError(t, gc.DeleteWithToken("group-del"))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/delete", msg.Topic)

		var req deleteRequest
		require.NoError(t, json.Unmarshal(msg.Payload, &req))
		assert.Equal(t, "group-del", req.ClientToken)
	})

	t.Run("SubscribeDelta", func(t *testing.T) {
		client := newMockClient("dev1")
		gc := NewGroupClient(client, "room-101")

		require.NoError(t, gc.SubscribeDelta(func(_ *mqttv5.Message) {}))

		client.mu.Lock()
		_, ok := client.subscribed["$things/$shared/room-101/shadow/update/delta"]
		client.mu.Unlock()

		assert.True(t, ok)
	})

	t.Run("SubscribeDocuments", func(t *testing.T) {
		client := newMockClient("dev1")
		gc := NewGroupClient(client, "room-101")

		require.NoError(t, gc.SubscribeDocuments(func(_ *mqttv5.Message) {}))

		client.mu.Lock()
		_, ok := client.subscribed["$things/$shared/room-101/shadow/update/documents"]
		client.mu.Unlock()

		assert.True(t, ok)
	})

	t.Run("SubscribeAll", func(t *testing.T) {
		client := newMockClient("dev1")
		gc := NewGroupClient(client, "room-101")

		require.NoError(t, gc.SubscribeAll(Handlers{
			Accepted:  func(_ *mqttv5.Message) {},
			Rejected:  func(_ *mqttv5.Message) {},
			Delta:     func(_ *mqttv5.Message) {},
			Documents: func(_ *mqttv5.Message) {},
		}))

		client.mu.Lock()
		assert.Len(t, client.subscribed, 4)
		assert.Contains(t, client.subscribed, "$things/$shared/room-101/shadow/update/accepted")
		assert.Contains(t, client.subscribed, "$things/$shared/room-101/shadow/update/rejected")
		assert.Contains(t, client.subscribed, "$things/$shared/room-101/shadow/update/delta")
		assert.Contains(t, client.subscribed, "$things/$shared/room-101/shadow/update/documents")
		client.mu.Unlock()
	})

	t.Run("Unsubscribe", func(t *testing.T) {
		client := newMockClient("dev1")
		gc := NewGroupClient(client, "room-101")

		require.NoError(t, gc.Unsubscribe())

		client.mu.Lock()
		assert.Len(t, client.unsubscribed, 10)
		assert.Contains(t, client.unsubscribed, "$things/$shared/room-101/shadow/update/accepted")
		assert.Contains(t, client.unsubscribed, "$things/$shared/room-101/shadow/update/rejected")
		assert.Contains(t, client.unsubscribed, "$things/$shared/room-101/shadow/update/delta")
		assert.Contains(t, client.unsubscribed, "$things/$shared/room-101/shadow/update/documents")
		client.mu.Unlock()
	})

	t.Run("named shared shadow", func(t *testing.T) {
		client := newMockClient("dev1")
		gc := NewGroupClient(client, "room-101")
		named := gc.Shadow("config")

		require.NoError(t, named.Get())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/name/config/get", msg.Topic)
	})

	t.Run("named shared shadow update", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewGroupClient(client, "room-101").Shadow("config")

		require.NoError(t, named.Update(UpdateState{
			Desired: map[string]any{"mode": "auto"},
		}))

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/name/config/update", msg.Topic)
	})

	t.Run("named shared shadow delete", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewGroupClient(client, "room-101").Shadow("config")

		require.NoError(t, named.Delete())

		msg := client.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/name/config/delete", msg.Topic)
	})

	t.Run("named shared shadow subscribe delta", func(t *testing.T) {
		client := newMockClient("dev1")
		named := NewGroupClient(client, "room-101").Shadow("config")

		require.NoError(t, named.SubscribeDelta(func(_ *mqttv5.Message) {}))

		client.mu.Lock()
		_, ok := client.subscribed["$things/$shared/room-101/shadow/name/config/update/delta"]
		client.mu.Unlock()

		assert.True(t, ok)
	})
}

// errorMockClient implements MQTTClient and fails Subscribe on a matching filter.
type errorMockClient struct {
	failOnFilter string // substring match
}

func (m *errorMockClient) ClientID() string { return "dev1" }

func (m *errorMockClient) Subscribe(filter string, _ byte, _ mqttv5.MessageHandler) error {
	if strings.Contains(filter, m.failOnFilter) {
		return errSubscribeFailed
	}
	return nil
}

func (m *errorMockClient) Unsubscribe(_ ...string) error   { return nil }
func (m *errorMockClient) Publish(_ *mqttv5.Message) error { return nil }
