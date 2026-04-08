package shadow

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

var errStoreFailure = errors.New("store failure")

// errorStore is a Store that returns errors for testing.
type errorStore struct {
	failOn string // "Get", "Save", "Delete"; empty means fail on all
	err    error
}

func (s *errorStore) Get(_ Key) (*Document, error) {
	if s.failOn == "" || s.failOn == "Get" {
		return nil, s.err
	}
	return nil, nil
}

func (s *errorStore) Save(_ Key, _ *Document) error {
	if s.failOn == "" || s.failOn == "Save" {
		return s.err
	}
	return nil
}

func (s *errorStore) Delete(_ Key) (*Document, error) {
	if s.failOn == "" || s.failOn == "Delete" {
		return nil, s.err
	}
	return nil, nil
}

// errorWatcherStore is a Watcher that returns an error from Watch.
type errorWatcherStore struct {
	*MemoryStore
	watchErr error
}

func (s *errorWatcherStore) Watch(_ func(Key, *Document)) (func(), error) {
	return nil, s.watchErr
}

// errorListerStore is a Store+Lister that returns errors from ListNamedShadows.
type errorListerStore struct {
	*MemoryStore
	listErr error
}

func (s *errorListerStore) ListNamedShadows(_ Key, _ int, _ string) (*ListResult, error) {
	return nil, s.listErr
}

// errorCounterStore is a Store+Counter that returns errors from CountNamedShadows.
type errorCounterStore struct {
	*MemoryStore
	countErr error
}

func (s *errorCounterStore) CountNamedShadows(_ Key) (int, error) {
	return 0, s.countErr
}

// mockServerClient implements ServerClient for testing.
type mockServerClient struct {
	clientID  string
	namespace string
	mu        sync.Mutex
	sent      []*mqttv5.Message
}

func newMockServerClient(clientID, namespace string) *mockServerClient {
	return &mockServerClient{
		clientID:  clientID,
		namespace: namespace,
	}
}

func (m *mockServerClient) ClientID() string  { return m.clientID }
func (m *mockServerClient) Namespace() string { return m.namespace }

func (m *mockServerClient) Send(msg *mqttv5.Message) error {
	m.mu.Lock()
	m.sent = append(m.sent, msg)
	m.mu.Unlock()
	return nil
}

func (m *mockServerClient) lastMessage() *mqttv5.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sent) == 0 {
		return nil
	}
	return m.sent[len(m.sent)-1]
}

func (m *mockServerClient) allMessages() []*mqttv5.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*mqttv5.Message, len(m.sent))
	copy(cp, m.sent)
	return cp
}

func TestHandler_FeatureFlags(t *testing.T) {
	t.Run("classic disabled by default", func(t *testing.T) {
		h := NewHandler()
		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		assert.False(t, handled)
	})

	t.Run("named disabled by default", func(t *testing.T) {
		h := NewHandler()
		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/name/config/get",
		})

		assert.False(t, handled)
	})

	t.Run("classic enabled named disabled", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		assert.True(t, h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		}))

		assert.False(t, h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/name/config/get",
		}))
	})

	t.Run("named enabled classic disabled", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		client := newMockServerClient("dev1", "default")

		assert.False(t, h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		}))

		assert.True(t, h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/name/config/get",
		}))
	})

	t.Run("both enabled", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithNamedShadow())
		client := newMockServerClient("dev1", "default")

		assert.True(t, h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		}))

		assert.True(t, h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/name/config/get",
		}))
	})

	t.Run("shared disabled by default", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/$shared/room-101/shadow/get",
		})

		assert.False(t, handled)
	})

	t.Run("shared enabled", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(func(_, _ string) bool { return true }))
		client := newMockServerClient("dev1", "default")

		assert.True(t, h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/$shared/room-101/shadow/get",
		}))
	})

	t.Run("shared nil resolver allows get", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(nil))
		client := newMockServerClient("dev1", "default")

		assert.True(t, h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/$shared/room-101/shadow/get",
		}))

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/get/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeNotFound, errResp.Code)
	})

	t.Run("shared nil resolver denies update", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(nil))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/$shared/room-101/shadow/update",
			Payload: payload,
		})
		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeForbidden, errResp.Code)
	})

	t.Run("shared nil resolver denies delete", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(nil))
		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/$shared/room-101/shadow/delete",
		})
		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/delete/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeForbidden, errResp.Code)
	})
}

func TestHandler_HandleMessage(t *testing.T) {
	t.Run("non-shadow topic returns false", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic:   "devices/dev1/telemetry",
			Payload: []byte("{}"),
		})

		assert.False(t, handled)
	})

	t.Run("response topic returns true but no action", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/update/accepted",
		})

		assert.True(t, handled)
		assert.Nil(t, client.lastMessage())
	})

	t.Run("get non-existent shadow", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeNotFound, errResp.Code)
		assert.NotZero(t, errResp.Timestamp)
	})

	t.Run("update then get", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{"temp": 22.0},
			},
		})

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})
		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))
		assert.Equal(t, float64(1), accepted["version"])
		state := accepted["state"].(map[string]any)
		assert.Equal(t, 22.0, state["desired"].(map[string]any)["temp"])

		// Now get
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg = client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get/accepted", msg.Topic)

		var doc Document
		require.NoError(t, json.Unmarshal(msg.Payload, &doc))
		assert.Equal(t, int64(1), doc.Version)
		assert.Equal(t, 22.0, doc.State.Desired["temp"])
	})

	t.Run("update with delta", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithClassicShadow(), WithPublishFunc(publishFunc))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)

		// Accepted response contains only request fields, not delta
		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))
		acceptedState := accepted["state"].(map[string]any)
		assert.Equal(t, 22.0, acceptedState["desired"].(map[string]any)["temp"])
		assert.Equal(t, 20.0, acceptedState["reported"].(map[string]any)["temp"])

		// Delta and documents are published
		mu.Lock()
		assert.Len(t, published, 2)
		assert.Contains(t, published[0].Topic, "update/delta")
		assert.Contains(t, published[1].Topic, "update/documents")
		mu.Unlock()
	})

	t.Run("update no delta when matching", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithClassicShadow(), WithPublishFunc(publishFunc))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 22.0},
			},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		mu.Lock()
		assert.Len(t, published, 1)
		assert.Contains(t, published[0].Topic, "update/documents")
		mu.Unlock()
	})

	t.Run("update invalid JSON", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: []byte("not json"),
		})

		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.NotZero(t, errResp.Timestamp)
	})

	t.Run("update empty state", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)
	})

	t.Run("version conflict", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		wrongVersion := int64(99)
		payload, _ = json.Marshal(UpdateRequest{
			State:   UpdateState{Desired: map[string]any{"temp": 25.0}},
			Version: &wrongVersion,
		})

		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeVersionConflict, errResp.Code)
	})

	t.Run("delete existing shadow", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		client.sent = nil
		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/delete",
		})

		assert.True(t, handled)
		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/delete/accepted", msg.Topic)
	})

	t.Run("delete non-existent shadow", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/delete",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/delete/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeNotFound, errResp.Code)
	})

	t.Run("named shadow", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"mode": "auto"}},
		})

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/name/config/update",
			Payload: payload,
		})

		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/name/config/update/accepted", msg.Topic)
	})

	t.Run("client ID resolver", func(t *testing.T) {
		h := NewHandler(
			WithClassicShadow(),
			WithClientIDResolver(func(clientID string) string {
				return fmt.Sprintf("resolved-%s", clientID)
			}),
		)

		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		assert.True(t, handled)
	})
}

func TestHandler_SharedShadow(t *testing.T) {
	allowAll := func(_, _ string) bool { return true }

	t.Run("shared get/update/delete", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))
		client := newMockServerClient("dev1", "default")

		// Update
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/$shared/room-101/shadow/update",
			Payload: payload,
		})
		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/update/accepted", msg.Topic)

		// Get
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/$shared/room-101/shadow/get",
		})

		msg = client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/get/accepted", msg.Topic)

		// Delete
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/$shared/room-101/shadow/delete",
		})

		msg = client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/delete/accepted", msg.Topic)
	})

	t.Run("shared named shadow", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(allowAll))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"mode": "auto"}},
		})

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/$shared/room-101/shadow/name/config/update",
			Payload: payload,
		})
		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/name/config/update/accepted", msg.Topic)
	})

	t.Run("resolver deny returns 403", func(t *testing.T) {
		denyAll := func(_, _ string) bool { return false }
		h := NewHandler(WithSharedShadow(denyAll))
		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/$shared/room-101/shadow/get",
		})
		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/get/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeForbidden, errResp.Code)
	})

	t.Run("resolver deny on update returns 403", func(t *testing.T) {
		denyAll := func(_, _ string) bool { return false }
		h := NewHandler(WithSharedShadow(denyAll))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/$shared/room-101/shadow/update",
			Payload: payload,
		})
		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeForbidden, errResp.Code)
	})

	t.Run("resolver deny on delete returns 403", func(t *testing.T) {
		denyAll := func(_, _ string) bool { return false }
		h := NewHandler(WithSharedShadow(denyAll))
		client := newMockServerClient("dev1", "default")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/$shared/room-101/shadow/delete",
		})
		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/$shared/room-101/shadow/delete/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeForbidden, errResp.Code)
	})

	t.Run("shared with publishFunc", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithSharedShadow(allowAll), WithPublishFunc(publishFunc))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/$shared/room-101/shadow/update",
			Payload: payload,
		})

		mu.Lock()
		assert.Len(t, published, 2)
		assert.Equal(t, "$things/$shared/room-101/shadow/update/delta", published[0].Topic)
		assert.Equal(t, "$things/$shared/room-101/shadow/update/documents", published[1].Topic)
		assert.Equal(t, "default", published[0].Namespace)
		mu.Unlock()
	})

	t.Run("shared publishNotifications uses shared topics", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithPublishFunc(publishFunc))
		key := Key{Namespace: "default", GroupName: "room-101", ShadowName: "config"}

		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, doc)

		mu.Lock()
		assert.Len(t, published, 2)

		var hasDelta, hasDocs bool
		for _, msg := range published {
			if msg.Topic == "$things/$shared/room-101/shadow/name/config/update/delta" {
				hasDelta = true
			}
			if msg.Topic == "$things/$shared/room-101/shadow/name/config/update/documents" {
				hasDocs = true
			}
		}
		assert.True(t, hasDelta)
		assert.True(t, hasDocs)
		mu.Unlock()
	})
}

func TestHandler_NullDeletion(t *testing.T) {
	for _, tc := range []struct {
		name       string
		section    string
		initState  UpdateState
		nullJSON   string
		checkKey   string
		checkValue float64
		removedKey string
	}{
		{
			name:       "null removes desired key",
			section:    "desired",
			initState:  UpdateState{Desired: map[string]any{"temp": 22.0, "mode": "cool"}},
			nullJSON:   `{"state":{"desired":{"mode":null}}}`,
			checkKey:   "temp",
			checkValue: 22.0,
			removedKey: "mode",
		},
		{
			name:       "null removes reported key",
			section:    "reported",
			initState:  UpdateState{Reported: map[string]any{"temp": 20.0, "mode": "cool"}},
			nullJSON:   `{"state":{"reported":{"mode":null}}}`,
			checkKey:   "temp",
			checkValue: 20.0,
			removedKey: "mode",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(WithClassicShadow())
			client := newMockServerClient("dev1", "default")

			payload, _ := json.Marshal(UpdateRequest{State: tc.initState})
			h.HandleMessage(client, &mqttv5.Message{
				Topic:   "$things/dev1/shadow/update",
				Payload: payload,
			})

			client.sent = nil
			h.HandleMessage(client, &mqttv5.Message{
				Topic:   "$things/dev1/shadow/update",
				Payload: []byte(tc.nullJSON),
			})

			client.sent = nil
			h.HandleMessage(client, &mqttv5.Message{
				Topic: "$things/dev1/shadow/get",
			})

			msg := client.lastMessage()
			require.NotNil(t, msg)

			var doc Document
			require.NoError(t, json.Unmarshal(msg.Payload, &doc))

			state := doc.State.Desired
			if tc.section == "reported" {
				state = doc.State.Reported
			}
			assert.Equal(t, tc.checkValue, state[tc.checkKey])
			assert.NotContains(t, state, tc.removedKey)
		})
	}

	t.Run("null all keys clears section", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		client.sent = nil
		payload = []byte(`{"state":{"desired":{"temp":null}}}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Get the full document
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var doc Document
		require.NoError(t, json.Unmarshal(msg.Payload, &doc))
		assert.Nil(t, doc.State.Desired)
		assert.Nil(t, doc.State.Delta)
	})

	t.Run("null desired key removes from delta", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Set desired and reported with different temp
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0, "mode": "cool"},
				Reported: map[string]any{"temp": 20.0, "mode": "cool"},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Remove temp from desired
		client.sent = nil
		payload = []byte(`{"state":{"desired":{"temp":null}}}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Get the full document
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var doc Document
		require.NoError(t, json.Unmarshal(msg.Payload, &doc))
		assert.NotContains(t, doc.State.Desired, "temp")
		assert.Equal(t, "cool", doc.State.Desired["mode"])
		assert.Nil(t, doc.State.Delta) // mode matches, temp removed
	})

	t.Run("metadata removed with null key", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, _ = h.Update(key, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0, "mode": "cool"}},
		})

		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"mode": nil}},
		})
		require.NoError(t, err)
		assert.Contains(t, doc.Metadata.Desired, "temp")
		assert.NotContains(t, doc.Metadata.Desired, "mode")
	})

	t.Run("desired null clears entire section", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Set desired
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0, "mode": "cool"},
				Reported: map[string]any{"temp": 20.0},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Clear entire desired section with "desired": null
		client.sent = nil
		payload = []byte(`{"state":{"desired":null}}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Get the full document
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var doc Document
		require.NoError(t, json.Unmarshal(msg.Payload, &doc))
		assert.Nil(t, doc.State.Desired)
		assert.Nil(t, doc.State.Delta)
		assert.Equal(t, 20.0, doc.State.Reported["temp"]) // reported untouched
	})

	t.Run("reported null clears entire section", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0, "mode": "cool"},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		client.sent = nil
		payload = []byte(`{"state":{"reported":null}}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Get the full document
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var doc Document
		require.NoError(t, json.Unmarshal(msg.Payload, &doc))
		assert.Nil(t, doc.State.Reported)
		assert.Equal(t, 22.0, doc.State.Desired["temp"]) // desired untouched
		assert.Equal(t, map[string]any{"temp": 22.0}, doc.State.Delta)
	})
}

func TestHandler_RecursiveDelta(t *testing.T) {
	t.Run("nested diff only includes differing leaves", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithClassicShadow(), WithPublishFunc(publishFunc))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{
					"network": map[string]any{"dns": "8.8.8.8", "gw": "10.0.0.1"},
				},
				Reported: map[string]any{
					"network": map[string]any{"dns": "8.8.8.8", "gw": "10.0.0.2"},
				},
			},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Verify delta via published delta topic
		mu.Lock()
		require.GreaterOrEqual(t, len(published), 1)
		assert.Contains(t, published[0].Topic, "update/delta")

		var delta deltaResponse
		require.NoError(t, json.Unmarshal(published[0].Payload, &delta))
		expected := map[string]any{
			"network": map[string]any{"gw": "10.0.0.1"},
		}
		assert.Equal(t, expected, delta.State)
		mu.Unlock()
	})

	t.Run("deeply nested diff", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{
					"lights": map[string]any{
						"color": map[string]any{"r": 255.0, "g": 255.0, "b": 255.0},
					},
				},
				Reported: map[string]any{
					"lights": map[string]any{
						"color": map[string]any{"r": 255.0, "g": 0.0, "b": 255.0},
					},
				},
			},
		})

		require.NoError(t, err)
		expected := map[string]any{
			"lights": map[string]any{
				"color": map[string]any{"g": 255.0},
			},
		}
		assert.Equal(t, expected, doc.State.Delta)
	})
}

func TestHandler_ClientToken(t *testing.T) {
	t.Run("echoed in accepted response", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload := []byte(`{"state":{"desired":{"temp":22}},"clientToken":"myToken123"}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))
		assert.Equal(t, "myToken123", accepted["clientToken"])
	})

	t.Run("echoed in rejected response", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// First create a shadow
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Send version conflict with clientToken
		payload = []byte(`{"state":{"desired":{"temp":25}},"version":99,"clientToken":"failToken"}`)
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeVersionConflict, errResp.Code)
		assert.Equal(t, "failToken", errResp.ClientToken)
	})

	t.Run("echoed in published documents", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithClassicShadow(), WithPublishFunc(publishFunc))
		client := newMockServerClient("dev1", "default")

		payload := []byte(`{"state":{"desired":{"temp":22}},"clientToken":"docToken"}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		mu.Lock()
		defer mu.Unlock()

		// Find the documents message
		for _, msg := range published {
			if msg.Topic == "$things/dev1/shadow/update/documents" {
				var docsMsg DocumentsMessage
				require.NoError(t, json.Unmarshal(msg.Payload, &docsMsg))
				assert.Equal(t, "docToken", docsMsg.ClientToken)
				return
			}
		}
		t.Fatal("documents message not found")
	})

	t.Run("omitted when empty", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		// Verify clientToken is not present in JSON when empty
		var raw map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &raw))
		_, hasToken := raw["clientToken"]
		assert.False(t, hasToken)
	})
}

func TestHandler_NestedMetadata(t *testing.T) {
	t.Run("flat state produces leaf metadata", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		require.NoError(t, err)

		// Metadata should be {"temp": {"timestamp": <int64>}}
		tempMeta, ok := doc.Metadata.Desired["temp"].(map[string]any)
		require.True(t, ok)
		assert.Contains(t, tempMeta, "timestamp")
	})

	t.Run("nested state produces nested metadata", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{
					"network": map[string]any{"dns": "8.8.8.8", "gw": "10.0.0.1"},
				},
			},
		})
		require.NoError(t, err)

		// Metadata should be {"network": {"dns": {"timestamp": ...}, "gw": {"timestamp": ...}}}
		networkMeta, ok := doc.Metadata.Desired["network"].(map[string]any)
		require.True(t, ok)

		dnsMeta, ok := networkMeta["dns"].(map[string]any)
		require.True(t, ok)
		assert.Contains(t, dnsMeta, "timestamp")

		gwMeta, ok := networkMeta["gw"].(map[string]any)
		require.True(t, ok)
		assert.Contains(t, gwMeta, "timestamp")
	})

	t.Run("deeply nested metadata", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{
					"lights": map[string]any{
						"color": map[string]any{"r": 255.0, "g": 255.0, "b": 255.0},
					},
				},
			},
		})
		require.NoError(t, err)

		lightsMeta, ok := doc.Metadata.Desired["lights"].(map[string]any)
		require.True(t, ok)

		colorMeta, ok := lightsMeta["color"].(map[string]any)
		require.True(t, ok)

		rMeta, ok := colorMeta["r"].(map[string]any)
		require.True(t, ok)
		assert.Contains(t, rMeta, "timestamp")
	})
}

func TestHandler_DocumentsMessage(t *testing.T) {
	t.Run("contains previous and current", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithClassicShadow(), WithPublishFunc(publishFunc))
		client := newMockServerClient("dev1", "default")

		// First update
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Second update
		published = nil
		client.sent = nil
		payload, _ = json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 25.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		mu.Lock()
		defer mu.Unlock()

		// Find documents message
		for _, msg := range published {
			if msg.Topic == "$things/dev1/shadow/update/documents" {
				var docsMsg DocumentsMessage
				require.NoError(t, json.Unmarshal(msg.Payload, &docsMsg))

				assert.Equal(t, int64(1), docsMsg.Previous.Version)
				assert.Equal(t, 22.0, docsMsg.Previous.State.Desired["temp"])
				assert.Equal(t, int64(2), docsMsg.Current.Version)
				assert.Equal(t, 25.0, docsMsg.Current.State.Desired["temp"])
				assert.NotZero(t, docsMsg.Timestamp)
				return
			}
		}
		t.Fatal("documents message not found")
	})

	t.Run("first update has empty previous", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithClassicShadow(), WithPublishFunc(publishFunc))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		mu.Lock()
		defer mu.Unlock()

		for _, msg := range published {
			if msg.Topic == "$things/dev1/shadow/update/documents" {
				var docsMsg DocumentsMessage
				require.NoError(t, json.Unmarshal(msg.Payload, &docsMsg))

				assert.Equal(t, int64(0), docsMsg.Previous.Version)
				assert.Nil(t, docsMsg.Previous.State.Desired)
				assert.Equal(t, int64(1), docsMsg.Current.Version)
				assert.Equal(t, 22.0, docsMsg.Current.State.Desired["temp"])
				return
			}
		}
		t.Fatal("documents message not found")
	})
}

func TestHandler_AcceptedResponse(t *testing.T) {
	t.Run("contains only requested fields", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// First set multiple desired fields
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{"temp": 22.0, "mode": "cool", "fan": "high"},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Update only temp
		client.sent = nil
		payload, _ = json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 25.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))

		// Accepted response should only contain temp, not mode or fan
		acceptedState := accepted["state"].(map[string]any)
		desired := acceptedState["desired"].(map[string]any)
		assert.Equal(t, 25.0, desired["temp"])
		assert.NotContains(t, desired, "mode")
		assert.NotContains(t, desired, "fan")
		assert.NotZero(t, accepted["version"])
		assert.NotZero(t, accepted["timestamp"])
	})

	t.Run("includes metadata for requested fields", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{
					"network": map[string]any{"dns": "8.8.8.8"},
				},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))

		meta := accepted["metadata"].(map[string]any)
		desiredMeta := meta["desired"].(map[string]any)
		networkMeta, ok := desiredMeta["network"].(map[string]any)
		require.True(t, ok)
		dnsMeta, ok := networkMeta["dns"].(map[string]any)
		require.True(t, ok)
		assert.Contains(t, dnsMeta, "timestamp")
	})
}

func TestHandler_DirectMethods(t *testing.T) {
	t.Run("Update and Get", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithPublishFunc(publishFunc))
		key := Key{Namespace: "default", ClientID: "dev1"}

		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{"temp": 22.0},
			},
		})

		require.NoError(t, err)
		require.NotNil(t, doc)
		assert.Equal(t, int64(1), doc.Version)
		assert.Equal(t, 22.0, doc.State.Desired["temp"])

		mu.Lock()
		assert.GreaterOrEqual(t, len(published), 1)
		mu.Unlock()

		doc, err = h.Get(key)
		require.NoError(t, err)
		require.NotNil(t, doc)
		assert.Equal(t, int64(1), doc.Version)
	})

	t.Run("Get non-existent", func(t *testing.T) {
		h := NewHandler()
		_, err := h.Get(Key{Namespace: "default", ClientID: "dev1"})
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeNotFound, errResp.Code)
	})

	t.Run("Update empty state", func(t *testing.T) {
		h := NewHandler()
		_, err := h.Update(Key{Namespace: "default", ClientID: "dev1"}, UpdateRequest{
			State: UpdateState{},
		})
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
	})

	t.Run("Delete", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, _ = h.Update(key, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		doc, err := h.Delete(key)
		require.NoError(t, err)
		require.NotNil(t, doc)

		_, err = h.Get(key)
		require.Error(t, err)
	})

	t.Run("Delete non-existent", func(t *testing.T) {
		h := NewHandler()
		_, err := h.Delete(Key{Namespace: "default", ClientID: "dev1"})
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeNotFound, errResp.Code)
	})

	t.Run("Update publishes delta and documents", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithPublishFunc(publishFunc))
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, _ = h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})

		mu.Lock()
		defer mu.Unlock()

		assert.Len(t, published, 2)

		var hasDelta, hasDocs bool
		for _, msg := range published {
			if msg.Topic == "$things/dev1/shadow/update/delta" {
				hasDelta = true
				assert.Equal(t, "default", msg.Namespace)
			}
			if msg.Topic == "$things/dev1/shadow/update/documents" {
				hasDocs = true

				// Verify it's a DocumentsMessage
				var docsMsg DocumentsMessage
				require.NoError(t, json.Unmarshal(msg.Payload, &docsMsg))
				assert.Equal(t, int64(1), docsMsg.Current.Version)
			}
		}
		assert.True(t, hasDelta)
		assert.True(t, hasDocs)
	})
}

func TestHandler_WithWatcher(t *testing.T) {
	t.Run("watcher triggers publish", func(t *testing.T) {
		watcher := &mockWatcherStore{
			MemoryStore: NewMemoryStore(),
		}

		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(
			WithStore(watcher),
			WithPublishFunc(publishFunc),
		)
		defer h.Close()

		key := Key{Namespace: "default", ClientID: "dev1"}
		doc := &Document{
			State: State{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
				Delta:    map[string]any{"temp": 22.0},
			},
			Version: 1,
		}

		watcher.triggerChange(key, doc)

		mu.Lock()
		assert.Len(t, published, 2)
		mu.Unlock()
	})
}

func TestHandler_StoreErrors(t *testing.T) {
	t.Run("get with store error via HandleMessage", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithStore(&errorStore{err: errStoreFailure}))
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeInternal, errResp.Code)
	})

	t.Run("update with store Get error via HandleMessage", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithStore(&errorStore{err: errStoreFailure}))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeInternal, errResp.Code)
	})

	t.Run("update with store Save error via HandleMessage", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithStore(&errorStore{failOn: "Save", err: errStoreFailure}))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)
	})

	t.Run("delete with store error via HandleMessage", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithStore(&errorStore{err: errStoreFailure}))
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/delete",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/delete/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeInternal, errResp.Code)
	})

	t.Run("Get direct with store error", func(t *testing.T) {
		h := NewHandler(WithStore(&errorStore{err: errStoreFailure}))
		_, err := h.Get(Key{Namespace: "default", ClientID: "dev1"})
		require.Error(t, err)
		assert.Equal(t, errStoreFailure, err)
	})

	t.Run("Update direct with store error", func(t *testing.T) {
		h := NewHandler(WithStore(&errorStore{err: errStoreFailure}))
		_, err := h.Update(Key{Namespace: "default", ClientID: "dev1"}, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		require.Error(t, err)
	})

	t.Run("Delete direct with store error", func(t *testing.T) {
		h := NewHandler(WithStore(&errorStore{err: errStoreFailure}))
		_, err := h.Delete(Key{Namespace: "default", ClientID: "dev1"})
		require.Error(t, err)
		assert.Equal(t, errStoreFailure, err)
	})

	t.Run("update without publishFunc does not panic", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)
	})
}

func TestHandler_WatcherError(t *testing.T) {
	t.Run("watcher Watch returns error", func(t *testing.T) {
		watcher := &errorWatcherStore{
			MemoryStore: NewMemoryStore(),
			watchErr:    errStoreFailure,
		}

		h := NewHandler(
			WithStore(watcher),
			WithPublishFunc(func(_ *mqttv5.Message) error { return nil }),
		)

		assert.Nil(t, h.stopWatch)
		assert.NoError(t, h.Close())
	})
}

func TestHandler_Close(t *testing.T) {
	t.Run("close without watcher", func(t *testing.T) {
		h := NewHandler()
		assert.NoError(t, h.Close())
	})

	t.Run("close with watcher", func(t *testing.T) {
		watcher := &mockWatcherStore{
			MemoryStore: NewMemoryStore(),
		}

		h := NewHandler(
			WithStore(watcher),
			WithPublishFunc(func(_ *mqttv5.Message) error { return nil }),
		)

		assert.NoError(t, h.Close())
		assert.Nil(t, watcher.onChange)
	})
}

func TestHandler_DeltaResponse(t *testing.T) {
	t.Run("delta includes metadata for delta keys only", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithClassicShadow(), WithPublishFunc(publishFunc))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0, "mode": "cool"},
				Reported: map[string]any{"temp": 20.0, "mode": "cool"},
			},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		mu.Lock()
		defer mu.Unlock()

		require.GreaterOrEqual(t, len(published), 1)
		assert.Contains(t, published[0].Topic, "update/delta")

		var delta deltaResponse
		require.NoError(t, json.Unmarshal(published[0].Payload, &delta))

		// Delta should only contain temp (mode matches)
		assert.Equal(t, map[string]any{"temp": 22.0}, delta.State)
		assert.NotZero(t, delta.Version)
		assert.NotZero(t, delta.Timestamp)

		// Metadata should only have temp entry
		require.NotNil(t, delta.Metadata)
		tempMeta, ok := delta.Metadata["temp"].(map[string]any)
		require.True(t, ok)
		assert.Contains(t, tempMeta, "timestamp")
		assert.NotContains(t, delta.Metadata, "mode")
	})

	t.Run("delta includes clientToken", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithClassicShadow(), WithPublishFunc(publishFunc))
		client := newMockServerClient("dev1", "default")

		payload := []byte(`{"state":{"desired":{"temp":22},"reported":{"temp":20}},"clientToken":"delta-token"}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		mu.Lock()
		defer mu.Unlock()

		require.GreaterOrEqual(t, len(published), 1)

		var delta deltaResponse
		require.NoError(t, json.Unmarshal(published[0].Payload, &delta))
		assert.Equal(t, "delta-token", delta.ClientToken)
	})
}

func TestHandler_AcceptedNullEcho(t *testing.T) {
	t.Run("clear desired echoes null", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Create shadow with desired
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Clear desired
		client.sent = nil
		payload = []byte(`{"state":{"desired":null}}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))
		state := accepted["state"].(map[string]any)

		// desired should be explicitly null (present but nil)
		desiredVal, hasDesired := state["desired"]
		assert.True(t, hasDesired, "desired should be present in state")
		assert.Nil(t, desiredVal, "desired should be null")
	})

	t.Run("clear reported echoes null", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Reported: map[string]any{"temp": 20.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		client.sent = nil
		payload = []byte(`{"state":{"reported":null}}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))
		state := accepted["state"].(map[string]any)

		reportedVal, hasReported := state["reported"]
		assert.True(t, hasReported)
		assert.Nil(t, reportedVal)
	})
}

func TestHandler_DeleteAcceptedResponse(t *testing.T) {
	t.Run("returns version and timestamp only", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/delete",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/delete/accepted", msg.Topic)

		var resp deleteAcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Equal(t, int64(1), resp.Version)
		assert.NotZero(t, resp.Timestamp)

		// Verify no state fields in response
		var raw map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &raw))
		assert.NotContains(t, raw, "state")
		assert.NotContains(t, raw, "metadata")
	})
}

func TestHandler_VersionInheritance(t *testing.T) {
	t.Run("recreated shadow inherits version", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Create shadow (version 1)
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Update (version 2)
		client.sent = nil
		payload, _ = json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 25.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Delete
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/delete",
		})

		// Recreate — should inherit version 2, so new version is 3
		client.sent = nil
		payload, _ = json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 30.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))
		assert.Equal(t, float64(3), accepted["version"])
	})

	t.Run("expired TTL does not inherit version", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithDeletedVersionTTL(0))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Delete
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/delete",
		})

		// Recreate — TTL is 0, so version should start at 1
		client.sent = nil
		payload, _ = json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 30.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))
		assert.Equal(t, float64(1), accepted["version"])
	})
}

func TestHandler_Throttling(t *testing.T) {
	t.Run("rejects when inflight limit reached", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithLimits(Limits{
			MaxPayloadSize:       defaultMaxPayloadSize,
			MaxDepth:             defaultMaxDepth,
			MaxInflightPerClient: 1,
		}))

		// Pre-fill the semaphore
		h.acquireInflight("dev1")

		client := newMockServerClient("dev1", "default")
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeTooManyRequests, errResp.Code)
		assert.Equal(t, "too many requests", errResp.Message)

		h.releaseInflight("dev1")
	})

	t.Run("allows requests within limit", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithLimits(Limits{
			MaxPayloadSize:       defaultMaxPayloadSize,
			MaxDepth:             defaultMaxDepth,
			MaxInflightPerClient: 10,
		}))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)
	})

	t.Run("zero limit means unlimited", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithLimits(Limits{
			MaxPayloadSize:       defaultMaxPayloadSize,
			MaxDepth:             defaultMaxDepth,
			MaxInflightPerClient: 0,
		}))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)
	})
}

func TestHandler_MaxDocumentSize(t *testing.T) {
	t.Run("rejects update exceeding max document size", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithLimits(Limits{
			MaxPayloadSize:  defaultMaxPayloadSize,
			MaxDepth:        defaultMaxDepth,
			MaxDocumentSize: 64,
		}))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{
				"data": "this is a fairly long string value that should exceed the limit",
			}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodePayloadTooLarge, errResp.Code)
		assert.Contains(t, errResp.Message, "resulting document exceeds maximum size")
	})

	t.Run("allows update within max document size", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithLimits(Limits{
			MaxPayloadSize:  defaultMaxPayloadSize,
			MaxDepth:        defaultMaxDepth,
			MaxDocumentSize: 4096,
		}))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)
	})
}

func TestHandler_MaxNamedShadows(t *testing.T) {
	t.Run("rejects when max named shadows reached", func(t *testing.T) {
		h := NewHandler(WithNamedShadow(), WithLimits(Limits{
			MaxPayloadSize:  defaultMaxPayloadSize,
			MaxDepth:        defaultMaxDepth,
			MaxNamedShadows: 2,
		}))
		client := newMockServerClient("dev1", "default")

		// Create 2 named shadows
		for _, name := range []string{"config", "firmware"} {
			payload, _ := json.Marshal(UpdateRequest{
				State: UpdateState{Desired: map[string]any{"v": 1}},
			})
			h.HandleMessage(client, &mqttv5.Message{
				Topic:   fmt.Sprintf("$things/dev1/shadow/name/%s/update", name),
				Payload: payload,
			})
		}

		// Try to create a 3rd
		client.sent = nil
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"v": 1}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/name/extra/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/name/extra/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "maximum of 2 named shadows reached")
	})

	t.Run("allows update to existing named shadow at limit", func(t *testing.T) {
		h := NewHandler(WithNamedShadow(), WithLimits(Limits{
			MaxPayloadSize:  defaultMaxPayloadSize,
			MaxDepth:        defaultMaxDepth,
			MaxNamedShadows: 1,
		}))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"v": 1}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/name/config/update",
			Payload: payload,
		})

		// Update same shadow should work
		client.sent = nil
		payload, _ = json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"v": 2}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/name/config/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/name/config/update/accepted", msg.Topic)
	})

	t.Run("does not apply to shared shadows", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(func(_, _ string) bool { return true }), WithLimits(Limits{
			MaxPayloadSize:  defaultMaxPayloadSize,
			MaxDepth:        defaultMaxDepth,
			MaxNamedShadows: 1,
		}))
		client := newMockServerClient("dev1", "default")

		// Create multiple shared named shadows — should not be limited
		for _, name := range []string{"config", "firmware"} {
			payload, _ := json.Marshal(UpdateRequest{
				State: UpdateState{Desired: map[string]any{"v": 1}},
			})
			h.HandleMessage(client, &mqttv5.Message{
				Topic:   fmt.Sprintf("$things/$shared/room-101/shadow/name/%s/update", name),
				Payload: payload,
			})
		}

		msgs := client.allMessages()
		for _, msg := range msgs {
			assert.Contains(t, msg.Topic, "accepted")
		}
	})
}

func TestHandler_DeleteWithClientToken(t *testing.T) {
	t.Run("delete without payload", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Create a shadow first
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Reported: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/delete",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/delete/accepted", msg.Topic)

		var resp deleteAcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Empty(t, resp.ClientToken)
		assert.NotZero(t, resp.Version)
	})

	t.Run("delete with clientToken", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Create a shadow
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Reported: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Delete with clientToken
		delPayload, _ := json.Marshal(deleteRequest{ClientToken: "del-123"})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/delete",
			Payload: delPayload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/delete/accepted", msg.Topic)

		var resp deleteAcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Equal(t, "del-123", resp.ClientToken)
	})

	t.Run("delete not found with clientToken", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		delPayload, _ := json.Marshal(deleteRequest{ClientToken: "del-404"})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/delete",
			Payload: delPayload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/delete/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeNotFound, errResp.Code)
		assert.Equal(t, "del-404", errResp.ClientToken)
	})

	t.Run("delete with invalid payload", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/delete",
			Payload: []byte("not json"),
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/delete/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
	})
}

func TestHandler_GetWithClientToken(t *testing.T) {
	t.Run("get without payload", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Create a shadow first
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Reported: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get/accepted", msg.Topic)

		var resp getAcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Empty(t, resp.ClientToken)
		assert.Equal(t, 22.0, resp.State.Reported["temp"])
	})

	t.Run("get with clientToken", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Create a shadow
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Reported: map[string]any{"temp": 22.0}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Get with clientToken
		getPayload, _ := json.Marshal(getRequest{ClientToken: "req-123"})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/get",
			Payload: getPayload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get/accepted", msg.Topic)

		var resp getAcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Equal(t, "req-123", resp.ClientToken)
	})

	t.Run("get with invalid payload", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/get",
			Payload: []byte("not json"),
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
	})

	t.Run("get not found with clientToken", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		getPayload, _ := json.Marshal(getRequest{ClientToken: "tok-404"})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/get",
			Payload: getPayload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/get/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeNotFound, errResp.Code)
		assert.Equal(t, "tok-404", errResp.ClientToken)
	})
}

func TestHandler_List(t *testing.T) {
	t.Run("list named shadows", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		client := newMockServerClient("dev1", "default")

		// Create named shadows
		for _, name := range []string{"config", "firmware"} {
			payload, _ := json.Marshal(UpdateRequest{
				State: UpdateState{Desired: map[string]any{"v": 1}},
			})
			h.HandleMessage(client, &mqttv5.Message{
				Topic:   fmt.Sprintf("$things/dev1/shadow/name/%s/update", name),
				Payload: payload,
			})
		}

		// List
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/list",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/list/accepted", msg.Topic)

		var resp listAcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Equal(t, []string{"config", "firmware"}, resp.Results)
		assert.Empty(t, resp.NextToken)
	})

	t.Run("list with clientToken", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		client := newMockServerClient("dev1", "default")

		listPayload, _ := json.Marshal(listRequest{ClientToken: "list-tok"})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/list",
			Payload: listPayload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/list/accepted", msg.Topic)

		var resp listAcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Equal(t, "list-tok", resp.ClientToken)
		assert.Empty(t, resp.Results)
	})

	t.Run("list with pagination", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		client := newMockServerClient("dev1", "default")

		for _, name := range []string{"a", "b", "c", "d", "e"} {
			payload, _ := json.Marshal(UpdateRequest{
				State: UpdateState{Desired: map[string]any{"v": 1}},
			})
			h.HandleMessage(client, &mqttv5.Message{
				Topic:   fmt.Sprintf("$things/dev1/shadow/name/%s/update", name),
				Payload: payload,
			})
		}

		// Page 1
		listPayload, _ := json.Marshal(listRequest{PageSize: 3})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/list",
			Payload: listPayload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var resp listAcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Len(t, resp.Results, 3)
		assert.NotEmpty(t, resp.NextToken)

		// Page 2
		listPayload, _ = json.Marshal(listRequest{PageSize: 3, NextToken: resp.NextToken})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/list",
			Payload: listPayload,
		})

		msg = client.lastMessage()
		var resp2 listAcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp2))
		assert.Len(t, resp2.Results, 2)
		assert.Empty(t, resp2.NextToken)
	})

	t.Run("list rejected when named disabled", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/list",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/list/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeForbidden, errResp.Code)
	})

	t.Run("list rejected with invalid payload", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/list",
			Payload: []byte("not json"),
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/list/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
	})

	t.Run("list with store not implementing Lister", func(t *testing.T) {
		store := &errorStore{failOn: "none", err: nil}
		h := NewHandler(WithNamedShadow(), WithStore(store))
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/list",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/list/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeInternal, errResp.Code)
	})
}

func TestHandler_SetConnected(t *testing.T) {
	t.Run("set connected on existing shadow", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{Reported: map[string]any{"temp": 22.0}},
		})
		require.NoError(t, err)

		require.NoError(t, h.SetConnected(key, true))

		doc, err := h.Get(key)
		require.NoError(t, err)
		require.NotNil(t, doc.Metadata.Connection)
		assert.True(t, doc.Metadata.Connection.Connected)
		assert.NotZero(t, doc.Metadata.Connection.ConnectionTimestamp)
	})

	t.Run("set disconnected", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{Reported: map[string]any{"temp": 22.0}},
		})
		require.NoError(t, err)

		require.NoError(t, h.SetConnected(key, true))
		require.NoError(t, h.SetConnected(key, false))

		doc, err := h.Get(key)
		require.NoError(t, err)
		require.NotNil(t, doc.Metadata.Connection)
		assert.False(t, doc.Metadata.Connection.Connected)
	})

	t.Run("no-op on non-existent shadow", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		key := Key{Namespace: "default", ClientID: "dev1"}

		err := h.SetConnected(key, true)
		require.NoError(t, err)
	})
}

func TestHandler_ListDirect(t *testing.T) {
	t.Run("list via direct method", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, err := h.Update(Key{Namespace: "default", ClientID: "dev1", ShadowName: "config"}, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"v": 1}},
		})
		require.NoError(t, err)

		result, err := h.List(key, 25, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"config"}, result.Results)
	})

	t.Run("list with non-Lister store", func(t *testing.T) {
		store := &errorStore{failOn: "none", err: nil}
		h := NewHandler(WithNamedShadow(), WithStore(store))
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, err := h.List(key, 25, "")
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeInternal, errResp.Code)
	})
}

func TestHandler_PublishDelta(t *testing.T) {
	t.Run("publishes pending delta", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithPublishFunc(publishFunc))
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})
		require.NoError(t, err)

		// Clear published messages from update
		mu.Lock()
		published = nil
		mu.Unlock()

		// Simulate reconnect
		err = h.PublishDelta(key)
		require.NoError(t, err)

		mu.Lock()
		require.Len(t, published, 1)
		assert.Equal(t, "$things/dev1/shadow/update/delta", published[0].Topic)
		assert.Equal(t, "default", published[0].Namespace)

		var delta deltaResponse
		require.NoError(t, json.Unmarshal(published[0].Payload, &delta))
		assert.Equal(t, 22.0, delta.State["temp"])
		assert.Equal(t, int64(1), delta.Version)
		mu.Unlock()
	})

	t.Run("no-op when no delta", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithPublishFunc(publishFunc))
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 22.0},
			},
		})
		require.NoError(t, err)

		mu.Lock()
		published = nil
		mu.Unlock()

		err = h.PublishDelta(key)
		require.NoError(t, err)

		mu.Lock()
		assert.Empty(t, published)
		mu.Unlock()
	})

	t.Run("no-op when shadow does not exist", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithPublishFunc(publishFunc))
		key := Key{Namespace: "default", ClientID: "dev1"}

		err := h.PublishDelta(key)
		require.NoError(t, err)

		mu.Lock()
		assert.Empty(t, published)
		mu.Unlock()
	})

	t.Run("no-op without publishFunc", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})
		require.NoError(t, err)

		err = h.PublishDelta(key)
		require.NoError(t, err)
	})

	t.Run("shared shadow delta", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithPublishFunc(publishFunc))
		key := Key{Namespace: "default", GroupName: "room-101", ShadowName: "config"}

		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})
		require.NoError(t, err)

		mu.Lock()
		published = nil
		mu.Unlock()

		err = h.PublishDelta(key)
		require.NoError(t, err)

		mu.Lock()
		require.Len(t, published, 1)
		assert.Equal(t, "$things/$shared/room-101/shadow/name/config/update/delta", published[0].Topic)
		mu.Unlock()
	})

	t.Run("store error propagates", func(t *testing.T) {
		h := NewHandler(
			WithStore(&errorStore{err: errStoreFailure}),
			WithPublishFunc(func(_ *mqttv5.Message) error { return nil }),
		)
		key := Key{Namespace: "default", ClientID: "dev1"}

		err := h.PublishDelta(key)
		assert.ErrorIs(t, err, errStoreFailure)
	})
}

func TestHandler_DeleteVersionInheritanceDirect(t *testing.T) {
	t.Run("direct delete tracks version for inheritance", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		// Create shadow with multiple updates to get version > 1
		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})
		require.NoError(t, err)

		_, err = h.Update(key, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 23.0}},
		})
		require.NoError(t, err)

		doc, err := h.Delete(key)
		require.NoError(t, err)
		assert.Equal(t, int64(2), doc.Version)

		// Re-create shadow, version should continue from deleted version
		newDoc, err := h.Update(key, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 25.0}},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(3), newDoc.Version)
	})

	t.Run("direct delete not found", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, err := h.Delete(key)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeNotFound, errResp.Code)
	})
}

func TestHandler_DeltaClearing(t *testing.T) {
	t.Run("delta cleared when reported matches desired", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex
		publishFunc := func(msg *mqttv5.Message) error {
			mu.Lock()
			published = append(published, msg)
			mu.Unlock()
			return nil
		}

		h := NewHandler(WithClassicShadow(), WithPublishFunc(publishFunc))
		client := newMockServerClient("dev1", "default")

		// Create shadow with delta (desired != reported)
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Verify delta exists
		mu.Lock()
		hasDelta := false
		for _, msg := range published {
			if msg.Topic == "$things/dev1/shadow/update/delta" {
				hasDelta = true
			}
		}
		assert.True(t, hasDelta, "delta should be published on first update")
		published = nil
		mu.Unlock()

		// Device reports matching value, clearing the delta
		client.sent = nil
		payload, _ = json.Marshal(UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{"temp": 22.0},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// No delta should be published
		mu.Lock()
		for _, msg := range published {
			assert.NotContains(t, msg.Topic, "update/delta", "delta should not be published when reported matches desired")
		}
		mu.Unlock()

		// Verify document has no delta
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var doc Document
		require.NoError(t, json.Unmarshal(msg.Payload, &doc))
		assert.Nil(t, doc.State.Delta)
		assert.Equal(t, 22.0, doc.State.Desired["temp"])
		assert.Equal(t, 22.0, doc.State.Reported["temp"])
	})

	t.Run("partial delta clearing", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Create shadow with two keys in delta
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0, "mode": "cool"},
				Reported: map[string]any{"temp": 20.0, "mode": "auto"},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Report only one matching value
		client.sent = nil
		payload, _ = json.Marshal(UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{"temp": 22.0},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Get document and verify partial delta
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var doc Document
		require.NoError(t, json.Unmarshal(msg.Payload, &doc))
		assert.Equal(t, map[string]any{"mode": "cool"}, doc.State.Delta)
	})

	t.Run("nested delta clearing", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Create shadow with nested delta (fan differs)
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{
					"config": map[string]any{"fan": "high", "mode": "auto"},
				},
				Reported: map[string]any{
					"config": map[string]any{"fan": "low", "mode": "auto"},
				},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Report matching nested value (merge is shallow, must include all nested keys)
		client.sent = nil
		payload, _ = json.Marshal(UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"config": map[string]any{"fan": "high", "mode": "auto"},
				},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Verify delta is fully cleared
		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var doc Document
		require.NoError(t, json.Unmarshal(msg.Payload, &doc))
		assert.Nil(t, doc.State.Delta)
	})
}

func TestHandler_AcceptedResponseFields(t *testing.T) {
	t.Run("accepted contains only desired when only desired updated", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{"temp": 22.0},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))

		state := accepted["state"].(map[string]any)
		assert.Contains(t, state, "desired")
		assert.NotContains(t, state, "reported")
	})

	t.Run("accepted contains only reported when only reported updated", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{"temp": 20.0},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))

		state := accepted["state"].(map[string]any)
		assert.NotContains(t, state, "desired")
		assert.Contains(t, state, "reported")
	})

	t.Run("accepted contains both when both updated", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))

		state := accepted["state"].(map[string]any)
		assert.Contains(t, state, "desired")
		assert.Contains(t, state, "reported")
		assert.NotContains(t, state, "delta")
	})

	t.Run("accepted contains request values not full document", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// First update with multiple keys
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{"temp": 22.0, "mode": "cool"},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Second update with only one key
		client.sent = nil
		payload, _ = json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{"temp": 25.0},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))

		state := accepted["state"].(map[string]any)
		desired := state["desired"].(map[string]any)
		assert.Equal(t, 25.0, desired["temp"])
		assert.NotContains(t, desired, "mode", "accepted should only include keys from the request")
	})

	t.Run("accepted includes clientToken", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload := []byte(`{"state":{"desired":{"temp":22}},"clientToken":"tok-123"}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))
		assert.Equal(t, "tok-123", accepted["clientToken"])
	})

	t.Run("accepted for clear desired shows null", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Create shadow
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Clear desired
		client.sent = nil
		payload = []byte(`{"state":{"desired":null}}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var accepted map[string]any
		require.NoError(t, json.Unmarshal(msg.Payload, &accepted))

		state := accepted["state"].(map[string]any)
		assert.Contains(t, state, "desired")
		assert.Nil(t, state["desired"])
		assert.NotContains(t, state, "reported")
	})
}

func TestHandler_SetConnectedErrors(t *testing.T) {
	t.Run("store get error", func(t *testing.T) {
		store := &errorStore{failOn: "Get", err: errStoreFailure}
		h := NewHandler(WithStore(store))
		key := Key{Namespace: "default", ClientID: "dev1"}

		err := h.SetConnected(key, true)
		assert.ErrorIs(t, err, errStoreFailure)
	})
}

func TestHandler_ThrottleListRequest(t *testing.T) {
	t.Run("throttle on list returns 429", func(t *testing.T) {
		h := NewHandler(
			WithNamedShadow(),
			WithLimits(Limits{MaxInflightPerClient: 1}),
		)
		client := newMockServerClient("dev1", "default")

		// Fill the inflight slot
		h.acquireInflight(client.ClientID())

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/list",
		})
		assert.True(t, handled)

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/list/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeTooManyRequests, errResp.Code)

		h.releaseInflight(client.ClientID())
	})
}

func TestHandler_HandleListStoreError(t *testing.T) {
	t.Run("lister store error", func(t *testing.T) {
		store := &errorListerStore{
			MemoryStore: NewMemoryStore(),
			listErr:     errStoreFailure,
		}
		h := NewHandler(WithNamedShadow(), WithStore(store))
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/list",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/list/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeInternal, errResp.Code)
	})
}

func TestHandler_CheckNamedShadowLimit(t *testing.T) {
	t.Run("unlimited when MaxNamedShadows is zero", func(t *testing.T) {
		h := NewHandler(
			WithNamedShadow(),
			WithLimits(Limits{MaxNamedShadows: 0}),
		)
		key := Key{Namespace: "default", ClientID: "dev1", ShadowName: "s1"}

		// Should not fail even with many shadows
		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"v": 1}},
		})
		require.NoError(t, err)
	})

	t.Run("non-Counter store skips limit check", func(t *testing.T) {
		store := &errorStore{failOn: "none", err: nil}
		h := NewHandler(
			WithNamedShadow(),
			WithStore(store),
			WithLimits(Limits{MaxNamedShadows: 1}),
		)
		key := Key{Namespace: "default", ClientID: "dev1", ShadowName: "s1"}

		// errorStore doesn't implement Counter, so limit check is skipped
		// But the update will fail because Get returns nil and Save will also
		// return nil (failOn="none" means no error). Let me use a real store...
		// Actually errorStore with failOn="none" doesn't fail on anything.
		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"v": 1}},
		})
		require.NoError(t, err)
	})

	t.Run("counter error returns internal error", func(t *testing.T) {
		store := &errorCounterStore{
			MemoryStore: NewMemoryStore(),
			countErr:    errStoreFailure,
		}
		h := NewHandler(
			WithNamedShadow(),
			WithStore(store),
			WithLimits(Limits{MaxNamedShadows: 5}),
		)
		key := Key{Namespace: "default", ClientID: "dev1", ShadowName: "s1"}

		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"v": 1}},
		})
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeInternal, errResp.Code)
	})
}

func TestHandler_NullAllReportedKeys(t *testing.T) {
	t.Run("null all reported keys clears section", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{"temp": 20.0, "mode": "auto"},
			},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		// Null both reported keys individually
		client.sent = nil
		payload = []byte(`{"state":{"reported":{"temp":null,"mode":null}}}`)
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		client.sent = nil
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)

		var doc Document
		require.NoError(t, json.Unmarshal(msg.Payload, &doc))
		assert.Nil(t, doc.State.Reported)
		assert.Nil(t, doc.Metadata.Reported)
	})
}

func TestHandler_ValidateStateDirectAPI(t *testing.T) {
	t.Run("desired exceeds MaxTotalKeys", func(t *testing.T) {
		h := NewHandler(WithLimits(Limits{MaxTotalKeys: 2}))
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{"a": 1, "b": 2, "c": 3},
			},
		})
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "total keys")
	})

	t.Run("reported exceeds MaxTotalKeys", func(t *testing.T) {
		h := NewHandler(WithLimits(Limits{MaxTotalKeys: 2}))
		key := Key{Namespace: "default", ClientID: "dev1"}

		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{"a": 1, "b": 2, "c": 3},
			},
		})
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "total keys")
	})
}

func TestHandler_DeepMerge(t *testing.T) {
	t.Run("partial nested update preserves sibling keys", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		// Set initial nested state
		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"config": map[string]any{
						"wifi":      "enabled",
						"bluetooth": "disabled",
						"volume":    float64(50),
					},
				},
			},
		})
		require.NoError(t, err)

		// Update only one nested key
		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"config": map[string]any{
						"volume": float64(80),
					},
				},
			},
		})
		require.NoError(t, err)

		config, ok := doc.State.Reported["config"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "enabled", config["wifi"])
		assert.Equal(t, "disabled", config["bluetooth"])
		assert.Equal(t, float64(80), config["volume"])
	})

	t.Run("null nested key removes only that key", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		// Set initial nested state
		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{
					"settings": map[string]any{
						"color": "blue",
						"size":  float64(10),
						"mode":  "auto",
					},
				},
			},
		})
		require.NoError(t, err)

		// Delete only "size" inside "settings"
		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Desired: map[string]any{
					"settings": map[string]any{
						"size": nil,
					},
				},
			},
		})
		require.NoError(t, err)

		settings, ok := doc.State.Desired["settings"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "blue", settings["color"])
		assert.Equal(t, "auto", settings["mode"])
		_, exists := settings["size"]
		assert.False(t, exists)

		// Metadata should also not have "size"
		settingsMeta, ok := doc.Metadata.Desired["settings"].(map[string]any)
		require.True(t, ok)
		_, metaExists := settingsMeta["size"]
		assert.False(t, metaExists)
		assert.Contains(t, settingsMeta, "color")
		assert.Contains(t, settingsMeta, "mode")
	})

	t.Run("scalar to map replacement", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		// Set initial state with a scalar value
		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"network": "simple",
				},
			},
		})
		require.NoError(t, err)

		// Replace scalar with a nested map
		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"network": map[string]any{
						"ssid":     "home",
						"password": "secret",
					},
				},
			},
		})
		require.NoError(t, err)

		network, ok := doc.State.Reported["network"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "home", network["ssid"])
		assert.Equal(t, "secret", network["password"])
	})

	t.Run("map to scalar replacement", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		// Set initial state with a nested map
		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"network": map[string]any{
						"ssid":     "home",
						"password": "secret",
					},
				},
			},
		})
		require.NoError(t, err)

		// Replace nested map with a scalar
		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"network": "disabled",
				},
			},
		})
		require.NoError(t, err)

		assert.Equal(t, "disabled", doc.State.Reported["network"])
	})

	t.Run("deeply nested merge preserves structure", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		// Set initial deeply nested state
		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"level1": map[string]any{
						"level2": map[string]any{
							"a": float64(1),
							"b": float64(2),
						},
						"other": "keep",
					},
				},
			},
		})
		require.NoError(t, err)

		// Update only the deeply nested key
		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"level1": map[string]any{
						"level2": map[string]any{
							"a": float64(99),
						},
					},
				},
			},
		})
		require.NoError(t, err)

		level1, ok := doc.State.Reported["level1"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "keep", level1["other"])

		level2, ok := level1["level2"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, float64(99), level2["a"])
		assert.Equal(t, float64(2), level2["b"])
	})

	t.Run("null all nested keys removes parent", func(t *testing.T) {
		h := NewHandler()
		key := Key{Namespace: "default", ClientID: "dev1"}

		// Set initial state
		_, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"config": map[string]any{
						"a": float64(1),
					},
					"other": "keep",
				},
			},
		})
		require.NoError(t, err)

		// Null the only key inside config
		doc, err := h.Update(key, UpdateRequest{
			State: UpdateState{
				Reported: map[string]any{
					"config": map[string]any{
						"a": nil,
					},
				},
			},
		})
		require.NoError(t, err)

		_, exists := doc.State.Reported["config"]
		assert.False(t, exists, "empty nested map should be removed")
		assert.Equal(t, "keep", doc.State.Reported["other"])
	})
}

func TestHandler_ConcurrentUpdates(t *testing.T) {
	h := NewHandler(
		WithClassicShadow(),
	)

	key := Key{
		Namespace: "default",
		ClientID:  "device1",
	}

	const goroutines = 10

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(val int) {
			defer wg.Done()

			_, err := h.Update(key, UpdateRequest{
				State: UpdateState{
					Reported: map[string]any{
						"counter": val,
					},
				},
			})
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()

	doc, err := h.Get(key)
	require.NoError(t, err)
	assert.Equal(t, int64(goroutines), doc.Version, "each update must increment version; no lost writes")
}

func TestHandler_WatcherNilDoc(t *testing.T) {
	t.Run("nil doc from watcher does not panic", func(t *testing.T) {
		watcher := &mockWatcherStore{
			MemoryStore: NewMemoryStore(),
		}

		var published []*mqttv5.Message
		var mu sync.Mutex

		h := NewHandler(
			WithClassicShadow(),
			WithStore(watcher),
			WithPublishFunc(func(msg *mqttv5.Message) error {
				mu.Lock()
				published = append(published, msg)
				mu.Unlock()
				return nil
			}),
		)
		defer h.Close()

		// Trigger watcher with nil doc — must not panic
		assert.NotPanics(t, func() {
			watcher.triggerChange(Key{Namespace: "default", ClientID: "dev1"}, nil)
		})

		mu.Lock()
		assert.Empty(t, published)
		mu.Unlock()
	})

	t.Run("non-nil doc from watcher publishes normally", func(t *testing.T) {
		watcher := &mockWatcherStore{
			MemoryStore: NewMemoryStore(),
		}

		var published []*mqttv5.Message
		var mu sync.Mutex

		h := NewHandler(
			WithClassicShadow(),
			WithStore(watcher),
			WithPublishFunc(func(msg *mqttv5.Message) error {
				mu.Lock()
				published = append(published, msg)
				mu.Unlock()
				return nil
			}),
		)
		defer h.Close()

		doc := &Document{
			State:     State{Delta: map[string]any{"temp": 22.0}},
			Version:   1,
			Timestamp: 100,
		}

		watcher.triggerChange(Key{Namespace: "default", ClientID: "dev1"}, doc)

		mu.Lock()
		assert.NotEmpty(t, published)
		mu.Unlock()
	})
}

func TestHandler_ShadowLockCleanup(t *testing.T) {
	h := NewHandler(WithClassicShadow())

	key := Key{Namespace: "default", ClientID: "dev1"}

	// Perform an update to create a lock entry
	_, err := h.Update(key, UpdateRequest{
		State: UpdateState{Reported: map[string]any{"temp": 20.0}},
	})
	require.NoError(t, err)

	// After the update completes, the lock entry should be cleaned up
	h.shadowLocksMu.Lock()
	_, exists := h.shadowLocks[shadowKey(key)]
	h.shadowLocksMu.Unlock()

	assert.False(t, exists, "lock entry should be removed after unlock")
}

func TestHandler_CorrelationData(t *testing.T) {
	corrData := []byte("corr-123")

	t.Run("get forwards correlation data", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:           "$things/dev1/shadow/get",
			CorrelationData: corrData,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, corrData, msg.CorrelationData)
	})

	t.Run("update forwards correlation data on accepted", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:           "$things/dev1/shadow/update",
			Payload:         payload,
			CorrelationData: corrData,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)
		assert.Equal(t, corrData, msg.CorrelationData)
	})

	t.Run("update forwards correlation data on rejected", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:           "$things/dev1/shadow/update",
			Payload:         []byte("bad"),
			CorrelationData: corrData,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)
		assert.Equal(t, corrData, msg.CorrelationData)
	})

	t.Run("delete forwards correlation data", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:           "$things/dev1/shadow/delete",
			CorrelationData: corrData,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, corrData, msg.CorrelationData)
	})

	t.Run("list forwards correlation data", func(t *testing.T) {
		h := NewHandler(WithNamedShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:           "$things/dev1/shadow/list",
			CorrelationData: corrData,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, corrData, msg.CorrelationData)
	})

	t.Run("nil correlation data is not set", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Nil(t, msg.CorrelationData)
	})

	t.Run("throttle error forwards correlation data", func(t *testing.T) {
		h := NewHandler(
			WithClassicShadow(),
			WithLimits(Limits{MaxInflightPerClient: 1}),
		)
		client := newMockServerClient("dev1", "default")

		// Fill the inflight slot
		h.acquireInflight("dev1")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:           "$things/dev1/shadow/get",
			CorrelationData: corrData,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, corrData, msg.CorrelationData)

		h.releaseInflight("dev1")
	})

	t.Run("shared forbidden forwards correlation data", func(t *testing.T) {
		h := NewHandler(WithSharedShadow(func(_, _ string) bool { return false }))
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:           "$things/$shared/room-1/shadow/get",
			CorrelationData: corrData,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, corrData, msg.CorrelationData)
	})
}

func TestHandler_ContentType(t *testing.T) {
	t.Run("client responses have application/json content type", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Get rejected (not found)
		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/get",
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "application/json", msg.ContentType)
	})

	t.Run("update accepted has content type", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "application/json", msg.ContentType)
	})

	t.Run("published messages have content type", func(t *testing.T) {
		var published []*mqttv5.Message
		var mu sync.Mutex

		h := NewHandler(
			WithClassicShadow(),
			WithPublishFunc(func(msg *mqttv5.Message) error {
				mu.Lock()
				published = append(published, msg)
				mu.Unlock()
				return nil
			}),
		)
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{
				Desired:  map[string]any{"temp": 22.0},
				Reported: map[string]any{"temp": 20.0},
			},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		mu.Lock()
		defer mu.Unlock()
		require.NotEmpty(t, published)
		for _, msg := range published {
			assert.Equal(t, "application/json", msg.ContentType, "topic: %s", msg.Topic)
		}
	})
}
