package shadow

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

// typedMockClient implements MQTTClient and captures subscribe handlers
// so tests can simulate server responses.
type typedMockClient struct {
	clientID  string
	mu        sync.Mutex
	handlers  map[string]mqttv5.MessageHandler
	published []*mqttv5.Message
}

func newTypedMockClient(clientID string) *typedMockClient {
	return &typedMockClient{
		clientID: clientID,
		handlers: make(map[string]mqttv5.MessageHandler),
	}
}

func (m *typedMockClient) ClientID() string { return m.clientID }

func (m *typedMockClient) Subscribe(filter string, _ byte, handler mqttv5.MessageHandler) error {
	m.mu.Lock()
	m.handlers[filter] = handler
	m.mu.Unlock()

	return nil
}

func (m *typedMockClient) Unsubscribe(_ ...string) error { return nil }

func (m *typedMockClient) Publish(msg *mqttv5.Message) error {
	m.mu.Lock()
	m.published = append(m.published, msg)
	m.mu.Unlock()

	return nil
}

func (m *typedMockClient) simulateResponse(topic string, payload []byte) {
	m.mu.Lock()
	handler, ok := m.handlers[topic]
	m.mu.Unlock()

	if ok {
		handler(&mqttv5.Message{Topic: topic, Payload: payload})
	}
}

func (m *typedMockClient) lastPublished() *mqttv5.Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.published) == 0 {
		return nil
	}

	return m.published[len(m.published)-1]
}

// waitAndRespond polls for a published message and sends a response on the given topic.
func (m *typedMockClient) waitAndRespond(responseTopic string, buildPayload func(clientToken string) []byte) {
	go func() {
		for {
			msg := m.lastPublished()
			if msg != nil {
				var tok tokenResponse
				_ = json.Unmarshal(msg.Payload, &tok)

				data := buildPayload(tok.ClientToken)
				m.simulateResponse(responseTopic, data)

				return
			}

			time.Sleep(time.Millisecond)
		}
	}()
}

// rejectWith builds a rejected error response payload with the given code and message.
func rejectWith(code int, message, clientToken string) []byte {
	data, _ := json.Marshal(ErrorResponse{
		Code:        code,
		Message:     message,
		ClientToken: clientToken,
	})

	return data
}

func TestTypedClient(t *testing.T) {
	t.Run("GetShadow accepted", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		mock.waitAndRespond("$things/dev1/shadow/get/accepted", func(clientToken string) []byte {
			data, _ := json.Marshal(getAcceptedResponse{
				State:       State{Reported: map[string]any{"temp": 22.0}},
				Version:     3,
				Timestamp:   1000,
				ClientToken: clientToken,
			})

			return data
		})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		doc, err := tc.GetShadow(ctx)
		require.NoError(t, err)
		require.NotNil(t, doc)
		assert.Equal(t, 22.0, doc.State.Reported["temp"])
		assert.Equal(t, int64(3), doc.Version)
		assert.Equal(t, int64(1000), doc.Timestamp)
	})

	t.Run("rejected responses", func(t *testing.T) {
		tests := []struct {
			name       string
			rejectCode int
			topic      string
			invoke     func(tc *TypedClient, ctx context.Context) error
		}{
			{
				name:       "GetShadow",
				rejectCode: ErrCodeNotFound,
				topic:      "$things/dev1/shadow/get/rejected",
				invoke: func(tc *TypedClient, ctx context.Context) error {
					_, err := tc.GetShadow(ctx)
					return err
				},
			},
			{
				name:       "UpdateShadow",
				rejectCode: ErrCodeVersionConflict,
				topic:      "$things/dev1/shadow/update/rejected",
				invoke: func(tc *TypedClient, ctx context.Context) error {
					_, err := tc.UpdateShadow(ctx, UpdateState{Desired: map[string]any{"x": 1}})
					return err
				},
			},
			{
				name:       "DeleteShadow",
				rejectCode: ErrCodeNotFound,
				topic:      "$things/dev1/shadow/delete/rejected",
				invoke: func(tc *TypedClient, ctx context.Context) error {
					return tc.DeleteShadow(ctx)
				},
			},
			{
				name:       "ListNamedShadows",
				rejectCode: ErrCodeForbidden,
				topic:      "$things/dev1/shadow/list/rejected",
				invoke: func(tc *TypedClient, ctx context.Context) error {
					_, err := tc.ListNamedShadows(ctx)
					return err
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				mock := newTypedMockClient("dev1")
				tc := NewTypedClient(mock)

				mock.waitAndRespond(tt.topic, func(clientToken string) []byte {
					return rejectWith(tt.rejectCode, "rejected", clientToken)
				})

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				err := tt.invoke(tc, ctx)
				require.Error(t, err)

				var shadowErr *ErrorResponse
				require.ErrorAs(t, err, &shadowErr)
				assert.Equal(t, tt.rejectCode, shadowErr.Code)
			})
		}
	})

	t.Run("UpdateShadow accepted", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		mock.waitAndRespond("$things/dev1/shadow/update/accepted", func(clientToken string) []byte {
			data, _ := json.Marshal(map[string]any{
				"state":       State{Desired: map[string]any{"mode": "auto"}},
				"metadata":    Metadata{},
				"version":     5,
				"timestamp":   2000,
				"clientToken": clientToken,
			})

			return data
		})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		doc, err := tc.UpdateShadow(ctx, UpdateState{
			Desired: map[string]any{"mode": "auto"},
		})
		require.NoError(t, err)
		require.NotNil(t, doc)
		assert.Equal(t, int64(5), doc.Version)
	})

	t.Run("DeleteShadow accepted", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		mock.waitAndRespond("$things/dev1/shadow/delete/accepted", func(clientToken string) []byte {
			data, _ := json.Marshal(deleteAcceptedResponse{
				Version:     1,
				Timestamp:   3000,
				ClientToken: clientToken,
			})

			return data
		})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := tc.DeleteShadow(ctx)
		require.NoError(t, err)
	})

	t.Run("ListNamedShadows accepted", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		mock.waitAndRespond("$things/dev1/shadow/list/accepted", func(clientToken string) []byte {
			data, _ := json.Marshal(listAcceptedResponse{
				Results:     []string{"config", "firmware"},
				Timestamp:   4000,
				ClientToken: clientToken,
			})

			return data
		})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		result, err := tc.ListNamedShadows(ctx)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, []string{"config", "firmware"}, result.Results)
	})

	t.Run("context timeout", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		doc, err := tc.GetShadow(ctx)
		assert.Nil(t, doc)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("context cancel", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		doc, err := tc.GetShadow(ctx)
		assert.Nil(t, doc)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("concurrent requests routed correctly", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		go func() {
			seen := make(map[string]bool)

			for range 100 {
				mock.mu.Lock()
				published := make([]*mqttv5.Message, len(mock.published))
				copy(published, mock.published)
				mock.mu.Unlock()

				for _, msg := range published {
					if seen[string(msg.Payload)] {
						continue
					}
					seen[string(msg.Payload)] = true

					var req getRequest
					if err := json.Unmarshal(msg.Payload, &req); err != nil || req.ClientToken == "" {
						continue
					}

					resp := getAcceptedResponse{
						State:       State{Reported: map[string]any{"token": req.ClientToken}},
						Version:     1,
						Timestamp:   1000,
						ClientToken: req.ClientToken,
					}

					data, _ := json.Marshal(resp)
					mock.simulateResponse("$things/dev1/shadow/get/accepted", data)
				}

				time.Sleep(5 * time.Millisecond)
			}
		}()

		var wg sync.WaitGroup
		results := make([]*Document, 5)
		errs := make([]error, 5)

		for i := range 5 {
			wg.Add(1)

			go func(idx int) {
				defer wg.Done()

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				results[idx], errs[idx] = tc.GetShadow(ctx)
			}(i)
		}

		wg.Wait()

		for i := range 5 {
			require.NoError(t, errs[i], "request %d failed", i)
			require.NotNil(t, results[i], "request %d returned nil", i)
		}
	})

	t.Run("Close cancels pending requests", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		errCh := make(chan error, 1)

		go func() {
			_, err := tc.GetShadow(ctx)
			errCh <- err
		}()

		time.Sleep(50 * time.Millisecond)

		require.NoError(t, tc.Close())

		err := <-errCh
		require.Error(t, err)

		var shadowErr *ErrorResponse
		require.ErrorAs(t, err, &shadowErr)
		assert.Equal(t, ErrCodeInternal, shadowErr.Code)
	})

	t.Run("operations after Close return error", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		require.NoError(t, tc.Close())

		ctx := context.Background()

		_, err := tc.GetShadow(ctx)
		require.ErrorIs(t, err, ErrTypedClientClosed)

		_, err = tc.UpdateShadow(ctx, UpdateState{})
		require.ErrorIs(t, err, ErrTypedClientClosed)

		err = tc.DeleteShadow(ctx)
		require.ErrorIs(t, err, ErrTypedClientClosed)

		_, err = tc.ListNamedShadows(ctx)
		require.ErrorIs(t, err, ErrTypedClientClosed)
	})

	t.Run("auto-subscribe is idempotent", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, _ = tc.GetShadow(ctx)

		ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel2()

		_, _ = tc.GetShadow(ctx2)

		mock.mu.Lock()
		handlerCount := len(mock.handlers)
		mock.mu.Unlock()

		assert.Equal(t, 2, handlerCount)
	})

	t.Run("named shadow via Shadow", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)
		named := tc.Shadow("config")

		mock.waitAndRespond("$things/dev1/shadow/name/config/get/accepted", func(clientToken string) []byte {
			data, _ := json.Marshal(getAcceptedResponse{
				State:       State{Desired: map[string]any{"mode": "auto"}},
				Version:     1,
				Timestamp:   1000,
				ClientToken: clientToken,
			})

			return data
		})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		doc, err := named.GetShadow(ctx)
		require.NoError(t, err)
		require.NotNil(t, doc)
		assert.Equal(t, "auto", doc.State.Desired["mode"])

		msg := mock.lastPublished()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/name/config/get", msg.Topic)
	})

	t.Run("SubscribeEvents delta", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		var gotState map[string]any
		var gotVersion int64
		done := make(chan struct{})

		require.NoError(t, tc.SubscribeEvents(TypedHandlers{
			OnDelta: func(state map[string]any, version int64) {
				gotState = state
				gotVersion = version
				close(done)
			},
		}))

		delta := deltaResponse{
			State:   map[string]any{"temp": 25.0},
			Version: 7,
		}

		data, _ := json.Marshal(delta)
		mock.simulateResponse("$things/dev1/shadow/update/delta", data)

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for delta callback")
		}

		assert.Equal(t, 25.0, gotState["temp"])
		assert.Equal(t, int64(7), gotVersion)
	})

	t.Run("SubscribeEvents documents", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		var gotPrev, gotCurrent DocumentSnapshot
		done := make(chan struct{})

		require.NoError(t, tc.SubscribeEvents(TypedHandlers{
			OnDocuments: func(prev, current DocumentSnapshot) {
				gotPrev = prev
				gotCurrent = current
				close(done)
			},
		}))

		docsMsg := DocumentsMessage{
			Previous: DocumentSnapshot{Version: 1},
			Current: DocumentSnapshot{
				State:   State{Reported: map[string]any{"temp": 20.0}},
				Version: 2,
			},
			Timestamp: 5000,
		}

		data, _ := json.Marshal(docsMsg)
		mock.simulateResponse("$things/dev1/shadow/update/documents", data)

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for documents callback")
		}

		assert.Equal(t, int64(1), gotPrev.Version)
		assert.Equal(t, int64(2), gotCurrent.Version)
		assert.Equal(t, 20.0, gotCurrent.State.Reported["temp"])
	})

	t.Run("SubscribeEvents on closed client", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		require.NoError(t, tc.Close())

		err := tc.SubscribeEvents(TypedHandlers{
			OnDelta: func(_ map[string]any, _ int64) {},
		})
		require.ErrorIs(t, err, ErrTypedClientClosed)
	})

	t.Run("Close is idempotent", func(t *testing.T) {
		mock := newTypedMockClient("dev1")
		tc := NewTypedClient(mock)

		require.NoError(t, tc.Close())
		require.NoError(t, tc.Close())
	})
}
