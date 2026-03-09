package shadow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"

	"github.com/vitalvas/mqttv5"
)

// ErrTypedClientClosed is returned when operations are attempted on a closed TypedClient.
var ErrTypedClientClosed = errors.New("shadow: typed client closed")

// response is the internal type for routing parsed responses to pending callers.
type response struct {
	payload []byte
	err     *ErrorResponse
}

// TypedHandlers holds typed callbacks for shadow push notifications.
type TypedHandlers struct {
	OnDelta     func(state map[string]any, version int64)
	OnDocuments func(prev, current DocumentSnapshot)
}

// TypedClient provides typed, request/response-correlated shadow operations.
// It wraps an MQTTClient and manages response subscriptions internally.
type TypedClient struct {
	mqttClient   MQTTClient
	topicBuilder topicBuilderFunc
	shadowName   string

	mu        sync.Mutex
	pending   map[string]chan response
	subGroups map[string]bool
	subTopics []string
	closed    bool
}

// NewTypedClient creates a new typed shadow client for the classic (unnamed) shadow.
func NewTypedClient(client MQTTClient, opts ...ClientOption) *TypedClient {
	c := &Client{
		client:   client,
		clientID: client.ClientID(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return &TypedClient{
		mqttClient:   client,
		topicBuilder: c.topicBuilder,
		pending:      make(map[string]chan response),
		subGroups:    make(map[string]bool),
	}
}

// Shadow returns a new TypedClient scoped to a named shadow.
// The returned client shares the same MQTT connection but operates on the named shadow.
func (tc *TypedClient) Shadow(name string) *TypedClient {
	return &TypedClient{
		mqttClient:   tc.mqttClient,
		topicBuilder: tc.topicBuilder,
		shadowName:   name,
		pending:      make(map[string]chan response),
		subGroups:    make(map[string]bool),
	}
}

// generateToken creates a random 32-character hex string for request correlation.
func generateToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}

// subGroup defines the accepted/rejected suffix pairs for each operation group.
type subGroup struct {
	accepted string
	rejected string
}

var opGroups = map[string]subGroup{
	"get":    {accepted: suffixGetAccepted, rejected: suffixGetRejected},
	"update": {accepted: suffixUpdateAccepted, rejected: suffixUpdateRejected},
	"delete": {accepted: suffixDeleteAccepted, rejected: suffixDeleteRejected},
	"list":   {accepted: suffixListAccepted, rejected: suffixListRejected},
}

// ensureSubscribed lazily subscribes to accepted/rejected topics for the given operation group.
func (tc *TypedClient) ensureSubscribed(group string) error {
	if tc.subGroups[group] {
		return nil
	}

	sg, ok := opGroups[group]
	if !ok {
		return errors.New("shadow: unknown operation group: " + group)
	}

	acceptedTopic := tc.topicBuilder(tc.shadowName, sg.accepted)
	rejectedTopic := tc.topicBuilder(tc.shadowName, sg.rejected)

	if err := tc.mqttClient.Subscribe(acceptedTopic, 1, tc.makeHandler(false)); err != nil {
		return err
	}

	if err := tc.mqttClient.Subscribe(rejectedTopic, 1, tc.makeHandler(true)); err != nil {
		return err
	}

	tc.subTopics = append(tc.subTopics, acceptedTopic, rejectedTopic)
	tc.subGroups[group] = true

	return nil
}

// tokenResponse is used internally to extract clientToken from any response JSON.
type tokenResponse struct {
	ClientToken string `json:"clientToken"`
}

// makeHandler creates a message handler that routes responses to pending callers.
func (tc *TypedClient) makeHandler(isRejected bool) mqttv5.MessageHandler {
	return func(msg *mqttv5.Message) {
		var tok tokenResponse
		if err := json.Unmarshal(msg.Payload, &tok); err != nil || tok.ClientToken == "" {
			return
		}

		var resp response
		if isRejected {
			var errResp ErrorResponse
			if err := json.Unmarshal(msg.Payload, &errResp); err != nil {
				return
			}

			resp.err = &errResp
		} else {
			resp.payload = msg.Payload
		}

		tc.routeResponse(tok.ClientToken, resp)
	}
}

// routeResponse delivers a response to the pending caller identified by clientToken.
func (tc *TypedClient) routeResponse(clientToken string, resp response) {
	tc.mu.Lock()
	ch, ok := tc.pending[clientToken]
	if ok {
		delete(tc.pending, clientToken)
	}
	tc.mu.Unlock()

	if ok {
		ch <- resp
	}
}

// doRequest performs the core request-response loop:
// 1. Generate a unique clientToken
// 2. Ensure the operation group is subscribed
// 3. Register a pending response channel
// 4. Execute the publish function
// 5. Wait for response or context cancellation
func (tc *TypedClient) doRequest(ctx context.Context, group string, publishFn func(clientToken string) error) ([]byte, error) {
	tc.mu.Lock()
	if tc.closed {
		tc.mu.Unlock()
		return nil, ErrTypedClientClosed
	}

	if err := tc.ensureSubscribed(group); err != nil {
		tc.mu.Unlock()
		return nil, err
	}

	token, err := generateToken()
	if err != nil {
		tc.mu.Unlock()
		return nil, err
	}

	ch := make(chan response, 1)
	tc.pending[token] = ch
	tc.mu.Unlock()

	if err := publishFn(token); err != nil {
		tc.mu.Lock()
		delete(tc.pending, token)
		tc.mu.Unlock()

		return nil, err
	}

	select {
	case resp := <-ch:
		if resp.err != nil {
			return nil, resp.err
		}

		return resp.payload, nil
	case <-ctx.Done():
		tc.mu.Lock()
		delete(tc.pending, token)
		tc.mu.Unlock()

		return nil, ctx.Err()
	}
}

// GetShadow requests the current shadow document and blocks until a response or context cancellation.
func (tc *TypedClient) GetShadow(ctx context.Context) (*Document, error) {
	data, err := tc.doRequest(ctx, "get", func(clientToken string) error {
		payload, _ := json.Marshal(getRequest{ClientToken: clientToken})

		return tc.mqttClient.Publish(&mqttv5.Message{
			Topic:   tc.topicBuilder(tc.shadowName, suffixGet),
			Payload: payload,
		})
	})
	if err != nil {
		return nil, err
	}

	var resp getAcceptedResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}

	return &Document{
		State:     resp.State,
		Metadata:  resp.Metadata,
		Version:   resp.Version,
		Timestamp: resp.Timestamp,
	}, nil
}

// UpdateShadow publishes desired and/or reported state and blocks until a response or context cancellation.
func (tc *TypedClient) UpdateShadow(ctx context.Context, state UpdateState) (*Document, error) {
	data, err := tc.doRequest(ctx, "update", func(clientToken string) error {
		payload, marshalErr := json.Marshal(UpdateRequest{
			State:       state,
			ClientToken: clientToken,
		})
		if marshalErr != nil {
			return marshalErr
		}

		return tc.mqttClient.Publish(&mqttv5.Message{
			Topic:   tc.topicBuilder(tc.shadowName, suffixUpdate),
			Payload: payload,
		})
	})
	if err != nil {
		return nil, err
	}

	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	return &doc, nil
}

// DeleteShadow removes the shadow and blocks until a response or context cancellation.
func (tc *TypedClient) DeleteShadow(ctx context.Context) error {
	_, err := tc.doRequest(ctx, "delete", func(clientToken string) error {
		payload, _ := json.Marshal(deleteRequest{ClientToken: clientToken})

		return tc.mqttClient.Publish(&mqttv5.Message{
			Topic:   tc.topicBuilder(tc.shadowName, suffixDelete),
			Payload: payload,
		})
	})

	return err
}

// ListNamedShadows lists all named shadows and blocks until a response or context cancellation.
func (tc *TypedClient) ListNamedShadows(ctx context.Context) (*ListResult, error) {
	data, err := tc.doRequest(ctx, "list", func(clientToken string) error {
		payload, _ := json.Marshal(listRequest{ClientToken: clientToken})

		return tc.mqttClient.Publish(&mqttv5.Message{
			Topic:   tc.topicBuilder(tc.shadowName, suffixList),
			Payload: payload,
		})
	})
	if err != nil {
		return nil, err
	}

	var resp listAcceptedResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}

	return &ListResult{
		Results:   resp.Results,
		NextToken: resp.NextToken,
	}, nil
}

// SubscribeEvents subscribes to delta and/or documents push notifications with typed callbacks.
func (tc *TypedClient) SubscribeEvents(handlers TypedHandlers) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.closed {
		return ErrTypedClientClosed
	}

	if handlers.OnDelta != nil {
		topic := tc.topicBuilder(tc.shadowName, suffixUpdateDelta)

		if err := tc.mqttClient.Subscribe(topic, 1, func(msg *mqttv5.Message) {
			var dr deltaResponse
			if err := json.Unmarshal(msg.Payload, &dr); err != nil {
				return
			}

			handlers.OnDelta(dr.State, dr.Version)
		}); err != nil {
			return err
		}

		tc.subTopics = append(tc.subTopics, topic)
	}

	if handlers.OnDocuments != nil {
		topic := tc.topicBuilder(tc.shadowName, suffixUpdateDocuments)

		if err := tc.mqttClient.Subscribe(topic, 1, func(msg *mqttv5.Message) {
			var dm DocumentsMessage
			if err := json.Unmarshal(msg.Payload, &dm); err != nil {
				return
			}

			handlers.OnDocuments(dm.Previous, dm.Current)
		}); err != nil {
			return err
		}

		tc.subTopics = append(tc.subTopics, topic)
	}

	return nil
}

// Close unsubscribes from all managed topics and cancels all pending requests.
func (tc *TypedClient) Close() error {
	tc.mu.Lock()
	if tc.closed {
		tc.mu.Unlock()
		return nil
	}

	tc.closed = true

	pending := tc.pending
	tc.pending = make(map[string]chan response)

	topics := tc.subTopics
	tc.subTopics = nil
	tc.mu.Unlock()

	for _, ch := range pending {
		ch <- response{err: &ErrorResponse{
			Code:    ErrCodeInternal,
			Message: "typed client closed",
		}}
	}

	if len(topics) > 0 {
		return tc.mqttClient.Unsubscribe(topics...)
	}

	return nil
}
