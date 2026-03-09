package shadow

import (
	"encoding/json"

	"github.com/vitalvas/mqttv5"
)

// MQTTClient defines the interface required for shadow client operations.
type MQTTClient interface {
	ClientID() string
	Subscribe(filter string, qos byte, handler mqttv5.MessageHandler) error
	Unsubscribe(filters ...string) error
	Publish(msg *mqttv5.Message) error
}

// Handlers holds handlers for shadow notification topics.
type Handlers struct {
	// Update response handlers.
	Accepted  mqttv5.MessageHandler
	Rejected  mqttv5.MessageHandler
	Delta     mqttv5.MessageHandler
	Documents mqttv5.MessageHandler

	// Get response handlers.
	GetAccepted mqttv5.MessageHandler
	GetRejected mqttv5.MessageHandler

	// Delete response handlers.
	DeleteAccepted mqttv5.MessageHandler
	DeleteRejected mqttv5.MessageHandler

	// List response handlers.
	ListAccepted mqttv5.MessageHandler
	ListRejected mqttv5.MessageHandler
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithClientID sets the client ID for the shadow client.
func WithClientID(name string) ClientOption {
	return func(c *Client) {
		c.clientID = name
	}
}

// Client provides convenience methods for devices to interact with shadows.
// A single Client instance can operate on both classic and named shadows
// via the Shadow method.
type Client struct {
	client   MQTTClient
	clientID string
}

// NewClient creates a new shadow client.
// Default client ID is client.ClientID().
// Use the returned Client for the classic (unnamed) shadow, and call
// Shadow(name) to get a handle for named shadows.
func NewClient(client MQTTClient, opts ...ClientOption) *Client {
	c := &Client{
		client:   client,
		clientID: client.ClientID(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Shadow returns a handle scoped to a named shadow.
// The handle shares the same MQTT connection and client ID.
func (c *Client) Shadow(name string) *NamedClient {
	return &NamedClient{
		client:       c.client,
		shadowName:   name,
		topicBuilder: c.topicBuilder,
	}
}

// Get requests the current classic shadow document.
func (c *Client) Get() error {
	return get(c.client, c.topicBuilder, "", "")
}

// GetWithToken requests the current classic shadow document with a clientToken.
func (c *Client) GetWithToken(clientToken string) error {
	return get(c.client, c.topicBuilder, "", clientToken)
}

// ListShadows lists all named shadows for this client.
func (c *Client) ListShadows() error {
	return listShadows(c.client, c.topicBuilder, "")
}

// ListShadowsWithToken lists all named shadows for this client with a clientToken.
func (c *Client) ListShadowsWithToken(clientToken string) error {
	return listShadows(c.client, c.topicBuilder, clientToken)
}

// Update publishes desired and/or reported state to the classic shadow.
func (c *Client) Update(state UpdateState) error {
	return update(c.client, c.topicBuilder, "", state)
}

// Delete removes the classic shadow.
func (c *Client) Delete() error {
	return del(c.client, c.topicBuilder, "", "")
}

// DeleteWithToken removes the classic shadow with a clientToken.
func (c *Client) DeleteWithToken(clientToken string) error {
	return del(c.client, c.topicBuilder, "", clientToken)
}

// SubscribeDelta subscribes to delta notifications for the classic shadow.
func (c *Client) SubscribeDelta(handler mqttv5.MessageHandler) error {
	return subscribeDelta(c.client, c.topicBuilder, "", handler)
}

// SubscribeDocuments subscribes to full document notifications for the classic shadow.
func (c *Client) SubscribeDocuments(handler mqttv5.MessageHandler) error {
	return subscribeDocuments(c.client, c.topicBuilder, "", handler)
}

// SubscribeAll subscribes to all notification topics for the classic shadow.
func (c *Client) SubscribeAll(handlers Handlers) error {
	return subscribeAll(c.client, c.topicBuilder, "", handlers)
}

// Unsubscribe unsubscribes from all classic shadow notification topics.
func (c *Client) Unsubscribe() error {
	return unsubscribe(c.client, c.topicBuilder, "")
}

func (c *Client) topicBuilder(shadowName, suffix string) string {
	return buildTopic(c.clientID, shadowName, suffix)
}

// NamedClient is a handle scoped to a specific named shadow.
// It shares the MQTT connection and client ID with the parent Client.
type NamedClient struct {
	client       MQTTClient
	shadowName   string
	topicBuilder func(shadowName, suffix string) string
}

// Get requests the current named shadow document.
func (nc *NamedClient) Get() error {
	return get(nc.client, nc.topicBuilder, nc.shadowName, "")
}

// GetWithToken requests the current named shadow document with a clientToken.
func (nc *NamedClient) GetWithToken(clientToken string) error {
	return get(nc.client, nc.topicBuilder, nc.shadowName, clientToken)
}

// Update publishes desired and/or reported state to the named shadow.
func (nc *NamedClient) Update(state UpdateState) error {
	return update(nc.client, nc.topicBuilder, nc.shadowName, state)
}

// Delete removes the named shadow.
func (nc *NamedClient) Delete() error {
	return del(nc.client, nc.topicBuilder, nc.shadowName, "")
}

// DeleteWithToken removes the named shadow with a clientToken.
func (nc *NamedClient) DeleteWithToken(clientToken string) error {
	return del(nc.client, nc.topicBuilder, nc.shadowName, clientToken)
}

// SubscribeDelta subscribes to delta notifications for the named shadow.
func (nc *NamedClient) SubscribeDelta(handler mqttv5.MessageHandler) error {
	return subscribeDelta(nc.client, nc.topicBuilder, nc.shadowName, handler)
}

// SubscribeDocuments subscribes to full document notifications for the named shadow.
func (nc *NamedClient) SubscribeDocuments(handler mqttv5.MessageHandler) error {
	return subscribeDocuments(nc.client, nc.topicBuilder, nc.shadowName, handler)
}

// SubscribeAll subscribes to all notification topics for the named shadow.
func (nc *NamedClient) SubscribeAll(handlers Handlers) error {
	return subscribeAll(nc.client, nc.topicBuilder, nc.shadowName, handlers)
}

// Unsubscribe unsubscribes from all named shadow notification topics.
func (nc *NamedClient) Unsubscribe() error {
	return unsubscribe(nc.client, nc.topicBuilder, nc.shadowName)
}

// topicBuilderFunc builds a topic from shadow name and suffix.
type topicBuilderFunc func(shadowName, suffix string) string

// Shared implementations

func get(client MQTTClient, tb topicBuilderFunc, shadowName string, clientToken string) error {
	topic := tb(shadowName, suffixGet)

	var payload []byte
	if clientToken != "" {
		payload, _ = json.Marshal(getRequest{ClientToken: clientToken})
	}

	return client.Publish(&mqttv5.Message{Topic: topic, Payload: payload})
}

func listShadows(client MQTTClient, tb topicBuilderFunc, clientToken string) error {
	topic := tb("", suffixList)

	var payload []byte
	if clientToken != "" {
		payload, _ = json.Marshal(listRequest{ClientToken: clientToken})
	}

	return client.Publish(&mqttv5.Message{Topic: topic, Payload: payload})
}

func update(client MQTTClient, tb topicBuilderFunc, shadowName string, state UpdateState) error {
	topic := tb(shadowName, suffixUpdate)

	payload, err := json.Marshal(UpdateRequest{State: state})
	if err != nil {
		return err
	}

	return client.Publish(&mqttv5.Message{
		Topic:   topic,
		Payload: payload,
	})
}

func del(client MQTTClient, tb topicBuilderFunc, shadowName string, clientToken string) error {
	topic := tb(shadowName, suffixDelete)

	var payload []byte
	if clientToken != "" {
		payload, _ = json.Marshal(deleteRequest{ClientToken: clientToken})
	}

	return client.Publish(&mqttv5.Message{Topic: topic, Payload: payload})
}

func subscribeDelta(client MQTTClient, tb topicBuilderFunc, shadowName string, handler mqttv5.MessageHandler) error {
	topic := tb(shadowName, suffixUpdateDelta)
	return client.Subscribe(topic, 1, handler)
}

func subscribeDocuments(client MQTTClient, tb topicBuilderFunc, shadowName string, handler mqttv5.MessageHandler) error {
	topic := tb(shadowName, suffixUpdateDocuments)
	return client.Subscribe(topic, 1, handler)
}

func subscribeAll(client MQTTClient, tb topicBuilderFunc, shadowName string, handlers Handlers) error {
	subs := []struct {
		handler mqttv5.MessageHandler
		suffix  string
	}{
		{handlers.Accepted, suffixUpdateAccepted},
		{handlers.Rejected, suffixUpdateRejected},
		{handlers.Delta, suffixUpdateDelta},
		{handlers.Documents, suffixUpdateDocuments},
		{handlers.GetAccepted, suffixGetAccepted},
		{handlers.GetRejected, suffixGetRejected},
		{handlers.DeleteAccepted, suffixDeleteAccepted},
		{handlers.DeleteRejected, suffixDeleteRejected},
		{handlers.ListAccepted, suffixListAccepted},
		{handlers.ListRejected, suffixListRejected},
	}

	for _, sub := range subs {
		if sub.handler != nil {
			topic := tb(shadowName, sub.suffix)
			if err := client.Subscribe(topic, 1, sub.handler); err != nil {
				return err
			}
		}
	}

	return nil
}

func unsubscribe(client MQTTClient, tb topicBuilderFunc, shadowName string) error {
	return client.Unsubscribe(
		tb(shadowName, suffixUpdateAccepted),
		tb(shadowName, suffixUpdateRejected),
		tb(shadowName, suffixUpdateDelta),
		tb(shadowName, suffixUpdateDocuments),
		tb(shadowName, suffixGetAccepted),
		tb(shadowName, suffixGetRejected),
		tb(shadowName, suffixDeleteAccepted),
		tb(shadowName, suffixDeleteRejected),
		tb(shadowName, suffixListAccepted),
		tb(shadowName, suffixListRejected),
	)
}

// GroupClient provides convenience methods for interacting with shared shadows.
type GroupClient struct {
	client    MQTTClient
	groupName string
}

// NewGroupClient creates a new shared shadow client for the given group.
func NewGroupClient(client MQTTClient, groupName string) *GroupClient {
	return &GroupClient{
		client:    client,
		groupName: groupName,
	}
}

// Shadow returns a handle scoped to a named shared shadow.
func (gc *GroupClient) Shadow(name string) *NamedClient {
	return &NamedClient{
		client:       gc.client,
		shadowName:   name,
		topicBuilder: gc.topicBuilder,
	}
}

// Get requests the current shared classic shadow document.
func (gc *GroupClient) Get() error {
	return get(gc.client, gc.topicBuilder, "", "")
}

// GetWithToken requests the current shared classic shadow document with a clientToken.
func (gc *GroupClient) GetWithToken(clientToken string) error {
	return get(gc.client, gc.topicBuilder, "", clientToken)
}

// Update publishes desired and/or reported state to the shared classic shadow.
func (gc *GroupClient) Update(state UpdateState) error {
	return update(gc.client, gc.topicBuilder, "", state)
}

// Delete removes the shared classic shadow.
func (gc *GroupClient) Delete() error {
	return del(gc.client, gc.topicBuilder, "", "")
}

// DeleteWithToken removes the shared classic shadow with a clientToken.
func (gc *GroupClient) DeleteWithToken(clientToken string) error {
	return del(gc.client, gc.topicBuilder, "", clientToken)
}

// SubscribeDelta subscribes to delta notifications for the shared classic shadow.
func (gc *GroupClient) SubscribeDelta(handler mqttv5.MessageHandler) error {
	return subscribeDelta(gc.client, gc.topicBuilder, "", handler)
}

// SubscribeDocuments subscribes to full document notifications for the shared classic shadow.
func (gc *GroupClient) SubscribeDocuments(handler mqttv5.MessageHandler) error {
	return subscribeDocuments(gc.client, gc.topicBuilder, "", handler)
}

// SubscribeAll subscribes to all notification topics for the shared classic shadow.
func (gc *GroupClient) SubscribeAll(handlers Handlers) error {
	return subscribeAll(gc.client, gc.topicBuilder, "", handlers)
}

// Unsubscribe unsubscribes from all shared classic shadow notification topics.
func (gc *GroupClient) Unsubscribe() error {
	return unsubscribe(gc.client, gc.topicBuilder, "")
}

func (gc *GroupClient) topicBuilder(shadowName, suffix string) string {
	return buildSharedTopic(gc.groupName, shadowName, suffix)
}
