package lifecycle

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/vitalvas/mqttv5"
)

// sanitizeTopicSegment percent-encodes MQTT-unsafe characters in a string
// so it can be safely embedded as a single topic segment. Consumers can
// reverse the encoding with url.PathUnescape.
func sanitizeTopicSegment(s string) string {
	// url.PathEscape encodes / and # but not +, which is an MQTT wildcard.
	return strings.ReplaceAll(url.PathEscape(s), "+", "%2B")
}

const (
	topicPresenceConnected     = "$events/presence/connected/%s"
	topicPresenceConnectFailed = "$events/presence/connect_failed/%s"
	topicPresenceDisconnected  = "$events/presence/disconnected/%s"
	topicSubsSubscribed        = "$events/subscriptions/subscribed/%s"
	topicSubsUnsubscribed      = "$events/subscriptions/unsubscribed/%s"
)

// Publisher is a broker interface required for publishing lifecycle events.
type Publisher interface {
	Publish(msg *mqttv5.Message) error
}

// Handler publishes client lifecycle events to MQTT topics.
type Handler struct {
	publisher          Publisher
	onPublishError     func(topic string, err error)
	namespace          string
	useClientNamespace bool
}

// resolveNamespace returns the namespace to publish the event in.
// Priority: WithNamespace > WithClientNamespace > DefaultNamespace.
func (h *Handler) resolveNamespace(clientNamespace string) string {
	if h.namespace != "" {
		return h.namespace
	}

	if h.useClientNamespace && clientNamespace != "" {
		return clientNamespace
	}

	return mqttv5.DefaultNamespace
}

// New creates a new lifecycle event handler.
func New(publisher Publisher, opts ...Option) *Handler {
	h := &Handler{
		publisher: publisher,
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

// OnConnect handles a client connection event.
// Use with mqttv5.OnConnect(handler.OnConnect).
func (h *Handler) OnConnect(client *mqttv5.ServerClient) {
	event := ConnectedEvent{
		ClientID:              client.ClientID(),
		Timestamp:             time.Now(),
		EventType:             EventConnected,
		Namespace:             client.Namespace(),
		Username:              client.Username(),
		RemoteAddr:            client.Conn().RemoteAddr().String(),
		LocalAddr:             client.Conn().LocalAddr().String(),
		CleanStart:            client.CleanStart(),
		KeepAlive:             client.KeepAlive(),
		SessionExpiryInterval: client.SessionExpiryInterval(),
	}

	topic := fmt.Sprintf(topicPresenceConnected, sanitizeTopicSegment(client.ClientID()))
	h.publish(topic, h.resolveNamespace(client.Namespace()), event)
}

// OnConnectFailed handles a failed connection attempt.
// Use with mqttv5.OnConnectFailed(handler.OnConnectFailed).
func (h *Handler) OnConnectFailed(ctx *mqttv5.ConnectFailedContext) {
	clientID := ctx.ClientID
	if clientID == "" {
		clientID = "unknown"
	}
	clientID = sanitizeTopicSegment(clientID)

	var remoteAddr, localAddr string
	if ctx.RemoteAddr != nil {
		remoteAddr = ctx.RemoteAddr.String()
	}
	if ctx.LocalAddr != nil {
		localAddr = ctx.LocalAddr.String()
	}

	event := ConnectFailedEvent{
		ClientID:   ctx.ClientID,
		Timestamp:  time.Now(),
		EventType:  EventConnectFailed,
		Username:   ctx.Username,
		RemoteAddr: remoteAddr,
		LocalAddr:  localAddr,
		ReasonCode: byte(ctx.ReasonCode),
		Reason:     ctx.ReasonCode.String(),
	}

	topic := fmt.Sprintf(topicPresenceConnectFailed, clientID)
	h.publish(topic, mqttv5.DefaultNamespace, event)
}

// OnDisconnect handles a client disconnection event.
// Use with mqttv5.OnDisconnect(handler.OnDisconnect).
func (h *Handler) OnDisconnect(client *mqttv5.ServerClient) {
	connectedAt := client.ConnectedAt()

	event := DisconnectedEvent{
		ClientID:        client.ClientID(),
		Timestamp:       time.Now(),
		EventType:       EventDisconnected,
		Namespace:       client.Namespace(),
		RemoteAddr:      client.Conn().RemoteAddr().String(),
		LocalAddr:       client.Conn().LocalAddr().String(),
		CleanDisconnect: client.IsCleanDisconnect(),
		ConnectedAt:     connectedAt,
		SessionDuration: time.Since(connectedAt).Seconds(),
	}

	topic := fmt.Sprintf(topicPresenceDisconnected, sanitizeTopicSegment(client.ClientID()))
	h.publish(topic, h.resolveNamespace(client.Namespace()), event)
}

// OnSubscribe handles a client subscription event.
// Use with mqttv5.OnSubscribe(handler.OnSubscribe).
func (h *Handler) OnSubscribe(client *mqttv5.ServerClient, subs []mqttv5.Subscription) {
	entries := make([]SubscriptionEntry, len(subs))
	for i, sub := range subs {
		entries[i] = SubscriptionEntry{
			TopicFilter: sub.TopicFilter,
			QoS:         sub.QoS,
		}
	}

	event := SubscribedEvent{
		ClientID:      client.ClientID(),
		Timestamp:     time.Now(),
		EventType:     EventSubscribed,
		Namespace:     client.Namespace(),
		Subscriptions: entries,
	}

	topic := fmt.Sprintf(topicSubsSubscribed, sanitizeTopicSegment(client.ClientID()))
	h.publish(topic, h.resolveNamespace(client.Namespace()), event)
}

// OnUnsubscribe handles a client unsubscription event.
// Use with mqttv5.OnUnsubscribe(handler.OnUnsubscribe).
func (h *Handler) OnUnsubscribe(client *mqttv5.ServerClient, topics []string) {
	event := UnsubscribedEvent{
		ClientID:     client.ClientID(),
		Timestamp:    time.Now(),
		EventType:    EventUnsubscribed,
		Namespace:    client.Namespace(),
		TopicFilters: topics,
	}

	topic := fmt.Sprintf(topicSubsUnsubscribed, sanitizeTopicSegment(client.ClientID()))
	h.publish(topic, h.resolveNamespace(client.Namespace()), event)
}

func (h *Handler) publish(topic, namespace string, event any) {
	payload, err := json.Marshal(event)
	if err != nil {
		h.reportError(topic, err)
		return
	}

	if err := h.publisher.Publish(&mqttv5.Message{
		Topic:     topic,
		Payload:   payload,
		Namespace: namespace,
	}); err != nil {
		h.reportError(topic, err)
	}
}

func (h *Handler) reportError(topic string, err error) {
	if h.onPublishError != nil {
		h.onPublishError(topic, err)
	}
}
