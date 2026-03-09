package lifecycle

import "time"

// EventType identifies the kind of lifecycle event.
type EventType string

const (
	EventConnected     EventType = "connected"
	EventConnectFailed EventType = "connectFailed"
	EventDisconnected  EventType = "disconnected"
	EventSubscribed    EventType = "subscribed"
	EventUnsubscribed  EventType = "unsubscribed"
)

// ConnectedEvent is published when a client connects to the broker.
type ConnectedEvent struct {
	ClientID              string    `json:"clientId"`
	Timestamp             time.Time `json:"timestamp"`
	EventType             EventType `json:"eventType"`
	Namespace             string    `json:"namespace,omitempty"`
	Username              string    `json:"username,omitempty"`
	RemoteAddr            string    `json:"remoteAddr,omitempty"`
	LocalAddr             string    `json:"localAddr,omitempty"`
	CleanStart            bool      `json:"cleanStart"`
	KeepAlive             uint16    `json:"keepAlive"`
	SessionExpiryInterval uint32    `json:"sessionExpiryInterval"`
}

// ConnectFailedEvent is published when a client connection attempt is rejected.
type ConnectFailedEvent struct {
	ClientID   string    `json:"clientId,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
	EventType  EventType `json:"eventType"`
	Username   string    `json:"username,omitempty"`
	RemoteAddr string    `json:"remoteAddr,omitempty"`
	LocalAddr  string    `json:"localAddr,omitempty"`
	ReasonCode byte      `json:"reasonCode"`
	Reason     string    `json:"reason"`
}

// DisconnectedEvent is published when a client disconnects from the broker.
type DisconnectedEvent struct {
	ClientID        string    `json:"clientId"`
	Timestamp       time.Time `json:"timestamp"`
	EventType       EventType `json:"eventType"`
	Namespace       string    `json:"namespace,omitempty"`
	RemoteAddr      string    `json:"remoteAddr,omitempty"`
	LocalAddr       string    `json:"localAddr,omitempty"`
	CleanDisconnect bool      `json:"cleanDisconnect"`
	ConnectedAt     time.Time `json:"connectedAt"`
	SessionDuration float64   `json:"sessionDuration"`
}

// SubscribedEvent is published when a client subscribes to topics.
type SubscribedEvent struct {
	ClientID      string              `json:"clientId"`
	Timestamp     time.Time           `json:"timestamp"`
	EventType     EventType           `json:"eventType"`
	Namespace     string              `json:"namespace,omitempty"`
	Subscriptions []SubscriptionEntry `json:"subscriptions"`
}

// SubscriptionEntry represents a single topic subscription.
type SubscriptionEntry struct {
	TopicFilter string `json:"topicFilter"`
	QoS         byte   `json:"qos"`
}

// UnsubscribedEvent is published when a client unsubscribes from topics.
type UnsubscribedEvent struct {
	ClientID     string    `json:"clientId"`
	Timestamp    time.Time `json:"timestamp"`
	EventType    EventType `json:"eventType"`
	Namespace    string    `json:"namespace,omitempty"`
	TopicFilters []string  `json:"topicFilters"`
}
