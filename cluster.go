package mqttv5

import (
	"context"
	"net"
	"time"
)

// ClusterNodeID represents a unique identifier for a cluster node.
type ClusterNodeID string

// ClusterNodeState represents the state of a cluster node.
type ClusterNodeState int

const (
	ClusterNodeStateUnknown ClusterNodeState = 0
	ClusterNodeStateJoining ClusterNodeState = 1
	ClusterNodeStateActive  ClusterNodeState = 2
	ClusterNodeStateLeaving ClusterNodeState = 3
	ClusterNodeStateDead    ClusterNodeState = 4
)

// ClusterNode represents a node in an MQTT broker cluster.
type ClusterNode interface {
	// ID returns the unique identifier for this node.
	ID() ClusterNodeID

	// Address returns the network address of this node.
	Address() net.Addr

	// State returns the current state of this node.
	State() ClusterNodeState

	// Metadata returns node metadata (version, capabilities, etc.).
	Metadata() map[string]string

	// LastSeen returns the last time this node was seen.
	LastSeen() time.Time
}

// ClusterTransport defines the transport interface for cluster communication.
type ClusterTransport interface {
	// Start starts the transport.
	Start(ctx context.Context) error

	// Stop stops the transport.
	Stop() error

	// LocalNode returns the local cluster node.
	LocalNode() ClusterNode

	// Nodes returns all known nodes in the cluster.
	Nodes() []ClusterNode

	// Send sends a message to a specific node.
	Send(ctx context.Context, nodeID ClusterNodeID, msg ClusterMessage) error

	// Broadcast sends a message to all nodes in the cluster.
	Broadcast(ctx context.Context, msg ClusterMessage) error

	// SetMessageHandler sets the handler for incoming cluster messages.
	SetMessageHandler(handler ClusterMessageHandler)
}

// ClusterMessageType represents the type of cluster message.
type ClusterMessageType int

const (
	ClusterMessageTypeSubscriptionSync   ClusterMessageType = 0
	ClusterMessageTypeSubscriptionRemove ClusterMessageType = 1
	ClusterMessageTypeRetainedSync       ClusterMessageType = 2
	ClusterMessageTypeRetainedRemove     ClusterMessageType = 3
	ClusterMessageTypeSessionMigrate     ClusterMessageType = 4
	ClusterMessageTypeSessionSync        ClusterMessageType = 5
	ClusterMessageTypePublishForward     ClusterMessageType = 6
)

// ClusterMessage represents a message exchanged between cluster nodes.
type ClusterMessage interface {
	// Type returns the message type.
	Type() ClusterMessageType

	// SourceNode returns the source node ID.
	SourceNode() ClusterNodeID

	// Timestamp returns when the message was created.
	Timestamp() time.Time

	// Payload returns the message payload.
	Payload() []byte
}

// ClusterMessageHandler handles incoming cluster messages.
type ClusterMessageHandler func(ctx context.Context, msg ClusterMessage) error

// SubscriptionSyncMessage contains subscription information for cluster sync.
type SubscriptionSyncMessage struct {
	ClientID      string
	Subscriptions []Subscription
}

// SubscriptionRemoveMessage contains subscription removal information.
type SubscriptionRemoveMessage struct {
	ClientID     string
	TopicFilters []string
}

// RetainedSyncMessage contains retained message for cluster sync.
type RetainedSyncMessage struct {
	Message *RetainedMessage
}

// RetainedRemoveMessage contains retained message removal information.
type RetainedRemoveMessage struct {
	Topic string
}

// SessionMigrateMessage contains session migration data.
type SessionMigrateMessage struct {
	ClientID      string
	Session       SessionData
	Subscriptions []Subscription
}

// SessionData represents serializable session state for migration.
type SessionData struct {
	ClientID        string
	ExpiryInterval  uint32
	Subscriptions   []Subscription
	PendingMessages []PendingMessageData
}

// PendingMessageData represents a pending message for session migration.
type PendingMessageData struct {
	PacketID uint16
	Message  *Message
	QoS      byte
	State    int // QoS flow state
}

// PublishForwardMessage contains a publish message to forward to other nodes.
type PublishForwardMessage struct {
	Message    *Message
	SourceNode ClusterNodeID
}

// SubscriptionSync defines the interface for subscription synchronization.
type SubscriptionSync interface {
	// SyncSubscription synchronizes a subscription across the cluster.
	SyncSubscription(clientID string, sub Subscription) error

	// RemoveSubscription removes a subscription across the cluster.
	RemoveSubscription(clientID string, filter string) error

	// GetRemoteSubscribers returns remote subscribers for a topic.
	GetRemoteSubscribers(topic string) []ClusterNodeID
}

// RetainedSync defines the interface for retained message synchronization.
type RetainedSync interface {
	// SyncRetained synchronizes a retained message across the cluster.
	SyncRetained(msg *RetainedMessage) error

	// RemoveRetained removes a retained message across the cluster.
	RemoveRetained(topic string) error
}

// SessionMigration defines the interface for session migration.
type SessionMigration interface {
	// ExportSession exports a session for migration.
	ExportSession(clientID string) (*SessionData, error)

	// ImportSession imports a migrated session.
	ImportSession(data *SessionData) error

	// RequestMigration requests session migration from another node.
	RequestMigration(ctx context.Context, nodeID ClusterNodeID, clientID string) error
}

// Cluster combines all clustering capabilities.
type Cluster interface {
	ClusterTransport
	SubscriptionSync
	RetainedSync
	SessionMigration

	// Join joins an existing cluster.
	Join(ctx context.Context, seeds []string) error

	// Leave leaves the cluster gracefully.
	Leave(ctx context.Context) error

	// IsLeader returns true if this node is the cluster leader.
	IsLeader() bool

	// Leader returns the current leader node ID.
	Leader() ClusterNodeID
}
