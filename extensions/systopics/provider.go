package systopics

import (
	"time"

	"github.com/vitalvas/mqttv5"
)

// Broker defines the interface for reading broker statistics and publishing messages.
// The *mqttv5.Server satisfies this interface directly.
type Broker interface {
	// ClientCount returns the number of currently connected clients.
	// When namespaces are provided, counts only clients in those namespaces.
	ClientCount(namespaces ...string) int

	// Namespaces returns the list of active namespaces.
	Namespaces() []string

	// Connections returns the current number of active connections.
	Connections() int64

	// ConnectionsTotal returns the total number of connections since start.
	ConnectionsTotal() int64

	// MaxConnections returns the peak concurrent connection count.
	MaxConnections() int64

	// Subscriptions returns the current number of active subscriptions.
	Subscriptions() int64

	// RetainedMessages returns the current number of retained messages.
	RetainedMessages() int64

	// TotalBytesReceived returns total bytes received.
	TotalBytesReceived() int64

	// TotalBytesSent returns total bytes sent.
	TotalBytesSent() int64

	// TotalMessagesReceived returns total messages received for a QoS level.
	TotalMessagesReceived(qos byte) int64

	// TotalMessagesSent returns total messages sent for a QoS level.
	TotalMessagesSent(qos byte) int64

	// PacketsReceived returns the packet count for a type.
	PacketsReceived(packetType mqttv5.PacketType) int64

	// PacketsSent returns the packet count for a type.
	PacketsSent(packetType mqttv5.PacketType) int64

	// TopicCount returns the number of tracked topics.
	TopicCount() int64

	// StartedAt returns the time when the server was created.
	StartedAt() time.Time

	// Publish sends a message to subscribers.
	Publish(msg *mqttv5.Message) error
}
