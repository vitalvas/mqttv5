package mqttv5

import (
	"errors"
	"time"
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrSessionExists   = errors.New("session already exists")
)

// Session represents an MQTT session state.
type Session interface {
	// ClientID returns the client identifier.
	ClientID() string

	// Subscriptions returns a copy of all subscriptions.
	Subscriptions() []Subscription

	// AddSubscription adds or updates a subscription.
	AddSubscription(sub Subscription)

	// RemoveSubscription removes a subscription by filter.
	RemoveSubscription(filter string) bool

	// HasSubscription checks if a subscription exists.
	HasSubscription(filter string) bool

	// GetSubscription returns a subscription by filter.
	GetSubscription(filter string) (Subscription, bool)

	// NextPacketID returns the next available packet ID.
	NextPacketID() uint16

	// AddPendingMessage adds a message awaiting acknowledgment.
	AddPendingMessage(packetID uint16, msg *Message)

	// GetPendingMessage retrieves a pending message by packet ID.
	GetPendingMessage(packetID uint16) (*Message, bool)

	// RemovePendingMessage removes a pending message.
	RemovePendingMessage(packetID uint16) bool

	// PendingMessages returns all pending messages.
	PendingMessages() map[uint16]*Message

	// ExpiryTime returns the session expiry time.
	ExpiryTime() time.Time

	// SetExpiryTime sets the session expiry time.
	SetExpiryTime(t time.Time)

	// IsExpired returns true if the session has expired.
	IsExpired() bool

	// CreatedAt returns when the session was created.
	CreatedAt() time.Time

	// LastActivity returns the last activity time.
	LastActivity() time.Time

	// UpdateLastActivity updates the last activity time.
	UpdateLastActivity()

	// AddInflightQoS1 adds a QoS 1 message awaiting PUBACK.
	AddInflightQoS1(packetID uint16, msg *QoS1Message)

	// GetInflightQoS1 retrieves a QoS 1 inflight message.
	GetInflightQoS1(packetID uint16) (*QoS1Message, bool)

	// RemoveInflightQoS1 removes a QoS 1 inflight message.
	RemoveInflightQoS1(packetID uint16) bool

	// InflightQoS1 returns all QoS 1 inflight messages.
	InflightQoS1() map[uint16]*QoS1Message

	// AddInflightQoS2 adds a QoS 2 message in the publish flow.
	AddInflightQoS2(packetID uint16, msg *QoS2Message)

	// GetInflightQoS2 retrieves a QoS 2 inflight message.
	GetInflightQoS2(packetID uint16) (*QoS2Message, bool)

	// RemoveInflightQoS2 removes a QoS 2 inflight message.
	RemoveInflightQoS2(packetID uint16) bool

	// InflightQoS2 returns all QoS 2 inflight messages.
	InflightQoS2() map[uint16]*QoS2Message
}

// SessionStore defines the interface for session persistence.
type SessionStore interface {
	// Create creates a new session.
	Create(session Session) error

	// Get retrieves a session by client ID.
	Get(clientID string) (Session, error)

	// Update updates an existing session.
	Update(session Session) error

	// Delete deletes a session by client ID.
	Delete(clientID string) error

	// List returns all sessions.
	List() []Session

	// Cleanup removes expired sessions.
	Cleanup() int
}

// SessionExpiryHandler is called when a session expires.
type SessionExpiryHandler func(session Session)

// SessionFactory creates new Session instances.
// This allows custom session implementations to be used with the server.
type SessionFactory func(clientID string) Session

// DefaultSessionFactory returns a factory that creates MemorySession instances.
func DefaultSessionFactory() SessionFactory {
	return func(clientID string) Session {
		return NewMemorySession(clientID)
	}
}
