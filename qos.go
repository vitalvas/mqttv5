package mqttv5

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrPacketIDExhausted = errors.New("no available packet IDs")
	ErrPacketIDNotFound  = errors.New("packet ID not found")
	ErrMessageNotFound   = errors.New("message not found")
)

// PacketIDManager manages allocation and release of packet IDs (1-65535).
// MQTT v5.0 spec: Section 2.2.1
type PacketIDManager struct {
	mu     sync.Mutex
	used   map[uint16]struct{}
	next   uint16
	maxIDs int
}

// NewPacketIDManager creates a new packet ID manager.
func NewPacketIDManager() *PacketIDManager {
	return &PacketIDManager{
		used:   make(map[uint16]struct{}),
		next:   1,
		maxIDs: 65535,
	}
}

// Allocate returns the next available packet ID.
func (m *PacketIDManager) Allocate() (uint16, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.used) >= m.maxIDs {
		return 0, ErrPacketIDExhausted
	}

	start := m.next
	for {
		if _, ok := m.used[m.next]; !ok {
			id := m.next
			m.used[id] = struct{}{}
			m.next++
			if m.next == 0 {
				m.next = 1
			}
			return id, nil
		}
		m.next++
		if m.next == 0 {
			m.next = 1
		}
		if m.next == start {
			return 0, ErrPacketIDExhausted
		}
	}
}

// Release releases a packet ID for reuse.
func (m *PacketIDManager) Release(id uint16) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.used[id]; !ok {
		return ErrPacketIDNotFound
	}
	delete(m.used, id)
	return nil
}

// IsUsed returns true if the packet ID is currently in use.
func (m *PacketIDManager) IsUsed(id uint16) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.used[id]
	return ok
}

// InUse returns the count of packet IDs currently in use.
func (m *PacketIDManager) InUse() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.used)
}

// QoS1State represents the state of a QoS 1 publish flow.
// MQTT v5.0 spec: Section 4.3.2
type QoS1State int

const (
	QoS1AwaitingPuback QoS1State = 0
	QoS1Complete       QoS1State = 1
)

// QoS1Message represents a QoS 1 message awaiting acknowledgment.
type QoS1Message struct {
	PacketID     uint16
	Message      *Message
	State        QoS1State
	SentAt       time.Time
	RetryCount   int
	RetryTimeout time.Duration
}

// ShouldRetry returns true if the message should be retried.
func (m *QoS1Message) ShouldRetry() bool {
	if m.State != QoS1AwaitingPuback {
		return false
	}
	return time.Since(m.SentAt) > m.RetryTimeout
}

// QoS1Tracker tracks QoS 1 messages awaiting acknowledgment.
// MQTT v5.0 spec: Section 4.3.2
type QoS1Tracker struct {
	mu           sync.RWMutex
	messages     map[uint16]*QoS1Message
	retryTimeout time.Duration
	maxRetries   int
}

// NewQoS1Tracker creates a new QoS 1 tracker.
func NewQoS1Tracker(retryTimeout time.Duration, maxRetries int) *QoS1Tracker {
	return &QoS1Tracker{
		messages:     make(map[uint16]*QoS1Message),
		retryTimeout: retryTimeout,
		maxRetries:   maxRetries,
	}
}

// Track starts tracking a QoS 1 message.
func (t *QoS1Tracker) Track(packetID uint16, msg *Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.messages[packetID] = &QoS1Message{
		PacketID:     packetID,
		Message:      msg,
		State:        QoS1AwaitingPuback,
		SentAt:       time.Now(),
		RetryTimeout: t.retryTimeout,
	}
}

// Acknowledge marks a message as acknowledged and removes it.
func (t *QoS1Tracker) Acknowledge(packetID uint16) (*QoS1Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg, ok := t.messages[packetID]
	if !ok {
		return nil, false
	}
	msg.State = QoS1Complete
	delete(t.messages, packetID)
	return msg, true
}

// Get returns a tracked message.
func (t *QoS1Tracker) Get(packetID uint16) (*QoS1Message, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	msg, ok := t.messages[packetID]
	return msg, ok
}

// GetPendingRetries returns messages that need to be retried.
func (t *QoS1Tracker) GetPendingRetries() []*QoS1Message {
	t.mu.Lock()
	defer t.mu.Unlock()

	var pending []*QoS1Message
	for _, msg := range t.messages {
		if msg.ShouldRetry() && msg.RetryCount < t.maxRetries {
			msg.RetryCount++
			msg.SentAt = time.Now()
			pending = append(pending, msg)
		}
	}
	return pending
}

// Remove removes a message from tracking.
func (t *QoS1Tracker) Remove(packetID uint16) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.messages[packetID]; !ok {
		return false
	}
	delete(t.messages, packetID)
	return true
}

// Count returns the number of tracked messages.
func (t *QoS1Tracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.messages)
}

// CleanupExpired removes messages that have exceeded max retries.
// Returns the number of removed messages.
func (t *QoS1Tracker) CleanupExpired() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	count := 0
	for packetID, msg := range t.messages {
		if msg.RetryCount >= t.maxRetries && msg.ShouldRetry() {
			delete(t.messages, packetID)
			count++
		}
	}
	return count
}

// RetryTimeout returns the configured retry timeout.
func (t *QoS1Tracker) RetryTimeout() time.Duration {
	return t.retryTimeout
}

// QoS2State represents the state of a QoS 2 publish flow.
// MQTT v5.0 spec: Section 4.3.3
type QoS2State int

const (
	// Sender states
	QoS2AwaitingPubrec  QoS2State = 0
	QoS2AwaitingPubcomp QoS2State = 1

	// Receiver states
	QoS2ReceivedPublish QoS2State = 2
	QoS2AwaitingPubrel  QoS2State = 3

	QoS2Complete QoS2State = 4
)

// QoS2Message represents a QoS 2 message in the flow.
type QoS2Message struct {
	PacketID     uint16
	Message      *Message
	State        QoS2State
	SentAt       time.Time
	RetryCount   int
	RetryTimeout time.Duration
	IsSender     bool
}

// ShouldRetry returns true if the message should be retried.
func (m *QoS2Message) ShouldRetry() bool {
	if m.State == QoS2Complete {
		return false
	}
	return time.Since(m.SentAt) > m.RetryTimeout
}

// QoS2Tracker tracks QoS 2 messages in the publish flow.
// MQTT v5.0 spec: Section 4.3.3
type QoS2Tracker struct {
	mu           sync.RWMutex
	messages     map[uint16]*QoS2Message
	completed    map[uint16]time.Time // Track completed packet IDs for PUBCOMP retransmission
	retryTimeout time.Duration
	maxRetries   int
}

// NewQoS2Tracker creates a new QoS 2 tracker.
func NewQoS2Tracker(retryTimeout time.Duration, maxRetries int) *QoS2Tracker {
	return &QoS2Tracker{
		messages:     make(map[uint16]*QoS2Message),
		completed:    make(map[uint16]time.Time),
		retryTimeout: retryTimeout,
		maxRetries:   maxRetries,
	}
}

// TrackSend starts tracking a sent QoS 2 message (sender side).
func (t *QoS2Tracker) TrackSend(packetID uint16, msg *Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.messages[packetID] = &QoS2Message{
		PacketID:     packetID,
		Message:      msg,
		State:        QoS2AwaitingPubrec,
		SentAt:       time.Now(),
		RetryTimeout: t.retryTimeout,
		IsSender:     true,
	}
}

// TrackReceive starts tracking a received QoS 2 message (receiver side).
func (t *QoS2Tracker) TrackReceive(packetID uint16, msg *Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.messages[packetID] = &QoS2Message{
		PacketID:     packetID,
		Message:      msg,
		State:        QoS2ReceivedPublish,
		SentAt:       time.Now(),
		RetryTimeout: t.retryTimeout,
		IsSender:     false,
	}
}

// HandlePubrec handles receiving PUBREC (sender side).
func (t *QoS2Tracker) HandlePubrec(packetID uint16) (*QoS2Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg, ok := t.messages[packetID]
	if !ok || msg.State != QoS2AwaitingPubrec {
		return nil, false
	}
	msg.State = QoS2AwaitingPubcomp
	msg.SentAt = time.Now()
	msg.RetryCount = 0
	return msg, true
}

// HandlePubrel handles receiving PUBREL (receiver side).
// Returns (message, shouldSendPubcomp). If packet ID was already completed,
// returns (nil, true) to allow PUBCOMP retransmission per MQTT spec.
func (t *QoS2Tracker) HandlePubrel(packetID uint16) (*QoS2Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if this is a retransmitted PUBREL for an already completed flow
	if _, completed := t.completed[packetID]; completed {
		// Update completion time to extend the cache
		t.completed[packetID] = time.Now()
		return nil, true // Should send PUBCOMP
	}

	msg, ok := t.messages[packetID]
	if !ok {
		return nil, false
	}
	// Can receive PUBREL in either ReceivedPublish or AwaitingPubrel state
	if msg.State != QoS2ReceivedPublish && msg.State != QoS2AwaitingPubrel {
		return nil, false
	}
	msg.State = QoS2Complete
	delete(t.messages, packetID)

	// Track as completed for potential PUBREL retransmissions
	t.completed[packetID] = time.Now()

	return msg, true
}

// HandlePubcomp handles receiving PUBCOMP (sender side).
func (t *QoS2Tracker) HandlePubcomp(packetID uint16) (*QoS2Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg, ok := t.messages[packetID]
	if !ok || msg.State != QoS2AwaitingPubcomp {
		return nil, false
	}
	msg.State = QoS2Complete
	delete(t.messages, packetID)
	return msg, true
}

// SendPubrec transitions receiver state after sending PUBREC.
func (t *QoS2Tracker) SendPubrec(packetID uint16) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg, ok := t.messages[packetID]
	if !ok || msg.State != QoS2ReceivedPublish {
		return false
	}
	msg.State = QoS2AwaitingPubrel
	msg.SentAt = time.Now()
	return true
}

// Get returns a tracked message.
func (t *QoS2Tracker) Get(packetID uint16) (*QoS2Message, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	msg, ok := t.messages[packetID]
	return msg, ok
}

// GetPendingRetries returns messages that need to be retried.
func (t *QoS2Tracker) GetPendingRetries() []*QoS2Message {
	t.mu.Lock()
	defer t.mu.Unlock()

	var pending []*QoS2Message
	for _, msg := range t.messages {
		if msg.ShouldRetry() && msg.RetryCount < t.maxRetries {
			msg.RetryCount++
			msg.SentAt = time.Now()
			pending = append(pending, msg)
		}
	}
	return pending
}

// Remove removes a message from tracking.
func (t *QoS2Tracker) Remove(packetID uint16) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.messages[packetID]; !ok {
		return false
	}
	delete(t.messages, packetID)
	return true
}

// Count returns the number of tracked messages.
func (t *QoS2Tracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.messages)
}

// CleanupCompleted removes completed packet IDs older than the retry timeout.
// This should be called periodically to prevent memory growth.
func (t *QoS2Tracker) CleanupCompleted() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	count := 0
	now := time.Now()
	for packetID, completedAt := range t.completed {
		if now.Sub(completedAt) > t.retryTimeout*2 {
			delete(t.completed, packetID)
			count++
		}
	}
	return count
}

// CleanupExpired removes messages that have exceeded max retries.
// Returns the number of removed messages.
func (t *QoS2Tracker) CleanupExpired() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	count := 0
	for packetID, msg := range t.messages {
		if msg.RetryCount >= t.maxRetries && msg.ShouldRetry() {
			delete(t.messages, packetID)
			count++
		}
	}
	return count
}

// RetryTimeout returns the configured retry timeout.
func (t *QoS2Tracker) RetryTimeout() time.Duration {
	return t.retryTimeout
}

// StoredMessage represents a message in the message store with expiry.
type StoredMessage struct {
	Message   *Message
	ExpiresAt time.Time
	StoredAt  time.Time
}

// IsExpired returns true if the message has expired.
func (m *StoredMessage) IsExpired() bool {
	if m.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(m.ExpiresAt)
}

// MessageStore defines the interface for message storage with namespace isolation.
type MessageStore interface {
	// Store stores a message with optional expiry in the specified namespace.
	Store(namespace, id string, msg *Message, expiry time.Duration) error

	// Get retrieves a message by namespace and ID.
	Get(namespace, id string) (*Message, bool)

	// Exists checks if a message exists by namespace and ID.
	Exists(namespace, id string) bool

	// Delete deletes a message by namespace and ID.
	Delete(namespace, id string) bool

	// Cleanup removes expired messages from the specified namespace.
	// If namespace is empty, cleans up all namespaces.
	Cleanup(namespace string) int

	// Count returns the number of stored messages in the specified namespace.
	// If namespace is empty, returns total count across all namespaces.
	Count(namespace string) int
}
