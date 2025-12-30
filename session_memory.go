package mqttv5

import (
	"maps"
	"sync"
	"time"
)

// MemorySession is an in-memory implementation of Session.
type MemorySession struct {
	mu              sync.RWMutex
	clientID        string
	subscriptions   map[string]Subscription
	pendingMessages map[uint16]*Message
	inflightQoS1    map[uint16]*QoS1Message
	inflightQoS2    map[uint16]*QoS2Message
	packetIDCounter uint16
	expiryTime      time.Time
	createdAt       time.Time
	lastActivity    time.Time
}

// NewMemorySession creates a new in-memory session.
func NewMemorySession(clientID string) *MemorySession {
	now := time.Now()
	return &MemorySession{
		clientID:        clientID,
		subscriptions:   make(map[string]Subscription),
		pendingMessages: make(map[uint16]*Message),
		inflightQoS1:    make(map[uint16]*QoS1Message),
		inflightQoS2:    make(map[uint16]*QoS2Message),
		createdAt:       now,
		lastActivity:    now,
	}
}

func (s *MemorySession) ClientID() string {
	return s.clientID
}

func (s *MemorySession) Subscriptions() []Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subs := make([]Subscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		subs = append(subs, sub)
	}
	return subs
}

func (s *MemorySession) AddSubscription(sub Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[sub.TopicFilter] = sub
}

func (s *MemorySession) RemoveSubscription(filter string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.subscriptions[filter]; ok {
		delete(s.subscriptions, filter)
		return true
	}
	return false
}

func (s *MemorySession) HasSubscription(filter string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.subscriptions[filter]
	return ok
}

func (s *MemorySession) GetSubscription(filter string) (Subscription, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.subscriptions[filter]
	return sub, ok
}

func (s *MemorySession) NextPacketID() uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Try to find an unused packet ID, checking against in-flight messages
	// Maximum attempts equals the number of possible packet IDs (65535)
	for range 65535 {
		s.packetIDCounter++
		if s.packetIDCounter == 0 {
			s.packetIDCounter = 1
		}

		// Check if this ID is already in use by in-flight messages
		if _, exists := s.inflightQoS1[s.packetIDCounter]; exists {
			continue
		}
		if _, exists := s.inflightQoS2[s.packetIDCounter]; exists {
			continue
		}
		if _, exists := s.pendingMessages[s.packetIDCounter]; exists {
			continue
		}

		return s.packetIDCounter
	}

	// All packet IDs exhausted (extremely unlikely with flow control)
	// Return the counter anyway - caller should handle this edge case
	return s.packetIDCounter
}

func (s *MemorySession) AddPendingMessage(packetID uint16, msg *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingMessages[packetID] = msg
}

func (s *MemorySession) GetPendingMessage(packetID uint16) (*Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msg, ok := s.pendingMessages[packetID]
	return msg, ok
}

func (s *MemorySession) RemovePendingMessage(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.pendingMessages[packetID]; ok {
		delete(s.pendingMessages, packetID)
		return true
	}
	return false
}

func (s *MemorySession) PendingMessages() map[uint16]*Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msgs := make(map[uint16]*Message, len(s.pendingMessages))
	maps.Copy(msgs, s.pendingMessages)
	return msgs
}

func (s *MemorySession) ExpiryTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.expiryTime
}

func (s *MemorySession) SetExpiryTime(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expiryTime = t
}

func (s *MemorySession) IsExpired() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.expiryTime.IsZero() {
		return false
	}
	return time.Now().After(s.expiryTime)
}

func (s *MemorySession) CreatedAt() time.Time {
	return s.createdAt
}

func (s *MemorySession) LastActivity() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastActivity
}

func (s *MemorySession) UpdateLastActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()
}

// MatchSubscriptions returns all subscriptions that match the given topic.
func (s *MemorySession) MatchSubscriptions(topic string) []Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matched []Subscription
	for _, sub := range s.subscriptions {
		if TopicMatch(sub.TopicFilter, topic) {
			matched = append(matched, sub)
		}
	}
	return matched
}

func (s *MemorySession) AddInflightQoS1(packetID uint16, msg *QoS1Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflightQoS1[packetID] = msg
}

func (s *MemorySession) GetInflightQoS1(packetID uint16) (*QoS1Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msg, ok := s.inflightQoS1[packetID]
	return msg, ok
}

func (s *MemorySession) RemoveInflightQoS1(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.inflightQoS1[packetID]; ok {
		delete(s.inflightQoS1, packetID)
		return true
	}
	return false
}

func (s *MemorySession) InflightQoS1() map[uint16]*QoS1Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msgs := make(map[uint16]*QoS1Message, len(s.inflightQoS1))
	maps.Copy(msgs, s.inflightQoS1)
	return msgs
}

func (s *MemorySession) AddInflightQoS2(packetID uint16, msg *QoS2Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflightQoS2[packetID] = msg
}

func (s *MemorySession) GetInflightQoS2(packetID uint16) (*QoS2Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msg, ok := s.inflightQoS2[packetID]
	return msg, ok
}

func (s *MemorySession) RemoveInflightQoS2(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.inflightQoS2[packetID]; ok {
		delete(s.inflightQoS2, packetID)
		return true
	}
	return false
}

func (s *MemorySession) InflightQoS2() map[uint16]*QoS2Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msgs := make(map[uint16]*QoS2Message, len(s.inflightQoS2))
	maps.Copy(msgs, s.inflightQoS2)
	return msgs
}

// MemorySessionStore is an in-memory implementation of SessionStore.
type MemorySessionStore struct {
	mu            sync.RWMutex
	sessions      map[string]Session
	expiryHandler SessionExpiryHandler
}

// NewMemorySessionStore creates a new in-memory session store.
func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{
		sessions: make(map[string]Session),
	}
}

// SetExpiryHandler sets the session expiry handler.
func (s *MemorySessionStore) SetExpiryHandler(handler SessionExpiryHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expiryHandler = handler
}

func (s *MemorySessionStore) Create(session Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[session.ClientID()]; ok {
		return ErrSessionExists
	}
	s.sessions[session.ClientID()] = session
	return nil
}

func (s *MemorySessionStore) Get(clientID string) (Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	session, ok := s.sessions[clientID]
	if !ok {
		return nil, ErrSessionNotFound
	}
	return session, nil
}

func (s *MemorySessionStore) Update(session Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[session.ClientID()]; !ok {
		return ErrSessionNotFound
	}
	s.sessions[session.ClientID()] = session
	return nil
}

func (s *MemorySessionStore) Delete(clientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.sessions[clientID]; !ok {
		return ErrSessionNotFound
	}
	delete(s.sessions, clientID)
	return nil
}

func (s *MemorySessionStore) List() []Session {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sessions := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

func (s *MemorySessionStore) Cleanup() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []Session
	for _, session := range s.sessions {
		if session.IsExpired() {
			expired = append(expired, session)
		}
	}

	for _, session := range expired {
		delete(s.sessions, session.ClientID())
		if s.expiryHandler != nil {
			s.expiryHandler(session)
		}
	}

	return len(expired)
}
