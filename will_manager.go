package mqttv5

import (
	"sync"
	"time"
)

// WillEntry represents a pending will message.
type WillEntry struct {
	ClientID      string
	Will          *WillMessage
	PublishAt     time.Time // when to publish (now + delay)
	SessionExpiry time.Time // session expiry deadline
}

// IsReady returns true if the will should be published now.
func (e *WillEntry) IsReady() bool {
	return time.Now().After(e.PublishAt)
}

// IsSessionExpired returns true if the session has expired.
func (e *WillEntry) IsSessionExpired() bool {
	if e.SessionExpiry.IsZero() {
		return false
	}
	return time.Now().After(e.SessionExpiry)
}

// WillManager manages will messages for client connections.
type WillManager struct {
	mu      sync.RWMutex
	active  map[string]*WillMessage // registered wills (during connection)
	pending map[string]*WillEntry   // pending wills (after disconnect)
}

// NewWillManager creates a new will manager.
func NewWillManager() *WillManager {
	return &WillManager{
		active:  make(map[string]*WillMessage),
		pending: make(map[string]*WillEntry),
	}
}

// Register registers a will message for a client.
func (m *WillManager) Register(clientID string, will *WillMessage) {
	if will == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.active[clientID] = will
	// Remove from pending if reconnecting
	delete(m.pending, clientID)
}

// Unregister removes a will message (clean disconnect).
func (m *WillManager) Unregister(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.active, clientID)
	delete(m.pending, clientID)
}

// TriggerWill moves a will to pending state with optional delay.
// sessionExpiry is when the session expires (will must be published before this).
func (m *WillManager) TriggerWill(clientID string, sessionExpiry time.Duration) *WillEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	will, ok := m.active[clientID]
	if !ok {
		return nil
	}

	delete(m.active, clientID)

	now := time.Now()
	publishAt := now

	if will.DelayInterval > 0 {
		publishAt = now.Add(time.Duration(will.DelayInterval) * time.Second)
	}

	var sessionExpiryTime time.Time
	if sessionExpiry > 0 {
		sessionExpiryTime = now.Add(sessionExpiry)
		// Will must be published before session expires (subtract 1 second buffer)
		if !sessionExpiryTime.IsZero() && publishAt.After(sessionExpiryTime) {
			publishAt = sessionExpiryTime.Add(-time.Second)
			// Ensure we don't go before 'now'
			if publishAt.Before(now) {
				publishAt = now
			}
		}
	}

	entry := &WillEntry{
		ClientID:      clientID,
		Will:          will,
		PublishAt:     publishAt,
		SessionExpiry: sessionExpiryTime,
	}

	m.pending[clientID] = entry
	return entry
}

// CancelPending cancels a pending will (client reconnected).
func (m *WillManager) CancelPending(clientID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.pending[clientID]
	if ok {
		delete(m.pending, clientID)
	}
	return ok
}

// GetReadyWills returns wills that are ready to be published.
func (m *WillManager) GetReadyWills() []*WillEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	var ready []*WillEntry
	for clientID, entry := range m.pending {
		if entry.IsReady() || entry.IsSessionExpired() {
			ready = append(ready, entry)
			delete(m.pending, clientID)
		}
	}
	return ready
}

// GetPendingCount returns the number of pending wills.
func (m *WillManager) GetPendingCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.pending)
}

// GetActiveCount returns the number of active (registered) wills.
func (m *WillManager) GetActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.active)
}

// HasWill checks if a client has a registered will.
func (m *WillManager) HasWill(clientID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.active[clientID]
	return ok
}

// HasPendingWill checks if a client has a pending will.
func (m *WillManager) HasPendingWill(clientID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.pending[clientID]
	return ok
}

// GetNextPublishTime returns the earliest time a pending will should be published.
// Returns zero time if no pending wills.
func (m *WillManager) GetNextPublishTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var earliest time.Time
	for _, entry := range m.pending {
		if earliest.IsZero() || entry.PublishAt.Before(earliest) {
			earliest = entry.PublishAt
		}
	}
	return earliest
}
