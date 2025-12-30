package mqttv5

import (
	"sync"
	"time"
)

// KeepAliveManager manages keep-alive tracking for client connections.
type KeepAliveManager struct {
	mu             sync.RWMutex
	clients        map[string]*keepAliveEntry
	serverOverride uint16  // server keep-alive override (0 = use client value)
	graceFactor    float64 // multiplier for timeout (MQTT spec suggests 1.5)
}

type keepAliveEntry struct {
	keepAlive    uint16
	lastActivity time.Time
	deadline     time.Time
}

// NewKeepAliveManager creates a new keep-alive manager.
func NewKeepAliveManager() *KeepAliveManager {
	return &KeepAliveManager{
		clients:     make(map[string]*keepAliveEntry),
		graceFactor: 1.5,
	}
}

// SetServerOverride sets the server keep-alive override value.
// When set, all clients must use this value instead of their requested value.
func (m *KeepAliveManager) SetServerOverride(seconds uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.serverOverride = seconds
}

// ServerOverride returns the server keep-alive override value.
func (m *KeepAliveManager) ServerOverride() uint16 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.serverOverride
}

// SetGraceFactor sets the grace period multiplier.
// MQTT spec suggests 1.5x the keep-alive interval.
func (m *KeepAliveManager) SetGraceFactor(factor float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if factor < 1.0 {
		factor = 1.0
	}
	m.graceFactor = factor
}

// Register registers a client with its keep-alive interval.
// Returns the effective keep-alive value (may be overridden by server).
func (m *KeepAliveManager) Register(clientID string, clientKeepAlive uint16) uint16 {
	m.mu.Lock()
	defer m.mu.Unlock()

	effectiveKeepAlive := clientKeepAlive
	if m.serverOverride > 0 {
		effectiveKeepAlive = m.serverOverride
	}

	now := time.Now()
	var deadline time.Time

	if effectiveKeepAlive > 0 {
		timeout := time.Duration(float64(effectiveKeepAlive)*m.graceFactor) * time.Second
		deadline = now.Add(timeout)
	}

	m.clients[clientID] = &keepAliveEntry{
		keepAlive:    effectiveKeepAlive,
		lastActivity: now,
		deadline:     deadline,
	}

	return effectiveKeepAlive
}

// Unregister removes a client from tracking.
func (m *KeepAliveManager) Unregister(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.clients, clientID)
}

// UpdateActivity updates the last activity time for a client.
func (m *KeepAliveManager) UpdateActivity(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.clients[clientID]
	if !ok {
		return
	}

	now := time.Now()
	entry.lastActivity = now

	if entry.keepAlive > 0 {
		timeout := time.Duration(float64(entry.keepAlive)*m.graceFactor) * time.Second
		entry.deadline = now.Add(timeout)
	}
}

// IsExpired checks if a client's keep-alive has expired.
func (m *KeepAliveManager) IsExpired(clientID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.clients[clientID]
	if !ok {
		return false
	}

	// Keep-alive of 0 means no timeout
	if entry.keepAlive == 0 {
		return false
	}

	return time.Now().After(entry.deadline)
}

// GetDeadline returns the deadline for a client.
func (m *KeepAliveManager) GetDeadline(clientID string) (time.Time, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.clients[clientID]
	if !ok {
		return time.Time{}, false
	}

	return entry.deadline, true
}

// GetExpiredClients returns a list of clients that have exceeded their keep-alive.
func (m *KeepAliveManager) GetExpiredClients() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	var expired []string

	for clientID, entry := range m.clients {
		if entry.keepAlive > 0 && now.After(entry.deadline) {
			expired = append(expired, clientID)
		}
	}

	return expired
}

// GetKeepAlive returns the keep-alive interval for a client.
func (m *KeepAliveManager) GetKeepAlive(clientID string) (uint16, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.clients[clientID]
	if !ok {
		return 0, false
	}

	return entry.keepAlive, true
}

// Count returns the number of tracked clients.
func (m *KeepAliveManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.clients)
}
