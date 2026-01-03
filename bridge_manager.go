package mqttv5

import (
	"errors"
	"sync"
)

// BridgeManager errors.
var (
	ErrBridgeExists   = errors.New("bridge with this ID already exists")
	ErrBridgeNotFound = errors.New("bridge not found")
)

// BridgeManagerMetricsSnapshot is a point-in-time snapshot of bridge manager state.
type BridgeManagerMetricsSnapshot struct {
	TotalBridges   int
	RunningBridges int
}

// BridgeManager coordinates multiple Bridge instances for P2MP (Point-to-Multipoint) scenarios.
// It manages bridge lifecycle and routes messages to appropriate bridges based on topic matching.
type BridgeManager struct {
	server  *Server
	bridges map[string]*Bridge
	mu      sync.RWMutex
}

// NewBridgeManager creates a new bridge manager for the given server.
func NewBridgeManager(server *Server) *BridgeManager {
	return &BridgeManager{
		server:  server,
		bridges: make(map[string]*Bridge),
	}
}

// Add creates and registers a new bridge with the given configuration.
// Returns an error if a bridge with the same ID already exists.
func (m *BridgeManager) Add(config BridgeConfig) (*Bridge, error) {
	bridge, err := NewBridge(m.server, config)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.bridges[bridge.ID()]; exists {
		return nil, ErrBridgeExists
	}

	m.bridges[bridge.ID()] = bridge
	return bridge, nil
}

// Remove removes a bridge by ID. Stops the bridge if it's running.
func (m *BridgeManager) Remove(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	bridge, exists := m.bridges[id]
	if !exists {
		return ErrBridgeNotFound
	}

	if bridge.IsRunning() {
		if err := bridge.Stop(); err != nil && !errors.Is(err, ErrBridgeNotRunning) {
			return err
		}
	}

	delete(m.bridges, id)
	return nil
}

// Get returns a bridge by ID.
func (m *BridgeManager) Get(id string) (*Bridge, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	bridge, exists := m.bridges[id]
	return bridge, exists
}

// List returns all bridge IDs.
func (m *BridgeManager) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.bridges))
	for id := range m.bridges {
		ids = append(ids, id)
	}
	return ids
}

// StartAll starts all registered bridges.
// Returns the first error encountered, but attempts to start all bridges.
func (m *BridgeManager) StartAll() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var firstErr error
	for _, bridge := range m.bridges {
		if !bridge.IsRunning() {
			if err := bridge.Start(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// StopAll stops all running bridges.
// Returns the first error encountered, but attempts to stop all bridges.
func (m *BridgeManager) StopAll() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var firstErr error
	for _, bridge := range m.bridges {
		if bridge.IsRunning() {
			if err := bridge.Stop(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Start starts a specific bridge by ID.
func (m *BridgeManager) Start(id string) error {
	m.mu.RLock()
	bridge, exists := m.bridges[id]
	m.mu.RUnlock()

	if !exists {
		return ErrBridgeNotFound
	}
	return bridge.Start()
}

// Stop stops a specific bridge by ID.
func (m *BridgeManager) Stop(id string) error {
	m.mu.RLock()
	bridge, exists := m.bridges[id]
	m.mu.RUnlock()

	if !exists {
		return ErrBridgeNotFound
	}
	return bridge.Stop()
}

// ForwardToRemote forwards a message to all matching bridges based on topic.
// Each bridge independently decides whether to forward based on its topic configuration.
// This should be called from the server's OnMessage callback.
func (m *BridgeManager) ForwardToRemote(msg *Message) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bridge := range m.bridges {
		bridge.ForwardToRemote(msg)
	}
}

// Count returns the number of registered bridges.
func (m *BridgeManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.bridges)
}

// RunningCount returns the number of running bridges.
func (m *BridgeManager) RunningCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, bridge := range m.bridges {
		if bridge.IsRunning() {
			count++
		}
	}
	return count
}

// Metrics returns a snapshot of bridge manager state.
func (m *BridgeManager) Metrics() BridgeManagerMetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := BridgeManagerMetricsSnapshot{
		TotalBridges: len(m.bridges),
	}

	for _, bridge := range m.bridges {
		if bridge.IsRunning() {
			snapshot.RunningBridges++
		}
	}

	return snapshot
}
