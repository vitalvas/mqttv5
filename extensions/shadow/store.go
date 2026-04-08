package shadow

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Store defines the interface for shadow persistence.
type Store interface {
	Get(key Key) (*Document, error)
	Save(key Key, doc *Document) error
	Delete(key Key) (*Document, error)
}

// Watcher extends Store with change notification support.
// Implement this for stores backed by Redis, NATS KV, etc. that can
// detect external changes.
type Watcher interface {
	Store
	// Watch starts watching for external shadow changes.
	// The handler calls onChange for each change, which triggers
	// delta computation and publishes to MQTT subscribers.
	// Returns a stop function to cancel watching.
	Watch(onChange func(key Key, doc *Document)) (stop func(), err error)
}

// Counter extends Store with the ability to count named shadows per thing or group.
// Used to enforce MaxNamedShadows limits.
// The key identifies the owner: ClientID for per-device, GroupName for shared.
// ShadowName in the key is ignored.
type Counter interface {
	CountNamedShadows(key Key) (int, error)
}

// Lister extends Store with the ability to list named shadows for a thing or group.
// The key identifies the owner: ClientID for per-device, GroupName for shared.
// ShadowName in the key is ignored.
type Lister interface {
	ListNamedShadows(key Key, pageSize int, nextToken string) (*ListResult, error)
}

// MemoryStore is a thread-safe in-memory implementation of Store.
// It deep-copies documents on Get and Save to prevent external mutation.
type MemoryStore struct {
	mu    sync.RWMutex
	store map[string]*Document
}

// NewMemoryStore creates a new in-memory shadow store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		store: make(map[string]*Document),
	}
}

func shadowKey(key Key) string {
	if key.GroupName != "" {
		return fmt.Sprintf("%s/$shared/%s/%s", key.Namespace, key.GroupName, key.ShadowName)
	}

	return fmt.Sprintf("%s/%s/%s", key.Namespace, key.ClientID, key.ShadowName)
}

func (m *MemoryStore) storeKey(key Key) string {
	return shadowKey(key)
}

// storePrefix returns the key prefix for listing/counting named shadows.
// For per-device: "namespace/clientID/"
// For shared: "namespace/$shared/groupName/"
func storePrefix(key Key) string {
	if key.GroupName != "" {
		return fmt.Sprintf("%s/$shared/%s/", key.Namespace, key.GroupName)
	}

	return fmt.Sprintf("%s/%s/", key.Namespace, key.ClientID)
}

// CountNamedShadows returns the number of named shadows for a given owner.
func (m *MemoryStore) CountNamedShadows(key Key) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	prefix := storePrefix(key)
	count := 0

	for k := range m.store {
		if strings.HasPrefix(k, prefix) {
			shadowName := k[len(prefix):]
			if shadowName != "" {
				count++
			}
		}
	}

	return count, nil
}

// Get retrieves a shadow document by key. Returns a deep copy.
// Returns nil, nil if the shadow does not exist.
func (m *MemoryStore) Get(key Key) (*Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	doc, ok := m.store[m.storeKey(key)]
	if !ok {
		return nil, nil
	}

	return deepCopy(doc)
}

// Save stores a shadow document. The document is deep-copied before storing.
func (m *MemoryStore) Save(key Key, doc *Document) error {
	cp, err := deepCopy(doc)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.store[m.storeKey(key)] = cp
	m.mu.Unlock()

	return nil
}

// Delete removes a shadow document and returns the last version before deletion.
// Returns nil, nil if the shadow does not exist.
func (m *MemoryStore) Delete(key Key) (*Document, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sk := m.storeKey(key)

	doc, ok := m.store[sk]
	if !ok {
		return nil, nil
	}

	delete(m.store, sk)

	return deepCopy(doc)
}

// ListNamedShadows returns named shadow names for a given owner with pagination.
func (m *MemoryStore) ListNamedShadows(key Key, pageSize int, nextToken string) (*ListResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	prefix := storePrefix(key)
	var names []string

	for k := range m.store {
		if strings.HasPrefix(k, prefix) {
			shadowName := k[len(prefix):]
			if shadowName != "" {
				names = append(names, shadowName)
			}
		}
	}

	sort.Strings(names)

	if pageSize <= 0 {
		pageSize = 25
	}

	startIdx := 0
	if nextToken != "" {
		for i, n := range names {
			if n == nextToken {
				startIdx = i + 1
				break
			}
		}
	}

	end := startIdx + pageSize
	var resultNextToken string

	if end < len(names) {
		resultNextToken = names[end-1]
		names = names[startIdx:end]
	} else {
		names = names[startIdx:]
	}

	if names == nil {
		names = []string{}
	}

	return &ListResult{
		Results:   names,
		NextToken: resultNextToken,
	}, nil
}

// deepCopy creates a deep copy of a Document via JSON round-trip.
func deepCopy(doc *Document) (*Document, error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	var cp Document
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}

	return &cp, nil
}
