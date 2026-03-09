package shadow

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStore(t *testing.T) {
	t.Run("Get non-existent", func(t *testing.T) {
		store := NewMemoryStore()
		doc, err := store.Get(Key{Namespace: "default", ClientID: "dev1"})
		require.NoError(t, err)
		assert.Nil(t, doc)
	})

	t.Run("Save and Get", func(t *testing.T) {
		store := NewMemoryStore()
		key := Key{Namespace: "default", ClientID: "dev1"}

		original := &Document{
			State: State{
				Desired: map[string]any{"temp": 22.0},
			},
			Version:   1,
			Timestamp: 1000,
		}

		require.NoError(t, store.Save(key, original))

		doc, err := store.Get(key)
		require.NoError(t, err)
		require.NotNil(t, doc)
		assert.Equal(t, int64(1), doc.Version)
		assert.Equal(t, 22.0, doc.State.Desired["temp"])
	})

	t.Run("deep copy on Save", func(t *testing.T) {
		store := NewMemoryStore()
		key := Key{Namespace: "default", ClientID: "dev1"}

		original := &Document{
			State: State{
				Desired: map[string]any{"temp": 22.0},
			},
			Version: 1,
		}

		require.NoError(t, store.Save(key, original))

		// Mutate original
		original.State.Desired["temp"] = 99.0

		doc, err := store.Get(key)
		require.NoError(t, err)
		assert.Equal(t, 22.0, doc.State.Desired["temp"])
	})

	t.Run("deep copy on Get", func(t *testing.T) {
		store := NewMemoryStore()
		key := Key{Namespace: "default", ClientID: "dev1"}

		require.NoError(t, store.Save(key, &Document{
			State: State{
				Desired: map[string]any{"temp": 22.0},
			},
			Version: 1,
		}))

		doc1, _ := store.Get(key)
		doc1.State.Desired["temp"] = 99.0

		doc2, _ := store.Get(key)
		assert.Equal(t, 22.0, doc2.State.Desired["temp"])
	})

	t.Run("Delete existing", func(t *testing.T) {
		store := NewMemoryStore()
		key := Key{Namespace: "default", ClientID: "dev1"}

		require.NoError(t, store.Save(key, &Document{
			State:   State{Desired: map[string]any{"temp": 22.0}},
			Version: 1,
		}))

		doc, err := store.Delete(key)
		require.NoError(t, err)
		require.NotNil(t, doc)
		assert.Equal(t, int64(1), doc.Version)

		// Verify deleted
		doc, err = store.Get(key)
		require.NoError(t, err)
		assert.Nil(t, doc)
	})

	t.Run("Delete non-existent", func(t *testing.T) {
		store := NewMemoryStore()
		doc, err := store.Delete(Key{Namespace: "default", ClientID: "dev1"})
		require.NoError(t, err)
		assert.Nil(t, doc)
	})

	t.Run("namespace isolation", func(t *testing.T) {
		store := NewMemoryStore()
		key1 := Key{Namespace: "ns1", ClientID: "dev1"}
		key2 := Key{Namespace: "ns2", ClientID: "dev1"}

		require.NoError(t, store.Save(key1, &Document{
			State:   State{Desired: map[string]any{"temp": 22.0}},
			Version: 1,
		}))

		require.NoError(t, store.Save(key2, &Document{
			State:   State{Desired: map[string]any{"temp": 30.0}},
			Version: 1,
		}))

		doc1, _ := store.Get(key1)
		doc2, _ := store.Get(key2)
		assert.Equal(t, 22.0, doc1.State.Desired["temp"])
		assert.Equal(t, 30.0, doc2.State.Desired["temp"])
	})

	t.Run("shared key format", func(t *testing.T) {
		store := NewMemoryStore()
		key := Key{Namespace: "default", GroupName: "room-101", ShadowName: "config"}

		require.NoError(t, store.Save(key, &Document{
			State:   State{Desired: map[string]any{"temp": 22.0}},
			Version: 1,
		}))

		doc, err := store.Get(key)
		require.NoError(t, err)
		require.NotNil(t, doc)
		assert.Equal(t, 22.0, doc.State.Desired["temp"])
	})

	t.Run("no collision between group and device", func(t *testing.T) {
		store := NewMemoryStore()
		deviceKey := Key{Namespace: "default", ClientID: "room-101"}
		groupKey := Key{Namespace: "default", GroupName: "room-101"}

		require.NoError(t, store.Save(deviceKey, &Document{
			State:   State{Desired: map[string]any{"source": "device"}},
			Version: 1,
		}))

		require.NoError(t, store.Save(groupKey, &Document{
			State:   State{Desired: map[string]any{"source": "group"}},
			Version: 1,
		}))

		deviceDoc, err := store.Get(deviceKey)
		require.NoError(t, err)
		require.NotNil(t, deviceDoc)
		assert.Equal(t, "device", deviceDoc.State.Desired["source"])

		groupDoc, err := store.Get(groupKey)
		require.NoError(t, err)
		require.NotNil(t, groupDoc)
		assert.Equal(t, "group", groupDoc.State.Desired["source"])
	})

	t.Run("concurrent access", func(t *testing.T) {
		store := NewMemoryStore()
		key := Key{Namespace: "default", ClientID: "dev1"}

		require.NoError(t, store.Save(key, &Document{
			State:   State{Desired: map[string]any{"temp": 22.0}},
			Version: 1,
		}))

		var wg sync.WaitGroup
		for i := range 100 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				_ = store.Save(key, &Document{
					State:   State{Desired: map[string]any{"temp": float64(i)}},
					Version: int64(i),
				})

				_, _ = store.Get(key)
			}(i)
		}

		wg.Wait()

		doc, err := store.Get(key)
		require.NoError(t, err)
		require.NotNil(t, doc)
	})
}

func TestMemoryStore_CountNamedShadows(t *testing.T) {
	t.Run("empty store", func(t *testing.T) {
		store := NewMemoryStore()
		count, err := store.CountNamedShadows(Key{Namespace: "default", ClientID: "dev1"})
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("counts only named shadows", func(t *testing.T) {
		store := NewMemoryStore()

		// Classic shadow (empty name)
		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1"}, &Document{Version: 1}))
		// Named shadow "config"
		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: "config"}, &Document{Version: 1}))
		// Named shadow "firmware"
		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: "firmware"}, &Document{Version: 1}))

		count, err := store.CountNamedShadows(Key{Namespace: "default", ClientID: "dev1"})
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("does not count other clients", func(t *testing.T) {
		store := NewMemoryStore()

		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: "config"}, &Document{Version: 1}))
		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev2", ShadowName: "config"}, &Document{Version: 1}))

		count, err := store.CountNamedShadows(Key{Namespace: "default", ClientID: "dev1"})
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("does not count shared shadows", func(t *testing.T) {
		store := NewMemoryStore()

		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: "config"}, &Document{Version: 1}))
		require.NoError(t, store.Save(Key{Namespace: "default", GroupName: "room-101", ShadowName: "config"}, &Document{Version: 1}))

		count, err := store.CountNamedShadows(Key{Namespace: "default", ClientID: "dev1"})
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("counts shared group named shadows", func(t *testing.T) {
		store := NewMemoryStore()

		require.NoError(t, store.Save(Key{Namespace: "default", GroupName: "room-101", ShadowName: "config"}, &Document{Version: 1}))
		require.NoError(t, store.Save(Key{Namespace: "default", GroupName: "room-101", ShadowName: "firmware"}, &Document{Version: 1}))
		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: "config"}, &Document{Version: 1}))

		count, err := store.CountNamedShadows(Key{Namespace: "default", GroupName: "room-101"})
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}

func TestMemoryStore_DeepCopyErrors(t *testing.T) {
	t.Run("Save with unmarshalable value", func(t *testing.T) {
		store := NewMemoryStore()
		key := Key{Namespace: "default", ClientID: "dev1"}

		doc := &Document{
			State: State{
				Desired: map[string]any{"fn": func() {}},
			},
		}

		err := store.Save(key, doc)
		require.Error(t, err)
	})

	t.Run("Get after direct store mutation with unmarshalable value", func(t *testing.T) {
		store := NewMemoryStore()
		key := Key{Namespace: "default", ClientID: "dev1"}

		// Save a valid document first
		require.NoError(t, store.Save(key, &Document{
			State:   State{Desired: map[string]any{"temp": 22.0}},
			Version: 1,
		}))

		// Directly inject unmarshalable value into stored doc to trigger Get error
		sk := store.storeKey(key)
		store.mu.Lock()
		store.store[sk].State.Desired["fn"] = func() {}
		store.mu.Unlock()

		_, err := store.Get(key)
		require.Error(t, err)
	})

	t.Run("Delete after direct store mutation with unmarshalable value", func(t *testing.T) {
		store := NewMemoryStore()
		key := Key{Namespace: "default", ClientID: "dev1"}

		require.NoError(t, store.Save(key, &Document{
			State:   State{Desired: map[string]any{"temp": 22.0}},
			Version: 1,
		}))

		sk := store.storeKey(key)
		store.mu.Lock()
		store.store[sk].State.Desired["fn"] = func() {}
		store.mu.Unlock()

		_, err := store.Delete(key)
		require.Error(t, err)
	})
}

func TestMemoryStore_ListNamedShadows(t *testing.T) {
	t.Run("empty store", func(t *testing.T) {
		store := NewMemoryStore()
		result, err := store.ListNamedShadows(Key{Namespace: "default", ClientID: "dev1"}, 25, "")
		require.NoError(t, err)
		assert.Empty(t, result.Results)
		assert.Empty(t, result.NextToken)
	})

	t.Run("lists only named shadows", func(t *testing.T) {
		store := NewMemoryStore()

		// Classic shadow (not listed)
		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1"}, &Document{Version: 1}))
		// Named shadows
		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: "config"}, &Document{Version: 1}))
		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: "firmware"}, &Document{Version: 1}))

		result, err := store.ListNamedShadows(Key{Namespace: "default", ClientID: "dev1"}, 25, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"config", "firmware"}, result.Results)
		assert.Empty(t, result.NextToken)
	})

	t.Run("does not list other clients", func(t *testing.T) {
		store := NewMemoryStore()

		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: "config"}, &Document{Version: 1}))
		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev2", ShadowName: "firmware"}, &Document{Version: 1}))

		result, err := store.ListNamedShadows(Key{Namespace: "default", ClientID: "dev1"}, 25, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"config"}, result.Results)
	})

	t.Run("pagination with nextToken", func(t *testing.T) {
		store := NewMemoryStore()

		for _, name := range []string{"a", "b", "c", "d", "e"} {
			require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: name}, &Document{Version: 1}))
		}

		// Page 1
		result, err := store.ListNamedShadows(Key{Namespace: "default", ClientID: "dev1"}, 3, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b", "c"}, result.Results)
		assert.NotEmpty(t, result.NextToken)

		// Page 2
		result, err = store.ListNamedShadows(Key{Namespace: "default", ClientID: "dev1"}, 3, result.NextToken)
		require.NoError(t, err)
		assert.Equal(t, []string{"d", "e"}, result.Results)
		assert.Empty(t, result.NextToken)
	})

	t.Run("default page size", func(t *testing.T) {
		store := NewMemoryStore()

		for i := range 30 {
			name := fmt.Sprintf("shadow-%02d", i)
			require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: name}, &Document{Version: 1}))
		}

		result, err := store.ListNamedShadows(Key{Namespace: "default", ClientID: "dev1"}, 0, "")
		require.NoError(t, err)
		assert.Len(t, result.Results, 25)
		assert.NotEmpty(t, result.NextToken)
	})

	t.Run("does not list shared shadows", func(t *testing.T) {
		store := NewMemoryStore()

		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: "config"}, &Document{Version: 1}))
		require.NoError(t, store.Save(Key{Namespace: "default", GroupName: "room-101", ShadowName: "config"}, &Document{Version: 1}))

		result, err := store.ListNamedShadows(Key{Namespace: "default", ClientID: "dev1"}, 25, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"config"}, result.Results)
	})

	t.Run("results are sorted", func(t *testing.T) {
		store := NewMemoryStore()

		for _, name := range []string{"zebra", "alpha", "middle"} {
			require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: name}, &Document{Version: 1}))
		}

		result, err := store.ListNamedShadows(Key{Namespace: "default", ClientID: "dev1"}, 25, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"alpha", "middle", "zebra"}, result.Results)
	})

	t.Run("lists shared group named shadows", func(t *testing.T) {
		store := NewMemoryStore()

		require.NoError(t, store.Save(Key{Namespace: "default", GroupName: "room-101", ShadowName: "config"}, &Document{Version: 1}))
		require.NoError(t, store.Save(Key{Namespace: "default", GroupName: "room-101", ShadowName: "firmware"}, &Document{Version: 1}))
		require.NoError(t, store.Save(Key{Namespace: "default", ClientID: "dev1", ShadowName: "config"}, &Document{Version: 1}))

		result, err := store.ListNamedShadows(Key{Namespace: "default", GroupName: "room-101"}, 25, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"config", "firmware"}, result.Results)
	})

	t.Run("namespace isolation for listing", func(t *testing.T) {
		store := NewMemoryStore()

		require.NoError(t, store.Save(Key{Namespace: "ns1", ClientID: "dev1", ShadowName: "config"}, &Document{Version: 1}))
		require.NoError(t, store.Save(Key{Namespace: "ns2", ClientID: "dev1", ShadowName: "firmware"}, &Document{Version: 1}))

		result, err := store.ListNamedShadows(Key{Namespace: "ns1", ClientID: "dev1"}, 25, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"config"}, result.Results)
	})
}

func TestShadowWatcherIntegration(t *testing.T) {
	t.Run("mock watcher", func(t *testing.T) {
		watcher := &mockWatcherStore{
			MemoryStore: NewMemoryStore(),
		}

		var received []Key
		var mu sync.Mutex

		stop, err := watcher.Watch(func(key Key, _ *Document) {
			mu.Lock()
			received = append(received, key)
			mu.Unlock()
		})
		require.NoError(t, err)
		require.NotNil(t, stop)

		key := Key{Namespace: "default", ClientID: "dev1"}
		watcher.triggerChange(key, &Document{Version: 1})

		mu.Lock()
		assert.Len(t, received, 1)
		assert.Equal(t, "dev1", received[0].ClientID)
		mu.Unlock()

		stop()
	})
}

// mockWatcherStore implements ShadowWatcher for testing.
type mockWatcherStore struct {
	*MemoryStore
	onChange func(key Key, doc *Document)
}

func (m *mockWatcherStore) Watch(onChange func(key Key, doc *Document)) (func(), error) {
	m.onChange = onChange
	return func() { m.onChange = nil }, nil
}

func (m *mockWatcherStore) triggerChange(key Key, doc *Document) {
	if m.onChange != nil {
		m.onChange(key, doc)
	}
}
