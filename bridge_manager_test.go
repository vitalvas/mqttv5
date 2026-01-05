package mqttv5

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBridgeManager(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	assert.NotNil(t, manager)
	assert.Equal(t, 0, manager.Count())
}

func TestBridgeManagerAdd(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	t.Run("adds bridge successfully", func(t *testing.T) {
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			ClientID:   "bridge-1",
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
		}

		bridge, err := manager.Add(config)
		require.NoError(t, err)
		assert.NotNil(t, bridge)
		assert.Equal(t, "bridge-1", bridge.ID())
		assert.Equal(t, 1, manager.Count())
	})

	t.Run("fails on duplicate ID", func(t *testing.T) {
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1884",
			ClientID:   "bridge-1",
			Topics: []BridgeTopic{
				{LocalPrefix: "other", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
		}

		bridge, err := manager.Add(config)
		assert.ErrorIs(t, err, ErrBridgeExists)
		assert.Nil(t, bridge)
	})

	t.Run("fails without topics", func(t *testing.T) {
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1885",
			ClientID:   "bridge-2",
			Topics:     []BridgeTopic{},
		}

		bridge, err := manager.Add(config)
		assert.ErrorIs(t, err, ErrBridgeNoTopics)
		assert.Nil(t, bridge)
	})
}

func TestBridgeManagerRemove(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		ClientID:   "bridge-1",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	_, err := manager.Add(config)
	require.NoError(t, err)

	t.Run("removes existing bridge", func(t *testing.T) {
		err := manager.Remove("bridge-1")
		require.NoError(t, err)
		assert.Equal(t, 0, manager.Count())
	})

	t.Run("fails on non-existent bridge", func(t *testing.T) {
		err := manager.Remove("non-existent")
		assert.ErrorIs(t, err, ErrBridgeNotFound)
	})
}

func TestBridgeManagerGet(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		ClientID:   "bridge-1",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	_, err := manager.Add(config)
	require.NoError(t, err)

	t.Run("gets existing bridge", func(t *testing.T) {
		bridge, exists := manager.Get("bridge-1")
		assert.True(t, exists)
		assert.NotNil(t, bridge)
		assert.Equal(t, "bridge-1", bridge.ID())
	})

	t.Run("returns false for non-existent bridge", func(t *testing.T) {
		bridge, exists := manager.Get("non-existent")
		assert.False(t, exists)
		assert.Nil(t, bridge)
	})
}

func TestBridgeManagerList(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	configs := []BridgeConfig{
		{
			RemoteAddr: "tcp://localhost:1883",
			ClientID:   "bridge-1",
			Topics:     []BridgeTopic{{LocalPrefix: "a", RemotePrefix: "b", Direction: BridgeDirectionBoth}},
		},
		{
			RemoteAddr: "tcp://localhost:1884",
			ClientID:   "bridge-2",
			Topics:     []BridgeTopic{{LocalPrefix: "c", RemotePrefix: "d", Direction: BridgeDirectionBoth}},
		},
		{
			RemoteAddr: "tcp://localhost:1885",
			ClientID:   "bridge-3",
			Topics:     []BridgeTopic{{LocalPrefix: "e", RemotePrefix: "f", Direction: BridgeDirectionBoth}},
		},
	}

	for _, cfg := range configs {
		_, err := manager.Add(cfg)
		require.NoError(t, err)
	}

	ids := manager.List()
	assert.Len(t, ids, 3)
	assert.Contains(t, ids, "bridge-1")
	assert.Contains(t, ids, "bridge-2")
	assert.Contains(t, ids, "bridge-3")
}

func TestBridgeManagerStartStop(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	t.Run("start non-existent bridge fails", func(t *testing.T) {
		err := manager.Start("non-existent")
		assert.ErrorIs(t, err, ErrBridgeNotFound)
	})

	t.Run("stop non-existent bridge fails", func(t *testing.T) {
		err := manager.Stop("non-existent")
		assert.ErrorIs(t, err, ErrBridgeNotFound)
	})
}

func TestBridgeManagerP2MPIntegration(t *testing.T) {
	t.Run("forwards to multiple brokers based on topic", func(t *testing.T) {
		// Start two remote brokers
		remote1Listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer remote1Listener.Close()

		remote2Listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer remote2Listener.Close()

		remote1Server := NewServer(WithListener(remote1Listener))
		go remote1Server.ListenAndServe()
		defer remote1Server.Close()

		remote2Server := NewServer(WithListener(remote2Listener))
		go remote2Server.ListenAndServe()
		defer remote2Server.Close()

		// Start local broker
		localListener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer localListener.Close()

		localServer := NewServer(WithListener(localListener))
		go localServer.ListenAndServe()
		defer localServer.Close()

		time.Sleep(50 * time.Millisecond)

		// Create bridge manager
		manager := NewBridgeManager(localServer)

		// Add bridge to remote1 for "sensors" topics
		_, err = manager.Add(BridgeConfig{
			RemoteAddr: "tcp://" + remote1Listener.Addr().String(),
			ClientID:   "bridge-sensors",
			Topics: []BridgeTopic{
				{LocalPrefix: "sensors", RemotePrefix: "data/sensors", Direction: BridgeDirectionIn, QoS: 1},
			},
		})
		require.NoError(t, err)

		// Add bridge to remote2 for "alerts" topics
		_, err = manager.Add(BridgeConfig{
			RemoteAddr: "tcp://" + remote2Listener.Addr().String(),
			ClientID:   "bridge-alerts",
			Topics: []BridgeTopic{
				{LocalPrefix: "alerts", RemotePrefix: "notifications", Direction: BridgeDirectionIn, QoS: 1},
			},
		})
		require.NoError(t, err)

		assert.Equal(t, 2, manager.Count())

		// Start all bridges
		err = manager.StartAll()
		require.NoError(t, err)
		defer manager.StopAll()

		assert.Equal(t, 2, manager.RunningCount())

		time.Sleep(50 * time.Millisecond)

		// Subscribe to local broker
		var receivedMsgs []*Message
		var mu sync.Mutex
		localSubscriber, err := Dial(WithServers("tcp://"+localListener.Addr().String()),
			WithClientID("local-subscriber"),
		)
		require.NoError(t, err)
		defer localSubscriber.Close()

		err = localSubscriber.Subscribe("#", 1, func(msg *Message) {
			mu.Lock()
			receivedMsgs = append(receivedMsgs, msg)
			mu.Unlock()
		})
		require.NoError(t, err)

		time.Sleep(50 * time.Millisecond)

		// Publish to remote1 (sensors)
		remote1Client, err := Dial(WithServers("tcp://"+remote1Listener.Addr().String()),
			WithClientID("remote1-publisher"),
		)
		require.NoError(t, err)
		defer remote1Client.Close()

		err = remote1Client.Publish(&Message{
			Topic:   "data/sensors/temp",
			Payload: []byte("25.5"),
			QoS:     1,
		})
		require.NoError(t, err)

		// Publish to remote2 (alerts)
		remote2Client, err := Dial(WithServers("tcp://"+remote2Listener.Addr().String()),
			WithClientID("remote2-publisher"),
		)
		require.NoError(t, err)
		defer remote2Client.Close()

		err = remote2Client.Publish(&Message{
			Topic:   "notifications/fire",
			Payload: []byte("building-a"),
			QoS:     1,
		})
		require.NoError(t, err)

		// Wait for messages
		time.Sleep(200 * time.Millisecond)

		// Verify messages
		mu.Lock()
		defer mu.Unlock()

		require.Len(t, receivedMsgs, 2, "should receive messages from both remotes")

		topics := make(map[string][]byte)
		for _, msg := range receivedMsgs {
			topics[msg.Topic] = msg.Payload
		}

		assert.Equal(t, []byte("25.5"), topics["sensors/temp"])
		assert.Equal(t, []byte("building-a"), topics["alerts/fire"])
	})
}

func TestBridgeManagerMetrics(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	// Add two bridges
	_, err := manager.Add(BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		ClientID:   "bridge-1",
		Topics: []BridgeTopic{
			{LocalPrefix: "a", RemotePrefix: "b", Direction: BridgeDirectionBoth},
		},
	})
	require.NoError(t, err)

	_, err = manager.Add(BridgeConfig{
		RemoteAddr: "tcp://localhost:1884",
		ClientID:   "bridge-2",
		Topics: []BridgeTopic{
			{LocalPrefix: "c", RemotePrefix: "d", Direction: BridgeDirectionBoth},
		},
	})
	require.NoError(t, err)

	t.Run("reports bridge counts", func(t *testing.T) {
		m := manager.Metrics()

		assert.Equal(t, 2, m.TotalBridges)
		assert.Equal(t, 0, m.RunningBridges)
	})
}

func BenchmarkBridgeManagerAdd(b *testing.B) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		manager := NewBridgeManager(srv)
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			ClientID:   "bridge-" + string(rune('a'+i%26)),
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
		}
		manager.Add(config)
	}
}

func BenchmarkBridgeManagerGet(b *testing.B) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	for i := range 10 {
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			ClientID:   "bridge-" + string(rune('a'+i)),
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
		}
		manager.Add(config)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		manager.Get("bridge-e")
	}
}

func BenchmarkBridgeManagerList(b *testing.B) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	for i := range 10 {
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			ClientID:   "bridge-" + string(rune('a'+i)),
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
		}
		manager.Add(config)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		manager.List()
	}
}

func BenchmarkBridgeManagerForwardToRemote(b *testing.B) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	// Add multiple bridges with different topic prefixes
	prefixes := []string{"sensors", "alerts", "logs", "metrics", "events"}
	for i, prefix := range prefixes {
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			ClientID:   "bridge-" + string(rune('a'+i)),
			Topics: []BridgeTopic{
				{LocalPrefix: prefix, RemotePrefix: "remote/" + prefix, Direction: BridgeDirectionOut},
			},
		}
		manager.Add(config)
	}

	msg := &Message{
		Topic:   "sensors/temperature/room1",
		Payload: []byte("25.5"),
		QoS:     1,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		manager.ForwardToRemote(msg)
	}
}

func BenchmarkBridgeManagerForwardToRemoteParallel(b *testing.B) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	prefixes := []string{"sensors", "alerts", "logs", "metrics", "events"}
	for i, prefix := range prefixes {
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			ClientID:   "bridge-" + string(rune('a'+i)),
			Topics: []BridgeTopic{
				{LocalPrefix: prefix, RemotePrefix: "remote/" + prefix, Direction: BridgeDirectionOut},
			},
		}
		manager.Add(config)
	}

	msg := &Message{
		Topic:   "sensors/temperature/room1",
		Payload: []byte("25.5"),
		QoS:     1,
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			manager.ForwardToRemote(msg)
		}
	})
}

func TestBridgeManagerConcurrency(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))
	manager := NewBridgeManager(srv)

	var wg sync.WaitGroup

	// Concurrent reads during adds
	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			config := BridgeConfig{
				RemoteAddr: "tcp://localhost:1883",
				ClientID:   "bridge-" + string(rune('a'+idx)),
				Topics: []BridgeTopic{
					{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
				},
			}
			manager.Add(config)
		}(i)
	}

	// Concurrent reads
	for range 10 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			manager.List()
		}()
		go func() {
			defer wg.Done()
			manager.Count()
		}()
	}

	wg.Wait()

	// All should be added with unique IDs
	assert.Equal(t, 10, manager.Count())
}
