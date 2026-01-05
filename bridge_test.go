package mqttv5

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBridge(t *testing.T) {
	t.Run("creates bridge with valid config", func(t *testing.T) {
		listener, _ := net.Listen("tcp", "127.0.0.1:0")
		defer listener.Close()

		srv := NewServer(WithListener(listener))

		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			ClientID:   "test-bridge",
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth, QoS: 1},
			},
		}

		bridge, err := NewBridge(srv, config)
		require.NoError(t, err)
		assert.NotNil(t, bridge)
		assert.Equal(t, "test-bridge", bridge.ID())
	})

	t.Run("generates client ID if not provided", func(t *testing.T) {
		listener, _ := net.Listen("tcp", "127.0.0.1:0")
		defer listener.Close()

		srv := NewServer(WithListener(listener))

		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
		}

		bridge, err := NewBridge(srv, config)
		require.NoError(t, err)
		assert.Contains(t, bridge.ID(), "bridge-")
	})

	t.Run("fails without topics", func(t *testing.T) {
		listener, _ := net.Listen("tcp", "127.0.0.1:0")
		defer listener.Close()

		srv := NewServer(WithListener(listener))

		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			Topics:     []BridgeTopic{},
		}

		bridge, err := NewBridge(srv, config)
		assert.ErrorIs(t, err, ErrBridgeNoTopics)
		assert.Nil(t, bridge)
	})
}

func TestBridgeTopicRemap(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	bridge, err := NewBridge(srv, config)
	require.NoError(t, err)

	tests := []struct {
		name       string
		topic      string
		fromPrefix string // pre-cleaned prefix (no trailing wildcards)
		toPrefix   string // pre-cleaned prefix (no trailing wildcards)
		expected   string
	}{
		{
			name:       "simple remap",
			topic:      "local/sensors/temp",
			fromPrefix: "local",
			toPrefix:   "remote",
			expected:   "remote/sensors/temp",
		},
		{
			name:       "remap with cleaned wildcard prefix",
			topic:      "local/sensors/temp",
			fromPrefix: "local",  // cleaned from "local/#"
			toPrefix:   "remote", // cleaned from "remote/#"
			expected:   "remote/sensors/temp",
		},
		{
			name:       "no match keeps original",
			topic:      "other/sensors/temp",
			fromPrefix: "local",
			toPrefix:   "remote",
			expected:   "other/sensors/temp",
		},
		{
			name:       "exact topic match",
			topic:      "local",
			fromPrefix: "local",
			toPrefix:   "remote",
			expected:   "remote",
		},
		{
			name:       "empty prefix matches all",
			topic:      "any/topic/here",
			fromPrefix: "",
			toPrefix:   "prefix",
			expected:   "prefix/any/topic/here",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bridge.remapTopic(tt.topic, tt.fromPrefix, tt.toPrefix, BridgeDirectionIn)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBridgeCustomTopicRemapper(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	t.Run("custom remapper overrides default", func(t *testing.T) {
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
			TopicRemapper: func(topic string, direction BridgeDirection) string {
				if topic == "local/sensors/temp" && direction == BridgeDirectionOut {
					return "custom/temperature"
				}
				return "" // fall back to default
			},
		}

		bridge, err := NewBridge(srv, config)
		require.NoError(t, err)

		// Custom remapper should be used
		result := bridge.remapTopic("local/sensors/temp", "local", "remote", BridgeDirectionOut)
		assert.Equal(t, "custom/temperature", result)

		// Different direction falls back to default
		result = bridge.remapTopic("local/sensors/temp", "local", "remote", BridgeDirectionIn)
		assert.Equal(t, "remote/sensors/temp", result)
	})

	t.Run("custom remapper returns empty falls back to default", func(t *testing.T) {
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
			TopicRemapper: func(_ string, _ BridgeDirection) string {
				return "" // always fall back to default
			},
		}

		bridge, err := NewBridge(srv, config)
		require.NoError(t, err)

		result := bridge.remapTopic("local/sensors/temp", "local", "remote", BridgeDirectionIn)
		assert.Equal(t, "remote/sensors/temp", result)
	})

	t.Run("no remapper uses default", func(t *testing.T) {
		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
		}

		bridge, err := NewBridge(srv, config)
		require.NoError(t, err)

		result := bridge.remapTopic("local/sensors/temp", "local", "remote", BridgeDirectionOut)
		assert.Equal(t, "remote/sensors/temp", result)
	})
}

func TestBridgeTopicMatchesPrefix(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	bridge, err := NewBridge(srv, config)
	require.NoError(t, err)

	tests := []struct {
		name     string
		topic    string
		prefix   string // pre-cleaned prefix (no trailing wildcards)
		expected bool
	}{
		{"matches prefix", "local/sensors/temp", "local", true},
		{"matches cleaned wildcard prefix", "local/sensors/temp", "local", true}, // cleaned from "local/#"
		{"exact match", "local", "local", true},
		{"no match", "other/sensors/temp", "local", false},
		{"empty prefix matches all", "any/topic", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bridge.topicMatchesPrefix(tt.topic, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBridgeLoopDetection(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		ClientID:   "test-bridge",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	bridge, err := NewBridge(srv, config)
	require.NoError(t, err)

	t.Run("detects message from same bridge", func(t *testing.T) {
		msg := &Message{
			Topic:   "test/topic",
			Payload: []byte("test"),
			UserProperties: []StringPair{
				{Key: bridgePropertyKey, Value: "test-bridge"},
			},
		}
		assert.True(t, bridge.isFromBridge(msg))
	})

	t.Run("allows message from different bridge", func(t *testing.T) {
		msg := &Message{
			Topic:   "test/topic",
			Payload: []byte("test"),
			UserProperties: []StringPair{
				{Key: bridgePropertyKey, Value: "other-bridge"},
			},
		}
		assert.False(t, bridge.isFromBridge(msg))
	})

	t.Run("allows message without bridge property", func(t *testing.T) {
		msg := &Message{
			Topic:   "test/topic",
			Payload: []byte("test"),
		}
		assert.False(t, bridge.isFromBridge(msg))
	})
}

func TestBridgeAddBridgeProperty(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		ClientID:   "test-bridge",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	bridge, err := NewBridge(srv, config)
	require.NoError(t, err)

	t.Run("adds bridge property to empty list", func(t *testing.T) {
		result := bridge.addBridgeProperty(nil)
		require.Len(t, result, 1)
		assert.Equal(t, bridgePropertyKey, result[0].Key)
		assert.Equal(t, "test-bridge", result[0].Value)
	})

	t.Run("appends bridge property to existing list", func(t *testing.T) {
		existing := []StringPair{{Key: "foo", Value: "bar"}}
		result := bridge.addBridgeProperty(existing)
		require.Len(t, result, 2)
		assert.Equal(t, "foo", result[0].Key)
		assert.Equal(t, bridgePropertyKey, result[1].Key)
	})
}

func TestBridgeStartStop(t *testing.T) {
	t.Run("stop without start returns error", func(t *testing.T) {
		listener, _ := net.Listen("tcp", "127.0.0.1:0")
		defer listener.Close()

		srv := NewServer(WithListener(listener))

		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
		}

		bridge, err := NewBridge(srv, config)
		require.NoError(t, err)

		err = bridge.Stop()
		assert.ErrorIs(t, err, ErrBridgeNotRunning)
	})

	t.Run("is not running initially", func(t *testing.T) {
		listener, _ := net.Listen("tcp", "127.0.0.1:0")
		defer listener.Close()

		srv := NewServer(WithListener(listener))

		config := BridgeConfig{
			RemoteAddr: "tcp://localhost:1883",
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
			},
		}

		bridge, err := NewBridge(srv, config)
		require.NoError(t, err)
		assert.False(t, bridge.IsRunning())
	})
}

func BenchmarkBridgeRemapTopic(b *testing.B) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	bridge, _ := NewBridge(srv, config)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bridge.remapTopic("local/sensors/temperature/room1", "local", "remote", BridgeDirectionOut)
	}
}

func BenchmarkBridgeRemapTopicWithCustomRemapper(b *testing.B) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
		TopicRemapper: func(topic string, direction BridgeDirection) string {
			if direction == BridgeDirectionOut {
				return "custom/" + topic
			}
			return ""
		},
	}

	bridge, _ := NewBridge(srv, config)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bridge.remapTopic("local/sensors/temperature/room1", "local", "remote", BridgeDirectionOut)
	}
}

func BenchmarkBridgeTopicMatchesPrefix(b *testing.B) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	bridge, _ := NewBridge(srv, config)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bridge.topicMatchesPrefix("local/sensors/temperature/room1", "local")
	}
}

func BenchmarkBridgeIsFromBridge(b *testing.B) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		ClientID:   "test-bridge",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	bridge, _ := NewBridge(srv, config)

	msg := &Message{
		Topic:   "test/topic",
		Payload: []byte("payload"),
		UserProperties: []StringPair{
			{Key: "other-key", Value: "other-value"},
			{Key: bridgePropertyKey, Value: "other-bridge"},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bridge.isFromBridge(msg)
	}
}

func BenchmarkBridgeAddBridgeProperty(b *testing.B) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		ClientID:   "test-bridge",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	bridge, _ := NewBridge(srv, config)

	props := []StringPair{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bridge.addBridgeProperty(props)
	}
}

func TestBridgeIntegration(t *testing.T) {
	t.Run("forwards messages between brokers", func(t *testing.T) {
		// Start remote broker
		remoteListener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer remoteListener.Close()

		remoteServer := NewServer(WithListener(remoteListener))
		go remoteServer.ListenAndServe()
		defer remoteServer.Close()

		// Start local broker
		localListener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer localListener.Close()

		localServer := NewServer(WithListener(localListener))
		go localServer.ListenAndServe()
		defer localServer.Close()

		time.Sleep(50 * time.Millisecond)

		// Connect a subscriber to local broker to receive forwarded messages
		var receivedMsg *Message
		var mu sync.Mutex
		localSubscriber, err := Dial(WithServers("tcp://"+localListener.Addr().String()),
			WithClientID("local-subscriber"),
		)
		require.NoError(t, err)
		defer localSubscriber.Close()

		err = localSubscriber.Subscribe("local/#", 1, func(msg *Message) {
			mu.Lock()
			receivedMsg = msg
			mu.Unlock()
		})
		require.NoError(t, err)

		time.Sleep(50 * time.Millisecond)

		// Create and start bridge
		config := BridgeConfig{
			RemoteAddr: "tcp://" + remoteListener.Addr().String(),
			ClientID:   "test-bridge",
			Topics: []BridgeTopic{
				{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionIn, QoS: 1},
			},
		}

		bridge, err := NewBridge(localServer, config)
		require.NoError(t, err)

		err = bridge.Start()
		require.NoError(t, err)
		defer bridge.Stop()

		assert.True(t, bridge.IsRunning())

		time.Sleep(50 * time.Millisecond)

		// Publish to remote broker
		remoteClient, err := Dial(WithServers("tcp://"+remoteListener.Addr().String()),
			WithClientID("remote-publisher"),
		)
		require.NoError(t, err)
		defer remoteClient.Close()

		err = remoteClient.Publish(&Message{
			Topic:   "remote/sensors/temp",
			Payload: []byte("25.5"),
			QoS:     1,
		})
		require.NoError(t, err)

		// Wait for message to be forwarded
		time.Sleep(200 * time.Millisecond)

		// Verify message was forwarded to local
		mu.Lock()
		defer mu.Unlock()
		require.NotNil(t, receivedMsg, "message should be forwarded from remote to local")
		assert.Equal(t, "local/sensors/temp", receivedMsg.Topic)
		assert.Equal(t, []byte("25.5"), receivedMsg.Payload)
	})
}

func TestBridgeHandleClientEvent(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	srv := NewServer(WithListener(listener))

	config := BridgeConfig{
		RemoteAddr: "tcp://localhost:1883",
		ClientID:   "test-bridge",
		Topics: []BridgeTopic{
			{LocalPrefix: "local", RemotePrefix: "remote", Direction: BridgeDirectionBoth},
		},
	}

	bridge, err := NewBridge(srv, config)
	require.NoError(t, err)

	t.Run("nil event is no-op", func(_ *testing.T) {
		// Should not panic
		bridge.handleClientEvent(nil)
	})

	t.Run("connection lost error", func(_ *testing.T) {
		event := &ConnectionLostError{Cause: assert.AnError}
		// Should not panic
		bridge.handleClientEvent(event)
	})

	t.Run("disconnect error", func(_ *testing.T) {
		event := &DisconnectError{ReasonCode: ReasonServerShuttingDown}
		// Should not panic
		bridge.handleClientEvent(event)
	})
}
