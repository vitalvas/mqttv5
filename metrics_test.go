package mqttv5

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNoOpMetrics(t *testing.T) {
	metrics := &NoOpMetrics{}

	t.Run("all operations are no-ops", func(_ *testing.T) {
		metrics.ConnectionOpened()
		metrics.ConnectionClosed()
		metrics.MessageReceived(0)
		metrics.MessageSent(1)
		metrics.BytesReceived(100)
		metrics.BytesSent(200)
		metrics.SubscriptionAdded()
		metrics.SubscriptionRemoved()
		metrics.RetainedMessageSet()
		metrics.RetainedMessageRemoved()
		metrics.PublishLatency(time.Millisecond)
		metrics.PacketReceived(PacketCONNECT)
		metrics.PacketSent(PacketCONNACK)
		metrics.BridgeForwardedToLocal()
		metrics.BridgeForwardedToRemote()
		metrics.BridgeDroppedLoop()
		metrics.BridgeError()
	})
}

func TestMemoryMetrics(t *testing.T) {
	t.Run("connection metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.ConnectionOpened()
		m.ConnectionOpened()
		m.ConnectionClosed()

		assert.Equal(t, int64(1), m.Connections())
		assert.Equal(t, int64(2), m.ConnectionsTotal())
	})

	t.Run("message metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.MessageReceived(0)
		m.MessageReceived(1)
		m.MessageReceived(1)
		m.MessageSent(2)

		assert.Equal(t, int64(1), m.MessagesReceived(0))
		assert.Equal(t, int64(2), m.MessagesReceived(1))
		assert.Equal(t, int64(1), m.MessagesSent(2))
	})

	t.Run("bytes metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.BytesReceived(100)
		m.BytesReceived(200)
		m.BytesSent(150)

		assert.Equal(t, int64(300), m.TotalBytesReceived())
		assert.Equal(t, int64(150), m.TotalBytesSent())
	})

	t.Run("subscription metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.SubscriptionAdded()
		m.SubscriptionAdded()
		m.SubscriptionRemoved()

		assert.Equal(t, int64(1), m.Subscriptions())
	})

	t.Run("retained message metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.RetainedMessageSet()
		m.RetainedMessageSet()
		m.RetainedMessageRemoved()

		assert.Equal(t, int64(1), m.RetainedMessages())
	})

	t.Run("latency metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.PublishLatency(10 * time.Millisecond)
		m.PublishLatency(20 * time.Millisecond)

		assert.Equal(t, int64(2), m.LatencyCount())
		assert.InDelta(t, 0.03, m.LatencySum(), 0.001)
	})

	t.Run("packet metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.PacketReceived(PacketCONNECT)
		m.PacketReceived(PacketPUBLISH)
		m.PacketReceived(PacketPUBLISH)
		m.PacketSent(PacketCONNACK)

		assert.Equal(t, int64(1), m.PacketsReceived(PacketCONNECT))
		assert.Equal(t, int64(2), m.PacketsReceived(PacketPUBLISH))
		assert.Equal(t, int64(1), m.PacketsSent(PacketCONNACK))
	})

	t.Run("qos bounds", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.MessageReceived(5) // should be capped to 2
		m.MessageSent(10)    // should be capped to 2

		assert.Equal(t, int64(1), m.MessagesReceived(2))
		assert.Equal(t, int64(1), m.MessagesSent(2))

		// Test getter bounds
		assert.Equal(t, int64(1), m.MessagesReceived(5)) // should cap to qos 2
		assert.Equal(t, int64(1), m.MessagesSent(10))    // should cap to qos 2
	})

	t.Run("packet metrics non-existent type", func(t *testing.T) {
		m := NewMemoryMetrics()

		// Query for packet types that were never recorded
		assert.Equal(t, int64(0), m.PacketsReceived(PacketDISCONNECT))
		assert.Equal(t, int64(0), m.PacketsSent(PacketAUTH))
	})

	t.Run("bridge metrics", func(t *testing.T) {
		m := NewMemoryMetrics()

		m.BridgeForwardedToLocal()
		m.BridgeForwardedToLocal()
		m.BridgeForwardedToRemote()
		m.BridgeDroppedLoop()
		m.BridgeError()
		m.BridgeError()
		m.BridgeError()

		assert.Equal(t, int64(2), m.BridgeForwardedToLocalTotal())
		assert.Equal(t, int64(1), m.BridgeForwardedToRemoteTotal())
		assert.Equal(t, int64(1), m.BridgeDroppedLoopTotal())
		assert.Equal(t, int64(3), m.BridgeErrorsTotal())
	})
}

func TestMetricsCollectorInterface(t *testing.T) {
	t.Run("NoOpMetrics implements MetricsCollector", func(_ *testing.T) {
		var _ MetricsCollector = &NoOpMetrics{}
	})

	t.Run("MemoryMetrics implements MetricsCollector", func(_ *testing.T) {
		var _ MetricsCollector = NewMemoryMetrics()
	})
}

func BenchmarkNoOpMetrics(b *testing.B) {
	metrics := &NoOpMetrics{}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		metrics.ConnectionOpened()
	}
}

func BenchmarkMemoryMetricsConnectionOpened(b *testing.B) {
	m := NewMemoryMetrics()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		m.ConnectionOpened()
	}
}

func BenchmarkMemoryMetricsMessageReceived(b *testing.B) {
	m := NewMemoryMetrics()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		m.MessageReceived(1)
	}
}

func TestExpvarMetrics(t *testing.T) {
	// Note: expvar counters persist across tests and NewMetrics panics on reuse
	// So we create the Metrics instance once and test relative changes
	m := NewMetrics()

	t.Run("connection metrics", func(t *testing.T) {
		initialConns := m.Connections()
		initialTotal := m.ConnectionsTotal()

		m.ConnectionOpened()
		assert.Equal(t, initialConns+1, m.Connections())
		assert.Equal(t, initialTotal+1, m.ConnectionsTotal())

		m.ConnectionClosed()
		assert.Equal(t, initialConns, m.Connections())
		assert.Equal(t, initialTotal+1, m.ConnectionsTotal())
	})

	t.Run("message metrics", func(t *testing.T) {
		initialQoS0 := m.messagesReceived[0].Value()
		initialQoS1Sent := m.messagesSent[1].Value()

		m.MessageReceived(0)
		m.MessageSent(1)

		assert.Equal(t, initialQoS0+1, m.messagesReceived[0].Value())
		assert.Equal(t, initialQoS1Sent+1, m.messagesSent[1].Value())
	})

	t.Run("bytes metrics", func(t *testing.T) {
		initialRecv := m.bytesReceived.Value()
		initialSent := m.bytesSent.Value()

		m.BytesReceived(100)
		m.BytesSent(200)

		assert.Equal(t, initialRecv+100, m.bytesReceived.Value())
		assert.Equal(t, initialSent+200, m.bytesSent.Value())
	})

	t.Run("subscription metrics", func(t *testing.T) {
		initial := m.Subscriptions()

		m.SubscriptionAdded()
		assert.Equal(t, initial+1, m.Subscriptions())

		m.SubscriptionRemoved()
		assert.Equal(t, initial, m.Subscriptions())
	})

	t.Run("retained message metrics", func(t *testing.T) {
		initial := m.RetainedMessages()

		m.RetainedMessageSet()
		assert.Equal(t, initial+1, m.RetainedMessages())

		m.RetainedMessageRemoved()
		assert.Equal(t, initial, m.RetainedMessages())
	})

	t.Run("latency metrics", func(t *testing.T) {
		m.PublishLatency(10 * time.Millisecond)

		// latencyCount and latencySum should have been updated
		assert.Greater(t, m.latencyCount.Value(), int64(0))
	})

	t.Run("packet metrics", func(t *testing.T) {
		m.PacketReceived(PacketCONNECT)
		m.PacketSent(PacketCONNACK)

		// Counters should be created and incremented
		assert.NotNil(t, m.packetsReceived[PacketCONNECT])
		assert.NotNil(t, m.packetsSent[PacketCONNACK])
	})

	t.Run("bridge metrics", func(t *testing.T) {
		initialLocal := m.bridgeForwardedToLocal.Value()
		initialRemote := m.bridgeForwardedToRemote.Value()
		initialLoop := m.bridgeDroppedLoop.Value()
		initialErrors := m.bridgeErrors.Value()

		m.BridgeForwardedToLocal()
		m.BridgeForwardedToRemote()
		m.BridgeDroppedLoop()
		m.BridgeError()

		assert.Equal(t, initialLocal+1, m.bridgeForwardedToLocal.Value())
		assert.Equal(t, initialRemote+1, m.bridgeForwardedToRemote.Value())
		assert.Equal(t, initialLoop+1, m.bridgeDroppedLoop.Value())
		assert.Equal(t, initialErrors+1, m.bridgeErrors.Value())
	})

	t.Run("qos bounds", func(t *testing.T) {
		initialQoS2Recv := m.messagesReceived[2].Value()
		initialQoS2Sent := m.messagesSent[2].Value()

		m.MessageReceived(5) // should be capped to 2
		m.MessageSent(10)    // should be capped to 2

		assert.Equal(t, initialQoS2Recv+1, m.messagesReceived[2].Value())
		assert.Equal(t, initialQoS2Sent+1, m.messagesSent[2].Value())
	})
}
