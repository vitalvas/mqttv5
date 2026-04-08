package mqttv5

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimitConfig(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		assert.False(t, RateLimitConfig{}.enabled())
		assert.False(t, RateLimitConfig{Rate: 0, Burst: 5}.enabled())
		assert.True(t, RateLimitConfig{Rate: 10}.enabled())
		assert.True(t, RateLimitConfig{Rate: 10, Burst: 5}.enabled())
	})

	t.Run("burst defaults to 1", func(t *testing.T) {
		assert.Equal(t, 1, RateLimitConfig{Rate: 10}.burst())
		assert.Equal(t, 1, RateLimitConfig{Rate: 10, Burst: 0}.burst())
	})

	t.Run("burst uses explicit value", func(t *testing.T) {
		assert.Equal(t, 5, RateLimitConfig{Rate: 10, Burst: 5}.burst())
	})
}

func TestRateLimitExceedAction(t *testing.T) {
	t.Run("constants are distinct", func(t *testing.T) {
		assert.NotEqual(t, RateLimitAllow, RateLimitDisconnect)
		assert.NotEqual(t, RateLimitAllow, RateLimitDropMessage)
		assert.NotEqual(t, RateLimitDisconnect, RateLimitDropMessage)
	})

	t.Run("zero value is RateLimitAllow", func(t *testing.T) {
		var action RateLimitExceedAction
		assert.Equal(t, RateLimitAllow, action)
	})
}

func TestTokenBucketConnectionLimiter(t *testing.T) {
	t.Run("global only", func(t *testing.T) {
		limiter := NewTokenBucketConnectionLimiter(ConnectionLimiterConfig{
			Global: RateLimitConfig{Rate: 1, Burst: 2},
		})
		conn := newRateLimitMockConn("192.168.1.1", 1234)

		assert.True(t, limiter.AllowConnection(conn))
		assert.True(t, limiter.AllowConnection(conn))
		assert.False(t, limiter.AllowConnection(conn))
	})

	t.Run("per-IP only", func(t *testing.T) {
		limiter := NewTokenBucketConnectionLimiter(ConnectionLimiterConfig{
			PerIP: RateLimitConfig{Rate: 1, Burst: 1},
		})
		conn1 := newRateLimitMockConn("192.168.1.1", 1234)
		conn2 := newRateLimitMockConn("192.168.1.2", 1234)

		assert.True(t, limiter.AllowConnection(conn1))
		assert.True(t, limiter.AllowConnection(conn2))
		assert.False(t, limiter.AllowConnection(conn1))
		assert.False(t, limiter.AllowConnection(conn2))
	})

	t.Run("both global and per-IP", func(t *testing.T) {
		limiter := NewTokenBucketConnectionLimiter(ConnectionLimiterConfig{
			Global: RateLimitConfig{Rate: 1, Burst: 3},
			PerIP:  RateLimitConfig{Rate: 1, Burst: 1},
		})
		conn := newRateLimitMockConn("192.168.1.1", 1234)

		assert.True(t, limiter.AllowConnection(conn))
		assert.False(t, limiter.AllowConnection(conn))
	})

	t.Run("IP extraction from TCP addr", func(t *testing.T) {
		limiter := NewTokenBucketConnectionLimiter(ConnectionLimiterConfig{
			PerIP: RateLimitConfig{Rate: 1, Burst: 1},
		})
		conn1 := newRateLimitMockConn("10.0.0.1", 5000)
		conn2 := newRateLimitMockConn("10.0.0.1", 6000)

		assert.True(t, limiter.AllowConnection(conn1))
		assert.False(t, limiter.AllowConnection(conn2))
	})

	t.Run("non-TCP addr uses string representation", func(t *testing.T) {
		limiter := NewTokenBucketConnectionLimiter(ConnectionLimiterConfig{
			PerIP: RateLimitConfig{Rate: 1, Burst: 1},
		})
		conn := &rateLimitMockConn{
			remoteAddr: &mockAddr{network: "unix", addr: "/tmp/test.sock"},
		}

		assert.True(t, limiter.AllowConnection(conn))
		assert.False(t, limiter.AllowConnection(conn))
	})

	t.Run("no limits configured", func(t *testing.T) {
		limiter := NewTokenBucketConnectionLimiter(ConnectionLimiterConfig{})
		conn := newRateLimitMockConn("192.168.1.1", 1234)

		for range 100 {
			assert.True(t, limiter.AllowConnection(conn))
		}
	})

	t.Run("no burst", func(t *testing.T) {
		limiter := NewTokenBucketConnectionLimiter(ConnectionLimiterConfig{
			Global: RateLimitConfig{Rate: 1},
		})
		conn := newRateLimitMockConn("192.168.1.1", 1234)

		// Burst defaults to 1, so only one token available
		assert.True(t, limiter.AllowConnection(conn))
		assert.False(t, limiter.AllowConnection(conn))
	})

	t.Run("concurrent access", func(_ *testing.T) {
		limiter := NewTokenBucketConnectionLimiter(ConnectionLimiterConfig{
			Global: RateLimitConfig{Rate: 1000, Burst: 1000},
			PerIP:  RateLimitConfig{Rate: 100, Burst: 100},
		})

		var wg sync.WaitGroup
		for i := range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn := newRateLimitMockConn("192.168.1.1", 5000+i)
				for range 50 {
					limiter.AllowConnection(conn)
				}
			}()
		}
		wg.Wait()
	})
}

func TestTokenBucketMessageLimiter(t *testing.T) {
	t.Run("global only", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			Global:       RateLimitConfig{Rate: 1, Burst: 2},
			ExceedAction: RateLimitDisconnect,
		})

		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "t"))
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client2", "default", "t"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client3", "default", "t"))
	})

	t.Run("per-client only", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerClient:    RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})

		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "t"))
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client2", "default", "t"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "default", "t"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client2", "default", "t"))
	})

	t.Run("both global and per-client", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			Global:       RateLimitConfig{Rate: 1, Burst: 10},
			PerClient:    RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})

		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "t"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "default", "t"))
	})

	t.Run("per-namespace only", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerNamespace: RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})

		// First message in ns1 — allowed
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "ns1", "t"))
		// Same namespace, different client — shared limiter, rate limited
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client2", "ns1", "t"))
		// Different namespace — independent
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "ns2", "t"))
	})

	t.Run("namespace isolation", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerClient:    RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})

		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "ns1", "t"))
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "ns2", "t"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "ns1", "t"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "ns2", "t"))
	})

	t.Run("per-topic only", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerTopic:     RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})

		// First message on topic — allowed
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "sensor/temp"))
		// Same topic, same client — rate limited
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "default", "sensor/temp"))
		// Different topic — independent
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "sensor/humidity"))
		// Different client, same topic — shared limiter, still rate limited
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client2", "default", "sensor/temp"))
	})

	t.Run("per-topic namespace isolation", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerTopic:     RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})

		// Same client+topic in different namespaces is independent
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "ns1", "sensor/temp"))
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "ns2", "sensor/temp"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "ns1", "sensor/temp"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "ns2", "sensor/temp"))
	})

	t.Run("per-client and per-topic combined", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerClient:    RateLimitConfig{Rate: 1, Burst: 10},
			PerTopic:     RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})

		// Per-topic is stricter
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "t"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "default", "t"))
	})

	t.Run("per-client-topic only", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerClientTopic: RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction:   RateLimitDisconnect,
		})

		// client1 + topic1 — allowed
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "sensor/temp"))
		// client1 + topic1 again — rate limited
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "default", "sensor/temp"))
		// client1 + different topic — independent
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "sensor/humidity"))
		// different client + same topic — independent (unlike PerTopic)
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client2", "default", "sensor/temp"))
	})

	t.Run("per-client-topic namespace isolation", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerClientTopic: RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction:   RateLimitDisconnect,
		})

		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "ns1", "sensor/temp"))
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "ns2", "sensor/temp"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "ns1", "sensor/temp"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "ns2", "sensor/temp"))
	})

	t.Run("per-topic and per-client-topic combined", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerTopic:       RateLimitConfig{Rate: 1, Burst: 10},
			PerClientTopic: RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction:   RateLimitDisconnect,
		})

		// Per-client-topic is stricter
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "t"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "default", "t"))
		// Different client same topic — allowed (per-topic has burst 10)
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client2", "default", "t"))
	})

	t.Run("exceed action drop message", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			Global:       RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDropMessage,
		})

		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "t"))
		assert.Equal(t, RateLimitDropMessage, limiter.AllowMessage("client1", "default", "t"))
	})

	t.Run("no limits configured", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			ExceedAction: RateLimitDisconnect,
		})

		for range 100 {
			assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "t"))
		}
	})

	t.Run("no burst", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerClient:    RateLimitConfig{Rate: 1},
			ExceedAction: RateLimitDisconnect,
		})

		// Burst defaults to 1, so only one token available
		assert.Equal(t, RateLimitAllow, limiter.AllowMessage("client1", "default", "t"))
		assert.Equal(t, RateLimitDisconnect, limiter.AllowMessage("client1", "default", "t"))
	})

	t.Run("concurrent access", func(_ *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			Global:       RateLimitConfig{Rate: 1000, Burst: 1000},
			PerClient:    RateLimitConfig{Rate: 100, Burst: 100},
			ExceedAction: RateLimitDisconnect,
		})

		var wg sync.WaitGroup
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 50 {
					limiter.AllowMessage("client1", "default", "t")
				}
			}()
		}
		wg.Wait()
	})
}

func TestConnectionRateLimiterCleanup(t *testing.T) {
	t.Run("removes stale entries", func(t *testing.T) {
		limiter := NewTokenBucketConnectionLimiter(ConnectionLimiterConfig{
			PerIP: RateLimitConfig{Rate: 1, Burst: 1},
		})
		conn := newRateLimitMockConn("192.168.1.1", 1234)
		limiter.AllowConnection(conn)

		limiter.mu.Lock()
		require.Len(t, limiter.ipLimiters, 1)
		limiter.ipLimiters["192.168.1.1"].lastSeen = time.Now().Add(-2 * time.Hour)
		limiter.mu.Unlock()

		limiter.Cleanup(1 * time.Hour)

		limiter.mu.Lock()
		assert.Empty(t, limiter.ipLimiters)
		limiter.mu.Unlock()
	})

	t.Run("keeps recent entries", func(t *testing.T) {
		limiter := NewTokenBucketConnectionLimiter(ConnectionLimiterConfig{
			PerIP: RateLimitConfig{Rate: 1, Burst: 1},
		})
		conn := newRateLimitMockConn("192.168.1.1", 1234)
		limiter.AllowConnection(conn)

		limiter.Cleanup(1 * time.Hour)

		limiter.mu.Lock()
		assert.Len(t, limiter.ipLimiters, 1)
		limiter.mu.Unlock()
	})
}

func TestMessageRateLimiterCleanup(t *testing.T) {
	t.Run("removes stale entries", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerClient:    RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})
		limiter.AllowMessage("client1", "default", "t")

		key := NamespaceKey("default", "client1")
		limiter.mu.Lock()
		require.Len(t, limiter.clientLimiters, 1)
		limiter.clientLimiters[key].lastSeen = time.Now().Add(-2 * time.Hour)
		limiter.mu.Unlock()

		limiter.Cleanup(1 * time.Hour)

		limiter.mu.Lock()
		assert.Empty(t, limiter.clientLimiters)
		limiter.mu.Unlock()
	})

	t.Run("keeps recent entries", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerClient:    RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})
		limiter.AllowMessage("client1", "default", "t")

		limiter.Cleanup(1 * time.Hour)

		limiter.mu.Lock()
		assert.Len(t, limiter.clientLimiters, 1)
		limiter.mu.Unlock()
	})

	t.Run("removes stale topic entries", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerTopic:     RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})
		limiter.AllowMessage("client1", "default", "sensor/temp")

		key := NamespaceKey("default", "sensor/temp")
		limiter.mu.Lock()
		require.Len(t, limiter.topicLimiters, 1)
		limiter.topicLimiters[key].lastSeen = time.Now().Add(-2 * time.Hour)
		limiter.mu.Unlock()

		limiter.Cleanup(1 * time.Hour)

		limiter.mu.Lock()
		assert.Empty(t, limiter.topicLimiters)
		limiter.mu.Unlock()
	})

	t.Run("removes stale namespace entries", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerNamespace: RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction: RateLimitDisconnect,
		})
		limiter.AllowMessage("client1", "default", "t")

		limiter.mu.Lock()
		require.Len(t, limiter.namespaceLimiters, 1)
		limiter.namespaceLimiters["default"].lastSeen = time.Now().Add(-2 * time.Hour)
		limiter.mu.Unlock()

		limiter.Cleanup(1 * time.Hour)

		limiter.mu.Lock()
		assert.Empty(t, limiter.namespaceLimiters)
		limiter.mu.Unlock()
	})

	t.Run("removes stale client-topic entries", func(t *testing.T) {
		limiter := NewTokenBucketMessageLimiter(MessageLimiterConfig{
			PerClientTopic: RateLimitConfig{Rate: 1, Burst: 1},
			ExceedAction:   RateLimitDisconnect,
		})
		limiter.AllowMessage("client1", "default", "sensor/temp")

		key := fmt.Sprintf("default%sclient1%ssensor/temp", namespaceDelimiter, namespaceDelimiter)
		limiter.mu.Lock()
		require.Len(t, limiter.clientTopicLimiters, 1)
		limiter.clientTopicLimiters[key].lastSeen = time.Now().Add(-2 * time.Hour)
		limiter.mu.Unlock()

		limiter.Cleanup(1 * time.Hour)

		limiter.mu.Lock()
		assert.Empty(t, limiter.clientTopicLimiters)
		limiter.mu.Unlock()
	})
}

func TestConnectionRateLimiterInterface(_ *testing.T) {
	var _ ConnectionRateLimiter = (*TokenBucketConnectionLimiter)(nil)
}

func TestMessageRateLimiterInterface(_ *testing.T) {
	var _ MessageRateLimiter = (*TokenBucketMessageLimiter)(nil)
}

type mockAddr struct {
	network string
	addr    string
}

func (a *mockAddr) Network() string { return a.network }
func (a *mockAddr) String() string  { return a.addr }

type rateLimitMockConn struct {
	net.Conn
	remoteAddr net.Addr
}

func (c *rateLimitMockConn) RemoteAddr() net.Addr { return c.remoteAddr }

func newRateLimitMockConn(ip string, port int) *rateLimitMockConn {
	return &rateLimitMockConn{
		remoteAddr: &net.TCPAddr{IP: net.ParseIP(ip), Port: port},
	}
}
