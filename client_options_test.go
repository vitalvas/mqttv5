package mqttv5

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultOptions(t *testing.T) {
	opts := defaultOptions()

	assert.Equal(t, uint16(60), opts.keepAlive)
	assert.True(t, opts.cleanStart)
	assert.Equal(t, 10*time.Second, opts.connectTimeout)
	assert.Equal(t, 5*time.Second, opts.writeTimeout)
	assert.Equal(t, 5*time.Second, opts.readTimeout)
	assert.False(t, opts.autoReconnect)
	assert.Equal(t, 10, opts.maxReconnects)
	assert.Equal(t, 1*time.Second, opts.reconnectBackoff)
	assert.Equal(t, 60*time.Second, opts.maxBackoff)
	assert.Equal(t, uint32(MaxPacketSizeDefault), opts.maxPacketSize)
	assert.Equal(t, uint16(65535), opts.receiveMaximum)
}

func TestWithClientID(t *testing.T) {
	opts := applyOptions(WithClientID("test-client"))
	assert.Equal(t, "test-client", opts.clientID)
}

func TestWithCredentials(t *testing.T) {
	opts := applyOptions(WithCredentials("user", "pass"))
	assert.Equal(t, "user", opts.username)
	assert.Equal(t, []byte("pass"), opts.password)
}

func TestWithKeepAlive(t *testing.T) {
	opts := applyOptions(WithKeepAlive(30))
	assert.Equal(t, uint16(30), opts.keepAlive)
}

func TestWithCleanStart(t *testing.T) {
	t.Run("set to false", func(t *testing.T) {
		opts := applyOptions(WithCleanStart(false))
		assert.False(t, opts.cleanStart)
	})

	t.Run("set to true", func(t *testing.T) {
		opts := applyOptions(WithCleanStart(true))
		assert.True(t, opts.cleanStart)
	})
}

func TestWithTLS(t *testing.T) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	opts := applyOptions(WithTLS(tlsConfig))
	assert.Equal(t, tlsConfig, opts.tlsConfig)
}

func TestWithConnectTimeout(t *testing.T) {
	opts := applyOptions(WithConnectTimeout(30 * time.Second))
	assert.Equal(t, 30*time.Second, opts.connectTimeout)
}

func TestWithWriteTimeout(t *testing.T) {
	opts := applyOptions(WithWriteTimeout(10 * time.Second))
	assert.Equal(t, 10*time.Second, opts.writeTimeout)
}

func TestWithReadTimeout(t *testing.T) {
	opts := applyOptions(WithReadTimeout(15 * time.Second))
	assert.Equal(t, 15*time.Second, opts.readTimeout)
}

func TestWithAutoReconnect(t *testing.T) {
	opts := applyOptions(WithAutoReconnect(true))
	assert.True(t, opts.autoReconnect)
}

func TestWithMaxReconnects(t *testing.T) {
	t.Run("positive value", func(t *testing.T) {
		opts := applyOptions(WithMaxReconnects(5))
		assert.Equal(t, 5, opts.maxReconnects)
	})

	t.Run("unlimited (-1)", func(t *testing.T) {
		opts := applyOptions(WithMaxReconnects(-1))
		assert.Equal(t, -1, opts.maxReconnects)
	})
}

func TestWithReconnectBackoff(t *testing.T) {
	opts := applyOptions(WithReconnectBackoff(5 * time.Second))
	assert.Equal(t, 5*time.Second, opts.reconnectBackoff)
}

func TestWithMaxBackoff(t *testing.T) {
	opts := applyOptions(WithMaxBackoff(120 * time.Second))
	assert.Equal(t, 120*time.Second, opts.maxBackoff)
}

func TestWithWill(t *testing.T) {
	opts := applyOptions(WithWill("status/client", []byte("offline"), true, 1))
	assert.Equal(t, "status/client", opts.willTopic)
	assert.Equal(t, []byte("offline"), opts.willPayload)
	assert.True(t, opts.willRetain)
	assert.Equal(t, byte(1), opts.willQoS)
}

func TestWithWillProps(t *testing.T) {
	props := &Properties{}
	opts := applyOptions(WithWillProps(props))
	assert.Equal(t, props, opts.willProps)
}

func TestWithMaxPacketSize(t *testing.T) {
	t.Run("set value", func(t *testing.T) {
		opts := applyOptions(WithMaxPacketSize(1024 * 1024))
		assert.Equal(t, uint32(1024*1024), opts.maxPacketSize)
	})

	t.Run("exceeding protocol limit is clamped", func(t *testing.T) {
		opts := applyOptions(WithMaxPacketSize(MaxPacketSizeProtocol + 1000))
		assert.Equal(t, uint32(MaxPacketSizeProtocol), opts.maxPacketSize)
	})
}

func TestWithSessionExpiryInterval(t *testing.T) {
	opts := applyOptions(WithSessionExpiryInterval(3600))
	assert.Equal(t, uint32(3600), opts.sessionExpiryInterval)
}

func TestWithReceiveMaximum(t *testing.T) {
	opts := applyOptions(WithReceiveMaximum(100))
	assert.Equal(t, uint16(100), opts.receiveMaximum)
}

func TestWithTopicAliasMaximum(t *testing.T) {
	opts := applyOptions(WithTopicAliasMaximum(10))
	assert.Equal(t, uint16(10), opts.topicAliasMaximum)
}

func TestWithUserProperties(t *testing.T) {
	props := map[string]string{"key1": "value1", "key2": "value2"}
	opts := applyOptions(WithUserProperties(props))
	assert.Equal(t, props, opts.userProperties)
}

func TestOnEvent(t *testing.T) {
	called := false
	handler := func(_ *Client, _ error) {
		called = true
	}

	opts := applyOptions(OnEvent(handler))
	assert.NotNil(t, opts.onEvent)

	// Verify handler is the one we set (can't compare funcs directly)
	opts.onEvent(nil, nil)
	assert.True(t, called)
}

func TestApplyMultipleOptions(t *testing.T) {
	opts := applyOptions(
		WithClientID("multi-test"),
		WithCredentials("admin", "secret"),
		WithKeepAlive(120),
		WithCleanStart(false),
		WithAutoReconnect(true),
		WithMaxReconnects(5),
	)

	assert.Equal(t, "multi-test", opts.clientID)
	assert.Equal(t, "admin", opts.username)
	assert.Equal(t, []byte("secret"), opts.password)
	assert.Equal(t, uint16(120), opts.keepAlive)
	assert.False(t, opts.cleanStart)
	assert.True(t, opts.autoReconnect)
	assert.Equal(t, 5, opts.maxReconnects)
}

func TestOptionsOverride(t *testing.T) {
	opts := applyOptions(
		WithClientID("first"),
		WithClientID("second"),
	)
	assert.Equal(t, "second", opts.clientID)
}

func TestWithMaxSubscriptions(t *testing.T) {
	t.Run("set value", func(t *testing.T) {
		opts := applyOptions(WithMaxSubscriptions(100))
		assert.Equal(t, 100, opts.maxSubscriptions)
	})

	t.Run("unlimited (0)", func(t *testing.T) {
		opts := applyOptions(WithMaxSubscriptions(0))
		assert.Equal(t, 0, opts.maxSubscriptions)
	})
}

func TestWithBackoffStrategy(t *testing.T) {
	t.Run("default backoff strategy is nil", func(t *testing.T) {
		opts := defaultOptions()
		assert.Nil(t, opts.backoffStrategy)
	})

	t.Run("custom backoff strategy", func(t *testing.T) {
		customStrategy := func(attempt int, currentBackoff time.Duration, _ error) time.Duration {
			return time.Duration(attempt) * currentBackoff
		}
		opts := applyOptions(WithBackoffStrategy(customStrategy))
		assert.NotNil(t, opts.backoffStrategy)

		// Verify the strategy works as expected
		result := opts.backoffStrategy(2, time.Second, nil)
		assert.Equal(t, 2*time.Second, result)
	})

	t.Run("nil backoff strategy keeps default", func(t *testing.T) {
		opts := applyOptions(WithBackoffStrategy(nil))
		assert.Nil(t, opts.backoffStrategy)
	})
}

// TestWithClientSessionFactory tests the client session factory option.
func TestWithClientSessionFactory(t *testing.T) {
	t.Run("default session factory", func(t *testing.T) {
		opts := defaultOptions()
		assert.NotNil(t, opts.sessionFactory)

		session := opts.sessionFactory("test-client", "")
		assert.Equal(t, "test-client", session.ClientID())
	})

	t.Run("custom session factory", func(t *testing.T) {
		customFactory := func(clientID, namespace string) Session {
			return NewMemorySession("custom-"+clientID, namespace)
		}
		opts := applyOptions(WithClientSessionFactory(customFactory))

		session := opts.sessionFactory("test", "")
		assert.Equal(t, "custom-test", session.ClientID())
	})

	t.Run("nil factory does not change default", func(t *testing.T) {
		opts := applyOptions(WithClientSessionFactory(nil))
		assert.NotNil(t, opts.sessionFactory)

		session := opts.sessionFactory("test-client", "")
		assert.Equal(t, "test-client", session.ClientID())
	})
}
