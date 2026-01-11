package mqttv5

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServerOptions(t *testing.T) {
	t.Run("default config", func(t *testing.T) {
		cfg := defaultServerConfig()

		assert.NotNil(t, cfg.sessionStore)
		assert.NotNil(t, cfg.retainedStore)
		assert.Equal(t, uint32(MaxPacketSizeDefault), cfg.maxPacketSize)
		assert.Equal(t, 0, cfg.maxConnections)
		assert.Equal(t, uint16(65535), cfg.receiveMaximum)
	})

	t.Run("with session store", func(t *testing.T) {
		store := NewMemorySessionStore()
		cfg := defaultServerConfig()
		WithSessionStore(store)(cfg)

		assert.Equal(t, store, cfg.sessionStore)
	})

	t.Run("with retained store", func(t *testing.T) {
		store := NewMemoryRetainedStore()
		cfg := defaultServerConfig()
		WithRetainedStore(store)(cfg)

		assert.Equal(t, store, cfg.retainedStore)
	})

	t.Run("with server auth", func(t *testing.T) {
		auth := &AllowAllAuthenticator{}
		cfg := defaultServerConfig()
		WithServerAuth(auth)(cfg)

		assert.Equal(t, auth, cfg.auth)
	})

	t.Run("with server authz", func(t *testing.T) {
		authz := &AllowAllAuthorizer{}
		cfg := defaultServerConfig()
		WithServerAuthz(authz)(cfg)

		assert.Equal(t, authz, cfg.authz)
	})

	t.Run("with max packet size", func(t *testing.T) {
		cfg := defaultServerConfig()
		WithServerMaxPacketSize(1024 * 1024)(cfg)

		assert.Equal(t, uint32(1024*1024), cfg.maxPacketSize)
	})

	t.Run("with max packet size exceeding protocol limit", func(t *testing.T) {
		cfg := defaultServerConfig()
		WithServerMaxPacketSize(MaxPacketSizeProtocol + 1000)(cfg)

		assert.Equal(t, uint32(MaxPacketSizeProtocol), cfg.maxPacketSize)
	})

	t.Run("with max connections", func(t *testing.T) {
		cfg := defaultServerConfig()
		WithMaxConnections(100)(cfg)

		assert.Equal(t, 100, cfg.maxConnections)
	})

	t.Run("with server keep alive", func(t *testing.T) {
		cfg := defaultServerConfig()
		WithServerKeepAlive(120)(cfg)

		assert.Equal(t, uint16(120), cfg.keepAliveOverride)
	})

	t.Run("with topic alias max", func(t *testing.T) {
		cfg := defaultServerConfig()
		WithServerTopicAliasMax(50)(cfg)

		assert.Equal(t, uint16(50), cfg.topicAliasMax)
	})

	t.Run("with receive maximum", func(t *testing.T) {
		cfg := defaultServerConfig()
		WithServerReceiveMaximum(100)(cfg)

		assert.Equal(t, uint16(100), cfg.receiveMaximum)
	})

	t.Run("with receive maximum zero defaults to max", func(t *testing.T) {
		cfg := defaultServerConfig()
		WithServerReceiveMaximum(0)(cfg)

		assert.Equal(t, uint16(65535), cfg.receiveMaximum)
	})

	t.Run("on connect callback", func(t *testing.T) {
		cfg := defaultServerConfig()
		var called bool
		OnConnect(func(_ *ServerClient) {
			called = true
		})(cfg)

		assert.NotNil(t, cfg.onConnect)
		cfg.onConnect(nil)
		assert.True(t, called)
	})

	t.Run("on disconnect callback", func(t *testing.T) {
		cfg := defaultServerConfig()
		var called bool
		OnDisconnect(func(_ *ServerClient) {
			called = true
		})(cfg)

		assert.NotNil(t, cfg.onDisconnect)
		cfg.onDisconnect(nil)
		assert.True(t, called)
	})

	t.Run("on message callback", func(t *testing.T) {
		cfg := defaultServerConfig()
		var called bool
		OnMessage(func(_ *ServerClient, _ *Message) {
			called = true
		})(cfg)

		assert.NotNil(t, cfg.onMessage)
		cfg.onMessage(nil, nil)
		assert.True(t, called)
	})

	t.Run("on subscribe callback", func(t *testing.T) {
		cfg := defaultServerConfig()
		var called bool
		OnSubscribe(func(_ *ServerClient, _ []Subscription) {
			called = true
		})(cfg)

		assert.NotNil(t, cfg.onSubscribe)
		cfg.onSubscribe(nil, nil)
		assert.True(t, called)
	})

	t.Run("on unsubscribe callback", func(t *testing.T) {
		cfg := defaultServerConfig()
		var called bool
		OnUnsubscribe(func(_ *ServerClient, _ []string) {
			called = true
		})(cfg)

		assert.NotNil(t, cfg.onUnsubscribe)
		cfg.onUnsubscribe(nil, nil)
		assert.True(t, called)
	})

	t.Run("with enhanced auth", func(t *testing.T) {
		auth := &mockEnhancedAuthenticator{methods: map[string]bool{"PLAIN": true}}
		cfg := defaultServerConfig()
		WithEnhancedAuth(auth)(cfg)

		assert.Equal(t, auth, cfg.enhancedAuth)
	})

	t.Run("with session factory", func(t *testing.T) {
		customFactory := func(clientID, namespace string) Session {
			return NewMemorySession("custom-"+clientID, namespace)
		}
		cfg := defaultServerConfig()
		WithSessionFactory(customFactory)(cfg)

		session := cfg.sessionFactory("test", testNS)
		assert.Equal(t, "custom-test", session.ClientID())
	})

	t.Run("with session factory nil", func(t *testing.T) {
		cfg := defaultServerConfig()
		originalFactory := cfg.sessionFactory
		WithSessionFactory(nil)(cfg)

		// Should not change when nil
		assert.NotNil(t, cfg.sessionFactory)
		session := cfg.sessionFactory("test", testNS)
		assert.Equal(t, originalFactory("test", testNS).ClientID(), session.ClientID())
	})

	t.Run("default session factory", func(t *testing.T) {
		cfg := defaultServerConfig()
		assert.NotNil(t, cfg.sessionFactory)

		session := cfg.sessionFactory("test-client", testNS)
		assert.Equal(t, "test-client", session.ClientID())
	})
}

func TestServerCapabilityOptions(t *testing.T) {
	t.Run("default capabilities", func(t *testing.T) {
		cfg := defaultServerConfig()

		assert.Equal(t, byte(2), cfg.maxQoS)
		assert.True(t, cfg.retainAvailable)
		assert.True(t, cfg.wildcardSubAvail)
		assert.True(t, cfg.subIDAvailable)
		assert.True(t, cfg.sharedSubAvailable)
	})

	t.Run("WithMaxQoS sets maximum QoS", func(t *testing.T) {
		cfg := defaultServerConfig()

		WithMaxQoS(0)(cfg)
		assert.Equal(t, byte(0), cfg.maxQoS)

		WithMaxQoS(1)(cfg)
		assert.Equal(t, byte(1), cfg.maxQoS)

		WithMaxQoS(2)(cfg)
		assert.Equal(t, byte(2), cfg.maxQoS)
	})

	t.Run("WithMaxQoS ignores invalid values", func(t *testing.T) {
		cfg := defaultServerConfig()
		cfg.maxQoS = 1

		WithMaxQoS(3)(cfg) // Invalid, should be ignored
		assert.Equal(t, byte(1), cfg.maxQoS)

		WithMaxQoS(255)(cfg) // Invalid, should be ignored
		assert.Equal(t, byte(1), cfg.maxQoS)
	})

	t.Run("WithRetainAvailable sets retain support", func(t *testing.T) {
		cfg := defaultServerConfig()

		WithRetainAvailable(false)(cfg)
		assert.False(t, cfg.retainAvailable)

		WithRetainAvailable(true)(cfg)
		assert.True(t, cfg.retainAvailable)
	})

	t.Run("WithWildcardSubAvailable sets wildcard subscription support", func(t *testing.T) {
		cfg := defaultServerConfig()

		WithWildcardSubAvailable(false)(cfg)
		assert.False(t, cfg.wildcardSubAvail)

		WithWildcardSubAvailable(true)(cfg)
		assert.True(t, cfg.wildcardSubAvail)
	})

	t.Run("WithSubIDAvailable sets subscription identifier support", func(t *testing.T) {
		cfg := defaultServerConfig()

		WithSubIDAvailable(false)(cfg)
		assert.False(t, cfg.subIDAvailable)

		WithSubIDAvailable(true)(cfg)
		assert.True(t, cfg.subIDAvailable)
	})

	t.Run("WithSharedSubAvailable sets shared subscription support", func(t *testing.T) {
		cfg := defaultServerConfig()

		WithSharedSubAvailable(false)(cfg)
		assert.False(t, cfg.sharedSubAvailable)

		WithSharedSubAvailable(true)(cfg)
		assert.True(t, cfg.sharedSubAvailable)
	})

	t.Run("WithLogger sets logger", func(t *testing.T) {
		cfg := defaultServerConfig()
		logger := NewStdLogger(nil, LogLevelDebug)

		WithLogger(logger)(cfg)
		assert.Equal(t, logger, cfg.logger)

		// nil logger should not change the config
		WithLogger(nil)(cfg)
		assert.Equal(t, logger, cfg.logger)
	})

	t.Run("WithTLSIdentityMapper sets mapper", func(t *testing.T) {
		cfg := defaultServerConfig()
		mapper := &CommonNameMapper{}

		WithTLSIdentityMapper(mapper)(cfg)
		assert.NotNil(t, cfg.tlsIdentityMapper)
	})

	t.Run("WithNamespaceValidator sets validator", func(t *testing.T) {
		cfg := defaultServerConfig()
		validator := func(_ string) error { return nil }

		WithNamespaceValidator(validator)(cfg)
		assert.NotNil(t, cfg.namespaceValidator)

		// nil validator should not change config
		WithNamespaceValidator(nil)(cfg)
		assert.NotNil(t, cfg.namespaceValidator)
	})
}
