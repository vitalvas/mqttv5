package mqttv5

import "net"

// ServerOption configures a Server.
type ServerOption func(*serverConfig)

type serverConfig struct {
	listeners         []net.Listener
	sessionStore      SessionStore
	sessionFactory    SessionFactory
	retainedStore     RetainedStore
	auth              Authenticator
	enhancedAuth      EnhancedAuthenticator
	authz             Authorizer
	logger            Logger
	metrics           MetricsCollector
	maxPacketSize     uint32
	maxConnections    int
	keepAliveOverride uint16
	topicAliasMax     uint16
	receiveMaximum    uint16
	onConnect         func(*ServerClient)
	onDisconnect      func(*ServerClient)
	onMessage         func(*ServerClient, *Message)
	onSubscribe       func(*ServerClient, []Subscription)
	onUnsubscribe     func(*ServerClient, []string)
}

func defaultServerConfig() *serverConfig {
	return &serverConfig{
		listeners:      make([]net.Listener, 0),
		sessionStore:   NewMemorySessionStore(),
		sessionFactory: DefaultSessionFactory(),
		retainedStore:  NewMemoryRetainedStore(),
		logger:         NewNoOpLogger(),
		metrics:        &NoOpMetrics{},
		maxPacketSize:  256 * 1024, // 256KB
		maxConnections: 0,          // unlimited
		receiveMaximum: 65535,
	}
}

// WithListener adds a listener to the server.
func WithListener(listener net.Listener) ServerOption {
	return func(c *serverConfig) {
		c.listeners = append(c.listeners, listener)
	}
}

// WithSessionStore sets the session store.
func WithSessionStore(store SessionStore) ServerOption {
	return func(c *serverConfig) {
		c.sessionStore = store
	}
}

// WithSessionFactory sets the session factory.
// The factory is used to create new sessions for connecting clients.
// This allows custom Session implementations to be used.
func WithSessionFactory(factory SessionFactory) ServerOption {
	return func(c *serverConfig) {
		if factory != nil {
			c.sessionFactory = factory
		}
	}
}

// WithRetainedStore sets the retained message store.
func WithRetainedStore(store RetainedStore) ServerOption {
	return func(c *serverConfig) {
		c.retainedStore = store
	}
}

// WithServerAuth sets the authenticator.
func WithServerAuth(auth Authenticator) ServerOption {
	return func(c *serverConfig) {
		c.auth = auth
	}
}

// WithEnhancedAuth sets the enhanced authenticator for SASL-style authentication.
// Enhanced authentication allows multi-step authentication exchanges using AUTH packets.
func WithEnhancedAuth(auth EnhancedAuthenticator) ServerOption {
	return func(c *serverConfig) {
		c.enhancedAuth = auth
	}
}

// WithServerAuthz sets the authorizer.
func WithServerAuthz(authz Authorizer) ServerOption {
	return func(c *serverConfig) {
		c.authz = authz
	}
}

// WithServerMaxPacketSize sets the maximum packet size.
func WithServerMaxPacketSize(size uint32) ServerOption {
	return func(c *serverConfig) {
		c.maxPacketSize = size
	}
}

// WithMaxConnections sets the maximum number of concurrent connections.
// 0 means unlimited.
func WithMaxConnections(n int) ServerOption {
	return func(c *serverConfig) {
		c.maxConnections = n
	}
}

// WithServerKeepAlive sets the server keep-alive override.
// When set, clients must use this value instead of their requested value.
func WithServerKeepAlive(seconds uint16) ServerOption {
	return func(c *serverConfig) {
		c.keepAliveOverride = seconds
	}
}

// WithServerTopicAliasMax sets the maximum topic alias value.
func WithServerTopicAliasMax(maxVal uint16) ServerOption {
	return func(c *serverConfig) {
		c.topicAliasMax = maxVal
	}
}

// WithServerReceiveMaximum sets the receive maximum.
func WithServerReceiveMaximum(maxVal uint16) ServerOption {
	return func(c *serverConfig) {
		if maxVal == 0 {
			maxVal = 65535
		}
		c.receiveMaximum = maxVal
	}
}

// OnConnect sets the callback for client connections.
func OnConnect(fn func(*ServerClient)) ServerOption {
	return func(c *serverConfig) {
		c.onConnect = fn
	}
}

// OnDisconnect sets the callback for client disconnections.
func OnDisconnect(fn func(*ServerClient)) ServerOption {
	return func(c *serverConfig) {
		c.onDisconnect = fn
	}
}

// OnMessage sets the callback for received messages.
func OnMessage(fn func(*ServerClient, *Message)) ServerOption {
	return func(c *serverConfig) {
		c.onMessage = fn
	}
}

// OnSubscribe sets the callback for subscribe requests.
func OnSubscribe(fn func(*ServerClient, []Subscription)) ServerOption {
	return func(c *serverConfig) {
		c.onSubscribe = fn
	}
}

// OnUnsubscribe sets the callback for unsubscribe requests.
func OnUnsubscribe(fn func(*ServerClient, []string)) ServerOption {
	return func(c *serverConfig) {
		c.onUnsubscribe = fn
	}
}

// WithLogger sets the logger.
func WithLogger(logger Logger) ServerOption {
	return func(c *serverConfig) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithMetrics sets the metrics collector.
func WithMetrics(metrics MetricsCollector) ServerOption {
	return func(c *serverConfig) {
		if metrics != nil {
			c.metrics = metrics
		}
	}
}
