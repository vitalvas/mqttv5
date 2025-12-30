package mqttv5

// ServerOption configures a Server.
type ServerOption func(*serverConfig)

type serverConfig struct {
	sessionStore      SessionStore
	retainedStore     RetainedStore
	auth              Authenticator
	authz             Authorizer
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
		sessionStore:   NewMemorySessionStore(),
		retainedStore:  NewMemoryRetainedStore(),
		maxPacketSize:  256 * 1024, // 256KB
		maxConnections: 0,          // unlimited
		receiveMaximum: 65535,
	}
}

// WithSessionStore sets the session store.
func WithSessionStore(store SessionStore) ServerOption {
	return func(c *serverConfig) {
		c.sessionStore = store
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
