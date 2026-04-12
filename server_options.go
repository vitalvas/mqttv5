package mqttv5

import (
	"crypto/tls"
	"net"
)

// Maximum packet size constants for MQTT brokers.
const (
	// MaxPacketSizeDefault is the default maximum packet size (4MB).
	// This is a common default for MQTT brokers.
	MaxPacketSizeDefault = 4 * 1024 * 1024 // 4MB

	// MaxPacketSizeProtocol is the maximum packet size allowed by MQTT protocol (256MB - 1 byte).
	// The remaining length field uses variable byte encoding with max 4 bytes.
	MaxPacketSizeProtocol = 268435455 // 256MB - 1

	// MaxPacketSizeMinimal is a minimal packet size for constrained devices (16KB).
	MaxPacketSizeMinimal = 16 * 1024 // 16KB
)

// ServerOption configures a Server.
type ServerOption func(*serverConfig)

// NamespaceValidator validates a namespace during client connection.
// Return an error to reject the connection with that namespace.
type NamespaceValidator func(namespace string) error

// ConnectFailedContext provides information about a failed connection attempt.
type ConnectFailedContext struct {
	// ClientID is the client identifier from the CONNECT packet (may be empty).
	ClientID string

	// Username is the username from the CONNECT packet (may be empty).
	Username string

	// RemoteAddr is the remote address of the client connection.
	RemoteAddr net.Addr

	// LocalAddr is the local address of the server connection.
	LocalAddr net.Addr

	// TLSConnectionState contains the TLS connection state, if available.
	TLSConnectionState *tls.ConnectionState

	// ReasonCode is the MQTT reason code sent in the CONNACK.
	ReasonCode ReasonCode
}

type serverConfig struct {
	listeners          []net.Listener
	sessionStore       SessionStore
	sessionFactory     SessionFactory
	retainedStore      RetainedStore
	auth               Authenticator
	enhancedAuth       EnhancedAuthenticator
	authz              Authorizer
	tlsIdentityMapper  TLSIdentityMapper
	namespaceValidator NamespaceValidator
	logger             Logger
	metrics            MetricsCollector
	maxPacketSize      uint32
	maxConnections     int
	keepAliveOverride  uint16
	topicAliasMax      uint16
	receiveMaximum     uint16
	onConnect          []func(*ServerClient)
	onConnectFailed    []func(*ConnectFailedContext)
	onDisconnect       []func(*ServerClient)
	onMessage          []func(*ServerClient, *Message)
	onSubscribe        []func(*ServerClient, []Subscription)
	onUnsubscribe      []func(*ServerClient, []string)

	// Server capabilities (MQTT v5 spec section 3.2.2.3)
	maxQoS             byte // Maximum QoS level (0, 1, or 2)
	retainAvailable    bool // Whether retained messages are supported
	wildcardSubAvail   bool // Whether wildcard subscriptions are supported
	subIDAvailable     bool // Whether subscription identifiers are supported
	sharedSubAvailable bool // Whether shared subscriptions are supported

	// Protocol version control
	allowedVersions map[ProtocolVersion]bool

	// Rate limiting
	connRateLimiter ConnectionRateLimiter
	msgRateLimiter  MessageRateLimiter

	// Interceptors
	producerInterceptors []ProducerInterceptor
	consumerInterceptors []ConsumerInterceptor
}

func defaultServerConfig() *serverConfig {
	return &serverConfig{
		listeners:          make([]net.Listener, 0),
		sessionStore:       NewMemorySessionStore(),
		sessionFactory:     DefaultSessionFactory(),
		retainedStore:      NewMemoryRetainedStore(),
		namespaceValidator: ValidateNamespace,
		logger:             NewNoOpLogger(),
		metrics:            &NoOpMetrics{},
		maxPacketSize:      MaxPacketSizeDefault,
		maxConnections:     0, // unlimited
		receiveMaximum:     65535,
		allowedVersions:    map[ProtocolVersion]bool{ProtocolV5: true}, // v5 only by default
		// Default capabilities (all features enabled)
		maxQoS:             QoS2, // QoS 0, 1, 2 all supported
		retainAvailable:    true, // Retained messages supported
		wildcardSubAvail:   true, // Wildcard subscriptions supported
		subIDAvailable:     true, // Subscription identifiers supported
		sharedSubAvailable: true, // Shared subscriptions supported
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

// WithTLSIdentityMapper sets the TLS identity mapper for mTLS authentication.
// The mapper is called during connection setup to extract identity from client certificates.
// The mapped identity is available in AuthContext.TLSIdentity for use by authenticators.
func WithTLSIdentityMapper(mapper TLSIdentityMapper) ServerOption {
	return func(c *serverConfig) {
		c.tlsIdentityMapper = mapper
	}
}

// WithNamespaceValidator sets the namespace validator.
// The validator is called during client connection to validate the namespace
// returned by the authenticator. If validation fails, the connection is rejected.
func WithNamespaceValidator(validator NamespaceValidator) ServerOption {
	return func(c *serverConfig) {
		if validator != nil {
			c.namespaceValidator = validator
		}
	}
}

// WithServerMaxPacketSize sets the maximum packet size the server will accept.
// This limits the size of incoming MQTT packets to prevent memory exhaustion.
//
// The MQTT protocol supports up to 256MB (MaxPacketSizeProtocol), but practical
// limits are much lower. Common values:
//   - MaxPacketSizeDefault (4MB): typical broker default
//   - MaxPacketSizeMinimal (16KB): constrained IoT devices
//
// Values exceeding MaxPacketSizeProtocol are clamped to the protocol maximum.
//
// Default: MaxPacketSizeDefault (4MB)
func WithServerMaxPacketSize(size uint32) ServerOption {
	return func(c *serverConfig) {
		if size > MaxPacketSizeProtocol {
			size = MaxPacketSizeProtocol
		}
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

// OnConnect adds callbacks for client connections.
// Multiple callbacks can be registered; they are called in order.
func OnConnect(fn ...func(*ServerClient)) ServerOption {
	return func(c *serverConfig) {
		c.onConnect = append(c.onConnect, fn...)
	}
}

// OnConnectFailed adds callbacks for failed connection attempts.
// Called when a client connection is rejected (auth failure, max connections, protocol error, etc.).
// Multiple callbacks can be registered; they are called in order.
func OnConnectFailed(fn ...func(*ConnectFailedContext)) ServerOption {
	return func(c *serverConfig) {
		c.onConnectFailed = append(c.onConnectFailed, fn...)
	}
}

// OnDisconnect adds callbacks for client disconnections.
// Multiple callbacks can be registered; they are called in order.
func OnDisconnect(fn ...func(*ServerClient)) ServerOption {
	return func(c *serverConfig) {
		c.onDisconnect = append(c.onDisconnect, fn...)
	}
}

// OnMessage adds callbacks for received messages.
// Multiple callbacks can be registered; they are called in order.
func OnMessage(fn ...func(*ServerClient, *Message)) ServerOption {
	return func(c *serverConfig) {
		c.onMessage = append(c.onMessage, fn...)
	}
}

// OnSubscribe adds callbacks for subscribe requests.
// Multiple callbacks can be registered; they are called in order.
func OnSubscribe(fn ...func(*ServerClient, []Subscription)) ServerOption {
	return func(c *serverConfig) {
		c.onSubscribe = append(c.onSubscribe, fn...)
	}
}

// OnUnsubscribe adds callbacks for unsubscribe requests.
// Multiple callbacks can be registered; they are called in order.
func OnUnsubscribe(fn ...func(*ServerClient, []string)) ServerOption {
	return func(c *serverConfig) {
		c.onUnsubscribe = append(c.onUnsubscribe, fn...)
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

// WithServerProducerInterceptors sets the producer interceptors for outgoing messages.
// Interceptors are called in order before a message is sent to subscribers.
// Each interceptor can modify the message before passing it to the next.
func WithServerProducerInterceptors(interceptors ...ProducerInterceptor) ServerOption {
	return func(c *serverConfig) {
		c.producerInterceptors = append(c.producerInterceptors, interceptors...)
	}
}

// WithServerConsumerInterceptors sets the consumer interceptors for incoming messages.
// Interceptors are called in order after a message is received from a client.
// Each interceptor can modify the message before passing it to the next.
func WithServerConsumerInterceptors(interceptors ...ConsumerInterceptor) ServerOption {
	return func(c *serverConfig) {
		c.consumerInterceptors = append(c.consumerInterceptors, interceptors...)
	}
}

// WithMaxQoS sets the maximum QoS level supported by the server.
// Valid values are 0, 1, or 2. Default is 2 (all QoS levels supported).
// Clients attempting to publish or subscribe with higher QoS will be rejected.
func WithMaxQoS(maxQoS byte) ServerOption {
	return func(c *serverConfig) {
		if maxQoS <= QoS2 {
			c.maxQoS = maxQoS
		}
	}
}

// WithRetainAvailable sets whether retained messages are supported.
// Default is true. If set to false, PUBLISH with retain flag will be rejected.
// Note: If retainedStore is nil, this is automatically set to false.
func WithRetainAvailable(available bool) ServerOption {
	return func(c *serverConfig) {
		c.retainAvailable = available
	}
}

// WithWildcardSubAvailable sets whether wildcard subscriptions are supported.
// Default is true. If set to false, subscriptions with # or + wildcards will be rejected.
func WithWildcardSubAvailable(available bool) ServerOption {
	return func(c *serverConfig) {
		c.wildcardSubAvail = available
	}
}

// WithSubIDAvailable sets whether subscription identifiers are supported.
// Default is true. If set to false, SUBSCRIBE with subscription identifiers will be rejected.
func WithSubIDAvailable(available bool) ServerOption {
	return func(c *serverConfig) {
		c.subIDAvailable = available
	}
}

// WithSharedSubAvailable sets whether shared subscriptions are supported.
// Default is true. If set to false, shared subscription filters ($share/...) will be rejected.
func WithSharedSubAvailable(available bool) ServerOption {
	return func(c *serverConfig) {
		c.sharedSubAvailable = available
	}
}

// WithConnectionRateLimiter sets the connection rate limiter.
// When set, new connections are checked against the limiter before being accepted.
// If nil (default), no connection rate limiting is applied.
func WithConnectionRateLimiter(limiter ConnectionRateLimiter) ServerOption {
	return func(c *serverConfig) {
		c.connRateLimiter = limiter
	}
}

// WithMessageRateLimiter sets the message rate limiter.
// When set, incoming PUBLISH messages are checked against the limiter before processing.
// If nil (default), no message rate limiting is applied.
func WithMessageRateLimiter(limiter MessageRateLimiter) ServerOption {
	return func(c *serverConfig) {
		c.msgRateLimiter = limiter
	}
}

// WithAcceptProtocolVersions sets the protocol versions the server will accept.
// By default, only MQTT v5 (ProtocolV5) is accepted.
// To accept MQTT 3.1.1 clients, include ProtocolV311:
//
//	WithAcceptProtocolVersions(ProtocolV5, ProtocolV311)
func WithAcceptProtocolVersions(versions ...ProtocolVersion) ServerOption {
	return func(c *serverConfig) {
		c.allowedVersions = make(map[ProtocolVersion]bool, len(versions))
		for _, v := range versions {
			c.allowedVersions[v] = true
		}
	}
}
