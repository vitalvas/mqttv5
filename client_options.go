package mqttv5

import (
	"context"
	"crypto/tls"
	"time"
)

// BackoffStrategy is a function that computes the next backoff duration.
// It receives the current attempt number (1-based), the previous backoff duration,
// and the error from the last connection attempt.
// Return the duration to wait before the next attempt.
// This allows implementing jitter, server hints, or custom strategies.
type BackoffStrategy func(attempt int, currentBackoff time.Duration, err error) time.Duration

// ServerResolver is a function that returns a list of server addresses.
// It is called before each connection attempt to enable dynamic service discovery.
// The addresses should be in URI format: scheme://host:port (e.g., "tcp://broker:1883").
type ServerResolver func(ctx context.Context) ([]string, error)

// clientOptions holds configuration for a Client.
type clientOptions struct {
	// Connection settings
	clientID   string
	username   string
	password   []byte
	keepAlive  uint16
	cleanStart bool

	// TLS configuration
	tlsConfig *tls.Config

	// Timeouts
	connectTimeout time.Duration
	writeTimeout   time.Duration
	readTimeout    time.Duration

	// Will message
	willTopic   string
	willPayload []byte
	willRetain  bool
	willQoS     byte
	willProps   *Properties

	// Auto reconnect settings
	autoReconnect    bool
	maxReconnects    int
	reconnectBackoff time.Duration
	maxBackoff       time.Duration
	backoffStrategy  BackoffStrategy

	// Event handler
	onEvent EventHandler

	// Limits
	maxPacketSize    uint32
	maxSubscriptions int // 0 means unlimited

	// Properties for CONNECT packet
	sessionExpiryInterval uint32
	receiveMaximum        uint16
	topicAliasMaximum     uint16
	userProperties        map[string]string

	// Session factory for creating custom sessions
	sessionFactory SessionFactory

	// Interceptors
	producerInterceptors []ProducerInterceptor
	consumerInterceptors []ConsumerInterceptor

	// Enhanced authentication
	enhancedAuth ClientEnhancedAuthenticator

	// Multi-server support
	servers        []string       // Static server list
	serverResolver ServerResolver // Dynamic server discovery
}

// defaultOptions returns options with sensible defaults.
func defaultOptions() *clientOptions {
	return &clientOptions{
		keepAlive:        60,
		cleanStart:       true,
		connectTimeout:   10 * time.Second,
		writeTimeout:     5 * time.Second,
		readTimeout:      5 * time.Second,
		autoReconnect:    false,
		maxReconnects:    10,
		reconnectBackoff: 1 * time.Second,
		maxBackoff:       60 * time.Second,
		maxPacketSize:    MaxPacketSizeDefault,
		receiveMaximum:   65535,
		sessionFactory:   DefaultSessionFactory(),
	}
}

// Option configures a Client.
type Option func(*clientOptions)

// WithClientID sets the client identifier.
func WithClientID(id string) Option {
	return func(o *clientOptions) {
		o.clientID = id
	}
}

// WithCredentials sets the username and password for authentication.
func WithCredentials(username, password string) Option {
	return func(o *clientOptions) {
		o.username = username
		o.password = []byte(password)
	}
}

// WithKeepAlive sets the keep-alive interval in seconds.
func WithKeepAlive(seconds uint16) Option {
	return func(o *clientOptions) {
		o.keepAlive = seconds
	}
}

// WithCleanStart sets whether to start with a clean session.
func WithCleanStart(clean bool) Option {
	return func(o *clientOptions) {
		o.cleanStart = clean
	}
}

// WithTLS sets the TLS configuration for secure connections.
func WithTLS(config *tls.Config) Option {
	return func(o *clientOptions) {
		o.tlsConfig = config
	}
}

// WithConnectTimeout sets the timeout for the initial connection.
func WithConnectTimeout(d time.Duration) Option {
	return func(o *clientOptions) {
		o.connectTimeout = d
	}
}

// WithWriteTimeout sets the timeout for write operations.
func WithWriteTimeout(d time.Duration) Option {
	return func(o *clientOptions) {
		o.writeTimeout = d
	}
}

// WithReadTimeout sets the timeout for read operations.
func WithReadTimeout(d time.Duration) Option {
	return func(o *clientOptions) {
		o.readTimeout = d
	}
}

// WithAutoReconnect enables automatic reconnection on connection loss.
func WithAutoReconnect(enabled bool) Option {
	return func(o *clientOptions) {
		o.autoReconnect = enabled
	}
}

// WithMaxReconnects sets the maximum number of reconnection attempts.
// Use -1 for unlimited attempts.
func WithMaxReconnects(n int) Option {
	return func(o *clientOptions) {
		o.maxReconnects = n
	}
}

// WithReconnectBackoff sets the initial backoff duration between reconnection attempts.
func WithReconnectBackoff(d time.Duration) Option {
	return func(o *clientOptions) {
		o.reconnectBackoff = d
	}
}

// WithMaxBackoff sets the maximum backoff duration between reconnection attempts.
func WithMaxBackoff(d time.Duration) Option {
	return func(o *clientOptions) {
		o.maxBackoff = d
	}
}

// WithBackoffStrategy sets a custom backoff strategy for reconnection attempts.
// If not set, uses exponential backoff (doubling) up to maxBackoff.
func WithBackoffStrategy(strategy BackoffStrategy) Option {
	return func(o *clientOptions) {
		o.backoffStrategy = strategy
	}
}

// WithWill sets the Will message that will be published if the client disconnects unexpectedly.
func WithWill(topic string, payload []byte, retain bool, qos byte) Option {
	return func(o *clientOptions) {
		o.willTopic = topic
		o.willPayload = payload
		o.willRetain = retain
		o.willQoS = qos
	}
}

// WithWillProps sets the properties for the Will message.
func WithWillProps(props *Properties) Option {
	return func(o *clientOptions) {
		o.willProps = props
	}
}

// WithMaxPacketSize sets the maximum packet size the client will accept.
// This limits the size of incoming MQTT packets to prevent memory exhaustion.
//
// Common values:
//   - MaxPacketSizeDefault (4MB): typical broker default
//   - MaxPacketSizeMinimal (16KB): constrained IoT devices
//
// Values exceeding MaxPacketSizeProtocol are clamped to the protocol maximum.
//
// Default: MaxPacketSizeDefault (4MB)
func WithMaxPacketSize(size uint32) Option {
	return func(o *clientOptions) {
		if size > MaxPacketSizeProtocol {
			size = MaxPacketSizeProtocol
		}
		o.maxPacketSize = size
	}
}

// WithMaxSubscriptions sets the maximum number of active subscriptions.
// Use 0 for unlimited subscriptions.
func WithMaxSubscriptions(maxValue int) Option {
	return func(o *clientOptions) {
		o.maxSubscriptions = maxValue
	}
}

// WithSessionExpiryInterval sets the session expiry interval in seconds.
func WithSessionExpiryInterval(seconds uint32) Option {
	return func(o *clientOptions) {
		o.sessionExpiryInterval = seconds
	}
}

// WithReceiveMaximum sets the maximum number of QoS 1 and 2 messages
// the client is willing to process concurrently.
func WithReceiveMaximum(maxValue uint16) Option {
	return func(o *clientOptions) {
		o.receiveMaximum = maxValue
	}
}

// WithTopicAliasMaximum sets the maximum number of topic aliases the client will accept.
func WithTopicAliasMaximum(maxValue uint16) Option {
	return func(o *clientOptions) {
		o.topicAliasMaximum = maxValue
	}
}

// WithUserProperties sets user properties for the CONNECT packet.
func WithUserProperties(props map[string]string) Option {
	return func(o *clientOptions) {
		o.userProperties = props
	}
}

// OnEvent sets the event handler for client lifecycle events and errors.
func OnEvent(handler EventHandler) Option {
	return func(o *clientOptions) {
		o.onEvent = handler
	}
}

// WithClientSessionFactory sets the session factory for creating client sessions.
// This allows custom Session implementations to be used.
func WithClientSessionFactory(factory SessionFactory) Option {
	return func(o *clientOptions) {
		if factory != nil {
			o.sessionFactory = factory
		}
	}
}

// WithProducerInterceptors sets the producer interceptors for outgoing messages.
// Interceptors are called in order before a message is published.
// Each interceptor can modify the message before passing it to the next.
func WithProducerInterceptors(interceptors ...ProducerInterceptor) Option {
	return func(o *clientOptions) {
		o.producerInterceptors = append(o.producerInterceptors, interceptors...)
	}
}

// WithConsumerInterceptors sets the consumer interceptors for incoming messages.
// Interceptors are called in order before a message is delivered to handlers.
// Each interceptor can modify the message before passing it to the next.
func WithConsumerInterceptors(interceptors ...ConsumerInterceptor) Option {
	return func(o *clientOptions) {
		o.consumerInterceptors = append(o.consumerInterceptors, interceptors...)
	}
}

// WithEnhancedAuthentication sets the enhanced authenticator for SASL-style authentication.
// Enhanced authentication allows multi-step authentication exchanges using AUTH packets.
func WithEnhancedAuthentication(auth ClientEnhancedAuthenticator) Option {
	return func(o *clientOptions) {
		o.enhancedAuth = auth
	}
}

// WithServers sets a static list of server addresses for connection attempts.
// Servers are tried in round-robin order on each connection/reconnection.
// Addresses should be in URI format: scheme://host:port (e.g., "tcp://broker:1883").
// Multiple calls append to the existing list.
func WithServers(servers ...string) Option {
	return func(o *clientOptions) {
		o.servers = append(o.servers, servers...)
	}
}

// WithServerResolver sets a dynamic server resolver for service discovery.
// The resolver is called before each connection/reconnection attempt.
// If the resolver returns an error or empty list, static servers are used as fallback.
// This enables integration with DNS SRV records, service registries, or custom discovery.
func WithServerResolver(resolver ServerResolver) Option {
	return func(o *clientOptions) {
		o.serverResolver = resolver
	}
}

// applyOptions applies all options to the default options.
func applyOptions(opts ...Option) *clientOptions {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}
