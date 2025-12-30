package mqttv5

import (
	"crypto/tls"
	"time"
)

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
		maxPacketSize:    256 * 1024,
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
func WithMaxPacketSize(size uint32) Option {
	return func(o *clientOptions) {
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

// applyOptions applies all options to the default options.
func applyOptions(opts ...Option) *clientOptions {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}
