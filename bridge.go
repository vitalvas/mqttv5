package mqttv5

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
)

// BridgeDirection specifies the direction of message forwarding.
type BridgeDirection int

const (
	// BridgeDirectionOut forwards messages from local to remote.
	BridgeDirectionOut BridgeDirection = iota
	// BridgeDirectionIn forwards messages from remote to local.
	BridgeDirectionIn
	// BridgeDirectionBoth forwards messages in both directions.
	BridgeDirectionBoth
)

// bridgePropertyKey is used to detect bridge loops.
const bridgePropertyKey = "x-bridge-id"

// Bridge errors.
var (
	ErrBridgeAlreadyRunning = errors.New("bridge is already running")
	ErrBridgeNotRunning     = errors.New("bridge is not running")
	ErrBridgeNoTopics       = errors.New("bridge requires at least one topic")
)

// TopicRemapFunc is a function that remaps topics during bridging.
// It receives the original topic and the forwarding direction.
// Return the remapped topic, or empty string to use default prefix-based remapping.
type TopicRemapFunc func(topic string, direction BridgeDirection) string

// BridgeTopic defines a topic mapping for the bridge.
type BridgeTopic struct {
	// LocalPrefix is the topic prefix on the local broker.
	LocalPrefix string
	// RemotePrefix is the topic prefix on the remote broker.
	RemotePrefix string
	// Direction specifies forwarding direction for this topic.
	Direction BridgeDirection
	// QoS is the QoS level for this topic subscription.
	QoS byte
}

// BridgeConfig contains configuration for a bridge.
type BridgeConfig struct {
	// RemoteAddr is the address of the remote broker (e.g., "tcp://remote:1883").
	RemoteAddr string
	// ClientID is the client ID used when connecting to the remote broker.
	ClientID string
	// Topics defines the topic mappings for the bridge.
	Topics []BridgeTopic
	// TopicRemapper is an optional function for custom topic remapping.
	// If provided, it is called before the default prefix-based remapping.
	// Return empty string to fall back to default remapping.
	TopicRemapper TopicRemapFunc
	// Credentials for remote broker authentication.
	Username string
	Password string
	// CleanStart specifies whether to start with a clean session.
	CleanStart bool
	// KeepAlive interval in seconds.
	KeepAlive uint16
}

// bridgeTopicCached holds pre-computed topic prefix data.
type bridgeTopicCached struct {
	localPrefix  string
	remotePrefix string
	direction    BridgeDirection
	qos          byte
}

// Bridge connects two MQTT brokers and forwards messages between them.
type Bridge struct {
	config          BridgeConfig
	server          *Server
	client          *Client
	id              string
	bridgeProp      StringPair // cached bridge property for loop detection
	cachedTopics    []bridgeTopicCached
	running         atomic.Bool
	initialConnDone atomic.Bool // tracks if initial connection is complete
	mu              sync.Mutex
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewBridge creates a new bridge between a local server and a remote broker.
func NewBridge(server *Server, config BridgeConfig) (*Bridge, error) {
	if len(config.Topics) == 0 {
		return nil, ErrBridgeNoTopics
	}

	id := config.ClientID
	if id == "" {
		id = "bridge-" + generateClientID()
	}

	// Pre-cache cleaned topic prefixes
	cached := make([]bridgeTopicCached, len(config.Topics))
	for i, t := range config.Topics {
		cached[i] = bridgeTopicCached{
			localPrefix:  cleanPrefix(t.LocalPrefix),
			remotePrefix: cleanPrefix(t.RemotePrefix),
			direction:    t.Direction,
			qos:          t.QoS,
		}
	}

	return &Bridge{
		config:       config,
		server:       server,
		id:           id,
		bridgeProp:   StringPair{Key: bridgePropertyKey, Value: id},
		cachedTopics: cached,
	}, nil
}

// cleanPrefix removes trailing wildcards from a prefix.
func cleanPrefix(prefix string) string {
	prefix = strings.TrimSuffix(prefix, "/#")
	prefix = strings.TrimSuffix(prefix, "/+")
	return prefix
}

// ID returns the bridge identifier.
func (b *Bridge) ID() string {
	return b.id
}

// Start connects to the remote broker and begins forwarding messages.
func (b *Bridge) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.running.Load() {
		return ErrBridgeAlreadyRunning
	}

	b.ctx, b.cancel = context.WithCancel(context.Background())

	// Build client options with auto-reconnect enabled for resilience
	opts := []Option{
		WithClientID(b.id),
		WithCleanStart(b.config.CleanStart),
		WithAutoReconnect(true),
		WithMaxReconnects(-1), // Unlimited reconnection attempts
		OnEvent(func(_ *Client, event error) {
			b.handleClientEvent(event)
		}),
	}

	if b.config.KeepAlive > 0 {
		opts = append(opts, WithKeepAlive(b.config.KeepAlive))
	}

	if b.config.Username != "" {
		opts = append(opts, WithCredentials(b.config.Username, b.config.Password))
	}

	// Connect to remote broker
	client, err := DialContext(b.ctx, b.config.RemoteAddr, opts...)
	if err != nil {
		return err
	}
	b.client = client

	// Set up subscriptions based on topic configuration
	if err := b.setupSubscriptions(); err != nil {
		b.client.Close()
		b.client = nil
		return err
	}

	// Set up local message forwarding (local -> remote)
	b.setupLocalForwarding()

	b.running.Store(true)
	b.initialConnDone.Store(true) // Mark initial connection complete
	return nil
}

// handleClientEvent handles events from the remote broker client.
func (b *Bridge) handleClientEvent(event error) {
	if event == nil {
		return
	}

	switch e := event.(type) {
	case *ConnectedEvent:
		// Only re-subscribe on reconnection, not initial connect
		// (Start() already sets up subscriptions for the initial connection)
		if b.initialConnDone.Load() {
			log.Printf("bridge %s: reconnected to remote broker", b.id)
			if err := b.setupSubscriptions(); err != nil {
				log.Printf("bridge %s: failed to re-subscribe after reconnect: %v", b.id, err)
			}
		}
	case *ConnectionLostError:
		log.Printf("bridge %s: connection lost: %v", b.id, e.Cause)
		b.server.Metrics().BridgeError()
	case *DisconnectError:
		log.Printf("bridge %s: disconnected: %s", b.id, e.ReasonCode)
	}
}

// Stop disconnects from the remote broker and stops forwarding.
func (b *Bridge) Stop() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.running.Load() {
		return ErrBridgeNotRunning
	}

	if b.cancel != nil {
		b.cancel()
	}

	if b.client != nil {
		b.client.Close()
		b.client = nil
	}

	b.running.Store(false)
	return nil
}

// IsRunning returns true if the bridge is running.
func (b *Bridge) IsRunning() bool {
	return b.running.Load()
}

// setupSubscriptions subscribes to topics on the remote broker for inbound forwarding.
func (b *Bridge) setupSubscriptions() error {
	for i, topic := range b.config.Topics {
		if topic.Direction == BridgeDirectionIn || topic.Direction == BridgeDirectionBoth {
			filter := topic.RemotePrefix
			if !strings.HasSuffix(filter, "#") && !strings.HasSuffix(filter, "+") {
				filter += "/#"
			}

			// Capture cached topic index for closure
			cachedIdx := i

			err := b.client.Subscribe(filter, topic.QoS, func(msg *Message) {
				ct := &b.cachedTopics[cachedIdx]
				b.forwardToLocal(msg, ct.remotePrefix, ct.localPrefix)
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// setupLocalForwarding sets up forwarding from local to remote using server callbacks.
func (b *Bridge) setupLocalForwarding() {
	// We use a producer interceptor on the server to catch outgoing messages
	// But since we can't modify server config after creation, we'll use OnMessage callback approach
	// The bridge needs to be set up before server starts, or we need a different approach

	// For now, we'll poll or the user can call ForwardToRemote manually
	// A better approach would be to add bridge support directly to Server
}

// forwardToLocal forwards a message from remote broker to local server.
func (b *Bridge) forwardToLocal(msg *Message, remotePrefix, localPrefix string) {
	// Check for bridge loop
	if b.isFromBridge(msg) {
		b.server.Metrics().BridgeDroppedLoop()
		return
	}

	// Remap topic
	newTopic := b.remapTopic(msg.Topic, remotePrefix, localPrefix, BridgeDirectionIn)

	// Create forwarded message
	fwdMsg := &Message{
		Topic:           newTopic,
		Payload:         msg.Payload,
		QoS:             msg.QoS,
		Retain:          msg.Retain,
		PayloadFormat:   msg.PayloadFormat,
		MessageExpiry:   msg.MessageExpiry,
		ContentType:     msg.ContentType,
		ResponseTopic:   msg.ResponseTopic,
		CorrelationData: msg.CorrelationData,
		UserProperties:  b.addBridgeProperty(msg.UserProperties),
	}

	// Publish to local server
	if err := b.server.Publish(fwdMsg); err != nil {
		b.server.Metrics().BridgeError()
		log.Printf("bridge: failed to forward message to local: %v", err)
		return
	}
	b.server.Metrics().BridgeForwardedToLocal()
}

// ForwardToRemote forwards a message from local server to remote broker.
// This should be called from server's OnMessage callback.
func (b *Bridge) ForwardToRemote(msg *Message) {
	if !b.running.Load() || b.client == nil {
		return
	}

	// Check for bridge loop
	if b.isFromBridge(msg) {
		b.server.Metrics().BridgeDroppedLoop()
		return
	}

	// Find matching topic configuration using cached prefixes
	for i := range b.cachedTopics {
		ct := &b.cachedTopics[i]
		if ct.direction != BridgeDirectionOut && ct.direction != BridgeDirectionBoth {
			continue
		}

		if b.topicMatchesPrefix(msg.Topic, ct.localPrefix) {
			// Remap topic using pre-cleaned prefixes
			newTopic := b.remapTopic(msg.Topic, ct.localPrefix, ct.remotePrefix, BridgeDirectionOut)

			// Create forwarded message
			fwdMsg := &Message{
				Topic:           newTopic,
				Payload:         msg.Payload,
				QoS:             msg.QoS,
				Retain:          msg.Retain,
				PayloadFormat:   msg.PayloadFormat,
				MessageExpiry:   msg.MessageExpiry,
				ContentType:     msg.ContentType,
				ResponseTopic:   msg.ResponseTopic,
				CorrelationData: msg.CorrelationData,
				UserProperties:  b.addBridgeProperty(msg.UserProperties),
			}

			if err := b.client.Publish(fwdMsg); err != nil {
				b.server.Metrics().BridgeError()
				log.Printf("bridge: failed to forward message to remote: %v", err)
				return
			}
			b.server.Metrics().BridgeForwardedToRemote()
			return
		}
	}
}

// remapTopic transforms a topic from one prefix to another.
// fromPrefix and toPrefix should be pre-cleaned (no trailing wildcards).
func (b *Bridge) remapTopic(topic, fromPrefix, toPrefix string, direction BridgeDirection) string {
	// Try custom remapper first
	if b.config.TopicRemapper != nil {
		if remapped := b.config.TopicRemapper(topic, direction); remapped != "" {
			return remapped
		}
	}

	// Default prefix-based remapping
	if suffix, found := strings.CutPrefix(topic, fromPrefix); found {
		if fromPrefix == "" && toPrefix != "" {
			return toPrefix + "/" + suffix
		}
		return toPrefix + suffix
	}
	return topic
}

// topicMatchesPrefix checks if a topic matches a prefix pattern.
// prefix should be pre-cleaned (no trailing wildcards).
// For MQTT topic semantics, "foo" matches "foo" exactly or "foo/..." but NOT "foobar".
func (b *Bridge) topicMatchesPrefix(topic, prefix string) bool {
	if prefix == "" {
		return true
	}
	// Exact match
	if topic == prefix {
		return true
	}
	// Prefix match requires the topic to have prefix followed by "/"
	// e.g., prefix "foo" matches "foo/bar" but not "foobar"
	return strings.HasPrefix(topic, prefix+"/")
}

// isFromBridge checks if a message originated from this bridge (loop detection).
func (b *Bridge) isFromBridge(msg *Message) bool {
	for _, prop := range msg.UserProperties {
		if prop.Key == bridgePropertyKey && prop.Value == b.id {
			return true
		}
	}
	return false
}

// addBridgeProperty adds the bridge identifier to user properties for loop detection.
func (b *Bridge) addBridgeProperty(props []StringPair) []StringPair {
	result := make([]StringPair, len(props)+1)
	copy(result, props)
	result[len(props)] = b.bridgeProp
	return result
}
