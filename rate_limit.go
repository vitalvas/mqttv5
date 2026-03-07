package mqttv5

import (
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ConnectionRateLimiter controls the rate of new incoming connections.
// Implementations must be safe for concurrent use.
type ConnectionRateLimiter interface {
	// AllowConnection checks if a new connection is allowed.
	// Returns true if the connection should be accepted, false if it should be rejected.
	AllowConnection(conn net.Conn) bool
}

// MessageRateLimiter controls the rate of incoming PUBLISH messages.
// Implementations must be safe for concurrent use.
type MessageRateLimiter interface {
	// AllowMessage checks if a message from the given client is allowed.
	// Returns RateLimitAllow if the message should be processed,
	// or a RateLimitExceedAction indicating how to handle the rate-limited message.
	AllowMessage(clientID string, namespace string, topic string) RateLimitExceedAction
}

// RateLimitExceedAction defines the action to take when a message rate limit is exceeded.
type RateLimitExceedAction int

const (
	// RateLimitAllow allows the message to be processed.
	RateLimitAllow RateLimitExceedAction = iota
	// RateLimitDisconnect disconnects the client when rate limit is exceeded.
	RateLimitDisconnect
	// RateLimitDropMessage drops the message silently (with appropriate ack for QoS > 0).
	RateLimitDropMessage
)

// RateLimitConfig defines rate and burst for a token bucket limiter tier.
// If Rate is 0 the tier is disabled.
// If Rate > 0 and Burst is 0, Burst defaults to 1 (no burst, strict rate).
type RateLimitConfig struct {
	Rate  float64 // tokens per second
	Burst int     // maximum burst size (0 = no burst when Rate > 0)
}

// enabled reports whether this tier is configured.
func (c RateLimitConfig) enabled() bool {
	return c.Rate > 0
}

// burst returns the effective burst size (at least 1 when enabled).
func (c RateLimitConfig) burst() int {
	if c.Burst > 0 {
		return c.Burst
	}
	return 1
}

// ConnectionLimiterConfig configures a TokenBucketConnectionLimiter.
type ConnectionLimiterConfig struct {
	Global RateLimitConfig // overall connection rate
	PerIP  RateLimitConfig // per-IP connection rate
}

// MessageLimiterConfig configures a TokenBucketMessageLimiter.
type MessageLimiterConfig struct {
	Global         RateLimitConfig       // overall message rate
	PerNamespace   RateLimitConfig       // per-namespace message rate (keyed by namespace)
	PerClient      RateLimitConfig       // per-client message rate (keyed by namespace+clientID)
	PerTopic       RateLimitConfig       // per-topic message rate (keyed by namespace+topic)
	PerClientTopic RateLimitConfig       // per-client-topic message rate (keyed by namespace+clientID+topic)
	ExceedAction   RateLimitExceedAction // action when rate exceeded
}

type ipLimiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// TokenBucketConnectionLimiter is a ConnectionRateLimiter using token bucket algorithm.
// It supports a global rate limit and per-IP rate limits.
type TokenBucketConnectionLimiter struct {
	globalLimiter *rate.Limiter
	perIPRate     rate.Limit
	perIPBurst    int
	mu            sync.Mutex
	ipLimiters    map[string]*ipLimiterEntry
}

// NewTokenBucketConnectionLimiter creates a new TokenBucketConnectionLimiter.
func NewTokenBucketConnectionLimiter(cfg ConnectionLimiterConfig) *TokenBucketConnectionLimiter {
	l := &TokenBucketConnectionLimiter{
		ipLimiters: make(map[string]*ipLimiterEntry),
	}
	if cfg.Global.enabled() {
		l.globalLimiter = rate.NewLimiter(rate.Limit(cfg.Global.Rate), cfg.Global.burst())
	}
	if cfg.PerIP.enabled() {
		l.perIPRate = rate.Limit(cfg.PerIP.Rate)
		l.perIPBurst = cfg.PerIP.burst()
	}
	return l
}

// AllowConnection checks if a new connection is allowed.
func (l *TokenBucketConnectionLimiter) AllowConnection(conn net.Conn) bool {
	if l.globalLimiter != nil && !l.globalLimiter.Allow() {
		return false
	}

	if l.perIPBurst > 0 {
		ip := extractIP(conn.RemoteAddr())

		l.mu.Lock()
		entry, exists := l.ipLimiters[ip]
		if !exists {
			entry = &ipLimiterEntry{
				limiter: rate.NewLimiter(l.perIPRate, l.perIPBurst),
			}
			l.ipLimiters[ip] = entry
		}
		entry.lastSeen = time.Now()
		l.mu.Unlock()

		if !entry.limiter.Allow() {
			return false
		}
	}

	return true
}

// Cleanup removes stale per-IP limiter entries older than maxAge.
func (l *TokenBucketConnectionLimiter) Cleanup(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)

	l.mu.Lock()
	defer l.mu.Unlock()

	for ip, entry := range l.ipLimiters {
		if entry.lastSeen.Before(cutoff) {
			delete(l.ipLimiters, ip)
		}
	}
}

type clientLimiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// TokenBucketMessageLimiter is a MessageRateLimiter using token bucket algorithm.
// It supports global, per-namespace, per-client, per-topic, and per-client-topic rate limits.
type TokenBucketMessageLimiter struct {
	globalLimiter       *rate.Limiter
	perNamespaceRate    rate.Limit
	perNamespaceBurst   int
	perClientRate       rate.Limit
	perClientBurst      int
	perTopicRate        rate.Limit
	perTopicBurst       int
	perClientTopicRate  rate.Limit
	perClientTopicBurst int
	exceedAction        RateLimitExceedAction
	mu                  sync.Mutex
	namespaceLimiters   map[string]*clientLimiterEntry
	clientLimiters      map[string]*clientLimiterEntry
	topicLimiters       map[string]*clientLimiterEntry
	clientTopicLimiters map[string]*clientLimiterEntry
}

// NewTokenBucketMessageLimiter creates a new TokenBucketMessageLimiter.
func NewTokenBucketMessageLimiter(cfg MessageLimiterConfig) *TokenBucketMessageLimiter {
	l := &TokenBucketMessageLimiter{
		exceedAction:        cfg.ExceedAction,
		namespaceLimiters:   make(map[string]*clientLimiterEntry),
		clientLimiters:      make(map[string]*clientLimiterEntry),
		topicLimiters:       make(map[string]*clientLimiterEntry),
		clientTopicLimiters: make(map[string]*clientLimiterEntry),
	}
	if cfg.Global.enabled() {
		l.globalLimiter = rate.NewLimiter(rate.Limit(cfg.Global.Rate), cfg.Global.burst())
	}
	if cfg.PerNamespace.enabled() {
		l.perNamespaceRate = rate.Limit(cfg.PerNamespace.Rate)
		l.perNamespaceBurst = cfg.PerNamespace.burst()
	}
	if cfg.PerClient.enabled() {
		l.perClientRate = rate.Limit(cfg.PerClient.Rate)
		l.perClientBurst = cfg.PerClient.burst()
	}
	if cfg.PerTopic.enabled() {
		l.perTopicRate = rate.Limit(cfg.PerTopic.Rate)
		l.perTopicBurst = cfg.PerTopic.burst()
	}
	if cfg.PerClientTopic.enabled() {
		l.perClientTopicRate = rate.Limit(cfg.PerClientTopic.Rate)
		l.perClientTopicBurst = cfg.PerClientTopic.burst()
	}
	return l
}

// AllowMessage checks if a message from the given client is allowed.
// Returns RateLimitAllow if the message should be processed, or the configured
// exceed action if rate limited.
// Checks are applied in order: global, per-namespace, per-client, per-topic, per-client-topic.
func (l *TokenBucketMessageLimiter) AllowMessage(clientID string, namespace string, topic string) RateLimitExceedAction {
	if l.globalLimiter != nil && !l.globalLimiter.Allow() {
		return l.exceedAction
	}

	l.mu.Lock()

	if l.perNamespaceBurst > 0 {
		entry, exists := l.namespaceLimiters[namespace]
		if !exists {
			entry = &clientLimiterEntry{
				limiter: rate.NewLimiter(l.perNamespaceRate, l.perNamespaceBurst),
			}
			l.namespaceLimiters[namespace] = entry
		}
		entry.lastSeen = time.Now()
		if !entry.limiter.Allow() {
			l.mu.Unlock()
			return l.exceedAction
		}
	}

	if l.perClientBurst > 0 {
		key := NamespaceKey(namespace, clientID)
		entry, exists := l.clientLimiters[key]
		if !exists {
			entry = &clientLimiterEntry{
				limiter: rate.NewLimiter(l.perClientRate, l.perClientBurst),
			}
			l.clientLimiters[key] = entry
		}
		entry.lastSeen = time.Now()
		if !entry.limiter.Allow() {
			l.mu.Unlock()
			return l.exceedAction
		}
	}

	if l.perTopicBurst > 0 {
		key := NamespaceKey(namespace, topic)
		entry, exists := l.topicLimiters[key]
		if !exists {
			entry = &clientLimiterEntry{
				limiter: rate.NewLimiter(l.perTopicRate, l.perTopicBurst),
			}
			l.topicLimiters[key] = entry
		}
		entry.lastSeen = time.Now()
		if !entry.limiter.Allow() {
			l.mu.Unlock()
			return l.exceedAction
		}
	}

	if l.perClientTopicBurst > 0 {
		key := namespace + namespaceDelimiter + clientID + namespaceDelimiter + topic
		entry, exists := l.clientTopicLimiters[key]
		if !exists {
			entry = &clientLimiterEntry{
				limiter: rate.NewLimiter(l.perClientTopicRate, l.perClientTopicBurst),
			}
			l.clientTopicLimiters[key] = entry
		}
		entry.lastSeen = time.Now()
		if !entry.limiter.Allow() {
			l.mu.Unlock()
			return l.exceedAction
		}
	}

	l.mu.Unlock()

	return RateLimitAllow
}

// Cleanup removes stale limiter entries older than maxAge.
func (l *TokenBucketMessageLimiter) Cleanup(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)

	l.mu.Lock()
	defer l.mu.Unlock()

	for key, entry := range l.namespaceLimiters {
		if entry.lastSeen.Before(cutoff) {
			delete(l.namespaceLimiters, key)
		}
	}

	for key, entry := range l.clientLimiters {
		if entry.lastSeen.Before(cutoff) {
			delete(l.clientLimiters, key)
		}
	}

	for key, entry := range l.topicLimiters {
		if entry.lastSeen.Before(cutoff) {
			delete(l.topicLimiters, key)
		}
	}

	for key, entry := range l.clientTopicLimiters {
		if entry.lastSeen.Before(cutoff) {
			delete(l.clientTopicLimiters, key)
		}
	}
}

func extractIP(addr net.Addr) string {
	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		return tcpAddr.IP.String()
	}
	return addr.String()
}
