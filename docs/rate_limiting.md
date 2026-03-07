# Rate Limiting

Rate limiting controls the rate of incoming connections and published
messages. The SDK provides interfaces, types, and default token bucket
implementations.

## Interfaces

### ConnectionRateLimiter

Controls the rate of new incoming connections. Checked in the accept
loop before a connection is handed off for processing.

```go
type ConnectionRateLimiter interface {
    AllowConnection(conn net.Conn) bool
}
```

### MessageRateLimiter

Controls the rate of incoming PUBLISH messages. Checked before flow
control and authorization.

```go
type MessageRateLimiter interface {
    AllowMessage(
        clientID string, namespace string, topic string,
    ) RateLimitExceedAction
}
```

## Exceed Action

`AllowMessage` returns one of the following actions:

| Action | Behavior |
|--------|----------|
| `RateLimitAllow` | Message is allowed through |
| `RateLimitDisconnect` | Disconnect with 0x96 (Message rate too high) |
| `RateLimitDropMessage` | Drop the message (see below) |

`RateLimitDropMessage` behavior by QoS:

- QoS 0: silent drop
- QoS 1: PUBACK with 0x97 (Quota exceeded)
- QoS 2: PUBREC with 0x97 (Quota exceeded)

Connection rate limiting always closes the connection silently.

## Default Implementations

Both implementations use `golang.org/x/time/rate` (token bucket).

`RateLimitConfig` fields:

| Field | Description |
|-------|-------------|
| `Rate` | Tokens per second. Set to 0 to disable the tier. |
| `Burst` | Max burst size. 0 = no burst (defaults to 1). |

### TokenBucketConnectionLimiter

```go
limiter := mqttv5.NewTokenBucketConnectionLimiter(
    mqttv5.ConnectionLimiterConfig{
        Global: mqttv5.RateLimitConfig{Rate: 100, Burst: 200},
        PerIP:  mqttv5.RateLimitConfig{Rate: 10, Burst: 20},
    },
)
```

### TokenBucketMessageLimiter

Supports five independent tiers, checked in order:

| Tier | Key | Description |
|------|-----|-------------|
| Global | single limiter | Overall message rate |
| PerNamespace | namespace | Per-namespace rate |
| PerClient | namespace + clientID | Per-client rate |
| PerTopic | namespace + topic | Per-topic rate (shared across clients) |
| PerClientTopic | namespace + clientID + topic | Per-client-topic rate |

Checks are applied in order: global, per-namespace, per-client,
per-topic, per-client-topic. All keyed tiers use namespace, so
the same client or topic in different namespaces is tracked
independently.

```go
limiter := mqttv5.NewTokenBucketMessageLimiter(
    mqttv5.MessageLimiterConfig{
        Global:         mqttv5.RateLimitConfig{Rate: 10000, Burst: 20000},
        PerNamespace:   mqttv5.RateLimitConfig{Rate: 5000, Burst: 10000},
        PerClient:      mqttv5.RateLimitConfig{Rate: 100, Burst: 200},
        PerTopic:       mqttv5.RateLimitConfig{Rate: 500, Burst: 1000},
        PerClientTopic: mqttv5.RateLimitConfig{Rate: 10, Burst: 20},
        ExceedAction:   mqttv5.RateLimitDropMessage,
    },
)
```

### Cleanup

Default implementations track per-IP and per-client limiters in maps.
Call `Cleanup` periodically to remove stale entries:

```go
go func() {
    for range time.Tick(10 * time.Minute) {
        connLimiter.Cleanup(1 * time.Hour)
        msgLimiter.Cleanup(1 * time.Hour)
    }
}()
```

## Server Configuration

```go
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithConnectionRateLimiter(connLimiter),
    mqttv5.WithMessageRateLimiter(msgLimiter),
)
```

## Custom Implementations

Implement the interfaces for custom logic (e.g., Redis-backed
distributed rate limiting, per-topic limiting, sliding window).

```go
type RedisMessageLimiter struct {
    client *redis.Client
}

func (r *RedisMessageLimiter) AllowMessage(
    clientID string, namespace string, topic string,
) mqttv5.RateLimitExceedAction {
    key := fmt.Sprintf("ratelimit:%s:%s:%s",
        namespace, clientID, topic)
    if r.checkRedis(key) {
        return mqttv5.RateLimitAllow
    }
    return mqttv5.RateLimitDropMessage
}
```
