# Interceptors

Interceptors allow interception and modification of MQTT messages during
processing. They follow a chain-of-responsibility pattern where each
interceptor receives the message from the previous one in the chain.

## Interfaces

### ProducerInterceptor

Intercepts outbound messages before delivery to subscribers.

```go
type ProducerInterceptor interface {
    OnSend(msg *Message) *Message
}
```

### ConsumerInterceptor

Intercepts inbound messages after receipt but before delivery to handlers.

```go
type ConsumerInterceptor interface {
    OnConsume(msg *Message) *Message
}
```

## Processing Order

### Server-side ConsumerInterceptor

When the server receives a PUBLISH from a client:

1. Topic alias resolution and validation
2. QoS and retain flag validation
3. Flow control check (Receive Maximum)
4. Authorization check (Authorizer.Authorize)
5. PUBACK sent (QoS 1 only)
6. **ConsumerInterceptor.OnConsume called**
7. PUBREC sent and message tracked (QoS 2 only)
8. OnMessage callback (QoS 0/1; QoS 2 on PUBREL)
9. Message published to subscribers

### Server-side ProducerInterceptor

When the server delivers a message to subscribers:

1. Message matched to subscriber
2. **ProducerInterceptor.OnSend called**
3. Message delivered to subscriber

## Behavior

- Interceptors are called in registration order
- Each interceptor receives output from the previous interceptor
- Return `nil` to filter/drop the message
- Panics are recovered; the original message passes through unchanged
- Messages are **not** copied; modifications affect the original

## Server Examples

### Logging Interceptor

```go
type LoggingInterceptor struct {
    logger *log.Logger
}

func (i *LoggingInterceptor) OnConsume(msg *mqttv5.Message) *mqttv5.Message {
    i.logger.Printf("Received: topic=%s qos=%d size=%d",
        msg.Topic, msg.QoS, len(msg.Payload))
    return msg
}

func (i *LoggingInterceptor) OnSend(msg *mqttv5.Message) *mqttv5.Message {
    i.logger.Printf("Sending: topic=%s qos=%d size=%d",
        msg.Topic, msg.QoS, len(msg.Payload))
    return msg
}
```

### Metrics Interceptor

```go
type MetricsInterceptor struct {
    received atomic.Int64
    sent     atomic.Int64
}

func (i *MetricsInterceptor) OnConsume(msg *mqttv5.Message) *mqttv5.Message {
    i.received.Add(1)
    return msg
}

func (i *MetricsInterceptor) OnSend(msg *mqttv5.Message) *mqttv5.Message {
    i.sent.Add(1)
    return msg
}

func (i *MetricsInterceptor) Stats() (received, sent int64) {
    return i.received.Load(), i.sent.Load()
}
```

### Topic Filter Interceptor

```go
type TopicFilterInterceptor struct {
    blocked []string
}

func (i *TopicFilterInterceptor) OnConsume(msg *mqttv5.Message) *mqttv5.Message {
    for _, prefix := range i.blocked {
        if strings.HasPrefix(msg.Topic, prefix) {
            return nil // Drop message
        }
    }
    return msg
}
```

### Payload Validation Interceptor

```go
type JSONValidator struct{}

func (i *JSONValidator) OnConsume(msg *mqttv5.Message) *mqttv5.Message {
    if !json.Valid(msg.Payload) {
        return nil // Drop invalid JSON
    }
    return msg
}
```

### Server Registration

```go
logging := &LoggingInterceptor{logger: log.Default()}
metrics := &MetricsInterceptor{}
filter := &TopicFilterInterceptor{blocked: []string{"$SYS/", "internal/"}}

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithServerConsumerInterceptors(
        logging,
        filter,
        metrics,
    ),
    mqttv5.WithServerProducerInterceptors(
        logging,
        metrics,
    ),
)
```

## Client Examples

### Topic Prefix Interceptor

```go
type TopicPrefixInterceptor struct {
    prefix string
}

func (i *TopicPrefixInterceptor) OnSend(msg *mqttv5.Message) *mqttv5.Message {
    msg.Topic = i.prefix + "/" + msg.Topic
    return msg
}
```

### Payload Transform Interceptor

```go
type CompressionInterceptor struct{}

func (i *CompressionInterceptor) OnSend(msg *mqttv5.Message) *mqttv5.Message {
    var buf bytes.Buffer
    w := gzip.NewWriter(&buf)
    w.Write(msg.Payload)
    w.Close()
    msg.Payload = buf.Bytes()
    msg.ContentType = "application/gzip"
    return msg
}

func (i *CompressionInterceptor) OnConsume(msg *mqttv5.Message) *mqttv5.Message {
    if msg.ContentType != "application/gzip" {
        return msg
    }
    r, err := gzip.NewReader(bytes.NewReader(msg.Payload))
    if err != nil {
        return msg
    }
    defer r.Close()
    data, _ := io.ReadAll(r)
    msg.Payload = data
    msg.ContentType = ""
    return msg
}
```

### Rate Limiting Interceptor

```go
type RateLimiterInterceptor struct {
    limiter *rate.Limiter
}

func NewRateLimiterInterceptor(rps float64, burst int) *RateLimiterInterceptor {
    return &RateLimiterInterceptor{
        limiter: rate.NewLimiter(rate.Limit(rps), burst),
    }
}

func (i *RateLimiterInterceptor) OnSend(msg *mqttv5.Message) *mqttv5.Message {
    if !i.limiter.Allow() {
        return nil // Drop message if rate exceeded
    }
    return msg
}
```

### Client Registration

```go
prefix := &TopicPrefixInterceptor{prefix: "devices/sensor-01"}
compress := &CompressionInterceptor{}
limiter := NewRateLimiterInterceptor(100, 10) // 100 msg/s, burst 10

client, err := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithClientID("sensor-01"),
    mqttv5.WithProducerInterceptors(
        prefix,
        compress,
        limiter,
    ),
    mqttv5.WithConsumerInterceptors(
        compress,
    ),
)
```

## Common Use Cases

| Use Case | Interceptor Type | Description |
|----------|------------------|-------------|
| Logging | Both | Log message metadata for debugging |
| Metrics | Both | Count messages, track sizes |
| Filtering | Consumer | Drop unwanted messages |
| Validation | Consumer | Validate payload format |
| Transformation | Both | Modify payload (compress, encrypt) |
| Enrichment | Both | Add user properties, timestamps |
| Rate limiting | Producer | Control message throughput |
| Topic rewriting | Producer | Add prefixes, transform topics |
