# Client Options

All options for configuring an MQTT v5.0 client.

## Connection

| Option | Default | Description |
|--------|---------|-------------|
| `WithServers(servers...)` | - | Server addresses (tcp://host:port) |
| `WithServerResolver(fn)` | - | Dynamic server discovery function |
| `WithClientID(id)` | "" | Client identifier |
| `WithCredentials(user, pass)` | - | Username and password |
| `WithKeepAlive(seconds)` | 60 | Keep-alive interval |
| `WithCleanStart(bool)` | true | Start with clean session |
| `WithTLS(config)` | nil | TLS configuration |

## Timeouts

| Option | Default | Description |
|--------|---------|-------------|
| `WithConnectTimeout(d)` | 10s | Connection timeout |
| `WithWriteTimeout(d)` | 5s | Write operation timeout |
| `WithReadTimeout(d)` | 5s | Read operation timeout |

## Reconnection

| Option | Default | Description |
|--------|---------|-------------|
| `WithAutoReconnect(bool)` | false | Enable auto-reconnect |
| `WithMaxReconnects(n)` | 10 | Max reconnect attempts (-1 unlimited) |
| `WithReconnectBackoff(d)` | 1s | Initial backoff duration |
| `WithMaxBackoff(d)` | 60s | Maximum backoff duration |
| `WithBackoffStrategy(fn)` | exp | Custom backoff function |

## Will Message

| Option | Default | Description |
|--------|---------|-------------|
| `WithWill(topic, payload, retain, qos)` | - | Will message |
| `WithWillProps(props)` | - | Will message properties |

## Limits

| Option | Default | Description |
|--------|---------|-------------|
| `WithMaxPacketSize(size)` | 4MB | Maximum packet size |
| `WithMaxSubscriptions(n)` | 0 | Max subscriptions (0 unlimited) |
| `WithReceiveMaximum(n)` | 65535 | Max inflight QoS 1/2 messages |
| `WithTopicAliasMaximum(n)` | 0 | Max topic aliases |

## Session

| Option | Default | Description |
|--------|---------|-------------|
| `WithSessionExpiryInterval(sec)` | 0 | Session expiry in seconds |
| `WithClientSessionFactory(fn)` | memory | Custom session factory |
| `WithUserProperties(map)` | - | CONNECT user properties |

## Interceptors

| Option | Default | Description |
|--------|---------|-------------|
| `WithProducerInterceptors(i...)` | - | Outbound message interceptors |
| `WithConsumerInterceptors(i...)` | - | Inbound message interceptors |

## Authentication

| Option | Default | Description |
|--------|---------|-------------|
| `WithEnhancedAuthentication(auth)` | - | SASL-style authenticator |

## Events

| Option | Default | Description |
|--------|---------|-------------|
| `OnEvent(handler)` | - | Lifecycle event handler |

## Examples

### Basic Connection

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithClientID("my-client"),
)
```

### With Authentication

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithCredentials("user", "password"),
)
```

### TLS Connection

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tls://localhost:8883"),
    mqttv5.WithTLS(&tls.Config{InsecureSkipVerify: true}),
)
```

### Auto-Reconnect

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithAutoReconnect(true),
    mqttv5.WithMaxReconnects(-1),
)
```

### Multiple Servers

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://broker1:1883", "tcp://broker2:1883"),
    mqttv5.WithAutoReconnect(true),
)
```

### Dynamic Discovery

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServerResolver(func(ctx context.Context) ([]string, error) {
        return []string{"tcp://discovered:1883"}, nil
    }),
)
```

### With Will Message

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithWill("client/status", []byte("offline"), true, mqttv5.QoS1),
)
```

### Persistent Session

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithClientID("persistent-client"),
    mqttv5.WithCleanStart(false),
    mqttv5.WithSessionExpiryInterval(3600),
)
```

### Event Handler

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.OnEvent(func(c *mqttv5.Client, ev error) {
        if errors.Is(ev, mqttv5.ErrConnected) {
            log.Println("Connected")
        }
    }),
)
```

### With Interceptors

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithProducerInterceptors(&LoggingInterceptor{}),
    mqttv5.WithConsumerInterceptors(&MetricsInterceptor{}),
)
```

### WebSocket

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("ws://localhost:8080/mqtt"),
)
```

### QUIC

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("quic://localhost:8883"),
    mqttv5.WithTLS(&tls.Config{NextProtos: []string{"mqtt"}}),
)
```
