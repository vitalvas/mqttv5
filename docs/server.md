# Server Options

All options for configuring an MQTT server/broker.

## Listeners

| Option | Default | Description |
|--------|---------|-------------|
| `WithListener(listener)` | - | Add a net.Listener |

## Storage

| Option | Default | Description |
|--------|---------|-------------|
| `WithSessionStore(store)` | memory | Session persistence store |
| `WithSessionFactory(fn)` | memory | Custom session factory |
| `WithRetainedStore(store)` | memory | Retained messages store |

## Authentication & Authorization

| Option | Default | Description |
|--------|---------|-------------|
| `WithServerAuth(auth)` | nil | Authenticator interface |
| `WithEnhancedAuth(auth)` | nil | SASL-style authenticator |
| `WithServerAuthz(authz)` | nil | Authorizer interface |
| `WithTLSIdentityMapper(mapper)` | nil | TLS certificate identity mapper |
| `WithNamespaceValidator(fn)` | default | Namespace validation function |
| `WithAuthTimeout(d)` | 15s | Deadline for auth/authz callbacks (0 disables) |

## Limits

| Option | Default | Description |
|--------|---------|-------------|
| `WithServerMaxPacketSize(size)` | 4MB | Maximum packet size |
| `WithMaxConnections(n)` | 10000 | Max concurrent connections (0 disables the cap) |
| `WithServerWriteTimeout(d)` | 30s | Per-packet server-side write deadline (0 disables) |
| `WithServerReceiveMaximum(n)` | 65535 | Max inflight QoS 1/2 |
| `WithServerTopicAliasMax(n)` | 0 | Max topic aliases |
| `WithServerKeepAlive(sec)` | 0 | Override client keep-alive |

## Rate Limiting

| Option | Default | Description |
|--------|---------|-------------|
| `WithConnectionRateLimiter(limiter)` | nil | Connection rate limiter interface |
| `WithMessageRateLimiter(limiter)` | nil | Message rate limiter interface |

## Protocol

| Option | Default | Description |
|--------|---------|-------------|
| `WithAcceptProtocolVersions(v...)` | `[ProtocolV5]` | Accepted protocol versions |

## Capabilities

| Option | Default | Description |
|--------|---------|-------------|
| `WithMaxQoS(qos)` | 2 | Maximum QoS level (0, 1, 2) |
| `WithRetainAvailable(bool)` | true | Allow retained messages |
| `WithWildcardSubAvailable(bool)` | true | Allow wildcard subscriptions |
| `WithSubIDAvailable(bool)` | true | Allow subscription IDs |
| `WithSharedSubAvailable(bool)` | true | Allow shared subscriptions |

## Callbacks

Multiple callbacks can be registered for the same event. They are called in registration order.

| Option | Default | Description |
|--------|---------|-------------|
| `OnConnect(fn...)` | - | Client connected callbacks |
| `OnConnectFailed(fn...)` | - | Connection failed callbacks |
| `OnDisconnect(fn...)` | - | Client disconnected callbacks |
| `OnMessage(fn...)` | - | Message received callbacks |
| `OnSubscribe(fn...)` | - | Subscribe request callbacks |
| `OnUnsubscribe(fn...)` | - | Unsubscribe request callbacks |

## Interceptors

| Option | Default | Description |
|--------|---------|-------------|
| `WithServerProducerInterceptors(i...)` | - | Outbound message interceptors |
| `WithServerConsumerInterceptors(i...)` | - | Inbound message interceptors |

## Observability

| Option | Default | Description |
|--------|---------|-------------|
| `WithLogger(logger)` | no-op | Logger interface |
| `WithMetrics(metrics)` | no-op | Metrics collector |

## Introspection

The server provides methods to inspect connected clients and their
subscriptions at runtime. See [Server Stats](server-stats.md) for
full details, field references, and examples.

## Examples

### Basic Server

```go
listener, _ := net.Listen("tcp", ":1883")
srv := mqttv5.NewServer(mqttv5.WithListener(listener))
srv.ListenAndServe()
```

### With Callbacks

```go
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.OnConnect(func(c *mqttv5.ServerClient) {
        log.Printf("Connected: %s", c.ClientID())
    }),
    mqttv5.OnMessage(func(c *mqttv5.ServerClient, m *mqttv5.Message) {
        log.Printf("Message: %s", m.Topic)
    }),
)
```

### Multiple Callbacks

Multiple callbacks can be registered for the same event. Each `OnXxx` call
appends to the list rather than replacing previous callbacks.

```go
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    // First callback: logging
    mqttv5.OnConnect(func(c *mqttv5.ServerClient) {
        log.Printf("Connected: %s", c.ClientID())
    }),
    // Second callback: lifecycle events
    mqttv5.OnConnect(lc.OnConnect),
)
```

### Authentication

```go
type MyAuth struct{}

func (a *MyAuth) Authenticate(ctx context.Context, c *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
    if c.Username == "admin" {
        return &mqttv5.AuthResult{Success: true}, nil
    }
    return &mqttv5.AuthResult{Success: false, ReasonCode: mqttv5.ReasonBadUserNameOrPassword}, nil
}

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithServerAuth(&MyAuth{}),
)
```

### SCRAM Authentication

`NewSCRAMAuthenticator(lookup, hashes...)` performs SCRAM-SHA-1, SCRAM-SHA-256,
or SCRAM-SHA-512 over the MQTT v5 AUTH packet exchange. The lookup callback
returns pre-computed credentials for a username; passwords never reach the
broker.

```go
lookup := mqttv5.SCRAMCredentialLookupFunc(
    func(_ context.Context, username string) (*mqttv5.SCRAMCredentials, error) {
        creds, ok := userStore[username]
        if !ok {
            return nil, nil
        }
        return creds, nil
    },
)

auth := mqttv5.NewSCRAMAuthenticator(
    lookup,
    mqttv5.SCRAMHashSHA256,
    mqttv5.SCRAMHashSHA512,
)

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithEnhancedAuth(auth),
)
```

Compute and store credentials once when a user is created:

```go
salt, _ := mqttv5.GenerateSalt()
creds := mqttv5.ComputeSCRAMCredentials(
    mqttv5.SCRAMHashSHA256, "user-password", salt, 4096,
)
creds.Namespace = "team-alpha" // optional tenant assignment
```

See [SCRAM documentation](scram.md) for the full handshake, mechanism choice,
and security properties.

### Authorization

```go
type MyAuthz struct{}

func (a *MyAuthz) Authorize(ctx context.Context, c *mqttv5.AuthzContext) (*mqttv5.AuthzResult, error) {
    if c.Action == mqttv5.AuthzPublish && c.Topic == "admin/secret" {
        return &mqttv5.AuthzResult{Allowed: false}, nil
    }
    return &mqttv5.AuthzResult{Allowed: true}, nil
}

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithServerAuthz(&MyAuthz{}),
)
```

### Multi-Tenancy

```go
type TenantAuth struct{}

func (a *TenantAuth) Authenticate(ctx context.Context, c *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
    return &mqttv5.AuthResult{Success: true, Namespace: "tenant1"}, nil
}

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithServerAuth(&TenantAuth{}),
)
```

### TLS Server

```go
cert, _ := tls.LoadX509KeyPair("server.crt", "server.key")
tlsListener, _ := tls.Listen("tcp", ":8883", &tls.Config{Certificates: []tls.Certificate{cert}})

srv := mqttv5.NewServer(mqttv5.WithListener(tlsListener))
```

### mTLS Server

```go
cert, _ := tls.LoadX509KeyPair("server.crt", "server.key")
caPool := x509.NewCertPool()
caPEM, _ := os.ReadFile("ca.crt")
caPool.AppendCertsFromPEM(caPEM)

tlsConfig := &tls.Config{
    Certificates: []tls.Certificate{cert},
    ClientCAs:    caPool,
    ClientAuth:   tls.RequireAndVerifyClientCert,
    MinVersion:   tls.VersionTLS12,
}
tlsListener, _ := tls.Listen("tcp", ":8883", tlsConfig)

srv := mqttv5.NewServer(
    mqttv5.WithListener(tlsListener),
    mqttv5.WithTLSIdentityMapper(&mqttv5.CommonNameMapper{}),
    mqttv5.WithServerAuth(&MTLSAuthenticator{}),
)
```

See [mTLS documentation](mtls.md) for detailed examples.

### WebSocket Server

```go
tcpListener, _ := net.Listen("tcp", ":8080")
wsListener := mqttv5.NewWebSocketListener(tcpListener, "/mqtt")

srv := mqttv5.NewServer(mqttv5.WithListener(wsListener))
```

### Multiple Listeners

```go
tcpListener, _ := net.Listen("tcp", ":1883")
wsListener := mqttv5.NewWebSocketListener(otherListener, "/mqtt")

srv := mqttv5.NewServer(
    mqttv5.WithListener(tcpListener),
    mqttv5.WithListener(wsListener),
)
```

### With Metrics

```go
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithMetrics(mqttv5.NewMetrics()),
    mqttv5.WithLogger(mqttv5.NewStdLogger(os.Stdout, mqttv5.LogLevelInfo)),
)
```

### With Interceptors

```go
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithServerConsumerInterceptors(&LoggingInterceptor{}),
    mqttv5.WithServerProducerInterceptors(&MetricsInterceptor{}),
)
```

### Restrict Capabilities

```go
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithMaxQoS(mqttv5.QoS1),
    mqttv5.WithRetainAvailable(false),
    mqttv5.WithSharedSubAvailable(false),
)
```

### Rate Limiting Example

```go
connLimiter := mqttv5.NewTokenBucketConnectionLimiter(
    mqttv5.ConnectionLimiterConfig{
        Global: mqttv5.RateLimitConfig{Rate: 100, Burst: 200},
        PerIP:  mqttv5.RateLimitConfig{Rate: 10, Burst: 20},
    },
)
msgLimiter := mqttv5.NewTokenBucketMessageLimiter(
    mqttv5.MessageLimiterConfig{
        Global:         mqttv5.RateLimitConfig{Rate: 10000, Burst: 20000},
        PerNamespace:   mqttv5.RateLimitConfig{Rate: 5000, Burst: 10000},
        PerClient:      mqttv5.RateLimitConfig{Rate: 100, Burst: 200},
        PerTopic:       mqttv5.RateLimitConfig{Rate: 500, Burst: 1000},
        PerClientTopic: mqttv5.RateLimitConfig{Rate: 10, Burst: 20},
        ExceedAction:   mqttv5.RateLimitDropMessage,
    },
)

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithConnectionRateLimiter(connLimiter),
    mqttv5.WithMessageRateLimiter(msgLimiter),
)
```

### Connection Limits

```go
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithMaxConnections(1000),
    mqttv5.WithServerMaxPacketSize(1024*1024),
    mqttv5.WithServerKeepAlive(30),
)
```

### Custom Session Store

```go
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithSessionStore(myRedisStore),
    mqttv5.WithRetainedStore(myRedisRetainedStore),
)
```
