# mqttv5

A complete MQTT implementation in Go for building clients and brokers.

Implements the [MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html) and [MQTT Version 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html) OASIS Standards.

## Features

- All 15 MQTT v5.0 control packet types
- MQTT 3.1.1 backward compatibility (server opt-in, client version fallback)
- Complete properties system (42 property identifiers)
- QoS 0, 1, 2 message flows with state machines
- Topic matching with wildcard support (`+`, `#`)
- Shared subscriptions (`$share/group/topic`)
- Multi-tenancy with namespace isolation
- Message interceptors (producer/consumer)
- Broker bridging with P2MP support
- Transport: TCP, TLS, WebSocket, WSS, Unix Socket, QUIC
- Pluggable authentication and authorization
- mTLS with certificate identity mapping
- Session persistence interface
- Retained messages
- Will messages
- Keep-alive management
- Flow control per MQTT v5.0 spec
- Rate limiting (connection and message)
- Server introspection (client info, subscription info, topic metrics)
- Metrics collection
- Multi-server support with round-robin selection
- Dynamic service discovery (DNS SRV, registries)

## Installation

```bash
go get github.com/vitalvas/mqttv5
```

## Quick Start

### Client

```go
package main

import (
    "fmt"
    "github.com/vitalvas/mqttv5"
)

func main() {
    client, err := mqttv5.Dial(
        mqttv5.WithServers("tcp://localhost:1883"),
        mqttv5.WithClientID("my-client"),
        mqttv5.WithKeepAlive(60),
    )
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Subscribe
    client.Subscribe("sensors/#", 1, func(msg *mqttv5.Message) {
        fmt.Printf("Received: %s\n", msg.Payload)
    })

    // Publish
    client.Publish(&mqttv5.Message{
        Topic:   "sensors/temperature",
        Payload: []byte("23.5"),
        QoS:     mqttv5.QoS1,
    })
}
```

### Broker

```go
package main

import (
    "log"
    "net"
    "github.com/vitalvas/mqttv5"
)

func main() {
    listener, _ := net.Listen("tcp", ":1883")

    srv := mqttv5.NewServer(
        mqttv5.WithListener(listener),
        mqttv5.OnConnect(func(c *mqttv5.ServerClient) {
            log.Printf("Client connected: %s", c.ClientID())
        }),
        mqttv5.OnMessage(func(c *mqttv5.ServerClient, m *mqttv5.Message) {
            log.Printf("Message: %s -> %s", m.Topic, m.Payload)
        }),
    )

    srv.ListenAndServe()
}
```

## Protocol Version

### Server

By default, the server only accepts MQTT v5 connections. To also accept MQTT 3.1.1 clients (ESP32, ESP8266, legacy services):

```go
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithAcceptProtocolVersions(mqttv5.ProtocolV5, mqttv5.ProtocolV311),
)
```

MQTT 3.1.1 and v5 clients share the same topic namespace and can exchange messages. v5-only features (properties, enhanced auth, topic aliases) are automatically stripped when delivering to v3.1.1 clients.

For v3.1.1 clients, session lifetime follows the `CleanSession` flag: `CleanSession=false` keeps the session (subscriptions and queued QoS messages) indefinitely until the client reconnects with `CleanSession=true`. MQTT 3.1.1 has no server-to-client DISCONNECT packet, so the server closes v3.1.1 connections without sending one.

### Client

By default, the client connects using MQTT v5. To support servers that only speak v3.1.1, configure version fallback:

```go
// Try v5 first, fall back to v3.1.1 if server rejects
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithProtocolVersions(mqttv5.ProtocolV5, mqttv5.ProtocolV311),
)

// Strict v3.1.1 only
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithProtocolVersions(mqttv5.ProtocolV311),
)
```

The client retries with the next version only when the server indicates "Unsupported Protocol Version". This covers both a valid v5 CONNACK reason code `0x84` and the v3.1.1 CONNACK return code `0x01` that real v3.1.1 brokers send in response to a v5 CONNECT. Other errors (bad credentials, server busy) fail immediately.

See [MQTT 5.0 vs 3.1.1 feature matrix](docs/v5_vs_v311.md) for a full list of which features are available in each protocol version.

## Transport Options

### TLS

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tls://localhost:8883"),
    mqttv5.WithTLS(&tls.Config{
        InsecureSkipVerify: true,
    }),
)
```

### WebSocket

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("ws://localhost:8080/mqtt"),
)
```

### Unix Socket

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("unix:///var/run/mqtt.sock"),
)
```

### QUIC

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers("quic://localhost:8883"),
    mqttv5.WithTLS(&tls.Config{NextProtos: []string{"mqtt"}}),
)
```

## Multi-Server and Service Discovery

### Multiple Servers (Round-Robin)

Connect to multiple servers with automatic failover:

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServers(
        "tcp://broker1:1883",
        "tcp://broker2:1883",
        "tcp://broker3:1883",
    ),
    mqttv5.WithAutoReconnect(true),
)
```

Servers are tried in round-robin order on each connection/reconnection attempt.

### Dynamic Service Discovery

Use a resolver function for dynamic server discovery (DNS SRV, Consul, etc.):

```go
resolver := func(ctx context.Context) ([]string, error) {
    // Example: DNS SRV lookup
    _, addrs, err := net.DefaultResolver.LookupSRV(ctx, "mqtt", "tcp", "example.com")
    if err != nil {
        return nil, err
    }

    servers := make([]string, len(addrs))
    for i, addr := range addrs {
        servers[i] = fmt.Sprintf("tcp://%s:%d", addr.Target, addr.Port)
    }
    return servers, nil
}

client, _ := mqttv5.Dial(
    mqttv5.WithServerResolver(resolver),
    mqttv5.WithAutoReconnect(true),
)
```

The resolver is called before each connection attempt, enabling dynamic discovery.

### Combining Static and Dynamic

Use both for fallback behavior:

```go
client, _ := mqttv5.Dial(
    mqttv5.WithServerResolver(dynamicResolver),  // Tried first
    mqttv5.WithServers("tcp://fallback:1883"),   // Fallback if resolver fails
)
```

## Authentication

Called once when a client sends a CONNECT packet. Validates credentials and assigns namespace:

```go
type MyAuth struct{}

func (a *MyAuth) Authenticate(ctx context.Context, authCtx *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
    if authCtx.Username == "admin" && string(authCtx.Password) == "secret" {
        return &mqttv5.AuthResult{Success: true}, nil
    }
    return &mqttv5.AuthResult{
        Success:    false,
        ReasonCode: mqttv5.ReasonBadUserNameOrPassword,
    }, nil
}

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithServerAuth(&MyAuth{}),
)
```

## Authorization

Called on every PUBLISH and SUBSCRIBE operation to check if the action is allowed:

```go
type MyAuthz struct{}

func (a *MyAuthz) Authorize(ctx context.Context, authzCtx *mqttv5.AuthzContext) (*mqttv5.AuthzResult, error) {
    if authzCtx.Action == mqttv5.AuthzPublish && authzCtx.Topic == "admin/logs" {
        return &mqttv5.AuthzResult{Allowed: false}, nil
    }
    return &mqttv5.AuthzResult{Allowed: true}, nil
}

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithServerAuthz(&MyAuthz{}),
)
```

## mTLS Authentication

Authenticate clients using TLS certificates with identity mapping:

```go
// Create TLS listener requiring client certificates
tlsConfig := &tls.Config{
    Certificates: []tls.Certificate{serverCert},
    ClientCAs:    caPool,
    ClientAuth:   tls.RequireAndVerifyClientCert,
}
listener, _ := tls.Listen("tcp", ":8883", tlsConfig)

// Map certificate CN to username, OU to namespace
mapper := mqttv5.TLSIdentityMapperFunc(func(_ context.Context, state *tls.ConnectionState) (*mqttv5.TLSIdentity, error) {
    if state == nil || len(state.PeerCertificates) == 0 {
        return nil, nil
    }
    cert := state.PeerCertificates[0]
    identity := &mqttv5.TLSIdentity{Username: cert.Subject.CommonName}
    if len(cert.Subject.OrganizationalUnit) > 0 {
        identity.Namespace = cert.Subject.OrganizationalUnit[0]
    }
    return identity, nil
})

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithTLSIdentityMapper(mapper),
    mqttv5.WithServerAuth(&MTLSAuthenticator{}),
)
```

Access certificate in authenticator and set session expiry:

```go
func (a *MTLSAuth) Authenticate(ctx context.Context, c *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
    if c.TLSIdentity == nil {
        return &mqttv5.AuthResult{Success: false, ReasonCode: mqttv5.ReasonNotAuthorized}, nil
    }

    cert := c.TLSConnectionState.PeerCertificates[0]
    return &mqttv5.AuthResult{
        Success:       true,
        Namespace:     c.TLSIdentity.Namespace,
        SessionExpiry: cert.NotAfter, // Auto-disconnect when cert expires
    }, nil
}
```

See [mTLS documentation](docs/mtls.md) for more details.

## Multi-tenancy

Isolate clients into separate namespaces. Clients in different namespaces cannot see each other's messages:

```go
type TenantAuth struct{}

func (a *TenantAuth) Authenticate(ctx context.Context, authCtx *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
    // Extract tenant from username (e.g., "user@tenant1")
    parts := strings.Split(authCtx.Username, "@")
    if len(parts) != 2 {
        return &mqttv5.AuthResult{Success: false}, nil
    }

    _, tenant := parts[0], parts[1]

    // Validate credentials...

    return &mqttv5.AuthResult{
        Success:   true,
        Namespace: tenant, // Isolate client to this namespace
    }, nil
}

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithServerAuth(&TenantAuth{}),
)
```

Clients authenticated with namespace `tenant1` can only publish/subscribe within that namespace.
Messages from `tenant1` are invisible to `tenant2`.

## Interceptors

Intercept and modify messages before sending or after receiving:

```go
type LoggingInterceptor struct{}

func (i *LoggingInterceptor) OnSend(msg *mqttv5.Message) *mqttv5.Message {
    log.Printf("Sending: %s", msg.Topic)
    return msg
}

func (i *LoggingInterceptor) OnConsume(msg *mqttv5.Message) *mqttv5.Message {
    log.Printf("Received: %s", msg.Topic)
    return msg
}

client, _ := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithProducerInterceptors(&LoggingInterceptor{}),
    mqttv5.WithConsumerInterceptors(&LoggingInterceptor{}),
)
```

## Session Management

```go
store := mqttv5.NewMemorySessionStore()

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithSessionStore(store),
)
```

## Metrics

```go
metrics := mqttv5.NewMetrics() // Uses expvar

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithMetrics(metrics),
)
```

## Rate Limiting

Control the rate of incoming connections and published messages:

```go
connLimiter := mqttv5.NewTokenBucketConnectionLimiter(
    mqttv5.ConnectionLimiterConfig{
        Global: mqttv5.RateLimitConfig{Rate: 100, Burst: 200},
        PerIP:  mqttv5.RateLimitConfig{Rate: 10, Burst: 20},
    },
)

msgLimiter := mqttv5.NewTokenBucketMessageLimiter(
    mqttv5.MessageLimiterConfig{
        Global:    mqttv5.RateLimitConfig{Rate: 10000, Burst: 20000},
        PerClient: mqttv5.RateLimitConfig{Rate: 100, Burst: 200},
    },
)

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithConnectionRateLimiter(connLimiter),
    mqttv5.WithMessageRateLimiter(msgLimiter),
)
```

See [rate limiting documentation](docs/rate_limiting.md) for all tiers and options.

## Server Stats

Inspect connected clients and subscriptions at runtime:

```go
// List namespaces
namespaces := srv.Namespaces()

// List clients (optionally filtered by namespace)
clients := srv.ClientsInfo("tenant1")
for _, c := range clients {
    fmt.Printf("%s: uptime=%s msgs_in=%d\n", c.ClientID, c.Uptime, c.MessagesIn)
}

// Subscription summary
summary := srv.GetSubscriptionSummary("tenant1")
fmt.Printf("Subscriptions: %d, Clients: %d\n",
    summary.TotalSubscriptions, summary.TotalClients)
```

See [server stats documentation](docs/server-stats.md) for full details.

## Bridging

Connect two MQTT brokers and forward messages between them with topic remapping and loop detection.

### Simple Bridge

```go
bridge, _ := mqttv5.NewBridge(localServer, mqttv5.BridgeConfig{
    RemoteAddr:       "tcp://remote-broker:1883",
    ClientID:         "bridge-1",
    ProtocolVersions: []mqttv5.ProtocolVersion{mqttv5.ProtocolV5, mqttv5.ProtocolV311},
    Topics: []mqttv5.BridgeTopic{
        {
            LocalPrefix:  "local/sensors",
            RemotePrefix: "remote/sensors",
            Direction:    mqttv5.BridgeDirectionBoth,
            QoS:          mqttv5.QoS1,
        },
    },
})

bridge.Start()
defer bridge.Stop()

// Forward local messages to remote (call from OnMessage callback)
localServer.OnMessage(func(c *mqttv5.ServerClient, msg *mqttv5.Message) {
    bridge.ForwardToRemote(msg)
})
```

### Point-to-Multipoint (P2MP)

Use `BridgeManager` to coordinate multiple bridges:

```go
manager := mqttv5.NewBridgeManager(localServer)

// Add bridges to different remote brokers
manager.Add(mqttv5.BridgeConfig{
    RemoteAddr: "tcp://broker-a:1883",
    ClientID:   "bridge-a",
    Topics: []mqttv5.BridgeTopic{
        {LocalPrefix: "sensors", RemotePrefix: "incoming/sensors", Direction: mqttv5.BridgeDirectionOut},
    },
})

manager.Add(mqttv5.BridgeConfig{
    RemoteAddr: "tcp://broker-b:1883",
    ClientID:   "bridge-b",
    Topics: []mqttv5.BridgeTopic{
        {LocalPrefix: "commands", RemotePrefix: "device/commands", Direction: mqttv5.BridgeDirectionIn},
    },
})

manager.StartAll()
defer manager.StopAll()

// Forward to all matching bridges
localServer.OnMessage(func(c *mqttv5.ServerClient, msg *mqttv5.Message) {
    manager.ForwardToRemote(msg)
})
```

### Custom Topic Remapping

```go
bridge, _ := mqttv5.NewBridge(server, mqttv5.BridgeConfig{
    RemoteAddr: "tcp://remote:1883",
    Topics: []mqttv5.BridgeTopic{
        {LocalPrefix: "local", RemotePrefix: "remote", Direction: mqttv5.BridgeDirectionBoth},
    },
    TopicRemapper: func(topic string, direction mqttv5.BridgeDirection) string {
        if direction == mqttv5.BridgeDirectionOut {
            return "custom/" + topic
        }
        return "" // Fall back to default prefix remapping
    },
})
```

### Bridge Metrics

Bridge metrics are reported through the server's `MetricsCollector`:

```go
// Server metrics include bridge counters
metrics := mqttv5.NewMetrics()
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithMetrics(metrics),
)

// Bridge manager state
managerMetrics := manager.Metrics()
fmt.Printf("Total bridges: %d, Running: %d\n", managerMetrics.TotalBridges, managerMetrics.RunningBridges)
```
