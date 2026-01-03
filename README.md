# mqttv5

A complete MQTT v5.0 implementation in Go for building clients and brokers.

Implements the [MQTT Version 5.0 OASIS Standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html).

## Features

- All 15 MQTT v5.0 control packet types
- Complete properties system (42 property identifiers)
- QoS 0, 1, 2 message flows with state machines
- Topic matching with wildcard support (`+`, `#`)
- Shared subscriptions (`$share/group/topic`)
- Multi-tenancy with namespace isolation
- Message interceptors (producer/consumer)
- Transport: TCP, TLS, WebSocket, WSS, Unix Socket, QUIC
- Pluggable authentication and authorization
- Session persistence interface
- Retained messages
- Will messages
- Keep-alive management
- Flow control per MQTT v5.0 spec
- Metrics collection

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
    client, err := mqttv5.Dial("tcp://localhost:1883",
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
        QoS:     1,
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

## Transport Options

### TLS

```go
client, _ := mqttv5.Dial("tls://localhost:8883",
    mqttv5.WithTLS(&tls.Config{
        InsecureSkipVerify: true,
    }),
)
```

### WebSocket

```go
client, _ := mqttv5.Dial("ws://localhost:8080/mqtt")
```

### Unix Socket

```go
client, _ := mqttv5.Dial("unix:///var/run/mqtt.sock")
```

### QUIC

```go
client, _ := mqttv5.DialQUIC("localhost:8883", &tls.Config{}, nil)
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

Clients authenticated with namespace `tenant1` can only publish/subscribe within that namespace. Messages from `tenant1` are invisible to `tenant2`.

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

client, _ := mqttv5.Dial("tcp://localhost:1883",
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
