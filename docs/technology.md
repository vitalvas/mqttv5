# Technology Stack

## Overview

MQTT v5.0 SDK providing building blocks for implementing MQTT clients and brokers. Designed for Industrial IoT (IIoT) deployments supporting up to 1 million concurrent devices.

**Specification**: [MQTT Version 5.0 - OASIS Standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)

## Language

- **Go (Golang)**: Primary implementation language
- **Minimum Version**: Go 1.21+

## SDK Scope

This SDK provides:

| Component | Description |
|-----------|-------------|
| Packet Types | All 15 MQTT v5.0 control packets with encoding/decoding |
| Properties | Complete properties system (42 property identifiers) |
| Codec | Packet serialization and deserialization |
| Transport | Interfaces and implementations for TCP, TLS, WebSocket, WSS |
| Session | Session state management interfaces and implementations |
| Topic Matching | Topic filter matching with wildcard support |
| QoS Handling | State machines for QoS 0, 1, 2 message flows |
| Message | High-level message struct for publish/receive |

Users build their own clients and brokers using these building blocks.

### Message Struct

User-facing struct with public fields:

```go
type Message struct {
    Topic                   string
    Payload                 []byte
    QoS                     byte
    Retain                  bool
    Namespace               string        // Tenant namespace for isolation
    PayloadFormat           *byte
    MessageExpiry           *uint32
    ContentType             string
    ResponseTopic           string
    CorrelationData         []byte
    UserProperties          []StringPair
    SubscriptionIdentifiers []uint32
}
```

Internal implementation structs (packets, codec buffers, etc.) use private fields.

## Package Structure

Single package design with all code in root directory:

```
mqttv5/
  - packet.go          # Packet type definitions
  - packet_*.go        # Individual packet implementations
  - properties.go      # Properties system
  - codec.go           # Encoding/decoding
  - transport.go       # Transport interfaces
  - transport_tcp.go   # TCP transport
  - transport_tls.go   # TLS transport
  - transport_ws.go    # WebSocket transport
  - session.go         # Session management
  - topic.go           # Topic matching
  - qos.go             # QoS state machines
  - ...
```

## Transport Protocols

| Protocol | Description |
|----------|-------------|
| TCP | Plain TCP connections |
| TLS | TCP with TLS encryption |
| WebSocket | WebSocket connections for web clients |
| WSS | WebSocket over TLS |

### WebSocket Embedding

WebSocket transport can be embedded into existing HTTP servers:

```go
// Initialize MQTT server with config
mqttServer := mqttv5.NewServer(opts)

// Embed into existing gorilla/mux router
router := mux.NewRouter()
router.Handle("/mqtt/server", mqttServer.WSHandler())

// Embed into standard net/http
http.Handle("/mqtt/server", mqttServer.WSHandler())
```

Supports any HTTP server compatible with `http.Handler` interface.

## API Design

| Style | Description |
|-------|-------------|
| Synchronous | Blocking operations for simple use cases |
| Asynchronous | Channel-based non-blocking operations |

## Authentication

| Type | Description |
|------|-------------|
| Interface | Pluggable Authenticator interface for custom authentication backends |
| Enhanced Auth | Support for AUTH packet exchange (SASL, challenge-response) |

Users implement their own authentication logic (LDAP, database, OAuth, etc.) via Authenticator interface.

## Multi-Tenancy / Namespaces

The SDK supports full tenant isolation via namespaces. Each namespace is completely isolated:

| Feature | Isolation |
|---------|-----------|
| Topics | Clients can only publish/subscribe within their namespace |
| Retained Messages | Retained messages are scoped to namespace |
| Sessions | Sessions are identified by namespace + clientID |
| Subscriptions | Subscription matching respects namespace boundaries |

### Configuration

Namespace is returned by the `Authenticator` during authentication:

```go
func (a *Auth) Authenticate(ctx context.Context, authCtx *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
    return &mqttv5.AuthResult{
        Success:    true,
        ReasonCode: mqttv5.ReasonSuccess,
        Namespace:  "tenant-1", // Assign tenant namespace
    }, nil
}
```

### Namespace Validation

Namespaces follow domain name rules:
- Lowercase letters (a-z), digits (0-9), hyphens (-), and dots (.)
- Labels separated by dots, each 1-63 characters
- Labels cannot start or end with hyphen
- Total length max 253 characters

Custom validation can be configured via `WithNamespaceValidator` option.

### Default Namespace

If no namespace is specified, `DefaultNamespace` ("default") is used.

## Persistence

| Type | Description |
|------|-------------|
| Interface | Pluggable interfaces for sessions, retained messages, and message store |
| In-memory | Reference implementation for testing and development |

Users implement their own persistence backends (Redis, PostgreSQL, etc.) via interfaces.

## Clustering

- Pluggable interface for custom clustering backends
- Allows integration with NATS, Kafka, Redis, or custom solutions

## Dependencies

Minimal dependency approach:

| Dependency | Purpose |
|------------|---------|
| Standard Library | Core functionality |
| gorilla/websocket | WebSocket transport support |

## Performance Targets

- Support for 1M concurrent device connections
- Optimized for IIoT workloads
- Low memory footprint per connection
- High message throughput

## MQTT v5.0 Specification Coverage

### Control Packets (15 types)

| Type | Value | Direction | Description |
|------|-------|-----------|-------------|
| CONNECT | 1 | C->S | Client connection request |
| CONNACK | 2 | S->C | Connection acknowledgment |
| PUBLISH | 3 | Both | Publish message |
| PUBACK | 4 | Both | QoS 1 acknowledgment |
| PUBREC | 5 | Both | QoS 2 received (step 1) |
| PUBREL | 6 | Both | QoS 2 release (step 2) |
| PUBCOMP | 7 | Both | QoS 2 complete (step 3) |
| SUBSCRIBE | 8 | C->S | Subscribe request |
| SUBACK | 9 | S->C | Subscribe acknowledgment |
| UNSUBSCRIBE | 10 | C->S | Unsubscribe request |
| UNSUBACK | 11 | S->C | Unsubscribe acknowledgment |
| PINGREQ | 12 | C->S | Ping request |
| PINGRESP | 13 | S->C | Ping response |
| DISCONNECT | 14 | Both | Disconnect notification |
| AUTH | 15 | Both | Authentication exchange |

### Properties (42 identifiers)

Connection, message, and server capability properties as defined in Section 2.2.2 of the specification.

### QoS Levels

| Level | Guarantee | Flow |
|-------|-----------|------|
| 0 | At most once | PUBLISH |
| 1 | At least once | PUBLISH -> PUBACK |
| 2 | Exactly once | PUBLISH -> PUBREC -> PUBREL -> PUBCOMP |

### Features

- Session management with configurable expiry
- Will messages with delay interval
- Topic aliases for bandwidth optimization
- Shared subscriptions ($share/group/topic)
- Request/Response pattern (Response Topic, Correlation Data)
- Enhanced authentication (AUTH packet exchange)
- Flow control (Receive Maximum)
- Server redirection
- Subscription identifiers
- User properties

## Testing Requirements

- TDD approach: write tests alongside each implementation step
- Minimum 70% code coverage (target 100%)
- Race detector enabled
- Tests must complete within 5 seconds
- Use `github.com/stretchr/testify` for assertions

### Test Types per Step

| Type | Purpose |
|------|---------|
| Unit tests | Correctness verification |
| Benchmark tests | Performance measurement (ns/op, B/op, allocs/op) |
| Fuzz tests | Edge cases and security validation |

## Performance Requirements

- Minimize memory allocations (zero-alloc where possible)
- Minimize CPU usage
- Use buffer pools for packet encoding/decoding
- Avoid unnecessary copying
- Profile and optimize hot paths

## Code Design Principles

- No wrappers around standard library functions
- No unnecessary abstraction layers
- Direct usage of types (net.Conn, io.Reader, etc.)
- Keep implementation simple and straightforward
