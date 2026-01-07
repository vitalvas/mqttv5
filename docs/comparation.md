# MQTT Broker Comparison

This document compares the mqttv5 library with other popular open-source MQTT brokers.

## Overview

| Feature | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|---------|--------|-----------|------|---------|--------|-----------|----------|------|
| Language | Go | C | Erlang | Erlang | C | Java | Erlang | Go |
| License | MIT | EPL/EDL | Apache 2.0 | Apache 2.0 | MIT | Apache 2.0 | MPL 2.0 | Apache 2.0 |
| Type | Library | Broker | Broker | Broker | Broker | Broker | Broker | Broker |
| Primary Protocol | MQTT | MQTT | MQTT | MQTT | MQTT | MQTT | AMQP | NATS |
| MQTT 3.1.1 | No | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| MQTT 5.0 | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Partial |

## Protocol Support

| Feature | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|---------|--------|-----------|------|---------|--------|-----------|----------|------|
| QoS 0 | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| QoS 1 | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| QoS 2 | Yes | Yes | Yes | Yes | Yes | Yes | Yes | No |
| Retained Messages | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Will Messages | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Will Delay | Yes | Yes | Yes | Yes | Yes | Yes | Yes | No |
| Session Expiry | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Partial |
| Topic Aliases | Yes | Yes | Yes | Yes | Yes | Yes | Yes | No |
| Shared Subscriptions | Yes | Yes | Yes | Yes | Yes | Yes | No | Yes |
| Request/Response | Yes | Yes | Yes | Yes | Yes | Yes | Yes | No |
| Flow Control | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Partial |
| Message Expiry | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Subscription IDs | Yes | Yes | Yes | Yes | Yes | Yes | Yes | No |
| User Properties | Yes | Yes | Yes | Yes | Yes | Yes | Yes | No |

## Transport Protocols

| Transport | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|-----------|--------|-----------|------|---------|--------|-----------|----------|------|
| TCP | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| TLS/SSL | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| WebSocket | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| WebSocket Secure | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Unix Socket | Yes | Yes | No | No | Yes | No | No | No |
| QUIC | Yes | No | Yes | No | Yes | No | No | No |

## Authentication

| Method | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|--------|--------|-----------|------|---------|--------|-----------|----------|------|
| Username/Password | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| TLS Certificates | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Enhanced Auth | Yes | Yes | Yes | Partial | Partial | Yes | No | No |
| SCRAM-SHA-1 | Yes | No | Yes | No | No | No | Yes | No |
| SCRAM-SHA-256 | Yes | No | Yes | No | No | No | Yes | Yes |
| SCRAM-SHA-512 | Yes | No | No | No | No | No | Yes | No |
| JWT | Pluggable | Plugin | Yes | Plugin | No | Plugin | No | Yes |
| OAuth 2.0 | Pluggable | No | Yes | Plugin | No | Plugin | Yes | No |
| LDAP | Pluggable | Plugin | Yes | Plugin | No | Plugin | Yes | No |
| Custom Auth | Yes | Plugin | Yes | Yes | Plugin | Yes | Plugin | Yes |

## Authorization

| Feature | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|---------|--------|-----------|------|---------|--------|-----------|----------|------|
| ACL Support | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Topic-level Permissions | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Publish Authorization | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Subscribe Authorization | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Custom Authorizer | Yes | Plugin | Yes | Plugin | Plugin | Plugin | Plugin | Yes |
| Dynamic ACL | Yes | Limited | Yes | Yes | Limited | Yes | Yes | Yes |

## Multi-Tenancy

| Feature | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|---------|--------|-----------|------|---------|--------|-----------|----------|------|
| Namespace Isolation | Yes | No | Limited | Limited | No | Enterprise | Yes | Yes |
| Tenant Separation | Yes | No | Yes | Limited | No | Enterprise | Yes | Yes |
| Per-Tenant Sessions | Yes | No | Yes | Limited | No | Enterprise | Yes | Yes |
| Per-Tenant Retained | Yes | No | Yes | Limited | No | Enterprise | Yes | Yes |

## Bridging

| Feature | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|---------|--------|-----------|------|---------|--------|-----------|----------|------|
| Broker Bridging | Yes | Yes | Yes | Yes | Yes | Enterprise | Yes | Yes |
| Topic Remapping | Yes | Yes | Yes | Yes | Yes | Enterprise | Yes | Yes |
| Bidirectional Bridge | Yes | Yes | Yes | Yes | Yes | Enterprise | Yes | Yes |
| Loop Detection | Yes | Yes | Yes | Yes | No | Enterprise | Yes | Yes |
| Multiple Bridges | Yes | Yes | Yes | Yes | Yes | Enterprise | Yes | Yes |
| Bridge Manager | Yes | No | Yes | No | No | Enterprise | No | No |

## Clustering and Scalability

| Feature | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|---------|--------|-----------|------|---------|--------|-----------|----------|------|
| Native Clustering | No | No | Yes | Yes | No | Enterprise | Yes | Yes |
| Horizontal Scaling | Manual | Manual | Yes | Yes | No | Enterprise | Yes | Yes |
| Session Replication | Pluggable | No | Yes | Yes | No | Enterprise | Yes | Yes |
| Load Balancing | External | External | Built-in | Built-in | External | Enterprise | Built-in | Built-in |

## Persistence

| Feature | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|---------|--------|-----------|------|---------|--------|-----------|----------|------|
| Session Persistence | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Retained Persistence | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Message Queue | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Pluggable Storage | Yes | No | Yes | Yes | No | Enterprise | No | Yes |
| Memory Store | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| File Store | Pluggable | Yes | No | Yes | Yes | Yes | Yes | Yes |
| Database Backend | Pluggable | No | Yes | Yes | No | Enterprise | No | No |

## Monitoring and Metrics

| Feature | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|---------|--------|-----------|------|---------|--------|-----------|----------|------|
| expvar Metrics | Yes | No | No | No | No | No | No | No |
| Prometheus | Pluggable | Plugin | Yes | Yes | Yes | Plugin | Yes | Yes |
| $SYS Topics | No | Yes | Yes | Yes | Yes | Yes | No | No |
| HTTP API | Pluggable | No | Yes | Yes | Yes | Plugin | Yes | Yes |
| Dashboard | No | No | Yes | No | No | Enterprise | Yes | No |
| Connection Metrics | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Message Metrics | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Latency Tracking | Yes | No | Yes | Yes | No | Yes | Yes | Yes |

## Extensions and Integrations

| Feature | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|---------|--------|-----------|------|---------|--------|-----------|----------|------|
| RPC over MQTT | Yes | No | No | No | No | No | Yes | No |
| Message Interceptors | Yes | Plugin | Yes | Plugin | Plugin | Plugin | Yes | No |
| Webhooks | Pluggable | Plugin | Yes | Plugin | Yes | Plugin | No | No |
| Rule Engine | No | No | Yes | No | Yes | Enterprise | No | No |
| Data Integration | Pluggable | Plugin | Yes | Plugin | Yes | Enterprise | Yes | Yes |

## Performance Characteristics

| Aspect | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|--------|--------|-----------|------|---------|--------|-----------|----------|------|
| Memory Footprint | Low | Very Low | High | High | Very Low | High | High | Low |
| Startup Time | Fast | Fast | Slow | Slow | Fast | Slow | Slow | Fast |
| Concurrent Connections | High | Medium | Very High | High | Medium | High | High | Very High |
| Message Throughput | High | Medium | Very High | High | Medium | High | High | Very High |
| Latency | Low | Low | Low | Low | Very Low | Medium | Medium | Very Low |

## Development

| Aspect | mqttv5 | Mosquitto | EMQX | VerneMQ | NanoMQ | HiveMQ CE | RabbitMQ | NATS |
|--------|--------|-----------|------|---------|--------|-----------|----------|------|
| Embeddable | Yes | Limited | No | No | Yes | No | No | Yes |
| Library Usage | Yes | Client Only | No | No | Yes | No | No | Yes |
| Custom Broker | Yes | No | No | No | Yes | No | No | No |
| Go Integration | Native | CGO | No | No | No | No | No | Native |
| API Style | Programmatic | Config File | Config/API | Config | Config | Config | Config/API | Config/API |

## Features Not Supported in mqttv5

This section highlights MQTT-related features available in other brokers that are not currently supported in mqttv5.

| Feature | Available In | Description |
|---------|--------------|-------------|
| MQTT 3.1.1 | All others | Legacy protocol version for older clients |
| $SYS Topics | Mosquitto, EMQX, VerneMQ, NanoMQ, HiveMQ CE | System topics for broker statistics |
| Native Clustering | EMQX, VerneMQ, HiveMQ (Enterprise) | Built-in distributed broker clustering |
| Session Replication | EMQX, VerneMQ, HiveMQ (Enterprise) | Replicate client sessions across nodes |
| Rate Limiting | EMQX, Mosquitto, HiveMQ | Built-in connection and message rate limits |
| Delayed Messages | EMQX, HiveMQ (Enterprise) | Schedule MQTT message delivery for future time |
| Offline Message Queue | EMQX, HiveMQ, VerneMQ | Queue messages for disconnected persistent sessions |
| Auto Topic Creation | EMQX, HiveMQ | Automatically create topics on first publish |
| Topic Metrics | EMQX, VerneMQ, Mosquitto | Per-topic message and subscription statistics |
| Connection Rate Limit | EMQX, Mosquitto, HiveMQ | Limit new connections per second |
| Message Rate Limit | EMQX, HiveMQ | Limit messages per client per second |
| Banned Clients | EMQX, HiveMQ | Block specific client IDs from connecting |
| Flapping Detection | EMQX | Detect and block clients reconnecting too frequently |

### Planned vs Out of Scope

**May be added in future:**

- $SYS topics for broker statistics
- Built-in rate limiting
- Offline message queuing
- Delayed message delivery
- Banned clients list

**Intentionally out of scope:**

- MQTT 3.1.1 (library focuses on MQTT 5.0 only)
- Native clustering (use external orchestration like Kubernetes)
- Web dashboard (use external monitoring tools)

## Key Differentiators

### mqttv5

- **Pure Go implementation** - no CGO dependencies, easy cross-compilation
- **Library-first design** - build custom brokers or embed in applications
- **Native namespace isolation** - built-in multi-tenancy support
- **SCRAM authentication** - includes SCRAM-SHA-512 (unique among compared brokers)
- **RPC extension** - request/response pattern built-in
- **Message interceptors** - Kafka-style producer/consumer interceptors
- **Zero-allocation optimizations** - sync.Pool, allocation-free topic matching
- **Pluggable everything** - authentication, authorization, storage, metrics

### Mosquitto

- **Lightweight and proven** - decades of production use
- **Very low resource usage** - ideal for embedded systems
- **Simple configuration** - easy to deploy and manage
- **Wide platform support** - runs on almost anything

### EMQX

- **Massive scalability** - millions of concurrent connections
- **Built-in clustering** - native horizontal scaling
- **Rule engine** - process messages without code
- **Comprehensive integrations** - databases, Kafka, cloud services
- **Enterprise dashboard** - visual management interface

### VerneMQ

- **Distributed by design** - Erlang/OTP clustering
- **Plugin ecosystem** - extensive customization options
- **Live configuration** - changes without restart
- **Strong consistency** - reliable message delivery

### NanoMQ

- **Ultra-lightweight** - designed for edge computing
- **Multi-protocol** - MQTT, ZeroMQ, nanomsg, DDS
- **Very low latency** - optimized for real-time
- **Embeddable** - as a library in C applications

### HiveMQ CE

- **Enterprise foundation** - upgrade path to commercial features
- **Java ecosystem** - JVM-based extensions
- **Control center** - web-based management (Enterprise)
- **Professional support** - commercial backing available

### RabbitMQ

- **Multi-protocol broker** - AMQP, MQTT, STOMP in one broker
- **Mature ecosystem** - extensive tooling and client libraries
- **Virtual hosts** - built-in multi-tenancy via vhosts
- **Management UI** - comprehensive web dashboard
- **Plugin architecture** - highly extensible

### NATS

- **Ultra-high performance** - optimized for low latency at scale
- **JetStream** - built-in persistence and streaming
- **Multi-protocol** - NATS, MQTT, WebSocket
- **Cloud-native** - designed for Kubernetes and microservices
- **Subject-based routing** - powerful topic hierarchy
