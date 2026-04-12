# Server Stats and Introspection

The server provides methods to inspect connected clients and their
subscriptions at runtime.

## Namespaces

| Method | Description |
|--------|-------------|
| `Namespaces()` | List unique namespaces with connected clients |

## Client Info

| Method | Description |
|--------|-------------|
| `Clients(namespaces ...string)` | List connected clients as `[]ClientIdentifier` with `Namespace` and `ClientID` fields |
| `ClientCount(namespaces ...string)` | Number of connected clients (filtered by namespaces if provided) |
| `ClientsInfo(namespaces ...string)` | All clients with connection info and stats (filtered if provided) |
| `GetClientInfo(namespace, clientID)` | Connection info for a specific client |
| `GetClient(namespace, clientID)` | Raw `*ServerClient` reference |
| `DisconnectClient(namespace, clientID, reason)` | Disconnect a specific client |

### ClientInfo Fields

| Field | Type | Description |
|-------|------|-------------|
| `ClientID` | `string` | Client identifier |
| `Username` | `string` | Username from CONNECT |
| `Namespace` | `string` | Multi-tenancy namespace |
| `ProtocolVersion` | `ProtocolVersion` | MQTT protocol version (4=v3.1.1, 5=v5.0) |
| `RemoteAddr` | `string` | Remote network address |
| `LocalAddr` | `string` | Local network address |
| `ConnectedAt` | `time.Time` | Connection time |
| `Uptime` | `time.Duration` | Time since connection |
| `Idle` | `time.Duration` | Time since last activity |
| `LastActivity` | `time.Time` | Last activity timestamp |
| `TLS` | `*tls.ConnectionState` | TLS connection state (nil for non-TLS) |
| `CleanStart` | `bool` | Clean start flag from CONNECT |
| `KeepAlive` | `uint16` | Keep-alive interval (seconds) |
| `MaxPacketSize` | `uint32` | Negotiated max packet size |
| `Subscriptions` | `int` | Number of active subscriptions |
| `MessagesIn` | `int64` | Messages received from client |
| `MessagesOut` | `int64` | Messages sent to client |
| `BytesIn` | `int64` | Bytes received from client |
| `BytesOut` | `int64` | Bytes sent to client |

## Subscription Info

### Server Methods

| Method | Description |
|--------|-------------|
| `GetSubscriptionSummary(namespace)` | Subscription summary (empty namespace = all) |
| `GetClientSubscriptions(namespace, clientID)` | Subscriptions for a specific client |

### SubscriptionManager Methods

These methods are available directly on `SubscriptionManager`:

| Method | Description |
|--------|-------------|
| `Summary(namespace)` | Returns `SubscriptionSummary` |
| `ClientSubscriptionInfo(namespace, clientID)` | Returns `[]SubscriptionInfo` |
| `Count()` | Total subscription count |
| `ClientCount()` | Clients with subscriptions |

### SubscriptionSummary Fields

| Field | Type | Description |
|-------|------|-------------|
| `TotalSubscriptions` | `int` | Total number of subscriptions |
| `TotalClients` | `int` | Clients with at least one subscription |
| `SharedGroups` | `int` | Number of shared subscription groups |
| `Subscriptions` | `[]SubscriptionInfo` | Detailed subscription list |

### SubscriptionInfo Fields

| Field | Type | Description |
|-------|------|-------------|
| `ClientID` | `string` | Owning client identifier |
| `Namespace` | `string` | Namespace |
| `TopicFilter` | `string` | MQTT topic filter |
| `QoS` | `byte` | Subscription QoS level |
| `NoLocal` | `bool` | No local flag |
| `RetainAsPublish` | `bool` | Retain as published flag |
| `RetainHandling` | `byte` | Retain handling option |
| `SubscriptionID` | `uint32` | Subscription identifier |
| `Shared` | `bool` | Whether this is a shared subscription |
| `ShareGroup` | `string` | Share group name (if shared) |

## Topic Metrics

Per-topic message and subscription statistics.

| Method | Description |
|--------|-------------|
| `TopicMetrics()` | Returns the `*TopicMetrics` tracker |
| `GetTopicMetrics(namespace, topic)` | Metrics for a topic (with subscriber count) |
| `AllTopicMetrics(namespace)` | All topic metrics (empty namespace = all) |

### TopicMetrics Methods

These methods are available directly on `TopicMetrics`:

| Method | Description |
|--------|-------------|
| `Get(namespace, topic)` | Get metrics for a topic |
| `All(namespace)` | All metrics (empty namespace = all) |
| `TopicCount(namespace)` | Number of tracked topics |
| `Reset()` | Clear all topic metrics |

### TopicMetricsInfo Fields

| Field | Type | Description |
|-------|------|-------------|
| `Topic` | `string` | Topic name |
| `Namespace` | `string` | Namespace |
| `MessagesIn` | `int64` | Messages published to topic |
| `MessagesOut` | `int64` | Messages delivered from topic |
| `BytesIn` | `int64` | Bytes published to topic |
| `BytesOut` | `int64` | Bytes delivered from topic |
| `Subscribers` | `int` | Current matching subscribers |

The `Subscribers` field is populated when accessed via `Server.GetTopicMetrics()`
or `Server.AllTopicMetrics()`.

## Examples

### List All Clients

```go
for _, info := range srv.ClientsInfo() {
    fmt.Printf("Client %s (ns=%s) uptime=%s msgs_in=%d msgs_out=%d\n",
        info.ClientID, info.Namespace, info.Uptime,
        info.MessagesIn, info.MessagesOut)
}
```

### List Clients in a Namespace

```go
clients := srv.ClientsInfo("tenant1")
count := srv.ClientCount("tenant1")
```

### Get Subscription Summary

```go
// All namespaces
summary := srv.GetSubscriptionSummary("")

// Specific namespace
summary := srv.GetSubscriptionSummary("tenant1")

fmt.Printf("Total: %d subs, %d clients, %d shared groups\n",
    summary.TotalSubscriptions, summary.TotalClients, summary.SharedGroups)
```

### Get Client Subscriptions

```go
subs := srv.GetClientSubscriptions("default", "client1")
for _, sub := range subs {
    fmt.Printf("  filter=%s qos=%d shared=%v\n",
        sub.TopicFilter, sub.QoS, sub.Shared)
}
```

### Get Topic Metrics

```go
info := srv.GetTopicMetrics("default", "sensor/temp")
if info != nil {
    fmt.Printf("topic=%s in=%d out=%d subs=%d\n",
        info.Topic, info.MessagesIn, info.MessagesOut, info.Subscribers)
}

// All topics in a namespace
for _, t := range srv.AllTopicMetrics("default") {
    fmt.Printf("%s: %d in, %d out, %d subs\n",
        t.Topic, t.MessagesIn, t.MessagesOut, t.Subscribers)
}
```
