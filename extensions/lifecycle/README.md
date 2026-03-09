# lifecycle

Package `lifecycle` publishes client lifecycle events as JSON messages to well-known MQTT topics under the `$events/` prefix.

It hooks into the broker's `OnConnect`, `OnConnectFailed`, `OnDisconnect`, `OnSubscribe`, and `OnUnsubscribe` callbacks and publishes structured event messages that other clients can subscribe to.

## Usage

```go
lc := lifecycle.New(server)

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.OnConnect(lc.OnConnect),
    mqttv5.OnConnectFailed(lc.OnConnectFailed),
    mqttv5.OnDisconnect(lc.OnDisconnect),
    mqttv5.OnSubscribe(lc.OnSubscribe),
    mqttv5.OnUnsubscribe(lc.OnUnsubscribe),
)
```

Multiple callbacks can be registered for the same event. Each `OnXxx` call appends to the list rather than replacing previous callbacks.

```go
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.OnConnect(lc.OnConnect),
    mqttv5.OnConnect(func(c *mqttv5.ServerClient) {
        log.Printf("Connected: %s", c.ClientID())
    }),
)
```

## Configuration

| Option | Default | Description |
|---|---|---|
| `WithOnPublishError(fn)` | nil | Callback invoked when publishing a lifecycle event fails |
| `WithNamespace(string)` | `""` | Fixed namespace for all events; takes priority over `WithClientNamespace` |
| `WithClientNamespace(bool)` | false | When true, events are published in the client's namespace instead of `DefaultNamespace` |

```go
lc := lifecycle.New(server, lifecycle.WithOnPublishError(func(topic string, err error) {
    log.Printf("lifecycle publish failed: topic=%s err=%v", topic, err)
}))
```

## Topics

| Event | Topic Pattern |
|---|---|
| Connected | `$events/presence/connected/{clientId}` |
| Connect Failed | `$events/presence/connect_failed/{clientId}` |
| Disconnected | `$events/presence/disconnected/{clientId}` |
| Subscribed | `$events/subscriptions/subscribed/{clientId}` |
| Unsubscribed | `$events/subscriptions/unsubscribed/{clientId}` |

## Event Payloads

### Connected

```json
{
  "clientId": "sensor-01",
  "timestamp": "2026-03-09T12:00:00Z",
  "eventType": "connected",
  "namespace": "tenant1",
  "username": "admin",
  "remoteAddr": "192.168.1.10:54321",
  "localAddr": "0.0.0.0:1883",
  "cleanStart": true,
  "keepAlive": 60,
  "sessionExpiryInterval": 3600
}
```

| Field | Type | Description |
|---|---|---|
| `clientId` | string | Client identifier |
| `timestamp` | string | RFC 3339 timestamp |
| `eventType` | string | Always `"connected"` |
| `namespace` | string | Namespace (omitted if empty) |
| `username` | string | Username (omitted if empty) |
| `remoteAddr` | string | Remote address (omitted if empty) |
| `localAddr` | string | Local server address (omitted if empty) |
| `cleanStart` | bool | Whether the client requested a clean session |
| `keepAlive` | int | Keep-alive interval in seconds |
| `sessionExpiryInterval` | int | Session expiry interval in seconds |

### Connect Failed

```json
{
  "clientId": "sensor-01",
  "timestamp": "2026-03-09T12:00:00Z",
  "eventType": "connectFailed",
  "username": "admin",
  "remoteAddr": "192.168.1.10:54321",
  "localAddr": "0.0.0.0:1883",
  "reasonCode": 135,
  "reason": "not authorized"
}
```

| Field | Type | Description |
|---|---|---|
| `clientId` | string | Client identifier (omitted if empty) |
| `timestamp` | string | RFC 3339 timestamp |
| `eventType` | string | Always `"connectFailed"` |
| `username` | string | Username (omitted if empty) |
| `remoteAddr` | string | Remote address (omitted if empty) |
| `localAddr` | string | Local server address (omitted if empty) |
| `reasonCode` | int | MQTT reason code from the CONNACK |
| `reason` | string | Human-readable reason string |

### Disconnected

```json
{
  "clientId": "sensor-01",
  "timestamp": "2026-03-09T12:05:00Z",
  "eventType": "disconnected",
  "namespace": "tenant1",
  "remoteAddr": "192.168.1.10:54321",
  "localAddr": "0.0.0.0:1883",
  "cleanDisconnect": true,
  "connectedAt": "2026-03-09T12:00:00Z",
  "sessionDuration": 300.5
}
```

| Field | Type | Description |
|---|---|---|
| `clientId` | string | Client identifier |
| `timestamp` | string | RFC 3339 timestamp |
| `eventType` | string | Always `"disconnected"` |
| `namespace` | string | Namespace (omitted if empty) |
| `remoteAddr` | string | Remote address (omitted if empty) |
| `localAddr` | string | Local server address (omitted if empty) |
| `cleanDisconnect` | bool | `true` if the client sent a DISCONNECT packet |
| `connectedAt` | string | RFC 3339 timestamp of when the client connected |
| `sessionDuration` | float | Session duration in seconds |

### Subscribed

```json
{
  "clientId": "sensor-01",
  "timestamp": "2026-03-09T12:01:00Z",
  "eventType": "subscribed",
  "namespace": "tenant1",
  "subscriptions": [
    {"topicFilter": "devices/+/telemetry", "qos": 1},
    {"topicFilter": "commands/#", "qos": 0}
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `clientId` | string | Client identifier |
| `timestamp` | string | RFC 3339 timestamp |
| `eventType` | string | Always `"subscribed"` |
| `namespace` | string | Namespace (omitted if empty) |
| `subscriptions` | array | List of topic filters with QoS |

### Unsubscribed

```json
{
  "clientId": "sensor-01",
  "timestamp": "2026-03-09T12:04:00Z",
  "eventType": "unsubscribed",
  "namespace": "tenant1",
  "topicFilters": ["devices/+/telemetry", "commands/#"]
}
```

| Field | Type | Description |
|---|---|---|
| `clientId` | string | Client identifier |
| `timestamp` | string | RFC 3339 timestamp |
| `eventType` | string | Always `"unsubscribed"` |
| `namespace` | string | Namespace (omitted if empty) |
| `topicFilters` | array | List of unsubscribed topic filters |

## Namespace Isolation

By default, all events are published in `mqttv5.DefaultNamespace`. Enable `WithClientNamespace(true)` to publish events in the client's own namespace instead, so that subscribers only receive events for clients in their own namespace.

```go
lc := lifecycle.New(server, lifecycle.WithClientNamespace(true))
```

When enabled, if the client's namespace is empty, events fall back to `DefaultNamespace`.

`connect_failed` events are always published in `DefaultNamespace` because authentication has not completed and the client's namespace is unknown.

## Topic Safety

Client IDs containing MQTT wildcard characters (`+`, `#`) or topic level separators (`/`) are percent-encoded before embedding in the topic path (e.g., `dev/1` becomes `dev%2F1`). Consumers can decode with `url.PathUnescape`. The original client ID is always preserved in the JSON payload.
