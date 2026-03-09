# systopics

Package `systopics` provides `$SYS` topic publishing for MQTT broker statistics.

It follows the standard `$SYS/broker/` topic hierarchy used by MQTT brokers, periodically publishing broker statistics as retained messages with QoS 0.

## Usage

All topic groups are disabled by default. Enable the groups you need with `WithTopicGroups`, or use `TopicGroupAll` to enable all.

```go
server := mqttv5.NewServer(/* ... */)

// Enable all topic groups.
pub := systopics.New(server,
    systopics.WithVersion("mybroker 1.0.0"),
    systopics.WithInterval(10 * time.Second),
    systopics.WithTopicGroups(systopics.TopicGroupAll),
)

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

go pub.Run(ctx)
```

Enable only specific groups:

```go
pub := systopics.New(server,
    systopics.WithTopicGroups(
        systopics.TopicGroupClients,
        systopics.TopicGroupMessages,
        systopics.TopicGroupLoad,
    ),
)
```

## Configuration

| Option | Default | Description |
|---|---|---|
| `WithInterval(d)` | 10s | Publishing interval |
| `WithVersion(v)` | `""` | Broker version string for `$SYS/broker/version` |
| `WithNamespaceMode(m)` | `NamespaceModeGlobal` | Namespace publishing mode |
| `WithTopicGroups(g...)` | none | Enable specific topic groups (use `TopicGroupAll` for all) |
| `WithOnPublishError(fn)` | none | Callback invoked when a publish to a `$SYS` topic fails |

## Topic Groups

All groups are disabled by default.

| Group | Topics | Description |
|---|---|---|
| `TopicGroupBrokerInfo` | version, uptime, timestamp | Broker identification and uptime |
| `TopicGroupClients` | clients/* | Connected, total, maximum, disconnected |
| `TopicGroupMessages` | messages/* | Received, sent, stored, publish/*, per-QoS |
| `TopicGroupBytes` | bytes/* | Bytes received and sent |
| `TopicGroupSubscriptions` | subscriptions/count | Active subscriptions |
| `TopicGroupRetained` | retained/messages/count | Retained message count |
| `TopicGroupLoad` | load/* | EMA rates for 1min, 5min, 15min |
| `TopicGroupNamespace` | namespaces/count, per-namespace topics | Namespace count and scoped client counts |

## Topics

### Broker Information

| Topic | Description |
|---|---|
| `$SYS/broker/version` | Broker version string |
| `$SYS/broker/uptime` | Seconds since broker started |
| `$SYS/broker/timestamp` | Current Unix timestamp |

### Client Statistics

| Topic | Description |
|---|---|
| `$SYS/broker/clients/connected` | Currently connected clients |
| `$SYS/broker/clients/total` | Total connections since start |
| `$SYS/broker/clients/maximum` | Peak concurrent connections |
| `$SYS/broker/clients/disconnected` | Disconnected persistent sessions |

### Message Statistics

| Topic | Description |
|---|---|
| `$SYS/broker/messages/received` | Total messages received (all QoS) |
| `$SYS/broker/messages/sent` | Total messages sent (all QoS) |
| `$SYS/broker/messages/stored` | Retained messages count |
| `$SYS/broker/messages/publish/received` | Total PUBLISH messages received |
| `$SYS/broker/messages/publish/sent` | Total PUBLISH messages sent |
| `$SYS/broker/messages/received/qos{0,1,2}` | Messages received per QoS level |
| `$SYS/broker/messages/sent/qos{0,1,2}` | Messages sent per QoS level |

### Byte Statistics

| Topic | Description |
|---|---|
| `$SYS/broker/bytes/received` | Total bytes received |
| `$SYS/broker/bytes/sent` | Total bytes sent |

### Subscription and Retained Statistics

| Topic | Description |
|---|---|
| `$SYS/broker/subscriptions/count` | Active subscriptions |
| `$SYS/broker/retained/messages/count` | Retained messages |

### Load Statistics

Load values are exponential moving averages (EMA) of the per-second rate, computed over 1-minute, 5-minute, and 15-minute windows.

| Topic | Description |
|---|---|
| `$SYS/broker/load/messages/received/{1min,5min,15min}` | Messages received per second |
| `$SYS/broker/load/messages/sent/{1min,5min,15min}` | Messages sent per second |
| `$SYS/broker/load/bytes/received/{1min,5min,15min}` | Bytes received per second |
| `$SYS/broker/load/bytes/sent/{1min,5min,15min}` | Bytes sent per second |
| `$SYS/broker/load/connections/{1min,5min,15min}` | New connections per second |

## Namespace Support

Namespace topics require `TopicGroupNamespace` to be enabled.

| Topic | Description |
|---|---|
| `$SYS/broker/namespaces/count` | Number of active namespaces |

### Global Mode (default)

Publishes broker-wide aggregate statistics on standard `$SYS/broker/` topics, plus per-namespace client counts:

```
$SYS/broker/clients/namespace/{ns}/connected
```

### Isolated Mode

Publishes the `$SYS/broker/clients/connected` topic with the `Namespace` field set on the message, so the server delivers it only to clients in that namespace. Each namespace sees its own client count. Namespace names are never exposed in topic paths -- clients see the same `$SYS/broker/clients/connected` topic but with their own scoped value.

When both `TopicGroupClients` and `TopicGroupNamespace` are enabled in isolated mode, `$SYS/broker/clients/connected` is only published per-namespace (not globally) to avoid double-publishing conflicting values.

Enable with:

```go
systopics.WithNamespaceMode(systopics.NamespaceModeIsolated)
```

## Authorization

The `Publisher.CheckAccess` method integrates with custom authorizers to control access to `$SYS` topics. It allows subscribe-only access to topics whose groups are enabled.

```go
func (a *MyAuthz) Authorize(ctx context.Context, c *mqttv5.AuthzContext) (*mqttv5.AuthzResult, error) {
    if result := a.sysPublisher.CheckAccess(c); result != nil {
        return result, nil
    }
    // other ACL checks...
    return &mqttv5.AuthzResult{Allowed: true}, nil
}
```

Rules:
- Returns `nil` for non-`$SYS` topics (not handled)
- Denies publish actions (clients never publish to `$SYS`)
- Allows subscribe only for topics in enabled groups
- Supports MQTT wildcard filters (e.g., `$SYS/#`, `$SYS/broker/clients/+`)
- Unknown `$SYS` topics are denied
- Subscriptions are capped at QoS 0

## Value Formats

- **Counts**: integer strings (e.g., `"42"`)
- **Uptime**: integer seconds (e.g., `"3600"`)
- **Timestamp**: Unix epoch seconds (e.g., `"1709913600"`)
- **Load rates**: float with 2 decimal places (e.g., `"12.34"`)
- **Version**: string as configured
