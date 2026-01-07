# Router Extension for MQTT v5.0

Message router with MQTT wildcard support and conditional filtering.

## Installation

```bash
go get github.com/vitalvas/mqttv5/extensions/router
```

## Quick Start

```go
r := router.New()

// Route by topic with MQTT wildcards
r.Handle(func(msg *mqttv5.Message) {
    fmt.Printf("Temperature: %s\n", msg.Payload)
}, router.WithTopic("sensors/+/temperature"))

// Route by multiple conditions
r.Handle(func(msg *mqttv5.Message) {
    fmt.Printf("JSON from device: %s\n", msg.Payload)
}, router.WithTopic("telemetry/#"),
   router.WithQoS(mqttv5.QoS1),
   router.WithContentType(regexp.MustCompile(`^application/json`)))

// Dispatch message
r.Route(msg)
```

## Usage

### Topic Wildcards

```go
r := router.New()

// Exact match
r.Handle(handler, router.WithTopic("sensors/temperature"))

// Single-level wildcard (+) - matches one level
r.Handle(handler, router.WithTopic("sensors/+/value"))

// Multi-level wildcard (#) - matches zero or more levels
r.Handle(handler, router.WithTopic("alerts/#"))
```

| Pattern | Matches |
|---------|---------|
| `sensors/temperature` | Exact match only |
| `sensors/+/value` | `sensors/X/value` (X = any single level) |
| `sensors/#` | `sensors`, `sensors/a`, `sensors/a/b/c` |
| `+/+/value` | Any 3-level topic ending in `value` |
| `#` | All topics |

### Condition Filters

All conditions are optional. Multiple conditions use AND logic.

```go
// Filter by QoS
r.Handle(handler, router.WithTopic("data/#"), router.WithQoS(mqttv5.QoS1))

// Filter by content type (regexp)
r.Handle(handler, router.WithTopic("api/#"), router.WithContentType(regexp.MustCompile(`^application/json`)))

// Filter by client ID (regexp)
r.Handle(handler, router.WithTopic("data/#"), router.WithClientID(regexp.MustCompile(`^sensor-`)))

// Filter by response topic (regexp)
r.Handle(handler, router.WithTopic("rpc/#"), router.WithResponseTopic(regexp.MustCompile(`^rpc/response/`)))

// Filter by user property (key/value regexp)
r.Handle(handler, router.WithTopic("data/#"), router.WithUserProperty(
    regexp.MustCompile(`^tenant$`),
    regexp.MustCompile(`^acme$`),
))

// Filter by namespace (server-side multi-tenancy)
r.Handle(handler, router.WithTopic("data/#"), router.WithNamespace("tenant-123"))

// No conditions - matches all messages
r.Handle(handler)
```

### With MQTT Client

```go
r := router.New()

r.Handle(func(msg *mqttv5.Message) {
    fmt.Printf("Sensor: %s = %s\n", msg.Topic, msg.Payload)
}, router.WithTopic("sensors/#"))

client, _ := mqttv5.Dial(mqttv5.WithServers("tcp://localhost:1883"))
client.Subscribe("#", 0, r.MessageHandler())
```

### With MQTT Server

```go
r := router.New()

r.Handle(func(msg *mqttv5.Message) {
    // Process by client
    fmt.Printf("From %s: %s\n", msg.ClientID, msg.Payload)
}, router.WithTopic("telemetry/#"))

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.OnMessage(func(c *mqttv5.ServerClient, msg *mqttv5.Message) {
        r.Route(msg)
    }),
)
```

### Multiple Handlers

When multiple handlers match, all are called:

```go
r.Handle(handler1, router.WithTopic("sensors/temp"))
r.Handle(handler2, router.WithTopic("sensors/+"))
r.Handle(handler3, router.WithTopic("#"))

r.Route(msg) // All three handlers called for "sensors/temp"
```

## API Reference

### Types

```go
type Handler func(msg *mqttv5.Message)
type ConditionOption func(*Condition)
```

### Router Methods

| Method | Description |
|--------|-------------|
| `New()` | Create new router |
| `Handle(handler, opts...)` | Register handler with conditions |
| `Route(msg)` | Dispatch to all matching handlers |
| `Filters() []string` | List registered topic filters |
| `Len() int` | Number of registered handlers |
| `Clear()` | Remove all handlers |
| `MessageHandler()` | Returns `mqttv5.MessageHandler` for client |

### Condition Options

| Option | Parameter | Description |
|--------|-----------|-------------|
| `WithTopic` | `string` | Topic filter with MQTT wildcards |
| `WithNamespace` | `string` | Namespace for multi-tenancy |
| `WithQoS` | `byte` | QoS level (0, 1, 2) |
| `WithContentType` | `*regexp.Regexp` | Content type pattern |
| `WithClientID` | `*regexp.Regexp` | Client ID pattern |
| `WithResponseTopic` | `*regexp.Regexp` | Response topic pattern |
| `WithUserProperty` | `*regexp.Regexp, *regexp.Regexp` | Key and value patterns |

## Thread Safety

Router is safe for concurrent use. Handlers are copied before invocation to avoid holding locks during callbacks.
