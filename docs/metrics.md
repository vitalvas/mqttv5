# Metrics

The MQTT v5.0 SDK provides built-in metrics collection using Go's `expvar` package.
Metrics are automatically exposed at `/debug/vars` when using the standard `net/http` package.

## Available Metrics

### Connection Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `mqtt_connections` | Gauge | Current number of active connections |
| `mqtt_connections_total` | Counter | Total number of connections since server start |

### Message Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `mqtt_messages_received_total_qos0` | Counter | Total messages received with QoS 0 |
| `mqtt_messages_received_total_qos1` | Counter | Total messages received with QoS 1 |
| `mqtt_messages_received_total_qos2` | Counter | Total messages received with QoS 2 |
| `mqtt_messages_sent_total_qos0` | Counter | Total messages sent with QoS 0 |
| `mqtt_messages_sent_total_qos1` | Counter | Total messages sent with QoS 1 |
| `mqtt_messages_sent_total_qos2` | Counter | Total messages sent with QoS 2 |

### Byte Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `mqtt_bytes_received_total` | Counter | Total bytes received |
| `mqtt_bytes_sent_total` | Counter | Total bytes sent |

### Subscription Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `mqtt_subscriptions` | Gauge | Current number of active subscriptions |
| `mqtt_retained_messages` | Gauge | Current number of retained messages |

### Packet Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `mqtt_packets_received_total_{TYPE}` | Counter | Total packets received by type (CONNECT, PUBLISH, etc.) |
| `mqtt_packets_sent_total_{TYPE}` | Counter | Total packets sent by type (CONNACK, PUBACK, etc.) |

### Latency Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `mqtt_publish_latency_count` | Counter | Number of publish operations processed |
| `mqtt_publish_latency_seconds_sum` | Counter | Sum of publish processing time in seconds |

## Usage

### Basic Setup with expvar

```go
package main

import (
    "net"
    "net/http"
    "os"

    "github.com/vitalvas/mqttv5"
)

func main() {
    // Create listener
    listener, err := net.Listen("tcp", ":1883")
    if err != nil {
        panic(err)
    }

    // Create metrics collector (expvar-based)
    metrics := mqttv5.NewMetrics()

    // Create server with metrics
    srv := mqttv5.NewServer(
        mqttv5.WithListener(listener),
        mqttv5.WithMetrics(metrics),
        mqttv5.WithLogger(mqttv5.NewStdLogger(os.Stdout, mqttv5.LogLevelInfo)),
    )

    // Expose expvar metrics on HTTP endpoint
    // Metrics available at http://localhost:8080/debug/vars
    go http.ListenAndServe(":8080", nil)

    // Start MQTT server
    srv.ListenAndServe()
}
```

### Using MemoryMetrics for Testing

```go
package main

import (
    "net"
    "testing"

    "github.com/vitalvas/mqttv5"
)

func TestWithMetrics(t *testing.T) {
    listener, _ := net.Listen("tcp", ":0")
    metrics := mqttv5.NewMemoryMetrics()

    srv := mqttv5.NewServer(
        mqttv5.WithListener(listener),
        mqttv5.WithMetrics(metrics),
    )
    defer srv.Close()

    // After some operations...
    if metrics.Connections() != expectedConnections {
        t.Errorf("expected %d connections, got %d", expectedConnections, metrics.Connections())
    }
}
```

### Disabling Metrics

```go
// NoOpMetrics is used by default, or can be explicitly set
listener, _ := net.Listen("tcp", ":1883")
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithMetrics(&mqttv5.NoOpMetrics{}),
)
```

## Prometheus Integration

Create a custom handler that converts expvar metrics to Prometheus format:

```go
package main

import (
    "expvar"
    "fmt"
    "net"
    "net/http"
    "os"
    "strings"

    "github.com/vitalvas/mqttv5"
)

func main() {
    listener, err := net.Listen("tcp", ":1883")
    if err != nil {
        panic(err)
    }

    metrics := mqttv5.NewMetrics()

    srv := mqttv5.NewServer(
        mqttv5.WithListener(listener),
        mqttv5.WithMetrics(metrics),
        mqttv5.WithLogger(mqttv5.NewStdLogger(os.Stdout, mqttv5.LogLevelInfo)),
    )

    // Custom Prometheus metrics endpoint
    http.HandleFunc("/metrics", prometheusHandler)

    go http.ListenAndServe(":8080", nil)

    srv.ListenAndServe()
}

func prometheusHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/plain; version=0.0.4")

    expvar.Do(func(kv expvar.KeyValue) {
        // Only export mqtt_ prefixed metrics
        if !strings.HasPrefix(kv.Key, "mqtt_") {
            return
        }

        name := kv.Key
        value := kv.Value.String()

        // Determine metric type based on name
        metricType := "counter"
        if strings.Contains(name, "connections") && !strings.Contains(name, "total") {
            metricType = "gauge"
        }
        if strings.Contains(name, "subscriptions") || strings.Contains(name, "retained") {
            metricType = "gauge"
        }

        fmt.Fprintf(w, "# TYPE %s %s\n", name, metricType)
        fmt.Fprintf(w, "%s %s\n", name, value)
    })
}
```
