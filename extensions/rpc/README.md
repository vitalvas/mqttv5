# RPC Extension for MQTT v5.0

This package provides request/response (RPC) functionality for MQTT v5.0 clients using correlation data and response topic properties as defined in MQTT v5.0 specification Section 4.10.

## Features

- Synchronous request/response pattern over MQTT
- Header support via MQTT v5.0 User Properties
- Context-based timeout and cancellation
- Content-Type support for payload MIME types
- Thread-safe for concurrent requests

## Installation

```bash
go get github.com/vitalvas/mqttv5/extensions/rpc
```

## Usage

### Basic Request/Response

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/vitalvas/mqttv5"
    "github.com/vitalvas/mqttv5/extensions/rpc"
)

func main() {
    // Connect to broker
    client, err := mqttv5.Dial("tcp://localhost:1883", mqttv5.WithClientID("my-client"))
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Create RPC handler
    handler, err := rpc.NewHandler(client, nil)
    if err != nil {
        panic(err)
    }
    defer handler.Close()

    // Make request with timeout
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    resp, err := handler.Request(ctx, "service/echo", []byte("hello"))
    if err != nil {
        panic(err)
    }

    fmt.Printf("Response: %s\n", resp.Payload)
}
```

### Request with Headers

```go
resp, err := handler.Call(ctx, "service/api", &rpc.Request{
    Payload:     []byte(`{"action":"getData"}`),
    ContentType: "application/json",
    Headers: rpc.Headers{
        "x-request-id": "req-123",
        "x-auth-token": "bearer-token",
    },
})
if err != nil {
    panic(err)
}

// Access response
fmt.Printf("Payload: %s\n", resp.Payload)
fmt.Printf("Content-Type: %s\n", resp.ContentType)
fmt.Printf("Status: %s\n", resp.Headers["x-status"])
```

### Convenience Methods

```go
// Request with timeout (no context needed)
resp, err := handler.RequestWithTimeout("service/echo", []byte("hello"), 5*time.Second)

// Call with timeout and headers
resp, err := handler.CallWithTimeout("service/api", &rpc.Request{
    Payload: []byte("data"),
    Headers: rpc.Headers{"x-key": "value"},
}, 5*time.Second)
```

### Custom Response Topic

```go
handler, err := rpc.NewHandler(client, &rpc.HandlerOptions{
    ResponseTopic: "my-app/responses/client-1",
    QoS:           mqttv5.QoS1,
})
```

## Implementing a Responder (Client-Side)

A client can act as a responder by subscribing to request topics and sending responses using the correlation data and response topic from incoming messages:

```go
client.Subscribe("service/echo", 0, func(msg *mqttv5.Message) {
    // Process request
    result := processRequest(msg.Payload)

    // Send response
    response := &mqttv5.Message{
        Topic:           msg.ResponseTopic,
        Payload:         result,
        CorrelationData: msg.CorrelationData,
        ContentType:     "application/json",
        UserProperties: []mqttv5.StringPair{
            {Key: "x-status", Value: "ok"},
        },
    }
    client.Publish(response)
})
```

## Handling RPC Requests (Broker-Side)

The broker can intercept and handle RPC requests directly using the `OnMessage` callback. This is useful for implementing server-side services without requiring a separate client connection.

### Basic Server-Side Handler

```go
package main

import (
    "encoding/json"
    "net"

    "github.com/vitalvas/mqttv5"
)

func main() {
    listener, _ := net.Listen("tcp", ":1883")

    server := mqttv5.NewServer(
        mqttv5.WithListener(listener),
        mqttv5.OnMessage(handleRPCRequest),
    )

    server.ListenAndServe()
}

func handleRPCRequest(client *mqttv5.ServerClient, msg *mqttv5.Message) {
    // Check if this is an RPC request (has response topic)
    if msg.ResponseTopic == "" {
        return // Not an RPC request, let it pass through
    }

    // Route based on topic
    switch msg.Topic {
    case "rpc/echo":
        handleEcho(client, msg)
    case "rpc/time":
        handleTime(client, msg)
    default:
        // Unknown service - optionally send error response
        sendErrorResponse(client, msg, "unknown service")
    }
}

func handleEcho(client *mqttv5.ServerClient, msg *mqttv5.Message) {
    response := &mqttv5.Message{
        Topic:           msg.ResponseTopic,
        Payload:         msg.Payload,
        CorrelationData: msg.CorrelationData,
        UserProperties: []mqttv5.StringPair{
            {Key: "x-status", Value: "ok"},
        },
    }
    client.Send(response)
}

func handleTime(client *mqttv5.ServerClient, msg *mqttv5.Message) {
    response := &mqttv5.Message{
        Topic:           msg.ResponseTopic,
        Payload:         []byte(time.Now().Format(time.RFC3339)),
        CorrelationData: msg.CorrelationData,
        ContentType:     "text/plain",
    }
    client.Send(response)
}

func sendErrorResponse(client *mqttv5.ServerClient, msg *mqttv5.Message, errMsg string) {
    response := &mqttv5.Message{
        Topic:           msg.ResponseTopic,
        Payload:         []byte(errMsg),
        CorrelationData: msg.CorrelationData,
        UserProperties: []mqttv5.StringPair{
            {Key: "x-status", Value: "error"},
        },
    }
    client.Send(response)
}
```

### Server-Side Handler with Headers

```go
func handleAPIRequest(client *mqttv5.ServerClient, msg *mqttv5.Message) {
    if msg.ResponseTopic == "" {
        return
    }

    // Extract request headers
    headers := make(map[string]string)
    for _, prop := range msg.UserProperties {
        headers[prop.Key] = prop.Value
    }

    // Check authentication
    authToken := headers["x-auth-token"]
    if !validateToken(authToken) {
        sendErrorResponse(client, msg, "unauthorized")
        return
    }

    // Process request
    requestID := headers["x-request-id"]
    result, err := processBusinessLogic(msg.Payload)

    // Build response with headers
    responseHeaders := []mqttv5.StringPair{
        {Key: "x-request-id", Value: requestID},
    }

    if err != nil {
        responseHeaders = append(responseHeaders, mqttv5.StringPair{
            Key: "x-status", Value: "error",
        })
        result = []byte(err.Error())
    } else {
        responseHeaders = append(responseHeaders, mqttv5.StringPair{
            Key: "x-status", Value: "ok",
        })
    }

    response := &mqttv5.Message{
        Topic:           msg.ResponseTopic,
        Payload:         result,
        CorrelationData: msg.CorrelationData,
        ContentType:     "application/json",
        UserProperties:  responseHeaders,
    }
    client.Send(response)
}
```

### Publishing Responses to Other Clients

The broker can also publish RPC responses to clients other than the requester by using the server's `Publish` method:

```go
func handleBroadcastRequest(server *mqttv5.Server, client *mqttv5.ServerClient, msg *mqttv5.Message) {
    if msg.ResponseTopic == "" {
        return
    }

    // Process and respond to requester
    response := &mqttv5.Message{
        Topic:           msg.ResponseTopic,
        Payload:         []byte("request received"),
        CorrelationData: msg.CorrelationData,
    }
    client.Send(response)

    // Also notify other subscribers
    notification := &mqttv5.Message{
        Topic:   "notifications/requests",
        Payload: msg.Payload,
    }
    server.Publish(notification)
}
```

## API Reference

### Types

#### Headers

```go
type Headers map[string]string
```

RPC headers as key-value pairs, transmitted as MQTT v5.0 User Properties.

#### Request

```go
type Request struct {
    Payload     []byte
    Headers     Headers
    ContentType string
}
```

#### Response

```go
type Response struct {
    Payload         []byte
    Headers         Headers
    ContentType     string
    CorrelationData []byte
}
```

#### HandlerOptions

```go
type HandlerOptions struct {
    ResponseTopic string  // Default: "rpc/response/{clientID}"
    QoS           byte    // Default: 0
}
```

### Handler Methods

| Method | Description |
|--------|-------------|
| `NewHandler(client, opts)` | Creates a new RPC handler |
| `Call(ctx, topic, req)` | Sends request with headers, waits for response |
| `CallWithTimeout(topic, req, timeout)` | Call with timeout duration |
| `Request(ctx, topic, payload)` | Simple request without headers |
| `RequestWithTimeout(topic, payload, timeout)` | Simple request with timeout |
| `ResponseTopic()` | Returns the configured response topic |
| `Close()` | Unsubscribes and cleans up resources |

### Errors

| Error | Description |
|-------|-------------|
| `ErrTimeout` | Request timed out waiting for response |
| `ErrClientClosed` | Client is not connected |
| `ErrNoResponseTopic` | No response topic configured |

## How It Works

1. The handler subscribes to a response topic on initialization
2. When `Call()` or `Request()` is invoked:
   - A unique correlation ID is generated (nanosecond timestamp)
   - The request is published with `ResponseTopic` and `CorrelationData` properties
   - Headers are added as User Properties
3. The responder receives the request and sends a response to the response topic with the same correlation data
4. The handler matches the response to the waiting request using the correlation ID
5. The response is returned to the caller
