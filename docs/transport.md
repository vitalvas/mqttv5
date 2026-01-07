# Transport Protocols

Supported transport protocols for MQTT v5.0 connections.

## Overview

| Transport | Client URI | Server Listener | TLS Required |
|-----------|------------|-----------------|--------------|
| TCP | `tcp://host:port` | `net.Listen("tcp", ...)` | No |
| TLS | `tls://host:port` | `tls.Listen(...)` | Yes |
| WebSocket | `ws://host:port/path` | `NewWSHandler(...)` | No |
| WebSocket Secure | `wss://host:port/path` | `NewWSHandler(...)` + TLS | Yes |
| Unix Socket | `unix:///path/to/sock` | `NewUnixListener(...)` | No |
| QUIC | `quic://host:port` | `NewQUICListener(...)` | Yes |

## TCP

```go
// Client
client, _ := mqttv5.Dial(mqttv5.WithServers("tcp://localhost:1883"))

// Server
listener, _ := net.Listen("tcp", ":1883")
srv := mqttv5.NewServer(mqttv5.WithListener(listener))
```

## TLS

```go
// Client
client, _ := mqttv5.Dial(
    mqttv5.WithServers("tls://localhost:8883"),
    mqttv5.WithTLS(&tls.Config{InsecureSkipVerify: true}),
)

// Server
cert, _ := tls.LoadX509KeyPair("server.crt", "server.key")
listener, _ := tls.Listen("tcp", ":8883", &tls.Config{
    Certificates: []tls.Certificate{cert},
})
srv := mqttv5.NewServer(mqttv5.WithListener(listener))
```

## WebSocket

```go
// Client
client, _ := mqttv5.Dial(mqttv5.WithServers("ws://localhost:8080/mqtt"))

// Server (standalone)
wsHandler := mqttv5.NewWSHandler(func(conn mqttv5.Conn) {
    srv.HandleConnection(conn)
})
http.Handle("/mqtt", wsHandler)
http.ListenAndServe(":8080", nil)

// Server (with existing HTTP server)
http.Handle("/mqtt", wsHandler)
http.Handle("/api", apiHandler)
```

## WebSocket Secure (WSS)

```go
// Client
client, _ := mqttv5.Dial(
    mqttv5.WithServers("wss://localhost:8443/mqtt"),
    mqttv5.WithTLS(&tls.Config{InsecureSkipVerify: true}),
)

// Server
cert, _ := tls.LoadX509KeyPair("server.crt", "server.key")
tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
tlsListener, _ := tls.Listen("tcp", ":8443", tlsConfig)

wsHandler := mqttv5.NewWSHandler(func(conn mqttv5.Conn) {
    srv.HandleConnection(conn)
})
http.Serve(tlsListener, wsHandler)
```

## Unix Socket

```go
// Client
client, _ := mqttv5.Dial(mqttv5.WithServers("unix:///var/run/mqtt.sock"))

// Server
listener, _ := mqttv5.NewUnixListener("/var/run/mqtt.sock")
srv := mqttv5.NewServer(mqttv5.WithListener(listener))
```

## QUIC

QUIC requires TLS 1.3 with ALPN protocol "mqtt".

```go
// Client
client, _ := mqttv5.Dial(
    mqttv5.WithServers("quic://localhost:8883"),
    mqttv5.WithTLS(&tls.Config{
        InsecureSkipVerify: true,
        NextProtos:         []string{"mqtt"},
    }),
)

// Server
cert, _ := tls.LoadX509KeyPair("server.crt", "server.key")
tlsConfig := &tls.Config{
    Certificates: []tls.Certificate{cert},
    NextProtos:   []string{"mqtt"},
}
quicListener, _ := mqttv5.NewQUICListener(":8883", tlsConfig, nil)
srv := mqttv5.NewServer(mqttv5.WithListener(quicListener.NetListener()))
```

## Multiple Transports

Run server on multiple transports simultaneously:

```go
tcpListener, _ := net.Listen("tcp", ":1883")
unixListener, _ := mqttv5.NewUnixListener("/var/run/mqtt.sock")

srv := mqttv5.NewServer(
    mqttv5.WithListener(tcpListener),
    mqttv5.WithListener(unixListener),
)

// Add WebSocket on existing HTTP server
wsHandler := mqttv5.NewWSHandler(func(conn mqttv5.Conn) {
    srv.HandleConnection(conn)
})
go http.ListenAndServe(":8080", wsHandler)

srv.ListenAndServe()
```

## WebSocket Options

```go
wsHandler := mqttv5.NewWSHandler(onConnect)

// Origin checking
wsHandler.AllowedOrigins = []string{"https://app.example.com"}

// Allow all origins (not recommended for production)
wsHandler.AllowedOrigins = []string{"*"}

// Max packet size
wsHandler.MaxPacketSize = 1024 * 1024 // 1MB
```

## Custom WebSocket Dialer

```go
dialer := mqttv5.NewWSDialer()
dialer.Header.Set("Authorization", "Bearer token")
dialer.Dialer.HandshakeTimeout = 10 * time.Second
```
