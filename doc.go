// Package mqttv5 provides an SDK for implementing MQTT v5.0 clients and brokers.
//
// This package implements the MQTT Version 5.0 OASIS Standard:
// https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
//
// Features:
//   - All 15 MQTT v5.0 control packet types
//   - Complete properties system (42 property identifiers)
//   - QoS 0, 1, 2 message flows
//   - Topic matching with wildcard support
//   - Transport: TCP, TLS, WebSocket, WSS
//   - Pluggable interfaces for session, authentication, and clustering
package mqttv5
