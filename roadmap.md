# MQTT v5.0 SDK Implementation Roadmap

SDK providing building blocks for implementing MQTT v5.0 clients and brokers.

**Specification**: https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html

## Development Approach

- **TDD**: Write tests alongside each implementation step
- **Coverage**: Minimum 70%, target 100%
- **Race detector**: All tests run with `-race` flag
- **Performance**: Minimize memory allocations and CPU usage
- **Each step requires**:
  - Unit tests (correctness)
  - Benchmark tests (ns/op, B/op, allocs/op)
  - Fuzz tests (edge cases, security)
- **No wrappers**: Direct usage of standard library types, no unnecessary abstractions
- **On finish each step**:
  1. Run `yake tests` - all tests must pass
  2. Git commit with descriptive message

## 1. Project Initialization

- [ ] Initialize Go module with `go mod init github.com/vitalvas/mqttv5`
- [ ] Add gorilla/websocket dependency
- [ ] Add testify dependency for testing
- [ ] Configure golangci-lint

## 2. Data Types and Encoding

### 2.1 Basic Types

- [ ] Implement UTF-8 string encoding (2-byte length prefix)
- [ ] Implement UTF-8 string decoding with validation
- [ ] Implement binary data encoding (2-byte length prefix)
- [ ] Implement binary data decoding
- [ ] Implement UTF-8 string pair encoding/decoding
- [ ] Write unit tests for basic types (empty strings, max length, invalid UTF-8, binary data)
- [ ] Write benchmarks for string/binary encoding/decoding (target: zero-alloc)
- [ ] Write fuzz tests for string/binary decoding

### 2.2 Variable Byte Integer

- [ ] Implement variable byte integer encoder (1-4 bytes)
- [ ] Implement variable byte integer decoder
- [ ] Add validation for maximum value (268,435,455)
- [ ] Add validation for minimum encoding length
- [ ] Write unit tests for variable byte integer (edge cases: 0, 127, 128, 16383, 16384, max value)
- [ ] Write benchmarks for variable byte integer (target: zero-alloc)
- [ ] Write fuzz tests for variable byte integer decoding

## 3. Reason Codes

- [ ] Define all reason codes as constants (0x00-0x9F)
- [ ] Create reason code to string mapping
- [ ] Group reason codes by packet type validity
- [ ] Implement reason code validation per packet type
- [ ] Write unit tests for reason codes (string mapping, validation per packet type)

## 4. Properties System

### 4.1 Property Identifiers

- [ ] Define all 42 property identifier constants
- [ ] Define property data types (byte, two-byte int, four-byte int, variable int, UTF-8 string, binary data, string pair)
- [ ] Create property identifier to type mapping

### 4.2 Properties Implementation

- [ ] Implement Property struct (identifier, value)
- [ ] Implement Properties collection type
- [ ] Implement properties encoder
- [ ] Implement properties decoder
- [ ] Implement property validation per packet type
- [ ] Add helper methods: Get, Set, Has, Delete
- [ ] Add typed getters: GetUint32, GetString, GetBinary, GetStringPair
- [ ] Write unit tests for properties (encode/decode, all property types, validation)
- [ ] Write benchmarks for properties encoding/decoding (target: minimal allocs)
- [ ] Write fuzz tests for properties decoding

## 5. Fixed Header

- [ ] Implement fixed header struct (packet type, flags, remaining length)
- [ ] Implement fixed header encoder
- [ ] Implement fixed header decoder
- [ ] Add flag validation per packet type
- [ ] Write unit tests for fixed header (all packet types, flag validation)
- [ ] Write benchmarks for fixed header encoding/decoding
- [ ] Write fuzz tests for fixed header decoding

## 6. Packet Interface

- [ ] Define Packet interface (Type, Encode, Decode, Validate)
- [ ] Define PacketWithID interface for packets with packet identifiers
- [ ] Define PacketWithProperties interface

## 6.1 Message Struct

- [ ] Define Message struct with public fields:
  - Topic, Payload, QoS, Retain
  - PayloadFormat, MessageExpiry, ContentType
  - ResponseTopic, CorrelationData
  - UserProperties, SubscriptionIdentifiers
- [ ] Implement Message to PublishPacket conversion
- [ ] Implement PublishPacket to Message conversion
- [ ] Write unit/benchmark tests for Message

## 7. Control Packets

### 7.1 CONNECT (Type 1)

- [ ] Define ConnectPacket struct
- [ ] Implement protocol name encoding ("MQTT")
- [ ] Implement protocol version (5)
- [ ] Implement connect flags (clean start, will flag, will QoS, will retain, password, username)
- [ ] Implement keep alive (2-byte)
- [ ] Implement CONNECT properties (session expiry, receive max, max packet size, topic alias max, request response info, request problem info, user properties, auth method, auth data)
- [ ] Implement payload: client ID, will properties, will topic, will payload, username, password
- [ ] Implement encoder and decoder
- [ ] Implement validation
- [ ] Write unit tests for CONNECT packet (all fields, properties, edge cases)
- [ ] Write benchmarks for CONNECT encoding/decoding
- [ ] Write fuzz tests for CONNECT decoding

### 7.2 CONNACK (Type 2)

- [ ] Define ConnackPacket struct
- [ ] Implement session present flag
- [ ] Implement reason code
- [ ] Implement CONNACK properties (session expiry, receive max, max QoS, retain available, max packet size, assigned client ID, topic alias max, reason string, user properties, wildcard subscription available, subscription identifiers available, shared subscription available, server keep alive, response information, server reference, auth method, auth data)
- [ ] Implement encoder and decoder
- [ ] Implement validation
- [ ] Write unit tests for CONNACK packet (all properties, reason codes)
- [ ] Write benchmarks for CONNACK encoding/decoding
- [ ] Write fuzz tests for CONNACK decoding

### 7.3 PUBLISH (Type 3)

- [ ] Define PublishPacket struct
- [ ] Implement fixed header flags (DUP, QoS, RETAIN)
- [ ] Implement topic name
- [ ] Implement packet identifier (QoS > 0)
- [ ] Implement PUBLISH properties (payload format indicator, message expiry interval, topic alias, response topic, correlation data, user properties, subscription identifier, content type)
- [ ] Implement payload
- [ ] Implement encoder and decoder
- [ ] Implement validation
- [ ] Write unit tests for PUBLISH packet (all QoS levels, flags, properties)
- [ ] Write benchmarks for PUBLISH encoding/decoding
- [ ] Write fuzz tests for PUBLISH decoding

### 7.4 PUBACK (Type 4)

- [ ] Define PubackPacket struct
- [ ] Implement packet identifier
- [ ] Implement reason code (optional if success with no properties)
- [ ] Implement PUBACK properties (reason string, user properties)
- [ ] Implement encoder and decoder
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for PUBACK packet

### 7.5 PUBREC (Type 5)

- [ ] Define PubrecPacket struct
- [ ] Implement packet identifier
- [ ] Implement reason code
- [ ] Implement PUBREC properties (reason string, user properties)
- [ ] Implement encoder and decoder
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for PUBREC packet

### 7.6 PUBREL (Type 6)

- [ ] Define PubrelPacket struct
- [ ] Implement packet identifier
- [ ] Implement reason code
- [ ] Implement PUBREL properties (reason string, user properties)
- [ ] Implement encoder and decoder
- [ ] Implement fixed header flags validation (must be 0x02)
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for PUBREL packet

### 7.7 PUBCOMP (Type 7)

- [ ] Define PubcompPacket struct
- [ ] Implement packet identifier
- [ ] Implement reason code
- [ ] Implement PUBCOMP properties (reason string, user properties)
- [ ] Implement encoder and decoder
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for PUBCOMP packet

### 7.8 SUBSCRIBE (Type 8)

- [ ] Define SubscribePacket struct
- [ ] Define Subscription struct (topic filter, options)
- [ ] Implement subscription options (max QoS, no local, retain as published, retain handling)
- [ ] Implement packet identifier
- [ ] Implement SUBSCRIBE properties (subscription identifier, user properties)
- [ ] Implement subscriptions list
- [ ] Implement encoder and decoder
- [ ] Implement fixed header flags validation (must be 0x02)
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for SUBSCRIBE packet

### 7.9 SUBACK (Type 9)

- [ ] Define SubackPacket struct
- [ ] Implement packet identifier
- [ ] Implement SUBACK properties (reason string, user properties)
- [ ] Implement reason codes list
- [ ] Implement encoder and decoder
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for SUBACK packet

### 7.10 UNSUBSCRIBE (Type 10)

- [ ] Define UnsubscribePacket struct
- [ ] Implement packet identifier
- [ ] Implement UNSUBSCRIBE properties (user properties)
- [ ] Implement topic filters list
- [ ] Implement encoder and decoder
- [ ] Implement fixed header flags validation (must be 0x02)
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for UNSUBSCRIBE packet

### 7.11 UNSUBACK (Type 11)

- [ ] Define UnsubackPacket struct
- [ ] Implement packet identifier
- [ ] Implement UNSUBACK properties (reason string, user properties)
- [ ] Implement reason codes list
- [ ] Implement encoder and decoder
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for UNSUBACK packet

### 7.12 PINGREQ (Type 12)

- [ ] Define PingreqPacket struct
- [ ] Implement encoder and decoder (no variable header, no payload)
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for PINGREQ packet

### 7.13 PINGRESP (Type 13)

- [ ] Define PingrespPacket struct
- [ ] Implement encoder and decoder (no variable header, no payload)
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for PINGRESP packet

### 7.14 DISCONNECT (Type 14)

- [ ] Define DisconnectPacket struct
- [ ] Implement reason code
- [ ] Implement DISCONNECT properties (session expiry interval, reason string, user properties, server reference)
- [ ] Implement encoder and decoder
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for DISCONNECT packet

### 7.15 AUTH (Type 15)

- [ ] Define AuthPacket struct
- [ ] Implement reason code
- [ ] Implement AUTH properties (auth method, auth data, reason string, user properties)
- [ ] Implement encoder and decoder
- [ ] Implement fixed header flags validation (must be 0x00)
- [ ] Implement validation
- [ ] Write unit/benchmark/fuzz tests for AUTH packet

## 8. Codec

- [ ] Implement ReadPacket function using io.Reader directly
- [ ] Implement WritePacket function using io.Writer directly
- [ ] Implement maximum packet size enforcement
- [ ] Add read/write timeout support
- [ ] Write unit tests for codec (round-trip all packet types, max size enforcement)
- [ ] Write benchmarks for codec (target: minimal allocs with buffer pool)
- [ ] Write fuzz tests for codec

## 9. Transport Layer

### 9.1 Transport Interface

- [ ] Define Conn interface (Read, Write, Close, SetDeadline, LocalAddr, RemoteAddr)
- [ ] Define Listener interface (Accept, Close, Addr)
- [ ] Define Dialer interface (Dial with context)

### 9.2 TCP Transport

- [ ] Implement TCP listener using net.Listen
- [ ] Implement TCP dialer with timeout using net.DialTimeout
- [ ] Write unit/benchmark tests for TCP transport

### 9.3 TLS Transport

- [ ] Implement TLS listener using tls.Listen
- [ ] Implement TLS dialer using tls.Dial
- [ ] Write unit/benchmark tests for TLS transport

### 9.4 WebSocket Transport

- [ ] Implement WebSocket dialer using gorilla/websocket.Dialer
- [ ] Implement subprotocol negotiation ("mqtt")
- [ ] Handle binary message framing
- [ ] Write unit/benchmark tests for WebSocket transport

### 9.5 WebSocket HTTP Handler (Embeddable)

- [ ] Implement WSHandler as http.Handler interface
- [ ] Support embedding into net/http server
- [ ] Support embedding into gorilla/mux router
- [ ] Implement connection callback for new WebSocket connections
- [ ] Implement WSS via existing TLS-enabled HTTP server
- [ ] Write unit tests for HTTP handler embedding

## 10. Topic Matching

- [ ] Implement topic name validation (UTF-8, no wildcards, no null)
- [ ] Implement topic filter validation (valid wildcard usage)
- [ ] Implement single-level wildcard (+) matching
- [ ] Implement multi-level wildcard (#) matching
- [ ] Implement shared subscription parsing ($share/group/filter)
- [ ] Implement system topic detection ($SYS/)
- [ ] Implement TopicMatcher interface
- [ ] Implement efficient topic tree data structure
- [ ] Write unit tests for topic matching (wildcards, shared subscriptions, edge cases)
- [ ] Write benchmarks for topic matching (target: O(levels) lookup)
- [ ] Write fuzz tests for topic validation and matching

## 11. Session State

### 11.1 Session Interface

- [ ] Define Session interface
- [ ] Define session state: client ID, subscriptions, pending messages, packet ID counter
- [ ] Define session expiry handling

### 11.2 Subscription State

- [ ] Define Subscription struct (filter, QoS, options, identifier)
- [ ] Implement subscription matching

### 11.3 Session Store Interface

- [ ] Define SessionStore interface (Create, Get, Update, Delete, List)
- [ ] Define session expiry callback interface
- [ ] Implement in-memory session store (reference implementation)
- [ ] Write unit/benchmark tests for session store

## 12. QoS State Machines

### 12.1 Packet ID Manager

- [ ] Implement packet ID allocator (1-65535)
- [ ] Implement packet ID release
- [ ] Handle wraparound
- [ ] Write unit/benchmark tests for packet ID manager

### 12.2 QoS 1 Flow

- [ ] Define QoS1State (awaiting PUBACK)
- [ ] Implement message tracking
- [ ] Implement retry logic
- [ ] Implement timeout handling
- [ ] Write unit/benchmark tests for QoS 1 flow

### 12.3 QoS 2 Flow

- [ ] Define QoS2State (PUBREC received, PUBREL sent, awaiting PUBCOMP)
- [ ] Implement sender state machine
- [ ] Implement receiver state machine
- [ ] Implement retry logic
- [ ] Implement timeout handling
- [ ] Write unit/benchmark tests for QoS 2 flow

### 12.4 Message Store

- [ ] Define MessageStore interface
- [ ] Implement in-memory message store
- [ ] Implement message expiry
- [ ] Write unit/benchmark tests for message store

## 13. Flow Control

- [ ] Implement receive maximum tracking
- [ ] Implement quota management
- [ ] Implement quota replenishment on acknowledgment
- [ ] Write unit/benchmark tests for flow control

## 14. Will Message

- [ ] Define WillMessage struct
- [ ] Implement will delay interval handling
- [ ] Implement will message properties
- [ ] Write unit tests for will message

## 15. Retained Messages

- [ ] Define RetainedStore interface (Set, Get, Delete, Match)
- [ ] Implement in-memory retained store (reference implementation)
- [ ] Write unit/benchmark tests for retained store

## 16. Clustering Interface

- [ ] Define ClusterNode interface
- [ ] Define ClusterTransport interface
- [ ] Define message types for cluster sync
- [ ] Define subscription sync interface
- [ ] Define retained message sync interface
- [ ] Define session migration interface

## 17. Authentication Interface

- [ ] Define Authenticator interface (Authenticate method)
- [ ] Define AuthResult struct (success, reason code, properties)
- [ ] Define AuthContext struct (client ID, username, password, connection info)
- [ ] Define EnhancedAuthenticator interface for AUTH packet exchange

## 18. Authorization Interface

- [ ] Define Authorizer interface
- [ ] Define ACLEntry struct (client pattern, topic pattern, publish, subscribe)
- [ ] Define authorization check methods

## 19. Metrics Interface

- [ ] Define Metrics interface
- [ ] Define metric types: counter, gauge, histogram
- [ ] Define standard metrics: connections, messages, bytes, latency

## 20. Logger Interface

- [ ] Define Logger interface (Debug, Info, Warn, Error)
- [ ] Implement no-op logger as default
- [ ] Add context fields support

## 21. Conformance Tests

- [ ] Test all reason codes per specification
- [ ] Test all properties per packet type
- [ ] Test error conditions from specification
- [ ] Test protocol edge cases
- [ ] Verify zero-alloc paths in hot code

## 22. Examples

- [ ] Create simple client example
- [ ] Create simple broker example
- [ ] Create TLS client example
- [ ] Create WebSocket client example
- [ ] Create embedded WebSocket handler example (net/http)
- [ ] Create embedded WebSocket handler example (gorilla/mux)
- [ ] Create shared subscription example

## 23. Documentation

- [ ] Document all public types and functions with godoc
- [ ] Document packet structures
- [ ] Document transport usage
- [ ] Document session management
- [ ] Document clustering interface implementation
