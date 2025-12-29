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
  - Fuzz tests (edge cases, security) - include random data seeds
- **No wrappers**: Direct usage of standard library types, no unnecessary abstractions
- **On finish each step**:
  1. Run `yake tests` - all tests must pass
  2. Git commit with descriptive message
  3. Update this roadmap - mark completed items with `[x]`

## 1. Project Initialization

- [x] Initialize Go module with `go mod init github.com/vitalvas/mqttv5`
- [x] Add gorilla/websocket dependency
- [x] Add testify dependency for testing
- [x] Configure golangci-lint

## 2. Data Types and Encoding

### 2.1 Basic Types

- [x] Implement UTF-8 string encoding (2-byte length prefix)
- [x] Implement UTF-8 string decoding with validation
- [x] Implement binary data encoding (2-byte length prefix)
- [x] Implement binary data decoding
- [x] Implement UTF-8 string pair encoding/decoding
- [x] Write unit tests for basic types (empty strings, max length, invalid UTF-8, binary data)
- [x] Write benchmarks for string/binary encoding/decoding (target: zero-alloc)
- [x] Write fuzz tests for string/binary decoding

### 2.2 Variable Byte Integer

- [x] Implement variable byte integer encoder (1-4 bytes)
- [x] Implement variable byte integer decoder
- [x] Add validation for maximum value (268,435,455)
- [x] Add validation for minimum encoding length
- [x] Write unit tests for variable byte integer (edge cases: 0, 127, 128, 16383, 16384, max value)
- [x] Write benchmarks for variable byte integer (target: zero-alloc)
- [x] Write fuzz tests for variable byte integer decoding

## 3. Reason Codes

- [x] Define all reason codes as constants (0x00-0x9F)
- [x] Create reason code to string mapping
- [x] Group reason codes by packet type validity
- [x] Implement reason code validation per packet type
- [x] Write unit tests for reason codes (string mapping, validation per packet type)

## 4. Properties System

### 4.1 Property Identifiers

- [x] Define all 42 property identifier constants
- [x] Define property data types (byte, two-byte int, four-byte int, variable int, UTF-8 string, binary data, string pair)
- [x] Create property identifier to type mapping

### 4.2 Properties Implementation

- [x] Implement Property struct (identifier, value)
- [x] Implement Properties collection type
- [x] Implement properties encoder
- [x] Implement properties decoder
- [x] Implement property validation per packet type
- [x] Add helper methods: Get, Set, Has, Delete
- [x] Add typed getters: GetUint32, GetString, GetBinary, GetStringPair
- [x] Write unit tests for properties (encode/decode, all property types, validation)
- [x] Write benchmarks for properties encoding/decoding (target: minimal allocs)
- [x] Write fuzz tests for properties decoding

## 5. Fixed Header

- [x] Implement fixed header struct (packet type, flags, remaining length)
- [x] Implement fixed header encoder
- [x] Implement fixed header decoder
- [x] Add flag validation per packet type
- [x] Write unit tests for fixed header (all packet types, flag validation)
- [x] Write benchmarks for fixed header encoding/decoding
- [x] Write fuzz tests for fixed header decoding

## 6. Packet Interface

- [x] Define Packet interface (Type, Encode, Decode, Validate)
- [x] Define PacketWithID interface for packets with packet identifiers
- [x] Define PacketWithProperties interface

## 6.1 Message Struct

- [x] Define Message struct with public fields:
  - Topic, Payload, QoS, Retain
  - PayloadFormat, MessageExpiry, ContentType
  - ResponseTopic, CorrelationData
  - UserProperties, SubscriptionIdentifiers
- [x] Implement Message to PublishPacket conversion
- [x] Implement PublishPacket to Message conversion
- [x] Write unit/benchmark tests for Message

## 7. Control Packets

### 7.1 CONNECT (Type 1)

- [x] Define ConnectPacket struct
- [x] Implement protocol name encoding ("MQTT")
- [x] Implement protocol version (5)
- [x] Implement connect flags (clean start, will flag, will QoS, will retain, password, username)
- [x] Implement keep alive (2-byte)
- [x] Implement CONNECT properties (session expiry, receive max, max packet size, topic alias max, request response info, request problem info, user properties, auth method, auth data)
- [x] Implement payload: client ID, will properties, will topic, will payload, username, password
- [x] Implement encoder and decoder
- [x] Implement validation
- [x] Write unit tests for CONNECT packet (all fields, properties, edge cases)
- [x] Write benchmarks for CONNECT encoding/decoding
- [x] Write fuzz tests for CONNECT decoding

### 7.2 CONNACK (Type 2)

- [x] Define ConnackPacket struct
- [x] Implement session present flag
- [x] Implement reason code
- [x] Implement CONNACK properties (session expiry, receive max, max QoS, retain available, max packet size, assigned client ID, topic alias max, reason string, user properties, wildcard subscription available, subscription identifiers available, shared subscription available, server keep alive, response information, server reference, auth method, auth data)
- [x] Implement encoder and decoder
- [x] Implement validation
- [x] Write unit tests for CONNACK packet (all properties, reason codes)
- [x] Write benchmarks for CONNACK encoding/decoding
- [x] Write fuzz tests for CONNACK decoding

### 7.3 PUBLISH (Type 3)

- [x] Define PublishPacket struct
- [x] Implement fixed header flags (DUP, QoS, RETAIN)
- [x] Implement topic name
- [x] Implement packet identifier (QoS > 0)
- [x] Implement PUBLISH properties (payload format indicator, message expiry interval, topic alias, response topic, correlation data, user properties, subscription identifier, content type)
- [x] Implement payload
- [x] Implement encoder and decoder
- [x] Implement validation
- [x] Write unit tests for PUBLISH packet (all QoS levels, flags, properties)
- [x] Write benchmarks for PUBLISH encoding/decoding
- [x] Write fuzz tests for PUBLISH decoding

### 7.4 PUBACK (Type 4)

- [x] Define PubackPacket struct
- [x] Implement packet identifier
- [x] Implement reason code (optional if success with no properties)
- [x] Implement PUBACK properties (reason string, user properties)
- [x] Implement encoder and decoder
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for PUBACK packet

### 7.5 PUBREC (Type 5)

- [x] Define PubrecPacket struct
- [x] Implement packet identifier
- [x] Implement reason code
- [x] Implement PUBREC properties (reason string, user properties)
- [x] Implement encoder and decoder
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for PUBREC packet

### 7.6 PUBREL (Type 6)

- [x] Define PubrelPacket struct
- [x] Implement packet identifier
- [x] Implement reason code
- [x] Implement PUBREL properties (reason string, user properties)
- [x] Implement encoder and decoder
- [x] Implement fixed header flags validation (must be 0x02)
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for PUBREL packet

### 7.7 PUBCOMP (Type 7)

- [x] Define PubcompPacket struct
- [x] Implement packet identifier
- [x] Implement reason code
- [x] Implement PUBCOMP properties (reason string, user properties)
- [x] Implement encoder and decoder
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for PUBCOMP packet

### 7.8 SUBSCRIBE (Type 8)

- [x] Define SubscribePacket struct
- [x] Define Subscription struct (topic filter, options)
- [x] Implement subscription options (max QoS, no local, retain as published, retain handling)
- [x] Implement packet identifier
- [x] Implement SUBSCRIBE properties (subscription identifier, user properties)
- [x] Implement subscriptions list
- [x] Implement encoder and decoder
- [x] Implement fixed header flags validation (must be 0x02)
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for SUBSCRIBE packet

### 7.9 SUBACK (Type 9)

- [x] Define SubackPacket struct
- [x] Implement packet identifier
- [x] Implement SUBACK properties (reason string, user properties)
- [x] Implement reason codes list
- [x] Implement encoder and decoder
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for SUBACK packet

### 7.10 UNSUBSCRIBE (Type 10)

- [x] Define UnsubscribePacket struct
- [x] Implement packet identifier
- [x] Implement UNSUBSCRIBE properties (user properties)
- [x] Implement topic filters list
- [x] Implement encoder and decoder
- [x] Implement fixed header flags validation (must be 0x02)
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for UNSUBSCRIBE packet

### 7.11 UNSUBACK (Type 11)

- [x] Define UnsubackPacket struct
- [x] Implement packet identifier
- [x] Implement UNSUBACK properties (reason string, user properties)
- [x] Implement reason codes list
- [x] Implement encoder and decoder
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for UNSUBACK packet

### 7.12 PINGREQ (Type 12)

- [x] Define PingreqPacket struct
- [x] Implement encoder and decoder (no variable header, no payload)
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for PINGREQ packet

### 7.13 PINGRESP (Type 13)

- [x] Define PingrespPacket struct
- [x] Implement encoder and decoder (no variable header, no payload)
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for PINGRESP packet

### 7.14 DISCONNECT (Type 14)

- [x] Define DisconnectPacket struct
- [x] Implement reason code
- [x] Implement DISCONNECT properties (session expiry interval, reason string, user properties, server reference)
- [x] Implement encoder and decoder
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for DISCONNECT packet

### 7.15 AUTH (Type 15)

- [x] Define AuthPacket struct
- [x] Implement reason code
- [x] Implement AUTH properties (auth method, auth data, reason string, user properties)
- [x] Implement encoder and decoder
- [x] Implement fixed header flags validation (must be 0x00)
- [x] Implement validation
- [x] Write unit/benchmark/fuzz tests for AUTH packet

## 8. Codec

- [x] Implement ReadPacket function using io.Reader directly
- [x] Implement WritePacket function using io.Writer directly
- [x] Implement maximum packet size enforcement
- [x] Write unit tests for codec (round-trip all packet types, max size enforcement)
- [x] Write benchmarks for codec (target: minimal allocs with buffer pool)
- [x] Write fuzz tests for codec

## 9. Transport Layer

### 9.1 Transport Interface

- [x] Define Conn interface (Read, Write, Close, SetDeadline, LocalAddr, RemoteAddr)
- [x] Define Listener interface (Accept, Close, Addr)
- [x] Define Dialer interface (Dial with context)

### 9.2 TCP Transport

- [x] Implement TCP listener using net.Listen
- [x] Implement TCP dialer with timeout using net.DialTimeout
- [x] Write unit/benchmark tests for TCP transport

### 9.3 TLS Transport

- [x] Implement TLS listener using tls.Listen
- [x] Implement TLS dialer using tls.Dial
- [x] Write unit/benchmark tests for TLS transport

### 9.4 WebSocket Transport

- [x] Implement WebSocket dialer using gorilla/websocket.Dialer
- [x] Implement subprotocol negotiation ("mqtt")
- [x] Handle binary message framing
- [x] Write unit/benchmark tests for WebSocket transport

### 9.5 WebSocket HTTP Handler (Embeddable)

- [x] Implement WSHandler as http.Handler interface
- [x] Support embedding into net/http server
- [x] Implement connection callback for new WebSocket connections
- [x] Write unit tests for HTTP handler embedding

## 10. Topic Matching

- [x] Implement topic name validation (UTF-8, no wildcards, no null)
- [x] Implement topic filter validation (valid wildcard usage)
- [x] Implement single-level wildcard (+) matching
- [x] Implement multi-level wildcard (#) matching
- [x] Implement shared subscription parsing ($share/group/filter)
- [x] Implement system topic detection ($SYS/)
- [x] Implement TopicMatcher interface
- [x] Implement efficient topic tree data structure
- [x] Write unit tests for topic matching (wildcards, shared subscriptions, edge cases)
- [x] Write benchmarks for topic matching (target: O(levels) lookup)
- [x] Write fuzz tests for topic validation and matching

## 11. Session State

### 11.1 Session Interface

- [x] Define Session interface
- [x] Define session state: client ID, subscriptions, pending messages, packet ID counter
- [x] Define session expiry handling

### 11.2 Subscription State

- [x] Define Subscription struct (filter, QoS, options, identifier)
- [x] Implement subscription matching

### 11.3 Session Store Interface

- [x] Define SessionStore interface (Create, Get, Update, Delete, List)
- [x] Define session expiry callback interface
- [x] Implement in-memory session store (reference implementation)
- [x] Write unit/benchmark tests for session store

## 12. QoS State Machines

### 12.1 Packet ID Manager

- [x] Implement packet ID allocator (1-65535)
- [x] Implement packet ID release
- [x] Handle wraparound
- [x] Write unit/benchmark tests for packet ID manager

### 12.2 QoS 1 Flow

- [x] Define QoS1State (awaiting PUBACK)
- [x] Implement message tracking
- [x] Implement retry logic
- [x] Implement timeout handling
- [x] Write unit/benchmark tests for QoS 1 flow

### 12.3 QoS 2 Flow

- [x] Define QoS2State (PUBREC received, PUBREL sent, awaiting PUBCOMP)
- [x] Implement sender state machine
- [x] Implement receiver state machine
- [x] Implement retry logic
- [x] Implement timeout handling
- [x] Write unit/benchmark tests for QoS 2 flow

### 12.4 Message Store

- [x] Define MessageStore interface
- [x] Implement in-memory message store
- [x] Implement message expiry
- [x] Write unit/benchmark tests for message store

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
