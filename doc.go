// Package mqttv5 provides an SDK for implementing MQTT v5.0 clients and brokers.
//
// This package implements the MQTT Version 5.0 OASIS Standard:
// https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
//
// # Features
//
//   - All 15 MQTT v5.0 control packet types
//   - Complete properties system (42 property identifiers)
//   - QoS 0, 1, 2 message flows with state machines
//   - Topic matching with wildcard support (+, #)
//   - Transport: TCP, TLS, WebSocket, WSS
//   - Pluggable interfaces for session, authentication, and clustering
//
// # Packet Types
//
// The package provides structs for all MQTT v5.0 control packets:
//
//   - ConnectPacket, ConnackPacket: Connection establishment
//   - PublishPacket, PubackPacket, PubrecPacket, PubrelPacket, PubcompPacket: Message delivery
//   - SubscribePacket, SubackPacket: Topic subscription
//   - UnsubscribePacket, UnsubackPacket: Topic unsubscription
//   - PingreqPacket, PingrespPacket: Keep-alive
//   - DisconnectPacket: Connection termination
//   - AuthPacket: Enhanced authentication
//
// Use ReadPacket and WritePacket to read/write packets from/to connections:
//
//	// Read a packet
//	pkt, n, err := mqttv5.ReadPacket(conn, maxPacketSize)
//
//	// Write a packet
//	n, err := mqttv5.WritePacket(conn, packet, maxPacketSize)
//
// # Client
//
// Use the high-level Client API for connecting to MQTT brokers:
//
//	client, err := mqttv5.Dial("tcp://localhost:1883",
//	    mqttv5.WithClientID("my-client"),
//	    mqttv5.WithKeepAlive(60),
//	)
//	defer client.Disconnect()
//
// TLS connections:
//
//	client, err := mqttv5.Dial("tls://localhost:8883",
//	    mqttv5.WithTLSConfig(&tls.Config{}),
//	)
//
// WebSocket connections:
//
//	client, err := mqttv5.Dial("ws://localhost:8080/mqtt")
//
// # Server
//
// Use the high-level Server API for building MQTT brokers:
//
//	listener, _ := net.Listen("tcp", ":1883")
//	srv := mqttv5.NewServer(
//	    mqttv5.WithListener(listener),
//	    mqttv5.OnConnect(func(c *mqttv5.ServerClient) { ... }),
//	    mqttv5.OnMessage(func(c *mqttv5.ServerClient, m *mqttv5.Message) { ... }),
//	)
//	srv.ListenAndServe()
//
// For TLS server, create a TLS listener:
//
//	tlsListener, _ := tls.Listen("tcp", ":8883", tlsConfig)
//	srv := mqttv5.NewServer(mqttv5.WithListener(tlsListener))
//
// Multiple listeners can be added to serve on multiple ports:
//
//	tcpListener, _ := net.Listen("tcp", ":1883")
//	tlsListener, _ := tls.Listen("tcp", ":8883", tlsConfig)
//	srv := mqttv5.NewServer(
//	    mqttv5.WithListener(tcpListener),
//	    mqttv5.WithListener(tlsListener),
//	)
//
// For WebSocket server, use WSServer as an http.Handler:
//
//	ws := mqttv5.NewWSServer(
//	    mqttv5.OnConnect(func(c *mqttv5.ServerClient) { ... }),
//	)
//	ws.Start()
//	http.Handle("/mqtt", ws)
//
// # Session Management
//
// Session state can be managed using the Session and SessionStore interfaces.
// A reference implementation is provided with MemorySession and MemorySessionStore:
//
//	store := mqttv5.NewMemorySessionStore()
//	session := mqttv5.NewMemorySession("client-id")
//	store.Create(session)
//
// Sessions track subscriptions, pending messages, and packet IDs:
//
//	session.AddSubscription(mqttv5.Subscription{
//	    TopicFilter: "sensors/#",
//	    QoS: 1,
//	})
//	packetID := session.NextPacketID()
//
// # QoS State Machines
//
// For QoS 1 and 2 message flows, use the provided state machines:
//
//	// QoS 1 tracking
//	tracker := mqttv5.NewQoS1Tracker(retryTimeout, maxRetries)
//	tracker.StartSend(packetID, message)
//	tracker.AcknowledgeSend(packetID)
//
//	// QoS 2 tracking
//	tracker := mqttv5.NewQoS2Tracker(retryTimeout, maxRetries)
//	tracker.StartSend(packetID, message)
//	tracker.HandlePubrec(packetID)
//	tracker.HandlePubcomp(packetID)
//
// # Flow Control
//
// Flow control prevents overwhelming clients with too many in-flight messages:
//
//	fc := mqttv5.NewFlowController(receiveMaximum)
//	if fc.CanSend() {
//	    fc.MessageSent()
//	}
//	fc.MessageAcknowledged()
//
// # Topic Matching
//
// Topic validation and matching support MQTT wildcards:
//
//	// Validate topic names and filters
//	err := mqttv5.ValidateTopicName("sensors/temperature")
//	err = mqttv5.ValidateTopicFilter("sensors/+/status")
//
//	// Match topics against filters
//	matched := mqttv5.TopicMatch("sensors/#", "sensors/room1/temp")
//
//	// Parse shared subscriptions
//	shared, _ := mqttv5.ParseSharedSubscription("$share/group/topic")
//
// # Authentication
//
// Implement the Authenticator interface for basic authentication:
//
//	type MyAuth struct{}
//	func (a *MyAuth) Authenticate(ctx mqttv5.AuthContext) mqttv5.AuthResult {
//	    if ctx.Username == "valid" {
//	        return mqttv5.AuthResult{Success: true}
//	    }
//	    return mqttv5.AuthResult{Success: false, ReasonCode: mqttv5.ReasonBadUserNameOrPassword}
//	}
//
// For enhanced authentication (multi-step), implement EnhancedAuthenticator.
//
// # Authorization
//
// Implement the Authorizer interface for access control:
//
//	authorizer := &mqttv5.ACLAuthorizer{}
//	authorizer.AddEntry(mqttv5.ACLEntry{
//	    ClientPattern: "*",
//	    TopicPattern: "public/#",
//	    CanPublish: true,
//	    CanSubscribe: true,
//	})
//
// # Clustering
//
// The package defines interfaces for building clustered MQTT brokers:
//
//   - ClusterNode: Represents a node in the cluster
//   - ClusterTransport: Handles inter-node communication
//   - SubscriptionSync: Synchronizes subscription state
//   - RetainedSync: Synchronizes retained messages
//   - SessionMigration: Handles session migration between nodes
//
// These interfaces are designed to be implemented by the user to support
// various clustering backends (Redis, etcd, Raft, etc.).
//
// # Metrics
//
// Implement the Metrics interface to collect operational metrics:
//
//	metrics := mqttv5.NewMemoryMetrics()
//	brokerMetrics := mqttv5.NewBrokerMetrics(metrics)
//	brokerMetrics.ConnectionOpened()
//	brokerMetrics.MessageReceived("PUBLISH")
//
// # Logging
//
// Implement the Logger interface for structured logging:
//
//	logger := mqttv5.NewStdLogger(os.Stdout, mqttv5.LogLevelInfo)
//	logger.Info("client connected", mqttv5.LogFields{"client_id": "test"})
package mqttv5
