package mqttv5

import (
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClient is a lightweight MQTT client for load testing.
type testClient struct {
	conn     net.Conn
	clientID string
}

func newTestClient(addr, clientID string) (*testClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	tc := &testClient{
		conn:     conn,
		clientID: clientID,
	}

	// Send CONNECT
	connect := &ConnectPacket{
		ClientID:   clientID,
		CleanStart: true,
		KeepAlive:  60,
	}
	if _, err := WritePacket(conn, connect, 0); err != nil {
		conn.Close()
		return nil, err
	}

	// Read CONNACK
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	pkt, _, err := ReadPacket(conn, 0)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return nil, err
	}

	connack, ok := pkt.(*ConnackPacket)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("expected CONNACK, got %T", pkt)
	}
	if connack.ReasonCode != ReasonSuccess {
		conn.Close()
		return nil, fmt.Errorf("connect failed: %v", connack.ReasonCode)
	}

	return tc, nil
}

func (tc *testClient) subscribe(topic string, qos byte) error {
	sub := &SubscribePacket{
		PacketID: 1,
		Subscriptions: []Subscription{
			{TopicFilter: topic, QoS: qos},
		},
	}
	if _, err := WritePacket(tc.conn, sub, 0); err != nil {
		return err
	}

	tc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	pkt, _, err := ReadPacket(tc.conn, 0)
	tc.conn.SetReadDeadline(time.Time{})
	if err != nil {
		return err
	}

	if _, ok := pkt.(*SubackPacket); !ok {
		return fmt.Errorf("expected SUBACK, got %T", pkt)
	}
	return nil
}

func (tc *testClient) publish(topic string, payload []byte, qos byte) error {
	pub := &PublishPacket{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
	}
	if qos > 0 {
		pub.PacketID = 1
	}
	_, err := WritePacket(tc.conn, pub, 0)
	return err
}

func (tc *testClient) readPacket(timeout time.Duration) (Packet, error) {
	tc.conn.SetReadDeadline(time.Now().Add(timeout))
	pkt, _, err := ReadPacket(tc.conn, 0)
	tc.conn.SetReadDeadline(time.Time{})
	return pkt, err
}

func (tc *testClient) close() {
	disconnect := &DisconnectPacket{ReasonCode: ReasonSuccess}
	WritePacket(tc.conn, disconnect, 0)
	tc.conn.Close()
}

func TestConcurrentConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	server := NewServer(WithListener(listener))
	go server.ListenAndServe()
	defer server.Close()

	addr := listener.Addr().String()

	numClients := 200
	clients := make([]*testClient, numClients)
	var wg sync.WaitGroup
	var connectErrors atomic.Int32

	start := time.Now()
	wg.Add(numClients)
	for i := range numClients {
		go func(idx int) {
			defer wg.Done()
			client, err := newTestClient(addr, fmt.Sprintf("client-%d", idx))
			if err != nil {
				connectErrors.Add(1)
				return
			}
			clients[idx] = client
		}(i)
	}
	wg.Wait()
	connectTime := time.Since(start)

	t.Logf("Connected %d clients in %v (%.0f conn/sec)", numClients, connectTime, float64(numClients)/connectTime.Seconds())
	assert.Equal(t, int32(0), connectErrors.Load(), "all clients should connect successfully")
	assert.Equal(t, numClients, server.ClientCount(), "server should track all clients")

	// Cleanup
	for _, client := range clients {
		if client != nil {
			client.close()
		}
	}

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, server.ClientCount(), "all clients should be disconnected")
}

func TestMessageThroughput(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	server := NewServer(WithListener(listener))
	go server.ListenAndServe()
	defer server.Close()

	addr := listener.Addr().String()

	numSubscribers := 50
	subscribers := make([]*testClient, numSubscribers)
	topic := "test/fanout"

	// Create subscribers
	for i := range numSubscribers {
		client, err := newTestClient(addr, fmt.Sprintf("sub-%d", i))
		require.NoError(t, err)
		require.NoError(t, client.subscribe(topic, 0))
		subscribers[i] = client
	}
	defer func() {
		for _, sub := range subscribers {
			sub.close()
		}
	}()

	// Create publisher
	publisher, err := newTestClient(addr, "publisher")
	require.NoError(t, err)
	defer publisher.close()

	// Publish message
	payload := []byte("hello world")
	start := time.Now()
	require.NoError(t, publisher.publish(topic, payload, 0))

	// Verify all subscribers receive the message
	var received atomic.Int32
	var wg sync.WaitGroup
	wg.Add(numSubscribers)

	for _, sub := range subscribers {
		go func(s *testClient) {
			defer wg.Done()
			pkt, err := s.readPacket(2 * time.Second)
			if err != nil {
				return
			}
			if pub, ok := pkt.(*PublishPacket); ok {
				if pub.Topic == topic && string(pub.Payload) == string(payload) {
					received.Add(1)
				}
			}
		}(sub)
	}
	wg.Wait()
	deliveryTime := time.Since(start)

	t.Logf("Delivered to %d/%d subscribers in %v", received.Load(), numSubscribers, deliveryTime)
	assert.Equal(t, int32(numSubscribers), received.Load(), "all subscribers should receive message")
}

func TestResourceUsage(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	server := NewServer(WithListener(listener))
	go server.ListenAndServe()
	defer server.Close()

	addr := listener.Addr().String()

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	numClients := 200
	clients := make([]*testClient, numClients)

	for i := range numClients {
		client, err := newTestClient(addr, fmt.Sprintf("mem-client-%d", i))
		require.NoError(t, err)
		clients[i] = client
	}
	defer func() {
		for _, client := range clients {
			if client != nil {
				client.close()
			}
		}
	}()

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	heapIncrease := m2.HeapAlloc - m1.HeapAlloc
	perClient := heapIncrease / uint64(numClients)

	t.Logf("Memory stats for %d clients:", numClients)
	t.Logf("  Total heap increase: %d KB (%.2f MB)", heapIncrease/1024, float64(heapIncrease)/(1024*1024))
	t.Logf("  Per client: %d bytes", perClient)
	t.Logf("  Goroutines: %d", runtime.NumGoroutine())
}

// setupTestServer creates a test server and returns the address and cleanup function.
func setupTestServer(t *testing.T) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := NewServer(WithListener(listener))
	go server.ListenAndServe()

	cleanup := func() {
		server.Close()
		listener.Close()
	}

	return listener.Addr().String(), cleanup
}

func TestRPCRequestResponse(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	t.Run("20 concurrent RPC pairs", func(t *testing.T) {
		// Simulates RPC: client sends request, another client responds
		numRPCPairs := 20
		var wg sync.WaitGroup
		var successfulRPCs atomic.Int32

		for i := range numRPCPairs {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				requestTopic := fmt.Sprintf("rpc/request/%d", idx)
				responseTopic := fmt.Sprintf("rpc/response/%d", idx)

				// Create requester
				requester, err := newTestClient(addr, fmt.Sprintf("requester-%d", idx))
				if err != nil {
					return
				}
				defer requester.close()

				// Create responder
				responder, err := newTestClient(addr, fmt.Sprintf("responder-%d", idx))
				if err != nil {
					return
				}
				defer responder.close()

				// Responder subscribes to requests
				if err := responder.subscribe(requestTopic, 0); err != nil {
					return
				}

				// Requester subscribes to responses
				if err := requester.subscribe(responseTopic, 0); err != nil {
					return
				}

				// Send request
				requestPayload := []byte(fmt.Sprintf(`{"id":%d,"method":"getData"}`, idx))
				if err := requester.publish(requestTopic, requestPayload, 0); err != nil {
					return
				}

				// Responder reads request
				pkt, err := responder.readPacket(2 * time.Second)
				if err != nil {
					return
				}
				if _, ok := pkt.(*PublishPacket); !ok {
					return
				}

				// Responder sends response
				responsePayload := []byte(fmt.Sprintf(`{"id":%d,"result":"ok"}`, idx))
				if err := responder.publish(responseTopic, responsePayload, 0); err != nil {
					return
				}

				// Requester reads response
				pkt, err = requester.readPacket(2 * time.Second)
				if err != nil {
					return
				}
				if pub, ok := pkt.(*PublishPacket); ok {
					if string(pub.Payload) == string(responsePayload) {
						successfulRPCs.Add(1)
					}
				}
			}(i)
		}
		wg.Wait()

		t.Logf("Successful RPCs: %d/%d", successfulRPCs.Load(), numRPCPairs)
		assert.GreaterOrEqual(t, successfulRPCs.Load(), int32(numRPCPairs*9/10), "at least 90%% RPCs should succeed")
	})
}

func TestBroadcastTo100Subscribers(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	numSubscribers := 100
	broadcastTopic := "broadcast/all"
	subscribers := make([]*testClient, numSubscribers)

	// Create all subscribers
	for i := range numSubscribers {
		client, err := newTestClient(addr, fmt.Sprintf("broadcast-sub-%d", i))
		require.NoError(t, err)
		require.NoError(t, client.subscribe(broadcastTopic, 0))
		subscribers[i] = client
	}
	defer func() {
		for _, s := range subscribers {
			s.close()
		}
	}()

	// Create broadcaster
	broadcaster, err := newTestClient(addr, "broadcaster")
	require.NoError(t, err)
	defer broadcaster.close()

	// Broadcast message
	payload := []byte(`{"type":"announcement","msg":"hello all"}`)
	start := time.Now()
	require.NoError(t, broadcaster.publish(broadcastTopic, payload, 0))

	// Count received messages
	var received atomic.Int32
	var wg sync.WaitGroup
	wg.Add(numSubscribers)

	for _, sub := range subscribers {
		go func(s *testClient) {
			defer wg.Done()
			pkt, err := s.readPacket(5 * time.Second)
			if err != nil {
				return
			}
			if pub, ok := pkt.(*PublishPacket); ok {
				if pub.Topic == broadcastTopic {
					received.Add(1)
				}
			}
		}(sub)
	}
	wg.Wait()
	deliveryTime := time.Since(start)

	t.Logf("Broadcast delivered to %d/%d subscribers in %v", received.Load(), numSubscribers, deliveryTime)
	assert.Equal(t, int32(numSubscribers), received.Load(), "all subscribers should receive broadcast")
}

func TestIoTTelemetrySimulation(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	t.Run("50 devices 3 messages each", func(t *testing.T) {
		// Simulates IoT: many devices sending telemetry, one backend collecting
		numDevices := 50
		messagesPerDevice := 3
		telemetryTopic := "telemetry/+"

		// Create backend collector
		collector, err := newTestClient(addr, "collector")
		require.NoError(t, err)
		require.NoError(t, collector.subscribe(telemetryTopic, 0))
		defer collector.close()

		// Create devices
		devices := make([]*testClient, numDevices)
		for i := range numDevices {
			device, err := newTestClient(addr, fmt.Sprintf("device-%d", i))
			require.NoError(t, err)
			devices[i] = device
		}
		defer func() {
			for _, d := range devices {
				d.close()
			}
		}()

		// Devices send telemetry concurrently
		var wg sync.WaitGroup
		var messagesSent atomic.Int32
		start := time.Now()

		for i, device := range devices {
			wg.Add(1)
			go func(idx int, d *testClient) {
				defer wg.Done()
				topic := fmt.Sprintf("telemetry/%d", idx)
				for j := range messagesPerDevice {
					payload := []byte(fmt.Sprintf(`{"device":%d,"seq":%d,"temp":%.1f}`, idx, j, 20.0+float64(j)*0.1))
					if err := d.publish(topic, payload, 0); err != nil {
						return
					}
					messagesSent.Add(1)
				}
			}(i, device)
		}
		wg.Wait()
		sendTime := time.Since(start)

		totalMessages := numDevices * messagesPerDevice
		t.Logf("Sent %d messages from %d devices in %v (%.0f msg/sec)",
			messagesSent.Load(), numDevices, sendTime, float64(messagesSent.Load())/sendTime.Seconds())
		assert.Equal(t, int32(totalMessages), messagesSent.Load(), "all messages should be sent")
	})
}

func TestChatRoomSimulation(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Simulates chat: multiple users in a room, each sends messages
	numUsers := 20
	messagesPerUser := 3
	chatTopic := "chat/room1"

	// Create users, all subscribed to same topic
	users := make([]*testClient, numUsers)
	for i := range numUsers {
		user, err := newTestClient(addr, fmt.Sprintf("user-%d", i))
		require.NoError(t, err)
		require.NoError(t, user.subscribe(chatTopic, 0))
		users[i] = user
	}
	defer func() {
		for _, u := range users {
			u.close()
		}
	}()

	// Each user sends messages
	var wg sync.WaitGroup
	var messagesSent atomic.Int32
	start := time.Now()

	for i, user := range users {
		wg.Add(1)
		go func(idx int, u *testClient) {
			defer wg.Done()
			for j := range messagesPerUser {
				payload := []byte(fmt.Sprintf(`{"user":%d,"msg":"hello %d"}`, idx, j))
				if err := u.publish(chatTopic, payload, 0); err != nil {
					return
				}
				messagesSent.Add(1)
				time.Sleep(time.Millisecond) // Simulate typing delay
			}
		}(i, user)
	}
	wg.Wait()
	sendTime := time.Since(start)

	totalMessages := numUsers * messagesPerUser
	t.Logf("Chat simulation: %d users sent %d messages in %v",
		numUsers, messagesSent.Load(), sendTime)
	assert.Equal(t, int32(totalMessages), messagesSent.Load(), "all chat messages should be sent")
}

func TestCommandResponseWith100Clients(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	t.Run("server to all clients", func(t *testing.T) {
		// Simulates: server sends command to all clients, clients respond
		numClients := 100
		commandTopic := "commands/all"
		responseTopicPrefix := "responses/"

		// Create clients
		clients := make([]*testClient, numClients)
		for i := range numClients {
			client, err := newTestClient(addr, fmt.Sprintf("cmd-client-%d", i))
			require.NoError(t, err)
			require.NoError(t, client.subscribe(commandTopic, 0))
			clients[i] = client
		}
		defer func() {
			for _, c := range clients {
				c.close()
			}
		}()

		// Create command server
		cmdServer, err := newTestClient(addr, "cmd-server")
		require.NoError(t, err)
		require.NoError(t, cmdServer.subscribe(responseTopicPrefix+"+", 0))
		defer cmdServer.close()

		// Send command
		commandPayload := []byte(`{"cmd":"status","id":12345}`)
		start := time.Now()
		require.NoError(t, cmdServer.publish(commandTopic, commandPayload, 0))

		// Clients receive command and respond
		var commandsReceived atomic.Int32
		var wg sync.WaitGroup
		wg.Add(numClients)

		for i, client := range clients {
			go func(idx int, c *testClient) {
				defer wg.Done()
				pkt, err := c.readPacket(5 * time.Second)
				if err != nil {
					return
				}
				if _, ok := pkt.(*PublishPacket); ok {
					commandsReceived.Add(1)
					// Send response
					responseTopic := fmt.Sprintf("%s%d", responseTopicPrefix, idx)
					responsePayload := []byte(fmt.Sprintf(`{"client":%d,"status":"ok"}`, idx))
					c.publish(responseTopic, responsePayload, 0)
				}
			}(i, client)
		}
		wg.Wait()
		commandTime := time.Since(start)

		t.Logf("Command delivered to %d/%d clients in %v", commandsReceived.Load(), numClients, commandTime)
		assert.Equal(t, int32(numClients), commandsReceived.Load(), "all clients should receive command")
	})
}

func TestSustainedLoad(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Simulates sustained traffic - fixed number of messages
	numPublishers := 5
	messagesPerPublisher := 200
	topic := "load/test"

	// Create publishers
	publishers := make([]*testClient, numPublishers)
	for i := range numPublishers {
		pub, err := newTestClient(addr, fmt.Sprintf("load-pub-%d", i))
		require.NoError(t, err)
		publishers[i] = pub
	}
	defer func() {
		for _, p := range publishers {
			p.close()
		}
	}()

	// Run load test
	var messagesSent atomic.Int64
	var wg sync.WaitGroup
	payload := make([]byte, 64)
	start := time.Now()

	for _, pub := range publishers {
		wg.Add(1)
		go func(p *testClient) {
			defer wg.Done()
			for range messagesPerPublisher {
				if err := p.publish(topic, payload, 0); err != nil {
					return
				}
				messagesSent.Add(1)
			}
		}(pub)
	}

	wg.Wait()
	duration := time.Since(start)

	totalSent := messagesSent.Load()
	rate := float64(totalSent) / duration.Seconds()
	t.Logf("Sustained load: %d messages in %v (%.0f msg/sec) with %d publishers",
		totalSent, duration, rate, numPublishers)
	assert.Equal(t, int64(numPublishers*messagesPerPublisher), totalSent, "all messages should be sent")
}

func BenchmarkConnectionRate(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	server := NewServer(WithListener(listener))
	go server.ListenAndServe()
	defer server.Close()

	addr := listener.Addr().String()

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		client, err := newTestClient(addr, fmt.Sprintf("bench-client-%d", i))
		if err != nil {
			b.Fatal(err)
		}
		client.close()
	}
}

func BenchmarkParallelConnections(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	server := NewServer(WithListener(listener))
	go server.ListenAndServe()
	defer server.Close()

	addr := listener.Addr().String()

	b.ResetTimer()
	b.ReportAllocs()

	var counter atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := counter.Add(1)
			client, err := newTestClient(addr, fmt.Sprintf("parallel-client-%d", id))
			if err != nil {
				b.Error(err)
				return
			}
			client.close()
		}
	})
}

func BenchmarkPublishQoS0(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	server := NewServer(WithListener(listener))
	go server.ListenAndServe()
	defer server.Close()

	addr := listener.Addr().String()

	client, err := newTestClient(addr, "bench-pub")
	if err != nil {
		b.Fatal(err)
	}
	defer client.close()

	payload := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		if err := client.publish("test/bench", payload, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishQoS0Parallel(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	server := NewServer(WithListener(listener))
	go server.ListenAndServe()
	defer server.Close()

	addr := listener.Addr().String()

	numClients := runtime.GOMAXPROCS(0)
	clients := make([]*testClient, numClients)

	for i := range numClients {
		client, err := newTestClient(addr, fmt.Sprintf("bench-parallel-pub-%d", i))
		if err != nil {
			b.Fatal(err)
		}
		clients[i] = client
	}
	defer func() {
		for _, c := range clients {
			c.close()
		}
	}()

	payload := make([]byte, 100)
	var clientIdx atomic.Int32

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		idx := int(clientIdx.Add(1)-1) % numClients
		client := clients[idx]

		for pb.Next() {
			if err := client.publish("test/bench", payload, 0); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkFanout(b *testing.B) {
	fanoutSizes := []int{10, 50, 100, 500}

	for _, numSubs := range fanoutSizes {
		b.Run(fmt.Sprintf("%d_subscribers", numSubs), func(b *testing.B) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			defer listener.Close()

			server := NewServer(WithListener(listener))
			go server.ListenAndServe()
			defer server.Close()

			addr := listener.Addr().String()
			topic := "test/fanout"

			// Create subscribers
			subscribers := make([]*testClient, numSubs)
			for i := range numSubs {
				client, err := newTestClient(addr, fmt.Sprintf("fanout-sub-%d", i))
				if err != nil {
					b.Fatal(err)
				}
				if err := client.subscribe(topic, 0); err != nil {
					b.Fatal(err)
				}
				subscribers[i] = client
			}
			defer func() {
				for _, c := range subscribers {
					c.close()
				}
			}()

			// Create publisher
			publisher, err := newTestClient(addr, "fanout-pub")
			if err != nil {
				b.Fatal(err)
			}
			defer publisher.close()

			payload := make([]byte, 100)

			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				if err := publisher.publish(topic, payload, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkManyClientsPublishing(b *testing.B) {
	clientCounts := []int{10, 100, 500}

	for _, numClients := range clientCounts {
		b.Run(fmt.Sprintf("%d_clients", numClients), func(b *testing.B) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			defer listener.Close()

			server := NewServer(WithListener(listener))
			go server.ListenAndServe()
			defer server.Close()

			addr := listener.Addr().String()

			// Create clients
			clients := make([]*testClient, numClients)
			for i := range numClients {
				client, err := newTestClient(addr, fmt.Sprintf("multi-pub-%d", i))
				if err != nil {
					b.Fatal(err)
				}
				clients[i] = client
			}
			defer func() {
				for _, c := range clients {
					c.close()
				}
			}()

			payload := make([]byte, 100)
			var clientIdx atomic.Int32

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				idx := int(clientIdx.Add(1)-1) % numClients
				client := clients[idx]
				topic := fmt.Sprintf("test/client/%d", idx)

				for pb.Next() {
					if err := client.publish(topic, payload, 0); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkSubscribeUnsubscribe(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	server := NewServer(WithListener(listener))
	go server.ListenAndServe()
	defer server.Close()

	addr := listener.Addr().String()

	client, err := newTestClient(addr, "bench-sub")
	if err != nil {
		b.Fatal(err)
	}
	defer client.close()

	b.ResetTimer()
	b.ReportAllocs()

	var packetID uint16 = 1

	for range b.N {
		// Subscribe
		sub := &SubscribePacket{
			PacketID: packetID,
			Subscriptions: []Subscription{
				{TopicFilter: "test/bench/+", QoS: 0},
			},
		}
		if _, err := WritePacket(client.conn, sub, 0); err != nil {
			b.Fatal(err)
		}

		// Read SUBACK
		client.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, err := ReadPacket(client.conn, 0)
		client.conn.SetReadDeadline(time.Time{})
		if err != nil {
			b.Fatal(err)
		}

		// Unsubscribe
		unsub := &UnsubscribePacket{
			PacketID:     packetID,
			TopicFilters: []string{"test/bench/+"},
		}
		if _, err := WritePacket(client.conn, unsub, 0); err != nil {
			b.Fatal(err)
		}

		// Read UNSUBACK
		client.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, _, err = ReadPacket(client.conn, 0)
		client.conn.SetReadDeadline(time.Time{})
		if err != nil {
			b.Fatal(err)
		}

		packetID++
		if packetID == 0 {
			packetID = 1
		}
	}
}

func BenchmarkPingPong(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()

	server := NewServer(WithListener(listener))
	go server.ListenAndServe()
	defer server.Close()

	addr := listener.Addr().String()

	client, err := newTestClient(addr, "bench-ping")
	if err != nil {
		b.Fatal(err)
	}
	defer client.close()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		// Send PINGREQ
		ping := &PingreqPacket{}
		if _, err := WritePacket(client.conn, ping, 0); err != nil {
			b.Fatal(err)
		}

		// Read PINGRESP
		client.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		pkt, _, err := ReadPacket(client.conn, 0)
		client.conn.SetReadDeadline(time.Time{})
		if err != nil {
			b.Fatal(err)
		}

		if _, ok := pkt.(*PingrespPacket); !ok {
			b.Fatalf("expected PINGRESP, got %T", pkt)
		}
	}
}
