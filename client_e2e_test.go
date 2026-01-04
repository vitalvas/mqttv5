//go:build e2e

package mqttv5

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// brokerConfig holds the configuration for a public MQTT broker.
type brokerConfig struct {
	name      string
	addr      string
	tlsConfig *tls.Config
	username  string
	password  string
	skip      string
}

// shouldSkip checks if the broker should be skipped and calls t.Skip if so.
func (b *brokerConfig) shouldSkip(t *testing.T) {
	if b.skip != "" {
		t.Skip(b.skip)
	}
}

// connect creates a new client connected to the broker.
func (b *brokerConfig) connect(t *testing.T, prefix string, extraOpts ...Option) *Client {
	t.Helper()
	opts := []Option{
		WithClientID(fmt.Sprintf("mqttv5-e2e-%s-%d", prefix, time.Now().UnixNano())),
		WithCleanStart(true),
		WithConnectTimeout(10 * time.Second),
	}
	if b.tlsConfig != nil {
		opts = append(opts, WithTLS(b.tlsConfig))
	}
	if b.username != "" {
		opts = append(opts, WithCredentials(b.username, b.password))
	}
	opts = append(opts, extraOpts...)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := DialContext(ctx, b.addr, opts...)
	require.NoError(t, err, "failed to connect to %s", b.addr)
	return client
}

// Public MQTT brokers for e2e testing.
// Run with: go test -tags=e2e -v -run TestE2E
//
// Broker documentation:
// - https://www.emqx.com/en/mqtt/public-mqtt5-broker
// - https://www.hivemq.com/mqtt/public-mqtt-broker/
// - https://test.mosquitto.org/
var publicBrokers = []brokerConfig{
	// ===== broker.emqx.io =====
	// Most reliable public MQTT 5.0 broker
	// Docs: https://www.emqx.com/en/mqtt/public-mqtt5-broker
	{name: "emqx/tcp:1883", addr: "tcp://broker.emqx.io:1883"},
	{name: "emqx/tls:8883", addr: "tls://broker.emqx.io:8883", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}},
	{name: "emqx/ws:8083", addr: "ws://broker.emqx.io:8083/mqtt"},
	{name: "emqx/wss:8084", addr: "wss://broker.emqx.io:8084/mqtt", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}},
	{name: "emqx/quic:14567", addr: "quic://broker.emqx.io:14567", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS13}},

	// ===== broker.hivemq.com =====
	// Docs: https://www.hivemq.com/mqtt/public-mqtt-broker/
	{name: "hivemq/tcp:1883", addr: "tcp://broker.hivemq.com:1883"},
	{name: "hivemq/tls:8883", addr: "tls://broker.hivemq.com:8883", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}},
	{name: "hivemq/ws:8000", addr: "ws://broker.hivemq.com:8000/mqtt"},
	{name: "hivemq/wss:8884", addr: "wss://broker.hivemq.com:8884/mqtt", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}},

	// ===== test.mosquitto.org =====
	// Docs: https://test.mosquitto.org/
	// Auth credentials: rw/readwrite, ro/readonly, wo/writeonly

	// TCP ports
	{name: "mosquitto/tcp:1883", addr: "tcp://test.mosquitto.org:1883"},
	{name: "mosquitto/tcp:1884-auth", addr: "tcp://test.mosquitto.org:1884", username: "rw", password: "readwrite"},

	// TLS ports
	{name: "mosquitto/tls:8883", addr: "tls://test.mosquitto.org:8883", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}},
	{name: "mosquitto/tls:8884-cert", addr: "tls://test.mosquitto.org:8884", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}, skip: "requires client certificate"},
	{name: "mosquitto/tls:8885-auth", addr: "tls://test.mosquitto.org:8885", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}, username: "rw", password: "readwrite"},
	{name: "mosquitto/tls:8886-letsencrypt", addr: "tls://test.mosquitto.org:8886", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}},
	{name: "mosquitto/tls:8887-expired", addr: "tls://test.mosquitto.org:8887", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}, skip: "deliberately expired certificate"},

	// WebSocket ports
	{name: "mosquitto/ws:8080", addr: "ws://test.mosquitto.org:8080/"},
	{name: "mosquitto/wss:8081", addr: "wss://test.mosquitto.org:8081/", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}},
	{name: "mosquitto/ws:8090-auth", addr: "ws://test.mosquitto.org:8090/", username: "rw", password: "readwrite"},
	{name: "mosquitto/wss:8091-auth", addr: "wss://test.mosquitto.org:8091/", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}, username: "rw", password: "readwrite"},
}

func TestE2EConnect(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)
			client := broker.connect(t, "connect")
			defer client.Close()

			assert.True(t, client.IsConnected(), "client should be connected")
			assert.NotEmpty(t, client.ClientID(), "client ID should not be empty")
		})
	}
}

func TestE2EPublishQoS0(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)
			client := broker.connect(t, "pub0")
			defer client.Close()

			err := client.Publish(&Message{
				Topic:   "mqttv5/e2e/test/qos0",
				Payload: []byte("hello qos0"),
				QoS:     0,
			})
			assert.NoError(t, err)
		})
	}
}

func TestE2EPublishQoS1(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)
			client := broker.connect(t, "pub1")
			defer client.Close()

			err := client.Publish(&Message{
				Topic:   "mqttv5/e2e/test/qos1",
				Payload: []byte("hello qos1"),
				QoS:     1,
			})
			assert.NoError(t, err)
		})
	}
}

func TestE2EPublishQoS2(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)
			client := broker.connect(t, "pub2")
			defer client.Close()

			err := client.Publish(&Message{
				Topic:   "mqttv5/e2e/test/qos2",
				Payload: []byte("hello qos2"),
				QoS:     2,
			})
			assert.NoError(t, err)
		})
	}
}

func TestE2ESubscribeAndReceive(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)

			topic := fmt.Sprintf("mqttv5/e2e/test/%d", time.Now().UnixNano())
			payload := []byte("hello subscribe test")

			client := broker.connect(t, "sub")
			defer client.Close()

			var wg sync.WaitGroup
			wg.Add(1)

			var received *Message
			err := client.Subscribe(topic, 1, func(msg *Message) {
				received = msg
				wg.Done()
			})
			require.NoError(t, err)

			// Give subscription time to propagate
			time.Sleep(500 * time.Millisecond)

			err = client.Publish(&Message{
				Topic:   topic,
				Payload: payload,
				QoS:     1,
			})
			require.NoError(t, err)

			// Wait for message with timeout
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				require.NotNil(t, received)
				assert.Equal(t, topic, received.Topic)
				assert.Equal(t, payload, received.Payload)
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for message")
			}
		})
	}
}

func TestE2ESubscribeWildcard(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)

			baseTopic := fmt.Sprintf("mqttv5/e2e/wildcard/%d", time.Now().UnixNano())
			wildcardFilter := baseTopic + "/+"
			publishTopic := baseTopic + "/test"
			payload := []byte("hello wildcard test")

			client := broker.connect(t, "wild")
			defer client.Close()

			var wg sync.WaitGroup
			wg.Add(1)

			var received *Message
			err := client.Subscribe(wildcardFilter, 1, func(msg *Message) {
				received = msg
				wg.Done()
			})
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)

			err = client.Publish(&Message{
				Topic:   publishTopic,
				Payload: payload,
				QoS:     1,
			})
			require.NoError(t, err)

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				require.NotNil(t, received)
				assert.Equal(t, publishTopic, received.Topic)
				assert.Equal(t, payload, received.Payload)
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for message")
			}
		})
	}
}

func TestE2EUnsubscribe(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)

			topic := fmt.Sprintf("mqttv5/e2e/unsub/%d", time.Now().UnixNano())

			client := broker.connect(t, "unsub")
			defer client.Close()

			var msgCount int
			var mu sync.Mutex

			err := client.Subscribe(topic, 1, func(_ *Message) {
				mu.Lock()
				msgCount++
				mu.Unlock()
			})
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)

			// Publish first message
			err = client.Publish(&Message{
				Topic:   topic,
				Payload: []byte("before unsub"),
				QoS:     1,
			})
			require.NoError(t, err)

			time.Sleep(1 * time.Second)

			// Unsubscribe
			err = client.Unsubscribe(topic)
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)

			// Publish second message
			err = client.Publish(&Message{
				Topic:   topic,
				Payload: []byte("after unsub"),
				QoS:     1,
			})
			require.NoError(t, err)

			time.Sleep(1 * time.Second)

			mu.Lock()
			count := msgCount
			mu.Unlock()

			// Should have received only the first message
			assert.Equal(t, 1, count, "should receive only one message before unsubscribe")
		})
	}
}

func TestE2ERetainedMessage(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)

			topic := fmt.Sprintf("mqttv5/e2e/retained/%d", time.Now().UnixNano())
			payload := []byte("retained message")

			// First client: publish retained message
			pub := broker.connect(t, "retain-pub")

			err := pub.Publish(&Message{
				Topic:   topic,
				Payload: payload,
				QoS:     1,
				Retain:  true,
			})
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)
			pub.Close()

			// Second client: subscribe and receive retained message
			sub := broker.connect(t, "retain-sub")
			defer sub.Close()

			var wg sync.WaitGroup
			wg.Add(1)

			var received *Message
			err = sub.Subscribe(topic, 1, func(msg *Message) {
				if received == nil {
					received = msg
					wg.Done()
				}
			})
			require.NoError(t, err)

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				require.NotNil(t, received)
				assert.Equal(t, topic, received.Topic)
				assert.Equal(t, payload, received.Payload)
				assert.True(t, received.Retain, "message should be marked as retained")
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for retained message")
			}

			// Clean up: publish empty retained message to delete
			err = sub.Publish(&Message{
				Topic:   topic,
				Payload: []byte{},
				QoS:     1,
				Retain:  true,
			})
			assert.NoError(t, err)
		})
	}
}

func TestE2EMultipleSubscriptions(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)

			base := fmt.Sprintf("mqttv5/e2e/multi/%d", time.Now().UnixNano())
			topic1 := base + "/topic1"
			topic2 := base + "/topic2"
			topic3 := base + "/topic3"

			client := broker.connect(t, "multi")
			defer client.Close()

			var wg sync.WaitGroup
			wg.Add(3)

			receivedTopics := make(map[string]bool)
			var mu sync.Mutex

			handler := func(msg *Message) {
				mu.Lock()
				if !receivedTopics[msg.Topic] {
					receivedTopics[msg.Topic] = true
					wg.Done()
				}
				mu.Unlock()
			}

			err := client.SubscribeMultiple(map[string]byte{
				topic1: 0,
				topic2: 1,
				topic3: 2,
			}, handler)
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)

			for _, topic := range []string{topic1, topic2, topic3} {
				err = client.Publish(&Message{
					Topic:   topic,
					Payload: []byte("test"),
					QoS:     1,
				})
				require.NoError(t, err)
			}

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				mu.Lock()
				assert.True(t, receivedTopics[topic1])
				assert.True(t, receivedTopics[topic2])
				assert.True(t, receivedTopics[topic3])
				mu.Unlock()
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for messages")
			}
		})
	}
}

func TestE2ELargePayload(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)

			topic := fmt.Sprintf("mqttv5/e2e/large/%d", time.Now().UnixNano())
			// 64KB payload
			payload := make([]byte, 64*1024)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			client := broker.connect(t, "large")
			defer client.Close()

			var wg sync.WaitGroup
			wg.Add(1)

			var received *Message
			err := client.Subscribe(topic, 1, func(msg *Message) {
				received = msg
				wg.Done()
			})
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)

			err = client.Publish(&Message{
				Topic:   topic,
				Payload: payload,
				QoS:     1,
			})
			require.NoError(t, err)

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				require.NotNil(t, received)
				assert.Equal(t, len(payload), len(received.Payload))
				assert.Equal(t, payload, received.Payload)
			case <-time.After(15 * time.Second):
				t.Fatal("timeout waiting for large message")
			}
		})
	}
}

func TestE2EUserProperties(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)

			topic := fmt.Sprintf("mqttv5/e2e/props/%d", time.Now().UnixNano())

			client := broker.connect(t, "props")
			defer client.Close()

			var wg sync.WaitGroup
			wg.Add(1)

			var received *Message
			err := client.Subscribe(topic, 1, func(msg *Message) {
				received = msg
				wg.Done()
			})
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)

			err = client.Publish(&Message{
				Topic:       topic,
				Payload:     []byte("test with properties"),
				QoS:         1,
				ContentType: "text/plain",
				UserProperties: []StringPair{
					{Key: "key1", Value: "value1"},
					{Key: "key2", Value: "value2"},
				},
			})
			require.NoError(t, err)

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				require.NotNil(t, received)
				assert.Equal(t, "text/plain", received.ContentType)
				assert.Len(t, received.UserProperties, 2)
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for message with properties")
			}
		})
	}
}

func TestE2ERequestResponse(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)

			requestTopic := fmt.Sprintf("mqttv5/e2e/request/%d", time.Now().UnixNano())
			responseTopic := fmt.Sprintf("mqttv5/e2e/response/%d", time.Now().UnixNano())
			correlationData := []byte("correlation-123")

			client := broker.connect(t, "reqres")
			defer client.Close()

			var wg sync.WaitGroup
			wg.Add(1)

			// Subscribe to response topic
			var response *Message
			err := client.Subscribe(responseTopic, 1, func(msg *Message) {
				response = msg
				wg.Done()
			})
			require.NoError(t, err)

			// Subscribe to request topic and simulate responder
			err = client.Subscribe(requestTopic, 1, func(msg *Message) {
				// Send response
				client.Publish(&Message{
					Topic:           msg.ResponseTopic,
					Payload:         []byte("response payload"),
					QoS:             1,
					CorrelationData: msg.CorrelationData,
				})
			})
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)

			// Send request
			err = client.Publish(&Message{
				Topic:           requestTopic,
				Payload:         []byte("request payload"),
				QoS:             1,
				ResponseTopic:   responseTopic,
				CorrelationData: correlationData,
			})
			require.NoError(t, err)

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				require.NotNil(t, response)
				assert.Equal(t, []byte("response payload"), response.Payload)
				assert.Equal(t, correlationData, response.CorrelationData)
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for response")
			}
		})
	}
}

func TestE2EKeepAlive(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)

			client := broker.connect(t, "keepalive", WithKeepAlive(5))
			defer client.Close()

			// Wait for keep-alive cycle
			time.Sleep(8 * time.Second)

			// Should still be connected after keep-alive exchange
			assert.True(t, client.IsConnected(), "client should remain connected after keep-alive")
		})
	}
}

func TestE2EGracefulDisconnect(t *testing.T) {
	for _, broker := range publicBrokers {
		t.Run(broker.name, func(t *testing.T) {
			broker.shouldSkip(t)

			client := broker.connect(t, "disconnect")

			assert.True(t, client.IsConnected())

			err := client.Close()
			assert.NoError(t, err)

			assert.False(t, client.IsConnected())
		})
	}
}
