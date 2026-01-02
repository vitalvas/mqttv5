// Package main demonstrates using ProducerInterceptor and ConsumerInterceptor
// to intercept, modify, and filter MQTT messages on both client and server.
package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vitalvas/mqttv5"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// LoggingProducerInterceptor logs all outgoing messages.
type LoggingProducerInterceptor struct {
	prefix string
}

func (i *LoggingProducerInterceptor) OnSend(msg *mqttv5.Message) *mqttv5.Message {
	log.Printf("[%s] Sending: topic=%s payload=%s qos=%d",
		i.prefix, msg.Topic, string(msg.Payload), msg.QoS)
	return msg
}

// LoggingConsumerInterceptor logs all incoming messages.
type LoggingConsumerInterceptor struct {
	prefix string
}

func (i *LoggingConsumerInterceptor) OnConsume(msg *mqttv5.Message) *mqttv5.Message {
	log.Printf("[%s] Received: topic=%s payload=%s qos=%d",
		i.prefix, msg.Topic, string(msg.Payload), msg.QoS)
	return msg
}

// MetricsInterceptor counts messages for metrics.
type MetricsInterceptor struct {
	sent     atomic.Int64
	received atomic.Int64
}

func (i *MetricsInterceptor) OnSend(msg *mqttv5.Message) *mqttv5.Message {
	i.sent.Add(1)
	return msg
}

func (i *MetricsInterceptor) OnConsume(msg *mqttv5.Message) *mqttv5.Message {
	i.received.Add(1)
	return msg
}

func (i *MetricsInterceptor) Stats() (sent, received int64) {
	return i.sent.Load(), i.received.Load()
}

// TopicPrefixInterceptor adds a prefix to outgoing message topics.
type TopicPrefixInterceptor struct {
	prefix string
}

func (i *TopicPrefixInterceptor) OnSend(msg *mqttv5.Message) *mqttv5.Message {
	msg.Topic = i.prefix + "/" + msg.Topic
	return msg
}

// PayloadTransformInterceptor transforms message payloads.
type PayloadTransformInterceptor struct{}

func (i *PayloadTransformInterceptor) OnConsume(msg *mqttv5.Message) *mqttv5.Message {
	// Convert payload to uppercase
	msg.Payload = []byte(strings.ToUpper(string(msg.Payload)))
	return msg
}

// FilterInterceptor filters out messages matching certain criteria.
type FilterInterceptor struct {
	blockedTopics []string
}

func (i *FilterInterceptor) OnSend(msg *mqttv5.Message) *mqttv5.Message {
	for _, blocked := range i.blockedTopics {
		if strings.HasPrefix(msg.Topic, blocked) {
			log.Printf("[Filter] Blocked message to topic: %s", msg.Topic)
			return nil // Returning nil filters out the message
		}
	}
	return msg
}

func (i *FilterInterceptor) OnConsume(msg *mqttv5.Message) *mqttv5.Message {
	for _, blocked := range i.blockedTopics {
		if strings.HasPrefix(msg.Topic, blocked) {
			log.Printf("[Filter] Blocked message from topic: %s", msg.Topic)
			return nil
		}
	}
	return msg
}

func run() error {
	// Create server interceptors
	serverMetrics := &MetricsInterceptor{}
	serverLogging := &LoggingConsumerInterceptor{prefix: "Server"}

	// Start server with interceptors
	listener, err := net.Listen("tcp", ":1883")
	if err != nil {
		return err
	}

	srv := mqttv5.NewServer(
		mqttv5.WithListener(listener),
		// Consumer interceptors: process messages received from clients
		mqttv5.WithServerConsumerInterceptors(
			serverLogging,
			serverMetrics,
		),
		// Producer interceptors: process messages before sending to subscribers
		mqttv5.WithServerProducerInterceptors(
			&LoggingProducerInterceptor{prefix: "Server"},
			serverMetrics,
		),
		mqttv5.OnConnect(func(client *mqttv5.ServerClient) {
			log.Printf("Client connected: %s", client.ClientID())
		}),
	)

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		close(done)
		srv.Close()
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	log.Println("Server started on :1883")
	time.Sleep(100 * time.Millisecond) // Wait for server to start

	// Create client interceptors
	clientMetrics := &MetricsInterceptor{}
	filter := &FilterInterceptor{blockedTopics: []string{"blocked/"}}

	// Connect client with interceptors
	client, err := mqttv5.Dial("tcp://localhost:1883",
		mqttv5.WithClientID("interceptor-demo"),
		// Producer interceptors: process messages before publishing
		mqttv5.WithProducerInterceptors(
			&LoggingProducerInterceptor{prefix: "Client"},
			&TopicPrefixInterceptor{prefix: "demo"},
			filter,
			clientMetrics,
		),
		// Consumer interceptors: process messages before delivering to handlers
		mqttv5.WithConsumerInterceptors(
			&LoggingConsumerInterceptor{prefix: "Client"},
			&PayloadTransformInterceptor{},
			filter,
			clientMetrics,
		),
		mqttv5.OnEvent(func(_ *mqttv5.Client, ev error) {
			if errors.Is(ev, mqttv5.ErrConnected) {
				log.Println("Client connected")
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	// Subscribe to receive messages
	err = client.Subscribe("demo/#", 1, func(msg *mqttv5.Message) {
		// Note: payload will be uppercase due to PayloadTransformInterceptor
		fmt.Printf("Handler received: topic=%s payload=%s\n", msg.Topic, string(msg.Payload))
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// Publish some messages
	messages := []struct {
		topic   string
		payload string
	}{
		{"test/hello", "Hello World"},           // Will become demo/test/hello
		{"test/greeting", "Good Morning"},       // Will become demo/test/greeting
		{"blocked/secret", "This is filtered"},  // Will be filtered out
		{"test/final", "Interceptors are cool"}, // Will become demo/test/final
	}

	for _, m := range messages {
		log.Printf("Publishing to %s: %s", m.topic, m.payload)
		err = client.Publish(&mqttv5.Message{
			Topic:   m.topic,
			Payload: []byte(m.payload),
			QoS:     1,
		})
		if err != nil {
			log.Printf("Failed to publish: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Wait a bit for messages to be processed
	time.Sleep(500 * time.Millisecond)

	// Print metrics
	clientSent, clientReceived := clientMetrics.Stats()
	serverSent, serverReceived := serverMetrics.Stats()

	fmt.Println("\n--- Metrics ---")
	fmt.Printf("Client: sent=%d, received=%d\n", clientSent, clientReceived)
	fmt.Printf("Server: sent=%d, received=%d\n", serverSent, serverReceived)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}

	return nil
}
