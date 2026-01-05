// Package main demonstrates MQTT v5.0 shared subscriptions using the mqttv5 SDK.
// Shared subscriptions allow multiple clients to share a subscription to a topic,
// with messages being distributed among them (load balancing).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/vitalvas/mqttv5"
)

const (
	brokerAddr  = "tcp://localhost:1883"
	shareName   = "mygroup"
	topicFilter = "sensors/temperature"
)

func main() {
	// Demonstrate shared subscription parsing
	sharedFilter := fmt.Sprintf("$share/%s/%s", shareName, topicFilter)
	fmt.Printf("Shared subscription filter: %s\n\n", sharedFilter)

	// Parse the shared subscription
	shared, err := mqttv5.ParseSharedSubscription(sharedFilter)
	if err != nil {
		log.Fatalf("Failed to parse shared subscription: %v", err)
	}

	if shared != nil {
		fmt.Printf("Parsed shared subscription:\n")
		fmt.Printf("  Share Name: %s\n", shared.ShareName)
		fmt.Printf("  Topic Filter: %s\n\n", shared.TopicFilter)
	}

	// Create multiple subscriber clients that share the subscription
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Track received messages per subscriber
	received := make([]int, 3)
	var mu sync.Mutex

	// Start 3 subscriber clients in the shared group
	for i := range 3 {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			runSubscriber(ctx, clientNum, sharedFilter, func() {
				mu.Lock()
				received[clientNum-1]++
				mu.Unlock()
			})
		}(i + 1)
	}

	// Give subscribers time to connect
	time.Sleep(500 * time.Millisecond)

	// Start a publisher that sends messages to the topic
	wg.Add(1)
	go func() {
		defer wg.Done()
		runPublisher(ctx, topicFilter)
	}()

	wg.Wait()

	// Print summary
	fmt.Println("\nDemo completed!")
	mu.Lock()
	for i, count := range received {
		fmt.Printf("Subscriber %d received %d messages\n", i+1, count)
	}
	mu.Unlock()
}

func runSubscriber(ctx context.Context, id int, sharedFilter string, onMessage func()) {
	clientID := fmt.Sprintf("shared-sub-%d", id)

	client, err := mqttv5.Dial(
		mqttv5.WithServers(brokerAddr),
		mqttv5.WithClientID(clientID),
		mqttv5.WithKeepAlive(60),
		mqttv5.OnEvent(func(_ *mqttv5.Client, ev error) {
			if errors.Is(ev, mqttv5.ErrConnected) {
				fmt.Printf("[%s] Connected\n", clientID)
			}
		}),
	)
	if err != nil {
		log.Printf("[%s] Failed to connect: %v", clientID, err)
		return
	}
	defer client.Close()

	// Subscribe to shared subscription
	err = client.Subscribe(sharedFilter, 1, func(msg *mqttv5.Message) {
		fmt.Printf("[%s] Received: %s\n", clientID, string(msg.Payload))
		onMessage()
	})
	if err != nil {
		log.Printf("[%s] Failed to subscribe: %v", clientID, err)
		return
	}
	fmt.Printf("[%s] Subscribed to shared subscription: %s\n", clientID, sharedFilter)

	// Wait for context cancellation
	<-ctx.Done()
	fmt.Printf("[%s] Disconnecting\n", clientID)
}

func runPublisher(ctx context.Context, topic string) {
	clientID := "publisher"

	client, err := mqttv5.Dial(
		mqttv5.WithServers(brokerAddr),
		mqttv5.WithClientID(clientID),
		mqttv5.WithKeepAlive(60),
		mqttv5.OnEvent(func(_ *mqttv5.Client, ev error) {
			if errors.Is(ev, mqttv5.ErrConnected) {
				fmt.Printf("[%s] Connected\n", clientID)
			}
		}),
	)
	if err != nil {
		log.Printf("[%s] Failed to connect: %v", clientID, err)
		return
	}
	defer client.Close()

	// Publish messages
	for i := 1; i <= 9; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		payload := fmt.Sprintf("Temperature reading %d: %.1fC", i, 20.0+float64(i)*0.5)
		err := client.Publish(&mqttv5.Message{
			Topic:   topic,
			Payload: []byte(payload),
			QoS:     mqttv5.QoS1,
		})
		if err != nil {
			log.Printf("[%s] Failed to publish message %d: %v", clientID, i, err)
			continue
		}

		fmt.Printf("[%s] Published message %d\n", clientID, i)
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("[%s] Disconnecting\n", clientID)
}
