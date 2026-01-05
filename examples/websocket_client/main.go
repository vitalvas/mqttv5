// Package main demonstrates an MQTT v5.0 client over WebSocket using the mqttv5 SDK.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vitalvas/mqttv5"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Connect to MQTT broker over WebSocket
	client, err := mqttv5.Dial(
		mqttv5.WithServers("ws://localhost:8080/mqtt"),
		mqttv5.WithClientID("websocket-client-example"),
		mqttv5.WithKeepAlive(60),
		mqttv5.OnEvent(func(_ *mqttv5.Client, ev error) {
			if errors.Is(ev, mqttv5.ErrConnected) {
				fmt.Println("Connected over WebSocket!")
			}
			if errors.Is(ev, mqttv5.ErrConnectionLost) {
				fmt.Println("Connection lost!")
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Subscribe to a topic with message handler
	err = client.Subscribe("websocket/test", 1, func(msg *mqttv5.Message) {
		fmt.Printf("Received message on %s: %s\n", msg.Topic, string(msg.Payload))
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	fmt.Println("Subscribed to websocket/test")

	// Publish a message
	err = client.Publish(&mqttv5.Message{
		Topic:   "websocket/test",
		Payload: []byte("Hello from WebSocket client!"),
		QoS:     mqttv5.QoS1,
	})
	if err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}
	fmt.Println("Published message!")

	// Wait for messages or interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		fmt.Println("\nReceived interrupt signal")
	case <-time.After(5 * time.Second):
		fmt.Println("\nTimeout reached")
	}

	// Graceful disconnect - Close() sends DISCONNECT packet to broker
	fmt.Println("Disconnecting...")
	client.Close()

	return nil
}
