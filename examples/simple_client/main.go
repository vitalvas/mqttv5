// Package main demonstrates a simple MQTT v5.0 client using the mqttv5 SDK.
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
	// Connect to MQTT broker with options
	client, err := mqttv5.Dial("tcp://localhost:1883",
		mqttv5.WithClientID("simple-client-example"),
		mqttv5.WithKeepAlive(60),
		mqttv5.OnEvent(func(_ *mqttv5.Client, ev error) {
			if errors.Is(ev, mqttv5.ErrConnected) {
				fmt.Println("Connected successfully!")
			}
			if errors.Is(ev, mqttv5.ErrConnectionLost) {
				fmt.Println("Connection lost!")
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	fmt.Printf("Client ID: %s\n", client.ClientID())

	// Subscribe to a topic with message handler
	err = client.Subscribe("example/topic", 1, func(msg *mqttv5.Message) {
		fmt.Printf("Received message on %s: %s\n", msg.Topic, string(msg.Payload))
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	fmt.Println("Subscribed to example/topic")

	// Publish a message
	err = client.Publish("example/topic", []byte("Hello, MQTT v5.0!"), 1, false)
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

	fmt.Println("Disconnecting...")

	return nil
}
