// Package main demonstrates an MQTT v5.0 client connecting via Unix domain socket.
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
	// Unix socket path - adjust to your broker's socket location
	socketPath := "/tmp/mqtt.sock"
	if len(os.Args) > 1 {
		socketPath = os.Args[1]
	}

	// Connect to MQTT broker via Unix socket
	// Format: unix:///path/to/socket or unix://localhost/path/to/socket
	client, err := mqttv5.Dial(fmt.Sprintf("unix://%s", socketPath),
		mqttv5.WithClientID("unix-client"),
		mqttv5.WithKeepAlive(60),
		mqttv5.OnEvent(func(_ *mqttv5.Client, ev error) {
			if errors.Is(ev, mqttv5.ErrConnected) {
				log.Println("Connected to broker via Unix socket")
			}
			if errors.Is(ev, mqttv5.ErrConnectionLost) {
				log.Println("Connection lost")
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", socketPath, err)
	}
	defer client.Close()

	log.Printf("Client ID: %s", client.ClientID())

	// Subscribe to a topic
	err = client.Subscribe("unix/messages", 1, func(msg *mqttv5.Message) {
		log.Printf("Received: topic=%s payload=%s", msg.Topic, string(msg.Payload))
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	log.Println("Subscribed to unix/messages")

	// Publish a message
	err = client.Publish(&mqttv5.Message{
		Topic:   "unix/messages",
		Payload: []byte("Hello from Unix socket client!"),
		QoS:     mqttv5.QoS1,
	})
	if err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}
	log.Println("Message published")

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Println("Received interrupt signal")
	case <-time.After(5 * time.Second):
		log.Println("Timeout reached")
	}

	log.Println("Disconnecting...")
	return nil
}
