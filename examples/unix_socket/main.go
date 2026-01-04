// Package main demonstrates MQTT v5.0 communication over Unix domain sockets.
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vitalvas/mqttv5"
)

const socketPath = "/tmp/mqtt_example.sock"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Clean up any existing socket file
	os.Remove(socketPath)

	// Create Unix socket listener
	listener, err := mqttv5.NewUnixListener(socketPath)
	if err != nil {
		return fmt.Errorf("failed to create unix listener: %w", err)
	}

	// Create MQTT server with Unix socket listener
	srv := mqttv5.NewServer(
		mqttv5.WithListener(listener),
		mqttv5.OnConnect(func(client *mqttv5.ServerClient) {
			log.Printf("Client connected: %s", client.ClientID())
		}),
		mqttv5.OnDisconnect(func(client *mqttv5.ServerClient) {
			log.Printf("Client disconnected: %s", client.ClientID())
		}),
		mqttv5.OnMessage(func(client *mqttv5.ServerClient, msg *mqttv5.Message) {
			log.Printf("Message from %s: topic=%s payload=%s",
				client.ClientID(), msg.Topic, string(msg.Payload))
		}),
	)

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down...")
		srv.Close()
		os.Remove(socketPath)
	}()

	// Start server in background
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	log.Printf("MQTT broker listening on unix://%s", socketPath)

	// Wait for server to be ready
	time.Sleep(100 * time.Millisecond)

	// Connect client via Unix socket
	client, err := mqttv5.Dial(fmt.Sprintf("unix://%s", socketPath),
		mqttv5.WithClientID("unix-socket-client"),
		mqttv5.WithKeepAlive(60),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	log.Printf("Client connected via Unix socket")

	// Subscribe to a topic
	err = client.Subscribe("unix/test", 1, func(msg *mqttv5.Message) {
		log.Printf("Received: topic=%s payload=%s", msg.Topic, string(msg.Payload))
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// Publish a message
	err = client.Publish(&mqttv5.Message{
		Topic:   "unix/test",
		Payload: []byte("Hello via Unix socket!"),
		QoS:     mqttv5.QoS1,
	})
	if err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	log.Println("Message published, waiting for delivery...")
	time.Sleep(time.Second)

	log.Println("Example completed successfully")
	return nil
}
