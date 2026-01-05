// Package main demonstrates an MQTT v5.0 client connecting via QUIC transport.
package main

import (
	"crypto/tls"
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
	// QUIC broker address - adjust to your broker's address
	address := "127.0.0.1:8883"
	if len(os.Args) > 1 {
		address = os.Args[1]
	}

	// TLS config for QUIC (QUIC requires TLS 1.3)
	tlsConfig := &tls.Config{
		NextProtos:         []string{"mqtt"},
		InsecureSkipVerify: true, // For testing only; use proper certs in production
	}

	// Connect to MQTT broker via QUIC
	// Format: quic://host:port
	client, err := mqttv5.Dial(
		mqttv5.WithServers(fmt.Sprintf("quic://%s", address)),
		mqttv5.WithClientID("quic-client"),
		mqttv5.WithKeepAlive(60),
		mqttv5.WithTLS(tlsConfig),
		mqttv5.OnEvent(func(_ *mqttv5.Client, ev error) {
			if errors.Is(ev, mqttv5.ErrConnected) {
				log.Println("Connected to broker via QUIC")
			}
			if errors.Is(ev, mqttv5.ErrConnectionLost) {
				log.Println("Connection lost")
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", address, err)
	}
	defer client.Close()

	log.Printf("Client ID: %s", client.ClientID())

	// Subscribe to a topic
	err = client.Subscribe("quic/messages", 1, func(msg *mqttv5.Message) {
		log.Printf("Received: topic=%s payload=%s", msg.Topic, string(msg.Payload))
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	log.Println("Subscribed to quic/messages")

	// Publish a message
	err = client.Publish(&mqttv5.Message{
		Topic:   "quic/messages",
		Payload: []byte("Hello from QUIC client!"),
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
