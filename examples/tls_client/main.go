// Package main demonstrates an MQTT v5.0 client with TLS using the mqttv5 SDK.
package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"

	"github.com/vitalvas/mqttv5"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Configure TLS
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // For testing only; use proper CA verification in production
		MinVersion:         tls.VersionTLS12,
	}

	// Connect to MQTT broker with TLS
	client, err := mqttv5.Dial("tls://localhost:8883",
		mqttv5.WithClientID("tls-client-example"),
		mqttv5.WithTLS(tlsConfig),
		mqttv5.WithKeepAlive(60),
		mqttv5.OnEvent(func(_ *mqttv5.Client, ev error) {
			if errors.Is(ev, mqttv5.ErrConnected) {
				fmt.Println("Connected with TLS!")
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Publish a secure message
	err = client.Publish(&mqttv5.Message{
		Topic:   "secure/topic",
		Payload: []byte("Secure message over TLS"),
		QoS:     mqttv5.QoS0,
	})
	if err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	fmt.Println("Published secure message!")

	// Graceful disconnect - Close() sends DISCONNECT packet to broker
	fmt.Println("Disconnecting...")
	client.Close()

	return nil
}
