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
	defer client.Close()

	// Publish a secure message
	err = client.Publish("secure/topic", []byte("Secure message over TLS"), 0, false)
	if err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	fmt.Println("Published secure message!")
	fmt.Println("Disconnecting...")

	return nil
}
