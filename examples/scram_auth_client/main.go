// Package main demonstrates an MQTT v5.0 client that authenticates with the
// broker using SCRAM enhanced authentication.
//
// SCRAM (Salted Challenge Response Authentication Mechanism) lets the client
// prove knowledge of the password without ever sending it to the broker.
// The broker, in turn, proves it knows the stored credentials by signing the
// session — so authentication is mutual.
//
// Pair this example with the scram_auth_broker example to see the full
// exchange in action.
package main

import (
	"errors"
	"flag"
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

// pickHash maps a method name from the command line to the SCRAMHash value.
func pickHash(method string) (mqttv5.SCRAMHash, error) {
	switch method {
	case "SCRAM-SHA-1":
		return mqttv5.SCRAMHashSHA1, nil
	case "SCRAM-SHA-256":
		return mqttv5.SCRAMHashSHA256, nil
	case "SCRAM-SHA-512":
		return mqttv5.SCRAMHashSHA512, nil
	}
	return 0, fmt.Errorf("unsupported SCRAM method %q (use SCRAM-SHA-1, SCRAM-SHA-256, or SCRAM-SHA-512)", method)
}

func run() error {
	server := flag.String("server", "tcp://localhost:1883", "MQTT broker URL")
	username := flag.String("user", "admin", "username")
	password := flag.String("pass", "admin-secret", "password")
	method := flag.String("method", "SCRAM-SHA-256", "SCRAM method: SCRAM-SHA-1, SCRAM-SHA-256, or SCRAM-SHA-512")
	topic := flag.String("topic", "example/topic", "topic to publish/subscribe to")
	flag.Parse()

	hash, err := pickHash(*method)
	if err != nil {
		return err
	}

	// Build the SCRAM client authenticator. The client announces a single
	// mechanism in CONNECT; the broker must support the same one.
	auth := mqttv5.NewClientSCRAMAuthenticator(hash, *username, *password)

	client, err := mqttv5.Dial(
		mqttv5.WithServers(*server),
		mqttv5.WithClientID(fmt.Sprintf("scram-client-%s", *username)),
		mqttv5.WithKeepAlive(60),
		mqttv5.WithEnhancedAuthentication(auth),
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

	fmt.Printf("Client ID:   %s\n", client.ClientID())
	fmt.Printf("Method:      %s\n", *method)
	fmt.Printf("User:        %s\n", *username)

	if err := client.Subscribe(*topic, mqttv5.QoS1, func(msg *mqttv5.Message) {
		fmt.Printf("Received on %s: %s\n", msg.Topic, string(msg.Payload))
	}); err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	fmt.Printf("Subscribed:  %s\n", *topic)

	if err := client.Publish(&mqttv5.Message{
		Topic:   *topic,
		Payload: fmt.Appendf(nil, "hello from %s", *username),
		QoS:     mqttv5.QoS1,
	}); err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}
	fmt.Println("Published a greeting; waiting briefly for delivery...")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigCh:
		fmt.Println("Received interrupt signal")
	case <-time.After(3 * time.Second):
	}

	fmt.Println("Disconnecting...")
	client.Close()
	return nil
}
