// Package main demonstrates a simple MQTT v5.0 broker using the mqttv5 SDK.
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vitalvas/mqttv5"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	srv, err := mqttv5.NewServer(":1883",
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
		mqttv5.OnSubscribe(func(client *mqttv5.ServerClient, subs []mqttv5.Subscription) {
			for _, sub := range subs {
				log.Printf("Client %s subscribed to: %s (QoS %d)",
					client.ClientID(), sub.TopicFilter, sub.QoS)
			}
		}),
		mqttv5.OnUnsubscribe(func(client *mqttv5.ServerClient, topics []string) {
			for _, topic := range topics {
				log.Printf("Client %s unsubscribed from: %s", client.ClientID(), topic)
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down...")
		srv.Close()
	}()

	log.Printf("MQTT broker listening on %s", srv.Addr())
	return srv.ListenAndServe()
}
