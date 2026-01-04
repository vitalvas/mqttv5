// Package main demonstrates a simple MQTT v5.0 broker using the mqttv5 SDK.
// It shows server-side publishing to broadcast messages to subscribed clients.
package main

import (
	"fmt"
	"log"
	"net"
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
	listener, err := net.Listen("tcp", ":1883")
	if err != nil {
		return err
	}

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

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		close(done)
		srv.Close()
	}()

	// Start server in background
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	addrs := srv.Addrs()
	if len(addrs) > 0 {
		log.Printf("MQTT broker listening on %s", addrs[0])
	}

	// Server-side publishing: broadcast status every 5 seconds
	// Clients can subscribe to "$SYS/broker/status" to receive these messages
	log.Println("Publishing server status to $SYS/broker/status every 5 seconds")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return nil
		case t := <-ticker.C:
			// Publish server status message
			msg := &mqttv5.Message{
				Topic:   "$SYS/broker/status",
				Payload: []byte(fmt.Sprintf(`{"time":"%s","clients":%d}`, t.Format(time.RFC3339), srv.ClientCount())),
				QoS:     mqttv5.QoS0,
				Retain:  true, // Retain so new subscribers get the last status
			}
			if err := srv.Publish(msg); err != nil {
				log.Printf("Failed to publish status: %v", err)
			}
		}
	}
}
