// Package main demonstrates MQTT v5.0 shared subscriptions using the mqttv5 SDK.
// Shared subscriptions allow multiple clients to share a subscription to a topic,
// with messages being distributed among them (load balancing).
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/vitalvas/mqttv5"
)

const (
	brokerAddr    = "localhost:1883"
	shareName     = "mygroup"
	topicFilter   = "sensors/temperature"
	maxPacketSize = 256 * 1024
)

func main() {
	// Demonstrate shared subscription parsing
	sharedFilter := fmt.Sprintf("$share/%s/%s", shareName, topicFilter)
	fmt.Printf("Shared subscription filter: %s\n\n", sharedFilter)

	// Parse the shared subscription
	shared, err := mqttv5.ParseSharedSubscription(sharedFilter)
	if err != nil {
		log.Fatalf("Failed to parse shared subscription: %v", err)
	}

	if shared != nil {
		fmt.Printf("Parsed shared subscription:\n")
		fmt.Printf("  Share Name: %s\n", shared.ShareName)
		fmt.Printf("  Topic Filter: %s\n\n", shared.TopicFilter)
	}

	// Create multiple subscriber clients that share the subscription
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start 3 subscriber clients in the shared group
	for i := range 3 {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			runSubscriber(ctx, clientNum, sharedFilter)
		}(i + 1)
	}

	// Give subscribers time to connect
	time.Sleep(500 * time.Millisecond)

	// Start a publisher that sends messages to the topic
	wg.Add(1)
	go func() {
		defer wg.Done()
		runPublisher(ctx, topicFilter)
	}()

	wg.Wait()
	fmt.Println("\nDemo completed!")
}

func runSubscriber(ctx context.Context, id int, sharedFilter string) {
	clientID := fmt.Sprintf("shared-sub-%d", id)

	conn, err := net.DialTimeout("tcp", brokerAddr, 5*time.Second)
	if err != nil {
		log.Printf("[%s] Failed to connect: %v", clientID, err)
		return
	}
	defer conn.Close()

	// Send CONNECT
	connectPkt := &mqttv5.ConnectPacket{
		ClientID:   clientID,
		CleanStart: true,
		KeepAlive:  60,
	}

	if _, err := mqttv5.WritePacket(conn, connectPkt, maxPacketSize); err != nil {
		log.Printf("[%s] Failed to send CONNECT: %v", clientID, err)
		return
	}

	// Read CONNACK
	pkt, _, err := mqttv5.ReadPacket(conn, maxPacketSize)
	if err != nil {
		log.Printf("[%s] Failed to read CONNACK: %v", clientID, err)
		return
	}

	connack, ok := pkt.(*mqttv5.ConnackPacket)
	if !ok || connack.ReasonCode != mqttv5.ReasonSuccess {
		log.Printf("[%s] Connection failed", clientID)
		return
	}

	fmt.Printf("[%s] Connected\n", clientID)

	// Subscribe to shared subscription
	subscribePkt := &mqttv5.SubscribePacket{
		PacketID: 1,
		Subscriptions: []mqttv5.Subscription{
			{TopicFilter: sharedFilter, QoS: 1},
		},
	}

	if _, err := mqttv5.WritePacket(conn, subscribePkt, maxPacketSize); err != nil {
		log.Printf("[%s] Failed to send SUBSCRIBE: %v", clientID, err)
		return
	}

	// Read SUBACK
	pkt, _, err = mqttv5.ReadPacket(conn, maxPacketSize)
	if err != nil {
		log.Printf("[%s] Failed to read SUBACK: %v", clientID, err)
		return
	}

	if _, ok := pkt.(*mqttv5.SubackPacket); ok {
		fmt.Printf("[%s] Subscribed to shared subscription: %s\n", clientID, sharedFilter)
	}

	// Read incoming messages
	messagesReceived := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[%s] Received %d messages\n", clientID, messagesReceived)
			disconnectPkt := &mqttv5.DisconnectPacket{ReasonCode: mqttv5.ReasonSuccess}
			mqttv5.WritePacket(conn, disconnectPkt, maxPacketSize)
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		pkt, _, err = mqttv5.ReadPacket(conn, maxPacketSize)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}

		if p, ok := pkt.(*mqttv5.PublishPacket); ok {
			messagesReceived++
			fmt.Printf("[%s] Received message #%d: %s\n", clientID, messagesReceived, string(p.Payload))

			if p.QoS == 1 {
				puback := &mqttv5.PubackPacket{
					PacketID:   p.PacketID,
					ReasonCode: mqttv5.ReasonSuccess,
				}
				mqttv5.WritePacket(conn, puback, maxPacketSize)
			}
		}
	}
}

func runPublisher(ctx context.Context, topic string) {
	clientID := "publisher"

	conn, err := net.DialTimeout("tcp", brokerAddr, 5*time.Second)
	if err != nil {
		log.Printf("[%s] Failed to connect: %v", clientID, err)
		return
	}
	defer conn.Close()

	// Send CONNECT
	connectPkt := &mqttv5.ConnectPacket{
		ClientID:   clientID,
		CleanStart: true,
		KeepAlive:  60,
	}

	if _, err := mqttv5.WritePacket(conn, connectPkt, maxPacketSize); err != nil {
		log.Printf("[%s] Failed to send CONNECT: %v", clientID, err)
		return
	}

	// Read CONNACK
	pkt, _, err := mqttv5.ReadPacket(conn, maxPacketSize)
	if err != nil {
		log.Printf("[%s] Failed to read CONNACK: %v", clientID, err)
		return
	}

	connack, ok := pkt.(*mqttv5.ConnackPacket)
	if !ok || connack.ReasonCode != mqttv5.ReasonSuccess {
		log.Printf("[%s] Connection failed", clientID)
		return
	}

	fmt.Printf("[%s] Connected\n", clientID)

	// Publish messages
	for i := 1; i <= 9; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		publishPkt := &mqttv5.PublishPacket{
			Topic:    topic,
			QoS:      1,
			PacketID: uint16(i),
			Payload:  fmt.Appendf(nil, "Temperature reading %d: %.1fC", i, 20.0+float64(i)*0.5),
		}

		if _, err := mqttv5.WritePacket(conn, publishPkt, maxPacketSize); err != nil {
			log.Printf("[%s] Failed to publish message %d: %v", clientID, i, err)
			continue
		}

		// Read PUBACK
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		pkt, _, _ = mqttv5.ReadPacket(conn, maxPacketSize)
		if puback, ok := pkt.(*mqttv5.PubackPacket); ok {
			fmt.Printf("[%s] Published message %d (acked: %d)\n", clientID, i, puback.PacketID)
		}

		time.Sleep(200 * time.Millisecond)
	}

	// Disconnect
	disconnectPkt := &mqttv5.DisconnectPacket{ReasonCode: mqttv5.ReasonSuccess}
	mqttv5.WritePacket(conn, disconnectPkt, maxPacketSize)
	fmt.Printf("[%s] Disconnected\n", clientID)
}
