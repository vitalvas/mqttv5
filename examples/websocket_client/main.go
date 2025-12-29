// Package main demonstrates an MQTT v5.0 client over WebSocket using the mqttv5 SDK.
//
//nolint:gocritic // Example code - OS handles cleanup on exit
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/vitalvas/mqttv5"
)

func main() {
	// Create WebSocket dialer with MQTT subprotocol
	dialer := mqttv5.NewWSDialer()

	// Connect to MQTT broker over WebSocket
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialer.Dial(ctx, "ws://localhost:8080/mqtt")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	maxPacketSize := uint32(256 * 1024)

	// Send CONNECT packet
	connectPkt := &mqttv5.ConnectPacket{
		ClientID:   "websocket-client-example",
		CleanStart: true,
		KeepAlive:  60,
	}

	if _, err := mqttv5.WritePacket(conn, connectPkt, maxPacketSize); err != nil {
		log.Fatalf("Failed to send CONNECT: %v", err)
	}

	// Read CONNACK
	pkt, _, err := mqttv5.ReadPacket(conn, maxPacketSize)
	if err != nil {
		log.Fatalf("Failed to read CONNACK: %v", err)
	}

	connack, ok := pkt.(*mqttv5.ConnackPacket)
	if !ok {
		log.Fatalf("Expected CONNACK, got %T", pkt)
	}

	if connack.ReasonCode != mqttv5.ReasonSuccess {
		log.Fatalf("Connection failed: %s", connack.ReasonCode.String())
	}

	fmt.Println("Connected over WebSocket!")

	// Subscribe to a topic
	subscribePkt := &mqttv5.SubscribePacket{
		PacketID: 1,
		Subscriptions: []mqttv5.Subscription{
			{TopicFilter: "websocket/test", QoS: 1},
		},
	}

	if _, err := mqttv5.WritePacket(conn, subscribePkt, maxPacketSize); err != nil {
		log.Fatalf("Failed to send SUBSCRIBE: %v", err)
	}

	// Read SUBACK
	pkt, _, err = mqttv5.ReadPacket(conn, maxPacketSize)
	if err != nil {
		log.Fatalf("Failed to read SUBACK: %v", err)
	}

	suback, ok := pkt.(*mqttv5.SubackPacket)
	if !ok {
		log.Fatalf("Expected SUBACK, got %T", pkt)
	}

	fmt.Printf("Subscribed with reason codes: %v\n", suback.ReasonCodes)

	// Publish a message
	publishPkt := &mqttv5.PublishPacket{
		Topic:    "websocket/test",
		QoS:      1,
		PacketID: 2,
		Payload:  []byte("Hello from WebSocket client!"),
	}

	if _, err := mqttv5.WritePacket(conn, publishPkt, maxPacketSize); err != nil {
		log.Fatalf("Failed to send PUBLISH: %v", err)
	}

	fmt.Println("Published message!")

	// Read a few packets with timeout
	for range 3 {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		pkt, _, err = mqttv5.ReadPacket(conn, maxPacketSize)
		if err != nil {
			break
		}

		switch p := pkt.(type) {
		case *mqttv5.PublishPacket:
			fmt.Printf("Received message on %s: %s\n", p.Topic, string(p.Payload))
			if p.QoS == 1 {
				puback := &mqttv5.PubackPacket{
					PacketID:   p.PacketID,
					ReasonCode: mqttv5.ReasonSuccess,
				}
				mqttv5.WritePacket(conn, puback, maxPacketSize)
			}
		case *mqttv5.PubackPacket:
			fmt.Printf("Received PUBACK for packet %d\n", p.PacketID)
		}
	}

	// Disconnect
	disconnectPkt := &mqttv5.DisconnectPacket{
		ReasonCode: mqttv5.ReasonSuccess,
	}
	mqttv5.WritePacket(conn, disconnectPkt, maxPacketSize)

	fmt.Println("Disconnected")
}
