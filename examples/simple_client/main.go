// Package main demonstrates a simple MQTT v5.0 client using the mqttv5 SDK.
//
//nolint:gocritic // Example code - OS handles cleanup on exit
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/vitalvas/mqttv5"
)

func main() {
	// Connect to MQTT broker
	conn, err := net.DialTimeout("tcp", "localhost:1883", 5*time.Second)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	maxPacketSize := uint32(256 * 1024)

	// Send CONNECT packet
	connectPkt := &mqttv5.ConnectPacket{
		ClientID:   "simple-client-example",
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

	fmt.Println("Connected successfully!")
	fmt.Printf("Session present: %v\n", connack.SessionPresent)

	// Subscribe to a topic
	subscribePkt := &mqttv5.SubscribePacket{
		PacketID: 1,
		Subscriptions: []mqttv5.Subscription{
			{TopicFilter: "example/topic", QoS: 1},
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
		Topic:    "example/topic",
		QoS:      1,
		PacketID: 2,
		Payload:  []byte("Hello, MQTT v5.0!"),
	}

	if _, err := mqttv5.WritePacket(conn, publishPkt, maxPacketSize); err != nil {
		log.Fatalf("Failed to send PUBLISH: %v", err)
	}

	fmt.Println("Published message!")

	// Read incoming packets with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Timeout waiting for messages")
			goto cleanup
		default:
		}

		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		pkt, _, err = mqttv5.ReadPacket(conn, maxPacketSize)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			log.Printf("Read error: %v", err)
			break
		}

		switch p := pkt.(type) {
		case *mqttv5.PublishPacket:
			fmt.Printf("Received message on %s: %s\n", p.Topic, string(p.Payload))
			if p.QoS == 1 {
				// Send PUBACK
				puback := &mqttv5.PubackPacket{
					PacketID:   p.PacketID,
					ReasonCode: mqttv5.ReasonSuccess,
				}
				mqttv5.WritePacket(conn, puback, maxPacketSize)
			}
		case *mqttv5.PubackPacket:
			fmt.Printf("Received PUBACK for packet %d\n", p.PacketID)
		case *mqttv5.PingrespPacket:
			fmt.Println("Received PINGRESP")
		}
	}

cleanup:
	// Disconnect gracefully
	disconnectPkt := &mqttv5.DisconnectPacket{
		ReasonCode: mqttv5.ReasonSuccess,
	}
	mqttv5.WritePacket(conn, disconnectPkt, maxPacketSize)

	fmt.Println("Disconnected")
}
