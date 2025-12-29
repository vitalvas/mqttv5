// Package main demonstrates an MQTT v5.0 client with TLS using the mqttv5 SDK.
//
//nolint:gocritic // Example code - OS handles cleanup on exit
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/vitalvas/mqttv5"
)

func main() {
	// Configure TLS
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // For testing only; use proper CA verification in production
	}

	// Create TLS dialer
	dialer := &mqttv5.TLSDialer{
		Config:  tlsConfig,
		Timeout: 5 * time.Second,
	}

	// Connect with TLS
	ctx := context.Background()
	conn, err := dialer.Dial(ctx, "localhost:8883")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	maxPacketSize := uint32(256 * 1024)

	// Send CONNECT packet
	connectPkt := &mqttv5.ConnectPacket{
		ClientID:   "tls-client-example",
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

	fmt.Println("Connected with TLS!")

	// Publish a message
	publishPkt := &mqttv5.PublishPacket{
		Topic:   "secure/topic",
		QoS:     0,
		Payload: []byte("Secure message over TLS"),
	}

	if _, err := mqttv5.WritePacket(conn, publishPkt, maxPacketSize); err != nil {
		log.Fatalf("Failed to send PUBLISH: %v", err)
	}

	fmt.Println("Published secure message!")

	// Disconnect
	disconnectPkt := &mqttv5.DisconnectPacket{
		ReasonCode: mqttv5.ReasonSuccess,
	}
	mqttv5.WritePacket(conn, disconnectPkt, maxPacketSize)

	fmt.Println("Disconnected")
}
