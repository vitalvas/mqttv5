// Package main demonstrates embedding MQTT WebSocket handler into net/http server.
package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/vitalvas/mqttv5"
)

// SimpleBroker handles MQTT connections.
type SimpleBroker struct {
	mu            sync.RWMutex
	clients       map[string]mqttv5.Conn
	maxPacketSize uint32
}

// NewSimpleBroker creates a new broker instance.
func NewSimpleBroker() *SimpleBroker {
	return &SimpleBroker{
		clients:       make(map[string]mqttv5.Conn),
		maxPacketSize: 256 * 1024,
	}
}

// HandleConnection processes a new MQTT connection.
func (b *SimpleBroker) HandleConnection(conn mqttv5.Conn) {
	defer conn.Close()

	// Read CONNECT packet
	pkt, _, err := mqttv5.ReadPacket(conn, b.maxPacketSize)
	if err != nil {
		log.Printf("Failed to read CONNECT: %v", err)
		return
	}

	connect, ok := pkt.(*mqttv5.ConnectPacket)
	if !ok {
		log.Printf("Expected CONNECT, got %T", pkt)
		return
	}

	clientID := connect.ClientID
	if clientID == "" {
		clientID = fmt.Sprintf("ws-client-%p", conn)
	}

	log.Printf("WebSocket client connected: %s", clientID)

	// Send CONNACK
	connack := &mqttv5.ConnackPacket{
		SessionPresent: false,
		ReasonCode:     mqttv5.ReasonSuccess,
	}

	if _, err := mqttv5.WritePacket(conn, connack, b.maxPacketSize); err != nil {
		log.Printf("Failed to send CONNACK: %v", err)
		return
	}

	// Register client
	b.mu.Lock()
	b.clients[clientID] = conn
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.clients, clientID)
		b.mu.Unlock()
		log.Printf("WebSocket client disconnected: %s", clientID)
	}()

	// Handle packets
	for {
		pkt, _, err := mqttv5.ReadPacket(conn, b.maxPacketSize)
		if err != nil {
			return
		}

		switch p := pkt.(type) {
		case *mqttv5.PublishPacket:
			log.Printf("PUBLISH from %s: topic=%s", clientID, p.Topic)
			if p.QoS == 1 {
				puback := &mqttv5.PubackPacket{
					PacketID:   p.PacketID,
					ReasonCode: mqttv5.ReasonSuccess,
				}
				mqttv5.WritePacket(conn, puback, b.maxPacketSize)
			}

		case *mqttv5.SubscribePacket:
			reasonCodes := make([]mqttv5.ReasonCode, len(p.Subscriptions))
			for i, sub := range p.Subscriptions {
				log.Printf("SUBSCRIBE from %s: filter=%s", clientID, sub.TopicFilter)
				reasonCodes[i] = mqttv5.ReasonCode(sub.QoS)
			}
			suback := &mqttv5.SubackPacket{
				PacketID:    p.PacketID,
				ReasonCodes: reasonCodes,
			}
			mqttv5.WritePacket(conn, suback, b.maxPacketSize)

		case *mqttv5.PingreqPacket:
			pingresp := &mqttv5.PingrespPacket{}
			mqttv5.WritePacket(conn, pingresp, b.maxPacketSize)

		case *mqttv5.DisconnectPacket:
			log.Printf("DISCONNECT from %s: reason=%s", clientID, p.ReasonCode.String())
			return
		}
	}
}

func main() {
	broker := NewSimpleBroker()

	// Create MQTT WebSocket handler
	wsHandler := mqttv5.NewWSHandler(broker.HandleConnection)

	// Create HTTP server with multiple endpoints
	mux := http.NewServeMux()

	// Mount MQTT WebSocket handler at /mqtt
	mux.Handle("/mqtt", wsHandler)

	// Add a health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Add a status endpoint
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		broker.mu.RLock()
		count := len(broker.clients)
		broker.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"connected_clients": %d}`, count)
	})

	log.Println("MQTT WebSocket server listening on :8080")
	log.Println("  - WebSocket endpoint: ws://localhost:8080/mqtt")
	log.Println("  - Health check: http://localhost:8080/health")
	log.Println("  - Status: http://localhost:8080/status")

	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
