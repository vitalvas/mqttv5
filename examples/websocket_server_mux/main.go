// Package main demonstrates embedding MQTT WebSocket handler into gorilla/mux router.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/vitalvas/mqttv5"
)

// Broker handles MQTT connections and message routing.
type Broker struct {
	mu            sync.RWMutex
	clients       map[string]*ClientInfo
	maxPacketSize uint32
}

// ClientInfo holds information about a connected client.
type ClientInfo struct {
	conn     mqttv5.Conn
	clientID string
}

// NewBroker creates a new broker instance.
func NewBroker() *Broker {
	return &Broker{
		clients:       make(map[string]*ClientInfo),
		maxPacketSize: 256 * 1024,
	}
}

// HandleConnection processes a new MQTT connection.
func (b *Broker) HandleConnection(conn mqttv5.Conn) {
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
		clientID = fmt.Sprintf("mux-client-%p", conn)
	}

	log.Printf("Client connected via gorilla/mux: %s", clientID)

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
	clientInfo := &ClientInfo{
		conn:     conn,
		clientID: clientID,
	}

	b.mu.Lock()
	b.clients[clientID] = clientInfo
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.clients, clientID)
		b.mu.Unlock()
		log.Printf("Client disconnected: %s", clientID)
	}()

	// Handle packets
	for {
		pkt, _, err := mqttv5.ReadPacket(conn, b.maxPacketSize)
		if err != nil {
			return
		}

		switch p := pkt.(type) {
		case *mqttv5.PublishPacket:
			log.Printf("PUBLISH from %s: topic=%s payload=%s", clientID, p.Topic, string(p.Payload))
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
				log.Printf("SUBSCRIBE from %s: filter=%s qos=%d", clientID, sub.TopicFilter, sub.QoS)
				reasonCodes[i] = mqttv5.ReasonCode(sub.QoS)
			}
			suback := &mqttv5.SubackPacket{
				PacketID:    p.PacketID,
				ReasonCodes: reasonCodes,
			}
			mqttv5.WritePacket(conn, suback, b.maxPacketSize)

		case *mqttv5.UnsubscribePacket:
			reasonCodes := make([]mqttv5.ReasonCode, len(p.TopicFilters))
			for i, filter := range p.TopicFilters {
				log.Printf("UNSUBSCRIBE from %s: filter=%s", clientID, filter)
				reasonCodes[i] = mqttv5.ReasonSuccess
			}
			unsuback := &mqttv5.UnsubackPacket{
				PacketID:    p.PacketID,
				ReasonCodes: reasonCodes,
			}
			mqttv5.WritePacket(conn, unsuback, b.maxPacketSize)

		case *mqttv5.PingreqPacket:
			pingresp := &mqttv5.PingrespPacket{}
			mqttv5.WritePacket(conn, pingresp, b.maxPacketSize)

		case *mqttv5.DisconnectPacket:
			log.Printf("DISCONNECT from %s: reason=%s", clientID, p.ReasonCode.String())
			return
		}
	}
}

// GetConnectedClients returns the list of connected client IDs.
func (b *Broker) GetConnectedClients() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	clients := make([]string, 0, len(b.clients))
	for id := range b.clients {
		clients = append(clients, id)
	}
	return clients
}

func main() {
	broker := NewBroker()

	// Create MQTT WebSocket handler
	wsHandler := mqttv5.NewWSHandler(broker.HandleConnection)

	// Create gorilla/mux router
	r := mux.NewRouter()

	// API routes
	api := r.PathPrefix("/api/v1").Subrouter()

	// Get broker status
	api.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		clients := broker.GetConnectedClients()
		response := map[string]any{
			"connected_clients": len(clients),
			"client_ids":        clients,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}).Methods(http.MethodGet)

	// Health check
	api.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "healthy"}`))
	}).Methods(http.MethodGet)

	// Mount MQTT WebSocket handler
	r.Handle("/mqtt", wsHandler)

	log.Println("MQTT WebSocket server with gorilla/mux listening on :8080")
	log.Println("  - WebSocket endpoint: ws://localhost:8080/mqtt")
	log.Println("  - API status: http://localhost:8080/api/v1/status")
	log.Println("  - API health: http://localhost:8080/api/v1/health")

	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
