// Package main demonstrates a simple MQTT v5.0 broker using the mqttv5 SDK.
package main

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/vitalvas/mqttv5"
)

type Client struct {
	conn          net.Conn
	clientID      string
	subscriptions []mqttv5.Subscription
}

type Broker struct {
	mu            sync.RWMutex
	clients       map[string]*Client
	retainedStore mqttv5.RetainedStore
	maxPacketSize uint32
}

func NewBroker() *Broker {
	return &Broker{
		clients:       make(map[string]*Client),
		retainedStore: mqttv5.NewMemoryRetainedStore(),
		maxPacketSize: 256 * 1024,
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
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
		clientID = fmt.Sprintf("auto-%d", conn.RemoteAddr())
	}

	log.Printf("Client connected: %s", clientID)

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
	client := &Client{
		conn:     conn,
		clientID: clientID,
	}

	b.mu.Lock()
	b.clients[clientID] = client
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
			b.handlePublish(client, p)

		case *mqttv5.SubscribePacket:
			b.handleSubscribe(client, p)

		case *mqttv5.UnsubscribePacket:
			b.handleUnsubscribe(client, p)

		case *mqttv5.PingreqPacket:
			pingresp := &mqttv5.PingrespPacket{}
			mqttv5.WritePacket(conn, pingresp, b.maxPacketSize)

		case *mqttv5.DisconnectPacket:
			log.Printf("Client %s disconnected with reason: %s", clientID, p.ReasonCode.String())
			return
		}
	}
}

func (b *Broker) handlePublish(client *Client, pub *mqttv5.PublishPacket) {
	log.Printf("PUBLISH from %s: topic=%s qos=%d", client.clientID, pub.Topic, pub.QoS)

	// Handle retained messages
	if pub.Retain {
		if len(pub.Payload) == 0 {
			b.retainedStore.Delete(pub.Topic)
		} else {
			b.retainedStore.Set(&mqttv5.RetainedMessage{
				Topic:   pub.Topic,
				Payload: pub.Payload,
				QoS:     pub.QoS,
			})
		}
	}

	// Send PUBACK for QoS 1
	if pub.QoS == 1 {
		puback := &mqttv5.PubackPacket{
			PacketID:   pub.PacketID,
			ReasonCode: mqttv5.ReasonSuccess,
		}
		mqttv5.WritePacket(client.conn, puback, b.maxPacketSize)
	}

	// Forward to subscribers
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, c := range b.clients {
		for _, sub := range c.subscriptions {
			if mqttv5.TopicMatch(sub.TopicFilter, pub.Topic) {
				forwardPub := &mqttv5.PublishPacket{
					Topic:    pub.Topic,
					QoS:      min(pub.QoS, sub.QoS),
					Payload:  pub.Payload,
					PacketID: pub.PacketID,
				}
				mqttv5.WritePacket(c.conn, forwardPub, b.maxPacketSize)
			}
		}
	}
}

func (b *Broker) handleSubscribe(client *Client, sub *mqttv5.SubscribePacket) {
	log.Printf("SUBSCRIBE from %s: %d topics", client.clientID, len(sub.Subscriptions))

	reasonCodes := make([]mqttv5.ReasonCode, len(sub.Subscriptions))

	for i, s := range sub.Subscriptions {
		client.subscriptions = append(client.subscriptions, s)
		reasonCodes[i] = mqttv5.ReasonCode(s.QoS) // Granted QoS

		// Send retained messages
		retained := b.retainedStore.Match(s.TopicFilter)
		for _, msg := range retained {
			pub := &mqttv5.PublishPacket{
				Topic:   msg.Topic,
				QoS:     min(msg.QoS, s.QoS),
				Payload: msg.Payload,
				Retain:  true,
			}
			mqttv5.WritePacket(client.conn, pub, b.maxPacketSize)
		}
	}

	suback := &mqttv5.SubackPacket{
		PacketID:    sub.PacketID,
		ReasonCodes: reasonCodes,
	}
	mqttv5.WritePacket(client.conn, suback, b.maxPacketSize)
}

func (b *Broker) handleUnsubscribe(client *Client, unsub *mqttv5.UnsubscribePacket) {
	log.Printf("UNSUBSCRIBE from %s: %d topics", client.clientID, len(unsub.TopicFilters))

	reasonCodes := make([]mqttv5.ReasonCode, len(unsub.TopicFilters))

	for i, filter := range unsub.TopicFilters {
		found := false
		for j, s := range client.subscriptions {
			if s.TopicFilter == filter {
				client.subscriptions = append(client.subscriptions[:j], client.subscriptions[j+1:]...)
				found = true
				break
			}
		}
		if found {
			reasonCodes[i] = mqttv5.ReasonSuccess
		} else {
			reasonCodes[i] = mqttv5.ReasonNoSubscriptionExisted
		}
	}

	unsuback := &mqttv5.UnsubackPacket{
		PacketID:    unsub.PacketID,
		ReasonCodes: reasonCodes,
	}
	mqttv5.WritePacket(client.conn, unsuback, b.maxPacketSize)
}

func main() {
	broker := NewBroker()

	listener, err := net.Listen("tcp", ":1883")
	if err != nil {
		log.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	log.Println("MQTT broker listening on :1883")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go broker.handleConnection(conn)
	}
}
