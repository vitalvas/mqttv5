package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/vitalvas/mqttv5"
)

func main() {
	// Start local broker
	localListener, err := net.Listen("tcp", ":1883")
	if err != nil {
		log.Fatal(err)
	}

	// Create bridge manager first (before server starts)
	var manager *mqttv5.BridgeManager

	localServer := mqttv5.NewServer(
		mqttv5.WithListener(localListener),
		mqttv5.OnConnect(func(c *mqttv5.ServerClient) {
			log.Printf("Client connected: %s", c.ClientID())
		}),
		mqttv5.OnMessage(func(_ *mqttv5.ServerClient, msg *mqttv5.Message) {
			log.Printf("Local message: %s -> %s", msg.Topic, msg.Payload)
			// Forward to remote brokers via bridge manager
			if manager != nil {
				manager.ForwardToRemote(msg)
			}
		}),
	)

	// Initialize bridge manager
	manager = mqttv5.NewBridgeManager(localServer)

	// Bridge to cloud broker for sensor data
	_, err = manager.Add(mqttv5.BridgeConfig{
		RemoteAddr: "tcp://cloud-broker.example.com:1883",
		ClientID:   "bridge-cloud",
		Username:   "bridge-user",
		Password:   "bridge-pass",
		Topics: []mqttv5.BridgeTopic{
			{
				LocalPrefix:  "sensors",
				RemotePrefix: "devices/local-site/sensors",
				Direction:    mqttv5.BridgeDirectionOut, // local -> cloud
				QoS:          1,
			},
			{
				LocalPrefix:  "commands",
				RemotePrefix: "devices/local-site/commands",
				Direction:    mqttv5.BridgeDirectionIn, // cloud -> local
				QoS:          1,
			},
		},
	})
	if err != nil {
		log.Printf("Failed to add cloud bridge: %v", err)
	}

	// Bridge to analytics broker for metrics
	_, err = manager.Add(mqttv5.BridgeConfig{
		RemoteAddr: "tcp://analytics-broker.example.com:1883",
		ClientID:   "bridge-analytics",
		Topics: []mqttv5.BridgeTopic{
			{
				LocalPrefix:  "metrics",
				RemotePrefix: "ingest/metrics",
				Direction:    mqttv5.BridgeDirectionOut,
				QoS:          0,
			},
		},
	})
	if err != nil {
		log.Printf("Failed to add analytics bridge: %v", err)
	}

	// Bridge with custom topic remapper
	_, err = manager.Add(mqttv5.BridgeConfig{
		RemoteAddr: "tcp://legacy-broker.example.com:1883",
		ClientID:   "bridge-legacy",
		Topics: []mqttv5.BridgeTopic{
			{
				LocalPrefix:  "alerts",
				RemotePrefix: "notifications",
				Direction:    mqttv5.BridgeDirectionBoth,
				QoS:          2,
			},
		},
		TopicRemapper: func(topic string, direction mqttv5.BridgeDirection) string {
			// Custom remapping logic for legacy system
			if direction == mqttv5.BridgeDirectionOut && topic == "alerts/critical" {
				return "CRITICAL_ALERT"
			}
			// Return empty to use default prefix-based remapping
			return ""
		},
	})
	if err != nil {
		log.Printf("Failed to add legacy bridge: %v", err)
	}

	log.Printf("Registered %d bridges", manager.Count())

	// Start all bridges
	if err := manager.StartAll(); err != nil {
		log.Printf("Some bridges failed to start: %v", err)
	}

	log.Printf("Running %d bridges", manager.RunningCount())

	// Start server
	go func() {
		log.Printf("Local broker listening on %s", localListener.Addr())
		if err := localServer.ListenAndServe(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")

	// Stop all bridges
	if err := manager.StopAll(); err != nil {
		log.Printf("Error stopping bridges: %v", err)
	}

	localServer.Close()
	log.Println("Shutdown complete")
}
