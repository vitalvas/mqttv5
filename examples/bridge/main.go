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

	var bridge *mqttv5.Bridge

	localServer := mqttv5.NewServer(
		mqttv5.WithListener(localListener),
		mqttv5.OnMessage(func(_ *mqttv5.ServerClient, msg *mqttv5.Message) {
			log.Printf("Local message: %s", msg.Topic)
			// Forward to remote broker
			if bridge != nil {
				bridge.ForwardToRemote(msg)
			}
		}),
	)

	// Create bridge to remote broker
	bridge, err = mqttv5.NewBridge(localServer, mqttv5.BridgeConfig{
		RemoteAddr: "tcp://remote-broker.example.com:1883",
		ClientID:   "my-bridge",
		Topics: []mqttv5.BridgeTopic{
			{
				LocalPrefix:  "local/sensors",
				RemotePrefix: "remote/sensors",
				Direction:    mqttv5.BridgeDirectionBoth,
				QoS:          mqttv5.QoS1,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Start bridge
	if err := bridge.Start(); err != nil {
		log.Printf("Failed to start bridge: %v", err)
	} else {
		log.Printf("Bridge %s started", bridge.ID())
	}

	// Start server
	go func() {
		log.Printf("Local broker listening on %s", localListener.Addr())
		if err := localServer.ListenAndServe(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// Wait for shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	bridge.Stop()
	localServer.Close()
}
