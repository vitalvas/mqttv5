// Package main demonstrates embedding MQTT WebSocket handler into net/http server.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
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
	srv := mqttv5.NewWSServer(
		mqttv5.OnConnect(func(client *mqttv5.ServerClient) {
			log.Printf("WebSocket client connected: %s", client.ClientID())
		}),
		mqttv5.OnDisconnect(func(client *mqttv5.ServerClient) {
			log.Printf("WebSocket client disconnected: %s", client.ClientID())
		}),
		mqttv5.OnMessage(func(client *mqttv5.ServerClient, msg *mqttv5.Message) {
			log.Printf("Message from %s: topic=%s", client.ClientID(), msg.Topic)
		}),
	)

	// Start background tasks
	srv.Start()
	defer srv.Close()

	// Create HTTP server with multiple endpoints
	mux := http.NewServeMux()

	// Mount MQTT WebSocket handler at /mqtt
	mux.Handle("/mqtt", srv)

	// Add a health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Add a status endpoint
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"connected_clients": srv.ClientCount(),
			"client_ids":        srv.Clients(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down...")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		httpServer.Shutdown(ctx)
	}()

	log.Println("MQTT WebSocket server listening on :8080")
	log.Println("  - WebSocket endpoint: ws://localhost:8080/mqtt")
	log.Println("  - Health check: http://localhost:8080/health")
	log.Println("  - Status: http://localhost:8080/status")

	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}

	return nil
}
