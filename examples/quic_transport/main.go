// Package main demonstrates MQTT v5.0 communication over QUIC transport.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
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
	// Generate self-signed certificate for demo
	cert, certPool, err := generateCertificate()
	if err != nil {
		return fmt.Errorf("failed to generate certificate: %w", err)
	}

	// Create QUIC listener with TLS config
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"mqtt"},
	}

	quicListener, err := mqttv5.NewQUICListener("127.0.0.1:8883", serverTLS, nil)
	if err != nil {
		return fmt.Errorf("failed to create quic listener: %w", err)
	}

	// Wrap QUIC listener for server compatibility
	netListener := quicListener.NetListener()

	// Create MQTT server with QUIC listener
	srv := mqttv5.NewServer(
		mqttv5.WithListener(netListener),
		mqttv5.OnConnect(func(client *mqttv5.ServerClient) {
			log.Printf("Client connected: %s", client.ClientID())
		}),
		mqttv5.OnDisconnect(func(client *mqttv5.ServerClient) {
			log.Printf("Client disconnected: %s", client.ClientID())
		}),
		mqttv5.OnMessage(func(client *mqttv5.ServerClient, msg *mqttv5.Message) {
			log.Printf("Message from %s: topic=%s payload=%s",
				client.ClientID(), msg.Topic, string(msg.Payload))
		}),
	)

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down...")
		srv.Close()
	}()

	// Start server in background
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	log.Printf("MQTT broker listening on quic://%s", netListener.Addr())

	// Wait for server to be ready
	time.Sleep(100 * time.Millisecond)

	// Create client TLS config
	clientTLS := &tls.Config{
		RootCAs:    certPool,
		NextProtos: []string{"mqtt"},
		ServerName: "localhost",
	}

	// Connect client via QUIC
	client, err := mqttv5.Dial(
		mqttv5.WithServers("quic://127.0.0.1:8883"),
		mqttv5.WithClientID("quic-client"),
		mqttv5.WithKeepAlive(60),
		mqttv5.WithTLS(clientTLS),
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	log.Printf("Client connected via QUIC")

	// Subscribe to a topic
	err = client.Subscribe("quic/test", 1, func(msg *mqttv5.Message) {
		log.Printf("Received: topic=%s payload=%s", msg.Topic, string(msg.Payload))
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// Publish a message
	err = client.Publish(&mqttv5.Message{
		Topic:   "quic/test",
		Payload: []byte("Hello via QUIC!"),
		QoS:     mqttv5.QoS1,
	})
	if err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	log.Println("Message published, waiting for delivery...")
	time.Sleep(time.Second)

	log.Println("Example completed successfully")
	return nil
}

func generateCertificate() (tls.Certificate, *x509.CertPool, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"MQTT QUIC Example"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(certPEM)

	return cert, certPool, nil
}
