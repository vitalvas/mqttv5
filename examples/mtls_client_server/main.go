// Package main demonstrates MQTT v5.0 mTLS authentication with certificate identity mapping.
//
// This example shows:
// - Server with mTLS requiring client certificates
// - TLS identity mapper extracting username from certificate CN
// - Namespace extraction from certificate Organizational Unit (OU)
// - Client authentication using client certificate
package main

import (
	"context"
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

// CertificateAuthority holds CA certificate and key for signing.
type CertificateAuthority struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
}

// generateCA creates a self-signed Certificate Authority.
func generateCA() (*CertificateAuthority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"MQTT Example CA"},
			CommonName:   "MQTT Example Root CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create CA certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	return &CertificateAuthority{
		Cert:    cert,
		Key:     key,
		CertPEM: certPEM,
	}, nil
}

// generateServerCert creates a server certificate signed by the CA.
func generateServerCert(ca *CertificateAuthority) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate server key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"MQTT Server"},
			CommonName:   "localhost",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create server certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal server key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return tls.X509KeyPair(certPEM, keyPEM)
}

// generateClientCert creates a client certificate signed by the CA.
// The CN becomes the username and OU becomes the namespace.
func generateClientCert(ca *CertificateAuthority, cn string, ou string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate client key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			Organization:       []string{"MQTT Clients"},
			OrganizationalUnit: []string{ou},
			CommonName:         cn,
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create client certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal client key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return tls.X509KeyPair(certPEM, keyPEM)
}

// MTLSAuthenticator authenticates clients based on their TLS certificate identity.
// It also sets SessionExpiry based on the certificate's NotAfter time,
// causing the server to automatically disconnect clients when their certificate expires.
type MTLSAuthenticator struct{}

func (a *MTLSAuthenticator) Authenticate(_ context.Context, ctx *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
	// Require TLS with client certificate
	if ctx.TLSConnectionState == nil {
		log.Printf("Auth failed: no TLS connection")
		return &mqttv5.AuthResult{
			Success:    false,
			ReasonCode: mqttv5.ReasonNotAuthorized,
		}, nil
	}

	if len(ctx.TLSConnectionState.PeerCertificates) == 0 {
		log.Printf("Auth failed: no client certificate")
		return &mqttv5.AuthResult{
			Success:    false,
			ReasonCode: mqttv5.ReasonNotAuthorized,
		}, nil
	}

	// Use identity from TLS mapper
	if ctx.TLSIdentity == nil {
		log.Printf("Auth failed: no TLS identity mapped")
		return &mqttv5.AuthResult{
			Success:    false,
			ReasonCode: mqttv5.ReasonNotAuthorized,
		}, nil
	}

	// Get certificate for expiry tracking
	cert := ctx.TLSConnectionState.PeerCertificates[0]

	// Log certificate details for demonstration
	log.Printf("Client authenticated via mTLS:")
	log.Printf("  Certificate CN: %s", cert.Subject.CommonName)
	log.Printf("  Certificate OU: %v", cert.Subject.OrganizationalUnit)
	log.Printf("  Certificate Expires: %s", cert.NotAfter.Format(time.RFC3339))
	log.Printf("  Mapped Username: %s", ctx.TLSIdentity.Username)
	log.Printf("  Mapped Namespace: %s", ctx.TLSIdentity.Namespace)

	// Use mapped identity
	namespace := ctx.TLSIdentity.Namespace
	if namespace == "" {
		namespace = mqttv5.DefaultNamespace
	}

	return &mqttv5.AuthResult{
		Success:    true,
		ReasonCode: mqttv5.ReasonSuccess,
		Namespace:  namespace,
		// Set session expiry to certificate expiry time
		// Server will automatically disconnect client when certificate expires
		SessionExpiry: cert.NotAfter,
	}, nil
}

func run() error {
	// Generate PKI: CA, server cert, and client certs
	log.Println("Generating PKI...")

	ca, err := generateCA()
	if err != nil {
		return fmt.Errorf("generate CA: %w", err)
	}

	serverCert, err := generateServerCert(ca)
	if err != nil {
		return fmt.Errorf("generate server cert: %w", err)
	}

	// Create client certificates for two different tenants
	client1Cert, err := generateClientCert(ca, "device-001", "tenant-alpha")
	if err != nil {
		return fmt.Errorf("generate client1 cert: %w", err)
	}

	client2Cert, err := generateClientCert(ca, "device-002", "tenant-beta")
	if err != nil {
		return fmt.Errorf("generate client2 cert: %w", err)
	}

	// Create CA pool for client verification
	caPool := x509.NewCertPool()
	caPool.AddCert(ca.Cert)

	// Server TLS config requiring client certificates
	serverTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}

	// Create TLS listener
	listener, err := tls.Listen("tcp", ":8883", serverTLSConfig)
	if err != nil {
		return fmt.Errorf("create TLS listener: %w", err)
	}

	// Configure TLS identity mapper:
	// - Username from certificate CN
	// - Namespace from certificate OU
	identityMapper := mqttv5.TLSIdentityMapperFunc(func(_ context.Context, state *tls.ConnectionState) (*mqttv5.TLSIdentity, error) {
		if state == nil || len(state.PeerCertificates) == 0 {
			return nil, nil
		}

		cert := state.PeerCertificates[0]
		identity := &mqttv5.TLSIdentity{
			Username: cert.Subject.CommonName,
		}

		if len(cert.Subject.OrganizationalUnit) > 0 {
			identity.Namespace = cert.Subject.OrganizationalUnit[0]
		}

		return identity, nil
	})

	// Create server with mTLS authentication
	srv := mqttv5.NewServer(
		mqttv5.WithListener(listener),
		mqttv5.WithTLSIdentityMapper(identityMapper),
		mqttv5.WithServerAuth(&MTLSAuthenticator{}),
		mqttv5.OnMessage(func(client *mqttv5.ServerClient, msg *mqttv5.Message) {
			log.Printf("Message from %s (ns: %s): %s -> %s",
				client.ClientID(), client.Namespace(), msg.Topic, string(msg.Payload))
		}),
	)

	// Start server in background
	go func() {
		log.Println("MQTT broker with mTLS on :8883")
		if err := srv.ListenAndServe(); err != nil && err != mqttv5.ErrServerClosed {
			log.Printf("Server error: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Connect client 1 (tenant-alpha)
	log.Println("\nConnecting client 1 (device-001 / tenant-alpha)...")
	client1, err := connectClient("device-001", client1Cert, ca.CertPEM)
	if err != nil {
		return fmt.Errorf("connect client1: %w", err)
	}
	defer client1.Close()

	// Connect client 2 (tenant-beta)
	log.Println("\nConnecting client 2 (device-002 / tenant-beta)...")
	client2, err := connectClient("device-002", client2Cert, ca.CertPEM)
	if err != nil {
		return fmt.Errorf("connect client2: %w", err)
	}
	defer client2.Close()

	// Subscribe and publish
	log.Println("\nSubscribing to topics...")

	if err := client1.Subscribe("sensors/#", mqttv5.QoS1, func(msg *mqttv5.Message) {
		log.Printf("[Client1] Received: %s -> %s", msg.Topic, string(msg.Payload))
	}); err != nil {
		return fmt.Errorf("client1 subscribe: %w", err)
	}

	if err := client2.Subscribe("sensors/#", mqttv5.QoS1, func(msg *mqttv5.Message) {
		log.Printf("[Client2] Received: %s -> %s", msg.Topic, string(msg.Payload))
	}); err != nil {
		return fmt.Errorf("client2 subscribe: %w", err)
	}

	time.Sleep(100 * time.Millisecond)

	log.Println("\nPublishing messages...")

	// Client 1 publishes (tenant-alpha namespace)
	if err := client1.Publish(&mqttv5.Message{
		Topic:   "sensors/temperature",
		Payload: []byte("22.5C from tenant-alpha"),
		QoS:     mqttv5.QoS1,
	}); err != nil {
		return fmt.Errorf("client1 publish: %w", err)
	}

	// Client 2 publishes (tenant-beta namespace)
	if err := client2.Publish(&mqttv5.Message{
		Topic:   "sensors/humidity",
		Payload: []byte("65% from tenant-beta"),
		QoS:     mqttv5.QoS1,
	}); err != nil {
		return fmt.Errorf("client2 publish: %w", err)
	}

	time.Sleep(500 * time.Millisecond)

	log.Println("\nNote: Clients in different namespaces are isolated.")
	log.Println("Each client only receives messages from their own namespace.")

	// Wait for shutdown signal
	log.Println("\nPress Ctrl+C to exit...")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	client1.Close()
	client2.Close()
	srv.Close()

	return nil
}

func connectClient(clientID string, cert tls.Certificate, caPEM []byte) (*mqttv5.Client, error) {
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caPEM)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}

	return mqttv5.Dial(
		mqttv5.WithServers("tls://localhost:8883"),
		mqttv5.WithClientID(clientID),
		mqttv5.WithTLS(tlsConfig),
		mqttv5.WithKeepAlive(60),
	)
}
