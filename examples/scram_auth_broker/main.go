// Package main demonstrates MQTT v5.0 broker with SCRAM enhanced authentication.
// Supports SCRAM-SHA-1, SCRAM-SHA-256, and SCRAM-SHA-512.
// SCRAM (Salted Challenge Response Authentication Mechanism) provides secure
// challenge-response authentication without transmitting passwords in clear text.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/vitalvas/mqttv5"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// UserStore holds user credentials. In production, this would be a database.
type UserStore struct {
	users map[string]*mqttv5.SCRAMCredentials
}

// LookupCredentials implements SCRAMCredentialLookup interface.
func (s *UserStore) LookupCredentials(_ context.Context, username string) (*mqttv5.SCRAMCredentials, error) {
	creds, ok := s.users[username]
	if !ok {
		return nil, nil
	}
	return creds, nil
}

// createUser creates SCRAM credentials for a user with the given hash, password, and namespace.
func createUser(hashType mqttv5.SCRAMHash, password, namespace string) *mqttv5.SCRAMCredentials {
	salt, err := mqttv5.GenerateSalt()
	if err != nil {
		log.Fatalf("Failed to generate salt: %v", err)
	}

	creds := mqttv5.ComputeSCRAMCredentials(hashType, password, salt, 4096)
	creds.Namespace = namespace
	return creds
}

func run() error {
	// Create user store with SCRAM credentials using different hash algorithms.
	// Each user has credentials for a specific hash algorithm.
	// In production, you might store multiple credential sets per user for migration.
	store := &UserStore{
		users: map[string]*mqttv5.SCRAMCredentials{
			// SHA-1 user (legacy compatibility)
			"legacy-user": createUser(mqttv5.SCRAMHashSHA1, "legacy-pass", "legacy"),

			// SHA-256 users (recommended)
			"admin": createUser(mqttv5.SCRAMHashSHA256, "admin-secret", mqttv5.DefaultNamespace),
			"alice": createUser(mqttv5.SCRAMHashSHA256, "alice-password", "team-alpha"),

			// SHA-512 user (highest security)
			"secure-user": createUser(mqttv5.SCRAMHashSHA512, "secure-pass", "high-security"),
		},
	}

	// Create multi-algorithm SCRAM authenticator - supports all three hash types.
	// Examples:
	// - NewSCRAMAuthenticator(store)                                              // SHA-256 only (default)
	// - NewSCRAMAuthenticator(store, mqttv5.SCRAMHashSHA1)                        // SHA-1 only
	// - NewSCRAMAuthenticator(store, mqttv5.SCRAMHashSHA256)                      // SHA-256 only
	// - NewSCRAMAuthenticator(store, mqttv5.SCRAMHashSHA512)                      // SHA-512 only
	// - NewSCRAMAuthenticator(store, SCRAMHashSHA1, SCRAMHashSHA256, SCRAMHashSHA512) // All three
	auth := mqttv5.NewSCRAMAuthenticator(store, mqttv5.SCRAMHashSHA1, mqttv5.SCRAMHashSHA256, mqttv5.SCRAMHashSHA512)

	listener, err := net.Listen("tcp", ":1883")
	if err != nil {
		return err
	}

	srv := mqttv5.NewServer(
		mqttv5.WithListener(listener),
		mqttv5.WithEnhancedAuth(auth),
	)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		srv.Close()
	}()

	log.Println("MQTT broker with SCRAM authentication on :1883")
	log.Println("")
	log.Println("Supported authentication methods:")
	log.Println("  - SCRAM-SHA-1   (legacy compatibility)")
	log.Println("  - SCRAM-SHA-256 (recommended)")
	log.Println("  - SCRAM-SHA-512 (highest security)")
	log.Println("")
	log.Println("Users:")
	log.Println("  legacy-user:legacy-pass     (SCRAM-SHA-1,   namespace: legacy)")
	log.Println("  admin:admin-secret          (SCRAM-SHA-256, namespace: default)")
	log.Println("  alice:alice-password        (SCRAM-SHA-256, namespace: team-alpha)")
	log.Println("  secure-user:secure-pass     (SCRAM-SHA-512, namespace: high-security)")
	log.Println("")
	log.Println("SCRAM provides:")
	log.Println("  - Challenge-response authentication")
	log.Println("  - No plaintext password transmission")
	log.Println("  - Mutual authentication (server proves identity)")
	log.Println("  - Replay attack protection via nonces")

	return srv.ListenAndServe()
}
