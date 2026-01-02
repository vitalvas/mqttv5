// Package main demonstrates MQTT v5.0 broker with custom authentication,
// authorization, and multi-tenant namespace isolation.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/vitalvas/mqttv5"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// User represents a user with credentials and namespace assignment.
type User struct {
	Password  string
	Namespace string
}

// Auth implements Authenticator interface with multi-tenant support.
type Auth struct {
	Users map[string]User
}

func (a *Auth) Authenticate(_ context.Context, ctx *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
	if user, ok := a.Users[ctx.Username]; ok && user.Password == string(ctx.Password) {
		return &mqttv5.AuthResult{
			Success:    true,
			ReasonCode: mqttv5.ReasonSuccess,
			Namespace:  user.Namespace, // Assign user to their tenant namespace
		}, nil
	}
	return &mqttv5.AuthResult{Success: false, ReasonCode: mqttv5.ReasonBadUserNameOrPassword}, nil
}

// Authz implements Authorizer interface.
type Authz struct{}

func (a *Authz) Authorize(_ context.Context, ctx *mqttv5.AuthzContext) (*mqttv5.AuthzResult, error) {
	// Admin has full access
	if ctx.Username == "admin" {
		return &mqttv5.AuthzResult{Allowed: true, MaxQoS: 2}, nil
	}
	// Users have RW access to their own topics
	if strings.HasPrefix(ctx.Topic, path.Join("users", ctx.Username)+"/") {
		return &mqttv5.AuthzResult{Allowed: true, MaxQoS: 1}, nil
	}
	// Users have RO access to public topics
	if ctx.Action == mqttv5.AuthzActionSubscribe && strings.HasPrefix(ctx.Topic, "public/") {
		return &mqttv5.AuthzResult{Allowed: true, MaxQoS: 1}, nil
	}
	return &mqttv5.AuthzResult{Allowed: false, ReasonCode: mqttv5.ReasonNotAuthorized}, nil
}

func run() error {
	// Multi-tenant user configuration:
	// - admin: full access in "default" namespace
	// - tenant1-user: isolated in "tenant1" namespace
	// - tenant2-user: isolated in "tenant2" namespace
	// Users in different namespaces cannot see each other's messages.
	auth := &Auth{Users: map[string]User{
		"admin":        {Password: "admin", Namespace: mqttv5.DefaultNamespace},
		"tenant1-user": {Password: "pass1", Namespace: "tenant1"},
		"tenant2-user": {Password: "pass2", Namespace: "tenant2"},
	}}
	authz := &Authz{}

	listener, err := net.Listen("tcp", ":1883")
	if err != nil {
		return err
	}

	srv := mqttv5.NewServer(
		mqttv5.WithListener(listener),
		mqttv5.WithServerAuth(auth),
		mqttv5.WithServerAuthz(authz),
	)

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		srv.Close()
	}()

	log.Println("MQTT broker on :1883")
	log.Println("Users:")
	log.Println("  admin:admin (namespace: default)")
	log.Println("  tenant1-user:pass1 (namespace: tenant1)")
	log.Println("  tenant2-user:pass2 (namespace: tenant2)")
	log.Println("Users in different namespaces are fully isolated.")
	return srv.ListenAndServe()
}
