// Package main demonstrates MQTT v5.0 broker with custom authentication and authorization.
package main

import (
	"context"
	"fmt"
	"log"
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

// Auth implements Authenticator interface.
type Auth struct {
	Users map[string]string
}

func (a *Auth) Authenticate(_ context.Context, ctx *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
	if pass, ok := a.Users[ctx.Username]; ok && pass == string(ctx.Password) {
		return &mqttv5.AuthResult{Success: true, ReasonCode: mqttv5.ReasonSuccess}, nil
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
	auth := &Auth{Users: map[string]string{"admin": "admin", "user1": "pass1"}}
	authz := &Authz{}

	srv, err := mqttv5.NewServer(":1883",
		mqttv5.WithServerAuth(auth),
		mqttv5.WithServerAuthz(authz),
	)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		srv.Close()
	}()

	log.Println("MQTT broker on :1883 (users: admin:admin, user1:pass1)")
	return srv.ListenAndServe()
}
