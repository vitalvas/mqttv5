package mqttv5

import (
	"context"
	"net"
)

// AuthResult represents the result of an authentication attempt.
type AuthResult struct {
	// Success indicates whether authentication was successful.
	Success bool

	// ReasonCode is the MQTT reason code to return to the client.
	ReasonCode ReasonCode

	// Properties contains additional properties to send in CONNACK or AUTH.
	Properties Properties

	// AssignedClientID is set if the server assigns a client ID.
	AssignedClientID string

	// SessionPresent indicates if a session already exists for this client.
	SessionPresent bool

	// ContinueAuth indicates that enhanced authentication should continue.
	ContinueAuth bool

	// AuthData contains authentication data to send back to the client.
	AuthData []byte
}

// AuthContext contains information about the authentication request.
type AuthContext struct {
	// ClientID is the client identifier from the CONNECT packet.
	ClientID string

	// Username is the username from the CONNECT packet (may be empty).
	Username string

	// Password is the password from the CONNECT packet (may be empty).
	Password []byte

	// RemoteAddr is the remote address of the client connection.
	RemoteAddr net.Addr

	// LocalAddr is the local address of the server connection.
	LocalAddr net.Addr

	// TLSCommonName is the common name from the client TLS certificate (if any).
	TLSCommonName string

	// TLSVerified indicates if the client presented a valid TLS certificate.
	TLSVerified bool

	// ConnectPacket provides access to the full CONNECT packet.
	ConnectPacket *ConnectPacket

	// AuthMethod is the authentication method from CONNECT properties.
	AuthMethod string

	// AuthData is the authentication data from CONNECT properties.
	AuthData []byte

	// CleanStart indicates if this is a clean session.
	CleanStart bool
}

// Authenticator defines the interface for authenticating MQTT clients.
type Authenticator interface {
	// Authenticate authenticates a client connection.
	// Returns AuthResult indicating success/failure and any properties to include.
	Authenticate(ctx context.Context, authCtx *AuthContext) (*AuthResult, error)
}

// EnhancedAuthContext contains information for enhanced authentication exchanges.
type EnhancedAuthContext struct {
	// ClientID is the client identifier.
	ClientID string

	// AuthMethod is the authentication method being used.
	AuthMethod string

	// AuthData is the authentication data from the AUTH packet.
	AuthData []byte

	// ReasonCode is the reason code from the AUTH packet.
	ReasonCode ReasonCode

	// RemoteAddr is the remote address of the client connection.
	RemoteAddr net.Addr

	// State holds authenticator-specific state between exchanges.
	State any
}

// EnhancedAuthResult represents the result of an enhanced authentication step.
type EnhancedAuthResult struct {
	// Success indicates authentication completed successfully.
	Success bool

	// Continue indicates more authentication exchanges are needed.
	Continue bool

	// ReasonCode is the reason code for the AUTH response.
	ReasonCode ReasonCode

	// AuthData is authentication data to send to the client.
	AuthData []byte

	// Properties contains additional properties for the AUTH packet.
	Properties Properties

	// State holds authenticator-specific state for the next exchange.
	State any
}

// EnhancedAuthenticator defines the interface for enhanced authentication.
// Enhanced authentication allows multi-step authentication exchanges using
// the AUTH packet (MQTT 5.0 feature).
type EnhancedAuthenticator interface {
	// SupportsMethod returns true if this authenticator supports the given method.
	SupportsMethod(method string) bool

	// AuthStart begins the enhanced authentication process.
	// Called when a CONNECT packet with AuthMethod property is received.
	AuthStart(ctx context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error)

	// AuthContinue continues the enhanced authentication process.
	// Called when an AUTH packet is received during authentication.
	AuthContinue(ctx context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error)
}

// AllowAllAuthenticator allows all connections without checking credentials.
type AllowAllAuthenticator struct{}

// Authenticate always returns success.
func (a *AllowAllAuthenticator) Authenticate(_ context.Context, _ *AuthContext) (*AuthResult, error) {
	return &AuthResult{Success: true, ReasonCode: ReasonSuccess}, nil
}

// DenyAllAuthenticator denies all connections.
type DenyAllAuthenticator struct{}

// Authenticate always returns not authorized.
func (d *DenyAllAuthenticator) Authenticate(_ context.Context, _ *AuthContext) (*AuthResult, error) {
	return &AuthResult{Success: false, ReasonCode: ReasonNotAuthorized}, nil
}
