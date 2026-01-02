package mqttv5

import (
	"context"
	"net"
)

// AuthzAction represents an authorization action.
type AuthzAction int

const (
	// AuthzActionPublish represents a publish action.
	AuthzActionPublish AuthzAction = 0
	// AuthzActionSubscribe represents a subscribe action.
	AuthzActionSubscribe AuthzAction = 1
)

// String returns the string representation of the action.
func (a AuthzAction) String() string {
	switch a {
	case AuthzActionPublish:
		return "publish"
	case AuthzActionSubscribe:
		return "subscribe"
	default:
		return "unknown"
	}
}

// AuthzContext contains information about the authorization request.
type AuthzContext struct {
	// ClientID is the client identifier.
	ClientID string

	// Username is the authenticated username (may be empty).
	Username string

	// Topic is the topic for publish or topic filter for subscribe.
	Topic string

	// Action is the action being performed.
	Action AuthzAction

	// QoS is the QoS level for publish/subscribe.
	QoS byte

	// Retain is true if the publish has retain flag set.
	Retain bool

	// RemoteAddr is the remote address of the client connection.
	RemoteAddr net.Addr

	// LocalAddr is the local address of the server connection.
	LocalAddr net.Addr

	// Namespace is the tenant namespace for multi-tenancy isolation.
	Namespace string
}

// AuthzResult represents the result of an authorization check.
type AuthzResult struct {
	// Allowed indicates if the action is allowed.
	Allowed bool

	// ReasonCode is the MQTT reason code if not allowed.
	ReasonCode ReasonCode

	// MaxQoS is the maximum QoS allowed (may downgrade requested QoS).
	MaxQoS byte
}

// Authorizer defines the interface for authorizing MQTT actions.
type Authorizer interface {
	// Authorize checks if an action is allowed.
	Authorize(ctx context.Context, authzCtx *AuthzContext) (*AuthzResult, error)
}

// AllowAllAuthorizer allows all actions.
type AllowAllAuthorizer struct{}

// Authorize always allows the action.
func (a *AllowAllAuthorizer) Authorize(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
	return &AuthzResult{Allowed: true, MaxQoS: 2}, nil
}

// DenyAllAuthorizer denies all actions.
type DenyAllAuthorizer struct{}

// Authorize always denies the action.
func (d *DenyAllAuthorizer) Authorize(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
	return &AuthzResult{Allowed: false, ReasonCode: ReasonNotAuthorized}, nil
}
