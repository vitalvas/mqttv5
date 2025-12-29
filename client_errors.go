package mqttv5

import (
	"errors"
	"time"
)

// Event handler function type.
type EventHandler func(client *Client, event error)

// Sentinel events for client lifecycle - check with errors.Is().
var (
	// ErrConnected is emitted when the client successfully connects.
	ErrConnected = errors.New("connected")

	// ErrDisconnected is emitted when the client disconnects gracefully.
	ErrDisconnected = errors.New("disconnected")

	// ErrConnectionLost is emitted when the connection is lost unexpectedly.
	ErrConnectionLost = errors.New("connection lost")

	// ErrReconnecting is emitted when the client is attempting to reconnect.
	ErrReconnecting = errors.New("reconnecting")

	// ErrReconnectFailed is emitted when all reconnection attempts have failed.
	ErrReconnectFailed = errors.New("reconnect failed")
)

// Sentinel errors for authentication - check with errors.Is().
var (
	// ErrAuthFailed is returned when authentication fails.
	ErrAuthFailed = errors.New("authentication failed")

	// ErrNotAuthorized is returned when the client is not authorized for an operation.
	ErrNotAuthorized = errors.New("not authorized")
)

// Sentinel errors for protocol issues - check with errors.Is().
var (
	// ErrProtocolError is returned when a protocol violation occurs.
	ErrProtocolError = errors.New("protocol error")

	// ErrServerDisconnect is emitted when the server sends a DISCONNECT packet.
	ErrServerDisconnect = errors.New("server disconnect")

	// ErrKeepAliveTimeout is emitted when the server doesn't respond to PINGREQ.
	ErrKeepAliveTimeout = errors.New("keep-alive timeout")
)

// Sentinel errors for operations - check with errors.Is().
var (
	// ErrPublishFailed is returned when a publish operation fails.
	ErrPublishFailed = errors.New("publish failed")

	// ErrSubscribeFailed is returned when a subscribe operation fails.
	ErrSubscribeFailed = errors.New("subscribe failed")

	// ErrUnsubscribeFailed is returned when an unsubscribe operation fails.
	ErrUnsubscribeFailed = errors.New("unsubscribe failed")

	// ErrClientClosed is returned when an operation is attempted on a closed client.
	ErrClientClosed = errors.New("client closed")

	// ErrNotConnected is returned when an operation requires an active connection.
	ErrNotConnected = errors.New("not connected")

	// ErrInvalidTopic is returned when a topic is invalid.
	ErrInvalidTopic = errors.New("invalid topic")
)

// ConnectedEvent contains details about a successful connection.
// Extract with errors.As().
type ConnectedEvent struct {
	err            error
	SessionPresent bool
	ServerProps    *Properties
}

func (e *ConnectedEvent) Error() string { return e.err.Error() }
func (e *ConnectedEvent) Unwrap() error { return e.err }

// NewConnectedEvent creates a new ConnectedEvent.
func NewConnectedEvent(sessionPresent bool, props *Properties) *ConnectedEvent {
	return &ConnectedEvent{
		err:            ErrConnected,
		SessionPresent: sessionPresent,
		ServerProps:    props,
	}
}

// DisconnectError contains details about a disconnection.
// Extract with errors.As().
type DisconnectError struct {
	err        error
	ReasonCode ReasonCode
	Properties *Properties
	Remote     bool // true if server sent disconnect
}

func (e *DisconnectError) Error() string {
	if e.Remote {
		return "server disconnect: " + e.ReasonCode.String()
	}
	return "disconnected: " + e.ReasonCode.String()
}

func (e *DisconnectError) Unwrap() error { return e.err }

// NewDisconnectError creates a new DisconnectError.
func NewDisconnectError(reason ReasonCode, props *Properties, remote bool) *DisconnectError {
	baseErr := ErrDisconnected
	if remote {
		baseErr = ErrServerDisconnect
	}
	return &DisconnectError{
		err:        baseErr,
		ReasonCode: reason,
		Properties: props,
		Remote:     remote,
	}
}

// ReconnectEvent contains details about a reconnection attempt.
// Extract with errors.As().
type ReconnectEvent struct {
	err         error
	Attempt     int
	MaxAttempts int
	Delay       time.Duration
	cancelFn    func()
}

func (e *ReconnectEvent) Error() string { return e.err.Error() }
func (e *ReconnectEvent) Unwrap() error { return e.err }

// Cancel stops further reconnection attempts.
func (e *ReconnectEvent) Cancel() {
	if e.cancelFn != nil {
		e.cancelFn()
	}
}

// NewReconnectEvent creates a new ReconnectEvent.
func NewReconnectEvent(attempt, maxAttempts int, delay time.Duration, cancelFn func()) *ReconnectEvent {
	return &ReconnectEvent{
		err:         ErrReconnecting,
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		Delay:       delay,
		cancelFn:    cancelFn,
	}
}

// PublishError contains details about a failed publish operation.
// Extract with errors.As().
type PublishError struct {
	err        error
	Topic      string
	PacketID   uint16
	ReasonCode ReasonCode
}

func (e *PublishError) Error() string {
	return "publish failed: " + e.ReasonCode.String()
}

func (e *PublishError) Unwrap() error { return e.err }

// NewPublishError creates a new PublishError.
func NewPublishError(topic string, packetID uint16, reason ReasonCode) *PublishError {
	return &PublishError{
		err:        ErrPublishFailed,
		Topic:      topic,
		PacketID:   packetID,
		ReasonCode: reason,
	}
}

// SubscribeError contains details about a failed subscribe operation.
// Extract with errors.As().
type SubscribeError struct {
	err        error
	Topic      string
	ReasonCode ReasonCode
}

func (e *SubscribeError) Error() string {
	return "subscribe failed: " + e.ReasonCode.String()
}

func (e *SubscribeError) Unwrap() error { return e.err }

// NewSubscribeError creates a new SubscribeError.
func NewSubscribeError(topic string, reason ReasonCode) *SubscribeError {
	return &SubscribeError{
		err:        ErrSubscribeFailed,
		Topic:      topic,
		ReasonCode: reason,
	}
}

// ConnectionLostError contains details about an unexpected disconnection.
// Extract with errors.As().
type ConnectionLostError struct {
	err   error
	Cause error
}

func (e *ConnectionLostError) Error() string {
	if e.Cause != nil {
		return "connection lost: " + e.Cause.Error()
	}
	return "connection lost"
}

func (e *ConnectionLostError) Unwrap() error { return e.err }

// NewConnectionLostError creates a new ConnectionLostError.
func NewConnectionLostError(cause error) *ConnectionLostError {
	return &ConnectionLostError{
		err:   ErrConnectionLost,
		Cause: cause,
	}
}

// ConnectError contains details about a failed connection attempt.
// Extract with errors.As().
type ConnectError struct {
	err        error
	ReasonCode ReasonCode
	Properties *Properties
}

func (e *ConnectError) Error() string {
	return "connect failed: " + e.ReasonCode.String()
}

func (e *ConnectError) Unwrap() error { return e.err }

// NewConnectError creates a new ConnectError from a reason code.
func NewConnectError(reason ReasonCode, props *Properties) *ConnectError {
	baseErr := ErrProtocolError
	if reason == ReasonBadUserNameOrPassword || reason == ReasonNotAuthorized {
		baseErr = ErrAuthFailed
	}
	return &ConnectError{
		err:        baseErr,
		ReasonCode: reason,
		Properties: props,
	}
}
