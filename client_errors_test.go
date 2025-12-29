package mqttv5

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSentinelErrors(t *testing.T) {
	t.Run("lifecycle events are distinct", func(t *testing.T) {
		assert.NotEqual(t, ErrConnected, ErrDisconnected)
		assert.NotEqual(t, ErrConnectionLost, ErrReconnecting)
		assert.NotEqual(t, ErrReconnecting, ErrReconnectFailed)
	})

	t.Run("auth errors are distinct", func(t *testing.T) {
		assert.NotEqual(t, ErrAuthFailed, ErrNotAuthorized)
	})

	t.Run("operation errors are distinct", func(t *testing.T) {
		assert.NotEqual(t, ErrPublishFailed, ErrSubscribeFailed)
		assert.NotEqual(t, ErrClientClosed, ErrNotConnected)
	})
}

func TestConnectedEvent(t *testing.T) {
	t.Run("errors.Is matches ErrConnected", func(t *testing.T) {
		event := NewConnectedEvent(true, nil)
		assert.True(t, errors.Is(event, ErrConnected))
		assert.False(t, errors.Is(event, ErrDisconnected))
	})

	t.Run("errors.As extracts event details", func(t *testing.T) {
		props := &Properties{}
		event := NewConnectedEvent(true, props)

		var ce *ConnectedEvent
		assert.True(t, errors.As(event, &ce))
		assert.True(t, ce.SessionPresent)
		assert.Equal(t, props, ce.ServerProps)
	})

	t.Run("Error returns string", func(t *testing.T) {
		event := NewConnectedEvent(false, nil)
		assert.Equal(t, "connected", event.Error())
	})
}

func TestDisconnectError(t *testing.T) {
	t.Run("remote disconnect matches ErrServerDisconnect", func(t *testing.T) {
		event := NewDisconnectError(ReasonSuccess, nil, true)
		assert.True(t, errors.Is(event, ErrServerDisconnect))
		assert.False(t, errors.Is(event, ErrDisconnected))
	})

	t.Run("local disconnect matches ErrDisconnected", func(t *testing.T) {
		event := NewDisconnectError(ReasonSuccess, nil, false)
		assert.True(t, errors.Is(event, ErrDisconnected))
		assert.False(t, errors.Is(event, ErrServerDisconnect))
	})

	t.Run("errors.As extracts error details", func(t *testing.T) {
		props := &Properties{}
		event := NewDisconnectError(ReasonQuotaExceeded, props, true)

		var de *DisconnectError
		assert.True(t, errors.As(event, &de))
		assert.Equal(t, ReasonQuotaExceeded, de.ReasonCode)
		assert.Equal(t, props, de.Properties)
		assert.True(t, de.Remote)
	})

	t.Run("Error returns descriptive string", func(t *testing.T) {
		remote := NewDisconnectError(ReasonQuotaExceeded, nil, true)
		assert.Contains(t, remote.Error(), "server disconnect")

		local := NewDisconnectError(ReasonSuccess, nil, false)
		assert.Contains(t, local.Error(), "disconnected")
	})
}

func TestReconnectEvent(t *testing.T) {
	t.Run("errors.Is matches ErrReconnecting", func(t *testing.T) {
		event := NewReconnectEvent(1, 10, time.Second, nil)
		assert.True(t, errors.Is(event, ErrReconnecting))
	})

	t.Run("errors.As extracts event details", func(t *testing.T) {
		event := NewReconnectEvent(3, 10, 5*time.Second, nil)

		var re *ReconnectEvent
		assert.True(t, errors.As(event, &re))
		assert.Equal(t, 3, re.Attempt)
		assert.Equal(t, 10, re.MaxAttempts)
		assert.Equal(t, 5*time.Second, re.Delay)
	})

	t.Run("Cancel invokes cancel function", func(t *testing.T) {
		cancelled := false
		event := NewReconnectEvent(1, 10, time.Second, func() {
			cancelled = true
		})

		event.Cancel()
		assert.True(t, cancelled)
	})

	t.Run("Cancel with nil function does not panic", func(t *testing.T) {
		event := NewReconnectEvent(1, 10, time.Second, nil)
		assert.NotPanics(t, func() {
			event.Cancel()
		})
	})
}

func TestPublishError(t *testing.T) {
	t.Run("errors.Is matches ErrPublishFailed", func(t *testing.T) {
		err := NewPublishError("test/topic", 123, ReasonQuotaExceeded)
		assert.True(t, errors.Is(err, ErrPublishFailed))
	})

	t.Run("errors.As extracts error details", func(t *testing.T) {
		err := NewPublishError("test/topic", 123, ReasonQuotaExceeded)

		var pe *PublishError
		assert.True(t, errors.As(err, &pe))
		assert.Equal(t, "test/topic", pe.Topic)
		assert.Equal(t, uint16(123), pe.PacketID)
		assert.Equal(t, ReasonQuotaExceeded, pe.ReasonCode)
	})

	t.Run("Error returns descriptive string", func(t *testing.T) {
		err := NewPublishError("test/topic", 123, ReasonQuotaExceeded)
		assert.Contains(t, err.Error(), "publish failed")
	})
}

func TestSubscribeError(t *testing.T) {
	t.Run("errors.Is matches ErrSubscribeFailed", func(t *testing.T) {
		err := NewSubscribeError("test/#", ReasonNotAuthorized)
		assert.True(t, errors.Is(err, ErrSubscribeFailed))
	})

	t.Run("errors.As extracts error details", func(t *testing.T) {
		err := NewSubscribeError("test/#", ReasonNotAuthorized)

		var se *SubscribeError
		assert.True(t, errors.As(err, &se))
		assert.Equal(t, "test/#", se.Topic)
		assert.Equal(t, ReasonNotAuthorized, se.ReasonCode)
	})
}

func TestConnectionLostError(t *testing.T) {
	t.Run("errors.Is matches ErrConnectionLost", func(t *testing.T) {
		err := NewConnectionLostError(nil)
		assert.True(t, errors.Is(err, ErrConnectionLost))
	})

	t.Run("errors.As extracts cause", func(t *testing.T) {
		cause := errors.New("network error")
		err := NewConnectionLostError(cause)

		var cle *ConnectionLostError
		assert.True(t, errors.As(err, &cle))
		assert.Equal(t, cause, cle.Cause)
	})

	t.Run("Error includes cause", func(t *testing.T) {
		cause := errors.New("connection reset")
		err := NewConnectionLostError(cause)
		assert.Contains(t, err.Error(), "connection reset")
	})

	t.Run("Error without cause", func(t *testing.T) {
		err := NewConnectionLostError(nil)
		assert.Equal(t, "connection lost", err.Error())
	})
}

func TestConnectError(t *testing.T) {
	t.Run("auth failure matches ErrAuthFailed", func(t *testing.T) {
		err := NewConnectError(ReasonBadUserNameOrPassword, nil)
		assert.True(t, errors.Is(err, ErrAuthFailed))

		err = NewConnectError(ReasonNotAuthorized, nil)
		assert.True(t, errors.Is(err, ErrAuthFailed))
	})

	t.Run("other failures match ErrProtocolError", func(t *testing.T) {
		err := NewConnectError(ReasonServerBusy, nil)
		assert.True(t, errors.Is(err, ErrProtocolError))
	})

	t.Run("errors.As extracts error details", func(t *testing.T) {
		props := &Properties{}
		err := NewConnectError(ReasonServerBusy, props)

		var ce *ConnectError
		assert.True(t, errors.As(err, &ce))
		assert.Equal(t, ReasonServerBusy, ce.ReasonCode)
		assert.Equal(t, props, ce.Properties)
	})

	t.Run("Error returns descriptive string", func(t *testing.T) {
		err := NewConnectError(ReasonServerBusy, nil)
		assert.Contains(t, err.Error(), "connect failed")
	})
}
