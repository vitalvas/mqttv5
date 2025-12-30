package mqttv5

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllowAllAuthorizer(t *testing.T) {
	authz := &AllowAllAuthorizer{}
	ctx := context.Background()

	result, err := authz.Authorize(ctx, &AuthzContext{Topic: "test", Action: AuthzActionPublish})
	assert.NoError(t, err)
	assert.True(t, result.Allowed)
	assert.Equal(t, byte(2), result.MaxQoS)
}

func TestDenyAllAuthorizer(t *testing.T) {
	authz := &DenyAllAuthorizer{}
	ctx := context.Background()

	result, err := authz.Authorize(ctx, &AuthzContext{Topic: "test", Action: AuthzActionPublish})
	assert.NoError(t, err)
	assert.False(t, result.Allowed)
	assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
}

func TestAuthzAction(t *testing.T) {
	t.Run("string representation", func(t *testing.T) {
		assert.Equal(t, "publish", AuthzActionPublish.String())
		assert.Equal(t, "subscribe", AuthzActionSubscribe.String())
		assert.Equal(t, "unknown", AuthzAction(99).String())
	})
}

func TestAuthzContext(t *testing.T) {
	t.Run("full context", func(t *testing.T) {
		ctx := &AuthzContext{
			ClientID: "client-123",
			Username: "user",
			Topic:    "test/topic",
			Action:   AuthzActionPublish,
			QoS:      1,
			Retain:   true,
		}

		assert.Equal(t, "client-123", ctx.ClientID)
		assert.Equal(t, "user", ctx.Username)
		assert.Equal(t, "test/topic", ctx.Topic)
		assert.Equal(t, AuthzActionPublish, ctx.Action)
		assert.Equal(t, byte(1), ctx.QoS)
		assert.True(t, ctx.Retain)
	})
}

func TestAuthzResult(t *testing.T) {
	t.Run("allowed result", func(t *testing.T) {
		result := &AuthzResult{
			Allowed: true,
			MaxQoS:  2,
		}

		assert.True(t, result.Allowed)
		assert.Equal(t, byte(2), result.MaxQoS)
	})

	t.Run("denied result", func(t *testing.T) {
		result := &AuthzResult{
			Allowed:    false,
			ReasonCode: ReasonNotAuthorized,
		}

		assert.False(t, result.Allowed)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})
}
