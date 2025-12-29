package mqttv5

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthzAction(t *testing.T) {
	t.Run("string representation", func(t *testing.T) {
		assert.Equal(t, "publish", AuthzActionPublish.String())
		assert.Equal(t, "subscribe", AuthzActionSubscribe.String())
		assert.Equal(t, "unknown", AuthzAction(99).String())
	})
}

func TestAllowAllAuthorizer(t *testing.T) {
	authz := &AllowAllAuthorizer{}
	ctx := context.Background()

	t.Run("allows publish", func(t *testing.T) {
		authzCtx := &AuthzContext{
			ClientID: "test",
			Topic:    "test/topic",
			Action:   AuthzActionPublish,
			QoS:      1,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
		assert.Equal(t, byte(2), result.MaxQoS)
	})

	t.Run("allows subscribe", func(t *testing.T) {
		authzCtx := &AuthzContext{
			ClientID: "test",
			Topic:    "test/#",
			Action:   AuthzActionSubscribe,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
	})
}

func TestDenyAllAuthorizer(t *testing.T) {
	authz := &DenyAllAuthorizer{}
	ctx := context.Background()

	t.Run("denies publish", func(t *testing.T) {
		authzCtx := &AuthzContext{
			ClientID: "test",
			Topic:    "test/topic",
			Action:   AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.False(t, result.Allowed)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("denies subscribe", func(t *testing.T) {
		authzCtx := &AuthzContext{
			ClientID: "test",
			Topic:    "test/#",
			Action:   AuthzActionSubscribe,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.False(t, result.Allowed)
	})
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern string
		s       string
		match   bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"test", "test", true},
		{"test", "other", false},
		{"test*", "test123", true},
		{"test*", "test", true},
		{"*test", "123test", true},
		{"*test*", "123test456", true},
		{"te?t", "test", true},
		{"te?t", "text", true},
		{"te?t", "teest", false},
		{"t??t", "test", true},
		{"t??t", "tt", false},
		{"", "", true},
		{"", "a", false},
		{"client-*", "client-123", true},
		{"client-*", "client-", true},
		{"user-?", "user-1", true},
		{"user-?", "user-12", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.s, func(t *testing.T) {
			result := matchPattern(tt.pattern, tt.s)
			assert.Equal(t, tt.match, result)
		})
	}
}

func TestACLEntry(t *testing.T) {
	t.Run("matches client pattern", func(t *testing.T) {
		entry := &ACLEntry{ClientPattern: "client-*"}
		authzCtx := &AuthzContext{ClientID: "client-123"}
		assert.True(t, entry.Matches(authzCtx))

		authzCtx.ClientID = "other-123"
		assert.False(t, entry.Matches(authzCtx))
	})

	t.Run("matches username pattern", func(t *testing.T) {
		entry := &ACLEntry{UsernamePattern: "admin*"}
		authzCtx := &AuthzContext{Username: "admin1"}
		assert.True(t, entry.Matches(authzCtx))

		authzCtx.Username = "user1"
		assert.False(t, entry.Matches(authzCtx))
	})

	t.Run("matches topic pattern", func(t *testing.T) {
		entry := &ACLEntry{TopicPattern: "sensors/+/temp"}
		authzCtx := &AuthzContext{Topic: "sensors/1/temp"}
		assert.True(t, entry.Matches(authzCtx))

		authzCtx.Topic = "sensors/1/humidity"
		assert.False(t, entry.Matches(authzCtx))
	})

	t.Run("matches all patterns", func(t *testing.T) {
		entry := &ACLEntry{
			ClientPattern:   "client-*",
			UsernamePattern: "user-*",
			TopicPattern:    "data/#",
		}

		authzCtx := &AuthzContext{
			ClientID: "client-123",
			Username: "user-456",
			Topic:    "data/test",
		}
		assert.True(t, entry.Matches(authzCtx))

		authzCtx.ClientID = "other"
		assert.False(t, entry.Matches(authzCtx))
	})

	t.Run("allows publish action", func(t *testing.T) {
		entry := &ACLEntry{AllowPublish: true, AllowRetain: true}
		assert.True(t, entry.AllowsAction(AuthzActionPublish, false))
		assert.True(t, entry.AllowsAction(AuthzActionPublish, true))
	})

	t.Run("allows publish but not retain", func(t *testing.T) {
		entry := &ACLEntry{AllowPublish: true, AllowRetain: false}
		assert.True(t, entry.AllowsAction(AuthzActionPublish, false))
		assert.False(t, entry.AllowsAction(AuthzActionPublish, true))
	})

	t.Run("allows subscribe action", func(t *testing.T) {
		entry := &ACLEntry{AllowSubscribe: true}
		assert.True(t, entry.AllowsAction(AuthzActionSubscribe, false))
	})

	t.Run("deny entry", func(t *testing.T) {
		entry := &ACLEntry{Deny: true, AllowPublish: true}
		assert.False(t, entry.AllowsAction(AuthzActionPublish, false))
	})

	t.Run("unknown action", func(t *testing.T) {
		entry := &ACLEntry{AllowPublish: true}
		assert.False(t, entry.AllowsAction(AuthzAction(99), false))
	})
}

func TestACLAuthorizer(t *testing.T) {
	ctx := context.Background()

	t.Run("allows matching entry", func(t *testing.T) {
		authz := &ACLAuthorizer{
			Entries: []ACLEntry{
				{
					TopicPattern: "sensors/#",
					AllowPublish: true,
					MaxQoS:       2,
				},
			},
		}

		authzCtx := &AuthzContext{
			Topic:  "sensors/temp",
			Action: AuthzActionPublish,
			QoS:    1,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
		assert.Equal(t, byte(1), result.MaxQoS)
	})

	t.Run("deny rule takes precedence", func(t *testing.T) {
		authz := &ACLAuthorizer{
			Entries: []ACLEntry{
				{
					TopicPattern:   "sensors/#",
					AllowPublish:   true,
					AllowSubscribe: true,
				},
				{
					TopicPattern: "sensors/private/#",
					Deny:         true,
				},
			},
		}

		authzCtx := &AuthzContext{
			Topic:  "sensors/private/secret",
			Action: AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.False(t, result.Allowed)
	})

	t.Run("default allow", func(t *testing.T) {
		authz := &ACLAuthorizer{
			Entries:      []ACLEntry{},
			DefaultAllow: true,
		}

		authzCtx := &AuthzContext{
			Topic:  "any/topic",
			Action: AuthzActionPublish,
			QoS:    2,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
		assert.Equal(t, byte(2), result.MaxQoS)
	})

	t.Run("default deny", func(t *testing.T) {
		authz := &ACLAuthorizer{
			Entries:      []ACLEntry{},
			DefaultAllow: false,
		}

		authzCtx := &AuthzContext{
			Topic:  "any/topic",
			Action: AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.False(t, result.Allowed)
	})

	t.Run("max QoS from highest matching entry", func(t *testing.T) {
		authz := &ACLAuthorizer{
			Entries: []ACLEntry{
				{
					TopicPattern: "data/#",
					AllowPublish: true,
					MaxQoS:       1,
				},
				{
					TopicPattern: "data/important/#",
					AllowPublish: true,
					MaxQoS:       2,
				},
			},
		}

		authzCtx := &AuthzContext{
			Topic:  "data/important/event",
			Action: AuthzActionPublish,
			QoS:    2,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
		assert.Equal(t, byte(2), result.MaxQoS)
	})
}

func TestTopicAuthorizer(t *testing.T) {
	ctx := context.Background()

	t.Run("allows matching topic pattern", func(t *testing.T) {
		authz := &TopicAuthorizer{
			Rules: map[string]TopicRules{
				"user1": {
					PublishPatterns:   []string{"users/user1/#"},
					SubscribePatterns: []string{"users/user1/#", "public/#"},
				},
			},
		}

		authzCtx := &AuthzContext{
			Username: "user1",
			Topic:    "users/user1/data",
			Action:   AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
	})

	t.Run("denies non-matching topic", func(t *testing.T) {
		authz := &TopicAuthorizer{
			Rules: map[string]TopicRules{
				"user1": {
					PublishPatterns: []string{"users/user1/#"},
				},
			},
		}

		authzCtx := &AuthzContext{
			Username: "user1",
			Topic:    "users/user2/data",
			Action:   AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.False(t, result.Allowed)
	})

	t.Run("denies unknown user", func(t *testing.T) {
		authz := &TopicAuthorizer{
			Rules: map[string]TopicRules{
				"user1": {
					PublishPatterns: []string{"users/user1/#"},
				},
			},
		}

		authzCtx := &AuthzContext{
			Username: "unknown",
			Topic:    "users/user1/data",
			Action:   AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.False(t, result.Allowed)
	})

	t.Run("expands %c for client ID", func(t *testing.T) {
		authz := &TopicAuthorizer{
			Rules: map[string]TopicRules{
				"user1": {
					PublishPatterns: []string{"clients/%c/#"},
				},
			},
		}

		authzCtx := &AuthzContext{
			ClientID: "client-123",
			Username: "user1",
			Topic:    "clients/client-123/status",
			Action:   AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)

		authzCtx.Topic = "clients/other-client/status"
		result, err = authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.False(t, result.Allowed)
	})

	t.Run("expands %u for username", func(t *testing.T) {
		authz := &TopicAuthorizer{
			Rules: map[string]TopicRules{
				"admin": {
					SubscribePatterns: []string{"users/%u/#"},
				},
			},
		}

		authzCtx := &AuthzContext{
			Username: "admin",
			Topic:    "users/admin/notifications",
			Action:   AuthzActionSubscribe,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
	})
}

func TestChainAuthorizer(t *testing.T) {
	ctx := context.Background()

	t.Run("empty chain allows", func(t *testing.T) {
		authz := &ChainAuthorizer{}

		authzCtx := &AuthzContext{
			Topic:  "any",
			Action: AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
	})

	t.Run("OR logic - any allows", func(t *testing.T) {
		authz := &ChainAuthorizer{
			Authorizers: []Authorizer{
				&DenyAllAuthorizer{},
				&AllowAllAuthorizer{},
			},
			RequireAll: false,
		}

		authzCtx := &AuthzContext{
			Topic:  "test",
			Action: AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
	})

	t.Run("OR logic - none allows", func(t *testing.T) {
		authz := &ChainAuthorizer{
			Authorizers: []Authorizer{
				&DenyAllAuthorizer{},
				&DenyAllAuthorizer{},
			},
			RequireAll: false,
		}

		authzCtx := &AuthzContext{
			Topic:  "test",
			Action: AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.False(t, result.Allowed)
	})

	t.Run("AND logic - all allow", func(t *testing.T) {
		authz := &ChainAuthorizer{
			Authorizers: []Authorizer{
				&AllowAllAuthorizer{},
				&AllowAllAuthorizer{},
			},
			RequireAll: true,
		}

		authzCtx := &AuthzContext{
			Topic:  "test",
			Action: AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
	})

	t.Run("AND logic - one denies", func(t *testing.T) {
		authz := &ChainAuthorizer{
			Authorizers: []Authorizer{
				&AllowAllAuthorizer{},
				&DenyAllAuthorizer{},
			},
			RequireAll: true,
		}

		authzCtx := &AuthzContext{
			Topic:  "test",
			Action: AuthzActionPublish,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.False(t, result.Allowed)
	})

	t.Run("propagates minimum QoS", func(t *testing.T) {
		authz := &ChainAuthorizer{
			Authorizers: []Authorizer{
				&ACLAuthorizer{
					Entries: []ACLEntry{{AllowPublish: true, MaxQoS: 2}},
				},
				&ACLAuthorizer{
					Entries: []ACLEntry{{AllowPublish: true, MaxQoS: 1}},
				},
			},
			RequireAll: true,
		}

		authzCtx := &AuthzContext{
			Topic:  "test",
			Action: AuthzActionPublish,
			QoS:    2,
		}

		result, err := authz.Authorize(ctx, authzCtx)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
		assert.Equal(t, byte(1), result.MaxQoS)
	})
}

func BenchmarkAllowAllAuthorizer(b *testing.B) {
	authz := &AllowAllAuthorizer{}
	ctx := context.Background()
	authzCtx := &AuthzContext{
		ClientID: "client",
		Topic:    "test/topic",
		Action:   AuthzActionPublish,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = authz.Authorize(ctx, authzCtx)
	}
}

func BenchmarkACLAuthorizer(b *testing.B) {
	authz := &ACLAuthorizer{
		Entries: []ACLEntry{
			{TopicPattern: "public/#", AllowPublish: true, AllowSubscribe: true, MaxQoS: 2},
			{TopicPattern: "users/+/data", AllowPublish: true, MaxQoS: 1},
			{TopicPattern: "admin/#", Deny: true},
		},
	}
	ctx := context.Background()
	authzCtx := &AuthzContext{
		ClientID: "client",
		Topic:    "users/123/data",
		Action:   AuthzActionPublish,
		QoS:      1,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = authz.Authorize(ctx, authzCtx)
	}
}

func BenchmarkMatchPattern(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		matchPattern("client-*-device-?", "client-123-device-1")
	}
}
