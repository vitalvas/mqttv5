package mqttv5

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllowAllAuthenticator(t *testing.T) {
	auth := &AllowAllAuthenticator{}
	ctx := context.Background()

	t.Run("allows any connection", func(t *testing.T) {
		authCtx := &AuthContext{
			ClientID: "test-client",
			Username: "user",
			Password: []byte("pass"),
		}

		result, err := auth.Authenticate(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Success)
		assert.Equal(t, ReasonSuccess, result.ReasonCode)
	})

	t.Run("allows empty credentials", func(t *testing.T) {
		authCtx := &AuthContext{
			ClientID: "test-client",
		}

		result, err := auth.Authenticate(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Success)
	})
}

func TestDenyAllAuthenticator(t *testing.T) {
	auth := &DenyAllAuthenticator{}
	ctx := context.Background()

	t.Run("denies any connection", func(t *testing.T) {
		authCtx := &AuthContext{
			ClientID: "test-client",
			Username: "user",
			Password: []byte("pass"),
		}

		result, err := auth.Authenticate(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("denies empty credentials", func(t *testing.T) {
		authCtx := &AuthContext{
			ClientID: "test-client",
		}

		result, err := auth.Authenticate(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
	})
}

func TestUsernamePasswordAuthenticator(t *testing.T) {
	ctx := context.Background()

	t.Run("valid credentials", func(t *testing.T) {
		auth := &UsernamePasswordAuthenticator{
			ValidateFunc: func(username string, password []byte) bool {
				return username == "admin" && string(password) == "secret"
			},
		}

		authCtx := &AuthContext{
			ClientID: "test-client",
			Username: "admin",
			Password: []byte("secret"),
		}

		result, err := auth.Authenticate(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Success)
		assert.Equal(t, ReasonSuccess, result.ReasonCode)
	})

	t.Run("invalid username", func(t *testing.T) {
		auth := &UsernamePasswordAuthenticator{
			ValidateFunc: func(username string, password []byte) bool {
				return username == "admin" && string(password) == "secret"
			},
		}

		authCtx := &AuthContext{
			ClientID: "test-client",
			Username: "wrong",
			Password: []byte("secret"),
		}

		result, err := auth.Authenticate(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonBadUserNameOrPassword, result.ReasonCode)
	})

	t.Run("invalid password", func(t *testing.T) {
		auth := &UsernamePasswordAuthenticator{
			ValidateFunc: func(username string, password []byte) bool {
				return username == "admin" && string(password) == "secret"
			},
		}

		authCtx := &AuthContext{
			ClientID: "test-client",
			Username: "admin",
			Password: []byte("wrong"),
		}

		result, err := auth.Authenticate(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonBadUserNameOrPassword, result.ReasonCode)
	})

	t.Run("nil validate func", func(t *testing.T) {
		auth := &UsernamePasswordAuthenticator{}

		authCtx := &AuthContext{
			ClientID: "test-client",
			Username: "admin",
			Password: []byte("secret"),
		}

		result, err := auth.Authenticate(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})
}

func TestAuthContext(t *testing.T) {
	t.Run("full context", func(t *testing.T) {
		addr := &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1883}
		connectPkt := &ConnectPacket{
			ClientID:   "test-client",
			Username:   "user",
			Password:   []byte("pass"),
			CleanStart: true,
		}

		authCtx := &AuthContext{
			ClientID:      "test-client",
			Username:      "user",
			Password:      []byte("pass"),
			RemoteAddr:    addr,
			TLSCommonName: "client.example.com",
			TLSVerified:   true,
			ConnectPacket: connectPkt,
			AuthMethod:    "SCRAM-SHA-256",
			AuthData:      []byte("auth-data"),
			CleanStart:    true,
		}

		assert.Equal(t, "test-client", authCtx.ClientID)
		assert.Equal(t, "user", authCtx.Username)
		assert.Equal(t, []byte("pass"), authCtx.Password)
		assert.Equal(t, addr, authCtx.RemoteAddr)
		assert.Equal(t, "client.example.com", authCtx.TLSCommonName)
		assert.True(t, authCtx.TLSVerified)
		assert.Equal(t, connectPkt, authCtx.ConnectPacket)
		assert.Equal(t, "SCRAM-SHA-256", authCtx.AuthMethod)
		assert.Equal(t, []byte("auth-data"), authCtx.AuthData)
		assert.True(t, authCtx.CleanStart)
	})
}

func TestAuthResult(t *testing.T) {
	t.Run("success result", func(t *testing.T) {
		result := &AuthResult{
			Success:          true,
			ReasonCode:       ReasonSuccess,
			AssignedClientID: "assigned-id",
			SessionPresent:   true,
		}

		assert.True(t, result.Success)
		assert.Equal(t, ReasonSuccess, result.ReasonCode)
		assert.Equal(t, "assigned-id", result.AssignedClientID)
		assert.True(t, result.SessionPresent)
	})

	t.Run("continue auth result", func(t *testing.T) {
		result := &AuthResult{
			Success:      false,
			ContinueAuth: true,
			AuthData:     []byte("challenge"),
		}

		assert.False(t, result.Success)
		assert.True(t, result.ContinueAuth)
		assert.Equal(t, []byte("challenge"), result.AuthData)
	})
}

func TestEnhancedAuthContext(t *testing.T) {
	t.Run("full enhanced context", func(t *testing.T) {
		addr := &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1883}

		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte("client-first"),
			ReasonCode: ReasonContinueAuth,
			RemoteAddr: addr,
			State:      map[string]string{"nonce": "abc123"},
		}

		assert.Equal(t, "test-client", authCtx.ClientID)
		assert.Equal(t, "SCRAM-SHA-256", authCtx.AuthMethod)
		assert.Equal(t, []byte("client-first"), authCtx.AuthData)
		assert.Equal(t, ReasonContinueAuth, authCtx.ReasonCode)
		assert.Equal(t, addr, authCtx.RemoteAddr)
		assert.NotNil(t, authCtx.State)
	})
}

func TestEnhancedAuthResult(t *testing.T) {
	t.Run("continue result", func(t *testing.T) {
		result := &EnhancedAuthResult{
			Success:    false,
			Continue:   true,
			ReasonCode: ReasonContinueAuth,
			AuthData:   []byte("server-challenge"),
			State:      map[string]string{"step": "2"},
		}

		assert.False(t, result.Success)
		assert.True(t, result.Continue)
		assert.Equal(t, ReasonContinueAuth, result.ReasonCode)
		assert.Equal(t, []byte("server-challenge"), result.AuthData)
		assert.NotNil(t, result.State)
	})

	t.Run("success result", func(t *testing.T) {
		result := &EnhancedAuthResult{
			Success:    true,
			Continue:   false,
			ReasonCode: ReasonSuccess,
		}

		assert.True(t, result.Success)
		assert.False(t, result.Continue)
		assert.Equal(t, ReasonSuccess, result.ReasonCode)
	})
}

// MockEnhancedAuthenticator implements EnhancedAuthenticator for testing.
type MockEnhancedAuthenticator struct {
	methods map[string]bool
}

func (m *MockEnhancedAuthenticator) SupportsMethod(method string) bool {
	return m.methods[method]
}

func (m *MockEnhancedAuthenticator) AuthStart(_ context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	if authCtx.AuthMethod == "PLAIN" {
		return &EnhancedAuthResult{
			Success:    true,
			ReasonCode: ReasonSuccess,
		}, nil
	}

	return &EnhancedAuthResult{
		Continue:   true,
		ReasonCode: ReasonContinueAuth,
		AuthData:   []byte("challenge"),
		State:      "step1",
	}, nil
}

func (m *MockEnhancedAuthenticator) AuthContinue(_ context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	if authCtx.State == "step1" && string(authCtx.AuthData) == "response" {
		return &EnhancedAuthResult{
			Success:    true,
			ReasonCode: ReasonSuccess,
		}, nil
	}

	return &EnhancedAuthResult{
		Success:    false,
		ReasonCode: ReasonNotAuthorized,
	}, nil
}

func TestMockEnhancedAuthenticator(t *testing.T) {
	auth := &MockEnhancedAuthenticator{
		methods: map[string]bool{
			"SCRAM-SHA-256": true,
			"PLAIN":         true,
		},
	}
	ctx := context.Background()

	t.Run("supports method", func(t *testing.T) {
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-256"))
		assert.True(t, auth.SupportsMethod("PLAIN"))
		assert.False(t, auth.SupportsMethod("UNKNOWN"))
	})

	t.Run("plain auth immediate success", func(t *testing.T) {
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "PLAIN",
			AuthData:   []byte("user\x00pass"),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Success)
		assert.False(t, result.Continue)
	})

	t.Run("challenge response flow", func(t *testing.T) {
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte("client-first"),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.True(t, result.Continue)
		assert.Equal(t, []byte("challenge"), result.AuthData)
		assert.Equal(t, "step1", result.State)

		authCtx.State = result.State
		authCtx.AuthData = []byte("response")

		result, err = auth.AuthContinue(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Success)
	})

	t.Run("challenge response failure", func(t *testing.T) {
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte("client-first"),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)

		authCtx.State = result.State
		authCtx.AuthData = []byte("wrong-response")

		result, err = auth.AuthContinue(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})
}

func BenchmarkAllowAllAuthenticator(b *testing.B) {
	auth := &AllowAllAuthenticator{}
	ctx := context.Background()
	authCtx := &AuthContext{
		ClientID: "test-client",
		Username: "user",
		Password: []byte("pass"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = auth.Authenticate(ctx, authCtx)
	}
}

func BenchmarkDenyAllAuthenticator(b *testing.B) {
	auth := &DenyAllAuthenticator{}
	ctx := context.Background()
	authCtx := &AuthContext{
		ClientID: "test-client",
		Username: "user",
		Password: []byte("pass"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = auth.Authenticate(ctx, authCtx)
	}
}

func BenchmarkUsernamePasswordAuthenticator(b *testing.B) {
	auth := &UsernamePasswordAuthenticator{
		ValidateFunc: func(username string, password []byte) bool {
			return username == "admin" && string(password) == "secret"
		},
	}
	ctx := context.Background()
	authCtx := &AuthContext{
		ClientID: "test-client",
		Username: "admin",
		Password: []byte("secret"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, _ = auth.Authenticate(ctx, authCtx)
	}
}
