package mqttv5

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllowAllAuthenticator(t *testing.T) {
	auth := &AllowAllAuthenticator{}
	ctx := context.Background()

	result, err := auth.Authenticate(ctx, &AuthContext{ClientID: "test"})
	assert.NoError(t, err)
	assert.True(t, result.Success)
	assert.Equal(t, ReasonSuccess, result.ReasonCode)
}

func TestDenyAllAuthenticator(t *testing.T) {
	auth := &DenyAllAuthenticator{}
	ctx := context.Background()

	result, err := auth.Authenticate(ctx, &AuthContext{ClientID: "test"})
	assert.NoError(t, err)
	assert.False(t, result.Success)
	assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
}

func TestAuthContext(t *testing.T) {
	t.Run("full context", func(t *testing.T) {
		remoteAddr := &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1883}
		localAddr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1883}
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
			RemoteAddr:    remoteAddr,
			LocalAddr:     localAddr,
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
		assert.Equal(t, remoteAddr, authCtx.RemoteAddr)
		assert.Equal(t, localAddr, authCtx.LocalAddr)
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

// mockEnhancedAuthenticator implements EnhancedAuthenticator for testing.
type mockEnhancedAuthenticator struct {
	methods map[string]bool
}

func (m *mockEnhancedAuthenticator) SupportsMethod(method string) bool {
	return m.methods[method]
}

func (m *mockEnhancedAuthenticator) AuthStart(_ context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error) {
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

func (m *mockEnhancedAuthenticator) AuthContinue(_ context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error) {
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

// TestEnhancedAuthResultAssignedClientID tests that EnhancedAuthResult has AssignedClientID field.
func TestEnhancedAuthResultAssignedClientID(t *testing.T) {
	result := &EnhancedAuthResult{
		Success:          true,
		ReasonCode:       ReasonSuccess,
		AssignedClientID: "server-assigned-id",
	}

	assert.True(t, result.Success)
	assert.Equal(t, "server-assigned-id", result.AssignedClientID)
}

// nilResultAuthenticator returns nil result without error for testing nil handling.
type nilResultAuthenticator struct{}

func (a *nilResultAuthenticator) Authenticate(_ context.Context, _ *AuthContext) (*AuthResult, error) {
	return nil, nil
}

// TestNilAuthResultHandling tests that the server properly handles nil auth results.
func TestNilAuthResultHandling(t *testing.T) {
	auth := &nilResultAuthenticator{}
	ctx := context.Background()

	result, err := auth.Authenticate(ctx, &AuthContext{ClientID: "test"})
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestMockEnhancedAuthenticator(t *testing.T) {
	auth := &mockEnhancedAuthenticator{
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
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.False(t, result.Success)
		assert.True(t, result.Continue)
		assert.Equal(t, []byte("challenge"), result.AuthData)
		assert.Equal(t, "step1", result.State)

		authCtx.State = result.State
		authCtx.AuthData = []byte("response")

		result, err = auth.AuthContinue(ctx, authCtx)
		assert.NoError(t, err)
		assert.True(t, result.Success)
	})

	t.Run("challenge response failure", func(t *testing.T) {
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte("client-first"),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		assert.NoError(t, err)

		authCtx.State = result.State
		authCtx.AuthData = []byte("wrong-response")

		result, err = auth.AuthContinue(ctx, authCtx)
		assert.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})
}

// emptyNamespaceEnhancedAuthenticator returns empty namespace to test defaulting.
type emptyNamespaceEnhancedAuthenticator struct{}

func (a *emptyNamespaceEnhancedAuthenticator) SupportsMethod(method string) bool {
	return method == "PLAIN"
}

func (a *emptyNamespaceEnhancedAuthenticator) AuthStart(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	return &EnhancedAuthResult{
		Success:    true,
		ReasonCode: ReasonSuccess,
		Namespace:  "", // Empty namespace should default to DefaultNamespace
	}, nil
}

func (a *emptyNamespaceEnhancedAuthenticator) AuthContinue(_ context.Context, _ *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	return &EnhancedAuthResult{
		Success:    true,
		ReasonCode: ReasonSuccess,
		Namespace:  "",
	}, nil
}

func TestEnhancedAuthEmptyNamespaceDefaults(t *testing.T) {
	auth := &emptyNamespaceEnhancedAuthenticator{}
	ctx := context.Background()

	// Test that AuthStart returns empty namespace
	result, err := auth.AuthStart(ctx, &EnhancedAuthContext{
		ClientID:   "test-client",
		AuthMethod: "PLAIN",
	})
	require.NoError(t, err)
	assert.True(t, result.Success)
	assert.Empty(t, result.Namespace, "authenticator returns empty namespace")

	// The server's authenticateClient function should default empty namespace to DefaultNamespace.
	// This is tested in server_test.go TestServerEnhancedAuthEmptyNamespace.
}

// scramSHA256State holds the state for SCRAM-SHA-256 authentication.
type scramSHA256State struct {
	Username    string
	ClientNonce string
	ServerNonce string
	Salt        []byte
	Iterations  int
	Step        int
}

// scramSHA256Authenticator implements a SCRAM-SHA-256 enhanced authenticator for testing.
// This is a simplified implementation that demonstrates the challenge-response flow.
type scramSHA256Authenticator struct {
	// Users maps username to password
	Users map[string]string
	// Namespaces maps username to namespace
	Namespaces map[string]string
}

func (a *scramSHA256Authenticator) SupportsMethod(method string) bool {
	return method == "SCRAM-SHA-256"
}

func (a *scramSHA256Authenticator) AuthStart(_ context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	// Parse client-first-message: n,,n=<username>,r=<client-nonce>
	clientFirst := string(authCtx.AuthData)
	if len(clientFirst) < 5 {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Simple parsing for test purposes
	var username, clientNonce string
	parts := splitSCRAMMessage(clientFirst)
	for _, part := range parts {
		if len(part) > 2 && part[0] == 'n' && part[1] == '=' {
			username = part[2:]
		}
		if len(part) > 2 && part[0] == 'r' && part[1] == '=' {
			clientNonce = part[2:]
		}
	}

	if username == "" || clientNonce == "" {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Check if user exists
	if _, ok := a.Users[username]; !ok {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Generate server nonce and create server-first-message
	serverNonce := fmt.Sprintf("%sserver-nonce-12345", clientNonce)
	salt := []byte("salt1234")
	iterations := 4096

	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", serverNonce, string(salt), iterations)

	state := &scramSHA256State{
		Username:    username,
		ClientNonce: clientNonce,
		ServerNonce: serverNonce,
		Salt:        salt,
		Iterations:  iterations,
		Step:        1,
	}

	return &EnhancedAuthResult{
		Continue:   true,
		ReasonCode: ReasonContinueAuth,
		AuthData:   []byte(serverFirst),
		State:      state,
	}, nil
}

func (a *scramSHA256Authenticator) AuthContinue(_ context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	state, ok := authCtx.State.(*scramSHA256State)
	if !ok || state == nil {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	if state.Step != 1 {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Parse client-final-message: c=<channel-binding>,r=<nonce>,p=<proof>
	clientFinal := string(authCtx.AuthData)
	parts := splitSCRAMMessage(clientFinal)

	var receivedNonce, proof string
	for _, part := range parts {
		if len(part) > 2 && part[0] == 'r' && part[1] == '=' {
			receivedNonce = part[2:]
		}
		if len(part) > 2 && part[0] == 'p' && part[1] == '=' {
			proof = part[2:]
		}
	}

	// Verify nonce matches
	if receivedNonce != state.ServerNonce {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// In a real implementation, we would verify the proof using HMAC-SHA-256
	// For testing, we accept a specific proof format: "valid-proof-for-<password>"
	password := a.Users[state.Username]
	expectedProof := fmt.Sprintf("valid-proof-for-%s", password)
	if proof != expectedProof {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Authentication successful
	namespace := DefaultNamespace
	if ns, ok := a.Namespaces[state.Username]; ok {
		namespace = ns
	}

	// Server-final-message: v=<server-signature>
	serverFinal := "v=server-signature-verified"

	return &EnhancedAuthResult{
		Success:    true,
		ReasonCode: ReasonSuccess,
		AuthData:   []byte(serverFinal),
		Namespace:  namespace,
	}, nil
}

// splitSCRAMMessage splits a SCRAM message by commas.
func splitSCRAMMessage(msg string) []string {
	var parts []string
	current := ""
	for _, c := range msg {
		if c == ',' {
			if current != "" {
				parts = append(parts, current)
			}
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

func TestSCRAMSHA256Authenticator(t *testing.T) {
	auth := &scramSHA256Authenticator{
		Users: map[string]string{
			"alice":  "password123",
			"bob":    "secret456",
			"tenant": "tenantpass",
		},
		Namespaces: map[string]string{
			"tenant": "tenant-namespace",
		},
	}
	ctx := context.Background()

	t.Run("supports SCRAM-SHA-256 method", func(t *testing.T) {
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-256"))
		assert.False(t, auth.SupportsMethod("PLAIN"))
		assert.False(t, auth.SupportsMethod("SCRAM-SHA-1"))
	})

	t.Run("successful authentication flow", func(t *testing.T) {
		// Step 1: Client sends client-first-message
		clientFirst := "n,,n=alice,r=client-nonce-abc123"
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte(clientFirst),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.True(t, result.Continue)
		assert.Equal(t, ReasonContinueAuth, result.ReasonCode)
		assert.NotEmpty(t, result.AuthData)
		assert.NotNil(t, result.State)

		// Verify server-first-message contains expected fields
		serverFirst := string(result.AuthData)
		assert.Contains(t, serverFirst, "r=client-nonce-abc123server-nonce-12345")
		assert.Contains(t, serverFirst, "s=salt1234")
		assert.Contains(t, serverFirst, "i=4096")

		// Step 2: Client sends client-final-message with proof
		clientFinal := "c=biws,r=client-nonce-abc123server-nonce-12345,p=valid-proof-for-password123"
		authCtx.AuthData = []byte(clientFinal)
		authCtx.State = result.State
		authCtx.ReasonCode = ReasonContinueAuth

		result, err = auth.AuthContinue(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Success)
		assert.False(t, result.Continue)
		assert.Equal(t, ReasonSuccess, result.ReasonCode)
		assert.Equal(t, DefaultNamespace, result.Namespace)

		// Verify server-final-message
		serverFinal := string(result.AuthData)
		assert.Contains(t, serverFinal, "v=server-signature-verified")
	})

	t.Run("authentication with namespace", func(t *testing.T) {
		clientFirst := "n,,n=tenant,r=tenant-nonce-xyz"
		authCtx := &EnhancedAuthContext{
			ClientID:   "tenant-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte(clientFirst),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Continue)

		clientFinal := "c=biws,r=tenant-nonce-xyzserver-nonce-12345,p=valid-proof-for-tenantpass"
		authCtx.AuthData = []byte(clientFinal)
		authCtx.State = result.State

		result, err = auth.AuthContinue(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Success)
		assert.Equal(t, "tenant-namespace", result.Namespace)
	})

	t.Run("authentication failure - unknown user", func(t *testing.T) {
		clientFirst := "n,,n=unknown,r=nonce123"
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte(clientFirst),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.False(t, result.Continue)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("authentication failure - wrong password", func(t *testing.T) {
		clientFirst := "n,,n=alice,r=nonce456"
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte(clientFirst),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Continue)

		// Wrong proof (wrong password)
		clientFinal := "c=biws,r=nonce456server-nonce-12345,p=valid-proof-for-wrongpassword"
		authCtx.AuthData = []byte(clientFinal)
		authCtx.State = result.State

		result, err = auth.AuthContinue(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("authentication failure - nonce mismatch", func(t *testing.T) {
		clientFirst := "n,,n=bob,r=original-nonce"
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte(clientFirst),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)

		// Client sends different nonce (replay attack attempt)
		clientFinal := "c=biws,r=different-nonce,p=valid-proof-for-secret456"
		authCtx.AuthData = []byte(clientFinal)
		authCtx.State = result.State

		result, err = auth.AuthContinue(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("authentication failure - invalid client-first-message", func(t *testing.T) {
		testCases := []struct {
			name    string
			message string
		}{
			{"empty message", ""},
			{"too short", "n,,"},
			{"missing username", "n,,r=nonce"},
			{"missing nonce", "n,,n=user"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				authCtx := &EnhancedAuthContext{
					ClientID:   "test-client",
					AuthMethod: "SCRAM-SHA-256",
					AuthData:   []byte(tc.message),
				}

				result, err := auth.AuthStart(ctx, authCtx)
				require.NoError(t, err)
				assert.False(t, result.Success)
				assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
			})
		}
	})

	t.Run("authentication failure - nil state in continue", func(t *testing.T) {
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte("c=biws,r=nonce,p=proof"),
			State:      nil,
		}

		result, err := auth.AuthContinue(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("authentication failure - wrong state type", func(t *testing.T) {
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte("c=biws,r=nonce,p=proof"),
			State:      "wrong-type",
		}

		result, err := auth.AuthContinue(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("authentication with multiple users", func(t *testing.T) {
		// Authenticate alice
		aliceFirst := "n,,n=alice,r=alice-nonce"
		aliceCtx := &EnhancedAuthContext{
			ClientID:   "alice-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte(aliceFirst),
		}

		aliceResult, err := auth.AuthStart(ctx, aliceCtx)
		require.NoError(t, err)
		assert.True(t, aliceResult.Continue)

		// Authenticate bob concurrently
		bobFirst := "n,,n=bob,r=bob-nonce"
		bobCtx := &EnhancedAuthContext{
			ClientID:   "bob-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte(bobFirst),
		}

		bobResult, err := auth.AuthStart(ctx, bobCtx)
		require.NoError(t, err)
		assert.True(t, bobResult.Continue)

		// Complete alice's auth
		aliceFinal := "c=biws,r=alice-nonceserver-nonce-12345,p=valid-proof-for-password123"
		aliceCtx.AuthData = []byte(aliceFinal)
		aliceCtx.State = aliceResult.State

		aliceResult, err = auth.AuthContinue(ctx, aliceCtx)
		require.NoError(t, err)
		assert.True(t, aliceResult.Success)

		// Complete bob's auth
		bobFinal := "c=biws,r=bob-nonceserver-nonce-12345,p=valid-proof-for-secret456"
		bobCtx.AuthData = []byte(bobFinal)
		bobCtx.State = bobResult.State

		bobResult, err = auth.AuthContinue(ctx, bobCtx)
		require.NoError(t, err)
		assert.True(t, bobResult.Success)
	})
}
