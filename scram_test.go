package mqttv5

import (
	"context"
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // Required for SCRAM-SHA-1 testing
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/pbkdf2"
)

func TestSCRAMHashString(t *testing.T) {
	tests := []struct {
		hash     SCRAMHash
		expected string
	}{
		{SCRAMHashSHA1, "SCRAM-SHA-1"},
		{SCRAMHashSHA256, "SCRAM-SHA-256"},
		{SCRAMHashSHA512, "SCRAM-SHA-512"},
		{SCRAMHash(99), "SCRAM-SHA-256"}, // default
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.hash.String())
	}
}

func TestSCRAMHashKeySize(t *testing.T) {
	assert.Equal(t, 20, SCRAMHashSHA1.keySize())
	assert.Equal(t, 32, SCRAMHashSHA256.keySize())
	assert.Equal(t, 64, SCRAMHashSHA512.keySize())
	assert.Equal(t, 32, SCRAMHash(99).keySize()) // default
}

func TestComputeSCRAMCredentialsAllHashes(t *testing.T) {
	password := "test-password"
	salt := []byte("test-salt-1234")
	iterations := 4096

	tests := []struct {
		name     string
		hashType SCRAMHash
		hashFunc func() hash.Hash
		keySize  int
	}{
		{"SHA-1", SCRAMHashSHA1, sha1.New, 20},
		{"SHA-256", SCRAMHashSHA256, sha256.New, 32},
		{"SHA-512", SCRAMHashSHA512, sha512.New, 64},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			creds := ComputeSCRAMCredentials(tc.hashType, password, salt, iterations)

			assert.Equal(t, tc.hashType, creds.Hash)
			assert.Equal(t, salt, creds.Salt)
			assert.Equal(t, iterations, creds.Iterations)
			assert.Len(t, creds.StoredKey, tc.keySize)
			assert.Len(t, creds.ServerKey, tc.keySize)

			// Verify the computation is correct
			saltedPassword := pbkdf2.Key([]byte(password), salt, iterations, tc.keySize, tc.hashFunc)

			clientKeyHMAC := hmac.New(tc.hashFunc, saltedPassword)
			clientKeyHMAC.Write([]byte("Client Key"))
			clientKey := clientKeyHMAC.Sum(nil)

			h := tc.hashFunc()
			h.Write(clientKey)
			expectedStoredKey := h.Sum(nil)

			assert.Equal(t, expectedStoredKey, creds.StoredKey)

			serverKeyHMAC := hmac.New(tc.hashFunc, saltedPassword)
			serverKeyHMAC.Write([]byte("Server Key"))
			expectedServerKey := serverKeyHMAC.Sum(nil)

			assert.Equal(t, expectedServerKey, creds.ServerKey)
		})
	}
}

func TestComputeSCRAMCredentials(t *testing.T) {
	creds := ComputeSCRAMCredentials(SCRAMHashSHA256, "password", []byte("salt"), 4096)
	assert.Equal(t, SCRAMHashSHA256, creds.Hash)
	assert.Len(t, creds.StoredKey, 32)
}

func TestGenerateSalt(t *testing.T) {
	salt1, err := GenerateSalt()
	require.NoError(t, err)
	assert.Len(t, salt1, 16)

	salt2, err := GenerateSalt()
	require.NoError(t, err)
	assert.Len(t, salt2, 16)

	// Salts should be unique
	assert.NotEqual(t, salt1, salt2)
}

func TestSCRAMAuthenticatorConstructors(t *testing.T) {
	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, _ string) (*SCRAMCredentials, error) {
		return nil, nil
	})

	t.Run("default", func(t *testing.T) {
		auth := NewSCRAMAuthenticator(lookup)
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-256"))
		assert.False(t, auth.SupportsMethod("SCRAM-SHA-1"))
		assert.False(t, auth.SupportsMethod("SCRAM-SHA-512"))
	})

	t.Run("SHA-1", func(t *testing.T) {
		auth := NewSCRAMAuthenticator(lookup, SCRAMHashSHA1)
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-1"))
		assert.False(t, auth.SupportsMethod("SCRAM-SHA-256"))
	})

	t.Run("SHA-256", func(t *testing.T) {
		auth := NewSCRAMAuthenticator(lookup, SCRAMHashSHA256)
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-256"))
		assert.False(t, auth.SupportsMethod("SCRAM-SHA-1"))
	})

	t.Run("SHA-512", func(t *testing.T) {
		auth := NewSCRAMAuthenticator(lookup, SCRAMHashSHA512)
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-512"))
		assert.False(t, auth.SupportsMethod("SCRAM-SHA-256"))
	})

	t.Run("multi", func(t *testing.T) {
		auth := NewSCRAMAuthenticator(lookup, SCRAMHashSHA1, SCRAMHashSHA256, SCRAMHashSHA512)
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-1"))
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-256"))
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-512"))
		assert.False(t, auth.SupportsMethod("PLAIN"))
	})

	t.Run("custom", func(t *testing.T) {
		auth := NewSCRAMAuthenticator(lookup, SCRAMHashSHA1, SCRAMHashSHA512)
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-1"))
		assert.False(t, auth.SupportsMethod("SCRAM-SHA-256"))
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-512"))
	})

	t.Run("empty defaults to SHA-256", func(t *testing.T) {
		auth := NewSCRAMAuthenticator(lookup)
		assert.True(t, auth.SupportsMethod("SCRAM-SHA-256"))
	})
}

func testSCRAMAuthFlow(t *testing.T, hashType SCRAMHash, password string, salt []byte, iterations int) {
	creds := ComputeSCRAMCredentials(hashType, password, salt, iterations)
	creds.Namespace = "test-namespace"

	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, username string) (*SCRAMCredentials, error) {
		if username == "testuser" {
			return creds, nil
		}
		return nil, nil
	})

	auth := NewSCRAMAuthenticator(lookup, hashType)
	ctx := context.Background()
	authMethod := hashType.String()
	hashFunc := hashType.hashFunc()

	clientNonce := "rOprNGfwEbeRWgbNEkqO"

	// Step 1: Client sends client-first-message
	clientFirst := fmt.Sprintf("n,,n=testuser,r=%s", clientNonce)
	authCtx := &EnhancedAuthContext{
		ClientID:   "test-client",
		AuthMethod: authMethod,
		AuthData:   []byte(clientFirst),
	}

	result, err := auth.AuthStart(ctx, authCtx)
	require.NoError(t, err)
	assert.False(t, result.Success)
	assert.True(t, result.Continue)
	assert.Equal(t, ReasonContinueAuth, result.ReasonCode)
	assert.NotNil(t, result.State)

	// Parse server-first-message
	serverFirst := string(result.AuthData)
	assert.Contains(t, serverFirst, "r="+clientNonce)
	assert.Contains(t, serverFirst, "s=")
	assert.Contains(t, serverFirst, "i=")

	// Extract server nonce
	var serverNonce string
	for _, part := range splitByComma(serverFirst) {
		if len(part) > 2 && part[:2] == "r=" {
			serverNonce = part[2:]
		}
	}

	// Step 2: Compute client proof
	clientFinalWithoutProof := fmt.Sprintf("c=biws,r=%s", serverNonce)
	clientFirstBare := fmt.Sprintf("n=testuser,r=%s", clientNonce)
	authMessage := fmt.Sprintf("%s,%s,%s", clientFirstBare, serverFirst, clientFinalWithoutProof)

	saltedPassword := pbkdf2.Key([]byte(password), salt, iterations, hashType.keySize(), hashFunc)

	clientKeyHMAC := hmac.New(hashFunc, saltedPassword)
	clientKeyHMAC.Write([]byte("Client Key"))
	clientKey := clientKeyHMAC.Sum(nil)

	h := hashFunc()
	h.Write(clientKey)
	storedKey := h.Sum(nil)

	clientSigHMAC := hmac.New(hashFunc, storedKey)
	clientSigHMAC.Write([]byte(authMessage))
	clientSignature := clientSigHMAC.Sum(nil)

	clientProof := make([]byte, len(clientKey))
	for i := range clientKey {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	clientFinal := fmt.Sprintf("%s,p=%s", clientFinalWithoutProof, base64.StdEncoding.EncodeToString(clientProof))

	authCtx.AuthData = []byte(clientFinal)
	authCtx.State = result.State
	authCtx.ReasonCode = ReasonContinueAuth

	result, err = auth.AuthContinue(ctx, authCtx)
	require.NoError(t, err)
	assert.True(t, result.Success)
	assert.False(t, result.Continue)
	assert.Equal(t, ReasonSuccess, result.ReasonCode)
	assert.Equal(t, "test-namespace", result.Namespace)

	// Verify server signature is present
	serverFinal := string(result.AuthData)
	assert.Contains(t, serverFinal, "v=")
}

func TestSCRAMAuthenticatorAllHashes(t *testing.T) {
	password := "secret123"
	salt := []byte("user-salt-abcd")
	iterations := 4096

	t.Run("SCRAM-SHA-1", func(t *testing.T) {
		testSCRAMAuthFlow(t, SCRAMHashSHA1, password, salt, iterations)
	})

	t.Run("SCRAM-SHA-256", func(t *testing.T) {
		testSCRAMAuthFlow(t, SCRAMHashSHA256, password, salt, iterations)
	})

	t.Run("SCRAM-SHA-512", func(t *testing.T) {
		testSCRAMAuthFlow(t, SCRAMHashSHA512, password, salt, iterations)
	})
}

func TestSCRAMAuthenticatorFailures(t *testing.T) {
	password := "secret123"
	salt := []byte("user-salt-abcd")
	iterations := 4096

	creds := ComputeSCRAMCredentials(SCRAMHashSHA256, password, salt, iterations)

	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, username string) (*SCRAMCredentials, error) {
		if username == "testuser" {
			return creds, nil
		}
		return nil, nil
	})

	auth := NewSCRAMAuthenticator(lookup)
	ctx := context.Background()

	t.Run("unknown user", func(t *testing.T) {
		clientFirst := "n,,n=unknownuser,r=nonce123"
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte(clientFirst),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("invalid client-first-message", func(t *testing.T) {
		testCases := []struct {
			name    string
			message string
		}{
			{"missing username", "n,,r=nonce"},
			{"missing nonce", "n,,n=user"},
			{"empty", ""},
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

	t.Run("wrong password - invalid proof", func(t *testing.T) {
		clientNonce := "wrongpassnonce"
		clientFirst := fmt.Sprintf("n,,n=testuser,r=%s", clientNonce)

		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte(clientFirst),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Continue)

		serverFirst := string(result.AuthData)
		var serverNonce string
		for _, part := range splitByComma(serverFirst) {
			if len(part) > 2 && part[:2] == "r=" {
				serverNonce = part[2:]
			}
		}

		// Send invalid proof
		clientFinal := fmt.Sprintf("c=biws,r=%s,p=%s", serverNonce, base64.StdEncoding.EncodeToString([]byte("invalid-proof-data-here!")))

		authCtx.AuthData = []byte(clientFinal)
		authCtx.State = result.State

		result, err = auth.AuthContinue(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("nonce mismatch", func(t *testing.T) {
		clientFirst := "n,,n=testuser,r=originalnonce"

		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte(clientFirst),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)

		// Send different nonce
		clientFinal := "c=biws,r=differentnonce,p=someproof"

		authCtx.AuthData = []byte(clientFinal)
		authCtx.State = result.State

		result, err = auth.AuthContinue(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("nil state in continue", func(t *testing.T) {
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

	t.Run("hash mismatch", func(t *testing.T) {
		// User has SHA-256 credentials but tries SHA-512
		sha512Auth := NewSCRAMAuthenticator(lookup, SCRAMHashSHA512)

		clientFirst := "n,,n=testuser,r=nonce"
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-512",
			AuthData:   []byte(clientFirst),
		}

		result, err := sha512Auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})

	t.Run("unsupported method", func(t *testing.T) {
		clientFirst := "n,,n=testuser,r=nonce"
		authCtx := &EnhancedAuthContext{
			ClientID:   "test-client",
			AuthMethod: "SCRAM-SHA-1", // Not supported by this authenticator
			AuthData:   []byte(clientFirst),
		}

		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})
}

func TestSCRAMDefaultNamespace(t *testing.T) {
	creds := ComputeSCRAMCredentials(SCRAMHashSHA256, "pass", []byte("salt"), 4096)
	// Namespace is empty

	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, username string) (*SCRAMCredentials, error) {
		if username == "user" {
			return creds, nil
		}
		return nil, nil
	})

	auth := NewSCRAMAuthenticator(lookup)
	ctx := context.Background()

	clientNonce := "testnonce"
	clientFirst := fmt.Sprintf("n,,n=user,r=%s", clientNonce)

	authCtx := &EnhancedAuthContext{
		ClientID:   "test-client",
		AuthMethod: "SCRAM-SHA-256",
		AuthData:   []byte(clientFirst),
	}

	result, err := auth.AuthStart(ctx, authCtx)
	require.NoError(t, err)

	serverFirst := string(result.AuthData)
	var serverNonce string
	for _, part := range splitByComma(serverFirst) {
		if len(part) > 2 && part[:2] == "r=" {
			serverNonce = part[2:]
		}
	}

	// Compute valid proof
	clientFinalWithoutProof := fmt.Sprintf("c=biws,r=%s", serverNonce)
	clientFirstBare := fmt.Sprintf("n=user,r=%s", clientNonce)
	authMessage := fmt.Sprintf("%s,%s,%s", clientFirstBare, serverFirst, clientFinalWithoutProof)

	saltedPassword := pbkdf2.Key([]byte("pass"), []byte("salt"), 4096, 32, sha256.New)

	clientKeyHMAC := hmac.New(sha256.New, saltedPassword)
	clientKeyHMAC.Write([]byte("Client Key"))
	clientKey := clientKeyHMAC.Sum(nil)

	storedKey := sha256.Sum256(clientKey)

	clientSigHMAC := hmac.New(sha256.New, storedKey[:])
	clientSigHMAC.Write([]byte(authMessage))
	clientSignature := clientSigHMAC.Sum(nil)

	clientProof := make([]byte, len(clientKey))
	for i := range clientKey {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	clientFinal := fmt.Sprintf("%s,p=%s", clientFinalWithoutProof, base64.StdEncoding.EncodeToString(clientProof))

	authCtx.AuthData = []byte(clientFinal)
	authCtx.State = result.State

	result, err = auth.AuthContinue(ctx, authCtx)
	require.NoError(t, err)
	assert.True(t, result.Success)
	assert.Equal(t, DefaultNamespace, result.Namespace)
}

func TestSCRAMCredentialLookupFunc(t *testing.T) {
	called := false
	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, username string) (*SCRAMCredentials, error) {
		called = true
		if username == "test" {
			return &SCRAMCredentials{}, nil
		}
		return nil, nil
	})

	creds, err := lookup.LookupCredentials(context.Background(), "test")
	require.NoError(t, err)
	assert.NotNil(t, creds)
	assert.True(t, called)
}

func TestSCRAMAuthenticatorLookupError(t *testing.T) {
	expectedErr := errors.New("database error")
	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, _ string) (*SCRAMCredentials, error) {
		return nil, expectedErr
	})

	auth := NewSCRAMAuthenticator(lookup)

	authCtx := &EnhancedAuthContext{
		ClientID:   "test-client",
		AuthMethod: "SCRAM-SHA-256",
		AuthData:   []byte("n,,n=user,r=nonce"),
	}

	result, err := auth.AuthStart(context.Background(), authCtx)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, result)
}

func TestSCRAMMultiAuthenticator(t *testing.T) {
	password := "secret"
	salt := []byte("salt1234")
	iterations := 4096

	// Create credentials for multiple hash types
	sha1Creds := ComputeSCRAMCredentials(SCRAMHashSHA1, password, salt, iterations)
	sha1Creds.Namespace = "sha1-ns"

	sha256Creds := ComputeSCRAMCredentials(SCRAMHashSHA256, password, salt, iterations)
	sha256Creds.Namespace = "sha256-ns"

	sha512Creds := ComputeSCRAMCredentials(SCRAMHashSHA512, password, salt, iterations)
	sha512Creds.Namespace = "sha512-ns"

	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, username string) (*SCRAMCredentials, error) {
		switch username {
		case "sha1user":
			return sha1Creds, nil
		case "sha256user":
			return sha256Creds, nil
		case "sha512user":
			return sha512Creds, nil
		}
		return nil, nil
	})

	auth := NewSCRAMAuthenticator(lookup, SCRAMHashSHA1, SCRAMHashSHA256, SCRAMHashSHA512)
	ctx := context.Background()

	// Test SHA-1 user can authenticate with SCRAM-SHA-1
	t.Run("SHA-1 user with SHA-1 method", func(t *testing.T) {
		authCtx := &EnhancedAuthContext{
			ClientID:   "test",
			AuthMethod: "SCRAM-SHA-1",
			AuthData:   []byte("n,,n=sha1user,r=nonce"),
		}
		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Continue) // Should succeed to continue
	})

	// Test SHA-256 user can authenticate with SCRAM-SHA-256
	t.Run("SHA-256 user with SHA-256 method", func(t *testing.T) {
		authCtx := &EnhancedAuthContext{
			ClientID:   "test",
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte("n,,n=sha256user,r=nonce"),
		}
		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Continue)
	})

	// Test SHA-512 user can authenticate with SCRAM-SHA-512
	t.Run("SHA-512 user with SHA-512 method", func(t *testing.T) {
		authCtx := &EnhancedAuthContext{
			ClientID:   "test",
			AuthMethod: "SCRAM-SHA-512",
			AuthData:   []byte("n,,n=sha512user,r=nonce"),
		}
		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.True(t, result.Continue)
	})

	// Test user with wrong method fails
	t.Run("SHA-256 user with SHA-512 method fails", func(t *testing.T) {
		authCtx := &EnhancedAuthContext{
			ClientID:   "test",
			AuthMethod: "SCRAM-SHA-512",
			AuthData:   []byte("n,,n=sha256user,r=nonce"),
		}
		result, err := auth.AuthStart(ctx, authCtx)
		require.NoError(t, err)
		assert.False(t, result.Success)
		assert.Equal(t, ReasonNotAuthorized, result.ReasonCode)
	})
}
