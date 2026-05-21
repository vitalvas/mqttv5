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
	"net"
	"strings"
	"testing"
	"time"

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

func TestGenerateScramNonce(t *testing.T) {
	n1, err := generateScramNonce()
	require.NoError(t, err)
	n2, err := generateScramNonce()
	require.NoError(t, err)

	// Base64 of 18 bytes = 24 chars.
	assert.Len(t, n1, 24)
	assert.Len(t, n2, 24)
	// Nonces must differ — identical nonces would mean replay-protection failure.
	assert.NotEqual(t, n1, n2)
}

func TestExtractScramBareMessage(t *testing.T) {
	t.Run("with gs2 header", func(t *testing.T) {
		got := extractScramBareMessage("n,,n=user,r=nonce")
		assert.Equal(t, "n=user,r=nonce", got)
	})

	t.Run("without n= returns unchanged", func(t *testing.T) {
		// Malformed messages without n= fall through and are returned as-is.
		got := extractScramBareMessage("malformed-no-nequals")
		assert.Equal(t, "malformed-no-nequals", got)
	})
}

func TestParseScramClientFinal(t *testing.T) {
	t.Run("complete message", func(t *testing.T) {
		cb, nonce, proof := parseScramClientFinal("c=biws,r=nonce123,p=proofvalue")
		assert.Equal(t, "biws", cb)
		assert.Equal(t, "nonce123", nonce)
		assert.Equal(t, "proofvalue", proof)
	})

	t.Run("short part is skipped", func(t *testing.T) {
		cb, nonce, proof := parseScramClientFinal("c=biws,x,r=nonce,p=proof")
		assert.Equal(t, "biws", cb)
		assert.Equal(t, "nonce", nonce)
		assert.Equal(t, "proof", proof)
	})
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
	assert.Contains(t, serverFirst, fmt.Sprintf("r=%s", clientNonce))
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

// runClientServerSCRAMExchange drives an in-memory SCRAM handshake between
// ClientSCRAMAuthenticator and the server SCRAMAuthenticator. It returns the
// final server result so callers can assert namespace/success/etc. Accepts
// testing.TB so it can be reused from benchmarks as well as tests.
func runClientServerSCRAMExchange(tb testing.TB, hashType SCRAMHash, username, password string, serverAuth *SCRAMAuthenticator) (*EnhancedAuthResult, error) {
	tb.Helper()
	ctx := context.Background()
	clientAuth := NewClientSCRAMAuthenticator(hashType, username, password)

	clientFirst, err := clientAuth.AuthStart(ctx)
	if err != nil {
		return nil, err
	}

	serverFirst, err := serverAuth.AuthStart(ctx, &EnhancedAuthContext{
		ClientID:   "test-client",
		AuthMethod: hashType.String(),
		AuthData:   clientFirst.AuthData,
	})
	if err != nil {
		return nil, err
	}
	if !serverFirst.Continue {
		return serverFirst, nil
	}

	clientFinal, err := clientAuth.AuthContinue(ctx, &ClientEnhancedAuthContext{
		AuthMethod: hashType.String(),
		AuthData:   serverFirst.AuthData,
		ReasonCode: serverFirst.ReasonCode,
		State:      clientFirst.State,
	})
	if err != nil {
		return nil, err
	}

	serverFinal, err := serverAuth.AuthContinue(ctx, &EnhancedAuthContext{
		ClientID:   "test-client",
		AuthMethod: hashType.String(),
		AuthData:   clientFinal.AuthData,
		ReasonCode: ReasonContinueAuth,
		State:      serverFirst.State,
	})
	if err != nil {
		return nil, err
	}
	if !serverFinal.Success {
		return serverFinal, nil
	}

	clientDone, err := clientAuth.AuthContinue(ctx, &ClientEnhancedAuthContext{
		AuthMethod: hashType.String(),
		AuthData:   serverFinal.AuthData,
		ReasonCode: ReasonSuccess,
		State:      clientFinal.State,
	})
	if err != nil {
		return nil, err
	}
	if !clientDone.Done {
		tb.Fatalf("client should be done after server-final-message")
	}
	return serverFinal, nil
}

func TestClientSCRAMAuthenticatorAuthMethod(t *testing.T) {
	cases := []struct {
		hash SCRAMHash
		want string
	}{
		{SCRAMHashSHA1, "SCRAM-SHA-1"},
		{SCRAMHashSHA256, "SCRAM-SHA-256"},
		{SCRAMHashSHA512, "SCRAM-SHA-512"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			c := NewClientSCRAMAuthenticator(tc.hash, "u", "p")
			assert.Equal(t, tc.want, c.AuthMethod())
		})
	}
}

func TestClientSCRAMAuthenticatorAuthStart(t *testing.T) {
	t.Run("emits client-first-message with username and nonce", func(t *testing.T) {
		c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
		res, err := c.AuthStart(context.Background())
		require.NoError(t, err)
		assert.False(t, res.Done)
		msg := string(res.AuthData)
		assert.True(t, strings.HasPrefix(msg, "n,,"), "must start with GS2 header n,,")
		user, nonce := parseScramClientFirst(msg)
		assert.Equal(t, "alice", user)
		assert.NotEmpty(t, nonce)
		state, ok := res.State.(*clientScramState)
		require.True(t, ok)
		assert.Equal(t, nonce, state.clientNonce)
		assert.Equal(t, clientScramStepStart, state.step)
	})

	t.Run("empty username errors", func(t *testing.T) {
		c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "", "secret")
		_, err := c.AuthStart(context.Background())
		require.Error(t, err)
	})

	t.Run("nonce generation failure is surfaced", func(t *testing.T) {
		c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
		c.nonceFn = func() (string, error) { return "", errors.New("boom") }
		_, err := c.AuthStart(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "boom")
	})
}

func TestClientSCRAMAuthenticatorFullExchange(t *testing.T) {
	password := "secret123"
	salt := []byte("salt-1234-abcd")
	iterations := 4096

	for _, hashType := range []SCRAMHash{SCRAMHashSHA1, SCRAMHashSHA256, SCRAMHashSHA512} {
		t.Run(hashType.String(), func(t *testing.T) {
			creds := ComputeSCRAMCredentials(hashType, password, salt, iterations)
			namespace := fmt.Sprintf("ns-%s", hashType.String())
			creds.Namespace = namespace
			lookup := SCRAMCredentialLookupFunc(func(_ context.Context, username string) (*SCRAMCredentials, error) {
				if username == "alice" {
					return creds, nil
				}
				return nil, nil
			})
			serverAuth := NewSCRAMAuthenticator(lookup, hashType)

			res, err := runClientServerSCRAMExchange(t, hashType, "alice", password, serverAuth)
			require.NoError(t, err)
			require.True(t, res.Success)
			assert.Equal(t, namespace, res.Namespace)
		})
	}
}

func TestClientSCRAMAuthenticatorWrongPassword(t *testing.T) {
	salt := []byte("salt-1234-abcd")
	creds := ComputeSCRAMCredentials(SCRAMHashSHA256, "rightpass", salt, 4096)
	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, _ string) (*SCRAMCredentials, error) {
		return creds, nil
	})
	serverAuth := NewSCRAMAuthenticator(lookup, SCRAMHashSHA256)

	res, err := runClientServerSCRAMExchange(t, SCRAMHashSHA256, "alice", "wrongpass", serverAuth)
	require.NoError(t, err)
	assert.False(t, res.Success)
	assert.Equal(t, ReasonNotAuthorized, res.ReasonCode)
}

func TestClientSCRAMAuthenticatorRejectsBadServerSignature(t *testing.T) {
	ctx := context.Background()
	c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
	start, err := c.AuthStart(ctx)
	require.NoError(t, err)

	salt := base64.StdEncoding.EncodeToString([]byte("salt"))
	state := start.State.(*clientScramState)
	serverFirst := fmt.Sprintf("r=%sSERVER,s=%s,i=4096", state.clientNonce, salt)

	final, err := c.AuthContinue(ctx, &ClientEnhancedAuthContext{
		AuthMethod: SCRAMHashSHA256.String(),
		AuthData:   []byte(serverFirst),
		ReasonCode: ReasonContinueAuth,
		State:      start.State,
	})
	require.NoError(t, err)
	require.False(t, final.Done)

	// Provide a server-final-message with a bogus signature.
	bogus := base64.StdEncoding.EncodeToString([]byte("not-the-real-signature"))
	_, err = c.AuthContinue(ctx, &ClientEnhancedAuthContext{
		AuthMethod: SCRAMHashSHA256.String(),
		AuthData:   fmt.Appendf(nil, "v=%s", bogus),
		ReasonCode: ReasonSuccess,
		State:      final.State,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSCRAMInvalidServerSignature)
}

func TestClientSCRAMAuthenticatorServerFirstErrors(t *testing.T) {
	mkState := func(t *testing.T) (*ClientSCRAMAuthenticator, *ClientEnhancedAuthResult) {
		t.Helper()
		c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
		start, err := c.AuthStart(context.Background())
		require.NoError(t, err)
		return c, start
	}

	t.Run("missing fields", func(t *testing.T) {
		c, start := mkState(t)
		saltB64 := base64.StdEncoding.EncodeToString([]byte("s"))
		_, err := c.AuthContinue(context.Background(), &ClientEnhancedAuthContext{
			AuthData: fmt.Appendf(nil, "r=xyz,s=%s", saltB64), // no i=
			State:    start.State,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("server nonce does not extend client nonce", func(t *testing.T) {
		c, start := mkState(t)
		saltB64 := base64.StdEncoding.EncodeToString([]byte("s"))
		_, err := c.AuthContinue(context.Background(), &ClientEnhancedAuthContext{
			AuthData: fmt.Appendf(nil, "r=different,s=%s,i=4096", saltB64),
			State:    start.State,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("non-numeric iteration count", func(t *testing.T) {
		c, start := mkState(t)
		state := start.State.(*clientScramState)
		saltB64 := base64.StdEncoding.EncodeToString([]byte("s"))
		_, err := c.AuthContinue(context.Background(), &ClientEnhancedAuthContext{
			AuthData: fmt.Appendf(nil, "r=%sX,s=%s,i=abc", state.clientNonce, saltB64),
			State:    start.State,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("zero iteration count", func(t *testing.T) {
		c, start := mkState(t)
		state := start.State.(*clientScramState)
		saltB64 := base64.StdEncoding.EncodeToString([]byte("s"))
		_, err := c.AuthContinue(context.Background(), &ClientEnhancedAuthContext{
			AuthData: fmt.Appendf(nil, "r=%sX,s=%s,i=0", state.clientNonce, saltB64),
			State:    start.State,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("invalid base64 salt", func(t *testing.T) {
		c, start := mkState(t)
		state := start.State.(*clientScramState)
		_, err := c.AuthContinue(context.Background(), &ClientEnhancedAuthContext{
			AuthData: fmt.Appendf(nil, "r=%sX,s=@@@notbase64@@@,i=4096", state.clientNonce),
			State:    start.State,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("missing state", func(t *testing.T) {
		c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
		_, err := c.AuthContinue(context.Background(), &ClientEnhancedAuthContext{
			AuthData: []byte("r=x,s=eA==,i=4096"),
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("nil state", func(t *testing.T) {
		c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
		var st *clientScramState
		_, err := c.AuthContinue(context.Background(), &ClientEnhancedAuthContext{
			AuthData: []byte("r=x,s=eA==,i=4096"),
			State:    st,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("short parts skipped", func(t *testing.T) {
		c, start := mkState(t)
		state := start.State.(*clientScramState)
		saltB64 := base64.StdEncoding.EncodeToString([]byte("s"))
		// Leading empty field (",,") and a 1-char field ("x") must be skipped without panicking.
		_, err := c.AuthContinue(context.Background(), &ClientEnhancedAuthContext{
			AuthData: fmt.Appendf(nil, ",,x,r=%sX,s=%s,i=4096", state.clientNonce, saltB64),
			State:    start.State,
		})
		require.NoError(t, err)
	})

	t.Run("unknown step", func(t *testing.T) {
		c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
		// Drive the state machine to a value none of the named steps match.
		bogusState := &clientScramState{step: clientScramStep(99)}
		_, err := c.AuthContinue(context.Background(), &ClientEnhancedAuthContext{
			AuthData: []byte(""),
			State:    bogusState,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
		assert.Contains(t, err.Error(), "unknown step")
	})
}

func TestClientSCRAMAuthenticatorServerFinalErrors(t *testing.T) {
	ctx := context.Background()
	advanceToFinal := func(t *testing.T) (*ClientSCRAMAuthenticator, *clientScramState) {
		t.Helper()
		c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
		start, err := c.AuthStart(ctx)
		require.NoError(t, err)
		s := start.State.(*clientScramState)
		serverFirst := fmt.Sprintf("r=%sX,s=%s,i=4096", s.clientNonce, base64.StdEncoding.EncodeToString([]byte("salt")))
		final, err := c.AuthContinue(ctx, &ClientEnhancedAuthContext{
			AuthData: []byte(serverFirst),
			State:    start.State,
		})
		require.NoError(t, err)
		return c, final.State.(*clientScramState)
	}

	t.Run("server reports e= error", func(t *testing.T) {
		c, state := advanceToFinal(t)
		_, err := c.AuthContinue(ctx, &ClientEnhancedAuthContext{
			AuthData: []byte("e=invalid-proof"),
			State:    state,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
		assert.Contains(t, err.Error(), "invalid-proof")
	})

	t.Run("missing v=", func(t *testing.T) {
		c, state := advanceToFinal(t)
		_, err := c.AuthContinue(ctx, &ClientEnhancedAuthContext{
			AuthData: []byte("x=y"),
			State:    state,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("invalid base64 signature", func(t *testing.T) {
		c, state := advanceToFinal(t)
		_, err := c.AuthContinue(ctx, &ClientEnhancedAuthContext{
			AuthData: []byte("v=@@@"),
			State:    state,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("unexpected continuation after done", func(t *testing.T) {
		c, state := advanceToFinal(t)
		state.step = clientScramStepDone
		_, err := c.AuthContinue(ctx, &ClientEnhancedAuthContext{
			AuthData: []byte("v=eA=="),
			State:    state,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("short parts skipped before v=", func(t *testing.T) {
		c, state := advanceToFinal(t)
		_, err := c.AuthContinue(ctx, &ClientEnhancedAuthContext{
			AuthData: []byte(",,x,v=@@@"), // short fields must be skipped, then v= invalid
			State:    state,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})
}

func TestParseScramIterations(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		n, err := parseScramIterations("4096")
		require.NoError(t, err)
		assert.Equal(t, 4096, n)
	})

	t.Run("empty", func(t *testing.T) {
		_, err := parseScramIterations("")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("non-numeric", func(t *testing.T) {
		_, err := parseScramIterations("12a3")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
	})

	t.Run("overflow", func(t *testing.T) {
		_, err := parseScramIterations("99999999999")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSCRAMProtocol)
		assert.Contains(t, err.Error(), "too large")
	})
}

func TestClientSCRAMAuthenticatorEndToEnd(t *testing.T) {
	// Real network: spin up the broker and connect with the new client.
	password := "secret123"
	salt, err := GenerateSalt()
	require.NoError(t, err)
	creds := ComputeSCRAMCredentials(SCRAMHashSHA256, password, salt, 4096)
	creds.Namespace = "team-alpha"

	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, username string) (*SCRAMCredentials, error) {
		if username == "alice" {
			return creds, nil
		}
		return nil, nil
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := NewServer(
		WithListener(listener),
		WithEnhancedAuth(NewSCRAMAuthenticator(lookup, SCRAMHashSHA256)),
	)
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addr := fmt.Sprintf("tcp://%s", listener.Addr().String())
	client, err := DialContext(ctx,
		WithServers(addr),
		WithClientID("scram-e2e-client"),
		WithCleanStart(true),
		WithConnectTimeout(3*time.Second),
		WithEnhancedAuthentication(NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", password)),
	)
	require.NoError(t, err)
	defer client.Close()

	// Sanity check the connection works end-to-end by subscribing and publishing.
	received := make(chan *Message, 1)
	err = client.Subscribe("scram/test", 0, func(msg *Message) {
		select {
		case received <- msg:
		default:
		}
	})
	require.NoError(t, err)

	err = client.Publish(&Message{Topic: "scram/test", Payload: []byte("hello"), QoS: 0})
	require.NoError(t, err)

	select {
	case msg := <-received:
		assert.Equal(t, []byte("hello"), msg.Payload)
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive published message")
	}
}

func TestClientSCRAMAuthenticatorEndToEndWrongPassword(t *testing.T) {
	password := "rightpass"
	salt, err := GenerateSalt()
	require.NoError(t, err)
	creds := ComputeSCRAMCredentials(SCRAMHashSHA256, password, salt, 4096)

	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, _ string) (*SCRAMCredentials, error) {
		return creds, nil
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := NewServer(
		WithListener(listener),
		WithEnhancedAuth(NewSCRAMAuthenticator(lookup, SCRAMHashSHA256)),
	)
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addr := fmt.Sprintf("tcp://%s", listener.Addr().String())
	_, err = DialContext(ctx,
		WithServers(addr),
		WithClientID("scram-e2e-bad-pass"),
		WithCleanStart(true),
		WithConnectTimeout(3*time.Second),
		WithEnhancedAuthentication(NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "wrongpass")),
	)
	require.Error(t, err)
}

func BenchmarkClientSCRAMAuthStart(b *testing.B) {
	c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := c.AuthStart(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func benchClientSCRAMFullExchange(b *testing.B, hashType SCRAMHash) {
	b.Helper()
	password := "secret123"
	salt := []byte("salt-1234-abcd")
	creds := ComputeSCRAMCredentials(hashType, password, salt, 4096)
	lookup := SCRAMCredentialLookupFunc(func(_ context.Context, _ string) (*SCRAMCredentials, error) {
		return creds, nil
	})
	serverAuth := NewSCRAMAuthenticator(lookup, hashType)

	b.ReportAllocs()
	for b.Loop() {
		res, err := runClientServerSCRAMExchange(b, hashType, "alice", password, serverAuth)
		if err != nil {
			b.Fatal(err)
		}
		if !res.Success {
			b.Fatalf("exchange failed: %s", res.ReasonCode)
		}
	}
}

func BenchmarkClientSCRAMFullExchange(b *testing.B) {
	b.Run("SCRAM-SHA-1", func(b *testing.B) { benchClientSCRAMFullExchange(b, SCRAMHashSHA1) })
	b.Run("SCRAM-SHA-256", func(b *testing.B) { benchClientSCRAMFullExchange(b, SCRAMHashSHA256) })
	b.Run("SCRAM-SHA-512", func(b *testing.B) { benchClientSCRAMFullExchange(b, SCRAMHashSHA512) })
}

func FuzzParseScramServerFirst(f *testing.F) {
	// Valid seeds
	f.Add("r=abc,s=eA==,i=4096")
	f.Add(fmt.Sprintf("r=abcDEF,s=%s,i=10000", base64.StdEncoding.EncodeToString([]byte("salt"))))
	// Edge cases the parser must tolerate without panicking
	f.Add("")
	f.Add(",,,")
	f.Add("r=,s=,i=")
	f.Add("i=abc")
	f.Add("i=99999999999999999")
	f.Add("r=x,s=@@@,i=-1")
	f.Add("x,y,z")

	f.Fuzz(func(_ *testing.T, msg string) {
		_, _, _, _ = parseScramServerFirst(msg)
	})
}

func FuzzParseScramServerFinal(f *testing.F) {
	f.Add(fmt.Sprintf("v=%s", base64.StdEncoding.EncodeToString([]byte("sig"))))
	f.Add("e=invalid-proof")
	f.Add("")
	f.Add(",,,")
	f.Add("v=")
	f.Add("v=@@@notbase64@@@")
	f.Add("x=y,v=eA==")
	f.Add("e=,v=eA==")

	f.Fuzz(func(_ *testing.T, msg string) {
		_, _ = parseScramServerFinal(msg)
	})
}

func FuzzClientSCRAMHandleServerFirst(f *testing.F) {
	// Valid-looking seed (full handshake won't complete on fuzz inputs;
	// the goal is to ensure the parser+state machine never panic).
	f.Add(fmt.Sprintf("r=ABCDEFGHIJKLMNOPQRST,s=%s,i=4096", base64.StdEncoding.EncodeToString([]byte("salt"))))
	f.Add("")
	f.Add("r=different,s=eA==,i=4096")
	f.Add("r=,s=,i=0")
	f.Add("garbage,,,more")

	ctx := context.Background()
	f.Fuzz(func(_ *testing.T, msg string) {
		c := NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
		start, err := c.AuthStart(ctx)
		if err != nil {
			return
		}
		_, _ = c.AuthContinue(ctx, &ClientEnhancedAuthContext{
			AuthData: []byte(msg),
			State:    start.State,
		})
	})
}
