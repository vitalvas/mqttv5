package mqttv5

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // SHA-1 required for SCRAM-SHA-1 compatibility
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

// SCRAMHash represents the hash algorithm used for SCRAM authentication.
type SCRAMHash int

const (
	// SCRAMHashSHA1 uses SHA-1 (for legacy compatibility, not recommended for new deployments).
	SCRAMHashSHA1 SCRAMHash = iota
	// SCRAMHashSHA256 uses SHA-256 (recommended).
	SCRAMHashSHA256
	// SCRAMHashSHA512 uses SHA-512 (highest security).
	SCRAMHashSHA512
)

// String returns the MQTT auth method name for this hash.
func (h SCRAMHash) String() string {
	switch h {
	case SCRAMHashSHA1:
		return "SCRAM-SHA-1"
	case SCRAMHashSHA256:
		return "SCRAM-SHA-256"
	case SCRAMHashSHA512:
		return "SCRAM-SHA-512"
	default:
		return "SCRAM-SHA-256"
	}
}

// hashFunc returns the hash.Hash constructor for this algorithm.
func (h SCRAMHash) hashFunc() func() hash.Hash {
	switch h {
	case SCRAMHashSHA1:
		return sha1.New
	case SCRAMHashSHA256:
		return sha256.New
	case SCRAMHashSHA512:
		return sha512.New
	default:
		return sha256.New
	}
}

// keySize returns the key size in bytes for this hash.
func (h SCRAMHash) keySize() int {
	switch h {
	case SCRAMHashSHA1:
		return 20
	case SCRAMHashSHA256:
		return 32
	case SCRAMHashSHA512:
		return 64
	default:
		return 32
	}
}

// SCRAMCredentials contains the pre-computed SCRAM credentials for a user.
// These should be computed once when the user sets their password and stored securely.
type SCRAMCredentials struct {
	// Hash is the hash algorithm used for these credentials.
	Hash SCRAMHash

	// Salt is the random salt used for key derivation (should be unique per user).
	Salt []byte

	// Iterations is the PBKDF2 iteration count (minimum 4096 recommended).
	Iterations int

	// StoredKey is H(ClientKey) where ClientKey = HMAC(SaltedPassword, "Client Key").
	StoredKey []byte

	// ServerKey is HMAC(SaltedPassword, "Server Key").
	ServerKey []byte

	// Namespace is the tenant namespace for this user (optional, defaults to DefaultNamespace).
	Namespace string
}

// SCRAMCredentialLookup is the interface users implement to provide SCRAM credentials.
type SCRAMCredentialLookup interface {
	// LookupCredentials returns the SCRAM credentials for the given username.
	// Returns nil if the user does not exist.
	LookupCredentials(ctx context.Context, username string) (*SCRAMCredentials, error)
}

// SCRAMCredentialLookupFunc is a function adapter for SCRAMCredentialLookup.
type SCRAMCredentialLookupFunc func(ctx context.Context, username string) (*SCRAMCredentials, error)

// LookupCredentials implements SCRAMCredentialLookup.
func (f SCRAMCredentialLookupFunc) LookupCredentials(ctx context.Context, username string) (*SCRAMCredentials, error) {
	return f(ctx, username)
}

// scramState holds authentication state between SCRAM exchanges.
type scramState struct {
	username    string
	clientNonce string
	serverNonce string
	authMessage string
	credentials *SCRAMCredentials
	hashType    SCRAMHash
}

// SCRAMAuthenticator implements SCRAM enhanced authentication.
// It handles all SCRAM protocol details; users only need to provide credential lookup.
// Supports SCRAM-SHA-1, SCRAM-SHA-256, and SCRAM-SHA-512.
type SCRAMAuthenticator struct {
	lookup         SCRAMCredentialLookup
	supportedHashs []SCRAMHash
}

// NewSCRAMAuthenticator creates a new SCRAM authenticator supporting the specified hash algorithms.
// If no hash types are provided, defaults to SCRAM-SHA-256.
// Multiple hash types can be provided for backwards compatibility or migration scenarios.
//
// Examples:
//
//	NewSCRAMAuthenticator(lookup)                                              // SHA-256 only
//	NewSCRAMAuthenticator(lookup, SCRAMHashSHA512)                             // SHA-512 only
//	NewSCRAMAuthenticator(lookup, SCRAMHashSHA1, SCRAMHashSHA256)              // SHA-1 and SHA-256
//	NewSCRAMAuthenticator(lookup, SCRAMHashSHA1, SCRAMHashSHA256, SCRAMHashSHA512) // All three
func NewSCRAMAuthenticator(lookup SCRAMCredentialLookup, hashes ...SCRAMHash) *SCRAMAuthenticator {
	if len(hashes) == 0 {
		hashes = []SCRAMHash{SCRAMHashSHA256}
	}
	return &SCRAMAuthenticator{
		lookup:         lookup,
		supportedHashs: hashes,
	}
}

// SupportsMethod returns true if the authenticator supports the given SCRAM method.
func (a *SCRAMAuthenticator) SupportsMethod(method string) bool {
	for _, h := range a.supportedHashs {
		if h.String() == method {
			return true
		}
	}
	return false
}

// getHashForMethod returns the SCRAMHash for the given method string.
func (a *SCRAMAuthenticator) getHashForMethod(method string) (SCRAMHash, bool) {
	for _, h := range a.supportedHashs {
		if h.String() == method {
			return h, true
		}
	}
	return SCRAMHashSHA256, false
}

// AuthStart processes the client-first-message and returns the server challenge.
func (a *SCRAMAuthenticator) AuthStart(ctx context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	// Determine hash type from auth method
	hashType, ok := a.getHashForMethod(authCtx.AuthMethod)
	if !ok {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Parse client-first-message: n,,n=<username>,r=<client-nonce>
	clientFirst := string(authCtx.AuthData)

	username, clientNonce := parseScramClientFirst(clientFirst)
	if username == "" || clientNonce == "" {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Look up user credentials
	creds, err := a.lookup.LookupCredentials(ctx, username)
	if err != nil {
		return nil, err
	}
	if creds == nil {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Verify credentials match requested hash type
	if creds.Hash != hashType {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Generate server nonce
	serverNonce := fmt.Sprintf("%s%s", clientNonce, generateScramNonce())

	// Build server-first-message
	saltB64 := base64.StdEncoding.EncodeToString(creds.Salt)
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", serverNonce, saltB64, creds.Iterations)

	// Build auth message for signature verification
	clientFirstBare := extractScramBareMessage(clientFirst)
	authMessage := fmt.Sprintf("%s,%s", clientFirstBare, serverFirst)

	state := &scramState{
		username:    username,
		clientNonce: clientNonce,
		serverNonce: serverNonce,
		authMessage: authMessage,
		credentials: creds,
		hashType:    hashType,
	}

	return &EnhancedAuthResult{
		Continue:   true,
		ReasonCode: ReasonContinueAuth,
		AuthData:   []byte(serverFirst),
		State:      state,
	}, nil
}

// AuthContinue processes the client-final-message and verifies the client proof.
func (a *SCRAMAuthenticator) AuthContinue(_ context.Context, authCtx *EnhancedAuthContext) (*EnhancedAuthResult, error) {
	state, ok := authCtx.State.(*scramState)
	if !ok || state == nil {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	hashFunc := state.hashType.hashFunc()

	// Parse client-final-message: c=<channel-binding>,r=<nonce>,p=<proof>
	clientFinal := string(authCtx.AuthData)
	channelBinding, nonce, proofB64 := parseScramClientFinal(clientFinal)

	// Verify nonce matches (prevents replay attacks)
	if nonce != state.serverNonce {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Decode client proof
	clientProof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Build client-final-message-without-proof for auth message
	clientFinalWithoutProof := fmt.Sprintf("c=%s,r=%s", channelBinding, nonce)
	fullAuthMessage := fmt.Sprintf("%s,%s", state.authMessage, clientFinalWithoutProof)

	// Compute ClientSignature = HMAC(StoredKey, AuthMessage)
	clientSigHMAC := hmac.New(hashFunc, state.credentials.StoredKey)
	clientSigHMAC.Write([]byte(fullAuthMessage))
	clientSignature := clientSigHMAC.Sum(nil)

	// Recover ClientKey = ClientProof XOR ClientSignature
	if len(clientProof) != len(clientSignature) {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	clientKey := make([]byte, len(clientProof))
	for i := range clientProof {
		clientKey[i] = clientProof[i] ^ clientSignature[i]
	}

	// Verify: H(ClientKey) should equal StoredKey
	h := hashFunc()
	h.Write(clientKey)
	computedStoredKey := h.Sum(nil)
	if !hmac.Equal(computedStoredKey, state.credentials.StoredKey) {
		return &EnhancedAuthResult{
			Success:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	// Compute ServerSignature for mutual authentication
	serverSigHMAC := hmac.New(hashFunc, state.credentials.ServerKey)
	serverSigHMAC.Write([]byte(fullAuthMessage))
	serverSignature := serverSigHMAC.Sum(nil)

	// Build server-final-message
	serverFinal := fmt.Sprintf("v=%s", base64.StdEncoding.EncodeToString(serverSignature))

	namespace := state.credentials.Namespace
	if namespace == "" {
		namespace = DefaultNamespace
	}

	return &EnhancedAuthResult{
		Success:    true,
		ReasonCode: ReasonSuccess,
		AuthData:   []byte(serverFinal),
		Namespace:  namespace,
	}, nil
}

// ComputeSCRAMCredentials computes SCRAM credentials from a plaintext password.
// Use this when creating or updating user passwords.
// The salt should be randomly generated and unique per user.
// Iterations should be at least 4096 (higher is more secure but slower).
//
// Examples:
//
//	ComputeSCRAMCredentials(SCRAMHashSHA256, "password", salt, 4096) // Recommended
//	ComputeSCRAMCredentials(SCRAMHashSHA512, "password", salt, 4096) // Highest security
//	ComputeSCRAMCredentials(SCRAMHashSHA1, "password", salt, 4096)   // Legacy compatibility
func ComputeSCRAMCredentials(hashType SCRAMHash, password string, salt []byte, iterations int) *SCRAMCredentials {
	hashFunc := hashType.hashFunc()
	keySize := hashType.keySize()

	// SaltedPassword = PBKDF2(password, salt, iterations, keySize, Hash)
	saltedPassword := pbkdf2.Key([]byte(password), salt, iterations, keySize, hashFunc)

	// ClientKey = HMAC(SaltedPassword, "Client Key")
	clientKeyHMAC := hmac.New(hashFunc, saltedPassword)
	clientKeyHMAC.Write([]byte("Client Key"))
	clientKey := clientKeyHMAC.Sum(nil)

	// StoredKey = H(ClientKey)
	h := hashFunc()
	h.Write(clientKey)
	storedKey := h.Sum(nil)

	// ServerKey = HMAC(SaltedPassword, "Server Key")
	serverKeyHMAC := hmac.New(hashFunc, saltedPassword)
	serverKeyHMAC.Write([]byte("Server Key"))
	serverKey := serverKeyHMAC.Sum(nil)

	return &SCRAMCredentials{
		Hash:       hashType,
		Salt:       salt,
		Iterations: iterations,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
	}
}

// GenerateSalt generates a random salt for SCRAM credential computation.
func GenerateSalt() ([]byte, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	return salt, nil
}

// ErrSCRAMInvalidCredentials is returned when SCRAM credentials are invalid.
var ErrSCRAMInvalidCredentials = errors.New("invalid SCRAM credentials")

// parseScramClientFirst extracts username and nonce from client-first-message.
func parseScramClientFirst(msg string) (username, nonce string) {
	for _, part := range strings.Split(msg, ",") {
		if len(part) < 2 {
			continue
		}
		switch part[:2] {
		case "n=":
			username = part[2:]
		case "r=":
			nonce = part[2:]
		}
	}
	return
}

// extractScramBareMessage removes the GS2 header from client-first-message.
func extractScramBareMessage(msg string) string {
	// Skip "n,," or "y,," or "p=..." prefix
	if idx := strings.Index(msg, "n="); idx >= 0 {
		return msg[idx:]
	}
	return msg
}

// parseScramClientFinal extracts channel binding, nonce, and proof from client-final-message.
func parseScramClientFinal(msg string) (channelBinding, nonce, proof string) {
	for _, part := range strings.Split(msg, ",") {
		if len(part) < 2 {
			continue
		}
		switch part[:2] {
		case "c=":
			channelBinding = part[2:]
		case "r=":
			nonce = part[2:]
		case "p=":
			proof = part[2:]
		}
	}
	return
}

// generateScramNonce creates a cryptographically secure random nonce.
func generateScramNonce() string {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		// Fallback to less secure but functional nonce
		return "fallback-nonce"
	}
	return base64.StdEncoding.EncodeToString(b)
}
