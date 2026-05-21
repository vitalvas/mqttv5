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
	noncePart, err := generateScramNonce()
	if err != nil {
		return nil, err
	}
	serverNonce := fmt.Sprintf("%s%s", clientNonce, noncePart)

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

// ErrSCRAMInvalidServerSignature is returned when the server-final-message
// signature (v=...) does not match the value the client computed, indicating
// the server cannot prove knowledge of the user's credentials.
var ErrSCRAMInvalidServerSignature = errors.New("invalid SCRAM server signature")

// ErrSCRAMProtocol is returned when the server sends a SCRAM message that
// cannot be parsed or violates the protocol (wrong nonce, missing fields).
var ErrSCRAMProtocol = errors.New("SCRAM protocol error")

// clientScramStep tracks where the client is in the SCRAM exchange.
type clientScramStep int

const (
	clientScramStepStart clientScramStep = iota
	clientScramStepFinal
	clientScramStepDone
)

// clientScramState carries data between client-side SCRAM exchanges.
type clientScramState struct {
	step                    clientScramStep
	clientNonce             string
	clientFirstBare         string
	expectedServerSignature []byte
}

// ClientSCRAMAuthenticator implements client-side SCRAM enhanced authentication.
// It handles all SCRAM protocol details; users only need to provide the hash
// algorithm, username, and password. Supports SCRAM-SHA-1, SCRAM-SHA-256, and
// SCRAM-SHA-512.
//
// The ServerSignature returned in the server-final-message is verified against
// the value derived from the supplied password; a mismatch aborts the exchange.
type ClientSCRAMAuthenticator struct {
	hashType SCRAMHash
	username string
	password string
	nonceFn  func() (string, error)
}

// NewClientSCRAMAuthenticator creates a client-side SCRAM authenticator that
// announces a single mechanism in CONNECT.
//
// Examples:
//
//	NewClientSCRAMAuthenticator(SCRAMHashSHA256, "alice", "secret")
//	NewClientSCRAMAuthenticator(SCRAMHashSHA512, "admin", "topsecret")
//	NewClientSCRAMAuthenticator(SCRAMHashSHA1, "legacy", "legacypass")
func NewClientSCRAMAuthenticator(hashType SCRAMHash, username, password string) *ClientSCRAMAuthenticator {
	return &ClientSCRAMAuthenticator{
		hashType: hashType,
		username: username,
		password: password,
		nonceFn:  generateScramNonce,
	}
}

// AuthMethod returns the SCRAM mechanism name (e.g., "SCRAM-SHA-256").
func (c *ClientSCRAMAuthenticator) AuthMethod() string {
	return c.hashType.String()
}

// AuthStart builds the client-first-message that goes into the CONNECT packet.
func (c *ClientSCRAMAuthenticator) AuthStart(_ context.Context) (*ClientEnhancedAuthResult, error) {
	if c.username == "" {
		return nil, fmt.Errorf("scram: username is required")
	}

	clientNonce, err := c.nonceFn()
	if err != nil {
		return nil, err
	}

	clientFirstBare := fmt.Sprintf("n=%s,r=%s", c.username, clientNonce)
	clientFirst := fmt.Sprintf("n,,%s", clientFirstBare)

	return &ClientEnhancedAuthResult{
		Done:     false,
		AuthData: []byte(clientFirst),
		State: &clientScramState{
			step:            clientScramStepStart,
			clientNonce:     clientNonce,
			clientFirstBare: clientFirstBare,
		},
	}, nil
}

// AuthContinue handles the server-first-message and the server-final-message.
func (c *ClientSCRAMAuthenticator) AuthContinue(_ context.Context, authCtx *ClientEnhancedAuthContext) (*ClientEnhancedAuthResult, error) {
	state, ok := authCtx.State.(*clientScramState)
	if !ok || state == nil {
		return nil, fmt.Errorf("%w: missing state", ErrSCRAMProtocol)
	}

	switch state.step {
	case clientScramStepStart:
		return c.handleServerFirst(state, authCtx.AuthData)
	case clientScramStepFinal:
		return c.handleServerFinal(state, authCtx.AuthData)
	case clientScramStepDone:
		return nil, fmt.Errorf("%w: unexpected continuation after success", ErrSCRAMProtocol)
	default:
		return nil, fmt.Errorf("%w: unknown step", ErrSCRAMProtocol)
	}
}

// handleServerFirst parses server-first-message, computes the client proof,
// and emits client-final-message.
func (c *ClientSCRAMAuthenticator) handleServerFirst(state *clientScramState, data []byte) (*ClientEnhancedAuthResult, error) {
	serverFirst := string(data)
	serverNonce, saltB64, iterations, err := parseScramServerFirst(serverFirst)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(serverNonce, state.clientNonce) {
		return nil, fmt.Errorf("%w: server nonce does not extend client nonce", ErrSCRAMProtocol)
	}

	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid salt: %v", ErrSCRAMProtocol, err)
	}

	hashFunc := c.hashType.hashFunc()
	keySize := c.hashType.keySize()

	saltedPassword := pbkdf2.Key([]byte(c.password), salt, iterations, keySize, hashFunc)

	clientKeyHMAC := hmac.New(hashFunc, saltedPassword)
	clientKeyHMAC.Write([]byte("Client Key"))
	clientKey := clientKeyHMAC.Sum(nil)

	h := hashFunc()
	h.Write(clientKey)
	storedKey := h.Sum(nil)

	// "biws" is the base64 of "n,," — the GS2 header without channel binding.
	clientFinalWithoutProof := fmt.Sprintf("c=biws,r=%s", serverNonce)
	authMessage := fmt.Sprintf("%s,%s,%s", state.clientFirstBare, serverFirst, clientFinalWithoutProof)

	clientSigHMAC := hmac.New(hashFunc, storedKey)
	clientSigHMAC.Write([]byte(authMessage))
	clientSignature := clientSigHMAC.Sum(nil)

	clientProof := make([]byte, len(clientKey))
	for i := range clientKey {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	serverKeyHMAC := hmac.New(hashFunc, saltedPassword)
	serverKeyHMAC.Write([]byte("Server Key"))
	serverKey := serverKeyHMAC.Sum(nil)

	serverSigHMAC := hmac.New(hashFunc, serverKey)
	serverSigHMAC.Write([]byte(authMessage))
	expectedServerSignature := serverSigHMAC.Sum(nil)

	clientFinal := fmt.Sprintf("%s,p=%s", clientFinalWithoutProof, base64.StdEncoding.EncodeToString(clientProof))

	state.step = clientScramStepFinal
	state.expectedServerSignature = expectedServerSignature

	return &ClientEnhancedAuthResult{
		Done:     false,
		AuthData: []byte(clientFinal),
		State:    state,
	}, nil
}

// handleServerFinal verifies the server signature from the server-final-message.
func (c *ClientSCRAMAuthenticator) handleServerFinal(state *clientScramState, data []byte) (*ClientEnhancedAuthResult, error) {
	serverSignature, err := parseScramServerFinal(string(data))
	if err != nil {
		return nil, err
	}

	if !hmac.Equal(serverSignature, state.expectedServerSignature) {
		return nil, ErrSCRAMInvalidServerSignature
	}

	state.step = clientScramStepDone
	state.expectedServerSignature = nil

	return &ClientEnhancedAuthResult{
		Done:  true,
		State: state,
	}, nil
}

// parseScramServerFirst extracts nonce, salt (base64), and iteration count from
// the server-first-message: r=<nonce>,s=<salt>,i=<iterations>.
func parseScramServerFirst(msg string) (nonce, saltB64 string, iterations int, err error) {
	var hasNonce, hasSalt, hasIter bool
	for _, part := range strings.Split(msg, ",") {
		if len(part) < 2 {
			continue
		}
		switch part[:2] {
		case "r=":
			nonce = part[2:]
			hasNonce = true
		case "s=":
			saltB64 = part[2:]
			hasSalt = true
		case "i=":
			iterations, err = parseScramIterations(part[2:])
			if err != nil {
				return "", "", 0, err
			}
			hasIter = true
		}
	}
	if !hasNonce || !hasSalt || !hasIter {
		return "", "", 0, fmt.Errorf("%w: server-first-message missing required field", ErrSCRAMProtocol)
	}
	if iterations <= 0 {
		return "", "", 0, fmt.Errorf("%w: server-first-message iteration count must be positive", ErrSCRAMProtocol)
	}
	return nonce, saltB64, iterations, nil
}

// parseScramServerFinal extracts the server signature from server-final-message:
// v=<signature> on success, or e=<error> on failure.
func parseScramServerFinal(msg string) ([]byte, error) {
	for _, part := range strings.Split(msg, ",") {
		if len(part) < 2 {
			continue
		}
		switch part[:2] {
		case "v=":
			sig, err := base64.StdEncoding.DecodeString(part[2:])
			if err != nil {
				return nil, fmt.Errorf("%w: invalid server signature encoding: %v", ErrSCRAMProtocol, err)
			}
			return sig, nil
		case "e=":
			return nil, fmt.Errorf("%w: server error: %s", ErrSCRAMProtocol, part[2:])
		}
	}
	return nil, fmt.Errorf("%w: server-final-message missing v=", ErrSCRAMProtocol)
}

// parseScramIterations parses the iteration count from server-first-message.
func parseScramIterations(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty iteration count", ErrSCRAMProtocol)
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("%w: non-numeric iteration count %q", ErrSCRAMProtocol, s)
		}
		n = n*10 + int(r-'0')
		if n > 1<<30 {
			return 0, fmt.Errorf("%w: iteration count too large", ErrSCRAMProtocol)
		}
	}
	return n, nil
}

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
// Returns an error when the system's CSPRNG fails; callers must abort
// authentication rather than continue with a predictable nonce, since a
// predictable nonce defeats SCRAM's replay protection.
func generateScramNonce() (string, error) {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("scram: generate nonce: %w", err)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
