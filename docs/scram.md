# SCRAM Enhanced Authentication

SCRAM (Salted Challenge Response Authentication Mechanism) is a family of
authentication mechanisms defined in [RFC 5802](https://www.rfc-editor.org/rfc/rfc5802)
and exposed through the MQTT v5 AUTH packet exchange. It lets a client prove
knowledge of a password without ever sending the password over the wire, and
lets the server prove it knows the stored credentials — so authentication is
mutual.

This package implements SCRAM for both broker (server) and client roles.

## Supported mechanisms

| Mechanism | `SCRAMHash` value | Notes |
|-----------|-------------------|-------|
| SCRAM-SHA-1 | `mqttv5.SCRAMHashSHA1` | Legacy compatibility only |
| SCRAM-SHA-256 | `mqttv5.SCRAMHashSHA256` | Recommended default |
| SCRAM-SHA-512 | `mqttv5.SCRAMHashSHA512` | Highest security |

The broker can advertise multiple mechanisms simultaneously. Each client
announces exactly one mechanism per connection — the `auth_method` property
in the CONNECT packet is a single string.

## Security properties

- **No plaintext password transmission.** Only proofs derived from the password
  travel over the wire.
- **Mutual authentication.** The server signs the session with a key derived
  from the stored credentials; the client verifies that signature and aborts
  the connection if it does not match.
- **Replay protection.** Each handshake mixes a client-generated nonce with a
  server-generated nonce; the proof is bound to the combined nonce.
- **Server compromise resistance.** The broker stores only `StoredKey` and
  `ServerKey` (derived via PBKDF2). An attacker who reads the credential store
  cannot recover the password without offline PBKDF2 brute force.

## Credential storage

Compute credentials once when the user sets or changes their password and store
the resulting `SCRAMCredentials` (the salt, iteration count, `StoredKey`, and
`ServerKey`). The plaintext password is not needed afterwards.

```go
salt, err := mqttv5.GenerateSalt()
if err != nil {
    return err
}
creds := mqttv5.ComputeSCRAMCredentials(
    mqttv5.SCRAMHashSHA256, // hash algorithm
    "user-password",        // plaintext password
    salt,                   // per-user random salt
    4096,                   // PBKDF2 iterations (>= 4096 recommended)
)
creds.Namespace = "team-alpha" // optional: assign tenant namespace
```

Use a different salt per user. Increase the iteration count to slow down
offline attacks; values from 10,000 to 100,000 are common in 2026 hardware.

## Server side

Implement `SCRAMCredentialLookup` (or use `SCRAMCredentialLookupFunc`) to
return credentials for a username, then wire the authenticator with
`WithEnhancedAuth`:

```go
lookup := mqttv5.SCRAMCredentialLookupFunc(
    func(_ context.Context, username string) (*mqttv5.SCRAMCredentials, error) {
        creds, ok := userStore[username]
        if !ok {
            return nil, nil // unknown user
        }
        return creds, nil
    },
)

auth := mqttv5.NewSCRAMAuthenticator(
    lookup,
    mqttv5.SCRAMHashSHA1,
    mqttv5.SCRAMHashSHA256,
    mqttv5.SCRAMHashSHA512,
)

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithEnhancedAuth(auth),
)
```

If no hash types are passed, the authenticator defaults to SCRAM-SHA-256 only.
A credential whose `Hash` field does not match the mechanism the client
announced is rejected.

## Client side

Build a `ClientSCRAMAuthenticator` with one mechanism, username, and password,
then pass it to `WithEnhancedAuthentication`:

```go
auth := mqttv5.NewClientSCRAMAuthenticator(
    mqttv5.SCRAMHashSHA256,
    "alice",
    "alice-password",
)

client, err := mqttv5.Dial(
    mqttv5.WithServers("tcp://localhost:1883"),
    mqttv5.WithClientID("alice-client"),
    mqttv5.WithEnhancedAuthentication(auth),
)
```

The client handles the full SCRAM exchange and verifies the server's signature
before reporting a successful connection. A signature mismatch returns
`mqttv5.ErrSCRAMInvalidServerSignature` from `Dial` / `DialContext`; malformed
server messages return errors wrapping `mqttv5.ErrSCRAMProtocol`.

## Handshake

```
Client                                                       Server
  |                                                            |
  | CONNECT                                                    |
  |   auth_method = SCRAM-SHA-256                              |
  |   auth_data   = n,,n=alice,r=<client-nonce>                |
  |----------------------------------------------------------->|
  |                                                            |
  |                                          AUTH (Continue)   |
  |                                  auth_data = r=<combined>, |
  |                                              s=<salt>,     |
  |                                              i=<iters>     |
  |<-----------------------------------------------------------|
  |                                                            |
  | AUTH (Continue)                                            |
  |   auth_data = c=biws,r=<combined>,p=<client-proof>         |
  |----------------------------------------------------------->|
  |                                                            |
  |                                          AUTH (Success)    |
  |                                  auth_data = v=<server-sig>|
  |<-----------------------------------------------------------|
  |                                                            |
  |                                                  CONNACK   |
  |<-----------------------------------------------------------|
```

The client verifies `v=<server-sig>` against the value it computed from the
password. CONNACK is delivered only after the verification succeeds; otherwise
the client closes the connection without ever exchanging application
messages.

## Migration and multiple mechanisms

`NewSCRAMAuthenticator` accepts multiple hashes in a single call so the broker
can support several mechanisms in parallel. A common pattern is to keep
SCRAM-SHA-1 enabled for legacy clients while introducing SCRAM-SHA-256 for new
ones. Per-user records bind to a specific hash via the `Hash` field of
`SCRAMCredentials`, so it is safe to mix.

## Related

- [Client documentation](client.md)
- [Server documentation](server.md)
