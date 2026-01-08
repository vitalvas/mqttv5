# mTLS Support

Mutual TLS (mTLS) authentication with certificate identity mapping for MQTT v5.0.

## Overview

The library provides comprehensive mTLS support with:

- Full access to native Go TLS types (`*tls.ConnectionState`, `*x509.Certificate`)
- Pluggable identity mapping from certificates to application identity
- Automatic session expiry based on certificate validity
- Multi-tenant namespace extraction from certificates

## TLS Identity

The `TLSIdentity` struct represents identity extracted from a client certificate:

```go
type TLSIdentity struct {
    Username   string            // Mapped username/principal
    Namespace  string            // Tenant namespace
    Attributes map[string]string // Additional attributes
}
```

## Identity Mapper Interface

The `TLSIdentityMapper` interface allows custom certificate-to-identity mapping:

```go
type TLSIdentityMapper interface {
    MapIdentity(ctx context.Context, state *tls.ConnectionState) (*TLSIdentity, error)
}
```

The mapper receives the full `*tls.ConnectionState`, providing access to:

- `PeerCertificates[0]` - Client certificate (`*x509.Certificate`)
- `VerifiedChains` - Certificate verification chains
- All standard TLS connection information

## Built-in Mappers

### CommonNameMapper

Maps the certificate Common Name (CN) to username:

```go
mapper := &mqttv5.CommonNameMapper{}

// Certificate with CN="device-001" produces:
// TLSIdentity{Username: "device-001"}
```

### TLSIdentityMapperFunc

Wraps a function as a mapper for custom logic:

```go
mapper := mqttv5.TLSIdentityMapperFunc(
    func(ctx context.Context, state *tls.ConnectionState) (*mqttv5.TLSIdentity, error) {
        if state == nil || len(state.PeerCertificates) == 0 {
            return nil, nil
        }

        cert := state.PeerCertificates[0]
        return &mqttv5.TLSIdentity{
            Username: cert.Subject.CommonName,
        }, nil
    })
```

## Server Configuration

Configure mTLS with `WithTLSIdentityMapper`:

```go
// Create TLS listener with client certificate requirement
tlsConfig := &tls.Config{
    Certificates: []tls.Certificate{serverCert},
    ClientCAs:    caPool,
    ClientAuth:   tls.RequireAndVerifyClientCert,
    MinVersion:   tls.VersionTLS12,
}
listener, _ := tls.Listen("tcp", ":8883", tlsConfig)

// Create server with identity mapper
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.WithTLSIdentityMapper(&mqttv5.CommonNameMapper{}),
    mqttv5.WithServerAuth(&MyAuthenticator{}),
)
```

## AuthContext Fields

When a client connects over TLS, the `AuthContext` provides:

| Field | Type | Description |
|-------|------|-------------|
| `TLSConnectionState` | `*tls.ConnectionState` | Full TLS connection state |
| `TLSIdentity` | `*TLSIdentity` | Mapped identity (if mapper configured) |

Access the client certificate in your authenticator:

```go
func (a *MyAuth) Authenticate(ctx context.Context, c *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
    // Check for TLS connection
    if c.TLSConnectionState == nil {
        return &mqttv5.AuthResult{
            Success:    false,
            ReasonCode: mqttv5.ReasonNotAuthorized,
        }, nil
    }

    // Access client certificate
    if len(c.TLSConnectionState.PeerCertificates) > 0 {
        cert := c.TLSConnectionState.PeerCertificates[0]
        // Full x509.Certificate access:
        // - cert.Subject.CommonName
        // - cert.Subject.Organization
        // - cert.Subject.OrganizationalUnit
        // - cert.DNSNames
        // - cert.EmailAddresses
        // - cert.URIs
        // - cert.NotAfter
        // - cert.Extensions
    }

    // Use pre-mapped identity
    if c.TLSIdentity != nil {
        return &mqttv5.AuthResult{
            Success:   true,
            Namespace: c.TLSIdentity.Namespace,
        }, nil
    }

    return &mqttv5.AuthResult{Success: true}, nil
}
```

## Session Expiry

Set `SessionExpiry` in `AuthResult` to automatically disconnect clients when credentials expire:

```go
func (a *MyAuth) Authenticate(ctx context.Context, c *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
    cert := c.TLSConnectionState.PeerCertificates[0]

    return &mqttv5.AuthResult{
        Success:       true,
        SessionExpiry: cert.NotAfter, // Disconnect when certificate expires
    }, nil
}
```

The server checks credential expiry every 10 seconds and disconnects expired clients with `ReasonAdministrativeAction`.

## Examples

### CN to Username Mapping

```go
mapper := &mqttv5.CommonNameMapper{}

srv := mqttv5.NewServer(
    mqttv5.WithListener(tlsListener),
    mqttv5.WithTLSIdentityMapper(mapper),
)
```

### CN and OU Mapping

Extract username from CN and namespace from Organizational Unit:

```go
mapper := mqttv5.TLSIdentityMapperFunc(func(_ context.Context, state *tls.ConnectionState) (*mqttv5.TLSIdentity, error) {
    if state == nil || len(state.PeerCertificates) == 0 {
        return nil, nil
    }

    cert := state.PeerCertificates[0]
    identity := &mqttv5.TLSIdentity{
        Username: cert.Subject.CommonName,
    }

    if len(cert.Subject.OrganizationalUnit) > 0 {
        identity.Namespace = cert.Subject.OrganizationalUnit[0]
    }

    return identity, nil
})
```

### SPIFFE Identity Mapping

Extract identity from SPIFFE URI in certificate SAN:

```go
mapper := mqttv5.TLSIdentityMapperFunc(func(_ context.Context, state *tls.ConnectionState) (*mqttv5.TLSIdentity, error) {
    if state == nil || len(state.PeerCertificates) == 0 {
        return nil, nil
    }

    cert := state.PeerCertificates[0]
    for _, uri := range cert.URIs {
        if uri.Scheme == "spiffe" {
            return &mqttv5.TLSIdentity{
                Username:  uri.Path,
                Namespace: uri.Host,
            }, nil
        }
    }

    return nil, nil
})
```

### Email-based Identity

Use email address from certificate as username:

```go
mapper := mqttv5.TLSIdentityMapperFunc(func(_ context.Context, state *tls.ConnectionState) (*mqttv5.TLSIdentity, error) {
    if state == nil || len(state.PeerCertificates) == 0 {
        return nil, nil
    }

    cert := state.PeerCertificates[0]
    if len(cert.EmailAddresses) > 0 {
        return &mqttv5.TLSIdentity{
            Username: cert.EmailAddresses[0],
        }, nil
    }

    return nil, nil
})
```

### Full mTLS Authentication

Complete example with certificate validation and session expiry:

```go
type MTLSAuthenticator struct{}

func (a *MTLSAuthenticator) Authenticate(_ context.Context, ctx *mqttv5.AuthContext) (*mqttv5.AuthResult, error) {
    // Require TLS
    if ctx.TLSConnectionState == nil {
        return &mqttv5.AuthResult{
            Success:    false,
            ReasonCode: mqttv5.ReasonNotAuthorized,
        }, nil
    }

    // Require client certificate
    if len(ctx.TLSConnectionState.PeerCertificates) == 0 {
        return &mqttv5.AuthResult{
            Success:    false,
            ReasonCode: mqttv5.ReasonNotAuthorized,
        }, nil
    }

    // Require mapped identity
    if ctx.TLSIdentity == nil {
        return &mqttv5.AuthResult{
            Success:    false,
            ReasonCode: mqttv5.ReasonNotAuthorized,
        }, nil
    }

    cert := ctx.TLSConnectionState.PeerCertificates[0]

    // Use mapped namespace or default
    namespace := ctx.TLSIdentity.Namespace
    if namespace == "" {
        namespace = mqttv5.DefaultNamespace
    }

    return &mqttv5.AuthResult{
        Success:       true,
        Namespace:     namespace,
        SessionExpiry: cert.NotAfter, // Auto-disconnect on cert expiry
    }, nil
}
```

## Client Management

Programmatically manage connected clients:

```go
// Get a specific client
client := srv.GetClient("namespace", "client-id")
if client != nil {
    // Check credential expiry
    if client.IsCredentialExpired() {
        srv.DisconnectClient("namespace", "client-id", mqttv5.ReasonAdministrativeAction)
    }
}
```
