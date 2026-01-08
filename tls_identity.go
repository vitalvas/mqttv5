package mqttv5

import (
	"context"
	"crypto/tls"
)

// TLSIdentity represents the identity extracted from a TLS certificate.
type TLSIdentity struct {
	// Username is the mapped username/principal from the certificate.
	Username string

	// Namespace is the tenant namespace extracted from the certificate.
	Namespace string

	// Attributes contains additional identity attributes from the certificate.
	Attributes map[string]string
}

// TLSIdentityMapper maps TLS certificate information to application identity.
type TLSIdentityMapper interface {
	// MapIdentity extracts identity from TLS connection state.
	// The peer certificate is the client certificate (first in PeerCertificates).
	// Returns nil, nil if no identity should be mapped.
	MapIdentity(ctx context.Context, state *tls.ConnectionState) (*TLSIdentity, error)
}

// CommonNameMapper maps the certificate CN to username.
type CommonNameMapper struct{}

// MapIdentity extracts the CommonName from the peer certificate as username.
func (m *CommonNameMapper) MapIdentity(_ context.Context, state *tls.ConnectionState) (*TLSIdentity, error) {
	if state == nil || len(state.PeerCertificates) == 0 {
		return nil, nil
	}

	cert := state.PeerCertificates[0]
	if cert.Subject.CommonName == "" {
		return nil, nil
	}

	return &TLSIdentity{Username: cert.Subject.CommonName}, nil
}

// TLSIdentityMapperFunc is a function type that implements TLSIdentityMapper.
type TLSIdentityMapperFunc func(ctx context.Context, state *tls.ConnectionState) (*TLSIdentity, error)

// MapIdentity calls the underlying function.
func (f TLSIdentityMapperFunc) MapIdentity(ctx context.Context, state *tls.ConnectionState) (*TLSIdentity, error) {
	return f(ctx, state)
}
