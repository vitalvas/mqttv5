package mqttv5

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommonNameMapper(t *testing.T) {
	mapper := &CommonNameMapper{}
	ctx := context.Background()

	t.Run("nil state returns nil", func(t *testing.T) {
		identity, err := mapper.MapIdentity(ctx, nil)

		assert.NoError(t, err)
		assert.Nil(t, identity)
	})

	t.Run("empty peer certificates returns nil", func(t *testing.T) {
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{},
		}

		identity, err := mapper.MapIdentity(ctx, state)

		assert.NoError(t, err)
		assert.Nil(t, identity)
	})

	t.Run("empty common name returns nil", func(t *testing.T) {
		cert := &x509.Certificate{
			Subject: pkix.Name{
				CommonName: "",
			},
		}
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		identity, err := mapper.MapIdentity(ctx, state)

		assert.NoError(t, err)
		assert.Nil(t, identity)
	})

	t.Run("extracts common name as username", func(t *testing.T) {
		cert := &x509.Certificate{
			Subject: pkix.Name{
				CommonName: "client-device-001",
			},
		}
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		identity, err := mapper.MapIdentity(ctx, state)

		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, "client-device-001", identity.Username)
		assert.Empty(t, identity.Namespace)
	})
}

func TestTLSIdentityMapperFunc(t *testing.T) {
	ctx := context.Background()

	t.Run("wraps function as mapper", func(t *testing.T) {
		called := false
		mapper := TLSIdentityMapperFunc(func(_ context.Context, state *tls.ConnectionState) (*TLSIdentity, error) {
			called = true
			if len(state.PeerCertificates) == 0 {
				return nil, nil
			}
			return &TLSIdentity{
				Username:  "custom-user",
				Namespace: "custom-ns",
			}, nil
		})

		cert := &x509.Certificate{}
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		identity, err := mapper.MapIdentity(ctx, state)

		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.True(t, called)
		assert.Equal(t, "custom-user", identity.Username)
		assert.Equal(t, "custom-ns", identity.Namespace)
	})

	t.Run("handles nil return", func(t *testing.T) {
		mapper := TLSIdentityMapperFunc(func(_ context.Context, _ *tls.ConnectionState) (*TLSIdentity, error) {
			return nil, nil
		})

		state := &tls.ConnectionState{}

		identity, err := mapper.MapIdentity(ctx, state)

		assert.NoError(t, err)
		assert.Nil(t, identity)
	})
}

func TestTLSIdentity(t *testing.T) {
	t.Run("can set fields", func(t *testing.T) {
		identity := &TLSIdentity{
			Username:  "user",
			Namespace: "ns",
		}

		assert.Equal(t, "user", identity.Username)
		assert.Equal(t, "ns", identity.Namespace)
	})
}
