package mqttv5

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateNamespace(t *testing.T) {
	t.Run("valid namespaces", func(t *testing.T) {
		validNamespaces := []string{
			"default",
			"tenant1",
			"my-tenant",
			"acme",
			"acme.corp",
			"my-tenant.example.com",
			"a",
			"1",
			"a1",
			"1a",
			"a-1",
			"tenant-123",
			"sub.domain.example",
			strings.Repeat("a", 63), // max label length
			strings.Repeat("a", 63) + "." + strings.Repeat("b", 63), // multiple max labels
		}

		for _, ns := range validNamespaces {
			err := ValidateNamespace(ns)
			assert.NoError(t, err, "namespace %q should be valid", ns)
		}
	})

	t.Run("empty namespace rejected", func(t *testing.T) {
		err := ValidateNamespace("")
		assert.ErrorIs(t, err, ErrNamespaceEmpty)
	})

	t.Run("too long namespace rejected", func(t *testing.T) {
		longNS := strings.Repeat("a", 254)
		err := ValidateNamespace(longNS)
		assert.ErrorIs(t, err, ErrNamespaceTooLong)
	})

	t.Run("empty label rejected", func(t *testing.T) {
		testCases := []string{
			".tenant",
			"tenant.",
			"tenant..name",
			".",
			"..",
		}
		for _, ns := range testCases {
			err := ValidateNamespace(ns)
			assert.ErrorIs(t, err, ErrNamespaceLabelEmpty, "namespace %q should have empty label error", ns)
		}
	})

	t.Run("label too long rejected", func(t *testing.T) {
		longLabel := strings.Repeat("a", 64)
		err := ValidateNamespace(longLabel)
		assert.ErrorIs(t, err, ErrNamespaceLabelTooLong)

		err = ValidateNamespace("valid." + longLabel)
		assert.ErrorIs(t, err, ErrNamespaceLabelTooLong)
	})

	t.Run("invalid characters rejected", func(t *testing.T) {
		invalidNamespaces := []string{
			"Tenant",       // uppercase
			"TENANT",       // all uppercase
			"tenant_name",  // underscore
			"tenant name",  // space
			"tenant@name",  // special char
			"tenant/name",  // slash
			"tenant\\name", // backslash
			"tenant:name",  // colon
			"tenant!",      // exclamation
			"tenant#",      // hash
			"tenant$",      // dollar
			"tenant%",      // percent
			"tenant&",      // ampersand
			"tenant*",      // asterisk
			"tenant+",      // plus
			"tenant=",      // equals
			"tenant[",      // bracket
			"tenant]",      // bracket
			"tenant{",      // brace
			"tenant}",      // brace
			"tenant|",      // pipe
			"tenant'",      // quote
			"tenant\"",     // double quote
			"tenant<",      // less than
			"tenant>",      // greater than
			"tenant?",      // question mark
			"tenant,",      // comma
		}
		for _, ns := range invalidNamespaces {
			err := ValidateNamespace(ns)
			assert.ErrorIs(t, err, ErrNamespaceInvalidChar, "namespace %q should have invalid char error", ns)
		}
	})

	t.Run("hyphen at start or end rejected", func(t *testing.T) {
		invalidNamespaces := []string{
			"-tenant",
			"tenant-",
			"-",
			"--",
			"valid.-invalid",
			"valid.invalid-",
			"-invalid.valid",
			"invalid-.valid",
		}
		for _, ns := range invalidNamespaces {
			err := ValidateNamespace(ns)
			assert.ErrorIs(t, err, ErrNamespaceInvalidFormat, "namespace %q should have invalid format error", ns)
		}
	})

	t.Run("default namespace constant is valid", func(t *testing.T) {
		err := ValidateNamespace(DefaultNamespace)
		assert.NoError(t, err)
		assert.Equal(t, "default", DefaultNamespace)
	})
}

func TestNamespaceKey(t *testing.T) {
	t.Run("creates composite key", func(t *testing.T) {
		key := NamespaceKey("tenant1", "client1")
		assert.Equal(t, "tenant1||client1", key)
	})

	t.Run("handles empty namespace", func(t *testing.T) {
		key := NamespaceKey("", "client1")
		assert.Equal(t, "||client1", key)
	})

	t.Run("handles empty id", func(t *testing.T) {
		key := NamespaceKey("tenant1", "")
		assert.Equal(t, "tenant1||", key)
	})

	t.Run("handles both empty", func(t *testing.T) {
		key := NamespaceKey("", "")
		assert.Equal(t, "||", key)
	})

	t.Run("default namespace key", func(t *testing.T) {
		key := NamespaceKey(DefaultNamespace, "client1")
		assert.Equal(t, "default||client1", key)
	})
}

func TestParseNamespaceKey(t *testing.T) {
	t.Run("parses composite key", func(t *testing.T) {
		namespace, id := ParseNamespaceKey("tenant1||client1")
		assert.Equal(t, "tenant1", namespace)
		assert.Equal(t, "client1", id)
	})

	t.Run("parses key with empty namespace", func(t *testing.T) {
		namespace, id := ParseNamespaceKey("||client1")
		assert.Equal(t, "", namespace)
		assert.Equal(t, "client1", id)
	})

	t.Run("parses key with empty id", func(t *testing.T) {
		namespace, id := ParseNamespaceKey("tenant1||")
		assert.Equal(t, "tenant1", namespace)
		assert.Equal(t, "", id)
	})

	t.Run("parses key without delimiter", func(t *testing.T) {
		namespace, id := ParseNamespaceKey("client1")
		assert.Equal(t, "", namespace)
		assert.Equal(t, "client1", id)
	})

	t.Run("parses empty key", func(t *testing.T) {
		namespace, id := ParseNamespaceKey("")
		assert.Equal(t, "", namespace)
		assert.Equal(t, "", id)
	})

	t.Run("handles multiple delimiters", func(t *testing.T) {
		namespace, id := ParseNamespaceKey("tenant||client||extra")
		assert.Equal(t, "tenant", namespace)
		assert.Equal(t, "client||extra", id)
	})

	t.Run("roundtrip with NamespaceKey", func(t *testing.T) {
		testCases := []struct {
			namespace string
			id        string
		}{
			{"tenant1", "client1"},
			{DefaultNamespace, "my-client"},
			{"", "client"},
			{"tenant", ""},
			{"a.b.c", "x-y-z"},
		}

		for _, tc := range testCases {
			key := NamespaceKey(tc.namespace, tc.id)
			ns, id := ParseNamespaceKey(key)
			assert.Equal(t, tc.namespace, ns)
			assert.Equal(t, tc.id, id)
		}
	})
}
