package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	t.Run("WithOnPublishError sets callback", func(t *testing.T) {
		h := &Handler{}
		fn := func(string, error) {}
		WithOnPublishError(fn)(h)
		assert.NotNil(t, h.onPublishError)
	})

	t.Run("WithNamespace sets namespace", func(t *testing.T) {
		h := &Handler{}
		assert.Empty(t, h.namespace)

		WithNamespace("custom-ns")(h)
		assert.Equal(t, "custom-ns", h.namespace)

		WithNamespace("")(h)
		assert.Empty(t, h.namespace)
	})

	t.Run("WithClientNamespace enables flag", func(t *testing.T) {
		h := &Handler{}
		assert.False(t, h.useClientNamespace)

		WithClientNamespace(true)(h)
		assert.True(t, h.useClientNamespace)

		WithClientNamespace(false)(h)
		assert.False(t, h.useClientNamespace)
	})
}
