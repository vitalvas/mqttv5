package shadow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorResponse(t *testing.T) {
	t.Run("Error", func(t *testing.T) {
		err := &ErrorResponse{Code: 404, Message: "shadow not found"}
		assert.Equal(t, "shadow: 404 shadow not found", err.Error())
		assert.Implements(t, (*error)(nil), err)
	})

	t.Run("error codes", func(t *testing.T) {
		assert.Equal(t, 400, ErrCodeBadRequest)
		assert.Equal(t, 401, ErrCodeUnauthorized)
		assert.Equal(t, 403, ErrCodeForbidden)
		assert.Equal(t, 404, ErrCodeNotFound)
		assert.Equal(t, 409, ErrCodeVersionConflict)
		assert.Equal(t, 413, ErrCodePayloadTooLarge)
		assert.Equal(t, 415, ErrCodeUnsupportedEncoding)
		assert.Equal(t, 429, ErrCodeTooManyRequests)
		assert.Equal(t, 500, ErrCodeInternal)
	})
}
