package shadow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildMetadataForValue(t *testing.T) {
	t.Run("scalar value", func(t *testing.T) {
		result := buildMetadataForValue(22.0, 1000)
		expected := map[string]any{"timestamp": int64(1000)}
		assert.Equal(t, expected, result)
	})

	t.Run("string value", func(t *testing.T) {
		result := buildMetadataForValue("hello", 1000)
		expected := map[string]any{"timestamp": int64(1000)}
		assert.Equal(t, expected, result)
	})

	t.Run("array value", func(t *testing.T) {
		result := buildMetadataForValue([]any{"a", "b"}, 1000)
		expected := map[string]any{"timestamp": int64(1000)}
		assert.Equal(t, expected, result)
	})

	t.Run("map value", func(t *testing.T) {
		result := buildMetadataForValue(map[string]any{"dns": "8.8.8.8", "gw": "10.0.0.1"}, 1000)
		expected := map[string]any{
			"dns": map[string]any{"timestamp": int64(1000)},
			"gw":  map[string]any{"timestamp": int64(1000)},
		}
		assert.Equal(t, expected, result)
	})

	t.Run("nested map value", func(t *testing.T) {
		result := buildMetadataForValue(map[string]any{
			"color": map[string]any{"r": 255.0, "g": 0.0},
		}, 1000)
		expected := map[string]any{
			"color": map[string]any{
				"r": map[string]any{"timestamp": int64(1000)},
				"g": map[string]any{"timestamp": int64(1000)},
			},
		}
		assert.Equal(t, expected, result)
	})
}

func TestFilterMetadataForDelta(t *testing.T) {
	t.Run("nil delta", func(t *testing.T) {
		assert.Nil(t, filterMetadataForDelta(nil, map[string]any{"temp": map[string]any{"timestamp": int64(1000)}}))
	})

	t.Run("nil metadata", func(t *testing.T) {
		assert.Nil(t, filterMetadataForDelta(map[string]any{"temp": 22.0}, nil))
	})

	t.Run("filters to delta keys only", func(t *testing.T) {
		delta := map[string]any{"temp": 22.0}
		desiredMeta := map[string]any{
			"temp": map[string]any{"timestamp": int64(1000)},
			"mode": map[string]any{"timestamp": int64(1000)},
		}
		result := filterMetadataForDelta(delta, desiredMeta)
		expected := map[string]any{
			"temp": map[string]any{"timestamp": int64(1000)},
		}
		assert.Equal(t, expected, result)
	})

	t.Run("nested delta", func(t *testing.T) {
		delta := map[string]any{
			"network": map[string]any{"gw": "10.0.0.1"},
		}
		desiredMeta := map[string]any{
			"network": map[string]any{
				"dns": map[string]any{"timestamp": int64(1000)},
				"gw":  map[string]any{"timestamp": int64(1000)},
			},
		}
		result := filterMetadataForDelta(delta, desiredMeta)
		expected := map[string]any{
			"network": map[string]any{
				"gw": map[string]any{"timestamp": int64(1000)},
			},
		}
		assert.Equal(t, expected, result)
	})

	t.Run("delta key not in metadata", func(t *testing.T) {
		delta := map[string]any{"newkey": 1}
		desiredMeta := map[string]any{
			"temp": map[string]any{"timestamp": int64(1000)},
		}
		assert.Nil(t, filterMetadataForDelta(delta, desiredMeta))
	})
}

func TestBuildMetadata(t *testing.T) {
	t.Run("nil state", func(t *testing.T) {
		assert.Nil(t, buildMetadata(nil, 1000))
	})

	t.Run("empty state", func(t *testing.T) {
		assert.Nil(t, buildMetadata(map[string]any{}, 1000))
	})

	t.Run("flat state", func(t *testing.T) {
		result := buildMetadata(map[string]any{"temp": 22.0, "mode": "cool"}, 1000)
		expected := map[string]any{
			"temp": map[string]any{"timestamp": int64(1000)},
			"mode": map[string]any{"timestamp": int64(1000)},
		}
		assert.Equal(t, expected, result)
	})

	t.Run("nested state", func(t *testing.T) {
		result := buildMetadata(map[string]any{
			"network": map[string]any{"dns": "8.8.8.8"},
			"temp":    22.0,
		}, 1000)
		expected := map[string]any{
			"network": map[string]any{
				"dns": map[string]any{"timestamp": int64(1000)},
			},
			"temp": map[string]any{"timestamp": int64(1000)},
		}
		assert.Equal(t, expected, result)
	})

	t.Run("skips nil values", func(t *testing.T) {
		result := buildMetadata(map[string]any{"temp": 22.0, "mode": nil}, 1000)
		expected := map[string]any{
			"temp": map[string]any{"timestamp": int64(1000)},
		}
		assert.Equal(t, expected, result)
	})

	t.Run("all nil values returns nil", func(t *testing.T) {
		result := buildMetadata(map[string]any{"a": nil, "b": nil}, 1000)
		assert.Nil(t, result)
	})
}
