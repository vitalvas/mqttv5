package shadow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputeDelta(t *testing.T) {
	tests := []struct {
		name     string
		desired  map[string]any
		reported map[string]any
		expected map[string]any
	}{
		{
			name:     "identical states",
			desired:  map[string]any{"temp": 22.0},
			reported: map[string]any{"temp": 22.0},
			expected: nil,
		},
		{
			name:     "different values",
			desired:  map[string]any{"temp": 22.0},
			reported: map[string]any{"temp": 20.0},
			expected: map[string]any{"temp": 22.0},
		},
		{
			name:     "key in desired missing from reported",
			desired:  map[string]any{"temp": 22.0, "humidity": 50.0},
			reported: map[string]any{"temp": 22.0},
			expected: map[string]any{"humidity": 50.0},
		},
		{
			name:     "extra key in reported ignored",
			desired:  map[string]any{"temp": 22.0},
			reported: map[string]any{"temp": 22.0, "humidity": 50.0},
			expected: nil,
		},
		{
			name:     "nil desired",
			desired:  nil,
			reported: map[string]any{"temp": 22.0},
			expected: nil,
		},
		{
			name:     "nil reported",
			desired:  map[string]any{"temp": 22.0},
			reported: nil,
			expected: map[string]any{"temp": 22.0},
		},
		{
			name:     "both nil",
			desired:  nil,
			reported: nil,
			expected: nil,
		},
		{
			name:     "empty desired",
			desired:  map[string]any{},
			reported: map[string]any{"temp": 22.0},
			expected: nil,
		},
		{
			name:     "multiple differences",
			desired:  map[string]any{"a": 1, "b": 2, "c": 3},
			reported: map[string]any{"a": 1, "b": 99, "c": 3},
			expected: map[string]any{"b": 2},
		},

		// Recursive nested diff
		{
			name: "nested map only differing leaf",
			desired: map[string]any{
				"network": map[string]any{"dns": "8.8.8.8", "gw": "10.0.0.1"},
			},
			reported: map[string]any{
				"network": map[string]any{"dns": "8.8.8.8", "gw": "10.0.0.2"},
			},
			expected: map[string]any{
				"network": map[string]any{"gw": "10.0.0.1"},
			},
		},
		{
			name: "nested map fully matching",
			desired: map[string]any{
				"config": map[string]any{"mode": "auto"},
			},
			reported: map[string]any{
				"config": map[string]any{"mode": "auto"},
			},
			expected: nil,
		},
		{
			name: "nested map key missing in reported",
			desired: map[string]any{
				"config": map[string]any{"mode": "auto", "interval": 10},
			},
			reported: map[string]any{
				"config": map[string]any{"mode": "auto"},
			},
			expected: map[string]any{
				"config": map[string]any{"interval": 10},
			},
		},
		{
			name: "deeply nested diff",
			desired: map[string]any{
				"lights": map[string]any{
					"color": map[string]any{"r": 255, "g": 255, "b": 255},
				},
			},
			reported: map[string]any{
				"lights": map[string]any{
					"color": map[string]any{"r": 255, "g": 0, "b": 255},
				},
			},
			expected: map[string]any{
				"lights": map[string]any{
					"color": map[string]any{"g": 255},
				},
			},
		},
		{
			name: "mixed nested and flat",
			desired: map[string]any{
				"temp":   22.0,
				"config": map[string]any{"mode": "auto", "fan": "high"},
			},
			reported: map[string]any{
				"temp":   22.0,
				"config": map[string]any{"mode": "auto", "fan": "low"},
			},
			expected: map[string]any{
				"config": map[string]any{"fan": "high"},
			},
		},
		{
			name: "desired map reported scalar",
			desired: map[string]any{
				"config": map[string]any{"mode": "auto"},
			},
			reported: map[string]any{
				"config": "flat-string",
			},
			expected: map[string]any{
				"config": map[string]any{"mode": "auto"},
			},
		},
		{
			name: "desired scalar reported map",
			desired: map[string]any{
				"config": "flat-string",
			},
			reported: map[string]any{
				"config": map[string]any{"mode": "auto"},
			},
			expected: map[string]any{
				"config": "flat-string",
			},
		},
		{
			name: "nested map absent from reported entirely",
			desired: map[string]any{
				"network": map[string]any{"dns": "8.8.8.8"},
			},
			reported: map[string]any{},
			expected: map[string]any{
				"network": map[string]any{"dns": "8.8.8.8"},
			},
		},
		{
			name: "array atomic comparison different",
			desired: map[string]any{
				"tags": []any{"a", "b", "c"},
			},
			reported: map[string]any{
				"tags": []any{"a", "b"},
			},
			expected: map[string]any{
				"tags": []any{"a", "b", "c"},
			},
		},
		{
			name: "array atomic comparison same",
			desired: map[string]any{
				"tags": []any{"a", "b"},
			},
			reported: map[string]any{
				"tags": []any{"a", "b"},
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeDelta(tt.desired, tt.reported)
			assert.Equal(t, tt.expected, result)
		})
	}
}
