package shadow

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseShadowTopic(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		expected *parsedTopic
	}{
		{
			name:  "classic update",
			topic: "$things/myDevice/shadow/update",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "update",
			},
		},
		{
			name:  "classic get",
			topic: "$things/myDevice/shadow/get",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "get",
			},
		},
		{
			name:  "classic delete",
			topic: "$things/myDevice/shadow/delete",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "delete",
			},
		},
		{
			name:  "classic update/accepted",
			topic: "$things/myDevice/shadow/update/accepted",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "update/accepted",
			},
		},
		{
			name:  "classic update/rejected",
			topic: "$things/myDevice/shadow/update/rejected",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "update/rejected",
			},
		},
		{
			name:  "classic update/delta",
			topic: "$things/myDevice/shadow/update/delta",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "update/delta",
			},
		},
		{
			name:  "classic update/documents",
			topic: "$things/myDevice/shadow/update/documents",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "update/documents",
			},
		},
		{
			name:  "classic get/accepted",
			topic: "$things/myDevice/shadow/get/accepted",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "get/accepted",
			},
		},
		{
			name:  "classic get/rejected",
			topic: "$things/myDevice/shadow/get/rejected",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "get/rejected",
			},
		},
		{
			name:  "classic delete/accepted",
			topic: "$things/myDevice/shadow/delete/accepted",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "delete/accepted",
			},
		},
		{
			name:  "classic delete/rejected",
			topic: "$things/myDevice/shadow/delete/rejected",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "delete/rejected",
			},
		},
		{
			name:  "named shadow update",
			topic: "$things/myDevice/shadow/name/config/update",
			expected: &parsedTopic{
				ClientID:   "myDevice",
				ShadowName: "config",
				Suffix:     "update",
			},
		},
		{
			name:  "named shadow get/accepted",
			topic: "$things/myDevice/shadow/name/config/get/accepted",
			expected: &parsedTopic{
				ClientID:   "myDevice",
				ShadowName: "config",
				Suffix:     "get/accepted",
			},
		},
		{
			name:     "not a shadow topic",
			topic:    "devices/myDevice/data",
			expected: nil,
		},
		{
			name:     "missing client ID",
			topic:    "$things//shadow/update",
			expected: nil,
		},
		{
			name:     "empty suffix",
			topic:    "$things/myDevice/shadow/",
			expected: nil,
		},
		{
			name:     "invalid suffix",
			topic:    "$things/myDevice/shadow/unknown",
			expected: nil,
		},
		{
			name:     "named shadow missing name",
			topic:    "$things/myDevice/shadow/name//update",
			expected: nil,
		},
		{
			name:     "named shadow missing suffix",
			topic:    "$things/myDevice/shadow/name/config",
			expected: nil,
		},
		{
			name:     "no shadow infix",
			topic:    "$things/myDevice/other/update",
			expected: nil,
		},
		{
			name:     "empty topic",
			topic:    "",
			expected: nil,
		},

		// Non-shadow and adversarial topics
		{
			name:     "regular user topic",
			topic:    "sensors/temperature/living-room",
			expected: nil,
		},
		{
			name:     "sys topic",
			topic:    "$SYS/broker/clients/connected",
			expected: nil,
		},
		{
			name:     "similar prefix but wrong",
			topic:    "$thing/myDevice/shadow/update",
			expected: nil,
		},
		{
			name:     "extra prefix segment",
			topic:    "prefix/$things/myDevice/shadow/update",
			expected: nil,
		},
		{
			name:     "path traversal in clientID",
			topic:    "$things/../admin/shadow/update",
			expected: nil,
		},
		{
			name:     "path traversal in shadow name",
			topic:    "$things/myDevice/shadow/name/../update",
			expected: nil,
		},
		{
			name:     "wildcard plus in clientID",
			topic:    "$things/+/shadow/update",
			expected: nil,
		},
		{
			name:     "wildcard hash in clientID",
			topic:    "$things/#/shadow/update",
			expected: nil,
		},
		{
			name:     "wildcard plus in shadow name",
			topic:    "$things/myDevice/shadow/name/+/update",
			expected: nil,
		},
		{
			name:     "wildcard hash at end",
			topic:    "$things/myDevice/shadow/#",
			expected: nil,
		},
		{
			name:     "extra segments after valid suffix",
			topic:    "$things/myDevice/shadow/update/accepted/extra",
			expected: nil,
		},
		{
			name:     "extra segments after action suffix",
			topic:    "$things/myDevice/shadow/get/extra",
			expected: nil,
		},
		{
			name:     "double shadow infix",
			topic:    "$things/myDevice/shadow/shadow/update",
			expected: nil,
		},
		{
			name:     "only prefix",
			topic:    "$things/",
			expected: nil,
		},
		{
			name:     "only prefix no slash",
			topic:    "$things",
			expected: nil,
		},
		{
			name:     "clientID with spaces",
			topic:    "$things/my device/shadow/update",
			expected: nil,
		},
		{
			name:     "null byte in topic",
			topic:    "$things/my\x00Device/shadow/update",
			expected: nil,
		},
		{
			name:     "unicode clientID",
			topic:    "$things/\u0434\u0435\u0432\u0430\u0439\u0441/shadow/update",
			expected: nil,
		},
		{
			name:     "named shadow with extra name segments",
			topic:    "$things/myDevice/shadow/name/a/b/update",
			expected: nil,
		},
		{
			name:     "without dollar prefix",
			topic:    "things/myDevice/shadow/update",
			expected: nil,
		},
		{
			name:     "case sensitive prefix",
			topic:    "$Things/myDevice/shadow/update",
			expected: nil,
		},
		{
			name:     "case sensitive shadow infix",
			topic:    "$things/myDevice/Shadow/update",
			expected: nil,
		},
		{
			name:     "trailing slash",
			topic:    "$things/myDevice/shadow/update/",
			expected: nil,
		},
		{
			name:     "dollar in clientID rejected",
			topic:    "$things/$shared/shadow/update",
			expected: nil,
		},
		{
			name:     "dollar prefix in clientID",
			topic:    "$things/$device/shadow/update",
			expected: nil,
		},

		// Shared shadow topics
		{
			name:  "shared classic update",
			topic: "$things/$shared/room-101/shadow/update",
			expected: &parsedTopic{
				GroupName: "room-101",
				Suffix:    "update",
			},
		},
		{
			name:  "shared classic get",
			topic: "$things/$shared/room-101/shadow/get",
			expected: &parsedTopic{
				GroupName: "room-101",
				Suffix:    "get",
			},
		},
		{
			name:  "shared classic delete",
			topic: "$things/$shared/room-101/shadow/delete",
			expected: &parsedTopic{
				GroupName: "room-101",
				Suffix:    "delete",
			},
		},
		{
			name:  "shared classic get/accepted",
			topic: "$things/$shared/room-101/shadow/get/accepted",
			expected: &parsedTopic{
				GroupName: "room-101",
				Suffix:    "get/accepted",
			},
		},
		{
			name:  "shared named update",
			topic: "$things/$shared/room-101/shadow/name/config/update",
			expected: &parsedTopic{
				GroupName:  "room-101",
				ShadowName: "config",
				Suffix:     "update",
			},
		},
		{
			name:  "shared named get/accepted",
			topic: "$things/$shared/room-101/shadow/name/config/get/accepted",
			expected: &parsedTopic{
				GroupName:  "room-101",
				ShadowName: "config",
				Suffix:     "get/accepted",
			},
		},
		{
			name:  "shared named update/delta",
			topic: "$things/$shared/room-101/shadow/name/config/update/delta",
			expected: &parsedTopic{
				GroupName:  "room-101",
				ShadowName: "config",
				Suffix:     "update/delta",
			},
		},
		{
			name:     "shared missing group name",
			topic:    "$things/$shared//shadow/update",
			expected: nil,
		},
		{
			name:     "shared group name with dollar",
			topic:    "$things/$shared/$room/shadow/update",
			expected: nil,
		},
		{
			name:     "shared missing suffix",
			topic:    "$things/$shared/room-101/shadow/",
			expected: nil,
		},
		{
			name:     "shared invalid suffix",
			topic:    "$things/$shared/room-101/shadow/unknown",
			expected: nil,
		},
		{
			name:     "shared named missing shadow name",
			topic:    "$things/$shared/room-101/shadow/name//update",
			expected: nil,
		},
		{
			name:     "shared group name with wildcard",
			topic:    "$things/$shared/+/shadow/update",
			expected: nil,
		},
		{
			name:     "shared no shadow infix",
			topic:    "$things/$shared/room-101/other/update",
			expected: nil,
		},

		// Shadow name character validation (only [a-zA-Z0-9:_-] allowed)
		{
			name:  "shadow name with letters and digits",
			topic: "$things/myDevice/shadow/name/config123/update",
			expected: &parsedTopic{
				ClientID:   "myDevice",
				ShadowName: "config123",
				Suffix:     "update",
			},
		},
		{
			name:  "shadow name with colons",
			topic: "$things/myDevice/shadow/name/fw:v2/update",
			expected: &parsedTopic{
				ClientID:   "myDevice",
				ShadowName: "fw:v2",
				Suffix:     "update",
			},
		},
		{
			name:  "shadow name with underscores",
			topic: "$things/myDevice/shadow/name/my_config/update",
			expected: &parsedTopic{
				ClientID:   "myDevice",
				ShadowName: "my_config",
				Suffix:     "update",
			},
		},
		{
			name:  "shadow name with hyphens",
			topic: "$things/myDevice/shadow/name/my-config/update",
			expected: &parsedTopic{
				ClientID:   "myDevice",
				ShadowName: "my-config",
				Suffix:     "update",
			},
		},
		{
			name:  "shadow name with uppercase",
			topic: "$things/myDevice/shadow/name/MyConfig/update",
			expected: &parsedTopic{
				ClientID:   "myDevice",
				ShadowName: "MyConfig",
				Suffix:     "update",
			},
		},
		{
			name:  "shadow name mixed valid chars",
			topic: "$things/myDevice/shadow/name/My_Config-v2:latest/update",
			expected: &parsedTopic{
				ClientID:   "myDevice",
				ShadowName: "My_Config-v2:latest",
				Suffix:     "update",
			},
		},
		{
			name:     "shadow name with dot rejected",
			topic:    "$things/myDevice/shadow/name/my.config/update",
			expected: nil,
		},
		{
			name:     "shadow name with at sign rejected",
			topic:    "$things/myDevice/shadow/name/my@config/update",
			expected: nil,
		},
		{
			name:     "shadow name with equals rejected",
			topic:    "$things/myDevice/shadow/name/key=value/update",
			expected: nil,
		},
		{
			name:     "shadow name with space rejected",
			topic:    "$things/myDevice/shadow/name/my config/update",
			expected: nil,
		},
		{
			name:     "shadow name with tilde rejected",
			topic:    "$things/myDevice/shadow/name/my~config/update",
			expected: nil,
		},
		{
			name:     "shadow name with percent rejected",
			topic:    "$things/myDevice/shadow/name/my%config/update",
			expected: nil,
		},
		{
			name:     "shared shadow name with dot rejected",
			topic:    "$things/$shared/room-101/shadow/name/my.config/update",
			expected: nil,
		},

		// Shadow name length limits
		{
			name:  "shadow name at 64 bytes",
			topic: "$things/myDevice/shadow/name/" + strings.Repeat("a", 64) + "/update",
			expected: &parsedTopic{
				ClientID:   "myDevice",
				ShadowName: strings.Repeat("a", 64),
				Suffix:     "update",
			},
		},
		{
			name:     "shadow name exceeds 64 bytes",
			topic:    "$things/myDevice/shadow/name/" + strings.Repeat("a", 65) + "/update",
			expected: nil,
		},
		{
			name:     "shared shadow name exceeds 64 bytes",
			topic:    "$things/$shared/room-101/shadow/name/" + strings.Repeat("a", 65) + "/update",
			expected: nil,
		},

		// List topics
		{
			name:  "classic list",
			topic: "$things/myDevice/shadow/list",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "list",
			},
		},
		{
			name:  "classic list/accepted",
			topic: "$things/myDevice/shadow/list/accepted",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "list/accepted",
			},
		},
		{
			name:  "classic list/rejected",
			topic: "$things/myDevice/shadow/list/rejected",
			expected: &parsedTopic{
				ClientID: "myDevice",
				Suffix:   "list/rejected",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseShadowTopic(tt.topic)
			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				assert.Equal(t, tt.expected.ClientID, result.ClientID)
				assert.Equal(t, tt.expected.GroupName, result.GroupName)
				assert.Equal(t, tt.expected.ShadowName, result.ShadowName)
				assert.Equal(t, tt.expected.Suffix, result.Suffix)
			}
		})
	}
}

func TestBuildTopic(t *testing.T) {
	tests := []struct {
		name       string
		clientID   string
		shadowName string
		suffix     string
		expected   string
	}{
		{
			name:     "classic update",
			clientID: "myDevice",
			suffix:   "update",
			expected: "$things/myDevice/shadow/update",
		},
		{
			name:     "classic get/accepted",
			clientID: "myDevice",
			suffix:   "get/accepted",
			expected: "$things/myDevice/shadow/get/accepted",
		},
		{
			name:       "named shadow update",
			clientID:   "myDevice",
			shadowName: "config",
			suffix:     "update",
			expected:   "$things/myDevice/shadow/name/config/update",
		},
		{
			name:       "named shadow update/delta",
			clientID:   "myDevice",
			shadowName: "config",
			suffix:     "update/delta",
			expected:   "$things/myDevice/shadow/name/config/update/delta",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildTopic(tt.clientID, tt.shadowName, tt.suffix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildSharedTopic(t *testing.T) {
	tests := []struct {
		name       string
		groupName  string
		shadowName string
		suffix     string
		expected   string
	}{
		{
			name:      "shared classic update",
			groupName: "room-101",
			suffix:    "update",
			expected:  "$things/$shared/room-101/shadow/update",
		},
		{
			name:      "shared classic get/accepted",
			groupName: "room-101",
			suffix:    "get/accepted",
			expected:  "$things/$shared/room-101/shadow/get/accepted",
		},
		{
			name:       "shared named update",
			groupName:  "room-101",
			shadowName: "config",
			suffix:     "update",
			expected:   "$things/$shared/room-101/shadow/name/config/update",
		},
		{
			name:       "shared named update/delta",
			groupName:  "room-101",
			shadowName: "config",
			suffix:     "update/delta",
			expected:   "$things/$shared/room-101/shadow/name/config/update/delta",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildSharedTopic(tt.groupName, tt.shadowName, tt.suffix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildSharedTopic_RoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		groupName  string
		shadowName string
		suffix     string
	}{
		{name: "classic update", groupName: "room-101", suffix: "update"},
		{name: "classic get/accepted", groupName: "room-101", suffix: "get/accepted"},
		{name: "named update", groupName: "room-101", shadowName: "config", suffix: "update"},
		{name: "named update/delta", groupName: "room-101", shadowName: "config", suffix: "update/delta"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := buildSharedTopic(tt.groupName, tt.shadowName, tt.suffix)
			parsed := parseShadowTopic(topic)
			require.NotNil(t, parsed)
			assert.Equal(t, tt.groupName, parsed.GroupName)
			assert.Equal(t, tt.shadowName, parsed.ShadowName)
			assert.Equal(t, tt.suffix, parsed.Suffix)
			assert.Empty(t, parsed.ClientID)
		})
	}
}

func TestResponseTopic(t *testing.T) {
	t.Run("per-device topic", func(t *testing.T) {
		parsed := &parsedTopic{ClientID: "dev1", ShadowName: "config"}
		result := responseTopic(parsed, "update/accepted")
		assert.Equal(t, "$things/dev1/shadow/name/config/update/accepted", result)
	})

	t.Run("shared topic", func(t *testing.T) {
		parsed := &parsedTopic{GroupName: "room-101", ShadowName: "config"}
		result := responseTopic(parsed, "update/accepted")
		assert.Equal(t, "$things/$shared/room-101/shadow/name/config/update/accepted", result)
	})

	t.Run("shared classic topic", func(t *testing.T) {
		parsed := &parsedTopic{GroupName: "room-101"}
		result := responseTopic(parsed, "get/accepted")
		assert.Equal(t, "$things/$shared/room-101/shadow/get/accepted", result)
	})
}

func TestIsValidShadowName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"lowercase", "config", true},
		{"uppercase", "CONFIG", true},
		{"digits", "123", true},
		{"mixed", "My_Config-v2:latest", true},
		{"hyphen", "my-shadow", true},
		{"underscore", "my_shadow", true},
		{"colon", "fw:v2", true},
		{"single char", "a", true},
		{"dot rejected", "my.config", false},
		{"at sign rejected", "my@config", false},
		{"equals rejected", "key=value", false},
		{"space rejected", "my config", false},
		{"tilde rejected", "my~config", false},
		{"percent rejected", "my%config", false},
		{"dollar rejected", "$config", false},
		{"hash rejected", "config#1", false},
		{"plus rejected", "config+1", false},
		{"slash rejected", "a/b", false},
		{"backslash rejected", `a\b`, false},
		{"exclamation rejected", "hello!", false},
		{"curly brace rejected", "cfg{1}", false},
		{"empty string", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isValidShadowName(tt.input))
		})
	}
}
