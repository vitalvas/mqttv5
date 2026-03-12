package filedelivery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStreamTopic(t *testing.T) {
	t.Run("valid describe", func(t *testing.T) {
		p := parseStreamTopic("$things/dev1/streams/fw-update/describe/json")
		require.NotNil(t, p)
		assert.Equal(t, "dev1", p.ClientID)
		assert.Equal(t, "fw-update", p.StreamID)
		assert.Equal(t, suffixDescribe, p.Suffix)
	})

	t.Run("valid description", func(t *testing.T) {
		p := parseStreamTopic("$things/dev1/streams/fw-update/description/json")
		require.NotNil(t, p)
		assert.Equal(t, suffixDescription, p.Suffix)
	})

	t.Run("valid get", func(t *testing.T) {
		p := parseStreamTopic("$things/dev1/streams/fw-update/get/json")
		require.NotNil(t, p)
		assert.Equal(t, suffixGet, p.Suffix)
	})

	t.Run("valid data", func(t *testing.T) {
		p := parseStreamTopic("$things/dev1/streams/fw-update/data/json")
		require.NotNil(t, p)
		assert.Equal(t, suffixData, p.Suffix)
	})

	t.Run("valid rejected", func(t *testing.T) {
		p := parseStreamTopic("$things/dev1/streams/fw-update/rejected/json")
		require.NotNil(t, p)
		assert.Equal(t, suffixRejected, p.Suffix)
	})

	t.Run("stream ID with colons and underscores", func(t *testing.T) {
		p := parseStreamTopic("$things/dev1/streams/fw:v2_update/describe/json")
		require.NotNil(t, p)
		assert.Equal(t, "fw:v2_update", p.StreamID)
	})

	t.Run("stream ID with uppercase", func(t *testing.T) {
		p := parseStreamTopic("$things/dev1/streams/FirmwareUpdate/describe/json")
		require.NotNil(t, p)
		assert.Equal(t, "FirmwareUpdate", p.StreamID)
	})

	t.Run("valid create", func(t *testing.T) {
		p := parseStreamTopic("$things/dev1/streams/fw-update/create/json")
		require.NotNil(t, p)
		assert.Equal(t, suffixCreate, p.Suffix)
	})

	t.Run("valid accepted", func(t *testing.T) {
		p := parseStreamTopic("$things/dev1/streams/fw-update/accepted/json")
		require.NotNil(t, p)
		assert.Equal(t, suffixAccepted, p.Suffix)
	})

	t.Run("not a things topic", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("devices/dev1/streams/s1/describe/json"))
	})

	t.Run("not a streams topic", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev1/shadow/update"))
	})

	t.Run("empty client ID", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things//streams/s1/describe/json"))
	})

	t.Run("empty stream ID", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev1/streams//describe/json"))
	})

	t.Run("invalid suffix", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev1/streams/s1/update/json"))
	})

	t.Run("no suffix", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev1/streams/s1"))
	})

	t.Run("client ID with wildcard +", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev+1/streams/s1/describe/json"))
	})

	t.Run("client ID with wildcard #", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev#1/streams/s1/describe/json"))
	})

	t.Run("client ID with $", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/$dev1/streams/s1/describe/json"))
	})

	t.Run("client ID with space", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev 1/streams/s1/describe/json"))
	})

	t.Run("client ID with path traversal", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/../streams/s1/describe/json"))
	})

	t.Run("client ID single dot", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/./streams/s1/describe/json"))
	})

	t.Run("stream ID with invalid chars", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev1/streams/s1@bad/describe/json"))
	})

	t.Run("stream ID with space", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev1/streams/s 1/describe/json"))
	})

	t.Run("stream ID with dot", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev1/streams/s.1/describe/json"))
	})

	t.Run("stream ID too long", func(t *testing.T) {
		longID := strings.Repeat("a", MaxStreamIDLen+1)
		assert.Nil(t, parseStreamTopic("$things/dev1/streams/"+longID+"/describe/json"))
	})

	t.Run("stream ID at max length", func(t *testing.T) {
		maxID := strings.Repeat("a", MaxStreamIDLen)
		p := parseStreamTopic("$things/dev1/streams/" + maxID + "/describe/json")
		require.NotNil(t, p)
		assert.Equal(t, maxID, p.StreamID)
	})

	t.Run("client ID with unicode", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/\xc3\xa9dev/streams/s1/describe/json"))
	})

	t.Run("stream ID with unicode", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/dev1/streams/\xc3\xa9stream/describe/json"))
	})

	t.Run("empty topic", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic(""))
	})

	t.Run("just prefix", func(t *testing.T) {
		assert.Nil(t, parseStreamTopic("$things/"))
	})
}

func TestBuildTopic(t *testing.T) {
	t.Run("describe", func(t *testing.T) {
		topic := buildTopic("dev1", "fw-update", suffixDescribe)
		assert.Equal(t, "$things/dev1/streams/fw-update/describe/json", topic)
	})

	t.Run("description", func(t *testing.T) {
		topic := buildTopic("dev1", "fw-update", suffixDescription)
		assert.Equal(t, "$things/dev1/streams/fw-update/description/json", topic)
	})

	t.Run("get", func(t *testing.T) {
		topic := buildTopic("dev1", "fw-update", suffixGet)
		assert.Equal(t, "$things/dev1/streams/fw-update/get/json", topic)
	})

	t.Run("data", func(t *testing.T) {
		topic := buildTopic("dev1", "fw-update", suffixData)
		assert.Equal(t, "$things/dev1/streams/fw-update/data/json", topic)
	})

	t.Run("rejected", func(t *testing.T) {
		topic := buildTopic("dev1", "fw-update", suffixRejected)
		assert.Equal(t, "$things/dev1/streams/fw-update/rejected/json", topic)
	})

	t.Run("create", func(t *testing.T) {
		topic := buildTopic("dev1", "fw-update", suffixCreate)
		assert.Equal(t, "$things/dev1/streams/fw-update/create/json", topic)
	})

	t.Run("accepted", func(t *testing.T) {
		topic := buildTopic("dev1", "fw-update", suffixAccepted)
		assert.Equal(t, "$things/dev1/streams/fw-update/accepted/json", topic)
	})
}

func TestBuildTopicRoundTrip(t *testing.T) {
	t.Run("describe round trip", func(t *testing.T) {
		topic := buildTopic("dev1", "fw-update", suffixDescribe)
		p := parseStreamTopic(topic)
		require.NotNil(t, p)
		assert.Equal(t, "dev1", p.ClientID)
		assert.Equal(t, "fw-update", p.StreamID)
		assert.Equal(t, suffixDescribe, p.Suffix)
	})

	t.Run("get round trip", func(t *testing.T) {
		topic := buildTopic("device-abc", "stream:v2_3", suffixGet)
		p := parseStreamTopic(topic)
		require.NotNil(t, p)
		assert.Equal(t, "device-abc", p.ClientID)
		assert.Equal(t, "stream:v2_3", p.StreamID)
		assert.Equal(t, suffixGet, p.Suffix)
	})
}

func TestIsValidIdentifier(t *testing.T) {
	t.Run("valid identifiers", func(t *testing.T) {
		valid := []string{"dev1", "device-abc", "device_123", "my.device.1", "a"}
		for _, s := range valid {
			assert.True(t, isValidIdentifier(s), "expected valid: %q", s)
		}
	})

	t.Run("invalid identifiers", func(t *testing.T) {
		invalid := []string{".", "..", "a+b", "a#b", "$sys", "a b", "a\x00b"}
		for _, s := range invalid {
			assert.False(t, isValidIdentifier(s), "expected invalid: %q", s)
		}
	})
}

func TestIsValidStreamID(t *testing.T) {
	t.Run("valid stream IDs", func(t *testing.T) {
		valid := []string{"abc", "ABC", "123", "a-b", "a_b", "a:b", "fw-v2_update:1"}
		for _, s := range valid {
			assert.True(t, isValidStreamID(s), "expected valid: %q", s)
		}
	})

	t.Run("invalid stream IDs", func(t *testing.T) {
		invalid := []string{"a.b", "a b", "a+b", "a#b", "a/b", "a$b"}
		for _, s := range invalid {
			assert.False(t, isValidStreamID(s), "expected invalid: %q", s)
		}
	})
}

func TestIsValidSuffix(t *testing.T) {
	t.Run("valid suffixes", func(t *testing.T) {
		valid := []string{suffixDescribe, suffixDescription, suffixGet, suffixData, suffixRejected, suffixCreate, suffixAccepted}
		for _, s := range valid {
			assert.True(t, isValidSuffix(s), "expected valid: %q", s)
		}
	})

	t.Run("invalid suffixes", func(t *testing.T) {
		invalid := []string{"update", "delete", "get", "describe", "json", ""}
		for _, s := range invalid {
			assert.False(t, isValidSuffix(s), "expected invalid: %q", s)
		}
	})
}
