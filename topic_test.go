package mqttv5

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateTopicName(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr error
	}{
		{"valid simple", "test", nil},
		{"valid with slash", "test/topic", nil},
		{"valid with multiple levels", "a/b/c/d", nil},
		{"valid starting with slash", "/test", nil},
		{"valid ending with slash", "test/", nil},
		{"valid UTF-8", "sensor/temperatur/C", nil},
		{"empty", "", ErrEmptyTopic},
		{"contains +", "test/+/topic", ErrInvalidTopicName},
		{"contains #", "test/#", ErrInvalidTopicName},
		{"contains null", "test\x00topic", ErrInvalidTopicName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTopicName(tt.topic)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateTopicFilter(t *testing.T) {
	tests := []struct {
		name    string
		filter  string
		wantErr error
	}{
		{"valid simple", "test", nil},
		{"valid with slash", "test/topic", nil},
		{"valid single wildcard", "+", nil},
		{"valid single wildcard in middle", "test/+/topic", nil},
		{"valid multi wildcard", "#", nil},
		{"valid multi wildcard at end", "test/#", nil},
		{"valid multi level with single", "+/+/+", nil},
		{"valid combined wildcards", "+/test/#", nil},
		{"empty", "", ErrEmptyTopic},
		{"invalid + not alone", "test+", ErrInvalidTopicFilter},
		{"invalid + mixed", "te+st", ErrInvalidTopicFilter},
		{"invalid # not alone", "test#", ErrInvalidTopicFilter},
		{"invalid # not at end", "#/test", ErrInvalidTopicFilter},
		{"invalid # in middle", "test/#/more", ErrInvalidTopicFilter},
		{"contains null", "test\x00filter", ErrInvalidTopicFilter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTopicFilter(tt.filter)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTopicMatch(t *testing.T) {
	tests := []struct {
		filter string
		topic  string
		match  bool
	}{
		// Exact matches
		{"test", "test", true},
		{"test/topic", "test/topic", true},
		{"a/b/c", "a/b/c", true},

		// Non-matches
		{"test", "other", false},
		{"test/topic", "test/other", false},
		{"a/b", "a/b/c", false},
		{"a/b/c", "a/b", false},

		// Single-level wildcard
		{"+", "test", true},
		{"test/+", "test/topic", true},
		{"+/topic", "test/topic", true},
		{"test/+/end", "test/middle/end", true},
		{"+/+/+", "a/b/c", true},
		{"+", "test/topic", false},
		{"test/+", "test", false},

		// Multi-level wildcard
		{"#", "test", true},
		{"#", "test/topic", true},
		{"#", "a/b/c/d/e", true},
		{"test/#", "test", true},
		{"test/#", "test/topic", true},
		{"test/#", "test/a/b/c", true},
		{"test/topic/#", "test/topic", true},
		{"test/topic/#", "test/topic/more", true},

		// Combined wildcards
		{"+/#", "test", true},
		{"+/#", "test/topic", true},
		{"+/+/#", "a/b/c/d", true},

		// System topics
		{"$SYS/test", "$SYS/test", true},
		{"#", "$SYS/test", false},      // # doesn't match $ at root
		{"+/test", "$SYS/test", false}, // + doesn't match $ at root
		{"$SYS/#", "$SYS/test", true},  // Explicit $SYS matches
		{"$SYS/+", "$SYS/test", true},  // Explicit $SYS matches

		// Empty
		{"", "test", false},
		{"test", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.filter+"_"+tt.topic, func(t *testing.T) {
			result := TopicMatch(tt.filter, tt.topic)
			assert.Equal(t, tt.match, result)
		})
	}
}

func TestIsSystemTopic(t *testing.T) {
	tests := []struct {
		topic    string
		isSystem bool
	}{
		{"$SYS", true},
		{"$SYS/broker/uptime", true},
		{"$SYS/clients/connected", true},
		{"test/topic", false},
		{"$OTHER/test", false},
		{"$share/group/topic", false},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			assert.Equal(t, tt.isSystem, IsSystemTopic(tt.topic))
		})
	}
}

func TestParseSharedSubscription(t *testing.T) {
	tests := []struct {
		name        string
		filter      string
		shareName   string
		topicFilter string
		wantErr     bool
	}{
		{"valid basic", "$share/consumer1/topic", "consumer1", "topic", false},
		{"valid with levels", "$share/group/a/b/c", "group", "a/b/c", false},
		{"valid with wildcard", "$share/group/sensor/+/data", "group", "sensor/+/data", false},
		{"valid with multi wildcard", "$share/group/#", "group", "#", false},
		{"not shared", "normal/topic", "", "", false},
		{"missing share name", "$share//topic", "", "", true},
		{"missing topic filter", "$share/group/", "", "", true},
		{"missing topic filter 2", "$share/group", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseSharedSubscription(tt.filter)
			switch {
			case tt.wantErr:
				assert.Error(t, err)
			case tt.shareName == "":
				assert.NoError(t, err)
				assert.Nil(t, result)
			default:
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, tt.shareName, result.ShareName)
				assert.Equal(t, tt.topicFilter, result.TopicFilter)
			}
		})
	}
}

func TestTopicMatcher(t *testing.T) {
	matcher := NewTopicMatcher()

	// Subscribe
	err := matcher.Subscribe("sensor/+/temperature", "sub1")
	require.NoError(t, err)

	err = matcher.Subscribe("sensor/#", "sub2")
	require.NoError(t, err)

	err = matcher.Subscribe("sensor/living/temperature", "sub3")
	require.NoError(t, err)

	// Match
	subscribers := matcher.Match("sensor/living/temperature")
	assert.Len(t, subscribers, 3)
	assert.Contains(t, subscribers, "sub1")
	assert.Contains(t, subscribers, "sub2")
	assert.Contains(t, subscribers, "sub3")

	// Match with only wildcard
	subscribers = matcher.Match("sensor/bedroom/humidity")
	assert.Len(t, subscribers, 1)
	assert.Contains(t, subscribers, "sub2")

	// Unsubscribe
	err = matcher.Unsubscribe("sensor/+/temperature", "sub1")
	require.NoError(t, err)

	subscribers = matcher.Match("sensor/living/temperature")
	assert.Len(t, subscribers, 2)
	assert.NotContains(t, subscribers, "sub1")
}

func TestTopicMatcherSystemTopics(t *testing.T) {
	matcher := NewTopicMatcher()

	err := matcher.Subscribe("#", "sub1")
	require.NoError(t, err)

	err = matcher.Subscribe("+/clients", "sub2")
	require.NoError(t, err)

	err = matcher.Subscribe("$SYS/#", "sub3")
	require.NoError(t, err)

	// System topic should only match explicit subscription
	subscribers := matcher.Match("$SYS/clients")
	assert.Len(t, subscribers, 1)
	assert.Contains(t, subscribers, "sub3")

	// Normal topic matches wildcards
	subscribers = matcher.Match("normal/clients")
	assert.Len(t, subscribers, 2)
	assert.Contains(t, subscribers, "sub1")
	assert.Contains(t, subscribers, "sub2")
}

func TestTopicMatcherInvalidFilter(t *testing.T) {
	matcher := NewTopicMatcher()

	err := matcher.Subscribe("test/+invalid", "sub1")
	assert.ErrorIs(t, err, ErrInvalidTopicFilter)
}

func TestMatchTopicNoAlloc(t *testing.T) {
	t.Run("exact matches", func(t *testing.T) {
		tests := []struct {
			filter string
			topic  string
			match  bool
		}{
			{"a", "a", true},
			{"test", "test", true},
			{"a/b", "a/b", true},
			{"a/b/c", "a/b/c", true},
			{"test/topic/name", "test/topic/name", true},
			// Note: empty strings are handled by TopicMatch before calling matchTopicNoAlloc
		}

		for _, tt := range tests {
			result := matchTopicNoAlloc(tt.filter, tt.topic)
			assert.Equal(t, tt.match, result, "filter=%q topic=%q", tt.filter, tt.topic)
		}
	})

	t.Run("non-matches", func(t *testing.T) {
		tests := []struct {
			filter string
			topic  string
		}{
			{"a", "b"},
			{"test", "other"},
			{"a/b", "a/c"},
			{"a/b", "a/b/c"},
			{"a/b/c", "a/b"},
			{"test/topic", "test/other"},
			{"foo/bar", "foo/baz"},
		}

		for _, tt := range tests {
			result := matchTopicNoAlloc(tt.filter, tt.topic)
			assert.False(t, result, "filter=%q topic=%q should not match", tt.filter, tt.topic)
		}
	})

	t.Run("single level wildcard +", func(t *testing.T) {
		tests := []struct {
			filter string
			topic  string
			match  bool
		}{
			{"+", "a", true},
			{"+", "test", true},
			{"+/b", "a/b", true},
			{"a/+", "a/b", true},
			{"a/+/c", "a/b/c", true},
			{"+/+", "a/b", true},
			{"+/+/+", "a/b/c", true},
			{"+", "a/b", false}, // + matches only one level
			{"a/+", "a", false},
			{"+/b", "a/c", false},
		}

		for _, tt := range tests {
			result := matchTopicNoAlloc(tt.filter, tt.topic)
			assert.Equal(t, tt.match, result, "filter=%q topic=%q", tt.filter, tt.topic)
		}
	})

	t.Run("multi level wildcard #", func(t *testing.T) {
		tests := []struct {
			filter string
			topic  string
			match  bool
		}{
			{"#", "a", true},
			{"#", "a/b", true},
			{"#", "a/b/c", true},
			{"#", "a/b/c/d/e", true},
			{"a/#", "a", true},
			{"a/#", "a/b", true},
			{"a/#", "a/b/c", true},
			{"a/b/#", "a/b", true},
			{"a/b/#", "a/b/c", true},
			{"a/b/#", "a/b/c/d", true},
		}

		for _, tt := range tests {
			result := matchTopicNoAlloc(tt.filter, tt.topic)
			assert.True(t, result, "filter=%q topic=%q should match", tt.filter, tt.topic)
		}
	})

	t.Run("combined wildcards", func(t *testing.T) {
		tests := []struct {
			filter string
			topic  string
			match  bool
		}{
			{"+/#", "a", true},
			{"+/#", "a/b", true},
			{"+/#", "a/b/c", true},
			{"+/+/#", "a/b", true},
			{"+/+/#", "a/b/c", true},
			{"+/b/#", "a/b/c", true},
			{"a/+/#", "a/b/c", true},
		}

		for _, tt := range tests {
			result := matchTopicNoAlloc(tt.filter, tt.topic)
			assert.True(t, result, "filter=%q topic=%q should match", tt.filter, tt.topic)
		}
	})

	t.Run("edge cases with separators", func(t *testing.T) {
		tests := []struct {
			filter string
			topic  string
			match  bool
		}{
			{"/", "/", true},
			{"/a", "/a", true},
			{"a/", "a/", true},
			{"/a/", "/a/", true},
			{"//", "//", true},
			{"+/", "a/", true},
			{"/+", "/a", true},
			{"/#", "/a/b", true},
		}

		for _, tt := range tests {
			result := matchTopicNoAlloc(tt.filter, tt.topic)
			assert.Equal(t, tt.match, result, "filter=%q topic=%q", tt.filter, tt.topic)
		}
	})

	t.Run("long topics", func(t *testing.T) {
		// Test with longer topic paths
		longFilter := "level1/level2/level3/level4/level5"
		longTopic := "level1/level2/level3/level4/level5"
		assert.True(t, matchTopicNoAlloc(longFilter, longTopic))

		wildcardFilter := "level1/+/level3/+/level5"
		assert.True(t, matchTopicNoAlloc(wildcardFilter, longTopic))

		hashFilter := "level1/level2/#"
		assert.True(t, matchTopicNoAlloc(hashFilter, longTopic))
	})
}

func BenchmarkMatchTopicNoAlloc(b *testing.B) {
	b.Run("exact_match", func(b *testing.B) {
		filter := "sensor/living/temperature"
		topic := "sensor/living/temperature"

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_ = matchTopicNoAlloc(filter, topic)
		}
	})

	b.Run("wildcard_plus", func(b *testing.B) {
		filter := "sensor/+/temperature"
		topic := "sensor/living/temperature"

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_ = matchTopicNoAlloc(filter, topic)
		}
	})

	b.Run("wildcard_hash", func(b *testing.B) {
		filter := "sensor/#"
		topic := "sensor/living/temperature"

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_ = matchTopicNoAlloc(filter, topic)
		}
	})

	b.Run("long_topic", func(b *testing.B) {
		filter := "level1/level2/level3/level4/level5"
		topic := "level1/level2/level3/level4/level5"

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			_ = matchTopicNoAlloc(filter, topic)
		}
	})
}

func BenchmarkValidateTopicName(b *testing.B) {
	topic := "sensor/living/temperature"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = ValidateTopicName(topic)
	}
}

func BenchmarkValidateTopicFilter(b *testing.B) {
	filter := "sensor/+/temperature"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = ValidateTopicFilter(filter)
	}
}

func TestParseSharedSubscriptionInvalidTopicFilter(t *testing.T) {
	// Test that ParseSharedSubscription validates the topic filter part
	_, err := ParseSharedSubscription("$share/group/test#invalid")
	assert.ErrorIs(t, err, ErrInvalidTopicFilter)
}

func TestTopicMatcherUnsubscribeNonExistent(t *testing.T) {
	matcher := NewTopicMatcher()

	t.Run("unsubscribe from non-existent filter", func(t *testing.T) {
		err := matcher.Unsubscribe("nonexistent/topic", "sub1")
		require.NoError(t, err)
	})

	t.Run("unsubscribe non-existent subscriber", func(t *testing.T) {
		err := matcher.Subscribe("test/topic", "sub1")
		require.NoError(t, err)

		err = matcher.Unsubscribe("test/topic", "sub2")
		require.NoError(t, err)

		// sub1 should still be there
		subs := matcher.Match("test/topic")
		assert.Len(t, subs, 1)
		assert.Contains(t, subs, "sub1")
	})
}

func TestTopicMatcherWithCustomMatcher(t *testing.T) {
	matcher := NewTopicMatcher()

	type matchableSub struct {
		id string
	}

	sub1 := matchableSub{id: "sub1"}
	sub2 := matchableSub{id: "sub2"}

	err := matcher.Subscribe("test/topic", sub1)
	require.NoError(t, err)

	err = matcher.Subscribe("test/topic", sub2)
	require.NoError(t, err)

	subs := matcher.Match("test/topic")
	assert.Len(t, subs, 2)
}

func TestTopicMatcherMatchInvalidTopic(t *testing.T) {
	matcher := NewTopicMatcher()
	err := matcher.Subscribe("test/+", "sub1")
	require.NoError(t, err)

	// Match with invalid topic name should return nil
	subs := matcher.Match("test/+/invalid")
	assert.Nil(t, subs)

	subs = matcher.Match("")
	assert.Nil(t, subs)
}

func TestTopicMatcherUnsubscribeInvalidFilter(t *testing.T) {
	matcher := NewTopicMatcher()

	err := matcher.Unsubscribe("test/+invalid", "sub1")
	assert.ErrorIs(t, err, ErrInvalidTopicFilter)
}

func BenchmarkTopicMatch(b *testing.B) {
	filter := "sensor/+/temperature"
	topic := "sensor/living/temperature"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = TopicMatch(filter, topic)
	}
}

func BenchmarkTopicMatcherMatch(b *testing.B) {
	matcher := NewTopicMatcher()
	_ = matcher.Subscribe("sensor/+/temperature", "sub1")
	_ = matcher.Subscribe("sensor/#", "sub2")
	_ = matcher.Subscribe("sensor/living/+", "sub3")

	topic := "sensor/living/temperature"

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_ = matcher.Match(topic)
	}
}

func BenchmarkTopicMatcherSubscribe(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		matcher := NewTopicMatcher()
		_ = matcher.Subscribe("sensor/living/temperature", "sub1")
	}
}

func FuzzValidateTopicName(f *testing.F) {
	f.Add("test")
	f.Add("test/topic")
	f.Add("a/b/c/d/e")
	f.Add("")
	f.Add("test\x00topic")

	for range 10 {
		size := rand.IntN(64) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(string(data))
	}

	f.Fuzz(func(_ *testing.T, topic string) {
		_ = ValidateTopicName(topic)
	})
}

func FuzzValidateTopicFilter(f *testing.F) {
	f.Add("test")
	f.Add("test/+/topic")
	f.Add("test/#")
	f.Add("+/+/+")
	f.Add("")

	for range 10 {
		size := rand.IntN(64) + 1
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rand.IntN(256))
		}
		f.Add(string(data))
	}

	f.Fuzz(func(_ *testing.T, filter string) {
		_ = ValidateTopicFilter(filter)
	})
}

func FuzzTopicMatch(f *testing.F) {
	f.Add("test", "test")
	f.Add("test/+", "test/topic")
	f.Add("#", "a/b/c")
	f.Add("$SYS/#", "$SYS/test")

	f.Fuzz(func(_ *testing.T, filter, topic string) {
		_ = TopicMatch(filter, topic)
	})
}

func TestContainsWildcard(t *testing.T) {
	t.Run("returns true for multi-level wildcard", func(t *testing.T) {
		assert.True(t, containsWildcard("#"))
		assert.True(t, containsWildcard("sensor/#"))
		assert.True(t, containsWildcard("home/+/temperature/#"))
	})

	t.Run("returns true for single-level wildcard", func(t *testing.T) {
		assert.True(t, containsWildcard("+"))
		assert.True(t, containsWildcard("+/temperature"))
		assert.True(t, containsWildcard("sensor/+/data"))
		assert.True(t, containsWildcard("home/+/+"))
	})

	t.Run("returns true for mixed wildcards", func(t *testing.T) {
		assert.True(t, containsWildcard("+/#"))
		assert.True(t, containsWildcard("+/+/#"))
	})

	t.Run("returns false for no wildcards", func(t *testing.T) {
		assert.False(t, containsWildcard("sensor"))
		assert.False(t, containsWildcard("sensor/temperature"))
		assert.False(t, containsWildcard("home/living-room/temperature"))
		assert.False(t, containsWildcard(""))
	})
}

func TestIsSharedSubscription(t *testing.T) {
	t.Run("returns true for valid shared subscriptions", func(t *testing.T) {
		assert.True(t, isSharedSubscription("$share/group/topic"))
		assert.True(t, isSharedSubscription("$share/mygroup/sensor/+/data"))
		assert.True(t, isSharedSubscription("$share/consumers/#"))
		assert.True(t, isSharedSubscription("$share/g/t"))
	})

	t.Run("returns false for non-shared subscriptions", func(t *testing.T) {
		assert.False(t, isSharedSubscription("sensor/temperature"))
		assert.False(t, isSharedSubscription("#"))
		assert.False(t, isSharedSubscription("+/data"))
		assert.False(t, isSharedSubscription(""))
	})

	t.Run("returns false for $SYS topics", func(t *testing.T) {
		assert.False(t, isSharedSubscription("$SYS/broker/clients"))
		assert.False(t, isSharedSubscription("$SYS/#"))
	})

	t.Run("returns false for incomplete shared prefix", func(t *testing.T) {
		assert.False(t, isSharedSubscription("$share"))
		assert.False(t, isSharedSubscription("$shar/group/topic"))
		assert.False(t, isSharedSubscription("share/group/topic"))
	})
}
