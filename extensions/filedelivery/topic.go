package filedelivery

import "strings"

const (
	topicPrefix  = "$things/"
	streamsInfix = "/streams/"

	suffixDescribe    = "describe/json"
	suffixDescription = "description/json"
	suffixGet         = "get/json"
	suffixData        = "data/json"
	suffixRejected    = "rejected/json"
	suffixCreate      = "create/json"
	suffixAccepted    = "accepted/json"
)

// parsedTopic is the result of parsing a stream topic.
type parsedTopic struct {
	ClientID string
	StreamID string
	Suffix   string
}

// parseStreamTopic parses a topic string into its stream components.
// Returns nil if the topic is not a valid stream topic.
//
// Topic format: $things/{clientID}/streams/{streamID}/{suffix}
func parseStreamTopic(topic string) *parsedTopic {
	if !strings.HasPrefix(topic, topicPrefix) {
		return nil
	}

	rest := topic[len(topicPrefix):]

	clientID, rest, ok := strings.Cut(rest, streamsInfix)
	if !ok || clientID == "" || rest == "" || !isValidIdentifier(clientID) {
		return nil
	}

	streamID, suffix, ok := strings.Cut(rest, "/")
	if !ok || streamID == "" || suffix == "" || !isValidStreamID(streamID) || len(streamID) > MaxStreamIDLen {
		return nil
	}

	if !isValidSuffix(suffix) {
		return nil
	}

	return &parsedTopic{
		ClientID: clientID,
		StreamID: streamID,
		Suffix:   suffix,
	}
}

// isValidSuffix checks whether the suffix is a recognized stream operation.
func isValidSuffix(suffix string) bool {
	switch suffix {
	case suffixDescribe, suffixDescription, suffixGet, suffixData, suffixRejected, suffixCreate, suffixAccepted:
		return true
	}
	return false
}

// isValidIdentifier checks that a clientID contains only safe characters.
// Rejects MQTT wildcards (+, #), path traversal (..), spaces, null bytes, and non-ASCII.
func isValidIdentifier(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= ' ' || c > '~' || c == '+' || c == '#' || c == '$' {
			return false
		}
	}
	return s != "." && s != ".." && !strings.Contains(s, "/")
}

// isValidStreamID checks that a stream ID matches [a-zA-Z0-9:_-].
func isValidStreamID(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == ':' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

// buildTopic constructs a stream topic from components.
func buildTopic(clientID, streamID, suffix string) string {
	var b strings.Builder

	b.WriteString(topicPrefix)
	b.WriteString(clientID)
	b.WriteString(streamsInfix)
	b.WriteString(streamID)
	b.WriteByte('/')
	b.WriteString(suffix)

	return b.String()
}
