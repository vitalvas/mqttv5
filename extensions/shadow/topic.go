package shadow

import "strings"

const (
	topicPrefix        = "$things/"
	sharedInfix        = "$shared/"
	shadowInfix        = "/shadow/"
	namedInfix         = "name/"
	maxShadowNameBytes = 64

	suffixUpdate          = "update"
	suffixUpdateAccepted  = "update/accepted"
	suffixUpdateRejected  = "update/rejected"
	suffixUpdateDelta     = "update/delta"
	suffixUpdateDocuments = "update/documents"
	suffixGet             = "get"
	suffixGetAccepted     = "get/accepted"
	suffixGetRejected     = "get/rejected"
	suffixDelete          = "delete"
	suffixDeleteAccepted  = "delete/accepted"
	suffixDeleteRejected  = "delete/rejected"
	suffixList            = "list"
	suffixListAccepted    = "list/accepted"
	suffixListRejected    = "list/rejected"
)

// parsedTopic is the result of parsing a shadow topic.
type parsedTopic struct {
	ClientID   string
	GroupName  string // non-empty for shared shadows
	ShadowName string // empty for classic shadow
	Suffix     string // e.g. "update", "get/accepted"
}

// parseShadowTopic parses a topic string into its shadow components.
// Returns nil if the topic is not a valid shadow topic.
//
// Supports both per-device and shared topics:
//
//	$things/{clientID}/shadow/{suffix}
//	$things/{clientID}/shadow/name/{shadowName}/{suffix}
//	$things/$shared/{groupName}/shadow/{suffix}
//	$things/$shared/{groupName}/shadow/name/{shadowName}/{suffix}
func parseShadowTopic(topic string) *parsedTopic {
	if !strings.HasPrefix(topic, topicPrefix) {
		return nil
	}

	rest := topic[len(topicPrefix):]

	var groupName string

	// Check for shared topic: $shared/{groupName}/shadow/...
	if strings.HasPrefix(rest, sharedInfix) {
		rest = rest[len(sharedInfix):]

		groupName, rest, _ = strings.Cut(rest, shadowInfix)
		if groupName == "" || rest == "" || !isValidIdentifier(groupName) {
			return nil
		}
	} else {
		// Per-device topic: {clientID}/shadow/...
		var clientID string
		var ok bool

		clientID, rest, ok = strings.Cut(rest, shadowInfix)
		if !ok || clientID == "" || !isValidIdentifier(clientID) {
			return nil
		}
		if rest == "" {
			return nil
		}

		return parseShadowSuffix(rest, clientID, "")
	}

	return parseShadowSuffix(rest, "", groupName)
}

func parseShadowSuffix(suffix, clientID, groupName string) *parsedTopic {
	var shadowName string

	// Check for named shadow: name/{shadowName}/...
	if strings.HasPrefix(suffix, namedInfix) {
		nameRest := suffix[len(namedInfix):]

		var ok bool
		shadowName, suffix, ok = strings.Cut(nameRest, "/")
		if !ok || shadowName == "" || suffix == "" || !isValidShadowName(shadowName) || len(shadowName) > maxShadowNameBytes {
			return nil
		}
	}

	if !isValidSuffix(suffix) {
		return nil
	}

	return &parsedTopic{
		ClientID:   clientID,
		GroupName:  groupName,
		ShadowName: shadowName,
		Suffix:     suffix,
	}
}

// isValidSuffix checks whether the suffix is a recognized shadow operation.
func isValidSuffix(suffix string) bool {
	switch suffix {
	case suffixUpdate, suffixUpdateAccepted, suffixUpdateRejected,
		suffixUpdateDelta, suffixUpdateDocuments,
		suffixGet, suffixGetAccepted, suffixGetRejected,
		suffixDelete, suffixDeleteAccepted, suffixDeleteRejected,
		suffixList, suffixListAccepted, suffixListRejected:
		return true
	}
	return false
}

// isValidIdentifier checks that a clientID or groupName contains only safe characters.
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

// isValidShadowName checks that a shadow name matches [a-zA-Z0-9:_-].
func isValidShadowName(s string) bool {
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

// buildTopic constructs a per-device shadow topic from components.
func buildTopic(clientID, shadowName, suffix string) string {
	var b strings.Builder

	b.WriteString(topicPrefix)
	b.WriteString(clientID)
	b.WriteString(shadowInfix)

	if shadowName != "" {
		b.WriteString(namedInfix)
		b.WriteString(shadowName)
		b.WriteByte('/')
	}

	b.WriteString(suffix)

	return b.String()
}

// buildSharedTopic constructs a shared shadow topic from components.
func buildSharedTopic(groupName, shadowName, suffix string) string {
	var b strings.Builder

	b.WriteString(topicPrefix)
	b.WriteString(sharedInfix)
	b.WriteString(groupName)
	b.WriteString(shadowInfix)

	if shadowName != "" {
		b.WriteString(namedInfix)
		b.WriteString(shadowName)
		b.WriteByte('/')
	}

	b.WriteString(suffix)

	return b.String()
}

// responseTopic builds the correct topic (per-device or shared) based on the parsed topic.
func responseTopic(parsed *parsedTopic, suffix string) string {
	if parsed.GroupName != "" {
		return buildSharedTopic(parsed.GroupName, parsed.ShadowName, suffix)
	}

	return buildTopic(parsed.ClientID, parsed.ShadowName, suffix)
}
