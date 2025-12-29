package mqttv5

import (
	"context"
	"strings"
)

// AuthzAction represents an authorization action.
type AuthzAction int

const (
	// AuthzActionPublish represents a publish action.
	AuthzActionPublish AuthzAction = 0
	// AuthzActionSubscribe represents a subscribe action.
	AuthzActionSubscribe AuthzAction = 1
)

// String returns the string representation of the action.
func (a AuthzAction) String() string {
	switch a {
	case AuthzActionPublish:
		return "publish"
	case AuthzActionSubscribe:
		return "subscribe"
	default:
		return "unknown"
	}
}

// AuthzContext contains information about the authorization request.
type AuthzContext struct {
	// ClientID is the client identifier.
	ClientID string

	// Username is the authenticated username (may be empty).
	Username string

	// Topic is the topic for publish or topic filter for subscribe.
	Topic string

	// Action is the action being performed.
	Action AuthzAction

	// QoS is the QoS level for publish/subscribe.
	QoS byte

	// Retain is true if the publish has retain flag set.
	Retain bool
}

// AuthzResult represents the result of an authorization check.
type AuthzResult struct {
	// Allowed indicates if the action is allowed.
	Allowed bool

	// ReasonCode is the MQTT reason code if not allowed.
	ReasonCode ReasonCode

	// MaxQoS is the maximum QoS allowed (may downgrade requested QoS).
	MaxQoS byte
}

// Authorizer defines the interface for authorizing MQTT actions.
type Authorizer interface {
	// Authorize checks if an action is allowed.
	Authorize(ctx context.Context, authzCtx *AuthzContext) (*AuthzResult, error)
}

// ACLEntry represents an access control list entry.
type ACLEntry struct {
	// ClientPattern is a pattern to match client IDs (supports wildcards * and ?).
	ClientPattern string

	// UsernamePattern is a pattern to match usernames (supports wildcards * and ?).
	UsernamePattern string

	// TopicPattern is a pattern to match topics (supports MQTT wildcards + and #).
	TopicPattern string

	// AllowPublish indicates if publishing is allowed.
	AllowPublish bool

	// AllowSubscribe indicates if subscribing is allowed.
	AllowSubscribe bool

	// AllowRetain indicates if retain flag is allowed on publish.
	AllowRetain bool

	// MaxQoS is the maximum QoS level allowed.
	MaxQoS byte

	// Deny indicates this is a deny rule (takes precedence over allow).
	Deny bool
}

// Matches checks if this ACL entry matches the given context.
func (e *ACLEntry) Matches(authzCtx *AuthzContext) bool {
	if e.ClientPattern != "" && !matchPattern(e.ClientPattern, authzCtx.ClientID) {
		return false
	}

	if e.UsernamePattern != "" && !matchPattern(e.UsernamePattern, authzCtx.Username) {
		return false
	}

	if e.TopicPattern != "" && !TopicMatch(e.TopicPattern, authzCtx.Topic) {
		return false
	}

	return true
}

// AllowsAction checks if this entry allows the specified action.
func (e *ACLEntry) AllowsAction(action AuthzAction, retain bool) bool {
	if e.Deny {
		return false
	}

	switch action {
	case AuthzActionPublish:
		if !e.AllowPublish {
			return false
		}
		if retain && !e.AllowRetain {
			return false
		}
		return true
	case AuthzActionSubscribe:
		return e.AllowSubscribe
	default:
		return false
	}
}

// matchPattern matches a string against a pattern with wildcards.
// * matches any sequence of characters
// ? matches any single character
func matchPattern(pattern, s string) bool {
	if pattern == "*" {
		return true
	}

	pLen := len(pattern)
	sLen := len(s)

	if pLen == 0 {
		return sLen == 0
	}

	// Dynamic programming approach for wildcard matching
	dp := make([][]bool, pLen+1)
	for i := range dp {
		dp[i] = make([]bool, sLen+1)
	}

	dp[0][0] = true

	// Handle leading *
	for i := 1; i <= pLen; i++ {
		if pattern[i-1] == '*' {
			dp[i][0] = dp[i-1][0]
		}
	}

	for i := 1; i <= pLen; i++ {
		for j := 1; j <= sLen; j++ {
			switch pattern[i-1] {
			case '*':
				dp[i][j] = dp[i-1][j] || dp[i][j-1]
			case '?':
				dp[i][j] = dp[i-1][j-1]
			default:
				if pattern[i-1] == s[j-1] {
					dp[i][j] = dp[i-1][j-1]
				}
			}
		}
	}

	return dp[pLen][sLen]
}

// AllowAllAuthorizer allows all actions.
type AllowAllAuthorizer struct{}

// Authorize always allows the action.
func (a *AllowAllAuthorizer) Authorize(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
	return &AuthzResult{
		Allowed: true,
		MaxQoS:  2,
	}, nil
}

// DenyAllAuthorizer denies all actions.
type DenyAllAuthorizer struct{}

// Authorize always denies the action.
func (d *DenyAllAuthorizer) Authorize(_ context.Context, _ *AuthzContext) (*AuthzResult, error) {
	return &AuthzResult{
		Allowed:    false,
		ReasonCode: ReasonNotAuthorized,
	}, nil
}

// ACLAuthorizer authorizes actions based on ACL entries.
type ACLAuthorizer struct {
	// Entries is the list of ACL entries.
	Entries []ACLEntry

	// DefaultAllow is the default action when no entry matches.
	DefaultAllow bool
}

// Authorize checks if an action is allowed based on ACL entries.
func (a *ACLAuthorizer) Authorize(_ context.Context, authzCtx *AuthzContext) (*AuthzResult, error) {
	var matchedAllow *ACLEntry

	for i := range a.Entries {
		entry := &a.Entries[i]
		if !entry.Matches(authzCtx) {
			continue
		}

		// Deny rules take precedence
		if entry.Deny {
			return &AuthzResult{
				Allowed:    false,
				ReasonCode: ReasonNotAuthorized,
			}, nil
		}

		// Check if this entry allows the action
		if entry.AllowsAction(authzCtx.Action, authzCtx.Retain) {
			if matchedAllow == nil || entry.MaxQoS > matchedAllow.MaxQoS {
				matchedAllow = entry
			}
		}
	}

	if matchedAllow != nil {
		return &AuthzResult{
			Allowed: true,
			MaxQoS:  min(matchedAllow.MaxQoS, authzCtx.QoS),
		}, nil
	}

	if a.DefaultAllow {
		return &AuthzResult{
			Allowed: true,
			MaxQoS:  authzCtx.QoS,
		}, nil
	}

	return &AuthzResult{
		Allowed:    false,
		ReasonCode: ReasonNotAuthorized,
	}, nil
}

// TopicAuthorizer authorizes based on topic patterns per user.
type TopicAuthorizer struct {
	// Rules maps username to allowed topic patterns for each action.
	Rules map[string]TopicRules
}

// TopicRules contains topic rules for a user.
type TopicRules struct {
	// PublishPatterns are topic patterns the user can publish to.
	PublishPatterns []string

	// SubscribePatterns are topic patterns the user can subscribe to.
	SubscribePatterns []string
}

// Authorize checks if an action is allowed based on topic rules.
func (t *TopicAuthorizer) Authorize(_ context.Context, authzCtx *AuthzContext) (*AuthzResult, error) {
	rules, ok := t.Rules[authzCtx.Username]
	if !ok {
		return &AuthzResult{
			Allowed:    false,
			ReasonCode: ReasonNotAuthorized,
		}, nil
	}

	var patterns []string
	if authzCtx.Action == AuthzActionPublish {
		patterns = rules.PublishPatterns
	} else {
		patterns = rules.SubscribePatterns
	}

	for _, pattern := range patterns {
		// Support %c for client ID and %u for username substitution
		expandedPattern := strings.ReplaceAll(pattern, "%c", authzCtx.ClientID)
		expandedPattern = strings.ReplaceAll(expandedPattern, "%u", authzCtx.Username)

		if TopicMatch(expandedPattern, authzCtx.Topic) {
			return &AuthzResult{
				Allowed: true,
				MaxQoS:  2,
			}, nil
		}
	}

	return &AuthzResult{
		Allowed:    false,
		ReasonCode: ReasonNotAuthorized,
	}, nil
}

// ChainAuthorizer chains multiple authorizers together.
type ChainAuthorizer struct {
	// Authorizers is the list of authorizers to check.
	Authorizers []Authorizer

	// RequireAll requires all authorizers to allow (AND logic).
	// If false, any authorizer allowing is sufficient (OR logic).
	RequireAll bool
}

// Authorize checks all authorizers in the chain.
func (c *ChainAuthorizer) Authorize(ctx context.Context, authzCtx *AuthzContext) (*AuthzResult, error) {
	if len(c.Authorizers) == 0 {
		return &AuthzResult{
			Allowed: true,
			MaxQoS:  2,
		}, nil
	}

	minQoS := byte(2)
	anyAllowed := false

	for _, authz := range c.Authorizers {
		result, err := authz.Authorize(ctx, authzCtx)
		if err != nil {
			return nil, err
		}

		if c.RequireAll {
			if !result.Allowed {
				return result, nil
			}
			minQoS = min(minQoS, result.MaxQoS)
		} else if result.Allowed {
			anyAllowed = true
			minQoS = min(minQoS, result.MaxQoS)
		}
	}

	if c.RequireAll {
		return &AuthzResult{
			Allowed: true,
			MaxQoS:  minQoS,
		}, nil
	}

	if anyAllowed {
		return &AuthzResult{
			Allowed: true,
			MaxQoS:  minQoS,
		}, nil
	}

	return &AuthzResult{
		Allowed:    false,
		ReasonCode: ReasonNotAuthorized,
	}, nil
}
