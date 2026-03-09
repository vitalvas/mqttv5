package shadow

import (
	"github.com/vitalvas/mqttv5"
)

// CheckAccess verifies whether a client is authorized to perform an action on a shadow topic.
// It respects the Handler's feature flags (WithClassicShadow/WithNamedShadow),
// enforces own-access only (clientID in the topic must match the caller),
// and direction-based ACL (publish for client-to-server, subscribe for server-to-client suffixes).
// Allowed requests are capped at QoS 1.
//
// Returns a non-nil *AuthzResult if the topic is a shadow topic (handled).
// Returns nil if the topic is not a shadow topic.
//
// Usage inside a custom authorizer:
//
//	func (a *MyAuthz) Authorize(ctx context.Context, c *mqttv5.AuthzContext) (*mqttv5.AuthzResult, error) {
//	    if result := a.shadowHandler.CheckAccess(c); result != nil {
//	        return result, nil
//	    }
//	    // other ACL checks...
//	    return &mqttv5.AuthzResult{Allowed: true}, nil
//	}
func (h *Handler) CheckAccess(c *mqttv5.AuthzContext) *mqttv5.AuthzResult {
	parsed := parseShadowTopic(c.Topic)
	if parsed == nil {
		return nil
	}

	deny := &mqttv5.AuthzResult{Allowed: false, ReasonCode: mqttv5.ReasonNotAuthorized}

	isShared := parsed.GroupName != ""
	isNamed := parsed.ShadowName != ""

	if isShared {
		if !h.sharedEnabled {
			return deny
		}

		if h.sharedResolver == nil {
			// No resolver: read-only (get only for publish, subscribe allowed)
			if c.Action == mqttv5.AuthzActionPublish && parsed.Suffix != suffixGet {
				return deny
			}
		} else if !h.sharedResolver(c.ClientID, parsed.GroupName) {
			return deny
		}
	} else {
		if isNamed && !h.namedEnabled {
			return deny
		}
		if !isNamed && !h.classicEnabled {
			if parsed.Suffix != suffixList || !h.namedEnabled {
				return deny
			}
		}

		effectiveClientID := c.ClientID
		if h.resolver != nil {
			effectiveClientID = h.resolver(c.ClientID)
		}
		if parsed.ClientID != effectiveClientID {
			return deny
		}
	}

	switch c.Action {
	case mqttv5.AuthzActionPublish:
		switch parsed.Suffix {
		case suffixGet, suffixUpdate, suffixDelete, suffixList:
			return &mqttv5.AuthzResult{Allowed: true, MaxQoS: mqttv5.QoS1}
		}
	case mqttv5.AuthzActionSubscribe:
		switch parsed.Suffix {
		case suffixGetAccepted, suffixGetRejected,
			suffixUpdateAccepted, suffixUpdateRejected,
			suffixUpdateDelta, suffixUpdateDocuments,
			suffixDeleteAccepted, suffixDeleteRejected,
			suffixListAccepted, suffixListRejected:
			return &mqttv5.AuthzResult{Allowed: true, MaxQoS: mqttv5.QoS1}
		}
	}

	return deny
}
