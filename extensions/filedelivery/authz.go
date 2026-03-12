package filedelivery

import (
	"github.com/vitalvas/mqttv5"
)

// CheckAccess verifies whether a client is authorized to perform an action on a stream topic.
// It enforces own-access only (clientID in the topic must match the caller),
// and direction-based ACL (publish for client-to-server, subscribe for server-to-client suffixes).
// Allowed requests are capped at QoS 1.
//
// Returns a non-nil *AuthzResult if the topic is a stream topic (handled).
// Returns nil if the topic is not a stream topic.
func (h *Handler) CheckAccess(c *mqttv5.AuthzContext) *mqttv5.AuthzResult {
	parsed := parseStreamTopic(c.Topic)
	if parsed == nil {
		return nil
	}

	deny := &mqttv5.AuthzResult{Allowed: false, ReasonCode: mqttv5.ReasonNotAuthorized}

	// Own-access only: clientID in topic must match caller.
	if parsed.ClientID != c.ClientID {
		return deny
	}

	switch c.Action {
	case mqttv5.AuthzActionPublish:
		switch parsed.Suffix {
		case suffixDescribe, suffixGet, suffixCreate, suffixData:
			return &mqttv5.AuthzResult{Allowed: true, MaxQoS: mqttv5.QoS1}
		}
	case mqttv5.AuthzActionSubscribe:
		switch parsed.Suffix {
		case suffixDescription, suffixData, suffixRejected, suffixAccepted:
			return &mqttv5.AuthzResult{Allowed: true, MaxQoS: mqttv5.QoS1}
		}
	}

	return deny
}
