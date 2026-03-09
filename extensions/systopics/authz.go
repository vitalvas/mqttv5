package systopics

import (
	"strings"

	"github.com/vitalvas/mqttv5"
)

const sysPrefix = "$SYS/"

// CheckAccess verifies whether a client is authorized to access a $SYS topic.
// Subscribe actions are always allowed; publish actions are always denied.
//
// Returns a non-nil *AuthzResult if the topic starts with "$SYS/" (handled).
// Returns nil if the topic is not a $SYS topic.
//
// Usage inside a custom authorizer:
//
//	func (a *MyAuthz) Authorize(ctx context.Context, c *mqttv5.AuthzContext) (*mqttv5.AuthzResult, error) {
//	    if result := a.sysPublisher.CheckAccess(c); result != nil {
//	        return result, nil
//	    }
//	    // other ACL checks...
//	    return &mqttv5.AuthzResult{Allowed: true}, nil
//	}
func (p *Publisher) CheckAccess(c *mqttv5.AuthzContext) *mqttv5.AuthzResult {
	if !strings.HasPrefix(c.Topic, sysPrefix) {
		return nil
	}

	if c.Action != mqttv5.AuthzActionSubscribe {
		return &mqttv5.AuthzResult{Allowed: false, ReasonCode: mqttv5.ReasonNotAuthorized}
	}

	return &mqttv5.AuthzResult{Allowed: true, MaxQoS: mqttv5.QoS0}
}
