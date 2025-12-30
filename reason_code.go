package mqttv5

// ReasonCode represents an MQTT v5.0 reason code.
// MQTT v5.0 spec: Section 2.4
type ReasonCode byte

// Reason codes as defined in MQTT v5.0 specification.
// MQTT v5.0 spec: Section 2.4
const (
	// Success / Normal disconnection / Granted QoS 0
	ReasonSuccess ReasonCode = 0x00
	// Granted QoS 1
	ReasonGrantedQoS1 ReasonCode = 0x01
	// Granted QoS 2
	ReasonGrantedQoS2 ReasonCode = 0x02
	// Disconnect with Will Message
	ReasonDisconnectWithWill ReasonCode = 0x04
	// No matching subscribers
	ReasonNoMatchingSubscribers ReasonCode = 0x10
	// No subscription existed
	ReasonNoSubscriptionExisted ReasonCode = 0x11
	// Continue authentication
	ReasonContinueAuth ReasonCode = 0x18
	// Re-authenticate
	ReasonReAuth ReasonCode = 0x19
	// Unspecified error
	ReasonUnspecifiedError ReasonCode = 0x80
	// Malformed Packet
	ReasonMalformedPacket ReasonCode = 0x81
	// Protocol Error
	ReasonProtocolError ReasonCode = 0x82
	// Implementation specific error
	ReasonImplSpecificError ReasonCode = 0x83
	// Unsupported Protocol Version
	ReasonUnsupportedProtocolVersion ReasonCode = 0x84
	// Client Identifier not valid
	ReasonClientIDNotValid ReasonCode = 0x85
	// Bad User Name or Password
	ReasonBadUserNameOrPassword ReasonCode = 0x86
	// Not authorized
	ReasonNotAuthorized ReasonCode = 0x87
	// Server unavailable
	ReasonServerUnavailable ReasonCode = 0x88
	// Server busy
	ReasonServerBusy ReasonCode = 0x89
	// Banned
	ReasonBanned ReasonCode = 0x8A
	// Server shutting down
	ReasonServerShuttingDown ReasonCode = 0x8B
	// Bad authentication method
	ReasonBadAuthMethod ReasonCode = 0x8C
	// Keep Alive timeout
	ReasonKeepAliveTimeout ReasonCode = 0x8D
	// Session taken over
	ReasonSessionTakenOver ReasonCode = 0x8E
	// Topic Filter invalid
	ReasonTopicFilterInvalid ReasonCode = 0x8F
	// Topic Name invalid
	ReasonTopicNameInvalid ReasonCode = 0x90
	// Packet Identifier in use
	ReasonPacketIDInUse ReasonCode = 0x91
	// Packet Identifier not found
	ReasonPacketIDNotFound ReasonCode = 0x92
	// Receive Maximum exceeded
	ReasonReceiveMaxExceeded ReasonCode = 0x93
	// Topic Alias invalid
	ReasonTopicAliasInvalid ReasonCode = 0x94
	// Packet too large
	ReasonPacketTooLarge ReasonCode = 0x95
	// Message rate too high
	ReasonMessageRateTooHigh ReasonCode = 0x96
	// Quota exceeded
	ReasonQuotaExceeded ReasonCode = 0x97
	// Administrative action
	ReasonAdminAction ReasonCode = 0x98
	// Payload format invalid
	ReasonPayloadFormatInvalid ReasonCode = 0x99
	// Retain not supported
	ReasonRetainNotSupported ReasonCode = 0x9A
	// QoS not supported
	ReasonQoSNotSupported ReasonCode = 0x9B
	// Use another server
	ReasonUseAnotherServer ReasonCode = 0x9C
	// Server moved
	ReasonServerMoved ReasonCode = 0x9D
	// Shared Subscriptions not supported
	ReasonSharedSubsNotSupported ReasonCode = 0x9E
	// Connection rate exceeded
	ReasonConnectionRateExceeded ReasonCode = 0x9F
	// Maximum connect time
	ReasonMaxConnectTime ReasonCode = 0xA0
	// Subscription Identifiers not supported
	ReasonSubIDsNotSupported ReasonCode = 0xA1
	// Wildcard Subscriptions not supported
	ReasonWildcardSubsNotSupported ReasonCode = 0xA2
)

var reasonCodeStrings = map[ReasonCode]string{
	ReasonSuccess:                    "Success",
	ReasonGrantedQoS1:                "Granted QoS 1",
	ReasonGrantedQoS2:                "Granted QoS 2",
	ReasonDisconnectWithWill:         "Disconnect with Will Message",
	ReasonNoMatchingSubscribers:      "No matching subscribers",
	ReasonNoSubscriptionExisted:      "No subscription existed",
	ReasonContinueAuth:               "Continue authentication",
	ReasonReAuth:                     "Re-authenticate",
	ReasonUnspecifiedError:           "Unspecified error",
	ReasonMalformedPacket:            "Malformed Packet",
	ReasonProtocolError:              "Protocol Error",
	ReasonImplSpecificError:          "Implementation specific error",
	ReasonUnsupportedProtocolVersion: "Unsupported Protocol Version",
	ReasonClientIDNotValid:           "Client Identifier not valid",
	ReasonBadUserNameOrPassword:      "Bad User Name or Password",
	ReasonNotAuthorized:              "Not authorized",
	ReasonServerUnavailable:          "Server unavailable",
	ReasonServerBusy:                 "Server busy",
	ReasonBanned:                     "Banned",
	ReasonServerShuttingDown:         "Server shutting down",
	ReasonBadAuthMethod:              "Bad authentication method",
	ReasonKeepAliveTimeout:           "Keep Alive timeout",
	ReasonSessionTakenOver:           "Session taken over",
	ReasonTopicFilterInvalid:         "Topic Filter invalid",
	ReasonTopicNameInvalid:           "Topic Name invalid",
	ReasonPacketIDInUse:              "Packet Identifier in use",
	ReasonPacketIDNotFound:           "Packet Identifier not found",
	ReasonReceiveMaxExceeded:         "Receive Maximum exceeded",
	ReasonTopicAliasInvalid:          "Topic Alias invalid",
	ReasonPacketTooLarge:             "Packet too large",
	ReasonMessageRateTooHigh:         "Message rate too high",
	ReasonQuotaExceeded:              "Quota exceeded",
	ReasonAdminAction:                "Administrative action",
	ReasonPayloadFormatInvalid:       "Payload format invalid",
	ReasonRetainNotSupported:         "Retain not supported",
	ReasonQoSNotSupported:            "QoS not supported",
	ReasonUseAnotherServer:           "Use another server",
	ReasonServerMoved:                "Server moved",
	ReasonSharedSubsNotSupported:     "Shared Subscriptions not supported",
	ReasonConnectionRateExceeded:     "Connection rate exceeded",
	ReasonMaxConnectTime:             "Maximum connect time",
	ReasonSubIDsNotSupported:         "Subscription Identifiers not supported",
	ReasonWildcardSubsNotSupported:   "Wildcard Subscriptions not supported",
}

// String returns the human-readable description of the reason code.
func (r ReasonCode) String() string {
	if s, ok := reasonCodeStrings[r]; ok {
		return s
	}
	return "Unknown reason code"
}

// IsError returns true if the reason code indicates an error (>= 0x80).
func (r ReasonCode) IsError() bool {
	return r >= 0x80
}

// IsSuccess returns true if the reason code indicates success (< 0x80).
func (r ReasonCode) IsSuccess() bool {
	return r < 0x80
}

// Valid reason codes per packet type.
var (
	// CONNACK valid reason codes
	connackReasonCodes = map[ReasonCode]bool{
		ReasonSuccess:                    true,
		ReasonUnspecifiedError:           true,
		ReasonMalformedPacket:            true,
		ReasonProtocolError:              true,
		ReasonImplSpecificError:          true,
		ReasonUnsupportedProtocolVersion: true,
		ReasonClientIDNotValid:           true,
		ReasonBadUserNameOrPassword:      true,
		ReasonNotAuthorized:              true,
		ReasonServerUnavailable:          true,
		ReasonServerBusy:                 true,
		ReasonBanned:                     true,
		ReasonBadAuthMethod:              true,
		ReasonTopicNameInvalid:           true,
		ReasonPacketTooLarge:             true,
		ReasonQuotaExceeded:              true,
		ReasonPayloadFormatInvalid:       true,
		ReasonRetainNotSupported:         true,
		ReasonQoSNotSupported:            true,
		ReasonUseAnotherServer:           true,
		ReasonServerMoved:                true,
		ReasonConnectionRateExceeded:     true,
	}

	// PUBACK valid reason codes
	pubackReasonCodes = map[ReasonCode]bool{
		ReasonSuccess:               true,
		ReasonNoMatchingSubscribers: true,
		ReasonUnspecifiedError:      true,
		ReasonImplSpecificError:     true,
		ReasonNotAuthorized:         true,
		ReasonTopicNameInvalid:      true,
		ReasonPacketIDInUse:         true,
		ReasonQuotaExceeded:         true,
		ReasonPayloadFormatInvalid:  true,
	}

	// PUBREC valid reason codes
	pubrecReasonCodes = map[ReasonCode]bool{
		ReasonSuccess:               true,
		ReasonNoMatchingSubscribers: true,
		ReasonUnspecifiedError:      true,
		ReasonImplSpecificError:     true,
		ReasonNotAuthorized:         true,
		ReasonTopicNameInvalid:      true,
		ReasonPacketIDInUse:         true,
		ReasonQuotaExceeded:         true,
		ReasonPayloadFormatInvalid:  true,
	}

	// PUBREL valid reason codes
	pubrelReasonCodes = map[ReasonCode]bool{
		ReasonSuccess:          true,
		ReasonPacketIDNotFound: true,
	}

	// PUBCOMP valid reason codes
	pubcompReasonCodes = map[ReasonCode]bool{
		ReasonSuccess:          true,
		ReasonPacketIDNotFound: true,
	}

	// SUBACK valid reason codes
	subackReasonCodes = map[ReasonCode]bool{
		ReasonGrantedQoS0:              true,
		ReasonGrantedQoS1:              true,
		ReasonGrantedQoS2:              true,
		ReasonUnspecifiedError:         true,
		ReasonImplSpecificError:        true,
		ReasonNotAuthorized:            true,
		ReasonTopicFilterInvalid:       true,
		ReasonPacketIDInUse:            true,
		ReasonQuotaExceeded:            true,
		ReasonSharedSubsNotSupported:   true,
		ReasonSubIDsNotSupported:       true,
		ReasonWildcardSubsNotSupported: true,
	}

	// UNSUBACK valid reason codes
	unsubackReasonCodes = map[ReasonCode]bool{
		ReasonSuccess:               true,
		ReasonNoSubscriptionExisted: true,
		ReasonUnspecifiedError:      true,
		ReasonImplSpecificError:     true,
		ReasonNotAuthorized:         true,
		ReasonTopicFilterInvalid:    true,
		ReasonPacketIDInUse:         true,
	}

	// DISCONNECT valid reason codes
	disconnectReasonCodes = map[ReasonCode]bool{
		ReasonSuccess:                  true,
		ReasonDisconnectWithWill:       true,
		ReasonUnspecifiedError:         true,
		ReasonMalformedPacket:          true,
		ReasonProtocolError:            true,
		ReasonImplSpecificError:        true,
		ReasonNotAuthorized:            true,
		ReasonServerBusy:               true,
		ReasonServerShuttingDown:       true,
		ReasonKeepAliveTimeout:         true,
		ReasonSessionTakenOver:         true,
		ReasonTopicFilterInvalid:       true,
		ReasonTopicNameInvalid:         true,
		ReasonReceiveMaxExceeded:       true,
		ReasonTopicAliasInvalid:        true,
		ReasonPacketTooLarge:           true,
		ReasonMessageRateTooHigh:       true,
		ReasonQuotaExceeded:            true,
		ReasonAdminAction:              true,
		ReasonPayloadFormatInvalid:     true,
		ReasonRetainNotSupported:       true,
		ReasonQoSNotSupported:          true,
		ReasonUseAnotherServer:         true,
		ReasonServerMoved:              true,
		ReasonSharedSubsNotSupported:   true,
		ReasonMaxConnectTime:           true,
		ReasonSubIDsNotSupported:       true,
		ReasonWildcardSubsNotSupported: true,
	}

	// AUTH valid reason codes
	authReasonCodes = map[ReasonCode]bool{
		ReasonSuccess:      true,
		ReasonContinueAuth: true,
		ReasonReAuth:       true,
	}
)

// Alias for ReasonSuccess as QoS 0 granted
const ReasonGrantedQoS0 = ReasonSuccess

// ValidForCONNACK returns true if the reason code is valid for CONNACK packet.
func (r ReasonCode) ValidForCONNACK() bool {
	return connackReasonCodes[r]
}

// ValidForPUBACK returns true if the reason code is valid for PUBACK packet.
func (r ReasonCode) ValidForPUBACK() bool {
	return pubackReasonCodes[r]
}

// ValidForPUBREC returns true if the reason code is valid for PUBREC packet.
func (r ReasonCode) ValidForPUBREC() bool {
	return pubrecReasonCodes[r]
}

// ValidForPUBREL returns true if the reason code is valid for PUBREL packet.
func (r ReasonCode) ValidForPUBREL() bool {
	return pubrelReasonCodes[r]
}

// ValidForPUBCOMP returns true if the reason code is valid for PUBCOMP packet.
func (r ReasonCode) ValidForPUBCOMP() bool {
	return pubcompReasonCodes[r]
}

// ValidForSUBACK returns true if the reason code is valid for SUBACK packet.
func (r ReasonCode) ValidForSUBACK() bool {
	return subackReasonCodes[r]
}

// ValidForUNSUBACK returns true if the reason code is valid for UNSUBACK packet.
func (r ReasonCode) ValidForUNSUBACK() bool {
	return unsubackReasonCodes[r]
}

// ValidForDISCONNECT returns true if the reason code is valid for DISCONNECT packet.
func (r ReasonCode) ValidForDISCONNECT() bool {
	return disconnectReasonCodes[r]
}

// ValidForAUTH returns true if the reason code is valid for AUTH packet.
func (r ReasonCode) ValidForAUTH() bool {
	return authReasonCodes[r]
}
