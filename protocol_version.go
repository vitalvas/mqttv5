package mqttv5

// ProtocolVersion represents the MQTT protocol version.
type ProtocolVersion byte

const (
	// ProtocolV311 is MQTT 3.1.1 (OASIS standard, protocol level 4).
	ProtocolV311 ProtocolVersion = 4

	// ProtocolV5 is MQTT 5.0 (OASIS standard, protocol level 5).
	ProtocolV5 ProtocolVersion = 5
)

// String returns the human-readable protocol version name.
func (v ProtocolVersion) String() string {
	switch v {
	case ProtocolV311:
		return "3.1.1"
	case ProtocolV5:
		return "5.0"
	default:
		return "unknown"
	}
}

// IsValid returns true if the protocol version is supported.
func (v ProtocolVersion) IsValid() bool {
	return v == ProtocolV311 || v == ProtocolV5
}

// MQTT 3.1.1 CONNACK return codes.
const (
	v311ConnAccepted          byte = 0x00
	v311ConnUnacceptableProto byte = 0x01
	v311ConnIDRejected        byte = 0x02
	v311ConnServerUnavailable byte = 0x03
	v311ConnBadCredentials    byte = 0x04
	v311ConnNotAuthorized     byte = 0x05
)

// reasonCodeToV311ConnReturn maps a v5 ReasonCode to a v3.1.1 CONNACK return code.
func reasonCodeToV311ConnReturn(rc ReasonCode) byte {
	switch rc {
	case ReasonSuccess:
		return v311ConnAccepted
	case ReasonUnsupportedProtocolVersion:
		return v311ConnUnacceptableProto
	case ReasonClientIDNotValid:
		return v311ConnIDRejected
	case ReasonServerUnavailable, ReasonServerBusy, ReasonServerShuttingDown:
		return v311ConnServerUnavailable
	case ReasonBadUserNameOrPassword:
		return v311ConnBadCredentials
	case ReasonNotAuthorized, ReasonBanned:
		return v311ConnNotAuthorized
	default:
		if rc.IsError() {
			return v311ConnServerUnavailable
		}
		return v311ConnAccepted
	}
}

// v311ConnReturnToReasonCode maps a v3.1.1 CONNACK return code to a v5 ReasonCode.
func v311ConnReturnToReasonCode(rc byte) ReasonCode {
	switch rc {
	case v311ConnAccepted:
		return ReasonSuccess
	case v311ConnUnacceptableProto:
		return ReasonUnsupportedProtocolVersion
	case v311ConnIDRejected:
		return ReasonClientIDNotValid
	case v311ConnServerUnavailable:
		return ReasonServerUnavailable
	case v311ConnBadCredentials:
		return ReasonBadUserNameOrPassword
	case v311ConnNotAuthorized:
		return ReasonNotAuthorized
	default:
		return ReasonUnspecifiedError
	}
}

// reasonCodeToV311SubReturn maps a v5 ReasonCode to a v3.1.1 SUBACK return code.
func reasonCodeToV311SubReturn(rc ReasonCode) byte {
	switch rc {
	case ReasonSuccess:
		return 0x00
	case ReasonGrantedQoS1:
		return 0x01
	case ReasonGrantedQoS2:
		return 0x02
	default:
		return 0x80 // failure
	}
}

// v311SubReturnToReasonCode maps a v3.1.1 SUBACK return code to a v5 ReasonCode.
func v311SubReturnToReasonCode(rc byte) ReasonCode {
	switch rc {
	case 0x00:
		return ReasonGrantedQoS0
	case 0x01:
		return ReasonGrantedQoS1
	case 0x02:
		return ReasonGrantedQoS2
	default:
		return ReasonUnspecifiedError
	}
}
