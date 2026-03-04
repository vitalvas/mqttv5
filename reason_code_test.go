package mqttv5

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReasonCodeString(t *testing.T) {
	tests := []struct {
		code ReasonCode
		want string
	}{
		{ReasonSuccess, "Success"},
		{ReasonGrantedQoS1, "Granted QoS 1"},
		{ReasonGrantedQoS2, "Granted QoS 2"},
		{ReasonDisconnectWithWill, "Disconnect with Will Message"},
		{ReasonNoMatchingSubscribers, "No matching subscribers"},
		{ReasonUnspecifiedError, "Unspecified error"},
		{ReasonMalformedPacket, "Malformed Packet"},
		{ReasonProtocolError, "Protocol Error"},
		{ReasonNotAuthorized, "Not authorized"},
		{ReasonServerBusy, "Server busy"},
		{ReasonPacketTooLarge, "Packet too large"},
		{ReasonCode(0xFF), "Unknown reason code"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.code.String())
		})
	}
}

func TestReasonCodeIsError(t *testing.T) {
	tests := []struct {
		code    ReasonCode
		isError bool
	}{
		{ReasonSuccess, false},
		{ReasonGrantedQoS1, false},
		{ReasonGrantedQoS2, false},
		{ReasonDisconnectWithWill, false},
		{ReasonContinueAuth, false},
		{ReasonUnspecifiedError, true},
		{ReasonMalformedPacket, true},
		{ReasonProtocolError, true},
		{ReasonNotAuthorized, true},
		{ReasonCode(0x7F), false},
		{ReasonCode(0x80), true},
	}

	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			assert.Equal(t, tt.isError, tt.code.IsError())
			assert.Equal(t, !tt.isError, tt.code.IsSuccess())
		})
	}
}

func TestReasonCodeValidForCONNACK(t *testing.T) {
	validCodes := []ReasonCode{
		ReasonSuccess,
		ReasonUnspecifiedError,
		ReasonMalformedPacket,
		ReasonProtocolError,
		ReasonImplSpecificError,
		ReasonUnsupportedProtocolVersion,
		ReasonClientIDNotValid,
		ReasonBadUserNameOrPassword,
		ReasonNotAuthorized,
		ReasonServerUnavailable,
		ReasonServerBusy,
		ReasonBanned,
		ReasonBadAuthMethod,
		ReasonTopicNameInvalid,
		ReasonPacketTooLarge,
		ReasonQuotaExceeded,
		ReasonPayloadFormatInvalid,
		ReasonRetainNotSupported,
		ReasonQoSNotSupported,
		ReasonUseAnotherServer,
		ReasonServerMoved,
		ReasonConnectionRateExceeded,
	}

	for _, code := range validCodes {
		assert.True(t, code.ValidForCONNACK(), "expected %s to be valid for CONNACK", code)
	}

	invalidCodes := []ReasonCode{
		ReasonGrantedQoS1,
		ReasonGrantedQoS2,
		ReasonDisconnectWithWill,
		ReasonContinueAuth,
		ReasonReAuth,
	}

	for _, code := range invalidCodes {
		assert.False(t, code.ValidForCONNACK(), "expected %s to be invalid for CONNACK", code)
	}
}

func TestReasonCodeValidForPUBACK(t *testing.T) {
	validCodes := []ReasonCode{
		ReasonSuccess,
		ReasonNoMatchingSubscribers,
		ReasonUnspecifiedError,
		ReasonImplSpecificError,
		ReasonNotAuthorized,
		ReasonTopicNameInvalid,
		ReasonPacketIDInUse,
		ReasonQuotaExceeded,
		ReasonPayloadFormatInvalid,
	}

	for _, code := range validCodes {
		assert.True(t, code.ValidForPUBACK(), "expected %s to be valid for PUBACK", code)
	}

	assert.False(t, ReasonGrantedQoS1.ValidForPUBACK())
	assert.False(t, ReasonServerBusy.ValidForPUBACK())
}

func TestReasonCodeValidForPUBREC(t *testing.T) {
	validCodes := []ReasonCode{
		ReasonSuccess,
		ReasonNoMatchingSubscribers,
		ReasonUnspecifiedError,
		ReasonImplSpecificError,
		ReasonNotAuthorized,
		ReasonTopicNameInvalid,
		ReasonPacketIDInUse,
		ReasonQuotaExceeded,
		ReasonPayloadFormatInvalid,
	}

	for _, code := range validCodes {
		assert.True(t, code.ValidForPUBREC(), "expected %s to be valid for PUBREC", code)
	}
}

func TestReasonCodeValidForPUBREL(t *testing.T) {
	assert.True(t, ReasonSuccess.ValidForPUBREL())
	assert.True(t, ReasonPacketIDNotFound.ValidForPUBREL())
	assert.False(t, ReasonUnspecifiedError.ValidForPUBREL())
}

func TestReasonCodeValidForPUBCOMP(t *testing.T) {
	assert.True(t, ReasonSuccess.ValidForPUBCOMP())
	assert.True(t, ReasonPacketIDNotFound.ValidForPUBCOMP())
	assert.False(t, ReasonUnspecifiedError.ValidForPUBCOMP())
}

func TestReasonCodeValidForSUBACK(t *testing.T) {
	validCodes := []ReasonCode{
		ReasonGrantedQoS0,
		ReasonGrantedQoS1,
		ReasonGrantedQoS2,
		ReasonUnspecifiedError,
		ReasonImplSpecificError,
		ReasonNotAuthorized,
		ReasonTopicFilterInvalid,
		ReasonPacketIDInUse,
		ReasonQuotaExceeded,
		ReasonSharedSubsNotSupported,
		ReasonSubIDsNotSupported,
		ReasonWildcardSubsNotSupported,
	}

	for _, code := range validCodes {
		assert.True(t, code.ValidForSUBACK(), "expected %s to be valid for SUBACK", code)
	}

	assert.False(t, ReasonServerBusy.ValidForSUBACK())
}

func TestReasonCodeValidForUNSUBACK(t *testing.T) {
	validCodes := []ReasonCode{
		ReasonSuccess,
		ReasonNoSubscriptionExisted,
		ReasonUnspecifiedError,
		ReasonImplSpecificError,
		ReasonNotAuthorized,
		ReasonTopicFilterInvalid,
		ReasonPacketIDInUse,
	}

	for _, code := range validCodes {
		assert.True(t, code.ValidForUNSUBACK(), "expected %s to be valid for UNSUBACK", code)
	}

	assert.False(t, ReasonServerBusy.ValidForUNSUBACK())
}

func TestReasonCodeValidForDISCONNECT(t *testing.T) {
	validCodes := []ReasonCode{
		ReasonSuccess,
		ReasonDisconnectWithWill,
		ReasonUnspecifiedError,
		ReasonMalformedPacket,
		ReasonProtocolError,
		ReasonServerBusy,
		ReasonServerShuttingDown,
		ReasonKeepAliveTimeout,
		ReasonSessionTakenOver,
		ReasonTopicFilterInvalid,
		ReasonTopicNameInvalid,
		ReasonPacketTooLarge,
		ReasonQuotaExceeded,
		ReasonAdminAction,
		ReasonMaxConnectTime,
	}

	for _, code := range validCodes {
		assert.True(t, code.ValidForDISCONNECT(), "expected %s to be valid for DISCONNECT", code)
	}
}

func TestReasonCodeValidForAUTH(t *testing.T) {
	assert.True(t, ReasonSuccess.ValidForAUTH())
	assert.True(t, ReasonContinueAuth.ValidForAUTH())
	assert.True(t, ReasonReAuth.ValidForAUTH())
	assert.False(t, ReasonUnspecifiedError.ValidForAUTH())
	assert.False(t, ReasonNotAuthorized.ValidForAUTH())
}

func TestGrantedQoS0Alias(t *testing.T) {
	assert.Equal(t, ReasonSuccess, ReasonGrantedQoS0)
	assert.Equal(t, ReasonCode(0x00), ReasonGrantedQoS0)
}

func TestReasonCodesPerSpecification(t *testing.T) {
	t.Run("success reason codes", func(t *testing.T) {
		assert.Equal(t, ReasonCode(0x00), ReasonSuccess)
		assert.Equal(t, ReasonCode(0x00), ReasonGrantedQoS0)
		assert.Equal(t, ReasonCode(0x01), ReasonGrantedQoS1)
		assert.Equal(t, ReasonCode(0x02), ReasonGrantedQoS2)
	})

	t.Run("disconnect and auth reason codes", func(t *testing.T) {
		assert.Equal(t, ReasonCode(0x04), ReasonDisconnectWithWill)
		assert.Equal(t, ReasonCode(0x10), ReasonNoMatchingSubscribers)
		assert.Equal(t, ReasonCode(0x11), ReasonNoSubscriptionExisted)
		assert.Equal(t, ReasonCode(0x18), ReasonContinueAuth)
		assert.Equal(t, ReasonCode(0x19), ReasonReAuth)
	})

	t.Run("error reason codes", func(t *testing.T) {
		errorCodes := map[byte]ReasonCode{
			0x80: ReasonUnspecifiedError,
			0x81: ReasonMalformedPacket,
			0x82: ReasonProtocolError,
			0x83: ReasonImplSpecificError,
			0x84: ReasonUnsupportedProtocolVersion,
			0x85: ReasonClientIDNotValid,
			0x86: ReasonBadUserNameOrPassword,
			0x87: ReasonNotAuthorized,
			0x88: ReasonServerUnavailable,
			0x89: ReasonServerBusy,
			0x8A: ReasonBanned,
			0x8B: ReasonServerShuttingDown,
			0x8C: ReasonBadAuthMethod,
			0x8D: ReasonKeepAliveTimeout,
			0x8E: ReasonSessionTakenOver,
			0x8F: ReasonTopicFilterInvalid,
			0x90: ReasonTopicNameInvalid,
			0x91: ReasonPacketIDInUse,
			0x92: ReasonPacketIDNotFound,
			0x93: ReasonReceiveMaxExceeded,
			0x94: ReasonTopicAliasInvalid,
			0x95: ReasonPacketTooLarge,
			0x96: ReasonMessageRateTooHigh,
			0x97: ReasonQuotaExceeded,
			0x98: ReasonAdminAction,
			0x99: ReasonPayloadFormatInvalid,
			0x9A: ReasonRetainNotSupported,
			0x9B: ReasonQoSNotSupported,
			0x9C: ReasonUseAnotherServer,
			0x9D: ReasonServerMoved,
			0x9E: ReasonSharedSubsNotSupported,
			0x9F: ReasonConnectionRateExceeded,
		}

		for expected, code := range errorCodes {
			assert.Equal(t, ReasonCode(expected), code, "Code 0x%X mismatch", expected)
		}
	})

	t.Run("extended error reason codes", func(t *testing.T) {
		assert.Equal(t, ReasonCode(0xA0), ReasonMaxConnectTime)
		assert.Equal(t, ReasonCode(0xA1), ReasonSubIDsNotSupported)
		assert.Equal(t, ReasonCode(0xA2), ReasonWildcardSubsNotSupported)
	})

	t.Run("all reason codes have string representation", func(t *testing.T) {
		codes := []ReasonCode{
			ReasonSuccess, ReasonGrantedQoS1, ReasonGrantedQoS2,
			ReasonDisconnectWithWill, ReasonNoMatchingSubscribers, ReasonNoSubscriptionExisted,
			ReasonContinueAuth, ReasonReAuth,
			ReasonUnspecifiedError, ReasonMalformedPacket, ReasonProtocolError,
			ReasonImplSpecificError, ReasonUnsupportedProtocolVersion, ReasonClientIDNotValid,
			ReasonBadUserNameOrPassword, ReasonNotAuthorized, ReasonServerUnavailable,
			ReasonServerBusy, ReasonBanned, ReasonServerShuttingDown,
			ReasonBadAuthMethod, ReasonKeepAliveTimeout, ReasonSessionTakenOver,
			ReasonTopicFilterInvalid, ReasonTopicNameInvalid, ReasonPacketIDInUse,
			ReasonPacketIDNotFound, ReasonReceiveMaxExceeded, ReasonTopicAliasInvalid,
			ReasonPacketTooLarge, ReasonMessageRateTooHigh, ReasonQuotaExceeded,
			ReasonAdminAction, ReasonPayloadFormatInvalid, ReasonRetainNotSupported,
			ReasonQoSNotSupported, ReasonUseAnotherServer, ReasonServerMoved,
			ReasonSharedSubsNotSupported, ReasonConnectionRateExceeded,
			ReasonMaxConnectTime, ReasonSubIDsNotSupported, ReasonWildcardSubsNotSupported,
		}

		for _, code := range codes {
			str := code.String()
			assert.NotEmpty(t, str, "Reason code %d should have string representation", code)
			assert.NotEqual(t, "Unknown reason code", str, "Reason code %d should have known string", code)
		}
	})
}

func TestReasonCodeValidityPerPacketType(t *testing.T) {
	t.Run("CONNACK valid reason codes", func(t *testing.T) {
		validCodes := []ReasonCode{
			ReasonSuccess, ReasonUnspecifiedError, ReasonMalformedPacket,
			ReasonProtocolError, ReasonImplSpecificError, ReasonUnsupportedProtocolVersion,
			ReasonClientIDNotValid, ReasonBadUserNameOrPassword, ReasonNotAuthorized,
			ReasonServerUnavailable, ReasonServerBusy, ReasonBanned,
			ReasonBadAuthMethod, ReasonTopicNameInvalid, ReasonPacketTooLarge,
			ReasonQuotaExceeded, ReasonPayloadFormatInvalid, ReasonRetainNotSupported,
			ReasonQoSNotSupported, ReasonUseAnotherServer, ReasonServerMoved,
			ReasonConnectionRateExceeded,
		}

		for _, code := range validCodes {
			assert.True(t, code.ValidForCONNACK(), "Code %d should be valid for CONNACK", code)
		}
	})

	t.Run("PUBACK valid reason codes", func(t *testing.T) {
		validCodes := []ReasonCode{
			ReasonSuccess, ReasonNoMatchingSubscribers, ReasonUnspecifiedError,
			ReasonImplSpecificError, ReasonNotAuthorized, ReasonTopicNameInvalid,
			ReasonPacketIDInUse, ReasonQuotaExceeded, ReasonPayloadFormatInvalid,
		}

		for _, code := range validCodes {
			assert.True(t, code.ValidForPUBACK(), "Code %d should be valid for PUBACK", code)
		}
	})

	t.Run("SUBACK valid reason codes", func(t *testing.T) {
		validCodes := []ReasonCode{
			ReasonGrantedQoS0, ReasonGrantedQoS1, ReasonGrantedQoS2,
			ReasonUnspecifiedError, ReasonImplSpecificError, ReasonNotAuthorized,
			ReasonTopicFilterInvalid, ReasonPacketIDInUse, ReasonQuotaExceeded,
			ReasonSharedSubsNotSupported, ReasonSubIDsNotSupported, ReasonWildcardSubsNotSupported,
		}

		for _, code := range validCodes {
			assert.True(t, code.ValidForSUBACK(), "Code %d should be valid for SUBACK", code)
		}
	})

	t.Run("UNSUBACK valid reason codes", func(t *testing.T) {
		validCodes := []ReasonCode{
			ReasonSuccess, ReasonNoSubscriptionExisted, ReasonUnspecifiedError,
			ReasonImplSpecificError, ReasonNotAuthorized, ReasonTopicFilterInvalid,
			ReasonPacketIDInUse,
		}

		for _, code := range validCodes {
			assert.True(t, code.ValidForUNSUBACK(), "Code %d should be valid for UNSUBACK", code)
		}
	})

	t.Run("DISCONNECT valid reason codes", func(t *testing.T) {
		validCodes := []ReasonCode{
			ReasonSuccess, ReasonDisconnectWithWill, ReasonUnspecifiedError,
			ReasonMalformedPacket, ReasonProtocolError, ReasonImplSpecificError,
			ReasonNotAuthorized, ReasonServerBusy, ReasonServerShuttingDown,
			ReasonKeepAliveTimeout, ReasonSessionTakenOver, ReasonTopicFilterInvalid,
			ReasonTopicNameInvalid, ReasonReceiveMaxExceeded, ReasonTopicAliasInvalid,
			ReasonPacketTooLarge, ReasonMessageRateTooHigh, ReasonQuotaExceeded,
			ReasonAdminAction, ReasonPayloadFormatInvalid, ReasonRetainNotSupported,
			ReasonQoSNotSupported, ReasonUseAnotherServer, ReasonServerMoved,
			ReasonSharedSubsNotSupported, ReasonMaxConnectTime,
			ReasonSubIDsNotSupported, ReasonWildcardSubsNotSupported,
		}

		for _, code := range validCodes {
			assert.True(t, code.ValidForDISCONNECT(), "Code %d should be valid for DISCONNECT", code)
		}
	})

	t.Run("AUTH valid reason codes", func(t *testing.T) {
		validCodes := []ReasonCode{
			ReasonSuccess, ReasonContinueAuth, ReasonReAuth,
		}

		for _, code := range validCodes {
			assert.True(t, code.ValidForAUTH(), "Code %d should be valid for AUTH", code)
		}
	})
}

func TestReasonCodeErrorClassification(t *testing.T) {
	t.Run("success codes are not errors", func(t *testing.T) {
		successCodes := []ReasonCode{
			ReasonSuccess, ReasonGrantedQoS0, ReasonGrantedQoS1, ReasonGrantedQoS2,
			ReasonDisconnectWithWill, ReasonNoMatchingSubscribers, ReasonNoSubscriptionExisted,
			ReasonContinueAuth, ReasonReAuth,
		}

		for _, code := range successCodes {
			assert.True(t, code.IsSuccess(), "Code %d should be success", code)
			assert.False(t, code.IsError(), "Code %d should not be error", code)
		}
	})

	t.Run("error codes are errors", func(t *testing.T) {
		errorCodes := []ReasonCode{
			ReasonUnspecifiedError, ReasonMalformedPacket, ReasonProtocolError,
			ReasonImplSpecificError, ReasonNotAuthorized, ReasonPacketTooLarge,
		}

		for _, code := range errorCodes {
			assert.True(t, code.IsError(), "Code %d should be error", code)
			assert.False(t, code.IsSuccess(), "Code %d should not be success", code)
		}
	})
}
