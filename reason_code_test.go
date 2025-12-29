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
