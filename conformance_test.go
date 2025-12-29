package mqttv5

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReasonCodesPerSpecification tests that all reason codes are correctly defined
// per MQTT v5.0 specification section 2.4.
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

// TestReasonCodeValidityPerPacketType tests reason code validity per packet type.
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

// TestPropertyIdentifierValues tests property identifier values match specification.
func TestPropertyIdentifierValues(t *testing.T) {
	t.Run("property identifier values", func(t *testing.T) {
		assert.Equal(t, PropertyID(0x01), PropPayloadFormatIndicator)
		assert.Equal(t, PropertyID(0x02), PropMessageExpiryInterval)
		assert.Equal(t, PropertyID(0x03), PropContentType)
		assert.Equal(t, PropertyID(0x08), PropResponseTopic)
		assert.Equal(t, PropertyID(0x09), PropCorrelationData)
		assert.Equal(t, PropertyID(0x0B), PropSubscriptionIdentifier)
		assert.Equal(t, PropertyID(0x11), PropSessionExpiryInterval)
		assert.Equal(t, PropertyID(0x12), PropAssignedClientIdentifier)
		assert.Equal(t, PropertyID(0x13), PropServerKeepAlive)
		assert.Equal(t, PropertyID(0x15), PropAuthenticationMethod)
		assert.Equal(t, PropertyID(0x16), PropAuthenticationData)
		assert.Equal(t, PropertyID(0x17), PropRequestProblemInfo)
		assert.Equal(t, PropertyID(0x18), PropWillDelayInterval)
		assert.Equal(t, PropertyID(0x19), PropRequestResponseInfo)
		assert.Equal(t, PropertyID(0x1A), PropResponseInformation)
		assert.Equal(t, PropertyID(0x1C), PropServerReference)
		assert.Equal(t, PropertyID(0x1F), PropReasonString)
		assert.Equal(t, PropertyID(0x21), PropReceiveMaximum)
		assert.Equal(t, PropertyID(0x22), PropTopicAliasMaximum)
		assert.Equal(t, PropertyID(0x23), PropTopicAlias)
		assert.Equal(t, PropertyID(0x24), PropMaximumQoS)
		assert.Equal(t, PropertyID(0x25), PropRetainAvailable)
		assert.Equal(t, PropertyID(0x26), PropUserProperty)
		assert.Equal(t, PropertyID(0x27), PropMaximumPacketSize)
		assert.Equal(t, PropertyID(0x28), PropWildcardSubAvailable)
		assert.Equal(t, PropertyID(0x29), PropSubscriptionIDAvailable)
		assert.Equal(t, PropertyID(0x2A), PropSharedSubAvailable)
	})
}

// TestPacketTypeValues tests packet type values match specification.
func TestPacketTypeValues(t *testing.T) {
	t.Run("packet type values", func(t *testing.T) {
		assert.Equal(t, PacketType(1), PacketCONNECT)
		assert.Equal(t, PacketType(2), PacketCONNACK)
		assert.Equal(t, PacketType(3), PacketPUBLISH)
		assert.Equal(t, PacketType(4), PacketPUBACK)
		assert.Equal(t, PacketType(5), PacketPUBREC)
		assert.Equal(t, PacketType(6), PacketPUBREL)
		assert.Equal(t, PacketType(7), PacketPUBCOMP)
		assert.Equal(t, PacketType(8), PacketSUBSCRIBE)
		assert.Equal(t, PacketType(9), PacketSUBACK)
		assert.Equal(t, PacketType(10), PacketUNSUBSCRIBE)
		assert.Equal(t, PacketType(11), PacketUNSUBACK)
		assert.Equal(t, PacketType(12), PacketPINGREQ)
		assert.Equal(t, PacketType(13), PacketPINGRESP)
		assert.Equal(t, PacketType(14), PacketDISCONNECT)
		assert.Equal(t, PacketType(15), PacketAUTH)
	})

	t.Run("packet type string representation", func(t *testing.T) {
		types := []PacketType{
			PacketCONNECT, PacketCONNACK, PacketPUBLISH, PacketPUBACK,
			PacketPUBREC, PacketPUBREL, PacketPUBCOMP, PacketSUBSCRIBE,
			PacketSUBACK, PacketUNSUBSCRIBE, PacketUNSUBACK, PacketPINGREQ,
			PacketPINGRESP, PacketDISCONNECT, PacketAUTH,
		}

		for _, pt := range types {
			str := pt.String()
			assert.NotEmpty(t, str, "Packet type %d should have string representation", pt)
			assert.NotEqual(t, "UNKNOWN", str, "Packet type %d should have known string", pt)
		}
	})
}

// TestProtocolEdgeCases tests edge cases from the MQTT v5.0 specification.
func TestProtocolEdgeCases(t *testing.T) {
	t.Run("empty client ID requires clean start", func(t *testing.T) {
		pkt := &ConnectPacket{
			ClientID:   "",
			CleanStart: false,
		}
		err := pkt.Validate()
		assert.Error(t, err, "Empty client ID without clean start should be invalid")
	})

	t.Run("empty client ID with clean start is valid", func(t *testing.T) {
		pkt := &ConnectPacket{
			ClientID:   "",
			CleanStart: true,
		}
		err := pkt.Validate()
		assert.NoError(t, err, "Empty client ID with clean start should be valid")
	})

	t.Run("QoS 0 publish has no packet ID requirement", func(t *testing.T) {
		pkt := &PublishPacket{
			Topic:    "test/topic",
			QoS:      0,
			PacketID: 0,
		}
		err := pkt.Validate()
		assert.NoError(t, err)
	})

	t.Run("QoS 1 publish requires packet ID", func(t *testing.T) {
		pkt := &PublishPacket{
			Topic:    "test/topic",
			QoS:      1,
			PacketID: 0,
		}
		err := pkt.Validate()
		assert.Error(t, err, "QoS 1 without packet ID should be invalid")
	})

	t.Run("QoS 2 publish requires packet ID", func(t *testing.T) {
		pkt := &PublishPacket{
			Topic:    "test/topic",
			QoS:      2,
			PacketID: 0,
		}
		err := pkt.Validate()
		assert.Error(t, err, "QoS 2 without packet ID should be invalid")
	})

	t.Run("topic filter with wildcards", func(t *testing.T) {
		assert.NoError(t, ValidateTopicFilter("sensor/+/temp"))
		assert.NoError(t, ValidateTopicFilter("sensor/#"))
		assert.NoError(t, ValidateTopicFilter("+/+/+"))
		assert.NoError(t, ValidateTopicFilter("#"))
	})

	t.Run("topic name without wildcards", func(t *testing.T) {
		assert.NoError(t, ValidateTopicName("sensor/1/temp"))
		assert.Error(t, ValidateTopicName("sensor/+/temp"), "Topic name should not contain wildcards")
		assert.Error(t, ValidateTopicName("sensor/#"), "Topic name should not contain wildcards")
	})

	t.Run("shared subscription parsing", func(t *testing.T) {
		ss, err := ParseSharedSubscription("$share/group1/sensor/+/temp")
		require.NoError(t, err)
		assert.Equal(t, "group1", ss.ShareName)
		assert.Equal(t, "sensor/+/temp", ss.TopicFilter)
	})

	t.Run("system topic matching", func(t *testing.T) {
		assert.True(t, IsSystemTopic("$SYS/broker/uptime"))
		assert.False(t, IsSystemTopic("sensor/temp"))

		// # should not match system topics
		assert.False(t, TopicMatch("#", "$SYS/broker/uptime"))

		// Explicit $SYS/# should match
		assert.True(t, TopicMatch("$SYS/#", "$SYS/broker/uptime"))
	})
}

// TestPacketEncodingRoundTrip tests that all packet types can be encoded and decoded.
func TestPacketEncodingRoundTrip(t *testing.T) {
	maxSize := uint32(1024 * 1024) // 1MB max size

	packets := []Packet{
		&ConnectPacket{
			ClientID:   "test-client",
			CleanStart: true,
			KeepAlive:  60,
		},
		&ConnackPacket{
			SessionPresent: true,
			ReasonCode:     ReasonSuccess,
		},
		&PublishPacket{
			Topic:    "test/topic",
			QoS:      1,
			PacketID: 1,
			Payload:  []byte("hello"),
		},
		&PubackPacket{
			PacketID:   1,
			ReasonCode: ReasonSuccess,
		},
		&PubrecPacket{
			PacketID:   1,
			ReasonCode: ReasonSuccess,
		},
		&PubrelPacket{
			PacketID:   1,
			ReasonCode: ReasonSuccess,
		},
		&PubcompPacket{
			PacketID:   1,
			ReasonCode: ReasonSuccess,
		},
		&SubscribePacket{
			PacketID: 1,
			Subscriptions: []Subscription{
				{TopicFilter: "test/#", QoS: 1},
			},
		},
		&SubackPacket{
			PacketID:    1,
			ReasonCodes: []ReasonCode{ReasonGrantedQoS1},
		},
		&UnsubscribePacket{
			PacketID:     1,
			TopicFilters: []string{"test/#"},
		},
		&UnsubackPacket{
			PacketID:    1,
			ReasonCodes: []ReasonCode{ReasonSuccess},
		},
		&PingreqPacket{},
		&PingrespPacket{},
		&DisconnectPacket{
			ReasonCode: ReasonSuccess,
		},
		&AuthPacket{
			ReasonCode: ReasonContinueAuth,
		},
	}

	for _, pkt := range packets {
		t.Run(pkt.Type().String(), func(t *testing.T) {
			buf := &bytes.Buffer{}
			n, err := WritePacket(buf, pkt, maxSize)
			require.NoError(t, err)
			assert.Greater(t, n, 0)

			decoded, m, err := ReadPacket(bytes.NewReader(buf.Bytes()), maxSize)
			require.NoError(t, err)
			assert.Equal(t, n, m)
			assert.Equal(t, pkt.Type(), decoded.Type())
		})
	}
}

// TestZeroAllocPaths verifies that hot code paths have minimal allocations.
func TestZeroAllocPaths(t *testing.T) {
	t.Run("fixed header encoding has minimal allocs", func(t *testing.T) {
		header := FixedHeader{
			PacketType:      PacketPUBLISH,
			Flags:           0x00,
			RemainingLength: 100,
		}
		buf := &bytes.Buffer{}

		result := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf.Reset()
				_, _ = header.Encode(buf)
			}
		})

		allocsPerOp := result.AllocsPerOp()
		// Allow up to 3 allocations for header encoding (buffer growth, etc.)
		assert.LessOrEqual(t, allocsPerOp, int64(3),
			"Fixed header encoding should have at most 3 allocs, got %d", allocsPerOp)
	})
}

// TestReasonCodeErrorClassification tests IsError and IsSuccess methods.
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
