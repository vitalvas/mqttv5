package mqttv5

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtocolVersionString(t *testing.T) {
	tests := []struct {
		version  ProtocolVersion
		expected string
	}{
		{ProtocolV311, "3.1.1"},
		{ProtocolV5, "5.0"},
		{ProtocolVersion(0), "unknown"},
		{ProtocolVersion(99), "unknown"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.version.String())
	}
}

func TestProtocolVersionIsValid(t *testing.T) {
	assert.True(t, ProtocolV311.IsValid())
	assert.True(t, ProtocolV5.IsValid())
	assert.False(t, ProtocolVersion(0).IsValid())
	assert.False(t, ProtocolVersion(3).IsValid())
	assert.False(t, ProtocolVersion(6).IsValid())
}

func TestReasonCodeToV311ConnReturn(t *testing.T) {
	tests := []struct {
		name     string
		reason   ReasonCode
		expected byte
	}{
		{"success", ReasonSuccess, v311ConnAccepted},
		{"unsupported version", ReasonUnsupportedProtocolVersion, v311ConnUnacceptableProto},
		{"client id not valid", ReasonClientIDNotValid, v311ConnIDRejected},
		{"server unavailable", ReasonServerUnavailable, v311ConnServerUnavailable},
		{"server busy", ReasonServerBusy, v311ConnServerUnavailable},
		{"server shutting down", ReasonServerShuttingDown, v311ConnServerUnavailable},
		{"bad credentials", ReasonBadUserNameOrPassword, v311ConnBadCredentials},
		{"not authorized", ReasonNotAuthorized, v311ConnNotAuthorized},
		{"banned", ReasonBanned, v311ConnNotAuthorized},
		{"generic error", ReasonUnspecifiedError, v311ConnServerUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, reasonCodeToV311ConnReturn(tt.reason))
		})
	}
}

func TestV311ConnReturnToReasonCode(t *testing.T) {
	tests := []struct {
		name     string
		rc       byte
		expected ReasonCode
	}{
		{"accepted", v311ConnAccepted, ReasonSuccess},
		{"unacceptable proto", v311ConnUnacceptableProto, ReasonUnsupportedProtocolVersion},
		{"id rejected", v311ConnIDRejected, ReasonClientIDNotValid},
		{"server unavailable", v311ConnServerUnavailable, ReasonServerUnavailable},
		{"bad credentials", v311ConnBadCredentials, ReasonBadUserNameOrPassword},
		{"not authorized", v311ConnNotAuthorized, ReasonNotAuthorized},
		{"unknown", 0x06, ReasonUnspecifiedError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, v311ConnReturnToReasonCode(tt.rc))
		})
	}
}

func TestReasonCodeToV311SubReturn(t *testing.T) {
	assert.Equal(t, byte(0x00), reasonCodeToV311SubReturn(ReasonSuccess))
	assert.Equal(t, byte(0x01), reasonCodeToV311SubReturn(ReasonGrantedQoS1))
	assert.Equal(t, byte(0x02), reasonCodeToV311SubReturn(ReasonGrantedQoS2))
	assert.Equal(t, byte(0x80), reasonCodeToV311SubReturn(ReasonUnspecifiedError))
	assert.Equal(t, byte(0x80), reasonCodeToV311SubReturn(ReasonNotAuthorized))
}

func TestV311SubReturnToReasonCode(t *testing.T) {
	assert.Equal(t, ReasonGrantedQoS0, v311SubReturnToReasonCode(0x00))
	assert.Equal(t, ReasonGrantedQoS1, v311SubReturnToReasonCode(0x01))
	assert.Equal(t, ReasonGrantedQoS2, v311SubReturnToReasonCode(0x02))
	assert.Equal(t, ReasonUnspecifiedError, v311SubReturnToReasonCode(0x80))
	assert.Equal(t, ReasonUnspecifiedError, v311SubReturnToReasonCode(0xFF))
}
