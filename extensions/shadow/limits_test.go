package shadow

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

func TestValidatePayload(t *testing.T) {
	t.Run("valid payload", func(t *testing.T) {
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0}},
		})

		req, err := validatePayload(payload, defaultLimits())
		require.NoError(t, err)
		require.NotNil(t, req)
		assert.Equal(t, 22.0, req.State.Desired["temp"])
	})

	t.Run("payload exceeds max size", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: 64}
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"key": strings.Repeat("x", 100)}},
		})

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodePayloadTooLarge, errResp.Code)
		assert.Contains(t, errResp.Message, "payload exceeds maximum size")
	})

	t.Run("invalid JSON", func(t *testing.T) {
		_, err := validatePayload([]byte("not json"), defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "invalid JSON")
	})

	t.Run("unknown fields rejected", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":{"temp":22}},"unknown_field":true}`)

		_, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "invalid JSON")
	})

	t.Run("empty state rejected", func(t *testing.T) {
		payload := []byte(`{"state":{}}`)

		_, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "state must contain desired or reported")
	})

	t.Run("nesting depth exceeded", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: 2}
		payload := []byte(`{"state":{"desired":{"a":{"b":{"c":1}}}}}`)

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "nesting depth exceeds maximum")
	})

	t.Run("nesting depth at limit is allowed", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: 2}
		payload := []byte(`{"state":{"desired":{"a":{"b":1}}}}`)

		req, err := validatePayload(payload, limits)
		require.NoError(t, err)
		require.NotNil(t, req)
	})

	t.Run("max keys per object exceeded", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: defaultMaxDepth, MaxKeysPerObject: 2}
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"a": 1, "b": 2, "c": 3}},
		})

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "keys, maximum is")
	})

	t.Run("max keys per object at limit is allowed", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: defaultMaxDepth, MaxKeysPerObject: 2}
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"a": 1, "b": 2}},
		})

		req, err := validatePayload(payload, limits)
		require.NoError(t, err)
		require.NotNil(t, req)
	})

	t.Run("max key length exceeded", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: defaultMaxDepth, MaxKeyLength: 5}
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"toolong": 1}},
		})

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "exceeds maximum length")
	})

	t.Run("max value length exceeded", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: defaultMaxDepth, MaxValueLength: 5}
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"k": "toolong"}},
		})

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "string value exceeds maximum length")
	})

	t.Run("nested map validated", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: defaultMaxDepth, MaxKeyLength: 3}
		payload := []byte(`{"state":{"desired":{"a":{"toolong":1}}}}`)

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "exceeds maximum length")
	})

	t.Run("array values validated", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: defaultMaxDepth, MaxValueLength: 3}
		payload := []byte(`{"state":{"desired":{"list":["ok","toolong"]}}}`)

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "string value exceeds maximum length")
	})

	t.Run("reported state validated", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: 1}
		payload := []byte(`{"state":{"reported":{"a":{"b":1}}}}`)

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "nesting depth exceeds maximum")
	})

	t.Run("zero limits means unlimited", func(t *testing.T) {
		limits := Limits{}
		deep := map[string]any{"a": map[string]any{"b": map[string]any{"c": map[string]any{"d": 1}}}}
		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: deep},
		})

		req, err := validatePayload(payload, limits)
		require.NoError(t, err)
		require.NotNil(t, req)
	})

	t.Run("version field allowed", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":{"temp":22}},"version":1}`)

		req, err := validatePayload(payload, defaultLimits())
		require.NoError(t, err)
		require.NotNil(t, req)
		require.NotNil(t, req.Version)
		assert.Equal(t, int64(1), *req.Version)
	})

	t.Run("clientToken parsed", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":{"temp":22}},"clientToken":"myToken"}`)

		req, err := validatePayload(payload, defaultLimits())
		require.NoError(t, err)
		require.NotNil(t, req)
		assert.Equal(t, "myToken", req.ClientToken)
	})

	t.Run("desired null sets ClearDesired", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":null,"reported":{"temp":20}}}`)

		req, err := validatePayload(payload, defaultLimits())
		require.NoError(t, err)
		require.NotNil(t, req)
		assert.True(t, req.ClearDesired)
		assert.Nil(t, req.State.Desired)
		assert.Equal(t, 20.0, req.State.Reported["temp"])
	})

	t.Run("reported null sets ClearReported", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":{"temp":22},"reported":null}}`)

		req, err := validatePayload(payload, defaultLimits())
		require.NoError(t, err)
		require.NotNil(t, req)
		assert.False(t, req.ClearDesired)
		assert.True(t, req.ClearReported)
		assert.Nil(t, req.State.Reported)
	})

	t.Run("both null is valid", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":null,"reported":null}}`)

		req, err := validatePayload(payload, defaultLimits())
		require.NoError(t, err)
		require.NotNil(t, req)
		assert.True(t, req.ClearDesired)
		assert.True(t, req.ClearReported)
	})

	t.Run("null in array rejected", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":{"tags":["a",null,"c"]}}}`)

		_, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "null values are not allowed in arrays")
	})

	t.Run("null in nested array rejected", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":{"config":{"items":[1,null,3]}}}}`)

		_, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "null values are not allowed in arrays")
	})

	t.Run("invalid UTF-8 rejected", func(t *testing.T) {
		payload := []byte{0x7b, 0x22, 0x73, 0x74, 0x61, 0x74, 0x65, 0x22, 0x3a, 0x7b, 0x22, 0x64, 0x65, 0x73, 0x69, 0x72, 0x65, 0x64, 0x22, 0x3a, 0x7b, 0x22, 0x6b, 0x22, 0x3a, 0x22, 0xff, 0xfe, 0x22, 0x7d, 0x7d, 0x7d}

		_, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeUnsupportedEncoding, errResp.Code)
		assert.Contains(t, errResp.Message, "not valid UTF-8")
	})

	t.Run("clientToken too long rejected", func(t *testing.T) {
		longToken := strings.Repeat("x", 65)
		payload := []byte(fmt.Sprintf(`{"state":{"desired":{"temp":22}},"clientToken":"%s"}`, longToken))

		req, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)
		require.NotNil(t, req)
		assert.Equal(t, longToken, req.ClientToken)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "clientToken exceeds maximum length")
	})

	t.Run("clientToken at limit allowed", func(t *testing.T) {
		token := strings.Repeat("x", 64)
		payload := []byte(fmt.Sprintf(`{"state":{"desired":{"temp":22}},"clientToken":"%s"}`, token))

		req, err := validatePayload(payload, defaultLimits())
		require.NoError(t, err)
		require.NotNil(t, req)
		assert.Equal(t, token, req.ClientToken)
	})

	t.Run("returns partial req on validation error", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: 1}
		payload := []byte(`{"state":{"desired":{"a":{"b":1}}},"clientToken":"partialToken"}`)

		req, err := validatePayload(payload, limits)
		require.Error(t, err)
		require.NotNil(t, req)
		assert.Equal(t, "partialToken", req.ClientToken)
	})
}

func TestValidateState(t *testing.T) {
	t.Run("valid state", func(t *testing.T) {
		err := validateState(UpdateState{
			Desired: map[string]any{"temp": 22.0},
		}, defaultLimits())
		require.NoError(t, err)
	})

	t.Run("depth exceeded in desired", func(t *testing.T) {
		limits := Limits{MaxDepth: 1}
		err := validateState(UpdateState{
			Desired: map[string]any{"a": map[string]any{"b": 1}},
		}, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
	})

	t.Run("depth exceeded in reported", func(t *testing.T) {
		limits := Limits{MaxDepth: 1}
		err := validateState(UpdateState{
			Reported: map[string]any{"a": map[string]any{"b": 1}},
		}, limits)
		require.Error(t, err)
	})

	t.Run("nil maps pass", func(t *testing.T) {
		err := validateState(UpdateState{}, defaultLimits())
		require.NoError(t, err)
	})

	t.Run("null in array via validateState", func(t *testing.T) {
		err := validateState(UpdateState{
			Desired: map[string]any{"tags": []any{"a", nil}},
		}, defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "null values are not allowed in arrays")
	})
}

func TestHandler_ValidationIntegration(t *testing.T) {
	t.Run("handleUpdate rejects oversized payload", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithLimits(Limits{
			MaxPayloadSize: 64,
			MaxDepth:       defaultMaxDepth,
		}))
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"key": strings.Repeat("x", 100)}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodePayloadTooLarge, errResp.Code)
		assert.Contains(t, errResp.Message, "payload exceeds maximum size")
	})

	t.Run("handleUpdate rejects unknown fields", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: []byte(`{"state":{"desired":{"temp":22}},"extra":true}`),
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
	})

	t.Run("handleUpdate rejects deep nesting", func(t *testing.T) {
		h := NewHandler(WithClassicShadow(), WithLimits(Limits{
			MaxPayloadSize: defaultMaxPayloadSize,
			MaxDepth:       2,
		}))
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: []byte(`{"state":{"desired":{"a":{"b":{"c":1}}}}}`),
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
	})

	t.Run("direct Update validates state", func(t *testing.T) {
		h := NewHandler(WithLimits(Limits{MaxDepth: 1}))
		_, err := h.Update(Key{Namespace: "default", ClientID: "dev1"}, UpdateRequest{
			State: UpdateState{Desired: map[string]any{"a": map[string]any{"b": 1}}},
		})
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
	})

	t.Run("default limits allow normal payloads", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		payload, _ := json.Marshal(UpdateRequest{
			State: UpdateState{Desired: map[string]any{"temp": 22.0, "mode": "auto"}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/accepted", msg.Topic)
	})

	t.Run("handleUpdate rejects null in array", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: []byte(`{"state":{"desired":{"tags":["a",null]}}}`),
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "null values are not allowed in arrays")
	})
}

func TestDefaultLimits(t *testing.T) {
	limits := defaultLimits()
	assert.Equal(t, 128*1024, limits.MaxPayloadSize)
	assert.Equal(t, 6, limits.MaxDepth)
	assert.Equal(t, 8*1024, limits.MaxDocumentSize)
	assert.Equal(t, 64, limits.MaxClientTokenLength)
	assert.Equal(t, 25, limits.MaxNamedShadows)
	assert.Equal(t, 10, limits.MaxInflightPerClient)
	assert.Equal(t, 128, limits.MaxKeyLength)
	assert.Equal(t, 500, limits.MaxTotalKeys)
}

func TestReservedKeyPrefix(t *testing.T) {
	t.Run("dollar prefix rejected in desired", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":{"$reserved":1}}}`)

		_, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "reserved prefix")
	})

	t.Run("dollar prefix rejected in reported", func(t *testing.T) {
		payload := []byte(`{"state":{"reported":{"$meta":true}}}`)

		_, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "reserved prefix")
	})

	t.Run("dollar prefix rejected in nested map", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":{"config":{"$internal":1}}}}`)

		_, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "reserved prefix")
	})

	t.Run("normal keys allowed", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":{"temp":22,"mode":"auto"}}}`)

		req, err := validatePayload(payload, defaultLimits())
		require.NoError(t, err)
		require.NotNil(t, req)
	})

	t.Run("validateState rejects dollar prefix", func(t *testing.T) {
		err := validateState(UpdateState{
			Desired: map[string]any{"$bad": 1},
		}, defaultLimits())
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "reserved prefix")
	})
}

func TestTotalKeyCount(t *testing.T) {
	t.Run("flat keys within limit", func(t *testing.T) {
		desired := make(map[string]any)
		for i := range 10 {
			desired[fmt.Sprintf("k%d", i)] = i
		}
		payload, _ := json.Marshal(UpdateRequest{State: UpdateState{Desired: desired}})

		req, err := validatePayload(payload, defaultLimits())
		require.NoError(t, err)
		require.NotNil(t, req)
	})

	t.Run("flat keys exceed limit", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: defaultMaxDepth, MaxTotalKeys: 5}
		desired := make(map[string]any)
		for i := range 6 {
			desired[fmt.Sprintf("k%d", i)] = i
		}
		payload, _ := json.Marshal(UpdateRequest{State: UpdateState{Desired: desired}})

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "total keys exceed maximum")
	})

	t.Run("nested keys counted", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: defaultMaxDepth, MaxTotalKeys: 3}
		// 2 top-level + 2 nested = 4 total > 3
		desired := map[string]any{
			"a": map[string]any{"x": 1, "y": 2},
			"b": 3,
		}
		payload, _ := json.Marshal(UpdateRequest{State: UpdateState{Desired: desired}})

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "total keys exceed maximum")
	})

	t.Run("reported section also checked", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: defaultMaxDepth, MaxTotalKeys: 2}
		reported := map[string]any{"a": 1, "b": 2, "c": 3}
		payload, _ := json.Marshal(UpdateRequest{State: UpdateState{Reported: reported}})

		_, err := validatePayload(payload, limits)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "total keys exceed maximum")
	})

	t.Run("zero limit means unlimited", func(t *testing.T) {
		limits := Limits{MaxPayloadSize: defaultMaxPayloadSize, MaxDepth: defaultMaxDepth, MaxTotalKeys: 0}
		desired := make(map[string]any)
		for i := range 100 {
			desired[fmt.Sprintf("k%d", i)] = i
		}
		payload, _ := json.Marshal(UpdateRequest{State: UpdateState{Desired: desired}})

		req, err := validatePayload(payload, limits)
		require.NoError(t, err)
		require.NotNil(t, req)
	})
}

func TestCountKeys(t *testing.T) {
	t.Run("empty map", func(t *testing.T) {
		assert.Equal(t, 0, countKeys(map[string]any{}))
	})

	t.Run("flat map", func(t *testing.T) {
		assert.Equal(t, 3, countKeys(map[string]any{"a": 1, "b": 2, "c": 3}))
	})

	t.Run("nested map", func(t *testing.T) {
		m := map[string]any{
			"a": map[string]any{"x": 1, "y": 2},
			"b": 3,
		}
		assert.Equal(t, 4, countKeys(m))
	})

	t.Run("array with nested maps", func(t *testing.T) {
		m := map[string]any{
			"items": []any{
				map[string]any{"id": 1},
				map[string]any{"id": 2},
			},
		}
		assert.Equal(t, 3, countKeys(m))
	})

	t.Run("array with non-map elements", func(t *testing.T) {
		m := map[string]any{
			"tags": []any{"a", "b", "c"},
		}
		assert.Equal(t, 1, countKeys(m))
	})
}

func TestHandler_UTF8Integration(t *testing.T) {
	t.Run("rejects invalid UTF-8 payload", func(t *testing.T) {
		h := NewHandler(WithClassicShadow())
		client := newMockServerClient("dev1", "default")

		// Invalid UTF-8 bytes
		payload := []byte{0x7b, 0x22, 0x73, 0x74, 0x61, 0x74, 0x65, 0x22, 0x3a, 0x7b, 0x22, 0x64, 0x65, 0x73, 0x69, 0x72, 0x65, 0x64, 0x22, 0x3a, 0x7b, 0x22, 0x6b, 0x22, 0x3a, 0x22, 0xff, 0xfe, 0x22, 0x7d, 0x7d, 0x7d}
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/shadow/update",
			Payload: payload,
		})

		msg := client.lastMessage()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/shadow/update/rejected", msg.Topic)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrCodeUnsupportedEncoding, errResp.Code)
	})
}

func TestValidatePayload_InvalidStateTypes(t *testing.T) {
	t.Run("desired as array", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":[1,2,3]}}`)
		req, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)
		require.NotNil(t, req)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "invalid desired state")
	})

	t.Run("desired as string", func(t *testing.T) {
		payload := []byte(`{"state":{"desired":"notanobject"}}`)
		req, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)
		require.NotNil(t, req)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "invalid desired state")
	})

	t.Run("reported as array", func(t *testing.T) {
		payload := []byte(`{"state":{"reported":[1,2,3]}}`)
		req, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)
		require.NotNil(t, req)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "invalid reported state")
	})

	t.Run("reported as number", func(t *testing.T) {
		payload := []byte(`{"state":{"reported":42}}`)
		req, err := validatePayload(payload, defaultLimits())
		require.Error(t, err)
		require.NotNil(t, req)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrCodeBadRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "invalid reported state")
	})
}
