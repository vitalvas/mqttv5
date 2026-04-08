package shadow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	defaultMaxPayloadSize       = 128 * 1024 // 128 KB
	defaultMaxDepth             = 6
	defaultMaxDocumentSize      = 8 * 1024 // 8 KB
	defaultMaxClientTokenLength = 64
	defaultMaxNamedShadows      = 25
	defaultMaxInflightPerClient = 10
	defaultMaxKeyLength         = 128
	defaultMaxTotalKeys         = 500
)

// Limits configures validation constraints for shadow payloads.
// Zero values mean no limit (except fields with documented defaults).
type Limits struct {
	// MaxPayloadSize is the maximum allowed payload size in bytes.
	// Default: 131072 (128 KB).
	MaxPayloadSize int

	// MaxDepth is the maximum nesting depth for state objects.
	// Default: 6.
	MaxDepth int

	// MaxDocumentSize is the maximum allowed document size in bytes after update.
	// Default: 8192 (8 KB).
	MaxDocumentSize int

	// MaxClientTokenLength is the maximum length of a clientToken string.
	// Default: 64.
	MaxClientTokenLength int

	// MaxNamedShadows is the maximum number of named shadows per thing.
	// Default: 25. Zero means unlimited.
	MaxNamedShadows int

	// MaxInflightPerClient is the maximum concurrent requests per client.
	// Default: 10. Zero means unlimited.
	MaxInflightPerClient int

	// MaxTotalKeys is the maximum total number of key-value pairs across all
	// nesting levels in a single state section (desired or reported).
	// Default: 500. Zero means unlimited.
	MaxTotalKeys int

	// MaxKeysPerObject is the maximum number of keys per JSON object in state.
	// Default: 0 (unlimited).
	MaxKeysPerObject int

	// MaxKeyLength is the maximum length of a key name in state.
	// Default: 128.
	MaxKeyLength int

	// MaxValueLength is the maximum length of a string value in state.
	// Default: 0 (unlimited).
	MaxValueLength int
}

func defaultLimits() Limits {
	return Limits{
		MaxPayloadSize:       defaultMaxPayloadSize,
		MaxDepth:             defaultMaxDepth,
		MaxDocumentSize:      defaultMaxDocumentSize,
		MaxClientTokenLength: defaultMaxClientTokenLength,
		MaxNamedShadows:      defaultMaxNamedShadows,
		MaxInflightPerClient: defaultMaxInflightPerClient,
		MaxKeyLength:         defaultMaxKeyLength,
		MaxTotalKeys:         defaultMaxTotalKeys,
	}
}

// rawUpdateRequest is used for initial JSON parsing to distinguish
// between absent fields and explicit null values (e.g., "desired": null).
type rawUpdateRequest struct {
	State       rawState `json:"state"`
	Version     *int64   `json:"version,omitempty"`
	ClientToken string   `json:"clientToken,omitempty"`
}

type rawState struct {
	Desired  json.RawMessage `json:"desired,omitempty"`
	Reported json.RawMessage `json:"reported,omitempty"`
}

// validatePayload checks the raw payload size and decodes the update request
// with strict unknown-field rejection. Distinguishes between absent and
// explicit null for desired/reported sections.
func validatePayload(payload []byte, limits Limits) (*UpdateRequest, error) {
	if !utf8.Valid(payload) {
		return nil, &ErrorResponse{
			Code:    ErrCodeUnsupportedEncoding,
			Message: "payload is not valid UTF-8",
		}
	}

	if limits.MaxPayloadSize > 0 && len(payload) > limits.MaxPayloadSize {
		return nil, &ErrorResponse{
			Code:    ErrCodePayloadTooLarge,
			Message: fmt.Sprintf("payload exceeds maximum size of %d bytes", limits.MaxPayloadSize),
		}
	}

	var raw rawUpdateRequest

	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&raw); err != nil {
		return nil, &ErrorResponse{Code: ErrCodeBadRequest, Message: fmt.Sprintf("invalid JSON: %s", err.Error())}
	}

	if limits.MaxClientTokenLength > 0 && len(raw.ClientToken) > limits.MaxClientTokenLength {
		return &UpdateRequest{ClientToken: raw.ClientToken}, &ErrorResponse{
			Code:    ErrCodeBadRequest,
			Message: fmt.Sprintf("clientToken exceeds maximum length of %d", limits.MaxClientTokenLength),
		}
	}

	req := &UpdateRequest{
		Version:     raw.Version,
		ClientToken: raw.ClientToken,
	}

	// Parse desired: distinguish absent vs null vs value
	if raw.State.Desired != nil {
		if string(raw.State.Desired) == "null" {
			req.ClearDesired = true
		} else {
			var desired map[string]any
			if err := json.Unmarshal(raw.State.Desired, &desired); err != nil {
				return req, &ErrorResponse{Code: ErrCodeBadRequest, Message: fmt.Sprintf("invalid desired state: %s", err.Error())}
			}

			req.State.Desired = desired
		}
	}

	// Parse reported: distinguish absent vs null vs value
	if raw.State.Reported != nil {
		if string(raw.State.Reported) == "null" {
			req.ClearReported = true
		} else {
			var reported map[string]any
			if err := json.Unmarshal(raw.State.Reported, &reported); err != nil {
				return req, &ErrorResponse{Code: ErrCodeBadRequest, Message: fmt.Sprintf("invalid reported state: %s", err.Error())}
			}

			req.State.Reported = reported
		}
	}

	// Must have at least one action
	if req.State.Desired == nil && req.State.Reported == nil && !req.ClearDesired && !req.ClearReported {
		return req, &ErrorResponse{Code: ErrCodeBadRequest, Message: "state must contain desired or reported"}
	}

	if req.State.Desired != nil {
		if err := validateMap(req.State.Desired, limits, 1); err != nil {
			return req, err
		}

		if limits.MaxTotalKeys > 0 && countKeys(req.State.Desired) > limits.MaxTotalKeys {
			return req, &ErrorResponse{
				Code:    ErrCodeBadRequest,
				Message: fmt.Sprintf("total keys exceed maximum of %d", limits.MaxTotalKeys),
			}
		}
	}

	if req.State.Reported != nil {
		if err := validateMap(req.State.Reported, limits, 1); err != nil {
			return req, err
		}

		if limits.MaxTotalKeys > 0 && countKeys(req.State.Reported) > limits.MaxTotalKeys {
			return req, &ErrorResponse{
				Code:    ErrCodeBadRequest,
				Message: fmt.Sprintf("total keys exceed maximum of %d", limits.MaxTotalKeys),
			}
		}
	}

	return req, nil
}

// validateState validates the state maps against the configured limits.
// Used by the direct Update method where raw payload size is not applicable.
func validateState(state UpdateState, limits Limits) error {
	if state.Desired != nil {
		if err := validateMap(state.Desired, limits, 1); err != nil {
			return err
		}

		if limits.MaxTotalKeys > 0 && countKeys(state.Desired) > limits.MaxTotalKeys {
			return &ErrorResponse{
				Code:    ErrCodeBadRequest,
				Message: fmt.Sprintf("total keys exceed maximum of %d", limits.MaxTotalKeys),
			}
		}
	}

	if state.Reported != nil {
		if err := validateMap(state.Reported, limits, 1); err != nil {
			return err
		}

		if limits.MaxTotalKeys > 0 && countKeys(state.Reported) > limits.MaxTotalKeys {
			return &ErrorResponse{
				Code:    ErrCodeBadRequest,
				Message: fmt.Sprintf("total keys exceed maximum of %d", limits.MaxTotalKeys),
			}
		}
	}

	return nil
}

// validateMap recursively validates a state map against the configured limits.
func validateMap(m map[string]any, limits Limits, depth int) error {
	if limits.MaxDepth > 0 && depth > limits.MaxDepth {
		return &ErrorResponse{
			Code:    ErrCodeBadRequest,
			Message: fmt.Sprintf("nesting depth exceeds maximum of %d", limits.MaxDepth),
		}
	}

	if limits.MaxKeysPerObject > 0 && len(m) > limits.MaxKeysPerObject {
		return &ErrorResponse{
			Code:    ErrCodeBadRequest,
			Message: fmt.Sprintf("object has %d keys, maximum is %d", len(m), limits.MaxKeysPerObject),
		}
	}

	for k, v := range m {
		if strings.HasPrefix(k, "$") {
			return &ErrorResponse{
				Code:    ErrCodeBadRequest,
				Message: fmt.Sprintf("key %q starts with reserved prefix \"$\"", k),
			}
		}

		if limits.MaxKeyLength > 0 && len(k) > limits.MaxKeyLength {
			return &ErrorResponse{
				Code:    ErrCodeBadRequest,
				Message: fmt.Sprintf("key %q exceeds maximum length of %d", k, limits.MaxKeyLength),
			}
		}

		if err := validateValue(v, limits, depth); err != nil {
			return err
		}
	}

	return nil
}

// validateValue validates a single value within the state map.
func validateValue(v any, limits Limits, depth int) error {
	switch val := v.(type) {
	case string:
		if limits.MaxValueLength > 0 && len(val) > limits.MaxValueLength {
			return &ErrorResponse{
				Code:    ErrCodeBadRequest,
				Message: fmt.Sprintf("string value exceeds maximum length of %d", limits.MaxValueLength),
			}
		}

	case map[string]any:
		if err := validateMap(val, limits, depth+1); err != nil {
			return err
		}

	case []any:
		for _, elem := range val {
			if elem == nil {
				return &ErrorResponse{
					Code:    ErrCodeBadRequest,
					Message: "null values are not allowed in arrays",
				}
			}

			if err := validateValue(elem, limits, depth+1); err != nil {
				return err
			}
		}
	}

	return nil
}

// countKeys recursively counts all key-value pairs across nested levels.
func countKeys(m map[string]any) int {
	count := len(m)

	for _, v := range m {
		switch val := v.(type) {
		case map[string]any:
			count += countKeys(val)
		case []any:
			for _, elem := range val {
				if sub, ok := elem.(map[string]any); ok {
					count += countKeys(sub)
				}
			}
		}
	}

	return count
}
