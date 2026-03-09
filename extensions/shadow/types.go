package shadow

import "fmt"

// Error codes for shadow operations.
const (
	ErrCodeBadRequest          = 400
	ErrCodeUnauthorized        = 401
	ErrCodeForbidden           = 403
	ErrCodeNotFound            = 404
	ErrCodeVersionConflict     = 409
	ErrCodePayloadTooLarge     = 413
	ErrCodeUnsupportedEncoding = 415
	ErrCodeTooManyRequests     = 429
	ErrCodeInternal            = 500
)

// SharedAccessResolver determines whether a client is allowed to access
// a shared shadow group. Return true to allow access.
type SharedAccessResolver func(clientID, groupName string) bool

// Key uniquely identifies a shadow within a namespace.
type Key struct {
	Namespace  string
	ClientID   string
	GroupName  string // non-empty for shared shadows
	ShadowName string // empty string means classic (unnamed) shadow
}

// Document represents the full shadow state.
type Document struct {
	State     State    `json:"state"`
	Metadata  Metadata `json:"metadata"`
	Version   int64    `json:"version"`
	Timestamp int64    `json:"timestamp"`
}

// State holds desired, reported, and computed delta states.
type State struct {
	Desired  map[string]any `json:"desired,omitempty"`
	Reported map[string]any `json:"reported,omitempty"`
	Delta    map[string]any `json:"delta,omitempty"`
}

// Metadata holds per-key timestamps for desired and reported states.
// For nested state values, metadata mirrors the nesting structure where
// each leaf is {"timestamp": int64}.
type Metadata struct {
	Desired    map[string]any      `json:"desired,omitempty"`
	Reported   map[string]any      `json:"reported,omitempty"`
	Connection *ConnectionMetadata `json:"connection,omitempty"`
}

// ConnectionMetadata tracks the connection state of the device.
type ConnectionMetadata struct {
	Connected           bool  `json:"connected"`
	ConnectionTimestamp int64 `json:"connectionTimestamp"`
}

// UpdateRequest is the payload for shadow update operations.
type UpdateRequest struct {
	State         UpdateState `json:"state"`
	Version       *int64      `json:"version,omitempty"`
	ClientToken   string      `json:"clientToken,omitempty"`
	ClearDesired  bool        `json:"-"` // set when "desired": null in JSON
	ClearReported bool        `json:"-"` // set when "reported": null in JSON
}

// UpdateState holds the desired and/or reported state to update.
type UpdateState struct {
	Desired  map[string]any `json:"desired,omitempty"`
	Reported map[string]any `json:"reported,omitempty"`
}

// ErrorResponse is returned when a shadow operation fails.
type ErrorResponse struct {
	Code        int    `json:"code"`
	Message     string `json:"message"`
	Timestamp   int64  `json:"timestamp,omitempty"`
	ClientToken string `json:"clientToken,omitempty"`
}

// Error implements the error interface.
func (e *ErrorResponse) Error() string {
	return fmt.Sprintf("shadow: %d %s", e.Code, e.Message)
}

// DocumentsMessage is published on the update/documents topic.
// It contains snapshots of the shadow before and after the update.
type DocumentsMessage struct {
	Previous    DocumentSnapshot `json:"previous"`
	Current     DocumentSnapshot `json:"current"`
	Timestamp   int64            `json:"timestamp"`
	ClientToken string           `json:"clientToken,omitempty"`
}

// DocumentSnapshot is a point-in-time snapshot of the shadow.
type DocumentSnapshot struct {
	State    State    `json:"state"`
	Metadata Metadata `json:"metadata"`
	Version  int64    `json:"version"`
}

// deltaResponse is published on the update/delta topic.
// Contains only the delta keys, their metadata, version, and timestamp.
type deltaResponse struct {
	State       map[string]any `json:"state"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Version     int64          `json:"version"`
	Timestamp   int64          `json:"timestamp"`
	ClientToken string         `json:"clientToken,omitempty"`
}

// deleteRequest is an optional payload for the delete operation.
type deleteRequest struct {
	ClientToken string `json:"clientToken,omitempty"`
}

// deleteAcceptedResponse is sent on the delete/accepted topic.
type deleteAcceptedResponse struct {
	Version     int64  `json:"version"`
	Timestamp   int64  `json:"timestamp"`
	ClientToken string `json:"clientToken,omitempty"`
}

// getRequest is an optional payload for the get operation.
type getRequest struct {
	ClientToken string `json:"clientToken,omitempty"`
}

// getAcceptedResponse wraps a Document with an optional clientToken.
type getAcceptedResponse struct {
	State       State    `json:"state"`
	Metadata    Metadata `json:"metadata"`
	Version     int64    `json:"version"`
	Timestamp   int64    `json:"timestamp"`
	ClientToken string   `json:"clientToken,omitempty"`
}

// listRequest is the optional payload for the list operation.
type listRequest struct {
	PageSize    int    `json:"pageSize,omitempty"`
	NextToken   string `json:"nextToken,omitempty"`
	ClientToken string `json:"clientToken,omitempty"`
}

// ListResult holds the result of a named shadow listing.
type ListResult struct {
	Results   []string `json:"results"`
	NextToken string   `json:"nextToken,omitempty"`
}

// listAcceptedResponse is sent on the list/accepted topic.
type listAcceptedResponse struct {
	Results     []string `json:"results"`
	Timestamp   int64    `json:"timestamp"`
	NextToken   string   `json:"nextToken,omitempty"`
	ClientToken string   `json:"clientToken,omitempty"`
}
