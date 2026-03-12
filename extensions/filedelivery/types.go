package filedelivery

import (
	"encoding/base64"
	"fmt"
)

// Validation constants for file delivery.
const (
	MinBlockSize    = 256
	MaxBlockSize    = 131072
	MaxFileID       = 255
	MaxClientToken  = 64
	MaxStreamIDLen  = 64
	MaxNumberBlocks = 131072
)

// Error code constants for file delivery operations.
const (
	ErrInvalidRequest       = "InvalidRequest"
	ErrBlockSizeOutOfBounds = "BlockSizeOutOfBounds"
	ErrOffsetOutOfBounds    = "OffsetOutOfBounds"
	ErrVersionMismatch      = "VersionMismatch"
	ErrResourceNotFound     = "ResourceNotFound"
	ErrChecksumMismatch     = "ChecksumMismatch"
	ErrUnauthorized         = "Unauthorized"
)

// StreamKey uniquely identifies a stream within a namespace.
type StreamKey struct {
	Namespace string
	ClientID  string
	StreamID  string
}

// FileInfo describes a file within a stream.
type FileInfo struct {
	FileID   int    `json:"f"`
	Size     int    `json:"z"`
	Checksum uint32 `json:"h,omitempty"`
}

// StreamDocument holds the metadata for a stream.
type StreamDocument struct {
	Description string     `json:"d,omitempty"`
	Version     int        `json:"s"`
	Files       []FileInfo `json:"r"`
}

// DescribeRequest is the payload for describe stream operations.
type DescribeRequest struct {
	ClientToken string `json:"c,omitempty"`
}

// DescribeResponse is the response to a describe stream request.
type DescribeResponse struct {
	ClientToken string     `json:"c,omitempty"`
	Version     int        `json:"s"`
	Description string     `json:"d,omitempty"`
	Files       []FileInfo `json:"r"`
}

// GetRequest is the payload for get stream operations.
type GetRequest struct {
	ClientToken    string `json:"c,omitempty"`
	Version        int    `json:"s,omitempty"`
	FileID         int    `json:"f"`
	BlockSize      int    `json:"l"`
	BlockOffset    int    `json:"o,omitempty"`
	NumberOfBlocks int    `json:"n,omitempty"`
	Bitmap         string `json:"b,omitempty"`
}

// DataBlockResponse is a single data block sent to the device.
type DataBlockResponse struct {
	ClientToken string `json:"c,omitempty"`
	FileID      int    `json:"f"`
	BlockSize   int    `json:"l"`
	BlockID     int    `json:"i"`
	Payload     string `json:"p"`
}

// Decode decodes the base64-encoded payload and returns the raw block data.
func (d *DataBlockResponse) Decode() ([]byte, error) {
	return base64.StdEncoding.DecodeString(d.Payload)
}

// CreateRequest is the payload for creating a stream (upload initiation).
type CreateRequest struct {
	ClientToken string     `json:"c,omitempty"`
	Description string     `json:"d,omitempty"`
	Files       []FileInfo `json:"r"`
}

// AcceptedResponse is the response when an upload operation succeeds.
type AcceptedResponse struct {
	ClientToken string `json:"c,omitempty"`
	FileID      int    `json:"f"`
}

// ErrorResponse is returned when a file delivery operation fails.
type ErrorResponse struct {
	Code        string `json:"o"`
	Message     string `json:"m"`
	ClientToken string `json:"c,omitempty"`
}

// Error implements the error interface.
func (e *ErrorResponse) Error() string {
	return fmt.Sprintf("filedelivery: %s %s", e.Code, e.Message)
}
