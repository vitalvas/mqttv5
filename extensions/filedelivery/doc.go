// Package filedelivery provides bidirectional MQTT-based file delivery
// between devices and the server.
//
// Devices can download files (describe streams, request blocks) and
// upload files (create streams, send blocks). All payloads use JSON
// with short field names for compact wire format.
//
// The package provides both server-side (Handler) and client-side (Client)
// components:
//
//   - Handler processes stream operations (describe/get/create/upload)
//     from MQTT messages.
//   - Client provides blocking methods: Describe, Download, and Upload.
//
// # Download topics
//
//	$things/{clientID}/streams/{streamID}/describe/json     - request stream metadata
//	$things/{clientID}/streams/{streamID}/description/json  - stream metadata response
//	$things/{clientID}/streams/{streamID}/get/json          - request file blocks
//	$things/{clientID}/streams/{streamID}/data/json         - file block data (both directions)
//	$things/{clientID}/streams/{streamID}/rejected/json     - error response
//
// # Upload topics
//
//	$things/{clientID}/streams/{streamID}/create/json       - create stream (upload initiation)
//	$things/{clientID}/streams/{streamID}/data/json         - upload file blocks
//	$things/{clientID}/streams/{streamID}/accepted/json     - upload acceptance
//	$things/{clientID}/streams/{streamID}/rejected/json     - error response
//
// # DescribeStream
//
// A device publishes to describe/json with an optional client token.
// The server responds on description/json with the stream version,
// description, and file list (file IDs and sizes).
//
// # GetStream
//
// A device publishes to get/json with the file ID, block size, block
// offset, and optionally a number of blocks or hex-encoded bitmap.
// The server responds with one data/json message per requested block,
// each containing the base64-encoded block payload.
//
// # CreateStream
//
// A device publishes to create/json with a description and file list.
// The server validates and responds with accepted/json or rejected/json.
// After acceptance, the device sends file blocks on data/json.
//
// # Bitmap support
//
// The GetStream request supports a hex-encoded bitmap field for
// selective block retrieval. Each bit (MSB first) indicates whether
// to send the corresponding block (1) or skip it (0).
//
// # Block size constraints
//
// Block size must be between 256 and 131072 bytes (inclusive).
// File IDs range from 0 to 255.
//
// # Error handling
//
// Errors are published to rejected/json with a code and message.
// Error codes: InvalidRequest, BlockSizeOutOfBounds, OffsetOutOfBounds,
// VersionMismatch, ResourceNotFound, Unauthorized.
package filedelivery
