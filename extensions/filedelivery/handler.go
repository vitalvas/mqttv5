package filedelivery

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/vitalvas/mqttv5"
)

// ServerClient defines the interface for server-side client interactions.
type ServerClient interface {
	ClientID() string
	Namespace() string
	Send(msg *mqttv5.Message) error
}

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// WithStore sets the file delivery store for downloads.
func WithStore(store Store) HandlerOption {
	return func(h *Handler) {
		h.store = store
	}
}

// WithWriteStore sets the write store for uploads.
// When nil (default), upload operations are rejected.
func WithWriteStore(store WriteStore) HandlerOption {
	return func(h *Handler) {
		h.writeStore = store
	}
}

// Handler processes file delivery operations from MQTT messages.
type Handler struct {
	store      Store
	writeStore WriteStore
}

// NewHandler creates a new file delivery handler with the given options.
// Default store is MemoryStore.
func NewHandler(opts ...HandlerOption) *Handler {
	h := &Handler{}

	for _, opt := range opts {
		opt(h)
	}

	if h.store == nil {
		h.store = NewMemoryStore()
	}

	return h
}

// HandleMessage processes stream topics from MQTT clients.
// Returns true if the topic was a stream topic (handled), false otherwise.
func (h *Handler) HandleMessage(client ServerClient, msg *mqttv5.Message) bool {
	parsed := parseStreamTopic(msg.Topic)
	if parsed == nil {
		return false
	}

	key := StreamKey{
		Namespace: client.Namespace(),
		ClientID:  parsed.ClientID,
		StreamID:  parsed.StreamID,
	}

	switch parsed.Suffix {
	case suffixDescribe:
		h.handleDescribe(client, key, parsed, msg.Payload)
	case suffixGet:
		h.handleGetStream(client, key, parsed, msg.Payload)
	case suffixCreate:
		h.handleCreate(client, key, parsed, msg.Payload)
	case suffixData:
		if h.writeStore != nil {
			h.handleUploadData(client, key, parsed, msg.Payload)
		}
		return true
	case suffixDescription, suffixAccepted, suffixRejected:
		// Response topics are server->client only. Ignore if client publishes.
		return true
	default:
		return true
	}

	return true
}

func (h *Handler) handleDescribe(client ServerClient, key StreamKey, parsed *parsedTopic, payload []byte) {
	var req DescribeRequest

	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &req); err != nil {
			h.sendError(client, parsed, ErrInvalidRequest, "invalid payload", "")
			return
		}
	}

	if len(req.ClientToken) > MaxClientToken {
		h.sendError(client, parsed, ErrInvalidRequest, "client token too long", "")
		return
	}

	doc, err := h.store.GetStream(key)
	if err != nil {
		h.sendError(client, parsed, ErrResourceNotFound, "stream not found", req.ClientToken)
		return
	}

	if doc == nil {
		h.sendError(client, parsed, ErrResourceNotFound, "stream not found", req.ClientToken)
		return
	}

	resp := &DescribeResponse{
		ClientToken: req.ClientToken,
		Version:     doc.Version,
		Description: doc.Description,
		Files:       doc.Files,
	}

	h.sendJSON(client, parsed, suffixDescription, resp)
}

func (h *Handler) handleCreate(client ServerClient, key StreamKey, parsed *parsedTopic, payload []byte) {
	if h.writeStore == nil {
		h.sendError(client, parsed, ErrInvalidRequest, "uploads not enabled", "")
		return
	}

	if len(payload) == 0 {
		h.sendError(client, parsed, ErrInvalidRequest, "payload required", "")
		return
	}

	var req CreateRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		h.sendError(client, parsed, ErrInvalidRequest, "invalid payload", "")
		return
	}

	if len(req.ClientToken) > MaxClientToken {
		h.sendError(client, parsed, ErrInvalidRequest, "client token too long", "")
		return
	}

	if len(req.Files) == 0 {
		h.sendError(client, parsed, ErrInvalidRequest, "at least one file required", req.ClientToken)
		return
	}

	seenFileIDs := make(map[int]bool, len(req.Files))
	for _, f := range req.Files {
		if f.FileID < 0 || f.FileID > MaxFileID {
			h.sendError(client, parsed, ErrInvalidRequest,
				fmt.Sprintf("file ID must be between 0 and %d", MaxFileID),
				req.ClientToken)
			return
		}

		if f.Size < 0 {
			h.sendError(client, parsed, ErrInvalidRequest, "file size must not be negative", req.ClientToken)
			return
		}

		if seenFileIDs[f.FileID] {
			h.sendError(client, parsed, ErrInvalidRequest, "duplicate file ID", req.ClientToken)
			return
		}
		seenFileIDs[f.FileID] = true
	}

	doc := &StreamDocument{
		Description: req.Description,
		Version:     1,
		Files:       req.Files,
	}

	if err := h.writeStore.CreateStream(key, doc); err != nil {
		var errResp *ErrorResponse
		if isErrorResponse(err, &errResp) {
			h.sendError(client, parsed, errResp.Code, errResp.Message, req.ClientToken)
		} else {
			h.sendError(client, parsed, ErrInvalidRequest, err.Error(), req.ClientToken)
		}
		return
	}

	h.sendJSON(client, parsed, suffixAccepted, &AcceptedResponse{
		ClientToken: req.ClientToken,
	})
}

func (h *Handler) handleUploadData(client ServerClient, key StreamKey, parsed *parsedTopic, payload []byte) {
	if len(payload) == 0 {
		h.sendError(client, parsed, ErrInvalidRequest, "payload required", "")
		return
	}

	var block DataBlockResponse
	if json.Unmarshal(payload, &block) != nil {
		h.sendError(client, parsed, ErrInvalidRequest, "invalid payload", "")
		return
	}

	if block.BlockID < 0 || block.BlockSize < MinBlockSize || block.BlockSize > MaxBlockSize {
		h.sendError(client, parsed, ErrInvalidRequest, "invalid block metadata", block.ClientToken)
		return
	}

	decoded, err := block.Decode()
	if err != nil {
		h.sendError(client, parsed, ErrInvalidRequest, "invalid base64 payload", block.ClientToken)
		return
	}

	if len(decoded) > block.BlockSize {
		h.sendError(client, parsed, ErrInvalidRequest, "payload exceeds block size", block.ClientToken)
		return
	}

	// Validate undersized blocks: only the final block may be shorter than BlockSize.
	if len(decoded) < block.BlockSize {
		doc, docErr := h.writeStore.GetStream(key)
		if docErr != nil || doc == nil {
			h.sendError(client, parsed, ErrResourceNotFound, "stream not found", block.ClientToken)
			return
		}

		var fileSize int
		var fileFound bool
		for _, f := range doc.Files {
			if f.FileID == block.FileID {
				fileSize = f.Size
				fileFound = true
				break
			}
		}

		if !fileFound {
			h.sendError(client, parsed, ErrResourceNotFound, "file not found in stream", block.ClientToken)
			return
		}

		totalBlocks := (fileSize + block.BlockSize - 1) / block.BlockSize
		if block.BlockID != totalBlocks-1 {
			h.sendError(client, parsed, ErrInvalidRequest, "undersized non-final block", block.ClientToken)
			return
		}

		expectedLen := fileSize - block.BlockID*block.BlockSize
		if len(decoded) != expectedLen {
			h.sendError(client, parsed, ErrInvalidRequest, "invalid final block size", block.ClientToken)
			return
		}
	}

	offset := int64(block.BlockID) * int64(block.BlockSize)

	if writeErr := h.writeStore.WriteBlock(key, block.FileID, offset, decoded); writeErr != nil {
		var errResp *ErrorResponse
		if isErrorResponse(writeErr, &errResp) {
			h.sendError(client, parsed, errResp.Code, errResp.Message, block.ClientToken)
		} else {
			h.sendError(client, parsed, ErrInvalidRequest, writeErr.Error(), block.ClientToken)
		}
		return
	}

	h.sendJSON(client, parsed, suffixAccepted, &AcceptedResponse{
		ClientToken: block.ClientToken,
		FileID:      block.FileID,
	})
}

func (h *Handler) handleGetStream(client ServerClient, key StreamKey, parsed *parsedTopic, payload []byte) {
	var req GetRequest

	if len(payload) == 0 {
		h.sendError(client, parsed, ErrInvalidRequest, "payload required", "")
		return
	}

	if err := json.Unmarshal(payload, &req); err != nil {
		h.sendError(client, parsed, ErrInvalidRequest, "invalid payload", "")
		return
	}

	if len(req.ClientToken) > MaxClientToken {
		h.sendError(client, parsed, ErrInvalidRequest, "client token too long", "")
		return
	}

	if req.BlockSize < MinBlockSize || req.BlockSize > MaxBlockSize {
		h.sendError(client, parsed, ErrBlockSizeOutOfBounds,
			fmt.Sprintf("block size must be between %d and %d", MinBlockSize, MaxBlockSize),
			req.ClientToken)
		return
	}

	if req.FileID < 0 || req.FileID > MaxFileID {
		h.sendError(client, parsed, ErrInvalidRequest,
			fmt.Sprintf("file ID must be between 0 and %d", MaxFileID),
			req.ClientToken)
		return
	}

	if req.BlockOffset < 0 {
		h.sendError(client, parsed, ErrOffsetOutOfBounds, "block offset must not be negative", req.ClientToken)
		return
	}

	doc, err := h.store.GetStream(key)
	if err != nil {
		h.sendError(client, parsed, ErrResourceNotFound, "stream not found", req.ClientToken)
		return
	}

	if doc == nil {
		h.sendError(client, parsed, ErrResourceNotFound, "stream not found", req.ClientToken)
		return
	}

	if req.Version != 0 && req.Version != doc.Version {
		h.sendError(client, parsed, ErrVersionMismatch,
			fmt.Sprintf("expected version %d, got %d", doc.Version, req.Version),
			req.ClientToken)
		return
	}

	// Determine which file to read
	var fileFound bool
	for _, f := range doc.Files {
		if f.FileID == req.FileID {
			fileFound = true
			break
		}
	}

	if !fileFound {
		h.sendError(client, parsed, ErrResourceNotFound, "file not found in stream", req.ClientToken)
		return
	}

	// Determine which blocks to send
	blockIDs, err := h.computeBlockIDs(req)
	if err != nil {
		var errResp *ErrorResponse
		if ok := isErrorResponse(err, &errResp); ok {
			h.sendError(client, parsed, errResp.Code, errResp.Message, req.ClientToken)
		} else {
			h.sendError(client, parsed, ErrInvalidRequest, err.Error(), req.ClientToken)
		}
		return
	}

	// Send each block
	var blocksSent int
	for _, blockID := range blockIDs {
		offset := int64(blockID) * int64(req.BlockSize)

		block, readErr := h.store.ReadBlock(key, req.FileID, offset, req.BlockSize)
		if readErr != nil {
			var errResp *ErrorResponse
			if isErrorResponse(readErr, &errResp) && errResp.Code == ErrOffsetOutOfBounds {
				if blocksSent == 0 {
					h.sendError(client, parsed, ErrOffsetOutOfBounds, errResp.Message, req.ClientToken)
				}
				break
			}

			h.sendError(client, parsed, ErrResourceNotFound, readErr.Error(), req.ClientToken)
			return
		}

		resp := &DataBlockResponse{
			ClientToken: req.ClientToken,
			FileID:      req.FileID,
			BlockSize:   req.BlockSize,
			BlockID:     blockID,
			Payload:     base64.StdEncoding.EncodeToString(block),
		}

		h.sendJSON(client, parsed, suffixData, resp)
		blocksSent++
	}
}

func (h *Handler) computeBlockIDs(req GetRequest) ([]int, error) {
	if req.Bitmap != "" {
		ids, err := decodeBitmap(req.Bitmap, req.BlockOffset)
		if err != nil {
			return nil, err
		}
		if len(ids) > MaxNumberBlocks {
			return nil, &ErrorResponse{
				Code:    ErrInvalidRequest,
				Message: fmt.Sprintf("number of blocks must not exceed %d", MaxNumberBlocks),
			}
		}
		return ids, nil
	}

	numBlocks := req.NumberOfBlocks
	if numBlocks <= 0 {
		numBlocks = 1
	}

	if numBlocks > MaxNumberBlocks {
		return nil, &ErrorResponse{
			Code:    ErrInvalidRequest,
			Message: fmt.Sprintf("number of blocks must not exceed %d", MaxNumberBlocks),
		}
	}

	ids := make([]int, numBlocks)
	for i := range numBlocks {
		ids[i] = req.BlockOffset + i
	}

	return ids, nil
}

// decodeBitmap decodes a hex-encoded bitmap into block IDs relative to the given offset.
// Each bit in the bitmap (MSB first) represents a block: 1 means request, 0 means skip.
func decodeBitmap(hexStr string, offset int) ([]int, error) {
	maxBitmapBytes := MaxNumberBlocks / 8
	if len(hexStr) > maxBitmapBytes*2 {
		return nil, &ErrorResponse{Code: ErrInvalidRequest,
			Message: fmt.Sprintf("bitmap too large, max %d bytes", maxBitmapBytes)}
	}

	data, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, &ErrorResponse{Code: ErrInvalidRequest, Message: "invalid bitmap hex encoding"}
	}

	if len(data) == 0 {
		return nil, &ErrorResponse{Code: ErrInvalidRequest, Message: "bitmap must not be empty"}
	}

	var ids []int
	for byteIdx, b := range data {
		for bitIdx := 7; bitIdx >= 0; bitIdx-- {
			if b&(1<<uint(bitIdx)) != 0 {
				blockID := offset + byteIdx*8 + (7 - bitIdx)
				ids = append(ids, blockID)
			}
		}
	}

	return ids, nil
}

func isErrorResponse(err error, target **ErrorResponse) bool {
	e, ok := err.(*ErrorResponse)
	if ok {
		*target = e
	}
	return ok
}

func (h *Handler) sendJSON(client ServerClient, parsed *parsedTopic, suffix string, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}

	topic := buildTopic(parsed.ClientID, parsed.StreamID, suffix)
	_ = client.Send(&mqttv5.Message{
		Topic:       topic,
		Payload:     data,
		ContentType: "application/json",
	})
}

func (h *Handler) sendError(client ServerClient, parsed *parsedTopic, code, message, clientToken string) {
	h.sendJSON(client, parsed, suffixRejected, &ErrorResponse{
		Code:        code,
		Message:     message,
		ClientToken: clientToken,
	})
}
