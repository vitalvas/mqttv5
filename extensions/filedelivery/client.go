package filedelivery

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"sync"

	"github.com/vitalvas/mqttv5"
)

// ErrClientClosed is returned when operations are attempted on a closed Client.
var ErrClientClosed = errors.New("filedelivery: client closed")

// MQTTClient defines the interface required for file delivery client operations.
type MQTTClient interface {
	ClientID() string
	Subscribe(filter string, qos byte, handler mqttv5.MessageHandler) error
	Unsubscribe(filters ...string) error
	Publish(msg *mqttv5.Message) error
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithClientID overrides the client ID used in topic construction.
func WithClientID(name string) ClientOption {
	return func(c *Client) {
		c.clientID = name
	}
}

// WithBlockSize sets the default block size for downloads and uploads.
// Default: 4096. Values outside MinBlockSize..MaxBlockSize are clamped.
func WithBlockSize(size int) ClientOption {
	return func(c *Client) {
		if size < MinBlockSize {
			size = MinBlockSize
		} else if size > MaxBlockSize {
			size = MaxBlockSize
		}
		c.blockSize = size
	}
}

// Client provides blocking methods for file delivery over MQTT.
//
// Usage:
//
//	c := filedelivery.NewClient(mqttClient)
//	defer c.Close()
//
//	desc, err := c.Describe(ctx, "firmware-v2")
//	data, err := c.Download(ctx, "firmware-v2", 0)
type Client struct {
	mqtt     MQTTClient
	clientID string

	blockSize int

	mu        sync.Mutex
	streams   map[string]bool // streamID -> subscribed
	subTopics []string
	pending   map[string]chan clientResponse
	downloads map[string]*responseQueue // token -> unbounded queue for downloads
	closed    bool
}

type clientResponse struct {
	descResp  *DescribeResponse
	dataBlock *DataBlockResponse
	accepted  *AcceptedResponse
	err       error
}

// responseQueue is a thread-safe response queue for downloads.
// It does not deduplicate: deduplication happens after validation in the
// Download loop. The queue is unbounded so that duplicate deliveries
// cannot evict unseen blocks; the Download loop drains entries
// concurrently, keeping memory bounded in practice.
type responseQueue struct {
	mu     sync.Mutex
	buf    []clientResponse
	notify chan struct{} // capacity 1, signals new items available
}

func newResponseQueue() *responseQueue {
	return &responseQueue{
		notify: make(chan struct{}, 1),
	}
}

func (q *responseQueue) enqueue(r clientResponse) {
	q.mu.Lock()
	q.buf = append(q.buf, r)
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *responseQueue) dequeue() (clientResponse, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.buf) == 0 {
		return clientResponse{}, false
	}

	r := q.buf[0]
	q.buf[0] = clientResponse{}
	q.buf = q.buf[1:]

	return r, true
}

// NewClient creates a new file delivery client with blocking methods.
func NewClient(client MQTTClient, opts ...ClientOption) *Client {
	c := &Client{
		mqtt:      client,
		clientID:  client.ClientID(),
		blockSize: 4096,
		streams:   make(map[string]bool),
		pending:   make(map[string]chan clientResponse),
		downloads: make(map[string]*responseQueue),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Describe returns the stream metadata (version, description, file list).
func (c *Client) Describe(ctx context.Context, streamID string) (*DescribeResponse, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClientClosed
	}

	if err := c.ensureSubscribed(streamID); err != nil {
		c.mu.Unlock()
		return nil, err
	}

	token, err := generateToken()
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	ch := make(chan clientResponse, 1)
	c.pending[token] = ch
	c.mu.Unlock()

	payload, _ := json.Marshal(DescribeRequest{ClientToken: token})
	topic := buildTopic(c.clientID, streamID, suffixDescribe)

	if err := c.mqtt.Publish(&mqttv5.Message{Topic: topic, Payload: payload}); err != nil {
		c.removePending(token)
		return nil, err
	}

	select {
	case resp := <-ch:
		c.removePending(token)
		if resp.err != nil {
			return nil, resp.err
		}
		if resp.descResp == nil {
			return nil, &ErrorResponse{Code: ErrInvalidRequest, Message: "unexpected response type"}
		}
		return resp.descResp, nil
	case <-ctx.Done():
		c.removePending(token)
		return nil, ctx.Err()
	}
}

// Download retrieves a complete file from a stream. It describes the stream,
// requests all blocks, reassembles them, and returns the file data.
// The fileID identifies which file in the stream to download (usually 0).
func (c *Client) Download(ctx context.Context, streamID string, fileID int) ([]byte, error) {
	desc, err := c.Describe(ctx, streamID)
	if err != nil {
		return nil, err
	}

	var fileInfo *FileInfo
	for _, f := range desc.Files {
		if f.FileID == fileID {
			fileInfo = &f
			break
		}
	}

	if fileInfo == nil {
		return nil, &ErrorResponse{Code: ErrResourceNotFound, Message: "file not found in stream"}
	}

	if fileInfo.Size == 0 {
		return []byte{}, nil
	}

	blockSize := c.blockSize
	totalBlocks := (fileInfo.Size + blockSize - 1) / blockSize

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClientClosed
	}

	token, err := generateToken()
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	queue := newResponseQueue()
	c.downloads[token] = queue
	c.mu.Unlock()

	payload, _ := json.Marshal(GetRequest{
		ClientToken:    token,
		Version:        desc.Version,
		FileID:         fileID,
		BlockSize:      blockSize,
		BlockOffset:    0,
		NumberOfBlocks: totalBlocks,
	})
	topic := buildTopic(c.clientID, streamID, suffixGet)

	if err := c.mqtt.Publish(&mqttv5.Message{Topic: topic, Payload: payload}); err != nil {
		c.removeDownload(token)
		return nil, err
	}

	result := make([]byte, fileInfo.Size)
	seen := make(map[int]bool, totalBlocks)

	for len(seen) < totalBlocks {
		// Drain all available items from queue
		for {
			resp, ok := queue.dequeue()
			if !ok {
				break
			}

			if resp.err != nil {
				c.removeDownload(token)
				return nil, resp.err
			}

			if resp.dataBlock == nil {
				continue
			}

			db := resp.dataBlock
			if db.FileID != fileID || db.BlockSize != blockSize || db.BlockID < 0 || db.BlockID >= totalBlocks {
				continue
			}

			if seen[db.BlockID] {
				continue
			}

			block, decErr := db.Decode()
			if decErr != nil {
				c.removeDownload(token)
				return nil, decErr
			}

			offset := db.BlockID * blockSize
			expectedLen := min(blockSize, len(result)-offset)

			if len(block) != expectedLen {
				continue
			}

			copy(result[offset:], block)
			seen[db.BlockID] = true
		}

		if len(seen) >= totalBlocks {
			break
		}

		select {
		case <-queue.notify:
		case <-ctx.Done():
			c.removeDownload(token)
			return nil, ctx.Err()
		}
	}

	c.removeDownload(token)

	if fileInfo.Checksum != 0 {
		if actual := crc32.ChecksumIEEE(result); actual != fileInfo.Checksum {
			return nil, &ErrorResponse{Code: ErrChecksumMismatch, Message: "file checksum mismatch"}
		}
	}

	return result, nil
}

// Upload sends a file to a stream. It creates the stream, splits the data
// into blocks, sends each block, and waits for acceptance of each block.
func (c *Client) Upload(ctx context.Context, streamID, description string, data []byte) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClientClosed
	}

	if err := c.ensureSubscribed(streamID); err != nil {
		c.mu.Unlock()
		return err
	}

	token, err := generateToken()
	if err != nil {
		c.mu.Unlock()
		return err
	}

	ch := make(chan clientResponse, 1)
	c.pending[token] = ch
	c.mu.Unlock()

	// Create the stream
	createPayload, _ := json.Marshal(CreateRequest{
		ClientToken: token,
		Description: description,
		Files:       []FileInfo{{FileID: 0, Size: len(data), Checksum: crc32.ChecksumIEEE(data)}},
	})
	createTopic := buildTopic(c.clientID, streamID, suffixCreate)

	if err := c.mqtt.Publish(&mqttv5.Message{Topic: createTopic, Payload: createPayload}); err != nil {
		c.removePending(token)
		return err
	}

	// Wait for create acceptance
	select {
	case resp := <-ch:
		if resp.err != nil {
			c.removePending(token)
			return resp.err
		}
		if resp.accepted == nil {
			c.removePending(token)
			return &ErrorResponse{Code: ErrInvalidRequest, Message: "unexpected response type"}
		}
	case <-ctx.Done():
		c.removePending(token)
		return ctx.Err()
	}
	c.removePending(token)

	if len(data) == 0 {
		return nil
	}

	// Send blocks
	blockSize := c.blockSize
	totalBlocks := (len(data) + blockSize - 1) / blockSize

	for i := range totalBlocks {
		offset := i * blockSize
		end := min(offset+blockSize, len(data))
		block := data[offset:end]

		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return ErrClientClosed
		}

		blockToken, err := generateToken()
		if err != nil {
			c.mu.Unlock()
			return err
		}

		blockCh := make(chan clientResponse, 1)
		c.pending[blockToken] = blockCh
		c.mu.Unlock()

		blockPayload, _ := json.Marshal(DataBlockResponse{
			ClientToken: blockToken,
			FileID:      0,
			BlockSize:   blockSize,
			BlockID:     i,
			Payload:     base64.StdEncoding.EncodeToString(block),
		})
		dataTopic := buildTopic(c.clientID, streamID, suffixData)

		if err := c.mqtt.Publish(&mqttv5.Message{Topic: dataTopic, Payload: blockPayload}); err != nil {
			c.removePending(blockToken)
			return err
		}

		select {
		case resp := <-blockCh:
			c.removePending(blockToken)
			if resp.err != nil {
				return resp.err
			}
			if resp.accepted == nil {
				return &ErrorResponse{Code: ErrInvalidRequest, Message: "unexpected response type"}
			}
		case <-ctx.Done():
			c.removePending(blockToken)
			return ctx.Err()
		}
	}

	return nil
}

// Close unsubscribes from all managed topics and cancels pending requests.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}

	c.closed = true

	pending := c.pending
	c.pending = make(map[string]chan clientResponse)

	downloads := c.downloads
	c.downloads = make(map[string]*responseQueue)

	topics := c.subTopics
	c.subTopics = nil
	c.mu.Unlock()

	closedResp := clientResponse{err: ErrClientClosed}
	for _, ch := range pending {
		select {
		case ch <- closedResp:
		default:
		}
	}
	for _, q := range downloads {
		q.enqueue(closedResp)
	}

	if len(topics) > 0 {
		return c.mqtt.Unsubscribe(topics...)
	}

	return nil
}

func (c *Client) ensureSubscribed(streamID string) error {
	if c.streams[streamID] {
		return nil
	}

	descTopic := buildTopic(c.clientID, streamID, suffixDescription)
	dataTopic := buildTopic(c.clientID, streamID, suffixData)
	rejTopic := buildTopic(c.clientID, streamID, suffixRejected)
	accTopic := buildTopic(c.clientID, streamID, suffixAccepted)

	type sub struct {
		topic   string
		handler mqttv5.MessageHandler
	}

	subs := []sub{
		{descTopic, c.makeDescriptionHandler()},
		{dataTopic, c.makeDataHandler()},
		{rejTopic, c.makeRejectedHandler()},
		{accTopic, c.makeAcceptedHandler()},
	}

	var subscribed []string
	for _, s := range subs {
		if err := c.mqtt.Subscribe(s.topic, 1, s.handler); err != nil {
			if len(subscribed) > 0 {
				_ = c.mqtt.Unsubscribe(subscribed...)
			}
			return err
		}
		subscribed = append(subscribed, s.topic)
	}

	c.subTopics = append(c.subTopics, subscribed...)
	c.streams[streamID] = true

	return nil
}

func (c *Client) makeDescriptionHandler() mqttv5.MessageHandler {
	return func(msg *mqttv5.Message) {
		var resp DescribeResponse
		if json.Unmarshal(msg.Payload, &resp) != nil || resp.ClientToken == "" {
			return
		}

		c.routeResponse(resp.ClientToken, clientResponse{descResp: &resp})
	}
}

func (c *Client) makeDataHandler() mqttv5.MessageHandler {
	return func(msg *mqttv5.Message) {
		var resp DataBlockResponse
		if json.Unmarshal(msg.Payload, &resp) != nil || resp.ClientToken == "" {
			return
		}

		c.routeResponse(resp.ClientToken, clientResponse{dataBlock: &resp})
	}
}

func (c *Client) makeAcceptedHandler() mqttv5.MessageHandler {
	return func(msg *mqttv5.Message) {
		var resp AcceptedResponse
		if json.Unmarshal(msg.Payload, &resp) != nil || resp.ClientToken == "" {
			return
		}

		c.routeResponse(resp.ClientToken, clientResponse{accepted: &resp})
	}
}

func (c *Client) makeRejectedHandler() mqttv5.MessageHandler {
	return func(msg *mqttv5.Message) {
		var resp ErrorResponse
		if json.Unmarshal(msg.Payload, &resp) != nil || resp.ClientToken == "" {
			return
		}

		c.routeResponse(resp.ClientToken, clientResponse{err: &resp})
	}
}

func (c *Client) routeResponse(token string, resp clientResponse) {
	c.mu.Lock()
	if q, ok := c.downloads[token]; ok {
		c.mu.Unlock()
		q.enqueue(resp)
		return
	}
	ch, ok := c.pending[token]
	c.mu.Unlock()

	if ok {
		select {
		case ch <- resp:
		default:
		}
	}
}

func (c *Client) removePending(token string) {
	c.mu.Lock()
	delete(c.pending, token)
	c.mu.Unlock()
}

func (c *Client) removeDownload(token string) {
	c.mu.Lock()
	delete(c.downloads, token)
	c.mu.Unlock()
}

func generateToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
