package filedelivery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

var errSubscribeFailed = errors.New("subscribe failed")

// simulatedClient implements MQTTClient and simulates server responses.
// On Publish, it intercepts describe/get requests and responds via the
// registered subscription handlers.
type simulatedClient struct {
	clientID string
	mu       sync.Mutex
	handlers map[string]mqttv5.MessageHandler

	// streams is the simulated server-side data
	streams map[string]*StreamDocument
	files   map[string][]byte // "streamID/fileID" -> data

	// uploaded tracks data written by upload simulation
	uploaded map[string][]byte // "streamID/fileID" -> data

	publishErr   error
	subscribeErr string // fail subscribe on topics containing this
	unsubscribed []string
	rejectCreate bool // if true, reject create requests
}

func newSimulatedClient(clientID string) *simulatedClient {
	return &simulatedClient{
		clientID: clientID,
		handlers: make(map[string]mqttv5.MessageHandler),
		streams:  make(map[string]*StreamDocument),
		files:    make(map[string][]byte),
		uploaded: make(map[string][]byte),
	}
}

func (s *simulatedClient) ClientID() string { return s.clientID }

func (s *simulatedClient) Subscribe(filter string, _ byte, handler mqttv5.MessageHandler) error {
	if s.subscribeErr != "" && strings.Contains(filter, s.subscribeErr) {
		return errSubscribeFailed
	}

	s.mu.Lock()
	s.handlers[filter] = handler
	s.mu.Unlock()
	return nil
}

func (s *simulatedClient) Unsubscribe(filters ...string) error {
	s.mu.Lock()
	s.unsubscribed = append(s.unsubscribed, filters...)
	s.mu.Unlock()
	return nil
}

func (s *simulatedClient) Publish(msg *mqttv5.Message) error {
	if s.publishErr != nil {
		return s.publishErr
	}

	parsed := parseStreamTopic(msg.Topic)
	if parsed == nil {
		return nil
	}

	go s.simulateResponse(parsed, msg.Payload)
	return nil
}

func (s *simulatedClient) simulateResponse(parsed *parsedTopic, payload []byte) {
	switch parsed.Suffix {
	case suffixDescribe:
		s.simulateDescribe(parsed, payload)
	case suffixGet:
		s.simulateGet(parsed, payload)
	case suffixCreate:
		s.simulateCreate(parsed, payload)
	case suffixData:
		s.simulateUploadData(parsed, payload)
	}
}

func (s *simulatedClient) simulateDescribe(parsed *parsedTopic, payload []byte) {
	var req DescribeRequest
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &req)
	}

	doc, ok := s.streams[parsed.StreamID]
	if !ok {
		s.sendToHandler(parsed, suffixRejected, &ErrorResponse{
			Code:        ErrResourceNotFound,
			Message:     "stream not found",
			ClientToken: req.ClientToken,
		})
		return
	}

	s.sendToHandler(parsed, suffixDescription, &DescribeResponse{
		ClientToken: req.ClientToken,
		Version:     doc.Version,
		Description: doc.Description,
		Files:       doc.Files,
	})
}

func (s *simulatedClient) simulateGet(parsed *parsedTopic, payload []byte) {
	var req GetRequest
	if json.Unmarshal(payload, &req) != nil {
		return
	}

	fileData, ok := s.files[parsed.StreamID+"/"+itoa(req.FileID)]
	if !ok {
		return // silently ignore, simulating no response from server
	}

	for i := range req.NumberOfBlocks {
		offset := (req.BlockOffset + i) * req.BlockSize
		if offset >= len(fileData) {
			break
		}

		end := min(offset+req.BlockSize, len(fileData))
		block := fileData[offset:end]

		s.sendToHandler(parsed, suffixData, &DataBlockResponse{
			ClientToken: req.ClientToken,
			FileID:      req.FileID,
			BlockSize:   req.BlockSize,
			BlockID:     req.BlockOffset + i,
			Payload:     base64.StdEncoding.EncodeToString(block),
		})
	}
}

func (s *simulatedClient) simulateCreate(parsed *parsedTopic, payload []byte) {
	var req CreateRequest
	if json.Unmarshal(payload, &req) != nil {
		return
	}

	if s.rejectCreate {
		s.sendToHandler(parsed, suffixRejected, &ErrorResponse{
			Code:        ErrInvalidRequest,
			Message:     "create rejected",
			ClientToken: req.ClientToken,
		})
		return
	}

	// Allocate uploaded file storage
	for _, f := range req.Files {
		key := parsed.StreamID + "/" + itoa(f.FileID)
		s.mu.Lock()
		s.uploaded[key] = make([]byte, f.Size)
		s.mu.Unlock()
	}

	s.sendToHandler(parsed, suffixAccepted, &AcceptedResponse{
		ClientToken: req.ClientToken,
	})
}

func (s *simulatedClient) simulateUploadData(parsed *parsedTopic, payload []byte) {
	var block DataBlockResponse
	if json.Unmarshal(payload, &block) != nil {
		return
	}

	decoded, err := block.Decode()
	if err != nil {
		return
	}

	key := parsed.StreamID + "/" + itoa(block.FileID)
	s.mu.Lock()
	fileData, ok := s.uploaded[key]
	if ok {
		offset := block.BlockID * block.BlockSize
		copy(fileData[offset:], decoded)
	}
	s.mu.Unlock()

	s.sendToHandler(parsed, suffixAccepted, &AcceptedResponse{
		ClientToken: block.ClientToken,
		FileID:      block.FileID,
	})
}

func (s *simulatedClient) sendToHandler(parsed *parsedTopic, suffix string, v any) {
	topic := buildTopic(parsed.ClientID, parsed.StreamID, suffix)

	s.mu.Lock()
	handler, ok := s.handlers[topic]
	s.mu.Unlock()

	if !ok {
		return
	}

	data, _ := json.Marshal(v)
	handler(&mqttv5.Message{Topic: topic, Payload: data})
}

// checksumCaptureSim wraps simulatedClient and captures create payloads.
type checksumCaptureSim struct {
	*simulatedClient
	createPayload []byte
}

func (c *checksumCaptureSim) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixCreate {
		c.createPayload = make([]byte, len(msg.Payload))
		copy(c.createPayload, msg.Payload)
	}
	return c.simulatedClient.Publish(msg)
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

func TestDescribe(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["fw-update"] = &StreamDocument{
			Description: "firmware v2",
			Version:     3,
			Files:       []FileInfo{{FileID: 0, Size: 1024}},
		}

		c := NewClient(sim)
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		desc, err := c.Describe(ctx, "fw-update")
		require.NoError(t, err)
		assert.Equal(t, 3, desc.Version)
		assert.Equal(t, "firmware v2", desc.Description)
		require.Len(t, desc.Files, 1)
		assert.Equal(t, 0, desc.Files[0].FileID)
		assert.Equal(t, 1024, desc.Files[0].Size)
	})

	t.Run("pending cleaned up after success", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 10}}}

		c := NewClient(sim)
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Describe(ctx, "s1")
		require.NoError(t, err)

		c.mu.Lock()
		assert.Empty(t, c.pending)
		c.mu.Unlock()
	})

	t.Run("stream not found", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		c := NewClient(sim)
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Describe(ctx, "nonexistent")
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("context cancelled", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		// No streams configured, no response will come
		c := NewClient(sim)
		defer c.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		_, err := c.Describe(ctx, "fw-update")
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("closed client", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		c := NewClient(sim)
		c.Close()

		_, err := c.Describe(context.Background(), "fw-update")
		require.ErrorIs(t, err, ErrClientClosed)
	})

	t.Run("custom client ID", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 10}}}

		c := NewClient(sim, WithClientID("custom"))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		desc, err := c.Describe(ctx, "s1")
		require.NoError(t, err)
		assert.Equal(t, 1, desc.Version)
	})

	t.Run("subscribe error", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.subscribeErr = "description"

		c := NewClient(sim)
		defer c.Close()

		_, err := c.Describe(context.Background(), "s1")
		require.ErrorIs(t, err, errSubscribeFailed)
	})

	t.Run("publish error", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.publishErr = errors.New("publish failed")

		c := NewClient(sim)
		defer c.Close()

		_, err := c.Describe(context.Background(), "s1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "publish failed")
	})
}

func TestDownload(t *testing.T) {
	t.Run("small file single block", func(t *testing.T) {
		fileData := []byte("0123456789")
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10, Checksum: crc32.ChecksumIEEE(fileData)}},
		}
		sim.files["s1/0"] = fileData

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Equal(t, fileData, data)
	})

	t.Run("multi-block file", func(t *testing.T) {
		fileData := make([]byte, 1024)
		for i := range fileData {
			fileData[i] = byte(i % 256)
		}

		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: len(fileData)}},
		}
		sim.files["s1/0"] = fileData

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Equal(t, fileData, data)
	})

	t.Run("file not in stream", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 100}},
		}

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Download(ctx, "s1", 5)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("empty file", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 0}},
		}

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("stream not found", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Download(ctx, "nonexistent", 0)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("context cancelled during download", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10000}},
		}
		// File data intentionally missing - get will not respond with blocks
		// so Download will block waiting for data

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := c.Download(ctx, "s1", 0)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("closed client", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		c := NewClient(sim)
		c.Close()

		_, err := c.Download(context.Background(), "s1", 0)
		require.ErrorIs(t, err, ErrClientClosed)
	})

	t.Run("default block size", func(t *testing.T) {
		c := NewClient(newSimulatedClient("dev1"))
		assert.Equal(t, 4096, c.blockSize)
		c.Close()
	})

	t.Run("block size clamped to min", func(t *testing.T) {
		c := NewClient(newSimulatedClient("dev1"), WithBlockSize(0))
		assert.Equal(t, MinBlockSize, c.blockSize)
		c.Close()
	})

	t.Run("block size clamped to max", func(t *testing.T) {
		c := NewClient(newSimulatedClient("dev1"), WithBlockSize(MaxBlockSize+1))
		assert.Equal(t, MaxBlockSize, c.blockSize)
		c.Close()
	})

	t.Run("non-aligned file size", func(t *testing.T) {
		fileData := []byte("hello world!") // 12 bytes, not aligned to 256

		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: len(fileData)}},
		}
		sim.files["s1/0"] = fileData

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Equal(t, fileData, data)
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		fileData := []byte("0123456789")
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10, Checksum: 0xBADBAD}},
		}
		sim.files["s1/0"] = fileData

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Download(ctx, "s1", 0)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrChecksumMismatch, errResp.Code)
	})

	t.Run("zero checksum skips verification", func(t *testing.T) {
		fileData := []byte("0123456789")
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10}}, // no checksum
		}
		sim.files["s1/0"] = fileData

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Equal(t, fileData, data)
	})
}

func TestUpload(t *testing.T) {
	t.Run("small file single block", func(t *testing.T) {
		sim := newSimulatedClient("dev1")

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		fileData := []byte("hello upload")
		err := c.Upload(ctx, "sensor", "sensor data", fileData)
		require.NoError(t, err)

		sim.mu.Lock()
		assert.Equal(t, fileData, sim.uploaded["sensor/0"][:len(fileData)])
		sim.mu.Unlock()
	})

	t.Run("multi-block file", func(t *testing.T) {
		sim := newSimulatedClient("dev1")

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		fileData := make([]byte, 1024)
		for i := range fileData {
			fileData[i] = byte(i % 256)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := c.Upload(ctx, "sensor", "big data", fileData)
		require.NoError(t, err)

		sim.mu.Lock()
		assert.Equal(t, fileData, sim.uploaded["sensor/0"])
		sim.mu.Unlock()
	})

	t.Run("empty data", func(t *testing.T) {
		sim := newSimulatedClient("dev1")

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := c.Upload(ctx, "sensor", "empty", []byte{})
		require.NoError(t, err)
	})

	t.Run("create rejected", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.rejectCreate = true

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := c.Upload(ctx, "sensor", "data", []byte("test"))
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("closed client", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		c := NewClient(sim)
		c.Close()

		err := c.Upload(context.Background(), "sensor", "data", []byte("test"))
		require.ErrorIs(t, err, ErrClientClosed)
	})

	t.Run("publish error", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.publishErr = errors.New("publish failed")

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		err := c.Upload(context.Background(), "sensor", "data", []byte("test"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "publish failed")
	})

	t.Run("includes checksum", func(t *testing.T) {
		var capturedPayload []byte
		sim := newSimulatedClient("dev1")
		origPublish := sim.Publish
		_ = origPublish // use the simulatedClient publish

		// Wrap to capture create payload
		sim2 := &checksumCaptureSim{simulatedClient: sim}

		c := NewClient(sim2, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		fileData := []byte("checksum test data")
		err := c.Upload(ctx, "sensor", "test", fileData)
		require.NoError(t, err)

		require.NotNil(t, sim2.createPayload)

		var req CreateRequest
		require.NoError(t, json.Unmarshal(sim2.createPayload, &req))
		require.Len(t, req.Files, 1)
		assert.Equal(t, crc32.ChecksumIEEE(fileData), req.Files[0].Checksum)
		_ = capturedPayload
	})

	t.Run("context cancelled during upload", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		// rejectCreate not set, but we cancel context before it can process
		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := c.Upload(ctx, "sensor", "data", []byte("test"))
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestDownloadBlockValidation(t *testing.T) {
	t.Run("duplicate blocks are ignored", func(t *testing.T) {
		fileData := []byte("0123456789")
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10}},
		}
		sim.files["s1/0"] = fileData

		// Wrapper that sends each block twice
		wrapper := &duplicateBlocks{simulatedClient: sim}
		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Equal(t, fileData, data)
	})

	t.Run("heavy duplicates do not drop unique blocks", func(t *testing.T) {
		// Multi-block file where each block is sent 5 times.
		// The unbounded queue absorbs all duplicates without dropping unique blocks.
		fileData := make([]byte, 1024)
		for i := range fileData {
			fileData[i] = byte(i % 256)
		}

		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: len(fileData)}},
		}
		sim.files["s1/0"] = fileData

		wrapper := &heavyDuplicateBlocks{simulatedClient: sim, copies: 5}
		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Equal(t, fileData, data)
	})

	t.Run("malformed block then valid block succeeds", func(t *testing.T) {
		// A malformed block (wrong BlockSize) for a valid (FileID, BlockID)
		// uses a different dedup key, so the valid copy is still accepted.
		fileData := []byte("0123456789")
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10}},
		}
		sim.files["s1/0"] = fileData

		wrapper := &malformedThenValid{simulatedClient: sim}
		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Equal(t, fileData, data)
	})

	t.Run("out of range block ID is ignored", func(t *testing.T) {
		fileData := []byte("0123456789")
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10}},
		}
		sim.files["s1/0"] = fileData

		// Wrapper that sends a block with out-of-range ID before real data
		wrapper := &outOfRangeBlock{simulatedClient: sim}
		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Equal(t, fileData, data)
	})

	t.Run("wrong file ID is ignored", func(t *testing.T) {
		fileData := []byte("0123456789")
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10}},
		}
		sim.files["s1/0"] = fileData

		wrapper := &wrongFileIDBlock{simulatedClient: sim}
		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Equal(t, fileData, data)
	})
}

func TestResponseTypeValidation(t *testing.T) {
	t.Run("describe rejects non-description response", func(t *testing.T) {
		sim := newSimulatedClient("dev1")

		// Wrapper that responds with accepted instead of description
		wrapper := &respondWithAccepted{simulatedClient: sim}
		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Describe(ctx, "s1")
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "unexpected response type")
	})

	t.Run("upload create rejects non-accepted response", func(t *testing.T) {
		sim := newSimulatedClient("dev1")

		// Wrapper that responds with description instead of accepted
		wrapper := &respondCreateWithDescription{simulatedClient: sim}
		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := c.Upload(ctx, "s1", "test", []byte("data"))
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "unexpected response type")
	})
}

// respondWithAccepted responds to describe with an accepted response instead.
type respondWithAccepted struct {
	*simulatedClient
}

func (r *respondWithAccepted) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixDescribe {
		var req DescribeRequest
		if len(msg.Payload) > 0 {
			_ = json.Unmarshal(msg.Payload, &req)
		}
		go r.sendToHandler(parsed, suffixAccepted, &AcceptedResponse{
			ClientToken: req.ClientToken,
		})
		return nil
	}
	return r.simulatedClient.Publish(msg)
}

func (r *respondWithAccepted) Subscribe(f string, q byte, h mqttv5.MessageHandler) error {
	return r.simulatedClient.Subscribe(f, q, h)
}
func (r *respondWithAccepted) Unsubscribe(f ...string) error {
	return r.simulatedClient.Unsubscribe(f...)
}
func (r *respondWithAccepted) ClientID() string { return r.simulatedClient.ClientID() }

// respondCreateWithDescription responds to create with a description response.
type respondCreateWithDescription struct {
	*simulatedClient
}

func (r *respondCreateWithDescription) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixCreate {
		var req CreateRequest
		_ = json.Unmarshal(msg.Payload, &req)
		go r.sendToHandler(parsed, suffixDescription, &DescribeResponse{
			ClientToken: req.ClientToken,
			Version:     1,
		})
		return nil
	}
	return r.simulatedClient.Publish(msg)
}

func (r *respondCreateWithDescription) Subscribe(f string, q byte, h mqttv5.MessageHandler) error {
	return r.simulatedClient.Subscribe(f, q, h)
}
func (r *respondCreateWithDescription) Unsubscribe(f ...string) error {
	return r.simulatedClient.Unsubscribe(f...)
}
func (r *respondCreateWithDescription) ClientID() string { return r.simulatedClient.ClientID() }

func TestDownloadUndersizedBlock(t *testing.T) {
	t.Run("truncated block is rejected", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 512}},
		}
		// Provide only 100 bytes of data so the server sends a truncated block
		sim.files["s1/0"] = make([]byte, 100)

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		// The server sends a block of 100 bytes when 256 is expected for block 0.
		// Download should ignore it and time out.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := c.Download(ctx, "s1", 0)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestRouteResponseNonBlocking(t *testing.T) {
	sim := newSimulatedClient("dev1")
	sim.streams["s1"] = &StreamDocument{
		Version: 1,
		Files:   []FileInfo{{FileID: 0, Size: 10}},
	}
	sim.files["s1/0"] = []byte("0123456789")

	c := NewClient(sim, WithBlockSize(256))
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Describe succeeds - this registers handlers
	desc, err := c.Describe(ctx, "s1")
	require.NoError(t, err)
	assert.Equal(t, 1, desc.Version)

	// Send a duplicate description with unknown token - should not block
	sim.mu.Lock()
	handler := sim.handlers[buildTopic("dev1", "s1", suffixDescription)]
	sim.mu.Unlock()

	if handler != nil {
		// This would block forever in the old code if the channel was full
		done := make(chan struct{})
		go func() {
			payload, _ := json.Marshal(DescribeResponse{ClientToken: "unknown-token"})
			handler(&mqttv5.Message{Payload: payload})
			close(done)
		}()

		select {
		case <-done:
			// Non-blocking - good
		case <-time.After(time.Second):
			t.Fatal("routeResponse blocked on unknown token")
		}
	}
}

func TestDownloadEdgeCases(t *testing.T) {
	t.Run("publish error after describe", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 100}},
		}
		sim.files["s1/0"] = make([]byte, 100)

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		// Describe will succeed, then set publishErr for the get request
		// Use a wrapper that fails on get
		wrapper := &publishFailAfterDescribe{simulatedClient: sim}
		c.mqtt = wrapper

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Download(ctx, "s1", 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get publish fail")
	})

	t.Run("error response during block reception", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 1000}},
		}
		// Provide partial data so server sends error for remaining blocks
		sim.files["s1/0"] = make([]byte, 1000)

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		// Use wrapper that sends an error response instead of data blocks
		wrapper := &rejectGetBlocks{simulatedClient: sim}
		c.mqtt = wrapper

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Download(ctx, "s1", 0)
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
	})

	t.Run("nil dataBlock in response is skipped", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10}},
		}
		sim.files["s1/0"] = []byte("0123456789")

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		// Wrapper that sends an accepted response (nil dataBlock) on the accepted topic
		// with the same token, then sends real data blocks
		wrapper := &injectAcceptedBeforeData{simulatedClient: sim}
		c.mqtt = wrapper

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		data, err := c.Download(ctx, "s1", 0)
		require.NoError(t, err)
		assert.Equal(t, []byte("0123456789"), data)
	})

	t.Run("decode error in data block", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 10}},
		}

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		// Wrapper that sends invalid base64
		wrapper := &sendBadBase64Block{simulatedClient: sim}
		c.mqtt = wrapper

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Download(ctx, "s1", 0)
		require.Error(t, err)
	})

	t.Run("closed client after describe in download", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 100}},
		}
		sim.files["s1/0"] = make([]byte, 100)

		// closeOnDescribeResp closes the client when the describe response arrives,
		// before Download can acquire the lock at line 162
		wrapper := &closeOnDescribeResp{simulatedClient: sim}
		c := NewClient(wrapper, WithBlockSize(256))
		wrapper.client = c

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Download(ctx, "s1", 0)
		require.ErrorIs(t, err, ErrClientClosed)
	})
}

func TestUploadEdgeCases(t *testing.T) {
	t.Run("subscribe error on data topic", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.subscribeErr = "data"

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		err := c.Upload(context.Background(), "sensor", "data", []byte("test"))
		require.ErrorIs(t, err, errSubscribeFailed)
	})

	t.Run("subscribe error on rejected topic", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.subscribeErr = "rejected"

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		err := c.Upload(context.Background(), "sensor", "data", []byte("test"))
		require.ErrorIs(t, err, errSubscribeFailed)
	})

	t.Run("subscribe error on accepted topic", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.subscribeErr = "accepted"

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		err := c.Upload(context.Background(), "sensor", "data", []byte("test"))
		require.ErrorIs(t, err, errSubscribeFailed)
	})

	t.Run("subscribe rollback on partial failure", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.subscribeErr = "rejected" // description and data succeed, rejected fails

		c := NewClient(sim, WithBlockSize(256))
		defer c.Close()

		err := c.Upload(context.Background(), "sensor", "data", []byte("test"))
		require.ErrorIs(t, err, errSubscribeFailed)

		// Verify earlier subscriptions were rolled back
		sim.mu.Lock()
		assert.Len(t, sim.unsubscribed, 2) // description + data unsubscribed
		sim.mu.Unlock()

		// Stream should NOT be marked as subscribed
		c.mu.Lock()
		assert.False(t, c.streams["sensor"])
		assert.Empty(t, c.subTopics)
		c.mu.Unlock()
	})

	t.Run("closed during block send loop", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		wrapper := &closeAfterCreate{simulatedClient: sim}

		c := NewClient(wrapper, WithBlockSize(256))
		wrapper.client = c

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := c.Upload(ctx, "sensor", "data", make([]byte, 512))
		require.ErrorIs(t, err, ErrClientClosed)
	})

	t.Run("publish error during block send", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		wrapper := &failPublishOnData{simulatedClient: sim}

		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := c.Upload(ctx, "sensor", "data", make([]byte, 256))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data publish fail")
	})

	t.Run("block acceptance rejected", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		wrapper := &rejectUploadBlocks{simulatedClient: sim}

		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := c.Upload(ctx, "sensor", "data", make([]byte, 256))
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
	})

	t.Run("context timeout during block wait", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		wrapper := &noBlockAcceptance{simulatedClient: sim}

		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := c.Upload(ctx, "sensor", "data", make([]byte, 256))
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("unexpected response type during block acceptance", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		wrapper := &respondBlockWithDescription{simulatedClient: sim}

		c := NewClient(wrapper, WithBlockSize(256))
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := c.Upload(ctx, "sensor", "data", make([]byte, 256))
		require.Error(t, err)

		var errResp *ErrorResponse
		require.ErrorAs(t, err, &errResp)
		assert.Contains(t, errResp.Message, "unexpected response type")
	})
}

func TestClientHandlerInvalidPayloads(t *testing.T) {
	cases := []struct {
		name        string
		suffix      string
		noTokenJSON string
	}{
		{"description", suffixDescription, `{"s":1}`},
		{"data", suffixData, `{"f":0}`},
		{"accepted", suffixAccepted, `{"f":0}`},
		{"rejected", suffixRejected, `{"o":"err"}`},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("invalid %s payload", tc.name), func(_ *testing.T) {
			sim := newSimulatedClient("dev1")
			sim.streams["s1"] = &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 5}}}
			sim.files["s1/0"] = []byte("aaaaa")

			c := NewClient(sim, WithBlockSize(256))
			defer c.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_, _ = c.Describe(ctx, "s1")

			sim.mu.Lock()
			handler := sim.handlers[buildTopic("dev1", "s1", tc.suffix)]
			sim.mu.Unlock()

			if handler != nil {
				handler(&mqttv5.Message{Payload: []byte("bad json")})
				handler(&mqttv5.Message{Payload: []byte(tc.noTokenJSON)})
			}
		})
	}
}

func TestEnsureSubscribedIdempotent(t *testing.T) {
	sim := newSimulatedClient("dev1")
	sim.streams["s1"] = &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 5}}}
	sim.files["s1/0"] = []byte("aaaaa")

	c := NewClient(sim, WithBlockSize(256))
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// First describe subscribes
	_, err := c.Describe(ctx, "s1")
	require.NoError(t, err)

	// Second describe reuses existing subscription
	_, err = c.Describe(ctx, "s1")
	require.NoError(t, err)

	sim.mu.Lock()
	// Only 4 handlers registered (not 8)
	assert.Len(t, sim.handlers, 4)
	sim.mu.Unlock()
}

// --- Helper wrappers for edge case testing ---

// publishFailAfterDescribe fails on get/create but not describe.
type publishFailAfterDescribe struct {
	*simulatedClient
	described bool
}

func (p *publishFailAfterDescribe) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixDescribe {
		p.described = true
		return p.simulatedClient.Publish(msg)
	}
	if p.described {
		return errors.New("get publish fail")
	}
	return p.simulatedClient.Publish(msg)
}

// rejectGetBlocks sends error responses instead of data blocks.
type rejectGetBlocks struct {
	*simulatedClient
}

func (r *rejectGetBlocks) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixGet {
		var req GetRequest
		_ = json.Unmarshal(msg.Payload, &req)

		go r.sendToHandler(parsed, suffixRejected, &ErrorResponse{
			Code:        ErrResourceNotFound,
			Message:     "file not found",
			ClientToken: req.ClientToken,
		})
		return nil
	}
	return r.simulatedClient.Publish(msg)
}

// injectAcceptedBeforeData sends an accepted response on the accepted topic
// before real data blocks. The accepted response routes to the same pending
// channel and has dataBlock == nil, testing the nil dataBlock continue path.
type injectAcceptedBeforeData struct {
	*simulatedClient
}

func (i *injectAcceptedBeforeData) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixGet {
		var req GetRequest
		_ = json.Unmarshal(msg.Payload, &req)

		go func() {
			// Send accepted response with same token (has nil dataBlock in clientResponse)
			i.sendToHandler(parsed, suffixAccepted, &AcceptedResponse{
				ClientToken: req.ClientToken,
			})
			// Small delay then send real data
			time.Sleep(5 * time.Millisecond)
			i.simulateGet(parsed, msg.Payload)
		}()
		return nil
	}
	return i.simulatedClient.Publish(msg)
}

func (i *injectAcceptedBeforeData) Subscribe(filter string, qos byte, handler mqttv5.MessageHandler) error {
	return i.simulatedClient.Subscribe(filter, qos, handler)
}

func (i *injectAcceptedBeforeData) Unsubscribe(filters ...string) error {
	return i.simulatedClient.Unsubscribe(filters...)
}

func (i *injectAcceptedBeforeData) ClientID() string { return i.simulatedClient.ClientID() }

// sendBadBase64Block sends data blocks with invalid base64.
type sendBadBase64Block struct {
	*simulatedClient
}

func (s *sendBadBase64Block) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixGet {
		var req GetRequest
		_ = json.Unmarshal(msg.Payload, &req)

		go s.sendToHandler(parsed, suffixData, &DataBlockResponse{
			ClientToken: req.ClientToken,
			FileID:      0,
			BlockSize:   256,
			BlockID:     0,
			Payload:     "not-valid-base64!!!",
		})
		return nil
	}
	return s.simulatedClient.Publish(msg)
}

// closeOnDescribeResp wraps the description handler to close the client
// when the describe response arrives, before Download can proceed.
type closeOnDescribeResp struct {
	*simulatedClient
	client *Client
}

func (c *closeOnDescribeResp) Subscribe(filter string, qos byte, handler mqttv5.MessageHandler) error {
	if strings.Contains(filter, suffixDescription) {
		origHandler := handler
		handler = func(msg *mqttv5.Message) {
			// Close client before routing the describe response
			c.client.mu.Lock()
			c.client.closed = true
			c.client.mu.Unlock()
			origHandler(msg)
		}
	}
	return c.simulatedClient.Subscribe(filter, qos, handler)
}

func (c *closeOnDescribeResp) Publish(msg *mqttv5.Message) error {
	return c.simulatedClient.Publish(msg)
}

func (c *closeOnDescribeResp) Unsubscribe(filters ...string) error {
	return c.simulatedClient.Unsubscribe(filters...)
}

func (c *closeOnDescribeResp) ClientID() string { return c.simulatedClient.ClientID() }

// closeAfterCreate closes the client after create acceptance.
type closeAfterCreate struct {
	*simulatedClient
	client    *Client
	gotCreate bool
}

func (c *closeAfterCreate) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixCreate {
		c.gotCreate = true
		err := c.simulatedClient.Publish(msg)
		// Wait a bit for create to be processed, then close
		time.Sleep(10 * time.Millisecond)
		c.client.mu.Lock()
		c.client.closed = true
		c.client.mu.Unlock()
		return err
	}
	return c.simulatedClient.Publish(msg)
}

func (c *closeAfterCreate) Subscribe(filter string, qos byte, handler mqttv5.MessageHandler) error {
	return c.simulatedClient.Subscribe(filter, qos, handler)
}

func (c *closeAfterCreate) Unsubscribe(filters ...string) error {
	return c.simulatedClient.Unsubscribe(filters...)
}

func (c *closeAfterCreate) ClientID() string { return c.simulatedClient.ClientID() }

// failPublishOnData fails Publish on data topics.
type failPublishOnData struct {
	*simulatedClient
}

func (f *failPublishOnData) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixData {
		return errors.New("data publish fail")
	}
	return f.simulatedClient.Publish(msg)
}

func (f *failPublishOnData) Subscribe(filter string, qos byte, handler mqttv5.MessageHandler) error {
	return f.simulatedClient.Subscribe(filter, qos, handler)
}

func (f *failPublishOnData) Unsubscribe(filters ...string) error {
	return f.simulatedClient.Unsubscribe(filters...)
}

func (f *failPublishOnData) ClientID() string { return f.simulatedClient.ClientID() }

// rejectUploadBlocks rejects data blocks with error.
type rejectUploadBlocks struct {
	*simulatedClient
}

func (r *rejectUploadBlocks) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixData {
		var block DataBlockResponse
		_ = json.Unmarshal(msg.Payload, &block)

		go r.sendToHandler(parsed, suffixRejected, &ErrorResponse{
			Code:        ErrInvalidRequest,
			Message:     "block rejected",
			ClientToken: block.ClientToken,
		})
		return nil
	}
	return r.simulatedClient.Publish(msg)
}

func (r *rejectUploadBlocks) Subscribe(filter string, qos byte, handler mqttv5.MessageHandler) error {
	return r.simulatedClient.Subscribe(filter, qos, handler)
}

func (r *rejectUploadBlocks) Unsubscribe(filters ...string) error {
	return r.simulatedClient.Unsubscribe(filters...)
}

func (r *rejectUploadBlocks) ClientID() string { return r.simulatedClient.ClientID() }

// noBlockAcceptance doesn't respond to data blocks (for timeout testing).
type noBlockAcceptance struct {
	*simulatedClient
}

func (n *noBlockAcceptance) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixData {
		// Don't respond - causes timeout
		return nil
	}
	return n.simulatedClient.Publish(msg)
}

func (n *noBlockAcceptance) Subscribe(filter string, qos byte, handler mqttv5.MessageHandler) error {
	return n.simulatedClient.Subscribe(filter, qos, handler)
}

func (n *noBlockAcceptance) Unsubscribe(filters ...string) error {
	return n.simulatedClient.Unsubscribe(filters...)
}

func (n *noBlockAcceptance) ClientID() string { return n.simulatedClient.ClientID() }

// duplicateBlocks sends each data block twice.
type duplicateBlocks struct {
	*simulatedClient
}

func (d *duplicateBlocks) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixGet {
		var req GetRequest
		_ = json.Unmarshal(msg.Payload, &req)

		go func() {
			d.simulateGet(parsed, msg.Payload)
			// Send duplicates
			d.simulateGet(parsed, msg.Payload)
		}()
		return nil
	}
	return d.simulatedClient.Publish(msg)
}

func (d *duplicateBlocks) Subscribe(f string, q byte, h mqttv5.MessageHandler) error {
	return d.simulatedClient.Subscribe(f, q, h)
}
func (d *duplicateBlocks) Unsubscribe(f ...string) error { return d.simulatedClient.Unsubscribe(f...) }
func (d *duplicateBlocks) ClientID() string              { return d.simulatedClient.ClientID() }

// heavyDuplicateBlocks sends each data block N times to stress the unbounded queue.
type heavyDuplicateBlocks struct {
	*simulatedClient
	copies int
}

func (h *heavyDuplicateBlocks) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixGet {
		go func() {
			for range h.copies {
				h.simulateGet(parsed, msg.Payload)
			}
		}()
		return nil
	}
	return h.simulatedClient.Publish(msg)
}

func (h *heavyDuplicateBlocks) Subscribe(f string, q byte, handler mqttv5.MessageHandler) error {
	return h.simulatedClient.Subscribe(f, q, handler)
}
func (h *heavyDuplicateBlocks) Unsubscribe(f ...string) error {
	return h.simulatedClient.Unsubscribe(f...)
}
func (h *heavyDuplicateBlocks) ClientID() string { return h.simulatedClient.ClientID() }

// outOfRangeBlock sends a block with an out-of-range BlockID before real data.
type outOfRangeBlock struct {
	*simulatedClient
}

func (o *outOfRangeBlock) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixGet {
		var req GetRequest
		_ = json.Unmarshal(msg.Payload, &req)

		go func() {
			o.sendToHandler(parsed, suffixData, &DataBlockResponse{
				ClientToken: req.ClientToken,
				FileID:      0,
				BlockSize:   req.BlockSize,
				BlockID:     9999,
				Payload:     base64.StdEncoding.EncodeToString([]byte("bad")),
			})
			o.simulateGet(parsed, msg.Payload)
		}()
		return nil
	}
	return o.simulatedClient.Publish(msg)
}

func (o *outOfRangeBlock) Subscribe(f string, q byte, h mqttv5.MessageHandler) error {
	return o.simulatedClient.Subscribe(f, q, h)
}
func (o *outOfRangeBlock) Unsubscribe(f ...string) error { return o.simulatedClient.Unsubscribe(f...) }
func (o *outOfRangeBlock) ClientID() string              { return o.simulatedClient.ClientID() }

// wrongFileIDBlock sends a block with a wrong FileID before real data.
type wrongFileIDBlock struct {
	*simulatedClient
}

func (w *wrongFileIDBlock) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixGet {
		var req GetRequest
		_ = json.Unmarshal(msg.Payload, &req)

		go func() {
			w.sendToHandler(parsed, suffixData, &DataBlockResponse{
				ClientToken: req.ClientToken,
				FileID:      99,
				BlockSize:   req.BlockSize,
				BlockID:     0,
				Payload:     base64.StdEncoding.EncodeToString([]byte("bad")),
			})
			w.simulateGet(parsed, msg.Payload)
		}()
		return nil
	}
	return w.simulatedClient.Publish(msg)
}

func (w *wrongFileIDBlock) Subscribe(f string, q byte, h mqttv5.MessageHandler) error {
	return w.simulatedClient.Subscribe(f, q, h)
}
func (w *wrongFileIDBlock) Unsubscribe(f ...string) error {
	return w.simulatedClient.Unsubscribe(f...)
}
func (w *wrongFileIDBlock) ClientID() string { return w.simulatedClient.ClientID() }

// malformedThenValid sends a block with wrong BlockSize first, then the valid block.
type malformedThenValid struct {
	*simulatedClient
}

func (m *malformedThenValid) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixGet {
		var req GetRequest
		_ = json.Unmarshal(msg.Payload, &req)

		go func() {
			// Send malformed block: correct FileID/BlockID but wrong BlockSize
			m.sendToHandler(parsed, suffixData, &DataBlockResponse{
				ClientToken: req.ClientToken,
				FileID:      req.FileID,
				BlockSize:   512, // wrong block size
				BlockID:     0,
				Payload:     base64.StdEncoding.EncodeToString([]byte("bad")),
			})
			// Then send valid data
			m.simulateGet(parsed, msg.Payload)
		}()
		return nil
	}
	return m.simulatedClient.Publish(msg)
}

func (m *malformedThenValid) Subscribe(f string, q byte, h mqttv5.MessageHandler) error {
	return m.simulatedClient.Subscribe(f, q, h)
}
func (m *malformedThenValid) Unsubscribe(f ...string) error {
	return m.simulatedClient.Unsubscribe(f...)
}
func (m *malformedThenValid) ClientID() string { return m.simulatedClient.ClientID() }

// respondBlockWithDescription responds to data block uploads with a description
// response (wrong type) instead of accepted, to trigger unexpected response type.
type respondBlockWithDescription struct {
	*simulatedClient
}

func (r *respondBlockWithDescription) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixData {
		var block DataBlockResponse
		_ = json.Unmarshal(msg.Payload, &block)

		go r.sendToHandler(parsed, suffixDescription, &DescribeResponse{
			ClientToken: block.ClientToken,
			Version:     1,
		})
		return nil
	}
	return r.simulatedClient.Publish(msg)
}

func (r *respondBlockWithDescription) Subscribe(f string, q byte, h mqttv5.MessageHandler) error {
	return r.simulatedClient.Subscribe(f, q, h)
}
func (r *respondBlockWithDescription) Unsubscribe(f ...string) error {
	return r.simulatedClient.Unsubscribe(f...)
}
func (r *respondBlockWithDescription) ClientID() string { return r.simulatedClient.ClientID() }

// blockPublish blocks on Publish for describe requests until unblock is called.
type blockPublish struct {
	*simulatedClient
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockPublish) Publish(msg *mqttv5.Message) error {
	parsed := parseStreamTopic(msg.Topic)
	if parsed != nil && parsed.Suffix == suffixDescribe {
		b.once.Do(func() { close(b.blocked) })
		<-b.release
		return nil
	}
	return b.simulatedClient.Publish(msg)
}

func (b *blockPublish) waitBlocked() { <-b.blocked }
func (b *blockPublish) unblock()     { close(b.release) }

func (b *blockPublish) Subscribe(f string, q byte, h mqttv5.MessageHandler) error {
	return b.simulatedClient.Subscribe(f, q, h)
}
func (b *blockPublish) Unsubscribe(f ...string) error {
	return b.simulatedClient.Unsubscribe(f...)
}
func (b *blockPublish) ClientID() string { return b.simulatedClient.ClientID() }

func TestCloseNonBlocking(t *testing.T) {
	sim := newSimulatedClient("dev1")
	sim.streams["s1"] = &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 10}}}
	sim.files["s1/0"] = []byte("0123456789")

	c := NewClient(sim, WithBlockSize(256))

	// Manually add a pending channel that is already full
	c.mu.Lock()
	ch := make(chan clientResponse, 1)
	ch <- clientResponse{descResp: &DescribeResponse{Version: 1}}
	c.pending["full-token"] = ch
	c.mu.Unlock()

	// Close should not block even though the channel is full
	done := make(chan struct{})
	go func() {
		c.Close()
		close(done)
	}()

	select {
	case <-done:
		// Success - Close returned without blocking
	case <-time.After(time.Second):
		t.Fatal("Close() blocked on full pending channel")
	}
}

func TestResponseQueue(t *testing.T) {
	t.Run("enqueue and dequeue in order", func(t *testing.T) {
		q := newResponseQueue()

		q.enqueue(clientResponse{dataBlock: &DataBlockResponse{FileID: 0, BlockID: 0}})
		q.enqueue(clientResponse{dataBlock: &DataBlockResponse{FileID: 0, BlockID: 0}}) // dup allowed
		q.enqueue(clientResponse{dataBlock: &DataBlockResponse{FileID: 0, BlockID: 1}})

		r1, ok := q.dequeue()
		require.True(t, ok)
		assert.Equal(t, 0, r1.dataBlock.BlockID)

		r2, ok := q.dequeue()
		require.True(t, ok)
		assert.Equal(t, 0, r2.dataBlock.BlockID) // dup is queued

		r3, ok := q.dequeue()
		require.True(t, ok)
		assert.Equal(t, 1, r3.dataBlock.BlockID)

		_, ok = q.dequeue()
		assert.False(t, ok)
	})

	t.Run("error responses accepted", func(t *testing.T) {
		q := newResponseQueue()

		q.enqueue(clientResponse{err: &ErrorResponse{Code: ErrInvalidRequest}})
		q.enqueue(clientResponse{err: &ErrorResponse{Code: ErrResourceNotFound}})

		r1, ok := q.dequeue()
		require.True(t, ok)
		var errResp1 *ErrorResponse
		require.ErrorAs(t, r1.err, &errResp1)
		assert.Equal(t, ErrInvalidRequest, errResp1.Code)

		r2, ok := q.dequeue()
		require.True(t, ok)
		var errResp2 *ErrorResponse
		require.ErrorAs(t, r2.err, &errResp2)
		assert.Equal(t, ErrResourceNotFound, errResp2.Code)
	})

	t.Run("no items dropped under high volume", func(t *testing.T) {
		q := newResponseQueue()

		for i := range 100 {
			q.enqueue(clientResponse{dataBlock: &DataBlockResponse{FileID: 0, BlockID: i}})
		}

		for i := range 100 {
			r, ok := q.dequeue()
			require.True(t, ok)
			assert.Equal(t, i, r.dataBlock.BlockID)
		}

		_, ok := q.dequeue()
		assert.False(t, ok)
	})
}

func TestClose(t *testing.T) {
	t.Run("unsubscribes all topics", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 10}}}
		sim.files["s1/0"] = []byte("0123456789")

		c := NewClient(sim, WithBlockSize(256))

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, _ = c.Describe(ctx, "s1")

		err := c.Close()
		require.NoError(t, err)

		sim.mu.Lock()
		assert.Len(t, sim.unsubscribed, 4) // description, data, rejected, accepted
		sim.mu.Unlock()
	})

	t.Run("double close is safe", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		c := NewClient(sim)

		require.NoError(t, c.Close())
		require.NoError(t, c.Close())
	})

	t.Run("in-flight pending returns ErrClientClosed", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 10}}}
		sim.files["s1/0"] = []byte("0123456789")

		// Use a wrapper that blocks publish so the describe stays in-flight
		wrapper := &blockPublish{
			simulatedClient: sim,
			blocked:         make(chan struct{}),
			release:         make(chan struct{}),
		}
		c := NewClient(wrapper, WithBlockSize(256))

		errCh := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := c.Describe(ctx, "s1")
			errCh <- err
		}()

		// Wait for publish to be blocked, then close
		wrapper.waitBlocked()
		c.Close()
		wrapper.unblock()

		err := <-errCh
		require.ErrorIs(t, err, ErrClientClosed)
	})

	t.Run("in-flight download returns ErrClientClosed", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 100}}}
		// No file data in sim.files — get requests will be silently dropped,
		// leaving the download blocked waiting for data blocks.

		c := NewClient(sim, WithBlockSize(256))

		started := make(chan struct{})
		errCh := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			close(started)
			_, err := c.Download(ctx, "s1", 0)
			errCh <- err
		}()

		<-started
		// Give the goroutine time to reach the download wait loop
		time.Sleep(50 * time.Millisecond)

		c.Close()

		err := <-errCh
		require.ErrorIs(t, err, ErrClientClosed)
	})

	t.Run("lazy subscribe per stream", func(t *testing.T) {
		sim := newSimulatedClient("dev1")
		sim.streams["s1"] = &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 5}}}
		sim.streams["s2"] = &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 5}}}
		sim.files["s1/0"] = []byte("aaaaa")
		sim.files["s2/0"] = []byte("bbbbb")

		c := NewClient(sim, WithBlockSize(256))

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, _ = c.Describe(ctx, "s1")
		_, _ = c.Describe(ctx, "s2")

		err := c.Close()
		require.NoError(t, err)

		sim.mu.Lock()
		assert.Len(t, sim.unsubscribed, 8) // 4 per stream
		sim.mu.Unlock()
	})
}
