package filedelivery

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

// mockServerClient implements ServerClient for testing.
type mockServerClient struct {
	clientID  string
	namespace string
	mu        sync.Mutex
	sent      []*mqttv5.Message
}

func newMockServerClient(clientID, namespace string) *mockServerClient {
	return &mockServerClient{clientID: clientID, namespace: namespace}
}

func (m *mockServerClient) ClientID() string  { return m.clientID }
func (m *mockServerClient) Namespace() string { return m.namespace }

func (m *mockServerClient) Send(msg *mqttv5.Message) error {
	m.mu.Lock()
	m.sent = append(m.sent, msg)
	m.mu.Unlock()
	return nil
}

func (m *mockServerClient) lastSent() *mqttv5.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sent) == 0 {
		return nil
	}
	return m.sent[len(m.sent)-1]
}

func (m *mockServerClient) allSent() []*mqttv5.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*mqttv5.Message, len(m.sent))
	copy(cp, m.sent)
	return cp
}

// errorStore always returns errors.
type errorStore struct{}

func (e *errorStore) GetStream(_ StreamKey) (*StreamDocument, error) {
	return nil, errors.New("store error")
}

func (e *errorStore) ReadBlock(_ StreamKey, _ int, _ int64, _ int) ([]byte, error) {
	return nil, errors.New("store error")
}

func setupTestStore() (*MemoryStore, StreamKey) {
	store := NewMemoryStore()
	key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "fw-update"}
	doc := &StreamDocument{
		Description: "firmware v2",
		Version:     1,
		Files:       []FileInfo{{FileID: 0, Size: 1024}},
	}
	store.PutStream(key, doc)
	store.PutFile(key, 0, make([]byte, 1024))
	return store, key
}

func TestHandleMessage(t *testing.T) {
	t.Run("not a stream topic", func(t *testing.T) {
		h := NewHandler()
		client := newMockServerClient("dev1", "ns1")
		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "devices/dev1/telemetry",
		})
		assert.False(t, handled)
	})

	t.Run("shadow topic not handled", func(t *testing.T) {
		h := NewHandler()
		client := newMockServerClient("dev1", "ns1")
		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/shadow/update",
		})
		assert.False(t, handled)
	})

	t.Run("response topics ignored", func(t *testing.T) {
		h := NewHandler()
		client := newMockServerClient("dev1", "ns1")

		for _, suffix := range []string{suffixDescription, suffixAccepted, suffixRejected} {
			topic := buildTopic("dev1", "s1", suffix)
			handled := h.HandleMessage(client, &mqttv5.Message{Topic: topic})
			assert.True(t, handled, "suffix: %s", suffix)
			assert.Nil(t, client.lastSent(), "suffix: %s should not produce response", suffix)
		}
	})

	t.Run("data topic ignored without write store", func(t *testing.T) {
		h := NewHandler()
		client := newMockServerClient("dev1", "ns1")

		topic := buildTopic("dev1", "s1", suffixData)
		handled := h.HandleMessage(client, &mqttv5.Message{Topic: topic, Payload: []byte(`{}`)})
		assert.True(t, handled)
		assert.Nil(t, client.lastSent())
	})
}

func TestHandleDescribe(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/describe/json",
			Payload: []byte(`{"c":"tok123"}`),
		})

		assert.True(t, handled)
		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/streams/fw-update/description/json", msg.Topic)
		assert.Equal(t, "application/json", msg.ContentType)

		var resp DescribeResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Equal(t, "tok123", resp.ClientToken)
		assert.Equal(t, 1, resp.Version)
		assert.Equal(t, "firmware v2", resp.Description)
		require.Len(t, resp.Files, 1)
		assert.Equal(t, 0, resp.Files[0].FileID)
		assert.Equal(t, 1024, resp.Files[0].Size)
	})

	t.Run("no payload", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/streams/fw-update/describe/json",
		})

		assert.True(t, handled)
		msg := client.lastSent()
		require.NotNil(t, msg)

		var resp DescribeResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Empty(t, resp.ClientToken)
		assert.Equal(t, 1, resp.Version)
	})

	t.Run("invalid payload", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/describe/json",
			Payload: []byte("not json"),
		})

		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "rejected/json")

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("client token too long", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		longToken := strings.Repeat("a", MaxClientToken+1)
		payload, _ := json.Marshal(DescribeRequest{ClientToken: longToken})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/describe/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "token too long")
	})

	t.Run("stream not found", func(t *testing.T) {
		h := NewHandler()
		client := newMockServerClient("dev1", "ns1")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/streams/nonexistent/describe/json",
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("store error", func(t *testing.T) {
		h := NewHandler(WithStore(&errorStore{}))
		client := newMockServerClient("dev1", "ns1")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/streams/fw-update/describe/json",
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})
}

func TestHandleGetStream(t *testing.T) {
	t.Run("success single block", func(t *testing.T) {
		store := NewMemoryStore()
		key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "fw-update"}
		store.PutStream(key, &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 512}},
		})
		store.PutFile(key, 0, []byte("hello world"))

		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{
			ClientToken: "tok",
			Version:     1,
			FileID:      0,
			BlockSize:   256,
			BlockOffset: 0,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Equal(t, "$things/dev1/streams/fw-update/data/json", msg.Topic)

		var resp DataBlockResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Equal(t, "tok", resp.ClientToken)
		assert.Equal(t, 0, resp.FileID)
		assert.Equal(t, 256, resp.BlockSize)
		assert.Equal(t, 0, resp.BlockID)

		decoded, err := base64.StdEncoding.DecodeString(resp.Payload)
		require.NoError(t, err)
		assert.Equal(t, "hello world", string(decoded))
	})

	t.Run("multiple blocks", func(t *testing.T) {
		store := NewMemoryStore()
		key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "s1"}
		data := make([]byte, 1024)
		for i := range data {
			data[i] = byte(i % 256)
		}
		store.PutStream(key, &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 1024}},
		})
		store.PutFile(key, 0, data)

		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{
			FileID:         0,
			BlockSize:      256,
			NumberOfBlocks: 4,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/s1/get/json",
			Payload: payload,
		})

		msgs := client.allSent()
		assert.Len(t, msgs, 4)

		for i, msg := range msgs {
			var resp DataBlockResponse
			require.NoError(t, json.Unmarshal(msg.Payload, &resp))
			assert.Equal(t, i, resp.BlockID)
			assert.Equal(t, 256, resp.BlockSize)
		}
	})

	t.Run("no payload", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/streams/fw-update/get/json",
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("invalid payload", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: []byte("bad"),
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("block size too small", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: 0, BlockSize: 100})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrBlockSizeOutOfBounds, errResp.Code)
	})

	t.Run("block size too large", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: 0, BlockSize: MaxBlockSize + 1})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrBlockSizeOutOfBounds, errResp.Code)
	})

	t.Run("version mismatch", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{
			Version:   99,
			FileID:    0,
			BlockSize: 256,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrVersionMismatch, errResp.Code)
	})

	t.Run("version zero skips check", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{
			Version:   0,
			FileID:    0,
			BlockSize: 256,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "data/json")
	})

	t.Run("file not found in stream", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{
			FileID:    5,
			BlockSize: 256,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("stream not found", func(t *testing.T) {
		h := NewHandler()
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: 0, BlockSize: 256})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/nonexistent/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("negative file ID", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: -1, BlockSize: 256})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("file ID exceeds max", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: MaxFileID + 1, BlockSize: 256})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("negative block offset", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: 0, BlockSize: 256, BlockOffset: -1})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrOffsetOutOfBounds, errResp.Code)
	})

	t.Run("client token too long", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		longToken := strings.Repeat("x", MaxClientToken+1)
		payload, _ := json.Marshal(GetRequest{
			ClientToken: longToken,
			FileID:      0,
			BlockSize:   256,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("default number of blocks is 1", func(t *testing.T) {
		store := NewMemoryStore()
		key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "s1"}
		store.PutStream(key, &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 2048}},
		})
		store.PutFile(key, 0, make([]byte, 2048))

		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: 0, BlockSize: 256})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/s1/get/json",
			Payload: payload,
		})

		assert.Len(t, client.allSent(), 1)
	})

	t.Run("with bitmap", func(t *testing.T) {
		store := NewMemoryStore()
		key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "s1"}
		store.PutStream(key, &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 4096}},
		})
		store.PutFile(key, 0, make([]byte, 4096))

		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		// bitmap "c0" = 11000000 => blocks 0, 1
		payload, _ := json.Marshal(GetRequest{
			FileID:    0,
			BlockSize: 256,
			Bitmap:    "c0",
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/s1/get/json",
			Payload: payload,
		})

		msgs := client.allSent()
		assert.Len(t, msgs, 2)

		var resp0, resp1 DataBlockResponse
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &resp0))
		require.NoError(t, json.Unmarshal(msgs[1].Payload, &resp1))
		assert.Equal(t, 0, resp0.BlockID)
		assert.Equal(t, 1, resp1.BlockID)
	})

	t.Run("bitmap with offset", func(t *testing.T) {
		store := NewMemoryStore()
		key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "s1"}
		store.PutStream(key, &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 8192}},
		})
		store.PutFile(key, 0, make([]byte, 8192))

		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		// bitmap "80" = 10000000 => block at offset+0 = 10
		payload, _ := json.Marshal(GetRequest{
			FileID:      0,
			BlockSize:   256,
			BlockOffset: 10,
			Bitmap:      "80",
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/s1/get/json",
			Payload: payload,
		})

		msgs := client.allSent()
		require.Len(t, msgs, 1)

		var resp DataBlockResponse
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &resp))
		assert.Equal(t, 10, resp.BlockID)
	})

	t.Run("oversized sparse bitmap rejected early", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		// Sparse bitmap: only one bit set but byte length exceeds max
		oversized := "01" + strings.Repeat("00", MaxNumberBlocks/8)
		payload, _ := json.Marshal(GetRequest{
			FileID:    0,
			BlockSize: 256,
			Bitmap:    oversized,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "bitmap too large")
	})

	t.Run("bitmap exceeds max number of blocks", func(t *testing.T) {
		store := NewMemoryStore()
		key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "s1"}
		store.PutStream(key, &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 1024 * 1024 * 1024}},
		})
		store.PutFile(key, 0, make([]byte, 256))

		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		// Build a bitmap with all bits set, large enough to exceed MaxNumberBlocks.
		// MaxNumberBlocks=131072, so we need 131072/8=16384 bytes = 32768 hex chars of "ff"
		bitmapHex := strings.Repeat("ff", (MaxNumberBlocks/8)+1)

		payload, _ := json.Marshal(GetRequest{
			FileID:    0,
			BlockSize: 256,
			Bitmap:    bitmapHex,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/s1/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "bitmap too large")
	})

	t.Run("invalid bitmap hex", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{
			FileID:    0,
			BlockSize: 256,
			Bitmap:    "zz",
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})
}

func TestDecodeBitmap(t *testing.T) {
	t.Run("single byte all set", func(t *testing.T) {
		ids, err := decodeBitmap("ff", 0)
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, ids)
	})

	t.Run("single byte MSB only", func(t *testing.T) {
		ids, err := decodeBitmap("80", 0)
		require.NoError(t, err)
		assert.Equal(t, []int{0}, ids)
	})

	t.Run("single byte LSB only", func(t *testing.T) {
		ids, err := decodeBitmap("01", 0)
		require.NoError(t, err)
		assert.Equal(t, []int{7}, ids)
	})

	t.Run("with offset", func(t *testing.T) {
		ids, err := decodeBitmap("80", 5)
		require.NoError(t, err)
		assert.Equal(t, []int{5}, ids)
	})

	t.Run("two bytes", func(t *testing.T) {
		// "c080" = 11000000 10000000 => blocks 0, 1, 8
		ids, err := decodeBitmap("c080", 0)
		require.NoError(t, err)
		assert.Equal(t, []int{0, 1, 8}, ids)
	})

	t.Run("invalid hex", func(t *testing.T) {
		_, err := decodeBitmap("zz", 0)
		require.Error(t, err)
	})

	t.Run("empty bitmap", func(t *testing.T) {
		_, err := decodeBitmap("", 0)
		require.Error(t, err)
	})

	t.Run("no bits set", func(t *testing.T) {
		ids, err := decodeBitmap("00", 0)
		require.NoError(t, err)
		assert.Nil(t, ids)
	})
}

func TestHandleCreate(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(CreateRequest{
			ClientToken: "tok",
			Description: "sensor data",
			Files:       []FileInfo{{FileID: 0, Size: 100}},
		})

		handled := h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		assert.True(t, handled)
		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "accepted/json")

		var resp AcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Equal(t, "tok", resp.ClientToken)
	})

	t.Run("uploads not enabled", func(t *testing.T) {
		h := NewHandler()
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(CreateRequest{
			Files: []FileInfo{{FileID: 0, Size: 100}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "uploads not enabled")
	})

	t.Run("no payload", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/streams/sensor/create/json",
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("invalid payload", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: []byte("bad"),
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("no files", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(CreateRequest{Files: []FileInfo{}})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "at least one file")
	})

	t.Run("invalid file ID", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(CreateRequest{
			Files: []FileInfo{{FileID: MaxFileID + 1, Size: 100}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})

	t.Run("zero file size allowed", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(CreateRequest{
			ClientToken: "tok",
			Files:       []FileInfo{{FileID: 0, Size: 0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "accepted/json")

		var resp AcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Equal(t, "tok", resp.ClientToken)
	})

	t.Run("negative file size rejected", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(CreateRequest{
			Files: []FileInfo{{FileID: 0, Size: -1}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "size must not be negative")
	})

	t.Run("duplicate file IDs", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(CreateRequest{
			Files: []FileInfo{{FileID: 0, Size: 100}, {FileID: 0, Size: 200}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "duplicate file ID")
	})

	t.Run("zero size file creates stream", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(CreateRequest{
			ClientToken: "tok",
			Description: "empty file",
			Files:       []FileInfo{{FileID: 0, Size: 0}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "accepted/json")

		// Verify stream was created
		key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "sensor"}
		doc, err := store.GetStream(key)
		require.NoError(t, err)
		require.NotNil(t, doc)
		assert.Len(t, doc.Files, 1)
		assert.Equal(t, 0, doc.Files[0].Size)
	})

	t.Run("stream already exists", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(CreateRequest{
			Files: []FileInfo{{FileID: 0, Size: 100}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		// Second create should fail
		client2 := newMockServerClient("dev1", "ns1")
		h.HandleMessage(client2, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		msg := client2.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "rejected/json")
	})

	t.Run("client token too long", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		longToken := strings.Repeat("a", MaxClientToken+1)
		payload, _ := json.Marshal(CreateRequest{
			ClientToken: longToken,
			Files:       []FileInfo{{FileID: 0, Size: 100}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
	})
}

func TestHandleUploadData(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		// First create the stream
		createPayload, _ := json.Marshal(CreateRequest{
			Files: []FileInfo{{FileID: 0, Size: 256}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: createPayload,
		})

		// Upload a full block (256 bytes = full file, single block)
		blockData := make([]byte, 256)
		for i := range blockData {
			blockData[i] = byte(i)
		}
		dataPayload, _ := json.Marshal(DataBlockResponse{
			ClientToken: "tok",
			FileID:      0,
			BlockSize:   256,
			BlockID:     0,
			Payload:     base64.StdEncoding.EncodeToString(blockData),
		})

		client2 := newMockServerClient("dev1", "ns1")
		h.HandleMessage(client2, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		msg := client2.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "accepted/json")

		var resp AcceptedResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &resp))
		assert.Equal(t, "tok", resp.ClientToken)
		assert.Equal(t, 0, resp.FileID)

		// Verify data was written
		key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "sensor"}
		readBlock, err := store.ReadBlock(key, 0, 0, 256)
		require.NoError(t, err)
		assert.Equal(t, blockData, readBlock)
	})

	t.Run("file not found with full block preserves error code", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		// Full-sized block bypasses stream lookup, hits WriteBlock error
		fullData := make([]byte, 256)
		dataPayload, _ := json.Marshal(DataBlockResponse{
			FileID:    0,
			BlockSize: 256,
			BlockID:   0,
			Payload:   base64.StdEncoding.EncodeToString(fullData),
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "rejected/json")

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("undersized block without stream rejected", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		dataPayload, _ := json.Marshal(DataBlockResponse{
			FileID:    0,
			BlockSize: 256,
			BlockID:   0,
			Payload:   base64.StdEncoding.EncodeToString([]byte("data")),
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "rejected/json")
	})

	t.Run("empty payload rejected", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		h.HandleMessage(client, &mqttv5.Message{
			Topic: "$things/dev1/streams/sensor/data/json",
		})

		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "rejected/json")
	})
}

func TestHandleUploadDataEdgeCases(t *testing.T) {
	assertRejected := func(t *testing.T, client *mockServerClient) {
		t.Helper()
		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "rejected/json")
	}

	t.Run("invalid JSON payload", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: []byte("not json"),
		})

		assertRejected(t, client)
	})

	t.Run("invalid base64 payload", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		dataPayload, _ := json.Marshal(DataBlockResponse{
			FileID:    0,
			BlockSize: 256,
			BlockID:   0,
			Payload:   "not-valid-base64!!!",
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		assertRejected(t, client)
	})

	t.Run("negative block ID rejected", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		dataPayload, _ := json.Marshal(DataBlockResponse{
			FileID:    0,
			BlockSize: 256,
			BlockID:   -1,
			Payload:   base64.StdEncoding.EncodeToString([]byte("data")),
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		assertRejected(t, client)
	})

	t.Run("block size too small rejected", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		dataPayload, _ := json.Marshal(DataBlockResponse{
			FileID:    0,
			BlockSize: 10,
			BlockID:   0,
			Payload:   base64.StdEncoding.EncodeToString([]byte("data")),
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		assertRejected(t, client)
	})

	t.Run("block size too large rejected", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		dataPayload, _ := json.Marshal(DataBlockResponse{
			FileID:    0,
			BlockSize: MaxBlockSize + 1,
			BlockID:   0,
			Payload:   base64.StdEncoding.EncodeToString([]byte("data")),
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		assertRejected(t, client)
	})

	t.Run("payload larger than block size rejected", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		oversized := make([]byte, MinBlockSize+1)
		dataPayload, _ := json.Marshal(DataBlockResponse{
			FileID:    0,
			BlockSize: MinBlockSize,
			BlockID:   0,
			Payload:   base64.StdEncoding.EncodeToString(oversized),
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		assertRejected(t, client)
	})
}

func TestHandleUploadDataBlockSizeValidation(t *testing.T) {
	setupUploadHandler := func(t *testing.T, fileSize int) *Handler {
		t.Helper()
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		createPayload, _ := json.Marshal(CreateRequest{
			Files: []FileInfo{{FileID: 0, Size: fileSize}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: createPayload,
		})
		return h
	}

	sendBlock := func(h *Handler, blockID, blockSize, dataLen int) *mockServerClient {
		client := newMockServerClient("dev1", "ns1")
		dataPayload, _ := json.Marshal(DataBlockResponse{
			ClientToken: "tok",
			FileID:      0,
			BlockSize:   blockSize,
			BlockID:     blockID,
			Payload:     base64.StdEncoding.EncodeToString(make([]byte, dataLen)),
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})
		return client
	}

	t.Run("undersized non-final block rejected", func(t *testing.T) {
		h := setupUploadHandler(t, 512) // 2 blocks of 256
		client := sendBlock(h, 0, 256, 100)
		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "rejected/json")
	})

	t.Run("correctly sized final block accepted", func(t *testing.T) {
		h := setupUploadHandler(t, 300) // block 0=256, block 1=44
		client := sendBlock(h, 1, 256, 44)
		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "accepted/json")
	})

	t.Run("wrongly sized final block rejected", func(t *testing.T) {
		h := setupUploadHandler(t, 300) // block 1 should be 44 bytes
		client := sendBlock(h, 1, 256, 50)
		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "rejected/json")
	})

	t.Run("full-sized non-final block accepted", func(t *testing.T) {
		h := setupUploadHandler(t, 512) // 2 full blocks of 256
		client := sendBlock(h, 0, 256, 256)
		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "accepted/json")
	})

	t.Run("undersized block with wrong file ID", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store), WithWriteStore(store))
		client := newMockServerClient("dev1", "ns1")

		// Create stream with file ID 0 only
		createPayload, _ := json.Marshal(CreateRequest{
			Files: []FileInfo{{FileID: 0, Size: 512}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: createPayload,
		})

		// Send undersized block for non-existent file ID 5
		client2 := newMockServerClient("dev1", "ns1")
		dataPayload, _ := json.Marshal(DataBlockResponse{
			ClientToken: "tok",
			FileID:      5,
			BlockSize:   256,
			BlockID:     0,
			Payload:     base64.StdEncoding.EncodeToString(make([]byte, 100)),
		})
		h.HandleMessage(client2, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		msg := client2.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "rejected/json")

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
		assert.Equal(t, "tok", errResp.ClientToken)
	})

	t.Run("WriteBlock plain error falls back to InvalidRequest", func(t *testing.T) {
		h := NewHandler(WithWriteStore(&failingWriteStore{}))
		client := newMockServerClient("dev1", "ns1")

		// Full-sized block goes straight to WriteBlock
		dataPayload, _ := json.Marshal(DataBlockResponse{
			ClientToken: "tok",
			FileID:      0,
			BlockSize:   256,
			BlockID:     0,
			Payload:     base64.StdEncoding.EncodeToString(make([]byte, 256)),
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "rejected/json")

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "write store error")
	})

	t.Run("separate read and write stores", func(t *testing.T) {
		readStore := NewMemoryStore()
		writeStore := NewMemoryStore()
		h := NewHandler(WithStore(readStore), WithWriteStore(writeStore))
		client := newMockServerClient("dev1", "ns1")

		// Create stream in writeStore only (readStore has no knowledge of it)
		createPayload, _ := json.Marshal(CreateRequest{
			Files: []FileInfo{{FileID: 0, Size: 300}},
		})
		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: createPayload,
		})

		// Send correctly sized final block (44 bytes) - should succeed via writeStore lookup
		client2 := newMockServerClient("dev1", "ns1")
		dataPayload, _ := json.Marshal(DataBlockResponse{
			ClientToken: "tok",
			FileID:      0,
			BlockSize:   256,
			BlockID:     1,
			Payload:     base64.StdEncoding.EncodeToString(make([]byte, 44)),
		})
		h.HandleMessage(client2, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/data/json",
			Payload: dataPayload,
		})

		msg := client2.lastSent()
		require.NotNil(t, msg)
		assert.Contains(t, msg.Topic, "accepted/json")
	})
}

func TestHandleGetStreamEdgeCases(t *testing.T) {
	t.Run("store error on GetStream", func(t *testing.T) {
		h := NewHandler(WithStore(&errorStore{}))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: 0, BlockSize: 256})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("store ReadBlock error sends rejection", func(t *testing.T) {
		h := NewHandler(WithStore(&readBlockErrorStore{
			doc: &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 2048}}},
		}))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: 0, BlockSize: 256, NumberOfBlocks: 4})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msgs := client.allSent()
		require.Len(t, msgs, 1)
		assert.Contains(t, msgs[0].Topic, "rejected/json")

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &errResp))
		assert.Equal(t, ErrResourceNotFound, errResp.Code)
	})

	t.Run("first block out of bounds sends rejection", func(t *testing.T) {
		h := NewHandler(WithStore(&eofReadBlockStore{
			doc: &StreamDocument{Version: 1, Files: []FileInfo{{FileID: 0, Size: 2048}}},
		}))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: 0, BlockSize: 256, NumberOfBlocks: 4})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msgs := client.allSent()
		require.Len(t, msgs, 1)
		assert.Contains(t, msgs[0].Topic, "rejected/json")

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &errResp))
		assert.Equal(t, ErrOffsetOutOfBounds, errResp.Code)
	})

	t.Run("EOF after some blocks sent is silent", func(t *testing.T) {
		store := NewMemoryStore()
		key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "fw-update"}
		// File is 300 bytes: block 0 (256 bytes) succeeds, block 1 requests at offset 256
		// which is within range (300 bytes), but requesting 4 blocks will hit EOF at block 2
		store.PutStream(key, &StreamDocument{
			Version: 1,
			Files:   []FileInfo{{FileID: 0, Size: 300}},
		})
		store.PutFile(key, 0, make([]byte, 300))

		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{FileID: 0, BlockSize: 256, NumberOfBlocks: 4})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msgs := client.allSent()
		// Should get 2 data blocks (0 and 1), no rejection for EOF after them
		assert.Len(t, msgs, 2)
		for _, msg := range msgs {
			assert.Contains(t, msg.Topic, "data/json")
		}
	})

	t.Run("number of blocks exceeds max", func(t *testing.T) {
		store, _ := setupTestStore()
		h := NewHandler(WithStore(store))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(GetRequest{
			FileID:         0,
			BlockSize:      256,
			NumberOfBlocks: MaxNumberBlocks + 1,
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/fw-update/get/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "number of blocks")
	})
}

func TestHandleCreateNonErrorResponseFromStore(t *testing.T) {
	t.Run("non-ErrorResponse error from CreateStream", func(t *testing.T) {
		h := NewHandler(WithWriteStore(&failingWriteStore{}))
		client := newMockServerClient("dev1", "ns1")

		payload, _ := json.Marshal(CreateRequest{
			Files: []FileInfo{{FileID: 0, Size: 100}},
		})

		h.HandleMessage(client, &mqttv5.Message{
			Topic:   "$things/dev1/streams/sensor/create/json",
			Payload: payload,
		})

		msg := client.lastSent()
		require.NotNil(t, msg)

		var errResp ErrorResponse
		require.NoError(t, json.Unmarshal(msg.Payload, &errResp))
		assert.Equal(t, ErrInvalidRequest, errResp.Code)
		assert.Contains(t, errResp.Message, "write store error")
	})
}

// readBlockErrorStore returns a valid stream but errors on ReadBlock.
type readBlockErrorStore struct {
	doc *StreamDocument
}

func (r *readBlockErrorStore) GetStream(_ StreamKey) (*StreamDocument, error) {
	return r.doc, nil
}

func (r *readBlockErrorStore) ReadBlock(_ StreamKey, _ int, _ int64, _ int) ([]byte, error) {
	return nil, errors.New("read error")
}

// eofReadBlockStore returns ErrOffsetOutOfBounds to simulate EOF.
type eofReadBlockStore struct {
	doc *StreamDocument
}

func (e *eofReadBlockStore) GetStream(_ StreamKey) (*StreamDocument, error) {
	return e.doc, nil
}

func (e *eofReadBlockStore) ReadBlock(_ StreamKey, _ int, _ int64, _ int) ([]byte, error) {
	return nil, &ErrorResponse{Code: ErrOffsetOutOfBounds, Message: "offset beyond file size"}
}

// failingWriteStore always returns a plain error from CreateStream.
type failingWriteStore struct{}

func (f *failingWriteStore) GetStream(_ StreamKey) (*StreamDocument, error) {
	return nil, errors.New("write store error")
}

func (f *failingWriteStore) CreateStream(_ StreamKey, _ *StreamDocument) error {
	return errors.New("write store error")
}

func (f *failingWriteStore) WriteBlock(_ StreamKey, _ int, _ int64, _ []byte) error {
	return errors.New("write store error")
}

func TestNewHandler(t *testing.T) {
	t.Run("default store", func(t *testing.T) {
		h := NewHandler()
		assert.NotNil(t, h.store)
	})

	t.Run("custom store", func(t *testing.T) {
		store := NewMemoryStore()
		h := NewHandler(WithStore(store))
		assert.Equal(t, store, h.store)
	})
}
