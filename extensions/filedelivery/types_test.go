package filedelivery

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorResponse(t *testing.T) {
	t.Run("implements error", func(t *testing.T) {
		err := &ErrorResponse{Code: ErrResourceNotFound, Message: "stream not found"}
		assert.Implements(t, (*error)(nil), err)
		assert.Equal(t, "filedelivery: ResourceNotFound stream not found", err.Error())
	})

	t.Run("error codes", func(t *testing.T) {
		assert.Equal(t, "InvalidRequest", ErrInvalidRequest)
		assert.Equal(t, "BlockSizeOutOfBounds", ErrBlockSizeOutOfBounds)
		assert.Equal(t, "OffsetOutOfBounds", ErrOffsetOutOfBounds)
		assert.Equal(t, "VersionMismatch", ErrVersionMismatch)
		assert.Equal(t, "ResourceNotFound", ErrResourceNotFound)
		assert.Equal(t, "ChecksumMismatch", ErrChecksumMismatch)
		assert.Equal(t, "Unauthorized", ErrUnauthorized)
	})
}

func TestValidationConstants(t *testing.T) {
	t.Run("block size bounds", func(t *testing.T) {
		assert.Equal(t, 256, MinBlockSize)
		assert.Equal(t, 131072, MaxBlockSize)
	})

	t.Run("file ID max", func(t *testing.T) {
		assert.Equal(t, 255, MaxFileID)
	})

	t.Run("client token max", func(t *testing.T) {
		assert.Equal(t, 64, MaxClientToken)
	})

	t.Run("stream ID max length", func(t *testing.T) {
		assert.Equal(t, 64, MaxStreamIDLen)
	})

	t.Run("max number blocks", func(t *testing.T) {
		assert.Equal(t, 131072, MaxNumberBlocks)
	})
}

func TestFileInfoJSON(t *testing.T) {
	t.Run("marshal without checksum", func(t *testing.T) {
		fi := FileInfo{FileID: 0, Size: 131072}
		data, err := json.Marshal(fi)
		require.NoError(t, err)
		assert.JSONEq(t, `{"f":0,"z":131072}`, string(data))
	})

	t.Run("marshal with checksum", func(t *testing.T) {
		fi := FileInfo{FileID: 0, Size: 131072, Checksum: 0xDEADBEEF}
		data, err := json.Marshal(fi)
		require.NoError(t, err)
		assert.JSONEq(t, `{"f":0,"z":131072,"h":3735928559}`, string(data))
	})

	t.Run("unmarshal without checksum", func(t *testing.T) {
		var fi FileInfo
		require.NoError(t, json.Unmarshal([]byte(`{"f":1,"z":4096}`), &fi))
		assert.Equal(t, 1, fi.FileID)
		assert.Equal(t, 4096, fi.Size)
		assert.Equal(t, uint32(0), fi.Checksum)
	})

	t.Run("unmarshal with checksum", func(t *testing.T) {
		var fi FileInfo
		require.NoError(t, json.Unmarshal([]byte(`{"f":1,"z":4096,"h":12345}`), &fi))
		assert.Equal(t, uint32(12345), fi.Checksum)
	})
}

func TestDescribeRequestJSON(t *testing.T) {
	t.Run("with token", func(t *testing.T) {
		req := DescribeRequest{ClientToken: "tok123"}
		data, err := json.Marshal(req)
		require.NoError(t, err)
		assert.JSONEq(t, `{"c":"tok123"}`, string(data))
	})

	t.Run("without token", func(t *testing.T) {
		req := DescribeRequest{}
		data, err := json.Marshal(req)
		require.NoError(t, err)
		assert.JSONEq(t, `{}`, string(data))
	})
}

func TestDescribeResponseJSON(t *testing.T) {
	t.Run("full response", func(t *testing.T) {
		resp := DescribeResponse{
			ClientToken: "tok",
			Version:     1,
			Description: "firmware v2",
			Files:       []FileInfo{{FileID: 0, Size: 131072}},
		}
		data, err := json.Marshal(resp)
		require.NoError(t, err)

		var decoded DescribeResponse
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, resp, decoded)
	})
}

func TestGetRequestJSON(t *testing.T) {
	t.Run("full request", func(t *testing.T) {
		req := GetRequest{
			ClientToken:    "tok",
			Version:        1,
			FileID:         0,
			BlockSize:      4096,
			BlockOffset:    0,
			NumberOfBlocks: 10,
			Bitmap:         "ff",
		}
		data, err := json.Marshal(req)
		require.NoError(t, err)

		var decoded GetRequest
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, req, decoded)
	})

	t.Run("minimal request", func(t *testing.T) {
		req := GetRequest{FileID: 0, BlockSize: 4096}
		data, err := json.Marshal(req)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"l":4096`)
	})
}

func TestDataBlockResponseJSON(t *testing.T) {
	t.Run("marshal", func(t *testing.T) {
		resp := DataBlockResponse{
			ClientToken: "tok",
			FileID:      0,
			BlockSize:   4096,
			BlockID:     2,
			Payload:     "SGVsbG8=",
		}
		data, err := json.Marshal(resp)
		require.NoError(t, err)

		var decoded DataBlockResponse
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, resp, decoded)
	})
}

func TestDataBlockResponseDecode(t *testing.T) {
	t.Run("valid base64", func(t *testing.T) {
		resp := DataBlockResponse{Payload: "aGVsbG8gd29ybGQ="}
		data, err := resp.Decode()
		require.NoError(t, err)
		assert.Equal(t, []byte("hello world"), data)
	})

	t.Run("empty payload", func(t *testing.T) {
		resp := DataBlockResponse{Payload: ""}
		data, err := resp.Decode()
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("invalid base64", func(t *testing.T) {
		resp := DataBlockResponse{Payload: "not-valid-base64!!!"}
		_, err := resp.Decode()
		require.Error(t, err)
	})
}

func TestStreamDocumentJSON(t *testing.T) {
	t.Run("round trip", func(t *testing.T) {
		doc := StreamDocument{
			Description: "firmware",
			Version:     3,
			Files: []FileInfo{
				{FileID: 0, Size: 131072},
				{FileID: 1, Size: 65536},
			},
		}
		data, err := json.Marshal(doc)
		require.NoError(t, err)

		var decoded StreamDocument
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, doc, decoded)
	})
}

func TestCreateRequestJSON(t *testing.T) {
	t.Run("full request", func(t *testing.T) {
		req := CreateRequest{
			ClientToken: "tok",
			Description: "sensor logs",
			Files:       []FileInfo{{FileID: 0, Size: 4096}},
		}
		data, err := json.Marshal(req)
		require.NoError(t, err)

		var decoded CreateRequest
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, req, decoded)
	})

	t.Run("minimal request", func(t *testing.T) {
		req := CreateRequest{Files: []FileInfo{{FileID: 0, Size: 100}}}
		data, err := json.Marshal(req)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"r":[`)
	})
}

func TestAcceptedResponseJSON(t *testing.T) {
	t.Run("round trip", func(t *testing.T) {
		resp := AcceptedResponse{ClientToken: "tok", FileID: 0}
		data, err := json.Marshal(resp)
		require.NoError(t, err)

		var decoded AcceptedResponse
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, resp, decoded)
	})
}

func TestStreamKey(t *testing.T) {
	t.Run("fields", func(t *testing.T) {
		key := StreamKey{Namespace: "ns1", ClientID: "dev1", StreamID: "stream-1"}
		assert.Equal(t, "ns1", key.Namespace)
		assert.Equal(t, "dev1", key.ClientID)
		assert.Equal(t, "stream-1", key.StreamID)
	})
}
