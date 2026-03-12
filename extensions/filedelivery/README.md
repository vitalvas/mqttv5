# File Delivery Extension

Bidirectional MQTT-based file delivery between devices and the server.
Devices can download files (firmware, config) and upload files (sensor data, logs).

## Topics

### Download (Server -> Device)

| Operation      | Direction        | Topic                                                    |
|----------------|------------------|----------------------------------------------------------|
| DescribeStream | Device -> Server | `$things/{clientID}/streams/{streamID}/describe/json`    |
| Description    | Server -> Device | `$things/{clientID}/streams/{streamID}/description/json` |
| GetStream      | Device -> Server | `$things/{clientID}/streams/{streamID}/get/json`         |
| Data           | Server -> Device | `$things/{clientID}/streams/{streamID}/data/json`        |
| Rejected       | Server -> Device | `$things/{clientID}/streams/{streamID}/rejected/json`    |

### Upload (Device -> Server)

| Operation     | Direction        | Topic                                                   |
|---------------|------------------|---------------------------------------------------------|
| CreateStream  | Device -> Server | `$things/{clientID}/streams/{streamID}/create/json`     |
| Data (upload) | Device -> Server | `$things/{clientID}/streams/{streamID}/data/json`       |
| Accepted      | Server -> Device | `$things/{clientID}/streams/{streamID}/accepted/json`   |
| Rejected      | Server -> Device | `$things/{clientID}/streams/{streamID}/rejected/json`   |

## Payloads

All payloads use JSON with short field names.

### DescribeStream Request

```json
{"c": "client-token"}
```

- `c` (optional): Client token for request correlation (max 64 bytes).

### DescribeStream Response

```json
{
  "c": "client-token",
  "s": 1,
  "d": "firmware v2.0",
  "r": [{"f": 0, "z": 131072}]
}
```

- `c`: Echoed client token.
- `s`: Stream version.
- `d`: Stream description.
- `r`: Array of files. Each file has `f` (file ID), `z` (file size in bytes), and optional `h` (CRC32 IEEE checksum).

### GetStream Request

```json
{
  "c": "client-token",
  "s": 1,
  "f": 0,
  "l": 4096,
  "o": 0,
  "n": 10,
  "b": "ff00"
}
```

- `c` (optional): Client token.
- `s` (optional): Expected stream version. If set and mismatched, the request is rejected.
- `f`: File ID (0-255).
- `l`: Block size in bytes (256-131072).
- `o` (optional): Block offset (default 0).
- `n` (optional): Number of blocks to retrieve (default 1). Ignored if bitmap is set.
- `b` (optional): Hex-encoded bitmap for selective block retrieval.

### CreateStream Request

```json
{
  "c": "client-token",
  "d": "sensor logs",
  "r": [{"f": 0, "z": 4096}]
}
```

- `c` (optional): Client token.
- `d` (optional): Stream description.
- `r`: Array of files with file ID, size, and CRC32 checksum.

### Accepted Response

```json
{"c": "client-token", "f": 0}
```

- `c`: Echoed client token.
- `f`: File ID.

### Data Block

One message per block (used for both download and upload):

```json
{
  "c": "client-token",
  "f": 0,
  "l": 4096,
  "i": 2,
  "p": "base64-encoded-data"
}
```

- `c`: Echoed client token.
- `f`: File ID.
- `l`: Block size.
- `i`: Block ID (sequence number).
- `p`: Base64-encoded block payload.

### Error Response

```json
{
  "o": "ResourceNotFound",
  "m": "stream not found",
  "c": "client-token"
}
```

Error codes:
- `InvalidRequest`: Malformed request or invalid parameters.
- `BlockSizeOutOfBounds`: Block size outside 256-131072 range.
- `OffsetOutOfBounds`: Block offset beyond file size.
- `VersionMismatch`: Stream version does not match requested version.
- `ResourceNotFound`: Stream or file not found.
- `ChecksumMismatch`: File CRC32 checksum does not match.
- `Unauthorized`: Client not authorized.

## Checksum

File integrity is verified using CRC32 IEEE checksums at the file level
(not per-block). The checksum is stored in the `h` field of `FileInfo`.

- **Download**: If the stream metadata includes a non-zero checksum, the client
  verifies it after reassembling all blocks. Returns `ChecksumMismatch` on failure.
- **Upload**: The client automatically computes the CRC32 checksum and includes
  it in the `CreateRequest`.
- **Provisioning**: When storing streams via `PutStream`, include the checksum
  in `FileInfo` for download verification. Zero means no verification.

## Bitmap

The bitmap field enables selective block retrieval. Each bit (MSB first)
represents a block relative to the block offset:

- `"80"` = `10000000` = request block at offset+0
- `"c0"` = `11000000` = request blocks at offset+0 and offset+1
- `"ff"` = `11111111` = request blocks at offset+0 through offset+7

## Server-Side Usage

### MQTT integration

```go
store := filedelivery.NewMemoryStore()
fh := filedelivery.NewHandler(
    filedelivery.WithStore(store),
    filedelivery.WithWriteStore(store), // enable uploads
)

srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.OnMessage(func(c *mqttv5.ServerClient, msg *mqttv5.Message) {
        if fh.HandleMessage(c, msg) {
            return
        }
        // normal routing...
    }),
)
```

To disable uploads, omit `WithWriteStore`. Create requests will be rejected.

### Authorization

Wire `CheckAccess` into your authorizer so devices can only access their own
streams and only in the correct direction:

```go
func (a *MyAuthz) Authorize(ctx context.Context, c *mqttv5.AuthzContext) (*mqttv5.AuthzResult, error) {
    if result := a.fileHandler.CheckAccess(c); result != nil {
        return result, nil
    }
    // other ACL checks...
    return &mqttv5.AuthzResult{Allowed: true}, nil
}
```

### Provisioning streams (for downloads)

Streams and files are provisioned externally. The handler only reads from the
store. Use the concrete `MemoryStore` methods to manage streams:

```go
store := filedelivery.NewMemoryStore()

firmwareData, _ := os.ReadFile("firmware-v2.0.bin")

key := filedelivery.StreamKey{
    Namespace: mqttv5.DefaultNamespace,
    ClientID:  "device-1",
    StreamID:  "firmware-v2",
}

store.PutStream(key, &filedelivery.StreamDocument{
    Description: "Firmware v2.0",
    Version:     1,
    Files: []filedelivery.FileInfo{
        {FileID: 0, Size: len(firmwareData)},
    },
})
store.PutFile(key, 0, firmwareData)
```

To remove a stream and all its files:

```go
store.DeleteStream(key)
```

### Handler Options

| Option | Description |
|--------|-------------|
| `WithStore(store)` | Read store for downloads (default: `NewMemoryStore()`) |
| `WithWriteStore(store)` | Write store for uploads (default: nil, uploads disabled) |

### Custom Store

Implement the `Store` interface for download backends:

```go
type Store interface {
    GetStream(key StreamKey) (*StreamDocument, error)
    ReadBlock(key StreamKey, fileID int, offset int64, size int) ([]byte, error)
}
```

Implement the `WriteStore` interface for upload backends:

```go
type WriteStore interface {
    CreateStream(key StreamKey, doc *StreamDocument) error
    WriteBlock(key StreamKey, fileID int, offset int64, data []byte) error
}
```

`MemoryStore` implements both interfaces.

## Client-Side Usage

### Describe a stream

```go
c := filedelivery.NewClient(mqttClient)
defer c.Close()

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

desc, err := c.Describe(ctx, "firmware-v2")
if err != nil {
    log.Fatal(err)
}
log.Printf("Stream version %d, %d files", desc.Version, len(desc.Files))
```

### Download a file

```go
data, err := c.Download(ctx, "firmware-v2", 0)
if err != nil {
    log.Fatal(err)
}
os.WriteFile("firmware.bin", data, 0644)
```

`Download` handles the full flow: describes the stream, requests all blocks,
reassembles them, and returns the complete file data.

### Upload a file

```go
sensorData, _ := os.ReadFile("sensor-log.bin")
err := c.Upload(ctx, "sensor-data", "sensor logs march", sensorData)
if err != nil {
    log.Fatal(err)
}
```

`Upload` handles the full flow: creates the stream, splits into blocks,
sends each block, and waits for acceptance.

### Client Options

| Option | Description |
|--------|-------------|
| `WithClientID(name)` | Override client ID (default: `mqttClient.ClientID()`) |
| `WithBlockSize(size)` | Block size for transfers (default: 4096) |
