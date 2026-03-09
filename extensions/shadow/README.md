# Shadow Extension

Device Shadow for MQTT v5.

Shadows maintain a virtual representation of a device's state using **desired** and **reported** states, automatically computing **deltas** when they differ.

## Topic Structure

### Classic (unnamed) shadow

| Topic | Direction | Description |
|-------|-----------|-------------|
| `$things/{clientID}/shadow/update` | Client -> Server | Publish desired/reported state |
| `$things/{clientID}/shadow/update/accepted` | Server -> Client | Update succeeded, full document |
| `$things/{clientID}/shadow/update/rejected` | Server -> Client | Update failed, error response |
| `$things/{clientID}/shadow/update/delta` | Server -> Client | Desired differs from reported |
| `$things/{clientID}/shadow/update/documents` | Server -> Client | Full document after change |
| `$things/{clientID}/shadow/get` | Client -> Server | Request current shadow |
| `$things/{clientID}/shadow/get/accepted` | Server -> Client | Current shadow document |
| `$things/{clientID}/shadow/get/rejected` | Server -> Client | Shadow not found |
| `$things/{clientID}/shadow/delete` | Client -> Server | Delete shadow |
| `$things/{clientID}/shadow/delete/accepted` | Server -> Client | Delete succeeded |
| `$things/{clientID}/shadow/delete/rejected` | Server -> Client | Shadow not found |
| `$things/{clientID}/shadow/list` | Client -> Server | List named shadows |
| `$things/{clientID}/shadow/list/accepted` | Server -> Client | List result with shadow names |
| `$things/{clientID}/shadow/list/rejected` | Server -> Client | List failed |

### Named shadow

| Topic | Direction | Description |
|-------|-----------|-------------|
| `$things/{clientID}/shadow/name/{shadowName}/update` | Client -> Server | Publish desired/reported state |
| `$things/{clientID}/shadow/name/{shadowName}/update/accepted` | Server -> Client | Update succeeded, full document |
| `$things/{clientID}/shadow/name/{shadowName}/update/rejected` | Server -> Client | Update failed, error response |
| `$things/{clientID}/shadow/name/{shadowName}/update/delta` | Server -> Client | Desired differs from reported |
| `$things/{clientID}/shadow/name/{shadowName}/update/documents` | Server -> Client | Full document after change |
| `$things/{clientID}/shadow/name/{shadowName}/get` | Client -> Server | Request current shadow |
| `$things/{clientID}/shadow/name/{shadowName}/get/accepted` | Server -> Client | Current shadow document |
| `$things/{clientID}/shadow/name/{shadowName}/get/rejected` | Server -> Client | Shadow not found |
| `$things/{clientID}/shadow/name/{shadowName}/delete` | Client -> Server | Delete shadow |
| `$things/{clientID}/shadow/name/{shadowName}/delete/accepted` | Server -> Client | Delete succeeded |
| `$things/{clientID}/shadow/name/{shadowName}/delete/rejected` | Server -> Client | Shadow not found |

### Shared classic shadow

| Topic | Direction | Description |
|-------|-----------|-------------|
| `$things/$shared/{groupName}/shadow/update` | Client -> Server | Publish desired/reported state |
| `$things/$shared/{groupName}/shadow/update/accepted` | Server -> Client | Update succeeded, full document |
| `$things/$shared/{groupName}/shadow/update/rejected` | Server -> Client | Update failed, error response |
| `$things/$shared/{groupName}/shadow/update/delta` | Server -> Client | Desired differs from reported |
| `$things/$shared/{groupName}/shadow/update/documents` | Server -> Client | Full document after change |
| `$things/$shared/{groupName}/shadow/get` | Client -> Server | Request current shadow |
| `$things/$shared/{groupName}/shadow/get/accepted` | Server -> Client | Current shadow document |
| `$things/$shared/{groupName}/shadow/get/rejected` | Server -> Client | Shadow not found |
| `$things/$shared/{groupName}/shadow/delete` | Client -> Server | Delete shadow |
| `$things/$shared/{groupName}/shadow/delete/accepted` | Server -> Client | Delete succeeded |
| `$things/$shared/{groupName}/shadow/delete/rejected` | Server -> Client | Shadow not found |

### Shared named shadow

| Topic | Direction | Description |
|-------|-----------|-------------|
| `$things/$shared/{groupName}/shadow/name/{shadowName}/update` | Client -> Server | Publish desired/reported state |
| `$things/$shared/{groupName}/shadow/name/{shadowName}/update/accepted` | Server -> Client | Update succeeded, full document |
| `$things/$shared/{groupName}/shadow/name/{shadowName}/update/rejected` | Server -> Client | Update failed, error response |
| `$things/$shared/{groupName}/shadow/name/{shadowName}/update/delta` | Server -> Client | Desired differs from reported |
| `$things/$shared/{groupName}/shadow/name/{shadowName}/update/documents` | Server -> Client | Full document after change |
| `$things/$shared/{groupName}/shadow/name/{shadowName}/get` | Client -> Server | Request current shadow |
| `$things/$shared/{groupName}/shadow/name/{shadowName}/get/accepted` | Server -> Client | Current shadow document |
| `$things/$shared/{groupName}/shadow/name/{shadowName}/get/rejected` | Server -> Client | Shadow not found |
| `$things/$shared/{groupName}/shadow/name/{shadowName}/delete` | Client -> Server | Delete shadow |
| `$things/$shared/{groupName}/shadow/name/{shadowName}/delete/accepted` | Server -> Client | Delete succeeded |
| `$things/$shared/{groupName}/shadow/name/{shadowName}/delete/rejected` | Server -> Client | Shadow not found |

## Shadow Document

```json
{
  "state": {
    "desired": { "temp": 22 },
    "reported": { "temp": 20 },
    "delta": { "temp": 22 }
  },
  "metadata": {
    "desired": { "temp": { "timestamp": 1234567890 } },
    "reported": { "temp": { "timestamp": 1234567890 } },
    "connection": { "connected": true, "connectionTimestamp": 1234567890 }
  },
  "version": 42,
  "timestamp": 1234567890
}
```

## State Behavior

### Desired, Reported, and Delta

The shadow tracks two independent states:

- **desired** - the target state set by the application or cloud
- **reported** - the actual state reported by the device
- **delta** - automatically computed: keys from desired that differ from reported

Delta is recomputed on every update. It contains only keys present in desired whose values do not match reported.

### Update Merge Semantics

Updates use merge semantics: each key in the update payload is merged into the existing shadow document.

- Keys set to `null` are deleted from the document.
- Keys not present in the update are left unchanged.
- Setting an entire state section to `null` (e.g., `"desired": null`) clears that section.

Example: if the current reported state is `{"temp": 22, "mode": "auto"}` and an update sends `{"reported": {"temp": 25}}`, the resulting reported state is `{"temp": 25, "mode": "auto"}`. Sending `{"reported": {"mode": null}}` removes the `mode` key, resulting in `{"temp": 25}`.

### Example: flat state

**Step 1.** Application sets desired temperature:

```json
// Publish to .../shadow/update
{ "state": { "desired": { "temp": 22, "mode": "cool" } } }
```

Shadow after update:

```json
{
  "state": {
    "desired": { "temp": 22, "mode": "cool" },
    "reported": null,
    "delta": { "temp": 22, "mode": "cool" }
  },
  "version": 1
}
```

Delta contains all desired keys because reported is empty.

**Step 2.** Device reports current state:

```json
// Publish to .../shadow/update
{ "state": { "reported": { "temp": 20, "mode": "cool" } } }
```

Shadow after update:

```json
{
  "state": {
    "desired": { "temp": 22, "mode": "cool" },
    "reported": { "temp": 20, "mode": "cool" },
    "delta": { "temp": 22 }
  },
  "version": 2
}
```

`mode` matches, so only `temp` remains in delta. The delta is published to `.../update/delta`.

**Step 3.** Device reaches target temperature:

```json
{ "state": { "reported": { "temp": 22 } } }
```

Shadow after update:

```json
{
  "state": {
    "desired": { "temp": 22, "mode": "cool" },
    "reported": { "temp": 22, "mode": "cool" },
    "delta": null
  },
  "version": 3
}
```

All desired keys match reported. Delta is empty, no `.../update/delta` message is published.

### Example: nested state

Nested objects are diffed recursively. Only differing leaf keys appear in the delta, with their full path preserved.

**Step 1.** Set desired with nested config:

```json
{
  "state": {
    "desired": {
      "network": { "dns": "8.8.8.8", "gateway": "10.0.0.1" },
      "display": { "brightness": 80, "theme": "dark" }
    }
  }
}
```

**Step 2.** Device reports partial match:

```json
{
  "state": {
    "reported": {
      "network": { "dns": "8.8.8.8", "gateway": "10.0.0.1" },
      "display": { "brightness": 50, "theme": "dark" }
    }
  }
}
```

Resulting delta:

```json
{
  "delta": {
    "display": { "brightness": 80 }
  }
}
```

`network` matches entirely, so it is absent from delta. Inside `display`, only `brightness` differs -- `theme` matches and is excluded.

**Step 3.** Device updates brightness:

```json
{ "state": { "reported": { "display": { "brightness": 80 } } } }
```

Delta becomes empty. No delta message is published.

### Example: deeply nested state

```json
{
  "state": {
    "desired":  { "lights": { "color": { "r": 255, "g": 255, "b": 255 } } },
    "reported": { "lights": { "color": { "r": 255, "g": 0,   "b": 255 } } }
  }
}
```

Delta:

```json
{ "lights": { "color": { "g": 255 } } }
```

Only the differing leaf (`g`) appears. Matching siblings (`r`, `b`) are excluded.

### Null value deletion

Setting a key to `null` removes it from the shadow document:

```json
{ "state": { "desired": { "mode": null } } }
```

This removes `mode` from desired state and its metadata. If all keys are removed, the entire section becomes `null`. Removing a desired key also removes it from delta.

### Delta clearing

Delta is automatically recomputed on every update. When a device reports values matching the desired state, those keys are removed from delta:

- Reporting a value equal to the desired value removes that key from delta.
- When all desired keys match reported, delta becomes `null` and no `.../update/delta` message is published.
- Partial matches remove only the matching keys from delta; remaining differences persist.

### Accepted response

The `update/accepted` response contains only the fields from the request, not the full shadow document. This allows the client to correlate which keys were accepted without downloading the entire document.

For example, if the shadow has `{"desired": {"temp": 22, "mode": "cool"}}` and an update sends `{"desired": {"temp": 25}}`, the accepted response contains `{"state": {"desired": {"temp": 25}}}` without `mode`.

### Key rules

- Delta only contains keys present in **desired**. Keys only in reported are ignored.
- Nested maps are diffed recursively. Only differing leaf keys appear in delta with their full path.
- Arrays are compared atomically (as a whole, not element-by-element).
- If desired has a map but reported has a scalar (or vice versa) for the same key, the entire desired value appears in delta.
- `null` delta means desired and reported are in sync. No `.../update/delta` message is published.
- Setting a key to `null` removes it from the document. If all keys are removed, the section becomes `null`.
- Each update increments the document version.

### Key restrictions

- Key names must not start with the reserved prefix `$`.
- Key names must not exceed `MaxKeyLength` (default: 128 bytes).
- Total number of keys across all nesting levels must not exceed `MaxTotalKeys` (default: 500) per state section.
- Shadow names must match `[a-zA-Z0-9:_-]` and must not exceed 64 bytes.

## Operations

### Get with clientToken

The `get` operation supports an optional `clientToken` in the payload for request-response correlation:

```json
// Publish to .../shadow/get
{ "clientToken": "req-123" }
```

The `clientToken` is echoed back in the `get/accepted` or `get/rejected` response.
An empty payload (or no payload) is also valid and works as a simple get.

### Delete with clientToken

The `delete` operation supports an optional `clientToken` in the payload:

```json
// Publish to .../shadow/delete
{ "clientToken": "del-456" }
```

The `clientToken` is echoed back in the `delete/accepted` or `delete/rejected` response.
An empty payload (or no payload) is also valid.

### List Named Shadows

The `list` operation returns the names of all named shadows for a thing.
Requires the `WithNamedShadow()` option to be enabled.

```json
// Publish to $things/{clientID}/shadow/list
{ "pageSize": 10, "nextToken": "", "clientToken": "list-789" }
```

Response on `list/accepted`:

```json
{
  "results": ["config", "firmware"],
  "timestamp": 1234567890,
  "nextToken": "",
  "clientToken": "list-789"
}
```

All fields in the request payload are optional. Default page size is 25.

### Reconnect Delta Delivery

When a device reconnects, it may have missed delta updates while offline. Use `PublishDelta` to push the current pending delta to the device:

```go
// In your OnConnect handler
func onConnect(c *mqttv5.ServerClient) {
    key := shadow.Key{
        Namespace: c.Namespace(),
        ClientID:  c.ClientID(),
    }

    sh.SetConnected(key, true)
    sh.PublishDelta(key) // publishes to .../update/delta if delta exists
}
```

`PublishDelta` is a no-op if the shadow does not exist, has no pending delta, or no `PublishFunc` is configured.

### Connection Metadata

Track device connection state using `SetConnected`:

```go
// When device connects
sh.SetConnected(shadow.Key{Namespace: ns, ClientID: clientID}, true)

// When device disconnects
sh.SetConnected(shadow.Key{Namespace: ns, ClientID: clientID}, false)
```

Connection metadata appears in the shadow document under `metadata.connection`:

```json
{
  "metadata": {
    "connection": {
      "connected": true,
      "connectionTimestamp": 1234567890
    }
  }
}
```

## Server-Side Usage

### MQTT only

```go
sh := shadow.NewHandler(
    shadow.WithClassicShadow(),
    shadow.WithNamedShadow(),
)
srv := mqttv5.NewServer(
    mqttv5.WithListener(listener),
    mqttv5.OnMessage(func(c *mqttv5.ServerClient, msg *mqttv5.Message) {
        if sh.HandleMessage(c, msg) {
            return
        }
        // normal routing...
    }),
)
```

### REST API pushing config to devices

```go
sh := shadow.NewHandler(
    shadow.WithClassicShadow(),
    shadow.WithPublishFunc(srv.Publish),
)

func handleSetConfig(w http.ResponseWriter, r *http.Request) {
    clientID := r.PathValue("clientID")

    var config map[string]any
    json.NewDecoder(r.Body).Decode(&config)

    doc, err := sh.Update(shadow.Key{
        Namespace: mqttv5.DefaultNamespace,
        ClientID: clientID,
    }, shadow.UpdateRequest{
        State: shadow.UpdateState{Desired: config},
    })
    // doc contains the full shadow with computed delta
    // delta is auto-published to $things/{clientID}/shadow/update/delta
}
```

### Shared shadow (server-side)

```go
sh := shadow.NewHandler(
    shadow.WithSharedShadow(func(clientID, groupName string) bool {
        // Check if clientID is allowed to access groupName
        return db.IsMemberOf(clientID, groupName)
    }),
    shadow.WithPublishFunc(srv.Publish),
)
```

### Handler Options

| Option | Description |
|--------|-------------|
| `WithStore(store)` | Custom shadow store (default: `NewMemoryStore()`) |
| `WithClientIDResolver(fn)` | ACL identity resolver used by `CheckAccess` to map a connecting client's ID to its shadow identity (default: uses client ID as-is) |
| `WithPublishFunc(fn)` | Required for `Update`/`Delete` to publish notifications |
| `WithClassicShadow()` | Enable classic (unnamed) shadows (disabled by default) |
| `WithNamedShadow()` | Enable named shadows (disabled by default) |
| `WithSharedShadow(resolver)` | Enable shared shadows with access control resolver |
| `WithLimits(limits)` | Custom validation limits (see Limits section) |
| `WithDeletedVersionTTL(ttl)` | TTL for deleted version tracking (default: 48 hours) |

### Direct API Methods

The handler provides direct methods for use from REST APIs or other systems:

| Method | Description |
|--------|-------------|
| `Get(key)` | Retrieve current shadow document |
| `Update(key, req)` | Update shadow, publish notifications |
| `Delete(key)` | Remove shadow, return last document (tracks deleted version for inheritance) |
| `SetConnected(key, connected)` | Update connection metadata |
| `List(key, pageSize, nextToken)` | List named shadows (requires Lister store) |
| `PublishDelta(key)` | Publish pending delta to subscribers (for device reconnect) |

### REST API Examples

Get shadow:

```go
// GET /api/v1/things/{thingName}/shadow?name={shadowName}
func handleGetShadow(w http.ResponseWriter, r *http.Request) {
    key := shadow.Key{
        Namespace:  mqttv5.DefaultNamespace,
        ClientID:   r.PathValue("thingName"),
        ShadowName: r.URL.Query().Get("name"),
    }

    doc, err := sh.Get(key)
    if err != nil {
        var errResp *shadow.ErrorResponse
        if errors.As(err, &errResp) {
            http.Error(w, errResp.Message, errResp.Code)
            return
        }
        http.Error(w, "internal error", http.StatusInternalServerError)
        return
    }

    json.NewEncoder(w).Encode(doc)
}
```

Update shadow:

```go
// POST /api/v1/things/{thingName}/shadow?name={shadowName}
func handleUpdateShadow(w http.ResponseWriter, r *http.Request) {
    key := shadow.Key{
        Namespace:  mqttv5.DefaultNamespace,
        ClientID:   r.PathValue("thingName"),
        ShadowName: r.URL.Query().Get("name"),
    }

    var req shadow.UpdateRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "invalid payload", http.StatusBadRequest)
        return
    }

    doc, err := sh.Update(key, req)
    if err != nil {
        var errResp *shadow.ErrorResponse
        if errors.As(err, &errResp) {
            http.Error(w, errResp.Message, errResp.Code)
            return
        }
        http.Error(w, "internal error", http.StatusInternalServerError)
        return
    }

    json.NewEncoder(w).Encode(doc)
}
```

Delete shadow:

```go
// DELETE /api/v1/things/{thingName}/shadow?name={shadowName}
func handleDeleteShadow(w http.ResponseWriter, r *http.Request) {
    key := shadow.Key{
        Namespace:  mqttv5.DefaultNamespace,
        ClientID:   r.PathValue("thingName"),
        ShadowName: r.URL.Query().Get("name"),
    }

    doc, err := sh.Delete(key)
    if err != nil {
        var errResp *shadow.ErrorResponse
        if errors.As(err, &errResp) {
            http.Error(w, errResp.Message, errResp.Code)
            return
        }
        http.Error(w, "internal error", http.StatusInternalServerError)
        return
    }

    json.NewEncoder(w).Encode(doc)
}
```

List named shadows:

```go
// GET /api/v1/things/{thingName}/shadows
func handleListShadows(w http.ResponseWriter, r *http.Request) {
    key := shadow.Key{
        Namespace: mqttv5.DefaultNamespace,
        ClientID:  r.PathValue("thingName"),
    }

    pageSize := 25
    nextToken := r.URL.Query().Get("nextToken")

    result, err := sh.List(key, pageSize, nextToken)
    if err != nil {
        var errResp *shadow.ErrorResponse
        if errors.As(err, &errResp) {
            http.Error(w, errResp.Message, errResp.Code)
            return
        }
        http.Error(w, "internal error", http.StatusInternalServerError)
        return
    }

    json.NewEncoder(w).Encode(result)
}
```

## Client-Side Usage

A single `Client` instance handles both classic and named shadows via the `Shadow` method:

```go
sc := shadow.NewClient(client)

// Subscribe to all response topics
sc.SubscribeAll(shadow.Handlers{
    // Update notifications
    Accepted:  onUpdateAccepted,
    Rejected:  onUpdateRejected,
    Delta:     onDelta,
    Documents: onDocuments,
    // Get responses
    GetAccepted: onGetAccepted,
    GetRejected: onGetRejected,
    // Delete responses
    DeleteAccepted: onDeleteAccepted,
    DeleteRejected: onDeleteRejected,
    // List responses
    ListAccepted: onListAccepted,
    ListRejected: onListRejected,
})

// Classic (unnamed) shadow operations
sc.Get()                            // request shadow
sc.GetWithToken("req-1")            // request shadow with clientToken
sc.Update(shadow.UpdateState{       // publish state
    Reported: map[string]any{"temp": 22},
})
sc.Delete()                         // delete shadow
sc.DeleteWithToken("del-1")         // delete with clientToken
sc.ListShadows()                    // list named shadows

// Named shadow operations
config := sc.Shadow("config")
config.Get()
config.GetWithToken("req-2")
config.Update(shadow.UpdateState{
    Desired: map[string]any{"mode": "auto"},
})
config.Delete()
config.DeleteWithToken("del-2")

// Unsubscribe from all response topics
sc.Unsubscribe()
```

### Shared shadow (client-side)

```go
gc := shadow.NewGroupClient(client, "room-101")

// Classic shared shadow
gc.SubscribeAll(shadow.Handlers{
    Delta:          onDelta,
    GetAccepted:    onGetAccepted,
    DeleteAccepted: onDeleteAccepted,
})
gc.Get()
gc.GetWithToken("req-1")
gc.Update(shadow.UpdateState{
    Reported: map[string]any{"temp": 22.0},
})
gc.Delete()
gc.DeleteWithToken("del-1")

// Named shared shadow
config := gc.Shadow("config")
config.Get()
config.Delete()
config.DeleteWithToken("del-2")
```

### Typed Client (request/response with context)

`TypedClient` provides blocking, context-aware methods that handle request/response
correlation, response subscriptions, and JSON parsing internally.

```go
tc := shadow.NewTypedClient(client)

// Blocking get with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

doc, err := tc.GetShadow(ctx)
if err != nil {
    var shadowErr *shadow.ErrorResponse
    if errors.As(err, &shadowErr) {
        log.Printf("shadow error: %d %s", shadowErr.Code, shadowErr.Message)
    }
    // err may also be context.DeadlineExceeded or context.Canceled
}

// Blocking update
doc, err = tc.UpdateShadow(ctx, shadow.UpdateState{
    Reported: map[string]any{"temp": 22},
})

// Blocking delete
err = tc.DeleteShadow(ctx)

// List named shadows
result, err := tc.ListNamedShadows(ctx)

// Named shadow
config := tc.Shadow("config")
doc, err = config.GetShadow(ctx)
```

Subscribe to typed push notifications (delta and documents):

```go
tc.SubscribeEvents(shadow.TypedHandlers{
    OnDelta: func(state map[string]any, version int64) {
        log.Printf("delta v%d: %v", version, state)
    },
    OnDocuments: func(prev, current shadow.DocumentSnapshot) {
        log.Printf("document changed: v%d -> v%d", prev.Version, current.Version)
    },
})
```

Close unsubscribes all managed topics and cancels pending requests:

```go
tc.Close()
```

### Client Options

- `WithClientID(name)` - Override client ID (default: `client.ClientID()`)

## Limits

| Limit | Default | Description |
|-------|---------|-------------|
| `MaxPayloadSize` | 131072 (128 KB) | Maximum update payload size in bytes |
| `MaxDepth` | 6 | Maximum nesting depth for state objects |
| `MaxDocumentSize` | 8192 (8 KB) | Maximum document size after update |
| `MaxClientTokenLength` | 64 | Maximum clientToken string length |
| `MaxNamedShadows` | 25 | Maximum named shadows per thing (0 = unlimited) |
| `MaxInflightPerClient` | 10 | Maximum concurrent requests per client (0 = unlimited) |
| `MaxTotalKeys` | 500 | Maximum total keys per state section (0 = unlimited) |
| `MaxKeysPerObject` | 0 (unlimited) | Maximum keys per JSON object in state |
| `MaxKeyLength` | 128 | Maximum key name length |
| `MaxValueLength` | 0 (unlimited) | Maximum string value length |

Shadow names must match `[a-zA-Z0-9:_-]` and are limited to 64 bytes.

## Custom Store

Implement `Store` for persistent storage. Optionally implement `Watcher`
for change notification support (e.g., Redis keyspace notifications, NATS KV watches).

```go
type Store interface {
    Get(key Key) (*Document, error)
    Save(key Key, doc *Document) error
    Delete(key Key) (*Document, error)
}

type Watcher interface {
    Store
    Watch(onChange func(key Key, doc *Document)) (stop func(), err error)
}
```

Optional interfaces for extended functionality:

```go
// Counter enables MaxNamedShadows enforcement.
type Counter interface {
    CountNamedShadows(key Key) (int, error)
}

// Lister enables the list operation.
type Lister interface {
    ListNamedShadows(key Key, pageSize int, nextToken string) (*ListResult, error)
}
```

The built-in `MemoryStore` implements `Store`, `Counter`, and `Lister` (but not `Watcher`).

If the store implements `Watcher`, the `Handler` automatically starts watching
during `NewHandler()` and publishes delta notifications via `PublishFunc` when external
changes are detected. Call `Handler.Close()` to stop watching.

## Authorization (ACL)

`CheckAccess` is a standalone helper for enforcing shadow topic ACL inside your own authorizer.
It checks own-access (clientID must match) and direction-based rules (publish vs subscribe suffixes).

```go
func (a *MyAuthz) Authorize(ctx context.Context, c *mqttv5.AuthzContext) (*mqttv5.AuthzResult, error) {
    if result := a.shadowHandler.CheckAccess(c); result != nil {
        return result, nil
    }

    // other ACL checks...
    return &mqttv5.AuthzResult{Allowed: true}, nil
}
```

Rules enforced:

| Action | Allowed suffixes |
|--------|-----------------|
| Publish | `get`, `update`, `delete`, `list` |
| Subscribe | `get/accepted`, `get/rejected`, `update/accepted`, `update/rejected`, `update/delta`, `update/documents`, `delete/accepted`, `delete/rejected`, `list/accepted`, `list/rejected` |

A client can only access topics containing its own client ID. Cross-client access is denied.
Shared shadow access is controlled by the `SharedAccessResolver`. With a nil resolver, shared shadows are read-only.
All allowed operations are capped at QoS 1.

## Error Codes

| Code | Meaning |
|------|---------|
| 400 | Invalid JSON, payload, or request |
| 401 | Unauthorized |
| 403 | Forbidden (shared shadow access denied) |
| 404 | Shadow not found |
| 409 | Version conflict |
| 413 | Payload or document too large |
| 415 | Unsupported encoding (not UTF-8) |
| 429 | Too many requests (inflight limit exceeded) |
| 500 | Internal error |
