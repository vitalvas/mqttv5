package shadow

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vitalvas/mqttv5"
)

// ServerClient defines the interface for server-side client interactions.
type ServerClient interface {
	ClientID() string
	Namespace() string
	Send(msg *mqttv5.Message) error
}

// PublishFunc broadcasts a message to all matching subscribers.
type PublishFunc func(msg *mqttv5.Message) error

// ClientIDResolver maps a connecting client's ID to its shadow identity for ACL checks.
type ClientIDResolver func(clientID string) string

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// WithStore sets the shadow store.
func WithStore(store Store) HandlerOption {
	return func(h *Handler) {
		h.store = store
	}
}

// WithClientIDResolver sets the ACL identity resolver used by CheckAccess
// to map a connecting client's ID to its shadow identity.
func WithClientIDResolver(resolver ClientIDResolver) HandlerOption {
	return func(h *Handler) {
		h.resolver = resolver
	}
}

// WithPublishFunc sets the function used to publish messages to subscribers.
func WithPublishFunc(fn PublishFunc) HandlerOption {
	return func(h *Handler) {
		h.publishFunc = fn
	}
}

// WithClassicShadow enables classic (unnamed) shadow support.
// Disabled by default.
func WithClassicShadow() HandlerOption {
	return func(h *Handler) {
		h.classicEnabled = true
	}
}

// WithNamedShadow enables named shadow support.
// Disabled by default.
func WithNamedShadow() HandlerOption {
	return func(h *Handler) {
		h.namedEnabled = true
	}
}

// WithSharedShadow enables shared shadow support with the given access resolver.
// The resolver determines whether a client may access a given group.
// If resolver is nil, shared shadows are read-only: only get operations are allowed.
func WithSharedShadow(resolver SharedAccessResolver) HandlerOption {
	return func(h *Handler) {
		h.sharedEnabled = true
		h.sharedResolver = resolver
	}
}

// WithLimits sets validation limits for shadow payloads.
// See Limits for default values.
func WithLimits(limits Limits) HandlerOption {
	return func(h *Handler) {
		h.limits = limits
	}
}

const defaultDeletedVersionTTL = 48 * time.Hour

// WithDeletedVersionTTL sets the TTL for tracking deleted shadow versions.
// When a shadow is recreated within this TTL, it inherits the previous version.
// Default: 48 hours.
func WithDeletedVersionTTL(ttl time.Duration) HandlerOption {
	return func(h *Handler) {
		h.deletedVersionTTL = ttl
	}
}

type deletedVersion struct {
	version   int64
	deletedAt time.Time
}

// refCountedMutex is a mutex with a reference count for cleanup.
type refCountedMutex struct {
	sync.Mutex
	refs int
}

// Handler processes shadow operations from MQTT messages.
type Handler struct {
	store             Store
	resolver          ClientIDResolver
	publishFunc       PublishFunc
	stopWatch         func()
	limits            Limits
	classicEnabled    bool
	namedEnabled      bool
	sharedEnabled     bool
	sharedResolver    SharedAccessResolver
	deletedVersionTTL time.Duration
	deletedVersions   sync.Map // shadowKey -> deletedVersion
	inflight          sync.Map // clientID -> chan struct{}
	shadowLocksMu     sync.Mutex
	shadowLocks       map[string]*refCountedMutex
}

// lockShadow acquires a per-shadow mutex for the given key, serializing
// concurrent read-modify-write operations on the same shadow.
// Returns an unlock function that the caller must defer.
func (h *Handler) lockShadow(key Key) func() {
	sk := shadowKey(key)

	h.shadowLocksMu.Lock()
	if h.shadowLocks == nil {
		h.shadowLocks = make(map[string]*refCountedMutex)
	}

	rcm, ok := h.shadowLocks[sk]
	if !ok {
		rcm = &refCountedMutex{}
		h.shadowLocks[sk] = rcm
	}

	rcm.refs++
	h.shadowLocksMu.Unlock()

	rcm.Lock()

	return func() {
		rcm.Unlock()

		h.shadowLocksMu.Lock()
		rcm.refs--
		if rcm.refs == 0 {
			delete(h.shadowLocks, sk)
		}
		h.shadowLocksMu.Unlock()
	}
}

// NewHandler creates a new shadow handler with the given options.
// Default store is MemoryStore. Default client ID resolver uses ClientID().
// If the store implements Watcher, Watch is started automatically.
func NewHandler(opts ...HandlerOption) *Handler {
	h := &Handler{
		limits:            defaultLimits(),
		deletedVersionTTL: defaultDeletedVersionTTL,
	}

	for _, opt := range opts {
		opt(h)
	}

	if h.store == nil {
		h.store = NewMemoryStore()
	}

	if watcher, ok := h.store.(Watcher); ok && h.publishFunc != nil {
		stop, err := watcher.Watch(func(key Key, doc *Document) {
			if doc == nil {
				return
			}
			h.publishNotifications(key, nil, doc, "")
		})
		if err == nil {
			h.stopWatch = stop
		}
	}

	return h
}

// HandleMessage processes shadow topics from MQTT clients.
// Returns true if the topic was a shadow topic (handled), false otherwise.
func (h *Handler) HandleMessage(client ServerClient, msg *mqttv5.Message) bool {
	parsed := parseShadowTopic(msg.Topic)
	if parsed == nil {
		return false
	}

	isShared := parsed.GroupName != ""
	isNamed := parsed.ShadowName != ""

	corrData := msg.CorrelationData

	if isShared {
		if !h.sharedEnabled {
			return false
		}

		if h.sharedResolver == nil {
			// No resolver: read-only access (get only)
			if parsed.Suffix != suffixGet {
				h.sendError(client, parsed, rejectedSuffix(parsed.Suffix), shadowErrorDetail{code: ErrCodeForbidden, message: "forbidden"}, corrData)
				return true
			}
		} else if !h.sharedResolver(client.ClientID(), parsed.GroupName) {
			h.sendError(client, parsed, rejectedSuffix(parsed.Suffix), shadowErrorDetail{code: ErrCodeForbidden, message: "forbidden"}, corrData)
			return true
		}
	} else {
		if isNamed && !h.namedEnabled {
			return false
		}
		if !isNamed && !h.classicEnabled {
			// Allow list even when classic is disabled, as long as named is enabled
			if parsed.Suffix != suffixList || !h.namedEnabled {
				return false
			}
		}
	}

	key := Key{
		Namespace:  client.Namespace(),
		ClientID:   parsed.ClientID,
		GroupName:  parsed.GroupName,
		ShadowName: parsed.ShadowName,
	}

	// Throttle concurrent requests per client
	if !h.acquireInflight(client.ClientID()) {
		h.sendError(client, parsed, rejectedSuffix(parsed.Suffix), shadowErrorDetail{code: ErrCodeTooManyRequests, message: "too many requests"}, corrData)
		return true
	}
	defer h.releaseInflight(client.ClientID())

	switch parsed.Suffix {
	case suffixGet:
		h.handleGet(client, key, parsed, msg.Payload, corrData)
	case suffixUpdate:
		h.handleUpdate(client, key, parsed, msg.Payload, corrData)
	case suffixDelete:
		h.handleDelete(client, key, parsed, msg.Payload, corrData)
	case suffixList:
		h.handleList(client, key, parsed, msg.Payload, corrData)
	default:
		// Response topics (accepted/rejected/delta/documents) are server->client only.
		// Ignore them if a client publishes to them.
		return true
	}

	return true
}

// rejectedSuffix returns the rejected suffix for a given action suffix.
func rejectedSuffix(suffix string) string {
	switch suffix {
	case suffixGet:
		return suffixGetRejected
	case suffixUpdate:
		return suffixUpdateRejected
	case suffixDelete:
		return suffixDeleteRejected
	case suffixList:
		return suffixListRejected
	default:
		return suffixGetRejected
	}
}

// Get retrieves the current shadow document.
func (h *Handler) Get(key Key) (*Document, error) {
	doc, err := h.store.Get(key)
	if err != nil {
		return nil, err
	}

	if doc == nil {
		return nil, &ErrorResponse{Code: ErrCodeNotFound, Message: "shadow not found"}
	}

	return doc, nil
}

// Update updates a shadow directly (for REST API / external systems).
// Publishes delta/documents to subscribers via the configured PublishFunc.
func (h *Handler) Update(key Key, req UpdateRequest) (*Document, error) {
	if req.State.Desired == nil && req.State.Reported == nil && !req.ClearDesired && !req.ClearReported {
		return nil, &ErrorResponse{Code: ErrCodeBadRequest, Message: "state must contain desired or reported"}
	}

	if err := validateState(req.State, h.limits); err != nil {
		return nil, err
	}

	prev, doc, err := h.applyUpdate(key, req)
	if err != nil {
		return nil, err
	}

	h.publishNotifications(key, prev, doc, req.ClientToken)

	return doc, nil
}

// Delete removes a shadow. Returns the last document before deletion.
// Tracks the deleted version for inheritance if the shadow is recreated
// within the configured TTL (see WithDeletedVersionTTL).
func (h *Handler) Delete(key Key) (*Document, error) {
	unlock := h.lockShadow(key)
	defer unlock()

	doc, err := h.store.Delete(key)
	if err != nil {
		return nil, err
	}

	if doc == nil {
		return nil, &ErrorResponse{Code: ErrCodeNotFound, Message: "shadow not found"}
	}

	sk := shadowKey(key)
	h.deletedVersions.Store(sk, deletedVersion{
		version:   doc.Version,
		deletedAt: time.Now(),
	})

	return doc, nil
}

// SetConnected updates the connection metadata for a shadow.
// Does nothing if the shadow does not exist.
func (h *Handler) SetConnected(key Key, connected bool) error {
	unlock := h.lockShadow(key)
	defer unlock()

	doc, err := h.store.Get(key)
	if err != nil {
		return err
	}

	if doc == nil {
		return nil
	}

	doc.Metadata.Connection = &ConnectionMetadata{
		Connected:           connected,
		ConnectionTimestamp: time.Now().Unix(),
	}

	return h.store.Save(key, doc)
}

// List lists named shadows for a thing. Requires the store to implement Lister.
func (h *Handler) List(key Key, pageSize int, nextToken string) (*ListResult, error) {
	lister, ok := h.store.(Lister)
	if !ok {
		return nil, &ErrorResponse{Code: ErrCodeInternal, Message: "listing not supported"}
	}

	return lister.ListNamedShadows(key, pageSize, nextToken)
}

// PublishDelta publishes the current delta for a shadow to subscribers.
// This is intended to be called when a device reconnects, so it receives
// any pending delta that accumulated while it was offline.
// Returns nil if the shadow has no delta or does not exist.
// Requires a PublishFunc to be configured (see WithPublishFunc).
func (h *Handler) PublishDelta(key Key) error {
	if h.publishFunc == nil {
		return nil
	}

	doc, err := h.store.Get(key)
	if err != nil {
		return err
	}

	if doc == nil || len(doc.State.Delta) == 0 {
		return nil
	}

	delta := buildDeltaResponse(doc, "")

	data, err := json.Marshal(delta)
	if err != nil {
		return err
	}

	var topic string
	if key.GroupName != "" {
		topic = buildSharedTopic(key.GroupName, key.ShadowName, suffixUpdateDelta)
	} else {
		topic = buildTopic(key.ClientID, key.ShadowName, suffixUpdateDelta)
	}

	return h.publishFunc(&mqttv5.Message{
		Topic:       topic,
		Payload:     data,
		ContentType: "application/json",
		Namespace:   key.Namespace,
	})
}

// Close stops the shadow watcher if the store implements Watcher.
func (h *Handler) Close() error {
	if h.stopWatch != nil {
		h.stopWatch()
	}
	return nil
}

func (h *Handler) handleGet(client ServerClient, key Key, parsed *parsedTopic, payload []byte, corrData []byte) {
	var clientToken string

	if len(payload) > 0 {
		var req getRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			h.sendError(client, parsed, suffixGetRejected, shadowErrorDetail{code: ErrCodeBadRequest, message: "invalid payload"}, corrData)
			return
		}

		clientToken = req.ClientToken
	}

	doc, err := h.store.Get(key)
	if err != nil {
		h.sendError(client, parsed, suffixGetRejected, shadowErrorDetail{code: ErrCodeInternal, message: "internal error", clientToken: clientToken}, corrData)
		return
	}

	if doc == nil {
		h.sendError(client, parsed, suffixGetRejected, shadowErrorDetail{code: ErrCodeNotFound, message: "shadow not found", clientToken: clientToken}, corrData)
		return
	}

	resp := &getAcceptedResponse{
		State:       doc.State,
		Metadata:    doc.Metadata,
		Version:     doc.Version,
		Timestamp:   doc.Timestamp,
		ClientToken: clientToken,
	}

	h.sendJSON(client, parsed, suffixGetAccepted, resp, corrData)
}

func (h *Handler) handleUpdate(client ServerClient, key Key, parsed *parsedTopic, payload []byte, corrData []byte) {
	req, err := validatePayload(payload, h.limits)
	if err != nil {
		errResp := &ErrorResponse{Code: ErrCodeBadRequest, Message: "invalid payload"}
		errors.As(err, &errResp)

		var clientToken string
		if req != nil {
			clientToken = req.ClientToken
		}

		h.sendError(client, parsed, suffixUpdateRejected, shadowErrorDetail{code: errResp.Code, message: errResp.Message, clientToken: clientToken}, corrData)

		return
	}

	prev, doc, err := h.applyUpdate(key, *req)
	if err != nil {
		errResp := &ErrorResponse{Code: ErrCodeInternal, Message: "internal error"}
		errors.As(err, &errResp)
		h.sendError(client, parsed, suffixUpdateRejected, shadowErrorDetail{code: errResp.Code, message: errResp.Message, clientToken: req.ClientToken}, corrData)

		return
	}

	// Send accepted response with only the request's fields
	accepted := buildAcceptedResponse(req, doc)
	h.sendJSON(client, parsed, suffixUpdateAccepted, accepted, corrData)

	if len(doc.State.Delta) > 0 {
		delta := buildDeltaResponse(doc, req.ClientToken)
		h.publishTopicAny(key, parsed, suffixUpdateDelta, delta)
	}

	// Send documents message with previous and current snapshots
	docsMsg := buildDocumentsMessage(prev, doc, req.ClientToken)
	h.publishTopicAny(key, parsed, suffixUpdateDocuments, docsMsg)
}

func (h *Handler) handleDelete(client ServerClient, key Key, parsed *parsedTopic, payload []byte, corrData []byte) {
	var clientToken string

	if len(payload) > 0 {
		var req deleteRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			h.sendError(client, parsed, suffixDeleteRejected, shadowErrorDetail{code: ErrCodeBadRequest, message: "invalid payload"}, corrData)
			return
		}

		clientToken = req.ClientToken
	}

	unlock := h.lockShadow(key)
	defer unlock()

	doc, err := h.store.Delete(key)
	if err != nil {
		h.sendError(client, parsed, suffixDeleteRejected, shadowErrorDetail{code: ErrCodeInternal, message: "internal error", clientToken: clientToken}, corrData)
		return
	}

	if doc == nil {
		h.sendError(client, parsed, suffixDeleteRejected, shadowErrorDetail{code: ErrCodeNotFound, message: "shadow not found", clientToken: clientToken}, corrData)
		return
	}

	// Store deleted version for inheritance on re-creation
	sk := shadowKey(key)
	h.deletedVersions.Store(sk, deletedVersion{
		version:   doc.Version,
		deletedAt: time.Now(),
	})

	h.sendJSON(client, parsed, suffixDeleteAccepted, &deleteAcceptedResponse{
		Version:     doc.Version,
		Timestamp:   time.Now().Unix(),
		ClientToken: clientToken,
	}, corrData)
}

func (h *Handler) handleList(client ServerClient, key Key, parsed *parsedTopic, payload []byte, corrData []byte) {
	if !h.namedEnabled {
		h.sendError(client, parsed, suffixListRejected, shadowErrorDetail{code: ErrCodeForbidden, message: "named shadows not enabled"}, corrData)
		return
	}

	lister, ok := h.store.(Lister)
	if !ok {
		h.sendError(client, parsed, suffixListRejected, shadowErrorDetail{code: ErrCodeInternal, message: "listing not supported"}, corrData)
		return
	}

	var req listRequest
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &req); err != nil {
			h.sendError(client, parsed, suffixListRejected, shadowErrorDetail{code: ErrCodeBadRequest, message: "invalid payload"}, corrData)
			return
		}
	}

	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 25
	}

	result, err := lister.ListNamedShadows(key, pageSize, req.NextToken)
	if err != nil {
		h.sendError(client, parsed, suffixListRejected, shadowErrorDetail{code: ErrCodeInternal, message: "internal error", clientToken: req.ClientToken}, corrData)
		return
	}

	results := result.Results
	if results == nil {
		results = []string{}
	}

	h.sendJSON(client, parsed, suffixListAccepted, &listAcceptedResponse{
		Results:     results,
		Timestamp:   time.Now().Unix(),
		NextToken:   result.NextToken,
		ClientToken: req.ClientToken,
	}, corrData)
}

// mergeState recursively merges src into dst state and updates metadata accordingly.
// nil values in src delete the corresponding key (recursively).
func mergeState(dst, src, dstMeta map[string]any, ts int64) (map[string]any, map[string]any) {
	if dst == nil {
		dst = make(map[string]any)
	}
	if dstMeta == nil {
		dstMeta = make(map[string]any)
	}

	for k, v := range src {
		if v == nil {
			delete(dst, k)
			delete(dstMeta, k)
			continue
		}

		srcMap, srcIsMap := v.(map[string]any)
		dstVal, dstExists := dst[k]
		dstMap, dstIsMap := dstVal.(map[string]any)

		if srcIsMap && dstExists && dstIsMap {
			existingMeta, _ := dstMeta[k].(map[string]any)
			merged, mergedMeta := mergeState(dstMap, srcMap, existingMeta, ts)
			if len(merged) == 0 {
				delete(dst, k)
				delete(dstMeta, k)
			} else {
				dst[k] = merged
				dstMeta[k] = mergedMeta
			}
		} else {
			dst[k] = v
			dstMeta[k] = buildMetadataForValue(v, ts)
		}
	}

	return dst, dstMeta
}

func (h *Handler) applyUpdate(key Key, req UpdateRequest) (*Document, *Document, error) {
	unlock := h.lockShadow(key)
	defer unlock()

	doc, err := h.store.Get(key)
	if err != nil {
		return nil, nil, &ErrorResponse{Code: ErrCodeInternal, Message: "internal error"}
	}

	now := time.Now().Unix()
	isNew := doc == nil

	if isNew {
		// Check max named shadows limit for per-device named shadows
		if key.ShadowName != "" && key.GroupName == "" {
			if err := h.checkNamedShadowLimit(key); err != nil {
				return nil, nil, err
			}
		}

		doc = &Document{
			Metadata: Metadata{},
		}

		// Inherit version from deleted shadow if within TTL
		sk := shadowKey(key)
		if val, ok := h.deletedVersions.LoadAndDelete(sk); ok {
			dv := val.(deletedVersion)
			if time.Since(dv.deletedAt) <= h.deletedVersionTTL {
				doc.Version = dv.version
			}
		}
	}

	// Version conflict check
	if req.Version != nil && *req.Version != doc.Version {
		return nil, nil, &ErrorResponse{Code: ErrCodeVersionConflict, Message: "version conflict"}
	}

	// Snapshot previous state
	prev, _ := deepCopy(doc)

	// Clear desired section if explicitly set to null
	if req.ClearDesired {
		doc.State.Desired = nil
		doc.Metadata.Desired = nil
	}

	// Clear reported section if explicitly set to null
	if req.ClearReported {
		doc.State.Reported = nil
		doc.Metadata.Reported = nil
	}

	// Apply desired state
	if req.State.Desired != nil {
		doc.State.Desired, doc.Metadata.Desired = mergeState(doc.State.Desired, req.State.Desired, doc.Metadata.Desired, now)
		if len(doc.State.Desired) == 0 {
			doc.State.Desired = nil
			doc.Metadata.Desired = nil
		}
	}

	// Apply reported state
	if req.State.Reported != nil {
		doc.State.Reported, doc.Metadata.Reported = mergeState(doc.State.Reported, req.State.Reported, doc.Metadata.Reported, now)
		if len(doc.State.Reported) == 0 {
			doc.State.Reported = nil
			doc.Metadata.Reported = nil
		}
	}

	// Compute delta
	doc.State.Delta = computeDelta(doc.State.Desired, doc.State.Reported)

	doc.Version++
	doc.Timestamp = now

	// Check max document size
	if h.limits.MaxDocumentSize > 0 {
		data, err := json.Marshal(doc)
		if err != nil {
			return nil, nil, &ErrorResponse{Code: ErrCodeInternal, Message: "internal error"}
		}

		if len(data) > h.limits.MaxDocumentSize {
			return nil, nil, &ErrorResponse{
				Code:    ErrCodePayloadTooLarge,
				Message: fmt.Sprintf("resulting document exceeds maximum size of %d bytes", h.limits.MaxDocumentSize),
			}
		}
	}

	if err := h.store.Save(key, doc); err != nil {
		return nil, nil, &ErrorResponse{Code: ErrCodeInternal, Message: "internal error"}
	}

	return prev, doc, nil
}

func buildAcceptedResponse(req *UpdateRequest, doc *Document) map[string]any {
	state := make(map[string]any)
	meta := make(map[string]any)

	if req.ClearDesired {
		state["desired"] = nil
	} else if req.State.Desired != nil {
		state["desired"] = req.State.Desired

		if m := buildMetadata(req.State.Desired, doc.Timestamp); m != nil {
			meta["desired"] = m
		}
	}

	if req.ClearReported {
		state["reported"] = nil
	} else if req.State.Reported != nil {
		state["reported"] = req.State.Reported

		if m := buildMetadata(req.State.Reported, doc.Timestamp); m != nil {
			meta["reported"] = m
		}
	}

	resp := map[string]any{
		"state":     state,
		"metadata":  meta,
		"version":   doc.Version,
		"timestamp": doc.Timestamp,
	}

	if req.ClientToken != "" {
		resp["clientToken"] = req.ClientToken
	}

	return resp
}

func buildDeltaResponse(doc *Document, clientToken string) *deltaResponse {
	resp := &deltaResponse{
		State:       doc.State.Delta,
		Metadata:    filterMetadataForDelta(doc.State.Delta, doc.Metadata.Desired),
		Version:     doc.Version,
		Timestamp:   doc.Timestamp,
		ClientToken: clientToken,
	}

	return resp
}

func (h *Handler) acquireInflight(clientID string) bool {
	if h.limits.MaxInflightPerClient <= 0 {
		return true
	}

	sem, _ := h.inflight.LoadOrStore(clientID, make(chan struct{}, h.limits.MaxInflightPerClient))
	ch := sem.(chan struct{})

	select {
	case ch <- struct{}{}:
		return true
	default:
		return false
	}
}

func (h *Handler) releaseInflight(clientID string) {
	if h.limits.MaxInflightPerClient <= 0 {
		return
	}

	if sem, ok := h.inflight.Load(clientID); ok {
		ch := sem.(chan struct{})
		<-ch
	}
}

func (h *Handler) checkNamedShadowLimit(key Key) error {
	if h.limits.MaxNamedShadows <= 0 {
		return nil
	}

	counter, ok := h.store.(Counter)
	if !ok {
		return nil
	}

	count, err := counter.CountNamedShadows(key)
	if err != nil {
		return &ErrorResponse{Code: ErrCodeInternal, Message: "internal error"}
	}

	if count >= h.limits.MaxNamedShadows {
		return &ErrorResponse{
			Code:    ErrCodeBadRequest,
			Message: fmt.Sprintf("maximum of %d named shadows reached", h.limits.MaxNamedShadows),
		}
	}

	return nil
}

func buildDocumentsMessage(prev, current *Document, clientToken string) *DocumentsMessage {
	msg := &DocumentsMessage{
		Timestamp:   current.Timestamp,
		ClientToken: clientToken,
		Current: DocumentSnapshot{
			State:    current.State,
			Metadata: current.Metadata,
			Version:  current.Version,
		},
	}

	if prev != nil {
		msg.Previous = DocumentSnapshot{
			State:    prev.State,
			Metadata: prev.Metadata,
			Version:  prev.Version,
		}
	}

	return msg
}

func (h *Handler) publishNotifications(key Key, prev, doc *Document, clientToken string) {
	if h.publishFunc == nil {
		return
	}

	buildFn := func(suffix string) string {
		if key.GroupName != "" {
			return buildSharedTopic(key.GroupName, key.ShadowName, suffix)
		}

		return buildTopic(key.ClientID, key.ShadowName, suffix)
	}

	if len(doc.State.Delta) > 0 {
		topic := buildFn(suffixUpdateDelta)
		delta := buildDeltaResponse(doc, clientToken)
		if data, err := json.Marshal(delta); err == nil {
			_ = h.publishFunc(&mqttv5.Message{
				Topic:       topic,
				Payload:     data,
				ContentType: "application/json",
				Namespace:   key.Namespace,
			})
		}
	}

	docsMsg := buildDocumentsMessage(prev, doc, clientToken)
	topic := buildFn(suffixUpdateDocuments)

	if data, err := json.Marshal(docsMsg); err == nil {
		_ = h.publishFunc(&mqttv5.Message{
			Topic:       topic,
			Payload:     data,
			ContentType: "application/json",
			Namespace:   key.Namespace,
		})
	}
}

func (h *Handler) publishTopicAny(key Key, parsed *parsedTopic, suffix string, v any) {
	if h.publishFunc == nil {
		return
	}

	topic := responseTopic(parsed, suffix)
	data, err := json.Marshal(v)
	if err != nil {
		return
	}

	_ = h.publishFunc(&mqttv5.Message{
		Topic:       topic,
		Payload:     data,
		ContentType: "application/json",
		Namespace:   key.Namespace,
	})
}

func (h *Handler) sendJSON(client ServerClient, parsed *parsedTopic, suffix string, v any, correlationData []byte) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}

	topic := responseTopic(parsed, suffix)
	_ = client.Send(&mqttv5.Message{
		Topic:           topic,
		Payload:         data,
		ContentType:     "application/json",
		CorrelationData: correlationData,
	})
}

type shadowErrorDetail struct {
	code        int
	message     string
	clientToken string
}

func (h *Handler) sendError(client ServerClient, parsed *parsedTopic, suffix string, detail shadowErrorDetail, correlationData []byte) {
	h.sendJSON(client, parsed, suffix, &ErrorResponse{
		Code:        detail.code,
		Message:     detail.message,
		Timestamp:   time.Now().Unix(),
		ClientToken: detail.clientToken,
	}, correlationData)
}
