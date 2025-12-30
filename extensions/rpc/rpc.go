// Package rpc provides request/response functionality for MQTT v5.0 clients.
// It uses MQTT v5.0 correlation data and response topic properties to match
// requests with their responses.
// MQTT v5.0 spec: Section 4.10 (Request / Response)
package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vitalvas/mqttv5"
)

var (
	// ErrTimeout is returned when a request times out waiting for a response.
	ErrTimeout = errors.New("rpc: request timeout")

	// ErrClientClosed is returned when the client is closed during a request.
	ErrClientClosed = errors.New("rpc: client closed")

	// ErrNoResponseTopic is returned when no response topic is configured.
	ErrNoResponseTopic = errors.New("rpc: no response topic configured")
)

// Headers represents RPC headers as key-value pairs.
// Headers are transmitted using MQTT v5.0 User Properties.
type Headers map[string]string

// Request represents an RPC request with optional headers.
type Request struct {
	// Payload is the request body.
	Payload []byte

	// Headers contains optional request headers.
	// These are transmitted as MQTT v5.0 User Properties.
	Headers Headers

	// ContentType is the MIME type of the payload (optional).
	ContentType string
}

// Response represents an RPC response with headers.
type Response struct {
	// Payload is the response body.
	Payload []byte

	// Headers contains response headers from User Properties.
	Headers Headers

	// ContentType is the MIME type of the payload.
	ContentType string

	// CorrelationData is the correlation ID used to match this response.
	CorrelationData []byte
}

// Client defines the interface required for RPC operations.
type Client interface {
	// ClientID returns the client identifier.
	ClientID() string

	// Subscribe subscribes to a topic with a message handler.
	Subscribe(filter string, qos byte, handler mqttv5.MessageHandler) error

	// Unsubscribe unsubscribes from topic filters.
	Unsubscribe(filters ...string) error

	// Publish sends a message to the broker.
	Publish(msg *mqttv5.Message) error

	// IsConnected returns true if the client is connected.
	IsConnected() bool
}

// Handler provides request/response functionality using MQTT v5.0 properties.
type Handler struct {
	mu            sync.Mutex
	client        Client
	correlData    map[string]chan *Response
	responseTopic string
	qos           byte
}

// HandlerOptions configures the RPC handler.
type HandlerOptions struct {
	// ResponseTopic is the topic where responses will be received.
	// If empty, defaults to "rpc/response/{clientID}".
	ResponseTopic string

	// QoS is the quality of service level for requests and subscriptions.
	// Defaults to 0.
	QoS byte
}

// NewHandler creates a new RPC handler and subscribes to the response topic.
func NewHandler(client Client, opts *HandlerOptions) (*Handler, error) {
	if client == nil {
		return nil, errors.New("rpc: client is required")
	}

	if opts == nil {
		opts = &HandlerOptions{}
	}

	responseTopic := opts.ResponseTopic
	if responseTopic == "" {
		responseTopic = fmt.Sprintf("rpc/response/%s", client.ClientID())
	}

	h := &Handler{
		client:        client,
		correlData:    make(map[string]chan *Response),
		responseTopic: responseTopic,
		qos:           opts.QoS,
	}

	// Subscribe to response topic
	if err := client.Subscribe(responseTopic, opts.QoS, h.handleResponse); err != nil {
		return nil, fmt.Errorf("rpc: failed to subscribe to response topic: %w", err)
	}

	return h, nil
}

// ResponseTopic returns the configured response topic.
func (h *Handler) ResponseTopic() string {
	return h.responseTopic
}

// Call sends an RPC request with headers and waits for a response.
// The request is published to the specified topic with the response topic,
// correlation data, and headers set. The method blocks until a response
// is received or the context is cancelled.
func (h *Handler) Call(ctx context.Context, topic string, req *Request) (*Response, error) {
	if !h.client.IsConnected() {
		return nil, ErrClientClosed
	}

	if req == nil {
		req = &Request{}
	}

	// Generate correlation ID using nanosecond timestamp
	correlID := fmt.Sprintf("%d", time.Now().UnixNano())

	// Create response channel
	respChan := make(chan *Response, 1)
	h.addCorrelID(correlID, respChan)
	defer h.removeCorrelID(correlID)

	// Build and send request
	msg := &mqttv5.Message{
		Topic:           topic,
		Payload:         req.Payload,
		QoS:             h.qos,
		ResponseTopic:   h.responseTopic,
		CorrelationData: []byte(correlID),
		ContentType:     req.ContentType,
	}

	// Add headers as User Properties
	if len(req.Headers) > 0 {
		msg.UserProperties = make([]mqttv5.StringPair, 0, len(req.Headers))
		for k, v := range req.Headers {
			msg.UserProperties = append(msg.UserProperties, mqttv5.StringPair{Key: k, Value: v})
		}
	}

	if err := h.client.Publish(msg); err != nil {
		return nil, fmt.Errorf("rpc: failed to publish request: %w", err)
	}

	// Wait for response or context cancellation
	select {
	case resp := <-respChan:
		return resp, nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrTimeout
		}
		return nil, ctx.Err()
	}
}

// CallWithTimeout is a convenience method that creates a context with timeout.
func (h *Handler) CallWithTimeout(topic string, req *Request, timeout time.Duration) (*Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return h.Call(ctx, topic, req)
}

// Request sends a simple request without headers and waits for a response.
// For requests with headers, use Call instead.
func (h *Handler) Request(ctx context.Context, topic string, payload []byte) (*Response, error) {
	return h.Call(ctx, topic, &Request{Payload: payload})
}

// RequestWithTimeout is a convenience method that creates a context with timeout.
func (h *Handler) RequestWithTimeout(topic string, payload []byte, timeout time.Duration) (*Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return h.Request(ctx, topic, payload)
}

// Close unsubscribes from the response topic and cleans up resources.
func (h *Handler) Close() error {
	h.mu.Lock()
	// Close all pending response channels
	for correlID, ch := range h.correlData {
		close(ch)
		delete(h.correlData, correlID)
	}
	h.mu.Unlock()

	return h.client.Unsubscribe(h.responseTopic)
}

// addCorrelID stores a correlation ID with its response channel.
func (h *Handler) addCorrelID(correlID string, ch chan *Response) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.correlData[correlID] = ch
}

// removeCorrelID removes a correlation ID and returns its channel.
func (h *Handler) removeCorrelID(correlID string) chan *Response {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := h.correlData[correlID]
	delete(h.correlData, correlID)
	return ch
}

// getCorrelChan retrieves the channel for a correlation ID without removing it.
func (h *Handler) getCorrelChan(correlID string) chan *Response {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.correlData[correlID]
}

// handleResponse processes incoming response messages.
func (h *Handler) handleResponse(msg *mqttv5.Message) {
	if msg == nil || len(msg.CorrelationData) == 0 {
		return
	}

	correlID := string(msg.CorrelationData)
	ch := h.getCorrelChan(correlID)
	if ch == nil {
		return // No waiting request for this correlation ID
	}

	// Convert message to response with headers
	resp := &Response{
		Payload:         msg.Payload,
		ContentType:     msg.ContentType,
		CorrelationData: msg.CorrelationData,
	}

	// Extract headers from User Properties
	if len(msg.UserProperties) > 0 {
		resp.Headers = make(Headers, len(msg.UserProperties))
		for _, prop := range msg.UserProperties {
			resp.Headers[prop.Key] = prop.Value
		}
	}

	// Non-blocking send to prevent goroutine leaks
	select {
	case ch <- resp:
	default:
		// Channel full or closed, response dropped
	}
}
