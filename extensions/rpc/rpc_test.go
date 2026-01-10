package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

func setupTestServer(t *testing.T) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := mqttv5.NewServer(mqttv5.WithListener(listener))
	go server.ListenAndServe()

	cleanup := func() {
		server.Close()
		listener.Close()
	}

	return listener.Addr().String(), cleanup
}

func TestNewHandler(t *testing.T) {
	t.Run("nil client returns error", func(t *testing.T) {
		h, err := NewHandler(nil, nil)
		assert.Nil(t, h)
		assert.Error(t, err)
	})

	t.Run("creates handler with default options", func(t *testing.T) {
		addr, cleanup := setupTestServer(t)
		defer cleanup()

		client, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("test-client"))
		require.NoError(t, err)
		defer client.Close()

		h, err := NewHandler(client, nil)
		require.NoError(t, err)
		defer h.Close()

		assert.Equal(t, "rpc/response/test-client", h.ResponseTopic())
	})

	t.Run("creates handler with custom response topic", func(t *testing.T) {
		addr, cleanup := setupTestServer(t)
		defer cleanup()

		client, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("test-client"))
		require.NoError(t, err)
		defer client.Close()

		h, err := NewHandler(client, &HandlerOptions{
			ResponseTopic: "custom/response/topic",
			QoS:           1,
		})
		require.NoError(t, err)
		defer h.Close()

		assert.Equal(t, "custom/response/topic", h.ResponseTopic())
	})
}

func TestRequestResponse(t *testing.T) {
	t.Run("successful request-response", func(t *testing.T) {
		addr, cleanup := setupTestServer(t)
		defer cleanup()

		// Create requester client
		requester, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("requester"))
		require.NoError(t, err)
		defer requester.Close()

		// Create responder client
		responder, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("responder"))
		require.NoError(t, err)
		defer responder.Close()

		// Setup RPC handler for requester
		rpcHandler, err := NewHandler(requester, nil)
		require.NoError(t, err)
		defer rpcHandler.Close()

		// Setup responder to echo back with modified payload
		requestTopic := "service/echo"
		err = responder.Subscribe(requestTopic, 0, func(msg *mqttv5.Message) {
			// Send response using correlation data and response topic
			response := &mqttv5.Message{
				Topic:           msg.ResponseTopic,
				Payload:         append([]byte("echo: "), msg.Payload...),
				QoS:             0,
				CorrelationData: msg.CorrelationData,
			}
			responder.Publish(response)
		})
		require.NoError(t, err)

		// Give time for subscription to be processed
		time.Sleep(50 * time.Millisecond)

		// Make RPC request
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		resp, err := rpcHandler.Request(ctx, requestTopic, []byte("hello"))
		require.NoError(t, err)
		assert.Equal(t, []byte("echo: hello"), resp.Payload)
	})

	t.Run("request timeout", func(t *testing.T) {
		addr, cleanup := setupTestServer(t)
		defer cleanup()

		client, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("timeout-test"))
		require.NoError(t, err)
		defer client.Close()

		rpcHandler, err := NewHandler(client, nil)
		require.NoError(t, err)
		defer rpcHandler.Close()

		// Request to topic with no responder
		resp, err := rpcHandler.RequestWithTimeout("service/nonexistent", []byte("test"), 100*time.Millisecond)
		assert.Nil(t, resp)
		assert.Equal(t, ErrTimeout, err)
	})

	t.Run("context cancellation", func(t *testing.T) {
		addr, cleanup := setupTestServer(t)
		defer cleanup()

		client, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("cancel-test"))
		require.NoError(t, err)
		defer client.Close()

		rpcHandler, err := NewHandler(client, nil)
		require.NoError(t, err)
		defer rpcHandler.Close()

		ctx, cancel := context.WithCancel(context.Background())

		// Cancel immediately
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		resp, err := rpcHandler.Request(ctx, "service/test", []byte("test"))
		assert.Nil(t, resp)
		assert.Equal(t, context.Canceled, err)
	})
}

func TestMultipleSequentialRequests(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Create requester client
	requester, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("sequential-requester"))
	require.NoError(t, err)
	defer requester.Close()

	// Create responder client
	responder, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("sequential-responder"))
	require.NoError(t, err)
	defer responder.Close()

	// Setup RPC handler
	rpcHandler, err := NewHandler(requester, nil)
	require.NoError(t, err)
	defer rpcHandler.Close()

	// Setup responder
	requestTopic := "service/sequential"
	err = responder.Subscribe(requestTopic, 0, func(msg *mqttv5.Message) {
		response := &mqttv5.Message{
			Topic:           msg.ResponseTopic,
			Payload:         msg.Payload,
			CorrelationData: msg.CorrelationData,
		}
		responder.Publish(response)
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// Send multiple sequential requests
	numRequests := 5
	for i := range numRequests {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		payload := []byte{byte(i)}
		resp, err := rpcHandler.Request(ctx, requestTopic, payload)
		cancel()

		require.NoError(t, err, "request %d should succeed", i)
		require.Len(t, resp.Payload, 1)
		assert.Equal(t, byte(i), resp.Payload[0], "response %d should match request", i)
	}
}

func TestCallWithHeaders(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	// Create requester client
	requester, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("headers-requester"))
	require.NoError(t, err)
	defer requester.Close()

	// Create responder client
	responder, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("headers-responder"))
	require.NoError(t, err)
	defer responder.Close()

	// Setup RPC handler
	rpcHandler, err := NewHandler(requester, nil)
	require.NoError(t, err)
	defer rpcHandler.Close()

	// Setup responder that echoes headers back
	requestTopic := "service/headers"
	err = responder.Subscribe(requestTopic, 0, func(msg *mqttv5.Message) {
		// Echo back with response headers
		response := &mqttv5.Message{
			Topic:           msg.ResponseTopic,
			Payload:         msg.Payload,
			CorrelationData: msg.CorrelationData,
			ContentType:     "application/json",
			UserProperties: []mqttv5.StringPair{
				{Key: "x-response-id", Value: "resp-123"},
				{Key: "x-status", Value: "ok"},
			},
		}
		// Copy request headers to response
		for _, prop := range msg.UserProperties {
			response.UserProperties = append(response.UserProperties,
				mqttv5.StringPair{Key: "echo-" + prop.Key, Value: prop.Value})
		}
		responder.Publish(response)
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// Make RPC call with headers
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := rpcHandler.Call(ctx, requestTopic, &Request{
		Payload:     []byte(`{"action":"test"}`),
		ContentType: "application/json",
		Headers: Headers{
			"x-request-id": "req-456",
			"x-client":     "test-client",
		},
	})
	require.NoError(t, err)

	// Verify response
	assert.Equal(t, []byte(`{"action":"test"}`), resp.Payload)
	assert.Equal(t, "application/json", resp.ContentType)

	// Verify response headers
	assert.Equal(t, "resp-123", resp.Headers["x-response-id"])
	assert.Equal(t, "ok", resp.Headers["x-status"])

	// Verify echoed request headers
	assert.Equal(t, "req-456", resp.Headers["echo-x-request-id"])
	assert.Equal(t, "test-client", resp.Headers["echo-x-client"])
}

func TestCallWithTimeout(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	client, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("call-timeout-test"))
	require.NoError(t, err)
	defer client.Close()

	rpcHandler, err := NewHandler(client, nil)
	require.NoError(t, err)
	defer rpcHandler.Close()

	// Request to topic with no responder
	resp, err := rpcHandler.CallWithTimeout("service/nonexistent", &Request{
		Payload: []byte("test"),
		Headers: Headers{"x-test": "value"},
	}, 100*time.Millisecond)
	assert.Nil(t, resp)
	assert.Equal(t, ErrTimeout, err)
}

func TestHandlerClose(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	client, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("close-test"))
	require.NoError(t, err)
	defer client.Close()

	rpcHandler, err := NewHandler(client, nil)
	require.NoError(t, err)

	// Close should not error
	err = rpcHandler.Close()
	assert.NoError(t, err)
}

func TestCallDisconnectedClient(t *testing.T) {
	mock := &mockClient{
		clientID:  "disconnected-client",
		connected: false,
	}

	h := &Handler{
		client:        mock,
		correlData:    make(map[string]chan *Response),
		responseTopic: "rpc/response/disconnected-client",
	}

	ctx := context.Background()
	resp, err := h.Call(ctx, "test/topic", nil)
	assert.Nil(t, resp)
	assert.Equal(t, ErrClientClosed, err)
}

func TestCallPublishError(t *testing.T) {
	mock := &mockClient{
		clientID:    "publish-error-client",
		connected:   true,
		publishErr:  assert.AnError,
		subscribeFn: func(_ string, _ byte, _ mqttv5.MessageHandler) error { return nil },
	}

	h, err := NewHandler(mock, nil)
	require.NoError(t, err)

	ctx := context.Background()
	resp, err := h.Call(ctx, "test/topic", &Request{Payload: []byte("test")})
	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish request")
}

func TestNewHandlerSubscribeError(t *testing.T) {
	mock := &mockClient{
		clientID:  "subscribe-error-client",
		connected: true,
		subscribeFn: func(_ string, _ byte, _ mqttv5.MessageHandler) error {
			return assert.AnError
		},
	}

	h, err := NewHandler(mock, nil)
	assert.Nil(t, h)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to subscribe to response topic")
}

func TestHandleResponseEdgeCases(t *testing.T) {
	h := &Handler{
		correlData: make(map[string]chan *Response),
	}

	t.Run("nil message", func(_ *testing.T) {
		// Should not panic
		h.handleResponse(nil)
	})

	t.Run("empty correlation data", func(_ *testing.T) {
		msg := &mqttv5.Message{
			Payload:         []byte("test"),
			CorrelationData: nil,
		}
		// Should not panic
		h.handleResponse(msg)
	})

	t.Run("unknown correlation ID", func(_ *testing.T) {
		msg := &mqttv5.Message{
			Payload:         []byte("test"),
			CorrelationData: []byte("unknown-id"),
		}
		// Should not panic, just return
		h.handleResponse(msg)
	})

	t.Run("channel full - non-blocking send", func(_ *testing.T) {
		// Create a full channel (capacity 1, already has an item)
		correlID := "full-channel"
		ch := make(chan *Response, 1)
		ch <- &Response{Payload: []byte("existing")}
		h.correlData[correlID] = ch

		msg := &mqttv5.Message{
			Payload:         []byte("new"),
			CorrelationData: []byte(correlID),
		}
		// Should not block, message dropped
		h.handleResponse(msg)

		// Original message should still be there
		resp := <-ch
		assert.Equal(t, []byte("existing"), resp.Payload)
	})
}

func TestCloseWithPendingRequests(t *testing.T) {
	mock := &mockClient{
		clientID:  "close-pending-client",
		connected: true,
		subscribeFn: func(_ string, _ byte, _ mqttv5.MessageHandler) error {
			return nil
		},
		unsubscribeFn: func(_ ...string) error {
			return nil
		},
	}

	h, err := NewHandler(mock, nil)
	require.NoError(t, err)

	// Add some pending correlation IDs
	ch1 := make(chan *Response, 1)
	ch2 := make(chan *Response, 1)
	h.addCorrelID("pending-1", ch1)
	h.addCorrelID("pending-2", ch2)

	// Close should close all pending channels
	err = h.Close()
	assert.NoError(t, err)

	// Channels should be closed
	_, ok1 := <-ch1
	_, ok2 := <-ch2
	assert.False(t, ok1, "channel 1 should be closed")
	assert.False(t, ok2, "channel 2 should be closed")
}

func TestCallWithNilRequest(t *testing.T) {
	addr, cleanup := setupTestServer(t)
	defer cleanup()

	requester, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("nil-req-requester"))
	require.NoError(t, err)
	defer requester.Close()

	responder, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("nil-req-responder"))
	require.NoError(t, err)
	defer responder.Close()

	rpcHandler, err := NewHandler(requester, nil)
	require.NoError(t, err)
	defer rpcHandler.Close()

	requestTopic := "service/nil-req"
	err = responder.Subscribe(requestTopic, 0, func(msg *mqttv5.Message) {
		response := &mqttv5.Message{
			Topic:           msg.ResponseTopic,
			Payload:         []byte("received nil request"),
			CorrelationData: msg.CorrelationData,
		}
		responder.Publish(response)
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Call with nil request - should work with empty request
	resp, err := rpcHandler.Call(ctx, requestTopic, nil)
	require.NoError(t, err)
	assert.Equal(t, []byte("received nil request"), resp.Payload)
}

// mockClient implements the Client interface for testing
type mockClient struct {
	clientID      string
	connected     bool
	publishErr    error
	subscribeFn   func(filter string, qos byte, handler mqttv5.MessageHandler) error
	unsubscribeFn func(filters ...string) error
}

func (m *mockClient) ClientID() string {
	return m.clientID
}

func (m *mockClient) Subscribe(filter string, qos byte, handler mqttv5.MessageHandler) error {
	if m.subscribeFn != nil {
		return m.subscribeFn(filter, qos, handler)
	}
	return nil
}

func (m *mockClient) Unsubscribe(filters ...string) error {
	if m.unsubscribeFn != nil {
		return m.unsubscribeFn(filters...)
	}
	return nil
}

func (m *mockClient) Publish(_ *mqttv5.Message) error {
	return m.publishErr
}

func (m *mockClient) IsConnected() bool {
	return m.connected
}

// Fuzz tests

func FuzzHandleResponse(f *testing.F) {
	// Seed corpus
	f.Add([]byte("test-payload"), []byte("correl-123"))
	f.Add([]byte(""), []byte(""))
	f.Add([]byte("large payload with lots of data"), []byte("id"))
	f.Add([]byte{0x00, 0x01, 0x02}, []byte{0xff, 0xfe, 0xfd})

	f.Fuzz(func(t *testing.T, payload, correlData []byte) {
		h := &Handler{
			correlData: make(map[string]chan *Response),
		}

		// Test with unknown correlation ID
		msg := &mqttv5.Message{
			Payload:         payload,
			CorrelationData: correlData,
		}
		h.handleResponse(msg)

		// Test with known correlation ID
		if len(correlData) > 0 {
			correlID := string(correlData)
			ch := make(chan *Response, 1)
			h.correlData[correlID] = ch

			h.handleResponse(msg)

			select {
			case resp := <-ch:
				if resp.Payload == nil && payload != nil {
					t.Error("expected payload in response")
				}
			default:
				// No response is also valid if channel was full
			}
		}
	})
}

func FuzzHeaders(f *testing.F) {
	// Seed corpus with various header key-value pairs
	f.Add("Content-Type", "application/json")
	f.Add("x-custom-header", "value with spaces")
	f.Add("", "")
	f.Add("key", "value\nwith\nnewlines")

	f.Fuzz(func(t *testing.T, key, value string) {
		headers := Headers{key: value}

		// Test that headers can be iterated
		for k, v := range headers {
			if k != key || v != value {
				t.Errorf("header mismatch: got (%s, %s), want (%s, %s)", k, v, key, value)
			}
		}
	})
}

// Benchmarks

func BenchmarkHandleResponse(b *testing.B) {
	h := &Handler{
		correlData: make(map[string]chan *Response),
	}

	// Pre-add correlation IDs
	for i := range 100 {
		correlID := string(rune('a' + i%26))
		h.correlData[correlID] = make(chan *Response, 1)
	}

	msg := &mqttv5.Message{
		Payload:         []byte("benchmark payload"),
		CorrelationData: []byte("a"),
		ContentType:     "application/json",
		UserProperties: []mqttv5.StringPair{
			{Key: "header1", Value: "value1"},
			{Key: "header2", Value: "value2"},
		},
	}

	b.ResetTimer()
	for range b.N {
		// Refill the channel to prevent blocking
		ch := h.correlData["a"]
		select {
		case <-ch:
		default:
		}
		h.handleResponse(msg)
	}
}

func BenchmarkCorrelIDOperations(b *testing.B) {
	h := &Handler{
		correlData: make(map[string]chan *Response),
	}

	b.Run("add", func(b *testing.B) {
		for i := range b.N {
			correlID := string(rune(i % 1000))
			ch := make(chan *Response, 1)
			h.addCorrelID(correlID, ch)
		}
	})

	b.Run("get", func(b *testing.B) {
		// Pre-populate
		for i := range 1000 {
			correlID := string(rune(i))
			h.correlData[correlID] = make(chan *Response, 1)
		}
		b.ResetTimer()

		for i := range b.N {
			correlID := string(rune(i % 1000))
			h.getCorrelChan(correlID)
		}
	})

	b.Run("remove", func(b *testing.B) {
		// Pre-populate
		for i := range b.N {
			correlID := string(rune(i % 1000))
			h.correlData[correlID] = make(chan *Response, 1)
		}
		b.ResetTimer()

		for i := range b.N {
			correlID := string(rune(i % 1000))
			h.removeCorrelID(correlID)
		}
	})
}

func BenchmarkCall(b *testing.B) {
	// Setup mock that responds immediately
	mock := &mockClient{
		clientID:  "bench-client",
		connected: true,
	}

	var messageHandler mqttv5.MessageHandler
	mock.subscribeFn = func(_ string, _ byte, handler mqttv5.MessageHandler) error {
		messageHandler = handler
		return nil
	}

	h, err := NewHandler(mock, nil)
	if err != nil {
		b.Fatal(err)
	}

	// Mock publish to simulate immediate response
	mock.publishErr = nil

	b.ResetTimer()
	for range b.N {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		// Start call in goroutine
		done := make(chan struct{})
		go func() {
			_, _ = h.Call(ctx, "test/topic", &Request{Payload: []byte("test")})
			close(done)
		}()

		// Simulate response after brief delay
		time.Sleep(time.Microsecond)

		// Find and respond to the pending request
		h.mu.Lock()
		for correlID := range h.correlData {
			if messageHandler != nil {
				messageHandler(&mqttv5.Message{
					Payload:         []byte("response"),
					CorrelationData: []byte(correlID),
				})
			}
			break
		}
		h.mu.Unlock()

		<-done
		cancel()
	}
}

func BenchmarkRequestResponse(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}

	server := mqttv5.NewServer(mqttv5.WithListener(listener))
	go server.ListenAndServe()
	defer func() {
		server.Close()
		listener.Close()
	}()

	addr := listener.Addr().String()

	requester, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("bench-requester"))
	if err != nil {
		b.Fatal(err)
	}
	defer requester.Close()

	responder, err := mqttv5.Dial(mqttv5.WithServers("tcp://"+addr), mqttv5.WithClientID("bench-responder"))
	if err != nil {
		b.Fatal(err)
	}
	defer responder.Close()

	rpcHandler, err := NewHandler(requester, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer rpcHandler.Close()

	requestTopic := "bench/echo"
	if err := responder.Subscribe(requestTopic, 0, func(msg *mqttv5.Message) {
		response := &mqttv5.Message{
			Topic:           msg.ResponseTopic,
			Payload:         msg.Payload,
			CorrelationData: msg.CorrelationData,
		}
		responder.Publish(response)
	}); err != nil {
		b.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	b.ResetTimer()
	for range b.N {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := rpcHandler.Request(ctx, requestTopic, []byte("benchmark"))
		cancel()
		if err != nil {
			b.Fatal(err)
		}
	}
}
