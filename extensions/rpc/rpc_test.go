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

	server := mqttv5.NewServerWithListener(listener)
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

		client, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("test-client"))
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

		client, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("test-client"))
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
		requester, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("requester"))
		require.NoError(t, err)
		defer requester.Close()

		// Create responder client
		responder, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("responder"))
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
			responder.PublishMessage(response)
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

		client, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("timeout-test"))
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

		client, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("cancel-test"))
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
	requester, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("sequential-requester"))
	require.NoError(t, err)
	defer requester.Close()

	// Create responder client
	responder, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("sequential-responder"))
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
		responder.PublishMessage(response)
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
	requester, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("headers-requester"))
	require.NoError(t, err)
	defer requester.Close()

	// Create responder client
	responder, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("headers-responder"))
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
		responder.PublishMessage(response)
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

	client, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("call-timeout-test"))
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

	client, err := mqttv5.Dial("tcp://"+addr, mqttv5.WithClientID("close-test"))
	require.NoError(t, err)
	defer client.Close()

	rpcHandler, err := NewHandler(client, nil)
	require.NoError(t, err)

	// Close should not error
	err = rpcHandler.Close()
	assert.NoError(t, err)
}
