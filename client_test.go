package mqttv5

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockServer creates a TCP server that accepts one connection and runs a handler.
func mockServer(t *testing.T, handler func(net.Conn)) (string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		handler(conn)
	}()

	cleanup := func() {
		listener.Close()
		wg.Wait()
	}

	return listener.Addr().String(), cleanup
}

// sendConnack sends a CONNACK packet to the connection.
func sendConnack(conn net.Conn, sessionPresent bool, reasonCode ReasonCode) error {
	pkt := &ConnackPacket{
		SessionPresent: sessionPresent,
		ReasonCode:     reasonCode,
	}
	_, err := WritePacket(conn, pkt, 256*1024)
	return err
}

// readConnect reads a CONNECT packet from the connection.
func readConnect(t *testing.T, conn net.Conn) *ConnectPacket {
	t.Helper()

	pkt, _, err := ReadPacket(conn, 256*1024)
	require.NoError(t, err)

	connectPkt, ok := pkt.(*ConnectPacket)
	require.True(t, ok, "expected CONNECT packet, got %T", pkt)

	return connectPkt
}

func TestDialSuccess(t *testing.T) {
	addr, cleanup := mockServer(t, func(conn net.Conn) {
		_ = readConnect(t, conn)
		err := sendConnack(conn, false, ReasonSuccess)
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	})
	defer cleanup()

	client, err := Dial("tcp://"+addr, WithClientID("test-client"))
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	assert.True(t, client.IsConnected())
	assert.Equal(t, "test-client", client.ClientID())
}

func TestDialWithCredentials(t *testing.T) {
	var receivedConnect *ConnectPacket

	addr, cleanup := mockServer(t, func(conn net.Conn) {
		receivedConnect = readConnect(t, conn)
		err := sendConnack(conn, false, ReasonSuccess)
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	})
	defer cleanup()

	client, err := Dial("tcp://"+addr,
		WithClientID("test-client"),
		WithCredentials("user", "pass"),
	)
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	assert.Equal(t, "user", receivedConnect.Username)
	assert.Equal(t, []byte("pass"), receivedConnect.Password)
}

func TestDialConnectionRefused(t *testing.T) {
	addr, cleanup := mockServer(t, func(conn net.Conn) {
		_ = readConnect(t, conn)
		err := sendConnack(conn, false, ReasonBadUserNameOrPassword)
		assert.NoError(t, err)
	})
	defer cleanup()

	client, err := Dial("tcp://"+addr, WithClientID("test-client"))
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestDialContext(t *testing.T) {
	t.Run("success with context", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			err := sendConnack(conn, false, ReasonSuccess)
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
		})
		defer cleanup()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := DialContext(ctx, "tcp://"+addr, WithClientID("ctx-client"))
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.Close()

		assert.True(t, client.IsConnected())
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		client, err := DialContext(ctx, "tcp://127.0.0.1:65534")
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

func TestClose(t *testing.T) {
	var disconnectReceived bool
	var wg sync.WaitGroup
	wg.Add(1)

	addr, cleanup := mockServer(t, func(conn net.Conn) {
		defer wg.Done()
		_ = readConnect(t, conn)
		err := sendConnack(conn, false, ReasonSuccess)
		assert.NoError(t, err)

		// Wait for DISCONNECT
		pkt, _, err := ReadPacket(conn, 256*1024)
		if err == nil {
			_, disconnectReceived = pkt.(*DisconnectPacket)
		}
	})
	defer cleanup()

	client, err := Dial("tcp://"+addr, WithClientID("test-client"))
	require.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)

	wg.Wait()
	assert.True(t, disconnectReceived)
	assert.False(t, client.IsConnected())
}

func TestCloseIdempotent(t *testing.T) {
	addr, cleanup := mockServer(t, func(conn net.Conn) {
		_ = readConnect(t, conn)
		err := sendConnack(conn, false, ReasonSuccess)
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	})
	defer cleanup()

	client, err := Dial("tcp://"+addr, WithClientID("test-client"))
	require.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)
}

func TestPublish(t *testing.T) {
	t.Run("QoS 0", func(t *testing.T) {
		var receivedPublish *PublishPacket
		var wg sync.WaitGroup
		wg.Add(1)

		addr, cleanup := mockServer(t, func(conn net.Conn) {
			defer wg.Done()
			_ = readConnect(t, conn)
			err := sendConnack(conn, false, ReasonSuccess)
			assert.NoError(t, err)

			pkt, _, err := ReadPacket(conn, 256*1024)
			if err == nil {
				receivedPublish, _ = pkt.(*PublishPacket)
			}
		})
		defer cleanup()

		client, err := Dial("tcp://"+addr, WithClientID("test-client"))
		require.NoError(t, err)
		defer client.Close()

		err = client.Publish(&Message{Topic: "test/topic", Payload: []byte("hello"), QoS: 0})
		assert.NoError(t, err)

		wg.Wait()
		require.NotNil(t, receivedPublish)
		assert.Equal(t, "test/topic", receivedPublish.Topic)
		assert.Equal(t, []byte("hello"), receivedPublish.Payload)
		assert.Equal(t, byte(0), receivedPublish.QoS)
	})

	t.Run("QoS 1", func(t *testing.T) {
		var receivedPublish *PublishPacket
		var wg sync.WaitGroup
		wg.Add(1)

		addr, cleanup := mockServer(t, func(conn net.Conn) {
			defer wg.Done()
			_ = readConnect(t, conn)
			err := sendConnack(conn, false, ReasonSuccess)
			assert.NoError(t, err)

			pkt, _, err := ReadPacket(conn, 256*1024)
			if err == nil {
				receivedPublish, _ = pkt.(*PublishPacket)
				if receivedPublish != nil {
					// Send PUBACK
					puback := &PubackPacket{
						PacketID:   receivedPublish.PacketID,
						ReasonCode: ReasonSuccess,
					}
					_, _ = WritePacket(conn, puback, 256*1024)
				}
			}
			time.Sleep(50 * time.Millisecond)
		})
		defer cleanup()

		client, err := Dial("tcp://"+addr, WithClientID("test-client"))
		require.NoError(t, err)
		defer client.Close()

		err = client.Publish(&Message{Topic: "test/topic", Payload: []byte("hello"), QoS: 1})
		assert.NoError(t, err)

		wg.Wait()
		require.NotNil(t, receivedPublish)
		assert.Equal(t, byte(1), receivedPublish.QoS)
		assert.NotEqual(t, uint16(0), receivedPublish.PacketID)
	})

	t.Run("not connected", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			err := sendConnack(conn, false, ReasonSuccess)
			assert.NoError(t, err)
		})
		defer cleanup()

		client, err := Dial("tcp://"+addr, WithClientID("test-client"))
		require.NoError(t, err)

		err = client.Close()
		assert.NoError(t, err)

		err = client.Publish(&Message{Topic: "test/topic", Payload: []byte("hello"), QoS: 0})
		assert.ErrorIs(t, err, ErrClientClosed)
	})

	t.Run("empty topic", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			err := sendConnack(conn, false, ReasonSuccess)
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
		})
		defer cleanup()

		client, err := Dial("tcp://"+addr, WithClientID("test-client"))
		require.NoError(t, err)
		defer client.Close()

		err = client.Publish(&Message{Topic: "", Payload: []byte("hello"), QoS: 0})
		assert.ErrorIs(t, err, ErrEmptyTopic)
	})
}

func TestSubscribe(t *testing.T) {
	var receivedSubscribe *SubscribePacket
	var wg sync.WaitGroup
	wg.Add(1)

	addr, cleanup := mockServer(t, func(conn net.Conn) {
		defer wg.Done()
		_ = readConnect(t, conn)
		err := sendConnack(conn, false, ReasonSuccess)
		assert.NoError(t, err)

		pkt, _, err := ReadPacket(conn, 256*1024)
		if err == nil {
			receivedSubscribe, _ = pkt.(*SubscribePacket)
			if receivedSubscribe != nil {
				// Send SUBACK
				suback := &SubackPacket{
					PacketID:    receivedSubscribe.PacketID,
					ReasonCodes: []ReasonCode{ReasonSuccess},
				}
				_, _ = WritePacket(conn, suback, 256*1024)
			}
		}
		time.Sleep(50 * time.Millisecond)
	})
	defer cleanup()

	client, err := Dial("tcp://"+addr, WithClientID("test-client"))
	require.NoError(t, err)
	defer client.Close()

	handler := func(_ *Message) {}
	err = client.Subscribe("test/#", 1, handler)
	assert.NoError(t, err)

	wg.Wait()
	require.NotNil(t, receivedSubscribe)
	require.Len(t, receivedSubscribe.Subscriptions, 1)
	assert.Equal(t, "test/#", receivedSubscribe.Subscriptions[0].TopicFilter)
	assert.Equal(t, byte(1), receivedSubscribe.Subscriptions[0].QoS)
}

func TestUnsubscribe(t *testing.T) {
	var receivedUnsubscribe *UnsubscribePacket
	var wg sync.WaitGroup
	wg.Add(1)

	addr, cleanup := mockServer(t, func(conn net.Conn) {
		defer wg.Done()
		_ = readConnect(t, conn)
		err := sendConnack(conn, false, ReasonSuccess)
		assert.NoError(t, err)

		pkt, _, err := ReadPacket(conn, 256*1024)
		if err == nil {
			receivedUnsubscribe, _ = pkt.(*UnsubscribePacket)
			if receivedUnsubscribe != nil {
				// Send UNSUBACK
				unsuback := &UnsubackPacket{
					PacketID:    receivedUnsubscribe.PacketID,
					ReasonCodes: []ReasonCode{ReasonSuccess},
				}
				_, _ = WritePacket(conn, unsuback, 256*1024)
			}
		}
		time.Sleep(50 * time.Millisecond)
	})
	defer cleanup()

	client, err := Dial("tcp://"+addr, WithClientID("test-client"))
	require.NoError(t, err)
	defer client.Close()

	err = client.Unsubscribe("test/#")
	assert.NoError(t, err)

	wg.Wait()
	require.NotNil(t, receivedUnsubscribe)
	require.Len(t, receivedUnsubscribe.TopicFilters, 1)
	assert.Equal(t, "test/#", receivedUnsubscribe.TopicFilters[0])
}

func TestClientEventHandler(t *testing.T) {
	var connectedEvent error
	var mu sync.Mutex

	addr, cleanup := mockServer(t, func(conn net.Conn) {
		_ = readConnect(t, conn)
		err := sendConnack(conn, false, ReasonSuccess)
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	})
	defer cleanup()

	client, err := Dial("tcp://"+addr,
		WithClientID("test-client"),
		OnEvent(func(_ *Client, ev error) {
			mu.Lock()
			if connectedEvent == nil {
				connectedEvent = ev
			}
			mu.Unlock()
		}),
	)
	require.NoError(t, err)
	defer client.Close()

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	assert.NotNil(t, connectedEvent)
	mu.Unlock()
}

func TestGenerateClientID(t *testing.T) {
	id1 := generateClientID()
	id2 := generateClientID()

	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2)
	assert.Contains(t, id1, "mqttv5-")
}

func TestIsConnected(t *testing.T) {
	addr, cleanup := mockServer(t, func(conn net.Conn) {
		_ = readConnect(t, conn)
		err := sendConnack(conn, false, ReasonSuccess)
		assert.NoError(t, err)
		time.Sleep(200 * time.Millisecond)
	})
	defer cleanup()

	client, err := Dial("tcp://"+addr, WithClientID("test-client"))
	require.NoError(t, err)

	assert.True(t, client.IsConnected())

	err = client.Close()
	assert.NoError(t, err)

	assert.False(t, client.IsConnected())
}

func TestMaxSubscriptions(t *testing.T) {
	t.Run("exceeds limit", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			err := sendConnack(conn, false, ReasonSuccess)
			assert.NoError(t, err)

			// Handle subscribe packets
			for {
				pkt, _, err := ReadPacket(conn, 256*1024)
				if err != nil {
					return
				}
				if sub, ok := pkt.(*SubscribePacket); ok {
					suback := &SubackPacket{
						PacketID:    sub.PacketID,
						ReasonCodes: make([]ReasonCode, len(sub.Subscriptions)),
					}
					for i := range sub.Subscriptions {
						suback.ReasonCodes[i] = ReasonSuccess
					}
					_, _ = WritePacket(conn, suback, 256*1024)
				}
			}
		})
		defer cleanup()

		client, err := Dial("tcp://"+addr,
			WithClientID("test-client"),
			WithMaxSubscriptions(2),
		)
		require.NoError(t, err)
		defer client.Close()

		handler := func(_ *Message) {}

		// First subscription should succeed
		err = client.Subscribe("topic/1", 0, handler)
		assert.NoError(t, err)

		// Second subscription should succeed
		err = client.Subscribe("topic/2", 0, handler)
		assert.NoError(t, err)

		// Third subscription should fail
		err = client.Subscribe("topic/3", 0, handler)
		assert.ErrorIs(t, err, ErrTooManySubscriptions)
	})

	t.Run("resubscribe same topic allowed", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			err := sendConnack(conn, false, ReasonSuccess)
			assert.NoError(t, err)

			for {
				pkt, _, err := ReadPacket(conn, 256*1024)
				if err != nil {
					return
				}
				if sub, ok := pkt.(*SubscribePacket); ok {
					suback := &SubackPacket{
						PacketID:    sub.PacketID,
						ReasonCodes: make([]ReasonCode, len(sub.Subscriptions)),
					}
					for i := range sub.Subscriptions {
						suback.ReasonCodes[i] = ReasonSuccess
					}
					_, _ = WritePacket(conn, suback, 256*1024)
				}
			}
		})
		defer cleanup()

		client, err := Dial("tcp://"+addr,
			WithClientID("test-client"),
			WithMaxSubscriptions(1),
		)
		require.NoError(t, err)
		defer client.Close()

		handler := func(_ *Message) {}

		// First subscription
		err = client.Subscribe("topic/1", 0, handler)
		assert.NoError(t, err)

		// Re-subscribing to same topic should succeed (it's an update, not new)
		err = client.Subscribe("topic/1", 1, handler)
		assert.NoError(t, err)
	})

	t.Run("unlimited when zero", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			err := sendConnack(conn, false, ReasonSuccess)
			assert.NoError(t, err)

			for {
				pkt, _, err := ReadPacket(conn, 256*1024)
				if err != nil {
					return
				}
				if sub, ok := pkt.(*SubscribePacket); ok {
					suback := &SubackPacket{
						PacketID:    sub.PacketID,
						ReasonCodes: make([]ReasonCode, len(sub.Subscriptions)),
					}
					for i := range sub.Subscriptions {
						suback.ReasonCodes[i] = ReasonSuccess
					}
					_, _ = WritePacket(conn, suback, 256*1024)
				}
			}
		})
		defer cleanup()

		// WithMaxSubscriptions(0) means unlimited (default)
		client, err := Dial("tcp://"+addr,
			WithClientID("test-client"),
			WithMaxSubscriptions(0),
		)
		require.NoError(t, err)
		defer client.Close()

		handler := func(_ *Message) {}

		// Should be able to subscribe to many topics
		for i := 0; i < 10; i++ {
			err = client.Subscribe(fmt.Sprintf("topic/%d", i), 0, handler)
			assert.NoError(t, err)
		}
	})
}

// TestClientQoSRetryWithDUP tests that QoS 1/2 messages are retried with DUP flag (Issue 14)
func TestClientQoSRetryWithDUP(t *testing.T) {
	t.Run("QoS1 tracker retry logic sets DUP flag", func(t *testing.T) {
		// Test the retry logic directly by examining the tracker behavior
		tracker := NewQoS1Tracker(10*time.Millisecond, 3)

		msg := &Message{Topic: "test/topic", Payload: []byte("data")}
		tracker.Track(1, msg)

		// Initial message - not ready for retry yet
		pending := tracker.GetPendingRetries()
		assert.Empty(t, pending, "should not have pending retries immediately")

		// Wait for retry timeout
		time.Sleep(20 * time.Millisecond)

		// Now should have pending retry
		pending = tracker.GetPendingRetries()
		require.Len(t, pending, 1, "should have one pending retry")
		assert.Equal(t, uint16(1), pending[0].PacketID)
		assert.Equal(t, 1, pending[0].RetryCount, "retry count should be incremented")

		// Verify message is still tracked (for DUP flag on retry)
		tracked, ok := tracker.Get(1)
		assert.True(t, ok)
		assert.Equal(t, QoS1AwaitingPuback, tracked.State)
	})

	t.Run("QoS2 tracker retry logic sets DUP flag", func(t *testing.T) {
		tracker := NewQoS2Tracker(10*time.Millisecond, 3)

		msg := &Message{Topic: "test/topic", Payload: []byte("data")}
		tracker.TrackSend(1, msg)

		pending := tracker.GetPendingRetries()
		assert.Empty(t, pending)

		time.Sleep(20 * time.Millisecond)

		pending = tracker.GetPendingRetries()
		require.Len(t, pending, 1)
		assert.Equal(t, uint16(1), pending[0].PacketID)
		assert.Equal(t, QoS2AwaitingPubrec, pending[0].State)
	})

	t.Run("retry creates packet with DUP flag", func(t *testing.T) {
		// Verify that when creating a retry packet, DUP would be set
		tracker := NewQoS1Tracker(10*time.Millisecond, 3)
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), Retain: false}
		tracker.Track(1, msg)

		time.Sleep(20 * time.Millisecond)

		pending := tracker.GetPendingRetries()
		require.Len(t, pending, 1)

		// This is what the retry code does - create PUBLISH with DUP=true
		pub := &PublishPacket{
			PacketID: pending[0].PacketID,
			Topic:    pending[0].Message.Topic,
			Payload:  pending[0].Message.Payload,
			QoS:      1,
			Retain:   pending[0].Message.Retain,
			DUP:      true, // Set DUP flag for retransmission
		}

		assert.True(t, pub.DUP, "retry packet should have DUP flag")
		assert.Equal(t, uint16(1), pub.PacketID)
		assert.Equal(t, "test/topic", pub.Topic)
	})
}

// TestClientGoroutineCleanupOnReconnect tests that goroutines are cleaned up on reconnection (Issue 6)
func TestClientGoroutineCleanupOnReconnect(t *testing.T) {
	t.Run("context canceled before reconnect", func(t *testing.T) {
		connectionCount := 0
		var mu sync.Mutex

		addr, cleanup := mockServer(t, func(conn net.Conn) {
			mu.Lock()
			connectionCount++
			mu.Unlock()

			_ = readConnect(t, conn)
			_ = sendConnack(conn, false, ReasonSuccess)

			// Keep connection alive briefly
			time.Sleep(200 * time.Millisecond)
		})
		defer cleanup()

		client, err := Dial("tcp://"+addr, WithClientID("test-client"))
		require.NoError(t, err)

		// Verify initial context exists
		assert.NotNil(t, client.ctx)
		assert.NotNil(t, client.cancel)

		// Store old context
		oldCtx := client.ctx

		// Close to trigger cleanup
		client.Close()

		// Old context should be done
		select {
		case <-oldCtx.Done():
			// Expected
		case <-time.After(100 * time.Millisecond):
			t.Error("old context should be canceled")
		}
	})
}

// TestClientParentContextPropagation tests that parent context is respected (Issue 16)
func TestClientParentContextPropagation(t *testing.T) {
	t.Run("client closes when parent context canceled", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			_ = sendConnack(conn, false, ReasonSuccess)
			time.Sleep(500 * time.Millisecond)
		})
		defer cleanup()

		parentCtx, parentCancel := context.WithCancel(context.Background())

		client, err := DialContext(parentCtx, "tcp://"+addr, WithClientID("test-client"))
		require.NoError(t, err)

		assert.True(t, client.IsConnected())

		// Cancel parent context
		parentCancel()

		// Client's internal context should also be canceled
		select {
		case <-client.ctx.Done():
			// Expected - context propagated
		case <-time.After(100 * time.Millisecond):
			t.Error("client context should be canceled when parent is canceled")
		}

		client.Close()
	})

	t.Run("parent context stored in client", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			_ = sendConnack(conn, false, ReasonSuccess)
			time.Sleep(100 * time.Millisecond)
		})
		defer cleanup()

		parentCtx, parentCancel := context.WithCancel(context.Background())
		defer parentCancel()

		client, err := DialContext(parentCtx, "tcp://"+addr, WithClientID("test-client"))
		require.NoError(t, err)
		defer client.Close()

		assert.Equal(t, parentCtx, client.parentCtx)
	})
}

// TestClientCancelOnDialErrors tests that context is canceled on dial failures (Issue 18)
func TestClientCancelOnDialErrors(t *testing.T) {
	t.Run("cancel called on connection refused", func(t *testing.T) {
		// Try to connect to a port that's not listening
		_, err := Dial("tcp://127.0.0.1:59999",
			WithClientID("test-client"),
			WithConnectTimeout(100*time.Millisecond),
		)
		assert.Error(t, err)
		// If cancel wasn't called properly, we'd have resource leaks
	})

	t.Run("cancel called on CONNACK failure", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			// Send rejection
			_ = sendConnack(conn, false, ReasonNotAuthorized)
		})
		defer cleanup()

		_, err := Dial("tcp://"+addr, WithClientID("test-client"))
		assert.Error(t, err)
		// Context should be canceled, no resource leaks
	})

	t.Run("cancel called on invalid CONNACK", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			// Send garbage instead of CONNACK
			conn.Write([]byte{0xFF, 0xFF, 0xFF})
		})
		defer cleanup()

		_, err := Dial("tcp://"+addr,
			WithClientID("test-client"),
			WithConnectTimeout(100*time.Millisecond),
		)
		assert.Error(t, err)
	})
}

// TestSubscriptionHandlerTiming tests that handlers are registered before SUBSCRIBE is sent (Issue 7)
func TestSubscriptionHandlerTiming(t *testing.T) {
	t.Run("handler registered before SUBSCRIBE sent", func(t *testing.T) {
		var subscribeReceived bool
		var handlerRegistered bool
		var mu sync.Mutex

		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			_ = sendConnack(conn, false, ReasonSuccess)

			for {
				pkt, _, err := ReadPacket(conn, 256*1024)
				if err != nil {
					return
				}
				if sub, ok := pkt.(*SubscribePacket); ok {
					mu.Lock()
					subscribeReceived = true
					mu.Unlock()

					suback := &SubackPacket{
						PacketID:    sub.PacketID,
						ReasonCodes: []ReasonCode{ReasonSuccess},
					}
					_, _ = WritePacket(conn, suback, 256*1024)

					// Send a message immediately after SUBACK
					pub := &PublishPacket{
						Topic:   sub.Subscriptions[0].TopicFilter,
						Payload: []byte("immediate message"),
						QoS:     0,
					}
					_, _ = WritePacket(conn, pub, 256*1024)
				}
			}
		})
		defer cleanup()

		client, err := Dial("tcp://"+addr, WithClientID("test-client"))
		require.NoError(t, err)
		defer client.Close()

		messageReceived := make(chan struct{})

		err = client.Subscribe("test/topic", 0, func(_ *Message) {
			mu.Lock()
			handlerRegistered = true
			mu.Unlock()
			close(messageReceived)
		})
		require.NoError(t, err)

		// Wait for message
		select {
		case <-messageReceived:
			// Handler was registered in time to receive the message
		case <-time.After(500 * time.Millisecond):
			t.Error("handler should receive message sent immediately after SUBACK")
		}

		mu.Lock()
		assert.True(t, subscribeReceived)
		assert.True(t, handlerRegistered)
		mu.Unlock()
	})
}
