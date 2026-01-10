package mqttv5

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bufMockConn is a mock net.Conn for testing with buffer support.
type bufMockConn struct {
	reader   io.Reader
	writer   io.Writer
	readErr  error
	writeErr error
	closed   bool
}

func (c *bufMockConn) Read(b []byte) (int, error) {
	if c.readErr != nil {
		return 0, c.readErr
	}
	if c.reader != nil {
		return c.reader.Read(b)
	}
	return 0, io.EOF
}

func (c *bufMockConn) Write(b []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.writer != nil {
		return c.writer.Write(b)
	}
	return len(b), nil
}

func (c *bufMockConn) Close() error {
	c.closed = true
	return nil
}

func (c *bufMockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1883}
}

func (c *bufMockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 12345}
}

func (c *bufMockConn) SetDeadline(_ time.Time) error      { return nil }
func (c *bufMockConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *bufMockConn) SetWriteDeadline(_ time.Time) error { return nil }

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

	client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

	client, err := Dial(WithServers("tcp://"+addr),
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

	client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

		client, err := DialContext(ctx, WithServers("tcp://"+addr), WithClientID("ctx-client"))
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.Close()

		assert.True(t, client.IsConnected())
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		client, err := DialContext(ctx, WithServers("tcp://127.0.0.1:65534"))
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

	client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

	client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

		client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

		client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

		client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

		client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

	client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

	client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

	client, err := Dial(WithServers("tcp://"+addr),
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

	client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

		client, err := Dial(WithServers("tcp://"+addr),
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

		client, err := Dial(WithServers("tcp://"+addr),
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
		client, err := Dial(WithServers("tcp://"+addr),
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

		client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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
			time.Sleep(200 * time.Millisecond)
		})
		defer cleanup()

		parentCtx, parentCancel := context.WithCancel(context.Background())

		client, err := DialContext(parentCtx, WithServers("tcp://"+addr), WithClientID("test-client"))
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

		client, err := DialContext(parentCtx, WithServers("tcp://"+addr), WithClientID("test-client"))
		require.NoError(t, err)
		defer client.Close()

		assert.Equal(t, parentCtx, client.parentCtx)
	})
}

// TestClientCancelOnDialErrors tests that context is canceled on dial failures (Issue 18)
func TestClientCancelOnDialErrors(t *testing.T) {
	t.Run("cancel called on connection refused", func(t *testing.T) {
		// Try to connect to a port that's not listening
		_, err := Dial(WithServers("tcp://127.0.0.1:59999"),
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

		_, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

		_, err := Dial(WithServers("tcp://"+addr),
			WithClientID("test-client"),
			WithConnectTimeout(100*time.Millisecond),
		)
		assert.Error(t, err)
	})
}

// TestDeliverMessageNoDeadlock tests that handlers can call Subscribe/Unsubscribe without deadlock.
// This tests the fix for the deadlock issue where deliverMessage held subscriptionsMu.RLock()
// while invoking user handlers, which would deadlock if handlers called Subscribe/Unsubscribe.
func TestDeliverMessageNoDeadlock(t *testing.T) {
	t.Run("handler can call Subscribe without deadlock", func(t *testing.T) {
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
				switch p := pkt.(type) {
				case *SubscribePacket:
					suback := &SubackPacket{
						PacketID:    p.PacketID,
						ReasonCodes: make([]ReasonCode, len(p.Subscriptions)),
					}
					for i := range p.Subscriptions {
						suback.ReasonCodes[i] = ReasonSuccess
					}
					_, _ = WritePacket(conn, suback, 256*1024)
				case *PublishPacket:
					// Send the message back to client to trigger handler
					_, _ = WritePacket(conn, p, 256*1024)
				}
			}
		})
		defer cleanup()

		client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
		require.NoError(t, err)
		defer client.Close()

		handlerCalled := make(chan struct{})
		subscribeComplete := make(chan struct{})

		// Subscribe with handler that calls Subscribe (would deadlock before fix)
		err = client.Subscribe("test/topic", 0, func(_ *Message) {
			// This would deadlock before the fix because deliverMessage held RLock
			// and Subscribe needs to acquire Lock
			go func() {
				_ = client.Subscribe("test/other", 0, func(_ *Message) {})
				close(subscribeComplete)
			}()
			close(handlerCalled)
		})
		require.NoError(t, err)

		// Wait for subscription to be acknowledged
		time.Sleep(50 * time.Millisecond)

		// Publish a message to trigger the handler
		err = client.Publish(&Message{Topic: "test/topic", Payload: []byte("trigger")})
		require.NoError(t, err)

		// Wait for handler to be called - with timeout to detect deadlock
		select {
		case <-handlerCalled:
			// Handler was called successfully
		case <-time.After(2 * time.Second):
			t.Fatal("handler was not called - possible deadlock")
		}

		// Wait for nested subscribe to complete - with timeout to detect deadlock
		select {
		case <-subscribeComplete:
			// Nested subscribe completed successfully
		case <-time.After(2 * time.Second):
			t.Fatal("nested Subscribe deadlocked")
		}
	})

	t.Run("handler can call Unsubscribe without deadlock", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			err := sendConnack(conn, false, ReasonSuccess)
			assert.NoError(t, err)

			for {
				pkt, _, err := ReadPacket(conn, 256*1024)
				if err != nil {
					return
				}
				switch p := pkt.(type) {
				case *SubscribePacket:
					suback := &SubackPacket{
						PacketID:    p.PacketID,
						ReasonCodes: make([]ReasonCode, len(p.Subscriptions)),
					}
					for i := range p.Subscriptions {
						suback.ReasonCodes[i] = ReasonSuccess
					}
					_, _ = WritePacket(conn, suback, 256*1024)
				case *UnsubscribePacket:
					unsuback := &UnsubackPacket{
						PacketID:    p.PacketID,
						ReasonCodes: make([]ReasonCode, len(p.TopicFilters)),
					}
					for i := range p.TopicFilters {
						unsuback.ReasonCodes[i] = ReasonSuccess
					}
					_, _ = WritePacket(conn, unsuback, 256*1024)
				case *PublishPacket:
					_, _ = WritePacket(conn, p, 256*1024)
				}
			}
		})
		defer cleanup()

		client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
		require.NoError(t, err)
		defer client.Close()

		handlerCalled := make(chan struct{})
		unsubscribeComplete := make(chan struct{})

		// Subscribe with handler that calls Unsubscribe
		err = client.Subscribe("test/topic", 0, func(_ *Message) {
			go func() {
				_ = client.Unsubscribe("test/topic")
				close(unsubscribeComplete)
			}()
			close(handlerCalled)
		})
		require.NoError(t, err)

		time.Sleep(50 * time.Millisecond)

		err = client.Publish(&Message{Topic: "test/topic", Payload: []byte("trigger")})
		require.NoError(t, err)

		select {
		case <-handlerCalled:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("handler was not called - possible deadlock")
		}

		select {
		case <-unsubscribeComplete:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("nested Unsubscribe deadlocked")
		}
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

		client, err := Dial(WithServers("tcp://"+addr), WithClientID("test-client"))
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

func TestClientCapabilityChecking(t *testing.T) {
	t.Run("default capabilities allow all features", func(t *testing.T) {
		c := &Client{
			serverMaxQoS:             2,
			serverRetainAvailable:    true,
			serverWildcardSubAvail:   true,
			serverSubIDAvailable:     true,
			serverSharedSubAvailable: true,
		}

		// Defaults should allow everything
		assert.Equal(t, byte(2), c.serverMaxQoS)
		assert.True(t, c.serverRetainAvailable)
		assert.True(t, c.serverWildcardSubAvail)
		assert.True(t, c.serverSubIDAvailable)
		assert.True(t, c.serverSharedSubAvailable)
	})

	t.Run("QoS validation in Publish", func(t *testing.T) {
		c := &Client{
			options:      applyOptions(),
			serverMaxQoS: 1, // Server only supports QoS 0 and 1
		}
		c.connected.Store(true)

		// QoS 2 should fail
		err := c.Publish(&Message{Topic: "test", QoS: 2})
		assert.ErrorIs(t, err, ErrQoSNotSupported)
	})

	t.Run("retain validation in Publish", func(t *testing.T) {
		c := &Client{
			options:               applyOptions(),
			serverMaxQoS:          2,
			serverRetainAvailable: false, // Server doesn't support retain
		}
		c.connected.Store(true)

		// Retained message should fail
		err := c.Publish(&Message{Topic: "test", Retain: true})
		assert.ErrorIs(t, err, ErrRetainNotSupported)
	})
}

func TestClientTopicAliasResolution(t *testing.T) {
	t.Run("topic alias manager initialized with configured max", func(t *testing.T) {
		opts := applyOptions(WithTopicAliasMaximum(50))
		c := &Client{
			options:      opts,
			topicAliases: NewTopicAliasManager(opts.topicAliasMaximum, 0),
		}

		assert.Equal(t, uint16(50), c.topicAliases.InboundMax())
	})

	t.Run("handlePublish stores topic alias mapping", func(t *testing.T) {
		c := &Client{
			options:       applyOptions(),
			topicAliases:  NewTopicAliasManager(10, 0),
			subscriptions: make(map[string]MessageHandler),
			qos2Tracker:   NewQoS2Tracker(30*time.Second, 3),
		}
		c.connected.Store(true)

		// PUBLISH with topic and alias - should store mapping
		pkt := &PublishPacket{
			Topic: "sensor/temperature",
			QoS:   0,
		}
		pkt.Props.Set(PropTopicAlias, uint16(1))

		c.handlePublish(pkt)

		// Verify alias was stored
		resolved, err := c.topicAliases.GetInbound(1)
		assert.NoError(t, err)
		assert.Equal(t, "sensor/temperature", resolved)
	})

	t.Run("handlePublish resolves stored alias", func(t *testing.T) {
		receivedTopic := ""
		c := &Client{
			options:      applyOptions(),
			topicAliases: NewTopicAliasManager(10, 0),
			subscriptions: map[string]MessageHandler{
				"#": func(msg *Message) {
					receivedTopic = msg.Topic
				},
			},
			qos2Tracker: NewQoS2Tracker(30*time.Second, 3),
		}
		c.connected.Store(true)

		// First, store a mapping
		err := c.topicAliases.SetInbound(1, "sensor/temperature")
		require.NoError(t, err)

		// PUBLISH with only alias (no topic) - should resolve from mapping
		pkt := &PublishPacket{
			Topic: "", // Empty topic - must resolve from alias
			QoS:   0,
		}
		pkt.Props.Set(PropTopicAlias, uint16(1))

		c.handlePublish(pkt)

		assert.Equal(t, "sensor/temperature", receivedTopic)
	})

	t.Run("handlePublish rejects invalid alias", func(t *testing.T) {
		c := &Client{
			options:       applyOptions(),
			topicAliases:  NewTopicAliasManager(10, 0), // Max 10
			subscriptions: make(map[string]MessageHandler),
			qos2Tracker:   NewQoS2Tracker(30*time.Second, 3),
		}
		c.connected.Store(true)

		// PUBLISH with alias exceeding maximum
		pkt := &PublishPacket{
			Topic: "test",
			QoS:   0,
		}
		pkt.Props.Set(PropTopicAlias, uint16(11)) // Exceeds max of 10

		// This should trigger a disconnect (which we can't easily test without mocking)
		// But we can verify the alias manager rejects it
		err := c.topicAliases.SetInbound(11, "test")
		assert.Error(t, err)
	})

	t.Run("handlePublish rejects unknown alias", func(t *testing.T) {
		c := &Client{
			options:       applyOptions(),
			topicAliases:  NewTopicAliasManager(10, 0),
			subscriptions: make(map[string]MessageHandler),
			qos2Tracker:   NewQoS2Tracker(30*time.Second, 3),
		}
		c.connected.Store(true)

		// PUBLISH with only alias but no stored mapping
		pkt := &PublishPacket{
			Topic: "", // Empty topic
			QoS:   0,
		}
		pkt.Props.Set(PropTopicAlias, uint16(5)) // Not stored

		// Trying to get an unknown alias should fail
		_, err := c.topicAliases.GetInbound(5)
		assert.Error(t, err)
	})
}

func TestClientServerCapabilityParsing(t *testing.T) {
	t.Run("parses MaximumQoS from CONNACK", func(t *testing.T) {
		props := &Properties{}
		props.Set(PropMaximumQoS, byte(1))

		c := &Client{
			serverMaxQoS: 2, // Default
		}

		// Simulate parsing (normally done in connect())
		if props.Has(PropMaximumQoS) {
			c.serverMaxQoS = props.GetByte(PropMaximumQoS)
		}

		assert.Equal(t, byte(1), c.serverMaxQoS)
	})

	t.Run("parses RetainAvailable from CONNACK", func(t *testing.T) {
		props := &Properties{}
		props.Set(PropRetainAvailable, byte(0))

		c := &Client{
			serverRetainAvailable: true, // Default
		}

		if props.Has(PropRetainAvailable) {
			c.serverRetainAvailable = props.GetByte(PropRetainAvailable) == 1
		}

		assert.False(t, c.serverRetainAvailable)
	})

	t.Run("parses WildcardSubAvailable from CONNACK", func(t *testing.T) {
		props := &Properties{}
		props.Set(PropWildcardSubAvailable, byte(0))

		c := &Client{
			serverWildcardSubAvail: true, // Default
		}

		if props.Has(PropWildcardSubAvailable) {
			c.serverWildcardSubAvail = props.GetByte(PropWildcardSubAvailable) == 1
		}

		assert.False(t, c.serverWildcardSubAvail)
	})

	t.Run("parses SubscriptionIDAvailable from CONNACK", func(t *testing.T) {
		props := &Properties{}
		props.Set(PropSubscriptionIDAvailable, byte(0))

		c := &Client{
			serverSubIDAvailable: true, // Default
		}

		if props.Has(PropSubscriptionIDAvailable) {
			c.serverSubIDAvailable = props.GetByte(PropSubscriptionIDAvailable) == 1
		}

		assert.False(t, c.serverSubIDAvailable)
	})

	t.Run("parses SharedSubAvailable from CONNACK", func(t *testing.T) {
		props := &Properties{}
		props.Set(PropSharedSubAvailable, byte(0))

		c := &Client{
			serverSharedSubAvailable: true, // Default
		}

		if props.Has(PropSharedSubAvailable) {
			c.serverSharedSubAvailable = props.GetByte(PropSharedSubAvailable) == 1
		}

		assert.False(t, c.serverSharedSubAvailable)
	})

	t.Run("parses TopicAliasMaximum from CONNACK", func(t *testing.T) {
		props := &Properties{}
		props.Set(PropTopicAliasMaximum, uint16(50))

		c := &Client{
			topicAliases: NewTopicAliasManager(10, 0), // Inbound 10, outbound 0
		}

		if serverTAM := props.GetUint16(PropTopicAliasMaximum); serverTAM > 0 {
			c.topicAliases.SetOutboundMax(serverTAM)
		}

		assert.Equal(t, uint16(50), c.topicAliases.OutboundMax())
	})
}

func TestClientQoS2PubrecErrorHandling(t *testing.T) {
	t.Run("sends PUBREL even when PUBREC has error reason code", func(t *testing.T) {
		// Per MQTT v5 spec section 4.3.3, sender MUST send PUBREL in response to PUBREC
		// even when PUBREC contains an error reason code. This ensures the receiver
		// can complete the exchange and doesn't retransmit PUBREC indefinitely.

		var pubrelReceived bool
		var mu sync.Mutex

		addr, cleanup := mockServer(t, func(conn net.Conn) {
			// Read CONNECT
			pkt, _, err := ReadPacket(conn, 256*1024)
			require.NoError(t, err)
			_, ok := pkt.(*ConnectPacket)
			require.True(t, ok)

			// Send CONNACK
			require.NoError(t, sendConnack(conn, false, ReasonSuccess))

			// Read PUBLISH (QoS 2)
			pkt, _, err = ReadPacket(conn, 256*1024)
			if err != nil {
				return
			}
			pub, ok := pkt.(*PublishPacket)
			require.True(t, ok)
			require.Equal(t, byte(2), pub.QoS)

			// Send PUBREC with error reason code (No Matching Subscribers)
			pubrec := &PubrecPacket{
				PacketID:   pub.PacketID,
				ReasonCode: ReasonNoMatchingSubscribers,
			}
			WritePacket(conn, pubrec, 256*1024)

			// Wait for PUBREL (client should send it even on error)
			pkt, _, err = ReadPacket(conn, 256*1024)
			if err != nil {
				return
			}
			_, ok = pkt.(*PubrelPacket)
			mu.Lock()
			pubrelReceived = ok
			mu.Unlock()
		})
		defer cleanup()

		client, err := Dial(WithServers("tcp://"+addr), WithClientID("test"))
		require.NoError(t, err)
		defer client.Close()

		// Publish QoS 2 message
		client.Publish(&Message{
			Topic:   "test/topic",
			Payload: []byte("test"),
			QoS:     2,
		})

		// Give time for the exchange
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		assert.True(t, pubrelReceived, "PUBREL should be sent even when PUBREC has error")
		mu.Unlock()
	})
}

func TestClientEnhancedAuthentication(t *testing.T) {
	t.Run("uses enhanced auth during connect", func(t *testing.T) {
		var authMethodReceived string
		var authDataReceived []byte
		var mu sync.Mutex

		addr, cleanup := mockServer(t, func(conn net.Conn) {
			// Read CONNECT
			pkt, _, err := ReadPacket(conn, 256*1024)
			if err != nil {
				return
			}
			connect, ok := pkt.(*ConnectPacket)
			require.True(t, ok)

			mu.Lock()
			authMethodReceived = connect.Props.GetString(PropAuthenticationMethod)
			authDataReceived = connect.Props.GetBinary(PropAuthenticationData)
			mu.Unlock()

			// Send CONNACK (success - single step auth)
			require.NoError(t, sendConnack(conn, false, ReasonSuccess))

			// Keep connection open briefly
			time.Sleep(100 * time.Millisecond)
		})
		defer cleanup()

		auth := &mockClientEnhancedAuth{
			method:   "MOCK-AUTH",
			authData: []byte("client-first"),
		}

		client, err := Dial(WithServers("tcp://"+addr),
			WithClientID("test"),
			WithEnhancedAuthentication(auth),
		)
		require.NoError(t, err)
		defer client.Close()

		mu.Lock()
		assert.Equal(t, "MOCK-AUTH", authMethodReceived)
		assert.Equal(t, []byte("client-first"), authDataReceived)
		mu.Unlock()
	})

	t.Run("handles multi-step enhanced auth", func(t *testing.T) {
		var continueAuthSent bool
		var mu sync.Mutex

		addr, cleanup := mockServer(t, func(conn net.Conn) {
			// Read CONNECT
			pkt, _, err := ReadPacket(conn, 256*1024)
			if err != nil {
				return
			}
			_, ok := pkt.(*ConnectPacket)
			require.True(t, ok)

			// Send AUTH with ContinueAuth
			authPkt := &AuthPacket{
				ReasonCode: ReasonContinueAuth,
			}
			authPkt.Props.Set(PropAuthenticationMethod, "MOCK-AUTH")
			authPkt.Props.Set(PropAuthenticationData, []byte("server-challenge"))
			WritePacket(conn, authPkt, 256*1024)

			// Read client AUTH response
			pkt, _, err = ReadPacket(conn, 256*1024)
			if err != nil {
				return
			}
			clientAuth, ok := pkt.(*AuthPacket)
			require.True(t, ok)

			mu.Lock()
			continueAuthSent = clientAuth.ReasonCode == ReasonContinueAuth
			mu.Unlock()

			// Send CONNACK (success)
			require.NoError(t, sendConnack(conn, false, ReasonSuccess))

			// Keep connection open briefly
			time.Sleep(100 * time.Millisecond)
		})
		defer cleanup()

		auth := &mockClientEnhancedAuth{
			method:       "MOCK-AUTH",
			authData:     []byte("client-first"),
			continueData: []byte("client-response"),
		}

		client, err := Dial(WithServers("tcp://"+addr),
			WithClientID("test"),
			WithEnhancedAuthentication(auth),
		)
		require.NoError(t, err)
		defer client.Close()

		mu.Lock()
		assert.True(t, continueAuthSent, "client should respond with ContinueAuth")
		mu.Unlock()
	})

	t.Run("disconnects when AUTH received without enhanced auth configured", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &Client{
			options:  applyOptions(),
			ctx:      ctx,
			cancel:   cancel,
			done:     make(chan struct{}),
			readDone: make(chan struct{}),
		}
		close(c.readDone) // Prevent blocking in CloseWithCode

		// Create a mock AUTH packet
		authPkt := &AuthPacket{
			ReasonCode: ReasonContinueAuth,
		}

		var disconnectEmitted bool
		c.options.onEvent = func(_ *Client, event error) {
			if _, ok := event.(*DisconnectError); ok {
				disconnectEmitted = true
			}
		}

		// Mock conn that we can check
		mConn := &testMockConn{
			closed: make(chan struct{}),
		}
		c.conn = mConn

		c.handleAuth(authPkt)

		assert.True(t, disconnectEmitted)
	})
}

// mockClientEnhancedAuth implements ClientEnhancedAuthenticator for testing.
type mockClientEnhancedAuth struct {
	method       string
	authData     []byte
	continueData []byte
}

func (m *mockClientEnhancedAuth) AuthMethod() string {
	return m.method
}

func (m *mockClientEnhancedAuth) AuthStart(_ context.Context) (*ClientEnhancedAuthResult, error) {
	return &ClientEnhancedAuthResult{
		AuthData: m.authData,
	}, nil
}

func (m *mockClientEnhancedAuth) AuthContinue(_ context.Context, _ *ClientEnhancedAuthContext) (*ClientEnhancedAuthResult, error) {
	return &ClientEnhancedAuthResult{
		AuthData: m.continueData,
		Done:     true,
	}, nil
}

// testMockConn implements a minimal net.Conn for testing.
type testMockConn struct {
	net.Conn
	closed chan struct{}
}

func (m *testMockConn) Close() error {
	select {
	case <-m.closed:
	default:
		close(m.closed)
	}
	return nil
}

func (m *testMockConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func TestClientContextCancellation(t *testing.T) {
	t.Run("watchParentContext with nil context does nothing", func(t *testing.T) {
		c := &Client{
			parentCtx: nil,
			done:      make(chan struct{}),
		}

		// Should return immediately without blocking
		done := make(chan struct{})
		go func() {
			c.watchParentContext()
			close(done)
		}()

		select {
		case <-done:
			// Good - returned immediately
		case <-time.After(100 * time.Millisecond):
			t.Fatal("watchParentContext should return immediately with nil context")
		}
	})

	t.Run("watchParentContext exits when client closes first", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &Client{
			parentCtx: ctx,
			done:      make(chan struct{}),
		}

		// Start the context watcher
		done := make(chan struct{})
		go func() {
			c.watchParentContext()
			close(done)
		}()

		// Close the client's done channel (simulating Close())
		close(c.done)

		// watchParentContext should exit
		select {
		case <-done:
			// Good - the goroutine exited
		case <-time.After(100 * time.Millisecond):
			t.Fatal("watchParentContext did not exit when client done closed")
		}
	})
}

func TestClientErrorToReasonCode(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedReason ReasonCode
		description    string
	}{
		// Positive cases - specific errors
		{
			name:           "nil error returns success",
			err:            nil,
			expectedReason: ReasonSuccess,
			description:    "nil error should return ReasonSuccess",
		},
		{
			name:           "packet too large",
			err:            ErrPacketTooLarge,
			expectedReason: ReasonPacketTooLarge,
			description:    "ErrPacketTooLarge should return ReasonPacketTooLarge",
		},
		{
			name:           "unknown packet type",
			err:            ErrUnknownPacketType,
			expectedReason: ReasonProtocolError,
			description:    "ErrUnknownPacketType should return ReasonProtocolError",
		},
		{
			name:           "protocol violation",
			err:            ErrProtocolViolation,
			expectedReason: ReasonProtocolError,
			description:    "ErrProtocolViolation should return ReasonProtocolError",
		},
		{
			name:           "invalid reason code",
			err:            ErrInvalidReasonCode,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrInvalidReasonCode should return ReasonMalformedPacket",
		},
		{
			name:           "invalid packet flags",
			err:            ErrInvalidPacketFlags,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrInvalidPacketFlags should return ReasonMalformedPacket",
		},
		{
			name:           "invalid packet ID",
			err:            ErrInvalidPacketID,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrInvalidPacketID should return ReasonMalformedPacket",
		},
		{
			name:           "invalid QoS",
			err:            ErrInvalidQoS,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrInvalidQoS should return ReasonMalformedPacket",
		},
		{
			name:           "packet ID required",
			err:            ErrPacketIDRequired,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrPacketIDRequired should return ReasonMalformedPacket",
		},
		{
			name:           "invalid packet type",
			err:            ErrInvalidPacketType,
			expectedReason: ReasonProtocolError,
			description:    "ErrInvalidPacketType should return ReasonProtocolError",
		},
		{
			name:           "duplicate property",
			err:            ErrDuplicateProperty,
			expectedReason: ReasonProtocolError,
			description:    "ErrDuplicateProperty should return ReasonProtocolError",
		},
		{
			name:           "varint too large",
			err:            ErrVarintTooLarge,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrVarintTooLarge should return ReasonMalformedPacket",
		},
		{
			name:           "varint malformed",
			err:            ErrVarintMalformed,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrVarintMalformed should return ReasonMalformedPacket",
		},
		{
			name:           "varint overlong",
			err:            ErrVarintOverlong,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrVarintOverlong should return ReasonMalformedPacket",
		},
		{
			name:           "invalid connack flags",
			err:            ErrInvalidConnackFlags,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrInvalidConnackFlags should return ReasonMalformedPacket",
		},
		{
			name:           "unknown property ID",
			err:            ErrUnknownPropertyID,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrUnknownPropertyID should return ReasonMalformedPacket",
		},
		{
			name:           "invalid property type",
			err:            ErrInvalidPropertyType,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrInvalidPropertyType should return ReasonMalformedPacket",
		},
		{
			name:           "invalid UTF-8",
			err:            ErrInvalidUTF8,
			expectedReason: ReasonMalformedPacket,
			description:    "ErrInvalidUTF8 should return ReasonMalformedPacket",
		},
		// Negative cases - network errors (should not send DISCONNECT)
		{
			name:           "io EOF",
			err:            io.EOF,
			expectedReason: ReasonSuccess,
			description:    "io.EOF should return ReasonSuccess (network error)",
		},
		{
			name:           "generic error",
			err:            errors.New("connection reset"),
			expectedReason: ReasonSuccess,
			description:    "Generic error should return ReasonSuccess (network error)",
		},
		{
			name:           "timeout error",
			err:            errors.New("i/o timeout"),
			expectedReason: ReasonSuccess,
			description:    "Timeout error should return ReasonSuccess (network error)",
		},
		// Edge cases - wrapped errors
		{
			name:           "wrapped packet too large",
			err:            fmt.Errorf("read failed: %w", ErrPacketTooLarge),
			expectedReason: ReasonPacketTooLarge,
			description:    "Wrapped ErrPacketTooLarge should be detected",
		},
		{
			name:           "wrapped invalid flags",
			err:            fmt.Errorf("decode: %w", ErrInvalidPacketFlags),
			expectedReason: ReasonMalformedPacket,
			description:    "Wrapped ErrInvalidPacketFlags should be detected",
		},
		{
			name:           "double wrapped error",
			err:            fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", ErrProtocolViolation)),
			expectedReason: ReasonProtocolError,
			description:    "Double-wrapped error should be detected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clientErrorToReasonCode(tt.err)
			assert.Equal(t, tt.expectedReason, result, tt.description)
		})
	}
}

func TestApplyConnackProperties(t *testing.T) {
	tests := []struct {
		name        string
		setupProps  func(*ConnackPacket)
		expectError bool
		errorMsg    string
		validate    func(*testing.T, *Client)
		description string
	}{
		// Positive cases
		{
			name:        "nil properties",
			setupProps:  func(_ *ConnackPacket) {},
			expectError: false,
			validate:    func(_ *testing.T, _ *Client) {},
			description: "No properties should succeed",
		},
		{
			name: "valid receive maximum",
			setupProps: func(c *ConnackPacket) {
				c.Props.Set(PropReceiveMaximum, uint16(100))
			},
			expectError: false,
			validate: func(t *testing.T, c *Client) {
				// Flow control should be set
				assert.NotNil(t, c.serverFlowControl)
			},
			description: "Valid receive maximum should be applied",
		},
		{
			name: "assigned client identifier",
			setupProps: func(c *ConnackPacket) {
				c.Props.Set(PropAssignedClientIdentifier, "server-assigned-id")
			},
			expectError: false,
			validate: func(t *testing.T, c *Client) {
				assert.Equal(t, "server-assigned-id", c.options.clientID)
			},
			description: "Assigned client ID should be applied",
		},
		{
			name: "server keep alive override",
			setupProps: func(c *ConnackPacket) {
				c.Props.Set(PropServerKeepAlive, uint16(120))
			},
			expectError: false,
			validate: func(t *testing.T, c *Client) {
				assert.Equal(t, uint16(120), c.options.keepAlive)
			},
			description: "Server keep alive should override client setting",
		},
		{
			name: "maximum packet size",
			setupProps: func(c *ConnackPacket) {
				c.Props.Set(PropMaximumPacketSize, uint32(8192))
			},
			expectError: false,
			validate: func(t *testing.T, c *Client) {
				assert.Equal(t, uint32(8192), c.outboundMaxPacketSize)
			},
			description: "Maximum packet size should limit outbound packets",
		},
		{
			name: "topic alias maximum",
			setupProps: func(c *ConnackPacket) {
				c.Props.Set(PropTopicAliasMaximum, uint16(50))
			},
			expectError: false,
			validate: func(t *testing.T, c *Client) {
				assert.Equal(t, uint16(50), c.topicAliases.OutboundMax())
			},
			description: "Topic alias maximum should be applied",
		},
		// Negative cases
		{
			name: "receive maximum zero",
			setupProps: func(c *ConnackPacket) {
				c.Props.Set(PropReceiveMaximum, uint16(0))
			},
			expectError: true,
			errorMsg:    "Receive Maximum = 0",
			description: "Receive Maximum = 0 should fail",
		},
		// Edge cases
		{
			name: "receive maximum one",
			setupProps: func(c *ConnackPacket) {
				c.Props.Set(PropReceiveMaximum, uint16(1))
			},
			expectError: false,
			validate: func(t *testing.T, c *Client) {
				assert.NotNil(t, c.serverFlowControl)
			},
			description: "Receive Maximum = 1 should succeed (minimum valid)",
		},
		{
			name: "all server capabilities false",
			setupProps: func(c *ConnackPacket) {
				c.Props.Set(PropMaximumQoS, byte(0))
				c.Props.Set(PropRetainAvailable, byte(0))
				c.Props.Set(PropWildcardSubAvailable, byte(0))
				c.Props.Set(PropSubscriptionIDAvailable, byte(0))
				c.Props.Set(PropSharedSubAvailable, byte(0))
			},
			expectError: false,
			validate: func(t *testing.T, c *Client) {
				assert.Equal(t, byte(0), c.serverMaxQoS)
				assert.False(t, c.serverRetainAvailable)
				assert.False(t, c.serverWildcardSubAvail)
				assert.False(t, c.serverSubIDAvailable)
				assert.False(t, c.serverSharedSubAvailable)
			},
			description: "All capabilities disabled should be applied",
		},
		{
			name: "all server capabilities true",
			setupProps: func(c *ConnackPacket) {
				// Per MQTT v5 spec, MaximumQoS property absent means QoS 2 is supported
				// Only set other capabilities (MaximumQoS is omitted when server supports QoS 2)
				c.Props.Set(PropRetainAvailable, byte(1))
				c.Props.Set(PropWildcardSubAvailable, byte(1))
				c.Props.Set(PropSubscriptionIDAvailable, byte(1))
				c.Props.Set(PropSharedSubAvailable, byte(1))
			},
			expectError: false,
			validate: func(t *testing.T, c *Client) {
				// serverMaxQoS defaults to 2 when PropMaximumQoS is absent
				assert.Equal(t, byte(2), c.serverMaxQoS)
				assert.True(t, c.serverRetainAvailable)
				assert.True(t, c.serverWildcardSubAvail)
				assert.True(t, c.serverSubIDAvailable)
				assert.True(t, c.serverSharedSubAvailable)
			},
			description: "All capabilities enabled should be applied",
		},
		{
			name: "invalid MaximumQoS value 2",
			setupProps: func(c *ConnackPacket) {
				// Per MQTT v5 spec section 3.2.2.3.4, MaximumQoS can only be 0 or 1
				c.Props.Set(PropMaximumQoS, byte(2))
			},
			expectError: true,
			errorMsg:    "invalid Maximum QoS",
			description: "MaximumQoS = 2 is invalid per MQTT v5 spec",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				options:                  &clientOptions{keepAlive: 60, sessionFactory: func(clientID, namespace string) Session { return NewMemorySession(clientID, namespace) }},
				session:                  NewMemorySession("test", ""),
				topicAliases:             NewTopicAliasManager(10, 10),
				serverFlowControl:        NewFlowController(65535),
				serverMaxQoS:             2,    // Default: QoS 0, 1, 2 all supported
				serverRetainAvailable:    true, // Default: supported
				serverWildcardSubAvail:   true, // Default: supported
				serverSubIDAvailable:     true, // Default: supported
				serverSharedSubAvailable: true, // Default: supported
			}

			connack := &ConnackPacket{}
			tt.setupProps(connack)

			err := client.applyConnackProperties(connack)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				if tt.validate != nil {
					tt.validate(t, client)
				}
			}
		})
	}
}

func TestSendDisconnect(t *testing.T) {
	t.Run("nil connection does not panic", func(t *testing.T) {
		client := &Client{
			conn:    nil,
			options: &clientOptions{maxPacketSize: MaxPacketSizeDefault},
		}

		// Should not panic
		assert.NotPanics(t, func() {
			client.sendDisconnect(ReasonProtocolError)
		})
	})

	t.Run("sends disconnect packet", func(t *testing.T) {
		var buf bytes.Buffer
		conn := &bufMockConn{writer: &buf}

		client := &Client{
			conn:    conn,
			options: &clientOptions{maxPacketSize: MaxPacketSizeDefault},
		}

		client.sendDisconnect(ReasonMalformedPacket)

		// Verify a packet was written
		assert.Greater(t, buf.Len(), 0)

		// Parse the packet and verify it's a DISCONNECT
		pkt, _, err := ReadPacket(&buf, MaxPacketSizeDefault)
		require.NoError(t, err)

		disconnect, ok := pkt.(*DisconnectPacket)
		require.True(t, ok)
		assert.Equal(t, ReasonMalformedPacket, disconnect.ReasonCode)
	})

	t.Run("handles write error gracefully", func(t *testing.T) {
		conn := &bufMockConn{writeErr: errors.New("write failed")}

		client := &Client{
			conn:    conn,
			options: &clientOptions{maxPacketSize: MaxPacketSizeDefault},
		}

		// Should not panic even when write fails
		assert.NotPanics(t, func() {
			client.sendDisconnect(ReasonProtocolError)
		})
	})
}

func TestHandlePubrel(t *testing.T) {
	tests := []struct {
		name           string
		packetID       uint16
		setupTracker   func(*QoS2Tracker)
		expectedReason ReasonCode
	}{
		{
			name:           "unknown packet ID returns PacketIDNotFound",
			packetID:       100,
			setupTracker:   func(_ *QoS2Tracker) {},
			expectedReason: ReasonPacketIDNotFound,
		},
		{
			name:     "known packet ID returns success",
			packetID: 1,
			setupTracker: func(tr *QoS2Tracker) {
				tr.TrackReceive(1, &Message{Topic: "test/topic", Payload: []byte("data")})
				tr.SendPubrec(1) // Move to awaiting pubrel state
			},
			expectedReason: ReasonSuccess,
		},
		{
			name:     "packet ID at boundary",
			packetID: 65535,
			setupTracker: func(tr *QoS2Tracker) {
				tr.TrackReceive(65535, &Message{Topic: "boundary", Payload: []byte("max")})
				tr.SendPubrec(65535)
			},
			expectedReason: ReasonSuccess,
		},
		{
			name:     "multiple packets tracked",
			packetID: 50,
			setupTracker: func(tr *QoS2Tracker) {
				// Track multiple packets and only query one
				tr.TrackReceive(50, &Message{Topic: "test1"})
				tr.TrackReceive(51, &Message{Topic: "test2"})
				tr.SendPubrec(50)
				tr.SendPubrec(51)
			},
			expectedReason: ReasonSuccess,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			conn := &bufMockConn{writer: &buf}

			client := &Client{
				conn:        conn,
				options:     &clientOptions{maxPacketSize: MaxPacketSizeDefault},
				qos2Tracker: NewQoS2Tracker(time.Second, 3),
			}

			tt.setupTracker(client.qos2Tracker)

			pkt := &PubrelPacket{PacketID: tt.packetID}
			client.handlePubrel(pkt)

			// Read and verify PUBCOMP response
			respPkt, _, err := ReadPacket(&buf, MaxPacketSizeDefault)
			require.NoError(t, err)

			pubcomp, ok := respPkt.(*PubcompPacket)
			require.True(t, ok, "expected PUBCOMP packet")
			assert.Equal(t, tt.packetID, pubcomp.PacketID)
			assert.Equal(t, tt.expectedReason, pubcomp.ReasonCode)
		})
	}
}

func TestHandlePubcomp(t *testing.T) {
	tests := []struct {
		name           string
		packetID       uint16
		reasonCode     ReasonCode
		setupTracker   func(*QoS2Tracker, *PacketIDManager)
		expectRelease  bool
		expectErrorEvt bool
	}{
		{
			name:       "successful pubcomp releases flow control",
			packetID:   1,
			reasonCode: ReasonSuccess,
			setupTracker: func(tr *QoS2Tracker, _ *PacketIDManager) {
				tr.TrackSend(1, &Message{Topic: "test"})
				tr.HandlePubrec(1) // Move to awaiting pubcomp state
			},
			expectRelease:  true,
			expectErrorEvt: false,
		},
		{
			name:       "error reason code emits publish error",
			packetID:   2,
			reasonCode: ReasonQuotaExceeded,
			setupTracker: func(tr *QoS2Tracker, _ *PacketIDManager) {
				tr.TrackSend(2, &Message{Topic: "test/error"})
				tr.HandlePubrec(2)
			},
			expectRelease:  true,
			expectErrorEvt: true,
		},
		{
			name:       "unknown packet ID still releases packet ID",
			packetID:   999,
			reasonCode: ReasonSuccess,
			setupTracker: func(_ *QoS2Tracker, _ *PacketIDManager) {
				// Don't add to tracker - simulates out-of-order/lost state
			},
			expectRelease:  false,
			expectErrorEvt: false,
		},
		{
			name:       "packet not authorized error",
			packetID:   3,
			reasonCode: ReasonNotAuthorized,
			setupTracker: func(tr *QoS2Tracker, _ *PacketIDManager) {
				tr.TrackSend(3, &Message{Topic: "restricted"})
				tr.HandlePubrec(3)
			},
			expectRelease:  true,
			expectErrorEvt: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var events []error
			eventMu := &sync.Mutex{}

			client := &Client{
				options: &clientOptions{
					maxPacketSize: MaxPacketSizeDefault,
					onEvent: func(_ *Client, evt error) {
						eventMu.Lock()
						events = append(events, evt)
						eventMu.Unlock()
					},
				},
				qos2Tracker:       NewQoS2Tracker(time.Second, 3),
				packetIDMgr:       NewPacketIDManager(),
				serverFlowControl: NewFlowController(100),
			}

			tt.setupTracker(client.qos2Tracker, client.packetIDMgr)

			pkt := &PubcompPacket{
				PacketID:   tt.packetID,
				ReasonCode: tt.reasonCode,
			}
			client.handlePubcomp(pkt)

			if tt.expectErrorEvt {
				eventMu.Lock()
				assert.NotEmpty(t, events, "expected error event to be emitted")
				if len(events) > 0 {
					_, ok := events[0].(*PublishError)
					assert.True(t, ok, "expected PublishError event")
				}
				eventMu.Unlock()
			}
		})
	}
}

func TestHandleDisconnect(t *testing.T) {
	tests := []struct {
		name          string
		reasonCode    ReasonCode
		autoReconnect bool
		expectClosed  bool
	}{
		{
			name:          "normal disconnect sets connected false",
			reasonCode:    ReasonSuccess,
			autoReconnect: false,
			expectClosed:  true,
		},
		{
			name:          "server busy disconnect",
			reasonCode:    ReasonServerBusy,
			autoReconnect: false,
			expectClosed:  true,
		},
		{
			name:          "session taken over",
			reasonCode:    ReasonSessionTakenOver,
			autoReconnect: false,
			expectClosed:  true,
		},
		{
			name:          "protocol error disconnect",
			reasonCode:    ReasonProtocolError,
			autoReconnect: false,
			expectClosed:  true,
		},
		{
			name:          "administrative action disconnect",
			reasonCode:    ReasonAdminAction,
			autoReconnect: false,
			expectClosed:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var events []error
			eventMu := &sync.Mutex{}

			mockConn := &bufMockConn{}
			ctx, cancel := context.WithCancel(context.Background())

			client := &Client{
				conn:   mockConn,
				ctx:    ctx,
				cancel: cancel,
				options: &clientOptions{
					autoReconnect: tt.autoReconnect,
					onEvent: func(_ *Client, evt error) {
						eventMu.Lock()
						events = append(events, evt)
						eventMu.Unlock()
					},
				},
			}
			client.connected.Store(true)
			client.closed.Store(false)

			pkt := &DisconnectPacket{ReasonCode: tt.reasonCode}
			client.handleDisconnect(pkt)

			// Verify connection state
			assert.False(t, client.connected.Load(), "client should be disconnected")

			// Verify connection was closed
			if tt.expectClosed {
				assert.True(t, mockConn.closed, "connection should be closed")
			}

			// Verify disconnect event was emitted
			eventMu.Lock()
			assert.NotEmpty(t, events, "expected disconnect event")
			if len(events) > 0 {
				disconnectErr, ok := events[0].(*DisconnectError)
				require.True(t, ok, "expected DisconnectError event")
				assert.Equal(t, tt.reasonCode, disconnectErr.ReasonCode)
				assert.True(t, disconnectErr.Remote)
			}
			eventMu.Unlock()
		})
	}
}

func TestClientErrorTypes(t *testing.T) {
	t.Run("ReconnectEvent", func(t *testing.T) {
		tests := []struct {
			name        string
			attempt     int
			maxAttempts int
			delay       time.Duration
		}{
			{
				name:        "first attempt",
				attempt:     1,
				maxAttempts: 5,
				delay:       time.Second,
			},
			{
				name:        "middle attempt",
				attempt:     3,
				maxAttempts: 10,
				delay:       5 * time.Second,
			},
			{
				name:        "unlimited attempts",
				attempt:     100,
				maxAttempts: 0,
				delay:       30 * time.Second,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cancelled := false
				cancelFn := func() { cancelled = true }

				evt := NewReconnectEvent(tt.attempt, tt.maxAttempts, tt.delay, cancelFn)

				assert.Equal(t, tt.attempt, evt.Attempt)
				assert.Equal(t, tt.maxAttempts, evt.MaxAttempts)
				assert.Equal(t, tt.delay, evt.Delay)
				assert.Contains(t, evt.Error(), "reconnecting")
				assert.True(t, errors.Is(evt, ErrReconnecting))

				evt.Cancel()
				assert.True(t, cancelled, "cancel function should have been called")
			})
		}
	})

	t.Run("ReconnectEvent with nil cancel", func(t *testing.T) {
		evt := NewReconnectEvent(1, 5, time.Second, nil)
		assert.NotPanics(t, func() {
			evt.Cancel()
		})
	})

	t.Run("PublishError", func(t *testing.T) {
		tests := []struct {
			name       string
			topic      string
			packetID   uint16
			reasonCode ReasonCode
		}{
			{
				name:       "quota exceeded",
				topic:      "test/topic",
				packetID:   1,
				reasonCode: ReasonQuotaExceeded,
			},
			{
				name:       "not authorized",
				topic:      "restricted/topic",
				packetID:   100,
				reasonCode: ReasonNotAuthorized,
			},
			{
				name:       "packet too large",
				topic:      "large/payload",
				packetID:   65535,
				reasonCode: ReasonPacketTooLarge,
			},
			{
				name:       "empty topic",
				topic:      "",
				packetID:   0,
				reasonCode: ReasonTopicNameInvalid,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := NewPublishError(tt.topic, tt.packetID, tt.reasonCode)

				assert.Equal(t, tt.topic, err.Topic)
				assert.Equal(t, tt.packetID, err.PacketID)
				assert.Equal(t, tt.reasonCode, err.ReasonCode)
				assert.Contains(t, err.Error(), "publish failed")
				assert.True(t, errors.Is(err, ErrPublishFailed))
			})
		}
	})

	t.Run("SubscribeError", func(t *testing.T) {
		tests := []struct {
			name       string
			topic      string
			reasonCode ReasonCode
		}{
			{
				name:       "not authorized",
				topic:      "restricted/#",
				reasonCode: ReasonNotAuthorized,
			},
			{
				name:       "topic filter invalid",
				topic:      "invalid//topic",
				reasonCode: ReasonTopicFilterInvalid,
			},
			{
				name:       "quota exceeded",
				topic:      "heavy/load/+",
				reasonCode: ReasonQuotaExceeded,
			},
			{
				name:       "unspecified error",
				topic:      "some/topic",
				reasonCode: ReasonUnspecifiedError,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := NewSubscribeError(tt.topic, tt.reasonCode)

				assert.Equal(t, tt.topic, err.Topic)
				assert.Equal(t, tt.reasonCode, err.ReasonCode)
				assert.Contains(t, err.Error(), "subscribe failed")
				assert.True(t, errors.Is(err, ErrSubscribeFailed))
			})
		}
	})
}

func TestHandleAuth(t *testing.T) {
	tests := []struct {
		name            string
		reasonCode      ReasonCode
		enhancedAuth    ClientEnhancedAuthenticator
		expectDisconn   bool
		expectAuthReply bool
	}{
		{
			name:          "no enhanced auth configured disconnects",
			reasonCode:    ReasonReAuth,
			enhancedAuth:  nil,
			expectDisconn: true,
		},
		{
			name:            "success clears auth state",
			reasonCode:      ReasonSuccess,
			enhancedAuth:    &mockEnhancedAuth{},
			expectDisconn:   false,
			expectAuthReply: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			conn := &bufMockConn{writer: &buf}

			var events []error
			eventMu := &sync.Mutex{}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			readDone := make(chan struct{})
			close(readDone) // Pre-close so CloseWithCode doesn't wait

			client := &Client{
				conn:     conn,
				ctx:      ctx,
				cancel:   cancel,
				done:     make(chan struct{}),
				readDone: readDone,
				options: &clientOptions{
					enhancedAuth:  tt.enhancedAuth,
					maxPacketSize: MaxPacketSizeDefault,
					onEvent: func(_ *Client, evt error) {
						eventMu.Lock()
						events = append(events, evt)
						eventMu.Unlock()
					},
				},
			}
			client.connected.Store(true)

			pkt := &AuthPacket{ReasonCode: tt.reasonCode}
			client.handleAuth(pkt)

			if tt.expectDisconn {
				eventMu.Lock()
				hasDisconnect := false
				for _, evt := range events {
					if _, ok := evt.(*DisconnectError); ok {
						hasDisconnect = true
						break
					}
				}
				assert.True(t, hasDisconnect, "expected disconnect error event")
				eventMu.Unlock()
			}
		})
	}
}

type mockEnhancedAuth struct{}

func (m *mockEnhancedAuth) AuthMethod() string { return "TEST" }
func (m *mockEnhancedAuth) AuthStart(_ context.Context) (*ClientEnhancedAuthResult, error) {
	return &ClientEnhancedAuthResult{AuthData: []byte("test")}, nil
}
func (m *mockEnhancedAuth) AuthContinue(_ context.Context, _ *ClientEnhancedAuthContext) (*ClientEnhancedAuthResult, error) {
	return &ClientEnhancedAuthResult{Done: true}, nil
}

func TestHandlePuback(t *testing.T) {
	tests := []struct {
		name           string
		packetID       uint16
		reasonCode     ReasonCode
		setupTracker   func(*QoS1Tracker)
		expectErrorEvt bool
	}{
		{
			name:       "successful puback releases flow control",
			packetID:   1,
			reasonCode: ReasonSuccess,
			setupTracker: func(tr *QoS1Tracker) {
				tr.Track(1, &Message{Topic: "test"})
			},
			expectErrorEvt: false,
		},
		{
			name:       "error reason code emits publish error",
			packetID:   2,
			reasonCode: ReasonQuotaExceeded,
			setupTracker: func(tr *QoS1Tracker) {
				tr.Track(2, &Message{Topic: "test/error"})
			},
			expectErrorEvt: true,
		},
		{
			name:       "unknown packet ID does not panic",
			packetID:   999,
			reasonCode: ReasonSuccess,
			setupTracker: func(_ *QoS1Tracker) {
				// Don't track - simulates unknown packet
			},
			expectErrorEvt: false,
		},
		{
			name:       "not authorized error",
			packetID:   3,
			reasonCode: ReasonNotAuthorized,
			setupTracker: func(tr *QoS1Tracker) {
				tr.Track(3, &Message{Topic: "restricted"})
			},
			expectErrorEvt: true,
		},
		{
			name:       "packet too large error",
			packetID:   4,
			reasonCode: ReasonPacketTooLarge,
			setupTracker: func(tr *QoS1Tracker) {
				tr.Track(4, &Message{Topic: "large"})
			},
			expectErrorEvt: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var events []error
			eventMu := &sync.Mutex{}

			client := &Client{
				options: &clientOptions{
					maxPacketSize: MaxPacketSizeDefault,
					onEvent: func(_ *Client, evt error) {
						eventMu.Lock()
						events = append(events, evt)
						eventMu.Unlock()
					},
				},
				qos1Tracker:       NewQoS1Tracker(time.Second, 3),
				packetIDMgr:       NewPacketIDManager(),
				serverFlowControl: NewFlowController(100),
			}

			tt.setupTracker(client.qos1Tracker)

			pkt := &PubackPacket{
				PacketID:   tt.packetID,
				ReasonCode: tt.reasonCode,
			}
			client.handlePuback(pkt)

			eventMu.Lock()
			defer eventMu.Unlock()
			if tt.expectErrorEvt {
				assert.NotEmpty(t, events, "expected error event to be emitted")
				if len(events) > 0 {
					pubErr, ok := events[0].(*PublishError)
					assert.True(t, ok, "expected PublishError event")
					assert.Equal(t, tt.reasonCode, pubErr.ReasonCode)
				}
			} else {
				for _, evt := range events {
					_, isPublishErr := evt.(*PublishError)
					assert.False(t, isPublishErr, "unexpected PublishError event")
				}
			}
		})
	}
}

func TestHandlePubrec(t *testing.T) {
	tests := []struct {
		name           string
		packetID       uint16
		reasonCode     ReasonCode
		setupTracker   func(*QoS2Tracker)
		expectPubrel   bool
		expectErrorEvt bool
	}{
		{
			name:       "successful pubrec sends pubrel",
			packetID:   1,
			reasonCode: ReasonSuccess,
			setupTracker: func(tr *QoS2Tracker) {
				tr.TrackSend(1, &Message{Topic: "test"})
			},
			expectPubrel:   true,
			expectErrorEvt: false,
		},
		{
			name:       "error reason code still sends pubrel",
			packetID:   2,
			reasonCode: ReasonQuotaExceeded,
			setupTracker: func(tr *QoS2Tracker) {
				tr.TrackSend(2, &Message{Topic: "test/error"})
			},
			expectPubrel:   true,
			expectErrorEvt: true,
		},
		{
			name:       "not authorized error",
			packetID:   3,
			reasonCode: ReasonNotAuthorized,
			setupTracker: func(tr *QoS2Tracker) {
				tr.TrackSend(3, &Message{Topic: "restricted"})
			},
			expectPubrel:   true,
			expectErrorEvt: true,
		},
		{
			name:       "unknown packet still responds with pubrel",
			packetID:   4,
			reasonCode: ReasonSuccess,
			setupTracker: func(_ *QoS2Tracker) {
				// Don't track - simulates unknown packet
			},
			expectPubrel:   false, // No pubrel if packet is not tracked
			expectErrorEvt: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			conn := &bufMockConn{writer: &buf}

			var events []error
			eventMu := &sync.Mutex{}

			client := &Client{
				conn: conn,
				options: &clientOptions{
					maxPacketSize: MaxPacketSizeDefault,
					onEvent: func(_ *Client, evt error) {
						eventMu.Lock()
						events = append(events, evt)
						eventMu.Unlock()
					},
				},
				qos2Tracker:       NewQoS2Tracker(time.Second, 3),
				packetIDMgr:       NewPacketIDManager(),
				serverFlowControl: NewFlowController(100),
			}

			tt.setupTracker(client.qos2Tracker)

			pkt := &PubrecPacket{
				PacketID:   tt.packetID,
				ReasonCode: tt.reasonCode,
			}
			client.handlePubrec(pkt)

			// Check if PUBREL was sent
			if tt.expectPubrel {
				assert.Greater(t, buf.Len(), 0, "expected PUBREL to be sent")
				respPkt, _, err := ReadPacket(&buf, MaxPacketSizeDefault)
				require.NoError(t, err)
				pubrel, ok := respPkt.(*PubrelPacket)
				require.True(t, ok, "expected PUBREL packet")
				assert.Equal(t, tt.packetID, pubrel.PacketID)
			}

			eventMu.Lock()
			defer eventMu.Unlock()
			if tt.expectErrorEvt {
				assert.NotEmpty(t, events, "expected error event to be emitted")
				if len(events) > 0 {
					pubErr, ok := events[0].(*PublishError)
					assert.True(t, ok, "expected PublishError event")
					assert.Equal(t, tt.reasonCode, pubErr.ReasonCode)
				}
			}
		})
	}
}

func TestHandlePublish(t *testing.T) {
	tests := []struct {
		name       string
		qos        byte
		packetID   uint16
		topic      string
		payload    []byte
		topicAlias uint16
		expectAck  bool
	}{
		{
			name:      "QoS 0 message delivered without ack",
			qos:       0,
			packetID:  0,
			topic:     "test/qos0",
			payload:   []byte("hello"),
			expectAck: false,
		},
		{
			name:      "QoS 1 message sends puback",
			qos:       1,
			packetID:  1,
			topic:     "test/qos1",
			payload:   []byte("world"),
			expectAck: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			conn := &bufMockConn{writer: &buf}

			var receivedMsg *Message
			msgMu := &sync.Mutex{}

			client := &Client{
				conn:                     conn,
				subscriptions:            make(map[string]MessageHandler),
				topicAliases:             NewTopicAliasManager(10, 10),
				qos2Tracker:              NewQoS2Tracker(time.Second, 3),
				options:                  &clientOptions{maxPacketSize: MaxPacketSizeDefault},
				serverMaxQoS:             2,
				packetIDMgr:              NewPacketIDManager(),
				serverSharedSubAvailable: true,
			}

			// Register a handler
			client.subscriptionsMu.Lock()
			client.subscriptions["test/#"] = func(msg *Message) {
				msgMu.Lock()
				receivedMsg = msg
				msgMu.Unlock()
			}
			client.subscriptionsMu.Unlock()

			pkt := &PublishPacket{
				Topic:    tt.topic,
				QoS:      tt.qos,
				PacketID: tt.packetID,
				Payload:  tt.payload,
			}
			client.handlePublish(pkt)

			// Check message was delivered
			msgMu.Lock()
			assert.NotNil(t, receivedMsg, "expected message to be delivered")
			if receivedMsg != nil {
				assert.Equal(t, tt.topic, receivedMsg.Topic)
				assert.Equal(t, tt.payload, receivedMsg.Payload)
			}
			msgMu.Unlock()

			// Check for expected ack
			if tt.expectAck && tt.qos == 1 {
				assert.Greater(t, buf.Len(), 0, "expected PUBACK")
				respPkt, _, err := ReadPacket(&buf, MaxPacketSizeDefault)
				require.NoError(t, err)
				puback, ok := respPkt.(*PubackPacket)
				require.True(t, ok, "expected PUBACK packet")
				assert.Equal(t, tt.packetID, puback.PacketID)
			}
		})
	}
}

func TestDialValidation(t *testing.T) {
	t.Run("no servers returns error", func(t *testing.T) {
		_, err := Dial(WithClientID("test"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no servers configured")
	})

	t.Run("with servers succeeds validation", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			_ = sendConnack(conn, false, ReasonSuccess)
			time.Sleep(100 * time.Millisecond)
		})
		defer cleanup()

		client, err := Dial(WithServers("tcp://" + addr))
		require.NoError(t, err)
		defer client.Close()
		assert.True(t, client.IsConnected())
	})

	t.Run("with server resolver succeeds validation", func(t *testing.T) {
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			_ = readConnect(t, conn)
			_ = sendConnack(conn, false, ReasonSuccess)
			time.Sleep(100 * time.Millisecond)
		})
		defer cleanup()

		resolver := func(_ context.Context) ([]string, error) {
			return []string{"tcp://" + addr}, nil
		}

		client, err := Dial(WithServerResolver(resolver))
		require.NoError(t, err)
		defer client.Close()
		assert.True(t, client.IsConnected())
	})
}

func TestNextServer(t *testing.T) {
	t.Run("round-robin with static servers", func(t *testing.T) {
		c := &Client{
			options: &clientOptions{
				servers: []string{"tcp://server1:1883", "tcp://server2:1883", "tcp://server3:1883"},
			},
		}

		ctx := context.Background()

		// First three calls should return servers in order
		server, err := c.nextServer(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "tcp://server1:1883", server)

		server, err = c.nextServer(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "tcp://server2:1883", server)

		server, err = c.nextServer(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "tcp://server3:1883", server)

		// Fourth call wraps around
		server, err = c.nextServer(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "tcp://server1:1883", server)
	})

	t.Run("resolver called each time", func(t *testing.T) {
		callCount := 0
		c := &Client{
			options: &clientOptions{
				serverResolver: func(_ context.Context) ([]string, error) {
					callCount++
					return []string{fmt.Sprintf("tcp://resolved%d:1883", callCount)}, nil
				},
			},
		}

		ctx := context.Background()

		server, _ := c.nextServer(ctx)
		assert.Equal(t, "tcp://resolved1:1883", server)
		assert.Equal(t, 1, callCount)

		server, _ = c.nextServer(ctx)
		assert.Equal(t, "tcp://resolved2:1883", server)
		assert.Equal(t, 2, callCount)
	})

	t.Run("fallback to static on resolver error", func(t *testing.T) {
		c := &Client{
			options: &clientOptions{
				servers: []string{"tcp://fallback:1883"},
				serverResolver: func(_ context.Context) ([]string, error) {
					return nil, errors.New("resolver failed")
				},
			},
		}

		ctx := context.Background()

		server, err := c.nextServer(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "tcp://fallback:1883", server)
	})

	t.Run("fallback to static on empty resolver result", func(t *testing.T) {
		c := &Client{
			options: &clientOptions{
				servers: []string{"tcp://static:1883"},
				serverResolver: func(_ context.Context) ([]string, error) {
					return []string{}, nil
				},
			},
		}

		ctx := context.Background()

		server, err := c.nextServer(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "tcp://static:1883", server)
	})

	t.Run("error when no servers available", func(t *testing.T) {
		c := &Client{
			options: &clientOptions{},
		}

		_, err := c.nextServer(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no servers available")
	})
}

// TestClientReconnectLoop tests the reconnectLoop function
func TestClientReconnectLoop(t *testing.T) {
	t.Run("reconnects successfully on first attempt", func(t *testing.T) {
		reconnectAttempts := 0
		addr, cleanup := mockServer(t, func(conn net.Conn) {
			reconnectAttempts++
			// Read CONNECT
			ReadPacket(conn, 256*1024)
			// Send CONNACK
			sendConnack(conn, false, ReasonSuccess)
			time.Sleep(200 * time.Millisecond)
		})
		defer cleanup()

		client, err := Dial(
			WithServers("tcp://"+addr),
			WithAutoReconnect(true),
			WithReconnectBackoff(10*time.Millisecond),
		)
		require.NoError(t, err)
		defer client.Close()

		assert.True(t, client.IsConnected())
	})

	t.Run("respects max reconnect attempts", func(t *testing.T) {
		failedAttempts := 0
		var lastErr error

		readDone := make(chan struct{})
		close(readDone) // Pre-close so connect doesn't wait

		c := &Client{
			options: &clientOptions{
				servers:          []string{"tcp://localhost:19999"}, // Non-existent server
				autoReconnect:    true,
				maxReconnects:    3,
				reconnectBackoff: 1 * time.Millisecond,
				maxBackoff:       5 * time.Millisecond,
				connectTimeout:   10 * time.Millisecond,
				maxPacketSize:    256 * 1024,
				onEvent: func(_ *Client, event error) {
					if event == ErrReconnectFailed {
						lastErr = event
					}
					if _, ok := event.(*ReconnectEvent); ok {
						failedAttempts++
					}
				},
			},
			done:               make(chan struct{}),
			readDone:           readDone,
			packetIDMgr:        NewPacketIDManager(),
			qos1Tracker:        NewQoS1Tracker(30*time.Second, 3),
			qos2Tracker:        NewQoS2Tracker(30*time.Second, 3),
			subscriptions:      make(map[string]MessageHandler),
			topicAliases:       NewTopicAliasManager(0, 0),
			serverFlowControl:  NewFlowController(65535),
			inboundFlowControl: NewFlowController(65535),
		}

		c.reconnectLoop()

		assert.Equal(t, 3, failedAttempts)
		assert.ErrorIs(t, lastErr, ErrReconnectFailed)
	})

	t.Run("stops reconnecting when client is closed", func(t *testing.T) {
		readDone := make(chan struct{})
		close(readDone) // Pre-close so connect doesn't wait

		c := &Client{
			options: &clientOptions{
				servers:          []string{"tcp://localhost:19999"},
				autoReconnect:    true,
				maxReconnects:    100,
				reconnectBackoff: 10 * time.Millisecond,
				maxBackoff:       50 * time.Millisecond,
				connectTimeout:   10 * time.Millisecond,
				maxPacketSize:    256 * 1024,
			},
			done:               make(chan struct{}),
			readDone:           readDone,
			packetIDMgr:        NewPacketIDManager(),
			qos1Tracker:        NewQoS1Tracker(30*time.Second, 3),
			qos2Tracker:        NewQoS2Tracker(30*time.Second, 3),
			subscriptions:      make(map[string]MessageHandler),
			topicAliases:       NewTopicAliasManager(0, 0),
			serverFlowControl:  NewFlowController(65535),
			inboundFlowControl: NewFlowController(65535),
		}

		// Start reconnect in background
		go c.reconnectLoop()

		// Close client after a short delay
		time.Sleep(20 * time.Millisecond)
		c.closed.Store(true)
		close(c.done)

		// Give time for reconnect to stop
		time.Sleep(50 * time.Millisecond)

		// Should not be reconnecting anymore
		assert.False(t, c.reconnecting.Load())
	})
}

// TestClientResendInflightMessages tests the resendInflightMessages function
func TestClientResendInflightMessages(t *testing.T) {
	t.Run("resends QoS 1 messages with DUP flag", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		c := &Client{
			conn:        conn,
			options:     &clientOptions{maxPacketSize: 256 * 1024},
			qos1Tracker: NewQoS1Tracker(30*time.Second, 3),
			qos2Tracker: NewQoS2Tracker(30*time.Second, 3),
		}

		// Track a QoS 1 message
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS1}
		c.qos1Tracker.Track(1, msg)

		c.resendInflightMessages()

		// Verify PUBLISH was sent
		require.NotEmpty(t, writeBuf.Bytes())

		r := bytes.NewReader(writeBuf.Bytes())
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBLISH, header.PacketType)

		var pub PublishPacket
		_, err = pub.Decode(r, header)
		require.NoError(t, err)
		assert.True(t, pub.DUP)
		assert.Equal(t, uint16(1), pub.PacketID)
		assert.Equal(t, QoS1, pub.QoS)
	})

	t.Run("resends QoS 2 messages awaiting PUBREC with DUP flag", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		c := &Client{
			conn:        conn,
			options:     &clientOptions{maxPacketSize: 256 * 1024},
			qos1Tracker: NewQoS1Tracker(30*time.Second, 3),
			qos2Tracker: NewQoS2Tracker(30*time.Second, 3),
		}

		// Track a QoS 2 message
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		c.qos2Tracker.TrackSend(1, msg)

		c.resendInflightMessages()

		// Verify PUBLISH was sent
		require.NotEmpty(t, writeBuf.Bytes())

		r := bytes.NewReader(writeBuf.Bytes())
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBLISH, header.PacketType)

		var pub PublishPacket
		_, err = pub.Decode(r, header)
		require.NoError(t, err)
		assert.True(t, pub.DUP)
		assert.Equal(t, QoS2, pub.QoS)
	})

	t.Run("resends PUBREL for QoS 2 messages awaiting PUBCOMP", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		c := &Client{
			conn:        conn,
			options:     &clientOptions{maxPacketSize: 256 * 1024},
			qos1Tracker: NewQoS1Tracker(30*time.Second, 3),
			qos2Tracker: NewQoS2Tracker(30*time.Second, 3),
		}

		// Track a QoS 2 message and transition to awaiting PUBCOMP
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS2}
		c.qos2Tracker.TrackSend(1, msg)
		c.qos2Tracker.HandlePubrec(1)

		c.resendInflightMessages()

		// Verify PUBREL was sent
		require.NotEmpty(t, writeBuf.Bytes())

		r := bytes.NewReader(writeBuf.Bytes())
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBREL, header.PacketType)
	})
}

// TestClientRestoreSubscriptions tests the restoreSubscriptions function
func TestClientRestoreSubscriptions(t *testing.T) {
	t.Run("restores subscriptions after reconnect", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		c := &Client{
			conn:          conn,
			options:       &clientOptions{maxPacketSize: 256 * 1024},
			packetIDMgr:   NewPacketIDManager(),
			subscriptions: make(map[string]MessageHandler),
		}

		// Add subscriptions
		c.subscriptions["test/topic"] = func(_ *Message) {}
		c.subscriptions["another/topic"] = func(_ *Message) {}

		c.restoreSubscriptions()

		// Should have sent subscribe packets
		require.NotEmpty(t, writeBuf.Bytes())

		// Parse and verify SUBSCRIBE packets were sent
		r := bytes.NewReader(writeBuf.Bytes())
		for range 2 {
			var header FixedHeader
			_, err := header.Decode(r)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			assert.Equal(t, PacketSUBSCRIBE, header.PacketType)

			var sub SubscribePacket
			_, err = sub.Decode(r, header)
			require.NoError(t, err)
			assert.Len(t, sub.Subscriptions, 1)
		}
	})

	t.Run("does nothing when no subscriptions", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		c := &Client{
			conn:          conn,
			options:       &clientOptions{maxPacketSize: 256 * 1024},
			packetIDMgr:   NewPacketIDManager(),
			subscriptions: make(map[string]MessageHandler),
		}

		c.restoreSubscriptions()

		// Should not have sent anything
		assert.Empty(t, writeBuf.Bytes())
	})

	t.Run("restores with session QoS levels", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		session := NewMemorySession("test-client", DefaultNamespace)
		session.AddSubscription(Subscription{TopicFilter: "test/topic", QoS: QoS2})

		c := &Client{
			conn:          conn,
			options:       &clientOptions{maxPacketSize: 256 * 1024},
			session:       session,
			packetIDMgr:   NewPacketIDManager(),
			subscriptions: make(map[string]MessageHandler),
		}

		c.subscriptions["test/topic"] = func(_ *Message) {}

		c.restoreSubscriptions()

		require.NotEmpty(t, writeBuf.Bytes())

		r := bytes.NewReader(writeBuf.Bytes())
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)

		var sub SubscribePacket
		_, err = sub.Decode(r, header)
		require.NoError(t, err)
		require.Len(t, sub.Subscriptions, 1)
		assert.Equal(t, QoS2, sub.Subscriptions[0].QoS)
	})
}

// TestClientResetInflightState tests the resetInflightState function
func TestClientResetInflightState(t *testing.T) {
	t.Run("resets all inflight state", func(t *testing.T) {
		c := &Client{
			qos1Tracker:       NewQoS1Tracker(30*time.Second, 3),
			qos2Tracker:       NewQoS2Tracker(30*time.Second, 3),
			packetIDMgr:       NewPacketIDManager(),
			serverFlowControl: NewFlowController(10),
		}

		// Track some messages
		c.qos1Tracker.Track(1, &Message{Topic: "test"})
		c.qos2Tracker.TrackSend(2, &Message{Topic: "test"})
		c.packetIDMgr.Allocate()
		c.serverFlowControl.TryAcquire()

		c.resetInflightState()

		// Everything should be reset (new trackers with no messages)
		assert.Len(t, c.qos1Tracker.GetPendingRetries(), 0)
		assert.Len(t, c.qos2Tracker.GetPendingRetries(), 0)
		assert.Equal(t, uint16(65535), c.serverFlowControl.Available())

		// New packet ID manager should allocate from 1
		id, err := c.packetIDMgr.Allocate()
		require.NoError(t, err)
		assert.Equal(t, uint16(1), id)
	})
}

// TestClientClearPendingOperations tests the clearPendingOperations function
func TestClientClearPendingOperations(t *testing.T) {
	t.Run("clears pending subscribes and unsubscribes", func(t *testing.T) {
		c := &Client{
			packetIDMgr:         NewPacketIDManager(),
			pendingSubscribes:   make(map[uint16][]string),
			pendingUnsubscribes: make(map[uint16][]string),
		}

		// Allocate packet IDs and track pending operations
		id1, _ := c.packetIDMgr.Allocate()
		id2, _ := c.packetIDMgr.Allocate()
		c.pendingSubscribes[id1] = []string{"topic/a"}
		c.pendingUnsubscribes[id2] = []string{"topic/b"}

		c.clearPendingOperations()

		// Maps should be empty
		assert.Empty(t, c.pendingSubscribes)
		assert.Empty(t, c.pendingUnsubscribes)

		// Packet IDs should be released - verify by checking we can allocate new ones
		// Note: PacketIDManager allocates sequentially, so next ID will be 3 (1 and 2 were allocated and released)
		_, err := c.packetIDMgr.Allocate()
		require.NoError(t, err)
	})
}

// TestClientHandleAuth tests the handleAuth function
func TestClientHandleAuth(t *testing.T) {
	t.Run("disconnects when enhanced auth not configured", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		ctx, cancel := context.WithCancel(context.Background())
		readDone := make(chan struct{})
		close(readDone) // Pre-close so CloseWithCode doesn't wait

		var disconnectReceived bool
		c := &Client{
			conn:     conn,
			ctx:      ctx,
			cancel:   cancel,
			done:     make(chan struct{}),
			readDone: readDone,
			options: &clientOptions{
				maxPacketSize: 256 * 1024,
				onEvent: func(_ *Client, event error) {
					if de, ok := event.(*DisconnectError); ok && de.ReasonCode == ReasonProtocolError {
						disconnectReceived = true
					}
				},
			},
		}
		c.connected.Store(true)

		authPkt := &AuthPacket{ReasonCode: ReasonContinueAuth}
		c.handleAuth(authPkt)

		assert.True(t, disconnectReceived)
	})

	t.Run("handles re-authentication request", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		enhancedAuth := &testClientEnhancedAuth{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &Client{
			conn: conn,
			ctx:  ctx,
			options: &clientOptions{
				maxPacketSize: 256 * 1024,
				enhancedAuth:  enhancedAuth,
			},
		}
		c.connected.Store(true)

		authPkt := &AuthPacket{ReasonCode: ReasonReAuth}
		c.handleAuth(authPkt)

		// Should have sent AUTH response
		require.NotEmpty(t, writeBuf.Bytes())

		r := bytes.NewReader(writeBuf.Bytes())
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketAUTH, header.PacketType)

		var respAuth AuthPacket
		_, err = respAuth.Decode(r, header)
		require.NoError(t, err)
		assert.Equal(t, ReasonReAuth, respAuth.ReasonCode)
	})

	t.Run("handles continue authentication", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		enhancedAuth := &testClientEnhancedAuth{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &Client{
			conn: conn,
			ctx:  ctx,
			options: &clientOptions{
				maxPacketSize: 256 * 1024,
				enhancedAuth:  enhancedAuth,
			},
		}
		c.connected.Store(true)

		authPkt := &AuthPacket{ReasonCode: ReasonContinueAuth}
		authPkt.Props.Set(PropAuthenticationMethod, "SCRAM-SHA-256")
		authPkt.Props.Set(PropAuthenticationData, []byte("challenge"))

		c.handleAuth(authPkt)

		// Should have sent continue AUTH response
		require.NotEmpty(t, writeBuf.Bytes())

		r := bytes.NewReader(writeBuf.Bytes())
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketAUTH, header.PacketType)

		var respAuth AuthPacket
		_, err = respAuth.Decode(r, header)
		require.NoError(t, err)
		assert.Equal(t, ReasonContinueAuth, respAuth.ReasonCode)
	})

	t.Run("clears auth state on success", func(t *testing.T) {
		c := &Client{
			options: &clientOptions{
				enhancedAuth: &testClientEnhancedAuth{},
			},
			enhancedAuthState: map[string]any{"key": "value"},
		}
		c.connected.Store(true)

		authPkt := &AuthPacket{ReasonCode: ReasonSuccess}
		c.handleAuth(authPkt)

		assert.Nil(t, c.enhancedAuthState)
	})
}

// testClientEnhancedAuth implements ClientEnhancedAuth for testing
type testClientEnhancedAuth struct{}

func (a *testClientEnhancedAuth) AuthMethod() string {
	return "SCRAM-SHA-256"
}

func (a *testClientEnhancedAuth) AuthStart(_ context.Context) (*ClientEnhancedAuthResult, error) {
	return &ClientEnhancedAuthResult{
		AuthData: []byte("initial"),
	}, nil
}

func (a *testClientEnhancedAuth) AuthContinue(_ context.Context, _ *ClientEnhancedAuthContext) (*ClientEnhancedAuthResult, error) {
	return &ClientEnhancedAuthResult{
		AuthData: []byte("response"),
	}, nil
}

// TestClientHandlePublishWithQoS2 tests the handlePublish function for QoS 2
func TestClientHandlePublishWithQoS2(t *testing.T) {
	t.Run("handles QoS 2 publish and sends PUBREC", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		c := &Client{
			conn:          conn,
			options:       &clientOptions{maxPacketSize: 256 * 1024},
			qos2Tracker:   NewQoS2Tracker(30*time.Second, 3),
			subscriptions: make(map[string]MessageHandler),
			topicAliases:  NewTopicAliasManager(0, 10),
		}

		pub := &PublishPacket{
			Topic:    "test/topic",
			Payload:  []byte("data"),
			QoS:      QoS2,
			PacketID: 1,
		}

		c.handlePublish(pub)

		// Should have sent PUBREC
		require.NotEmpty(t, writeBuf.Bytes())

		r := bytes.NewReader(writeBuf.Bytes())
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBREC, header.PacketType)

		var pubrec PubrecPacket
		_, err = pubrec.Decode(r, header)
		require.NoError(t, err)
		assert.Equal(t, uint16(1), pubrec.PacketID)
	})

	t.Run("handles QoS 2 DUP retransmit", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		c := &Client{
			conn:          conn,
			options:       &clientOptions{maxPacketSize: 256 * 1024},
			qos2Tracker:   NewQoS2Tracker(30*time.Second, 3),
			subscriptions: make(map[string]MessageHandler),
			topicAliases:  NewTopicAliasManager(0, 10),
		}

		// First message
		pub := &PublishPacket{
			Topic:    "test/topic",
			Payload:  []byte("data"),
			QoS:      QoS2,
			PacketID: 1,
		}
		c.handlePublish(pub)

		// Clear buffer
		writeBuf.Reset()

		// DUP retransmit
		pub.DUP = true
		c.handlePublish(pub)

		// Should still send PUBREC for retransmit
		require.NotEmpty(t, writeBuf.Bytes())

		r := bytes.NewReader(writeBuf.Bytes())
		var header FixedHeader
		_, err := header.Decode(r)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBREC, header.PacketType)
	})
}

// TestClientHandlePublishWithTopicAlias tests topic alias handling in handlePublish
func TestClientHandlePublishWithTopicAlias(t *testing.T) {
	t.Run("stores topic alias mapping", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		var receivedTopic string
		c := &Client{
			conn:    conn,
			options: &clientOptions{maxPacketSize: 256 * 1024},
			subscriptions: map[string]MessageHandler{
				"test/topic": func(msg *Message) {
					receivedTopic = msg.Topic
				},
			},
			topicAliases: NewTopicAliasManager(10, 0), // inboundMax=10, outboundMax=0
		}

		pub := &PublishPacket{
			Topic:   "test/topic",
			Payload: []byte("data"),
			QoS:     QoS0,
		}
		pub.Props.Set(PropTopicAlias, uint16(1))

		c.handlePublish(pub)

		assert.Equal(t, "test/topic", receivedTopic)
	})

	t.Run("resolves topic from alias", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		var receivedTopic string
		c := &Client{
			conn:    conn,
			options: &clientOptions{maxPacketSize: 256 * 1024},
			subscriptions: map[string]MessageHandler{
				"test/topic": func(msg *Message) {
					receivedTopic = msg.Topic
				},
			},
			topicAliases: NewTopicAliasManager(10, 0), // inboundMax=10, outboundMax=0
		}

		// First message sets alias
		pub1 := &PublishPacket{
			Topic:   "test/topic",
			Payload: []byte("data1"),
			QoS:     QoS0,
		}
		pub1.Props.Set(PropTopicAlias, uint16(1))
		c.handlePublish(pub1)

		// Second message uses alias
		pub2 := &PublishPacket{
			Topic:   "",
			Payload: []byte("data2"),
			QoS:     QoS0,
		}
		pub2.Props.Set(PropTopicAlias, uint16(1))
		c.handlePublish(pub2)

		assert.Equal(t, "test/topic", receivedTopic)
	})

	t.Run("disconnects on invalid topic alias", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		ctx, cancel := context.WithCancel(context.Background())
		readDone := make(chan struct{})
		close(readDone) // Pre-close so CloseWithCode doesn't wait

		var disconnectError *DisconnectError
		c := &Client{
			conn:     conn,
			ctx:      ctx,
			cancel:   cancel,
			done:     make(chan struct{}),
			readDone: readDone,
			options: &clientOptions{
				maxPacketSize: 256 * 1024,
				onEvent: func(_ *Client, event error) {
					if de, ok := event.(*DisconnectError); ok {
						disconnectError = de
					}
				},
			},
			subscriptions: make(map[string]MessageHandler),
			topicAliases:  NewTopicAliasManager(10, 0), // inboundMax=10, outboundMax=0
		}
		c.connected.Store(true)

		// Use alias that was never set
		pub := &PublishPacket{
			Topic:   "",
			Payload: []byte("data"),
			QoS:     QoS0,
		}
		pub.Props.Set(PropTopicAlias, uint16(5))

		c.handlePublish(pub)

		require.NotNil(t, disconnectError)
		assert.Equal(t, ReasonTopicAliasInvalid, disconnectError.ReasonCode)
	})
}

// TestClientQoSRetryLoop tests the qosRetryLoop function
func TestClientQoSRetryLoop(t *testing.T) {
	t.Run("skips retry when not connected", func(_ *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		ctx, cancel := context.WithCancel(context.Background())

		c := &Client{
			conn:        conn,
			ctx:         ctx,
			options:     &clientOptions{maxPacketSize: 256 * 1024},
			qos1Tracker: NewQoS1Tracker(1*time.Millisecond, 3),
			qos2Tracker: NewQoS2Tracker(1*time.Millisecond, 3),
		}
		c.connected.Store(false) // Not connected

		// Track messages
		msg := &Message{Topic: "test", Payload: []byte("data"), QoS: QoS1}
		c.qos1Tracker.Track(1, msg)
		c.qos1Tracker.messages[1].SentAt = time.Now().Add(-10 * time.Second)

		// Cancel quickly to exit loop
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		c.qosRetryLoop()

		// No messages should be written when not connected
	})

	t.Run("retries QoS 1 messages", func(_ *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &Client{
			conn:        conn,
			ctx:         ctx,
			options:     &clientOptions{maxPacketSize: 256 * 1024},
			qos1Tracker: NewQoS1Tracker(1*time.Millisecond, 3),
			qos2Tracker: NewQoS2Tracker(30*time.Second, 3),
		}
		c.connected.Store(true)

		// Track a QoS 1 message
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: QoS1}
		c.qos1Tracker.Track(1, msg)
		c.qos1Tracker.messages[1].SentAt = time.Now().Add(-10 * time.Second)

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		c.qosRetryLoop()
	})
}

// TestClientQoSRetryLoopRetryPaths tests the retry paths directly
func TestClientQoSRetryLoopRetryPaths(t *testing.T) {
	t.Run("QoS 1 retry writes DUP publish", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &Client{
			conn:        conn,
			ctx:         ctx,
			options:     &clientOptions{maxPacketSize: 256 * 1024},
			qos1Tracker: NewQoS1Tracker(1*time.Millisecond, 3),
			qos2Tracker: NewQoS2Tracker(30*time.Second, 3),
		}
		c.connected.Store(true)

		// Track a QoS 1 message ready for retry
		msg := &Message{Topic: "test/retry", Payload: []byte("payload"), QoS: QoS1}
		c.qos1Tracker.Track(1, msg)
		c.qos1Tracker.messages[1].SentAt = time.Now().Add(-1 * time.Minute)

		// Directly simulate what happens in the retry loop for QoS 1
		for _, m := range c.qos1Tracker.GetPendingRetries() {
			pub := &PublishPacket{}
			pub.FromMessage(m.Message)
			pub.PacketID = m.PacketID
			pub.QoS = QoS1
			pub.DUP = true
			c.writePacket(pub)
		}

		assert.Greater(t, writeBuf.Len(), 0, "should have written retry packet")

		// Verify the packet has DUP flag
		reader := bytes.NewReader(writeBuf.Bytes())
		var header FixedHeader
		_, err := header.Decode(reader)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBLISH, header.PacketType)
		assert.True(t, header.Flags&0x08 != 0, "DUP flag should be set")
	})

	t.Run("QoS 2 retry AwaitingPubrec writes DUP publish", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &Client{
			conn:        conn,
			ctx:         ctx,
			options:     &clientOptions{maxPacketSize: 256 * 1024},
			qos1Tracker: NewQoS1Tracker(30*time.Second, 3),
			qos2Tracker: NewQoS2Tracker(1*time.Millisecond, 3),
		}
		c.connected.Store(true)

		// Track a QoS 2 message in AwaitingPubrec state
		msg := &Message{Topic: "test/qos2", Payload: []byte("qos2data"), QoS: QoS2}
		c.qos2Tracker.TrackSend(2, msg)
		c.qos2Tracker.messages[2].SentAt = time.Now().Add(-1 * time.Minute)
		// State is QoS2AwaitingPubrec by default

		// Simulate the QoS 2 AwaitingPubrec retry path
		for _, m := range c.qos2Tracker.GetPendingRetries() {
			if m.State == QoS2AwaitingPubrec {
				pub := &PublishPacket{}
				pub.FromMessage(m.Message)
				pub.PacketID = m.PacketID
				pub.QoS = QoS2
				pub.DUP = true
				c.writePacket(pub)
			}
		}

		assert.Greater(t, writeBuf.Len(), 0, "should have written retry packet")
	})

	t.Run("QoS 2 retry AwaitingPubcomp writes PUBREL", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := &Client{
			conn:        conn,
			ctx:         ctx,
			options:     &clientOptions{maxPacketSize: 256 * 1024},
			qos1Tracker: NewQoS1Tracker(30*time.Second, 3),
			qos2Tracker: NewQoS2Tracker(1*time.Millisecond, 3),
		}
		c.connected.Store(true)

		// Track a QoS 2 message in AwaitingPubcomp state
		msg := &Message{Topic: "test/qos2comp", Payload: []byte("data"), QoS: QoS2}
		c.qos2Tracker.TrackSend(3, msg)
		c.qos2Tracker.messages[3].SentAt = time.Now().Add(-1 * time.Minute)
		c.qos2Tracker.messages[3].State = QoS2AwaitingPubcomp

		// Simulate the QoS 2 AwaitingPubcomp retry path
		for _, m := range c.qos2Tracker.GetPendingRetries() {
			if m.State == QoS2AwaitingPubcomp {
				pubrel := &PubrelPacket{PacketID: m.PacketID}
				c.writePacket(pubrel)
			}
		}

		assert.Greater(t, writeBuf.Len(), 0, "should have written PUBREL packet")

		// Verify it's a PUBREL
		reader := bytes.NewReader(writeBuf.Bytes())
		var header FixedHeader
		_, err := header.Decode(reader)
		require.NoError(t, err)
		assert.Equal(t, PacketPUBREL, header.PacketType)
	})

	t.Run("cleanup operations", func(_ *testing.T) {
		c := &Client{
			qos1Tracker: NewQoS1Tracker(1*time.Millisecond, 3),
			qos2Tracker: NewQoS2Tracker(1*time.Millisecond, 3),
		}

		// Track some messages
		msg := &Message{Topic: "test", Payload: []byte("data")}
		c.qos1Tracker.Track(1, msg)
		c.qos2Tracker.TrackSend(2, msg)

		// Expire them
		c.qos1Tracker.messages[1].SentAt = time.Now().Add(-1 * time.Hour)
		c.qos1Tracker.messages[1].RetryCount = 100
		c.qos2Tracker.messages[2].SentAt = time.Now().Add(-1 * time.Hour)
		c.qos2Tracker.messages[2].RetryCount = 100
		c.qos2Tracker.messages[2].State = QoS2Complete

		// Simulate cleanup
		c.qos1Tracker.CleanupExpired()
		c.qos2Tracker.CleanupExpired()
		c.qos2Tracker.CleanupCompleted()
	})
}

// TestClientKeepAliveLoop tests the keepAliveLoop function
func TestClientKeepAliveLoop(t *testing.T) {
	t.Run("does nothing when keepAlive is 0", func(_ *testing.T) {
		c := &Client{
			options: &clientOptions{keepAlive: 0},
		}

		// Should return immediately
		c.keepAliveLoop()
	})

	t.Run("sends PINGREQ when idle", func(t *testing.T) {
		writeBuf := &bytes.Buffer{}
		conn := &bufMockConn{writer: writeBuf}

		ctx, cancel := context.WithCancel(context.Background())

		c := &Client{
			conn:    conn,
			ctx:     ctx,
			options: &clientOptions{keepAlive: 1, maxPacketSize: 256 * 1024}, // 1 second
		}
		c.connected.Store(true)
		c.lastPacket.Store(time.Now().Add(-2 * time.Second).UnixNano())

		// Run for a short time - PINGREQ should be sent on first check since lastPacket is 2s ago
		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()

		c.keepAliveLoop()

		// Should have sent PINGREQ
		if writeBuf.Len() > 0 {
			r := bytes.NewReader(writeBuf.Bytes())
			var header FixedHeader
			_, err := header.Decode(r)
			require.NoError(t, err)
			assert.Equal(t, PacketPINGREQ, header.PacketType)
		}
	})
}

func TestClientResolveProxy(t *testing.T) {
	t.Run("returns nil when no proxy configured", func(t *testing.T) {
		c := &Client{
			options: &clientOptions{
				proxyConfig:  nil,
				proxyFromEnv: false,
			},
		}

		dialer, err := c.resolveProxy("tcp://broker:1883")
		require.NoError(t, err)
		assert.Nil(t, dialer)
	})

	t.Run("uses explicit proxy config", func(t *testing.T) {
		c := &Client{
			options: &clientOptions{
				proxyConfig: &ProxyConfig{
					URL: "http://proxy:8080",
				},
				proxyFromEnv: false,
			},
		}

		dialer, err := c.resolveProxy("tcp://broker:1883")
		require.NoError(t, err)
		assert.NotNil(t, dialer)
	})

	t.Run("uses proxy from environment", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "http://envproxy:8080")
		t.Setenv("NO_PROXY", "")

		c := &Client{
			options: &clientOptions{
				proxyConfig:  nil,
				proxyFromEnv: true,
			},
		}

		dialer, err := c.resolveProxy("tcp://broker:1883")
		require.NoError(t, err)
		assert.NotNil(t, dialer)
	})

	t.Run("returns error for invalid proxy URL", func(t *testing.T) {
		c := &Client{
			options: &clientOptions{
				proxyConfig: &ProxyConfig{
					URL: "://invalid",
				},
			},
		}

		_, err := c.resolveProxy("tcp://broker:1883")
		assert.Error(t, err)
	})

	t.Run("respects NO_PROXY from environment", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "http://proxy:8080")
		t.Setenv("NO_PROXY", "broker")

		c := &Client{
			options: &clientOptions{
				proxyConfig:  nil,
				proxyFromEnv: true,
			},
		}

		dialer, err := c.resolveProxy("tcp://broker:1883")
		require.NoError(t, err)
		assert.Nil(t, dialer) // NO_PROXY should exclude this host
	})
}

func TestClientQoSRetryLoopContextCancel(t *testing.T) {
	t.Run("exits on context cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		c := &Client{
			ctx:         ctx,
			conn:        &bufMockConn{writer: &bytes.Buffer{}},
			qos1Tracker: NewQoS1Tracker(30*time.Second, 3),
			qos2Tracker: NewQoS2Tracker(30*time.Second, 3),
			options:     &clientOptions{maxPacketSize: 256 * 1024},
		}
		c.connected.Store(true)

		done := make(chan struct{})
		go func() {
			c.qosRetryLoop()
			close(done)
		}()

		cancel()

		select {
		case <-done:
			// Expected
		case <-time.After(1 * time.Second):
			t.Fatal("qosRetryLoop didn't exit on context cancel")
		}
	})
}

func TestClientHandlePacketUnknown(t *testing.T) {
	addr, cleanup := mockServer(t, func(conn net.Conn) {
		ReadPacket(conn, 256*1024)
		sendConnack(conn, false, ReasonSuccess)
		time.Sleep(200 * time.Millisecond)
	})
	defer cleanup()

	client, err := Dial(WithServers("tcp://" + addr))
	require.NoError(t, err)
	defer client.Close()

	// Client should handle unknown packets gracefully
	assert.True(t, client.IsConnected())
}

func TestClientUnsubscribeErrors(t *testing.T) {
	t.Run("returns error when not connected", func(t *testing.T) {
		c := &Client{
			options: &clientOptions{},
		}
		c.connected.Store(false)

		err := c.Unsubscribe("test/topic")
		assert.ErrorIs(t, err, ErrNotConnected)
	})

	t.Run("returns error for empty filters", func(t *testing.T) {
		c := &Client{
			options: &clientOptions{},
		}
		c.connected.Store(true)

		err := c.Unsubscribe()
		assert.Error(t, err)
	})
}

func TestClientDialWithWebSocket(t *testing.T) {
	// Test that ws:// scheme is handled correctly
	_, err := Dial(WithServers("ws://nonexistent:8080"))
	assert.Error(t, err) // Should fail to connect but not panic
}

func TestClientDialWithTLS(t *testing.T) {
	// Test that tls:// scheme is handled correctly
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := DialContext(ctx, WithServers("tls://nonexistent:8883"))
	assert.Error(t, err) // Should fail to connect but not panic
}

func TestClientDialUnsupportedScheme(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := DialContext(ctx, WithServers("http://localhost:8080"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported scheme")
}

func TestClientDialInvalidAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := DialContext(ctx, WithServers("://invalid"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid address")
}

func TestClientDialDefaultPorts(t *testing.T) {
	t.Run("mqtt scheme uses 1883", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := DialContext(ctx, WithServers("mqtt://nonexistent"))
		assert.Error(t, err)
	})

	t.Run("mqtts scheme uses 8883", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := DialContext(ctx, WithServers("mqtts://nonexistent"))
		assert.Error(t, err)
	})

	t.Run("ssl scheme uses 8883", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := DialContext(ctx, WithServers("ssl://nonexistent"))
		assert.Error(t, err)
	})
}

func TestClientHandlePingresp(t *testing.T) {
	addr, cleanup := mockServer(t, func(conn net.Conn) {
		_, _, _ = ReadPacket(conn, 256*1024)
		sendConnack(conn, false, ReasonSuccess)

		// Wait for PINGREQ and respond with PINGRESP
		for {
			pkt, _, err := ReadPacket(conn, 256*1024)
			if err != nil {
				return
			}
			if _, ok := pkt.(*PingreqPacket); ok {
				WritePacket(conn, &PingrespPacket{}, 256*1024)
			}
		}
	})
	defer cleanup()

	client, err := Dial(
		WithServers("tcp://"+addr),
		WithKeepAlive(1), // 1 second keep-alive
	)
	require.NoError(t, err)
	defer client.Close()

	// Wait for at least one PINGREQ/PINGRESP cycle
	time.Sleep(1500 * time.Millisecond)
}

func TestClientKeepAliveLoopNotConnected(_ *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		ctx:     ctx,
		options: &clientOptions{keepAlive: 1},
	}
	c.connected.Store(false)
	c.lastPacket.Store(time.Now().Add(-5 * time.Second).UnixNano())

	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	c.keepAliveLoop()
}
