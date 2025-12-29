package mqttv5

import (
	"context"
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

		err = client.Publish("test/topic", []byte("hello"), 0, false)
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

		err = client.Publish("test/topic", []byte("hello"), 1, false)
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

		err = client.Publish("test/topic", []byte("hello"), 0, false)
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

		err = client.Publish("", []byte("hello"), 0, false)
		assert.ErrorIs(t, err, ErrInvalidTopic)
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
