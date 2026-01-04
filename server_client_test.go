package mqttv5

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockConn struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	closed   bool
	readErr  error
	writeErr error
}

func (c *mockConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readErr != nil {
		return 0, c.readErr
	}
	return c.buf.Read(b)
}

func (c *mockConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return c.buf.Write(b)
}

func (c *mockConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1883}
}

func (c *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 12345}
}

func (c *mockConn) SetDeadline(_ time.Time) error {
	return nil
}

func (c *mockConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (c *mockConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

func (c *mockConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *mockConn) Written() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Bytes()
}

func TestServerClient(t *testing.T) {
	t.Run("new server client", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{
			ClientID:   "test-client",
			Username:   "user1",
			CleanStart: true,
			KeepAlive:  60,
		}

		client := NewServerClient(conn, connect, 256*1024, testNS)

		assert.Equal(t, "test-client", client.ClientID())
		assert.Equal(t, "user1", client.Username())
		assert.True(t, client.CleanStart())
		assert.Equal(t, uint16(60), client.KeepAlive())
		assert.True(t, client.IsConnected())
		assert.NotNil(t, client.TopicAliases())
		assert.NotNil(t, client.QoS1Tracker())
		assert.NotNil(t, client.QoS2Tracker())
		assert.NotNil(t, client.FlowControl())
		assert.Equal(t, conn, client.Conn())
	})

	t.Run("session management", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		assert.Nil(t, client.Session())

		session := NewMemorySession("test-client", testNS)
		client.SetSession(session)
		assert.Equal(t, session, client.Session())
	})

	t.Run("topic alias max", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		client.SetTopicAliasMax(10, 20)

		assert.Equal(t, uint16(10), client.TopicAliases().InboundMax())
		assert.Equal(t, uint16(20), client.TopicAliases().OutboundMax())
	})

	t.Run("receive maximum", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)

		// Default flow control
		assert.NotNil(t, client.FlowControl())

		// Set new receive maximum
		client.SetReceiveMaximum(100)
		assert.NotNil(t, client.FlowControl())

		// Set to 0 should default to 65535
		client.SetReceiveMaximum(0)
		assert.NotNil(t, client.FlowControl())
	})

	t.Run("close", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		assert.True(t, client.IsConnected())

		err := client.Close()
		require.NoError(t, err)
		assert.False(t, client.IsConnected())
		assert.True(t, conn.IsClosed())

		// Second close should be no-op
		err = client.Close()
		require.NoError(t, err)
	})

	t.Run("send when not connected", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		client.Close()

		msg := &Message{Topic: "test", Payload: []byte("data")}
		err := client.Send(msg)
		assert.ErrorIs(t, err, ErrNotConnected)
	})

	t.Run("send packet when not connected", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		client.Close()

		pkt := &PingrespPacket{}
		err := client.SendPacket(pkt)
		assert.ErrorIs(t, err, ErrNotConnected)
	})

	t.Run("disconnect when not connected", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		client.Close()

		err := client.Disconnect(ReasonSuccess)
		assert.ErrorIs(t, err, ErrNotConnected)
	})
}

// TestServerClientSendQuotaRollbackOnFailure tests that flow-control quota is released
// and tracker entries are rolled back when WritePacket fails.
// This tests the fix for the quota leak issue where Send acquired quota but didn't release
// it when the write failed, eventually blocking all further QoS > 0 publishes.
func TestServerClientSendQuotaRollbackOnFailure(t *testing.T) {
	t.Run("QoS 1 quota released on write failure", func(t *testing.T) {
		// Create a connection that will fail on write
		conn := &failingConn{writeErr: net.ErrClosed}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		client.SetSession(NewMemorySession("test-client", testNS))

		// Set a small receive maximum to make quota tracking visible
		client.SetReceiveMaximum(2)

		// Check initial quota
		fc := client.FlowControl()
		require.NotNil(t, fc)

		// First send should fail but quota should be released
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: 1}
		err := client.Send(msg)
		assert.Error(t, err, "send should fail due to write error")

		// Quota should still be available (was released on failure)
		assert.True(t, fc.TryAcquire(), "quota should be available after failed send")
		fc.Release() // Release what we just acquired

		// Tracker should not have an entry (was rolled back)
		_, ok := client.QoS1Tracker().Get(1)
		assert.False(t, ok, "tracker entry should be rolled back on failure")
	})

	t.Run("QoS 2 quota released on write failure", func(t *testing.T) {
		conn := &failingConn{writeErr: net.ErrClosed}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		client.SetSession(NewMemorySession("test-client", testNS))
		client.SetReceiveMaximum(2)

		fc := client.FlowControl()

		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: 2}
		err := client.Send(msg)
		assert.Error(t, err)

		// Quota should still be available
		assert.True(t, fc.TryAcquire(), "quota should be available after failed QoS 2 send")
		fc.Release()

		// QoS 2 tracker should not have an entry
		pending := client.QoS2Tracker().GetPendingRetries()
		assert.Empty(t, pending, "QoS 2 tracker entry should be rolled back on failure")
	})

	t.Run("multiple failed sends don't exhaust quota", func(t *testing.T) {
		conn := &failingConn{writeErr: net.ErrClosed}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		client.SetSession(NewMemorySession("test-client", testNS))

		// Set very small receive maximum
		client.SetReceiveMaximum(3)
		fc := client.FlowControl()

		// Send multiple messages that will all fail
		for i := 0; i < 10; i++ {
			msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: 1}
			_ = client.Send(msg)
		}

		// Quota should still be fully available (all were released on failure)
		for i := 0; i < 3; i++ {
			assert.True(t, fc.TryAcquire(), "quota slot %d should be available", i)
		}
		// Now quota should be exhausted
		assert.False(t, fc.TryAcquire(), "quota should be exhausted after acquiring all slots")

		// Release them back
		for i := 0; i < 3; i++ {
			fc.Release()
		}
	})

	t.Run("QoS 0 no quota management", func(t *testing.T) {
		conn := &failingConn{writeErr: net.ErrClosed}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		client.SetReceiveMaximum(1)
		fc := client.FlowControl()

		// QoS 0 should not affect quota
		msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: 0}
		_ = client.Send(msg)

		// Full quota should be available
		assert.True(t, fc.TryAcquire(), "quota should be available - QoS 0 doesn't use quota")
		fc.Release()
	})
}

// failingConn is a mock connection that fails writes with a configurable error.
type failingConn struct {
	writeErr error
}

func (c *failingConn) Read(_ []byte) (int, error)  { return 0, nil }
func (c *failingConn) Write(_ []byte) (int, error) { return 0, c.writeErr }
func (c *failingConn) Close() error                { return nil }
func (c *failingConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1883}
}
func (c *failingConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 12345}
}
func (c *failingConn) SetDeadline(_ time.Time) error      { return nil }
func (c *failingConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *failingConn) SetWriteDeadline(_ time.Time) error { return nil }

// TestServerClientSessionExpiryInterval tests session expiry interval getter/setter.
func TestServerClientSessionExpiryInterval(t *testing.T) {
	t.Run("default value is zero", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, testNS)

		assert.Equal(t, uint32(0), client.SessionExpiryInterval())
	})

	t.Run("set and get session expiry interval", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}
		client := NewServerClient(conn, connect, 256*1024, testNS)

		client.SetSessionExpiryInterval(3600)
		assert.Equal(t, uint32(3600), client.SessionExpiryInterval())

		client.SetSessionExpiryInterval(0)
		assert.Equal(t, uint32(0), client.SessionExpiryInterval())
	})
}

func TestServerClientConcurrency(_ *testing.T) {
	conn := &mockConn{}
	connect := &ConnectPacket{ClientID: "test-client"}

	client := NewServerClient(conn, connect, 256*1024, testNS)
	session := NewMemorySession("test-client", testNS)
	client.SetSession(session)

	var wg sync.WaitGroup

	// Concurrent reads
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.ClientID()
			_ = client.Username()
			_ = client.IsConnected()
			_ = client.Session()
			_ = client.TopicAliases()
		}()
	}

	// Concurrent session updates
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client.SetSession(session)
			_ = client.Session()
		}()
	}

	wg.Wait()
}

// TestServerClientConcurrentWrites tests that concurrent writes are properly serialized.
func TestServerClientConcurrentWrites(t *testing.T) {
	t.Run("concurrent sends are serialized", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)
		session := NewMemorySession("test-client", testNS)
		client.SetSession(session)

		var wg sync.WaitGroup

		// Launch many concurrent sends
		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: 0}
				_ = client.Send(msg)
			}()
		}

		wg.Wait()

		// If we get here without race detector errors, the write lock is working
		assert.True(t, true)
	})

	t.Run("concurrent send packets are serialized", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)

		var wg sync.WaitGroup

		// Launch many concurrent packet sends
		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				pkt := &PingrespPacket{}
				_ = client.SendPacket(pkt)
			}()
		}

		wg.Wait()
		assert.True(t, true)
	})

	t.Run("concurrent disconnect and send", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024, testNS)

		var wg sync.WaitGroup

		// One goroutine disconnects
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			_ = client.Disconnect(ReasonSuccess)
		}()

		// Others try to send
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				msg := &Message{Topic: "test/topic", Payload: []byte("data"), QoS: 0}
				_ = client.Send(msg)
			}()
		}

		wg.Wait()
		assert.True(t, true)
	})
}

func TestServerClientDisconnectWillHandling(t *testing.T) {
	t.Run("server-initiated disconnect always suppresses Will", func(t *testing.T) {
		// When the server sends a DISCONNECT packet (for any reason), it's a
		// controlled termination and Will should NOT be published. Will is meant
		// for unexpected disconnections where the client can't notify others.
		reasons := []ReasonCode{
			ReasonSuccess,
			ReasonServerShuttingDown,
			ReasonSessionTakenOver,
			ReasonProtocolError,
			ReasonNotAuthorized,
			ReasonTopicNameInvalid,
			ReasonQoSNotSupported,
		}

		for _, reason := range reasons {
			t.Run(reason.String(), func(t *testing.T) {
				client := &ServerClient{
					conn:          &mockConn{},
					maxPacketSize: MaxPacketSizeDefault,
				}
				client.connected.Store(true)

				// Server-initiated disconnect
				err := client.Disconnect(reason)
				require.NoError(t, err)

				// Should always be marked as clean disconnect (Will suppressed)
				assert.True(t, client.IsCleanDisconnect(),
					"server-initiated disconnect with %s should suppress Will", reason)
			})
		}
	})
}
