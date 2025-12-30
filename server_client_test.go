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

		client := NewServerClient(conn, connect, 256*1024)

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

		client := NewServerClient(conn, connect, 256*1024)
		assert.Nil(t, client.Session())

		session := NewMemorySession("test-client")
		client.SetSession(session)
		assert.Equal(t, session, client.Session())
	})

	t.Run("topic alias max", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024)
		client.SetTopicAliasMax(10, 20)

		assert.Equal(t, uint16(10), client.TopicAliases().InboundMax())
		assert.Equal(t, uint16(20), client.TopicAliases().OutboundMax())
	})

	t.Run("receive maximum", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024)

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

		client := NewServerClient(conn, connect, 256*1024)
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

		client := NewServerClient(conn, connect, 256*1024)
		client.Close()

		msg := &Message{Topic: "test", Payload: []byte("data")}
		err := client.Send(msg)
		assert.ErrorIs(t, err, ErrNotConnected)
	})

	t.Run("send packet when not connected", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024)
		client.Close()

		pkt := &PingrespPacket{}
		err := client.SendPacket(pkt)
		assert.ErrorIs(t, err, ErrNotConnected)
	})

	t.Run("disconnect when not connected", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "test-client"}

		client := NewServerClient(conn, connect, 256*1024)
		client.Close()

		err := client.Disconnect(ReasonSuccess)
		assert.ErrorIs(t, err, ErrNotConnected)
	})
}

func TestServerClientConcurrency(_ *testing.T) {
	conn := &mockConn{}
	connect := &ConnectPacket{ClientID: "test-client"}

	client := NewServerClient(conn, connect, 256*1024)
	session := NewMemorySession("test-client")
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
