package mqttv5

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	t.Run("creates server with valid address", func(t *testing.T) {
		srv, err := NewServer(":0")
		require.NoError(t, err)
		require.NotNil(t, srv)
		defer srv.Close()

		assert.NotNil(t, srv.Addr())
	})

	t.Run("fails with invalid address", func(t *testing.T) {
		srv, err := NewServer("invalid:address:port")
		assert.Error(t, err)
		assert.Nil(t, srv)
	})
}

func TestNewServerWithListener(t *testing.T) {
	t.Run("creates server with custom listener", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServerWithListener(listener)
		require.NotNil(t, srv)
		defer srv.Close()

		assert.Equal(t, listener.Addr(), srv.Addr())
	})

	t.Run("applies options", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		var connected bool
		srv := NewServerWithListener(listener,
			WithMaxConnections(100),
			OnConnect(func(_ *ServerClient) {
				connected = true
			}),
		)
		require.NotNil(t, srv)
		defer srv.Close()

		assert.Equal(t, 100, srv.config.maxConnections)
		if srv.config.onConnect != nil {
			srv.config.onConnect(nil)
		}
		assert.True(t, connected)
	})

	t.Run("with keep alive override", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServerWithListener(listener,
			WithServerKeepAlive(120),
		)
		require.NotNil(t, srv)
		defer srv.Close()

		assert.Equal(t, uint16(120), srv.keepAlive.ServerOverride())
	})
}

func TestServerClients(t *testing.T) {
	t.Run("empty server has no clients", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServerWithListener(listener)
		defer srv.Close()

		assert.Equal(t, 0, srv.ClientCount())
		assert.Empty(t, srv.Clients())
	})
}

func TestServerPublish(t *testing.T) {
	t.Run("publish when server not running", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServerWithListener(listener)
		// Don't start the server

		msg := &Message{Topic: "test", Payload: []byte("data")}
		err = srv.Publish(msg)
		assert.ErrorIs(t, err, ErrServerClosed)

		srv.Close()
	})

	t.Run("publish retained message stores it", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServerWithListener(listener)
		defer srv.Close()

		// Manually set running to true for this test
		srv.running.Store(true)
		defer srv.running.Store(false)

		msg := &Message{
			Topic:   "test/retained",
			Payload: []byte("data"),
			Retain:  true,
		}
		err = srv.Publish(msg)
		require.NoError(t, err)

		// Check retained store
		retained := srv.config.retainedStore.Match("test/retained")
		require.Len(t, retained, 1)
		assert.Equal(t, "test/retained", retained[0].Topic)
		assert.Equal(t, []byte("data"), retained[0].Payload)
	})

	t.Run("publish empty retained message deletes it", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServerWithListener(listener)
		defer srv.Close()

		srv.running.Store(true)
		defer srv.running.Store(false)

		// First store a retained message
		srv.config.retainedStore.Set(&RetainedMessage{
			Topic:   "test/retained",
			Payload: []byte("data"),
		})

		// Then publish empty payload to delete
		msg := &Message{
			Topic:   "test/retained",
			Payload: []byte{},
			Retain:  true,
		}
		err = srv.Publish(msg)
		require.NoError(t, err)

		// Check retained store is empty
		retained := srv.config.retainedStore.Match("test/retained")
		assert.Empty(t, retained)
	})
}

func TestServerClose(t *testing.T) {
	t.Run("close stops server", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServerWithListener(listener)

		// Start server in background
		go srv.ListenAndServe()

		// Wait for it to start
		time.Sleep(50 * time.Millisecond)

		err = srv.Close()
		require.NoError(t, err)

		// Second close should be no-op
		err = srv.Close()
		require.NoError(t, err)
	})

	t.Run("close when not running", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServerWithListener(listener)

		err = srv.Close()
		require.NoError(t, err)
	})
}

func TestServerAddr(t *testing.T) {
	t.Run("returns listener address", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServerWithListener(listener)
		defer srv.Close()

		assert.Equal(t, listener.Addr(), srv.Addr())
	})

	t.Run("returns nil when no listener", func(t *testing.T) {
		srv := &Server{}
		assert.Nil(t, srv.Addr())
	})
}

func TestServerListenAndServe(t *testing.T) {
	t.Run("already running returns error", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)

		srv := NewServerWithListener(listener)

		// Start first instance
		go srv.ListenAndServe()
		time.Sleep(50 * time.Millisecond)

		// Try to start again
		errCh := make(chan error, 1)
		go func() {
			errCh <- srv.ListenAndServe()
		}()

		select {
		case err := <-errCh:
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "already running")
		case <-time.After(100 * time.Millisecond):
			// Expected - first instance is still running
		}

		srv.Close()
	})
}

func TestServerConcurrency(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	srv := NewServerWithListener(listener)
	defer srv.Close()

	srv.running.Store(true)
	defer srv.running.Store(false)

	var wg sync.WaitGroup

	// Concurrent client count reads
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.ClientCount()
			_ = srv.Clients()
		}()
	}

	// Concurrent publish
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.Publish(&Message{Topic: "test", Payload: []byte("data")})
		}()
	}

	wg.Wait()
}
