package mqttv5

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWSServer(t *testing.T) {
	t.Run("creates ws server with defaults", func(t *testing.T) {
		srv := NewWSServer()
		require.NotNil(t, srv)
		defer srv.Close()

		assert.NotNil(t, srv.Server)
		assert.NotNil(t, srv.handler)
	})

	t.Run("applies options", func(t *testing.T) {
		var connected bool
		srv := NewWSServer(
			WithMaxConnections(50),
			OnConnect(func(_ *ServerClient) {
				connected = true
			}),
		)
		require.NotNil(t, srv)
		defer srv.Close()

		assert.Equal(t, 50, srv.config.maxConnections)
		if srv.config.onConnect != nil {
			srv.config.onConnect(nil)
		}
		assert.True(t, connected)
	})

	t.Run("with keep alive override", func(t *testing.T) {
		srv := NewWSServer(
			WithServerKeepAlive(60),
		)
		require.NotNil(t, srv)
		defer srv.Close()

		assert.Equal(t, uint16(60), srv.keepAlive.ServerOverride())
	})
}

func TestWSServerStart(t *testing.T) {
	t.Run("starts background tasks", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()

		// Should be running now
		assert.True(t, srv.running.Load())
	})

	t.Run("start is idempotent", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()
		srv.Start() // Second call should be no-op

		assert.True(t, srv.running.Load())
	})
}

func TestWSServerServeHTTP(t *testing.T) {
	t.Run("serves http requests", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()

		// Create test request (not a valid WebSocket upgrade, so should fail)
		req := httptest.NewRequest(http.MethodGet, "/mqtt", nil)
		w := httptest.NewRecorder()

		srv.ServeHTTP(w, req)

		// Without proper WebSocket headers, this should fail
		// The important thing is it doesn't panic
		assert.True(t, w.Code >= 400 || w.Code == 200)
	})
}

func TestWSServerClose(t *testing.T) {
	t.Run("close stops server", func(t *testing.T) {
		srv := NewWSServer()
		srv.Start()

		// Give it time to start background tasks
		time.Sleep(50 * time.Millisecond)

		err := srv.Close()
		require.NoError(t, err)

		assert.False(t, srv.running.Load())
	})

	t.Run("close when not started", func(t *testing.T) {
		srv := NewWSServer()

		err := srv.Close()
		require.NoError(t, err)
	})
}

func TestWSServerPublish(t *testing.T) {
	t.Run("publish when not running", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		msg := &Message{Topic: "test", Payload: []byte("data")}
		err := srv.Publish(msg)
		assert.ErrorIs(t, err, ErrServerClosed)
	})

	t.Run("publish when running", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()

		msg := &Message{Topic: "test", Payload: []byte("data")}
		err := srv.Publish(msg)
		require.NoError(t, err)
	})
}

func TestWSServerClientManagement(t *testing.T) {
	t.Run("client count starts at zero", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()

		assert.Equal(t, 0, srv.ClientCount())
		assert.Empty(t, srv.Clients())
	})
}

func TestWSServerConcurrency(_ *testing.T) {
	srv := NewWSServer()
	defer srv.Close()

	srv.Start()

	var wg sync.WaitGroup

	// Concurrent operations
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.ClientCount()
			_ = srv.Clients()
		}()
	}

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.Publish(&Message{Topic: "test", Payload: []byte("data")})
		}()
	}

	wg.Wait()
}

func TestWSServerWithHTTPMux(t *testing.T) {
	t.Run("can be mounted on http mux", func(t *testing.T) {
		srv := NewWSServer()
		defer srv.Close()

		srv.Start()

		mux := http.NewServeMux()
		mux.Handle("/mqtt", srv)

		// Create test server
		ts := httptest.NewServer(mux)
		defer ts.Close()

		// Make a regular HTTP request (not WebSocket)
		resp, err := http.Get(ts.URL + "/mqtt")
		require.NoError(t, err)
		resp.Body.Close()

		// Should get some response (likely error since not WebSocket)
		assert.True(t, resp.StatusCode >= 200)
	})
}
