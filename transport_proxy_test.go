package mqttv5

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProxyDialer(t *testing.T) {
	t.Run("valid HTTP proxy", func(t *testing.T) {
		d, err := NewProxyDialer("http://proxy:8080", "", "")
		require.NoError(t, err)
		assert.NotNil(t, d)
		assert.Equal(t, "http", d.proxyURL.Scheme)
		assert.Equal(t, "proxy:8080", d.proxyURL.Host)
	})

	t.Run("valid SOCKS5 proxy", func(t *testing.T) {
		d, err := NewProxyDialer("socks5://proxy:1080", "", "")
		require.NoError(t, err)
		assert.NotNil(t, d)
		assert.Equal(t, "socks5", d.proxyURL.Scheme)
	})

	t.Run("with credentials", func(t *testing.T) {
		d, err := NewProxyDialer("http://proxy:8080", "user", "pass")
		require.NoError(t, err)
		assert.Equal(t, "user", d.username)
		assert.Equal(t, "pass", d.password)
	})

	t.Run("credentials from URL", func(t *testing.T) {
		d, err := NewProxyDialer("http://user:pass@proxy:8080", "", "")
		require.NoError(t, err)
		assert.Equal(t, "user", d.username)
		assert.Equal(t, "pass", d.password)
	})

	t.Run("invalid URL", func(t *testing.T) {
		_, err := NewProxyDialer("://invalid", "", "")
		assert.Error(t, err)
	})
}

func TestProxyFromEnvironment(t *testing.T) {
	t.Run("no proxy set", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "")
		t.Setenv("http_proxy", "")
		t.Setenv("HTTPS_PROXY", "")
		t.Setenv("https_proxy", "")
		t.Setenv("NO_PROXY", "")
		t.Setenv("no_proxy", "")

		proxyURL, err := ProxyFromEnvironment("tcp://broker:1883")
		require.NoError(t, err)
		assert.Nil(t, proxyURL)
	})

	t.Run("HTTP_PROXY for TCP", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "http://proxy:8080")
		t.Setenv("HTTPS_PROXY", "")
		t.Setenv("NO_PROXY", "")

		proxyURL, err := ProxyFromEnvironment("tcp://broker:1883")
		require.NoError(t, err)
		require.NotNil(t, proxyURL)
		assert.Equal(t, "http://proxy:8080", proxyURL.String())
	})

	t.Run("HTTPS_PROXY for TLS", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "http://httpproxy:8080")
		t.Setenv("HTTPS_PROXY", "http://httpsproxy:8080")
		t.Setenv("NO_PROXY", "")

		proxyURL, err := ProxyFromEnvironment("tls://broker:8883")
		require.NoError(t, err)
		require.NotNil(t, proxyURL)
		assert.Equal(t, "http://httpsproxy:8080", proxyURL.String())
	})

	t.Run("fallback to HTTP_PROXY for TLS", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "http://httpproxy:8080")
		t.Setenv("HTTPS_PROXY", "")
		t.Setenv("NO_PROXY", "")

		proxyURL, err := ProxyFromEnvironment("tls://broker:8883")
		require.NoError(t, err)
		require.NotNil(t, proxyURL)
		assert.Equal(t, "http://httpproxy:8080", proxyURL.String())
	})

	t.Run("NO_PROXY exact match", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "http://proxy:8080")
		t.Setenv("NO_PROXY", "broker")

		proxyURL, err := ProxyFromEnvironment("tcp://broker:1883")
		require.NoError(t, err)
		assert.Nil(t, proxyURL)
	})

	t.Run("NO_PROXY wildcard", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "http://proxy:8080")
		t.Setenv("NO_PROXY", "*")

		proxyURL, err := ProxyFromEnvironment("tcp://broker:1883")
		require.NoError(t, err)
		assert.Nil(t, proxyURL)
	})

	t.Run("NO_PROXY suffix match", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "http://proxy:8080")
		t.Setenv("NO_PROXY", ".example.com")

		proxyURL, err := ProxyFromEnvironment("tcp://broker.example.com:1883")
		require.NoError(t, err)
		assert.Nil(t, proxyURL)
	})

	t.Run("NO_PROXY no match", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "http://proxy:8080")
		t.Setenv("NO_PROXY", "other.com")

		proxyURL, err := ProxyFromEnvironment("tcp://broker:1883")
		require.NoError(t, err)
		require.NotNil(t, proxyURL)
	})

	t.Run("lowercase env vars", func(t *testing.T) {
		t.Setenv("HTTP_PROXY", "")
		t.Setenv("http_proxy", "http://lowercase:8080")
		t.Setenv("NO_PROXY", "")
		t.Setenv("no_proxy", "")

		proxyURL, err := ProxyFromEnvironment("tcp://broker:1883")
		require.NoError(t, err)
		require.NotNil(t, proxyURL)
		assert.Equal(t, "http://lowercase:8080", proxyURL.String())
	})
}

func TestProxyDialerHTTPConnect(t *testing.T) {
	// Start a mock HTTP CONNECT proxy
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer proxyListener.Close()

	// Start a mock target server
	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer targetListener.Close()

	targetAddr := targetListener.Addr().String()

	// Proxy handler
	proxyDone := make(chan struct{})
	go func() {
		defer close(proxyDone)
		conn, err := proxyListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		req, err := http.ReadRequest(reader)
		if err != nil {
			return
		}

		// Verify CONNECT request
		if req.Method != http.MethodConnect {
			conn.Write([]byte("HTTP/1.1 405 Method Not Allowed\r\n\r\n"))
			return
		}

		// Connect to target
		target, err := net.Dial("tcp", targetAddr)
		if err != nil {
			conn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
			return
		}
		defer target.Close()

		// Send 200 OK
		conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))

		// Relay data
		go io.Copy(target, conn)
		io.Copy(conn, target)
	}()

	// Target handler
	targetDone := make(chan struct{})
	go func() {
		defer close(targetDone)
		conn, err := targetListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		buf := make([]byte, 5)
		n, _ := conn.Read(buf)
		conn.Write(buf[:n])
	}()

	// Test the proxy dialer
	proxyAddr := "http://" + proxyListener.Addr().String()
	dialer, err := NewProxyDialer(proxyAddr, "", "")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", targetAddr)
	require.NoError(t, err)
	defer conn.Close()

	// Test communication through proxy
	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err)

	buf := make([]byte, 5)
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(buf[:n]))
}

func TestProxyDialerHTTPConnectWithAuth(t *testing.T) {
	// Start a mock HTTP CONNECT proxy that requires auth
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer proxyListener.Close()

	proxyDone := make(chan struct{})
	go func() {
		defer close(proxyDone)
		conn, err := proxyListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		req, err := http.ReadRequest(reader)
		if err != nil {
			return
		}

		// Check Proxy-Authorization header
		auth := req.Header.Get("Proxy-Authorization")
		if auth != "Basic dXNlcjpwYXNz" { // base64("user:pass")
			conn.Write([]byte("HTTP/1.1 407 Proxy Authentication Required\r\n\r\n"))
			return
		}

		conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
	}()

	proxyAddr := "http://" + proxyListener.Addr().String()
	dialer, err := NewProxyDialer(proxyAddr, "user", "pass")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", "example.com:1883")
	require.NoError(t, err)
	conn.Close()
}

func TestProxyDialerUnsupportedScheme(t *testing.T) {
	dialer, err := NewProxyDialer("ftp://proxy:21", "", "")
	require.NoError(t, err)

	ctx := context.Background()
	_, err = dialer.DialContext(ctx, "tcp", "broker:1883")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported proxy scheme")
}

func TestClientProxyOptions(t *testing.T) {
	t.Run("WithProxy", func(t *testing.T) {
		opts := applyOptions(WithProxy("http://proxy:8080"))
		require.NotNil(t, opts.proxyConfig)
		assert.Equal(t, "http://proxy:8080", opts.proxyConfig.URL)
		assert.Empty(t, opts.proxyConfig.Username)
	})

	t.Run("WithProxyAuth", func(t *testing.T) {
		opts := applyOptions(WithProxyAuth("socks5://proxy:1080", "user", "pass"))
		require.NotNil(t, opts.proxyConfig)
		assert.Equal(t, "socks5://proxy:1080", opts.proxyConfig.URL)
		assert.Equal(t, "user", opts.proxyConfig.Username)
		assert.Equal(t, "pass", opts.proxyConfig.Password)
	})

	t.Run("WithProxyFromEnvironment", func(t *testing.T) {
		opts := applyOptions(WithProxyFromEnvironment(true))
		assert.True(t, opts.proxyFromEnv)

		opts = applyOptions(WithProxyFromEnvironment(false))
		assert.False(t, opts.proxyFromEnv)
	})
}

func TestWSDialerSetProxyFromEnvironment(t *testing.T) {
	d := NewWSDialer()
	require.NotNil(t, d.Dialer)

	// Initially no proxy
	assert.Nil(t, d.Dialer.Proxy)

	// Set proxy from environment
	d.SetProxyFromEnvironment()
	assert.NotNil(t, d.Dialer.Proxy)
}
