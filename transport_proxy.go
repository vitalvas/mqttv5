package mqttv5

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"

	"golang.org/x/net/proxy"
)

// ProxyConfig holds proxy configuration for client connections.
type ProxyConfig struct {
	// URL is the proxy URL in format: http://host:port or socks5://host:port
	URL string
	// Username for proxy authentication (optional)
	Username string
	// Password for proxy authentication (optional)
	Password string
}

// ProxyDialer implements dialing through HTTP CONNECT or SOCKS5 proxies.
type ProxyDialer struct {
	proxyURL *url.URL
	username string
	password string
	forward  net.Dialer
}

// NewProxyDialer creates a new proxy dialer from the given proxy URL.
// Supported schemes: http, https (HTTP CONNECT), socks5.
func NewProxyDialer(proxyURL, username, password string) (*ProxyDialer, error) {
	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}

	// Extract auth from URL if not provided separately
	if username == "" && u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}

	return &ProxyDialer{
		proxyURL: u,
		username: username,
		password: password,
		forward:  net.Dialer{},
	}, nil
}

// DialContext connects to the target address through the proxy.
func (d *ProxyDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	switch d.proxyURL.Scheme {
	case "http", "https":
		return d.dialHTTPConnect(ctx, addr)
	case "socks5", "socks5h":
		return d.dialSOCKS5(ctx, network, addr)
	default:
		return nil, fmt.Errorf("unsupported proxy scheme: %s", d.proxyURL.Scheme)
	}
}

// dialHTTPConnect establishes a connection through an HTTP CONNECT proxy.
func (d *ProxyDialer) dialHTTPConnect(ctx context.Context, targetAddr string) (net.Conn, error) {
	proxyAddr := d.proxyURL.Host
	if d.proxyURL.Port() == "" {
		if d.proxyURL.Scheme == "https" {
			proxyAddr = net.JoinHostPort(d.proxyURL.Hostname(), "443")
		} else {
			proxyAddr = net.JoinHostPort(d.proxyURL.Hostname(), "8080")
		}
	}

	// Connect to proxy
	conn, err := d.forward.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to proxy: %w", err)
	}

	// Build CONNECT request
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: targetAddr},
		Host:   targetAddr,
		Header: make(http.Header),
	}

	// Add proxy authentication if configured
	if d.username != "" {
		auth := d.username + ":" + d.password
		basicAuth := base64.StdEncoding.EncodeToString([]byte(auth))
		req.Header.Set("Proxy-Authorization", "Basic "+basicAuth)
	}

	// Send CONNECT request
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send CONNECT request: %w", err)
	}

	// Read response
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read CONNECT response: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		conn.Close()
		return nil, fmt.Errorf("proxy CONNECT failed: %s", resp.Status)
	}

	return conn, nil
}

// dialSOCKS5 establishes a connection through a SOCKS5 proxy.
func (d *ProxyDialer) dialSOCKS5(ctx context.Context, network, targetAddr string) (net.Conn, error) {
	proxyAddr := d.proxyURL.Host
	if d.proxyURL.Port() == "" {
		proxyAddr = net.JoinHostPort(d.proxyURL.Hostname(), "1080")
	}

	var auth *proxy.Auth
	if d.username != "" {
		auth = &proxy.Auth{
			User:     d.username,
			Password: d.password,
		}
	}

	dialer, err := proxy.SOCKS5("tcp", proxyAddr, auth, &d.forward)
	if err != nil {
		return nil, fmt.Errorf("failed to create SOCKS5 dialer: %w", err)
	}

	// proxy.Dialer doesn't have DialContext, so we use a channel to respect context
	type dialResult struct {
		conn net.Conn
		err  error
	}
	resultCh := make(chan dialResult, 1)

	go func() {
		conn, err := dialer.Dial(network, targetAddr)
		resultCh <- dialResult{conn, err}
	}()

	select {
	case <-ctx.Done():
		// Context canceled - the goroutine will complete but we ignore the result
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.err != nil {
			return nil, fmt.Errorf("SOCKS5 dial failed: %w", result.err)
		}
		return result.conn, nil
	}
}

// ProxyFromEnvironment returns the proxy URL for the given target address
// based on HTTP_PROXY, HTTPS_PROXY, and NO_PROXY environment variables.
// Returns nil if no proxy should be used.
func ProxyFromEnvironment(targetAddr string) (*url.URL, error) {
	// Parse target address to determine scheme
	u, err := url.Parse(targetAddr)
	if err != nil {
		return nil, nil
	}

	// Check NO_PROXY first
	noProxy, ok := os.LookupEnv("NO_PROXY")
	if !ok {
		noProxy = os.Getenv("no_proxy")
	}
	if noProxy != "" {
		host := u.Hostname()
		for _, pattern := range strings.Split(noProxy, ",") {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" {
				continue
			}
			if pattern == "*" {
				return nil, nil
			}
			// Match domain or suffix
			if strings.HasPrefix(pattern, ".") {
				if strings.HasSuffix(host, pattern) || host == pattern[1:] {
					return nil, nil
				}
			} else if host == pattern || strings.HasSuffix(host, "."+pattern) {
				return nil, nil
			}
		}
	}

	// Determine which proxy to use based on scheme
	var proxyEnv string
	switch u.Scheme {
	case "https", "tls", "ssl", "mqtts", "wss":
		proxyEnv, ok = os.LookupEnv("HTTPS_PROXY")
		if !ok || proxyEnv == "" {
			proxyEnv = os.Getenv("https_proxy")
		}
	default:
		proxyEnv, ok = os.LookupEnv("HTTP_PROXY")
		if !ok || proxyEnv == "" {
			proxyEnv = os.Getenv("http_proxy")
		}
	}

	// Fallback to HTTP_PROXY if HTTPS_PROXY is not set
	if proxyEnv == "" {
		proxyEnv, ok = os.LookupEnv("HTTP_PROXY")
		if !ok || proxyEnv == "" {
			proxyEnv = os.Getenv("http_proxy")
		}
	}

	if proxyEnv == "" {
		return nil, nil
	}

	return url.Parse(proxyEnv)
}
