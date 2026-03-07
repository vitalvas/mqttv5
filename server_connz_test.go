package mqttv5

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClientInfo(t *testing.T) {
	t.Run("basic fields", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{
			ClientID:   "test-client",
			Username:   "user1",
			CleanStart: true,
			KeepAlive:  60,
		}
		client := NewServerClient(conn, connect, MaxPacketSizeDefault, "tenant1")

		info := buildClientInfo(client, time.Now())

		assert.Equal(t, "test-client", info.ClientID)
		assert.Equal(t, "user1", info.Username)
		assert.Equal(t, "tenant1", info.Namespace)
		assert.True(t, info.CleanStart)
		assert.Equal(t, uint16(60), info.KeepAlive)
		assert.Equal(t, uint32(MaxPacketSizeDefault), info.MaxPacketSize)
		assert.Nil(t, info.TLS)
		assert.Equal(t, 0, info.Subscriptions)
	})

	t.Run("uptime and idle", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "c1"}
		client := NewServerClient(conn, connect, MaxPacketSizeDefault, DefaultNamespace)

		time.Sleep(10 * time.Millisecond)
		now := time.Now()
		info := buildClientInfo(client, now)

		assert.True(t, info.Uptime >= 10*time.Millisecond)
		assert.True(t, info.Idle >= 10*time.Millisecond)
		assert.False(t, info.ConnectedAt.IsZero())
		assert.False(t, info.LastActivity.IsZero())
	})

	t.Run("remote and local addr", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "c1"}
		client := NewServerClient(conn, connect, MaxPacketSizeDefault, DefaultNamespace)

		info := buildClientInfo(client, time.Now())

		assert.NotEmpty(t, info.RemoteAddr)
		assert.NotEmpty(t, info.LocalAddr)
	})

	t.Run("stats", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "c1"}
		client := NewServerClient(conn, connect, MaxPacketSizeDefault, DefaultNamespace)

		client.recordBytesIn(100)
		client.recordBytesIn(200)
		client.recordBytesOut(150)
		client.recordMessageIn()
		client.recordMessageIn()
		client.recordMessageOut()

		info := buildClientInfo(client, time.Now())

		assert.Equal(t, int64(300), info.BytesIn)
		assert.Equal(t, int64(150), info.BytesOut)
		assert.Equal(t, int64(2), info.MessagesIn)
		assert.Equal(t, int64(1), info.MessagesOut)
	})

	t.Run("subscriptions count", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "c1"}
		client := NewServerClient(conn, connect, MaxPacketSizeDefault, DefaultNamespace)

		session := NewMemorySession("c1", DefaultNamespace)
		session.AddSubscription(Subscription{TopicFilter: "a/b", QoS: QoS0})
		session.AddSubscription(Subscription{TopicFilter: "c/d", QoS: QoS1})
		client.SetSession(session)

		info := buildClientInfo(client, time.Now())

		assert.Equal(t, 2, info.Subscriptions)
	})

	t.Run("tls detected", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "c1"}
		client := NewServerClient(conn, connect, MaxPacketSizeDefault, DefaultNamespace)

		info := buildClientInfo(client, time.Now())
		assert.Nil(t, info.TLS)

		tlsState := &tls.ConnectionState{Version: tls.VersionTLS13}
		client.SetTLSConnectionState(tlsState)
		info = buildClientInfo(client, time.Now())
		assert.NotNil(t, info.TLS)
		assert.Equal(t, tls.VersionTLS13, int(info.TLS.Version))
	})
}

func TestServerClientStats(t *testing.T) {
	t.Run("connected at and uptime", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "c1"}
		client := NewServerClient(conn, connect, MaxPacketSizeDefault, DefaultNamespace)

		assert.False(t, client.ConnectedAt().IsZero())
		assert.True(t, client.Uptime() >= 0)
	})

	t.Run("last activity updated on bytes in", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "c1"}
		client := NewServerClient(conn, connect, MaxPacketSizeDefault, DefaultNamespace)

		before := client.LastActivity()
		time.Sleep(5 * time.Millisecond)
		client.recordBytesIn(10)
		after := client.LastActivity()

		assert.True(t, after.After(before))
	})

	t.Run("idle duration", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "c1"}
		client := NewServerClient(conn, connect, MaxPacketSizeDefault, DefaultNamespace)

		time.Sleep(10 * time.Millisecond)
		assert.True(t, client.IdleDuration() >= 10*time.Millisecond)

		client.recordBytesIn(1)
		assert.True(t, client.IdleDuration() < 5*time.Millisecond)
	})

	t.Run("bytes and messages counters", func(t *testing.T) {
		conn := &mockConn{}
		connect := &ConnectPacket{ClientID: "c1"}
		client := NewServerClient(conn, connect, MaxPacketSizeDefault, DefaultNamespace)

		client.recordBytesIn(100)
		client.recordBytesOut(200)
		client.recordMessageIn()
		client.recordMessageOut()

		assert.Equal(t, int64(100), client.BytesIn())
		assert.Equal(t, int64(200), client.BytesOut())
		assert.Equal(t, int64(1), client.MessagesIn())
		assert.Equal(t, int64(1), client.MessagesOut())
	})
}

func TestGetClientInfo(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		srv := NewServer()
		info := srv.GetClientInfo(DefaultNamespace, "nonexistent")
		assert.Nil(t, info)
	})
}
