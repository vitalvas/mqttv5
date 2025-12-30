package mqttv5

import "net"

// Conn represents a network connection for MQTT communication.
type Conn interface {
	net.Conn
}
