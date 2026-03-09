// Package systopics provides $SYS topic publishing for MQTT broker statistics.
//
// It follows the standard $SYS/broker/ topic hierarchy used by MQTT brokers,
// periodically publishing broker statistics as retained messages.
//
// # Topics published
//
// Broker information:
//
//	$SYS/broker/version              - broker version string
//	$SYS/broker/uptime               - seconds since broker started
//	$SYS/broker/timestamp            - current Unix timestamp
//
// Client statistics:
//
//	$SYS/broker/clients/connected    - currently connected clients
//	$SYS/broker/clients/total        - total connections since start
//	$SYS/broker/clients/maximum      - peak concurrent connections
//	$SYS/broker/clients/disconnected - disconnected persistent sessions
//
// Message statistics:
//
//	$SYS/broker/messages/received           - total messages received
//	$SYS/broker/messages/sent               - total messages sent
//	$SYS/broker/messages/publish/received   - total PUBLISH received
//	$SYS/broker/messages/publish/sent       - total PUBLISH sent
//	$SYS/broker/messages/stored             - retained messages count
//	$SYS/broker/messages/received/qos{0-2}  - messages received per QoS
//	$SYS/broker/messages/sent/qos{0-2}      - messages sent per QoS
//
// Byte statistics:
//
//	$SYS/broker/bytes/received       - total bytes received
//	$SYS/broker/bytes/sent           - total bytes sent
//
// Subscription statistics:
//
//	$SYS/broker/subscriptions/count  - active subscriptions
//
// Retained message statistics:
//
//	$SYS/broker/retained/messages/count - retained messages
//
// Load statistics (messages per second, averaged over interval):
//
//	$SYS/broker/load/messages/received/{1min,5min,15min}
//	$SYS/broker/load/messages/sent/{1min,5min,15min}
//	$SYS/broker/load/bytes/received/{1min,5min,15min}
//	$SYS/broker/load/bytes/sent/{1min,5min,15min}
//	$SYS/broker/load/connections/{1min,5min,15min}
//
// # Namespace support
//
// In global mode (default), the publisher also emits per-namespace client counts:
//
//	$SYS/broker/namespaces/count
//	$SYS/broker/clients/namespace/{ns}/connected
//
// In isolated mode, the publisher sends $SYS/broker/clients/connected with the
// Namespace field set on the message, so the server delivers it only to clients
// in that namespace. Each namespace sees its own client count.
// Namespace names are never exposed in topic paths.
// When both TopicGroupClients and TopicGroupNamespace are enabled,
// $SYS/broker/clients/connected is only published per-namespace.
//
// # Usage
//
//	server := mqttv5.NewServer(/* ... */)
//	pub := systopics.New(server,
//	    systopics.WithVersion("mybroker 1.0.0"),
//	    systopics.WithInterval(10 * time.Second),
//	    systopics.WithTopicGroups(systopics.TopicGroupAll),
//	)
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	go pub.Run(ctx)
package systopics
