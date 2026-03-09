package systopics

// Standard $SYS topic paths.
const (
	TopicVersion   = "$SYS/broker/version"
	TopicUptime    = "$SYS/broker/uptime"
	TopicTimestamp = "$SYS/broker/timestamp"

	TopicClientsConnected    = "$SYS/broker/clients/connected"
	TopicClientsTotal        = "$SYS/broker/clients/total"
	TopicClientsMaximum      = "$SYS/broker/clients/maximum"
	TopicClientsDisconnected = "$SYS/broker/clients/disconnected"

	TopicMessagesReceived     = "$SYS/broker/messages/received"
	TopicMessagesSent         = "$SYS/broker/messages/sent"
	TopicMessagesStored       = "$SYS/broker/messages/stored"
	TopicMessagesPublishRecv  = "$SYS/broker/messages/publish/received"
	TopicMessagesPublishSent  = "$SYS/broker/messages/publish/sent"
	TopicMessagesReceivedQoS0 = "$SYS/broker/messages/received/qos0"
	TopicMessagesReceivedQoS1 = "$SYS/broker/messages/received/qos1"
	TopicMessagesReceivedQoS2 = "$SYS/broker/messages/received/qos2"
	TopicMessagesSentQoS0     = "$SYS/broker/messages/sent/qos0"
	TopicMessagesSentQoS1     = "$SYS/broker/messages/sent/qos1"
	TopicMessagesSentQoS2     = "$SYS/broker/messages/sent/qos2"

	TopicBytesReceived = "$SYS/broker/bytes/received"
	TopicBytesSent     = "$SYS/broker/bytes/sent"

	TopicSubscriptionsCount    = "$SYS/broker/subscriptions/count"
	TopicRetainedMessagesCount = "$SYS/broker/retained/messages/count"

	TopicNamespacesCount = "$SYS/broker/namespaces/count"

	TopicLoadMessagesRecv1min  = "$SYS/broker/load/messages/received/1min"
	TopicLoadMessagesRecv5min  = "$SYS/broker/load/messages/received/5min"
	TopicLoadMessagesRecv15min = "$SYS/broker/load/messages/received/15min"
	TopicLoadMessagesSent1min  = "$SYS/broker/load/messages/sent/1min"
	TopicLoadMessagesSent5min  = "$SYS/broker/load/messages/sent/5min"
	TopicLoadMessagesSent15min = "$SYS/broker/load/messages/sent/15min"
	TopicLoadBytesRecv1min     = "$SYS/broker/load/bytes/received/1min"
	TopicLoadBytesRecv5min     = "$SYS/broker/load/bytes/received/5min"
	TopicLoadBytesRecv15min    = "$SYS/broker/load/bytes/received/15min"
	TopicLoadBytesSent1min     = "$SYS/broker/load/bytes/sent/1min"
	TopicLoadBytesSent5min     = "$SYS/broker/load/bytes/sent/5min"
	TopicLoadBytesSent15min    = "$SYS/broker/load/bytes/sent/15min"
	TopicLoadConnections1min   = "$SYS/broker/load/connections/1min"
	TopicLoadConnections5min   = "$SYS/broker/load/connections/5min"
	TopicLoadConnections15min  = "$SYS/broker/load/connections/15min"

	TopicPacketsReceivedPrefix = "$SYS/broker/packets/received/"
	TopicPacketsSentPrefix     = "$SYS/broker/packets/sent/"

	TopicTopicsCount = "$SYS/broker/topics/count"
)

// Load metric names for the load calculator.
const (
	loadMessagesRecv = "messages_recv"
	loadMessagesSent = "messages_sent"
	loadBytesRecv    = "bytes_recv"
	loadBytesSent    = "bytes_sent"
	loadConnections  = "connections"
)
