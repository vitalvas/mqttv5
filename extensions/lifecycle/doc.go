// Package lifecycle publishes client lifecycle events to MQTT topics.
//
// The extension hooks into the broker's OnConnect, OnConnectFailed,
// OnDisconnect, OnSubscribe, and OnUnsubscribe callbacks and publishes
// JSON event messages to well-known topics under the $events/ prefix.
//
// # Topics
//
// The following topics are published:
//
//   - $events/presence/connected/{clientId}
//   - $events/presence/connect_failed/{clientId}
//   - $events/presence/disconnected/{clientId}
//   - $events/subscriptions/subscribed/{clientId}
//   - $events/subscriptions/unsubscribed/{clientId}
//
// Each message contains a JSON payload with event-specific fields such as
// clientId, timestamp (RFC 3339), eventType, namespace, and additional context.
//
// # Usage
//
//	lc := lifecycle.New(server)
//
//	srv := mqttv5.NewServer(
//	    mqttv5.WithListener(listener),
//	    mqttv5.OnConnect(lc.OnConnect),
//	    mqttv5.OnConnectFailed(lc.OnConnectFailed),
//	    mqttv5.OnDisconnect(lc.OnDisconnect),
//	    mqttv5.OnSubscribe(lc.OnSubscribe),
//	    mqttv5.OnUnsubscribe(lc.OnUnsubscribe),
//	)
//
// Multiple hooks can be registered on the same event; they are called in order.
//
// # Namespace
//
// By default all events are published in mqttv5.DefaultNamespace.
// Use WithNamespace to set a fixed namespace for all events, or
// WithClientNamespace(true) to publish in the client's own namespace.
// Priority: WithNamespace > WithClientNamespace > DefaultNamespace.
package lifecycle
