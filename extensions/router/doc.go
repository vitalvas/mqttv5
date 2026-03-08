// Package router provides message routing for MQTT v5.0 with topic wildcard
// support and conditional filtering.
//
// The router dispatches incoming MQTT messages to registered handlers based on
// topic filters and optional conditions. Multiple handlers can match the same
// message, and all matching handlers are called.
//
// # Basic Usage
//
//	r := router.New()
//
//	r.Handle(func(msg *mqttv5.Message) {
//	    fmt.Printf("Temperature: %s\n", msg.Payload)
//	}, router.WithTopic("sensors/+/temperature"))
//
//	r.Route(msg)
//
// # Topic Wildcards
//
// Standard MQTT wildcards are supported in topic filters:
//
//   - + (single level): matches exactly one topic level
//   - # (multi level): matches zero or more topic levels
//
// Examples:
//
//	router.WithTopic("sensors/+/value")   // sensors/X/value
//	router.WithTopic("sensors/#")         // sensors, sensors/a, sensors/a/b/c
//
// # Condition Filters
//
// Handlers can be filtered by multiple conditions using AND logic.
// All conditions are optional; a handler with no conditions matches all messages.
//
//	// Filter by QoS level (incoming message filtering)
//	router.WithQoS(mqttv5.QoS1)
//
//	// Filter by content type (regexp)
//	router.WithContentType(regexp.MustCompile(`^application/json`))
//
//	// Filter by client ID (regexp)
//	router.WithClientID(regexp.MustCompile(`^sensor-`))
//
//	// Filter by response topic (regexp)
//	router.WithResponseTopic(regexp.MustCompile(`^rpc/response/`))
//
//	// Filter by user property key/value (regexp)
//	router.WithUserProperty(regexp.MustCompile(`^tenant$`), regexp.MustCompile(`^acme$`))
//
//	// Filter by namespace (server-side multi-tenancy)
//	router.WithNamespace("tenant-123")
//
// # Per-Topic Subscription QoS
//
// Use [WithSubscribeQoS] to set different QoS levels per topic filter in the
// SUBSCRIBE packet. This is separate from [WithQoS], which filters incoming
// messages by QoS level.
//
//	r.Handle(handleTask,
//	    router.WithTopic("tasks/#"),
//	    router.WithSubscribeQoS(mqttv5.QoS0),
//	)
//	r.Handle(handleCommand,
//	    router.WithTopic("commands/#"),
//	    router.WithSubscribeQoS(mqttv5.QoS1),
//	)
//
// When calling [Router.Subscribe], handlers with [WithSubscribeQoS] use their
// specified QoS; others fall back to the provided default QoS.
//
//	r.Subscribe(client, mqttv5.QoS1) // tasks/# at QoS0, commands/# at QoS1
//
// If multiple handlers register the same topic filter with different QoS
// values, the highest QoS is used.
//
// Use [Router.FiltersWithQoS] to retrieve the filter-to-QoS map directly.
//
// # Integration with MQTT Client
//
//	r := router.New()
//	r.Handle(handler, router.WithTopic("sensors/#"))
//
//	client, _ := mqttv5.Dial("tcp://localhost:1883")
//	r.Subscribe(client, mqttv5.QoS1)
//
// # Thread Safety
//
// Router is safe for concurrent use. Handler registration and message routing
// can happen from different goroutines.
package router
