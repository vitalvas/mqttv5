package router

import (
	"regexp"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqttv5"
)

func TestRouterHandle(t *testing.T) {
	r := New()

	var called bool
	r.Handle(func(_ *mqttv5.Message) {
		called = true
	}, WithTopic("test/topic"))

	assert.Equal(t, 1, r.Len())

	r.Route(&mqttv5.Message{Topic: "test/topic"})
	assert.True(t, called)
}

func TestRouterExactMatch(t *testing.T) {
	r := New()

	var received string
	r.Handle(func(msg *mqttv5.Message) {
		received = msg.Topic
	}, WithTopic("sensors/temperature"))

	r.Route(&mqttv5.Message{Topic: "sensors/temperature"})
	assert.Equal(t, "sensors/temperature", received)

	received = ""
	r.Route(&mqttv5.Message{Topic: "sensors/humidity"})
	assert.Empty(t, received)
}

func TestRouterSingleLevelWildcard(t *testing.T) {
	r := New()

	var topics []string
	r.Handle(func(msg *mqttv5.Message) {
		topics = append(topics, msg.Topic)
	}, WithTopic("sensors/+/value"))

	r.Route(&mqttv5.Message{Topic: "sensors/temp/value"})
	r.Route(&mqttv5.Message{Topic: "sensors/humidity/value"})
	r.Route(&mqttv5.Message{Topic: "sensors/pressure/value"})
	r.Route(&mqttv5.Message{Topic: "sensors/temp/other"}) // Should not match

	require.Len(t, topics, 3)
	assert.Contains(t, topics, "sensors/temp/value")
	assert.Contains(t, topics, "sensors/humidity/value")
	assert.Contains(t, topics, "sensors/pressure/value")
}

func TestRouterMultiLevelWildcard(t *testing.T) {
	r := New()

	var topics []string
	r.Handle(func(msg *mqttv5.Message) {
		topics = append(topics, msg.Topic)
	}, WithTopic("sensors/#"))

	r.Route(&mqttv5.Message{Topic: "sensors"})
	r.Route(&mqttv5.Message{Topic: "sensors/temp"})
	r.Route(&mqttv5.Message{Topic: "sensors/temp/value"})
	r.Route(&mqttv5.Message{Topic: "sensors/a/b/c/d"})
	r.Route(&mqttv5.Message{Topic: "other/topic"}) // Should not match

	require.Len(t, topics, 4)
}

func TestRouterMultipleHandlers(t *testing.T) {
	r := New()

	var count int32
	r.Handle(func(_ *mqttv5.Message) {
		atomic.AddInt32(&count, 1)
	}, WithTopic("topic/+"))
	r.Handle(func(_ *mqttv5.Message) {
		atomic.AddInt32(&count, 1)
	}, WithTopic("topic/test"))
	r.Handle(func(_ *mqttv5.Message) {
		atomic.AddInt32(&count, 1)
	}, WithTopic("#"))

	r.Route(&mqttv5.Message{Topic: "topic/test"})
	assert.Equal(t, int32(3), atomic.LoadInt32(&count))
}

func TestRouterFilters(t *testing.T) {
	r := New()

	r.Handle(func(_ *mqttv5.Message) {}, WithTopic("topic/a"))
	r.Handle(func(_ *mqttv5.Message) {}, WithTopic("topic/b"))
	r.Handle(func(_ *mqttv5.Message) {}, WithTopic("topic/+"))

	filters := r.Filters()
	assert.Len(t, filters, 3)
	assert.Contains(t, filters, "topic/a")
	assert.Contains(t, filters, "topic/b")
	assert.Contains(t, filters, "topic/+")
}

func TestRouterClear(t *testing.T) {
	r := New()

	r.Handle(func(_ *mqttv5.Message) {}, WithTopic("topic/a"))
	r.Handle(func(_ *mqttv5.Message) {}, WithTopic("topic/b"))

	r.Clear()
	assert.Equal(t, 0, r.Len())
}

func TestRouterNilMessage(t *testing.T) {
	r := New()

	var called bool
	r.Handle(func(_ *mqttv5.Message) {
		called = true
	}, WithTopic("#"))

	r.Route(nil)
	assert.False(t, called)
}

func TestRouterMultipleHandlersSameFilter(t *testing.T) {
	r := New()

	var count int32
	r.Handle(func(_ *mqttv5.Message) {
		atomic.AddInt32(&count, 1)
	}, WithTopic("test"))
	r.Handle(func(_ *mqttv5.Message) {
		atomic.AddInt32(&count, 1)
	}, WithTopic("test"))

	assert.Equal(t, 2, r.Len()) // Two handlers now

	r.Route(&mqttv5.Message{Topic: "test"})
	assert.Equal(t, int32(2), atomic.LoadInt32(&count)) // Both handlers called
}

func TestRouterMessageHandler(t *testing.T) {
	r := New()

	var received string
	r.Handle(func(msg *mqttv5.Message) {
		received = msg.Topic
	}, WithTopic("test/#"))

	handler := r.MessageHandler()
	handler(&mqttv5.Message{Topic: "test/topic"})

	assert.Equal(t, "test/topic", received)
}

func TestRouterWithQoS(t *testing.T) {
	r := New()

	var qos0Called, qos1Called, qos2Called bool

	r.Handle(func(_ *mqttv5.Message) {
		qos0Called = true
	}, WithTopic("sensors/#"), WithQoS(mqttv5.QoS0))
	r.Handle(func(_ *mqttv5.Message) {
		qos1Called = true
	}, WithTopic("sensors/#"), WithQoS(mqttv5.QoS1))
	r.Handle(func(_ *mqttv5.Message) {
		qos2Called = true
	}, WithTopic("sensors/#"), WithQoS(mqttv5.QoS2))

	// Route QoS 1 message
	r.Route(&mqttv5.Message{Topic: "sensors/temp", QoS: mqttv5.QoS1})

	assert.False(t, qos0Called)
	assert.True(t, qos1Called)
	assert.False(t, qos2Called)
}

func TestRouterWithQoSMixed(t *testing.T) {
	r := New()

	var allCalled, qos2Called bool

	// Handler for all QoS levels
	r.Handle(func(_ *mqttv5.Message) {
		allCalled = true
	}, WithTopic("sensors/#"))

	// Handler only for QoS 2
	r.Handle(func(_ *mqttv5.Message) {
		qos2Called = true
	}, WithTopic("sensors/#"), WithQoS(mqttv5.QoS2))

	// Route QoS 0 message
	r.Route(&mqttv5.Message{Topic: "sensors/temp", QoS: mqttv5.QoS0})

	assert.True(t, allCalled)
	assert.False(t, qos2Called)

	// Reset
	allCalled = false
	qos2Called = false

	// Route QoS 2 message
	r.Route(&mqttv5.Message{Topic: "sensors/temp", QoS: mqttv5.QoS2})

	assert.True(t, allCalled)
	assert.True(t, qos2Called)
}

func TestRouterWithContentType(t *testing.T) {
	r := New()

	var jsonCalled, textCalled, xmlCalled bool

	r.Handle(func(_ *mqttv5.Message) {
		jsonCalled = true
	}, WithTopic("data/#"), WithContentType(regexp.MustCompile(`^application/json$`)))
	r.Handle(func(_ *mqttv5.Message) {
		textCalled = true
	}, WithTopic("data/#"), WithContentType(regexp.MustCompile(`^text/plain$`)))
	r.Handle(func(_ *mqttv5.Message) {
		xmlCalled = true
	}, WithTopic("data/#"), WithContentType(regexp.MustCompile(`^application/xml$`)))

	// Route JSON message
	r.Route(&mqttv5.Message{Topic: "data/sensor", ContentType: "application/json"})

	assert.True(t, jsonCalled)
	assert.False(t, textCalled)
	assert.False(t, xmlCalled)
}

func TestRouterWithContentTypeMixed(t *testing.T) {
	r := New()

	var allCalled, jsonCalled bool

	// Handler for all content types
	r.Handle(func(_ *mqttv5.Message) {
		allCalled = true
	}, WithTopic("data/#"))

	// Handler only for JSON
	r.Handle(func(_ *mqttv5.Message) {
		jsonCalled = true
	}, WithTopic("data/#"), WithContentType(regexp.MustCompile(`^application/json$`)))

	// Route text message
	r.Route(&mqttv5.Message{Topic: "data/sensor", ContentType: "text/plain"})

	assert.True(t, allCalled)
	assert.False(t, jsonCalled)

	// Reset
	allCalled = false
	jsonCalled = false

	// Route JSON message
	r.Route(&mqttv5.Message{Topic: "data/sensor", ContentType: "application/json"})

	assert.True(t, allCalled)
	assert.True(t, jsonCalled)
}

func TestRouterWithMultipleConditions(t *testing.T) {
	r := New()

	var called bool

	// Handler for QoS 1 JSON messages only
	r.Handle(func(_ *mqttv5.Message) {
		called = true
	}, WithTopic("data/#"), WithQoS(mqttv5.QoS1), WithContentType(regexp.MustCompile(`^application/json$`)))

	// QoS 0 JSON - should not match
	r.Route(&mqttv5.Message{Topic: "data/sensor", QoS: mqttv5.QoS0, ContentType: "application/json"})
	assert.False(t, called)

	// QoS 1 text - should not match
	r.Route(&mqttv5.Message{Topic: "data/sensor", QoS: mqttv5.QoS1, ContentType: "text/plain"})
	assert.False(t, called)

	// QoS 1 JSON - should match
	r.Route(&mqttv5.Message{Topic: "data/sensor", QoS: mqttv5.QoS1, ContentType: "application/json"})
	assert.True(t, called)
}

func TestRouterWithClientID(t *testing.T) {
	r := New()

	var sensorCalled, deviceCalled, adminCalled bool

	// Handler for sensor clients (sensor-*)
	r.Handle(func(_ *mqttv5.Message) {
		sensorCalled = true
	}, WithTopic("data/#"), WithClientID(regexp.MustCompile(`^sensor-`)))

	// Handler for device clients (device-*)
	r.Handle(func(_ *mqttv5.Message) {
		deviceCalled = true
	}, WithTopic("data/#"), WithClientID(regexp.MustCompile(`^device-`)))

	// Handler for admin clients
	r.Handle(func(_ *mqttv5.Message) {
		adminCalled = true
	}, WithTopic("data/#"), WithClientID(regexp.MustCompile(`^admin$`)))

	// Route message from sensor client
	r.Route(&mqttv5.Message{Topic: "data/temp", ClientID: "sensor-001"})

	assert.True(t, sensorCalled)
	assert.False(t, deviceCalled)
	assert.False(t, adminCalled)
}

func TestRouterWithClientIDMixed(t *testing.T) {
	r := New()

	var allCalled, sensorCalled bool

	// Handler for all clients
	r.Handle(func(_ *mqttv5.Message) {
		allCalled = true
	}, WithTopic("data/#"))

	// Handler only for sensor clients
	r.Handle(func(_ *mqttv5.Message) {
		sensorCalled = true
	}, WithTopic("data/#"), WithClientID(regexp.MustCompile(`^sensor-`)))

	// Route message from device client
	r.Route(&mqttv5.Message{Topic: "data/temp", ClientID: "device-001"})

	assert.True(t, allCalled)
	assert.False(t, sensorCalled)

	// Reset
	allCalled = false
	sensorCalled = false

	// Route message from sensor client
	r.Route(&mqttv5.Message{Topic: "data/temp", ClientID: "sensor-001"})

	assert.True(t, allCalled)
	assert.True(t, sensorCalled)
}

func TestRouterWithClientIDEmpty(t *testing.T) {
	r := New()

	var called bool

	// Handler for non-empty client IDs
	r.Handle(func(_ *mqttv5.Message) {
		called = true
	}, WithTopic("data/#"), WithClientID(regexp.MustCompile(`.+`)))

	// Route message with empty client ID
	r.Route(&mqttv5.Message{Topic: "data/temp", ClientID: ""})

	assert.False(t, called)

	// Route message with client ID
	r.Route(&mqttv5.Message{Topic: "data/temp", ClientID: "client-1"})

	assert.True(t, called)
}

func TestRouterWithResponseTopic(t *testing.T) {
	r := New()

	var rpcCalled, eventCalled bool

	// Handler for RPC response topics
	r.Handle(func(_ *mqttv5.Message) {
		rpcCalled = true
	}, WithTopic("data/#"), WithResponseTopic(regexp.MustCompile(`^rpc/response/`)))

	// Handler for event response topics
	r.Handle(func(_ *mqttv5.Message) {
		eventCalled = true
	}, WithTopic("data/#"), WithResponseTopic(regexp.MustCompile(`^events/`)))

	// Route message with RPC response topic
	r.Route(&mqttv5.Message{Topic: "data/sensor", ResponseTopic: "rpc/response/123"})

	assert.True(t, rpcCalled)
	assert.False(t, eventCalled)
}

func TestRouterWithUserProperty(t *testing.T) {
	r := New()

	var called bool

	// Handler for messages with specific user property
	r.Handle(func(_ *mqttv5.Message) {
		called = true
	}, WithTopic("data/#"), WithUserProperty(regexp.MustCompile(`^tenant$`), regexp.MustCompile(`^acme$`)))

	// Route message without matching property
	r.Route(&mqttv5.Message{
		Topic: "data/sensor",
		UserProperties: []mqttv5.StringPair{
			{Key: "other", Value: "value"},
		},
	})
	assert.False(t, called)

	// Route message with matching property
	r.Route(&mqttv5.Message{
		Topic: "data/sensor",
		UserProperties: []mqttv5.StringPair{
			{Key: "tenant", Value: "acme"},
		},
	})
	assert.True(t, called)
}

func TestRouterWithUserPropertyMultiple(t *testing.T) {
	r := New()

	var called bool

	// Handler requiring multiple user properties
	r.Handle(func(_ *mqttv5.Message) {
		called = true
	}, WithTopic("data/#"),
		WithUserProperty(regexp.MustCompile(`^tenant$`), regexp.MustCompile(`^acme$`)),
		WithUserProperty(regexp.MustCompile(`^env$`), regexp.MustCompile(`^prod$`)),
	)

	// Route message with only one matching property
	r.Route(&mqttv5.Message{
		Topic: "data/sensor",
		UserProperties: []mqttv5.StringPair{
			{Key: "tenant", Value: "acme"},
		},
	})
	assert.False(t, called)

	// Route message with both matching properties
	r.Route(&mqttv5.Message{
		Topic: "data/sensor",
		UserProperties: []mqttv5.StringPair{
			{Key: "tenant", Value: "acme"},
			{Key: "env", Value: "prod"},
		},
	})
	assert.True(t, called)
}

func TestRouterWithUserPropertyRegexp(t *testing.T) {
	r := New()

	var called bool

	// Handler matching any key starting with "x-" and numeric value
	r.Handle(func(_ *mqttv5.Message) {
		called = true
	}, WithTopic("data/#"), WithUserProperty(regexp.MustCompile(`^x-`), regexp.MustCompile(`^\d+$`)))

	// Route message with non-matching property
	r.Route(&mqttv5.Message{
		Topic: "data/sensor",
		UserProperties: []mqttv5.StringPair{
			{Key: "x-custom", Value: "text"},
		},
	})
	assert.False(t, called)

	// Route message with matching property
	r.Route(&mqttv5.Message{
		Topic: "data/sensor",
		UserProperties: []mqttv5.StringPair{
			{Key: "x-priority", Value: "123"},
		},
	})
	assert.True(t, called)
}

func TestRouterWithNamespace(t *testing.T) {
	r := New()

	var acmeCalled, betaCalled bool

	// Handler for acme namespace
	r.Handle(func(_ *mqttv5.Message) {
		acmeCalled = true
	}, WithTopic("data/#"), WithNamespace("acme"))

	// Handler for beta namespace
	r.Handle(func(_ *mqttv5.Message) {
		betaCalled = true
	}, WithTopic("data/#"), WithNamespace("beta"))

	// Route message from acme namespace
	r.Route(&mqttv5.Message{Topic: "data/sensor", Namespace: "acme"})

	assert.True(t, acmeCalled)
	assert.False(t, betaCalled)
}

func TestRouterWithNamespaceMixed(t *testing.T) {
	r := New()

	var allCalled, acmeCalled bool

	// Handler for all namespaces
	r.Handle(func(_ *mqttv5.Message) {
		allCalled = true
	}, WithTopic("data/#"))

	// Handler only for acme namespace
	r.Handle(func(_ *mqttv5.Message) {
		acmeCalled = true
	}, WithTopic("data/#"), WithNamespace("acme"))

	// Route message from beta namespace
	r.Route(&mqttv5.Message{Topic: "data/sensor", Namespace: "beta"})

	assert.True(t, allCalled)
	assert.False(t, acmeCalled)

	// Reset
	allCalled = false
	acmeCalled = false

	// Route message from acme namespace
	r.Route(&mqttv5.Message{Topic: "data/sensor", Namespace: "acme"})

	assert.True(t, allCalled)
	assert.True(t, acmeCalled)
}

func TestRouterWithoutTopic(t *testing.T) {
	r := New()

	var called bool

	// Handler without topic filter matches all topics
	r.Handle(func(_ *mqttv5.Message) {
		called = true
	}, WithQoS(mqttv5.QoS1))

	// Route QoS 0 - should not match
	r.Route(&mqttv5.Message{Topic: "any/topic", QoS: mqttv5.QoS0})
	assert.False(t, called)

	// Route QoS 1 - should match
	r.Route(&mqttv5.Message{Topic: "any/topic", QoS: mqttv5.QoS1})
	assert.True(t, called)
}

func TestRouterConcurrentAccess(t *testing.T) {
	r := New()

	// Register handlers concurrently
	done := make(chan struct{})
	for i := range 10 {
		go func(n int) {
			r.Handle(func(_ *mqttv5.Message) {}, WithTopic("topic/"+string(rune('a'+n))))
			done <- struct{}{}
		}(i)
	}
	for range 10 {
		<-done
	}

	// Route messages concurrently
	for range 10 {
		go func() {
			r.Route(&mqttv5.Message{Topic: "topic/a"})
			done <- struct{}{}
		}()
	}
	for range 10 {
		<-done
	}

	assert.Equal(t, 10, r.Len())
}
