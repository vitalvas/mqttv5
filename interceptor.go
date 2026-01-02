package mqttv5

import "log"

// ProducerInterceptor is an interface that allows interception and modification
// of messages before they are published. Interceptors are called in the order
// they are configured, and each interceptor receives the message from the
// previous interceptor in the chain.
//
// Similar to Sarama's ProducerInterceptor for Kafka clients.
type ProducerInterceptor interface {
	// OnSend is called when a message is about to be published.
	// The interceptor can modify the message before it is sent.
	// Return the (potentially modified) message to continue the chain.
	//
	// WARNING: The message is NOT a copy. Modifications will affect the original.
	// Use msg.Clone() if you need to preserve the original message.
	OnSend(msg *Message) *Message
}

// ConsumerInterceptor is an interface that allows interception and modification
// of messages after they are received but before they are delivered to handlers.
// Interceptors are called in the order they are configured, and each interceptor
// receives the message from the previous interceptor in the chain.
//
// Similar to Sarama's ConsumerInterceptor for Kafka clients.
type ConsumerInterceptor interface {
	// OnConsume is called when a message is received.
	// The interceptor can modify the message before it is delivered to handlers.
	// Return the (potentially modified) message to continue the chain.
	//
	// WARNING: The message is NOT a copy. Modifications will affect the original.
	// Use msg.Clone() if you need to preserve the original message.
	OnConsume(msg *Message) *Message
}

// safelyApplyProducerInterceptor applies a producer interceptor with panic recovery.
// If the interceptor panics, the original message is returned unchanged.
func safelyApplyProducerInterceptor(interceptor ProducerInterceptor, msg *Message) (result *Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("mqttv5: producer interceptor panic: %v", r)
			result = msg
		}
	}()
	return interceptor.OnSend(msg)
}

// safelyApplyConsumerInterceptor applies a consumer interceptor with panic recovery.
// If the interceptor panics, the original message is returned unchanged.
func safelyApplyConsumerInterceptor(interceptor ConsumerInterceptor, msg *Message) (result *Message) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("mqttv5: consumer interceptor panic: %v", r)
			result = msg
		}
	}()
	return interceptor.OnConsume(msg)
}

// applyProducerInterceptors applies all producer interceptors in order.
// Each interceptor receives the message from the previous interceptor.
// If any interceptor returns nil, the chain is broken and nil is returned.
func applyProducerInterceptors(interceptors []ProducerInterceptor, msg *Message) *Message {
	if len(interceptors) == 0 {
		return msg
	}
	current := msg
	for _, interceptor := range interceptors {
		if current == nil {
			return nil
		}
		current = safelyApplyProducerInterceptor(interceptor, current)
	}
	return current
}

// applyConsumerInterceptors applies all consumer interceptors in order.
// Each interceptor receives the message from the previous interceptor.
// If any interceptor returns nil, the chain is broken and nil is returned.
func applyConsumerInterceptors(interceptors []ConsumerInterceptor, msg *Message) *Message {
	if len(interceptors) == 0 {
		return msg
	}
	current := msg
	for _, interceptor := range interceptors {
		if current == nil {
			return nil
		}
		current = safelyApplyConsumerInterceptor(interceptor, current)
	}
	return current
}
