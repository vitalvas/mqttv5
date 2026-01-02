package mqttv5

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testProducerInterceptor is a test implementation of ProducerInterceptor.
type testProducerInterceptor struct {
	called   bool
	modifier func(*Message) *Message
}

func (i *testProducerInterceptor) OnSend(msg *Message) *Message {
	i.called = true
	if i.modifier != nil {
		return i.modifier(msg)
	}
	return msg
}

// testConsumerInterceptor is a test implementation of ConsumerInterceptor.
type testConsumerInterceptor struct {
	called   bool
	modifier func(*Message) *Message
}

func (i *testConsumerInterceptor) OnConsume(msg *Message) *Message {
	i.called = true
	if i.modifier != nil {
		return i.modifier(msg)
	}
	return msg
}

// panicProducerInterceptor panics when OnSend is called.
type panicProducerInterceptor struct{}

func (i *panicProducerInterceptor) OnSend(_ *Message) *Message {
	panic("producer interceptor panic")
}

// panicConsumerInterceptor panics when OnConsume is called.
type panicConsumerInterceptor struct{}

func (i *panicConsumerInterceptor) OnConsume(_ *Message) *Message {
	panic("consumer interceptor panic")
}

func TestApplyProducerInterceptors(t *testing.T) {
	t.Run("no interceptors returns original message", func(t *testing.T) {
		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyProducerInterceptors(nil, msg)
		assert.Equal(t, msg, result)
	})

	t.Run("empty interceptors slice returns original message", func(t *testing.T) {
		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyProducerInterceptors([]ProducerInterceptor{}, msg)
		assert.Equal(t, msg, result)
	})

	t.Run("single interceptor is called", func(t *testing.T) {
		interceptor := &testProducerInterceptor{}
		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyProducerInterceptors([]ProducerInterceptor{interceptor}, msg)

		assert.True(t, interceptor.called)
		assert.Equal(t, msg, result)
	})

	t.Run("interceptor can modify message", func(t *testing.T) {
		interceptor := &testProducerInterceptor{
			modifier: func(msg *Message) *Message {
				msg.Topic = "modified"
				return msg
			},
		}
		msg := &Message{Topic: "original", Payload: []byte("data")}
		result := applyProducerInterceptors([]ProducerInterceptor{interceptor}, msg)

		assert.Equal(t, "modified", result.Topic)
	})

	t.Run("interceptor can filter message by returning nil", func(t *testing.T) {
		interceptor := &testProducerInterceptor{
			modifier: func(_ *Message) *Message {
				return nil
			},
		}
		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyProducerInterceptors([]ProducerInterceptor{interceptor}, msg)

		assert.Nil(t, result)
	})

	t.Run("multiple interceptors are called in order", func(t *testing.T) {
		callOrder := make([]int, 0, 3)
		interceptor1 := &testProducerInterceptor{
			modifier: func(msg *Message) *Message {
				callOrder = append(callOrder, 1)
				msg.Topic += "-1"
				return msg
			},
		}
		interceptor2 := &testProducerInterceptor{
			modifier: func(msg *Message) *Message {
				callOrder = append(callOrder, 2)
				msg.Topic += "-2"
				return msg
			},
		}
		interceptor3 := &testProducerInterceptor{
			modifier: func(msg *Message) *Message {
				callOrder = append(callOrder, 3)
				msg.Topic += "-3"
				return msg
			},
		}

		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyProducerInterceptors([]ProducerInterceptor{interceptor1, interceptor2, interceptor3}, msg)

		assert.Equal(t, []int{1, 2, 3}, callOrder)
		assert.Equal(t, "test-1-2-3", result.Topic)
	})

	t.Run("chain breaks when interceptor returns nil", func(t *testing.T) {
		interceptor1 := &testProducerInterceptor{}
		interceptor2 := &testProducerInterceptor{
			modifier: func(_ *Message) *Message {
				return nil
			},
		}
		interceptor3 := &testProducerInterceptor{}

		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyProducerInterceptors([]ProducerInterceptor{interceptor1, interceptor2, interceptor3}, msg)

		assert.True(t, interceptor1.called)
		assert.True(t, interceptor2.called)
		assert.False(t, interceptor3.called) // Should not be called after nil
		assert.Nil(t, result)
	})

	t.Run("panic in interceptor recovers and returns original message", func(t *testing.T) {
		panicInterceptor := &panicProducerInterceptor{}
		msg := &Message{Topic: "test", Payload: []byte("data")}

		result := safelyApplyProducerInterceptor(panicInterceptor, msg)

		assert.Equal(t, msg, result)
	})
}

func TestApplyConsumerInterceptors(t *testing.T) {
	t.Run("no interceptors returns original message", func(t *testing.T) {
		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyConsumerInterceptors(nil, msg)
		assert.Equal(t, msg, result)
	})

	t.Run("empty interceptors slice returns original message", func(t *testing.T) {
		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyConsumerInterceptors([]ConsumerInterceptor{}, msg)
		assert.Equal(t, msg, result)
	})

	t.Run("single interceptor is called", func(t *testing.T) {
		interceptor := &testConsumerInterceptor{}
		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyConsumerInterceptors([]ConsumerInterceptor{interceptor}, msg)

		assert.True(t, interceptor.called)
		assert.Equal(t, msg, result)
	})

	t.Run("interceptor can modify message", func(t *testing.T) {
		interceptor := &testConsumerInterceptor{
			modifier: func(msg *Message) *Message {
				msg.Payload = []byte("modified")
				return msg
			},
		}
		msg := &Message{Topic: "test", Payload: []byte("original")}
		result := applyConsumerInterceptors([]ConsumerInterceptor{interceptor}, msg)

		assert.Equal(t, []byte("modified"), result.Payload)
	})

	t.Run("interceptor can filter message by returning nil", func(t *testing.T) {
		interceptor := &testConsumerInterceptor{
			modifier: func(_ *Message) *Message {
				return nil
			},
		}
		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyConsumerInterceptors([]ConsumerInterceptor{interceptor}, msg)

		assert.Nil(t, result)
	})

	t.Run("multiple interceptors are called in order", func(t *testing.T) {
		callOrder := make([]int, 0, 3)
		interceptor1 := &testConsumerInterceptor{
			modifier: func(msg *Message) *Message {
				callOrder = append(callOrder, 1)
				return msg
			},
		}
		interceptor2 := &testConsumerInterceptor{
			modifier: func(msg *Message) *Message {
				callOrder = append(callOrder, 2)
				return msg
			},
		}
		interceptor3 := &testConsumerInterceptor{
			modifier: func(msg *Message) *Message {
				callOrder = append(callOrder, 3)
				return msg
			},
		}

		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyConsumerInterceptors([]ConsumerInterceptor{interceptor1, interceptor2, interceptor3}, msg)

		assert.Equal(t, []int{1, 2, 3}, callOrder)
		assert.NotNil(t, result)
	})

	t.Run("chain breaks when interceptor returns nil", func(t *testing.T) {
		interceptor1 := &testConsumerInterceptor{}
		interceptor2 := &testConsumerInterceptor{
			modifier: func(_ *Message) *Message {
				return nil
			},
		}
		interceptor3 := &testConsumerInterceptor{}

		msg := &Message{Topic: "test", Payload: []byte("data")}
		result := applyConsumerInterceptors([]ConsumerInterceptor{interceptor1, interceptor2, interceptor3}, msg)

		assert.True(t, interceptor1.called)
		assert.True(t, interceptor2.called)
		assert.False(t, interceptor3.called)
		assert.Nil(t, result)
	})

	t.Run("panic in interceptor recovers and returns original message", func(t *testing.T) {
		panicInterceptor := &panicConsumerInterceptor{}
		msg := &Message{Topic: "test", Payload: []byte("data")}

		result := safelyApplyConsumerInterceptor(panicInterceptor, msg)

		assert.Equal(t, msg, result)
	})
}

func TestClientInterceptorOptions(t *testing.T) {
	t.Run("WithProducerInterceptors adds interceptors", func(t *testing.T) {
		i1, i2 := &testProducerInterceptor{}, &testProducerInterceptor{}
		opts := applyOptions(WithProducerInterceptors(i1), WithProducerInterceptors(i2))
		require.Len(t, opts.producerInterceptors, 2)
	})

	t.Run("WithConsumerInterceptors adds interceptors", func(t *testing.T) {
		i1, i2 := &testConsumerInterceptor{}, &testConsumerInterceptor{}
		opts := applyOptions(WithConsumerInterceptors(i1), WithConsumerInterceptors(i2))
		require.Len(t, opts.consumerInterceptors, 2)
	})

	t.Run("multiple interceptors in single call", func(t *testing.T) {
		i1, i2 := &testProducerInterceptor{}, &testProducerInterceptor{}
		opts := applyOptions(WithProducerInterceptors(i1, i2))
		require.Len(t, opts.producerInterceptors, 2)
		assert.Equal(t, i1, opts.producerInterceptors[0])
		assert.Equal(t, i2, opts.producerInterceptors[1])
	})
}

func TestServerInterceptorOptions(t *testing.T) {
	t.Run("WithServerProducerInterceptors adds interceptors", func(t *testing.T) {
		i1, i2 := &testProducerInterceptor{}, &testProducerInterceptor{}
		config := defaultServerConfig()
		WithServerProducerInterceptors(i1)(config)
		WithServerProducerInterceptors(i2)(config)
		require.Len(t, config.producerInterceptors, 2)
	})

	t.Run("WithServerConsumerInterceptors adds interceptors", func(t *testing.T) {
		i1, i2 := &testConsumerInterceptor{}, &testConsumerInterceptor{}
		config := defaultServerConfig()
		WithServerConsumerInterceptors(i1)(config)
		WithServerConsumerInterceptors(i2)(config)
		require.Len(t, config.consumerInterceptors, 2)
	})

	t.Run("multiple interceptors in single call", func(t *testing.T) {
		i1, i2 := &testProducerInterceptor{}, &testProducerInterceptor{}
		config := defaultServerConfig()
		WithServerProducerInterceptors(i1, i2)(config)
		require.Len(t, config.producerInterceptors, 2)
		assert.Equal(t, i1, config.producerInterceptors[0])
		assert.Equal(t, i2, config.producerInterceptors[1])
	})
}
