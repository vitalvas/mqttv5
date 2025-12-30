package mqttv5

import (
	"errors"
	"sync"
)

var (
	ErrQuotaExceeded = errors.New("receive quota exceeded")
)

// FlowController manages flow control for MQTT v5.0 connections.
// It tracks the receive maximum and manages in-flight message quotas.
// MQTT v5.0 spec: Section 4.9
type FlowController struct {
	mu             sync.Mutex
	receiveMaximum uint16
	inFlight       uint16
}

// NewFlowController creates a new flow controller with the given receive maximum.
// The receive maximum is the maximum number of QoS > 0 PUBLISH packets that
// can be outstanding (sent but not yet acknowledged) at any time.
func NewFlowController(receiveMaximum uint16) *FlowController {
	if receiveMaximum == 0 {
		receiveMaximum = 65535 // Default per MQTT spec
	}
	return &FlowController{
		receiveMaximum: receiveMaximum,
	}
}

// ReceiveMaximum returns the configured receive maximum.
func (f *FlowController) ReceiveMaximum() uint16 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.receiveMaximum
}

// SetReceiveMaximum updates the receive maximum.
func (f *FlowController) SetReceiveMaximum(maximum uint16) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if maximum == 0 {
		maximum = 65535
	}
	f.receiveMaximum = maximum
}

// Available returns the number of available slots for in-flight messages.
func (f *FlowController) Available() uint16 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inFlight >= f.receiveMaximum {
		return 0
	}
	return f.receiveMaximum - f.inFlight
}

// InFlight returns the current number of in-flight messages.
func (f *FlowController) InFlight() uint16 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inFlight
}

// CanSend returns true if there is quota available to send a message.
func (f *FlowController) CanSend() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inFlight < f.receiveMaximum
}

// Acquire attempts to acquire quota for sending a message.
// Returns an error if the quota is exceeded.
func (f *FlowController) Acquire() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.inFlight >= f.receiveMaximum {
		return ErrQuotaExceeded
	}
	f.inFlight++
	return nil
}

// TryAcquire attempts to acquire quota without blocking.
// Returns true if quota was acquired, false otherwise.
func (f *FlowController) TryAcquire() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.inFlight >= f.receiveMaximum {
		return false
	}
	f.inFlight++
	return true
}

// Release releases quota when a message is acknowledged.
func (f *FlowController) Release() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.inFlight > 0 {
		f.inFlight--
	}
}

// Reset resets the in-flight count to zero.
func (f *FlowController) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inFlight = 0
}

// BidirectionalFlowController manages flow control for both sending and receiving.
// MQTT v5.0 spec: Section 4.9
type BidirectionalFlowController struct {
	send    *FlowController
	receive *FlowController
}

// NewBidirectionalFlowController creates a new bidirectional flow controller.
// sendMax is the remote's receive maximum (limits what we can send).
// receiveMax is our receive maximum (limits what we accept).
func NewBidirectionalFlowController(sendMax, receiveMax uint16) *BidirectionalFlowController {
	return &BidirectionalFlowController{
		send:    NewFlowController(sendMax),
		receive: NewFlowController(receiveMax),
	}
}

// Send returns the send flow controller.
func (b *BidirectionalFlowController) Send() *FlowController {
	return b.send
}

// Receive returns the receive flow controller.
func (b *BidirectionalFlowController) Receive() *FlowController {
	return b.receive
}

// CanSend returns true if we can send a message.
func (b *BidirectionalFlowController) CanSend() bool {
	return b.send.CanSend()
}

// AcquireSend attempts to acquire send quota.
func (b *BidirectionalFlowController) AcquireSend() error {
	return b.send.Acquire()
}

// ReleaseSend releases send quota when a message is acknowledged.
func (b *BidirectionalFlowController) ReleaseSend() {
	b.send.Release()
}

// CanReceive returns true if we can receive a message.
func (b *BidirectionalFlowController) CanReceive() bool {
	return b.receive.CanSend()
}

// AcquireReceive attempts to acquire receive quota.
func (b *BidirectionalFlowController) AcquireReceive() error {
	return b.receive.Acquire()
}

// ReleaseReceive releases receive quota when we acknowledge a message.
func (b *BidirectionalFlowController) ReleaseReceive() {
	b.receive.Release()
}

// UpdateSendMaximum updates the send maximum (remote's receive maximum).
func (b *BidirectionalFlowController) UpdateSendMaximum(maximum uint16) {
	b.send.SetReceiveMaximum(maximum)
}

// UpdateReceiveMaximum updates the receive maximum (our receive maximum).
func (b *BidirectionalFlowController) UpdateReceiveMaximum(maximum uint16) {
	b.receive.SetReceiveMaximum(maximum)
}

// Reset resets both flow controllers.
func (b *BidirectionalFlowController) Reset() {
	b.send.Reset()
	b.receive.Reset()
}
