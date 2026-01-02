package mqttv5

import "time"

// WillMessage represents an MQTT Last Will and Testament message.
type WillMessage struct {
	// Topic is the will topic.
	Topic string

	// Payload is the will payload.
	Payload []byte

	// QoS is the quality of service level (0, 1, or 2).
	QoS byte

	// Retain indicates if the will message should be retained.
	Retain bool

	// DelayInterval is the will delay interval in seconds.
	// The server delays publishing the will message until this interval expires
	// or the session expires, whichever happens first.
	DelayInterval uint32

	// PayloadFormat indicates if the payload is UTF-8 encoded (1) or binary (0).
	PayloadFormat byte

	// MessageExpiry is the message expiry interval in seconds.
	MessageExpiry uint32

	// ContentType is the MIME type of the payload.
	ContentType string

	// ResponseTopic is the topic name for a response message.
	ResponseTopic string

	// CorrelationData is used to correlate a request with a response.
	CorrelationData []byte

	// UserProperties are application-defined properties.
	UserProperties []StringPair

	// Namespace is the namespace for multi-tenancy isolation.
	Namespace string
}

// WillMessageFromConnect extracts the will message from a CONNECT packet.
func WillMessageFromConnect(pkt *ConnectPacket) *WillMessage {
	if !pkt.WillFlag {
		return nil
	}

	will := &WillMessage{
		Topic:   pkt.WillTopic,
		Payload: pkt.WillPayload,
		QoS:     pkt.WillQoS,
		Retain:  pkt.WillRetain,
	}

	// Extract will properties
	if pkt.WillProps.Len() > 0 {
		will.DelayInterval = pkt.WillProps.GetUint32(PropWillDelayInterval)
		will.PayloadFormat = pkt.WillProps.GetByte(PropPayloadFormatIndicator)
		will.MessageExpiry = pkt.WillProps.GetUint32(PropMessageExpiryInterval)
		will.ContentType = pkt.WillProps.GetString(PropContentType)
		will.ResponseTopic = pkt.WillProps.GetString(PropResponseTopic)
		will.CorrelationData = pkt.WillProps.GetBinary(PropCorrelationData)
		will.UserProperties = pkt.WillProps.GetAllStringPairs(PropUserProperty)
	}

	return will
}

// ToMessage converts a WillMessage to a Message for publishing.
func (w *WillMessage) ToMessage() *Message {
	return &Message{
		Topic:           w.Topic,
		Payload:         w.Payload,
		QoS:             w.QoS,
		Retain:          w.Retain,
		PayloadFormat:   w.PayloadFormat,
		MessageExpiry:   w.MessageExpiry,
		ContentType:     w.ContentType,
		ResponseTopic:   w.ResponseTopic,
		CorrelationData: w.CorrelationData,
		UserProperties:  w.UserProperties,
		Namespace:       w.Namespace,
	}
}

// ToProperties converts will message properties to Properties.
func (w *WillMessage) ToProperties() *Properties {
	props := &Properties{}

	if w.DelayInterval > 0 {
		props.Set(PropWillDelayInterval, w.DelayInterval)
	}
	if w.PayloadFormat > 0 {
		props.Set(PropPayloadFormatIndicator, w.PayloadFormat)
	}
	if w.MessageExpiry > 0 {
		props.Set(PropMessageExpiryInterval, w.MessageExpiry)
	}
	if w.ContentType != "" {
		props.Set(PropContentType, w.ContentType)
	}
	if w.ResponseTopic != "" {
		props.Set(PropResponseTopic, w.ResponseTopic)
	}
	if len(w.CorrelationData) > 0 {
		props.Set(PropCorrelationData, w.CorrelationData)
	}
	for _, up := range w.UserProperties {
		props.Add(PropUserProperty, up)
	}

	return props
}

// Validate validates the will message.
func (w *WillMessage) Validate() error {
	if err := ValidateTopicName(w.Topic); err != nil {
		return err
	}
	if w.QoS > 2 {
		return ErrInvalidQoS
	}
	return nil
}

// PendingWill represents a will message pending publication after delay.
type PendingWill struct {
	Will      *WillMessage
	ClientID  string
	PublishAt time.Time
}

// NewPendingWill creates a pending will with the appropriate delay.
func NewPendingWill(clientID string, will *WillMessage) *PendingWill {
	publishAt := time.Now()
	if will.DelayInterval > 0 {
		publishAt = publishAt.Add(time.Duration(will.DelayInterval) * time.Second)
	}

	return &PendingWill{
		Will:      will,
		ClientID:  clientID,
		PublishAt: publishAt,
	}
}

// IsReady returns true if the will is ready to be published.
func (p *PendingWill) IsReady() bool {
	return time.Now().After(p.PublishAt) || time.Now().Equal(p.PublishAt)
}

// TimeUntilPublish returns the duration until the will should be published.
func (p *PendingWill) TimeUntilPublish() time.Duration {
	remaining := time.Until(p.PublishAt)
	if remaining < 0 {
		return 0
	}
	return remaining
}
