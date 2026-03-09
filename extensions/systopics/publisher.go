package systopics

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/vitalvas/mqttv5"
)

// Publisher periodically publishes broker statistics to $SYS topics.
type Publisher struct {
	broker Broker
	config config
	load   *loadCalculator
}

// New creates a new $SYS topic publisher.
func New(broker Broker, opts ...Option) *Publisher {
	cfg := config{
		interval: defaultInterval,
		groups:   make(map[TopicGroup]bool),
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return &Publisher{
		broker: broker,
		config: cfg,
		load:   newLoadCalculator(cfg.interval),
	}
}

// Run starts the periodic publishing loop. It blocks until the context is cancelled.
func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.config.interval)
	defer ticker.Stop()

	// Publish immediately on start.
	p.publish()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.publish()
		}
	}
}

func (p *Publisher) enabled(g TopicGroup) bool {
	return p.config.groups[g]
}

func (p *Publisher) publish() {
	now := time.Now()

	// Collect raw counters for load calculation.
	totalMsgRecv := p.totalMessagesReceived()
	totalMsgSent := p.totalMessagesSent()
	totalBytesRecv := p.broker.TotalBytesReceived()
	totalBytesSent := p.broker.TotalBytesSent()
	totalConns := p.broker.ConnectionsTotal()

	// Always update load calculator to keep EMAs accurate.
	p.load.Update(loadMessagesRecv, totalMsgRecv)
	p.load.Update(loadMessagesSent, totalMsgSent)
	p.load.Update(loadBytesRecv, totalBytesRecv)
	p.load.Update(loadBytesSent, totalBytesSent)
	p.load.Update(loadConnections, totalConns)

	var topics []sysMessage

	if p.enabled(TopicGroupBrokerInfo) {
		topics = append(topics,
			sysMessage{topic: TopicVersion, value: p.config.version},
			sysMessage{topic: TopicUptime, value: formatInt(int64(now.Sub(p.broker.StartedAt()).Seconds()))},
			sysMessage{topic: TopicTimestamp, value: formatInt(now.Unix())},
		)
	}

	if p.enabled(TopicGroupClients) {
		connected := p.broker.Connections()
		disconnected := max(totalConns-connected, 0)

		// In isolated namespace mode, skip TopicClientsConnected here
		// because publishNamespaceTopics publishes it per-namespace.
		if p.config.namespaceMode == NamespaceModeIsolated && p.enabled(TopicGroupNamespace) {
			topics = append(topics,
				sysMessage{topic: TopicClientsTotal, value: formatInt(totalConns)},
				sysMessage{topic: TopicClientsMaximum, value: formatInt(p.broker.MaxConnections())},
				sysMessage{topic: TopicClientsDisconnected, value: formatInt(disconnected)},
			)
		} else {
			topics = append(topics,
				sysMessage{topic: TopicClientsConnected, value: formatInt(connected)},
				sysMessage{topic: TopicClientsTotal, value: formatInt(totalConns)},
				sysMessage{topic: TopicClientsMaximum, value: formatInt(p.broker.MaxConnections())},
				sysMessage{topic: TopicClientsDisconnected, value: formatInt(disconnected)},
			)
		}
	}

	if p.enabled(TopicGroupMessages) {
		topics = append(topics,
			sysMessage{topic: TopicMessagesReceived, value: formatInt(totalMsgRecv)},
			sysMessage{topic: TopicMessagesSent, value: formatInt(totalMsgSent)},
			sysMessage{topic: TopicMessagesStored, value: formatInt(p.broker.RetainedMessages())},
			sysMessage{topic: TopicMessagesPublishRecv, value: formatInt(totalMsgRecv)},
			sysMessage{topic: TopicMessagesPublishSent, value: formatInt(totalMsgSent)},
			sysMessage{topic: TopicMessagesReceivedQoS0, value: formatInt(p.broker.TotalMessagesReceived(0))},
			sysMessage{topic: TopicMessagesReceivedQoS1, value: formatInt(p.broker.TotalMessagesReceived(1))},
			sysMessage{topic: TopicMessagesReceivedQoS2, value: formatInt(p.broker.TotalMessagesReceived(2))},
			sysMessage{topic: TopicMessagesSentQoS0, value: formatInt(p.broker.TotalMessagesSent(0))},
			sysMessage{topic: TopicMessagesSentQoS1, value: formatInt(p.broker.TotalMessagesSent(1))},
			sysMessage{topic: TopicMessagesSentQoS2, value: formatInt(p.broker.TotalMessagesSent(2))},
		)
	}

	if p.enabled(TopicGroupBytes) {
		topics = append(topics,
			sysMessage{topic: TopicBytesReceived, value: formatInt(totalBytesRecv)},
			sysMessage{topic: TopicBytesSent, value: formatInt(totalBytesSent)},
		)
	}

	if p.enabled(TopicGroupSubscriptions) {
		topics = append(topics,
			sysMessage{topic: TopicSubscriptionsCount, value: formatInt(p.broker.Subscriptions())},
		)
	}

	if p.enabled(TopicGroupRetained) {
		topics = append(topics,
			sysMessage{topic: TopicRetainedMessagesCount, value: formatInt(p.broker.RetainedMessages())},
		)
	}

	if p.enabled(TopicGroupLoad) {
		topics = append(topics, p.loadTopics()...)
	}

	if p.enabled(TopicGroupPackets) {
		topics = append(topics, p.packetTopics()...)
	}

	if p.enabled(TopicGroupTopics) {
		topics = append(topics,
			sysMessage{topic: TopicTopicsCount, value: formatInt(p.broker.TopicCount())},
		)
	}

	for _, tv := range topics {
		if err := p.broker.Publish(&mqttv5.Message{
			Topic:   tv.topic,
			Payload: []byte(tv.value),
			Retain:  true,
		}); err != nil {
			p.reportError(tv.topic, err)
		}
	}

	if p.enabled(TopicGroupNamespace) {
		p.publishNamespaceTopics()
	}
}

func (p *Publisher) publishNamespaceTopics() {
	namespaces := p.broker.Namespaces()

	if err := p.broker.Publish(&mqttv5.Message{
		Topic:   TopicNamespacesCount,
		Payload: []byte(formatInt(int64(len(namespaces)))),
		Retain:  true,
	}); err != nil {
		p.reportError(TopicNamespacesCount, err)
	}

	if len(namespaces) == 0 {
		return
	}

	switch p.config.namespaceMode {
	case NamespaceModeGlobal:
		for _, ns := range namespaces {
			count := p.broker.ClientCount(ns)
			topic := fmt.Sprintf("$SYS/broker/clients/namespace/%s/connected", ns)

			if err := p.broker.Publish(&mqttv5.Message{
				Topic:   topic,
				Payload: []byte(formatInt(int64(count))),
				Retain:  true,
			}); err != nil {
				p.reportError(topic, err)
			}
		}

	case NamespaceModeIsolated:
		// Publish standard $SYS topics with Namespace set on the message,
		// so the server delivers them only to clients in that namespace.
		// Clients never see the namespace name in the topic path.
		for _, ns := range namespaces {
			count := p.broker.ClientCount(ns)

			if err := p.broker.Publish(&mqttv5.Message{
				Topic:     TopicClientsConnected,
				Payload:   []byte(formatInt(int64(count))),
				Retain:    true,
				Namespace: ns,
			}); err != nil {
				p.reportError(TopicClientsConnected, err)
			}
		}
	}
}

func (p *Publisher) totalMessagesReceived() int64 {
	var total int64
	for qos := byte(0); qos <= 2; qos++ {
		total += p.broker.TotalMessagesReceived(qos)
	}

	return total
}

func (p *Publisher) totalMessagesSent() int64 {
	var total int64
	for qos := byte(0); qos <= 2; qos++ {
		total += p.broker.TotalMessagesSent(qos)
	}

	return total
}

func (p *Publisher) loadTopics() []sysMessage {
	msgRecv := p.load.Rates(loadMessagesRecv)
	msgSent := p.load.Rates(loadMessagesSent)
	bytesRecv := p.load.Rates(loadBytesRecv)
	bytesSent := p.load.Rates(loadBytesSent)
	conns := p.load.Rates(loadConnections)

	return []sysMessage{
		{topic: TopicLoadMessagesRecv1min, value: formatFloat(msgRecv.Min1)},
		{topic: TopicLoadMessagesRecv5min, value: formatFloat(msgRecv.Min5)},
		{topic: TopicLoadMessagesRecv15min, value: formatFloat(msgRecv.Min15)},
		{topic: TopicLoadMessagesSent1min, value: formatFloat(msgSent.Min1)},
		{topic: TopicLoadMessagesSent5min, value: formatFloat(msgSent.Min5)},
		{topic: TopicLoadMessagesSent15min, value: formatFloat(msgSent.Min15)},
		{topic: TopicLoadBytesRecv1min, value: formatFloat(bytesRecv.Min1)},
		{topic: TopicLoadBytesRecv5min, value: formatFloat(bytesRecv.Min5)},
		{topic: TopicLoadBytesRecv15min, value: formatFloat(bytesRecv.Min15)},
		{topic: TopicLoadBytesSent1min, value: formatFloat(bytesSent.Min1)},
		{topic: TopicLoadBytesSent5min, value: formatFloat(bytesSent.Min5)},
		{topic: TopicLoadBytesSent15min, value: formatFloat(bytesSent.Min15)},
		{topic: TopicLoadConnections1min, value: formatFloat(conns.Min1)},
		{topic: TopicLoadConnections5min, value: formatFloat(conns.Min5)},
		{topic: TopicLoadConnections15min, value: formatFloat(conns.Min15)},
	}
}

// allPacketTypes lists all MQTT v5 packet types for iteration.
var allPacketTypes = []mqttv5.PacketType{
	mqttv5.PacketCONNECT, mqttv5.PacketCONNACK,
	mqttv5.PacketPUBLISH, mqttv5.PacketPUBACK,
	mqttv5.PacketPUBREC, mqttv5.PacketPUBREL, mqttv5.PacketPUBCOMP,
	mqttv5.PacketSUBSCRIBE, mqttv5.PacketSUBACK,
	mqttv5.PacketUNSUBSCRIBE, mqttv5.PacketUNSUBACK,
	mqttv5.PacketPINGREQ, mqttv5.PacketPINGRESP,
	mqttv5.PacketDISCONNECT, mqttv5.PacketAUTH,
}

func (p *Publisher) packetTopics() []sysMessage {
	msgs := make([]sysMessage, 0, 2*len(allPacketTypes))

	for _, pt := range allPacketTypes {
		name := strings.ToLower(pt.String())

		recv := p.broker.PacketsReceived(pt)
		sent := p.broker.PacketsSent(pt)

		msgs = append(msgs,
			sysMessage{topic: TopicPacketsReceivedPrefix + name, value: formatInt(recv)},
			sysMessage{topic: TopicPacketsSentPrefix + name, value: formatInt(sent)},
		)
	}

	return msgs
}

func (p *Publisher) reportError(topic string, err error) {
	if p.config.onPublishError != nil {
		p.config.onPublishError(topic, err)
	}
}

type sysMessage struct {
	topic string
	value string
}

func formatInt(v int64) string {
	return strconv.FormatInt(v, 10)
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', 2, 64)
}
