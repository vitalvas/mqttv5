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
			sysMessage{topic: TopicUptime, value: strconv.FormatInt(int64(now.Sub(p.broker.StartedAt()).Seconds()), 10)},
			sysMessage{topic: TopicTimestamp, value: strconv.FormatInt(now.Unix(), 10)},
		)
	}

	if p.enabled(TopicGroupClients) {
		connected := p.broker.Connections()
		disconnected := max(totalConns-connected, 0)

		// In isolated namespace mode, skip TopicClientsConnected here
		// because publishNamespaceTopics publishes it per-namespace.
		if p.config.namespaceMode == NamespaceModeIsolated && p.enabled(TopicGroupNamespace) {
			topics = append(topics,
				sysMessage{topic: TopicClientsTotal, value: strconv.FormatInt(totalConns, 10)},
				sysMessage{topic: TopicClientsMaximum, value: strconv.FormatInt(p.broker.MaxConnections(), 10)},
				sysMessage{topic: TopicClientsDisconnected, value: strconv.FormatInt(disconnected, 10)},
			)
		} else {
			topics = append(topics,
				sysMessage{topic: TopicClientsConnected, value: strconv.FormatInt(connected, 10)},
				sysMessage{topic: TopicClientsTotal, value: strconv.FormatInt(totalConns, 10)},
				sysMessage{topic: TopicClientsMaximum, value: strconv.FormatInt(p.broker.MaxConnections(), 10)},
				sysMessage{topic: TopicClientsDisconnected, value: strconv.FormatInt(disconnected, 10)},
			)
		}
	}

	if p.enabled(TopicGroupMessages) {
		topics = append(topics,
			sysMessage{topic: TopicMessagesReceived, value: strconv.FormatInt(totalMsgRecv, 10)},
			sysMessage{topic: TopicMessagesSent, value: strconv.FormatInt(totalMsgSent, 10)},
			sysMessage{topic: TopicMessagesStored, value: strconv.FormatInt(p.broker.RetainedMessages(), 10)},
			sysMessage{topic: TopicMessagesPublishRecv, value: strconv.FormatInt(totalMsgRecv, 10)},
			sysMessage{topic: TopicMessagesPublishSent, value: strconv.FormatInt(totalMsgSent, 10)},
			sysMessage{topic: TopicMessagesReceivedQoS0, value: strconv.FormatInt(p.broker.TotalMessagesReceived(0), 10)},
			sysMessage{topic: TopicMessagesReceivedQoS1, value: strconv.FormatInt(p.broker.TotalMessagesReceived(1), 10)},
			sysMessage{topic: TopicMessagesReceivedQoS2, value: strconv.FormatInt(p.broker.TotalMessagesReceived(2), 10)},
			sysMessage{topic: TopicMessagesSentQoS0, value: strconv.FormatInt(p.broker.TotalMessagesSent(0), 10)},
			sysMessage{topic: TopicMessagesSentQoS1, value: strconv.FormatInt(p.broker.TotalMessagesSent(1), 10)},
			sysMessage{topic: TopicMessagesSentQoS2, value: strconv.FormatInt(p.broker.TotalMessagesSent(2), 10)},
		)
	}

	if p.enabled(TopicGroupBytes) {
		topics = append(topics,
			sysMessage{topic: TopicBytesReceived, value: strconv.FormatInt(totalBytesRecv, 10)},
			sysMessage{topic: TopicBytesSent, value: strconv.FormatInt(totalBytesSent, 10)},
		)
	}

	if p.enabled(TopicGroupSubscriptions) {
		topics = append(topics,
			sysMessage{topic: TopicSubscriptionsCount, value: strconv.FormatInt(p.broker.Subscriptions(), 10)},
		)
	}

	if p.enabled(TopicGroupRetained) {
		topics = append(topics,
			sysMessage{topic: TopicRetainedMessagesCount, value: strconv.FormatInt(p.broker.RetainedMessages(), 10)},
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
			sysMessage{topic: TopicTopicsCount, value: strconv.FormatInt(p.broker.TopicCount(), 10)},
		)
	}

	namespaces := p.broker.Namespaces()
	if len(namespaces) == 0 {
		namespaces = []string{""}
	}

	// Filter to namespaces that have $SYS/ subscribers.
	sysNamespaces := make([]string, 0, len(namespaces))
	for _, ns := range namespaces {
		if p.broker.HasSubscribersWithPrefix(ns, sysPrefix) {
			sysNamespaces = append(sysNamespaces, ns)
		}
	}

	for _, tv := range topics {
		for _, ns := range sysNamespaces {
			if err := p.broker.Publish(&mqttv5.Message{
				Topic:     tv.topic,
				Payload:   []byte(tv.value),
				Retain:    true,
				Namespace: ns,
			}); err != nil {
				p.reportError(tv.topic, err)
			}
		}
	}

	if p.enabled(TopicGroupNamespace) {
		p.publishNamespaceTopics()
	}
}

func (p *Publisher) publishNamespaceTopics() {
	namespaces := p.broker.Namespaces()
	nsCountPayload := []byte(strconv.FormatInt(int64(len(namespaces)), 10))

	// Publish namespace count to all namespaces with $SYS subscribers.
	targets := namespaces
	if len(targets) == 0 {
		targets = []string{""}
	}

	for _, ns := range targets {
		if !p.broker.HasSubscribersWithPrefix(ns, sysPrefix) {
			continue
		}

		if err := p.broker.Publish(&mqttv5.Message{
			Topic:     TopicNamespacesCount,
			Payload:   nsCountPayload,
			Retain:    true,
			Namespace: ns,
		}); err != nil {
			p.reportError(TopicNamespacesCount, err)
		}
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
				Payload: []byte(strconv.FormatInt(int64(count), 10)),
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
			if !p.broker.HasSubscribersWithPrefix(ns, sysPrefix) {
				continue
			}

			count := p.broker.ClientCount(ns)

			if err := p.broker.Publish(&mqttv5.Message{
				Topic:     TopicClientsConnected,
				Payload:   []byte(strconv.FormatInt(int64(count), 10)),
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
		{topic: TopicLoadMessagesRecv1min, value: strconv.FormatFloat(msgRecv.Min1, 'f', 2, 64)},
		{topic: TopicLoadMessagesRecv5min, value: strconv.FormatFloat(msgRecv.Min5, 'f', 2, 64)},
		{topic: TopicLoadMessagesRecv15min, value: strconv.FormatFloat(msgRecv.Min15, 'f', 2, 64)},
		{topic: TopicLoadMessagesSent1min, value: strconv.FormatFloat(msgSent.Min1, 'f', 2, 64)},
		{topic: TopicLoadMessagesSent5min, value: strconv.FormatFloat(msgSent.Min5, 'f', 2, 64)},
		{topic: TopicLoadMessagesSent15min, value: strconv.FormatFloat(msgSent.Min15, 'f', 2, 64)},
		{topic: TopicLoadBytesRecv1min, value: strconv.FormatFloat(bytesRecv.Min1, 'f', 2, 64)},
		{topic: TopicLoadBytesRecv5min, value: strconv.FormatFloat(bytesRecv.Min5, 'f', 2, 64)},
		{topic: TopicLoadBytesRecv15min, value: strconv.FormatFloat(bytesRecv.Min15, 'f', 2, 64)},
		{topic: TopicLoadBytesSent1min, value: strconv.FormatFloat(bytesSent.Min1, 'f', 2, 64)},
		{topic: TopicLoadBytesSent5min, value: strconv.FormatFloat(bytesSent.Min5, 'f', 2, 64)},
		{topic: TopicLoadBytesSent15min, value: strconv.FormatFloat(bytesSent.Min15, 'f', 2, 64)},
		{topic: TopicLoadConnections1min, value: strconv.FormatFloat(conns.Min1, 'f', 2, 64)},
		{topic: TopicLoadConnections5min, value: strconv.FormatFloat(conns.Min5, 'f', 2, 64)},
		{topic: TopicLoadConnections15min, value: strconv.FormatFloat(conns.Min15, 'f', 2, 64)},
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
			sysMessage{topic: TopicPacketsReceivedPrefix + name, value: strconv.FormatInt(recv, 10)},
			sysMessage{topic: TopicPacketsSentPrefix + name, value: strconv.FormatInt(sent, 10)},
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
