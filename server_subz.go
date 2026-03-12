package mqttv5

// SubscriptionInfo represents detailed information about a single subscription.
type SubscriptionInfo struct {
	ClientID        string `json:"client_id"`
	Namespace       string `json:"namespace"`
	TopicFilter     string `json:"topic_filter"`
	QoS             byte   `json:"qos"`
	NoLocal         bool   `json:"no_local,omitempty"`
	RetainAsPublish bool   `json:"retain_as_publish,omitempty"`
	RetainHandling  byte   `json:"retain_handling,omitempty"`
	SubscriptionID  uint32 `json:"subscription_id,omitempty"`
	Shared          bool   `json:"shared,omitempty"`
	ShareGroup      string `json:"share_group,omitempty"`
}

// SubscriptionSummary represents a summary of subscription statistics.
type SubscriptionSummary struct {
	TotalSubscriptions int                `json:"total_subscriptions"`
	TotalClients       int                `json:"total_clients"`
	SharedGroups       int                `json:"shared_groups"`
	Subscriptions      []SubscriptionInfo `json:"subscriptions"`
}

// GetNamespaceSubscriptions returns all subscriptions in a namespace.
// If namespace is empty, returns subscriptions across all namespaces.
func (s *Server) GetNamespaceSubscriptions(namespace string) []SubscriptionInfo {
	return s.subs.NamespaceSubscriptions(namespace)
}

// GetSubscriptionSummary returns subscription summary for a namespace.
// If namespace is empty, returns subscriptions across all namespaces.
func (s *Server) GetSubscriptionSummary(namespace string) SubscriptionSummary {
	return s.subs.Summary(namespace)
}

// GetClientSubscriptions returns subscriptions for a specific client.
func (s *Server) GetClientSubscriptions(namespace, clientID string) []SubscriptionInfo {
	return s.subs.ClientSubscriptionInfo(namespace, clientID)
}

// TopicMetrics returns the topic metrics tracker.
func (s *Server) TopicMetrics() *TopicMetrics {
	return s.topicMetrics
}

// GetTopicMetrics returns metrics for a specific topic including subscriber count.
// Returns nil if no metrics have been recorded for this topic.
func (s *Server) GetTopicMetrics(namespace, topic string) *TopicMetricsInfo {
	info := s.topicMetrics.Get(namespace, topic)
	if info == nil {
		return nil
	}
	info.Subscribers = s.subs.SubscriberCount(namespace, topic)
	return info
}

// AllTopicMetrics returns metrics for all tracked topics, optionally filtered by namespace.
// Each entry is enriched with the current subscriber count.
func (s *Server) AllTopicMetrics(namespace string) []TopicMetricsInfo {
	infos := s.topicMetrics.All(namespace)
	for i := range infos {
		infos[i].Subscribers = s.subs.SubscriberCount(infos[i].Namespace, infos[i].Topic)
	}
	return infos
}
