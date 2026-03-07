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

// GetSubscriptionSummary returns subscription summary for a namespace.
// If namespace is empty, returns subscriptions across all namespaces.
func (s *Server) GetSubscriptionSummary(namespace string) SubscriptionSummary {
	return s.subs.Summary(namespace)
}

// GetClientSubscriptions returns subscriptions for a specific client.
func (s *Server) GetClientSubscriptions(namespace, clientID string) []SubscriptionInfo {
	return s.subs.ClientSubscriptionInfo(namespace, clientID)
}
