package lifecycle

// Option configures the Handler.
type Option func(*Handler)

// WithOnPublishError sets a callback invoked when publishing a lifecycle event fails.
func WithOnPublishError(fn func(topic string, err error)) Option {
	return func(h *Handler) {
		h.onPublishError = fn
	}
}

// WithNamespace sets a fixed namespace for all published lifecycle events.
// When set, this takes priority over WithClientNamespace and DefaultNamespace.
func WithNamespace(namespace string) Option {
	return func(h *Handler) {
		h.namespace = namespace
	}
}

// WithClientNamespace enables publishing events in the client's namespace
// instead of the default namespace. When enabled, subscribers only receive
// events for clients in their own namespace.
// Default: false (all events published in DefaultNamespace).
func WithClientNamespace(enabled bool) Option {
	return func(h *Handler) {
		h.useClientNamespace = enabled
	}
}
