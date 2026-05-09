package mqttv5

import "errors"

// ErrTLSRequired is returned when TLS configuration is required but not provided.
var ErrTLSRequired = errors.New("TLS configuration is required for QUIC")

// ErrQUICNotEnabled is returned when QUIC transport is used but the binary
// was built without the "quic" build tag.
var ErrQUICNotEnabled = errors.New("QUIC support not compiled in (rebuild with -tags quic)")
