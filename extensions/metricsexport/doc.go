// Package metricsexport provides a Prometheus-compatible HTTP handler that exports
// MQTT broker metrics collected via Go's expvar package.
//
// The handler iterates over all published expvar variables with the "mqtt_" prefix
// and emits them in the Prometheus text exposition format (text/plain; version=0.0.4).
//
// Usage:
//
//	http.Handle("/metrics", metricsexport.PrometheusHandler())
package metricsexport
