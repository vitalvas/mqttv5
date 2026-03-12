package metricsexport

import (
	"expvar"
	"fmt"
	"net/http"
	"strings"
)

const mqttPrefix = "mqtt_"

// gaugeMetrics is the set of metric names that are gauges.
// All other mqtt_ metrics are counters.
var gaugeMetrics = map[string]bool{
	"mqtt_connections":       true,
	"mqtt_subscriptions":     true,
	"mqtt_retained_messages": true,
}

// PrometheusHandler returns an http.HandlerFunc that exports all mqtt_ prefixed
// expvar metrics in Prometheus text exposition format.
func PrometheusHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		expvar.Do(func(kv expvar.KeyValue) {
			if !strings.HasPrefix(kv.Key, mqttPrefix) {
				return
			}

			metricType := "counter"
			if gaugeMetrics[kv.Key] {
				metricType = "gauge"
			}

			fmt.Fprintf(w, "# TYPE %s %s\n", kv.Key, metricType)
			fmt.Fprintf(w, "%s %s\n", kv.Key, kv.Value.String())
		})
	}
}
