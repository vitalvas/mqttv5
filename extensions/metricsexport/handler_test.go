package metricsexport

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrometheusHandler(t *testing.T) {
	t.Run("content type", func(t *testing.T) {
		handler := PrometheusHandler()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

		handler(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "text/plain; version=0.0.4; charset=utf-8", rec.Header().Get("Content-Type"))
	})

	t.Run("exports mqtt prefixed metrics", func(t *testing.T) {
		counter := expvar.NewInt("mqtt_test_counter_total")
		counter.Set(42)

		handler := PrometheusHandler()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

		handler(rec, req)

		body := rec.Body.String()
		assert.Contains(t, body, "# TYPE mqtt_test_counter_total counter")
		assert.Contains(t, body, "mqtt_test_counter_total 42")
	})

	t.Run("skips non mqtt metrics", func(t *testing.T) {
		other := expvar.NewInt("other_metric")
		other.Set(99)

		handler := PrometheusHandler()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

		handler(rec, req)

		body := rec.Body.String()
		assert.NotContains(t, body, "other_metric")
	})

	t.Run("gauge metrics", func(t *testing.T) {
		for _, name := range []string{"mqtt_connections", "mqtt_subscriptions", "mqtt_retained_messages"} {
			v := expvar.Get(name)
			if v == nil {
				expvar.NewInt(name)
			}
		}

		handler := PrometheusHandler()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

		handler(rec, req)

		body := rec.Body.String()
		assert.Contains(t, body, "# TYPE mqtt_connections gauge")
		assert.Contains(t, body, "# TYPE mqtt_subscriptions gauge")
		assert.Contains(t, body, "# TYPE mqtt_retained_messages gauge")
	})

	t.Run("counter metrics", func(t *testing.T) {
		names := []string{
			"mqtt_connections_total",
			"mqtt_bytes_received_total",
			"mqtt_publish_latency_count",
			"mqtt_publish_latency_seconds_sum",
		}
		for _, name := range names {
			v := expvar.Get(name)
			if v == nil {
				expvar.NewInt(name)
			}
		}

		handler := PrometheusHandler()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

		handler(rec, req)

		body := rec.Body.String()
		for _, name := range names {
			assert.Contains(t, body, fmt.Sprintf("# TYPE %s counter", name))
		}
	})

	t.Run("output format", func(t *testing.T) {
		v := expvar.Get("mqtt_test_format_total")
		if v == nil {
			c := expvar.NewInt("mqtt_test_format_total")
			c.Set(7)
		}

		handler := PrometheusHandler()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

		handler(rec, req)

		body := rec.Body.String()
		lines := strings.Split(body, "\n")

		var found bool
		for i, line := range lines {
			if line == "# TYPE mqtt_test_format_total counter" {
				require.Greater(t, len(lines), i+1)
				assert.Equal(t, "mqtt_test_format_total 7", lines[i+1])
				found = true
				break
			}
		}
		assert.True(t, found, "expected to find mqtt_test_format_total in output")
	})
}
