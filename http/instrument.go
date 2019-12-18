package http

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (hs *Server) instrument(method string, path string) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return promhttp.InstrumentHandlerDuration(
			httpRequestDuration(method, path).MustCurryWith(prometheus.Labels{"handler": path}),
			promhttp.InstrumentHandlerCounter(
				httpRequestsTotalCounterVec(method, path),
				next,
			),
		)
	}
}

func httpRequestsTotalCounterVec(method string, path string) *prometheus.CounterVec {
	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "http_requests_total",
			Help:        "A counter for requests to the wrapped handler.",
			ConstLabels: prometheus.Labels{"path": path, "method": method},
		},
		[]string{"code"},
	)

	prometheus.MustRegister(counter)

	return counter
}

func httpRequestDuration(method string, path string) *prometheus.HistogramVec {
	// duration is partitioned by the HTTP method and handler. It uses custom
	// buckets based on the expected request duration.
	duration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "http_request_duration_seconds",
			Help:        "A histogram of latencies for requests.",
			Buckets:     []float64{.25, .5, 1, 2.5, 5, 10},
			ConstLabels: prometheus.Labels{"path": path, "method": method},
		},
		[]string{"handler"},
	)

	prometheus.MustRegister(duration)

	return duration
}
