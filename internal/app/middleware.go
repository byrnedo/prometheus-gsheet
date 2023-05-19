package app

import (
	"github.com/justinas/alice"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"net/http"
	"strconv"
)

type StatusRecordingWriter struct {
	status int
	http.ResponseWriter
}

func (w *StatusRecordingWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func metricsMiddleware() alice.Constructor {

	httpResponsesMetric := promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gsheet_remote_write_http_responses_total",
		Help: "Total number of Gsheet Remote Write HTTP responses",
	}, []string{"response_code"})

	totalRequestsMetric := promauto.NewCounter(prometheus.CounterOpts{
		Name: "gsheet_remote_write_received_requests",
		Help: "The total number of received requests from Prometheus server",
	})

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			totalRequestsMetric.Inc()
			rw := &StatusRecordingWriter{ResponseWriter: w}
			next.ServeHTTP(rw, r)
			httpResponsesMetric.With(prometheus.Labels{"response_code": strconv.Itoa(rw.status)}).Inc()
		})
	}
}
