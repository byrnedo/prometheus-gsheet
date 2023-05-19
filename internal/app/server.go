package app

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/rs/zerolog/log"
	"io/ioutil"
	"net/http"

	"github.com/golang/snappy"
	"github.com/justinas/alice"
	"github.com/prometheus/prometheus/prompb"
)

type Server struct {
	Addr                string
	srv                 *http.Server
	totalRequestsMetric prometheus.Counter
	httpResponsesMetric *prometheus.CounterVec
}

func (s *Server) ListenAndServe() error {

	rtr := s.router()

	s.srv = &http.Server{
		Addr:    s.Addr,
		Handler: rtr,
	}

	log.Info().Msgf("server listening on %s", s.srv.Addr)

	return s.srv.ListenAndServe()
}

func (s *Server) protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}
	return samples
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	samples := s.protoToSamples(&req)

	fmt.Println(samples)

	return
}

func (s *Server) router() http.Handler {

	m := http.NewServeMux()
	m.Handle("/", alice.New(metricsMiddleware()).ThenFunc(s.handle))
	return m
}

func (s Server) Close() error {
	if s.srv != nil {
		return s.srv.Close()
	}
	return nil
}
