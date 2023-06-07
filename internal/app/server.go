package app

import (
	"github.com/byrnedo/prometheus-gsheet/internal/pkg"
	"github.com/fatih/color"
	"github.com/gogo/protobuf/proto"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/rs/zerolog/log"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/snappy"
	"github.com/justinas/alice"
	"github.com/prometheus/prometheus/prompb"
)

var bluePrint = color.New(color.FgBlue).SprintfFunc()

type Server struct {
	Addr                string
	srv                 *http.Server
	totalRequestsMetric prometheus.Counter
	httpResponsesMetric *prometheus.CounterVec
	Queue               *pkg.Queue
}

func (s *Server) ListenAndServe() error {

	rtr := s.router()

	s.srv = &http.Server{
		Addr:    s.Addr,
		Handler: rtr,
	}

	go func() {
		if err := s.Queue.ListenAndProcess(); err != nil {
			log.Err(err)
		}
	}()

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
			mt := model.Time(s.Timestamp)
			if time.Since(mt.Time()) > 5*time.Minute {
				// skipping since too old
				continue
			}
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: mt,
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

	s.Queue.Put(samples)

	log.Info().Msgf(bluePrint("received %d samples", len(samples)))

	return
}

func (s *Server) router() http.Handler {

	m := httprouter.New()
	m.Handler("POST", "/", alice.New(metricsMiddleware()).ThenFunc(s.handle))
	return m
}

func (s Server) Close() error {
	//if s.Queue != nil {
	//	s.Queue.Close()
	//}
	if s.srv != nil {
		return s.srv.Close()
	}
	return nil
}
