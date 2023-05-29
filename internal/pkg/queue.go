package pkg

import (
	"context"
	"github.com/fatih/color"
	"github.com/prometheus/common/model"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
	"strings"
	"time"
)

var (
	redPrint   = color.New(color.FgRed).SprintfFunc()
	greenPrint = color.New(color.FgGreen).SprintfFunc()
)

type Queue struct {
	BufferSize        int
	Chan              chan *model.Sample
	Client            *Client
	RequestTimeout    time.Duration
	sheetLimitReached bool
}

func (q Queue) Put(samples model.Samples) {
	for _, s := range samples {
		q.Chan <- s
	}
}

func eternalRetry(op func() error, sleep time.Duration, attempt int) {
	if err := op(); err != nil {
		time.Sleep(sleep)
		log.Info().Msgf("retry attempt #%d", attempt+1)
		eternalRetry(op, sleep, attempt+1)
	}
}

func (q Queue) getConfig(ctx context.Context) (*Config, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cncl := context.WithTimeout(context.Background(), 10*time.Second)
	defer cncl()

	return q.Client.GetConfig(ctx)
}

func (q Queue) retireOldMetrics() (int, error) {

	ctx, cncl := context.WithTimeout(context.Background(), 30*time.Second)
	defer cncl()
	return q.Client.RetireMetrics(ctx)
}

func (q Queue) ListenAndProcess() error {
	newBuf := func() []*model.Sample {
		return make([]*model.Sample, 0, q.BufferSize)
	}

	buf := newBuf()

	config, err := q.getConfig(nil)
	if err != nil {
		return err
	}
	retired, err := q.retireOldMetrics()
	if err != nil {
		return err
	}
	log.Info().Msgf(redPrint("retired %d metrics", retired))

	flustTimerDur := 5 * time.Second
	flushTimer := time.NewTimer(flustTimerDur)
	defer flushTimer.Stop()

	confTicker := time.NewTicker(1 * time.Minute)
	defer confTicker.Stop()

	cleanupTicker := time.NewTicker(20 * time.Second)
	defer cleanupTicker.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		// send to google
		eternalRetry(func() error {
			log.Info().Msgf(greenPrint("sending request of %d", len(buf)))
			ctx, cncl := context.WithTimeout(context.Background(), q.RequestTimeout)
			defer cncl()

			if err := q.Client.Write(ctx, buf, q.sheetLimitReached); err != nil {
				if strings.Contains(err.Error(), "This action would increase the number of cells in the workbook above the limit of 10000000 cells.") {
					q.sheetLimitReached = true
				}

				log.Err(err).Msgf("failed to send to google sheets: %s", err)
				return err
			}
			buf = newBuf()
			return nil
		}, 2*time.Second, 0)
	}

	for {
		select {
		case s := <-q.Chan:

			metricName := strings.ToLower(string(s.Metric[model.MetricNameLabel]))
			if !slices.Contains(config.Metrics, metricName) {
				continue
			}

			buf = append(buf, s)

			if len(buf) == q.BufferSize {
				log.Debug().Msgf("buffer filled")
				// send
				flush()

				if !flushTimer.Stop() {
					<-flushTimer.C
				}
				flushTimer.Reset(flustTimerDur)
			}

		case <-flushTimer.C:
			log.Debug().Msgf("flush timer triggered")
			flush()
			flushTimer.Stop()
			flushTimer.Reset(flustTimerDur)
		case <-confTicker.C:
			log.Debug().Msgf("fetching config")
			if updConf, err := q.getConfig(nil); err != nil {
				log.Err(err).Msgf("failed to get config: %s", err)
			} else {
				config = updConf
			}
		case <-cleanupTicker.C:
			if retired, err := q.retireOldMetrics(); err != nil {
				log.Err(err).Msgf("error trying to retire metrics: %s", err)
				continue
			} else {
				log.Info().Msgf(redPrint("retired %d metrics", retired))
			}

		}

	}
}
