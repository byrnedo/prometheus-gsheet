package pkg

import (
	"context"
	"github.com/prometheus/common/model"
	"github.com/rs/zerolog/log"
	"strings"
	"time"
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

func (q Queue) ListenAndProcess() error {
	newBuf := func() []*model.Sample {
		return make([]*model.Sample, 0, q.BufferSize)
	}

	buf := newBuf()

	cooldown := 5 * time.Second
	timer := time.NewTimer(cooldown)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		// send to google
		eternalRetry(func() error {
			log.Info().Msgf("sending request of %d", len(buf))
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

	defer timer.Stop()
	for {
		select {
		case s := <-q.Chan:
			buf = append(buf, s)

			if len(buf) == q.BufferSize {
				log.Debug().Msgf("buffer filled")
				// send
				flush()

				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(cooldown)
			}

		case <-timer.C:
			log.Debug().Msgf("flush timer triggered")
			flush()
			timer.Stop()
			timer.Reset(cooldown)
		}
	}
}
