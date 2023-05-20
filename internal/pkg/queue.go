package pkg

import (
	"context"
	"github.com/prometheus/common/model"
	"github.com/rs/zerolog/log"
	"time"
)

type Queue struct {
	BufferSize     int
	Chan           chan *model.Sample
	Client         *Client
	RequestTimeout time.Duration
}

func (q Queue) Put(samples model.Samples) {
	for _, s := range samples {
		q.Chan <- s
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
		ctx, cncl := context.WithTimeout(context.Background(), q.RequestTimeout)
		defer cncl()
		log.Info().Msgf("sending request of %d", len(buf))
		if err := q.Client.Write(ctx, buf); err != nil {
			log.Err(err).Msgf("failed to send to google sheets: %s", err)
		}
		buf = newBuf()
	}

	defer timer.Stop()
	for {
		select {
		case s := <-q.Chan:
			buf = append(buf, s)

			if len(buf) == q.BufferSize {
				log.Info().Msgf("buffer filled")
				// send
				flush()

				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(cooldown)
			}

		case <-timer.C:
			log.Info().Msgf("timer triggered")
			flush()
			timer.Stop()
			timer.Reset(cooldown)
		}
	}
}
