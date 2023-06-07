package main

import (
	"context"
	"fmt"
	"github.com/byrnedo/prometheus-gsheet/internal/app"
	"github.com/byrnedo/prometheus-gsheet/internal/pkg"
	"github.com/prometheus/common/model"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func envDefault(name string, deflt string) string {
	if v, found := os.LookupEnv(name); !found {
		return deflt
	} else {
		return v
	}
}
func mustEnv(name string) string {
	if v, found := os.LookupEnv(name); !found {
		panic("Missing environment variable: " + name)
	} else {
		return v
	}
}

var (
	cAddr          = envDefault("ADDR", ":4700")
	cLogFormat     = envDefault("LOG_FORMAT", "console")
	cCredentials   = mustEnv("CREDENTIALS")
	cSpreadsheetID = mustEnv("SPREADSHEET_ID")
)

func init() {
	if cLogFormat == "console" {
		log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano}).With().Timestamp().Logger()
	}
}

func main() {

	done := make(chan int, 1)
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT)
		sig := <-sigChan

		log.Info().Msgf("received %s", sig)
		done <- 1
	}()

	sheetsSvc := pkg.NewClient(cSpreadsheetID, 0)

	if err := sheetsSvc.Authenticate(context.Background(), cCredentials); err != nil {
		panic(fmt.Sprintf("CREDENTIALS: %s", err))
	}

	// create server struct
	server := app.Server{Addr: cAddr, Queue: &pkg.Queue{
		BufferSize:     500,
		Client:         sheetsSvc,
		RequestTimeout: 120 * time.Second,
		Chan:           make(chan *model.Sample, 500),
	}}
	go func() {
		_ = http.ListenAndServe(":6060", nil)
	}()

	// listen
	go func() {
		if err := server.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				log.Err(err).Msgf("server error: %w", err)
			}
			done <- 2
			return
		}
		done <- 0
	}()

	exitCode := <-done
	log.Info().Msg("shutting down")
	// cleanup
	if err := server.Close(); err != nil {
		log.Fatal().Err(err).Msgf("failed to close server cleanly: %w", err)
	}

	os.Exit(exitCode)
}
