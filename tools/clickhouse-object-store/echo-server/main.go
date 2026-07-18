package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grafana/pyroscope-go"
)

const automaticWorkloadInterval = 750 * time.Millisecond

func main() {
	os.Exit(run())
}

func run() int {
	profiler, err := pyroscope.Start(pyroscope.Config{
		ApplicationName: envOrDefault("PYROSCOPE_APPLICATION_NAME", "clickhouse.echo.server"),
		ServerAddress:   envOrDefault("PYROSCOPE_SERVER_ADDRESS", "http://localhost:4040"),
		Logger:          pyroscope.StandardLogger,
	})
	if err != nil {
		log.Printf("failed to start profiler: %v", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	ticker := time.NewTicker(automaticWorkloadInterval)
	workerDone := make(chan struct{})
	go func() {
		runAutomaticWorkload(ctx, ticker.C)
		close(workerDone)
	}()

	server := newServer()
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.Start(envOrDefault("LISTEN_ADDRESS", ":8080"))
	}()

	var serveErr error
	serveStopped := false
	select {
	case <-ctx.Done():
	case serveErr = <-serveErrors:
		serveStopped = true
	}

	cancel()
	ticker.Stop()
	<-workerDone

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownErr := server.Shutdown(shutdownCtx)
	shutdownCancel()
	if !serveStopped {
		serveErr = <-serveErrors
	}

	exitCode := 0
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		log.Printf("Echo server failed: %v", serveErr)
		exitCode = 1
	}
	if shutdownErr != nil {
		log.Printf("failed to shut down Echo server: %v", shutdownErr)
		exitCode = 1
	}
	if err := profiler.Stop(); err != nil {
		log.Printf("failed to stop profiler: %v", err)
		exitCode = 1
	}

	return exitCode
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
