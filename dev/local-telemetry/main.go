package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"golang.org/x/sync/errgroup"
)

func main() {
	dataDir := flag.String("data-dir", "./data", "Directory for telemetry output files")
	otlpHost := flag.String("otlp-host", "127.0.0.1", "OTLP/HTTP receiver bind host")
	otlpPort := flag.Int("otlp-port", 4318, "OTLP/HTTP receiver port")
	httpHost := flag.String("http-host", "127.0.0.1", "Viewer + SQL bind host")
	httpPort := flag.Int("http-port", 4319, "Viewer + SQL HTTP port")
	retentionDays := flag.Int("retention-days", 2, "Days of rotated files to keep")
	flag.Parse()

	absData, err := filepath.Abs(*dataDir)
	if err != nil {
		log.Fatalf("resolve data dir: %v", err)
	}
	if err := os.MkdirAll(absData, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}
	log.Printf("data dir: %s", absData)

	bundle, err := buildUIBundle()
	if err != nil {
		log.Fatalf("build ui: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return runCollector(ctx, *otlpHost, *otlpPort, absData, *retentionDays)
	})

	g.Go(func() error {
		return runHTTPServer(ctx, *httpHost, *httpPort, absData, bundle)
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("exit: %v", err)
	}
	log.Println("shutdown complete")
}
