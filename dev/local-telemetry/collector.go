package main

import (
	"context"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
)

const memScheme = "memcfg"

func runCollector(ctx context.Context, host string, port int, dataDir string, retentionDays int) error {
	factories := func() (otelcol.Factories, error) {
		otlpRecv := otlpreceiver.NewFactory()
		fileExp := fileexporter.NewFactory()
		batchProc := batchprocessor.NewFactory()
		return otelcol.Factories{
			Receivers: map[component.Type]receiver.Factory{
				otlpRecv.Type(): otlpRecv,
			},
			Processors: map[component.Type]processor.Factory{
				batchProc.Type(): batchProc,
			},
			Exporters: map[component.Type]exporter.Factory{
				fileExp.Type(): fileExp,
			},
			Telemetry: otelconftelemetry.NewFactory(),
		}, nil
	}

	cfg := buildConfigMap(host, port, dataDir, retentionDays)

	settings := otelcol.CollectorSettings{
		Factories: factories,
		BuildInfo: component.BuildInfo{
			Command:     "local-telemetry",
			Description: "Jazz local telemetry collector",
			Version:     "0.1.0",
		},
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{memScheme + ":"},
				ProviderFactories: []confmap.ProviderFactory{
					confmap.NewProviderFactory(func(_ confmap.ProviderSettings) confmap.Provider {
						return &inMemoryProvider{cfg: cfg}
					}),
				},
			},
		},
		DisableGracefulShutdown: true,
	}

	col, err := otelcol.NewCollector(settings)
	if err != nil {
		return fmt.Errorf("new collector: %w", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- col.Run(context.Background()) }()

	select {
	case <-ctx.Done():
		col.Shutdown()
		<-errCh
		return nil
	case err := <-errCh:
		return err
	}
}

type inMemoryProvider struct {
	cfg map[string]any
}

func (p *inMemoryProvider) Retrieve(_ context.Context, _ string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	return confmap.NewRetrieved(p.cfg)
}

func (p *inMemoryProvider) Scheme() string                   { return memScheme }
func (p *inMemoryProvider) Shutdown(_ context.Context) error { return nil }

func buildConfigMap(host string, port int, dataDir string, retentionDays int) map[string]any {
	fileExp := func(signal string) map[string]any {
		return map[string]any{
			"path":             fmt.Sprintf("%s/%s.jsonl", dataDir, signal),
			"format":           "json",
			"create_directory": true,
			"rotation": map[string]any{
				"max_megabytes": 100,
				"max_days":      retentionDays,
				"max_backups":   100,
			},
		}
	}

	return map[string]any{
		"receivers": map[string]any{
			"otlp": map[string]any{
				"protocols": map[string]any{
					"http": map[string]any{
						"endpoint": fmt.Sprintf("%s:%d", host, port),
						"cors": map[string]any{
							"allowed_origins": []any{"*"},
						},
					},
				},
			},
		},
		"exporters": map[string]any{
			"file/traces":  fileExp("traces"),
			"file/logs":    fileExp("logs"),
			"file/metrics": fileExp("metrics"),
		},
		"processors": map[string]any{
			"batch": map[string]any{
				"timeout": "200ms",
			},
		},
		"service": map[string]any{
			"telemetry": map[string]any{
				"logs": map[string]any{"level": "info"},
			},
			"pipelines": map[string]any{
				"traces": map[string]any{
					"receivers":  []any{"otlp"},
					"processors": []any{"batch"},
					"exporters":  []any{"file/traces"},
				},
				"logs": map[string]any{
					"receivers":  []any{"otlp"},
					"processors": []any{"batch"},
					"exporters":  []any{"file/logs"},
				},
				"metrics": map[string]any{
					"receivers":  []any{"otlp"},
					"processors": []any{"batch"},
					"exporters":  []any{"file/metrics"},
				},
			},
		},
	}
}
