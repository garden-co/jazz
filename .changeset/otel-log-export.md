---
"jazz-tools": patch
---

Export `tracing` events as OpenTelemetry logs over OTLP, correlated with traces via the active span's `trace_id`/`span_id`. The CLI now keys both trace and log export off `OTEL_EXPORTER_OTLP_ENDPOINT`; the `JAZZ_OTEL=1` opt-in and the stdout span-exporter fallback are removed. Stdout continues to emit human-readable text logs.
