# Observability

Deep instrumentation across client and server for debugging, billing, and operational insight.

## Phasing

- **MVP**: Instrument early using Rust OpenTelemetry crates. Use a simple log drain (stdout / file / cloud log service) for immediate operational visibility.
- **Launch**: Proper collection infrastructure (Grafana, custom dashboard UI), trace exposure to app developers, billing-grade usage metering.

## MVP: Instrumentation + Log Drain — Done

OTel spans and events cover all key server-side code paths:

- ✅ Sync message handling (inbound/outbound per client)
- ✅ Query graph evaluation (per query, per node — debug logs with input/output row counts)
- ✅ Storage operations (read/write/flush timing via debug_spans)
- ✅ OTel export: feature-gated OTLP gRPC exporter (`--features otel` + `JAZZ_OTEL=1`) with stdout fallback
- ✅ Local dev stack: `dev/observability/` with OTel Collector → grafana/otel-lgtm (Tempo, Prometheus, Loki, Grafana on port 3000)

### Resolved open questions

- **OTel exporters**: OTLP gRPC (tonic) for collector-based setups, stdout for quick local dev. Both behind the `otel` feature gate.
- **Client-side tracing in WASM**: Deferred — `tracing-wasm` integration is specced but not yet implemented (see `tracing_instrumentation.md`).

### Still open

- Trace context propagation through the sync protocol (how to carry trace IDs in sync messages for distributed traces?)
- Sampling strategy for high-volume production use

## Launch: Collection & Exposure

### For us (framework maintainers)

- Operational visibility into hosted infra, error rates, performance regressions
- Billing usage metering derived from the telemetry pipeline (storage bytes, sync bandwidth, active connections)

### For app developers

- Understand mutation and query settling latency, sync health, storage usage
- Distributed traces showing exact timing across all tiers:
  1. Mutation on client (local write)
  2. Sync to worker tier
  3. Sync to edge server
  4. Propagation to core/store shards
  5. Query settlement back down through each tier

### Infrastructure

- Grafana or custom UI in developer dashboard
- Per-app trace filtering
- Export to developer's own OTel collector
