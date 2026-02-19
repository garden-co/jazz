# Observability — TODO

Deep instrumentation across client and server for debugging, billing, and operational insight.

## Phasing

- **MVP**: Instrument early using Rust OpenTelemetry crates. Use a simple log drain (stdout / file / cloud log service) for immediate operational visibility.
- **Launch**: Proper collection infrastructure (Grafana, custom dashboard UI), trace exposure to app developers, billing-grade usage metering.

## MVP: Instrumentation + Log Drain

Add OTel spans and metrics to key code paths now, even before collection infrastructure exists:

- Sync message handling (inbound/outbound per client)
- Query graph evaluation (per query, per node)
- Storage operations (read/write/flush timing)
- Export to stdout or a simple log drain (e.g., Datadog, Axiom) for immediate use

This gives us debugging capability from day one without building dashboards.

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

## Open Questions

- Which OTel exporters to bundle (OTLP, Jaeger, stdout)?
- How much client-side tracing is feasible in WASM/browser (performance overhead)?
- Trace context propagation through the sync protocol (how to carry trace IDs in sync messages?)
- Sampling strategy for high-volume production use
