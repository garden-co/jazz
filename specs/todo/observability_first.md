# Observability — TODO

Deep instrumentation across client and server for debugging, billing, and operational insight.

## Overview

Build on the Rust OpenTelemetry crates to provide distributed tracing, metrics, and logging across every tier:

- **For us (framework maintainers)** — operational visibility into hosted infra, error rates, performance regressions
- **For app developers** — understand mutation and query settling latency, sync health, storage usage
- **For billing** — usage metering derived from the same telemetry pipeline (storage bytes, sync bandwidth, active connections)

### Ideal outcome

Distributed traces that show exact timing for:
1. Mutation on client (local write)
2. Sync to worker tier
3. Sync to edge server
4. Propagation to core/store shards
5. Query settlement back down through each tier

## Open Questions

- Which OTel exporters to bundle (OTLP, Jaeger, stdout)?
- How much client-side tracing is feasible in WASM/browser (performance overhead)?
- Trace context propagation through the sync protocol (how to carry trace IDs in sync messages?)
- Sampling strategy for high-volume production use
- How to expose traces/metrics to app developers (dashboard integration, or export to their own OTel collector)?
