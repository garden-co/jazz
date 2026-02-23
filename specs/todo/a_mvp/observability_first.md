# Observability + Tracing Instrumentation

Deep instrumentation across client and server for debugging, billing, and operational insight.

## Phasing

- **MVP**: Instrument early using Rust OpenTelemetry crates and `tracing`, with a simple log drain (stdout / file / cloud log service) for immediate operational visibility.
- **Launch**: Proper collection infrastructure (Grafana/custom dashboard), trace exposure to app developers, and billing-grade usage metering.

## Done (Completed)

### MVP instrumentation and export pipeline

- ✅ Server-side spans and events cover key paths:
  - Sync message handling (inbound/outbound per client)
  - Query graph evaluation (per query/per node with row-count logs)
  - Storage operations (read/write/flush timing)
- ✅ `jazz-tools` crate instrumentation across ObjectManager, QueryManager, SyncManager, SchemaManager, RuntimeCore, and storage operations.
- ✅ `jazz-tools` CLI tracing subscriber with optional OTel layer (`--features otel` + `JAZZ_OTEL=1`).
- ✅ OTel export via feature-gated OTLP gRPC (tonic) with stdout fallback.
- ✅ Provider lifecycle wiring (global provider + flush on shutdown).
- ✅ Local dev stack in `dev/observability/`:
  - OTel Collector
  - grafana/otel-lgtm (Tempo, Prometheus, Loki, Grafana on port 3000)

### Additional tracing already in place

- ✅ `jazz-wasm` has debug logs at JS↔WASM boundaries for insert/update/delete/subscribe and debug span on unsubscribe.
- ✅ `opfs-btree` has trace spans on get/put/delete/range and debug span on checkpoint.

### Resolved questions

- ✅ Exporter choice resolved: OTLP gRPC for collector setups, stdout for local dev.

## Remaining (To Complete)

### MVP completion tasks (from tracing instrumentation work)

1. Add `tracing-wasm` dependency to `jazz-wasm` `Cargo.toml`.
2. Initialize `tracing-wasm` in `WasmRuntime::new()`.
3. Add spans to remaining `jazz-wasm` public methods (`new`, `addServer`, `addClient`, `handleSyncMessages`, `tick`).
4. Add a parent `batched_tick` span in `RuntimeCore` (with tick number).
5. Add `[main]` / `[worker]` prefixes to TypeScript console logs at key boundaries.
6. Verify end-to-end write→read trace flow in browser console/Performance timeline.
7. Add browser-configurable tracing level (for example URL query param like `?log=trace`) instead of always using fixed TRACE in WASM init.

### Still-open MVP design questions

- Trace context propagation through the sync protocol (trace IDs across tiers).
- Sampling strategy for high-volume production use.

## Design Notes

### Tracing model

Use `#[instrument]` plus manual `span!`/`event!` macros.

Levels:

- `ERROR`: invariant violations, unrecoverable failures.
- `WARN`: recoverable issues.
- `INFO`: lifecycle events.
- `DEBUG`: per-operation events.
- `TRACE`: internal detail (scan ranges, branch resolution, WAL writes).

### `tracing-wasm` integration intent

`tracing-wasm` should surface Rust tracing in browser developer tools:

- Console output (`console.log`/`console.debug`) with span nesting.
- Performance timeline (`performance.mark/measure`) for profiling.

Current status: tracing is initialized in WASM, but level selection is not browser-configurable yet (fixed TRACE at init).

### TypeScript logging intent

Keep TS logging lightweight and boundary-focused; Rust/WASM tracing remains the source of deep detail.

## Implementation Targets (Detailed)

Use this section as the execution checklist for remaining work. Keep this aligned with code as tasks land.

### Rust instrumentation levels and patterns

- Use `#[instrument]` for function-level spans on public/runtime boundaries.
- Use manual `span!`/`event!` for high-cardinality operations where field control matters.
- Keep structured fields stable (`client_id`, `query_id`, `object_id`, `table`, `branch`, `message_type`).

### RuntimeCore target

- Add a parent span around `RuntimeCore::batched_tick` including tick number.
- Child work inside that span should include:
  - sync message handling,
  - dirty query settlement,
  - outbox flush.

### WASM runtime targets

Already present:

- insert/update/delete/subscribe boundary logs.
- unsubscribe debug span.

Remaining instrumentation targets:

- `WasmRuntime::new` (startup/app + schema context).
- `WasmRuntime::addServer` (server URL/identity).
- `WasmRuntime::addClient` (client setup path).
- `WasmRuntime::handleSyncMessages` (batch size / source context).
- `WasmRuntime::tick` (high-frequency tick span; keep lightweight).

### Browser tracing initialization and config

Current behavior:

- `wasm_tracing` is initialized once with fixed `TRACE` level.

Remaining behavior:

- Add configurable browser log level (e.g. URL query param `?log=trace|debug|info|warn|error`).
- Keep safe fallback default when param missing/invalid.

Reference initialization pattern:

```rust
let config = wasm_tracing::WasmLayerConfig::new()
    .with_max_level(tracing::Level::TRACE)
    .with_console_group_spans();
let _ = wasm_tracing::set_as_global_default_with_config(config);
```

### TypeScript boundary logging targets

Add lightweight prefixed logs (`[main]`, `[worker]`) at these boundaries:

- `db.ts`: bridge init, subscribe/unsubscribe requests, message relay to worker.
- `groove-worker.ts`: worker init, stream connect/disconnect, sync POST, relay to WASM.
- `client.ts`: SSE connect, schema context send, sync POST lifecycle.

### Filtering and run config

Server filtering is env-driven via `RUST_LOG`.

Example local run with OTel:

```sh
JAZZ_OTEL=1 \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
RUST_LOG=jazz_tools=debug,groove=debug \
cargo run -p jazz-tools --features otel -- server <APP_ID>
```

Browser filtering:

- Console-level filtering available via DevTools.
- Add URL-based level selection as noted above for runtime-level filtering.

### Verification checklist

- Start observability stack (`dev/observability/`) and instrumented server.
- Run browser app with tracing enabled.
- Execute one write and one read/reload path.
- Confirm spans are visible:
  - in browser console/performance timeline (WASM side),
  - in collector/Grafana trace backend (server side),
  - with correlated operation context for debugging.

## Launch: Collection & Exposure

### For framework maintainers

- Operational visibility into hosted infrastructure, error rates, and regressions.
- Billing usage metering from telemetry (storage bytes, bandwidth, active connections).

### For app developers

- Visibility into mutation/query settlement latency and sync health.
- Distributed trace view across:
  1. client mutation,
  2. worker sync,
  3. edge sync,
  4. core/store propagation,
  5. settlement propagation back down.

### Infrastructure direction

- Grafana or custom developer dashboard UI.
- Per-app trace filtering.
- Export to developer-owned OTel collectors.

## Non-goals

- Production tracing at full verbosity by default.
- Custom log-viewer UI in MVP.
- Deep tracing of every TS↔Worker message hop beyond boundary logging.
