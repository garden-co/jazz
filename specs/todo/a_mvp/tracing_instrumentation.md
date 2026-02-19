# Tracing Instrumentation

Add structured tracing throughout the Rust codebase using the `tracing` crate, with `tracing-wasm` for browser console/Performance timeline output and `tracing-subscriber` for server-side terminal output. Goal: see the entire request flow from browser main thread → worker → WASM runtime (and separately, jazz-cli server) at TRACE level.

## Motivation

Data written to OPFS doesn't survive reload in the example apps. Rather than guessing, we want to watch the full lifecycle of a write and a read — where does the data go, what branches does it land on, and what happens when a fresh session tries to load it back.

More generally: as the system grows, we need a way to understand what's happening without reading code. Spans with timings also feed the browser Performance profiler.

## Current state

Server-side Rust instrumentation is complete. OTel export is wired up and working with a local Grafana/Tempo dev stack. Browser-side (`tracing-wasm`) and TypeScript logging are not yet done.

### Done

- **jazz-tools crate**: Full span/event coverage across all subsystems — ObjectManager (create, get_or_load, add_commit), QueryManager (subscribe, settle, graph node evaluation, writes), SyncManager (inbox processing, forwarding, sync logic), SchemaManager (insert, delete, process), RuntimeCore (callback counter, ack watcher, flush_wal), storage layer (flush, flush_wal for both OpfsBTree and SurrealKv).
- **jazz-wasm**: Debug logs at JS→WASM boundary for insert, update, delete, subscribe; debug_span on unsubscribe.
- **jazz-tools CLI**: Layered tracing subscriber with optional OTel layer (`--features otel` + `JAZZ_OTEL=1`). sync_handler span with client_id and payload_size. events_handler span (scoped to avoid !Send issue with Entered guard across .await). /events excluded from tower-http TraceLayer to avoid long-lived request spans.
- **opfs-btree**: trace_spans on get/put/delete/range, debug_span on checkpoint with dirty_pages/total_pages.
- **OTel export**: Feature-gated OTLP gRPC exporter (tonic) with stdout fallback. Provider stored in OnceLock, flushed on shutdown.
- **Dev stack**: `dev/observability/` with OTel Collector + grafana/otel-lgtm docker-compose.

### Remaining

- **tracing-wasm**: Not integrated — browser-side spans don't appear in console or Performance timeline yet.
- **TypeScript logging**: `[main]`/`[worker]` prefixes not added to console calls.
- **RuntimeCore tick-level span**: Sub-spans exist but no parent span around `batched_tick` with tick number.
- **End-to-end verification**: Haven't confirmed full write→read flow visible in browser console with tracing-wasm.

## Design

### Rust: `tracing` spans and events

Use `#[instrument]` and manual `span!`/`event!` macros. Every interesting boundary gets a span; key decision points get events.

Levels:

- **ERROR** — invariant violations, unrecoverable failures
- **WARN** — recoverable issues (e.g. unknown client in sync message)
- **INFO** — lifecycle events: runtime created, client added, server connected, schema registered
- **DEBUG** — per-operation: query subscribe/unsettle, object insert/update/delete, sync message sent/received
- **TRACE** — internal detail: index scan ranges, row materialization, branch resolution, WAL writes

### Where to instrument

Priority areas (these trace a write + read end-to-end):

**groove crate — core engine** ✅

```
SchemaManager::insert/update/delete_with_session
  ├─ span: object_id, table, branch
  ├─ ObjectManager::insert_object / update_object
  │    └─ event: object stored, metadata keys
  ├─ index_insert / index_remove
  │    └─ event: index name, column value
  ├─ mark_subscriptions_dirty
  │    └─ event: which subscriptions dirtied
  └─ forward_update_to_servers
       └─ event: outbox message, target client

QueryManager::subscribe
  ├─ span: query SQL, branches
  ├─ settle()
  │    ├─ IndexScanNode::evaluate
  │    │    └─ event: scan condition, result count
  │    ├─ Materialize::evaluate
  │    │    └─ event: rows materialized
  │    └─ event: subscription settled, row count
  └─ event: subscription result delivered

SyncManager::handle_sync_message (inbox processing)
  ├─ span: source client, message type
  ├─ ObjectUpdated → event: object_id, branch
  ├─ QuerySubscription → event: query, client
  └─ outbox flush → event: message count, target clients

RuntimeCore::batched_tick
  ├─ span: tick number                          ← TODO: parent span not yet added
  ├─ handle_sync_messages → event: messages processed
  ├─ settle dirty queries → event: queries settled
  └─ flush outbox → event: messages sent
```

**jazz-wasm — WASM bindings** ✅ (partially)

```
WasmRuntime::new        → span: app_id, schema summary       ← not yet
WasmRuntime::addServer  → span: server URL                   ← not yet
WasmRuntime::addClient  → event: generated client_id         ← not yet
WasmRuntime::subscribe  → event: subscription_id             ✅
WasmRuntime::insert/update/delete → event: object_id         ✅
WasmRuntime::unsubscribe → span: sub_id                      ✅
WasmRuntime::handleSyncMessages   → span: message count      ← not yet
WasmRuntime::tick        → span (TRACE level, frequent)      ← not yet
```

**jazz-cli — server** ✅

```
events_handler  → span: client_id (scoped)                   ✅
sync_handler    → span: client_id, payload size              ✅
  └─ per message → event: message type, object_id/query      ✅
```

**opfs-btree** ✅

```
get/put/delete/range → trace_spans                           ✅
checkpoint → debug_span with dirty_pages, total_pages        ✅
```

### WASM: `tracing-wasm` — TODO

[`tracing-wasm`](https://crates.io/crates/tracing-wasm) bridges `tracing` to:

- `console.log` / `console.debug` / etc. (with collapsible groups for spans)
- `performance.mark()` / `performance.measure()` (shows up in browser Performance tab)

Initialize in `WasmRuntime::new()`:

```rust
use tracing_wasm::{WASMLayerConfigBuilder, set_as_global_default_with_config};

let config = WASMLayerConfigBuilder::default()
    .set_max_level(tracing::Level::TRACE)
    .build();
set_as_global_default_with_config(config);
```

This is a one-liner at startup. All `#[instrument]` spans and `event!` macros throughout groove then automatically appear in the browser console with proper nesting.

### TypeScript: console.log with prefixes — TODO

Keep TypeScript logging lightweight — the heavy lifting is in Rust/WASM. Add `[main]` and `[worker]` prefixes to existing console calls for clarity. The worker's WASM tracing output already appears in the worker's console; no bridging needed.

Key TypeScript events to log (DEBUG level, behind a flag):

- `db.ts`: bridge init, query subscribe/unsubscribe, message relay to worker
- `groove-worker.ts`: init, stream connect/disconnect, sync POST, message relay to WASM
- `client.ts`: SSE connect, schema context sent, sync POST

### Filtering

**Server (jazz-cli):** Uses `RUST_LOG` env filter. Example:

```
RUST_LOG=groove=trace,jazz_cli=debug cargo run -- server ...
```

**WASM (browser):** `tracing-wasm` respects the max level set at init. We can make this configurable via a query param or init option:

```
http://localhost:5173/?log=trace
```

Or just hardcode TRACE during development — the browser console has its own level filter.

## Remaining implementation steps

1. Add `tracing-wasm` dependency to `jazz-wasm` Cargo.toml
2. Initialize `tracing-wasm` in `WasmRuntime::new()`
3. Add spans to remaining jazz-wasm public methods (new, addServer, addClient, handleSyncMessages, tick)
4. Add `batched_tick` parent span to RuntimeCore
5. Add `[main]`/`[worker]` prefixes to TypeScript console calls
6. Test: run todo example with tracing, verify full write→read flow visible in browser console

## Non-goals

- Tracing in production builds (compile out with level filter)
- Custom console UI / log viewer
- Tracing the TypeScript ↔ Worker postMessage bridge (just log at boundaries)
