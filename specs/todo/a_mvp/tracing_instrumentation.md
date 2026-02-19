# Tracing Instrumentation

Add structured tracing throughout the Rust codebase using the `tracing` crate, with `tracing-wasm` for browser console/Performance timeline output and `tracing-subscriber` for server-side terminal output. Goal: see the entire request flow from browser main thread → worker → WASM runtime (and separately, jazz-cli server) at TRACE level.

## Motivation

Data written to OPFS doesn't survive reload in the example apps. Rather than guessing, we want to watch the full lifecycle of a write and a read — where does the data go, what branches does it land on, and what happens when a fresh session tries to load it back.

More generally: as the system grows, we need a way to understand what's happening without reading code. Spans with timings also feed the browser Performance profiler.

## Current state

- **groove crate**: zero tracing
- **jazz-wasm**: zero tracing/logging
- **jazz-cli**: `tracing` + `tracing-subscriber` with `env-filter`; ~5 ad-hoc `info!`/`warn!`/`error!` calls in routes
- **jazz-tools client module**: `tracing` available but instrumentation coverage is uneven
- **opfs-btree**: trace points available in the storage crate, not fully wired into end-to-end spans yet
- **TypeScript**: scattered `console.error` for failures, no structured logging

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

**groove crate — core engine**

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
  ├─ span: tick number
  ├─ handle_sync_messages → event: messages processed
  ├─ settle dirty queries → event: queries settled
  └─ flush outbox → event: messages sent
```

**jazz-wasm — WASM bindings**

```
WasmRuntime::new        → span: app_id, schema summary
WasmRuntime::addServer  → span: server URL
WasmRuntime::addClient  → event: generated client_id
WasmRuntime::subscribe  → span: query
WasmRuntime::insert/update/delete → span: table, object_id
WasmRuntime::handleSyncMessages   → span: message count
WasmRuntime::tick        → span (TRACE level, frequent)
```

**jazz-cli — server**

```
events_handler  → span: client_id, session info
sync_handler    → span: client_id, payload size
  └─ per message → event: message type, object_id/query
```

**opfs-btree**

```
Add focused storage spans/events for checkpoint, page allocation, and range scans.
```

### WASM: `tracing-wasm`

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

### TypeScript: console.log with prefixes

Keep TypeScript logging lightweight — the heavy lifting is in Rust/WASM. Add `[main]` and `[worker]` prefixes to existing console calls for clarity. The worker's WASM tracing output already appears in the worker's console; no bridging needed.

Key TypeScript events to log (DEBUG level, behind a flag):

- `db.ts`: bridge init, query subscribe/unsubscribe, message relay to worker
- `groove-worker.ts`: init, stream connect/disconnect, sync POST, message relay to WASM
- `client.ts`: SSE connect, schema context sent, sync POST

### Filtering

**Server (jazz-cli):** Already uses `RUST_LOG` env filter. Example:

```
RUST_LOG=groove=trace,jazz_cli=debug cargo run -- server ...
```

**WASM (browser):** `tracing-wasm` respects the max level set at init. We can make this configurable via a query param or init option:

```
http://localhost:5173/?log=trace
```

Or just hardcode TRACE during development — the browser console has its own level filter.

## Implementation steps

1. Ensure `tracing` dependency coverage in unified `crates/jazz-tools/Cargo.toml` for core + client modules
2. Add `tracing-wasm` dependency to `jazz-wasm` Cargo.toml
3. Initialize `tracing-wasm` in `WasmRuntime::new()`
4. Instrument groove core: SchemaManager write path (insert/update/delete)
5. Instrument groove core: QueryManager subscribe + settle path
6. Instrument groove core: SyncManager message handling + outbox
7. Instrument groove core: RuntimeCore::batched_tick
8. Instrument jazz-wasm: all public `WasmRuntime` methods
9. Add/expand `opfs-btree` tracing hooks used by groove storage adapters
10. Instrument jazz-cli routes (expand existing sparse tracing)
11. Add `[main]`/`[worker]` prefixes to TypeScript console calls
12. Test: run todo example with tracing, verify full write→read flow visible in console

## Non-goals

- Distributed tracing / OpenTelemetry (later)
- Tracing in production builds (compile out with level filter)
- Custom console UI / log viewer
- Tracing the TypeScript ↔ Worker postMessage bridge (just log at boundaries)
