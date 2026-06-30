# Changelog

## 2.0.0-alpha.52

### Performance — OPFS B-tree Overhaul

The OPFS B-tree storage backend received a major overhaul, delivering **~5x faster reads and ~4x faster writes** in browser persistence workloads, with the engine now up to **13x faster than SQLite** on large-value update paths. Key changes:

- **Durability flushes append to a WAL** instead of doing full checkpoints on every flush, with crash-safe tail replay on open.
- **Tree descents operate on raw page bytes** — internal pages are no longer fully decoded at each level, and a 4-slot MRU leaf hint cache lets sequential reads and appends skip the descent entirely.
- **Page checksums deferred to flush/checkpoint** rather than recomputed on every mutation.
- **Cache eviction is pinned-aware**, protecting WAL-resident pages and eliminating per-put eviction scans.
- **Range starts resolve through leaf hints** with gap detection and pagination cursors.
- **WAL reads and writes batched in 64-page runs** on WASM/OPFS, minimizing JS bridge calls.
- **Free-page allocation uses a bitmap** with O(1) insert/remove and word-scan run search, eliminating the per-flush freelist sort.

### Breaking Change — Browser Support

Persistent browser mode now requires `SharedWorker`, `MessageChannel`, and Web Locks support. Tabs for the same Jazz app share one OPFS-backed leader runtime via a SharedWorker broker instead of each opening independent storage handles. Browsers or embedded webviews missing these APIs must switch to the memory driver with a `serverUrl`.

### Solid.js Support

First-class Solid.js support is now available in `jazz-tools` (`createSolidJazzClient`, `JazzProvider`, `useAll`, `useLocalFirstAuth`) with full lifecycle, auth, and query coverage — thanks to **@FelipeEmos** for the contribution!

### Other Changes

- `db.one(...)` now executes with a root query limit of one instead of fetching all matching rows.
- New `merge("g-set")` strategy for non-nullable array columns: concurrent writes converge to a grow-only union of every replica's elements, deduplicated and sorted into a canonical order.
- New `jazz-tools/shared` entry point exposing framework-agnostic reactive query utilities (`applyDelta`, `reconcileArray`, etc.) for building custom framework bindings.
- New `jazz.server.active_websockets` OpenTelemetry gauge (requires `otel` feature + `OTEL_EXPORTER_OTLP_ENDPOINT`).
- The SharedWorker broker is now shipped as self-contained bundled ESM, fixing crashes under `next dev` / `next build` / `vite build`.
- Fix `deleteClientStorage()` hanging when called on a persistent browser Db before any table or query has been used.
- Removed `TestingServer` and `pushSchemaCatalogue` — use `startLocalJazzServer` and `deploy` instead.
