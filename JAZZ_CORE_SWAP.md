# Jazz Core Engine Swap Baseline

This branch is a destructive engine-swap branch based on
`feat/merge-batches-and-transaction`.

The goal is to replace the alpha engine with the imported workspace engine
implementation, not to keep both implementations behind flags or adapters.
The alpha code that remains should be treated as integration target material:
tests, examples, public TypeScript API shape, and thin binding/worker/server
entrypoints.

## Core Location

The imported Rust engine crates now live in first-class workspace
locations:

```text
crates/groove
crates/jazz
crates/jazz-server
crates/jazz-sim
crates/jazz-core-wasm
crates/opfs-btree
```

`vendor` is no longer an active source location for the engine swap. The
remaining copied repository shell was removed instead of being kept as a second
workspace root.

Deleted vendor-only material included:

- copied root Cargo metadata and GitHub workflow files
- benchmark result snapshots and one-off benchmark helper scripts
- copied browser/TypeScript prototype fixtures that depended on a conflicting
  local `jazz-tools` package name
- local build/install artifacts such as `target`, `node_modules`, and `dist`

Active benchmark, workflow, docs, and example work should use the root repo's
first-class `dev`, `.github`, `docs`, `examples`, and `crates` trees.

## Keep

- `packages/jazz-tools/src/index.ts`, `schema.ts`, `dsl.ts`,
  `typed-app.ts`, `migrations.ts`, `ir.ts`, `where-operators.ts`
  - Preserve the public TypeScript schema DSL, typed app/query builder surface,
    migration API, and exported type contracts.

- `packages/jazz-tools/src/react`, `react-core`, `solid`, `svelte`, `vue`,
  `web`, `expo`, `react-native`
  - Preserve framework adapters and public hooks/providers; retarget internals
    to the swapped engine.

- `packages/jazz-tools/src/backend`, `better-auth-adapter`,
  `passphrase.ts`, `passkey-backup.ts`
  - Preserve high-level auth/backend integration APIs; implementation can become
    thin engine glue.

- `packages/jazz-tools/tests`, `packages/jazz-tools/src/**/*.test.ts`,
  `packages/jazz-tools/tests/browser`, `packages/jazz-tools/tests/ts-dsl`
  - Keep as API and behavior regression targets.

- `crates/jazz-tools/tests`, `crates/jazz-tools/src/**/tests*`
  - Keep or port as conformance/regression tests for sync, query, storage,
    policies, and transactions.

- `examples`, `starters`, `docs`, `packages/create-jazz`,
  `packages/create-jazz-e2e`, `packages/inspector`
  - Preserve examples, starters, docs, CLI scaffolding, and inspector as
    consumer-facing validation targets.

## Rebuild Thinly

- `packages/jazz-tools/src/runtime/client.ts`, `runtime/db.ts`,
  `runtime/index.ts`, `runtime/context.ts`, `runtime/client-session.ts`
  - Keep the high-level `JazzClient`, `Db`, transaction/query/subscription API
    shape; rebuild as thin bindings over the workspace `jazz` engine.

- `packages/jazz-tools/src/runtime/wasm-runtime-module.ts`,
  `runtime/db-runtime-module.ts`, `runtime/worker-bridge.ts`, `worker`
  - Preserve JS/worker loading surface; replace bridge protocol details with the
    new core bridge.

- `packages/jazz-tools/src/runtime/query-adapter.ts`,
  `runtime/value-converter.ts`, `runtime/row-transformer.ts`,
  `runtime/dynamic-query.ts`, `runtime/select-projection.ts`
  - Thin compatibility layer from TypeScript query/value shapes to new engine
    row/query protocol.

- `crates/jazz-wasm/src`, `crates/jazz-napi/src`, `crates/jazz-rn`
  - Keep package/native binding roles, but regenerate/rebuild bindings around
    the workspace engine APIs.

- `crates/jazz-tools/src/server`, `middleware`, `commands`, `main.rs`,
  `transport_protocol.rs`, `transport_manager.rs`, `ws_stream`
  - Preserve server/CLI/transport entrypoints where useful; reimplement handlers
    around the new sync/storage engine.

- `packages/jazz-tools/src/dev`, `dev-tools`, `mcp`, `codegen`
  - Keep developer-facing workflows; adjust schema/runtime calls to the swapped
    engine.

## Replace Or Delete

These are old alpha internals and should not survive as a parallel engine.
Until deleted, treat them as replacement targets only.

- `crates/jazz-tools/src/runtime_core`
  - Old Rust runtime engine: ticks, writes, durability, subscriptions, sync
    orchestration.

- `crates/jazz-tools/src/storage`
  - Old storage abstraction and backends: `memory`, `sqlite`, `rocksdb`,
    `opfs_btree`, key codecs, conformance harness.

- old alpha OPFS B-tree integration paths
  - Replace with the first-class `crates/opfs-btree` core storage integration
    path.

- `crates/jazz-tools/src/sync_manager`
  - Deleted old sync internals, inbox, forwarding, settlement, transaction
    sealing, and permission sync checks.

- `crates/jazz-tools/src/query_manager/manager.rs`, `graph`, `graph_nodes`,
  `subscriptions.rs`, `writes.rs`, `indices.rs`, `policy*.rs`,
  `settlement_eval_cache.rs`
  - Old query execution, incremental graph engine, write path, policy execution
    internals.

- `crates/jazz-tools/src/row_histories`, `row_format.rs`, `batch_fate.rs`,
  `commit.rs`, `object.rs`, `metadata.rs`, `catalogue.rs`
  - Old row-history, batch/object/catalogue internals. Migrate only tests/specs
    that remain useful.

- `crates/jazz-tools/src/schema_manager/encoding.rs`,
  `query_manager/encoding.rs`, `storage/key_codec.rs`,
  `packages/jazz-tools/src/runtime/native-row-format.ts`,
  `runtime/json-text.ts`, `runtime/ffi-value.ts`
  - Duplicate/old encoders and wire/storage formats. Replace with a single
    engine codec boundary.

- `packages/jazz-tools/src/runtime/subscription-manager.ts`,
  `runtime/sync-transport.ts`, `runtime/sync-telemetry.ts`
  - Old client-side sync/subscription bookkeeping. Replace or shrink to adapter
    glue.

- `packages/jazz-tools/src/permissions/index.ts` and
  `crates/jazz-tools/src/query_manager/policy*`
  - Preserve exported permission DSL/tests; replace old permission evaluation
    internals.

## Uncertain

- `crates/jazz-tools/src/schema_manager`
  - Some schema/lens/catalogue concepts may remain valuable, but encoding and
    catalogue persistence likely belong in the workspace engine.

- `crates/jazz-tools/src/query_manager/query.rs`, `query_wire.rs`,
  `query_to_relation_ir.rs`, `relation_ir*`, `types`
  - Keep only public query vocabulary and payload glue; old compiler and
    materialization internals should stay deleted.

- `crates/wasm-tracing`
  - Generic maintained tracing fork; keep only if still needed by new wasm
    build.

- `dev/benchmarks`, `dev/local-telemetry`, `dev/observability`, `specs`
  - Useful for validation and historical context; not engine runtime, but many
    docs describe old internals.

- `packages/jazz-tools/src/runtime/file-storage.ts`
  - High-level file API likely should survive, but storage/chunking integration
    should be redesigned against engine binary large values.

## Mechanical Verification So Far

- `cargo metadata --no-deps --format-version 1`
  - Passes with the copied core left out of the alpha workspace.

- `pnpm -s list --depth -1 --recursive`
  - Passes with pnpm workspace metadata unchanged.

## Immediate Integration Gaps

- Delete or gut old alpha engine internals listed under "Replace Or Delete".
  The server no longer has `ServerState::catalogue_runtime`; admin
  schema/permissions/lens HTTP now uses a storage-backed `StoredCatalogue`,
  while websocket sync stays on the local engine path. The test-only
  alpha websocket frame injection method has been removed; tests must use direct
  websocket/public-client paths or push catalogue payloads into the catalogue
  store explicitly.
- Remaining catalogue-store simplification gap: `StoredCatalogue` now uses
  a storage-backed `CatalogueIndex` for schema hashes, publish timestamps,
  migration/lens connectivity, and permissions heads/bundles. The old
  `SchemaManager` no longer serves admin catalogue reads/writes and production
  server startup no longer constructs one to seed catalogue state. The broad
  test-only `with_schema_manager` / `with_sync_manager` shims are gone too;
  remaining server tests use narrow catalogue observability helpers for client
  registration, local durability tiers, branch names, and schema diagnostics.
- Direct prepared reads now cover the browser gate's include, hop, UUID-array
  hop, gather, and array-membership `IN` shapes without a broad table fallback.
  Relation-shaped subscriptions reuse the same direct recompute path and now
  maintain multiple native trigger subscriptions for supported hop, gather, and
  reverse-include target rows. Unsupported relation shapes still fail closed;
  forward include target-row wakes are intentionally not implemented when they
  would require a whole target-table trigger.
- Richer public-builder gates cover arrays, binary large values, nullable refs,
  integer predicates, filtered subscriptions, and websocket convergence. Public
  `schema.int()` is represented in core as `U32`, with writes and query
  literals restricted to the non-negative signed 32-bit subset (`0..=i32::MAX`).
- The core WebSocket route is the intended sync boundary. Old
  `transport_protocol.rs` and `transport_manager.rs` code should not regain
  ownership of `/ws` semantics; the old `sync_manager` module has been deleted.
- Runtime-facing HTTP/auth error DTOs now live in `transport_error.rs`; any
  remaining `transport_protocol` dependency should be treated as legacy alpha
  transport surface or test compatibility, not shared server API.
- The old alpha `TransportManager`, `transport_protocol`, runtime transport
  slot, and Tokio `connect`/`ws_stream` path are gated behind
  `transport`/`transport-websocket`. Core browser/server flows should
  depend only on the core WebSocket route/client helper, not the alpha
  transport manager machinery.
- `ServerState::process_ws_client_frame` has been removed. Remaining public
  `transport_protocol` symbols exist only because the gated legacy
  `TransportManager` still uses them internally; they should not become a server
  ingress path again.
- Edge upstream sync currently fails closed when `--upstream-url` is set. This
  intentionally removes the old alpha `TransportManager` path that was pointed
  at the core WebSocket route; server-to-server sync should be rebuilt on
  core wire frames instead.
- Persistent browser runtime should stay a worker-owned core DB, with the
  main thread acting as the public API/proxy surface. `CoreRuntime::fromDb`
  exists so the worker no longer pretends an already-open OPFS DB is
  `openMemory`. The main-thread/worker write protocol no longer sends a
  duplicate transaction id; the main thread keeps the public write handle while
  the worker accepts core mutation calls over encoded row/patch-shaped
  arguments. The worker implementation now names only the core
  capabilities it needs internally, instead of importing the full legacy
  `Runtime` interface. Persistent browser reset is implemented as a core
  runtime capability: `db.logout({ wipeData: true })` / `deleteClientStorage()`
  asks the worker to settle pending local writes, close/free the core WASM DB,
  and destroy the scoped browser storage namespace.
- The public TypeScript runtime/package root no longer exports internal helper
  plumbing such as query translators, row/value converters,
  `SubscriptionManager`, dynamic table helpers, or client-session resolvers.
  Public rows, query builders, `Db`, `JazzClient`, and subscription delta
  types remain the intended high-level shell.
- The base TypeScript `Runtime` interface no longer requires transaction
  methods. Transaction-capable runtimes opt into `TransactionalRuntime`; this
  removes throw-only transaction methods from persistent browser core
  runtime while keeping real `CoreRuntime` mergeable transaction plumbing
  explicit.
- Core mergeable transactions support session-scoped writes through
  identity-aware staging (`mergeable_tx_for_identity` /
  `mergeableTxForIdentity`). A mergeable transaction chooses its identity on
  first write and rejects mixed identities rather than silently writing under
  the wrong author.
- Core exclusive transactions are now exposed through WASM
  (`exclusiveTx`) and the TypeScript runtime routes `beginExclusiveTransaction`
  through the same transaction lifecycle as mergeable transactions. Session-
  scoped exclusive writes still fail closed because core does not expose
  identity-aware exclusive staging.
- The Rust `storage` module is crate-private even under `test-utils`. Existing
  files under `crates/jazz-tools/tests/*storage*_integration.rs` and
  `client_restart_integration.rs` still reference the old storage API, but they
  are inactive because the crate has `autotests = false` and no explicit
  `[[test]]` targets for them. Any revived storage regression coverage should
  be moved crate-internal or exposed through narrow `test_support` helpers.
- Public Rust sync identity/durability/tracing types now live in
  `jazz_tools::sync` and are re-exported from the crate root. The old
  `sync_manager` module has been deleted; needed compatibility vocabulary must
  live under `sync` or another non-legacy module.
- Rust `JazzClient` no longer has a legacy/core engine split:
  core clients are the only live construction path. Server-backed
  clients still connect over the core WebSocket route; offline persistent
  clients now use core RocksDB storage at `jazz-core.rocksdb` and have
  first public row rehydrate coverage. Legacy catalogue rehydrate is
  intentionally not revived as a parallel engine.
- Rust `JazzClient` mergeable transactions now stage writes on the core
  path, expose open transaction writes through transaction-scoped reads, and
  commit to a direct mergeable transaction instead of falling back to the old
  alpha batch runtime.
- Restore replay through the core WebSocket server is covered by core
  regressions plus the unskipped TS server convergence and browser public alpha
  gates. Restore writes parent the current content/deletion winners, and view
  replay ships deletion-register witnesses for restored result rows.
- Public shell APIs (`Db`, `JazzClient`, schema DSL, framework adapters, tests,
  examples) should remain; duplicate execution/query/sync/storage internals
  should be hollowed or deleted once core gates cover the behavior.
- Remaining TS runtime cleanup pressure is concentrated around relation query
  and subscription recompute helpers in `core-runtime/runtime.ts` plus the
  query adapter's relation IR compiler. Those are useful integration scaffolds
  but should be replaced by core prepared query/subscription lowering,
  not allowed to become a second query engine.
- Resolve crate/package name collisions before adding copied core crates to the
  root Cargo or pnpm workspaces.
