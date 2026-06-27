# Jazz Core Engine Swap Baseline

This branch is a destructive engine-swap branch based on
`feat/merge-batches-and-transaction`.

The goal is to replace the alpha engine with the copied `jazz_core`
implementation, not to keep both implementations behind flags or adapters.
The alpha code that remains should be treated as integration target material:
tests, examples, public TypeScript API shape, and thin binding/worker/server
entrypoints.

## Copied Core

The current `jazz_core` repo was copied into:

```text
vendor/jazz_core
```

Generated and local-only artifacts were excluded from the copy:

- `.git`
- `target`
- `.DS_Store`
- `node_modules`
- `.pnpm-store`
- `dist`
- `build`

The copied source includes:

- `vendor/jazz_core/groove`
- `vendor/jazz_core/jazz`
- `vendor/jazz_core/jazz-server`
- `vendor/jazz_core/jazz-sim`
- `vendor/jazz_core/jazz-wasm`
- `vendor/jazz_core/opfs-btree`
- `vendor/jazz_core/benchmarks`
- `vendor/jazz_core/examples`
- `vendor/jazz_core/docs`

The copied core is intentionally not yet added to the alpha root Cargo or pnpm
workspaces. That wiring is not purely mechanical because the copied repo has
its own workspace root and includes package/crate names that collide with alpha
wrappers (`jazz-wasm`, `opfs-btree`). The next integration pass should delete or
replace the old alpha internals first, then wire the surviving wrapper package
names to the new core.

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
    shape; rebuild as thin bindings over `jazz_core`.

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
    `jazz_core` APIs.

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

- `crates/opfs-btree`
  - Engine-specific OPFS B-tree storage backend; replace with the copied
    `jazz_core` storage integration path.

- `crates/jazz-tools/src/sync_manager`
  - Old sync internals, inbox, forwarding, settlement, transaction sealing, and
    permission sync checks.

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
    `jazz_core` codec boundary.

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
    catalogue persistence likely belong in `jazz_core`.

- `crates/jazz-tools/src/query_manager/query.rs`, `query_wire.rs`,
  `query_to_relation_ir.rs`, `relation_ir*`, `types`
  - Could remain as public/intermediate query contracts if compatible; otherwise
    replace with new core query IR.

- `crates/wasm-tracing`
  - Generic maintained tracing fork; keep only if still needed by new wasm
    build.

- `dev/benchmarks`, `dev/local-telemetry`, `dev/observability`, `specs`
  - Useful for validation and historical context; not engine runtime, but many
    docs describe old internals.

- `packages/jazz-tools/src/runtime/file-storage.ts`
  - High-level file API likely should survive, but storage/chunking integration
    should be redesigned against `jazz_core` binary large values.

## Mechanical Verification So Far

- `cargo metadata --no-deps --format-version 1`
  - Passes with the copied core left out of the alpha workspace.

- `pnpm -s list --depth -1 --recursive`
  - Passes with pnpm workspace metadata unchanged.

## Immediate Integration Gaps

- Delete or gut old alpha engine internals listed under "Replace Or Delete",
  starting at the active dual-runtime server wiring:
  `crates/jazz-tools/src/server/builder.rs` still builds both the old
  `TokioRuntime` path and the direct `CoreServer` path.
- The direct websocket route is the intended sync boundary. Old
  `transport_protocol.rs`, `transport_manager.rs`, and `sync_manager` code
  should not regain ownership of `/ws` semantics.
- `ServerState::process_ws_client_frame` is gated to `test-utils`; it exists
  only for legacy in-process tests that still inject alpha `SyncPayload` frames,
  not for production server traffic.
- Persistent browser runtime should stay a worker-owned direct core DB, with the
  main thread acting as the public API/proxy surface. `CoreRuntime::fromDb`
  exists so the worker no longer pretends an already-open OPFS DB is
  `openMemory`.
- The current sharp correctness gap is restore replay through the direct
  websocket server. The copied core now has unit coverage for cold maintained
  view rehydrate after restore, but
  `packages/jazz-tools/src/runtime/core-runtime/server-convergence.test.ts`
  still contains a skipped end-to-end regression: after insert -> delete ->
  restore, a fresh websocket subscriber does not replay the restored row. The
  browser public alpha gate has the same gap in
  `packages/jazz-tools/tests/browser/alpha-public-flow-gate.test.ts`.
- Public shell APIs (`Db`, `JazzClient`, schema DSL, framework adapters, tests,
  examples) should remain; duplicate execution/query/sync/storage internals
  should be hollowed or deleted once direct-core gates cover the behavior.
- Resolve crate/package name collisions before adding copied core crates to the
  root Cargo or pnpm workspaces.
