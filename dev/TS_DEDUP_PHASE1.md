# TS dedup phase 1

## Scope

Phase 1 replaces the TypeScript transaction read overlay with native reads against an open core transaction. The public TypeScript `Db.transaction()` path uses mergeable transactions: `Db.transaction()` calls `beginTransaction()` with no explicit kind, and `beginTransaction()` constructs `Transaction<"mergeable">` in `packages/jazz-tools/src/runtime/db.ts`. The adapter then opens `mergeableTx` / `mergeableTxForIdentity` in `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts`.

Because the JavaScript default is mergeable, the wasm implementation now keeps both paths for mergeable transactions:

- A core exclusive open transaction is created immediately and each write is staged there as it occurs. Native in-transaction reads evaluate against that core staging state.
- The existing `Vec<WasmTxWrite>` mergeable buffer is retained for commit so mergeable commit semantics and payload shape stay unchanged.
- On mergeable commit or rollback, the open core transaction is abandoned after the legacy mergeable path has committed or discarded.
- Exclusive wasm transactions commit the open core transaction directly.

## Native implementation

- `crates/jazz/src/node/query_eval.rs` keeps `NodeState::tx_query` as the `AuthorId::SYSTEM` compatibility path and adds `tx_query_for_identity` so open-transaction reads can evaluate with the caller identity instead of system identity.
- `crates/jazz/src/db.rs` exposes `exclusive_all` / `exclusive_all_for_identity` and an abandon helper for open transaction handles.
- `crates/jazz-wasm/src/lib.rs` backs every `WasmTx` with an `OpenTxId`, stages `insert` / `restore` / `update` / `upsert` / `delete` into that open transaction as writes happen, and exposes `allInTransaction`, `allInTransactionForIdentity`, `oneInTransaction`, and `oneInTransactionForIdentity`.
- The wasm boundary still rejects `read_view` in the normal read options parser. This slice uses explicit in-transaction read methods rather than adding `ReadViewSpec` plumbing.

## TypeScript adapter change

- `NativeRuntimeAdapter.query()` now detects `transactionId` and routes plain reads through `readPlainRows(...)`.
- For ordinary reads it still calls `all` / `allForIdentity`.
- For transaction reads it calls `allInTransaction` / `allInTransactionForIdentity` when a native transaction handle is present.
- The old semantic overlay evaluator was removed: `applyTransactionReadOverlay`, `rowMatchesQuery`, `sortRowsForQuery`, and the comparison helpers are gone.
- The staged row mirror remains only for write composition and conflict checks. The helper is named `stagedRowForWriteMerge` to make that purpose explicit; query reads no longer consult it.

## Tests

Black-box TypeScript coverage through the public runtime API was added in `packages/jazz-tools/src/runtime/db.transaction.test.ts`:

- `reads insert update delete effects inside a mergeable callback transaction`
- `uses core ordering and default ordering for in-transaction reads`
- `applies non-eq predicates and limit offset inside a mergeable transaction`
- `applies contains predicates inside a mergeable transaction`

Adapter boundary coverage was added in `packages/jazz-tools/src/runtime/native-runtime/runtime.test.ts`:

- `routes session-scoped transaction reads through the identity-aware native method`

The public `Transaction` API does not expose running one session's open transaction under a second session identity. The boundary test verifies that the adapter passes the session identity to the native in-transaction read method and returns the native-filtered result for that identity.

## Gate receipts

| Gate                                                                                                                                                                                                                     | Exit code | Notes                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------: | ----------------------------------------------------- |
| `cargo test -p jazz -j 2`                                                                                                                                                                                                |         0 | Passed                                                |
| `cargo test -p jazz-tools --features test -j 2`                                                                                                                                                                          |         0 | Passed                                                |
| `cargo fmt --check -p jazz -p jazz-wasm`                                                                                                                                                                                 |         0 | Passed after running `cargo fmt -p jazz -p jazz-wasm` |
| `wasm-pack build --target web --release` in `crates/jazz-wasm`                                                                                                                                                           |         0 | Passed                                                |
| `pnpm --dir packages/jazz-tools exec tsc --noEmit`                                                                                                                                                                       |         0 | Passed                                                |
| `pnpm --dir packages/jazz-tools exec vitest run src/runtime/db.transaction.test.ts src/runtime/native-runtime/runtime.test.ts`                                                                                           |         0 | Passed, 98 passed and 1 skipped                       |
| `dev/gates/ts-wire-codec.sh`                                                                                                                                                                                             |         0 | Passed, 85 passed and 1 skipped                       |
| `pnpm exec oxfmt --check packages/jazz-tools/src/runtime/db.transaction.test.ts packages/jazz-tools/src/runtime/native-runtime/runtime.test.ts packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts` |         0 | Passed after running `pnpm exec oxfmt ...`            |

`pnpm --dir <main checkout>/packages/jazz-tools test` is not runnable from this stacked worktree; the reviewer should run it from the main checkout after merge/artifact setup.

Tooling-friction: having the wasm package artifact already built in the stacked checkout would have avoided the first focused vitest setup failure and a separate wasm build cycle.
