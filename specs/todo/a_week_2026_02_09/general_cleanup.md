# General Cleanup — TODO

Codebase audit findings. Excludes the stale `groove-rocksdb` driver (tracked separately) and blob removal (done on `removing-blob-feature` branch).

## 1. ~~Stringly-Typed Metadata Keys~~ ✅

Done. New `metadata.rs` module with three enums:
- `MetadataKey` — `Table`, `Type`, `Delete`, `AppId`, `SchemaHash`, `SourceHash`, `TargetHash`, `NoSync`
- `ObjectType` — `CatalogueSchema`, `CatalogueLens`, `Index`
- `DeleteKind` — `Soft`, `Hard`

Plus `soft_delete_metadata()` / `hard_delete_metadata()` helpers. Removed `CATALOGUE_TYPE_SCHEMA` / `CATALOGUE_TYPE_LENS` constants. All raw string metadata keys now live only in the enum `as_str()` definitions.

## 2. TypeScript Duplication (MEDIUM)

Three pieces of logic are copy-pasted between `client.ts` (main-thread client) and `groove-worker.ts` (worker):

| Logic | client.ts | groove-worker.ts |
|---|---|---|
| `isCataloguePayload()` | line 580 (private method) | line 150 (module fn) |
| `connectStream()` SSE→binary frame reader | lines 597–657 | lines 164–254 |
| `sendSyncMessage()` / `sendToServer()` | lines 540–575 | lines 115–148 |

The stream reader is ~80 lines of identical frame-parsing with minor variations (worker waits for "Connected" handshake, client doesn't). Should extract shared logic into a utility module.

## 3. Placeholder TODOs in TypeScript (MEDIUM)

These stubs break real functionality:

- **Schema hash is hardcoded zeros** — `client.ts:468`: `schema_hash: "0".repeat(64)`. All schemas hash to the same value, which means branch composition (`{env}-{schemaHash}-{userBranch}`) collapses. `blake3` is declared as a dependency but never imported.
- **Client ID is hardcoded zeros** — both `client.ts:560` and `groove-worker.ts:133` send `"00000000-0000-0000-0000-000000000000"` as `client_id` in sync POSTs.
- **Nested array relation mapping** — `row-transformer.ts:70–77`: TODO to map nested arrays from array subqueries to relation names. Currently returns unnamed extra values.
- **Token refresh doesn't reconnect** — `groove-worker.ts:277–280`: `update-auth` message updates `jwtToken` in memory but doesn't reconnect the stream, so the server still sees the old token.

## 4. `#[allow(dead_code)]` Annotations (MEDIUM)

36 explicit `#[allow(dead_code)]` annotations across the codebase:

**Should investigate and likely remove:**
- `jazz-rs/src/transport.rs:3` — crate-level `#![allow(dead_code)]` blanket-suppresses the entire file
- `jazz-rs/src/client.rs:33` — `context: AppContext` field stored but never read
- `jazz-rs/src/lib.rs:125` — `handle: SubscriptionHandle` stored but never read
- `groove/src/query_manager/manager.rs:137,155` — dead fields/types in query manager
- `groove/src/query_manager/graph_nodes/array_subquery.rs:61` — dead field

**Acceptable (external library, test utilities, benchmarks):**
- `bf-tree/` internal utilities (13 annotations) — third-party-ish code, low priority
- `jazz-cli/src/middleware/auth.rs` (4 annotations) — Axum extractors, consumed by tuple destructuring
- `benches/common/` (6 annotations) — benchmark helpers, conditional usage
- `jazz-cli/tests/` (2 annotations) — test utilities

## 5. `delete()` vs `delete_with_session()` Duplication (LOW-MEDIUM)

`query_manager/manager.rs` has two ~80-line delete implementations:
- `delete()` (lines 1108–1186) — no session
- `delete_with_session()` (lines 1192–1290) — adds policy check, otherwise identical commit/index logic

Same pattern exists for `insert()` → `insert_with_session()` and `update()` → `update_with_session()`, but those delegate cleanly. `delete` doesn't — it duplicates the commit construction, index teardown, and metadata creation.

Action: make `delete()` delegate to `delete_with_session(…, None)` like the other CRUD methods do.

## 6. SyncManager Constructor Duplication (LOW)

`sync_manager.rs` has `new()` (line 432) and `with_object_manager()` (line 452) that repeat 14 identical field initializations. Only `object_manager` differs.

Action: have `new()` call `with_object_manager(ObjectManager::new())`.

## 7. Test Quality Issues (LOW)

**Weak assertions** — several tests assert only `is_ok()` / `is_some()` without checking the value:
- `manager_tests.rs:272` — `assert!(sub_id.is_ok())`
- `manager_tests.rs:216–219` — checks row exists but not its content
- `integration_tests.rs:357` — `assert!(manager.validate().is_ok())`

**Documented bugs in tests** — two tests document known limitations but assert they work:
- `manager_tests.rs:2665–2671` — bug in `mark_subscriptions_dirty()` for join queries
- `manager_tests.rs:2781–2806` — filter on joined table column evaluates against wrong column

**Implementation-coupled tests** — ~20 tests use internal APIs (`test_get_row_if_loaded`, `is_indexed`, `test_subscriptions`) instead of observable query results. These will break on internal refactors even when external behavior is unchanged.

**Missing edge cases:**
- No concurrency tests for runtime_core
- No cascade delete tests
- No tests for invalid join conditions or circular joins
- Schema migration tests only cover happy paths

## 8. Large Files (LOW — awareness)

These files are getting unwieldy but don't need immediate action:

| File | Lines | Notes |
|---|---|---|
| `manager_tests.rs` | 5,648 | Test file; size is expected |
| `sync_manager.rs` | 3,881 | Could extract sub-modules (client state, server state, inbox processing) |
| `manager.rs` | 3,422 | QueryManager; 49 public methods spanning CRUD + subscriptions + index management |
| `object_manager.rs` | 2,294 | Clean after blob removal |
| `types.rs` | 2,419 | Type definitions; cohesive |

## 9. Unused `blake3` Dependency (LOW)

`packages/jazz-ts/package.json` declares `blake3` (line 15) but it's never imported anywhere in the TypeScript code. Was presumably added for schema hash computation (see item 4) but never wired up.

Action: either use it to implement the schema hash, or remove the dependency.

## 10. Worker Bridge Error Swallowing (LOW)

`db.ts:198–204` catches worker bridge init errors with `console.error` but doesn't propagate them. If the bridge fails to init, subsequent operations will fail with unrelated errors instead of a clear "bridge not initialized" failure.

`client.ts:568–574` similarly logs sync POST failures but doesn't surface them to callers.
