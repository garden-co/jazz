# General Cleanup — TODO

Codebase audit findings. Excludes the stale `groove-rocksdb` driver (tracked separately) and blob removal (done on `removing-blob-feature` branch).

## 1. ~~Stringly-Typed Metadata Keys~~ ✅

Done. New `metadata.rs` module with three enums:

- `MetadataKey` — `Table`, `Type`, `Delete`, `AppId`, `SchemaHash`, `SourceHash`, `TargetHash`, `NoSync`
- `ObjectType` — `CatalogueSchema`, `CatalogueLens`, `Index`
- `DeleteKind` — `Soft`, `Hard`

Plus `soft_delete_metadata()` / `hard_delete_metadata()` helpers. Removed `CATALOGUE_TYPE_SCHEMA` / `CATALOGUE_TYPE_LENS` constants. All raw string metadata keys now live only in the enum `as_str()` definitions.

## 2. ~~TypeScript Duplication~~ ✅

Done. New `runtime/sync-transport.ts` with `isCataloguePayload()`, `sendSyncPayload()`, and `readBinaryFrames()`. Both `client.ts` and `groove-worker.ts` delegate to these. Worker's Connected-handshake pattern preserved via `onConnected` callback.

## 3. Placeholder TODOs in TypeScript (MEDIUM)

These stubs break real functionality:

- ~~**Schema hash is hardcoded zeros**~~ ✅
  - Exposed Rust `SchemaHash::compute` through runtime bindings as `getSchemaHash()` in WASM/NAPI.
  - `client.ts:getSchemaContext()` now uses `runtime.getSchemaHash()` instead of `"0".repeat(64)`.
- ~~**Client ID is hardcoded zeros**~~ ✅
  - Added generated UUID client IDs for both main-thread client and worker sync paths.
  - `sync-transport.ts` now uses a generated stable fallback ID instead of all-zero UUID.
  - `/events` stream now includes `client_id` from first connect attempt so `/sync` and `/events` stay identity-consistent before first `Connected` frame.
- ~~**Nested array relation mapping**~~ ✅
  - `transformRows()` now accepts include metadata and maps `array_subqueries` extras to relation names.
  - Handles nested include trees recursively (e.g., `owner.manager`) using schema-derived relation metadata.
  - Forward includes deserialize to a single nested object; reverse includes deserialize to arrays.
- ~~**Token refresh doesn't reconnect**~~ ✅
  - `update-auth` now aborts the stream and schedules reconnect so new auth is used.

## 4. ~~`#[allow(dead_code)]` Annotations~~ ✅

Removed all actionable dead code:

- **groove**: deleted `SubscriptionMode` enum + `mode` field, two unused `load_row_from_object_multi_branch*` methods, `array_column_name` field, `parse_object_id_hex` function
- **jazz-rs**: removed blanket `#![allow(dead_code)]` from transport.rs, deleted `context` field from `JazzClient`, `query`/`server_query_id` from `SubscriptionState`, `handle` from `SubscriptionStream`, `connection_id` field + `connection_id()`/`has_backend_secret()` methods. Also fixed stringly-typed metadata in `is_catalogue_payload`.

Remaining `#[allow(dead_code)]` are acceptable: bf-tree internals, Axum extractors, benchmark helpers, test utilities.

## 5. ~~`delete()` vs `delete_with_session()` Duplication~~ ✅

Done. `delete()` now delegates to `delete_with_session(…, None)`, matching `insert()` and `update()`. Removed ~70 duplicate lines. Also fixed a latent bug: `delete()` was missing the `forward_update_to_servers` call that `delete_with_session()` had.

## 6. ~~SyncManager Constructor Duplication~~ ✅

Done. `new()` now delegates to `with_object_manager(ObjectManager::new())`.

## 7. Test Quality Issues (LOW)

**WASM test build precondition**:

- `subscription-manager.wasm-integration.test.ts` requires built `groove-wasm/pkg` artifacts.
- Turbo now enforces this in the normal graph (`jazz-ts#build` depends on `groove-wasm#build`).
- Direct targeted Vitest runs still hard-fail if artifacts are missing, with an explicit instruction to run `pnpm --filter @jazz/rust build:crates` first.
- For local focused runs, do a one-time `pnpm build` (or the crate-only build above) before `pnpm --filter jazz-ts exec vitest ...`.

**Weak assertions** — several tests assert only `is_ok()` / `is_some()` without checking the value:

- `manager_tests.rs:272` — `assert!(sub_id.is_ok())`
- `manager_tests.rs:216–219` — checks row exists but not its content
- `integration_tests.rs:357` — `assert!(manager.validate().is_ok())`

**Documented bugs in tests** — two tests document known limitations but assert they work:

- `manager_tests.rs:2665–2671` — bug in `mark_subscriptions_dirty()` for join queries
- `manager_tests.rs:2781–2806` — filter on joined table column evaluates against wrong column

**Implementation-coupled tests** — ~20 tests use internal APIs (`test_get_row_if_loaded`, `is_indexed`, `test_subscriptions`) instead of observable query results. These will break on internal refactors even when external behavior is unchanged.

Additional examples from this pass:

- `manager_tests.rs:2631–2652` mutates internal subscription graph state (`test_subscriptions_mut().graph.clear_dirty()`) and asserts internal dirty flags rather than observable subscription output.
- `schema_manager/integration_tests.rs:1898–1905` “E2E” coverage checks `row_is_indexed_on_branch(...)` (internal index state) instead of asserting query-visible behavior.

**Boundary-bypassing setup in “integration” tests** — tests manually build low-level object/index state that production code normally constructs:

- `schema_manager/integration_tests.rs:564–593` uses `object_manager.create_with_id(...)`, `add_commit(...)`, and direct `index_insert(...)` with a comment that this bypasses real `handle_object_update` flow.
- `schema_manager/integration_tests.rs:1490–1493` explicitly skips A↔B sync pumping and manually exercises transform logic instead of full message-path behavior.

**Under-specified assertions** — tests that can pass while important behavior is wrong:

- `sync_manager/tests.rs:1507` (`regular_object_still_syncs_to_server`) asserts only `outbox.len() == 1` without validating destination/payload/object.
- `manager_tests.rs:2726–2730` (`join_produces_combined_tuples`) asserts only that one row exists and `row.data` is non-empty; does not verify joined column semantics.
- `rebac_tests.rs:630` allows two outcomes (`tips.is_err() || !contains(...)`) instead of asserting a single expected postcondition.

**Known behavior gaps currently accepted by tests**:

- `manager_tests.rs:681–684` documents that synced content updates do not emit subscription deltas yet (“not wired into settle flow”), so test coverage validates query visibility but not subscription reactivity.

**Low-hanging fixes completed in this pass**:

- `sync_manager/tests.rs:1475+` (`regular_object_still_syncs_to_server`) now validates destination, object id, branch, commit id, and metadata contents (not only outbox length).
- `manager_tests.rs:2676+` (`join_produces_combined_tuples`) now validates base-row identity and verifies payload includes both base-table and joined-table text values.
- `rebac_tests.rs:624+` (`rebac_exists_clause_denies_non_matching_insert`) now asserts a single deterministic postcondition (`get_tip_ids(..., "main")` is an error) instead of allowing multiple outcomes.
- `schema_manager/integration_tests.rs:e2e_catalogue_sync_with_data_query` now uses real `outbox -> inbox` row sync from client A to client B instead of manually calling the row transformer.
- `schema_manager/integration_tests.rs:e2e_two_clients_server_schema_sync` now asserts query-visible behavior by verifying the server emits `ObjectUpdated` to subscribed client B, replacing the internal `row_is_indexed_on_branch(...)` check.

**Missing edge cases:**

- No concurrency tests for runtime_core
- No cascade delete tests
- No tests for invalid join conditions or circular joins
- Schema migration tests only cover happy paths

## 8. Large Files (LOW — awareness)

These files are getting unwieldy but don't need immediate action:

| File                | Lines | Notes                                                                            |
| ------------------- | ----- | -------------------------------------------------------------------------------- |
| `manager_tests.rs`  | 5,648 | Test file; size is expected                                                      |
| `sync_manager.rs`   | 3,881 | Could extract sub-modules (client state, server state, inbox processing)         |
| `manager.rs`        | 3,422 | QueryManager; 49 public methods spanning CRUD + subscriptions + index management |
| `object_manager.rs` | 2,294 | Clean after blob removal                                                         |
| `types.rs`          | 2,419 | Type definitions; cohesive                                                       |

## 9. ~~Unused `blake3` Dependency~~ ✅

Resolved by removing the TypeScript-side `blake3` dependency and using Rust hashing via runtime bindings.

## 10. ~~Examples Lose Data on Reload~~ ✅

Re-verified OPFS persistence behavior in both example apps after the schema hash/client ID fixes:

- `todo-client-localfirst-ts` browser E2E suite passes, including `persists todos across app destroy and remount (OPFS)`.
- `todo-client-localfirst-react` browser E2E persistence case passes: `persists todos across app unmount and remount (OPFS)`.

No data-loss-on-reload behavior reproduced in current example harness.

## 11. ~~Worker Bridge Error Swallowing~~ ✅

Done. Error swallowing removed in both bridge init and sync POST transport:

- `db.ts` now stores `bridge.init(...)` directly in `bridgeReady` (no `.catch(console.error)`), so bridge init failures reject and propagate through `ensureBridgeReady()`.
- `sync-transport.ts:sendSyncPayload()` now throws on network errors and non-2xx responses instead of logging and continuing.
- `client.ts` and `worker/groove-worker.ts` now catch rejected sync POSTs at the call site, log once, and trigger reconnect (`detachServer()` + `scheduleReconnect()`), avoiding silent drop behavior.
- Added `sync-transport` tests covering both rejection paths:
  - non-2xx response rejects
  - fetch/network failure rejects
