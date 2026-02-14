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

**Weak assertions** — direct `assert!(...is_ok())` / `assert!(...is_some())` patterns in `crates/groove/src/*tests*` were removed in recent passes. Follow-up audit should still look for equivalent weak forms (`matches!(...)`, `len()`-only checks, etc.).

**Implementation-coupled tests** — ~20 tests use internal APIs (`test_get_row_if_loaded`, `is_indexed`, `test_subscriptions`) instead of observable query results. These will break on internal refactors even when external behavior is unchanged.

Additional examples from this pass:

- `schema_manager/integration_tests.rs:1898–1905` “E2E” coverage checks `row_is_indexed_on_branch(...)` (internal index state) instead of asserting query-visible behavior.

**Boundary-bypassing setup in “integration” tests** — tests manually build low-level object/index state that production code normally constructs:

- `schema_manager/integration_tests.rs:564–593` uses `object_manager.create_with_id(...)`, `add_commit(...)`, and direct `index_insert(...)` with a comment that this bypasses real `handle_object_update` flow.
- `schema_manager/integration_tests.rs:1490–1493` explicitly skips A↔B sync pumping and manually exercises transform logic instead of full message-path behavior.

**Under-specified assertions** — still present in parts of the suite; continue converting shape-only checks into content/identity assertions.

**Known behavior gaps currently accepted by tests**:

- `manager_tests.rs:681–684` documents that synced content updates do not emit subscription deltas yet (“not wired into settle flow”), so test coverage validates query visibility but not subscription reactivity.

**Low-hanging fixes completed in this pass**:

- `sync_manager/tests.rs:1475+` (`regular_object_still_syncs_to_server`) now validates destination, object id, branch, commit id, and metadata contents (not only outbox length).
- `manager_tests.rs:2676+` (`join_produces_combined_tuples`) now validates base-row identity and verifies payload includes both base-table and joined-table text values.
- `rebac_tests.rs:624+` (`rebac_exists_clause_denies_non_matching_insert`) now asserts a single deterministic postcondition (`get_tip_ids(..., "main")` is an error) instead of allowing multiple outcomes.
- `schema_manager/integration_tests.rs:e2e_catalogue_sync_with_data_query` now uses real `outbox -> inbox` row sync from client A to client B instead of manually calling the row transformer.
- `schema_manager/integration_tests.rs:e2e_two_clients_server_schema_sync` now asserts query-visible behavior by verifying the server emits `ObjectUpdated` to subscribed client B, replacing the internal `row_is_indexed_on_branch(...)` check.
- `manager_tests.rs:217+` (`insert_returns_handle_with_commit_id`) now asserts the loaded row content matches inserted values, not just presence.
- `manager_tests.rs:276+` (`can_register_query_immediately`) now unwraps subscription registration and asserts subscription tracking state.
- `manager_tests.rs:340+` (`multiple_inserts_all_visible_in_query`) now verifies all inserted row identities and values, not just `is_some()`.
- `manager_tests.rs:2287+` (`include_deleted_query_returns_soft_deleted_rows`) now uses a deterministic `expect(...)` path for the soft-deleted row assertion.
- `manager_tests.rs:2648+` (`join_subscription_marks_dirty_for_joined_table`) now asserts observable post-insert subscription delta output instead of mutating/inspecting internal dirty-node state.
- `manager_tests.rs:2750+` (`join_filter_on_joined_table_column`) now validates matched row identity and joined payload contents (Bob + Learning Rust), not just row count.
- `manager_tests.rs:4328+` (`index_key_includes_branch`) now verifies branch isolation via query-visible results instead of internal index-state checks.
- `schema_manager/integration_tests.rs:336+` (`context_validation`) now asserts live hash membership and lens-path reachability before final context validation.
- `rebac_tests.rs:230+` (`rebac_insert_denied_by_simple_policy`) now uses deterministic `expect(...)` for the error response target before matching payload shape.
- `schema_manager/integration_tests.rs:478+` (`query_graph_compile_with_schema_context`) now uses `expect(...)` for graph compilation success instead of a separate `is_some()` assertion.
- `sync_manager/tests.rs:52+` (`add_server_receives_existing_objects`) now validates metadata object id and empty metadata map for first-sync payloads.
- `sync_manager/tests.rs:1170+` (`metadata_sent_only_once_per_destination`) now validates first-sync metadata id and key/value content.
- `sync_manager/tests.rs:1575+` (`set_query_scope_stores_session`) now unwraps stored session and asserts `user_id` directly.
- `sync_manager/tests.rs:1606+` (`send_query_subscription_includes_session`) now unwraps payload session and asserts `user_id` directly.
- `sync_manager/tests.rs:1788+` (`persistence_ack_direct`) now unwraps commit presence before checking tier ack state.
- Removed `QueryManager::test_subscriptions_mut()` test-only accessor after converting join reactivity coverage to observable behavior assertions.

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
