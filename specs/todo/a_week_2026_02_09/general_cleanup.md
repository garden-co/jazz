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

## 10. Examples Lose Data on Reload (MEDIUM)

The example apps (e.g., `todo-client-localfirst-ts`) lose all data when the page reloads, despite browser persistence tests passing. Previously suspected root cause was the hardcoded schema hash/client ID placeholders in item 3.

Schema hash and client ID placeholders are now fixed. Re-verify reload persistence in examples and close this item if behavior is now stable.

## 11. Worker Bridge Error Swallowing (LOW)

`db.ts:198–204` catches worker bridge init errors with `console.error` but doesn't propagate them. If the bridge fails to init, subsequent operations will fail with unrelated errors instead of a clear "bridge not initialized" failure.

`client.ts:568–574` similarly logs sync POST failures but doesn't surface them to callers.
