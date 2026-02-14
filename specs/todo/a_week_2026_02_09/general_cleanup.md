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

**Current status (after #7 hardening passes):**

- Direct weak forms (`assert!(...is_ok())` / `assert!(...is_some())`) were removed from target suites, and many assertions were upgraded from shape-only to identity/content checks.
- QueryManager tests no longer rely on internal test hooks for core CRUD coverage; `test_get_row_if_loaded` and `test_subscriptions` usage was removed, and the corresponding test-only accessors were deleted.
- SchemaManager integration tests were moved off manual object/index mutation. The suite now uses `ingest_remote_row(...)` and real `insert(...)` / `outbox -> inbox` flows instead of `create_with_id(...) + add_commit(...) + index_insert(...)`.
- Join-path edge coverage was expanded:
  - join fails without `ON`
  - join fails for invalid columns
  - join fails for circular/self join chains
- Runtime behavior edge coverage was expanded:
  - concurrent inserts in `RuntimeCore`
  - non-cascading delete behavior across related tables
- The previously documented synced-update reactivity gap was closed in tests (`synced_update_emits_subscription_delta`), so this is no longer treated as an accepted gap.

**Remaining follow-up audit items:**

- Continue scanning for weak equivalents (`matches!(...)` with broad patterns, `len()`-only assertions without content checks) in older tests not yet touched by this pass.
- Add a few negative-path schema/catalogue cases (malformed catalogue payloads, incomplete lens chains under sync pressure) to complement the current happy-path-heavy migration E2E coverage.

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
