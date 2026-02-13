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

Remaining stubs that still break real functionality:

- **Schema hash is hardcoded zeros** — `client.ts:468`: `schema_hash: "0".repeat(64)`. All schemas hash to the same value, which means branch composition (`{env}-{schemaHash}-{userBranch}`) collapses. `blake3` is declared as a dependency but never imported.
- ~~**Client ID is hardcoded zeros**~~ ✅ — fixed by generating/validating real UUID client IDs and wiring them through main-thread + worker sync transport.
- **Nested array relation mapping** — `row-transformer.ts:70–77`: TODO to map nested arrays from array subqueries to relation names. Currently returns unnamed extra values.
- ~~**Token refresh doesn't reconnect**~~ ✅ — worker now aborts and reconnects the stream when `update-auth` is received.

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

## 9. Unused `blake3` Dependency (LOW)

`packages/jazz-ts/package.json` declares `blake3` (line 15) but it's never imported anywhere in the TypeScript code. Was presumably added for schema hash computation (see item 4) but never wired up.

Action: either use it to implement the schema hash, or remove the dependency.

## 10. Examples Lose Data on Reload (MEDIUM)

The example apps (e.g., `todo-client-localfirst-ts`) lose all data when the page reloads, despite browser persistence tests passing. Likely related to the hardcoded-zeros issue in item 3 (schema hash and client ID are all zeros → branch mismatch between sessions, so the new session can't find data written by the old one).

Investigate: does fixing the schema hash / client ID placeholders also fix persistence in the examples?

## 11. Worker Bridge Error Swallowing (LOW)

`db.ts:198–204` catches worker bridge init errors with `console.error` but doesn't propagate them. If the bridge fails to init, subsequent operations will fail with unrelated errors instead of a clear "bridge not initialized" failure.

`client.ts:568–574` similarly logs sync POST failures but doesn't surface them to callers.

## 12. Client ID Simplification (HIGH, in progress)

The current client-id path grew extra concepts (`syncClientId`, `serverClientId`, worker-local stream IDs, fallback transport IDs). This makes identity behavior hard to reason about.

### Target model

- Use one stable **sync client ID** per local client identity.
- Use one ephemeral **connection ID** per stream connection (already server-generated).
- Keep runtime peer IDs (`runtime.addClient()`) internal to local runtime/worker bridging, not sync identity.

### Why

Primary purpose of client IDs is server-side sync-state continuity across short disconnects and reconnects.

### Implementation plan

1. **TS client identity path**
   - Resolve a single `sync_client_id` at startup (provided config ID or generated UUID).
   - Persist and reuse it across reconnects/reloads.
   - Always use it for both:
     - `GET /events?client_id=<id>`
     - `POST /sync { ..., client_id: <id> }`
   - Stop switching IDs based on `Connected.client_id` (can validate/log mismatch, but do not adopt).
   - Remove transport-level fallback ID generation.

2. **Server reconnect grace window**
   - Do not immediately purge client sync state on stream close.
   - Mark client disconnected and keep state for a short lease window (e.g. 30–120s).
   - Purge only after lease expiry.

3. **Validation**
   - Add tests for stable ID reuse across remount/reconnect.
   - Add tests for server-side short-disconnect resume (state retained within lease, purged after).

### Progress

- ✅ Phase 1 started in `jazz-ts`:
  - Stable `sync_client_id` resolution + browser persistence (`localStorage`)
  - Single-ID usage for both `/events` query param and `/sync` body
  - `Connected.client_id` is no longer adopted as local identity
  - Removed transport fallback client-id generation path
  - Added unit coverage for client-id helpers, transport headers, and persistence keying
- ✅ Phase 2 started in `jazz-cli`:
  - Stream disconnect now schedules delayed client cleanup (default 60s grace window)
  - Reconnect cancels pending cleanup for that `client_id`
  - Cleanup runs only when no active stream remains for that client
- ⏳ Follow-up: add dedicated tests for lease expiry behavior (retain within grace, purge after)
