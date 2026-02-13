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

- ~~**Schema hash is hardcoded zeros**~~ ✅ — replaced with real deterministic schema hashing (BLAKE3 over canonicalized schema structure, matching Rust ordering rules).
- ~~**Client ID is hardcoded zeros**~~ ✅ — fixed by generating/validating real UUID client IDs and wiring them through main-thread + worker sync transport.
- **Nested array relation mapping** — `row-transformer.ts:70–77`: TODO to map nested arrays from array subqueries to relation names. Currently returns unnamed extra values.
- ~~**Token refresh doesn't reconnect**~~ ✅ — worker now aborts and reconnects the stream when `update-auth` is received.

## 4. ~~`#[allow(dead_code)]` Annotations~~ ✅

Removed all actionable dead code:

- **groove**: deleted `SubscriptionMode` enum + `mode` field, two unused `load_row_from_object_multi_branch*` methods, `array_column_name` field, `parse_object_id_hex` function
- **jazz-rs**: removed blanket `#![allow(dead_code)]` from transport.rs, deleted `context` field from `JazzClient`, `query`/`server_query_id` from `SubscriptionState`, `handle` from `SubscriptionStream`, `connection_id` field + `connection_id()`/`has_backend_secret()` methods. Also fixed stringly-typed metadata in `is_catalogue_payload`.

Remaining `#[allow(dead_code)]` are acceptable: legacy storage internals, Axum extractors, benchmark helpers, test utilities.

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

## 9. ~~Unused `blake3` Dependency~~ ✅

Done. `blake3` is now used to compute schema hashes in `jazz-ts` runtime schema context.

## 10. ~~Examples Lose Data on Reload~~ ✅

Done. Real schema hashes plus stable sync client IDs fixed reload persistence in `todo-client-localfirst-ts`.

Coverage now includes:

- `persists todos across app destroy and remount (OPFS)`
- `persists todos across real page reload (OPFS)`

## 11. Worker Bridge Error Swallowing (LOW)

`db.ts:198–204` catches worker bridge init errors with `console.error` but doesn't propagate them. If the bridge fails to init, subsequent operations will fail with unrelated errors instead of a clear "bridge not initialized" failure.

`client.ts:568–574` similarly logs sync POST failures but doesn't surface them to callers.

## 12. ~~Client ID Simplification~~ ✅

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

- ✅ Phase 1 completed in `jazz-ts`:
  - Stable `sync_client_id` resolution + browser persistence (`localStorage`)
  - Single-ID usage for both `/events` query param and `/sync` body
  - `Connected.client_id` is no longer adopted as local identity
  - Removed transport fallback client-id generation path
  - Added unit coverage for client-id helpers, transport headers, and persistence keying
- ✅ Phase 2 completed in `jazz-cli`:
  - Stream disconnect now schedules delayed client cleanup (default 60s grace window)
  - Reconnect cancels pending cleanup for that `client_id`
  - Cleanup runs only when no active stream remains for that client
- ✅ Phase 3 validation completed:
  - Browser e2e covers stable ID reuse across remount/reload in `todo-client-localfirst-ts`
  - `jazz-cli` integration tests now cover reconnect lease behavior:
    - `test_reconnect_within_grace_retains_client_sync_state`
    - `test_reconnect_after_grace_purges_client_sync_state`

## 13. Server Query Subscription Cleanup Gap (HIGH)

When a client is removed, RuntimeCore only removes SyncManager state, but QueryManager server-side subscriptions remain.

- `crates/groove/src/runtime_core.rs:896-901` — `remove_client()` delegates only to `sync_manager.remove_client()`
- `crates/groove/src/query_manager/manager.rs:233-235` — `server_subscriptions` is independently tracked
- `crates/jazz-cli/src/routes.rs:99` — disconnect cleanup calls `runtime.remove_client(client_id)`

Impact:

- Stale server-side QueryGraphs can accumulate for disconnected clients.
- Unnecessary settle work and memory retention over time.

Suggested fix:

- Add `QueryManager::remove_client(client_id)` that purges `server_subscriptions` (and related pending state) for the client.
- Call it from RuntimeCore’s `remove_client()` path.
- Add integration coverage that no server-side subscriptions remain after grace-expiry cleanup.

Related spec: `specs/todo/b_mvp/query_subscription_disconnect_cleanup.md`

## 14. Silent Server Query Compilation Failures (HIGH)

Server-side query compile failures are dropped without client feedback.

- `crates/groove/src/query_manager/server_queries.rs:63-66` — compile failure hits `continue` with TODO to send error.

Impact:

- Client subscription can appear to hang with no explicit error.
- Hard to diagnose schema/query mismatches in production.

Suggested fix:

- Emit structured failure to the originating client (either `SyncPayload::Error` or `ServerEvent::Error`) with query/schema context.

Related spec: `specs/todo/b_mvp/silent_query_compilation_errors.md`

## 15. Best-Effort Outbound Sync Can Drop Writes (HIGH)

Outgoing sync delivery is fire-and-forget in all clients; failures are logged but not retried or surfaced as hard failures.

- `packages/jazz-ts/src/runtime/client.ts:543-560` — sends server-bound payloads without durable retry semantics
- `packages/jazz-ts/src/runtime/sync-transport.ts:64-75` — non-OK and exception paths only log
- `packages/jazz-ts/src/worker/groove-worker.ts:80-91` and `packages/jazz-ts/src/worker/groove-worker.ts:119-130` — same best-effort POST behavior in worker mode
- `crates/jazz-rs/src/client.rs:126-129` — push failure only warns

Impact:

- Transient outages/auth failures can permanently drop upstream sync attempts for local writes.
- Weak delivery guarantees for multi-device convergence.

Suggested fix:

- Introduce retryable outbox semantics (bounded queue + exponential backoff + jitter + explicit failure state).
- Surface delivery failures to app/runtime so callers can react.

## 16. Auth/CORS Production Hardening Gaps (MEDIUM)

Two production-facing shortcuts are still present:

- `crates/jazz-cli/src/routes.rs:34` — `CorsLayer::permissive()` is unconditional
- `crates/jazz-cli/src/middleware/auth.rs:250-253` — JWKS path is TODO (HMAC-only validation active today)

Impact:

- Overly broad browser access defaults.
- Missing standard key-rotation path for JWT verification.

Suggested fix:

- Make allowed origins configurable (strict-by-default outside local dev).
- Implement JWKS fetch/cache/rotation and issuer/audience validation.

Related spec: `specs/todo/b_mvp/jwks_key_rotation.md`

## 17. Debug `eprintln!` Noise in Runtime Paths (LOW)

Several non-test runtime paths still use `eprintln!` debug output instead of structured tracing.

- `crates/jazz-cli/src/commands/server.rs:121-125`
- `crates/groove/src/sync_manager/inbox.rs:129`
- `crates/jazz-rs/src/client.rs:118-123`
- `crates/jazz-rs/src/client.rs:132`
- `crates/jazz-rs/src/client.rs:206-209`

Impact:

- Noisy stderr in production.
- Harder to filter/route logs by level/target.

Suggested fix:

- Replace with `tracing::{debug,warn,error}` and gate verbose logs behind log level.

## 18. Duplicate Test-Server Infrastructure in `jazz-cli` Tests (LOW)

Server spawn/wait scaffolding is duplicated and diverging across test files.

- `crates/jazz-cli/tests/test_server.rs:11-110`
- `crates/jazz-cli/tests/integration.rs:48-116`

Impact:

- Higher maintenance cost.
- Easy for auth/env behavior to drift between suites.

Suggested fix:

- Consolidate into one shared helper with per-test env overrides (e.g., grace/heartbeat tuning).

## 19. TODO Spec Drift (LOW)

At least one TODO spec appears stale relative to current code and this cleanup log.

- `specs/todo/b_mvp/codegen_schema_hash_and_client_id.md:1-8` still tracks schema hash/client ID placeholders that are already completed in this branch.

Impact:

- Backlog signal gets noisier and less trustworthy.

Suggested fix:

- Archive/close stale TODO specs or rewrite them to track only remaining follow-up work.
