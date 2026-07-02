# TODO

## Issues

### High

- [**change-user-id-on-live-client**](todo/issues/change-user-id-on-live-client.md) — Changing auth principal on a live Jazz client is currently unsupported. We need a focused follow-up
- [**firefox-private-browsing-opfs-unavailable**](todo/issues/firefox-private-browsing-opfs-unavailable.md) — Firefox's private browsing mode blocks `navigator.storage.getDirectory()`, so Jazz 2 fails to initialise entirely — there is no fallback to ephemeral/in-memory storage.
- [**prune-edge-local-catalogue-entries**](todo/issues/prune-edge-local-catalogue-entries.md) — When an edge reconnects with a stale catalogue hash, core replay currently upserts core catalogue entries but does not remove same-app catalogue entries that exist only on the edge. Add an authoritative reconnect path that prunes edge-local catalogue entries absent from core, so edge catalogue state converges exactly to core.
- [**test_multi-server-sync**](todo/issues/test_multi-server-sync.md) — Missing integration tests simulating client -> edge -> server communication topology.
- [**verbose-batch-payloads**](todo/issues/verbose-batch-payloads.md) — Replayable settlements still repeat per-member batch identity that is already fixed by the outer batch, wasting durable bytes and in-memory copies.

### Medium

- [**better-auth-generalize-unique-field-enforcement**](todo/issues/better-auth-generalize-unique-field-enforcement.md) — The adapter currently guards unique columns in `create` / `update` / `updateMany` with a separate `SELECT` before the write (excluding the current row id on updates). That works end-to-end for the common case (`email` on users) but has two shortcomings:
- [**cors-authorization-header-wildcard**](todo/issues/cors-authorization-header-wildcard.md) — The dev server spawned by `startLocalJazzServer` (and the production
- [**cross-device-signin-orphans-anonymous-data**](todo/issues/cross-device-signin-orphans-anonymous-data.md) — When a user has already signed up on device A (pinning their BetterAuth `user.id` to Jazz user_id `X`), opens the app on device B, creates data under a fresh anonymous Jazz user_id `Y`, then signs in to BetterAuth on device B — the subsequent JWT carries `sub: X`, and any rows on the server created under `Y` become orphaned. They're still on the server but inaccessible to the signed-in principal, since all policies match on `$createdBy: session.user_id` (or equivalent).
- [**duplicated-batch-bookkeeping-storage**](todo/issues/duplicated-batch-bookkeeping-storage.md) — Local batch records duplicate sealed submissions and settlements that are also stored in dedicated durable tables, increasing persistent size and widening the replay state surface.
- [**expo-android-maestro-e2e-ws-unverified**](todo/issues/expo-android-maestro-e2e-ws-unverified.md) — `.github/workflows/expo-android-maestro-e2e.yml` runs the Expo app against a `jazz-tools server` inside a Vercel Sandbox, then fails at the "Verify server evidence" step because `grep -c 'ws client connected' server.log` returns 0. Suspected root cause: the Vercel Sandbox public-port proxy may not forward WebSocket Upgrade requests. `/health` (plain HTTP) works; WS upgrades appear to be dropped or rejected at the edge before reaching `jazz-tools server`.
- [**intentional-index-staleness-fallback**](todo/issues/intentional-index-staleness-fallback.md) — Update paths tolerate stale indexing when old row content is missing, making query correctness probabilistic under some sync histories.
- [**nextjs-plugin-tsx-not-loading**](todo/issues/nextjs-plugin-tsx-not-loading.md) — The Next.js dev server plugin fails to load `.tsx` files.
- [**oversized-visible-row-storage**](todo/issues/oversized-visible-row-storage.md) — The visible-row region stores a full current history row plus visibility bookkeeping, which duplicates history-only fields and makes the hot visible prefix heavier than it needs to be.
- [**policy-error-reasons**](todo/issues/policy-error-reasons.md) — Policy-denied errors (e.g. `WriteError("policy denied INSERT on table todos")`) include
- [**rn-connect-spawns-fresh-tokio-runtime**](todo/issues/rn-connect-spawns-fresh-tokio-runtime.md) — `crates/jazz-rn/rust/src/lib.rs:847-853` spawns a new OS thread and builds a new `tokio::runtime::Builder::new_current_thread().enable_all()` runtime on every `connect()` call. A rustls-enabled Tokio runtime is not a cheap object. Explicit `disconnect()` + `connect()` cycles (server URL change, manual reconnect) thrash runtimes; first connect on cold start adds a thread-spawn + runtime-build to the critical path.
- [**row-storage-common-case-encoding**](todo/issues/row-storage-common-case-encoding.md) — The flat row formats encode common singleton and empty cases verbosely, especially visible branch frontiers and empty metadata, which wastes space on the dominant row shapes.
- [**sync-sent-batch-id-retention**](todo/issues/sync-sent-batch-id-retention.md) — Per-peer sync state retains every sent batch id in memory, which can grow with history size and may keep more tracking state than replay actually needs.
- [**worker-upstream-connected-is-optimistic**](todo/issues/worker-upstream-connected-is-optimistic.md) — The bridge/runtime signal that unblocks edge/global-tier queries fires as soon as `runtime.connect()` returns, not when the Rust WebSocket actually opens. Edge queries unblock while the WS is still handshaking or unreachable — they then fail at the Rust/WS layer (or silently resolve against stale local state) instead of waiting.

### Low

- [**postmessage-stream-adapter-for-worker-bridge**](todo/issues/postmessage-stream-adapter-for-worker-bridge.md) — The browser main-thread ↔ worker bridge currently uses a bespoke `postMessage` path. A future cleanup should carry the same core wire-frame batches across worker and network boundaries, without reviving the removed alpha transport manager.
- [**text-encoded-storage-enums**](todo/issues/text-encoded-storage-enums.md) — Flat row storage currently encodes enum-like fields as text, which is larger than necessary and adds avoidable decode overhead on hot storage paths.
- [**ws-route-clones-every-inbound-frame**](todo/issues/ws-route-clones-every-inbound-frame.md) — `crates/jazz-tools/src/routes.rs:1345` does `let inner = inner.to_vec();` before handing the payload to `process_ws_client_frame(&inner)`. The borrow from `frame_decode(&data)` would survive the `.await` because `data` owns the bytes, so the clone is avoidable — pass `&[u8]` through directly.

### Unknown

- [**unsealed-pending-rows-cleanup**](todo/issues/unsealed-pending-rows-cleanup.md) — Clean up hidden pending rows that were staged for a transaction but never sealed, for example because the user rolled back, disconnected before sending the seal, threw during application code, or hit a client bug.

## Ideas

### Mvp

- [**async-persistence-layer**](todo/ideas/1_mvp/async-persistence-layer.md) — Non-blocking persistence for mobile. The Storage trait is synchronous, so RN currently blocks the UI on I/O. We need an async boundary between Jazz and the persisted storage.
- [**auth-mode-gating-via-permissions**](todo/ideas/1_mvp/auth-mode-gating-via-permissions.md) — Replace `AuthConfig.allow_local_first_auth: bool` with declarative calls on the
- [**column-metadata**](todo/ideas/1_mvp/column-metadata.md) — Allow attaching arbitrary key/value metadata to schema columns, like Zod's `.meta()`, so UI code can drive itself off the schema as a single source of truth (impossible to forget to render a field).
- [**complex-merge-strategies**](todo/ideas/1_mvp/complex-merge-strategies.md) — Per-column/per-table merge strategies beyond LWW (counters, sets, rich text, custom logic).
- [**configurable-client-ttl**](todo/ideas/1_mvp/configurable-client-ttl.md) — Expose the client state TTL as a configurable option per app instead of leaving it hardcoded to 5 minutes and only adjustable through test-only plumbing.
- [**count-aggregation**](todo/ideas/1_mvp/count-aggregation.md) — Add terminal `.count()` queries for filtered relations, with the MVP limited to reactive `COUNT(*)` returning `{ count: number }`.
- [**durability-guarantees-and-rate-limits**](todo/ideas/1_mvp/durability-guarantees-and-rate-limits.md) — Document and enforce a clear durability contract: `await db.insert(...).wait({tier})` resolving guarantees server persistence. Everything else (`db.insert`, etc.) is best-effort — the server may safely drop these requests for rate-limiting or resource reclamation without violating any contract.
- [**e2e-test-remote-scaffold-path**](todo/ideas/1_mvp/e2e-test-remote-scaffold-path.md) — Add an integration test that drives `create-jazz` through its real production
- [**explicit-indices**](todo/ideas/1_mvp/explicit-indices.md) — Developer-declared indices in the schema language, replacing auto-index-all-columns.
- [**lens-hardening**](todo/ideas/1_mvp/lens-hardening.md) — Harden Jazz lens semantics and tooling so schema evolution stays deterministic, reviewable, and safe under mixed-version traffic. This includes preserving hidden newer fields during old-client writes, making lens-path selection ambiguity-aware, supporting corrected or asymmetric migrations for the same schema pair, and defining an explicit story for type-changing migrations.
- [**optimistic-update-dx**](todo/ideas/1_mvp/optimistic-update-dx.md) — Developer-facing API for mutation settlement state — show pending/confirmed/rejected status on rows and filter queries by settlement tier.
- [**storage-limits-and-eviction**](todo/ideas/1_mvp/storage-limits-and-eviction.md) — Bounded storage with LRU eviction of cold data on clients and edge servers, with lazy re-fetch from upstream.
- [**sync-protocol-reliability**](todo/ideas/1_mvp/sync-protocol-reliability.md) — Fix critical reliability gaps in the sync path and unify the transport layer across network sync (client-server), worker communication (main thread-worker), and peer replication (server-server).

### Later

- [**dedupe-array-subquery-tables**](todo/ideas/3_later/dedupe-array-subquery-tables.md) — `QueryGraph.array_subquery_tables` is a `Vec<(NodeId, TableName)>` populated by `compile.rs` and consumed by `involves_table` / `mark_dirty_for_table`. The list can carry duplicate `(node_id, table)` pairs — already possible pre-existing, and more likely now that nested array subqueries register their tables against the outer node. Consumers tolerate duplicates (idempotent dirty bits, short-circuit `.iter().any`), so the cost is a few wasted bool writes per mutation. Worth deduping defensively to keep the list small and make consumer code easier to reason about.

## Projects

- [**ordered-index-topk-query-path**](todo/projects/ordered-index-topk-query-path/)
- [**relational-row-history-engine**](todo/projects/relational-row-history-engine/)
