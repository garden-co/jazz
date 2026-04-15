# TODO

## Issues

### Critical

- [**update-auth-noop-breaks-jwt-refresh**](todo/issues/update-auth-noop-breaks-jwt-refresh.md) — The `update-auth` message is now a no-op, so JWT refreshes from `Db.applyAuthUpdate()` never reach the worker's Rust transport. Once the original token expires (or auth context changes), the worker keeps using stale credentials and cannot recover without a full worker restart, which breaks long-lived authenticated sessions.

### High

- [**change-user-id-on-live-client**](todo/issues/change-user-id-on-live-client.md) — Changing auth principal on a live Jazz client is currently unsupported. We need a focused follow-up
- [**forward-inherits-select-bug**](todo/issues/forward-inherits-select-bug.md) — Forward `INHERITS VIA <fk>` select policies fail to expose child rows to sessions that should inherit access from the parent row.
- [**stale-client-cache-after-scope-removal**](todo/issues/stale-client-cache-after-scope-removal.md) — When a row is deleted (or otherwise exits a query's result set) while a client has no active server-side subscription for that query, the client's local object manager retains stale data indefinitely. Subsequent one-shot `query()` calls with `tier: "edge"` return the stale row because the server never sends the deletion to the client — it considers the object "out of scope" and skips it.
- [**test_multi-server-sync**](todo/issues/test_multi-server-sync.md) — Missing integration tests simulating client -> edge -> server communication topology.
- [**transport-manager-reconnect-loop-leak**](todo/issues/transport-manager-reconnect-loop-leak.md) — `connect()` spawns `manager.run()` and drops the `JoinHandle` immediately, but `TransportManager::run` only exits on dropped handle inside `run_connected` and explicitly requires aborting during connect/backoff phases. If `disconnect()` happens before a successful connection (or while reconnecting), the task keeps retrying indefinitely, leaking background reconnect loops across shutdown/reconnect cycles.
- [**update-inherits-policy-bug**](todo/issues/update-inherits-policy-bug.md) — UPDATE operations fail with PolicyDenied even when an INHERITS chain should grant access.
- [**worker-never-emits-auth-failed**](todo/issues/worker-never-emits-auth-failed.md) — When the WebSocket server rejects a JWT, the worker needs to post `{ type: "auth-failed", reason }` back to the main thread so `WorkerBridge` can drive `getAuthState().status` to `"unauthenticated"`. The message type is already defined in `WorkerToMainMessage` and handled in `WorkerBridge`, but `jazz-worker.ts` never emits it. As a result, the main thread never learns of a server-side auth rejection and the auth state machine cannot transition from that path.
- [**worker-server-url-not-converted-to-ws**](todo/issues/worker-server-url-not-converted-to-ws.md) — `runtime.connect` is called with `msg.serverUrl` verbatim, but worker init receives the app-level `serverUrl`/`serverPathPrefix` config (typically HTTP + optional prefix). Passing raw `http(s)` URLs (or ignoring `serverPathPrefix`) causes the Rust WebSocket transport to dial the wrong endpoint, so worker upstream sync never attaches in those deployments. The worker should normalize to the `/ws` URL the same way `httpUrlToWs` is used elsewhere.

### Medium

- [**duplicated-sync-transport-state-machines**](todo/issues/duplicated-sync-transport-state-machines.md) — Main-thread client and worker each implement similar reconnect/auth/streaming logic, creating divergence risk and duplicated bug-fix cost.
- [**intentional-index-staleness-fallback**](todo/issues/intentional-index-staleness-fallback.md) — Update paths tolerate stale indexing when old row content is missing, making query correctness probabilistic under some sync histories.
- [**magic-is-local-first-identity-permission**](todo/issues/magic-is-local-first-identity-permission.md) — Add a `$isLocalFirstIdentity: true` magic check in the permissions DSL that resolves to `true` when the current session was established via local-first auth (i.e. `claims.auth_mode === "local-first"`). This gives policy authors a first-class shorthand instead of manually matching on `"claims.auth_mode": "local-first"`.
- [**nextjs-plugin-tsx-not-loading**](todo/issues/nextjs-plugin-tsx-not-loading.md) — The Next.js dev server plugin fails to load `.tsx` files.
- [**policy-error-reasons**](todo/issues/policy-error-reasons.md) — Policy-denied errors (e.g. `WriteError("policy denied INSERT on table todos")`) include

## Ideas

### Mvp

- [**async-persistence-layer**](todo/ideas/1_mvp/async-persistence-layer.md) — Non-blocking persistence for mobile. The Storage trait is synchronous, so RN currently blocks the UI on I/O. We need an async boundary between Jazz and the persisted storage.
- [**complex-merge-strategies**](todo/ideas/1_mvp/complex-merge-strategies.md) — Per-column/per-table merge strategies beyond LWW (counters, sets, rich text, custom logic).
- [**configurable-client-ttl**](todo/ideas/1_mvp/configurable-client-ttl.md) — Expose the client state TTL as a configurable option per app instead of leaving it hardcoded to 5 minutes and only adjustable through test-only plumbing.
- [**count-aggregation**](todo/ideas/1_mvp/count-aggregation.md) — Add terminal `.count()` queries for filtered relations, with the MVP limited to reactive `COUNT(*)` returning `{ count: number }`.
- [**durability-guarantees-and-rate-limits**](todo/ideas/1_mvp/durability-guarantees-and-rate-limits.md) — Document and enforce a clear durability contract: `await db.insertDurable(...)` resolving guarantees server persistence. Everything else (`db.insert`, etc.) is best-effort — the server may safely drop these requests for rate-limiting or resource reclamation without violating any contract.
- [**explicit-indices**](todo/ideas/1_mvp/explicit-indices.md) — Developer-declared indices in the schema language, replacing auto-index-all-columns.
- [**lens-hardening**](todo/ideas/1_mvp/lens-hardening.md) — Harden Jazz lens semantics and tooling so schema evolution stays deterministic, reviewable, and safe under mixed-version traffic. This includes preserving hidden newer fields during old-client writes, making lens-path selection ambiguity-aware, supporting corrected or asymmetric migrations for the same schema pair, and defining an explicit story for type-changing migrations.
- [**optimistic-update-dx**](todo/ideas/1_mvp/optimistic-update-dx.md) — Developer-facing API for mutation settlement state — show pending/confirmed/rejected status on rows and filter queries by settlement tier.
- [**schema-push-dev-automation**](todo/ideas/1_mvp/schema-push-dev-automation.md) — Push schema and permissions to the server automatically during development, via NAPI + a Vite plugin and a Next.js plugin. In dev mode this should happen transparently on startup or schema change, with no manual step required.
- [**storage-limits-and-eviction**](todo/ideas/1_mvp/storage-limits-and-eviction.md) — Bounded storage with LRU eviction of cold data on clients and edge servers, with lazy re-fetch from upstream.
- [**sync-protocol-reliability**](todo/ideas/1_mvp/sync-protocol-reliability.md) — Fix critical reliability gaps in the sync path and unify the transport layer across network sync (client-server), worker communication (main thread-worker), and peer replication (server-server).

## Projects

- [**ordered-index-topk-query-path**](todo/projects/ordered-index-topk-query-path/)
- [**relational-row-history-engine**](todo/projects/relational-row-history-engine/)
