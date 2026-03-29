# TODO

## Issues

### High

- [**change-user-id-on-live-client**](todo/issues/change-user-id-on-live-client.md) — Changing auth principal on a live Jazz client is currently unsupported. We need a focused follow-up
- [**forward-inherits-select-bug**](todo/issues/forward-inherits-select-bug.md) — Forward `INHERITS VIA <fk>` select policies fail to expose child rows to sessions that should inherit access from the parent row.
- [**stale-client-cache-after-scope-removal**](todo/issues/stale-client-cache-after-scope-removal.md) — When a row is deleted (or otherwise exits a query's result set) while a client has no active server-side subscription for that query, the client's local object manager retains stale data indefinitely. Subsequent one-shot `query()` calls with `tier: "edge"` return the stale row because the server never sends the deletion to the client — it considers the object "out of scope" and skips it.
- [**test_multi-server-sync**](todo/issues/test_multi-server-sync.md) — Missing integration tests simulating client -> edge -> server communication topology.
- [**update-inherits-policy-bug**](todo/issues/update-inherits-policy-bug.md) — UPDATE operations fail with PolicyDenied even when an INHERITS chain should grant access.

### Medium

- [**duplicated-sync-transport-state-machines**](todo/issues/duplicated-sync-transport-state-machines.md) — Main-thread client and worker each implement similar reconnect/auth/streaming logic, creating divergence risk and duplicated bug-fix cost.
- [**id-and-string-in-operator-support**](todo/issues/id-and-string-in-operator-support.md) — The public query surface documents `id: { in: [...] }` as supported, but the core Jazz query path does not have clear end-to-end coverage for it. We also need string-column `in` support in the core path so batch string lookups can stay queryable instead of degrading into client-side filtering in optimization-sensitive integrations.
- [**intentional-index-staleness-fallback**](todo/issues/intentional-index-staleness-fallback.md) — Update paths tolerate stale indexing when old row content is missing, making query correctness probabilistic under some sync histories.
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
