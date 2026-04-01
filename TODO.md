# TODO

## Issues

### High

- [**stale-client-cache-after-scope-removal**](todo/issues/stale-client-cache-after-scope-removal.md) — When a row is deleted (or otherwise exits a query's result set) while a client has no active server-side subscription for that query, the client's local object manager retains stale data indefinitely. Subsequent one-shot `query()` calls with `tier: "edge"` return the stale row because the server never sends the deletion to the client — it considers the object "out of scope" and skips it.
- [**test_multi-server-sync**](todo/issues/test_multi-server-sync.md) — Missing integration tests simulating client -> edge -> server communication topology.
- [**update-inherits-policy-bug**](todo/issues/update-inherits-policy-bug.md) — UPDATE operations fail with PolicyDenied even when an INHERITS chain should grant access.

### Medium

- [**duplicated-sync-transport-state-machines**](todo/issues/duplicated-sync-transport-state-machines.md) — Main-thread client and worker each implement similar reconnect/auth/streaming logic, creating divergence risk and duplicated bug-fix cost.
- [**intentional-index-staleness-fallback**](todo/issues/intentional-index-staleness-fallback.md) — Update paths tolerate stale indexing when old row content is missing, making query correctness probabilistic under some sync histories.
- [**lens-transform-silent-degradation**](todo/issues/lens-transform-silent-degradation.md) — Failed lens transforms fall back to original data and continue, silently propagating schema mismatches.
- [**test_client-storage-opfs**](todo/issues/test_client-storage-opfs.md) — Missing integration tests for the OPFS B-tree storage backend in client/browser context.
- [**test_server-storage-fjall**](todo/issues/test_server-storage-fjall.md) — Missing integration tests for the fjall storage backend in server context.

## Ideas

### Mvp

- [**client-state-cleanup**](todo/ideas/1_mvp/client-state-cleanup.md) — Garbage collection of server-side state (sync cursors, query subscriptions, session records) for permanently disconnected clients.
- [**complex-merge-strategies**](todo/ideas/1_mvp/complex-merge-strategies.md) — Per-column/per-table merge strategies beyond LWW (counters, sets, rich text, custom logic).
- [**count-aggregation**](todo/ideas/1_mvp/count-aggregation.md) — Add terminal `.count()` queries for filtered relations, with the MVP limited to reactive `COUNT(*)` returning `{ count: number }`.
- [**explicit-indices**](todo/ideas/1_mvp/explicit-indices.md) — Developer-declared indices in the schema language, replacing auto-index-all-columns.
- [**lens-hardening**](todo/ideas/1_mvp/lens-hardening.md) — Harden Jazz lens semantics and tooling so schema evolution stays deterministic, reviewable, and safe under mixed-version traffic. This includes preserving hidden newer fields during old-client writes, making lens-path selection ambiguity-aware, supporting corrected or asymmetric migrations for the same schema pair, and defining an explicit story for type-changing migrations.
- [**optimistic-update-dx**](todo/ideas/1_mvp/optimistic-update-dx.md) — Developer-facing API for mutation settlement state — show pending/confirmed/rejected status on rows and filter queries by settlement tier.
- [**storage-limits-and-eviction**](todo/ideas/1_mvp/storage-limits-and-eviction.md) — Bounded storage with LRU eviction of cold data on clients and edge servers, with lazy re-fetch from upstream.
- [**sync-protocol-reliability**](todo/ideas/1_mvp/sync-protocol-reliability.md) — Fix critical reliability gaps in the sync path and unify the transport layer across network sync (client-server), worker communication (main thread-worker), and peer replication (server-server).

## Projects

- [**auth-expiry-and-auth-state**](todo/projects/auth-expiry-and-auth-state/)
- [**auth-expiry-and-cookie-transport**](todo/projects/auth-expiry-and-cookie-transport/)
- [**ordered-index-topk-query-path**](todo/projects/ordered-index-topk-query-path/)
- [**storage-backend-alternatives**](todo/projects/storage-backend-alternatives/)
