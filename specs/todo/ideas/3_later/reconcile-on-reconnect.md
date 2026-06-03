# Reconcile-on-Reconnect for Hard-Delete Erasure

## What

Narrow the offline-settle residual left by reconcile-on-settle (see [hard delete spec](../../projects/hard-delete/spec.md), Propagation to clients). Reconcile-on-settle only erases a cached row when its covering query **settles against the server while the client is online**. A client that only ever runs that query offline — or is online only while unsubscribed — never reconciles the row, so an erased row can persist in its cache indefinitely.

Reconcile-on-reconnect would reconcile cached rows when the connection comes up, independent of whether the user happens to re-open the covering view while online.

## Notes

- **Depends on reconcile-on-settle** (the per-row pull in the hard-delete spec) landing first; every flavour below reuses its scope-diff and by-id pull.
- **What already happens:** reconnect replays _active_ subscriptions, so rows under a live subscription already reconcile on reconnect. The residual is purely rows from recently-run, now-inactive queries. The client retains only active `subscriptions`; there is no record of recently-used inactive queries, so any "re-settle recent queries" approach must add one.
- **Where it attaches:** there is no explicit reconnect hook in the engine sync layer; reconnect and subscription-replay live above it (transport/client layer). A reconcile-on-reconnect step would hang off that reconnect path.

Flavours, lightest to heaviest:

- **Re-settle recently-used queries.** The client keeps an LRU of recently-run query specs; on reconnect it transiently re-issues them so each settles against the server and triggers the existing per-row-pull reconcile, then drops them. Light, reuses reconcile-on-settle, needs no new server protocol. Residual narrows to rows whose only covering query has been evicted from the LRU or was never tracked.
- **Possession digest on reconnect.** The client sends the set (or a digest) of cached `row_id`s; the server returns those that are hard-deleted, resolved by id via the retained locator. Fully closes the residual, but enumerates the cache on every reconnect — heavy for large caches. A digest/Merkle scheme could cut traffic at the cost of complexity. This is client-driven possession reconciliation: like the rejected server-side possession tracking, but without persistent per-client server state.
- **Deletion-log catch-up.** On reconnect, replay hard-delete tombstones since the client's last-seen sequence. Same blocker as the rejected frontier-catch-up transport — there is no `HistoryScan::SinceSeq`, so it needs a new seq-indexed deletion scan.

- **Recommendation:** if the offline-settle residual proves too broad for the compliance bar, the re-settle-recently-used-queries flavour is the lightest next step and reuses the per-row-pull machinery already specced. Reserve the possession-digest flavour for a hard guarantee that does not depend on the client re-running a covering query.
- **Likely a configuration knob, not a fixed behaviour.** Whichever flavour is built, the natural shape is a developer-facing setting — let cached data persist (the residual the hard-delete MVP accepts) or force reconciliation on reconnect — rather than a single hard-wired policy. Either setting still requires the reconcile-on-reconnect machinery to exist.
