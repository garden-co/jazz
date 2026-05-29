# History And Projection

## 12. Row History And Current Projection

For each structural storage layout of each application table, the engine creates
append-only history storage. A structural layout may serve multiple
catalogue/schema versions when their physical storage shape is compatible.

History rows contain enough data to rebuild current projection:

- logical row id
- transaction id
- branch/view context or source metadata
- operation: insert, update, delete
- application values
- immutable creation metadata
- update metadata
- conflict metadata or explicit empty conflict state
- engine edit metadata needed for sync and API semantics

The selected baseline is columnar current projection plus JSONB-style history
payloads as the first experiment. System columns and hot keys remain relational
so history can be scanned by row, branch, transaction, global epoch, operation,
and schema/layout. User values in cold history may be stored as an inspectable
JSON payload, with generated/side indexes added only for proven hot historical
queries. Current projection remains columnar because ordinary reads, policies,
subscriptions, and indexes are hot.

Ordinary deletes are append-only history rows. Restore/undelete is also
append-only: restoring a deleted row writes a new transaction/version derived
from preserved deleted-row values rather than erasing or mutating the delete
tombstone. Restore reuses insert authorization semantics over the restored
visible row.
Stale sync replay whose latest known version is an older delete must not hide a
newer restored version or append duplicate history.

Ordinary delete is a history row version, not physical removal. Hard delete and
history truncate remain product-visible destructive retention operations, but
they are distinct from ordinary delete. They must be explicit, policy- or
admin-authorized, and must have deterministic replication semantics so peers do
not resurrect truncated state from stale history.

Main must have a current projection for fast ordinary reads. Current projection
rows contain the resolved visible row value plus conflict metadata.

Projection rebuild:

1. ignore rejected transactions
2. consider history visible in the projection's branch view
3. group candidates by logical row
4. apply branch source precedence
5. apply transaction ordering for linear histories
6. preserve concurrent candidates when merge strategy cannot reduce them
7. apply delete semantics

Long-lived stores may compact accepted history into sealed history blocks once
the open rows are no longer needed for hot current projection. Sealed history is
still authoritative history: exports, historical point reads, pinned branch base
reads, policy dependency reconstruction, and repair must behave the same as
they did before compaction. Compaction must not reopen rejected transactions or
turn locally visible pending state into durable accepted history.

Branch bases are anchors across compaction. If a branch is pinned to a base
epoch or version frontier, branch reads and branch-scoped exports must continue
to resolve that base after main-branch history has been sealed. A sealed-history
implementation therefore needs point-read and row-scope decode paths, not only a
bulk archival format.

Sync should treat open history, sealed history blocks, and any sidecar state
needed to decode them as one coherent history delta. A receiver must not apply
one part far enough to expose a row while missing another part required to
reconstruct or validate that row. This argues for a `HistoryDelta`-shaped
boundary even if the first implementation still encodes ordinary bundles only.

Accepted global transactions are ordered by `(global_epoch, tie_breaker)`,
because several transactions may share a global epoch. Local pending
transactions are ordered by `(node, local_epoch)` only within one node.
Cross-node same-row pending writes are conflict candidates unless a merge rule
resolves them.

Remote pending history must not displace durable accepted/global current state.
It may materialize only when no durable version exists for that row and branch.
Local pending mergeable writes may sort after durable rows for optimistic UX.
Pending exclusive writes are not visible until globally accepted.

If a delete and update are concurrent visible candidates, the reducer must apply
a specified merge/delete rule or preserve candidates. It must not silently pick
one by incidental database row order.

Open issues:

- full concurrent-row merge semantics
- exact conflict metadata shape
- exact hard-delete/truncate authorization, sync, and historical-query semantics
- hot branch projection heuristics
- sealed history block format, eligibility rules, and decode/query planning
- coherent history delta wire/apply shape for open rows, sealed blocks, and
  sidecars
