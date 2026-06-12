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
