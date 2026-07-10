# Pending-delete intent record

Type: `wayfinder:grilling`
Status: open
Assignee: guido (claimed 2026-07-10)
Blocked by: (none — B closed)

> Rewritten 2026-07-10: this ticket was "Upload-resume records". The
> invisible-core pivot ([B — device file store](B-staging-store.md))
> moved upload resume out of scope with the offline package (see the
> [design inventory](../notes/offline-package-inventory.md)); what
> remains is the addendum this ticket already owned — the pending-delete
> intent, now the **only** durable client-side file-plane record in core.

## Question

What is the shape and home of the durable pending-delete intent record —
persisted by the SDK at `jazz.files.delete(fileId)` and retried across
restarts until the origin confirms (per
[Explicit-delete execution](G-deletion-queue.md))?

Contents to pin: file id, retry state (attempt count/backoff position, or
nothing — recompute on restart?), and whatever dedupes concurrent calls.
Decide: keyed by file id; stored as a `__`-prefixed raw-table namespace
vs new `Storage` trait methods alongside the batch records
(`crates/jazz-tools/src/batch_fate.rs`); lifecycle — created at the
`delete()` call, deleted on origin confirmation or permanent denial;
restart recovery scanning it alongside the existing batch-record scan;
and the namespace's versioning/migration story (the former map-level
"schema versioning" fog entry, now folded down to this one namespace).
