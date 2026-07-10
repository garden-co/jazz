# Pending-delete intent record

Type: `wayfinder:grilling`
Status: closed (resolved 2026-07-10)
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

## Resolution (2026-07-10)

**There is no record — the durable pending-delete intent is out of the
MVP.** `jazz.files.delete(fileId)` returns a Promise that resolves on
origin confirmation and rejects on failure; the retry obligation is the
caller's. A restart drops any in-flight delete silently —
"fire-and-forget survives restarts" is no longer promised. This is safe
by construction: the server-side DELETE is idempotent (an already-absent
key answers success), so re-calling `delete()` is always correct and
cheap. The server half of the
[Explicit-delete execution](G-deletion-queue.md) decision (synchronous
in-request DELETE, zero server state) is unchanged; only the client
durability half is dropped, recorded there as an addendum.

Client-side durable file-plane state in core is therefore **zero**:
descriptor cells are ordinary rows, and nothing else exists. The two
decisions grilled before this cut still stand as the deferred design,
preserved here for whoever reinstates the record (opt-in package or a
later core version): key = file id (call-dedupe by construction), value =
created-at only with retry state in memory, home = a
`__pending_file_delete` raw table behind default `Storage`-trait helpers
with the standard `RawTableHeader` kind+format versioning.

With this, the map has no open tickets and no fog: the destination is
reached.
