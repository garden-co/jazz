# Native WAL readers for subscription openings

The native server keeps one writer and adds two bounded workers for committed
subscription openings. Public remote one-shot queries use subscription openings,
so a long opening can execute while the owner accepts writes and another worker
answers a short query. The worker count bounds concurrent jobs and pinned
snapshots; each worker has its own SQLite page and immutable-chunk caches.

Before this change, the owner evaluated each opening's SQL to completion before
serving other sessions. With this change, eligible openings register their normal
dependency watches and wait for a read worker. Existing group sharing, read
policies, schema views, sync coverage and output publication remain on their
ordinary paths.

## Execution and ordering

The owner compiles the query under its admitted schema and request claims. The
worker runs the same `Inner::execute_on` implementation the owner uses, including
large-value preps and row decoding. Its main-file connection is read-only.
`_jazz_lv_scratch` is a TEMP table on that connection, shadowing the persistent
scratch table, so a logical read never takes the main file's writer lock for
scratch answers.

Before handing a job to a worker, the owner starts and actually reads from both
the row connection and the large-value chunk reader. Both WAL snapshots are
therefore pinned before this store's writer advances. A worker cannot combine
an older row descriptor with chunks reclaimed after that descriptor changed.
The worker releases both snapshots before returning its connection to the pool.

Only the owner publishes results. While an opening is pending, ordinary keyed
invalidation cancels it if its data or policy dependencies change. Claim changes
and schema-policy activation cancel it through the same existing refresh paths.
An obsolete result, including an empty result, is discarded. The opening then
executes again with the current schema, claims and committed data. Unrelated
writes do not invalidate it. Repeated relevant changes can delay an opening;
the existing remote-read deadline still applies.

The owner polls only pending opening groups. When both workers are occupied,
new openings remain registered on the owner: they create no queued job or pinned
snapshot until a connection is available. Completion wakes the store thread;
it does not wait for the one-second idle maintenance tick.

For example, Alice's slow event filter can run on one worker while Bob commits
a write to another table and Carol reads a point row on the second worker.
If Bob instead deletes a membership that authorizes Alice's event query, her
in-flight result is discarded and her opening is re-evaluated with that
membership absent. A changed schema policy or identity claim follows the same
rule. The permission decision is never replaced by a cached authorization flag.

## Boundaries and failures

This applies to file-backed native authorities on the platform filesystem VFS
in WAL mode. In-memory stores, browser VFSes and replicas retain their existing
execution. Transaction reads, local direct reads, nested array-relation assembly
and maintenance of already-open subscriptions stay on the owner. Those operations
can still block the owner; this change is not parallel execution of every engine
operation. Two expensive openings can also occupy both workers and queue later
openings.

Dropping an opening interrupts its SQL and discards its result. A worker failure
rejects the read; a closed or unrecoverable pool fails subsequent reads with a
storage error instead of falling back to a permission-bypassing path or waiting
forever. Closing the database interrupts and joins workers and closes all their
connections before the writer closes, preserving the final WAL checkpoint.

The durable schema, storage format version, canonical encodings and wire bytes
do not change. Worker state and TEMP scratch answers are process-local. The
ordinary persistent scratch table is unchanged.

## Validation

`crates/jazz/tests/concurrent_readers.rs` uses public database APIs and a test-only
worker barrier to fix the interleaving after snapshots are pinned. It checks
unrelated reader/writer progress, row-policy and membership revocation, claim
changes, required-include changes, schema-policy changes, bounded saturation,
chunked predicates and per-connection scratch, abandoned reads, worker panic
recovery and final WAL cleanup. A server lifecycle test checks that completion
wakers do not retain a dropped store thread. The barrier is
gated with `cfg(any(test, feature = "testing"))` in the lower database crate.

The benchmark gives each point request a distinct, true predicate binding so
live-group reuse cannot answer it without fresh SQL. Both before/after builds
use that identical harness, including the control runtime exported from the
benchmark-only commit. Receipts identify each executable and source tree and use
the same fixture and request parameters. Developer-machine measurements do not
replace the repository's same-base CodSpeed comparison for a performance-win
claim.

The [local comparison](../benchmarks/server-read-pool.md) retains positive and
negative results: short-request isolation under one expensive query, queueing
when both readers are occupied, and overhead in the idle arm.
