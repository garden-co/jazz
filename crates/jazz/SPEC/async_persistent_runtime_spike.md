# Async persistent runtime spike (design note)

This note is an executable-design spike, not an approved storage API. It holds
the existing browser leader election, SharedWorker broker, Web Locks,
`LeaderWorkerConnectionRole`, follower `MessagePort`s, and the normal Jazz sync
protocol fixed.

## Current seams

`groove::storage::OrderedKvStorage` is synchronous. Its `get`, scans, and
`write_many` are used directly below `groove::Database::commit_batch`. The
`WriteOperation` batch is the encoded atomic unit. `NodeState` owns history,
IVM maintained graphs, and storage; `Db::tick` drives peer messages and turns
node consequences into `SyncMessage::{CommitUnit,ViewUpdate,FateUpdate}`.

The exact maintained-notification seam is
`groove::ivm::runtime::IvmRuntime::tick_with_params`: after
`TickEvaluator::update_node`, it constructs `QueuedMultisinkDeltas` and sends
it to each subscription. An async persistent runtime cannot use that sender as
the immediate publication boundary. It must collect owned notification payloads
in the worker tick output and route them only after the enclosing storage gate
has completed.

The existing async browser page store/B-tree experiment is relevant only below
that boundary: IndexedDB is a dumb page store for Jazz's own B-tree pages, not
an ordered-key-value database. Its page loads and writes require owned buffers
and an explicit completion boundary.

## Proposed propagation shape

1. Replace persistent-runtime storage calls with a pollable owned-buffer
   interface. A memory implementation returns `Ready` immediately, retaining
   the synchronous main-thread fast path.
2. When an IVM path encounters a cold page, retain an owned continuation at the
   `NodeState` scheduler/tick boundary and return `Pending`. Do not retain Rust
   borrows through IndexedDB and do not make individual maintained operators
   independently publish partial effects. Cold loads receive store-issued
   request tokens; explicit cancellation terminalizes before an abort attempt,
   while Rust `Drop` only queues its token in a scheduler-owned cancellation
   registry. The scheduler must drain that registry before replacement work;
   an ambiguous cancellation keeps its token and later queued tokens in that
   registry for recovery rather than losing them;
   restart creates a fresh token rather than resuming an ambiguous old IndexedDB
   request.
3. `Db::tick` must become a pollable driver for persistent nodes. It may first
   compute a received `CommitUnit` and its maintained-view effects, but keeps
   the actual `IvmRuntime::tick_with_params` subscription outputs, all outgoing
   view updates, fates/local receipts, and upstream/core messages inside a
   commit-publication gate.
4. Submit the complete `OwnedWriteOperation` vector through one IndexedDB
   transaction. Release the gate only after that transaction succeeds. On
   failure discard the withheld output and transition the peer/worker to a
   fail-closed error/recovery state.

The pollable store has one serialized in-flight request token. A second worker
tick must wait to begin rather than invoke IndexedDB concurrently with a batch
whose outcome is unknown. Abort/cancel releases no output; a retry is a fresh
request after durable recovery/idempotence checks, not a re-poll of a failed
gate.

The executable `async_persistence_spike` module proves items 1, 3, and 4 with
a controllable pending/failing store. It deliberately does not claim that
today's synchronous `Db::tick` already has this property.

## Jazz transaction boundary

The JavaScript/Rust application-level transaction may await arbitrary work
while its local overlay is open. That does not keep an IndexedDB transaction
alive. Only its final `CommitUnit`, received by the persistent worker, is
encoded as one batch. The persistence adapter must make the whole batch visible
or none, including rows, history, IVM materialization, and fate metadata.

### Existing `NodeState` recovery marker boundary

This is stricter than simply delaying a `write_many`: current ingestion can
persist its initial history/current/IVM work and then separately write storage
consistency markers through `persist_storage_consistency_marker_through`.
Therefore production must choose one of two explicit contracts before it can
claim complete `CommitUnit`/Fate atomicity:

- merge all semantic commit effects **and the durable completion/consistency
  marker** into the same IndexedDB transaction; or
- persist a durable `finalizing(tx_id)` recovery record in the initial batch,
  suppress publication while finalization runs, and on reopen deterministically
  complete or roll back/replay it before admitting peers.

The second option requires an idempotent marker/fate finalizer. Merely treating
the first batch as Local durable would be unsound because a worker crash falls
between it and the marker batch.

## Required spec/API amendments before production

- Define the tick scheduler's `Pending` state, fairness, reentrancy, and what
  happens to new inbound peer messages while one commit is awaiting storage.
- State that `Local` durability and every downstream/upstream consequence of a
  received commit occur after the durable worker batch, never after staging.
- Define retry/idempotence after an IndexedDB transaction abort or worker
  restart. No fate/ack may be invented from the staged state.
- Define the in-flight request serialization, cancellation, and fairness rules
  so no second IndexedDB batch races an unknown first outcome.
- Replace borrowed scan callbacks in cold paths with owned scan/page results or
  resumable cursors.
- Specify cold-load token ownership, cancellation/drop, and worker-restart
  semantics alongside write-request recovery; no old page completion may be
  delivered into a replacement continuation.
- Add end-to-end browser tests with a real asynchronous IndexedDB page store,
  including pending, abort, restart, subscriptions, and core acknowledgements.
