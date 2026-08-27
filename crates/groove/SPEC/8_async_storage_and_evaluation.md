# Async storage and interruptible evaluation

## Overview

Groove supports ordered storage whose operations may suspend. This is a Groove
engine property: Jazz and other consumers await Groove operations, but do not
discover missing Groove inputs, load them on Groove's behalf, or retry Groove
evaluation.

The migration deliberately completes Groove before adapting Jazz. During that
migration Jazz may not compile. Compatibility with Jazz must not introduce a
second storage trait, evaluator, database owner, or subscription path inside
Groove.

## Required properties

1. `OrderedKvStorage` is the one storage interface for immediate and suspending
   backends. Memory storage completes the same operations immediately.
2. Storage requests and results are owned across suspension. In particular, a
   scan returns an owned, executor-local cursor yielding owned batches; it does
   not retain a borrowed visitor or require complete-range materialization.
3. Evaluation is explicitly interruptible. A recursively nested Rust future is
   not the durable representation of partially completed IVM work.
4. Hash-equal graph nodes share one evaluation entry and one in-flight storage
   request for the same semantic evaluation key.
5. Evaluation may suspend only before publication. A tick is prepared fully,
   then its persistent IVM mutation and subscription output are published as one
   non-suspending transition.
6. Opening a terminal session completes one initial value before later deltas
   become observable. The low-level session then emits only incremental deltas
   or ends with an error.
7. No storage miss, load request, retry outcome, or reset protocol escapes
   Groove.

Chapter 9 extends the owned evaluation-request registry with immutable chunk
requests for large values. That extension does not weaken this boundary:
Groove still owns dependency discovery, suspension, sharing, resumption and
failure scope, while the host supplies only the request capability.

## Storage contract

`OrderedKvStorage` evolves in place. Point reads, scans, writes, explicit
durability boundaries, close, and reopen operations become suspendable where
the backing implementation can require it. The public contract uses owned
request and result values.

The exact Rust future representation may differ between native and local
browser executors, so the interface must not require `Send` merely for
convenience. Type erasure must preserve the same semantic interface; it must
not select an alternate implementation path.

Database ownership is type-erased once, at construction. `Database`, its
layout mapper, publication handles, staged batches, and record-store handles do
not propagate the concrete backend type. The erased backend retains the same
ordered-storage and reopen contracts; evaluation does not downcast it or choose
a backend-specific path.

Cursor lifetimes are explicit. A storage adapter may wrap a cursor while
borrowing executor-local state such as metrics or a transaction session; the
cursor is not required to be `'static`. Those borrows belong to the cursor and
never become borrows held by an IVM node continuation.

Atomic `write_many` is the required backend boundary. The existing
read-your-writes overlay remains available during migration, but whether it is
ultimately an ordered-storage transaction or a Groove-owned prepared write set
is intentionally open; see the storage-model open question. The async migration
must not accidentally make backend transaction lifecycle a new requirement.

## Evaluation session

### One evaluator lifecycle

Hydration and incremental maintenance use the same owned evaluation session.
Hydration is the initial delta from empty state; it is not a second evaluator
or snapshot-shaped installation path. Incremental input can discover a newly
needed non-resident source just as hydration can, so interruptibility is a
property of node evaluation rather than of either caller.

A session reads installed runtime state but does not speculatively mutate it.
Node work first discovers dependencies, then owns any required storage request,
then produces a prepared state edit and output delta. Applying that prepared
edit is non-suspending. There is no whole-runtime clone and no general
speculative runtime overlay. Cancellation discards the session's private node
values and prepared edits without requiring rollback.

A boxed recursive future MUST NOT become the durable representation of blocked
work. Dropping such a future also drops its storage request, and recreating it
on the next poll is not equivalent to retaining an owned request. Blocked node
entries and storage requests therefore live explicitly in the session.

An evaluation is owned state advanced by the runtime:

```rust,ignore
enum EvaluationProgress {
    Complete(EvaluationOutput),
    Blocked,
    Yielded,
}

struct EvaluationSession {
    runnable: WorkQueue,
    nodes: HashMap<EvaluationKey, EvaluationEntry>,
    pending_storage: HashMap<StorageRequestKey, PendingStorage>,
    prepared: PreparedEvaluationState,
}
```

Every input publication is driven synchronously to resident quiescence. A
blocked branch does not prevent independent runnable branches or terminals from
publishing. Updates for one blocked terminal remain ordered behind its earliest
blocked publication, while unrelated terminals continue. Work is shared by
`(node identity, scope/binding, input publication)`; hash-equal consumers join
the same node entry and the same in-flight storage request.

`EvaluationEntry` distinguishes vacant, runnable, blocked, ready, and failed
work. `EvaluationKey` includes the shared graph node identity and the semantic
context that affects its value, such as binding/scope and tick frontier.

Advancing a runnable node borrows the durable `IvmRuntime` only for the duration
of one non-suspending step. The step may:

- produce its result;
- schedule another node dependency;
- register an owned storage request and become blocked; or
- yield after a bounded amount of work.

The driver continues independent runnable nodes before waiting. Equal storage
requests join one in-flight operation. Completion stores the owned result and
wakes every dependent evaluation entry.

Pure operators remain ordinary synchronous transformations over ready inputs.
Interruptible state is concentrated at table/index sources, persisted
arrangements and operators, recursive hydration, and other storage-dependent
seams.

An indexed-row source is the canonical example of dependency discovery within
one source evaluation. Its first retained request scans the durable index. The
result reveals a set of primary keys; all corresponding row reads are then
started together and the source remains blocked until they are ready. The
source projects those rows into its declared output descriptor. Subsequent
table deltas apply the same index predicate and projection synchronously, so
the hydrated source and its incremental form remain one hash-consed node and
one delta path. A higher layer must not emulate this by awaiting an index read
and lowering the returned rows as inline records.

## Tick lifecycle

The tick boundary is:

```text
construct owned evaluation session
  -> advance runnable work
  -> await and install storage results as needed
  -> produce a complete prepared tick
  -> publish IVM state and terminal deltas without suspension
  -> persist the owned ordered batch
```

Until publication, cancellation or storage failure discards the evaluation
session without changing visible IVM state. Publication never awaits storage.
If persistence failure has different semantics for a particular operation,
that operation must model those semantics explicitly rather than leaving a
partially advanced evaluator behind.

### Immediate resident publication and durable release

Host-local visibility and external durability-dependent release are distinct
boundaries. For an immediate local publication, Groove must synchronously
advance every resident base row and unblocked maintained terminal before
waiting for ordered storage persistence. Flat resident one-shot reads observe
the same publication through Groove's resident write overlay. A terminal whose
new include or join dependency is non-resident may remain blocked without
delaying unrelated resident terminals.

Terminal installation and one-shot reads use the same resident overlay as
immediate maintained evaluation. A terminal opened after an unpublished local
write therefore includes that resident write in its initial value. Hydration
still runs through the ordinary evaluator against the database's current read
view; this is not a second snapshot path.

Each resident publication has a monotone `PublicationId`. Incremental terminal
output carries that identity, and successful ordered persistence advances a
contiguous durable publication frontier. Groove does not know whether a
consumer is a Jazz peer: Jazz may deliver local output immediately while
holding peer-visible effects until their publication is at or below Groove's
durable frontier. A later publication must never become externally releasable
past an earlier unresolved publication.

Durability-before-publication remains an explicit policy for operations such
as schema installation that must not become optimistically visible. The policy
is named at the existing Groove database boundary; it does not select another
database, storage, or subscription implementation.

## Terminal installation

Opening a terminal uses the ordinary evaluation session but installs nothing
until its initial delta and prepared maintained-state edits are ready. The
session records frontiers only for dependencies it actually reads. If one of
those frontiers advances while evaluation is blocked, affected work is
invalidated and reevaluated; unrelated writes do not restart installation.
Updates affecting that terminal remain ordered after its installation
frontier and can never race ahead of the initial value.

After installation the terminal owns exactly:

```text
initial value + incremental receiver
```

Loss of continuity ends that low-level session. Reinstallation, if desired, is
a consumer lifecycle concern, not a reset-shaped incremental update.

### Failure lifecycle

Evaluation failures are classified explicitly:

- A storage or node-evaluation failure is scoped to that node's downstream
  closure for the current publication. Every low-level terminal depending on
  the closure receives one terminal error and then closes. Hash-equal terminals
  sharing the failed work fail together. Independent nodes and terminals keep
  running, including when the failure is immediately ready on its first poll.
- Failed maintained state, arrangements, and memo entries in the affected
  closure are invalidated before later work proceeds. Temporal waiter chains
  remove the failed evaluation and release their next entry. A fresh terminal
  installation may then evaluate cleanly.
- An orchestration/invariant failure without a node scope is fatal. It poisons
  the database rather than masquerading as a recoverable subscription failure.

Persistence failure remains publication/database-level because it can make the
durable frontier ambiguous; it is not converted into a terminal retry or reset.

## Migration boundary

The implementation order is Groove-only:

1. Replace borrowed scan/write shapes with owned suspension-safe shapes and
   evolve `OrderedKvStorage` directly.
2. Convert Groove backends, layout mapping, record stores, and transactions.
3. Add the owned evaluation-session driver while immediate storage proves its
   semantics.
4. Move every storage-dependent IVM seam into resumable node work.
5. Convert Groove database reads, commit preparation, persistence, and terminal
   installation.
6. Add delayed-storage black-box tests for interruption, cancellation,
   deduplication, ordering, and scale-independent incremental delivery.
7. Pass the complete Groove test suite and relevant benchmark gates.

Only after this contract is coherent and independently green should Jazz be
adapted to await it. Jazz compile failures during steps 1-7 are expected and
must not be repaired with Groove compatibility layers.

Groove's `TestStorage` is the deterministic suspension and fault-injection
harness for these tests. It wraps in-memory storage, implements the production
contract directly, and makes cold operations yield at least once. Completing a
point read or an entire scan retains that result as resident, after which
covered reads are immediately ready until explicit eviction. Writes keep the
retained view coherent. Its independent controller can pause cold operations;
tests release explicit permits rather than depending on wall-clock delays. The
persistent backend used by storage-fidelity tests is `RocksDbStorage`; the
controlled in-memory test double is `TestStorage`, so the two roles are not
conflated.

## Open questions

- Whether native and browser implementations should expose associated future
  types or one boxed local-future representation.
- Whether read-your-writes belongs to `OrderedKvStorage`, or to a Groove-owned
  prepared write set/read view above atomic `write_many`. Public batch reads
  have genuine users; the IVM's current same-tick usage is not assumed to be
  the target design.
- The exact final shape of Groove's resident write overlay and blocked-terminal
  continuation. The two publication policies and their visibility ordering are
  no longer open: immediate local updates publish resident work before
  persistence, while durability-before-publication remains available for
  explicitly durable operations.
