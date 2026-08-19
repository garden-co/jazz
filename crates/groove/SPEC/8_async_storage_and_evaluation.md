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

## Storage contract

`OrderedKvStorage` evolves in place. Point reads, scans, writes, explicit
durability boundaries, close, and reopen operations become suspendable where
the backing implementation can require it. The public contract uses owned
request and result values.

The exact Rust future representation may differ between native and local
browser executors, so the interface must not require `Send` merely for
convenience. Type erasure must preserve the same semantic interface; it must
not select an alternate implementation path.

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

### Migration bridge versus target

The minimal bridge may drive hydration with a boxed executor-local future, but
it MUST evaluate against private staged operator, arrangement, memo, and node
metadata. Those staged values are installed only after the complete hydration
succeeds. Cancellation or a storage error therefore discards all partial IVM
work. This is a worthwhile intermediate safety property, not the target
scheduler: it neither discovers independent blocked branches eagerly nor
shares in-flight requests.

The target remains the explicit owned session below. In particular, a boxed
recursive future MUST NOT become the durable representation of blocked work or
be credited with request coalescing, bounded stepping, or in-flight node
sharing that it does not provide.

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

## Terminal installation

Opening a terminal is itself an evaluation session. Updates that commit while
the initial value is being prepared must be ordered after the installation
frontier, either by holding an appropriate frontier or by replaying retained
deltas. They must never race ahead of the initial value.

After installation the terminal owns exactly:

```text
initial value + incremental receiver
```

Loss of continuity ends that low-level session. Reinstallation, if desired, is
a consumer lifecycle concern, not a reset-shaped incremental update.

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
contract directly, and is immediate unless its independent controller pauses
operations. Tests release explicit permits rather than depending on wall-clock
delays. The persistent B-tree backend used by storage-fidelity tests is named
`TestBtreeStorage` so the two roles are not conflated.

## Open questions

- Whether native and browser implementations should expose associated future
  types or one boxed local-future representation.
- Whether read-your-writes belongs to `OrderedKvStorage`, or to a Groove-owned
  prepared write set/read view above atomic `write_many`. Public batch reads
  have genuine users; the IVM's current same-tick usage is not assumed to be
  the target design.
- Whether tick persistence precedes publication for every current operation or
  whether Groove needs two explicitly named publication policies. Either way,
  no policy may suspend in the middle of visible IVM mutation.
