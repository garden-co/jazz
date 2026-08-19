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
   scan does not retain a borrowed visitor while its backend is suspended.
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

`StorageTransaction` retains an owned write overlay. Its reads consult the
overlay synchronously and then await the underlying storage when necessary.
Commit submits one owned ordered write batch.

## Evaluation session

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

## Open questions

- Whether native and browser implementations should expose associated future
  types or one boxed local-future representation.
- Whether large scans initially return one owned batch or an owned async cursor.
  The choice must preserve bounded-memory operation where existing callers need
  it, without retaining borrowed callbacks across suspension.
- Whether tick persistence precedes publication for every current operation or
  whether Groove needs two explicitly named publication policies. Either way,
  no policy may suspend in the middle of visible IVM mutation.
