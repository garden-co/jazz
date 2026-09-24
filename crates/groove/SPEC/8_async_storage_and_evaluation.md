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

Immutable scheduling topology may be shared across evaluations. The graph owns
a bounded cache keyed by the canonical root set: compact node slots, dependency
counts, reverse edges (including repeated inputs), and storage-source slots.
Adding unrelated consumers leaves existing ancestor layouts valid; removing a
node invalidates layouts containing it, and mutable descriptor access clears
the cache. Layouts do not retain graph nodes and are never persisted.

Every evaluation owns a fresh readiness frame over its layout. Requests,
temporal blockers, row values, memo validity, and publication state are not
layout metadata. Sharing topology therefore does not share readiness or bypass
snapshot installation barriers. Runtime statistics expose layout builds and
hits to distinguish topology reuse from semantic memo reuse.

Physical tasks may contract private synchronous filter/projection chains without
changing graph identity. The cached topology contains only structural candidates;
each frame checks global consumers, live retainers and its observable roots before
contracting an edge. Shared, retained, durable and stateful boundaries remain
independently materialized. Filters requiring indirect-field I/O remain ordinary
tasks. Recursive child evaluators retain their scoped execution path.

A contracted task owns its source batch, row/stage cursor, two reusable encoded
row scratch buffers and private final output. Only its terminal batch enters the
memo. Yield budgets count row-stage work, including deep chains over few rows.
No intermediate completion or partial output is published before the entire task
succeeds. Error precedence remains stage-major, then input-row order: after an
error in a later stage, remaining rows still execute preceding stages so an
earlier-stage error wins. Cancellation drops all task-local work. These physical
plans and continuations have no storage or wire encoding.

Hydration memo reuse compiles its structural producer requirements once per
installed node, then checks the live scope, tick/sub-tick, input generation and
producer state on every reuse attempt. Stateless ancestry is not rediscovered
per hit. A recursive producer ends the caller's structural walk: its completion
proof owns child-scope readiness. Arrangement demand remains live because new
consumers can require a physical index without changing the producer's ancestry.
The requirement list is not itself proof that any producer is ready.

Ordinary operators execute as synchronous batch kernels after the driver has
resolved their inputs. This includes arrangements, joins, winner selection,
aggregates and collectors, not only private unary pipelines. Input resolution
still verifies memo generations and physical-producer readiness. A missing
input uses the scoped rebuild driver; I/O index sources, streaming checksums and
recursive child evaluation retain explicit async boundaries. Both entry paths
call the same resident kernels. A resident kernel never recursively schedules
its own predecessors. This separation does not yet bound every stateful kernel's
CPU work or replace the transactional state maps.

The execution layout also compiles forward input slots, preserving duplicate
edges. An evaluation's private batch registers carry completed canonical batches
and their generation/context keys. Ordinary kernels and private pipelines read
these slots directly after checking the same live physical-producer requirements
as memo reuse. Missing or stale slots fall back to the scoped resolver. Registers
are resolved only after a node's own memo misses, so an unused predecessor is not
driven or validated merely because the output is already available. Suspended
evaluations never share registers; recursive child scopes retain their own
context-keyed resolver. Registers do not enter the retained memo or durable
storage as a separate cache.

Join output compiles source field layouts and exact type compatibility once,
then copies selected encoded spans into its batch allocation. Each execution
still validates source row headers and selected offsets. The compiled plan is
in-memory only and emits the existing record byte format; it is not a new codec
or authority for interpreting arbitrary descriptors.

Terminal publication likewise caches only ordered candidate nodes and the
presence of a public root. Current per-evaluation terminal deltas are always
consulted; the cache never stores a selected terminal or a publication. Both
structural summaries retire with existing node metadata and do not keep nodes
or subscriptions alive.

Within a private task, total projections compose field routes back to the last
materialized input. Field selections, nested record paths, encoded constants and
nullable wrapping need no intermediate row encoding. Predicates use the same
comparison kernel against either a real record or those routed fields, including
SQL nulls, nested enum predicates and field-to-field comparisons. Only requested
predicate values are decoded; ordinary scalar literal comparisons retain their
encoded-field fast path. Final output uses the existing record framing writer.
Byte-identical output continues sharing the original record bytes.

Fallible constants and semantic enum conversions are not elided, even when their
outputs are subsequently dropped or filtered away. They materialize the preceding
virtual record and execute at their original semantic boundary. Resumable budget
slots remain for composed stages, so deep chains cannot bypass cooperative yields.
Composition assumes descriptor-valid input records, like ordinary compiler field
selection composition; it does not introduce a new storage or wire representation.

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
The backing I/O's wake must reach every registered consumer directly, without
first requiring the last consumer that polled it to run again. For example,
a parked background query and an awaited foreground read can share a cold
chunk; completion must wake the foreground read even if the background query
cannot receive another owner turn yet. Re-polling replaces that consumer's
previous waker rather than retaining obsolete task owners.

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

Installing a host query-progress waker does not weaken this same-turn contract.
If an overlapping hydration is paused between CPU-only steps, the direct write
finishes those steps before beginning its publication. It drives only that
hydration and its temporal predecessors. Once those operations need cold
storage, the write yields to the host; unrelated runnable queries and eager
storage wakes do not justify polling that cold request again.

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

Declared-index and other durable-node writes are prepared before a resident
publication can park its query-only work. Those completed writes MUST enter the
same staged atomic batch as that publication's base writes before its persistence
owner can take a snapshot. A terminal waiting for missing content does not defer
index durability to a later query turn. Otherwise a settled base publication can
lack its index, and an older terminal can append obsolete index writes after a
newer publication has deleted the row (regression #3015).

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

## Runtime retirement and replacement

A live catalogue replacement first waits for externally owned publications to
settle, before changing the catalogue. Waiting preserves the old usable runtime
and retains the ingress frame in order. The owner wake is registered with the
pending check; settlement, failure, and abandoned persistence wake that owner.
A failed database cannot register another settlement wait.

Retirement then makes the old runtime unavailable and finishes captured durable
writes in queue order through the existing write-outcome guards. It does not
wait for query chunks or deliver pending subscription output. Cancellation or
write failure leaves the old instance unavailable; only completed preparation
permits runtime replacement. Reconstructible hydration and query evaluation are
cancelled when the retired runtime is dropped. Resident publication index writes
already belong to their original atomic publication, not this retirement flush.

Runtime replacement retains the same layout storage, chunk services, and large
value lifecycle mutex while installing one fresh semantic runtime. Independently
suspended auxiliary local chunk reads may finish against that same storage;
they do not prevent a catalogue replacement. A replacement schema error leaves
the retired facade owned and unavailable. The separate raw `into_storage` API
requires unique external storage ownership and is not used for live Jazz runtime
replacement.
