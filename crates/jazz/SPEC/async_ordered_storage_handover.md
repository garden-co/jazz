# Async storage reimplementation handover

This document proposes a fresh implementation from the integration base
`codex/jazz-core-engine-swap` (`7695d9eae`). It is a design and landing plan,
not a description of an intermediate implementation.

The goal is one engine whose storage may complete immediately or asynchronously,
while preserving Jazz's local-first visibility, durability, synchronization,
and end-to-end incremental-delivery contracts. The smallest safe route is to
clarify the synchronous architecture first, make its operation boundaries
explicit and owned, and introduce suspension only after those boundaries are
covered by black-box tests.

The active reimplementation intentionally completes Groove in isolation before
adapting Jazz. Jazz is allowed to remain compile-red while `OrderedKvStorage`,
Groove database operations, IVM interruption, and terminal installation are
made coherent. The normative Groove design and migration boundary live in
`crates/groove/SPEC/8_async_storage_and_evaluation.md`. This document describes
the later cross-layer destination; it must not be used to justify a Jazz-side
loading or compatibility path during the Groove phase.

## Non-negotiable properties

1. There is one query and IVM implementation. Immediate and suspending storage
   do not select different semantic paths.
2. Important abstractions evolve in place. Do not add `Async*`,
   `DemandDriven*`, `Immediate*`, or similarly named wrapper owners that retain
   the old database, node, evaluator, or storage API as a parallel real path.
3. Groove owns query/IVM residency. A non-resident Groove input is an internal
   Groove scheduling concern, not a retry protocol driven by Jazz.
4. Jazz owns Jazz synchronization, authority, policy/read-view selection,
   optimistic publication, and durable/external publication.
5. A local write is visible to the appropriate local current-state view before
   its durability tier advances. `None`, `Local`, `Edge`, and `Global` remain
   provenance/durability facts, not storage-completion modes.
6. Preparation may suspend and must be mutation-free. Publication is one
   non-suspending state transition. Persistence and external release are later,
   explicitly ordered phases.
7. A low-level subscription has one initial value and then only incremental
   changes relative to it. If continuity is lost, it ends. A high-level Jazz
   subscription may install another low-level subscription.
8. Hash-equal IVM nodes remain shareable across queries and low-level
   subscription reinstallations.
9. Steady-state incremental delivery may not rebuild complete results merely
   to make a possible future restart convenient.
10. Existing correctness tests are specifications. Behavior changes must be
    explicit; tests must not be mechanically rewritten to make migration pass.

## Binding lifecycle decision (2026-08-20)

Single-threaded bindings keep resident mutation initiation synchronous. A
successful insert, update, delete, or transaction commit has already published
its resident change before returning, so an immediate query or subscriber sees
it in the same tick. The binding does not replace these methods with Promise
returning wrappers merely because durable storage may suspend.

The host explicitly enables deferred local persistence. In that mode Jazz
publishes resident state synchronously, enqueues the prepared persistence
outcome, and lets the existing asynchronous `Db::tick` lifecycle settle queued
outcomes in commit order. A write's Local tier remains pending until that
settlement completes. Peer upload is queued only afterward, preserving the
separate Jazz requirement that optimistic local visibility must not become
external visibility before local persistence.

Consequently the TypeScript runtime accepts synchronous native ticks and
Promise-returning WASM ticks through one adapter contract. Tick and transport
pumps serialize their asynchronous work; CRUD and transaction APIs remain
synchronous. The default Rust/NAPI lifecycle remains eager, while WASM opts in
to deferred persistence because its event loop cannot synchronously wait for
browser storage.

The synchronous binding currently polls resident engine operations through the
existing `block_on` bridge. This is safe only under the resident-ready
contract. Replace it with an explicit single-poll helper that reports an
invariant violation on `Pending`; never spin the browser event loop waiting for
hydration. A resident write whose changed include/join discovers a non-resident
row is the documented exception and must move through the interruptible query
lifecycle rather than weakening ordinary resident visibility.

## Evolve abstractions in place

The synchronous implementation is not a legacy core to wrap. It is the engine
whose storage-facing operations need to become suspendable.

The target ownership graph keeps the important existing names and roles:

```text
Db
 `-- Node
      |-- Jazz control-plane and canonical state
      |-- ordered storage session
      `-- Groove Database
           |-- resident working set
           |-- shared IVM graph
           `-- terminal sessions
```

`Db`, `Node`, and Groove `Database` should gain owned operation state and async
entry points directly. There must not be a lasting graph such as:

```text
AsyncDb -> Db
DemandDrivenNode -> Node
AsyncDatabase -> Database
```

During the initial Jazz conversion, `Node` serializes access to its canonical
`NodeState` with a thread-local async mutex. This is a pragmatic correctness
boundary: no `RefCell` guard may survive a storage suspension, cancellation
releases the mutex automatically, and concurrent local operations wait instead
of panicking on a dynamic borrow. It is not the ideal final scheduler. Revisit
whether explicit owned operation sessions can provide clearer fairness,
reentrancy, and observability once the operation lifecycles are stable, without
introducing a second database or node facade.

Temporary migration helpers are acceptable only when private, narrowly scoped,
and removed in the same bounded step that moves responsibility into the real
owner. A wrapper must not become the place where semantics live while the
wrapped abstraction remains a separately usable execution path.

Likewise, completion timing is not a constructor or backend mode. Memory and
warm native storage naturally make the same future ready immediately.

## Ownership boundaries

```text
durable ordered storage session
        |
        +-- Jazz control-plane and canonical-state operations
        |
        `-- Jazz-provided Groove storage capability
                    |
              Groove Database
                    |
              shared IVM graph nodes
                    |
              Groove terminal session
                    |
               Jazz query session
                    |
          high-level Jazz subscription
```

The durable backend may use one ordered scheduler, but each layer owns the
meaning of its requests. Jazz supplies Groove with a capability for a fixed
Jazz read view; Groove decides which inputs within it must become resident and
when to load them.

### Groove Database

Groove owns:

- the resident working set used by its synchronous evaluator;
- missing-input discovery across the whole runnable IVM frontier;
- deduplication and sharing of in-flight loads;
- hydration of loaded inputs;
- resumption of affected evaluations;
- prepared IVM transitions and terminal state;
- Groove persistence operations and their ordering where applicable.

The normal surface should look like an ordinary future or stream:

```rust,ignore
let result = groove.evaluate(read_capability, query).await?;
let terminal = groove.subscribe(read_capability, graph).await?;
```

The internal evaluator may report a missing-input frontier, but `NotResident`
must not escape to Jazz query, mutation, or subscription orchestration.

### Jazz Node and Db

Jazz owns:

- selection of database read view and durability tier;
- schema, lens, authorization, branch, and authority semantics;
- Jazz sync ingress and egress;
- canonical transaction and fate state;
- optimistic local publication;
- ordered durable publication and external release;
- construction of the capability supplied to Groove;
- composition of local Groove output with remote authority information.

Jazz does not inspect Groove storage misses, collect Groove load requests, or
retry Groove evaluation closures. It awaits Groove operations and handles only
completed semantic outputs or terminal failures.

### Groove storage capability

The capability fixes the Jazz context in which Groove may load:

```rust,ignore
struct GrooveReadCapability {
    read_view: ReadViewId,
    storage_mapping: StorageMappingId,
    scheduler: StorageSchedulerHandle,
}
```

It loads only data already available locally in that read view. It does not
perform Jazz sync or wait for remote authority. Later replicated facts enter
through Jazz sync, are committed locally, and advance Groove normally.

The adapter returns Groove-native inputs. Jazz-specific physical mapping and
decoding live in the adapter, not Groove's evaluator.

## Clarify the synchronous architecture first

These changes should land against the integration base while storage is still
synchronous. Each is independently useful and reduces the eventual async diff.

### 1. Name prepare, publish, persist, and release

For every mutation or ingress family, make these phases visible in types:

```text
prepare       acquire/validate inputs; no visible mutation
publish       one synchronous resident transition
persist       submit one ordered durable unit
release       expose durability-dependent receipts/messages/fates
```

Use operation-specific prepared values, not generic replayable closures:

```rust,ignore
struct PreparedLocalUpdate { /* owned validated inputs */ }
struct PreparedViewIngress { /* owned decoded frame */ }

fn prepare_local_update(...) -> Result<PreparedLocalUpdate, Error>;
fn publish_local_update(prepared: PreparedLocalUpdate) -> Publication;
```

Preparation purity should follow from ownership and visibility, not from a
runtime check that a replayed closure happened not to emit writes.

### 2. Put the residency frontier inside Groove

Introduce an internal Groove result for a complete blocked frontier:

```rust,ignore
enum ResidentAttempt<T> {
    Ready(T),
    Blocked(InputFrontier),
}
```

The evaluator visits independent runnable branches and unions their demands.
Newly loaded inputs may reveal another dependent frontier. Scheduling,
deduplication, hydration, and retry stay in Groove even while loading is still
synchronous.

This establishes the correct ownership and scaling without adding futures,
bindings, or a parallel database owner.

### 3. Separate subscription lifetimes

Define these layers behind the current public protocol:

```rust,ignore
struct GrooveTerminalSession {
    initial: TerminalValue,
    updates: Stream<Result<TerminalPatch, GrooveSessionEnd>>,
}

struct JazzQuerySession {
    initial: RelationSnapshot,
    updates: Stream<Result<JazzPatch, JazzSessionEnd>>,
    status: SubscriptionStatus,
}
```

A Groove session covers one terminal baseline. A Jazz session covers one
continuous public-result baseline and may compose a local terminal with remote
authority input. The stable high-level Jazz subscription reinstalls a Jazz
session after a recoverable end.

For compatibility, a new session's `initial` may initially be encoded as the
existing `reset: true` event. Ordinary low-level updates never reset.

Settlement is Jazz status, separate from data:

```rust,ignore
enum SubscriptionStatus {
    Provisional,
    Settled { through: AuthorityFrontier },
}
```

Settlement changes neither the Groove terminal baseline nor the public value.

### 4. Remove eager replacement-result maintenance

A structured terminal emits its initial value once and then structural patches.
Jazz must not also decode and store a complete nested root on every child
change solely to support a possible later reset.

When a low-level session is reinstalled, materialize its new initial value at
that boundary. Start with this rare O(result size) path. Consider a persistent
structurally shared tree only if measured reinstall latency requires it.

The relation/include incremental canary must remain scale-independent.

### 5. Separate status, reconciliation, and continuity

Authority generation or settlement changes do not inherently invalidate patch
continuity. For a Jazz query session:

- status-only change: publish status, no data work;
- exact visible change: publish patches in the same session;
- discontinuous replacement: end the session and let the high-level owner
  install a new one.

Do not use an empty patch list to mean unchanged, unsupported, reconcile, or
reset. Classify these where enough information remains.

### 6. Preserve public owners during semantic cleanup

Do not begin by deleting synchronous `Db`, `Node`, transaction, test, benchmark,
or binding surfaces. Improve the boundaries inside the existing owners and keep
the integration base green. Do not propagate a repository-wide `&mut Db`
migration while the async semantics are still being discovered.

## Make operations async-ready without suspending

After the synchronous clarifications are green, make state crossing operation
boundaries owned and restart-safe while still using immediate storage.

### 7. Introduce owned storage operations narrowly

Add owned point, range, commit, flush, and close values beside the existing
storage interface. The first consumer should be Groove's capability, not every
Jazz caller.

Keep these distinct:

- logical request value;
- backend request identity;
- backend execution/future;
- terminal success, failure, or cancellation.

Jazz query code must not manage Groove backend request identities.

### 8. Evolve the existing storage/session abstraction

Give the real ordered storage session a submission API returning a non-`Send`
future where browser thread affinity requires it. Do not introduce a second
pollable storage trait and adapt every backend into a parallel hierarchy.

The session owns:

- ordering of conflicting operations;
- read-after-write frontiers;
- cancellation and ambiguous outcomes;
- wakeups;
- poisoning and reopen requirements.

Unrelated reads should not automatically wait for every older durable commit.
Order them against the exact frontier they require.

### 9. Make prepared work fully owned

Prepared operations that may later survive a yield have:

- no borrowed rows, schemas, frames, or scan visitors;
- no visible mutation before publication;
- explicit read-view/frontier identity;
- explicit runtime/session generation where stale completion is possible;
- bounded retained memory with test receipts.

Do not stage complete maintained results when a compact source delta or terminal
patch is the semantic operation.

### 10. Establish stale-work guards

Work that can outlive a poll captures the generation of the owner or low-level
session it targets. Completion against a replacement generation is discarded
before mutation. These are internal guards, not necessarily public epochs.

### 11. Extract behavioral proof suites

Run the same black-box behavior through the evolved existing abstraction with
immediate storage:

- direct write visibility and callback ordering;
- one-shot reads after local writes;
- joins/includes with resident inputs;
- subscription initial/patch/session-end ordering;
- authority status changes without data replacement;
- durable publication exactly once;
- transaction, peer-ingress, and branch atomicity;
- all incremental-delivery scaling canaries.

Mechanism tests may inspect retained memory, load coalescing, or evaluator work,
but correctness should be asserted through public APIs.

## Introduce actual suspension incrementally

Only after the immediate path passes the proof suites should operations be
allowed to return `Pending`.

### 12. Async Groove reads first

Make the storage capability genuinely asynchronous for point and range loads.
Keep Jazz public ownership and persistence otherwise unchanged.

Prove:

- independent missing inputs load as one frontier;
- dependent inputs take frontier rounds, not one miss per retry;
- hash-equal nodes share in-flight loads;
- one slow load does not serialize unrelated resident evaluations;
- cancellation cannot hydrate a replacement runtime;
- Jazz never observes `NotResident`.

This is the first milestone where query evaluation actually suspends.

### 13. Async Groove subscription installation

Open a terminal at a named logical frontier, capture its initial value, and
queue every later patch before returning the session. If initial materialization
loads data, patches remain behind the initial publication barrier.

Do not publish a stream handle or Jazz coverage state until installation
succeeds. A failed installation leaves no retainers or staged graph nodes.

### 14. Async Groove writes and persistence

After reads and terminal installation are stable, make Groove write preparation
and persistence suspendable in the existing `Database`. Preserve one
non-suspending resident publication after preparation. Immediate storage makes
the same operation ready without selecting another type.

### 15. Async Jazz control-plane reads

Convert Jazz-owned storage consumers by narrow domain in the existing `Node`:

1. node-opening control metadata;
2. catalogue/schema and aliases;
3. branch metadata;
4. transaction/history witnesses;
5. settled binding and authorization state;
6. recovery and repair indices.

Each domain gets typed preparation and focused correctness/scaling tests. Do not
wrap arbitrary synchronous `NodeState` closures in a generic retry loop.

### 16. Async Jazz persistence and external release

Submit ordered durable work behind proven resident publication. On success,
release the correct durability-dependent consequences exactly once. On
ambiguous failure, stop dependent publication and reopen coherent durable state.

Keep optimistic visibility and durable/external release separate in code/tests.

### 17. Peer ingress

Treat one decoded inbound frame as the semantic operation:

```text
receive without consuming
prepare owned typed ingress
publish resident state exactly once
persist if required
advance connection/release output exactly once
```

Transport backpressure retains produced output; it never replays ingress.
Convert message families incrementally rather than routing the whole peer tick
through a generic async retry driver.

### 18. Simplify ownership after convergence

If temporary internal adapters remain, fold them into the existing `Database`,
`Node`, and `Db` as each domain converges. This step removes scaffolding; it must
not introduce a new owner hierarchy or define new semantics.

Schema views remain inert identity tokens or short borrows, never parallel
database owners. Constructor differences may describe client, authority, or
catalogue semantics, but not synchronous versus asynchronous completion.

### 19. Bindings and browser backend last

Adapt NAPI, WASM, React Native, server shells, and workers only after Rust owner
and operation contracts stabilize. Preserve callback ordering explicitly.

Land a production browser backend separately. Page-store work, artifact
staging, CI tuning, and engine semantics should not share one implementation
series.

## Subscription recovery and replay

End-to-end incrementality benefits from a resumable Jazz query session:

```rust,ignore
struct PatchCursor {
    session: SessionId,
    sequence: u64,
}
```

On reconnect, resume if the producer can replay from the cursor. Otherwise end
with `PatchHistoryUnavailable`; the high-level subscription installs another
session and publishes its initial value using the compatibility reset encoding.

A transport reconnect alone does not replace a Groove terminal or Jazz query
session. Terminal/runtime replacement ends the Groove session; Jazz ends its
query session only when exact public patch continuity cannot be preserved.

## Suggested landing sequence

1. synchronous prepare/publish types and tests;
2. synchronous Groove residency frontier owned by Groove;
3. low-level/high-level subscription types behind compatibility events;
4. remove eager structured replacement rows and restore scaling canaries;
5. owned request values in the existing storage/session abstraction;
6. owned prepared state and generation guards;
7. async Groove reads;
8. async Groove subscription installation;
9. async Groove writes/persistence;
10. typed Jazz control-plane domains, one family at a time;
11. Jazz persistence/external release;
12. peer-ingress families;
13. delete temporary internal migration seams;
14. bindings;
15. browser backend and end-to-end receipts.

Per step, run focused tests plus every incremental-delivery canary and a
low-seed maintained-vs-one-shot oracle. Before pushing a batch, run the full
canonical gates, benchmark smoke suite, and sensitive-data guard.

## Stop conditions

Stop and redesign the current step if any of these appear:

- a new async/demand-driven wrapper becomes an alternate database or node;
- the existing important abstraction remains usable as a parallel path;
- Jazz catches or interprets a Groove `NotResident` error;
- a generic closure may execute twice without type-level preparation purity;
- ordinary patch delivery rebuilds a complete result;
- unrelated operations share one untyped pending slot;
- a cold read drains all persistence without an explicit frontier dependency;
- a test is weakened solely to accommodate completion timing;
- a binding/backend migration dominates the semantic engine diff;
- the immediate path no longer preserves integration-base behavior;
- a scaling canary exceeds its constant-work band.

The desired result is not an async facade around Jazz. It is the existing Jazz
and Groove architecture, evolved in place with explicit operation and
subscription lifetimes, Groove-owned residency, and Jazz-owned durable and
control-plane scheduling that may suspend without changing semantics.

## Implementation decision log

### 2026-08-20: binding and example configuration parity

- Keep authenticated `DbConfig` inputs mutually exclusive at the type boundary.
  Callers choose the JWT or local-secret arm before constructing the config;
  they do not pass both keys with `undefined` values.
- Permission checks return `allowed`, `denied`, or `unknown`. UI consumers that
  need a boolean deliberately map only `allowed` to `true` rather than erasing
  the distinction in the runtime API.
- The React Native Rust crate remains paused as a root-workspace member, as the
  existing manifest comment specifies. It is explicitly excluded so Cargo can
  build it as the standalone crate used by the RN packaging tool; leaving it
  neither included nor excluded makes every standalone build fail before
  compiling source.
- A host-target check of that standalone crate reaches its source and confirms
  that the RN binding still depends wholesale on the removed pre-engine-swap
  `query_manager`, `runtime_core`, `SyncManager`, and transport APIs. This debt
  is already present on the integration base. Port RN only after defining its
  new binding boundary; do not restore those old subsystems or add a parallel
  runtime path merely to make the package compile in this async-storage PR.
