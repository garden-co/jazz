# Async ordered storage and immediate local visibility

_Implemented design experiment. This chapter records both the intended
boundary and the invariants exercised by the implementation while the change
remains under review._

## Implementation status

The experiment now uses the same demand-driven owner from Groove through Jazz:

- owned storage requests may complete immediately or suspend;
- `DemandLoadedStorage` admits only the exact input requested by an operation;
- `StorageTransaction` stages one read-your-own-writes overlay and publishes it
  only after acquisition succeeds;
- `DemandDrivenDatabase`, `DemandDrivenNode`, and `DemandDrivenDb` own the
  resident runtime and ordered persistence scheduler without a legacy parallel
  runtime;
- independently resumable work owns distinct acquisition state: public
  operations, Local-durability promotion, and node opening cannot consume one
  another's pending storage request;
- NAPI and WASM bindings hold one `DemandDrivenDb` owner plus inert typed-view
  tokens; neither binding retains a parallel `Db<MemoryStorage>` or
  `Db<OpfsStorage>` execution path;
- volatile Memory remains `Pending/None` even though its operations complete
  in their first poll, while persistent RocksDB and IndexedDB may advance the
  Local frontier only after their storage commit completes;
- a deterministic pending backend and a real browser IndexedDB backend exercise
  suspension, cancellation, failure, reopen, and immediate resident visibility;
- NAPI and browser-worker receipts assert that tier-none application callbacks
  are published before the corresponding mutation completion is returned to
  application code.

The browser IndexedDB implementation is currently a real boundary/proof backend;
the production browser driver may continue to choose OPFS independently. Backend
selection does not alter the owner, IVM, transaction, or publication model.

## Goal

`AsyncOrderedKvStorage` is an asynchronous, owned-data boundary. Memory,
RocksDB, and already-resident page-cache operations complete on their first
poll; IndexedDB and cold page operations may return `Pending`. Groove and Jazz
have one execution model above that boundary. They do not select a synchronous
or asynchronous core mode.

The application invariant is independent of physical durability:

> Once an application-facing database is ready and a current-state query or
> subscription has opened, committing a local write synchronously updates its
> node-local current view. Subscriptions using tier `None` or
> `localUpdates: immediate` invoke their callbacks before the write operation
> yields back to application code. A subsequent current-state one-shot read
> completes without persistence or network I/O and includes that write.

Initial opening may await storage hydration. Historical, Edge, Global, and
previously unhydrated server-side reads may also await. This permits loaders,
Suspense, and skeleton states during acquisition while preserving synchronous
controlled-input updates after readiness.

### Residently decidable visibility

The synchronous guarantee covers the directly written rows and every derived
result whose dependencies are already resident. It does not require the core
to invent absence for an unfilled cache or synchronously fetch a newly
referenced row:

- changing `post.title` synchronously updates an opened direct `posts` query;
- changing `post.author_id` synchronously updates an include or join when that
  author and the relevant query sources are already resident;
- when the new author is not resident, the direct post remains immediately
  visible but the include/join may remain pending until its exact dependency is
  fetched;
- an opened query therefore inherits synchronous updates only to the extent
  that the update is decidable from its admitted resident working set.

This is a weakening only for newly introduced, unloaded dependencies. It is
not permission to delay a direct written row, to treat an unknown cache entry
as missing, or to make an already-resident join asynchronous.

## One core, inherited readiness

The storage contract is pollable everywhere. An immediate backend satisfies the
same contract by returning `Ready` on the first poll. This property propagates:

- a memory-backed operation can finish in the caller's first poll;
- RocksDB can normally do the same;
- a warm browser page cache can do the same;
- a cold IndexedDB page request suspends the same operation without selecting a
  different query engine, transaction model, or IVM implementation.

Bindings may eagerly poll an operation once. If it completes, they drain local
subscription events inline. If it suspends, they return a promise and resume the
same owned operation later. The semantic phases and outputs are identical.

The Rust names reflect those roles:

- `OrderedKvStorage` is the synchronous interface Groove evaluates against. It
  means “this key range has already been admitted,” not “this is the durable
  backend.”
- `async_ordered::AsyncOrderedKvStorage` is the owned-request durable boundary.
- `ImmediateStorage<S>` adapts Memory, RocksDB, or another reopenable resident
  implementation to that boundary and completes every operation in its first
  poll.
- `DemandLoadedStorage` is the resident cache and operation journal used when
  the durable boundary can suspend.

Column-family/layout expansion is itself an ordered storage operation. It runs
before the first commit that can mention the new family. RocksDB performs its
reopen synchronously through `ImmediateStorage`; IndexedDB acknowledges it
immediately because family identity is encoded into ordered key prefixes.

## Visibility is not durability

Jazz already separates transaction fate from durability. A memory client authors
a committed write as `Pending/None`: it is visible to local reads but has not
reached a durable peer. `Local`, `Edge`, and `Global` are later frontiers.

The asynchronous runtime therefore distinguishes two publication domains:

1. **Local optimistic publication.** The resident application view and its
   immediate subscriptions observe a logically committed `Pending/None` write.
2. **Durable/external publication.** `Local` receipts, authority-backed views,
   fates, upstream forwarding, and edge/core broadcasts remain withheld until
   the complete persistent boundary succeeds.

`INV-TX-25` continues to govern the second domain. It must not be interpreted as
forbidding the first: an in-memory originating client already observes its own
write before a persistent worker receives it.

## Storage contract

The `AsyncOrderedKvStorage` surface owns every request and result across suspension:

- point reads own their column-family name and key;
- scans return owned key/value chunks and an owned continuation identity;
- atomic writes take `Vec<OwnedWriteOperation>`;
- flush, close, reopen, family discovery, and optional metering are pollable;
- browser futures may be thread-affine and therefore need not be `Send`;
- one storage session serializes requests whose outcomes would otherwise be
  ambiguous.

Borrowed scan visitors cannot cross this boundary. A scan continuation belongs
to the storage scheduler, is valid only for its exact request generation, and
cannot be resumed after ambiguous cancellation or worker replacement.

The async page store remains below Jazz's B-tree. IndexedDB and OPFS store
opaque pages; the B-tree owns key ordering, descent, splitting, scans, cache
eviction, and page encoding.

## Commit phases

One operation moves through four explicit phases:

1. **Prepare.** Await missing source, parent, winner, policy, or schema data and
   assemble owned logical and storage operations. Open transaction writes remain
   invisible.
2. **Local visibility commit.** Install the transaction in the resident local
   frontier, run the maintained local IVM tick, queue local deltas, and drain
   immediate callbacks synchronously. The transaction is `Pending/None`.
3. **Persistent commit.** Submit the complete owned atomic batch. No durable or
   external consequence is released while it is pending.
4. **Durable publication.** On success, advance the applicable durability
   frontier and release permitted receipts, uploads, views, and fates exactly
   once.

For an in-memory application client, phase 3 is performed by the persistent
peer receiving its `CommitUnit`. For a directly persistent application node,
the same node retains its resident local frontier while its backend operation is
pending. This is topology, not a second core mode.

## Groove implications

The following storage-touching seams use the pollable boundary while retaining
one deterministic evaluator:

- `LayoutStorage`, typed `RecordStore`, storage transactions, staged overlays,
  metering, reopen, close, and flush;
- database opening and schema/storage-layout validation;
- old-row lookup, duplicate checks, delta computation, unique-index probes,
  and atomic base/index/view commits;
- source and durable-index hydration;
- `Persist` operators and durable arrangement hydration;
- prepared-shape binding, subscription opening snapshots, and binding
  retractions;
- final write failure poisoning and staged notification ownership.

Incremental filters, joins, aggregates, recursion, arrangements, and terminal
assembly remain one IVM. They should run synchronously once their required
sources are resident. Arbitrary evaluator stack frames must not retain borrows
across a page load; hydration/acquisition is the async edge around the evaluator.

## Jazz implications

Pollable storage affects:

- node opening and recovery of aliases, catalogues, mappings, branches,
  histories, current indices, settled views, rejection state, and markers;
- local parent/winner lookup, upsert merging, open transaction snapshots,
  commit-unit construction, and exclusive read/write validation;
- inbound `CommitUnit` staging, current/merge-head maintenance, fate/global-seq
  finalization, rejection cleanup, and consistency markers;
- local, settled, branch, relation, historical, and time-travel reads;
- maintained-view hydration, version-witness lookup, known-state repair, and
  subscription refresh;
- peer tick scheduling, outbound-message ownership, eviction, shutdown, and
  persistence-error recovery.

Jazz collects every resident journal produced while applying one typed ingest
operation—including canonical history, currency maintenance, the node
checkpoint, and the consistency marker—into one owned persistent batch. There
is no second durable finalization state for a backend or reopen path to
reconcile.

### Query-driven node opening

Making storage pollable must not turn the current synchronous reopen procedure
into one asynchronous full-store hydration. A node may await the small durable
control plane required to identify and safely advance its state, but row and
query state is loaded by the operation that asks for it.

The open path separates these classes:

- catalogue genesis, active lineage payloads, the current schema pointer,
  physical mappings, the node's own alias, clock/global-sequence summaries, and
  clean/consistency markers are startup control state;
- complete node-alias and branch catalogues are point- or prefix-loaded
  when a referenced identity or branch is used, apart from the small identities
  required to finish opening;
- pending-edge and locally rejected-transaction recovery is driven by
  the retry/sync operation that consumes those records. Pending cascades use a
  parent-keyed durable index: rejecting one parent loads only that parent's
  children, then follows the same index recursively. An empty in-memory child
  map before that lookup means “not admitted”, not “durably empty”;
- settled result members, program facts, authorization progress, and known
  state are loaded for one `BindingViewKey` when that binding is opened;
- ahead-current winners are loaded for the requested physical table and
  row/range, not rebuilt for every physical table at startup;
- application history/current tables are always query-driven.

Clock and global-watermark reconstruction cannot depend on scanning all
transactions. New writes maintain compact durable high-water summaries in the
same atomic unit as the transaction. Existing stores may perform an explicit,
versioned one-time migration before becoming ready; that migration is not the
steady-state open contract.

The compact node checkpoint is authoritative for the clock and admitted global
sequence frontier, and is keyed by node identity. It contains no application
rows or query results. Every Jazz-owned durable batch updates it atomically with
the records whose clocks or sequence admission it summarizes. A current store
therefore opens with one exact checkpoint read; only a legacy store without the
checkpoint scans transaction/history indexes and installs the checkpoint on its
next Jazz-owned batch.

Removing those scans also removes their accidental role as a full-history
integrity audit. Current write paths validate records before committing them,
checkpoint decoding validates its own structure and monotonic frontier, and
individual historical records are validated when demand-loaded. Out-of-band
storage inspection belongs in an explicit integrity/audit operation rather
than making every ordinary node open proportional to all durable history.

Node opening itself is a pollable state machine built from restartable resident
storage transactions. An attempt sees one admitted working set, isolates every
write it makes, and either:

- reports one exact missing durable input and is discarded;
- fails without publishing state; or
- succeeds, becomes the node's resident state, and submits its complete metadata
  write set as one durable unit before readiness is published.

This is transaction retry, not mutationful constructor replay: failed attempts
cannot affect the admitted cache, the future node, subscriptions, or durable
storage. The transaction abstraction owns this rule so individual recovery
functions remain ordinary synchronous code. The implementation uses a
storage-shaped read-your-own-writes overlay: it stages only touched encoded keys
and reads untouched state from the resident base. Immediate storage inherits
first-poll readiness because all requested inputs and the final commit complete
in that poll.

The ready value owns the resident `NodeState`, admitted cache, durable storage
session, acquisition state, and ordered persistence queue together. Returning a
bare resident node and dropping the backend after opening is invalid: later cold
reads would have no acquisition owner, and later writes could not preserve their
durable ordering. The resident storage transaction journals every actual
base/index/IVM set or delete made by one successful Jazz operation and emits that
complete journal as one atomic durable unit; a separate higher-level batch list
must not become a partial second source of truth. Immediate backends drain all
queued units in the same poll; pending backends retain the oldest request and
never let a later unit overtake it. A failed unit poisons the owner and releases
no later durable work until clean reopen.

The high-level `Db` façade and this owner share the same resident `NodeState`.
Owner-level reads and subscription openings rerun their ordinary synchronous
resident operation after admitting an exact missing input; they do not evaluate
against a second database. A warm read or opening is therefore first-poll ready.
A cold subscription attempt uses Groove's transactional graph lifecycle: a
missing input removes the attempted subscription retainers and staged graph
nodes before suspension, and Jazz publishes its stream/coverage state only
after the resident opening succeeds. The resumed opening installs exactly one
real subscription and queues its initial reset before returning `Ready`.

### Local operation phases

A local mutation is one operation with two explicit phases, not a synchronous
and an asynchronous implementation mode:

1. **prepare/acquire** performs every durable-backed lookup that can influence
   the mutation. It may suspend and retry, but cannot advance clocks, publish an
   IVM delta, install a subscription, or emit a durable write;
2. **resident publish** applies the prepared mutation to canonical resident
   state and the IVM without suspension, then hands its complete journal to the
   ordered persistence queue.

Preparation must not clone the IVM into a disposable runtime or evaluate the
same graph once to discover storage inputs and again to publish. Storage
acquisition and IVM evaluation are separate boundaries: acquisition resolves
the durable inputs named by the operation and retained graph, then Groove
builds one transactional IVM transition against the real runtime. A failed
preparation discards only that transition; a successful transition is applied
exactly once during resident publish. The transaction should journal or stage
only state touched by the tick, not copy the graph, subscriptions, and unrelated
operator state.

`DemandDrivenNode::poll_local_operation` is the orchestration seam;
operation-specific entry points such as `poll_mergeable_commit` own their
typed preparation. With Memory/RocksDB, acquisition completes in the first
poll. With IndexedDB, a cold dependency can yield `Pending`, but the eventual
publish and its local subscription callbacks still occur in one synchronous
poll. Encountering a new cold input during publish is an invariant violation
and poisons the resident runtime rather than exposing a partial mutation.

### Peer ingress operations

A peer tick is a driver, not an atomic storage operation. Transport receive,
repair bookkeeping, authenticated-link state, resident mutation, durability,
and outbound delivery currently happen in one loop, but an asynchronous store
must not make that whole loop replayable. A cold miss after an inbound frame
has changed any of those domains would otherwise duplicate a request, lose a
frame, or expose a partially applied message.

The durable owner therefore stages each inbound frame under its connection and
drives one typed ingress operation through these phases:

1. **classify and preflight** validates the authenticated envelope and acquires
   every durable-backed input used by that message without consuming the
   frame, advancing connection cursors, mutating the resident node, or sending
   transport output;
2. **resident publish** applies the prepared message exactly once. Commit and
   fate messages use the same prepared authority/relay and publication-scope
   machinery as direct owner calls; view, catalogue, branch, and repair
   messages have their own typed preparation rather than a generic replayable
   `apply_sync_message` closure;
3. **durable/external release** commits the complete resident journal where the
   message changes durable Jazz state, then atomically marks the staged frame
   consumed and releases its receipts, routed fates, subscription events, and
   outbound responses.

Pure connection-control messages may complete without a storage commit, but
they still advance their connection state only once. Several frames may be
batched when their prepared operations and release ordering are explicit; the
semantic unit remains the staged frame, not an arbitrarily replayed tick.
Transport backpressure retains an already-produced outbound frame and never
reapplies its resident ingress operation. A persistence failure poisons the
owner before any durable/external release, and reconnect recovery starts from
the last coherent durable frame boundary.

`NodeState::apply_sync_message` remains the synchronous resident publication
surface for fully resident callers and tests. The asynchronous peer driver is
responsible for selecting a typed preparation before invoking it; a generic
`poll_operation(|| apply_sync_message(...))` is forbidden because many message
handlers intentionally update more than resident storage and are not replay
safe.

## Resident current-state requirement

An overlay containing only the newest row is insufficient for an immediate new
one-shot query: evaluating that query may also require older matching rows,
policy dependencies, or join sources. An application-facing ready database
therefore keeps its admitted local current-source closure and maintained
operator state resident.

This is bounded by Jazz's query-driven partial-sync model; it does not require a
client to retain complete global history. Historical and server-side cold reads
remain asynchronous.

## Failure and ordering

Once an optimistic callback has run, a persistence failure must not silently
roll the UI backward. The persistence owner instead fails closed:

- reject durability waits and report the storage/mutation error;
- release no fate, authority view, receipt, or forwarded commit;
- accept no later writes whose durable ordering depends on the unknown result;
- poison/close the affected persistent runtime;
- recover by reopening from coherent durable state.

Later queued commits cannot overtake an unresolved earlier commit. Cancellation
terminalizes the in-memory request before attempting backend abort; ambiguous
cancellation is recovered from durable state rather than retried under the old
request identity.

Recovery never tries to repair or roll back the poisoned resident runtime. The
owner is discarded, the binding opens a fresh backend session, and ordinary
query-driven node opening reconstructs the last coherent durable state. A
backend may reuse the same underlying database, but it must not reuse an
ambiguous request identity or the old resident node. Thus an optimistic row
whose persistent commit failed remains visible only until the owner reports the
failure; it is absent after clean reopen unless the backend proves that exact
commit durable.

Lazy physical schema is part of the same prepare/publish discipline. A first
branch write validates prospective Groove table registrations and prepares its
row batch against that prospective schema without changing the live runtime.
The resolving poll installs those table descriptors and publishes the durable
partition marker, transaction, and first row as one resident operation and one
persistence unit. An authorized maintained subscription may install an empty
process-local source earlier, but that source is not durable branch metadata
and denied subscriptions must not allocate it.

## Binding requirement

Core readiness is not enough if a binding defers delivery. Native bindings
drain queued subscription events inline before a mutation method returns. A
browser worker necessarily crosses an asynchronous message boundary, so its
equivalent contract is ordering: a tier-none mutation completion cannot reach
application code before the corresponding immediate subscription callback.
Storage- and network-driven events may continue through async streams.

### One owner, many typed views

Bindings must not model a typed schema view as another database owner. There is
exactly one durable scheduler, persistence queue, peer set, row-id source, and
logical clock for an opened database. Registering or attaching a schema yields
an inert, cloneable view token containing only the validated schema/view
identity. Reads, writes, transactions, subscriptions, and peer operations pass
that token back to the unique owner.

The owner may construct a short-lived resident `Db<DemandLoadedStorage>` view
internally to reuse validation and materialization code, but that facade never
escapes and never owns persistence. A binding therefore stores:

- one shared mutable `DemandDrivenDb` owner;
- one immutable typed-view token per public database handle;
- write, transaction, subscription, and transport handles tied back to that
  same owner.

Schema registration is itself an owner operation. If the schema is already
authority-admitted, registration only installs process-local typed metadata. If
it requires local catalogue admission, the owner publishes that catalogue
transition through the same ordered durable boundary before returning the
token. Dropping a view cannot close or flush the owner; only the handle that
owns the runtime lifetime may do so.

## Required proof matrix

The same behavioral suite must run against:

1. an immediate memory backend;
2. an immediate native/RocksDB backend;
3. a deterministic simulated backend that can pend each read, scan, commit,
   cancellation, flush, and failure boundary;
4. a real IndexedDB-backed B-tree in a browser worker.

The suite must prove:

- cold initial hydration may pend, but a ready local write performs no storage
  I/O before its immediate callback;
- a new current-state one-shot sees the write immediately;
- `Local` wait and all external publication remain pending until persistence;
- success releases durable consequences exactly once;
- failure publishes nothing externally, poisons the persistence owner, and
  reopens to coherent durable state;
- queued commits preserve order and cannot race an ambiguous predecessor;
- multi-row and exclusive commits have one local visibility transition;
- Edge/Global subscriptions never mistake optimistic rows for settled rows;
- dropped/cancelled page and write requests cannot complete into replacement
  operations;
- NAPI and WASM callback timing is asserted behaviorally, not inferred from an
  eventually delivered stream.
