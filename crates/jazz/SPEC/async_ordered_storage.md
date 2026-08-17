# Async ordered storage and immediate local visibility

_Design experiment. This chapter records the intended boundary and the
invariants the implementation must prove before it becomes normative._

## Goal

`OrderedKvStorage` should be an asynchronous, owned-data boundary. Memory,
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

The eventual `OrderedKvStorage` surface owns every request and result across
suspension:

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

The following storage-touching seams must become pollable while retaining one
deterministic evaluator:

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

The current multi-batch ingest plus separate consistency-marker write should be
collapsed into one owned persistent batch. If a backend cannot support that,
the first batch must contain a durable `finalizing(tx)` record and reopen must
complete or suppress it before serving peers.

### Query-driven node opening

Making storage pollable must not turn the current synchronous reopen procedure
into one asynchronous full-store hydration. A node may await the small durable
control plane required to identify and safely advance its state, but row and
query state is loaded by the operation that asks for it.

The current open path mixes these classes and must be separated:

- catalogue genesis, active lineage payloads, the current schema pointer,
  physical mappings, the node's own alias, clock/global-sequence summaries, and
  clean/consistency markers are startup control state;
- complete node-alias and branch catalogues should be point- or prefix-loaded
  when a referenced identity or branch is used, apart from the small identities
  required to finish opening;
- pending-edge and locally rejected-transaction recovery should be driven by
  the retry/sync operation that consumes those records;
- settled result members, program facts, authorization progress, and known
  state should be loaded for one `BindingViewKey` when that binding is opened;
- ahead-current winners should be loaded for the requested physical table and
  row/range, not rebuilt for every physical table at startup;
- application history/current tables are always query-driven.

Clock and global-watermark reconstruction cannot depend on scanning all
transactions. New writes maintain compact durable high-water summaries in the
same atomic unit as the transaction. Existing stores may perform an explicit,
versioned one-time migration before becoming ready; that migration is not the
steady-state open contract.

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
functions remain ordinary synchronous code. Its initial implementation may
snapshot the resident store; a copy-on-write overlay is an optimization behind
the same boundary. Immediate storage inherits first-poll readiness because all
requested inputs and the final commit complete in that poll.

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

## Binding requirement

Core readiness is not enough if a binding defers delivery. Native bindings
already drain queued subscription events inline after writes. The browser
binding needs an equivalent synchronous drain/notification path for immediate
local events; a `ReadableStream` microtask alone does not prove the application
invariant. Storage- and network-driven events may continue through async streams.

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
