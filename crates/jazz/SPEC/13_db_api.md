# jazz — Specification · 13. The high-level `Db` API

## Overview

`Db<S>` is the product-facing, runtime-typed API that applications and language
bindings call. It presents the local database as a small client facade: apps open
a database, read materialized query results, subscribe to query changes, and
submit mutations; bindings attach transports and drive synchronization. This
chapter depends on
the model established in the preceding chapters, but app builders reading
non-sequentially can start here (ch. 1, §1.1).

Invariant digest:

- `INV-API-1`: Db MUST be the high-level runtime-typed client facade, exposing the application API and connection-driving surface without introducing different application or sync semantics; it MUST validate user Query values before executing reads or subscriptions.
- `INV-API-2`: Db::open MUST construct a non-history-complete client. The history-complete opening path is reserved for core/Node use and MUST NOT make the ordinary application facade a fate authority.
- `INV-API-3`: `Db::read` and `Db::one` MUST be synchronous local reads and MUST NOT wait for upstream sync; `Db::all` MUST use `ReadOpts` to choose the effective durability tier.
- `INV-API-4`: When `ReadOpts.local_updates == LocalUpdates::Immediate`, the effective read tier MUST be at least `DurabilityTier::Local`; when it is `Deferred`, the effective read tier MUST be exactly `ReadOpts.tier`.
- `INV-API-5`: `ReadOpts::default()` MUST be `{ tier: DurabilityTier::Local, local_updates: LocalUpdates::Immediate, propagation: Propagation::Full }`.
- `INV-API-6`: `Db::subscribe` MUST support live subscriptions at the requested effective tier. Local subscriptions are first-class application-facing subscriptions that include the node's own pending committed writes and MUST publish their truthful node-local opening, including an empty opening, even when `Propagation::Full` concurrently requests upstream coverage; propagation does not raise the requested observation tier. Edge/global subscriptions apply the same query semantics over their accepted-state frontiers and MUST withhold an empty opening until it is authority-backed. The target implementation is maintained subscription views for every tier; until local maintained views are fully unified with the edge/global path, local effective-tier subscriptions MAY serve alpha-style local live reads from an explicitly named local materialized-row bridge. No tier may introduce a second facade-side query engine as the target semantics.
- `INV-API-7`: Subscription streams MUST expose maintained-view opened/reset/delta
  events and MUST NOT queue facade-side full-result diffs as the normal live
  subscription mechanism.
- `INV-API-8`: `Db::insert` MUST generate the row id using its configured `RowIdSource`; `Db::insert_with_id` MUST use the caller-supplied `RowUuid`.
- `INV-API-9`: `Db::update` MUST preserve omitted fields for a locally present row by merging the patch over the row's current local cells.
- `INV-API-10`: `Db::upsert` MUST merge supplied cells over current cells when the row exists locally and MUST write supplied cells directly when the row does not exist locally. Through a branch-view `WriteTarget`, it MUST merge a head-local row, copy an inherited base row into the head without a cross-branch parent, or insert into the head when the row is absent from the full view. A head-local deletion winner that hides the row MUST instead reject the upsert with `ErrorCode::WriteRejected`; upsert MUST NOT write tombstone-hidden content or implicitly restore the deletion register. Standalone and mergeable-transaction upserts MUST use the same rule.
- `INV-API-11`: `Db::delete` MUST lower to a mergeable commit with `DeletionEvent::Deleted` and make the row absent from current reads after local application.
- `INV-API-12`: `Db::restore` MUST reject empty cell data with `ErrorCode::Schema` and MUST lower a non-empty restore to content write plus `DeletionEvent::Restored`.
- `INV-API-13`: Every local write method MUST return a `WriteHandle` carrying the affected `RowUuid`, backing `TxId`, and local durability tier.
- `INV-API-14`: A local write on a `Db` MUST be `DurabilityTier::Local` and queued for upstream upload; a `Db` (always a client) MUST NOT self-finalize. Self-finalization to `Accepted`/`Global` is a core `Node` behavior (ch. 9).
- `INV-API-15`: `WriteHandle::wait(tier)` MUST return the handle `TxId` only when the requested tier is locally satisfied, MUST return `ErrorCode::WriteRejected` for rejected fates, and MUST return `ErrorCode::NotObserved` when the requested tier is not locally observed. A `Global` wait additionally MUST require `Fate::Accepted` and an authority-assigned `GlobalTime`; a bare `Global` durability claim MUST NOT complete it.
- `INV-API-16`: `Transport` implementations MUST be non-blocking; `try_recv() == None` MUST mean no inbound message is currently staged and MUST NOT be interpreted by `Db` as disconnect.
- `INV-API-17`: Db::connectupstream MUST make every already-registered facade subscription eligible for immediate upstream announcement without requiring re-registration.
- `INV-API-18`: `Db::subscribe` MUST announce newly registered subscriptions to all existing upstream connections so query-driven sync can request remote completion on the next tick.
- `INV-API-31`: `Db::disconnect` MUST mark the `Db` intentionally offline, disconnect every schema client from its server transport, and leave the local runtime and store alive; `Db::reconnect` MUST clear that marker and reconnect every schema client. A schema client created while intentionally offline MUST remain offline until `reconnect`.
- `INV-API-32`: `ReadOpts.tier` selects the sufficient materialized knowledge and first-result gate; `Propagation` only controls whether evaluation or coverage may be forwarded upstream and MUST NOT change local-tier result semantics. Thus a `Local` read resolves from current local materialized state even with `Propagation::Full`: a locally committed pending write is returned, while a row written remotely but not yet delivered locally is absent. `LocalOnly` prevents upstream routing; it is not what makes a `Local` read local.
- `INV-API-19`: Upstream announcement of a subscription MUST make its query definition available before the subscription that uses it, without re-announcing the same definition for that connection.
- `INV-API-20`: An upstream connection MUST upload each locally-authored transaction at most once.
- `INV-API-21`: A subscriber `PeerConnection::tick` MUST serve subscriptions under the `AuthorSubject` passed to `Node::accept_subscriber`, not under the serving node's own identity.
- `INV-API-22`: Db::tick() MUST service every registered connection exactly once.
- `INV-API-23`: A client binding tick driver MUST classify `Db::tick()` failures. A recoverable protocol condition MUST NOT terminate the driver; the driver MUST continue through its documented repair or reconnect path with bounded backoff. A fatal failure, or exhausted recovery, MUST stop the driver and be surfaced to the caller as an error rather than appearing as a stalled sync operation.
- `INV-API-24`: The query builder exposed through Db::table MUST expose the schema-validated query construction capabilities defined in ch. 6.
- `INV-API-26`: `Db::mergeable_tx()` MUST group multiple facade writes under one mergeable `TxId`, and the produced commit unit MUST set `Transaction.n_total_writes` to the number of grouped versions.
- `INV-API-27`: `Db::exclusive_tx()` MUST expose serializable exclusive transactions on the facade, preserving snapshot reads and returning `WriteRejected` when authority validation detects a conflict.
- `INV-API-28`: Permission advice is a three-valued, authority-scoped dry run: only the serving authority may issue definitive `Allowed`/`Denied`; client-local, offline, incomplete, not-ready, and timed-out requests yield `Unknown`. Advice is non-mutating and does not reserve a later mutation; its authenticated request/response exchange exposes no policy evidence and is correlation-, cancellation-, replay-, and dedup-safe.
- `INV-API-33`: Ordinary `Db` reads and subscriptions MUST use client-local lowering: policy is enforced by the trusted upstream before emission and is never re-applied to received rows. Local/None reads scan locally available data; Edge/Global settled reads consume the identity-scoped settled view received upstream.
- `INV-API-29`: A `Db` is a client: facade writes MUST keep `permission_subject == made_by`, and a `Db` MUST reject any attempt to attribute a write to another author. Cross-author attribution is a node-level concern on the ingest side (a trusted serving `Node`, `INV-RLS-18`, ch. 9), never a `Db` capability.
- `INV-API-30`: Reopening persistent storage with the same `DbIdentity` MUST schedule every locally originated transaction that reached `Local` durability and has not reached terminal settlement for upstream delivery. Locally originated means `TxId.node == DbIdentity.node` and `Transaction.made_by == DbIdentity.author`; delivery is at-least-once by `TxId` and relies on idempotent authority handling.
- `INV-API-34`: An edge outbox MUST retain an edge-accepted upload until an authenticated terminal rejection or an `Accepted` receipt carrying both Global durability and an authority-assigned `GlobalTime` for that `TxId` arrives directly from the currently admitted upstream fate authority; a featureless/unnegotiated link, local acceptance, hydrated state, staged/replayed updates, and receipts from detached or superseded authorities MUST NOT release it.
- `INV-SYNC-30`: A fresh `Edge`/`Global` settled one-shot read MUST obtain settled authority coverage for its exact current usage-site subscription; an update for a detached predecessor MUST NOT satisfy it even when shape, binding, and options are equal. This freshness rule MUST NOT change local-read semantics or prevent reuse of still-live maintained subscription coverage.

  A durable browser relay is the narrow topology exception to the exact-node
  recovery test: it also schedules unsettled transactions made by the same
  canonical author that it durably accepted from its paired main-tab client,
  whose node intentionally differs from the worker node. This relay exception
  does not authorize general same-author recovery by ordinary databases and
  does not weaken the exact node-and-author definition above outside the paired
  browser client/worker boundary.

  A terminal server or protocol transport failure is not an authority fate. A
  durable browser worker MUST relay it only to currently initialized foreground
  peers so their active Edge/Global waits and remote subscriptions reject with
  that transport error; Local durability remains valid. The worker MUST NOT
  fabricate `Rejected`, roll back local data, invoke `onMutationError`, or
  replay that transient foreground error to a peer attached later.

- `INV-API-35`: Once a local mutation is durably persisted or its ordered publication is owned by the node runtime, the mutation API MUST return its committed `WriteHandle`/`TxId`; a later resident-subscription refresh failure MUST be emitted through the subscription error channel and MUST NOT be returned as a generic mutation or peer-ingest failure.

## Details

### 13.1 Two audiences

The facade separates application concerns from synchronization concerns. An app
consumer works with the mutation API and the query subscription API, with no sync
vocabulary in ordinary application code. A binding author supplies the transport,
wires peer connections, and drives `tick` (§13.5).

**Quickstart.** The complete app-consumer flow — define a schema, open, write,
read, and subscribe — is shown here using the `todos` example:

```rust
use std::collections::BTreeMap;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, RowCells, SeededRowIdSource};
use jazz::groove::{records::Value, storage::MemoryStorage};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

let source = SchemaBuilder::new()
    .table(
        TableSchemaBuilder::new("todos")
            .column("title", ColumnType::Text)
            .column("done", ColumnType::Boolean),
    )
    .build();
let schema = JazzSchema::new(&source)?;
let storage = MemoryStorage::new(
    &schema.column_families().iter().map(String::as_str).collect::<Vec<_>>(),
);

// A `Db` is always a client. A local-first single-process app is just a client
// with no upstream: its writes stay at the `Local` tier (durable on disk) and it
// never needs `Global`. To sync later it connects an upstream and drives tick()
// (§13.5); its backlog uploads and settles — no API change, no role.
let db = jazz::block_on(Db::open(DbConfig {
    schema, storage,
    identity: DbIdentity {
        node: NodeUuid::from_bytes([0x11; 16]),
        author: AuthorSubject::for_test_bytes([0xa1; 16]),
    },
    id_source: Some(Box::new(SeededRowIdSource::new(0x1111))),
}))?;

let cells: RowCells = BTreeMap::from([
    ("title".into(), Value::String("buy milk".into())),
    ("done".into(),  Value::Bool(false)),
]);

// write — returns a handle; a client write settles at `Local` (an upstream would
// later carry it to `Global`). With no upstream, wait(`Global`) never completes.
let h = db.insert("todos", cells)?;
let id = h.row_uuid();
jazz::block_on(h.wait(DurabilityTier::Local))?;

// read — query is immutable/chainable, validated against the schema (ch. 6)
let q = db.table("todos").select(["title", "done"]);
let rows = jazz::block_on(db.all(&q, ReadOpts::default()))?;   // Local tier by default

// watch — conflated handle: current() + changed()
let watch = jazz::block_on(db.subscribe(&q, ReadOpts::default()))?;

// update merges over current cells; omitted columns keep their value
let patch: RowCells = BTreeMap::from([("done".into(), Value::Bool(true))]);
jazz::block_on(db.update("todos", id, patch)?.wait(DurabilityTier::Local))?;
jazz::block_on(db.delete("todos", id)?.wait(DurabilityTier::Local))?;
```

The application surface is exactly the set used above: `open`, the mutation
methods (§13.4), and the query/subscription methods (§13.3). Synchronization is
added by handing the same `Db` to a binding that wires a `Transport` and calls
`tick` (§13.5); the application read, subscribe, and write code does not change
when an upstream is attached.

### 13.2 Opening a `Db`

Opening a database binds a schema, storage backend, identity, and row-id source
into one client facade. `Db::open(DbConfig<S>)` is async and takes a
`JazzSchema`, storage, a `DbIdentity { node, author }`, and an optional
`RowIdSource`. It opens an ordinary non-history-complete client node; the facade
does not choose a topology role (`INV-API-2`, ch. 9). Row ids come either from
`ProductionRowIdSource` (uuidv7) or from `SeededRowIdSource` (deterministic, for
tests/DST). The core exposes simple `block_on` helpers so the async calls in this
surface (`Db::open`, `WriteHandle::wait`, and watch handles) can be used from a
plain `fn main` without a hand-rolled executor.

**The `Db` facade is the client-side application API only.** A `Db` has partial
history, uploads its writes to an upstream, never self-finalizes, and has no fate
authority. The server-side tiers — **core**, **edge**, and **relay** — are not
`Db` roles. They are operated at the `Node` level: a core is a `Node` over a
history-complete `NodeState` that self-finalizes via `finalize_*`; an edge is a
`PeerRole::ClientLink` link; and a relay is a `PeerRole::Relay` link (ch. 9,
appendix E). Keeping non-client topology at the `Node` layer preserves one
vocabulary for sync roles while leaving the app facade small.

The layering is: `NodeState` is the local engine; `Node` is the sync participant
that owns a `NodeState`, all upstream and downstream connections, and the serving
surface; and `Db` is the client wrapper over a `Node` that exposes the
application API while delegating connection setup and `tick` to that node.

Local-first single-process apps require no special mode. A standalone app is a
client with no upstream: its writes settle at the `Local` tier, durable on disk,
and it never needs `Global`. If the app later connects an upstream, the same
client uploads its backlog and settles through the ordinary client path with no
API change. It is its own "authority" only in the trivial sense that there is no
one else; there is no separate role for it.

### 13.3 Reads and subscriptions

Reads start from a schema-validated query builder. `Db::table(name)` returns a
runtime-typed `Query` (ch. 6) with `filter`, `join_via`, `reachable_via`,
`include`/`include_with`, `select`, `order_by`, aggregate helpers, `limit`, and
`offset`; the query is validated against the schema before execution
(`INV-API-1`, `INV-API-24`). Query builders are **immutable and chainable**:
each builder call returns a new query. Runtime schema errors are part of the
product contract: every validation error names what was found, what was
expected, and the nearest valid alternative, such as an unknown-column
suggestion, an expected/got type mismatch, or candidate table names for an
unknown table.

The facade offers both immediate local reads and durability-aware async reads.
`Db::read` returns all matching rows and `Db::one` returns the first row, or
none; both are **synchronous local reads** and never wait on upstream.
`Db::all(query, ReadOpts)` is async and chooses the effective durability tier
(`INV-API-3`). `ReadOpts` carries `tier`, `local_updates`, and `propagation`,
defaulting to `{ Local, Immediate, Full }`. `Immediate` local updates raise the
effective tier to at least `Local` (`INV-API-4`, `INV-API-5`), and `propagation`
is an advanced knob that application code rarely changes from `Full`.

Include payload breadth is not configurable: reads and subscriptions expose
matched include paths only. Alpha-style `requireIncludes()` maps to required
include match semantics, not to broader traversed/failed-path payload material.

Which `tier` to choose:

| `ReadOpts.tier`   | use it for                                     | sees                                                                                                                                   |
| ----------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `Local` (default) | optimistic UI, read-your-writes                | local currency, including your own pending committed writes                                                                            |
| `Global`          | confirmed server-accepted state                | only globally-accepted versions                                                                                                        |
| `Edge`            | edge-accepted state (between local and global) | versions an edge has finally judged (`Fate::Accepted` at `DurabilityTier::Edge`), excluding purely-local pending writes (ch. 5, ch. 9) |

Freshness is expressed by the requested tier. A `Local` read includes the
client's own optimistic writes immediately. A `Global` read shows accepted state
only after that state has been observed locally through synchronization (§13.5);
until then, the local view may be empty. Reads do not perform an implicit network
wait.

Repeated settled reads require a freshness proof, not merely a locally
materialized result from an earlier request. Each newly initiated `Edge` or
`Global` one-shot owns a fresh usage-site subscription and waits for settled
authority coverage addressed to that exact subscription. A late update for a
detached predecessor cannot satisfy the new read, even when its shape, binding,
and options are identical. Synchronous and local-tier reads retain their local
semantics, and still-live maintained subscriptions may continue sharing their
coverage group (`INV-SYNC-30`, ch. 8 and ch. 16).

`Db::subscribe(query, opts)` opens a live subscription at the requested effective
tier. `Local` subscriptions are first-class application-facing subscriptions:
they include the node's own pending committed writes and must be able to drive
synchronous local UI state after a local write. `Edge` and `Global`
subscriptions use the same query semantics, but their source/frontier and first
settlement/completeness rules are constrained to edge- or global-accepted data.

All live subscriptions use one maintained subscription mechanism, differing only
in read frontier, source resolution, and settlement semantics. The facade must
not grow a second query engine by rerunning `query_rows` and diffing full results
as its normal live-subscription mechanism. Subscription delivery is a thin event
bridge over the core subscription surface: it carries opened, reset, and delta
events, rather than facade-side diffs of full result sets (`INV-API-7`, and
`groove/SPEC/INVARIANTS.md::INV-INC-1` for the mechanism law it serves).

#### Binding read choices

`DurabilityTier` remains the protocol/core lattice and the write-settlement API.
Bindings expose the separate, read-only `ReadTier` vocabulary:

| `ReadTier`         | binding behavior                                                                                           | core lowering                                                                       |
| ------------------ | ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `LocalFirst`       | return/evaluate local knowledge                                                                            | `DurabilityTier::Local`                                                             |
| `Remote`           | wait for the ordinary remote/edge view                                                                     | legacy remote durability tier                                                       |
| `RemoteIfPossible` | use local knowledge only after an application explicitly disconnects; otherwise wait exactly like `Remote` | local only for that explicit-offline start, otherwise legacy remote durability tier |

`RemoteIfPossible` does **not** infer offline state from a timeout, connection
error, slow response, or an ordinary transport reconnect. A one-shot read
chooses once. A subscription that starts while explicitly offline starts local,
then atomically replaces that local native subscription with the remote one on
reconnect; it never creates a second query path or replays a historical remote
failure. Low-level `ReadOpts` and the legacy binding entrypoints still accept
`DurabilityTier` unchanged during the migration. The native Rust facade has no
public explicit-offline toggle, so its `RemoteIfPossible` is strict `Remote`
until such a host boundary exists.

Subscription finalization is also asynchronous ownership work. Dropping a
stream MUST synchronously enqueue one idempotent finalization command without
borrowing the storage-owning node; the next node-owner turn, including an
ordinary node tick or database shutdown, drains it. `SubscriptionStream::close()`
enqueues that same command and directly drives its drain under ordinary async
node ownership, so an uncontended explicit close does not require an external
tick. The stream MUST retain the in-flight completion before `close()` first
awaits. If that caller future is cancelled while waiting for the node owner, a
later `close()` MUST resume and await the same command; it MUST NOT return
success merely because cleanup was already enqueued. Once close begins, the
`Stream`, `next_event()`, and `try_next_event()` surfaces are terminal and MUST
return `None`.

The close acknowledgement is a local ownership-retirement boundary. It means
the local maintained-view subscription is retired, every propagated coverage
owner/refcount held by that stream is released, and an Unsubscribe for the last
shared owner has been applied locally and queued for the upstream connection.
It does not mean that a wire Unsubscribe has already been delivered or accepted;
that remains connection-tick work with the connection's transport error
semantics. Closing one of several shared owners MUST preserve the upstream
subscription; closing the last MUST queue exactly one retirement. Repeated
close, concurrent drain, and shutdown overlap MUST remain deterministic and
idempotent.

Finalization commands identify the owned subscription state, not a snapshot of
a Groove ID, so a catalogue/runtime refresh cannot replace a handle between
enqueue and drain. Enqueue synchronously marks that state closed; refresh MUST
NOT rehydrate it while retirement is waiting for the node mutex.

Database shutdown closes both subscription-finalization and transaction
admission before its first storage await. It snapshots every live stream into
its retirement set and transfers the open-transaction sweep to node-owned
maintenance before waiting for node ownership. Cancelling a pending `Db::close`
MUST NOT lose that sweep: the next node owner drains queued transaction
abandonment and terminalizes every remaining open transaction while the closed
gate rejects new openers and operations. Only after those ownership passes does
a completing close retire maintained runtime and connection bookkeeping and
close storage. A stream finalizer or transaction-handle drop arriving after
admission closes owns no separate resident work: the corresponding final sweep
already owns that runtime state.

**Implementation status (2026-07-27).** Local live reads currently use a named
local materialized-row bridge while maintained-view integration continues;
`db_facade_subscription_accepts_local_tier_for_alpha_style_live_reads`
(`crates/jazz/src/db/tests.rs:3886`) covers the local-tier behavior. This is an
implementation staging note, not a semantic exception.

### 13.4 Writes

Writes enter the local lane first and return a `WriteHandle<S>`. The mutation
surface consists of `insert`, `insert_with_id`, `update`, `upsert`, `delete`,
and `restore`. `insert` obtains its row id from the configured
`RowIdSource`; `insert_with_id` and `upsert` accept a caller-supplied id
(`INV-API-8`). `update`, and `upsert` when the row already exists, merge the
patch over the row's current local cells, so omitted fields keep their value
(`INV-API-9`).

An upsert's `WriteTarget` is the complete read view used to choose between
update and insert. For a head-over-base branch view, a head-local row is patched
with its local content winner as parent. A row inherited only from the base is
copied into the head and patched without making the base transaction a parent;
unchanged indirect large-value cells retain engine-only proof from that exact
inherited preimage. A row absent from both head and base is inserted into the
head.

A committed head-local deletion winner is not absence when it tombstone-hides
the row: the upsert returns `ErrorCode::WriteRejected` and emits no content
write. Callers that mean to revive committed deleted state must use `restore`
with `ExactWriteTarget::Branch(head)`, which publishes replacement content and
`DeletionEvent::Restored` together. Within one mergeable transaction, however,
a later upsert supersedes that same transaction's pending delete atomically, so
the commit cannot contain tombstone-hidden replacement content. Transactional
branch-view classification and session read visibility include the staged
overlay, allowing a session to upsert a branch row it inserted or upserted
earlier in the same transaction.

Low-level JavaScript upsert options use `{ head, base? }` for a branch view.
For compatibility, the former `{ branch }` shape is an exact alias for
`{ head: branch }`; it cannot be combined with either `head` or `base`.
Ambiguous combinations and `base` without `head` are rejected before the
root/default target can be selected.

Rust `UpsertOptions::target` now uses `WriteTarget` rather than
`ExactWriteTarget`. Root/default callers keep their runtime behaviour, but code
that constructs the field with `ExactWriteTarget` must migrate, and exhaustive
matches over the options field must handle `WriteTarget::BranchView`.

The write handle is the caller's durability and fate observation point. It
carries the affected `RowUuid`, the backing `TxId` (`mergeable_tx_id()`), and the local
durability tier (`INV-API-13`). `wait(tier)` returns only when the requested
tier's full observation condition is satisfied, returns `WriteRejected` when the
write's fate is rejected, and returns `NotObserved` while that condition is not
yet observed. In particular, a `Global` wait requires the conjunction
`Fate::Accepted`, `DurabilityTier::Global`, and an authority-assigned
`GlobalTime`; a hydrated or propagated `Global` durability claim without the
other two facts does not complete it (`INV-API-15`, ch. 3).

Mutation acceptance ends at publication ownership, not observer delivery
(`INV-API-35`). A pre-publication validation or persistence failure returns an
error and emits no observer update. After persistence succeeds, or after a
deferred publication enters the node-owned ordered queue, the API returns the
committed handle/transaction id. If subscription refresh then fails, each
affected stream receives its existing `Rejected::ServerFailure` event while the
write receipt remains successful. Cancellation cannot reclaim a queued
publication; the node retries its front publication in order before releasing
later publications or upstream upload work.

Each single-call write creates **one mergeable transaction**. `mergeable_tx()`
groups multiple facade writes under one `TxId`; the resulting commit unit carries
`n_total_writes` equal to the number of grouped versions (`INV-API-26`).
`exclusive_tx()` exposes the serializable transaction path from ch. 3/ch. 5 on
the facade and reports validation conflicts as `WriteRejected` (`INV-API-27`).

Owning Rust transaction handles use idempotent RAII abandonment. Dropping an
uncommitted `MergeableTx` or `ExclusiveTx` synchronously records a deduplicated
tombstone outside the storage-owning node, then retires it immediately when the
node is uncontended or schedules node maintenance. Every transaction open,
read, stage, and commit operation MUST inspect and drain its id's tombstone
after acquiring node ownership and before acting. Thus an operation already
waiting ahead of the maintenance tick cannot commit a logically abandoned
transaction. Tombstone drainage treats missing or already-terminal ids as
benign and processes every later id independently; drop never waits for async
ownership.

Write durability follows the client facade boundary. A `Db` write always lands
locally first, remains `Local`, and is queued in the shared outbox for upstream
upload (`INV-API-14`, ch. 3, ch. 8). Self-finalization to
`Accepted`/`Global` is core `Node` behavior, not a `Db` role.

The outbox itself is process-local, so reopening persistent storage rebuilds it
from durable transaction state (`INV-API-30`). With the same `DbIdentity`, the
facade schedules every locally originated transaction that reached `Local`
durability and is not terminally settled (rejected or `Global`) for delivery.
Here, locally originated means both `TxId.node == DbIdentity.node` and
`Transaction.made_by == DbIdentity.author`; shared history from another device
using the same author is not this client's backlog. Replayed delivery is
at-least-once by `TxId`; the authority's idempotent commit-unit handling makes
that safe, while each individual connection still sends a `TxId` at most once.

An edge-authority decision is likewise not permission to discard the edge's
upstream outbox entry. The edge retains an edge-accepted upload until a terminal
rejection, or an `Accepted` receipt that carries both Global durability and an
authority-assigned `GlobalTime`, arrives for that `TxId` directly on the
authenticated connection to the currently admitted upstream fate authority.
A featureless/unnegotiated link has no such authority identity. Local
acceptance, view hydration, staged or replayed updates, and receipts associated
with a featureless, detached, or superseded authority cannot release the entry
(`INV-API-34`, ch. 9).

Field-level semantics are the same regardless of the write method. An explicit
null clears a nullable column. A JSON column accepts only syntactically valid
JSON source text and, when it declares a JSON Schema, only instances accepted by
that schema; it preserves the accepted source text verbatim and is replaced
atomically. A rejected JSON write therefore leaves no new row or partial update.
A write to a soft-deleted row fails locally, and an offline racing write is
rejected at the authority. Unawaited write failures surface through an
`on_write_error` hook rather than being lost.

Trusted backends can perform core-only attributed writes: the backend sets
`Transaction.made_by` to a user while write policy is evaluated under the
backend's authenticated identity. Clients may attribute writes only to themselves
(`INV-API-29`, ch. 7).

_Further invariants._ `INV-API-10` — `upsert` uses its complete `WriteTarget`:
it merges a root or head-local row, copies and patches a base-inherited row into
the head without a cross-branch parent, or inserts into the target when absent.
A tombstone-hidden row is not insertable: root and branch-view upserts reject it,
and only `restore` emits `DeletionEvent::Restored`. Standalone and
mergeable-transaction upserts use the same choice.
`INV-API-11` — `delete` lowers to a mergeable `DeletionEvent::Deleted`.
`INV-API-12` — `restore` rejects empty data and lowers to content plus `DeletionEvent::Restored`. `INV-API-25` —

#### Permission advice dry runs

Permission probes (`can_insert`, `can_read`, `can_update`, `can_delete`) return
the three-valued `PermissionAdvice` result:

- `Allowed` means the serving authority evaluated the hypothetical operation
  under the authenticated link subject and allowed it at that point.
- `Denied` means that same authority definitively rejected the hypothetical
  operation at that point.
- `Unknown` means no definitive authority decision is available. It is not a
  denial, an allowance, or evidence about policy dependencies.

Only the serving authority may produce `Allowed` or `Denied` today. The local
client API deliberately returns `Unknown` rather than evaluating a replica that
may be offline, incomplete, or not permissions-ready; a request that cannot
reach a ready authority, including one that times out, also resolves to
`Unknown`. A partial replica is never a permission-advice authority.

Advice is a dry run, not a mutation precondition or reservation. It creates no
row/version, changes no local or authority state, and does not alter the normal
optimistic mutation path: an ordinary later write is still submitted and
authorized independently, and may be rejected if the relevant state or policy
has changed. The request carries only the hypothetical operation; the serving
side uses the authenticated link identity as its subject rather than a
client-provided identity. The response contains only the opaque request id and
one advice value: no supporting rows, policy reasons, or hidden dependency facts
cross the boundary.

Each live link correlates a request and response with a fresh opaque id. Dropping
or cancelling a request removes its waiter, so a late response is ignored;
replayed responses cannot resolve another request, including after reopening.
The serving side deduplicates responses by request id within a bounded cache, so
retransmission neither repeats evaluation nor mutates state (`INV-API-28`, ch.
7).

### 13.5 The sync/serve surface (binding-facing)

Synchronization is explicit and binding-facing. A `Db` embeds no runtime or
socket; the async boundary stays between nodes. The binding supplies a
`Transport { send(SyncMessage), try_recv() -> Option<SyncMessage> }`, with both
operations non-blocking. `try_recv() == None` means "nothing staged now," not
"closed" (`INV-API-16`).

`Db::connect_upstream(transport)` attaches an upstream connection and carries
already-registered subscriptions upstream immediately (`INV-API-17`).
`Db::accept_subscriber(transport, identity)` serves a subscriber under the
subscriber's identity, **not the serving Db's own** (`INV-API-21`, ch. 7).
`Db::subscribe` auto-announces new subscriptions to upstreams (`INV-API-18`).

App consumers never operate this layer directly. A language or platform binding
stages wire bytes into the transport and drives the tick. The connection state is
owned by the `Db`: a client-to-upstream connection carries this `Db`'s
subscriptions and queued commits upstream, while a server-to-subscriber
connection wraps peer state for the subscriber identity. An edge uses both
directions; relay/edge/core peer roles remain below the facade (ch. 9).

`Db::tick()` services every connection once (`INV-API-22`). For each connection,
`PeerConnection::tick` sends each unannounced subscription once
(`RegisterShape` then `Subscribe`, `INV-API-19`), uploads each local commit
once (`INV-API-20`), drains inbound messages, applies them, and refreshes
registered subscriptions (ch. 8). An ingest publication is persisted and its
ordered post-settlement work remains owned before that refresh runs. Refresh
failure is delivered to subscription owners and does not turn successful ingest
into a retryable peer failure (`INV-API-35`, `INV-TX-25`).

Bindings that schedule `Db::tick()` in a background client driver own the
driver's lifecycle. They classify a returned error before deciding whether to
stop: bounded-queue backpressure retries, and a closed upstream WebSocket detaches and reconnects.
Other errors are terminal because no repair is defined for them. Recovery uses
bounded exponential backoff; when it exhausts, the binding records a terminal
sync error. Queries, hydration, and durability waits must observe that error
promptly rather than waiting for ordinary coverage or settlement timeouts
(`INV-API-23`).

The binding-facing surface includes:

- **B1.** `Transport`, `PeerConnection`, `tick`, `connect_upstream`, and
  `accept_subscriber` under identity; subscription requests round-trip to
  initial and incremental `ViewUpdate`s. The current Rust facade observes those
  through `WatchHandle`; binding ABIs expose them as subscription stream events.
- **B1.5.** Client writes queue in the shared outbox, upstream ticks upload
  un-uploaded commit units, the authority accepts or rejects them and returns
  fate, and the client applies the result so a client write can reach `Global`.
  Together with core `Node` self-finalization, this is the write-to-serve-to-read
  loop exposed through the facade.

A `Db` is thread-affine — **not** a `Send` proxy to a remote node. Cross-thread or
cross-context sharing is done by running multiple nodes connected via peer sync,
not by sharing one `Db`. A pure-Rust server with no sync-UI constraint layers its
own sharing strategy, such as a `Mutex` or an actor adapter, on top; the core does
not impose the actor model.

### 13.6 Errors and what's callable today

Facade errors carry an `ErrorCode` plus a message:

| `ErrorCode`     | raised when                                                                                |
| --------------- | ------------------------------------------------------------------------------------------ |
| `Schema`        | schema/table/column validation failed (e.g. `restore` with empty data)                     |
| `Query`         | query validation or binding failed                                                         |
| `WriteRejected` | the authority rejected the write's fate — surfaced by `wait` and the `on_write_error` hook |
| `NotObserved`   | the requested durability tier is not yet locally observed                                  |
| `Storage`       | the storage backend failed                                                                 |
| `Protocol`      | a local node / protocol operation failed                                                   |

**Callable today:** `Db::open`; the mutation methods (§13.4), including
`mergeable_tx`, `exclusive_tx`, attributed writes, and `can_*` dry-runs; `table` /
`read` / `one` / `all` / `subscribe` (§13.3); and the binding sync surface
(§13.5). Read policies evaluate `claim("user")` plus admission/session-provided
runtime claims (ch. 7); client query bindings never supply policy claims.
`Db::open_history_complete` and `Db::at` provide history-complete facade reads;
ordinary client facades remain history-incomplete (`crates/jazz/src/db.rs:383`,
`crates/jazz/src/db.rs:637`). Branch operations otherwise remain at the `Node`
level (ch. 11). The initial binding ABI design is below; remaining
**designed but not yet on the facade** surface stays in the Open questions
section.

### 13.7 Initial TS/WASM/NAPI binding implementation notes (non-normative)

This section records a current ABI architecture and capability snapshot. It is
not a product-semantics contract: bindings may change object, payload, and queue
shapes so long as they preserve this chapter's facade semantics and §13.13's
single-owner query rule.

The binding surface is a thin host-language wrapper around Rust-owned `Db`,
transaction, subscription, and selected serving `Node` objects. It is not a
second semantic protocol. Sync semantics remain `SyncMessage` inside Rust
transports, byte transport uses `WireFrame`/`WireEnvelope` (ch. 8), and
TypeScript owns ergonomic objects, validation helpers, promise/stream adapters,
and framework integrations.

The binding surface is versioned separately from the Jazz wire protocol because
it describes host calls into one local database object, not peer-to-peer sync.
Rust owns semantic validation; bindings own host object identity, caches,
callbacks, promises, and user-facing API shape.

High-level structs such as `JazzSchema`, `DbConfig`, `DbIdentity`, `ReadOpts`,
`Query`, `WriteState`, and `Error` may cross a binding boundary through normal
host-native object mapping or postcard bytes. They are core types, not shadow
ABI payloads. Row-shaped input and output is the stable hot path where custom
encoding matters most. Reads, subscription streams, encoded-write variants, and
transaction encoded-stage variants use the shared groove `Record` encoding
family at this boundary: postcard envelopes carry table/operation metadata, a
`RecordDescriptor`, and raw encoded row/cell bytes.

Read-side row arrays should use the shared groove `Record` encoding end-to-end
across sync protocol records and binding returns: postcard envelopes carry a
table name, a `RecordDescriptor`, and raw encoded row bytes. Bindings are
expected to learn this row decoder once and may build descriptor/table-specialized
accessors instead of receiving re-encoded maps for the hottest cross-boundary
data path. This is the same lower-level groove descriptor/raw encoding family
used by sync records, but read results are projected current-row records rather
than sync `VersionRecord` payloads with parents/deletion/schema-version fields.

The binding boundary is intentionally thin. Current WASM/NAPI bindings expose
idiomatic host objects around Rust `Db`, transaction, subscription, and transport
APIs. Postcard can be used directly where byte payloads are useful; the listed
ABI shapes are implementation choices rather than a second public API.

Subscriptions cross the boundary as host streams/callbacks built on
`Db::subscribe`, with postcard-encoded chunks if a byte payload is needed.
Transport code moves encoded `WireFrame` bytes; it does not
decode `SyncMessage` as product API. Catalogue, branch-view, and lens APIs should be added only when their core runtime APIs and binding ergonomics are
settled.

#### 13.7.1 Binding Responsibilities

| responsibility        | binding contract                                                                                          |
| --------------------- | --------------------------------------------------------------------------------------------------------- |
| object ownership      | wrap real Rust core objects directly in idiomatic host classes/resources                                  |
| row-record decoder    | decode descriptor/raw `Record` rows and optionally compile descriptor-specialized accessors               |
| encoded writes/probes | send descriptor/raw `Record` patches for hot-path row input where map-shaped payloads would copy too much |
| subscriptions         | bridge Rust subscription streams into host callbacks/streams without a global event queue                 |
| transport byte queues | move encoded `WireFrame` bytes through host sockets/workers without inventing an app-level sync API       |
| errors                | translate core `Error`/`WireError` into host-native exceptions or rejected promises                       |

#### 13.7.2 Binding Payloads

Binding payloads use core types directly:

| core payload                                                    | purpose                                                                                      |
| --------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `DbConfig`, `DbIdentity`                                        | open/config payloads, with storage constructed by the binding                                |
| row-record envelopes                                            | future descriptor/raw row input and output payloads, to be shaped by the direct WASM binding |
| `ReadOpts`                                                      | read option payloads                                                                         |
| subscription stream chunks                                      | host stream events over `Db::subscribe`, encoded only where needed                           |
| `TxKind`                                                        | transaction kind                                                                             |
| `WriteState`                                                    | write fate/durability payload                                                                |
| `Error`, `ErrorCode`, `WireError`, `WireErrorCode`, `WireRetry` | structured local and wire errors                                                             |

`WriteState` is not a parallel binding shape. Rejection detail is represented by
the core `Fate::Rejected(RejectionReason)` variant, preserving
`ExclusiveConflict`, `AuthorizationDenied`, `Cascade { root }`, clock-skew,
causality, and malformed-commit detail without a second field that can drift
from the transaction fate.

#### 13.7.3 Byte transport calls

Bindings never decode `SyncMessage` as their primary sync API. The only portable
byte transport payload is an encoded `WireFrame`; when the frame is
`WireFrame::Message`, its `WireEnvelope.payload` contains the postcard-encoded
`SyncMessage` owned by ch. 8. The binding is responsible for moving bytes between
sockets, workers, or host channels and the Rust transport object exposed by the
binding.

`AttachTransport` chooses direction (`upstream` or `subscriber`), peer role, and
session/admission hints. Rust turns accepted frame bytes into the semantic
transport consumed by `Db::connect_upstream` or `Db::accept_subscriber`. Malformed
bytes produce `WireFrame::Error` when the peer should hear about the problem and
an `Error`/host error when the local binding must act.

Backpressure is explicit: send operations may return `Backpressure` with retry
guidance, and receive operations may accept max frame and byte budgets. A host
transport close does not imply `Db` close.

#### 13.7.4 Error shape

Binding-facing errors use stable machine codes plus structured context. Messages
are diagnostics, not compatibility keys.

```text
Error {
  code: ErrorCode,
  message: string,
}
```

Initial `ErrorCode` values are:

| code            | maps from / raised when                             |
| --------------- | --------------------------------------------------- |
| `Schema`        | schema/table/column validation failure              |
| `Query`         | query validation or binding failure                 |
| `WriteRejected` | authority rejected a write fate                     |
| `NotObserved`   | requested durability/tier not locally observed      |
| `Storage`       | storage backend failure or unavailable backend      |
| `Protocol`      | local node/protocol invariant failure               |
| `Backpressure`  | bounded queue or transport cannot accept more bytes |

Wire transport errors use `WireError { code: WireErrorCode, retry: WireRetry,
message }`.

#### 13.7.5 Cross-binding capability matrix

Legend: `Y` implemented or required for the first cross-binding gate; `P`
partial or designed with known gaps; `N` intentionally absent from that layer;
`Shell` means the server shell exposes operational wrapping around `Node`, not
the client `Db` facade.

| capability                       | Rust `Db` | TypeScript `Db` | WASM ABI | NAPI ABI | browser-worker |   server-shell |
| -------------------------------- | --------: | --------------: | -------: | -------: | -------------: | -------------: |
| open/close client db             |         Y |               Y |        Y |        Y |              Y |              N |
| storage open/config              |         P |               Y |        Y |        Y |              Y |          Shell |
| query builder objects            |         P |               Y |        N |        N |              N |              N |
| prepare validated query          |         Y |               Y |        Y |        Y |              Y |          Shell |
| local reads: `read`/`one`        |         Y |               Y |        Y |        Y |              Y |          Shell |
| tiered reads: `all(ReadOpts)`    |         P |               Y |        Y |        Y |              Y |          Shell |
| Rust facade watches              |         P |               N |        N |        N |              N |              N |
| subscription streams             |         P |               Y |        Y |        Y |              Y |          Shell |
| stream row changes/resets        |         P |               P |        P |        P |              P |          Shell |
| mergeable writes                 |         Y |               Y |        Y |        Y |              Y |          Shell |
| exclusive transactions           |         Y |               Y |        Y |        Y |              Y |          Shell |
| write wait/state                 |         Y |               Y |        Y |        Y |              Y |          Shell |
| dry-run permission probes        |         Y |               Y |        Y |        Y |              Y |          Shell |
| byte wire transport              |         Y |               N |        Y |        Y |              Y |          Shell |
| semantic `SyncMessage` transport |         Y |               N |        N |        N |              N | Shell-internal |
| auth/session admission           |         P |               Y |        P |        P |              P |          Shell |
| branch-view/time-travel facade   |         P |               P |        P |        P |              P |          Shell |
| lens/catalogue facade            |         P |               P |        P |        P |              P |          Shell |
| structured errors/events         |         P |               Y |        Y |        Y |              Y |          Shell |
| durability tier waits            |         Y |               Y |        Y |        Y |              Y |          Shell |
| worker/thread proxying           |         N |               Y |        N |        N |              Y |          Shell |
| health/metrics/shutdown          |         N |               N |        N |        N |              P |          Shell |

The parity target is behavioral: TypeScript should expose the same product
surface on WASM and NAPI, while lower layers expose only the handle/byte ABI
needed to implement it. Browser workers are proxy hosts for the same ABI, not a
separate API. The server shell is operational infrastructure around `Node`
roles, storage, auth admission, listeners, metrics, and shutdown; it must not
widen the client `Db` product surface to model core/edge roles.

Current executable binding harnesses live under `examples/jazz-tools`
and `examples/browser-wasm`. The Node harness proves the `WasmDb` method
surface, Record-encoded row reads/writes, permission probes, write-state/wait,
mergeable transaction commit/abort, catalogue publish/lens/pointer
acknowledgements, worker-thread ownership, and byte transport pumping. The
browser harness proves worker-owned `WasmDb`/transport objects through a Web
Worker, Record-encoded rows/cells, permission probes, write-state/wait, reads,
subscription stream snapshots, IndexedDB via `WasmDb.openBrowser`, websocket byte
batches, and a headless Chromium smoke gate. `db_read_at` remains
typed/API-surface-only in the TS harness until there is a serving-node setup for
that path.

### 13.12 Subsumed client, backend, and binding notes

The former TypeScript client and backend-context notes are folded into this
chapter. The public API should keep app code focused on tables, queries,
subscriptions, writes, and write state. Defaults are write-origin behavior
(ch. 10), query builders are immutable shape builders (ch. 6), and transaction
helpers must surface real commit/fate semantics rather than local-only batches.

Backend helpers need explicit authority and identity boundaries:
`asBackend()` is trusted server-owned work, request/session helpers are
caller-scoped, and any embedded/local-only `db()` helper must be documented as
such. Attribution-only writes are distinct from requester-scoped authorization.

Binding surfaces should expose host-native promises, callbacks, and streams over
Rust-owned objects. WASM, NAPI, React Native, and future language bindings all
consume the same `Db`/selected `Node` contract; packaging differences must not
fork query, transaction, or sync semantics.

### 13.13 Query semantics live in the core

Query semantics have exactly one owner: the core. TypeScript, wasm, napi, and
runtime client layers may build queries,
serialize query IR, cache prepared handles, transport frames, and hydrate typed
application objects — but they must not independently evaluate predicates,
ordering, limit/offset/windowing, relation/include membership, permission
visibility, identity/dedupe rules, or semantic delta coalescing. A client-side
reducer over delivered deltas is legitimate only as a _specified wire-protocol
reducer_: its behavior must be fully determined by the delivered stream, never
by re-evaluating the query against row sets.

When a client API needs an alternate read view — read-your-writes inside an
open transaction, a branch view, a historical snapshot — the API expresses that
view to the core (`ReadOpts.read_view`, open-transaction overlays) and the
normal lowered query executes there. The engine already evaluates queries
inside open exclusive transactions (`tx_query` over
`OverlayRef::OpenTransaction`); binding layers plumb handles to it rather than
overlaying pending writes above the engine.

The branch-view facade is a target surface owned by ch. 11. Bindings pass
normalized named head/base sources to the core and never recreate masking,
winner selection, or copy-on-write semantics in TypeScript or host code.

## Open Questions

### Open questions

These are designed but not landed:

- 🔶 **Server shell boundary.** A server executable/package should wrap `Node`
  rather than widening the client `Db` facade: config, WebSocket/transport
  listeners, auth admission, health/metrics, RocksDB/storage path, migration
  reporting, and shutdown live in the shell; transaction/query/sync semantics
  stay here and in ch. 8–9.
- 🔶 **Watch deltas/streams & stable row identity.** The design promises
  `delta()`, `into_stream()`, and stable row allocation identity; the current
  handle exposes only `current()` (cloned `Vec<CurrentRow>`) and `changed()`.
- 🔶 **Tier-gated first result & loading state.** The design has `all`/`subscribe`
  gating the first result on remote propagation; the current slice queries local
  state immediately and is woken by `tick`. Reads otherwise do not perform an
  implicit network wait: a `Local` read shows optimistic writes immediately, and a
  `Global` read shows only locally-observed accepted state, which may be empty
  until sync has been ticked. The product contract also distinguishes _undefined_
  (never settled) from _empty_ (settled, empty) — i.e. whether the subscriber has
  a settled subscription result set for the binding (ch. 6), surfaced as a
  queryable `settled()` bit on the handle before the first gate. Neither the
  gating nor `settled()` is implemented yet.
- 🔶 **Observable connection state, and cancelling a wait.** A wait at `Edge` or
  `Global` tier while disconnected has no honest answer today: rejecting loses a
  write's durability observation that would have resolved on reconnect, and
  waiting indefinitely gives the caller no way to distinguish "offline, will
  resolve later" from "something is broken". Neither carries a diagnosis.
  The intended shape is that the **core waits indefinitely** and cancellation is
  **caller policy**, because the core promises durability, not latency — only the
  caller knows whether a wait backs a background sync or a user pressing Save. In
  Rust this needs nothing new: `wait` is an `async fn`, so dropping the future
  cancels and `tokio::time::timeout` composes. In TypeScript the idiomatic form is
  an `AbortSignal` on the wait options, which yields timeouts via
  `AbortSignal.timeout`, composition via `AbortSignal.any`, and component-lifecycle
  cancellation for free; there is currently no `AbortSignal` anywhere in the
  runtime API. Three details are load-bearing whenever this is built: cancelling a
  _wait_ MUST NOT cancel the _write_, which is already committed and queued;
  abort MUST reject with a distinct reason so "I gave up" is never mistaken for
  "the write failed"; and abort MUST deregister the waiter, or an indefinite wait
  becomes a slow leak on a long-lived client.
  The missing complement is an **observable connection and pending state** — at
  minimum whether the client is connected, and how many writes are outstanding at
  each tier — so an application can render honestly rather than inferring from a
  promise that has not settled. This is the more valuable half: a timeout tells
  you only that time passed. A bulk import that hung on a global-tier wait was
  undiagnosable for exactly this reason; the wait was unbounded, uncancellable,
  and invisible, and the cause could only be found by instrumenting the core.
  None of this is implemented.
- 🔶 **Identity modes & admission.** `DbIdentity` is `{ node, author }` today;
  core-only attributed writes are callable, but the broader backend /
  no-identity-platform modes (ch. 9) and `accept_subscriber` admission policy are
  not yet represented.
- 🔶 **Exclusive transaction handles in the binding ABI.** The binding ABI opens
  real core `OpenTransactionId`/open-exclusive state through a small internal handle API
  for write-side exclusive transactions. They are not faked by replaying staged
  point writes at commit time. Tx reads, restore behavior, multi-row
  `WriteStarted` row ids, and rejected-write wait semantics for unmet higher
  durability tiers remain explicit follow-up decisions. Binding write state now
  includes structured rejection diagnostics.
- 🔶 **Transport backpressure/disconnect.** Local `send` paths are fallible and
  bounded queues now surface retryable backpressure; upstream uploads and
  subscription announcements are not marked delivered until local enqueue
  succeeds. ABI transport diagnostics expose runtime-local session id/epoch,
  fresh/resumed status, and queue depths for live attachments. `try_recv` still
  cannot signal closed/error, remote disconnect frames and durable resume
  credentials are not specified, and subscriber-side view-update generation still
  needs a deeper peer-state rollback/redo contract before every served update can
  claim retry-perfect delivery under backpressure.
- 🔶 **Binding storage backends beyond memory.** The first executable local-app
  slice supports memory storage only. Browser, RocksDB, and host-provided storage
  need explicit config payloads, migration reporting, corruption behavior, and
  durability tests before `OpenStorage` may advertise them as supported features.
- 🔶 **React Native relay artifact.** RN persistence is owned by the
  `jazz-native-relay` SQLite host, exposed through the thin `crates/jazz-rn`
  command transport rather than a JavaScript storage driver or a second JSI
  runtime. Define Android/iOS artifact packaging, migration reporting,
  corruption behavior, teardown, and durability tests before the binding
  advertises persistent runtime support.
- 🔶 **Postcard binding payload evolution.** Row-shaped outputs and target
  write-input variants should be descriptor/raw `Record` payloads carried inside
  postcard envelopes, but the concrete Rust structs should be introduced by the
  direct WASM binding work instead of kept as speculative core DTOs.
- 🔶 **Direct object completion semantics.** Bindings should use host-native
  promises, callbacks, and streams over real Rust objects. WASM and NAPI still
  need to prove equivalent completion and error ordering without a Rust-owned
  global event queue.

- 🔶 **Benchmark migration.** As each remaining sync slice lands, migrate the
  matching peer-layer benchmarks onto the `Db` surface: S3/S4 for
  permission-filtered sync, S5/S6 for current-row sync and resume, S7 for schema
  migration, and S9 for durable execution. The measurement target is the public
  user API end to end, not permanent internal peer hooks.
- 🔶 **Backend context helper cleanup.** Keep `asBackend()`, `forRequest(...)`,
  and `forSession(...)` semantically separate; decide whether `db()` remains
  public and, if so, document it as embedded/local-only rather than a
  server-connected default.
- 🔶 **Optimistic update DX.** Expose pending/confirmed/rejected mutation state
  on writes and rows, including filters by settlement tier, without inventing a
  second fate model.
- 🔶 **Full-mode subscription API.** Decide whether callers can opt into full
  result replacement, delta streams, or first-settle opt-out, and how those modes
  map to maintained-view terminal deltas.
- 🔶 **Live identity switching.** Changing the authenticated principal on a live
  client needs a teardown/rebind protocol for subscriptions, outbox attribution,
  claims, and local optimistic state.
- 🔶 **React Native runtime reuse.** RN `connect()` should reuse an owned runtime
  and expose deterministic connect/disconnect lifecycle signals rather than
  creating a fresh executor per call.
- 🔶 **WASM teardown trap true fix.** The current mitigation hides inert
  teardown traps; the durable fix is an explicit async shutdown and transport
  lifecycle boundary that prevents callbacks into torn-down linear memory.

### Intentional disconnect, tiers, and propagation

`Db::disconnect` marks the `Db` **intentionally offline**. It disconnects every
schema client from its server transport and leaves the local runtime and store
alive, so local reads and writes continue to work. `Db::reconnect` clears the
marker and reconnects every schema client using the configured server URL and
current auth configuration. A schema client created while the `Db` is
intentionally offline remains offline until `reconnect` (`INV-API-31`).

`ReadOpts.tier` selects the materialized knowledge sufficient for a result and
therefore its first-result gate. `Propagation` is independent: it controls
whether query evaluation or coverage may be forwarded upstream, and does not
change what a `Local` result means. Consequently, while intentionally offline,
a `Local` read with the default `Propagation::Full` still resolves from current
local materialized state (`INV-API-32`):

- a locally committed, pending write is returned immediately;
- a row written remotely during the offline period is absent — an empty result
  for a query matching only that row — until reconnect delivery reaches the
  local store.

`LocalOnly` prevents upstream routing. It is **not** what chooses the local
snapshot, nor is it a request to wait until that snapshot becomes complete
relative to an unavailable upstream. Convergence is asserted separately, after
`reconnect`.
