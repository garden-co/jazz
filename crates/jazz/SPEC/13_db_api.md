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

- `INV-API-1`: Db MUST be the high-level runtime-typed client facade — a thin wrapper over a participant Node (which owns the NodeState engine, connections, and serving) — and it MUS...
- `INV-API-2`: Db is a client only and has no role: Db::open MUST construct a non-history-complete client NodeState. A history-complete, fate-deciding authority is a core Node opened...
- `INV-API-3`: Db::read and Db::one MUST be synchronous local reads and MUST NOT wait for upstream sync; Db::all MUST use ReadOpts to choose the effective durability tier.
- `INV-API-4`: When ReadOpts.localupdates == LocalUpdates::Immediate, the effective read tier MUST be at least DurabilityTier::Local; when it is Deferred, the effective read tier MUS...
- `INV-API-5`: ReadOpts::default() MUST be { tier: DurabilityTier::Local, localupdates: LocalUpdates::Immediate, propagation: Propagation::Full }.
- `INV-API-6`: Db::subscribe MUST support live subscriptions at the requested effective tier. Local subscriptions are first-class application-facing subscriptions that include the no...
- `INV-API-8`: Db::insert MUST generate the row id using its configured RowIdSource; Db::insertwithid MUST use the caller-supplied RowUuid.
- `INV-API-9`: Db::update MUST preserve omitted fields for a locally present row by merging the patch over the row's current local cells.
- `INV-API-10`: Db::upsert MUST merge supplied cells over current cells when the row exists locally and MUST write supplied cells directly when the row does not exist locally.
- `INV-API-11`: Db::delete MUST lower to a mergeable commit with DeletionEvent::Deleted and make the row absent from current reads after local application.
- `INV-API-12`: Db::restore MUST reject empty cell data with ErrorCode::Schema and MUST lower a non-empty restore to content write plus DeletionEvent::Restored.
- `INV-API-13`: Every local write method MUST return a WriteHandle carrying the affected RowUuid, backing TxId, and local durability tier.
- `INV-API-14`: A local write on a Db MUST be DurabilityTier::Local and queued for upstream upload; a Db (always a client) MUST NOT self-finalize. Self-finalization to Accepted/Global...
- `INV-API-15`: WriteHandle::wait(tier) MUST return the handle TxId when the requested tier is locally satisfied, MUST return ErrorCode::WriteRejected for rejected fates, and MUST ret...
- `INV-API-16`: Transport implementations MUST be non-blocking; tryrecv() == None MUST mean no inbound message is currently staged and MUST NOT be interpreted by Db as disconnect.
- `INV-API-17`: Db::connectupstream MUST carry already-registered facade subscriptions upstream immediately by placing their (ValidatedQuery, Binding) pairs into the connection's pend...
- `INV-API-18`: Db::subscribe MUST announce newly registered subscriptions to all existing upstream connections so query-driven sync can request remote completion on the next tick.
- `INV-API-19`: An upstream PeerConnection::tick MUST send each unannounced usage-site subscription by first sending SyncMessage::RegisterShape once per shape and then SyncMessage::Su...
- `INV-API-20`: An upstream PeerConnection::tick MUST upload each locally-authored TxId at most once per connection by reading commitunitfor(txid), sending it, and marking it uploaded.
- `INV-API-21`: A subscriber PeerConnection::tick MUST serve subscriptions under the AuthorId passed to Node::acceptsubscriber, not under the serving node's own identity.
- `INV-API-22`: Db::tick() MUST service every registered PeerConnection exactly once by calling PeerConnection::tick.
- `INV-API-24`: The query builder exposed through Db::table MUST support OR/AND/NOT predicates, contains, inlist, isnull, includes with JoinMode::Holes, required includes, select, lim...
- `INV-API-25`: TextEdit operations MUST use byte offsets relative to the current local parent value for the column and MUST lower to LargeValueEditOp::Insert/LargeValueEditOp::Delete.
- `INV-API-26`: Db::mergeabletx() MUST group multiple facade writes under one mergeable TxId, and the produced commit unit MUST set Transaction.ntotalwrites to the number of grouped v...
- `INV-API-27`: Db::exclusivetx() MUST expose serializable exclusive transactions on the facade, preserving snapshot reads and returning WriteRejected when authority validation detect...
- `INV-API-28`: Db::caninsert, canread, canupdate, and candelete MUST evaluate permissions under the current DbIdentity.author without committing writes, changing local rows, or using...
- `INV-API-29`: A Db is a client: facade writes MUST keep permissionsubject == madeby, and a Db MUST reject any attempt to attribute a write to another author. Cross-author attributio...

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
use jazz::groove::{records::Value, schema::{ColumnSchema, ColumnType}, storage::MemoryStorage};
use jazz::ids::{AuthorId, NodeUuid};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

let schema = JazzSchema::new([
    TableSchema::new("todos", [
        ColumnSchema::new("title", ColumnType::String),
        ColumnSchema::new("done", ColumnType::Bool),
    ])
    .with_read_policy(Policy::public())     // or omit — a table with no policy is public
    .with_write_policy(Policy::public()),
]);
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
        author: AuthorId::from_bytes([0xa1; 16]),
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
`PeerRole::EdgeClient` link; and a relay is a `PeerRole::Relay` link (ch. 9,
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

`Db::subscribe(query, opts)` opens a live subscription at the requested effective
tier. `Local` subscriptions are first-class application-facing subscriptions:
they include the node's own pending committed writes and must be able to drive
synchronous local UI state after a local write. `Edge` and `Global`
subscriptions use the same query semantics, but their source/frontier and first
settlement/completeness rules are constrained to edge- or global-accepted data.

The design target is that **all** live subscriptions are backed by the unified
maintained subscription machinery from ch. 16, differing only in read frontier,
source resolution, and settlement semantics. The facade must not grow a second
query engine by rerunning `query_rows` and diffing full results as the normal
live-subscription mechanism. Until local maintained-view subscriptions are fully
unified with the edge/global path, implementations may keep an explicitly named
local materialized-row bridge for alpha-compatible local live reads, but that is
staging debt rather than a semantic exception (`INV-API-6`). Binding ABIs must
keep subscription delivery as a thin event bridge over the core subscription
surface (§13.7), not a second facade-side diff engine.

### 13.4 Writes

Writes enter the local lane first and return a `WriteHandle<S>`. The mutation
surface consists of `insert`, `insert_with_id`, `update`, `upsert`, `delete`,
`restore`, and `edit_text`. `insert` obtains its row id from the configured
`RowIdSource`; `insert_with_id` and `upsert` accept a caller-supplied id
(`INV-API-8`). `update`, and `upsert` when the row already exists, merge the
patch over the row's current local cells, so omitted fields keep their value
(`INV-API-9`).

The write handle is the caller's durability and fate observation point. It
carries the affected `RowUuid`, the backing `TxId` (`mergeable_tx_id()`), and the local
durability tier (`INV-API-13`). `wait(tier)` returns immediately when the tier is
locally satisfied, `WriteRejected` when the write's fate is rejected, and
`NotObserved` when the requested tier has not yet been observed (`INV-API-15`).

Each single-call write creates **one mergeable transaction**. `mergeable_tx()`
groups multiple facade writes under one `TxId`; the resulting commit unit carries
`n_total_writes` equal to the number of grouped versions (`INV-API-26`).
`exclusive_tx()` exposes the serializable transaction path from ch. 3/ch. 5 on
the facade and reports validation conflicts as `WriteRejected` (`INV-API-27`).

Write durability follows the client facade boundary. A `Db` write always lands
locally first, remains `Local`, and is queued in the shared outbox for upstream
upload (`INV-API-14`, ch. 3, ch. 8). Self-finalization to
`Accepted`/`Global` is core `Node` behavior, not a `Db` role.

Field-level semantics are the same regardless of the write method. An explicit
null clears a nullable column. A JSON column is replaced atomically. A write to a
soft-deleted row fails locally, and an offline racing write is rejected at the
authority. Unawaited write failures surface through an `on_write_error` hook
rather than being lost.

Trusted backends can perform core-only attributed writes: the backend sets
`Transaction.made_by` to a user while write policy is evaluated under the
backend's authenticated identity. Clients may attribute writes only to themselves
(`INV-API-29`, ch. 7).

_Further invariants._ `INV-API-10` — `upsert` merges over current cells when the
row exists locally, else writes the supplied cells. `INV-API-11` — `delete`
lowers to a mergeable `DeletionEvent::Deleted`. `INV-API-12` — `restore` rejects
empty data and lowers to content + `DeletionEvent::Restored`. `INV-API-25` —
`TextEdit` uses byte offsets relative to the current parent, lowering to
`LargeValueEditOp` (ch. 12).

Dry-run permission probes (`can_insert`, `can_read`, `can_update`,
`can_delete`) evaluate the same current-identity policy path as the corresponding
operation without ingesting versions or changing local rows (`INV-API-28`, ch. 7).

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
registered subscriptions (ch. 8).

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
(§13.5). Read policies evaluate `claim("sub")` plus admission/session-provided
runtime claims (ch. 7); client query bindings never supply policy claims.
Time-travel reads and branches exist at the `Node` level (ch. 11) but are not on
the `Db` facade yet. The initial binding ABI design is below; remaining
**designed but not yet on the facade** surface stays in the Open questions
section.

### 13.7 Initial TS/WASM/NAPI Binding Surface

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

The binding boundary is intentionally thin. WASM/NAPI bindings SHOULD expose
idiomatic host objects around the real Rust `Db`, transaction, subscription, and
transport APIs. Postcard can be called directly where byte payloads are useful;
there is no core ABI module, command bridge, handle registry, event queue, DTO
namespace, or second public API.

Subscriptions cross the boundary as host streams/callbacks built on
`Db::subscribe`, with postcard-encoded chunks if a byte payload is needed.
Transport code moves encoded `WireFrame` bytes; it does not
decode `SyncMessage` as product API. Catalogue, branch, lens, and large-value
APIs should be added only when their core runtime APIs and binding ergonomics are
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
| branch/time-travel facade        |         P |               P |        P |        P |              P |          Shell |
| lens/catalogue facade            |         P |               P |        P |        P |              P |          Shell |
| large-value read/edit handles    |         P |               P |        P |        P |              P |          Shell |
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
subscription stream snapshots, OPFS via `WasmDb.openBrowser`, websocket byte
batches, and a headless Chromium smoke gate. `db_read_at` and
`db_edit_text` remain typed/API-surface-only in the TS harness until there is a
serving-node or stable large-value setup for those paths.

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
- 🔶 **Identity modes & admission.** `DbIdentity` is `{ node, author }` today;
  core-only attributed writes are callable, but the broader backend /
  no-identity-platform modes (ch. 9) and `accept_subscriber` admission policy are
  not yet represented.
- 🔶 **Exclusive transaction handles in the binding ABI.** The binding ABI opens
  real core `OpenTxId`/open-exclusive state through a small internal handle API
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
- 🔶 **React Native storage driver.** The TypeScript RN/Expo binding scaffold
  exposes the future storage hook as a typed SQLite driver placeholder only.
  Decide whether RN persistence is owned by `op-sqlite`, `expo-sqlite`, or the
  `crates/jazz-rn` native-module/JSI route, and define how that choice maps onto
  the portable storage contract before the binding advertises persistent runtime
  support.
- 🔶 **Postcard binding payload evolution.** Row-shaped outputs and target
  write-input variants should be descriptor/raw `Record` payloads carried inside
  postcard envelopes, but the concrete Rust structs should be introduced by the
  direct WASM binding work instead of kept as speculative core DTOs.
- 🔶 **Direct object completion semantics.** Bindings should use host-native
  promises, callbacks, and streams over real Rust objects. WASM and NAPI still
  need to prove equivalent completion and error ordering without a Rust-owned
  global event queue.

B2–B4 have **landed** (the binding-facing surface is now complete): **B2** —
`PeerConnection::serve_current_rows`, a connection-owned `ResumeCursor`
(`accept_subscriber_with_resume`), and `last_resume_bytes()` accounting; **B3** —
`Db::open_history_complete`, `Db::at(GlobalSeq, &Query)` (returning
`HistoricalReadRequiresServer` when incomplete), and facade catalogue/lens ops
(`publish_schema` / `publish_lens` / `set_current_write_schema`, Core-gated);
**B4** — exclusive/CAS over the sync surface (the in-memory commit unit is uploaded
so the authority validates serializably) plus fate exposure via `Db::write_state` /
`WriteHandle::write_state` (`WriteState { fate, durability }`).

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
