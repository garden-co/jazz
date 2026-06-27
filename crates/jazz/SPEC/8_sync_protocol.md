# jazz — Specification · 8. Sync protocol

One protocol carries everything between nodes. This chapter defines that peer
protocol: how writes travel up as commit units, how fates and query-driven view
updates travel down, how payloads are deduplicated and rehydrated, and how
mergeable vs exclusive transactions are delivered. It ties together transactions
(ch. 3), history (ch. 4), queries (ch. 6), and authorization (ch. 7); the
deployment roles are chapter 9.

## 8.1 One protocol, roles not code

Sync uses one peer protocol everywhere in the deployment. UI, worker, edge, and
core links all exchange the same `SyncMessage` vocabulary; a tier's behavior is
determined by its role, not by a separate wire language (ch. 1, principle 2).
Roles include relay links (`PeerRole::Relay`), edge-client links
(`PeerRole::EdgeClient { identity }`), fate authority, durability, and eviction.

A relay link represents the system author (`AuthorId::SYSTEM`) and performs no
read narrowing. It registers each shape upstream **once** and forwards the
**union** of downstream binding sets, which makes subscription aggregation
composable at every hop. An edge-client link carries the terminated peer identity
and narrows reads under that identity (ch. 7, ch. 9).

The peer wire form is binary-first. `WireFrame` wraps `Hello`,
`Message(WireEnvelope)`, and `Error`; `WireEnvelope.payload` contains a
postcard-encoded `SyncMessage` plus protocol version and feature bits. Postcard
is the canonical runtime frame/envelope format; JSON fixtures are only
human-readable golden checks. Row/version payloads remain groove custom
`Record` bytes inside protocol messages; postcard wraps those bytes, it does not
replace row encoding. The same split applies at the binding ABI (ch. 13):
commands, acks, and event metadata are postcard envelopes, while row-shaped
payloads are descriptor/raw `Record` bytes at the hot boundary.

Inside Rust, `Db` and `PeerConnection` keep the semantic `Transport` surface over
`SyncMessage`. Binding/server byte transports use `WireFrame` and are bridged at
the edge of the core, so handshake, socket state, malformed-byte errors, and
backpressure do not become DB semantics. Transports such as websockets or
channels are binding-supplied drivers layered underneath these semantics after
they are proven in simulation (appendix A). The only ordering assumption is
**per-link FIFO**. Cross-link races and rehydration make stronger end-to-end
delivery guarantees unaffordable, so "parked orphan" is a first-class protocol
state with counters and tests (§8.2).

WebSocket carriers batch by default: one binary WebSocket message carries a
postcard `Vec<Vec<u8>>`, where each inner byte vector is one encoded
`WireFrame`. The batch envelope is transport-local and must not be confused
with row encoding or semantic sync messages; batching reduces socket/message
overhead while preserving the core's per-link FIFO `WireFrame` stream.

Fast reconnect currently uses Rust `ResumeCursor` as subscriber-connection
shipped-state: it records what that connection has already received so a
runtime-local reconnect can catch up from the cursor. This is separate from
`WireSession` metadata, which the byte transport adapter enforces when an
expected session is configured: missing, wrong-identity, stale-epoch, and
wrong-session frames fail admission with structured wire errors before semantic
sync messages are emitted. These are still runtime-local shipped-state and
admission scaffolds, not durable network resume credentials. The session
protocol still needs to specify portable session credentials, resume
acceptance/rejection, auth expiry, and unsupported-feature diagnostics through
`Hello`, message, and error frames.

The message variants and their payloads are:

| message                                                                    | direction      | payload                                                                                                                                                                                      |
| -------------------------------------------------------------------------- | -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CommitUnit`                                                               | up             | `{ tx: Transaction, versions: Vec<VersionRecord> }`                                                                                                                                          |
| `FateUpdate`                                                               | down           | `{ tx_id, fate, global_seq: Option<GlobalSeq>, durability: Option<DurabilityTier> }`                                                                                                         |
| `RegisterShape`                                                            | up             | `{ shape_id, ast: ShapeAst, opts: RegisterShapeOptions }`                                                                                                                                    |
| `BindingDelta`                                                             | up             | `{ shape_id, adds: Vec<(BindingId, Vec<Value>)>, removes: Vec<BindingId> }`                                                                                                                  |
| `ViewUpdate`                                                               | down           | `{ subscription: SubscriptionKey, reset_result_set: bool, version_bundles: Vec<VersionBundle>, peer_payload_inventory: PeerPayloadInventory, result_row_adds/removes: Vec<ResultRowEntry> }` |
| `Rehydrate`                                                                | up (request)   | `{ subscription: SubscriptionKey }`                                                                                                                                                          |
| `FetchContentExtent` / `ContentExtents`                                    | bulk lane      | `{ row, extent }` / `{ extents: Vec<ContentExtent> }`                                                                                                                                        |
| `PublishSchema` / `PublishLens` / `SetCurrentWriteSchema` / `CatalogueAck` | catalogue lane | ch. 10                                                                                                                                                                                       |

A `VersionBundle`, carried in `ViewUpdate.version_bundles`, is `{ tx, versions,
fate, global_seq, durability }`: a settled **view payload bundle** with the fate
state observed when it shipped. A bundle may cover a complete transaction, a
partial mergeable transaction, or the row/version witnesses that make an
exclusive transaction complete for this subscription view. A bundle whose
`versions.len() == tx.n_total_writes` is also a complete transaction payload and
may enter the peer's complete-transaction-payload inventory for later dedup.

## 8.2 Upstream: commit units

Upstream sync moves committed history, not in-progress edits. A committed
transaction travels as one atomic commit unit
(`SyncMessage::CommitUnit { tx, versions }`); open state never ships (ch. 3,
`INV-TX-2`).

Commit-unit delivery is idempotent by `tx_id`. If a known `tx_id` arrives with a
conflicting payload, the receiver rejects it as `ConflictingCommitUnit`
(`INV-TX-4`). The transaction's `n_total_writes` must equal the number of version
records in the unit (`INV-TX-3`). If the unit references parents, schema
versions, or content that the receiver does not yet know, the receiver parks the
unit until those dependencies arrive (`INV-TX-5`).

## 8.3 Fates downstream

Downstream fate messages tell peers how an already-submitted transaction has
settled. A verdict travels as
`SyncMessage::FateUpdate { tx_id, fate, global_seq, durability }`.

The `durability` field is an optional _claim_. A receiver raises observed
durability monotonically only when the message carries `Some(_)`; `None` leaves
the observed durability unchanged. A receiver also never moves `global_seq`
backward (`INV-SYNC-5`). When an authority accepts a commit, it assigns a
monotone `GlobalSeq` that advances the allocator and watermark (ch. 3,
`INV-TX-11`) and maintains the global-current tables and change stream (ch. 4).

## 8.4 Downstream: query-driven view updates

Downstream sync is driven by subscriptions rather than by raw transaction
broadcasts (ch. 6). Each view update applies to one
`SubscriptionKey { shape_id, binding_id }`, so peers receive the settled rows and
versions that are visible through that specific shape binding. Three protocol
rules govern these updates:

- View updates carry **accepted/settled state only** — pending versions are
  visible only on the creating node and are never emitted to non-origin peers
  (`INV-SYNC-12`).
- Result sets are **row-grained**: `result_row_adds`/`removes` entries are
  `(table, row_uuid, content_tx_id)`, not transaction-grained membership
  (`INV-SYNC-7`).
- Payload dedup is **per peer identity** for complete transaction payloads: once
  a peer has received all versions for a transaction, later mentions ride in
  `peer_payload_inventory.complete_tx_payloads: Vec<TxId>`. Those tx ids are
  peer payload inventory refs for complete transaction payloads only, not a
  coarse peer-known version set. Partial bundles, including mergeable and
  view-complete exclusive bundles, establish only their explicit row-version or
  view-scoped payload coverage; they do not establish complete-transaction
  payload coverage. A receiver rejects a `ViewUpdate` naming any inventory ref,
  add, or remove transaction it does not know enough to resolve for that
  subscription view
  (`INV-SYNC-8`, `INV-SYNC-9`).

Protocol state deliberately keeps four facts separate: concrete row-version
payloads received in bundles, transaction existence/metadata (`Transaction` by
`TxId`), full transaction-payload coverage
(`peer_payload_inventory.complete_tx_payloads`), and subscription-scoped
exclusive completeness. The last one is a visibility rule for a particular view,
not a reusable tx-level reference.

_Further invariants._ `INV-SYNC-17` — a result add carries enough
deletion-register witness to reconstruct the row's visible presence/absence.
`INV-SYNC-20` — incremental view updates are observationally equivalent to a full
rehydrate for the same `(shape_id, binding_id)` (ch. 6).

## 8.5 Rehydrate

Rehydration gives a peer a complete subscription result when incremental state is
not enough, such as after reconnect or result-set loss. A `Rehydrate` request
rebuilds the subscription with a response that sets `reset_result_set = true` and
provides a complete replacement. Applying that response clears the receiver's
settled subscription result set before applying the replacement rows (`INV-SYNC-10`),
because removals against a discarded server-side result set are no longer
expressible. Per-peer payload dedup survives when peer state survives, even as
the per-subscription result set is rebuilt (`INV-SYNC-11`).

## 8.6 Policy narrowing in sync

Sync never emits view material before applying the receiving peer's read policy.
During view construction, the peer identity's policy is checked before any result
entry, bundle, ref, or content extent is emitted (`INV-SYNC-13`, ch. 7).
Revocation affects future delivery: it removes a row from future settled result
sets but never redacts an already-delivered local copy (`INV-SYNC-14`).

## 8.7 Partial vs atomic delivery

Downstream delivery preserves view visibility, not transport completeness. A
mergeable transaction may be delivered and applied **partially**: each visible
mergeable version contributes independently (`INV-SYNC-16`). Exclusive payloads
may also be partial at the transaction level and may be stored immediately, but
each maintained subscription view exposes exclusive result rows only when the
payload required by that view is complete. This is a **view-complete exclusive
payload**, not necessarily a complete transaction payload. Otherwise the payload
remains stored but invisible for that view (`INV-SYNC-15`, ch. 3, ch. 7).

The implemented peer payload inventory is deliberately narrow:
`peer_payload_inventory.complete_tx_payloads: Vec<TxId>` names only complete
transaction payload coverage, not broad "known versions" and not partial row
payload coverage. If partial payload dedup becomes necessary, extend the
inventory with explicit row-version coverage and maintained-view-complete
exclusive coverage facts instead of reusing `complete_tx_payloads` for those
meanings.

The postcard `WireFrame`/`WireEnvelope` format and groove row `Record` encoding
do not change when future inventory fields are added.

## 8.8 Edge mergeable fate deferral and permission-scope subscriptions

An edge that acts as mergeable fate authority needs the relevant policy data
before it can decide a write's fate. It therefore must defer fate assignment
until the relevant **permission-scope subscription** has settled; until then it
stores the unit as pending relay history and defers (`INV-SYNC-18`).

A permission-scope subscription is an _upstream_ subscription opened by the edge
against core for the policy data required by its acceptance gate. It is keyed by
`(policy_shape, writer_claim)` (ch. 9 §9.5): the write policy's query shape bound
to the writer's `claim("sub")`. This hydrates only the policy rows that writer's
writes can depend on, never a whole table.

Many writers' policies read overlapping data, so permission scopes are
**deduplicated at the sync level**. Identical `(policy_shape, writer_claim)`
scopes resolve to a single upstream subscription whose settled result fans out to
every acceptance gate that needs it. The edge reference-counts gate dependents so
the upstream subscription is dropped only when the last dependent goes away
(`INV-SYNC-22`). A broader _covering_ scope can satisfy a narrower one when the
covering relation says it does. This is the same per-peer payload dedup machinery
(§8.4) applied to the edge's own upstream reads. The full edge-tier semantics —
staleness horizon, rehydration, eviction — are chapter 9.

## 8.9 Content extents and catalogue lanes

Large-value content uses a bulk lane rather than being forced through ordinary
view payloads. A `FetchContentExtent` request is authorized against row context
and read policy: an extent whose row mismatches the request or is not visible to
the peer is refused (`INV-SYNC-19`, ch. 12). Catalogue messages
(`PublishSchema`, `PublishLens`, `SetCurrentWriteSchema`, `CatalogueAck`) share
this protocol lane; their semantics are chapter 10.

_Further invariants._ `INV-SYNC-21` — wire `TxId` and row-version payloads use
node UUIDs and schema-version IDs, never node-local integer aliases (ch. 2).

## Open questions

- 🔶 **Cross-language wire envelope completion.** `WireFrame`/`WireEnvelope`
  now establish a postcard-first binary frame carrying protocol version, feature
  bits, optional enforced session metadata, structured errors, and an encoded
  `SyncMessage` payload. Before TS/WASM/NAPI/server integration treats this as
  frozen, the remaining envelope work is trace/replay ids, portable resume
  cursor acceptance/rejection, auth expiry, and unsupported-feature diagnostics.
- 🔶 **Canonical fixtures.** The wire contract needs golden encode/decode
  fixtures for every message family, including `CommitUnit`, `FateUpdate`,
  `RegisterShape`/`BindingDelta`, `ViewUpdate`, rehydrate, content extents, and
  catalogue/lens lanes, with explicit coverage that row/version payload bytes
  remain custom `Record` payloads under the postcard envelope. Fixtures should
  be consumable from Rust and TypeScript before the TS API binds to live
  transports.
- 🔶 **Transport state.** The current binding-facing send/poll surface can
  express "send" and "no message staged"; it cannot express closed/error/
  backpressure, auth expiry, protocol-version mismatch, or resume-cursor
  rejection. Define the protocol state machine here and expose the ergonomic
  binding surface in ch. 13.
- 🔶 **Content-extent fetch authorization** (`INV-SYNC-19`) has implementation but
  no direct test found — `untested` until covered.
- 🔶 **Client/edge/core rollout.** The protocol design is the same message
  vocabulary across UI ↔ worker, worker ↔ edge, and edge ↔ core. Current
  implementation and simulation are staged toward client ↔ edge ↔ core: the
  ordinary committed-unit path still often behaves as client ↔ core, while
  edge-client links, permission-scope deferral, and edge durability are being
  exercised toward the full topology.
- 🔶 **Parked-unit persistence.** Authority parking is in-memory and relies on
  client retry after restart; persisted parked units are not implemented. Decide
  whether ch. 8 states this as an implementation limitation or defers to
  durability/topology.
- 🔶 **View options & payload-inventory resubscribe.** `RegisterShapeOptions` is
  currently empty; `rows full` / `history shallow` view options, `history: full`,
  and delta-resubscribe from a peer payload inventory are reserved vocabulary,
  not an implemented wire contract. This should be specified independently from
  snapshot refs, which are read frontiers rather than peer payload inventories.
  Decide how much to pin now.
- 🔶 **Relay upstream aggregation** onto coarser covering shapes is the design;
  implementation does not make it a MUST yet (ch. 6, ch. 9).
- 🔶 **Covering-scope subsumption** is the design for broader permission scopes
  satisfying narrower ones; the implementation has exact-key sharing only, with
  no covering relation yet.
