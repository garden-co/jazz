# jazz — Specification · 8. Sync protocol

## Overview

One protocol carries everything between nodes. This chapter defines that peer
protocol: how writes travel up as commit units, how fates and query-driven view
updates travel down, how payloads are deduplicated and rehydrated, and how
mergeable vs exclusive transactions are delivered. It ties together transactions
(ch. 3), history (ch. 4), queries (ch. 6), and authorization (ch. 7); the
deployment roles are chapter 9.

Invariant digest:

- `INV-SYNC-5`: A receiver applying a fate update MUST NOT move globalseq backward and MUST raise observed durability only by a supplied Some(DurabilityTier) claim using monotone max...
- `INV-SYNC-7`: A ViewUpdate result set MUST be member-grained for result membership and typed-fact-grained for non-row program facts; it MUST NOT model subscription membership as a t...
- `INV-SYNC-8`: A view server MUST use peerpayloadinventory.completetxpayloads only for tx-level complete payloads covered by the peer payload inventory; payload dedup MUST be peer-sc...
- `INV-SYNC-9`: A receiver MUST reject a ViewUpdate that names a peerpayloadinventory.completetxpayloads, add, or remove transaction it lacks enough tx existence, row-version payload,...
- `INV-SYNC-10`: A reset-result-set ViewUpdate MUST set resetresultset = true; applying it MUST clear the receiver's settled subscription result set before applying the replacement res...
- `INV-SYNC-11`: Reset-result-set ViewUpdates MUST preserve per-peer payload dedup when peer state survives, while resending the subscription result set as a complete replacement.
- `INV-SYNC-12`: Downstream subscription view updates MUST contain accepted/settled state only and MUST NOT emit pending versions to non-origin peers.
- `INV-SYNC-13`: Downstream view construction MUST apply the peer identity's read policy before emitting result-set entries, version bundles, complete tx payload refs, or content extents.
- `INV-SYNC-14`: A read-policy revocation MUST remove the affected row from future settled subscription result sets but MUST NOT require redaction of previously delivered local copies.
- `INV-SYNC-15`: Exclusive transaction payloads MAY be delivered, stored, and participate partially at the transaction level; receiver-visible subscription state MUST expose them only...
- `INV-SYNC-16`: A mergeable transaction MAY be delivered and applied partially; each visible mergeable version can contribute without waiting for tx.ntotalwrites.
- `INV-SYNC-17`: ViewUpdate emission for a result add MUST include enough deletion-register context to reconstruct visible absence/presence for that row.
- `INV-SYNC-18`: An edge acting as mergeable fate authority MUST defer fate assignment until the relevant permission-scope subscription has settled for the writer and affected tables.
- `INV-SYNC-19`: FetchContentExtent handling MUST reject an extent whose row context mismatches the requested row or whose content is not visible to the peer identity.
- `INV-SYNC-20`: Incremental query view updates MUST be observationally equivalent to a full rehydrate for the same canonical program instance, including enter/leave churn within a sin...
- `INV-SYNC-21`: Wire TxId and row-version payloads MUST use node UUIDs and schema version IDs, not node-local integer aliases.
- `INV-SYNC-22`: An edge's upstream permission-scope subscriptions MUST be deduplicated at the sync level: identical or covering (policyshape, writerclaim) scopes share one upstream su...
- `INV-SYNC-23`: A serving peer MUST reject a capability-gapped live subscription with SyncMessage::SubscribeRejected addressed to the requested SubscriptionKey; the rejected subscript...
- `INV-SYNC-24`: Known-state payload dedup MUST omit only version bodies, never result membership, program facts, or inventory refs; a body may be omitted only under the skip rule — be...
- `INV-SYNC-25`: A stream served under known-state dedup followed by its repair responses MUST be observationally equivalent to the same stream served without dedup.
- `INV-SYNC-26`: A receiver detecting a referenced version without its body MUST be able to request exactly those (table, rowuuid, txtime, txnodeid) payloads, and the server MUST serve...
- `INV-SYNC-27`: A fast known-state declaration MUST only be made for contiguously applied, unevicted served streams; any local eviction touching stored row-version bodies invalidates...
- `INV-TX-2`: Committing an exclusive transaction MUST store the commit locally as Fate::Pending with DurabilityTier::Local and emit exactly one SyncMessage::CommitUnit.
- `INV-TX-3`: A commit unit whose Transaction.ntotalwrites does not equal the delivered version count MUST be rejected by the fate authority as RejectionReason::MalformedCommit(...)...
- `INV-TX-4`: Duplicate commit units with identical payloads MUST be idempotent and return the already-known fate; duplicate units with conflicting payloads MUST fail as Error::Conf...
- `INV-TX-5`: The authority MUST park a commit unit with missing parent/schema/content prerequisites and MUST decide it only after all prerequisites are present.
- `INV-TX-11`: Accepted authority commits MUST receive the next GlobalSeq, advance the allocator/watermark, and report DurabilityTier::Global.
- `INV-TX-23`: Fate authority MUST be structurally wired by the host. Applying a bare unfated commit unit on a non-authority sync path MUST stage or park it pending remote fate; it M...

## Details

### 8.1 One protocol, roles not code

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

Binding subscription row batches carry core-assigned positions alongside each
row identity. The wasm/native row payload is `{ row_id, index, deleted, raw }`;
removed rows are `{ table, row_id, index }`. The index is the zero-based
application-result position after additions/updates and before removals. Host
bindings must reduce these positions verbatim; they may transport, cache, or
batch the bytes, but must not recompute semantic result order from row values.
This binding ABI is downstream of `SyncMessage::ViewUpdate`: peer wire remains
member-grained (`ResultMemberEntry` adds/removes plus program facts), while the
Db subscription stream maps maintained root deltas to positional application
events.

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

| message                                                                    | direction      | payload                                                                                                                              |
| -------------------------------------------------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `CommitUnit`                                                               | up             | `{ tx: Transaction, versions: Vec<VersionRecord> }`                                                                                  |
| `FateUpdate`                                                               | down           | `{ tx_id, fate, global_seq: Option<GlobalSeq>, durability: Option<DurabilityTier> }`                                                 |
| `RegisterShape`                                                            | up             | `{ shape_id, ast: ShapeAst, opts: RegisterShapeOptions }`                                                                            |
| `Subscribe`                                                                | up             | `{ shape_id, subscription: SubscriptionKey, values: Vec<Value> }`                                                                    |
| `SubscribeRejected`                                                        | down           | `{ subscription: SubscriptionKey, reason: SubscribeRejectReason }`                                                                   |
| `Unsubscribe`                                                              | up             | `{ subscription: SubscriptionKey }`                                                                                                  |
| `ViewUpdate`                                                               | down           | `{ subscription, reset_result_set, version_bundles, peer_payload_inventory, result_member_adds/removes, program_fact_adds/removes }` |
| `FetchContentExtent` / `ContentExtents`                                    | bulk lane      | `{ owner: LargeValueOwnerRef, extent }` / `{ extents: Vec<ContentExtent> }`                                                          |
| `PublishSchema` / `PublishLens` / `SetCurrentWriteSchema` / `CatalogueAck` | catalogue lane | ch. 10                                                                                                                               |

A `VersionBundle`, carried in `ViewUpdate.version_bundles`, is `{ tx, versions,
fate, global_seq, durability }`: a settled **view payload bundle** with the fate
state observed when it shipped. A bundle may cover a complete transaction, a
partial mergeable transaction, or the row/version witnesses that make an
exclusive transaction complete for this subscription view. A bundle whose
`versions.len() == tx.n_total_writes` is also a complete transaction payload and
may enter the peer's complete-transaction-payload inventory for later dedup.

### 8.2 Upstream: commit units

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

Receiving a bare unfated commit unit is not authority. On a non-authority node,
`apply_sync_message` stages or parks that commit unit as pending remote fate and
waits for a `FateUpdate`; it must not accept the unit, assign global sequence, or
create merge versions from it (`INV-TX-23`). Only a structurally wired fate
authority path may decide fate (ch. 3 §3.6, ch. 9).

### 8.3 Fates downstream

Downstream fate messages tell peers how an already-submitted transaction has
settled. A verdict travels as
`SyncMessage::FateUpdate { tx_id, fate, global_seq, durability }`.

The `durability` field is an optional _claim_. A receiver raises observed
durability monotonically only when the message carries `Some(_)`; `None` leaves
the observed durability unchanged. A receiver also never moves `global_seq`
backward (`INV-SYNC-5`). When an authority accepts a commit, it assigns a
monotone `GlobalSeq` that advances the allocator and watermark (ch. 3,
`INV-TX-11`) and maintains the global-current tables and change stream (ch. 4).

### 8.4 Downstream: query-driven view updates

Downstream sync is driven by subscriptions rather than by raw transaction
broadcasts (ch. 6). Each view update applies to one
`SubscriptionKey { shape_id, binding_id, read_view }`, so peers receive the
settled rows and versions that are visible through that specific usage-site
shape binding and read-view identity. Three protocol rules govern these updates:

- View updates carry **accepted/settled state only** — pending versions are
  visible only on the creating node and are never emitted to non-origin peers
  (`INV-SYNC-12`).
- Result sets are **member-grained**: the ordinary current-row projection is
  `(table, row_uuid, content_tx_id)`, but protocol-visible membership is typed
  `ResultMemberEntry` data. Real-row members carry source/read-view,
  content/deletion layer, optional deletion tx, schema, branch/prefix, batch,
  and digest dimensions when those dimensions participate in identity.
  Synthetic aggregate/window rows and path tuple rows use the same member set
  rather than another result-set engine (`INV-SYNC-7`).
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

Protocol state deliberately keeps facts separate: concrete row-version payloads
received in bundles, transaction existence/metadata (`Transaction` by `TxId`),
non-versioned synthetic result payloads (`ResultPayload` program facts keyed by
typed result member), full transaction-payload coverage
(`peer_payload_inventory.complete_tx_payloads` / `CompleteTxPayloadCoverage`),
subscription-scoped exclusive completeness (`ViewCompleteExclusiveCoverage`),
source/read-frontier coverage, policy decisions/witnesses, predicate output
sets, and large-value extents. Subscription-scoped exclusive completeness is a
visibility rule for a particular view, not a reusable tx-level reference.

Receiver apply is single-mode at the semantic boundary. For each receiver apply
boundary, the runtime drains repair-clean inbound view updates, stages all bundle
effects in one storage batch, commits once, and therefore runs one IVM tick for
that receiver boundary. Per-link FIFO order is preserved while staging bundle
effects; cross-subscription ordering inside the same receiver tick carries no
protocol meaning beyond that FIFO stream.

The staged batch provides read-your-own-write behavior while the receiver
boundary is being built. That matters for same-tick transaction+fate delivery,
multiple transactions in one boundary competing for a row's current winner, and
ahead-overlay cleanup retractions following fate application.

Reset view updates keep their wire form, but the receiver internalizes them as
deltas: retract the previous result set for that subscription, then apply the
reset's adds and coverage/settlement state. A reset is not a separate storage
mode. Serve-dirty marking is also a receiver-boundary effect: if applying the
staged batch can change what any downstream subscriber would be served, the
subscriber connections are marked dirty at the same boundary as cache
invalidation and applied-global-sequence bookkeeping.

The current receiver direction is no separate bulk/non-bulk correctness mode, no
eligibility list that decides whether bundles bypass deltas, and no hidden
preloaded-transaction suppression that can starve maintained views. Bulk
shortcuts may return only as optimizations on top of the same staged-delta
semantics. The July 2026 receiver-batch receipts were the forcing function:
client per-bundle ingest collapsed to one commit/tick per receiver burst, and
admin 10% cold improved from the 60.7s baseline to about 5.0s once
staged-overlay point and prefix reads were indexed.

Planned consolidation: delete the remaining reset-specific bulk bypass, delete
initial-hydration eligibility state that only exists to select a bypass, delete
preloaded-transaction suppression once all reset snapshots use explicit
retractions, and move the receiver boundary onto an `OrderedKvStorage`
transaction once that storage transaction exists.

_Further invariants._ `INV-SYNC-17` — a result add carries enough
deletion-register witness to reconstruct the row's visible presence/absence.
`INV-SYNC-20` — incremental view updates are observationally equivalent to a full
reset `ViewUpdate` for the same canonical program instance (ch. 6).

### 8.5 Subscription Attach, Reset, And Detach

`Subscribe` attaches one usage-site subscription id to a registered shape and a
binding value vector. A peer may register the same `shape_id` under multiple
serving option sets; the serving side selects the option set by
`Subscribe.subscription.read_view`, the `ReadViewKey` derived from the resolved
read identity. The serving side groups subscriptions by canonical program
instance `(shape, resolved_read, policy, binding)` and maintains one shared view
for that key, then fans `ViewUpdate`s out to each usage-site `SubscriptionKey`. Remote serving
options are settled-only: `Local`/`None` are link-local facade tiers and must be
normalized before propagation or rejected by a serving peer. A new usage-site
subscription always receives a complete replacement response with
`reset_result_set = true`; later updates may be incremental. Applying a reset
response clears the receiver's settled subscription result set before applying
the replacement rows (`INV-SYNC-10`), because removals against a discarded
server-side result set are no longer expressible.

If a `Subscribe` request cannot be served because the registered shape/read-view
has a permanent maintained-subscription capability gap, the serving peer replies
with `SyncMessage::SubscribeRejected { subscription, reason }` addressed to the
same `SubscriptionKey`. The initial reason vocabulary is
`SubscribeRejectReason::UnsupportedShapeCapability { detail }`; `detail` is
human-readable diagnostic text mapped at the serving boundary, not the internal
lowering `CapabilityReport`. After `SubscribeRejected`, that subscription is not
active, the requester must not expect `ViewUpdate`s for it, and `Unsubscribe`
for the same key is a no-op. The connection and any other subscriptions on it
remain live (`INV-SYNC-23`).

`Unsubscribe` detaches one usage-site subscription. When the last usage-site
subscription for a canonical program instance detaches, the serving side may drop
the shared maintained view and its runtime subscription state. Per-peer payload dedup
survives view reset and detach while peer state survives (`INV-SYNC-11`).

### 8.6 Policy narrowing in sync

Sync never emits view material before applying the receiving peer's read policy.
During view construction, the peer identity's policy is checked before any result
entry, bundle, ref, or content extent is emitted (`INV-SYNC-13`, ch. 7).
Revocation affects future delivery: it removes a row from future settled result
sets but never redacts an already-delivered local copy (`INV-SYNC-14`).

### 8.7 Partial vs atomic delivery

Downstream delivery preserves view visibility, not transport completeness. A
mergeable transaction may be delivered and applied **partially**: each visible
mergeable version contributes independently (`INV-SYNC-16`). Exclusive payloads
may also be partial at the transaction level and may be stored immediately, but
each maintained subscription view exposes exclusive result members only when the
payload required by that view is complete. This is a **view-complete exclusive
payload**, not necessarily a complete transaction payload. Otherwise the payload
remains stored but invisible for that view (`INV-SYNC-15`, ch. 3, ch. 7).

The implemented peer payload inventory is deliberately narrow:
`peer_payload_inventory.complete_tx_payloads: Vec<TxId>` names only complete
transaction payload coverage, not broad "known versions" and not partial row
payload coverage. Partial and version-level dedup is the committed known-state
design (§8.11), which retires this inventory rather than extending it.

The postcard `WireFrame`/`WireEnvelope` format and groove row `Record` encoding
do not change when future inventory fields are added.

### 8.8 Protocol size limits

Protocol size limits are enforced at the layer that can recover correctly:

- An encoded `WireFrame` is capped at 2 MiB and an encoded
  `WireEnvelope.payload` / `SyncMessage` is capped at 2 MiB. These are
  wire-admission limits: an over-limit frame or payload is rejected before
  postcard decodes the bytes and produces a structured
  `WireError { code: MalformedFrame, retry: Never, ... }`. The connection-level
  admission failure closes or resumes according to the binding's normal
  structured-error handling; no semantic message is applied.
- A `RegisterShape` AST is capped at 64 KiB encoded. This is a semantic
  admission limit for the shape-registration request; the connection may
  continue after the rejected request. Server shells may expose this as
  configuration later for unusually large generated query shapes.
- A `CommitUnit` is capped at 4096 row-version records and 2 MiB encoded. These
  are transaction semantic limits: an over-limit commit unit is rejected as
  `Fate::Rejected(MalformedCommit(_))`, the connection remains live, and later
  well-formed commit units may still settle.
- A `ContentExtent` response is capped at 1 MiB of bytes per extent. This is a
  bulk-lane semantic admission limit: it is comfortably above ch. 12's current
  ~64 KiB blob chunk target while preventing one content response from becoming
  an unbounded allocation. The content lane may split larger values into
  multiple extents.

Outbound websocket batching is byte-budgeted by the same 2 MiB encoded-frame
limit: senders split batches across multiple binary messages instead of relying
on the historical count-only batch limit. If a single encoded `WireFrame` cannot
fit the budget, the sender must fail loudly rather than truncate or silently
drop it.

**Wire encoding posture (target optimization guidance).** High-rate serial
transactions (keystroke-grade chains: same author, same row, near-monotone
times) make consecutive sync messages highly redundant. The wire harvests that
redundancy generically, in two layers, rather than by introducing run-shaped
message semantics: (1) **per-connection stream compression** — a compression
context that persists across frames on one transport, so cross-message
repetition (subscription keys, row ids, authors, adjacent timestamps)
compresses without any wire-format change; and (2) **columnar `ViewUpdate`
internals** — a reserved append-only message variant whose member/bundle
payloads are column-encoded (the groove ch. 2 §2.9 window codec applied to a
message body). A lone single-edit transaction with nothing before or after it
pays full framing and transaction overhead by design — it is lone precisely
when there is nothing to amortize against. Windowed _storage_ representation
(groove ch. 2 §2.9) is never a wire obligation: the wire ships logical
messages; storage and transport each compress in their own layer.

Native transports advertise zstd-3 stream compression by default when the
feature is compiled in. WASM/browser artifacts keep transport compression
opt-in so bundle-size trade-offs stay explicit; reconnect resets the compression
context and relies on known-state redelivery for correctness.

### 8.9 Edge mergeable fate deferral and permission-scope subscriptions

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

### 8.10 Content extents and catalogue lanes

Large-value content uses a bulk lane rather than being forced through ordinary
view payloads. A `FetchContentExtent` request is authorized against row context
and read policy: an extent whose row mismatches the request or is not visible to
the peer is refused (`INV-SYNC-19`, ch. 12). Catalogue messages
(`PublishSchema`, `PublishLens`, `SetCurrentWriteSchema`, `CatalogueAck`) share
this protocol lane; their semantics are chapter 10.

_Further invariants._ `INV-SYNC-21` — wire `TxId` and row-version payloads use
node UUIDs and schema-version IDs, never node-local integer aliases (ch. 2).

### 8.11 Known state: reconnect declarations and payload dedup

Steady-state and reconnect payload dedup is built on three properties the
protocol already has: the **client is the sole authority on what it durably
holds**; every `ViewUpdate` is **self-auditing** because it references the row
versions it treats as in scope, so a receiver structurally detects
"referenced without body" at apply time; and the serving side may therefore
model receiver knowledge **optimistically**, updating its model at emission
time with no acknowledgement traffic. There is no durable-apply ack and the
`Hello` handshake does not carry knowledge state; declarations ride per query.

A subscriber declares its known state per usage-site query in one of two forms:

- **Fast declaration** — `(shape, binding, completeness class, position p)`:
  "I have contiguously applied the stream you served me for this query through
  global position `p`, and none of it has been locally evicted." In the current
  implementation `p` is the exact `settled_through` stamp previously emitted by
  the serving node for the same canonical binding view. The client records and
  persists this cursor when applying `ViewUpdate`s and echoes it on resubscribe.
  Any local eviction touching stored row-version bodies invalidates persisted
  fast facts before another declaration can be made (`INV-SYNC-27`).
- **Slow declaration** — an explicit set of row-version identities
  `(row_uuid, tx_time, tx_node_id)`: used when no valid fast fact exists
  (fresh store, eviction, corruption). The client evaluates the query locally
  and declares exactly the versions it holds. Oversized exact declarations
  degrade to no declaration and a full ship; they are never truncated because a
  partial exact declaration would silently overclaim. Version identities use the
  wire `TxId` form (`INV-SYNC-21`); unfated versions are declarable because
  `TxId`s exist before fate.

Every `ViewUpdate` carries `settled_through`, the serving node's applied global
watermark when the update was assembled. Its meaning is per binding view: this
update reflects every global change at or before that position for the served
view. A stale cursor can under-claim knowledge and cause extra bodies to ship;
it cannot over-claim because rows entering the view after `p` have membership
settle positions after `p`, and therefore do not satisfy the skip rule below.

The serving side's skip rule is one comparison (`INV-SYNC-24`): a version body
may be omitted iff the receiver's membership in it is believed — "row in the
query's scope now" under a fast declaration, exact set membership under a slow
declaration — and, for fast declarations, the version settled at or before
`p`. Not-yet-fated versions are always shipped under a fast declaration.
Result membership, program facts, and inventory refs are never omitted — only
payload bodies.

The optimism is bounded by two nets. First, the structural integrity check: a
receiver that encounters a referenced version without holding its body treats
this as a **known-state miss**, not an error. Second, the precise repair
request: the receiver requests exactly the missing `(row_uuid, tx_time,
tx_node_id)` payloads, and the server MUST serve them subject to ordinary read
policy (`INV-SYNC-26`). Convergence is preserved: a stream served under
known-state dedup followed by its repairs MUST be observationally equivalent
to the same stream served without dedup (`INV-SYNC-25`, cf. `INV-SYNC-20`).
The canonical repair-carrying case is visibility gained without a new version
being minted — a policy/membership change admitting rows whose versions settled
at or before `p` (ch. 7); version-minting scope entry is self-consistent
because the entering version settles above `p`.

Holdings from point-in-time reads dedup conservatively: a version is assumed
held only for rows **unchanged since the declared cut** (current version
settled at or before the cut). The serving side never reconstructs historical
winners for dedup — that is a per-row history walk (O(history) reads), and for
current-view serving it buys nothing: a row changed since the cut must ship
its current version regardless.

This section is the committed replacement for extending
`peer_payload_inventory.complete_tx_payloads` toward partial or version-level
coverage (§8.4, §8.7): the complete-tx inventory remains the implemented
mechanism for non-declared streams, and it is retired rather than extended as
known-state coverage grows.

_Further invariants._ `INV-SYNC-24` — fast and slow declarations omit only
eligible version bodies; `INV-SYNC-25` — dedup + repairs converge to the
undeduped stream; `INV-SYNC-26` — repair requests are exact and policy-checked;
`INV-SYNC-27` — persisted fast declarations require contiguous application and
no eviction; eviction invalidates the persisted fact. Persisting slow exact
declarations is intentionally not part of v1; they are derived from the
receiver's current local store when needed.

### 8.13 Subsumed sync and wire notes

The former SyncManager and query/sync integration notes are folded here as the
same protocol-level rule: subscriptions are desired-state declarations over
validated shapes and bindings, not a separate query transport. A peer registers
the shape, subscribes the binding, receives an initial coverage result, and then
receives live updates driven by maintained-view state (ch. 16). Reconnect should
replay desired subscriptions and locally-authored pending commit units before
falling back to broader snapshots.

There is one wire vocabulary across network links and worker bridges. Browser
main-thread to worker communication may use `postMessage` as a carrier, but the
semantic payload should remain the same wire-frame/SyncMessage envelope used by
network sync. Transport-local batching, compression, and resume metadata must
not leak into row/version encoding.

## Open Questions

### Open questions

- 🔶 **Receiver storage transaction surface.** The receiver boundary currently
  uses the core staged-batch seam. The end state is an `OrderedKvStorage`
  transaction surface with the same staged read-through and single-commit
  semantics, so receiver apply does not need a Jazz-side accumulator.
- 🔶 **Cross-language wire envelope completion.** `WireFrame`/`WireEnvelope`
  now establish a postcard-first binary frame carrying protocol version, feature
  bits, optional enforced session metadata, structured errors, and an encoded
  `SyncMessage` payload. Before TS/WASM/NAPI/server integration treats this as
  frozen, the remaining envelope work is trace/replay ids, portable resume
  cursor acceptance/rejection, auth expiry, and unsupported-feature diagnostics.
  **Alpha compatibility policy (decided 2026-07-02):** alpha releases make no
  cross-version protocol or storage compatibility promise; version tags exist so
  mismatch is a clean, diagnosable refusal, never corruption or silent
  misbehavior. Breaking is permitted but best-effort avoided; compatibility
  windows are a beta-era policy.
- 🔶 **Canonical fixtures.** The wire contract needs golden encode/decode
  fixtures for every message family, including `CommitUnit`, `FateUpdate`,
  `RegisterShape`/`Subscribe`/`Unsubscribe`, `ViewUpdate`, content extents, and
  catalogue/lens lanes, with explicit coverage that row/version payload bytes
  remain custom `Record` payloads under the postcard envelope. Fixtures should
  be consumable from Rust and TypeScript before the TS API binds to live
  transports.
- 🔶 **Transport state.** The current binding-facing send/poll surface can
  express "send" and "no message staged"; it cannot express closed/error/
  backpressure, auth expiry, protocol-version mismatch, or resume-cursor
  rejection. Define the protocol state machine here and expose the ergonomic
  binding surface in ch. 13.
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
- 🔶 **View options.** `RegisterShapeOptions` currently carries serving tier
  plus semantic read-view request. Richer row and history materialization
  options (`rows full`, `history shallow`, `history full`) are reserved
  vocabulary, not an implemented wire contract. (Delta-resubscribe is no longer
  open: it is the known-state design, §8.11.)
- 🔶 **Relay upstream aggregation** onto coarser covering shapes is the design;
  implementation does not make it a MUST yet (ch. 6, ch. 9).
- 🔶 **Covering-scope subsumption** is the design for broader permission scopes
  satisfying narrower ones; the implementation has exact-key sharing only, with
  no covering relation yet.
- 🔶 **Worker bridge carrier unification.** Replace bespoke worker messages with
  the same core wire-frame batches carried over WebSockets, while preserving the
  worker bridge's different disconnect and lifecycle semantics.
- 🔶 **Upstream-open signaling.** Binding surfaces need an explicit connected /
  handshaking / failed / reconnecting signal before edge/global-tier reads are
  unblocked; a synchronous `connect()` return is not enough.
- 🔶 **Sent-transaction retention.** Per-peer sent-id tracking should be bounded
  by resume/ack state rather than retaining unbounded transaction id history.
- 🔶 **Verbose payload cleanup.** Replayable settlements and wire batches should
  avoid repeating member identity already fixed by the outer transaction or
  envelope, while keeping idempotency and replay diagnostics intact.
- 🔶 **Protocol and storage version tags.** Version mismatches must fail loudly
  and diagnostically across wire envelopes, storage headers, and binding ABI
  fixtures; compatibility windows are a release-policy decision, not an alpha
  promise.
