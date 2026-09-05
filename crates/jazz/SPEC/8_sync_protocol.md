# jazz — Specification · 8. Sync protocol

## Overview

One protocol carries everything between nodes. This chapter defines that peer
protocol: how writes travel up as commit units, how fates and query-driven view
updates travel down, how payloads are deduplicated and rehydrated, and how
mergeable vs exclusive transactions are delivered. It ties together transactions
(ch. 3), history (ch. 4), queries (ch. 6), and authorization (ch. 7); the
deployment roles are chapter 9.

Invariant digest:

- `INV-SYNC-5`: A receiver applying a fate update MUST NOT move `global_time` backward and MUST raise observed durability only by a supplied `Some(DurabilityTier)` claim using monotone max semantics; `None` MUST leave durability unchanged.
- `INV-SYNC-7`: A `ViewUpdate` result set MUST be member-grained for result membership and typed-fact-grained for non-row program facts; it MUST NOT model subscription membership as a transaction-grained set. Ordinary current row entries are `ResultMemberEntry::Row(RealRowMemberEntry)` values with a `(table, row_uuid, content_tx_id)` projection. Synthetic payloads, relation/path, coverage, policy, and predicate material travel as typed `ProgramFactEntry` add/remove deltas. Relation facts MUST carry the dimensions needed by lowering (kind, versions, depth, edge id, branch, role, order, hole state) rather than requiring an opaque side channel.
- `INV-SYNC-8`: A view server MUST use `peer_payload_inventory.complete_tx_payloads` only for tx-level complete payloads covered by the peer payload inventory; payload dedup MUST be peer-scoped, not subscription-scoped, and partial bundles MUST remain eligible for later payload emission until complete-tx payload coverage is established.
- `INV-SYNC-9`: A receiver MUST reject a `ViewUpdate` that names a `peer_payload_inventory.complete_tx_payloads`, add, or remove transaction it lacks enough tx existence, row-version payload, complete-tx payload, or view-complete exclusive payload coverage to resolve for that subscription view.
- `INV-SYNC-10`: A reset-result-set `ViewUpdate` MUST set `reset_result_set = true`; applying it MUST clear the receiver's settled subscription result set before applying the replacement result members and program facts.
- `INV-SYNC-11`: Reset-result-set `ViewUpdate`s MUST preserve per-peer payload dedup when peer state survives, while resending the subscription result set as a complete replacement.
- `INV-SYNC-12`: Downstream subscription view updates MUST contain accepted/settled state only and MUST NOT emit pending versions to non-origin peers.
- `INV-SYNC-13`: Downstream view construction MUST apply the peer identity's read policy before emitting result-set entries, version bundles, or complete tx payload refs.
- `INV-SYNC-14`: A read-policy revocation MUST remove the affected row from future settled subscription result sets but MUST NOT require redaction of previously delivered local copies.
- `INV-SYNC-15`: Exclusive transaction payloads MAY be delivered, stored, and participate partially at the transaction level; receiver-visible subscription state MUST expose them only when complete for the maintained subscription view being served, and partial fragments MUST NOT update whole-database current indexes.
- `INV-SYNC-16`: A mergeable transaction MAY be delivered and applied partially; each visible mergeable version can contribute without waiting for `tx.n_total_writes`.
- `INV-SYNC-17`: `ViewUpdate` emission for a result add MUST include enough deletion-register context to reconstruct visible absence/presence for that row.
- `INV-SYNC-27`: Shared deletion-history storage is local representation only: sync payloads continue to identify deletion versions by logical table, branch key, row, transaction, and schema, and receivers MUST resolve the sender's record through their own stable physical mapping.
- `INV-SYNC-18`: An edge acting as mergeable fate authority MUST defer fate assignment until the relevant permission-scope subscription has settled for the writer and affected tables.
- `INV-SYNC-20`: Incremental query view updates MUST be observationally equivalent to a full rehydrate for the same canonical program instance, including enter/leave churn within a single drain cycle and closure-row replacement.
- `INV-SYNC-21`: Wire `TxId` and row-version payloads MUST use node UUIDs and schema version IDs, not node-local integer aliases.
- `INV-SYNC-22`: An edge MUST share upstream permission-scope subscriptions whenever one settled subscription can satisfy every dependent acceptance gate.
- `INV-SYNC-23`: A serving peer MUST reject a capability-gapped live subscription with `SyncMessage::SubscribeRejected` addressed to the requested `SubscriptionKey`; the rejected subscription MUST NOT become active, `Unsubscribe` for it is a no-op, and the connection MUST keep serving other subscriptions.
- `INV-SYNC-24`: Known-state payload dedup MUST omit only version bodies and MUST preserve result membership, program facts, and inventory refs. A version body MAY be omitted only when the receiver's membership is believed — under a fast declaration, the version also MUST have settled at or before the declared position; not-yet-fated versions MUST be shipped under a fast declaration.
- `INV-SYNC-25`: A stream served under known-state dedup followed by its repair responses MUST be observationally equivalent to the same stream served without dedup.
- `INV-SYNC-26`: A receiver detecting a referenced version without its body MUST be able to request exactly those `(table, row_uuid, tx_time, tx_node_id)` payloads, and the server MUST serve them subject to ordinary read policy. The repair vocabulary and server/client repair helpers are implemented and activated for declared known-state subscriptions.
- `INV-SYNC-27`: A fast known-state declaration MUST only be made for contiguously applied, unevicted served streams; any local eviction touching stored row-version bodies invalidates persisted fast declarations before another declaration can be made.
- `INV-SYNC-29`: A fast known-state declaration carrying authorization progress may suppress a reset for a pre-cursor membership difference only when its server-stamped authorization-progress token matches the serving peer's current token for that reader and canonical binding view. `crates/jazz/src/peer.rs::tests::fast_authorization_progress_bounds_membership_resets` enforces both bounds.
- `INV-SYNC-30`: `settled_through` is a durable canonical-view history cursor for known-state payload dedup and repair, not a subscription or one-shot coverage receipt. Edge/Global settlement and coverage additionally require a fresh confirming `ViewUpdate` from the selected continuously active upstream connection. A new settled one-shot requires confirmation for its exact current usage-site `SubscriptionKey`; an update for a detached predecessor cannot satisfy it even when shape, binding, and options are equal. Disconnect, restart, edge switch, or any update from a nonselected upstream invalidates all selected-authority receipts immediately unless an exact recomputation closure is proven.
- `INV-SYNC-28`: The pre-reconstruction terminal carrier is historical scaffolding and is retired by `INV-SYNC-36`; it is not an authority-output compatibility contract.
- `INV-SYNC-31`: A downstream subscription MUST synchronize canonical authored facts and their identity-preserving witness closure under an exact manifest/epoch/digest, never an application-projected row as replicated truth.
- `INV-SYNC-32`: A receiver MUST select branch-key-qualified authored-history winners before projection, decode each synchronized fact in its authored schema, project it through the ordered catalogue lineage into the subscription read schema, and derive terminal output with its local IVM without supplementing unrelated local history.
- `INV-SYNC-33`: The serving authority MUST decide visibility, membership, and settlement and ship only the safe, complete canonical closure plus identified authorized residual program from which the receiver can reproduce that authorized view; opaque admissions MUST be non-replayable across every authority/view/reader/branch-source/residual identity axis and their protected occurrence plus concrete version/layer witnesses.
- `INV-SYNC-34`: A subscription is settled only when its receiver has verified every class of the complete reproducible input closure for the authority's declared manifest/epoch; reconnect, repair, reset, and recovery must re-establish that closure before reporting settlement.
- `INV-SYNC-35`: A receiver MUST atomically and durably install a complete manifest, its facts, local IVM state/terminal, and any fast-known-state receipt before publication; it MUST expose neither a partial closure nor a fast receipt across a crash boundary.
- `INV-SYNC-36`: Peer sync carries an exact authorized input closure, never authority-produced application terminal rows or ordered terminal operations. The receiver reconciles admitted authority inputs with tier-eligible local inputs and derives the only application terminal by running its local copy of the identified maintained Groove program.
- `INV-TX-2`: Committing an exclusive transaction MUST store the commit locally as `Fate::Pending` with `DurabilityTier::Local` and emit exactly one `SyncMessage::CommitUnit`.
- `INV-TX-3`: A commit unit whose Transaction.ntotalwrites does not equal the delivered version count MUST be rejected by the fate authority as RejectionReason::MalformedCommit(...)...
- `INV-TX-4`: Duplicate commit units with identical payloads MUST be idempotent and return the already-known fate; duplicate units with conflicting payloads MUST fail as Error::Conf...
- `INV-TX-5`: The authority MUST park a commit unit with missing parent/schema/content prerequisites and MUST decide it only after all prerequisites are present.
- `INV-TX-11`: Accepted core commits MUST receive a strictly increasing authority-minted `GlobalTime`; accepted state and the core committed frontier MUST become durable atomically before publication.
- `INV-TX-23`: Fate authority MUST be structurally wired by the host. Applying a bare unfated commit unit on a non-authority sync path MUST stage or park it pending remote fate; it MUST NOT accept, assign global timestamp, or create merge versions from that payload.

## Details

### 8.1 One protocol, roles not code

Sync uses one peer protocol everywhere in the deployment. UI, worker, edge, and
core links all exchange the same `SyncMessage` vocabulary; a tier's behavior is
determined by its role, not by a separate wire language (ch. 1, principle 2).
Roles include relay links (`PeerRole::Relay`), edge-client links
(`PeerRole::ClientLink { identity }`), fate authority, durability, and eviction.

A relay link is an authenticated transport capability with no permission
subject. It neither implies `AuthorSubject::SYSTEM` nor independently narrows
reads. An edge-client link carries the terminated peer identity and narrows
reads under that identity. A scope-isolated client relay may carry only the
foreground binding admitted for that exact relay scope and attachment; the
upstream authority, not the relay, evaluates policy under that binding (ch. 7,
ch. 9).

A browser's scope-isolated client relay authenticates with its foreground
session, not an administrative credential incidentally present in application
configuration. Administrative admission must not replace that session or
silently change the relay's transport capability.

**Implementation status (2026-07-27).** Relay aggregation onto a shared upstream
shape is intended, but the current implementation does not guarantee it. Its
aggregation and covering-shape semantics remain an open design question below.

The peer wire form is binary-first. `WireFrame` wraps `Hello`,
`Message(WireEnvelope)`, and `Error`; `WireEnvelope.payload` contains a
postcard-encoded `SyncMessage` plus protocol version and feature bits. Postcard
is the canonical runtime frame/envelope format; JSON fixtures are only
human-readable golden checks. Row/version payloads remain groove custom
`Record` bytes inside protocol messages; postcard wraps those bytes, it does not
replace row encoding. The same split applies at the binding ABI (ch. 13):
commands, acks, and event metadata are postcard envelopes, while row-shaped
payloads are descriptor/raw `Record` bytes at the hot boundary.

**Decision, 2026-08-28 — the sole wire protocol is v1.** `ViewUpdate` carries
settled version payloads only through `version_carriers`; the transitional
duplicate `version_bundles` field is absent. Every endpoint advertises exactly
wire-protocol v1 and requires every peer Hello to advertise exactly
`min_protocol_version=1, max_protocol_version=1`; ranges such as `0..=1`,
`1..=2`, and `1..=15` reject before payload decoding. There are no compatibility
aliases, migration paths, or old wire decoders. `VersionBundle` remains the semantic unit produced when a
carrier is expanded and remains the direct payload of `RowVersionPayloads`
repair responses.

Transaction, row-version, session, and claim authors use the exact canonical
`[iss,sub]` JSON string. Large scalar descriptors use Groove's canonical
internal enum/record encoding rather than the former private tagged/postcard
payload. Wire row-version `$createdAt` and `$updatedAt` values are Unix
milliseconds; the packed HLC is internal ordering state and is not protocol
data. The wire-v1 golden fixture set is the only supported message layout.
Wire-protocol v1 is independent of other formats that are also labelled v1,
including storage, catalogue, migration-lens, and NAPI/WASM binding formats.
`MigrationLens` payloads in that fixture set are
their bounded canonical `jazz-migration-lens-v1` byte blob (with the lens id
derived on decode), replacing postcard's former field-by-field representation.

**Decision, 2026-08-31 — relay-delegated policy snapshots are v1 fields.**
`Subscribe` now ends with `delegated_session` and `FetchRowVersions` ends with
`delegated_session`; each is an optional `(identity, claims)` snapshot. `None`
is the ordinary direct-session form. `Some` has two admitted sources:

- an admitted `TrustedBackend` link may deliberately attribute work to an
  explicit backend session; or
- a scope-isolated client-relay link may carry the immutable foreground binding
  issued for its exact durable authentication scope and current admission
  epoch.

The receiver validates the snapshot against the link's server-issued
capability before shape admission or repair serving. An ordinary session/client
link, an unscoped relay, a mismatched scope, a stale admission epoch, or a raw
client-supplied binding MUST be rejected. A scope-isolated relay sends the
validated immutable snapshot for each upstream coverage or repair request it
owns, so two sessions with equal query bindings but distinct claims cannot
share an evaluator or repair authorization. The upstream authority scopes both
initial evaluation and row-version repair to that snapshot; the relay does not
evaluate policy. `SYSTEM` is never a relay transport identity or delegated
subject. This is a deliberate redefinition of the sole, unreleased v1 layout:
there is no old-shape decoder or compatibility path.

### 8.1.1 Frozen wire-protocol v1 byte contract

`WireFrame` and its `WireEnvelope.payload` are each **one complete postcard
value**. A conformant decoder MUST reject a valid prefix followed by any
trailing byte; concatenation belongs only to the documented WebSocket
`Vec<Vec<u8>>` batch carrier. In particular, a binding MUST NOT hand a byte
suffix from one frame to the semantic decoder, and a semantic decoder MUST NOT
silently leave a suffix for its caller (`INV-WIRE-1`).
Every postcard `u64`, including an enum discriminant or length prefix, MUST use
its shortest base-128 spelling. A redundant continuation byte, more than ten
bytes, or a tenth-byte payload above `1` is malformed rather than a compatible
alternate encoding. TypeScript decoders that expose a `u64` as a JavaScript
`number` MUST reject values above `Number.MAX_SAFE_INTEGER`; only fields kept
as `bigint` may retain the full `u64` domain.
Writers encode a ZigZag `i64` only from a safe JavaScript `number` or a
`bigint` in `[-2^63, 2^63-1]`; they MUST reject an unsafe number or either
out-of-range signed endpoint before emitting bytes.
The TypeScript WebSocket carrier MUST consume the entire outer batch and the
entire known `Hello`, `Message`, or `Error` frame whenever it parses or
classifies that frame; reading only the enum tag is not frame acceptance.
Within `WireHello.authority`, `WireAuthorityEndpoint.node` is the postcard
sequence representation of `NodeUuid`: a canonical length prefix of `16`
followed by exactly sixteen UUID bytes, then the postcard `u64` authority
epoch. It is not a bare fixed-width UUID. A carrier MUST consume this complete
optional endpoint before applying the exact-frame EOF check. Omitting the
sequence length, accepting a valid Hello plus a suffix, or treating a genuine
endpoint byte as a suffix is malformed framing, not version compatibility. A
length other than exactly `16` MUST be rejected even when the declared byte
sequence and the remaining Hello fields are otherwise well formed.

Postcard enum ordinals are wire data. The wire-protocol v1 baseline freezes these permanent
discriminants (decimal):

| enum            | frozen discriminants                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WireFrame`     | `Hello=0`, `Message=1`, `Error=2`, `MessageFragment=3`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `WirePeerRole`  | `Client=0`, `Core=1`, `Edge=2`, `Relay=3`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `WireErrorCode` | `UnsupportedProtocolVersion=0`, `UnsupportedFeature=1`, `MalformedFrame=2`, `AuthFailed=3`, `Backpressure=4`, `Internal=5`, `NotReady=6`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `WireRetry`     | `Never=0`, `AfterAuth=1`, `AfterResume=2`, `Later=3`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `SyncMessage`   | `ChunkRequestBatch=0`, `ChunkResponseBatch=1`, `SessionClaims=2`, `CommitUnit=3`, `FateUpdate=4`, `RegisterShape=5`, `Subscribe=6`, `SubscribeRejected=7`, `Unsubscribe=8`, `PublishSchema=9`, `PublishSchemaWithLens=10`, `PublishLens=11`, `SetCurrentWriteSchema=12`, `CatalogueAck=13`, `ViewUpdate=14`, `FetchRowVersions=15`, `RowVersionPayloads=16`, `CatalogueSnapshot=17`, `PermissionAdviceRequest=18`, `PermissionAdviceResponse=19`, `AuthorizationScopeSubscribe=20`, `AuthorizationScopeReceipt=21`, `AuthorizationScopeIntent=22`, `AuthorizationScopeView=23`, `AuthorizationScopeAggregateReceipt=24`, `AuthorizationScopeUnavailable=25`, `AuthorizationScopeDecision=26`, `ChunkUploadStart=27`, `ChunkUploadNodes=28`, `ChunkUploadResult=29`, `AuthorityPublication=30` |

Future variants MUST append after these values; existing variants, fields, and
their field order MUST NOT be reordered, inserted before, reused, or decoded
through a migration path. A new optional semantic variant additionally needs a
new negotiated feature bit. Wire-protocol v1 intentionally provides neither
old-version decoding nor migration.

`AuthorityPublication` has the postcard field order `tx_id`, `commits`. `tx_id`
is the upload/acknowledgement anchor and must occur among the members. `commits`
is a length-prefixed sequence in strictly increasing `TxId` order; each member
has field order `tx`, `versions`, using exactly the ordinary `CommitUnit`
transaction and complete-version encodings. It is not a new transaction or a
persistent storage format. Compression and fragmentation preserve the complete
logical publication; a receiver never admits a physical fragment or parks one
member for independent admission.

Only a host-authenticated authority control-plane connection may submit this
message to core. An admin-authenticated catalogue bootstrap connection also
possesses that authority capability. A wire role, `SYSTEM` author, ordinary
backend credential, or delegated session does not establish it. Normal
`CommitUnit` messages on the authority link still undergo their normal policy
checks; the prior-edge-admission privilege belongs to this publication only.

Before changing transaction state, core validates the whole publication's
structure, schema availability, replay identities, and parent closure. Members
include all not-yet-globally-acknowledged parents; omitted parents must already
be globally accepted at core. Missing catalogue/dependency context fails the
whole attempt without acknowledging or separately parking members. The sender
retains/reconstructs the complete publication for retry after context repair.
The members' canonical transaction, history, and current-state records are
persisted in one ordinary Groove batch. A crash or cancellation cannot expose
only an accepted prefix after reopen; process-local alias allocations may
precede this batch but are not admitted transactions. After all members are
durable, core reconciles residual cross-edge heads using
the shared merge machinery (ch. 4), then emits ordinary per-transaction fates.
The edge retains a publication until every member has a selected-authority
Global receipt, not merely its anchor. Partial acknowledgements never remove
the remaining members' authority binding or their reconnect replay obligation.
Edge recovery reconstructs unacknowledged publications from accepted history,
including other clients' writes and edge merges, without requiring those
clients to reconnect. It stops ancestry traversal at Global acknowledgements.

Feature bits are also permanent: `SyncMessagePayload=1<<0`,
`SessionFrame=1<<1`, `StructuredErrors=1<<2`, `PayloadLz4=1<<3`,
`PayloadZstd=1<<4`, `MessageFragmentation=1<<5`,
`AuthorizationScopeReceipts=1<<6`, `AuthorizationScopeViews=1<<7`, and
`AuxiliaryChunks=1<<8`, `ScopeIsolatedClientRelay=1<<9`, and
`AuthorityPublications=1<<10`. `Hello` negotiates only the intersection. A message
envelope or fragment MUST NOT declare a bit outside that intersection. Feature
masks are postcard `u64` values and MUST be decoded and compared across all 64
bits; a binding language MUST NOT apply a narrowing 32-bit bitwise operation.
Any unsupported low or high bit, including `1<<32`, rejects the Hello before
its accepted mask is converted to a narrower runtime type. The feature mask
and authority epoch remain `bigint` through wire decoding, so canonical values
through `2^64-1` are representable without a JavaScript number conversion. Exactly
one compression bit may be active on an envelope; when both codecs are
negotiated, an outbound wire-protocol v1 sender selects LZ4 and emits only its bit. A
receiver rejects an envelope declaring both codecs, a codec change within one
connection, corrupt compressed bytes, or a decompressed payload exceeding the
logical-message budget. Compression is applied before fragmentation and removed
only after complete fragment reassembly; it never changes semantic bytes.

`WireMessageFragment` is the complete physical-fragment layout in field order:
`protocol_version`, `features`, `session`, `message_id`, `message_digest`,
`total_len`, `offset`, `payload`. Its digest covers the entire compressed
payload; reassembly admits only negotiated, session-authenticated, in-range,
non-overlapping extents with exact contiguous coverage and matching metadata,
then verifies that digest before decompression or semantic decode. The resource
limits and expiry/deduplication rules are normative in
`SPEC/13_transport_message_fragmentation.md`.

JSON version cells use the schema-derived `StoredScalar(Json)` descriptor,
including inline cells. This is the same existing scalar codec used by local
physical JSON columns. The pre-freeze correction tracked with #2461 changes
the serialized `VersionRecord` descriptor for inline JSON, but leaves its raw
inline scalar bytes unchanged; the old `String` descriptor could not encode
indirect JSON at all. Receivers reject old inline JSON records whose descriptor
no longer matches the authored schema. This correction must be shared by the
contained and typed identity candidates before freezing v1; it introduces no
new durable storage encoding or compatibility fallback.
`fixtures/large_json_wire_v1.json` pins the old inline descriptor and the corrected
inline/indirect records. Rust checks exact bytes, decoded values, roundtrips,
and rejection of the old descriptor before storage.

The wire-protocol v1 frozen corpora are `crates/jazz/fixtures/wire_message_frames.json` and
`crates/jazz/fixtures/wire_hello_frames.json`:
Rust independently decodes every hard-coded frame, re-encodes the semantic
value to the exact same payload and frame bytes, and TypeScript independently
reads every transport envelope through its production postcard reader, rejects
a suffix on every corpus frame, and round-trips each through the exact batch
carrier. The Hello corpus crosses every frozen peer role with both absent and
present authority endpoints and one- and multi-byte feature masks; supported
Core cases additionally pass through the production TypeScript WebSocket
negotiation path. Its
binding companion, `binding_codec_golden.json`, covers NAPI/WASM's shared
Rust-produced relation-snapshot and subscription-delta byte ABI, consumed by
the production TypeScript decoder. These corpora are compatibility evidence,
not a permissive migration input. Adding a new case requires a SPEC decision,
an invariant citation, and a review of every language consumer.

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

| message                                                                            | direction      | payload                                                                                                                               |
| ---------------------------------------------------------------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `CommitUnit`                                                                       | up             | `{ tx: Transaction, versions: Vec<VersionRecord> }`                                                                                   |
| `FateUpdate`                                                                       | down           | `{ tx_id, fate, global_time: Option<GlobalTime>, durability: Option<DurabilityTier> }`                                                |
| `RegisterShape`                                                                    | up             | `{ shape_id, ast: ShapeAst, opts: RegisterShapeOptions }`                                                                             |
| `Subscribe`                                                                        | up             | `{ shape_id, subscription: SubscriptionKey, values: Vec<Value> }`                                                                     |
| `SubscribeRejected`                                                                | down           | `{ subscription: SubscriptionKey, reason: SubscribeRejectReason }`                                                                    |
| `Unsubscribe`                                                                      | up             | `{ subscription: SubscriptionKey }`                                                                                                   |
| `ViewUpdate`                                                                       | down           | `{ subscription, reset_result_set, version_carriers, peer_payload_inventory, result_member_adds/removes, program_fact_adds/removes }` |
| `PublishSchemaWithLens` / `PublishLens` / `SetCurrentWriteSchema` / `CatalogueAck` | catalogue lane | ch. 10                                                                                                                                |

A `VersionCarrier` in `ViewUpdate.version_carriers` is either one owned
`VersionBundle` or a packed run that expands to the same bundle sequence. A
`VersionBundle` is `{ tx, versions, scope, fate, global_time, durability }`: a
settled **view payload bundle** with the fate state observed when it shipped.
`scope` explicitly distinguishes
`CompleteTransaction` from `ViewScoped`; cardinality equality is not a scope
witness. A complete bundle carries the authored `tx.n_total_writes` and may
enter the peer's complete-transaction-payload inventory for later dedup. A
view-scoped bundle carries only the row/version witnesses admitted by that
selected view and MUST redact `tx.n_total_writes` to `versions.len()`. It never
establishes complete-payload coverage, even when those numbers happen to equal
the authored transaction's true cardinality.

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
waits for a `FateUpdate`; it must not accept the unit, assign global timestamp, or
create merge versions from it (`INV-TX-23`). Only a structurally wired fate
authority path may decide fate (ch. 3 §3.6, ch. 9).

### 8.3 Fates downstream

Downstream fate messages tell peers how an already-submitted transaction has
settled. A verdict travels as
`SyncMessage::FateUpdate { tx_id, fate, global_time, durability }`.

The `durability` field is an optional _claim_. A receiver raises observed
durability monotonically only when the message carries `Some(_)`; `None` leaves
the observed durability unchanged. A receiver also never moves `global_time`
backward (`INV-SYNC-5`). When an authority accepts a commit, it assigns a
monotone `GlobalTime` that advances the allocator and watermark (ch. 3,
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
  content/deletion layer, optional deletion tx, schema, branch key/prefix, batch,
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

### 8.4.1 Intended reconstruction model

**Design decision.** A peer-sync subscription replicates the _inputs_ to a
view, not the view's projected application rows. The authority owns the
security-sensitive decision of which inputs are visible, which result members
are admitted, and when that view is settled. The receiving node then runs the
same authorized residual program locally and is therefore able to recreate its
own terminal result deterministically. A terminal projected row is a cache and
an application/binding presentation value; it is never replicated truth.

For a canonical binding view at a declared frontier, the authority sends a safe
**witness closure** consisting of:

- the ordered, active catalogue/schema/lens lineage needed to interpret every
  included fact and to reach the binding's read schema;
- canonical authored content and deletion `VersionRecord`s, with their logical
  table, branch key, row, transaction, authored `SchemaVersionId`, source lineage,
  and fate/frontier identity intact;
- authority-maintained relation/correlation, winner, and replacement witnesses
  needed by the lowered program, including their source/witness identity;
- authority-produced membership, policy, and settlement facts sufficient to
  gate the local program without asking the receiver to infer a hidden policy
  witness; and
- a frontier/closure declaration identifying exactly the canonical binding view
  and the inputs complete at that frontier.

Two fact families are intentionally distinct. **Canonical authored history** is
immutable content/deletion history authored by a client, and remains meaningful
outside this subscription. **Authority-maintained facts** are correlation,
membership, admission, replacement, and settlement facts computed for this
authorized binding view. They are not re-authored history and cannot be reused
as ordinary rows or across another reader, branch view, policy revision, or binding.
The manifest identifies the exact **authorized residual program** that consumes
both families: the canonical shape/read view plus the authority-maintained
relations that replace policy evaluation or hidden correlation at the receiver.
Its identity and canonical digest are inputs to IVM, not an informal promise.

Every closure begins with a canonical **closure manifest**. It names the
authority database lineage and authority view epoch, canonical binding view,
reader/policy revision, normalized branch sources and `SnapshotRef` where applicable,
frontier, ordered catalogue segment and digest, residual-program/admission
identity and digest, plus a complete canonical inventory for every fact class.
Each inventory has stable fact identities, count, and digest; the manifest's
digest covers those inventories and their ordering. A receiver treats the
manifest as an exact completeness contract, not a best-effort hint: an unseen
fact identity, conflicting digest, later epoch, or fact from a different
manifest is parked/rejected rather than blended into the view.

This is deliberately not a request to ship all data used by a policy. The
authority evaluates policy and sends only facts the receiver is permitted to
hold. When an internal policy/join/reachability witness is needed to explain a
membership transition but revealing it would disclose hidden data, the protocol
uses an authority-owned opaque admission fact with stable identity, not the
hidden row and not a projected substitute. That fact can enable or retract the
authorized member in the receiver's graph, but cannot be reinterpreted as an
independently readable source row (`INV-SYNC-33`, ch. 7). Its identity is bound
to the authority database lineage, authority epoch, manifest digest, canonical
shape/binding/read view, reader/policy revision, branch sources/SnapshotRef, and
residual-program identity. A receiver rejects a replay under any other axis;
opaque admission is a scoped residual input, not a portable capability. It is
also bound to its **protected occurrence**: the exact output/result-member or
path occurrence it admits, its source logical table/row, the concrete
content/deletion version(s), and the selected exact branch key layer (current or
snapshot-qualified). Its identity names
every correlation, winner, replacement, and policy witness by stable witness id
_and_ concrete version/layer. A changed content/deletion winner, layer, witness,
or protected occurrence retires the old admission; it cannot be reused for a
different row/output, even if all reader and query axes still match.

Receiver application has one fixed order:

1. admit and order the catalogue closure; park data whose authored schema or
   lineage is unavailable;
2. decode each version using its **authored** schema and validate its canonical
   identity, branch key, and bytes; resolve branch-key-qualified history and
   current winners before any read-schema projection;
3. project only those selected winners through the ordered lens lineage into the
   subscription read schema;
4. install only the manifest-admitted canonical and authority facts into local
   input relations and run the local maintained IVM; then
5. expose only that local terminal's ordinary app delta to the facade/binding.

The receiver MUST NOT supplement the residual program with unrelated local
history, a broader local current index, stale facts from another subscription,
or unmanifested pending writes. A locally authored view may use its own local
frontier under its own manifest, but it is not evidence that the remote binding
view is settled. This isolation is what makes the authority's visibility and
membership decision authoritative while still allowing deterministic local IVM.

### 8.4.2 Atomic closure installation, transition, and publication

**Live delivery contract (2026-09-02).** Within one active subscription on a
live connection, `ViewUpdate`s form an ordered, nonduplicated stream. Transport
backpressure retries only messages that have not been accepted for delivery;
an internal queue/retry bug must not be normalized into application-level
at-least-once delivery. Reconnect establishes fresh subscription coverage rather
than replaying old incremental transitions against a new receiver. This contract
does not require introducing wire sequence numbers. Commit-unit idempotency
(§8.2) and resource-idempotent `Subscribe` (§8.5) do not imply idempotent
`ViewUpdate` deltas.

An incremental update must describe a possible transition from the exact state
already received for that subscription. Removing an absent fact, removing the
wrong version, adding an already-present exact fact, or retaining conflicting
versions of one source row is a protocol error, not a harmless no-op. Facts
inside one update are an unordered set transition: a valid remove-old/add-new
replacement is checked against the predecessor and final state, not arbitrary
vector order. A full reset explicitly replaces the closure and is validated as
such, including coverage for required sources with no rows. Late frames for a
detached subscription remain subject to the separate lifecycle/drop rules; they
must never be applied to a replacement subscription.

Reject an impossible transition atomically, before changing retained facts,
durable state, settlement, or application output. Surface an actionable protocol
error to the affected client read/subscription, with safe subscription/source
identity and the failed transition check, not row contents or private claims.
Do not silently discard the inconsistency, quietly convert it into a reset, or
leave the client waiting until a timeout. The error must expose the broken
producer/transport assumption so it can be fixed at its source.

The receiver stages incoming closure members under an **inactive** manifest id.
Staging validates identity, class, manifest inventory, catalogue order, and
residual-program identity, but it does not change an active view, publish a
terminal edit, advance `settled_through`, or create/advance a fast-known-state
receipt. An initial closure or reset is a **full manifest**: it enumerates every
required member of every fact class, and cannot become active until complete.

Once a full manifest is complete, one durable installation boundary atomically
swaps all of the following: the active manifest/epoch pointer; its canonical and
authority-maintained facts; the local IVM input/state and derived terminal; the
settlement frontier; and any persisted fast-known-state receipt. Only after that
transaction commits may the receiver enqueue the local terminal delta or expose
the fast receipt for reconnect dedup (`INV-SYNC-35`). Recovery must observe the
old complete closure or the new complete closure, never a durable mixture. An
inactive staged closure or a pre-commit terminal is disposable after a crash.

Steady-state changes need not reship a full closure. Each fact-class inventory
is a canonical authenticated sparse-Merkle dictionary. Its 256-bit key is
`BLAKE3("jazz sync closure fact key v1", class_tag, canonical_fact_identity)`;
its leaf is `BLAKE3("jazz sync closure fact leaf v1", class_tag,
canonical_fact_identity, canonical_fact_bytes)`; and its binary interior hash
is `BLAKE3("jazz sync closure node v1", level, left, right)`. Empty leaves and
subtrees use the same domain-separated hash with their level, so they are fixed
independently of local storage. A class commitment is its `u64` member count and
root; the manifest digest commits to the canonical class-tag ordering and every
class commitment. The exact byte framing is length-prefixed canonical encoding;
no implementation-defined map order or unhashed count is permitted. An
initial/reset full manifest carries every fact byte; the receiver constructs each
dictionary from those facts and verifies its advertised count/root before the
manifest can become complete.

An incremental transition names `(previous_manifest_digest, previous_epoch)`
and a successor manifest. For every affected class it supplies the old/new
commitments and a canonical sequence sorted by `(class_tag, key, op)`. Every
`Add` carries the full fact plus a current-root non-membership proof; every
`Remove` carries the full existing leaf plus a current-root membership proof. A
fact-content change is a remove followed by an add, never a mutable relabel.
The receiver verifies the predecessor commitment, applies each proof against
the root produced by the preceding canonical operation, updates the count, and
requires the final root/count to equal the successor commitment. It then
verifies the successor manifest digest. Thus each per-change proof has an
executable root-transition algorithm, rather than trusting a claimed new root.

The receiver accepts a transition only when its active manifest exactly equals
the named predecessor and every authenticated operation validates. It then
changes only the affected input relations and lets local IVM produce the
terminal delta. This is the manifest form of
`groove/SPEC/INVARIANTS.md::INV-INC-1`: neither normal updates nor manifest
bookkeeping permit a full terminal/cache rebuild.

If a live transition contradicts its received predecessor or any
count/root/add/remove check fails, the receiver leaves the active view unchanged
and surfaces the protocol error described above. Missing payload bytes during
an explicitly negotiated initial/reconnect repair can still request the exact
missing member or a full reset; that repair mechanism must not hide an impossible
live transition. It is never valid to infer a successor from a projected terminal
cache.

The required crash-point ladder covers: each inactive class member staged; IVM
precomputation before the durable swap; after the durable swap but before local
publication enqueue; after enqueue but before local consumer observation; and
restart before/after persisting a fast receipt. Each point must recover to one
complete manifest whose locally derived terminal equals the authority's
one-shot result, and must never report settled or known-state-fast from a
partial manifest.

In particular, a receiver MUST NOT relabel raw `Record` bytes authored under
`v1` as a `v2` row merely because a `v2` subscription requested them. It decodes
under `v1`, applies the explicit ordered lenses, and only then obtains a `v2`
logical row. The wire protocol MUST NOT add an ad hoc `ProjectedAppRow` (or any
equivalent "already selected" row carrier) to make a peer cache look current.
Nor may it use terminal root/path operations, packed app-row bytes, or a
`ResultPayload` as an alternate replicated source of truth. These values may be
locally cached, dropped, and recomputed from the closure; their descriptor,
projection, and host ABI have no peer-sync authority (`INV-SYNC-31`,
`INV-SYNC-32`).

This distinction applies equally to simple roots and to joins, arrays, nested
relations, ordering, windows, and aggregates: the local IVM receives
manifest-admitted authored-history facts plus authority-maintained witnesses and
produces the result member, relation/path, and terminal effects. It does not
reconstruct a query from a server-projected tree, and the server does not ask
bindings to interpret relation facts or rerun authorization.
The source identity of every row and witness is stable across this path, so a
replacement, deletion, or policy revocation retracts the same local fact that
caused the prior output.

Branch views are closed in the same way, but their closure is
source-specific. The manifest names normalized head/base branch keys and any frozen
base `SnapshotRef`, and includes only the selected layer winners. It MUST NOT
admit a same-row fact from another branch key or a post-cut base change merely because
that fact is present locally. A missing base contribution, head witness, or
branch-key-qualified authority fact prevents settlement and is repaired or reset
as part of that read-view closure (ch. 11).

The result is deterministic: with the same ordered catalogue closure, canonical
fact multiset, authority admission facts, canonical shape/binding/read view, and
frontier, two receivers must derive the same terminal result. A malformed,
missing, contradictory, or out-of-order component parks or rejects the update;
it never triggers byte relabeling, an independent semantic scan, or a best-effort
projected-row repair. A missing manifest member of **any** fact class is repaired
by its stable fact identity and class; if exact repair cannot prove the same
manifest/epoch, the authority sends a new reset closure with a new manifest.

Authority-produced terminal operations are not part of the replication
contract. They cannot be an input to correctness, repair, settlement, or
application publication and MUST NOT appear in a post-cut `ViewUpdate`. The
pre-cut `terminal_operations` carrier and terminal-reset cache are retired by
`INV-SYNC-36`; implementations remove them rather than retaining an empty,
diagnostic, compatibility, or facade-side application path. Ordered terminal
operations remain an internal output of the receiver's own Groove graph and may
cross the local Rust-to-host binding ABI, which is not peer sync.

Each post-cut `ViewUpdate` instead carries typed covered-input facts scoped by
its exact authority result and subscription. A covered input names the
canonical normalized source path (including aliases, recursive, correlated, and
policy roles), its logical source table and row occurrence, plus the logical
table, concrete content/deletion version, transaction, and branch identity of
the version body; the ordinary
`VersionBundle` path carries the corresponding version body. A retained result therefore emits a covered-input
remove/add when its nested child, deletion witness, or order-key source changes,
even when result membership and relation facts do not. An idempotent authority
tick emits neither fact transition nor body. This is the only publication seam
needed by receiver-local reconciliation; it neither exposes collector output
nor creates an authority ordering/index path.

**Hard aggregate boundary; exceptions require a new protocol decision.** An
ordinary aggregate is reconstructible only when its entire admitted canonical
input multiset, grouping/window/order facts, and deterministic aggregate
operator are in the closure; an authority-produced aggregate output is not a
shortcut fact. A genuinely non-reconstructible operator or a privacy-preserving
aggregate may not silently tunnel an output value through this rule. Until its
replay inputs, disclosure boundary, stable identity, repair/reset semantics,
and settlement proof have their own specified protocol, it is outside the
peer-sync maintained-subscription surface and must be rejected or exposed only
through an explicitly separate read API.

Protocol state deliberately keeps facts separate: concrete row-version payloads
received in bundles, transaction existence/metadata (`Transaction` by `TxId`),
non-versioned synthetic result payloads (`ResultPayload` program facts keyed by
typed result member), full transaction-payload coverage
(`peer_payload_inventory.complete_tx_payloads` / `CompleteTxPayloadCoverage`),
subscription-scoped exclusive completeness (`ViewCompleteExclusiveCoverage`),
source/read-frontier coverage, policy decisions/witnesses, predicate output
sets. Subscription-scoped exclusive completeness is a
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
invalidation and applied-global-time bookkeeping.

Under the intended reconstruction model, a reset replaces the receiver's
canonical closure manifest and every class inventory for that binding view; the
receiver re-runs local maintenance over that closure and publishes the resulting
local terminal reset. A reset of projected rows alone is insufficient because it
cannot prove that later local replacement, lens, policy, relation, branch-source, or
admission deltas have the inputs required to reproduce the same result.

**Implementation status (2026-07-27).** The receiver uses the staged-delta path
for non-reset bundles; `receiver_batch_ingests_non_reset_complete_bundles_once`
and `cold_reset_bulk_ingest_matches_incremental_ingest`
(`crates/jazz/src/node/tests/sync.rs`) cover the one-batch/one-tick behavior.
The remaining reset-specific bypass and the move to an `OrderedKvStorage`
transaction are implementation work, not protocol invariants.

**Structured-output delivery after the reconstruction cut.** `INV-SYNC-28` is
retired. Peer sync carries no terminal reset or root/path `Insert`, `Update`,
`Remove`, or `Move`. The receiver atomically installs the verified covered
inputs, runs its local maintained program, and publishes the resulting local
terminal reset/edit sequence only after that graph quiesces (`INV-SYNC-35..36`).
Bindings therefore see one receiver-local sequence, never a partial authority
sequence or a mixture of authority indices and local-overlay indices.
Row/version payload references and peer payload dedup remain separate from
local terminal delivery.

_Further invariants._ `INV-SYNC-17` — a result add carries enough
deletion-register witness to reconstruct the row's visible presence/absence.
`INV-SYNC-20` — incremental view updates are observationally equivalent to a full
reset `ViewUpdate` for the same canonical program instance (ch. 6).

The universal deletion-history table is not a wire namespace. A commit still
carries a logical table and a deletion `VersionRecord`; receiver catalogue
admission resolves that table/schema to its receiver-local `PhysicalTableId` and
persists the event under its local `(physical_table_id, branch_key, row)`
prefix. A payload cannot choose or forge a physical id, and a shared storage
layout never changes table-scoped sync or authorization semantics
(`INV-SYNC-27`).

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

While a usage-site `SubscriptionKey` is active on a live link, its canonical
attachment is immutable. The `Subscribe.shape_id` must equal
`Subscribe.subscription.shape_id`; a mismatch is a malformed peer request and
is dropped before registration lookup. After resolving the registration,
binding values, and serving options to a canonical program instance, replaying
the active key for that exact same instance is resource-idempotent: the serving
peer replaces the usage site's known-state declaration, marks it pending for an
initial refresh, and emits the resulting current-connection `ViewUpdate` and
applicable authorization receipt. The replay retains one canonical coverage
group, maintained view, and relayed subscription owner, and a later
`Unsubscribe` still detaches that sole attachment. Reusing the key for a
different canonical instance, or reusing a key already owned by the current-row
producer, is a malformed peer request: the serving peer drops it without a wire
response and preserves the original attachment and stream without any state or
delivery side effect.
Installing a current-row producer applies the same ownership rule after first
admitting any queued peer requests. An identical current-row owner is an
idempotent no-op; an ordinary subscription that already owns the derived key
causes a local protocol error before current-row update construction, sending,
or ownership registration.

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
entry, bundle, or ref is emitted (`INV-SYNC-13`, ch. 7).
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

**Implementation status (2026-07-27).** The peer payload inventory is deliberately narrow:
`peer_payload_inventory.complete_tx_payloads: Vec<TxId>` names only complete
transaction payload coverage, not broad "known versions" and not partial row
payload coverage. Partial and version-level dedup is the committed known-state
design (§8.11), which retires this inventory rather than extending it.

The postcard `WireFrame`/`WireEnvelope` format and groove row `Record` encoding
do not change when future inventory fields are added.

### 8.8 Protocol size limits

Protocol size limits are enforced at the layer that can recover correctly:

- An encoded `WireFrame` is capped at 2 MiB before postcard frame decode.
  `WireEnvelope.payload` is one physical fragment, not a semantic-message
  ceiling. Generic fragmentation/reassembly carries an encoded `SyncMessage`
  of any ordinary database size atomically across bounded frames. Receivers
  enforce fixed advertised-length, decompressed-output, concurrent-assembly,
  aggregate staged-byte, 30-second no-progress, and five-minute maximum-age
  limits as adversarial resource defences. Exact duplicates and rejected
  extents do not count as progress. Those budgets are transport policy, not
  query, catalogue, or transaction semantics.
- A `RegisterShape` AST is capped at 64 KiB encoded. This is a semantic
  admission limit for the shape-registration request; the connection may
  continue after the rejected request. Server shells may expose this as
  configuration later for unusually large generated query shapes.
- A `CommitUnit` is capped at 4096 row-version records, independently of its
  encoded byte size. This CPU/fan-out limit is transaction semantics: an
  over-limit commit unit is rejected as
  `Fate::Rejected(MalformedCommit(_))`, the connection remains live, and later
  well-formed commit units may still settle.
- Structured-output v4 adds named `MAX_STRUCTURED_RESULT_DEPTH` and
  `MAX_STRUCTURED_RESULT_WIDTH` limits in `protocol_limits.rs`. A receiver MUST
  enforce both before recursively decoding/allocating an untrusted structured
  snapshot, replacement, or chunk accumulation. Byte caps alone do not bound
  recursive decoder stack depth or the count of children/nodes allocated from a
  compact payload. The limits apply to the rendered payload at every nesting
  level and are protocol-admission limits: over-limit input is rejected before
  semantic application (`INV-SYNC-28`).

Outbound websocket batching is byte-budgeted at the physical layer: senders
split batches across binary messages rather than relying on a count-only batch
limit. A logical `SyncMessage` is fragmented first, so each encoded `WireFrame`
fits the wire-frame budget without truncation or semantic-layer chunking.

**Wire encoding posture (target optimization guidance).** High-rate serial
transactions (keystroke-grade chains: same author, same row, near-monotone
times) make consecutive sync messages highly redundant. The wire harvests that
redundancy generically, in two layers, rather than by introducing run-shaped
message semantics: (1) **per-connection stream compression** — a compression
context that persists across frames on one transport, so cross-message
repetition (subscription keys, row ids, authors, adjacent timestamps)
compresses without any wire-format change; and (2) **columnar `ViewUpdate`
internals** — a reserved append-only message variant whose member/bundle
payloads use this protocol's independent columnar wire encoding. A lone single-edit transaction with nothing before or after it pays full framing and transaction overhead by design — it is lone precisely when there is nothing to amortize against. Storage remains an independent row-only layer.

Native transports advertise zstd-3 stream compression by default when the
feature is compiled in. WASM/browser artifacts keep transport compression
opt-in so bundle-size trade-offs stay explicit; reconnect resets the compression
context and relies on known-state redelivery for correctness.

### 8.9 Edge mergeable fate deferral and permission-scope subscriptions

An edge that acts as mergeable fate authority needs the relevant policy data
before it can decide a write's fate. It therefore must defer fate assignment
until the relevant **permission-scope subscription** has settled; until then it
retains the unit only in its in-memory deferred-admission state, outside edge
history (`INV-SYNC-18`). Once the scope settles, the edge ingests the authorized
unit exactly once and routes its edge fate; a denied unit is rejected without
being ingested.

A permission-scope subscription is an _upstream_ subscription opened by the edge
against core for the policy data required by its acceptance gate. It is keyed by
`(policy_shape, writer_claim)` (ch. 9 §9.5): the write policy's query shape bound
to the writer's `claim("user")`. This hydrates only the policy rows that writer's
writes can depend on, never a whole table.

Permission scopes are shared at the sync level whenever one settled subscription
can satisfy every dependent acceptance gate (`INV-SYNC-22`).

**Implementation status (verified 2026-07-27).** Exact-key scopes are shared and
reference-counted by dependent gates; this is covered by
`edge_deduplicates_scope_subscription_for_repeated_deferred_units` and
`edge_releases_scope_subscription_after_last_deferred_unit_resolves`
(`crates/jazz/tests/four_tier.rs`). Whether and how a broader scope can satisfy a
narrower one remains an open design question below.

### 8.10 Catalogue lane

Catalogue messages (`PublishSchemaWithLens`, `PublishLens`,
`SetCurrentWriteSchema`, `CatalogueAck`) share this protocol lane; their
semantics are chapter 10.

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

#### Authorization progress

A fast declaration may additionally carry an **authorization-progress token**.
It is a server-stamped monotonic generation of the authorization state governing
this reader's visibility for this canonical binding view (shape, binding, and
read view). It is deliberately part of the declaration, rather than an
out-of-band connection hint: it qualifies exactly the state the subscriber is
claiming to have applied and persists with that state across reconnects.
`ViewUpdate` carries the server stamp beside its
peer-payload inventory, so the receiver persists it atomically with the
corresponding settled fast fact before later echoing it in the declaration.

The serving peer owns the token. Its granularity is **one reader plus one
canonical binding view**, not a global policy-head counter. It advances when
that reader/view is rebuilt because its effective authorization changed (for
example, session claims changed or a permissions head was installed). This
avoids forcing every reader to reset for unrelated policy churn. The cost of a
token that is too coarse is excess resets; the cost of one that is too fine is
unsafe suppression of a reset, so an absent token, an unknown server generation,
or a mismatch is always treated conservatively. The peer retains the generation
in its resumable peer state; if that state is not available after server loss,
the old token cannot match.

A matching token lets the server conclude that a pre-cursor membership
difference is not evidence of an authorization change. It does **not** assert
payload possession (the ordinary known-state body/repair rules still apply),
nor does a mismatched token itself prove that membership is unreconstructible.

The reset rule has two bounds. A reset is **required** when authorization
progress differs and the resulting membership cannot be reconstructed from the
data cursor (a removal or a newly visible member settled at or before `p`). A
reset is **forbidden** when authorization progress matches and the data cursor
is sufficient; when it is not sufficient, the server sends the smallest
expressible incremental repair and resets only if that repair cannot be encoded
as normal additions/removals. Conversely, an authorization-token mismatch with
only post-cursor additions is reconstructible and therefore must not reset.

Every `ViewUpdate` carries `settled_through`, the core-assigned global time
through which the canonical binding view was evaluated. Its meaning is per
binding view: this update reflects every global change at or before that
time that can affect the served view, including authorization and revocation
effects. It does not claim that the receiver possesses unrelated transactions,
and neither density nor numerical adjacency is required: the authority may
advance one binding directly across arbitrarily many irrelevant commits. It may
be persisted and reused across reconnects
or edges serving the same authoritative database lineage for known-state payload
dedup and repair. It is not an active-connection receipt: a subscription is
settled, and a usage-site one-shot attachment is remotely covered, only after
the selected continuously live upstream connection has sent a fresh confirming
`ViewUpdate`. A fresh `Edge`/`Global` one-shot requires that confirmation for
its exact current usage-site `SubscriptionKey`; a late update for a detached
predecessor cannot satisfy the new attachment even when shape, binding, and
options are equal. Disconnect, client restart, edge switch, or applying any
view update from a nonselected upstream immediately retires all
selected-authority receipts and
makes cached rows unsettled/local until the selected authority reconfirms. A stale cursor can
under-claim knowledge and cause extra bodies to ship;
it cannot over-claim because rows entering the view after `p` have membership
settle positions after `p`, and therefore do not satisfy the skip rule below.
After a nonselected update at cut `p`, a selected link's queued confirmation at
an earlier cut cannot restore settlement; its confirming `settled_through` must
reach at least `p`. The same floor applies to fallback-staged or deferred
updates marked ineligible for an authority receipt, even if their link becomes
selected before the update is finally applied.

Only cores are history-complete. An edge or client therefore tracks
`settled_through` per binding/subscription as proof that each exact result is
materialized. A fresh subscription requires its own authoritative evaluation; a
receipt for one binding says nothing about another binding's local result. A
validated receipt from the selected authority nevertheless advances the node's
`committed_global_time`: that field is the newest authority-committed coordinate
known to the node, not a claim of local data completeness. When a result is
assembled from multiple binding views, coverage is bounded by the required
views' confirmed cuts. The separate `history_complete` capability determines
whether `committed_global_time` is also a locally readable complete-history
frontier (ch. 3 and ch. 5).

For the reconstruction contract, `settled_through` is necessary but not
sufficient. Settlement means the receiver has installed a **complete,
reproducible input closure** for that cursor: ordered catalogue lineage,
authorized canonical version/witness facts, authoritative admission facts, and
the exact shape/binding/read-view identity. More precisely, the receiver must
hold one complete closure manifest for the selected authority epoch and verify
every catalogue, authored-history, branch-source, correlation, admission, replacement,
and settlement inventory named by it. A reconnect fast cursor may omit a known
version body only under the ordinary repair rule, but it cannot certify
settlement until every omitted body and every needed fact in every manifest
class is locally available, manifest digests match, and local IVM has drained.
Durable recovery rebuilds that closure from durable canonical state and/or
requests exact class-specific repairs; it does not rehydrate settlement from a
persisted projected terminal cache (`INV-SYNC-34`).

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
The closure manifest extends that discipline beyond version bodies: a receiver
requests the exact missing catalogue, branch-source, correlation, admission,
replacement, or settlement fact by its manifest class and identity. A server
that cannot provide a matching member of the current manifest sends a reset
with a new manifest; it must not leave the receiver partially settled or fill
the gap from another binding view. The canonical repair-carrying case is
visibility gained without a new version being minted — a policy/membership
change admitting rows whose versions settled at or before `p` (ch. 7);
version-minting scope entry is self-consistent because the entering version
settles above `p`.

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

**Implementation status (2026-07-27).** The receiver still uses the core
staged-batch seam rather than an `OrderedKvStorage` transaction. The wire
envelope has no portable resume credentials or trace/replay ids, and the
canonical cross-language fixture set is incomplete. The ordinary committed-unit
path also remains primarily client-to-core; the client-to-edge-to-core topology
is being exercised incrementally. Worker bridges have not yet converged on the
network wire-frame batches.

## Open Questions

- 🔶 [#2503](https://github.com/garden-co/jazz/issues/2503) — Bound restart-recovered authority publications without exposing an original write separately from its edge-generated merges.
- 🔶 [#1784](https://github.com/garden-co/jazz/issues/1784) — Protocol parking, transport state, materialization options, coverage/subsumption, retention, and version tags.
- 🔶 [#1779](https://github.com/garden-co/jazz/issues/1779) — Catalogue admission and synchronization.
