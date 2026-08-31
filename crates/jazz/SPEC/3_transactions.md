# jazz — Specification · 3. Transactions & durability

## Overview

jazz separates the decision to accept a write from the question of how widely
that write has propagated. A transaction is either an eventually consistent
write (`mergeable`) or a serializable write (`exclusive`). Its state is tracked
on two independent axes: **fate**, the authority's verdict on the transaction,
and **durability tier**, the extent to which the transaction has settled.

This chapter defines that vocabulary, then specifies the lifecycle shared by
both transaction kinds, the durability model, authority admission, exclusive
validation, and rejection handling. It builds on ch. 2 for identity and storage,
and it supplies the transaction rules used by ch. 4 (which versions enter
history) and ch. 8 (the wire protocol).

Invariant digest:

- `INV-EDGE-8`: Edge acceptance of a mergeable transaction MUST be a final authorization outcome; core MUST NOT re-evaluate or reject it solely because policy changed concurrently aft...
- `INV-TX-1`: A transaction MUST NOT expose `open` writes to ordinary reads or subscriptions before commit.
- `INV-TX-2`: Committing an exclusive transaction MUST store the commit locally as `Fate::Pending` with `DurabilityTier::Local` and emit exactly one `SyncMessage::CommitUnit`.
- `INV-TX-3`: A commit unit whose `Transaction.n_total_writes` does not equal the delivered version count MUST be rejected by the fate authority as `RejectionReason::MalformedCommit(...)` and MUST NOT ingest version rows.
- `INV-TX-4`: Duplicate commit units with identical payloads MUST be idempotent and return the already-known fate; duplicate units with conflicting payloads MUST fail as `Error::ConflictingCommitUnit`.
- `INV-TX-5`: The authority MUST park a commit unit with missing parent/schema/content prerequisites and MUST decide it only after all prerequisites are present.
- `INV-TX-6`: A commit unit MUST be rejected with `RejectionReason::CausalityViolation` if its `tx_id.time` is less than or equal to any same-row/layer history parent's `tx_id.time`, and its versions MUST NOT enter history.
- `INV-TX-7`: A commit unit whose `tx_id.time.physical_ms()` exceeds the authority admission clock by more than `SKEW_TOLERANCE_MS` MUST be rejected as `RejectionReason::ClientClockTooFarAhead` and MUST NOT leave visible version rows.
- `INV-TX-8`: Rejection MUST cascade to known pending descendants and later arriving children of rejected ancestors as `RejectionReason::Cascade { root }`, preserving the original root transaction id.
- `INV-TX-9`: Originating nodes MUST retain rejected local payloads in retry storage and remove the rejected versions from normal history; non-origin authorities MUST NOT retain foreign rejected retry payloads.
- `INV-TX-10`: Applying a fate update MUST NOT move `global_time` backward and MUST update `durability` only monotonically upward.
- `INV-TX-11`: Accepted core commits MUST receive a strictly increasing authority-minted `GlobalTime`; the accepted transaction, global-current maintenance, and core `committed_global_time` MUST become durable atomically before publication, and the fate MUST report `DurabilityTier::Global`.
- `INV-TX-12`: Local durability MUST NOT imply upstream survival; committed local transactions that have not reached an upstream tier MAY be lost if local storage is destroyed.
- `INV-TX-13`: An exclusive transaction opened on a history-complete core MUST capture that core's atomically committed `GlobalTime` as `base_snapshot.global_base`; a partial edge/client MUST NOT promote query-scoped settlement into a whole-database global base.
- `INV-TX-14`: Exclusive snapshot reads MUST remain stable after later commits and MUST record the read version (including deletion-register versions when deleted) or an absent read.
- `INV-TX-15`: Reads inside an exclusive transaction MUST observe that transaction's own pending writes.
- `INV-TX-16`: Exclusive authority validation MUST reject when any recorded row read is no longer the globally current content/deletion read version.
- `INV-TX-17`: Exclusive authority validation MUST reject when an absent row read has become globally present.
- `INV-TX-18`: Exclusive authority validation MUST reject predicate phantoms by comparing the `(RowUuid, TxId)` output set at `base_snapshot.global_base` against current global output for the same shape and binding.
- `INV-TX-19`: Exclusive predicate validation MUST be sensitive to `binding_id`/`binding_values` and MUST use the inline query shape without requiring prior shape registration.
- `INV-TX-20`: Exclusive write validation MUST be first-committer-wins: each written version's current global winner in that version's own content/deletion layer MUST equal the single recorded parent, or absence when no parent is recorded. Row and predicate read validation remains against the observed visible content/deletion state (`INV-TX-16/17/18`); a version parent is not that read precondition.
- `INV-TX-21`: Accepted global transactions MUST maintain per-layer global-current tables/change stream.
- `INV-TX-22`: Downstream incomplete exclusive bundles MUST be stored but remain invisible for subscription views whose required exclusive payload is incomplete; they MAY become visible for a maintained subscription view once that view's required exclusive versions are present, even before all `n_total_writes` versions are known.
- `INV-TX-24`: A caller-generated `OpenTransactionId` MUST name mutable work unchanged across local and worker runtimes, MUST be terminal after commit or rollback, and MUST never be accepted by an API requiring the post-commit `TransactionId`; only successful commit transitions `OpenTransactionId` to `TransactionId`.
- `INV-TX-25`: A `CommitUnit` is one durable-publication boundary: canonical transaction/history rows, current/maintained-view inputs, fate/durability metadata, and recovery markers MUST become observable together. A failed or ambiguous persistence finalization MUST emit no `FateUpdate`, view/subscription update, or edge broadcast; reopen MUST either recover the entire unit or suppress it. Once persistence has completed, or a local publication has transferred to the node-owned ordered persistence queue, observer refresh failure MUST NOT be reported as commit failure.

## Details

### 3.1 Vocabulary

Transactions are named, classified, judged, and tracked for durability with the
following terms:

- `TxId { time: TxTime, node: NodeUuid }` (ch. 2) names a transaction.
- `TxKind` is `Mergeable` or `Exclusive`.
- `Fate` is `Pending`, `Accepted`, or `Rejected(RejectionReason)`.
- `DurabilityTier` is `None`, `Local`, `Edge`, or `Global` — separate from fate.
- `OpenTransactionId` is a caller-generated UUIDv7 naming runtime-local mutable work. It
  is used unchanged for synchronous, thread-local, and worker-hosted runtimes.
- `TransactionId` names the immutable commit produced by a successful commit and is the
  only identifier accepted by durability waits. The two are nominally distinct.

### 3.2 Lifecycle and the atomic sync unit

A transaction starts as local work in progress. While it is **`open`**, that
state belongs only to the node performing the work; it is not a stored fate and
is not visible to ordinary reads or subscriptions. Open writes become part of
the sync system only at commit (`INV-TX-1`).

Commit is the boundary that turns the work into a syncable object. Both
transaction kinds sync _only at commit_, as one idempotent
`SyncMessage::CommitUnit { tx, versions }`; the authority answers with
`SyncMessage::FateUpdate { tx_id, fate, global_time, durability }` (ch. 8).
Nothing partial travels upstream, and the core holds no open-transaction state.

The API transition is exactly `commit(OpenTransactionId) -> TransactionId`. Opening rejects
a duplicate live `OpenTransactionId`; commit and rollback consume it, and every later
use fails as a closed or unknown open transaction. `TransactionId` does not exist before a
successful commit. A worker command queue therefore carries complete commands
with the caller's `OpenTransactionId`; it does not allocate or translate a second
worker-local handle.

An empty exclusive batch is a valid atomic commit and therefore produces a
`TransactionId`. An empty mergeable transaction has no committed unit in the mergeable
history representation, so commit rejects it explicitly and leaves the
`OpenTransactionId` open for rollback; callers must not receive a fabricated
`TransactionId` for that no-op.

The word "atomic" has two relevant meanings here, and the distinction matters.
Upstream, the commit is atomic because it syncs as one idempotent message and the
authority decides the unit as a whole. Downstream, visibility depends on the
maintained subscription view. Rows from a mergeable transaction may surface
independently. Rows from an exclusive transaction are view-atomic: a receiver may
ingest a partial exclusive payload, but a subscription result may expose rows from
that transaction only once the payload required for that specific view is
complete. Other versions from the same transaction may arrive later, or never be
visible to that view (`INV-TX-22`, ch. 8). In this chapter, "atomic sync unit"
refers to the upstream property.

The unit is protected by two integrity rules. `Transaction.n_total_writes` must
equal the number of delivered version records; if it does not, the authority
rejects the unit as `MalformedCommit` and ingests no rows (`INV-TX-3`). A
delivered commit unit is idempotent when its payload matches a previous
delivery, in which case the known fate is returned. If the same unit is
redelivered with a different payload, it fails as `ConflictingCommitUnit`
(`INV-TX-4`).

### 3.2.1 Durable publication boundary

`CommitUnit { tx, versions }` is the one atomic boundary for both storage and
publication (`INV-TX-25`). The store may internally stage canonical history,
currency/index state, IVM durable terminals, fate metadata, and recovery
markers, but neither an acknowledgement nor a derived/subscription payload may
escape until the required durable boundary completes. This also applies to edge
relays: a returned `FateUpdate`, a `ViewUpdate`, and an edge-forwarded unit are
publication, not speculative progress.

If a process stops after an implementation's first durable stage and before its
final marker/cleanup stage, recovery must inspect that state before serving it.
It may complete a coherent unit or suppress it, but it must never serve a
mixture such as history without currency, fate without versions, or a derived
row that cannot be recreated from the recovered canonical state. This is the
sync-core contract that the future asynchronous persistent instance preserves;
an async completion/ack is not a second semantic commit.

The commit result and observer refresh result are distinct after this boundary.
Before persistence completes or ordered publication ownership transfers, failure
returns no committed receipt and no subscription or peer update may escape. Once
either condition holds, the caller receives the committed `TxId`/write receipt
even if resident subscription refresh fails. The refresh failure belongs to the
affected subscription error channel (or to retained node-owned retry work), not
to the already-owned commit. Peer ingest follows the same rule: it retains
publication ownership, preserves ordered post-settlement work, and reports
refresh failure without causing the sender to retry an already-published unit.

### 3.3 Durability is not fate

Fate and durability answer different questions. Fate records whether an
authority has accepted or rejected a transaction. Durability records how far the
transaction has settled. Because those questions are independent, the two axes
move independently.

A freshly committed write on a durable local runtime is `Pending`/`Local`. An
in-memory client instead authors it as `Pending`/`None`: the write is immediately available
to local reads, but a `Local` wait remains pending until a durable peer explicitly
returns `Pending`/`Local`. When the global authority accepts it, the transaction
becomes `Accepted`, receives a strictly increasing core-assigned `GlobalTime`,
and reaches `DurabilityTier::Global` (`INV-TX-11`). `GlobalTime` is a packed
authority HLC: 46 bits of physical milliseconds followed by an 18-bit logical
counter. It is ordered and monotone but intentionally not dense. Skipped values
after failed speculative allocation carry no missing-transaction meaning.
The authority uses its own wall-clock sample for both the forward-skew check and
HLC allocation; an uploaded transaction timestamp is never the authority clock.
If all 262,144 values in one physical millisecond are consumed, allocation
advances to the next physical millisecond without wrapping or reusing a value.
Only exhaustion at the packed maximum physical millisecond is a typed failure.

The core maintains an HLC register separately from its committed frontier. The
register may advance speculatively; the accepted transaction, global-current
maintenance, and `committed_global_time` advance atomically at the durable
publication boundary (§3.2.1). Because only cores are history-complete and core
acceptance is serialized, the latest durably committed timestamp is the complete
core history frontier. No `+1` gap inference or above-watermark set participates
in that proof. Recovery restores both registers from durable accepted state.
Accepted global transactions then maintain the per-layer global-current tables
and change stream (`INV-TX-21`, ch. 4). Crucially, **local durability does not
imply upstream survival**: a committed local transaction that has not reached an
upstream tier can be lost if local storage is destroyed (`INV-TX-12`).

`Global` is therefore not a durability flag in isolation. An application
`Global` wait completes only after it has observed all three parts of authority
settlement for that transaction: `Fate::Accepted`, `DurabilityTier::Global`,
and an authority-assigned `GlobalTime`. Hydration or propagation that supplies
only a `Global` durability claim cannot complete that wait (`INV-API-15`, ch.
13).

_Further invariants._ `INV-TX-10` — applying a fate update never moves
`global_time` backward and raises `durability` only monotonically.

**Implementation status.** The reference implementation enforces this
monotonicity; `fate_regressions::stale_pending_fate_update_cannot_regress_accepted`
exercises a stale pending update after global acceptance.

### 3.4 Mergeable transactions

Mergeable transactions are the eventually consistent write path. They give a
writer atomic commit and read-your-own-writes, but **no serializable isolation**:
concurrent mergeable writes to the same row merge by column LWW (ch. 4).

Mergeable fate can be accepted before the transaction reaches the global
authority. When an edge authority has already accepted a mergeable transaction,
the core finalizes it by stamping a new `GlobalTime` and
`DurabilityTier::Global`; it does not re-judge write-policy authorization or the
merge outcome (`INV-EDGE-8`). Edge mergeable authority and its
permission-subscription gating are ch. 9.

### 3.5 Exclusive transactions

Exclusive transactions are the serializable write path. Each one evaluates
against a fixed `Snapshot { owner, global_base, local_base, dots }`. On a
history-complete core, `global_base` is the core's atomically committed global
time (`INV-TX-13`). A partial edge/client has no node-wide global possession
claim: query freshness is carried by per-binding receipts (ch. 8), and locally
held transactions outside a core base are represented by the snapshot's
owner-local component or explicit dots. `local_base` and `dots`
bound which local, not-yet-global transactions the snapshot also includes.
Together these values define the snapshot's _coverage_: the exact set of
versions it can see. The full snapshot model is ch. 5.

Serializable validation depends on knowing which reads influenced the result,
so an exclusive transaction records the read set it relied on. A _shape_ is a
content-addressed query graph, and a _binding_ is its concrete parameter values
(ch. 6); a `PredicateRead` records both so validation can re-run the exact query.
While the transaction is open, a point read records either
`RowRead { table, row_uuid, version }` or `AbsentRead`, and a predicate read
(`tx_query` / `tx_current_rows`) records `PredicateRead { shape_id, shape,
binding_id, binding_values }` carrying the inline shape. Snapshot reads stay
stable after later commits and observe the transaction's own pending writes
(`INV-TX-14`, `INV-TX-15`).

Commit closes the exclusive transaction and makes its writes syncable.
`commit_exclusive` mints the `TxId`, stores the writes locally as
`Pending`/`Local`, and emits one commit unit. Until that point, the writes remain
invisible outside the transaction (`INV-TX-2`).

### 3.6 Authority admission

Fate authority is **structural**. A node acts as fate authority exactly when the
host wires it as one: the core accept path for global authority, or the
edge-authority ingest entry point for edge-decided mergeable fates. There is no
row-content inference, topology guess, or ambient `is_authority` flag that turns
ordinary sync receipt into acceptance authority.

Authority admission ensures that a verdict is based on complete inputs and on
the same checks for every commit unit. The fate authority first parks — and does
not decide — any unit that is missing parent transactions or schema versions.
It decides only once all prerequisites are present; a
duplicate parked unit parks only once (`INV-TX-5`).

After prerequisites are present, the authority rejects units that violate
history causality or clock-skew limits. A version parent is an exact prior
version of the same physical table, branch key, row, and content/deletion
layer. It is not a general mergeable-transaction dependency or an observed
state precondition: mergeable transactions carry no read set or arbitrary
causal dependency graph. A caller that needs "only if I observed X" uses an
exclusive transaction and its read set (§3.7).

A unit whose `tx_id.time` is not strictly greater than every such history
parent's time is rejected as `CausalityViolation` (`INV-TX-6`). A unit whose `physical_ms` is more than `SKEW_TOLERANCE_MS` (~30
seconds) ahead of the authority's clock is rejected as
`ClientClockTooFarAhead` (`INV-TX-7`). In both cases, no visible version rows
remain. Write-policy authorization (ch. 7) and, for exclusive units, the
validation of §3.7 follow. Only after those checks pass does the authority
assign a new `GlobalTime` and emit the accept fate.

### 3.7 Exclusive validation (serializability)

Exclusive serializability comes from validating the assumptions captured by the
transaction's read set. For an exclusive unit, the authority re-checks the
recorded reads against current global state:

- a recorded **row read** must still be the globally-current visible
  content/deletion version, or the unit is rejected as `ExclusiveConflict`
  (`INV-TX-16`): reading visible content `C` conflicts if a later deletion
  `D` hides `C`, even though `C` remains the content-register winner. This is
  separate from a write's own-layer CAS below and is covered by
  `exclusive_row_read_conflicts_when_a_later_delete_hides_the_content`;
- an **absent read** must still be absent (`INV-TX-17`);
- a **predicate read** must not have gained or lost rows — checked by comparing
  the `(RowUuid, TxId)` output set for that shape+binding at
  the complete dotted `base_snapshot` against the current output (`INV-TX-18`);
- each **write** is first-committer-wins in its **written history layer**: a
  content version compares its parent to the row's current global content
  `TxId`, while a deletion or restore version compares its parent to the
  current global deletion-register `TxId` (or absence when no parent is
  recorded) (`INV-TX-20`). This is deliberately separate from row and
  predicate read validation above: those protect the observed visible
  content/deletion state, whereas a version parent is only a same-coordinate
  history edge. For example, after content version `C`, a first delete `D` has
  no parent because the deletion register is empty; authority accepts it when
  that register is still empty even though content `C` is globally current. A
  later restore must parent `D` and is checked against the deletion register.
  Deletion visibility does not erase `C`: an atomic content replacement plus
  restore from that deleted snapshot parents content `C` and deletion `D`
  independently.
  `exclusive_delete_compares_the_deletion_register_not_content` proves this
  first-delete path,
  `exclusive_replacement_and_restore_parent_their_own_registers` proves the
  two-register atomic restore path, while
  `known_parent_must_match_exact_row_coordinate_and_layer` proves cross-row
  and cross-layer parent encodings are rejected.

_Further invariants._ `INV-TX-19` — predicate validation is sensitive to
`binding_id`/`binding_values` and uses the inline shape without requiring a prior
shape registration on the authority.

### 3.8 Rejection and cascade

Rejection records the authority's decision without keeping rejected foreign
versions in the normal data path. At an authority that did not author the
versions, rejection is audit-only: the rejected versions do not remain in normal
history or current visibility.

Rejection also propagates through dependency chains. It cascades to known
pending descendants and to later-arriving children of the rejected ancestor, all
carrying `Cascade { root }` with the original root `TxId` (`INV-TX-8`). The
**originating** node retains its rejected local payload in the
`RejectedTransaction` / `RejectedVersion` retry stores (so it can retry), while a
non-origin authority does not retain foreign rejected payloads (`INV-TX-9`).

Rehydration deliberately does **not** provide application-notification
continuity. If a persistent worker receives an authority rejection while no
foreground runtime is attached (for example, the user closed the app after a
locally durable optimistic write), a later runtime rehydrates the reconciled
data view with that rejected transaction excluded. It does not receive a
retroactive `onMutationError` callback and therefore cannot reliably show a
belated error toast. A runtime attached when the rejection arrives receives the
ordinary one-shot rejection delivery and may acknowledge it; applications that
need durable user-visible outcomes must record their own domain-level status.

### 3.10 Subsumed batch and replay notes

The former batch specs are now interpreted through this chapter's transaction
vocabulary. The old "direct batch" is the ordinary mergeable commit path:
local work is grouped under one `TxId`, syncs as one commit unit, and receives an
authority fate plus durability observations. The old "transactional batch" maps
to explicit exclusive or future authority-decided multi-row work: staged writes
are not ordinary visible state until commit and authority acceptance.

Replayable reconciliation is part of the transaction contract rather than a
separate manager. A client may retransmit a locally-authored committed unit until
it observes the unit's fate; authorities answer idempotently for matching
payloads and reject conflicting reuses of the same transaction id. Pending local
state is preview state only. Rejected outcomes must become explicit write state
that applications can observe and acknowledge through the high-level API
(ch. 13).

Prefix/batch storage planning is treated as substrate design for the same model:
storage may choose prefixes, commit segments, or compact catalogues, but the
public semantics remain transaction identity, fate, durability, and view-scoped
atomicity.

## Open Questions

- 🔶 [#1783](https://github.com/garden-co/jazz/issues/1783) — Transaction facade, authority placement, pending cleanup, and durability wording.
- 🔶 [#1782](https://github.com/garden-co/jazz/issues/1782) — Timestamp sanity and merge/history strategy boundaries.
