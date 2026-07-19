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
- `INV-TX-1`: A transaction MUST NOT expose open writes to ordinary reads or subscriptions before commit.
- `INV-TX-2`: Committing an exclusive transaction MUST store the commit locally as Fate::Pending with DurabilityTier::Local and emit exactly one SyncMessage::CommitUnit.
- `INV-TX-3`: A commit unit whose Transaction.ntotalwrites does not equal the delivered version count MUST be rejected by the fate authority as RejectionReason::MalformedCommit(...)...
- `INV-TX-4`: Duplicate commit units with identical payloads MUST be idempotent and return the already-known fate; duplicate units with conflicting payloads MUST fail as Error::Conf...
- `INV-TX-5`: The authority MUST park a commit unit with missing parent/schema/content prerequisites and MUST decide it only after all prerequisites are present.
- `INV-TX-6`: A commit unit MUST be rejected with RejectionReason::CausalityViolation if its txid.time is less than or equal to any parent transaction's txid.time, and its versions...
- `INV-TX-7`: A commit unit whose txid.time.physicalms() exceeds the authority admission clock by more than SKEWTOLERANCEMS MUST be rejected as RejectionReason::ClientClockTooFarAhe...
- `INV-TX-8`: Rejection MUST cascade to known pending descendants and later arriving children of rejected ancestors as RejectionReason::Cascade { root }, preserving the original roo...
- `INV-TX-9`: Originating nodes MUST retain rejected local payloads in retry storage and remove the rejected versions from normal history; non-origin authorities MUST NOT retain for...
- `INV-TX-10`: Applying a fate update MUST NOT move globalseq backward and MUST update durability only monotonically upward.
- `INV-TX-11`: Accepted authority commits MUST receive the next GlobalSeq, advance the allocator/watermark, and report DurabilityTier::Global.
- `INV-TX-12`: Local durability MUST NOT imply upstream survival; committed local transactions that have not reached an upstream tier MAY be lost if local storage is destroyed.
- `INV-TX-13`: An exclusive transaction's basesnapshot.globalbase MUST be the contiguous applied global watermark.
- `INV-TX-14`: Exclusive snapshot reads MUST remain stable after later commits and MUST record the read version (including deletion-register versions when deleted) or an absent read.
- `INV-TX-15`: Reads inside an exclusive transaction MUST observe that transaction's own pending writes.
- `INV-TX-16`: Exclusive authority validation MUST reject when any recorded row read is no longer the globally current content/deletion read version.
- `INV-TX-17`: Exclusive authority validation MUST reject when an absent row read has become globally present.
- `INV-TX-18`: Exclusive authority validation MUST reject predicate phantoms by comparing the (RowUuid, TxId) output set at basesnapshot.globalbase against current global output for...
- `INV-TX-19`: Exclusive predicate validation MUST be sensitive to bindingid/bindingvalues and MUST use the inline query shape without requiring prior shape registration.
- `INV-TX-20`: Exclusive write validation MUST be first-committer-wins: each written row's current global content tx id MUST equal the single recorded parent, or absence when no pare...
- `INV-TX-21`: Accepted global transactions MUST maintain per-layer global-current tables/change stream.
- `INV-TX-22`: Downstream incomplete exclusive bundles MUST be stored but remain invisible for subscription views whose required exclusive payload is incomplete; they MAY become visi...

## Details

### 3.1 Vocabulary

Transactions are named, classified, judged, and tracked for durability with the
following terms:

- `TxId { time: TxTime, node: NodeUuid }` (ch. 2) names a transaction.
- `TxKind` is `Mergeable` or `Exclusive`.
- `Fate` is `Pending`, `Accepted`, or `Rejected(RejectionReason)`.
- `DurabilityTier` is `None`, `Local`, `Edge`, or `Global` — separate from fate.

### 3.2 Lifecycle and the atomic sync unit

A transaction starts as local work in progress. While it is **`open`**, that
state belongs only to the node performing the work; it is not a stored fate and
is not visible to ordinary reads or subscriptions. Open writes become part of
the sync system only at commit (`INV-TX-1`).

Commit is the boundary that turns the work into a syncable object. Both
transaction kinds sync _only at commit_, as one idempotent
`SyncMessage::CommitUnit { tx, versions }`; the authority answers with
`SyncMessage::FateUpdate { tx_id, fate, global_seq, durability }` (ch. 8).
Nothing partial travels upstream, and the core holds no open-transaction state.

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

### 3.3 Durability is not fate

Fate and durability answer different questions. Fate records whether an
authority has accepted or rejected a transaction. Durability records how far the
transaction has settled. Because those questions are independent, the two axes
move independently.

A freshly committed local write is `Pending`/`Local`. When the global authority
accepts it, the transaction becomes `Accepted`, receives the next `GlobalSeq`
(advancing the allocator and the contiguous watermark), and reaches
`DurabilityTier::Global` (`INV-TX-11`). Accepted global transactions then
maintain the per-layer global-current tables and change stream (`INV-TX-21`, ch.
4). Crucially, **local durability does not imply upstream survival**: a
committed local transaction that has not reached an upstream tier can be lost if
local storage is destroyed (`INV-TX-12`).

_Further invariants._ `INV-TX-10` — applying a fate update never moves
`global_seq` backward and raises `durability` only monotonically.

### 3.4 Mergeable transactions

Mergeable transactions are the eventually consistent write path. They give a
writer atomic commit and read-your-own-writes, but **no serializable isolation**:
concurrent mergeable writes to the same row merge by column LWW (ch. 4).

Mergeable fate can be accepted before the transaction reaches the global
authority. When an edge authority has already accepted a mergeable transaction,
the core finalizes it by stamping the next `GlobalSeq` and
`DurabilityTier::Global`; it does not re-judge write-policy authorization or the
merge outcome (`INV-EDGE-8`). Edge mergeable authority and its
permission-subscription gating are ch. 9.

### 3.5 Exclusive transactions

Exclusive transactions are the serializable write path. Each one evaluates
against a fixed `Snapshot { owner, global_base, local_base, dots }`. In that
snapshot, `global_base` is the **contiguous applied global watermark**, not
merely the maximum observed sequence (`INV-TX-13`). `local_base` and `dots`
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
ordinary sync receipt into acceptance authority. This decision was recorded by
Anselm on 2026-07-03.

Authority admission ensures that a verdict is based on complete inputs and on
the same checks for every commit unit. The fate authority first parks — and does
not decide — any unit that is missing parent transactions, schema versions, or
large-value content. It decides only once all prerequisites are present; a
duplicate parked unit parks only once (`INV-TX-5`).

After prerequisites are present, the authority rejects units that violate
causality or clock-skew limits. A unit whose `tx_id.time` is not strictly
greater than every parent's time is rejected as `CausalityViolation`
(`INV-TX-6`). A unit whose `physical_ms` is more than `SKEW_TOLERANCE_MS` (~30
seconds) ahead of the authority's clock is rejected as
`ClientClockTooFarAhead` (`INV-TX-7`). In both cases, no visible version rows
remain. Write-policy authorization (ch. 7) and, for exclusive units, the
validation of §3.7 follow. Only after those checks pass does the authority
assign the next `GlobalSeq` and emit the accept fate.

### 3.7 Exclusive validation (serializability)

Exclusive serializability comes from validating the assumptions captured by the
transaction's read set. For an exclusive unit, the authority re-checks the
recorded reads against current global state:

- a recorded **row read** must still be the globally-current content/deletion
  version, or the unit is rejected as `ExclusiveConflict` (`INV-TX-16`);
- an **absent read** must still be absent (`INV-TX-17`);
- a **predicate read** must not have gained or lost rows — checked by comparing
  the `(RowUuid, TxId)` output set for that shape+binding at
  `base_snapshot.global_base` against the current output (`INV-TX-18`);
- each **write** is first-committer-wins: the row's current global content `TxId`
  must equal the single recorded parent (or absence when none was recorded)
  (`INV-TX-20`).

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

### Open questions

- 🔶 **Monotonicity tests.** `INV-TX-10` (global_seq/durability monotonicity) has
  implementation but no direct test — `untested` in the registry until covered.
- 🔶 **Mergeable authority placement.** Edge mergeable authority and
  permission-subscription gating are the design; the implementation path
  described by this chapter currently has the core act as the mergeable fate
  authority before global finalization.
- 🔶 **Opt-in transaction facade.** The former replayable-reconciliation TODO
  defines explicit transactional writes with authority-decided fate, optional
  local pending overlay, schema-family validation, restart persistence, and
  rejected-state acknowledgement. Decide which pieces land as public `Db`
  transaction API versus lower-level `Node` authority plumbing.
- 🔶 **Unsealed pending cleanup.** Staged/open rows that never commit must be
  cleaned without ever becoming ordinary visible state, including rollback,
  disconnect, thrown user code, and restart cases.
- 🔶 **Durability guarantee wording.** `wait(tier)` is the durable contract;
  fire-and-forget writes may be dropped under backpressure/rate limiting only if
  the resulting write-state semantics remain explicit and observable.
- 🔶 **Timestamp sanity.** The history model accepts HLC ordering, but the policy
  for unrealistic future or past physical timestamps is still open: reject,
  clamp, quarantine, or accept with diagnostics.
