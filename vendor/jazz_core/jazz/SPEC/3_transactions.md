# jazz — Specification · 3. Transactions & durability

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

## 3.1 Vocabulary

Transactions are named, classified, judged, and tracked for durability with the
following terms:

- `TxId { time: TxTime, node: NodeUuid }` (ch. 2) names a transaction.
- `TxKind` is `Mergeable` or `Exclusive`.
- `Fate` is `Pending`, `Accepted`, or `Rejected(RejectionReason)`.
- `DurabilityTier` is `None`, `Local`, `Edge`, or `Global` — separate from fate.

## 3.2 Lifecycle and the atomic sync unit

A transaction starts as local work in progress. While it is **`open`**, that
state belongs only to the node performing the work; it is not a stored fate and
is not visible to ordinary reads or subscriptions. Open writes become part of
the sync system only at commit (`INV-TX-1`).

Commit is the boundary that turns the work into a syncable object. Both
transaction kinds sync *only at commit*, as one idempotent
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

## 3.3 Durability is not fate

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

*Further invariants.* `INV-TX-10` — applying a fate update never moves
`global_seq` backward and raises `durability` only monotonically.

## 3.4 Mergeable transactions

Mergeable transactions are the eventually consistent write path. They give a
writer atomic commit and read-your-own-writes, but **no serializable isolation**:
concurrent mergeable writes to the same row merge by column LWW (ch. 4).

Mergeable fate can be accepted before the transaction reaches the global
authority. When an edge authority has already accepted a mergeable transaction,
the core finalizes it by stamping the next `GlobalSeq` and
`DurabilityTier::Global`; it does not re-judge write-policy authorization or the
merge outcome (`INV-EDGE-8`). Edge mergeable authority and its
permission-subscription gating are ch. 9.

## 3.5 Exclusive transactions

Exclusive transactions are the serializable write path. Each one evaluates
against a fixed `Snapshot { owner, global_base, local_base, dots }`. In that
snapshot, `global_base` is the **contiguous applied global watermark**, not
merely the maximum observed sequence (`INV-TX-13`). `local_base` and `dots`
bound which local, not-yet-global transactions the snapshot also includes.
Together these values define the snapshot's *coverage*: the exact set of
versions it can see. The full snapshot model is ch. 5.

Serializable validation depends on knowing which reads influenced the result,
so an exclusive transaction records the read set it relied on. A *shape* is a
content-addressed query graph, and a *binding* is its concrete parameter values
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

## 3.6 Authority admission

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

## 3.7 Exclusive validation (serializability)

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

*Further invariants.* `INV-TX-19` — predicate validation is sensitive to
`binding_id`/`binding_values` and uses the inline shape without requiring a prior
shape registration on the authority.

## 3.8 Rejection and cascade

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

## Open questions

- 🔶 **Monotonicity tests.** `INV-TX-10` (global_seq/durability monotonicity) has
  implementation but no direct test — `untested` in the registry until covered.
- 🔶 **Mergeable authority placement.** Edge mergeable authority and
  permission-subscription gating are the design; the implementation path
  described by this chapter currently has the core act as the mergeable fate
  authority before global finalization.
