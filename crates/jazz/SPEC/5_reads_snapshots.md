# jazz — Specification · 5. Reads & snapshots

A read resolves the version DAG (ch. 4) against a deliberately chosen frontier:
either the current state at a durability tier, a fixed transaction snapshot, or a
historical global position. This chapter defines those read frontiers, the
current-row visibility rule, the snapshot model that gives exclusive
transactions stable reads, and historical (as-of) reads. It builds on the
currency and deletion semantics of chapter 4 and feeds queries (ch. 6) and the
`Db` read API (ch. 13).

## 5.1 Read tiers

Read tiers let callers choose how much durability a current read must have
before it is visible. The base derived state for a node is its **currency**: the
§4.2 content/deletion winner per `(row_uuid, layer)`, materialized over the
non-rejected versions held by that node (node-local derived state, ch. 2).
"Local currency" means this node's currency, as distinct from the global-current
tables described below.

A settled read names a `DurabilityTier` (ch. 3). A `none`/`local` read resolves
against local currency: the argmax-by-`TxId` winner per `(row_uuid, layer)` over
held non-rejected versions, independent of arrival order (`INV-READ-7`). This
means it **includes the reading node's own pending committed writes**. A
`global` read resolves against the per-layer global-current tables, which contain
accepted state only, and therefore **excludes a write that has not yet been
globally accepted** (`INV-READ-11`). An `edge` read occupies the tier between
`local` and `global`: it resolves against edge-accepted mergeable fates, meaning
state an edge has finally judged (ch. 9 §9.5) but that has not necessarily
reached global durability. Chapter 9 defines the full `edge` semantics.

## 5.2 Current-row visibility

Current-row reads return content only when the deletion register permits it. A
visible current row is the content-layer current winner **anti-joined with the
current deletion-register winner** (ch. 4): a current `Deleted` event hides the
content row, a later `Restored` reveals it, and a content write alone never
un-deletes a row (`INV-READ-10`).

The same visibility rule applies at global durability. Global current-row reads
perform the deletion anti-join over the global-current tables (`INV-READ-8`).
Those tables equal the accepted argmax winners and stay consistent across reopen
(`INV-READ-12`).

## 5.3 Snapshots

Snapshots give an exclusive transaction a stable read frontier. A snapshot
(`Snapshot { owner: NodeUuid, global_base: GlobalSeq, local_base: TxTime, dots:
Vec<TxId> }` in the reference implementation) is a compact dotted description of
that frontier, owned by the node that created it. A transaction is **covered** by
a snapshot when its stored `global_seq <= global_base`, or it is owned by
`owner` with `tx_id.time <= local_base`, or it is explicitly listed in `dots`
(`INV-READ-2`).

Opening an exclusive transaction captures `owner = self`,
`global_base = the contiguous applied global watermark` (not merely the highest
seen seq), `local_base = the current TxTime`, and empty `dots` (`INV-READ-1`).
Using the _contiguous_ watermark for `global_base` is what makes the snapshot a
clean prefix: gapped global seqs are excluded until their gaps fill.

The `dots` field is the escape hatch for the general snapshot model: a snapshot
ref can name explicit transaction dots outside the contiguous/global and
owner-local prefixes. An exclusive base snapshot carries no foreign dots: it
sees exactly the contiguous global prefix plus its own `owner`/`local_base`
transactions. Snapshot creation enforces that any admitted dots are owned by the
snapshot owner. Sync payload dedup and reconnect state are separate from this
read-frontier model (ch. 8); they must not overload `Snapshot.dots` to mean
"payloads already known by a peer."

## 5.4 Reads inside an exclusive transaction

Inside an exclusive transaction, reads are stable by construction. The read first
computes the domination winner among the **snapshot-covered** versions per layer,
then overlays the transaction's own pending writes (`INV-READ-3`). Because it
reads the covered set rather than the live currency tables, later arrivals can
change ordinary current reads but cannot change a read inside an already-open
transaction. The exclusive validation rules in chapter 3 depend on this
stability.

Every transactional read is recorded for that validation. A point read records a
`RowRead` when the row is present in the snapshot-visible view, or an
`AbsentRead` otherwise; a query records a `PredicateRead` (ch. 3).

_Further invariants._ `INV-READ-4` — reads overlay the transaction's own pending
writes on the covered base view. `INV-READ-5` — `tx_read` records a `RowRead`
for a present snapshot-visible row, an `AbsentRead` otherwise. `INV-READ-6` —
`tx_current_rows`/`tx_query` record a `PredicateRead` carrying the inline shape;
whole-table reads are degenerate query shapes.

## 5.5 Historical (as-of) reads

A historical read asks what was visible at a past global position. For a read at
a past `GlobalSeq`, the system chooses the per-layer winner from
`jazz_global_changes` at or before the requested position, then applies the
deletion anti-join before returning visible content (`INV-READ-9`). Time-travel
and snapshot-base branches build on this mechanism (ch. 11), and read policy is
evaluated at the historical cut (ch. 7).

## Open questions

None.
