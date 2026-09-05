# jazz — Specification · 5. Reads & snapshots

## Overview

A read resolves the version DAG (ch. 4) against a deliberately chosen frontier:
either the current state at a durability tier, a fixed transaction snapshot, or a
historical global position. This chapter defines those read frontiers, the
current-row visibility rule, the snapshot model that gives exclusive
transactions stable reads, and historical (as-of) reads. It builds on the
currency and deletion semantics of chapter 4 and feeds queries (ch. 6) and the
`Db` read API (ch. 13).

Invariant digest:

- `INV-READ-1`: Opening an exclusive transaction MUST capture a `Snapshot` whose `owner` is the node UUID, whose `global_base` is the highest authority-committed `GlobalTime` known to the node, and whose `local_base` is the current `TxTime`. A history-complete core has no dots; a partial node MUST represent held accepted global transactions above its known authority cut as explicit dots and hydrate missing snapshot data from the authority at that fixed cut.
- `INV-READ-2`: A snapshot MUST cover exactly transactions with stored `global_time <= Snapshot.global_base`, transactions from `Snapshot.owner` with `tx_id.time <= Snapshot.local_base`, or transactions explicitly listed in `Snapshot.dots`.
- `INV-READ-3`: Reads inside an open exclusive transaction MUST choose the domination winner among snapshot-covered versions per `VersionLayer` and MUST NOT observe later uncovered current-winner changes.
- `INV-READ-4`: Reads inside an open exclusive transaction MUST overlay that transaction's own pending writes on top of the snapshot-covered base view.
- `INV-READ-5`: `tx_read` MUST record a `RowRead` for a present snapshot-visible row and an `AbsentRead` for an absent snapshot-visible row.
- `INV-READ-6`: `tx_current_rows` and `tx_query` MUST record predicate reads as `PredicateRead` values carrying `table`, `shape_id`, `shape`, `binding_id`, and `binding_values`; whole-table transaction reads are degenerate query shapes.
- `INV-READ-7`: Local current-row reads MUST use argmax `TxId` currency per `(row_uuid, VersionLayer)` over held non-rejected versions, independent of sender arrival order.
- `INV-READ-8`: Global current-row reads MUST use the per-lineage combined global-current source and MUST exclude rows whose stored visibility is false.
- `INV-READ-9`: Global as-of reads at `GlobalTime` MUST choose independent content and deletion winners from `jazz_global_changes` at or before the requested `global_base`, then derive visibility before returning content.
- `INV-READ-10`: Current-row visibility MUST be derived from independent content and deletion-register winners; content writes alone MUST NOT restore a deleted row, while `DeletionEvent::Restored` reveals current content.
- `INV-READ-11`: A local-tier read on the writer node MUST include the node's own pending committed transaction, while a global-tier read MUST exclude it until global fate/current state is applied.
- `INV-READ-12`: Per-layer global-current tables MUST equal accepted argmax winners over stored versions and remain consistent after reopen.
- `INV-READ-13`: Ordinary current query lowering MUST consume one combined current-row source per logical table and MUST NOT introduce a deletion-register anti-join; historical and fixed-snapshot views MAY resolve the two immutable histories at their requested frontier.

## Details

### 5.1 Read tiers

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

### 5.2 Current-row visibility

Current-row reads return content only when the deletion register permits it. A
visible current row is derived from the independent content winner and deletion
winner (ch. 4): a current `Deleted` event hides the content row, a later
`Restored` reveals it, and a content write alone never un-deletes a row
(`INV-READ-10`). The result is materialized into the combined current row,
including both winner identities and the `visible` decision. An ordinary read
reads that one source and filters `visible`; it neither reads nor joins deletion
history/currentness separately (`INV-READ-13`).

The same visibility rule applies at global durability. Global current-row reads
consume the combined global-current source (`INV-READ-8`). Its embedded winner
references must equal the accepted argmax winners and stay consistent across
reopen (`INV-READ-12`).

**Implementation status.** Current global winner maintenance is covered by
`sync::accepted_fates_maintain_global_current_tables`; reopen consistency is
exercised by `sync::reopened_core_continues_sync_after_restart`.

### 5.3 Snapshots

Snapshots give an exclusive transaction a stable read frontier. A snapshot
(`Snapshot { owner: NodeUuid, global_base: GlobalTime, local_base: TxTime, dots:
Vec<TxId> }` in the reference implementation) is a compact dotted description of
that frontier, owned by the node that created it. A transaction is **covered** by
a snapshot when its stored `global_time <= global_base`, or it is owned by
`owner` with `tx_id.time <= local_base`, or it is explicitly listed in `dots`
(`INV-READ-2`).

Opening an exclusive transaction on a history-complete core captures `owner =
self`, `global_base = committed_global_time`, `local_base = the current TxTime`,
and empty `dots` (`INV-READ-1`). Core settlement serializes HLC allocation with
the durable accepted commit, so this is a clean prefix even though timestamp
values are sparse. Numerical adjacency has no semantic role.

Edges and clients are partial by design. A validated `settled_through` receipt
from the selected authority proves both that the authority has durably committed
through that coordinate and that this exact binding is complete there. The node
MUST advance its known `committed_global_time` from that receipt, but this does
not claim that its local store contains every transaction through the cut.
Missing transaction reads are hydrated by registering the same query against a
`ReadViewSourceSpec::Snapshot` carrying the transaction's frozen snapshot. The
per-binding receipt remains necessary to prove that the locally materialized
query result is complete. Locally held work above the authority cut is represented
through the owner-local cut and explicit dots; the core validates recorded row,
absent, and predicate reads at commit (ch. 3).

The `dots` field is the escape hatch for the general snapshot model: a snapshot
ref can name explicit transaction dots outside the contiguous/global and
owner-local prefixes. A history-complete core needs no dots. A partial node's
exclusive base may contain foreign dots, but only for locally held transactions
already carrying an accepted global fate; those dots describe the actual read
cut and authority validation evaluates the full dotted snapshot. They are not a
claim that intervening global history is complete. Sync payload dedup and
reconnect state remain separate (ch. 8): mere receipt of an unfated payload does
not create a snapshot dot.

### 5.4 Reads inside an exclusive transaction

Inside an exclusive transaction, reads are stable by construction. The read first
computes the domination winner among the **snapshot-covered** versions per layer,
then overlays the transaction's own pending writes (`INV-READ-3`). Because it
reads the covered set rather than the live currency tables, later arrivals can
change ordinary current reads but cannot change a read inside an already-open
transaction. The exclusive validation rules in chapter 3 depend on this
stability.

**Implementation status.** Stable snapshot reads and the pending-write overlay
are covered by
`exclusive_transactions::exclusive_tx_snapshot_read_ignores_newer_commits_after_open`
and
`exclusive_transactions::exclusive_tx_pending_writes_overlay_snapshot_for_point_and_table_reads`.

Every transactional read is recorded for that validation. A point read records a
`RowRead` when the row is present in the snapshot-visible view, or an
`AbsentRead` otherwise; a query records a `PredicateRead` (ch. 3).

_Further invariants._ `INV-READ-4` — reads overlay the transaction's own pending
writes on the covered base view. `INV-READ-5` — `tx_read` records a `RowRead`
for a present snapshot-visible row, an `AbsentRead` otherwise. `INV-READ-6` —
`tx_current_rows`/`tx_query` record a `PredicateRead` carrying the inline shape;
whole-table reads are degenerate query shapes.

### 5.5 Historical (as-of) reads

A historical read asks what was visible at a past global position. For a read at
a past `GlobalTime`, the system chooses independent content and deletion winners
from `jazz_global_changes` at or before the requested position, then derives
visibility before returning content (`INV-READ-9`). Time travel and frozen-base
branch sources build on this mechanism (ch. 11), and read policy is
evaluated at the historical cut (ch. 7).

### 5.7 Subsumed row-history read notes

The old table-first row-history project described current visible entries and
retained history as one model. This chapter owns the read side of that model:
ordinary current reads start from compact visible/current state, while
historical reads require an explicit cut and enough history completeness to
answer at that cut. Snapshot fallback during reconnect is an implementation
strategy for rebuilding coverage; it does not change the observable read
contract.

## Open Questions

- 🔶 [#1783](https://github.com/garden-co/jazz/issues/1783) — Read/snapshot lifecycle semantics.
