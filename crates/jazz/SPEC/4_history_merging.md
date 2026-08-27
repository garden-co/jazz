# jazz — Specification · 4. History, domination & merging

## Overview

jazz keeps full edit history. A row's stored state is a DAG of immutable versions,
and its "current" value is computed from the versions a node knows. This chapter
defines that version DAG, the domination rule that selects current content,
the merge semantics for concurrent writes, and the separate deletion layer. It
builds on the transaction lifecycle of chapter 3 and supplies the currentness
model used by reads (ch. 5) and sync (ch. 8).

Invariant digest:

- `INV-HIST-1`: A row version that lists a parent MUST dominate that parent for content-current selection when both versions are present in the same layer.
- `INV-HIST-2`: Among content heads not dominated by known parents, the current content version MUST be the head with the greatest made-at/`TxId` sort key.
- `INV-HIST-5`: An upstream node that observes two or more concurrent mergeable content heads for a row MUST create an accepted mergeable merge version with those heads as parents, unless a content version with the same sorted parent set already exists.
- `INV-HIST-6`: A merge version MUST dominate all of its parent heads and become the current content winner when present and accepted.
- `INV-HIST-7`: A merge version's transaction time MUST be strictly after the maximum made-at time of the observed heads.
- `INV-HIST-8`: For `MergeStrategy::Lww`, a merged column MUST take the value from the highest made-at/`TxId` head that sets the column, and if no head sets it, from the highest made-at/`TxId` parent-union version that sets it.
- `INV-HIST-9`: `MergeStrategy::Counter` MUST be declared only on non-nullable integer user columns.
- `INV-HIST-10`: For `MergeStrategy::Counter`, concurrent integer deltas from their observed parent bases MUST be summed exactly.
- `INV-HIST-11`: Content and deletion state MUST be separate layers; content writes MUST NOT change the deletion register, and a current `DeletionEvent::Deleted` MUST hide the content-current row until a current `DeletionEvent::Restored` reveals it.
- `INV-HIST-12`: Accepted globally settled versions that become per-layer winners MUST be reflected in `jazz_{table}_global_current` or `jazz_{table}_register_global_current`.
- `INV-HIST-13`: Re-ingesting the same commit unit with identical version rows in a different order MUST be idempotent and MUST NOT create a conflict.
- `INV-HIST-14`: Rejected transactions MUST NOT appear as accepted row-history entries and MUST NOT participate in currentness/domination.
- `INV-HIST-15`: Merge strategy behavior MUST be deterministic and grouping-insensitive over the parent/head set; write-time canonicalization remains validation and rejects loudly.
- `INV-HIST-16`: A merge value MUST be the deterministic fold over the de-duplicated raw head set, never a fold of already-merged values. Combining divergent merge versions MUST fold the union of their raw parent-closures de-duplicated by version identity (LWW argmax; `Counter` sums per-`TxId` deltas so shared ancestors count once), so divergent merges converge to the single-merger-over-the-union result.
- `INV-HIST-17`: Content and deletion history MUST remain independently immutable and independently selected; a combined current row is a derived cache over their winners and MUST be reproducible from retained histories after restart or rebuild.
- `INV-HIST-18`: A node-local content-frontier helper, if retained, MUST be keyed by the complete physical content-row coordinate and encode a strictly increasing, duplicate-free canonical `TxId` array using Groove values rather than an opaque collection payload.
- `INV-TX-6`: A commit unit MUST be rejected with RejectionReason::CausalityViolation if its txid.time is less than or equal to any parent transaction's txid.time, and its versions...

## Details

### 4.1 The version DAG

A row's history is modeled as a directed acyclic graph of **row versions**. Each
version is identified by the `TxId` that wrote it and names zero or more direct
`parents` (ch. 2). Ordering is based on `TxId.time`, the HLC input, with the full
sort key `(time, node)` used for deterministic tie-breaking.

Causality is enforced at acceptance time. A causal child has a strictly greater
time than every parent; the authority rejects a violation as
`CausalityViolation` (ch. 3, `INV-TX-6`). Within accepted history, therefore, a
parent always precedes its children.

A version **dominates** the parents it lists, and by transitivity it dominates
their ancestors. When both a version and its parent are present in the same
layer, the parent is not a content head (`INV-HIST-1`).

### 4.2 Selecting the current content version

Current content is selected from the frontier of known, non-dominated content
versions. These frontier versions are the **content heads**: versions that are
not dominated by any known version in the same layer. Among them, the current
content version is the head with the greatest `(time, node)` sort key
(`INV-HIST-2`) — **argmax by HLC, not by arrival order**. Any two nodes that know
the same versions therefore compute the same winner regardless of delivery order.

The rule is scoped to the node's _known_ history. Downstream nodes may hold
shallow or partial history and must not assume completeness (ch. 1, principle
4). The precise statement is: at most one content-current winner exists per
`(row_uuid, layer)` among the node's known non-rejected versions; the visible row
may still be absent (§4.4).

Current reads use this rule without walking the whole row history. `Global`
reads resolve the known current winner from the global-current overwrite tables
(§4.5, `INV-HIST-12`). `Local`/`None` reads start from that direct global base and
overlay only the small set of local versions ahead of global settlement. When no
versions are ahead of global settlement, local hydration is flat in the number of
current rows, not proportional to history depth. The overlay still applies the
same known-history domination and argmax rules (`INV-HIST-1`, `INV-HIST-2`); it
is a bounded currentness computation over the ahead set, not a history scan.

### 4.3 Merging concurrent heads

Concurrent writes are reconciled by adding a version that records the frontier it
merged. When an **upstream** node (edge or core — never a client) observes two or
more concurrent mergeable content heads for a row, it creates one accepted
mergeable **merge version** whose `parents` are those heads sorted, unless a
content version with the same sorted parent set already exists (`INV-HIST-5`).
The merge version dominates all of its parent heads and becomes the current
content winner when present and accepted (`INV-HIST-6`).

The cells of a merge version are computed per column. The default strategy
(`MergeStrategy::Lww`) fills each column independently: it takes the value from
the highest-sort-key head that sets that column; if no head sets it, it falls
back to the **parent-union** — the set of all direct parents of the merge's heads
— and takes the value from the highest-sort-key version in that set that sets it
(`INV-HIST-8`). For example, with two concurrent heads `A (t=5)` setting
`title="x"` and `B (t=7)` setting `body="y"`, the merge is `{title:"x",
body:"y"}`: each column comes from the head that set it. If both had set
`title`, `B`'s higher sort key would win.

Counter columns use delta summation instead of last-writer selection. The counter
strategy (`MergeStrategy::Counter`) may be declared only on non-nullable integer
columns (`INV-HIST-9`, ch. 2). It computes each
concurrent writer's delta from its observed base and sums those deltas exactly
(`INV-HIST-10`). Concurrent increments therefore converge to the exact total:
from a base of `10`, a concurrent `+3` and `+5` merge to `18`, not to a single
last-writer value.

_Further invariants._ `INV-HIST-7` — a merge version's transaction time is
strictly after the maximum made-at time of the observed heads. `INV-HIST-15` —
merge-strategy output is deterministic and grouping-insensitive over the
head/parent set, with no wall-clock or node-local state in merged values.

**Merging merges.** Distinct upstream nodes may each mint merge versions for the
same row. If those nodes observed different frontiers, one merge may include a
concurrent head the other has not yet seen. Such divergent merges reconcile by
the same rule that defines every merge: a merge value is the deterministic fold
over the **de-duplicated raw head set**, never a fold of already-merged values. A
merge version is therefore a _cache_ over its sorted raw parent set, not an
opaque value that is itself re-merged.

To combine two merge versions, an authority folds over the union of their raw
parent-closures, de-duplicated by version identity. LWW takes the argmax raw head
with the parent-union fallback; `Counter` sums each raw version's delta keyed by
its `TxId`, so a shared ancestor is counted exactly once and never
double-counted. Consequently, duplicate merges over the _same_ frontier carry
identical cells, with the deterministic `(time, node)` tie-break picking one.
Merges over divergent frontiers converge to exactly what a single merger over
the union would have produced (`INV-HIST-16`). Reconciliation re-folds the
underlying versions, deltas, and ops, which are replicated history and so always
on hand.

#### Durable content-frontier helper

An implementation may retain a node-local derived content-frontier helper to
avoid rewalking history while accepting a new content version or preparing a
merge. The helper belongs to the **content** layer only: deletion is an
independent register (§4.4) and has no merge-head row. Its complete physical
key is `(PhysicalTableId, canonical BranchKey, RowUuid)`; omitting a branch or
using a logical table name would alias independent histories.

The helper's `heads` field is one normal Groove `Array<Tuple<U64, Uuid>>`: one
canonical `(TxTime, NodeUuid)` tuple per `TxId`, in strictly increasing
canonical `TxId` order with no duplicate. It is neither a `Bytes` wrapper nor
a serde/postcard collection. For example, concurrent `A=(10, node-a)` and
`B=(10, node-b)` with `node-a < node-b` are stored as `[A, B]`; replaying `A`
does not append a second `A`. A malformed, out-of-order, duplicate, or
wrongly typed value fails closed before it affects a merge.

This helper is derived local state, never a wire identity or source of history
truth. Immutable content history remains authoritative and can rebuild the
helper. The helper is nevertheless durable whenever retained, so an existing
storage root must first pass the top-level epoch-manifest admission gate before
any row is decoded: an unsupported former-alpha opaque payload must not be
guessed as the new untagged array (`INV-HIST-18`; Groove storage §2).

### 4.4 Deletion as a separate layer

Deletion is modeled separately from content so that hiding and restoring a row do
not rewrite its content history. Deletion events live in their own register layer
(`VersionLayer::Deletion`) carrying `DeletionEvent::{Deleted, Restored}`, and a
version belongs to exactly one layer (ch. 2). A current `Deleted` event hides the
content-current row; a later current `Restored` event reveals it again; content
writes never touch the register (`INV-HIST-11`).

Physically, deletion history is one sparse, schema-independent relation shared
by all content lineages. Every event is keyed by stable physical table and
canonical branch key before row identity, so a seek for one branch-local row is
bounded to `(physical_table_id, branch_key, row_uuid)` and a branch-key scan
is bounded to `(physical_table_id, branch_key)`. It is not a universal scan
and it never identifies an branch-local row by `RowUuid` alone.

### 4.5 Global-current as derived state

Immutable history versions are the replicated source material. The separately
selected content and deletion winners are node-local derived inputs. The
per-lineage **combined current row** is then derived as:

```text
{ content_winner, deletion_winner, deletion_event, visible, projected_cells }
```

`visible` is true exactly when a content winner exists and the deletion winner
is absent or `Restored`. The current row is rewritten when either winning layer
changes; it must preserve both winner identities even while invisible. It is
not shipped and can be rebuilt atomically from retained accepted history. An
implementation may retain private per-layer helper indexes to make that rebuild
or ingestion cheap, but ordinary current reads consume the combined current
source and do not perform a deletion anti-join (`INV-HIST-17`).

The combined global-current table is the source of truth for `Global`
current-row reads and sync snapshots on a node that has observed the accepted
version. It carries only settled winner references and projected cells, so a
global current read is O(current) in the rows and values returned. Local/edge
tiers use corresponding combined current state or a bounded overlay above this
base; neither rehydrates the global baseline from either immutable history.

_Further invariants._ `INV-HIST-13` — re-ingesting the same commit unit with its
version rows in a different order is idempotent and conflict-free. `INV-HIST-14` —
rejected transactions never appear as accepted history and never participate in
currentness or domination.

### 4.8 Subsumed merge-strategy backlog

The former TODO notes on complex merge strategies are treated as future surface
area over this chapter's deterministic merge contract. Built-in strategies cover
the first engine paths; richer set/map/rich-text/custom strategies must still
produce deterministic, grouping-insensitive merge results and must fail closed
without wedging authority progress.

## Open Questions

- 🔶 [#1782](https://github.com/garden-co/jazz/issues/1782) — External merge strategies and schema-version movement.
