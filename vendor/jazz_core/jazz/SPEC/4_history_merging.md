# jazz — Specification · 4. History, domination & merging

jazz keeps full edit history. A row's stored state is a DAG of immutable versions,
and its "current" value is computed from the versions a node knows. This chapter
defines that version DAG, the domination rule that selects current content,
the merge semantics for concurrent writes, and the separate deletion layer. It
builds on the transaction lifecycle of chapter 3 and supplies the currentness
model used by reads (ch. 5) and sync (ch. 8).

## 4.1 The version DAG

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

## 4.2 Selecting the current content version

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

## 4.3 Merging concurrent heads

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
columns and never on large-value columns (`INV-HIST-9`, ch. 2). It computes each
concurrent writer's delta from its observed base and sums those deltas exactly
(`INV-HIST-10`). Concurrent increments therefore converge to the exact total:
from a base of `10`, a concurrent `+3` and `+5` merge to `18`, not to a single
last-writer value.

Large-value `text`/`blob` columns are excluded from the default LWW
pick-one-head cell rule at upstream authorities. They merge by
op-merge-since-LCA and store a primary-parent-relative op batch (`INV-LVAL-18`;
covered by
`jazz::node::tests::content_store::authority_merge_version_op_merges_concurrent_large_value_edits`
and
`jazz::node::ingest::large_value_merge_tests::three_head_large_value_fold_is_input_order_deterministic`).
The N-way fold processes large-value heads in causal sort-key order and carries
the folded accumulator's greatest causal origin, so same-position inserts
converge independently of delivery order (`INV-HIST-15`).

_Further invariants._ `INV-HIST-7` — a merge version's transaction time is
strictly after the maximum made-at time of the observed heads. `INV-HIST-15` —
merge-strategy output is deterministic and grouping-insensitive over the
head/parent set, with no wall-clock or node-local state in merged values (partial
coverage).

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
double-counted; large-value op-merge applies the raw ops de-duplicated by op
identity. Consequently, duplicate merges over the _same_ frontier carry
identical cells, with the deterministic `(time, node)` tie-break picking one.
Merges over divergent frontiers converge to exactly what a single merger over
the union would have produced (`INV-HIST-16`). Reconciliation re-folds the
underlying versions, deltas, and ops, which are replicated history and so always
on hand.

## 4.4 Deletion as a separate layer

Deletion is modeled separately from content so that hiding and restoring a row do
not rewrite its content history. Deletion events live in their own register layer
(`VersionLayer::Deletion`) carrying `DeletionEvent::{Deleted, Restored}`, and a
version belongs to exactly one layer (ch. 2). A current `Deleted` event hides the
content-current row; a later current `Restored` event reveals it again; content
writes never touch the register (`INV-HIST-11`). A row's _visible_ current state
is therefore the content-current winner (§4.2) gated by the register-current
event.

## 4.5 Global-current as derived state

Immutable history versions are the replicated source material. The per-layer
**global-current** winner tables are node-local derived state (ch. 2), so they
are not shipped. When the authority accepts a globally-settled version that
becomes a per-layer winner, it is reflected in `jazz_{table}_global_current` or
`jazz_{table}_register_global_current` (`INV-HIST-12`).

Those overwrite tables are the source of truth for `Global` current-row reads
and sync snapshots on a node that has observed the accepted version. They carry
only the settled per-layer winners, so a global current read is O(current) in the
rows and values returned. Local visibility layers optimistic writes over those
tables as described in §4.2; it does not rehydrate the global baseline from the
history/register DAG.

_Further invariants._ `INV-HIST-13` — re-ingesting the same commit unit with its
version rows in a different order is idempotent and conflict-free. `INV-HIST-14` —
rejected transactions never appear as accepted history and never participate in
currentness or domination.

## Open questions

- 🔶 **Merge-strategy extensibility.** The deterministic, grouping-insensitive
  merge contract (`INV-HIST-15`) is currently enforced only for the built-in
  strategies (`Lww`, `Counter`), not an external strategy-plugin surface. Decide
  whether the general contract is normative now or describes the built-ins only.
