# jazz — Specification · 11. Time-travel & branches

Full history (ch. 4) gives the database two related capabilities: a reader can
observe settled state at a historical cut, and a writer can fork that cut into a
snapshot-base branch. This chapter defines the read model, the branch model, the
authorization gates around both, and the branch operations that preserve the
ordinary current-state rules while isolating branch overlays.

## 11.1 Time-travel reads

A time-travel read exposes the database as it was at a settled global cut. The
cut is named by a `GlobalSeq`, and the read includes only globally settled
transactions with `global_seq <= position`. Over that subset, the database uses
the ordinary current-state rules from ch. 4 to select row and layer winners, then
evaluates query filters, joins, and read policy against the historical state at
that cut, not against the present state (`INV-BRANCH-1`, `INV-BRANCH-2`, ch. 7).
The exact address is `NodeState::at(position)`.

Wall-clock lookup is a convenience over the same model, not a stronger source of
truth. `NodeState::at_time(time)` resolves to the latest settled position whose
transaction time is `<= time`, or to `GlobalSeq(0)` if no such transaction
exists. Because transaction timestamps can be affected by clock skew, this
mapping is best-effort and is not wall-clock truth (`INV-BRANCH-3`).

A historical read handle is read-only and **refuses to answer from incomplete
local history**. If the node is not history-complete for the shape at the
requested cut, it returns `HistoricalReadRequiresServer` or routes the read to a
history-complete server instead of fabricating an answer (`INV-BRANCH-4`).
Historical read handles are cheap values, not resources. A past-state watch has
no subscription semantics in this model, because the result at a historical cut
is constant.

_Further invariants._ `INV-BRANCH-5` — a history-complete node at a sufficient
watermark answers `at(position).read(...)` locally at exactly that position.

## 11.2 Snapshot-base branches

The branch model has one branch kind: the **snapshot-base branch**. A branch is
identified by a branch record (`BranchRecord`) with
`{ branch_id, parent: Option<BranchId>, base: Option<SnapshotRef>, state }`, where
`state ∈ {Open, Merged, Discarded}`. A root branch has `parent: None` and no
base/fallback. An ordinary branch has a base snapshot that is **frozen at
creation**: later parent commits do not appear in the branch except through the
branch's own overlay writes (`INV-BRANCH-6`).

The branch base is conceptually a full `SnapshotRef`: a global sequence cut plus
the creator's local HLC and explicit dots, all pointing at a concrete database
cut. The branch's effective base cut is the whole `SnapshotRef`, not only
`global_base`. v1 execution currently supports only global-only `SnapshotRef`s:
`local_base` must be empty/zero-equivalent and `dots` must be empty. Persistence
and protocol should still represent the full `SnapshotRef` shape and reject
complex SnapshotRefs until branch reads can evaluate them. Schema-version/lens
partitions (ch. 10) are orthogonal to branch identity.

Creating a branch records metadata only. It is O(1)-style and never copies base
rows into the overlay (`INV-BRANCH-11`). Branch creation is itself a
**mergeable write that works offline**: an offline creator branches at _its own_
settled watermark, honestly "the base as this client saw it".

## 11.3 Branch reads

A branch read is authorized first by the branch-metadata row RLS gate: a session
may see branch overlay/base data only if it can read that branch's
`jazz_branches` row. After that gate passes, the branch view resolves rows
**overlay-first**. For each row, a current branch overlay winner hides the base
winner and is returned as the row's value. If the branch has no overlay winner
for the row, the read falls back to `at(base.global_base)` on the parent view
(`INV-BRANCH-7`). Ordinary table read policy is then evaluated inside the branch
view, so branch-local permission rows participate in the policy result
(`INV-BRANCH-15`). This overlay-first rule isolates the branch from its parent
and siblings: branch overlay writes never affect parent/main current reads
(`INV-BRANCH-8`), and a read on one branch never observes a sibling's overlay
(`INV-BRANCH-9`).

Branch overlays are stored in partition tables keyed by
`(table, schema_version, branch_id)`, with those partitions recorded in
`jazz_branch_partitions`.

## 11.4 Branch writes (v1: mergeable-only)

Branch writes are mergeable-only. A mergeable branch commit
(`commit_mergeable_on_branch`) first requires write permission on the branch's
`jazz_branches` metadata row, then evaluates ordinary table write policy inside
the branch view, then writes a pending transaction into the branch overlay
partition (`INV-BRANCH-15`). Evaluating policy inside the branch view lets a
branch preview its own permission-row edits.

Exclusive branch writes are not part of the branch write model:
`open_exclusive_on_branch` returns `UnsupportedBranchExclusive`
(`INV-BRANCH-13`). A write to a non-open or unknown branch fails rather than
creating an implicit branch (`INV-BRANCH-14`).

_Further invariants._ `INV-BRANCH-10` — branch metadata (including the frozen
`base_global` cut) is durably recoverable across reopen. `INV-BRANCH-12` —
overlay partitions are created lazily on first branch write, not at branch
creation.

## Open questions (branches: future contract)

The branch tier beyond §11.2–11.4 still has unresolved contract points, while
merge-back and discard have graduated:

- 🔶 **Binding-facing branch facade.** Rust `Db`, TypeScript, WASM, and NAPI need
  a stable branch facade over the `Node` operations: create, read on branch,
  merge-back, discard, explicit base `SnapshotRef`, lifecycle state, provenance,
  and branch-scoped subscription identity. The facade should expose
  `BranchId`/`SnapshotRef` as opaque stable values and must not leak overlay
  partition table names.
- ✅ **Merge-back / discard** (`INV-BRANCH-17`, `INV-BRANCH-18`). Merge-back emits
  one atomic mergeable squash of the branch's net effects into the parent,
  records typed `Transaction.source_branch` provenance, then flips state to
  `Merged` with overlay history retained. Content and deletion-register overlay
  winners are emitted independently, so a restored deletion-register winner can
  be squashed alongside the row's content winner. Discard is a metadata state
  flip to `Discarded`; both paths make the branch read-only. The correctness rule
  (the S8 oracle): a merge-back must equal the equivalent direct-on-parent write
  sequence for visible rows, deletion-register winners, and version parent
  frontiers. Open question: squash granularity for very large branches remains
  one unit vs chunked, gated on the S8 merge-back-cost metric. The design
  `mergedInto` field is not in the current `jazz_branches` schema.
- 🔶 **Branch-dimensioned subscriptions** (`INV-BRANCH-16`, target). Subscription identity
  gains `BranchId` — `(ShapeId, BindingId, BranchId)` — sharing parent-side
  prepared-graph work so per-branch cost is overlay-only.
- 🔶 **Rebase** (target). Moving a branch's frozen base from its creation cut to a
  newer parent `GlobalSeq` has two candidate semantics, unresolved: **(a) recompute**
  — re-evaluate each overlay write as if newly applied on the new base (cherry-pick
  shape), minting fresh `TxId`s; or **(b) reconcile** — a three-way merge between the
  old base, the new base, and the overlay, reusing the §4.3 per-column merge
  strategies (rebase-as-merge). Implications to settle: (i) conflict surface is
  exactly the overlay rows whose parent winner changed between old and new base —
  per-column merge (incl. the large-value op-merge strategy) should apply there, the
  same engine merge-back uses; (ii) **identity** — recompute mints new overlay
  `TxId`s (breaking any external references and changing dedup), reconcile can
  preserve them; (iii) **cost** — recompute is O(overlay writes), reconcile also
  needs the parent diff between the two base cuts; (iv) interaction with merge-back —
  whether rebase-then-merge and merge-directly must converge (they should, under the
  same merge oracle as S8). Pick (a) or (b) before pinning a contract.
- 🔶 **Branch-of-branch depth** (target). A branch whose `parent` is itself a branch
  is unbounded by construction. Implications: (i) **reads** resolve overlay-first up
  the _chain_ of bases, so read cost is O(depth) base-cut resolutions — measure
  before bounding; (ii) **base freezing under a mutable parent** — a child's base is
  the parent branch's overlay+base _at the creation cut_, but the parent overlay
  keeps growing, so the child needs a stable cut over a branch view (the same
  machinery as composing `at()` inside an overlay; see below);
  (iii) **merge-back** becomes multi-level (child→parent-branch→…→main), each hop a
  §4.3 squash with `source_branch` provenance; (iv) **RLS composes per level** — the
  branch-metadata-row read/write gate (`INV-BRANCH-15`) chains, so reaching a child
  requires passing every ancestor branch row's policy. Decide a depth bound (or
  prove unbounded is cheap enough) before shipping.
- 🔶 **Time-travel within a branch** (target). Composing `at(position)` inside a
  branch overlay requires an additional cut dimension: a branch view is already
  overlay-first over a frozen base cut, so an in-branch historical read needs to
  distinguish the branch's own settle order from the parent base `GlobalSeq`.
  Resolve the cut model (independent per-dimension with documented skew vs a
  composed `(branch_seq, global_seq)` vector, cf. the sharding
  per-shard-position question, ch. 15) before allowing it. Branch-of-branch
  multiplies this per level. The implementation does not allow this composition.
- 🔶 **Branch base persistence.** The design base is a full `SnapshotRef`
  (`global_base`, local HLC, and dots). The implementation persists only
  `base_global` and recovers a defaulted global-only base. v1 may continue to
  execute only global-only SnapshotRefs, but durable metadata should carry the
  full SnapshotRef shape and reject non-global-only bases until they are
  supported.
- 🔶 **Historical completeness watermark.** The design requires a
  history-complete node at a sufficient watermark to answer exactly at the
  requested position. The implementation's completeness check is conservative:
  `history_complete && position <= applied_global_watermark`.
