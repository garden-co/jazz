# jazz — Specification · 11. Time-travel & branches

## Overview

Full history (ch. 4) gives the database two related capabilities: a reader can
observe settled state at a historical cut, and a writer can fork that cut into a
snapshot-base branch. This chapter defines the read model, the branch model, the
authorization gates around both, and the branch operations that preserve the
ordinary current-state rules while isolating branch overlays.

Invariant digest:

- `INV-BRANCH-1`: A time-travel read at `GlobalSeq` position MUST consider only globally settled transactions with `global_seq <= position` and MUST choose row/layer winners using the ordinary current-state winner rules over that subset.
- `INV-BRANCH-2`: A time-travel read MUST evaluate read policy over the historical state at the requested cut, not over current state.
- `INV-BRANCH-3`: `Node::at_time(time)` MUST resolve to the latest settled global position whose transaction time is `<= time`, returning `GlobalSeq(0)` when no such settled transaction exists.
- `INV-BRANCH-4`: A local historical read handle MUST NOT answer from incomplete local history; if `is_history_complete_for(shape, position)` is false it MUST return `Error::HistoricalReadRequiresServer` or route to a history-complete server one-shot.
- `INV-BRANCH-5`: A history-complete node at a sufficient watermark MUST answer `Node::at(position).read(...)` locally at exactly that position.
- `INV-BRANCH-6`: A snapshot-base branch MUST freeze its base at creation; later parent/main commits MUST NOT appear in the branch unless represented by branch overlay writes or explicit rebase/merge operations.
- `INV-BRANCH-7`: A branch read MUST resolve rows overlay-first: for any row with a current branch overlay winner, the branch MUST return the overlay winner and MUST NOT also return the base winner for that row.
- `INV-BRANCH-8`: Branch overlay writes MUST NOT affect parent/main current reads.
- `INV-BRANCH-9`: Sibling branch overlays MUST be isolated; a read on one branch MUST NOT observe overlay versions written only to a sibling branch.
- `INV-BRANCH-10`: Branch metadata MUST be durably recoverable across node reopen, including the frozen `base_global` cut.
- `INV-BRANCH-11`: Branch creation MUST be O(1)-style metadata creation independent of base row count; it MUST NOT copy base rows into the branch overlay.
- `INV-BRANCH-12`: Branch overlay partitions MUST be created lazily on first branch write, not at branch creation.
- `INV-BRANCH-13`: A branch-scoped exclusive transaction MUST NOT be accepted unless its authority, validation, and serialization semantics are explicitly specified.
- `INV-BRANCH-14`: Writes to non-open or unknown branches MUST fail rather than creating/using an implicit branch.
- `INV-BRANCH-15`: Branch overlay data MUST NOT ship to a session that cannot read the branch metadata row; branch readability gates overlay visibility before ordinary per-row policy checks inside the branch view, and branch writes MUST pass branch-row write policy before table write policy evaluated inside the branch view.
- `INV-BRANCH-16`: A branch-scoped subscription MUST include BranchId in its identity.
- `INV-BRANCH-17`: Merge-back MUST commit an open branch's net effects to its parent as one atomic mergeable squash with typed provenance (`Transaction.source_branch`) and then transition the branch to `merged` with overlay retained.
- `INV-BRANCH-18`: Discarding or merging a branch MUST make that branch read-only while retaining overlay history for audit.
- `INV-BRANCH-19`: Rebase MUST move a branch's frozen base by three-way per-column reconcile between the old base, the new base, and the branch overlay, using the same merge engine and strategies as merge-back, including large-value op-merge.
- `INV-BRANCH-20`: Rebase MUST preserve overlay `TxId`s and original write provenance; it MUST NOT replay overlay writes, remint transaction identities, or treat rebased overlay versions as newly written.
- `INV-BRANCH-21`: Rebase-then-merge-back MUST converge with merge-directly under the same merge oracle and per-column merge strategies.

## Details

### 11.1 Time-travel reads

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

### 11.2 Snapshot-base branches

The branch model has one branch kind: the **snapshot-base branch**. A branch is
identified by a branch record (`BranchRecord`) with
`{ branch_id, parent: Option<BranchId>, base: Option<SnapshotRef>, state }`, where
`state ∈ {Open, Merged, Discarded}`. A root branch has `parent: None` and no
base/fallback. An ordinary branch has a base snapshot that is **frozen at
creation**: later parent commits do not appear in the branch except through the
branch's own overlay writes or an explicit rebase operation (`INV-BRANCH-6`).

The branch base is conceptually a full `SnapshotRef`: an owner, a global
sequence cut, the owner's local HLC cut, and explicit dots, all pointing at a
concrete database cut. The branch's effective base cut is the whole `SnapshotRef`, not only
`global_base`. v1 execution currently supports only global-only `SnapshotRef`s:
`local_base` must be empty/zero-equivalent for its owner and `dots` must be
empty. Persistence and protocol should still represent the full `SnapshotRef`
shape and reject complex SnapshotRefs until branch reads can evaluate them.
Schema-version/lens
partitions (ch. 10) are orthogonal to branch identity.

Creating a branch records metadata only. It is O(1)-style and never copies base
rows into the overlay (`INV-BRANCH-11`). Branch creation is itself a
**mergeable write that works offline**: an offline creator branches at _its own_
settled watermark, honestly "the base as this client saw it".

### 11.3 Branch reads

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

### 11.4 Branch writes (v1: mergeable-only)

Branch writes are mergeable-only. A mergeable branch commit
(`commit_mergeable_on_branch`) first requires write permission on the branch's
`jazz_branches` metadata row, then evaluates ordinary table write policy inside
the branch view, then writes a pending transaction into the branch overlay
partition (`INV-BRANCH-15`). Evaluating policy inside the branch view lets a
branch preview its own permission-row edits.

**Implementation status.** The current branch write model rejects exclusive
branch writes: `open_exclusive_on_branch` returns
`UnsupportedBranchExclusive`, and `branch_exclusive_returns_v1_error` covers
that behavior. Branch subscriptions and rebase are not yet implemented. Their
intended contracts are `INV-BRANCH-16` and `INV-BRANCH-19` through
`INV-BRANCH-21` respectively. A write to a non-open or unknown branch fails
rather than creating an implicit branch (`INV-BRANCH-14`).

_Further invariants._ `INV-BRANCH-10` — branch metadata (including the frozen
`base_global` cut) is durably recoverable across reopen. `INV-BRANCH-12` —
overlay partitions are created lazily on first branch write, not at branch
creation.

### 11.5 Branch subscriptions and rebase

A branch-scoped subscription is identified by its `BranchId` in addition to the
ordinary subscription identity (`INV-BRANCH-16`).

Rebasing moves a branch's frozen base from its creation cut to a newer parent
`GlobalSeq` by reconciling the old base, new base, and branch overlay with the
same three-way per-column merge engine and strategies as merge-back. It adjusts
the branch view without replaying overlay writes or reminting overlay `TxId`s;
the original write times and authors remain the provenance of those versions
(`INV-BRANCH-19`, `INV-BRANCH-20`). Rebase-then-merge-back MUST converge with
merge-directly under the same merge oracle and strategies (`INV-BRANCH-21`).

### 11.7 Subsumed branch and time-travel notes

The former branch/snapshot TODOs and row-history project notes are now expressed
as branch and historical-read surface here. Per-object time travel is the first
bounded product shape: expose a row's version timeline and read a single object
at a known cut. Full point-in-time queries are broader because they require
query-wide completeness, branch-aware source resolution, and stable cut evidence
across every table the shape touches.

Prefix/batch storage sketches treat branch and schema dimensions as storage
keys, but the semantic model remains branch overlays and frozen bases, not a
public dependency on physical prefixes.

### 11.7 Subsumed branch and time-travel notes

The former branch/snapshot TODOs and row-history project notes are now expressed
as branch and historical-read surface here. Per-object time travel is the first
bounded product shape: expose a row's version timeline and read a single object
at a known cut. Full point-in-time queries are broader because they require
query-wide completeness, branch-aware source resolution, and stable cut evidence
across every table the shape touches.

Prefix/batch storage sketches treat branch and schema dimensions as storage
keys, but the semantic model remains branch overlays and frozen bases, not a
public dependency on physical prefixes.

## Open Questions

### Open questions (branches: future contract)

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
  records typed `Transaction.source_branch` provenance, and returns a typed
  `Accepted`, `Pending`, or `Rejected` outcome. A pending squash has one durable,
  node-local reservation of its exact `(Transaction, VersionRecord[])` commit
  unit which is reused across retries and reopen; branch writes and discard are
  blocked until it settles. Recovery reconciles a durably accepted or rejected
  reserved transaction before allowing retry. Accepted recovery requires the
  exact durable transaction and full normal-version payload to equal the frozen
  reservation before closing the branch. Rejected recovery requires the exact
  transaction envelope only: rejection contributes no normal versions, may
  precede history insertion, and leaves the branch open after reservation clear.
  Only that exact locally reserved
  transaction—not peer-supplied `source_branch` provenance—may
  transition the branch to `Merged`, and only after `Accepted`; `Rejected` clears
  the reservation and leaves the branch open. Overlay history is retained.
  When authority admission translates the reserved versions through a newer
  current schema/lens path, it durably rewrites the trusted reservation to that
  exact canonical admitted payload before transaction/history persistence. The
  TxId and branch association do not change; foreign `source_branch` provenance
  cannot trigger this rewrite. While a locally reserved unit is parked or being
  retried, trust is bound to its exact pre-translation transaction and sorted
  version payload, not merely its TxId; a same-TxId mismatch conflicts before
  translation or staging. A successful canonical rewrite deliberately advances
  that exact trusted binding to the translated payload.
  Content and deletion-register overlay
  winners are emitted independently, so a restored deletion-register winner can
  be squashed alongside the row's content winner. Discard is a metadata state
  flip to `Discarded`; both paths make the branch read-only. The correctness rule
  (the S8 oracle): a merge-back must equal the equivalent direct-on-parent write
  sequence for visible rows, deletion-register winners, and version parent
  frontiers. Open question: squash granularity for very large branches remains
  one unit vs chunked, gated on the S8 merge-back-cost metric. The design
  `mergedInto` field is not in the current `jazz_branches` schema.
- 🔶 **Branch-exclusive transaction model** (`INV-BRANCH-13`). Before such a
  transaction can be accepted, decide its branch authority, validation, and
  serialization semantics.
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
  (`owner`, `global_base`, `local_base`, and dots). The implementation persists only
  `base_global` and recovers a defaulted global-only base. v1 may continue to
  execute only global-only SnapshotRefs, but durable metadata should carry the
  full SnapshotRef shape and reject non-global-only bases until they are
  supported.
- 🔶 **Historical completeness watermark.** The design requires a
  history-complete node at a sufficient watermark to answer exactly at the
  requested position. The implementation's completeness check is conservative:
  `history_complete && position <= applied_global_watermark`.
- 🔶 **Per-object time-travel facade.** Expose row-local history first: version
  list, authored metadata, deletion/restore events, and a read-at-version API
  that fails when the node lacks the required history.
- 🔶 **Full point-in-time queries.** General `at(position)` queries need
  query-wide completeness evidence, aligned schema/lens projection, and stable
  behavior for includes, array subqueries, and policy dependencies.
- 🔶 **Branch deletion witnesses.** Maintained views over branch overlays need
  explicit deletion-register current witnesses so a deletion/restore transition
  cannot be missed by a branch-scoped subscriber.
