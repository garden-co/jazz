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
- `INV-BRANCH-6`: A snapshot-base branch MUST freeze its base at creation; later parent/main commits MUST NOT appear in the branch unless represented by branch overlay writes, including ordinary calculated merge transactions.
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
- `INV-BRANCH-17`: A branch merge MUST be calculated locally from readable source and target views and emitted as one ordinary atomic mergeable transaction on the target, with no branch-specific fate or authority admission path; successful merge MUST leave the source branch open.
- `INV-BRANCH-18`: `Discarded` MUST be the only terminal branch state; discard makes a branch read-only while retaining overlay history, and merge MUST NOT close or otherwise mutate source lifecycle state.
- `INV-BRANCH-19`: Branch-merge provenance MUST define field-grained non-causal substitutions from each emitted target `(table,row,layer,column-or-operation)` to the exact source contribution dots the authorized merger claims it represents; a later local calculator MUST expand those substitutions rather than treating derived payload as new native contributions.
- `INV-BRANCH-20`: A local merge calculator MUST subtract the field-grained contribution closure already represented by the target from the source contribution closure, recursively expanding structurally valid substitutions, so merging in both directions MUST NOT echo target-originated counter deltas, large-value operations, or scalar writes back to their origin.
- `INV-BRANCH-21`: Incorporating another lineage into a branch MUST use the same ordinary merge-transaction calculation as merging a branch into main; branch rebase is not a separate operation.
- `INV-BRANCH-22`: A merge-back squash's row-version parents MUST be only the target row/layer heads observed at the merge snapshot; source-branch transactions MUST NOT be causal parents of the target transaction.
- `INV-BRANCH-23`: For each row/layer/column touched by novel source contributions, the local calculator MUST derive the equivalent ordinary target write contribution under that column's normal merge strategy, including cumulative explicit authorship and explicit writes equal to their prior value, while excluding inherited materialized cells.
- `INV-BRANCH-24`: Branch-merge provenance MAY carry a source lineage plus canonical `from_frontier` and `through_frontier` pointers as audit/calculation hints, but those pointers MUST NOT imply that every source contribution in that interval was transferred and MUST NOT be interpreted as an authoritative cursor, CAS, transaction parent, admission prerequisite, or global duplicate-prevention record.
- `INV-BRANCH-25`: Once minted, a branch merge transaction MUST use ordinary transaction identity, limits, fate, authorization, storage, synchronization, rejection, and exact-retransmission idempotency; receivers MUST NOT need branch metadata or source history to apply it.
- `INV-BRANCH-26`: Trusted cores and edges MAY inspect all source-branch history needed to calculate a merge for an authorized initiator, while client-facing branch data remains permission-scoped; target readers MUST be able to ingest the resulting ordinary transaction without receiving any source-branch transaction or payload.
- `INV-BRANCH-27`: Branch merge calculation MUST read both source and target through one current-schema view and emit one ordinary transaction in that schema; branch provenance MUST NOT introduce cross-schema authored-presence or lens protocol semantics.
- `INV-BRANCH-28`: A source frontier MUST be the canonical sorted de-duplicated maximal antichain of eligible transactions in that lineage's own version-parent graph; frozen-base and cross-lineage transaction parents are not source-frontier edges, while merge provenance contributes only to the separate contribution graph.
- `INV-BRANCH-29`: The local calculator MUST subtract every prior merge provenance visible in its target snapshot, but the system MUST NOT claim globally coordinated exactly-once behavior for independently calculated offline/concurrent merges; unobserved duplicate attempts are ordinary concurrent writes and coordination or reconciliation remains the merger's responsibility.
- `INV-BRANCH-30`: Branch-merge provenance is trusted merger-authored metadata to the same degree as the transaction's write payload: ordinary admission MUST NOT reconstruct or attest the claimed source calculation, while a local calculator MUST reject structurally malformed substitutions and MAY defensively recompute them when complete source history is available.
- `INV-BRANCH-31`: Contribution closure and subtraction MUST be tracked per exact `(table, row, layer, column-or-operation)` dot, never transaction-wide; sharing a multi-row `TxId` MUST NOT make unrelated dots known.
- `INV-BRANCH-32`: Every supported merge strategy MUST provide local `extract_native(parent contribution closure, stored value/ops)` and `encode_target_relative(novel contribution, target frontier)` semantics; merge calculation MUST fail locally when either capability is absent.
- `INV-BRANCH-33`: The calculator MUST consume an exact current-schema contribution view containing projected values, authored presence, and strategy operations; when storage/lenses cannot supply that view exactly, or the initiator cannot prove source-read authorization for every included content/deletion contribution, it MUST fail locally before minting the ordinary transaction.
- `INV-BRANCH-34`: A source contribution is target-known only when it is already present in the exact target-parent contribution closure or an exact field substitution names that dot; sharing a `TxId`, appearing inside `from_frontier`/`through_frontier`, or transferring another field from the same source version MUST NOT suppress an omitted row, layer, column, or operation, and a reducing strategy's substitution MUST name every novel dot reduced into its output, including losing concurrent dots.
- `INV-BRANCH-35`: Every transaction MUST carry one canonical operational target lineage (`Root` or a stable `BranchId`); all ordinary persistence, recovery, synchronization, authorization, fate, and exact-retransmission paths MUST route its complete commit unit to that lineage without interpreting branch-merge provenance.
- `INV-BRANCH-36`: Before a receiver may admit a commit unit or view payload targeted at a branch, trusted transport MUST deliver the durable branch record needed to route that lineage. A receiver that observes data first MUST park it, request the bounded missing branch record, and drain it only after exact idempotent metadata admission; a branch record received without a currently requested readable view reveals no branch row payload.

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
`state ∈ {Open, Discarded}`. A root branch has `parent: None` and no
base/fallback. An ordinary branch has a base snapshot that is **frozen at
creation**: later parent commits do not appear in the branch except through the
branch's own overlay writes, including an ordinary calculated merge transaction
that incorporates another lineage (`INV-BRANCH-6`).

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

Target lineage is part of the ordinary immutable `Transaction` payload, not an
out-of-band branch command. `Root` is an explicit canonical wire/storage value;
a branch target carries the wire-stable `BranchId`. The transaction and every
version in its atomic commit unit are stored, recovered, retransmitted, and
authorized against that target (`INV-BRANCH-35`). `BranchMergeProvenance` says
where a merger claims to have calculated effects; it never selects where those
effects are written. Consequently a target reader can process a merge as an
ordinary transaction once routing has selected its target partition, without
source-branch knowledge.

**Synchronization.** Branch metadata is a durable routing prerequisite, not a
replacement transaction format. Cores and edges may retain every branch record
and branch-target transaction. When a client registers a branch read view, the
serving node first applies the branch-row read gate, then sends the exact branch
record before any branch-target version or view payload. A client parks an
out-of-order branch-target unit, requests the missing record through the
ordinary bounded repair lane, and resumes normal ingest after idempotent
metadata admission (`INV-BRANCH-36`). The branch target remains solely the
ordinary transaction's `target_lineage`; metadata never reconstructs a facade
state or gives an unauthorized client branch rows.

**Implementation status.** The current branch write model rejects exclusive
branch writes: `open_exclusive_on_branch` returns
`UnsupportedBranchExclusive`, and `branch_exclusive_returns_v1_error` covers
that behavior. Branch subscriptions are not yet implemented. Their intended
contract is `INV-BRANCH-16` and `INV-BRANCH-36`. A write to a non-open or
unknown branch fails rather than creating an implicit branch (`INV-BRANCH-14`).

_Further invariants._ `INV-BRANCH-10` — branch metadata (including the frozen
`base_global` cut) is durably recoverable across reopen. `INV-BRANCH-12` —
overlay partitions are created lazily on first branch write, not at branch
creation.

### 11.5 Merge-back as target-local import

Branch merge is a maximally separate **local calculation layer**. The merger
reads an authorized source-branch view and an authorized target view, calculates
one target write, and then submits that write through the ordinary transaction
API. There is no branch-import authority operation. Edges and cores may perform
the calculation because they are trusted to inspect branch storage; an
untrusted client sees only rows allowed by the branch gate and row policies. In
either case the initiating identity must be able to read every source effect
included in the atomic calculation and must pass the target's ordinary
read-for-write and operation-specific write policies. The calculator fails the
whole merge rather than silently omitting an unreadable source effect
(`INV-BRANCH-15`, `INV-BRANCH-26`). `AuthorId::SYSTEM` does not replace the
initiator for these policy decisions.

The output is a normal `TxKind::Mergeable` transaction in the current schema.
It has ordinary fate, durability, transaction limits, RLS, storage, sync,
rejection, and retry behavior. Each emitted content or deletion version names
the complete current target frontier for that exact row and layer as parents;
it never names a source transaction. After minting, every receiver can apply the
transaction without the source branch, its provenance graph, or any witness or
repair request (`INV-BRANCH-22`, `INV-BRANCH-25`).

#### Source and contribution frontiers

A lineage's **source transaction graph** contains its own eligible durable
transactions and only version-parent edges whose endpoints are both in that
lineage. Frozen-base parents and transactions in another lineage are excluded.
Its frontier is the canonical sorted, de-duplicated maximal antichain of this
graph. The empty lineage has the empty frontier. This graph answers "where has
the source advanced?" but is deliberately not the target transaction's causal
graph (`INV-BRANCH-28`).

Merge calculation additionally uses a separate **contribution graph**. A native
user write introduces stable contribution dots:

```text
(origin lineage, origin TxId, table, row, layer, column-or-operation)
```

Large-value operations retain their existing operation identities. An ordinary
branch-merge transaction introduces no new native dots for its calculated
payload. Instead, its typed metadata records a non-causal provenance edge:

```text
BranchMergeProvenance {
    source_lineage,
    from_frontier,
    through_frontier,
    substitutions: [
        target (table, row, layer, column-or-operation)
            -> exact source contribution dots,
        ...
    ],
}
```

The merge transaction's contribution closure is its ordinary target parents'
closures plus the exact source dots named by each field substitution.
The `from_frontier` records a source cut the local calculation found represented
by its target snapshot, and `through_frontier` records the source cut it
examined. They are audit/calculation hints only: neither claims that every dot in
the interval was transferred. An omitted row, layer, column, or operation
remains novel even when it shares a source `TxId` with a transferred dot
(`INV-BRANCH-24`, `INV-BRANCH-34`). Neither pointer is a transaction parent,
durable cursor, CAS, admission prerequisite, or global duplicate-prevention
mechanism. A calculated merge transaction cannot also contain unrelated new
user edits; conflict-resolution edits are separate ordinary transactions so
their native contribution dots remain unambiguous.

Dots and closure are field-grained, not transaction-grained. A multi-row or
multi-column transaction can introduce many independent dots; learning or
importing one `(table, row, layer, column-or-operation)` dot never marks another
dot with the same `TxId` as represented (`INV-BRANCH-31`). For an ordinary
native write whose parents already contain imported contributions, a strategy
extracts only the new residual contribution from its stored representation:
counter subtracts the resolved parent contribution, a semilattice extracts its
new join component, and text/blob uses operation identities not present in the
parent closure. Imported parent state is therefore not re-labelled as a native
child contribution merely because the child authored the same column.

Provenance is trusted merger-authored transaction metadata, with the same trust
boundary as the merger's row effects. A merger authorized to overwrite target
data could already destroy or misrepresent that data; authority admission does
not gain a meaningful security boundary by reconstructing whether its lineage
claim is honest. Ordinary admission therefore persists and forwards provenance
without source-history access and without attesting to its truth. Future local
calculators follow structurally valid field substitutions. A history-complete
trusted node may defensively recompute a substitution and fail the local
calculation on a mismatch, but that is diagnostics/correctness hardening rather
than an authority or receiver-side security proof (`INV-BRANCH-30`).

To calculate source→target, the merger recursively expands native contribution
dots through ordinary lineage-parent edges and prior field
substitutions. It subtracts exactly the dots already represented by the target
snapshot from the source dots reachable at the examined source cut. The
remaining set is the **novel contribution set**. `from_frontier` and
`through_frontier` summarize cuts examined by the calculator. A dot is known to
the target only through the exact target-parent closure or an explicit
field substitution. A reducing strategy such as LWW names every novel dot it
reduced into the emitted field, not merely the dot whose value won, so a losing
concurrent write cannot echo in a later merge.

This recursive expansion is what prevents bidirectional and transitive echo.
For example, after main→B imports a main counter increment and text operation,
that merge transaction may be after B's previous source pointer. A later B→main
calculator expands the B merge transaction through its provenance edge, sees
that the counter contribution dot and text operation already exist in main's
target closure, and subtracts them. It does not interpret the materialized
counter value or target-relative text batch in B as a fresh B-authored effect.
The same rule works through A→B→C→A chains and for scalar writes
(`INV-BRANCH-20`).

Full retained history is the correctness source for this graph. Implementations
may cache closure summaries or checkpoint contribution sets, but a missing
provenance edge, source transaction, or native operation needed by the
calculation is a local merge-calculation error, never permission to guess.
Automatic history GC must not discard the only expansion path. Bounded summary,
checkpoint, and explicit-GC evidence are open design work below.

#### Calculating the ordinary target write

The local calculator reduces the novel contribution set under each column's
normal merge strategy, then encodes the result exactly as a caller-authored
target write relative to the captured target parents (`INV-BRANCH-23`):

- LWW-like scalar columns select the resolved value contributed by the novel
  source edits. Every column explicitly authored anywhere in the novel set is
  present, including an explicit write equal to its previous value; inherited
  materialized cells are absent.
- Grow-only/state-join strategies transfer their novel join contribution.
- Counter transfers the de-duplicated sum of native source deltas exactly once
  and emits the ordinary target-relative counter value.
- Text/blob transfers de-duplicated native operations, applies them to the
  target frontier, and emits the ordinary target-parent-relative operation batch
  plus self-contained, target-authorized extents/checkpoint material.
- A custom strategy must provide the local calculator with a deterministic way
  to reduce novel contribution dots and encode an ordinary target write. If it
  cannot, merge calculation fails locally before minting a transaction.

More generally, every supported strategy supplies two local calculation
capabilities: `extract_native(parent contribution closure, stored value/ops)`
and `encode_target_relative(novel contribution, target frontier)`. Counter's
first operation extracts a residual delta and its second emits the target value
plus that delta. Text/blob extracts stable operation ids and re-encodes their
effect relative to target heads. LWW extracts explicitly authored values.
Grow-only set is supported only when its strategy exposes the corresponding
residual join operation; otherwise it remains a named local capability gap.
Custom strategies fail locally when either operation is absent
(`INV-BRANCH-32`).

Content and deletion remain independent layers. Novel deletion/restore dots
reduce to the final deletion-register contribution; content contributions are
retained even when the resulting deletion state hides the row, so a later
restore reveals the correct content. The calculated versions then undergo the
same target authorization and column strategies as any other commit. There is
no strategy-specific behavior in authority admission or on receivers.

Both input views and the emitted transaction use one current-schema projection.
The calculator consumes a current-schema **contribution view** that must include
exact projected values, authored presence, deletion events, and strategy-native
operations/dots. This chapter does not define lens translation for those facts:
if underlying history cannot project them exactly into the current schema, the
calculator reports a local capability error. A same-schema implementation is
valid; silently falling back to materialized cells is not. Merge provenance adds
no authored-presence lens protocol (`INV-BRANCH-27`, `INV-BRANCH-33`).

Source authorization is checked against the initiating identity, not trusted
storage visibility. Current visible source content must pass the branch gate and
current branch-view read policy. A deletion contribution additionally needs a
defined source read/read-for-write proof for the affected historical content.
Until historical deleted-row policy evaluation is defined, a calculator that
cannot prove that authorization fails the whole local calculation rather than
including or silently omitting the deletion (`INV-BRANCH-33`). Target
authorization remains the ordinary target read-for-write and write-policy path.

Successful merge leaves the source branch `Open`. Incorporating main into B,
B into main, or one branch into another is always another calculated ordinary
merge transaction. There is no separate rebase operation. `Discarded` is the
only terminal state and retains history for audit (`INV-BRANCH-17`,
`INV-BRANCH-18`, `INV-BRANCH-21`).

#### Duplicate calculations and retries

Exact retransmission of the same minted transaction is idempotent by ordinary
`TxId` commit-unit rules. Before minting, a calculator subtracts all provenance
visible in its target snapshot, so a sequential repeat with no new source
contributions is a no-op calculation. There is intentionally no authoritative
source/target cursor or global branch-merge CAS.

Two offline or concurrent calculators can nevertheless both derive writes from
snapshots that do not include the other's merge. Those are distinct ordinary
transactions, not exact retries. Idempotent state-join contributions may absorb
the duplication, but non-idempotent counter or custom contributions can be
applied twice. Preventing, detecting, or compensating such a race is the
merger's coordination/reconciliation responsibility; Jazz does not claim
globally exactly-once branch import (`INV-BRANCH-29`).

### 11.6 Branch subscriptions

A branch-scoped subscription is identified by its `BranchId` in addition to the
ordinary subscription identity (`INV-BRANCH-16`).

There is no branch rebase operation. To incorporate a newer parent or another
lineage, calculate and commit an ordinary merge transaction into the branch as
defined in §11.5 (`INV-BRANCH-21`).

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
- ✅ **Merge-back / discard** (`INV-BRANCH-17` through `INV-BRANCH-27`).
  A local calculator emits one ordinary target transaction for the novel source
  contribution set, records typed `BranchMergeProvenance`, and leaves the branch
  open. Discard is a separate metadata state flip to `Discarded` that makes the
  branch read-only. The correctness rule (the S8 oracle): each merge equals the
  corresponding direct-on-target strategy contribution for visible rows,
  deletion-register winners, and target-only version parent frontiers.
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
  §11.5 ordinary transaction with `BranchMergeProvenance`; (iv) **RLS composes per level** — the
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
- 🔶 **Opaque import frontier pointer.** Raw source frontier `TxId`s are
  acceptable in typed `BranchMergeProvenance` today. If exposing transaction
  identities becomes undesirable, define a provenance-resolvable opaque pointer
  or commitment that preserves canonical frontier equality, recursive
  contribution expansion, and auditability without becoming a causal witness or
  requiring target readers to fetch source history.
- 🔶 **Contribution-summary scaling and GC.** Correct local calculation now
  expands retained native contribution and merge-provenance history. Define a
  bounded, verifiable closure summary/checkpoint and explicit history-GC evidence
  before allowing removal of the only path to a contribution dot. A cache may
  accelerate the calculation but cannot become an unverifiable source of truth.
- 🔶 **Concurrent duplicate merge calculations.** Two offline calculators
  can independently transfer the same non-idempotent contribution before either
  observes the other's provenance. The current contract assigns serialization
  or later reconciliation to the merger. Explore optional local coordination or
  contribution-aware compensation without adding branch-specific authority
  admission.
- 🔶 **Oversized atomic merge.** An emitted merge is one ordinary
  transaction and therefore inherits ordinary transaction size limits. Decide
  whether a logical merge may be chunked while retaining an atomic application
  boundary; do not add a branch-only authority envelope by accident.
