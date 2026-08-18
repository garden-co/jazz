# Partition-dimension overlays: removal-first implementation plan

## Outcome

Replace the unreleased core-owned branch subsystem with the ch. 11 partition
primitive in two deliberate movements:

1. remove every old branch identity, lifecycle, storage, transport, query, and
   test dependency until the ordinary shared-table core is green; then
2. introduce stable partition dimensions from the storage key upward, followed
   by exact-partition reads, head/base reduction, policy, sync, copy-on-write,
   and contribution merges.

Do not retain a compatibility adapter. There are no persisted or API users to
protect, and a shim would preserve the wrong transaction, protocol, and storage
boundaries. Each landing commit should leave one coherent model rather than
dual-running old and new branch semantics.

## Ground rules

- `RowUuid` remains global object identity.
- `PartitionTuple` is explicit immutable version metadata even though its values
  are also projected through ordinary bound application columns. Deletion rows
  have no user cells, so deriving the tuple from cells cannot be the storage
  contract.
- Content and deletion version parents never cross partition tuples.
- One transaction may contain versions from several tuples.
- The empty tuple is the only representation needed for ordinary shared tables;
  it is not a special root lineage.
- No core branch row, lifecycle, parent graph, metadata outbox, metadata repair,
  or transaction-level target lineage survives the cut.
- Old tests are not silently rewritten to bless intermediate behavior. Tests
  tied to retired APIs are removed from compilation and entered in a porting
  ledger before production code is deleted. Each ledger row is later restored,
  replaced by a new black-box test, or explicitly retired because its product
  requirement moved to userland.
- Prefer public schema, query, mutation, and policy builders in restored tests,
  following `crates/jazz/TESTING_GUIDELINES.md`.

## Phase 0 — inventory and a reversible test quarantine

### 0.1 Capture the removal manifest

Record every old symbol and durable artifact before editing:

```text
BranchId
BranchRecord / BranchState / BranchMetadata
BranchLineage::{Root, Branch}
Transaction.target_lineage
BranchMergeProvenance source lineage fields
jazz_branches
jazz_branch_partitions
physical_branch_* tables and projections
branch-qualified deletion encoding
FetchBranchMetadata and branch metadata acknowledgements
ReadViewSourceSpec::{Branch, MergedBranches}
Db::{create_branch, create_branch_with_id, insert_on_branch}
NodeState::{create_branch, discard_branch, query_rows_on_branch,
           commit_mergeable*_on_branch, merge_back_branch,
           merge_lineage_into, open_exclusive_on_branch}
```

Use `rg` receipts in the removal PR so the final zero-match check is mechanical.

### 0.2 Preserve tests without compiling the old API

Create `dev/partition-overlay-test-porting.md` with one row per affected test:

```text
old test | old guarantee | new invariant | disposition | replacement test
```

Use three dispositions:

- **port** — the guarantee remains, such as tuple isolation, frozen-base reads,
  delete/restore masking, subscription identity, or no-echo merges;
- **replace** — the useful scenario remains but the public setup changes, such
  as branch-row RLS becoming ordinary reference-traversing policy;
- **retire** — the guarantee belongs to the deleted model, such as creator
  matching, `Open -> Discarded`, metadata-before-target-lineage routing, or
  rejection of an unknown branch id.

Move legacy-only test modules out of Rust module discovery into a temporary
`dev/legacy-branch-tests/` archive or remove their `mod` registration in the same
commit that creates the complete ledger. Do not use `#[ignore]`: ignored tests
still compile and would force legacy public types to remain. Do not modify a
surviving test assertion merely to get an intermediate build green.

The first restored tests should be black-box integration tests. Keep internal
tests only for storage-key reconstruction, contribution-dot closure, or another
mechanism that cannot be observed through the public API; document that reason.

## Phase 1 — delete the old model and return to a shared-table core

This phase intentionally removes functionality before replacement. Its exit is
a green core in which every table behaves as `partitionBy: []`.

### 1.1 Remove facade and semantic read vocabulary

- Delete the branch methods from `Db`, serving/test shells, public query builders,
  and binding conversion.
- Remove `Branch`, `MergedBranches`, branch strings, `"main"` aliases, and
  branch-specific capability errors from `ReadViewSourceSpec` and public query
  options.
- Keep ordinary current, snapshot, schema, and transaction-overlay read forms.
- Remove branch subscription setup and cache discriminators. Do not temporarily
  map a branch request to current state.

### 1.2 Remove lifecycle and metadata transport

- Delete `BranchId`, `BranchRecord`, `BranchState`, metadata codecs, protocol
  limits, `BranchMetadata`, metadata fetch/ack messages, pending metadata
  outboxes, session creator validation, parking for missing branch metadata, and
  relay acknowledgement state.
- Delete `jazz_branches` and `jazz_branch_partitions` from schema lowering,
  recovery, descriptors, server helpers, and simulations.
- Delete open/discard/root-branch concepts rather than emulating them with the
  empty tuple.

### 1.3 Collapse storage to one empty partition tuple

- Replace `BranchLineage` parameters in content/deletion/current helpers with no
  lineage parameter temporarily.
- Remove physical branch-history tables, partition discovery, branch projection
  registration, and branch-specific recovery.
- Encode existing shared content and deletion history as the future empty-tuple
  case where practical, so phase 2 extends keys rather than redesigning them
  twice.
- Rebuild root currentness, fate, rejection, retransmission, and reopen behavior
  from retained history.

### 1.4 Remove transaction target routing

- Delete `Transaction.target_lineage` from the Rust type, codec, wire fixtures,
  persistence, ingest validation, fate paths, pending edges, repair, and exact
  retransmission checks.
- Route every version by table in the temporary shared-only model.
- Preserve transaction-wide identity, permission subject, limits, fate,
  durability, and atomic storage.
- Do not overload merge provenance as a routing field.

### 1.5 Temporarily remove cross-lineage merge entry points

- Remove branch merge APIs and old `BranchMergeProvenance` coordinate types.
- Preserve generic per-strategy native-contribution extraction and
  target-relative encoding code if it can compile without branch types; move it
  into a neutral contribution module.
- If separation is not clean, delete the calculator and restore it in phase 7
  from its tests and spec rather than retaining a branch-shaped abstraction.

### Phase 1 exit gate

- No old symbol or durable table names remain outside the porting ledger and git
  history.
- Ordinary mergeable/exclusive transactions, current reads, historical reads,
  policies, subscriptions, sync, recovery, and lenses pass their focused suites.
- Storage reopen reconstructs the empty-tuple current state.
- `cargo check -p jazz-sim --benches` passes after branch scenario references are
  removed or replaced with explicit target placeholders.

## Phase 2 — stable schema dimensions and canonical tuple encoding

### 2.1 Add schema identities

Introduce:

```rust
PartitionDimensionId(Uuid)
PartitionDimension {
    id,
    name,
    value_type,
    canonical_order,
    migration_default,
}
PartitionBinding {
    dimension: PartitionDimensionId,
    column: PhysicalColumnId,
}
PartitionTuple(Vec<(PartitionDimensionId, Value)>)
```

The public schema builder declares global dimensions and binds table columns to
subsets. Validation enforces one stable type/encoding per dimension, no duplicate
binding, non-null supported values, and canonical dimension order. Bind through
`PhysicalColumnId` so application columns can be renamed without changing tuple
identity.

Start with UUID, stable enum identity, and fixed-width integer dimensions.
Reject strings, floats, blobs, nullable values, and composite values until their
canonical identity contract is explicit.

### 2.2 Encode tuples once

- Define one canonical typed byte encoding sorted by dimension id/order.
- Normalize a table tuple to exactly its declared dimension subset.
- Represent the shared table tuple as canonical empty bytes.
- Store tuple bytes explicitly in every content and deletion `VersionRecord`.
- Validate that bound application cells agree with the explicit tuple after
  authored-schema decode.
- Include tuple bytes in canonical wire equality and exact retransmission.

### 2.3 Add monotone schema evolution

- A new dimension publication must include an immutable typed migration default.
- Project old versions and old-schema selectors by inserting that default.
- Require new-schema writes to supply every newly bound value explicitly.
- Allow cross-schema version parents only when normalized tuples are equal.
- Forbid dimension removal, stable identity/name change, type/encoding/default
  change, nullable binding, split, and collapse.
- Permit application column rename because the binding follows
  `PhysicalColumnId`.

### Phase 2 tests

- public schema acceptance/rejection matrix;
- column rename retains dimension association;
- two tables bind compatible subsets;
- old history normalizes into the reserved default bucket;
- new-schema omission fails while old-schema selector completion succeeds;
- tuple codec canonicality and malformed/duplicate dimension rejection.

## Phase 3 — tuple-qualified history, current state, and indices

### 3.1 Rekey physical state

Use these logical keys:

```text
content history:  (table, tuple, row, tx)
deletion history: (table, tuple, row, tx)
content current:  (table, tuple, row)
deletion current: (table, tuple, row)
combined current: (table, tuple, row)
global changes:   (table, tuple, row, layer, global_seq)
```

The exact Groove lowering may use canonical tuple bytes as one prefix field, but
all bounded scans and rebuilds must preserve the logical key. Content and
deletion winners remain independent.

### 3.2 Prefix indices implicitly

- Prefix every physical secondary and unique key with the exact normalized
  table tuple.
- Preserve user index declarations without redundant partition columns.
- Enforce uniqueness per exact tuple only.
- Rebuild index state when a dimension is added and historical tuples acquire a
  default component.

### 3.3 Admit cross-partition commit units

- Route each version by its explicit tuple.
- Permit several tuples and shared tables in one transaction.
- Validate all writes before committing any of them.
- Reject the whole transaction for one malformed tuple, cross-tuple parent,
  policy denial, schema gap, or storage failure.
- Preserve one fate and exact idempotency decision for the complete unit.

### Phase 3 canaries

- same `RowUuid` has independent winners in two tuples;
- tuple-local delete/restore cannot affect a sibling;
- same unique value is accepted in two tuples and rejected twice in one tuple;
- indexed update/delete retracts only the exact tuple key;
- cross-partition transaction is wholly visible or wholly absent after crash and
  reopen;
- a cross-tuple version-parent edge is rejected.

## Phase 4 — exact-partition reads and two-source overlay reduction

### 4.1 Add canonical read sources

Introduce normalized named values:

```rust
PartitionSource::Current { values }
PartitionSource::Snapshot { values, snapshot }

OverlayReadView {
    head: PartitionSource::Current,
    base: Option<PartitionSource>,
}
```

The facade may accept schema-ordered arrays, but validation immediately produces
named stable-dimension values. Reject missing required or unknown dimensions.
Project the selector onto each table's subset; collapse equal head/base
projections and always collapse the empty tuple.

### 4.2 Build layer reducers before query algebra

For every table source, lower independent head/base reducers for content and
deletion, then derive visibility. Head presence masks base presence even if the
head row later fails a user predicate. A live base remains maintained; a
snapshot base reads all participating tables and policy dependencies at one cut.

Expose two identities:

- effective bound partition columns contain requested head values;
- hidden typed provenance records the supplying tuple and version independently
  for content and deletion.

### 4.3 Make indexed plans mask correctly

For an indexed predicate, compute:

```text
head_matches
UNION
anti_join(base_matches, all_head_touched_ids_for_the_layer)
```

Do not anti-join only matching head rows. Add a planted regression where the
head changes an indexed value so it fails the predicate but must still hide the
base's old matching value.

### Phase 4 acceptance ladder

1. exact-partition one-shot reads;
2. shared and subset-dimension tables in one join;
3. live-base content and deletion fallback;
4. frozen-base isolation from post-cut base changes;
5. mask-before-filter and mask-before-index;
6. include/relation/reachability queries;
7. aggregate/window results;
8. one-shot versus independent semantic oracle.

## Phase 5 — policies, references, and copy-on-write mutations

### 5.1 Evaluate policy in the effective view

- Partition columns are normal policy-visible candidate values.
- Ordinary `RowUuid` references resolve through the operation's effective view.
- Policy dependencies project the same named selector onto their table subsets.
- Missing referenced branch/lifecycle/membership rows are ordinary missing
  evidence and fail closed.
- Do not add a core partition-existence or lifecycle hook.

### 5.2 Add exact and view-relative mutation targets

```rust
ExactRowRef { table, tuple, row_uuid }
ViewRowRef { table, read_view, row_uuid }
```

An exact update targets that incarnation. A view-relative update always targets
the head tuple; if the visible content is inherited, it creates the first head
version by copy-on-write without a cross-tuple parent. The operation explicitly
authors `Restored` when desired; content alone never restores an inherited
deletion.

Add an explicit atomic move helper implemented as source delete plus destination
insert/restore in one cross-partition transaction.

### Phase 5 black-box tests

- reference-traversing policy through an application-owned branch row;
- forged/nonexistent branch value fails because ordinary policy evidence is
  absent, not because the core recognizes branch ids;
- workspace-only membership table is shared across branch tuples;
- view-relative inherited update creates a head incarnation and leaves base
  unchanged;
- exact base update changes base and affects live but not frozen overlays;
- content-only update does not restore; explicit restore does;
- policy denial in one tuple rejects a multi-tuple transaction atomically.

## Phase 6 — maintained subscriptions and sync

### 6.1 Canonical identity

Normalize ordered head/base sources, snapshot cut, schema, tier, binding, and
policy scope into `ReadViewKey`. Ensure sibling tuples cannot share binding-view
state, known state, replacement witnesses, receipts, or unsubscribe cleanup.

### 6.2 Maintained source graph

- Exact partition-current is a maintained source keyed by tuple.
- Head content/deletion touches anti-join base winners independently.
- Live base changes flow when unmasked.
- Frozen base facts are immutable inline/historical inputs.
- Equal projected sources are installed once.
- Effective head values and supplying-layer provenance flow through terminal
  witness facts.

### 6.3 Replace metadata repair with generic closure repair

There is no branch metadata prerequisite. Tuple routing is self-contained in
each version. Application branch rows needed by policy travel as ordinary safe
policy dependencies or opaque admission facts under ch. 8.

### 6.4 Cross-partition confidentiality

- Trusted core/edge history links may receive complete commit units.
- Untrusted selected links receive only authorized version/program facts.
- A selected witness may name `TxId` for fate/settlement but must not reveal
  hidden sibling count, table, tuple, or payload.
- Audit `n_total_writes`, complete-payload refs, repair messages, rejection
  reasons, metrics, and logs for side channels.
- Publication waits for durable atomic authority fate even when only a subset of
  consequences is visible.

### Phase 6 canaries

- sibling subscriptions with identical shape/binding remain isolated;
- head add/replace/delete/restore updates only that tuple;
- live base changes update only unmasked rows;
- frozen base ignores later base writes;
- reference-policy grant/revoke uses the effective view;
- reconnect and known-state replay cannot cross tuple identities;
- a client authorized for one sibling of a cross-partition transaction learns
  neither payload nor count of the hidden sibling;
- maintained and one-shot overlay results match under seeded differential runs.

## Phase 7 — restore generic contribution merges

### 7.1 Neutral coordinate types

Replace lineage fields with:

```rust
PartitionCoordinate(Vec<(PartitionDimensionId, EncodedValue)>)
ContributionDot {
    partition,
    tx_id,
    table,
    row_uuid,
    layer,
    component,
}
```

Normalize old-schema dots by immutable dimension defaults. Keep provenance
non-causal and field-grained.

### 7.2 High-level helper

Expose a helper over explicit source and target partition views. It must:

- read exact current-schema contribution views;
- prove initiator source-read and target-write authority;
- recursively expand prior substitutions;
- subtract target-known dots;
- encode novel contributions relative to target incarnation heads;
- use only target-tuple version parents;
- emit one ordinary cross-partition-capable transaction;
- fail before minting on incomplete history or unsupported strategy.

Do not add merge cursors, lifecycle transitions, source closure on success, or
receiver-side source verification.

### Phase 7 tests

Port the current contribution oracles using public partition schemas:

- scalar and counter source-to-target equivalence;
- delete/restore;
- explicit authored value equal to inherited value;
- multi-row transaction dots remain field-grained;
- A -> B -> C -> A does not echo;
- prior observed provenance suppresses retry;
- concurrent unobserved calculators remain ordinary concurrent writes;
- target receiver applies result without source history;
- schema-added default dimension preserves provenance identity.

## Phase 8 — restore the public surface and delete the quarantine

- Add public schema dimension and table binding builders.
- Add named partition values and live/frozen base query options across Rust,
  TypeScript, WASM, NAPI, React Native, and server/test helpers.
- Add copy-on-write and contribution-merge helpers; do not add create/discard or
  a mandatory branch table.
- Port every **port** and **replace** ledger row to active tests.
- Delete retired test sources only after the ledger records why the old product
  requirement no longer belongs to Jazz.
- Remove `dev/legacy-branch-tests/` and the temporary ledger when every row has a
  final active-test or retire receipt.
- Update S8 to benchmark partition overlays, live/frozen bases, tuple-qualified
  indices, and cross-partition transactions.

## Suggested commit/lever sequence

Keep commits narrow enough to review but batch the full landing gates as allowed
by `AGENTS.md`:

1. test porting ledger and legacy test quarantine;
2. remove facade/read-view branch vocabulary;
3. remove lifecycle/metadata protocol and storage;
4. remove transaction target lineage and merge facade;
5. make empty-tuple shared core green;
6. add dimension schema identities and tuple codec;
7. tuple-qualify content/deletion/current/global-change storage;
8. tuple-prefix indices and per-tuple uniqueness;
9. admit cross-partition atomic commit units;
10. exact-partition read sources;
11. head/live-base and head/frozen-base reducers;
12. effective-value versus supplying-provenance projection;
13. policy/reference composition;
14. copy-on-write and move mutations;
15. maintained overlay subscriptions;
16. selected sync and cross-partition confidentiality;
17. generic contribution provenance and merge helper;
18. monotone dimension-addition lenses and default rekeying;
19. restore binding surfaces, benchmarks, and every ledger test;
20. zero-match cleanup and invariant status promotion.

## Verification cadence

### Per lever

- affected focused suites through `dev/t`;
- tuple isolation, deletion/restore, mask-before-filter/index, and
  cross-partition atomicity canaries once introduced;
- maintained relation/include scale canary;
- low-seed maintained-vs-one-shot oracle;
- `cargo check -p jazz-sim --benches` for protocol/storage/public-type changes;
- `/code-review` stopgap before the next lever.

### Before pushing each batch

Run the full canonical gates from `AGENTS.md`, including:

- all Jazz modes and Groove;
- `jazz-cli`, `jazz-otel`, jazz-sim benches;
- TS wire codec;
- invariant registry;
- maintained-vs-one-shot seed gate;
- incremental-delivery canary;
- benchmark smoke for every protocol/engine/storage lever;
- full workspace/examples for public Jazz types;
- private sensitive-data guard when the private checkout is available.

## Final zero-match receipt

Before declaring the migration complete, this search must return no production,
test, benchmark, protocol, or binding matches:

```text
BranchId|BranchRecord|BranchState|BranchLineage|BranchMetadata|
jazz_branches|jazz_branch_partitions|target_lineage|
commit_mergeable_on_branch|query_rows_on_branch|merge_back_branch
```

The words “branch” and “branch row” may remain only in application examples and
userland helper documentation. Core types and invariants use partition
dimensions, tuples, incarnations, and overlay sources.

## Tooling-friction

A generated symbol-to-test dependency inventory and a checked spec-ID refactoring
tool would save the most wall-clock during the destructive removal phase.
