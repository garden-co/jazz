# Attempt 3 Decision Log

## 2026-05-25 16:43 PDT

Attempt 3 starts from `SPEC.md` as the working contract.

Setup decisions:

- All stores, including memory-only nodes, use SQLite. In-memory means
  in-memory SQLite, not a fake store.
- The harness should test mixed topologies: in-memory local/main-thread-like
  nodes, durable worker/tab-broker-like nodes, optional edge nodes, and a durable
  global authority.
- Policies and lenses are in scope for this attempt, even if the first versions
  are narrow.
- Attempt 2 code and old specs were archived under `reference/attempt2/`.
- First implementation focus: whole-system invariant tests and a boring
  SQLite-backed runtime boundary.

## 2026-05-25 16:45 PDT

Implementation structure guardrail: split the crate into modules from the first
real code pass. Avoid recreating Attempt 2's large `store.rs` center of gravity.

Initial module plan:

- `types`: public domain structs and ids
- `storage`: SQLite connection/opening/migrations
- `tx`: transaction records, outcomes, receipts
- `schema`: fixed Attempt 3 project/todo schema and DDL helpers
- `sync`: query-scoped bundles and apply/export
- `runtime`: product-shaped facade used by whole-system tests

## 2026-05-25 16:49 PDT

First Attempt 3 implementation slice is green.

Added product-shaped whole-system tests proving:

- in-memory SQLite runtime writes through history/current projection
- query-scoped bundle sync converges an in-memory node and durable SQLite node
- rejected transactions remain in history but disappear from current reads
- durable SQLite nodes survive reopen while fresh in-memory SQLite nodes start
  empty

Implementation is deliberately fixed to the `projects`/`todos` harness schema
for now and split into modules:

- `error`
- `runtime`
- `schema`
- `storage`
- `sync`
- `tx`
- `types`

Learning: starting with SQLite from the first test is not slowing us down. The
boundary is already cleaner than Attempt 2 because storage opening, DDL, tx
facts, sync payloads, and runtime facade are separate from the beginning.

## 2026-05-25 16:49 PDT

Next target: complete more of the transaction/history/sync spine before moving
to policy/branches.

Planned tests:

- explicit transaction seals multiple mutations
- rebuilding current projection from history matches current reads
- delete appends history and hides current row
- applying the same bundle twice is idempotent
- replicas can use different physical ids for the same public ids

## 2026-05-25 16:51 PDT

Second spine slice is green: 9 whole-system tests pass.

Added/proved:

- explicit transaction builder seals project + multiple todos under one tx id
- projection rebuild from history restores current reads
- delete appends a history row and removes current visibility
- applying the same bundle twice is idempotent
- two replicas can use different physical row ids for the same public row ids

Important early catch: public tx ids cannot be based on local physical
`node_num`, because two replicas may both assign the writer physical node `1`.
Attempt 3 tx ids now include the public node id plus local epoch.

## 2026-05-25 16:52 PDT

Next target: start authority/scope semantics.

Planned tests:

- query-scoped sync is not table replication
- authority acceptance enriches an existing public transaction id with global
  epoch/receipt instead of replacing it

## 2026-05-25 16:53 PDT

Authority/scope slice is green: 11 whole-system tests pass.

Added/proved:

- open-todo query scope exports only referenced projects, not unrelated table
  rows
- global authority acceptance mutates/enriches the existing tx row with
  `global_epoch`
- global receipt state is separate from transaction identity and observable
  through transaction info

Learning: query-scoped export already forced us to stop treating "all history"
as the easy sync payload. The test is tiny, but it protects the core product
claim that Jazz syncs query scope rather than tables.

## 2026-05-25 16:54 PDT

Structural cleanup trigger: `runtime.rs` reached ~750 lines after only 11 tests.
Before adding policy/branches/lenses, split helper-heavy behavior out so the
runtime facade stays product-shaped.

Target split:

- `projection`: projection clearing/rebuild and rejection repair helpers
- `bundle`: query-scoped export/apply helpers
- `rows`: row-id and fixed-schema insert helpers

## 2026-05-25 16:56 PDT

Course correction before adding features: the runtime must not be hardcoded to
the todo fixture. The fixture should be a schema used by tests, not knowledge
baked into the runtime.

Next structural goal:

- introduce generic table/schema definitions
- generate history/current tables from schema
- route writes and projection rebuild through table definitions
- keep product-shaped `projects`/`todos` helpers only as test-fixture
  convenience over generic runtime operations

This is worth doing before policy/branches/lenses so those features attach to
the real architecture instead of to hardcoded tables.

## 2026-05-25 17:02 PDT

Autonomous stretch begins. Target runtime: at least until 21:00 PDT.

Operating rules for the stretch:

- keep using `date` before log entries
- prefer whole-system invariant tests over helper tests
- keep the runtime generic; fixture helpers are allowed only as wrappers
- make decisions when underspecified and log them
- commit at larger coherent checkpoints
- use subagents for review/planning sidecars, but keep implementation direction
  centralized

Near-term plan:

1. move query-scoped export/apply out of `runtime.rs`
2. add generic query/read support instead of todo-only reads
3. add first policy slice
4. add subscriptions/diffs
5. add branch view baseline
6. add narrow lens slice
7. add crash/reconnect style durable topology tests

## 2026-05-25 17:04 PDT

Subagent feedback confirmed the genericity risk: the crate was halfway migrated
to generic schemas but sync/projection/runtime still had todo-specific gravity.

Immediate correction:

- make bundle payloads generic `HistoryRecord`s
- make ref storage column naming uniform as `{field}_row_num`
- finish compile before adding any more feature behavior
- next invariant after compile: generic schema sync/rebuild, not another todo
  fixture test

## 2026-05-25 17:07 PDT

Generic bundle payload migration is repaired and green again: 12 whole-system
tests pass. Sync payload rows are now `HistoryRecord { table, row_id, tx_id,
op, values, ... }` instead of fixture-specific project/todo structs.

Decision: keep fixture query exports for now, but make their payload generic.
This preserves the currently tested product behavior while removing a bad
architectural dependency before policies/lenses arrive.

Next correction: make projection clear/rebuild schema-driven. A generic runtime
with a todo-specific projector is still too misleading for Attempt 3.

## 2026-05-25 17:08 PDT

Projection rebuild/clear is now schema-driven and still green. The non-todo
schema test now clears and rebuilds current state from history, so the generic
path is not just DDL/write-only.

Decision: keep `Runtime::open()` as a fixture convenience, but require core
maintenance behavior such as projection rebuild to operate from `SchemaDef`.
The next remaining fixture gravity is query/read/export helpers.

## 2026-05-25 17:10 PDT

Generic row read/export is now green in a real ref-shaped schema: docs and
comments are written, comments are exported as table history, applied on another
replica, projection is rebuilt, and refs read back as public row ids despite
different physical row nums.

Decision: add a generic `read_rows(table)` and `export_table_history(table)` as
the lowest useful semantic surface for upcoming policy/lens tests. Fixture
queries can stay as specialized query examples, but the runtime core now has a
schema-shaped path for whole-system tests.

## 2026-05-25 17:12 PDT

First policy slice is green: a table can declare `read_if_created_by_principal`,
and a child table can declare `read_if_ref_readable("parent_ref")`. Generic
`read_rows(table)` lowers this to SQL with an `EXISTS` against the parent
current table.

Decisions and limitations:

- Keep policy declarations attached to schema for now; this matches the product
  shape and forces SQL-lowerable policies.
- Treat required parent readability as a read filter on the child.
- Do not invent a separate local structural-schema mode.
- Recursive ref-readable policies currently error instead of silently doing the
  wrong thing. This is a major derisking target because recursive permissions
  are known-hard.
- Current policy-scoped sync is not solved yet: `export_table_history(table)`
  still exports table history without session policy filtering.

## 2026-05-25 17:13 PDT

Structural cleanup: policy lowering moved out of `runtime.rs` into `policy.rs`.
This keeps the runtime facade from absorbing every behavior and gives recursive
policy lowering a natural place to evolve.

Next: make generic table export respect the same read policy. Otherwise policy
reads and sync scope would diverge immediately.

## 2026-05-25 17:14 PDT

Generic table export now applies the same read-policy SQL as `read_rows`.
The test proves Alice exports only the todo whose required parent project is
readable to Alice.

Decision: a table-history export currently means "for each row visible in the
current policy-filtered projection, include that row's full history." This is
simple and aligns with the current spec direction of syncing full row history
for result rows, but it will need later scrutiny for pagination and for policy
changes that make only some historical versions sensitive.

## 2026-05-25 17:15 PDT

Write-policy slice is green. A table can declare `write_if_ref_readable("doc")`;
if Bob attempts to create a child row pointing at Alice's unreadable doc, the
write creates a transaction and history row, marks the transaction rejected with
`policy_denied`, and skips current projection.

Decision: keep denied writes replayable by storing the attempted history under a
mutable rejected tx fate. This matches the user's preferred model and so far
seems enough for projection rebuild and sync reasoning.

Open issue: creation policies based on `CreatedByPrincipal` are awkward because
a brand-new row has no prior current version. We will need separate create vs
update policy semantics or a more expressive policy language.

## 2026-05-25 17:17 PDT

Subscription baseline is green. `subscribe_rows(table)` captures the same rows
as the one-shot `read_rows(table)`, and `poll_subscription` reruns the query and
emits semantic `Added`, `Updated`, and `Removed` row diffs.

Decision: for Attempt 3, start with rerun-and-diff semantics instead of SQLite
triggers or an incremental query graph. This keeps correctness behavior clear
while leaving invalidation/indexing as an optimization layer.

Open issue: the current subscription object is an in-process test harness, not a
durable subscription protocol. It does not yet track read-set scope, policy
facts, pagination windows, or reconnect delivery cursors.

## 2026-05-25 17:21 PDT

First branch slice is green. The physical layout now has `jazz_branch` plus
`j_branch_num` on history/current rows, and `Runtime` can create/check out a
branch. A branch-local write is visible on that branch and invisible on main.

Decision: put branch identity directly on row versions/current projection
instead of modeling branches as separate projection tables. This preserves the
"many branches without many tables" direction.

Major limitation: this is not yet true pinned-base overlay semantics. Branch
reads currently filter to the active branch's current rows, so they prove
isolation but not "base snapshot plus sparse overlay." Next branch work should
make branch reads fall back to main/base rows when a row has no branch-local
candidate, then add branch provenance to sync payloads.

## 2026-05-25 17:22 PDT

Sparse branch overlay baseline is green. Generic reads on a branch now return
active-branch current rows plus main rows for row ids that do not have a
branch-local candidate. A branch-local version overrides main for that branch,
while main still sees its own version.

Decision: implement overlay reads as one SQL query over current projection, not
branch projection tables. This is the shape we wanted to test for "many
branches, sparse overlays."

Still underspecified: branch bases are currently effectively "main now," not a
pinned snapshot. To be correct, a branch needs to record its flattened effective
base and read main/base rows as of that base snapshot, not the latest main
projection.

## 2026-05-25 17:23 PDT

Pinned branch base slice is green. A branch can store `base_global_epoch`; when
checked out, generic reads combine branch-local current rows with main history
as of that global epoch. Later accepted main changes are invisible on the branch
unless the branch has its own overlay for that row.

Decision: for pinned bases, use query-time history lookup instead of creating a
per-branch base projection table. The test supports the earlier thesis that
pure-query snapshot reads are acceptable as the simple first implementation.

Limitations:

- Base snapshots currently use only `global_epoch`; local durable txs and dotted
  version vectors are not represented.
- Policy for historical base rows is not fully re-evaluated recursively; this
  path should be revisited once recursive policy lowering exists.
- Sync payloads do not carry branch provenance yet.

## 2026-05-25 17:25 PDT

First lens slice is green. A new schema can declare `text_lens("name",
"title")`; old sync/history values containing `title` apply into the new schema
and read back as semantic `name`.

Decision: model this first lens as field-level semantic-name-to-storage-name
mapping. This is the smallest useful compatibility mechanism and immediately
exposed that incoming sync must accept both the new semantic field name and the
old stored field name.

Limitations:

- This is not full schema-versioned storage. The table names are still
  `schema_v1`, and writes through the new schema store into the old column.
- No lens catalogue, compatibility check, generated inverse lens, or
  copy-on-write-forward mechanism exists yet.
- Policy/lens composition is untested.

## 2026-05-25 17:27 PDT

Branch sync provenance is green. `HistoryRecord` now carries `branch_id`; apply
ensures the branch exists locally and stores history/current rows under that
branch. A draft-row bundle remains invisible on Bob's main branch and appears
after Bob checks out `draft`.

Decision: branch identity must be part of row-version sync payloads, not merely
ambient connection/session state.

Limitation: branch base provenance is still not included in bundle metadata.
The receiver can recreate the branch id, but not yet its precise source/base
snapshot or multi-base provenance.

## 2026-05-25 17:28 PDT

Durable branch replay/reconnect slice is green. A file-backed worker can apply a
draft branch bundle twice, persist it, reopen, keep the row invisible on main,
and show it after checkout with only one history row.

Decision: the distributed-system harness should keep mixing in-memory SQLite
nodes with durable SQLite nodes. Even these tiny tests catch whether sync facts
are semantic/idempotent rather than process-local.

## 2026-05-25 17:31 PDT

Policy dependency sync is green. When a table's read policy depends on a parent
ref, exporting the child table now includes the visible child row history plus
the required parent row history, but not unrelated parent rows.

Learning: the first version of the test applied Alice's scoped bundle into a
runtime with Bob's ordinary principal and then expected Alice's result. That was
wrong for the model as currently written: policy-shaped result reproduction
requires either the same principal/session or an explicit trusted-peer context.
Attempt 3 still lacks a first-class trusted-peer/admin policy bypass context.

Decision: direct policy dependency inclusion belongs in sync/export, not only in
query results. This is the minimum way for a receiver to recreate a policy-gated
query without already having the parent facts.

## 2026-05-25 17:32 PDT

Policy dependency subscription invalidation is green. A subscription to child
rows removes the child when only the parent policy fact changes from
Alice-readable to Bob-owned.

Learning: rerun-and-diff subscriptions can correctly handle policy dependency
changes because `read_rows` is already policy-aware. The missing future piece is
efficient invalidation/read-set tracking, not the semantic model.

## 2026-05-25 17:32 PDT

Branch snapshot edge cases are green. A branch based at global epoch 2 does not
show a row whose latest accepted version at epoch 2 is a delete, and it also
does not show a pending main row with no global epoch.

Learning: the query-only branch base implementation already has the essential
shape for these cases: latest accepted history at-or-before base, `h.op != 3`,
and `global_epoch IS NOT NULL`.

## 2026-05-25 17:33 PDT

Rename lens write/export is green. A runtime using semantic field `name` over
stored column `title` writes successfully, exports sync payload values under
`name`, and another new-schema runtime reads the same semantic field after
apply.

Learning: field-level storage-name mapping gives us a partial write-forward
property on the wire even though physical SQLite storage is still the old column
shape. This does not replace real schema-versioned tables, but it clarifies one
useful compatibility lane.

## 2026-05-25 17:35 PDT

Generic transaction builder path is green. `transaction().insert_row(...)` can
seal multiple arbitrary-schema rows under one transaction id, and generic reads
show both rows with that tx id.

Decision: keep fixture transaction helpers for existing tests, but stop treating
them as the only transaction constructor. Future transaction semantics should be
implemented against generic row mutations first.

## 2026-05-25 17:38 PDT

Behavior-preserving module split started: branch SQL moved from `runtime.rs` to
`branch.rs`. Tests remain green.

Learning: small splits are cheap now that whole-system tests are broad. Continue
peeling runtime responsibilities into branch/query/sync/mutation modules instead
of letting `runtime.rs` become Attempt 2's `store.rs` again.

## 2026-05-25 17:40 PDT

Trusted peer runtime is green. `open_trusted_with_schema` creates a SQLite-backed
runtime that bypasses ordinary read-policy filtering, so a worker/trusted peer
can apply Alice's policy-scoped facts and inspect them without pretending to be
Alice's user principal.

Decision: model untrusted client principals and trusted peers separately, even
inside the local harness. This matches the product topology better than using
magic principals in tests.

Follow-up green: trusted writes now also work through the generic transaction
builder, bypassing user write policies without recording a rejected transaction.

## 2026-05-25 17:42 PDT

Generic rejection repair is green. `reject_transaction` now repairs current
projection by iterating schema tables instead of deleting from hardcoded
projects/todos current tables.

Learning: transaction fate handling needs to stay schema-driven and branch-aware
from the start. Fixture-specific repair paths are especially dangerous because
they pass product-looking tests while silently breaking generic schemas.

## 2026-05-25 17:44 PDT

First conflict-candidate probe is green. Branches can now record explicit source
branches in `jazz_branch_source`; a merge branch can expose multiple current
candidates for the same row via `read_row_candidates`.

Decision: keep conflict candidates as multiple visible row-version candidates,
not immediate last-writer-wins. For this slice they are exposed through a side
API rather than folded into ordinary query result metadata.

Limitations:

- Source branch provenance is local-only and not included in sync bundles yet.
- Candidate reads use source branch current projections, not arbitrary pinned
  source snapshots.
- Ordinary `read_rows` still returns normal overlay rows and does not include
  conflict metadata.

## 2026-05-25 17:48 PDT

Behavior-preserving query split is green. Generic row reads, pinned branch base
reads, sparse overlay reads, and conflict candidate reads now live in `query.rs`
instead of `runtime.rs`.

Learning: this is the right module boundary: policy lowering, branch visibility,
lens value decoding, and conflict candidates all converge at query execution.
`runtime.rs` is still large, but the highest-complexity read behavior now has a
place to evolve.

## 2026-05-25 17:51 PDT

Branch source metadata sync is green. Bundles now carry branch records with
`branch_id`, `base_global_epoch`, and `source_branch_ids`; apply recreates branch
metadata before applying row versions. A receiver can apply left/right source
branch histories plus a merge-branch bundle and recover the same conflict
candidates.

Decision: branch provenance belongs in sync metadata alongside row-version
payloads. Row `branch_id` alone is not enough once branches can have source
branches.

Follow-up green: branch table export is now active-branch scoped. It excludes
unrelated branch rows, but includes declared source branch rows for a merge
branch, so a receiver can recover conflict candidates from one branch-scoped
bundle.

Open issue: branch-scoped export still does not include pinned main-base
snapshot rows for branches based on `base_global_epoch`.

## 2026-05-25 17:52 PDT

Correction: I accidentally treated the previous checkpoint as a stopping point.
Continuing the autonomous stretch.

Next targets:

- branch-scoped bundles that include source branch candidate facts in one scope
- move sync import/export out of `runtime.rs`
- lens + policy composition
- better module boundaries around generic mutations

## 2026-05-25 17:54 PDT

Stretch goal recorded: recursive queries and recursive permission policies need
explicit derisking. They are likely to force recursive CTE lowering and careful
policy dependency tracking. Current priorities remain branch-scoped sync
correctness and module boundaries first.

## 2026-05-25 17:57 PDT

Starting recursive permission derisking with a narrow chain:
`todos.project -> projects.org -> orgs.created_by`. This should answer whether
our policy lowering can compose without introducing a separate policy runtime.
First target is read filtering. Transitive sync dependency export may become a
second step after the read path is green.

## 2026-05-25 17:58 PDT

Recursive read-policy lowering is green for a grandparent chain. The generated
predicate is still plain SQLite: nested `EXISTS` subqueries with fresh aliases
and a bounded recursion depth. This is not yet recursive CTE support, but it
proves that policy composition can remain SQL-lowerable for common parent-chain
permissions.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
34 tests.

Next obvious derisk: sync dependency export is still direct-parent only. A
recursive policy read can filter through grandparent rows, but a policy-scoped
bundle likely does not yet include all transitive rows needed to recreate the
query elsewhere.

## 2026-05-25 17:58 PDT

Starting the paired recursive sync-dependency test. Hypothesis: exporting
`todos` with a recursive `read_if_ref_readable` policy currently includes the
visible todo and its direct project, but omits the org row required to prove the
project is readable on the receiving node.

## 2026-05-25 17:59 PDT

Recursive policy-scoped sync is green for the same grandparent chain. The export
path now recursively follows `read_if_ref_readable` dependencies and carries the
concrete child row set downward, so exporting `todos` includes only the required
`projects` and only the required `orgs`.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
35 tests.

Design note: this is still not a complete read-set model. It handles policy
dependency rows for simple ref chains. Predicate/range reads, recursive graph
queries, and policy cycles remain future derisking targets.

## 2026-05-25 18:01 PDT

Switching to transaction fate propagation. Suspected bug: bundle apply uses
`INSERT OR IGNORE` for `jazz_tx`, so a peer that already saw an optimistic
transaction may ignore a later rejected/accepted fate update for the same tx.
This would violate the mutable fate decision we made earlier.

## 2026-05-25 18:02 PDT

Mutable transaction fate propagation is green for rejection. A peer can first
apply an optimistic visible transaction, later apply a bundle where the same
`tx_id` is rejected, and repair current projection while keeping history.

Implementation detail: `apply_bundle` now updates `jazz_tx.outcome` on tx-id
conflict and removes current rows whose `visible_tx_num` now points at rejected
transactions before replaying incoming history.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
36 tests.

## 2026-05-25 18:02 PDT

Next symmetric fate target: accepted/global metadata propagation. The existing
bundle `TxRecord` carries `outcome` but not `global_epoch` or receipts, so a
peer can likely learn that a tx is accepted without learning the epoch needed
for branch base snapshots and global durability reasoning.

## 2026-05-25 18:03 PDT

Accepted/global fate propagation is green. `TxRecord` now carries
`global_epoch`, apply upserts it with the mutable outcome, and apply recreates a
global receipt row when a global epoch is present. A peer can now learn that an
already-applied optimistic transaction became globally accepted at epoch 7.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
37 tests.

Design note: receipts are still represented minimally. We are not transporting
authority identity, signatures, or detailed receipt JSON yet. The important
shape for this attempt is that tx fate is mutable and replayable, with enough
global epoch metadata for snapshot semantics.

## 2026-05-25 18:04 PDT

Starting pinned branch-base export. Current branch export is active-branch
scoped plus source branches; it likely omits main-history rows at
`base_global_epoch`. That means a receiver can learn the branch metadata but
still render an empty branch if the branch relies on a main-base snapshot.

## 2026-05-25 18:06 PDT

Pinned branch-base export is green. Exporting a branch with
`base_global_epoch = 1` now includes:

- the active branch record, even when the branch has no overlay rows
- main-branch row versions at or before the pinned epoch that are needed to
  recreate the branch snapshot
- not later main updates after the branch base

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
38 tests.

Learning: branch-scoped sync has three kinds of provenance to carry:
active-branch metadata, source-branch metadata/history for merge candidates, and
pinned main-base snapshot history. Treating sync scope as "branches mentioned by
history rows" was insufficient.

## 2026-05-25 18:06 PDT

Starting branch snapshot policy check. Concern: branch base reads use history
queries rather than current projection queries, so they may bypass the
`read_policy_sql` predicates that current reads use.

## 2026-05-25 18:07 PDT

Branch base snapshot policy is green. History-backed snapshot reads now lower
read policy against an explicit SQL alias (`h`) instead of assuming the
projection alias `current`, so `read_if_created_by_principal` is enforced for
pinned base rows too.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
39 tests.

Open edge: recursive ref-readable policy over historical base rows still follows
the referenced parent through current projection. For same-branch current reads
that is fine; for precise historical snapshot policy it may need parent lookup
against the same historical base epoch.

## 2026-05-25 18:08 PDT

Starting exclusive transaction semantics. Goal for this attempt: one
transaction constructor can be parameterized as mergeable/pending or
exclusive/global. Exclusive without a global epoch should throw before writing
history; exclusive with a global epoch should create an accepted transaction
that is immediately visible and carries the global receipt metadata.

## 2026-05-25 18:09 PDT

Initial exclusive transaction semantics are green. The generic transaction
builder now has `.exclusive()` and `.exclusive_at_global(epoch)` modes:

- `.exclusive()` rejects before writing because there is no global authority
  acceptance in this local runtime
- `.exclusive_at_global(7)` writes an accepted transaction with exclusive
  conflict mode, global epoch 7, visible current rows, and a global receipt

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
40 tests.

Open issue: this is still a prototype API. A real authority path should probably
construct exclusive/global transactions through a distinct trusted submission
route rather than a local builder method named `exclusive_at_global`.

## 2026-05-25 18:09 PDT

Starting durable-worker topology reconciliation. Scenario: memory tab writes an
optimistic mergeable transaction, durable worker stores it, worker restarts, then
the tab receives/exports a rejected fate update and the reopened worker must
repair its current projection without losing history.

## 2026-05-25 18:10 PDT

Durable worker rejection reconciliation is green. A file-backed worker can apply
an optimistic table-scope bundle, restart, then apply the same tx with rejected
fate and repair current projection while preserving the replayable history row.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
41 tests.

Learning: table-scope export is intentionally narrower than query-scope export.
In this test the todo row's project ref is represented as an ID/row mapping, but
the project history row is not included because the export is not a todo query
scope. This distinction is worth keeping explicit in the next spec.

## 2026-05-25 18:11 PDT

Starting recursive query lowering. Narrow target: a self-referential table with
`parent -> same_table`, queried through a SQLite `WITH RECURSIVE` CTE from a
root row. Policy should apply to both the anchor and recursive children, so the
result is not just graph traversal but policy-filtered traversal.

## 2026-05-25 18:13 PDT

Recursive query lowering is green for a self-referential tree. `Runtime` now has
`read_recursive_refs(table, root_id, parent_field)`, implemented with SQLite
`WITH RECURSIVE` over current projection rows and read-policy predicates on both
the anchor row and recursive child rows.

Important learning: the first CTE hung because I tracked `(row_num, depth)` in
the recursive set while the fixture had a self-parenting root. `UNION` only
dedupes the full tuple, so the root kept reappearing at larger depths. The green
version dedupes by `row_num` only. If we need depth/order/path metadata later, it
must be computed without making visited identity include depth.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
42 tests.

Open issues: current recursive query support is deliberately narrow. It does not
yet cover branch base overlays, policy-scoped recursive query export, arbitrary
recursive predicates, or visited-row/read-set capture.

## 2026-05-25 18:13 PDT

Starting recursive query-scope export. Goal: reuse the recursive query result as
the row set to export, so a peer can apply the bundle and recreate the same
policy-filtered tree without receiving unrelated sibling/subtree rows.

## 2026-05-25 18:14 PDT

Recursive query-scope sync is green. `export_recursive_refs(...)` runs the
recursive query, maps returned public row IDs back to local row nums, exports
visible history for exactly those rows, and includes active branch metadata. A
peer can apply the bundle and recreate the same policy-filtered tree without
receiving unrelated rows.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
43 tests.

Design note: this is result-set export, not a full predicate/range read set.
For a live subscription or correctness-preserving sync scope, we would still
need to encode "all current/future children where parent is in this recursive
frontier" rather than only the rows returned right now.

## 2026-05-25 18:15 PDT

Starting durable branch-conflict/fate test. Goal: a merge branch's source
candidate rows should survive sync and durable reopen, then a rejected candidate
transaction should disappear from candidate reads without deleting the other
candidate.

## 2026-05-25 18:15 PDT

Durable branch conflict candidate fate is green. A durable worker can apply a
merge-branch bundle containing left/right source candidates, reopen, still see
both candidates, then apply a rejected fate update for the left candidate's tx
and see only the right candidate.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
44 tests.

Learning: the mutable-fate projection repair works for source-branch conflict
candidates too, because candidate reads join source branch current rows against
`jazz_tx.outcome`.

## 2026-05-25 18:16 PDT

Starting lens + policy composition. Target: old schema writes a ref field named
`project`; new schema exposes the same storage column as semantic field
`workspace` and uses `read_if_ref_readable("workspace")`. This should prove
renamed refs can still participate in policy lowering and sync.

## 2026-05-25 18:17 PDT

Renamed ref lenses now compose with read policy. Added `ref_lens(name,
stored_as, table)`, then verified an old-schema `project` ref can be read as
new-schema semantic field `workspace` and used by `read_if_ref_readable`.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
45 tests.

Learning: the existing field model (`name` plus `storage_name`) was already the
right abstraction for this. The missing piece was only exposing the same lens
operation for refs, not just text columns.

## 2026-05-25 18:18 PDT

Starting rejection reason surfacing. We already store `jazz_tx_rejection.code`,
but `transaction_info` does not expose it. This matters for the product contract:
rejection should be replayable history and also visible enough to reject promises
or call global error handlers with a useful reason.

## 2026-05-25 18:19 PDT

Rejection reason surfacing is green. `TransactionInfo` now includes
`rejection_code: Option<String>`, read from `jazz_tx_rejection`. The existing
rejection test now proves the stored policy denial reason is observable.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, still
45 tests.

## 2026-05-25 18:20 PDT

Small test-shape cleanup: added `tests/support/mod.rs` with shared schema
fixtures for notes/tasks/folders and switched the newest/repetitive tests to
use it. This is not a full integration-test split, but it gives the next slices
a place to put shared fixtures instead of continuing to bloat
`whole_system.rs`.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, still
45 tests.

## 2026-05-25 18:21 PDT

Starting trusted-edge topology test. Goal: an untrusted memory client can submit
a mergeable tx, a trusted durable edge can accept it globally, and untrusted
peers still apply their own read policy after receiving the edge's accepted
bundle.

## 2026-05-25 18:21 PDT

Trusted-edge topology is green. A memory Alice client writes a pending mergeable
transaction, a trusted file-backed edge applies and accepts it at global epoch
11, Alice's second untrusted peer receives the accepted result, and Bob's
untrusted peer receives the same bundle but still reads zero rows because local
read policy applies.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
46 tests.

Learning: the trusted/untrusted split is holding up as a useful axis separate
from local/edge/global durability. Trusted peers can bypass policy while
importing/exporting facts; untrusted peers still enforce their own visible
result locally.

## 2026-05-25 18:22 PDT

Starting historical recursive policy derisk. Current known edge: a pinned branch
base snapshot filters child history rows, but `read_if_ref_readable` may check
the referenced parent in current projection instead of at the same base epoch.
That would make a historical branch change visibility when a parent row changes
after the branch was created.

## 2026-05-25 18:23 PDT

Historical ref-readable policy is green for branch base snapshots. Added
snapshot-aware policy lowering that evaluates referenced parent rows through
history at the same `base_global_epoch`, recursively, instead of using current
projection. The same lowering is used for branch-base export filtering.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
47 tests.

Learning: policy lowering needs a context, not just a policy expression:
current-projection policy, historical-snapshot policy, and eventually
branch-overlay historical policy have different SQL shapes even for the same
high-level permission.

## 2026-05-25 18:24 PDT

Starting sync-side historical policy check. The previous fix made local branch
reads use parent rows at the base epoch; now verify branch export sends enough
base history for another node to recreate the same policy-filtered branch.

## 2026-05-25 18:26 PDT

Sync-side historical ref policy is green. Branch-base export now follows
snapshot policy dependencies recursively: exporting a pinned-base child row also
exports the referenced parent row versions at or before the same base epoch, so
a receiver can recreate branch visibility even if the parent changed later.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
48 tests.

Learning: query-scope export and branch-base export need parallel dependency
walkers, but one walks current projection and the other walks historical
snapshot rows. This is another point in favor of making "query context" an
explicit lowering parameter in attempt 4 or the next spec pass.

## 2026-05-25 18:26 PDT

Starting row-level write set materialization. Goal: every row mutation in a
transaction records whole-row write items (`tx_num`, table, row) so causality,
validation, conflict detection, and future read/write-set sync have a concrete
place to build from. Keeping this row-level matches the earlier decision not to
use column masks for write-set semantics.

## 2026-05-25 18:29 PDT

Row-level write sets are green. Added `jazz_tx_write` and record whole-row write
items for local generic writes, built-in project/todo writes, deletes, and
applied remote history. Added `transaction_write_rows(tx_id)` as a probe API and
verified both local multi-row transactions and synced generic rows retain write
sets.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, still
48 tests.

Design note: this is intentionally row-level. Per-column data may still be
useful for merge strategies, but write-set causality/conflict semantics should
start with the whole row as the item.

## 2026-05-25 18:29 PDT

Starting transaction mode propagation. Found while adding write sets: bundles
carry tx outcome and global epoch, but apply hardcodes `conflict_mode =
mergeable`. That would turn exclusive/global transactions into mergeable ones on
receiving peers.

## 2026-05-25 18:31 PDT

Transaction conflict mode propagation is green. `TxRecord` now carries
`conflict_mode`; export includes it, apply upserts it, and `TransactionInfo`
surfaces `"mergeable"` vs `"exclusive"` as a probe. Exclusive/global
transactions now remain exclusive after sync.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
49 tests.

## 2026-05-25 18:32 PDT

Starting monotonic transaction fate merge. Current suspicion: after a peer sees
an accepted/rejected tx, applying an older pending bundle for the same `tx_id`
can overwrite `outcome` and clear `global_epoch`. Fate updates should be
monotonic under reordering.

## 2026-05-25 18:33 PDT

Monotonic fate merge is green for accepted-vs-stale-pending. Applying an older
pending bundle after a global acceptance no longer clears `global_epoch` or
downgrades outcome. The tx upsert now uses max outcome/conflict mode and
preserves an existing global epoch when the incoming record has none.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
50 tests.

Open semantic question for the spec: numeric max works for this prototype
because `pending < accepted < rejected` and `mergeable < exclusive`, but real
transaction fate may need an explicit partial-order merge function if accepted
vs rejected can ever race.

## 2026-05-25 18:34 PDT

Starting write-set completeness cleanup: generic `delete_row` writes a delete
history version but does not yet materialize a `jazz_tx_write` row.

## 2026-05-25 18:35 PDT

Generic delete write-set completeness is green. `delete_row` now records the
deleted row in `jazz_tx_write`, and the subscription diff test probes the delete
transaction's write set.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, still
50 tests.

## 2026-05-25 18:36 PDT

Starting global epoch multiplicity test. The schema currently has
`UNIQUE(global_epoch)`. Need to clarify by implementation pressure whether a
global epoch is a single transaction index or can represent an accepted set of
transactions.

## 2026-05-25 18:37 PDT

Global epochs can now contain multiple accepted transactions. Removed
`UNIQUE(global_epoch)` from `jazz_tx` and added a test accepting two txs at epoch 7. This aligns better with the "global epoch base as prefix" mental model and
keeps room for an epoch to represent an authority step containing multiple txs.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
51 tests.

## 2026-05-25 18:37 PDT

Starting deletion-through-sync test. Concern from sidecar: export paths that use
current visibility to choose history rows can omit deletion history, so a peer
that previously synced a row may never learn to remove it.

## 2026-05-25 18:39 PDT

Deletion through table-scope sync is green. `apply_history_record` now actively
removes current rows for delete history records, and table export includes rows
whose latest branch version is a delete so peers can converge from visible to
removed state.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
52 tests.

Open issue: deleted-row export currently exports all history for rows whose
latest version is deleted. That is useful for replay but may be broader than
minimal "remove this row" sync payloads.

## 2026-05-25 18:40 PDT

Starting recursive query over branch base + sparse overlay. Current recursive
query walks only the current projection for `self.branch_num`, unlike normal
branch reads which also overlay pinned main-base rows.

## 2026-05-25 18:41 PDT

Recursive query over branch base + sparse overlay is green. For branches with a
pinned base, recursive traversal now composes over the already-materialized
visible branch rows from `read_rows`, so it sees base rows plus branch-local
overlay rows.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
53 tests.

Design note: this implementation deliberately uses an in-memory traversal for
the branch snapshot case. It proves semantics quickly, but a serious
implementation should lower the same visible-row relation into a recursive CTE
instead of materializing the whole visible table first.

## 2026-05-25 18:42 PDT

Starting trusted-edge authoritative rejection. Existing trusted-edge test only
covers accepted writes. Need a negative path where an edge receives a tx that
violates policy, marks it rejected with a reason, removes current projection,
and syncs that fate back to the submitter.

## 2026-05-25 18:43 PDT

Trusted-edge authoritative rejection is green. A Bob client submits a write
under Alice's policy-protected parent; the trusted edge receives the tx, rejects
it with `policy_denied`, and Bob learns the rejection reason after syncing edge
state back.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
54 tests.

Design note: this is still manually driven: the trusted edge does not yet
automatically validate and reject during `apply_bundle`. The test proves the
storage/sync/fate mechanics, not the final authority automation.

## 2026-05-25 18:44 PDT

Starting policy-cycle behavior. Current recursive policy lowering only has a
depth guard. I want a clearer schema-time rejection for direct policy cycles so
query execution does not fail unpredictably later.

## 2026-05-25 18:45 PDT

Schema-time policy-cycle rejection is green for direct self-ref cycles. Installing
a table whose policy says `read_if_ref_readable("parent")` where `parent`
references the same table now returns a clear policy-cycle error.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
55 tests.

Open issue: this only catches direct and immediate two-table cycles. A real
schema validator should walk the policy graph completely and report useful
diagnostics for longer cycles.

## 2026-05-25 18:46 PDT

Starting recursive branch query-scope export. We made recursive branch reads see
base+overlay rows, but `export_recursive_refs` still exports current visible
history for the returned row nums. It may omit pinned-base rows needed by a
receiver.

## 2026-05-25 18:47 PDT

Recursive branch query-scope export is green. `export_recursive_refs` now adds
pinned base history for returned rows when exporting from a branch with
`base_global_epoch`, so a receiver can recreate the same recursive result from
base rows plus overlay rows.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
56 tests.

Open issue: this mirrors the current semantic shortcut: it exports result rows,
not a resumable recursive scope/read predicate for future matching children.

## 2026-05-25 18:48 PDT

Starting rejected-vs-accepted fate ordering test. Current monotonic merge makes
rejected numerically win over accepted; make that behavior explicit so it is a
known semantic decision rather than an accidental enum trick.

## 2026-05-25 18:49 PDT

Rejected-over-accepted fate ordering is now explicit in tests. A peer that has
rejected a tx stays without current rows after receiving an accepted/global
bundle for the same tx, while still learning the global epoch metadata.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
57 tests.

Design question: this is a strong semantic stance. If authorities can reverse
rejections later, that should be modeled as a new fate transition with explicit
provenance rather than ordinary accepted/rejected bundle reordering.

## 2026-05-25 18:50 PDT

Starting first read-set materialization. Narrow target: when a write policy
checks a referenced parent row (`write_if_ref_readable`), record that parent row
as a policy read item for the transaction. This is not full predicate/range
read-set capture, but it gives transaction validation a concrete table to build
on.

## 2026-05-25 18:52 PDT

First policy read-set materialization is green. Added `jazz_tx_read` and record
reason `1` rows when `write_if_ref_readable` checks a parent ref. Added
`transaction_policy_read_rows(tx_id)` as a probe.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
58 tests.

Open issue: read-set export/sync and predicate/range read sets are still absent.
This only records direct row dependencies for write-policy checks.

## 2026-05-25 18:52 PDT

Starting policy read-set sync. Local read-set materialization is not enough:
receivers need the read set too if it will inform validation, causality, or
debugging after sync.

## 2026-05-25 18:55 PDT

Policy read-set sync is green. `Bundle` now carries `reads`, export includes
`jazz_tx_read`, and apply recreates read-set rows on the receiver. A tx whose
write policy depended on a parent project now preserves that dependency after
sync.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
59 tests.

Design note: read-set export is currently broad: all local read-set rows are
included in every bundle. That is acceptable for the attempt but too coarse for
real sync scopes.

## 2026-05-25 18:55 PDT

Starting scoped read-set export cleanup. Goal: a bundle should only include read
sets for transactions whose history is actually in that bundle, not every
read-set row known locally.

## 2026-05-25 18:57 PDT

Scoped read-set export is green. Bundle read sets are now filtered to tx IDs
present in exported history, so exporting `todos` does not leak read-set rows for
an unrelated `milestones` transaction.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
60 tests.

Design note: this is still tx-history scoped, not query-predicate scoped. It is
a meaningful improvement over "all reads everywhere" but not the final sync
scope model.

## 2026-05-25 18:58 PDT

Verification checkpoint: `cargo test -p mini-jazz-sqlite` passes. This includes
the full whole-system integration suite, now 60 tests, plus crate unit/doc test
targets.

## 2026-05-25 18:58 PDT

Starting automatic trusted-edge validation. The manual trusted-edge rejection
test proved fate mechanics, but authority behavior should be expressible as
"apply this untrusted bundle and validate policy", not as a separate manual
rejection call.

## 2026-05-25 19:00 PDT

Automatic trusted-edge validation is green. Added `apply_untrusted_bundle`,
which applies incoming facts, evaluates write policies using the record author
as principal, and rejects violating transactions with `policy_denied`. Added a
harness-only `open_trusted_as_with_schema` constructor to simulate Bob-authored
facts that bypass local client validation.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, now
61 tests.

Design note: this is still coarse: validation happens after apply, then repairs
projection by rejecting. A production authority likely wants a transactional
"stage, validate, then publish fate" path.

## 2026-05-25 19:02 PDT

Starting recursive query plus recursive permission export coverage. The current
runtime can query recursive self-ref trees and can export transitive policy
dependencies for ordinary table scopes, but recursive query scopes may still
omit ancestors required to re-evaluate nested read policies on the receiver.

## 2026-05-25 19:03 PDT

Recursive query plus policy dependency export is green. The red test confirmed
that `export_recursive_refs` exported the recursive rows but omitted parent rows
needed by `read_if_ref_readable`; the receiver then could not reproduce
visibility. Fixed by applying the existing current/snapshot policy-dependency
export expansion to recursive query scopes, filtered to the recursive result
row set.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
recursive_query_scope_sync_includes_recursive_policy_ancestors` passes.

## 2026-05-25 19:03 PDT

Starting branch pinned-base recursive query policy coverage. The previous fix
added snapshot policy dependencies to recursive exports, but there is not yet a
focused test proving that a receiver can reproduce a branch recursive query
whose base rows are visible only because historical policy ancestors were
included.

## 2026-05-25 19:04 PDT

Branch pinned-base recursive query policy coverage is green. The new test
creates accepted main history, pins a branch to that global epoch, exports a
recursive branch query, and verifies the bundle includes the historical org row
needed to satisfy the branch snapshot read policy on a peer.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
recursive_branch_query_export_includes_snapshot_policy_ancestors` passes.

## 2026-05-25 19:05 PDT

Starting query-scope precision cleanup. The hardcoded open-todos export proves
that unrelated parent rows are excluded, but it still looks suspiciously broad
for child rows: it may export todo history for rows that are not currently in
the open-todos result set.

## 2026-05-25 19:06 PDT

Query-scope precision cleanup is green. The red test showed
`export_query_scope_open_todos` exported history for a closed todo that was not
in the current open-todos result. Fixed the hardcoded helper to filter child
history by the current query result row IDs, matching the already-filtered
parent-project export.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
query_scope_excludes_rows_outside_current_result_set` passes.

## 2026-05-25 19:06 PDT

Starting integration-test module split. The file is now over sixty whole-system
tests, and continuing to add behavior into one file is slowing navigation. Plan:
keep `tests/whole_system.rs` as the integration crate root and move tests into
child modules with `use super::*` so imports and support helpers stay shared.

## 2026-05-25 19:07 PDT

Integration-test module split is green. `tests/whole_system.rs` is now a thin
crate root with child modules for branches, generic schema, policies, recursive
queries, schema lenses, storage/projection, subscriptions, sync/fate, and
transactions. This was a mechanical move plus explicit `#[path = ...]`
attributes so the files can live under `tests/whole_system/` without becoming
separate integration crates.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system` passes, 64
tests.

## 2026-05-25 19:08 PDT

Starting durable-edge rejection topology coverage. We have local rejection
repair, durable worker restart repair, and automatic trusted-edge validation,
but not yet one test that combines memory clients, a durable edge, an optimistic
invalid transaction, edge restart, and rejection propagation back to the memory
client.

## 2026-05-25 19:09 PDT

Durable-edge rejection topology is green and found a real metadata gap. The
current projection repaired from rejected `outcome`, but `TxRecord` did not
carry the rejection code, so a memory client could lose the user-facing
`policy_denied` reason after receiving fate from the durable edge. Added
`rejection_code` to transaction sync records and restored `jazz_tx_rejection`
on apply.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
durable_edge_rejects_after_restart_and_repairs_memory_client` passes.

## 2026-05-25 19:10 PDT

Starting generic point-predicate query/export slice. The hardcoded open-todos
query was useful scaffolding, but the system needs a generic path that can read
and export current rows for arbitrary schema fields while still including policy
dependencies. Targeting simple equality predicates first.

## 2026-05-25 19:11 PDT

Generic point-predicate query/export is green. Added
`read_rows_where_eq(table, field, value)` and `export_query_where_eq(...)`,
then proved a generic `tasks.done = false` scope exports only matching task rows
plus the parent project required to re-evaluate `read_if_ref_readable` on the
receiver.

Design note: this is deliberately semantic-first, not performance-first. It
filters over `read_rows` in memory and reuses existing row-ID-scoped export.
That validates the operation shape and policy composition, but a real version
should lower predicates into SQL and record predicate/range read sets.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
generic_equality_query_scope_exports_matching_rows_and_policy_dependencies`
passes.

## 2026-05-25 19:12 PDT

Starting SQL-lowered generic equality reads. The previous generic predicate
slice intentionally filtered semantic rows in memory. Next step: lower simple
field equality into SQLite for the current projection while preserving branch
overlay and policy semantics.

## 2026-05-25 19:13 PDT

SQL-lowered generic equality reads are green for current projections. Added a
`QueryContext::read_rows_where_eq` path that lowers user fields to physical
SQLite columns and converts public ref IDs to local row nums before querying.
Covered both boolean equality and ref equality.

Design note: pinned branch base snapshots still use `read_main_snapshot_rows`
followed by semantic filtering for the base portion. That is correct enough for
the attempt, but the real query planner should lower equality predicates across
snapshot reads too.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
generic_equality_query` passes.

## 2026-05-25 19:14 PDT

Starting SQLite page footprint introspection. We have external layout/perf
experiments, but the runtime itself should expose enough storage stats to write
small overhead probes without separate benchmark harnesses.

## 2026-05-25 19:15 PDT

SQLite page footprint introspection is green. `StorageStats` now includes
`page_count`, `page_size`, and computed `database_bytes` using SQLite PRAGMAs.
This gives future invariant/perf tests a cheap way to estimate storage overhead
from the same runtime harness.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
memory_runtime_writes_through_sqlite_current_projection` passes.

## 2026-05-25 19:16 PDT

Starting edge-accepted transaction fate. The current prototype can represent
global acceptance with a global epoch, but trusted edge acceptance should also
be representable as durable-enough visibility without pretending the tx has
entered global epoch history.

## 2026-05-25 19:17 PDT

Edge-accepted transaction fate is green. Added an edge receipt tier and
`accept_transaction_at_edge`, plus synced receipt tiers through `TxRecord`.
Peers now can see an edge-accepted transaction as accepted/visible while
`global_epoch` remains `None`.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
trusted_edge_acceptance_syncs_without_global_epoch` passes.

## 2026-05-25 19:18 PDT

Starting edge-to-global fate upgrade coverage. Need to prove a tx can become
edge-accepted first, then later receive a global epoch under the same public
transaction ID without losing the edge receipt.

## 2026-05-25 19:18 PDT

Edge-to-global fate upgrade coverage is green. A tx can sync as edge-accepted
with no global epoch, later receive `global_epoch = 42`, and peers preserve both
`edge` and `global` receipts for the same tx ID.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
edge_accepted_transaction_can_upgrade_to_global_epoch` passes.

## 2026-05-25 19:19 PDT

Starting `j_` user-column escaping. We decided row system columns use `j_`, so
user columns with that prefix need a deterministic physical escape rather than
colliding with system columns.

## 2026-05-25 19:20 PDT

`j_` user-column escaping is green. Schema fields and lens storage names that
start with `j_` now store physically as `u_j_*`, while semantic reads, writes,
sync payloads, equality queries, and indexes still use the user-facing field
name.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
user_columns_with_system_prefix_are_escaped_physically` passes.

## 2026-05-25 19:20 PDT

Starting arbitrary recursive policy-cycle rejection. Current validation only
catches direct and simple two-table cycles; a longer `a -> b -> c -> a`
permission chain should also fail before query lowering can recurse forever.

## 2026-05-25 19:21 PDT

Arbitrary recursive policy-cycle rejection is green. Replaced shallow cycle
checks with a DFS over the `read_if_ref_readable` graph, and proved both direct
self cycles and three-table cycles fail at schema install time.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
schema_rejects_` passes.

## 2026-05-25 19:22 PDT

Starting branch delete-over-base coverage. Hypothesis: deleting a row on a
branch with a pinned main base may fail to shadow the base snapshot because the
current projection currently removes the branch row entirely instead of keeping
a branch tombstone.

## 2026-05-25 19:24 PDT

Branch delete-over-base is green and found a real bug. Deleting a pinned-base
row on a branch did not shadow the base snapshot because there was no branch
current row to copy/delete. `delete_row` now creates delete history from the
visible semantic row when needed, and branch deletes keep an `is_deleted = 1`
current tombstone so base snapshot reads are shadowed. Apply/rebuild preserve
branch tombstones too.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
branch_delete_shadows_pinned_base_row` passes, including sync and projection
rebuild.

## 2026-05-25 19:25 PDT

Starting explicit generic update operation. So far the generic write API is
insert/upsert-shaped and delete-shaped; adding an explicit update op should
make history semantics closer to the intended create/update/delete model.

## 2026-05-25 19:25 PDT

Explicit generic update is green. Added `update_row`, which records history op
`2`, updates current projection, syncs through bundles, and preserves row-level
write-set materialization.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
generic_update_records_update_op_and_syncs_current_value` passes.

## 2026-05-25 19:26 PDT

Starting generic update support inside sealed transactions. The standalone
`update_row` path works, but the single parametrized transaction constructor
should be able to seal updates alongside other mutations too.

## 2026-05-25 19:26 PDT

Generic update support inside sealed transactions is green. `TransactionBuilder`
now has `update_row`, and generic transaction mutations carry the intended op
code so updates and creates can share one sealed tx.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
generic_transaction_can_seal_updates_atomically` passes.

## 2026-05-25 19:27 PDT

Starting index-only schema compatibility coverage. The spec says schema changes
that only affect indexes should be automatically lens-compatible, so peers with
different index declarations but identical row shapes should sync semantically.

## 2026-05-25 19:27 PDT

Index-only schema compatibility coverage is green. An unindexed writer can sync
to a peer whose schema adds an index over a user column and `$createdAt`, and
the peer can query semantically through the indexed schema shape.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
index_only_schema_changes_are_semantically_compatible` passes.

## 2026-05-25 19:28 PDT

Starting direct fate monotonicity coverage from sidecar review. Bundle apply has
monotonic outcome merging, but direct local authority calls may still downgrade
accepted/rejected state if `reject_transaction` and `accept_transaction_at_*`
are called in different orders.

## 2026-05-25 19:30 PDT

Direct fate monotonicity is green and found a real downgrade bug. Accepting a
rejected transaction directly changed `outcome` back to accepted while leaving
the rejection reason around. Direct edge/global accept now uses monotonic
`MAX(outcome, accepted)`, so rejected remains terminal while global metadata and
receipts can still attach.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system direct_`
passes for the direct accept/reject ordering cases.

## 2026-05-25 19:30 PDT

Starting pinned branch base same-epoch tie-breaking coverage from sidecar
review. We allow multiple transactions per global epoch, so snapshot selection
must still choose one version of a row when two versions share the base epoch.

## 2026-05-25 19:31 PDT

Pinned branch base same-epoch tie-breaking is green and found a duplicate
snapshot bug. Snapshot selection now treats `(global_epoch, tx_num)` as the
version order, so multiple versions of one row in the same global epoch collapse
to the latest local transaction version. Applied the same tie-breaker to
snapshot policy dependency lowering/export.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
branch_base_snapshot_chooses_latest_row_version_within_same_global_epoch`
passes.

## 2026-05-25 19:31 PDT

Starting rejected branch overlay fallback coverage from sidecar review. If a
branch-local update/delete shadows a pinned base row and then that tx is
rejected, branch reads should fall back to the pinned base version.

## 2026-05-25 19:32 PDT

Rejected branch overlay fallback coverage is green. Existing projection repair
already deletes rejected branch current rows, and the pinned-base read path then
reveals the base version again. Added explicit update and delete tests,
including projection rebuild for the update case.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
rejected_branch_` passes.

## 2026-05-25 19:33 PDT

Starting branch ref-policy visibility coverage from sidecar review. Suspicion:
`read_if_ref_readable` over current projections does not constrain the parent
row to the same branch-visible scope, so branch-local parent changes may be
ignored by policy checks.

## 2026-05-25 19:35 PDT

Branch ref-policy visibility is green and found a semantic hole. Base snapshot
children were evaluated against base snapshot parents even when the branch had
a local parent overlay. Added a branch-visible post-filter for pinned-base reads
so `read_if_ref_readable` sees the effective branch parent view. Current-read
policy lowering is also branch-constrained for parent refs.

Design note: the post-filter is intentionally semantic and recursive over
`read_rows`; it is correctness-first. A production lowering should push this
effective-branch parent policy into SQL.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
branch_ref_policy_uses_branch_local_parent_visibility` passes.

## 2026-05-25 19:35 PDT

Starting lens plus write-policy validation coverage from sidecar review. We have
read-policy lens coverage, but untrusted apply should also validate
`write_if_ref_readable` correctly when the receiving schema renamed the ref via
a lens.

## 2026-05-25 19:37 PDT

Lens plus write-policy validation is green and found a compatibility bug.
Untrusted apply of an old-schema payload failed validation because
`write_if_ref_readable("workspace")` only looked for the semantic field name,
not the stored/lensed name `project`. Policy write validation now falls back to
`field.storage_name`.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
renamed_ref_lens_participates_in_untrusted_write_policy_validation` passes.

## 2026-05-25 19:37 PDT

Starting branch subscription diff coverage from sidecar review. Since
subscriptions are implemented as rerun-and-diff, polling after checkout should
produce the same semantic transition as one-shot reads in the new active branch.

## 2026-05-25 19:38 PDT

Branch subscription diff coverage is green. Added tests for polling after
checkout to a branch with extra overlay rows, polling after checkout back to
main, and a pinned branch subscription ignoring later main updates until the
branch view itself changes.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
subscription_` passes.

## 2026-05-25 19:39 PDT

Starting deleted-row export scope coverage from sidecar review. Deleted history
is necessary for table sync, but branch/query scopes should not leak unrelated
tombstones beyond the requested result/scope.

## 2026-05-25 19:39 PDT

Deleted-row query-scope coverage is green. The generic equality query export
does not leak an unrelated deleted row in the same branch; it exports only the
matching live row. This did not require a runtime change.

Design note: full table history export intentionally includes table tombstones
so peers can remove deleted rows. Query-scoped export is the path where deleted
row leakage would be wrong.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
branch_scoped_export_excludes_unrelated_deleted_rows` passes.

## 2026-05-25 19:40 PDT

Starting untrusted validation atomicity/error-path coverage. Sidecar review
called out that `apply_untrusted_bundle` applies then validates, so validation
errors may leave current projection temporarily or permanently polluted.

## 2026-05-25 19:41 PDT

Untrusted validation missing-parent coverage is green. A todo that references a
missing policy parent is applied, then rejected with `policy_denied`, and the
trusted edge does not expose it in current reads. This did not require a runtime
change.

Design note: this still does not fully solve transactional staging/validation;
it only covers this concrete error-prone missing-parent case. The production
shape should validate in a staging transaction before publishing current rows.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
untrusted_validation_error_does_not_leave_invalid_current_row_visible` passes.

## 2026-05-25 19:41 PDT

Verification checkpoint: `cargo test -p mini-jazz-sqlite` passes. This covers
the crate target, doc tests, and the full whole-system integration suite at 86
tests.

## 2026-05-25 19:42 PDT

Starting generic delete support inside sealed transactions. The generic
transaction constructor can now create and update rows, but not delete them;
that leaves create/update/delete asymmetrical for sealed transactions.

## 2026-05-25 19:43 PDT

Generic delete support inside sealed transactions is green. `TransactionBuilder`
now supports `delete_row`, and a single sealed transaction can delete one row and
insert another while materializing both write-set rows under the same tx ID.

Limitation: this first version covers ordinary current-projection deletes. It
does not yet reuse the richer pinned-base branch delete logic from standalone
`delete_row`.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
generic_transaction_can_seal_delete_with_other_mutations` passes.

## 2026-05-25 19:44 PDT

Starting branch equality query effective-policy coverage from sidecar review.
`read_rows` now post-filters pinned-base rows through effective branch policy,
but `read_rows_where_eq` still appears to return combined branch/base rows
without that policy pass.

## 2026-05-25 19:45 PDT

Branch equality query effective-policy coverage is green and found the expected
bug. `read_rows_where_eq` now applies the same effective-branch policy filter as
`read_rows` after combining branch overlay rows with pinned base snapshot rows.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
branch_equality_query_uses_effective_branch_policy` passes.

## 2026-05-25 19:45 PDT

Starting out-of-order global epoch current-projection coverage from sidecar
review. If a peer receives an older global epoch after a newer version of the
same row, current projection should not regress to the older value.

## 2026-05-25 19:47 PDT

Out-of-order global epoch current projection is green and found a real apply
order bug. Applying epoch 10 after epoch 20 regressed current state to epoch 10.
`apply_history_record` now checks whether the incoming record is the newest
known version for that row/branch using global epoch first and tx_num as a
tie-breaker before publishing to current.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
out_of_order_global_epochs_do_not_regress_current_projection` passes.

## 2026-05-25 19:49 PDT

Starting untrusted delete validation coverage. Sidecar review noticed that
`apply_untrusted_bundle` skipped delete records entirely, which may let an
untrusted peer hide rows through deletes that an edge would have rejected if the
same policy were enforced symmetrically.

## 2026-05-25 19:51 PDT

Untrusted delete validation is green. The failing test showed Bob could delete
Alice's row at the edge because delete records bypassed policy validation. The
runtime now validates pending delete records too, uses `updated_by` as the actor
for updates/deletes, skips revalidating transactions that are no longer pending,
and rebuilds current projection after any untrusted rejection so rejected deletes
restore the prior visible version.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
trusted_edge_rejects_untrusted_delete_policy_violation` passes.

## 2026-05-25 19:53 PDT

Starting conflict-candidate policy coverage. `read_row_candidates` currently
reads directly from source branch current projections; I want to verify that
candidate inspection still respects the caller's effective row policy.

## 2026-05-25 19:54 PDT

Conflict-candidate policy filtering is green. The failing test showed merge
candidate inspection could reveal a candidate whose required parent row was not
visible to the caller. Candidates are now post-filtered by evaluating the row
policy in the source branch that produced each candidate.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
branch_conflict_candidates_respect_effective_row_policy` passes.

## 2026-05-25 19:54 PDT

Starting rebuild-order coverage for global epochs. Apply-time current projection
now uses global epoch ordering, but `projection::rebuild` still appears to scan
history by local `tx_num`, which can diverge from durable global order.

## 2026-05-25 19:55 PDT

Rebuild global ordering is green. The failing test created two versions whose
local tx order disagreed with global epoch order; apply-time current projection
kept epoch 20, but rebuild replayed to epoch 10. Rebuild now orders by
row/branch, durable global epoch, then tx_num, with local pending rows sorting
after durable rows to preserve optimistic current state.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
rebuild_uses_global_epoch_order_not_local_tx_order` passes.

## 2026-05-25 19:56 PDT

Starting generic transaction branch-delete coverage. Standalone `delete_row`
can create a branch tombstone for a row visible only through the pinned base
snapshot, but `TransactionBuilder::delete_row` still only deletes rows already
materialized in current projection.

## 2026-05-25 19:57 PDT

Generic transaction branch delete is green. The transactional path now stages
visible delete snapshots before opening the SQLite transaction and can write a
branch tombstone from a pinned-base row, matching standalone `delete_row`.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
generic_transaction_delete_shadows_pinned_base_row` passes.

## 2026-05-25 19:57 PDT

Starting direct global-acceptance projection repair. Apply-time and rebuild-time
ordering now use global epochs, but a local authority call that assigns an older
global epoch to the latest local tx can leave current projection on the older
durable version until an explicit rebuild.

## 2026-05-25 19:58 PDT

Direct global-acceptance projection repair is green. A local authority can now
assign global fate out of local tx order without leaving current projection on
the wrong version. Current fix is intentionally coarse: direct global acceptance
rebuilds projection from history instead of doing an incremental per-row repair.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
direct_global_acceptance_repairs_current_projection_order` passes.

## 2026-05-25 19:59 PDT

Starting created-by write policy creation semantics. Current
`write_if_created_by_principal` acts like an existing-row owner check and makes
ordinary self-authored inserts impossible, which is too narrow for row-owner
policy semantics.

## 2026-05-25 20:00 PDT

Created-by write policy semantics are green. Self-authored inserts are now
allowed for owner-write tables. The same test exposed a second bug: generic
updates rewrote `j_created_by` to the updater, effectively transferring row
ownership on every update. Generic writes now preserve creation metadata from
the current row for non-insert operations.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
created_by_write_policy_allows_self_create_but_rejects_other_writer` passes.

## 2026-05-25 20:01 PDT

Starting atomic untrusted transaction rejection. Sidecar review flagged that
`apply_untrusted_bundle` validates records after applying the bundle; if one
record in a multi-write transaction fails, all sibling current effects from the
same sealed transaction must disappear too.

## 2026-05-25 20:02 PDT

Atomic untrusted transaction rejection was already green after the earlier
projection rebuild-on-rejection fix. Added explicit coverage: a Bob-authored
two-write transaction with one allowed sibling and one denied sibling is rejected
as a whole, leaving no todo rows visible at the trusted edge.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
trusted_edge_rejects_untrusted_transaction_atomically` passes.

## 2026-05-25 20:03 PDT

Starting branch-aware write-policy validation. `write_if_ref_readable` appears
to evaluate parent readability against any current row with the referenced row
id, not the transaction's branch view. A parent visible only in one branch should
not authorize a child write in a different branch.

## 2026-05-25 20:04 PDT

Branch-aware write-policy validation is green. The failing test showed a parent
visible in branch `other` could authorize a child write in branch `draft`.
`write_if_ref_readable` validation now receives the transaction branch and
constrains the referenced parent current row to that branch.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
branch_write_policy_does_not_use_parent_from_different_branch` passes.

## 2026-05-25 20:06 PDT

Starting remote-pending visibility coverage. Local optimistic mergeable
transactions should be visible on their origin node, but a peer receiving a
pending remote transaction should not let it override an accepted/global current
version before a trusted tier accepts it.

## 2026-05-25 20:08 PDT

Remote-pending current ordering is green. First attempt was too broad and hid
all remote pending rows, breaking table/query scoped sync. Narrowed the rule:
remote pending rows may materialize when there is no durable version for that
row/branch, but they cannot displace an accepted/global version. Rebuild orders
remote pending before durable versions and local pending after durable versions.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
remote_pending_update_does_not_override_global_current_on_peer` passes, and full
`cargo test -p mini-jazz-sqlite` passes with 98 whole-system tests.

## 2026-05-25 20:09 PDT

Starting branch-base write-policy coverage. The previous branch-scope fix
prevents cross-branch parent leakage, but a branch write should still be
authorized by a parent row visible through the branch's pinned main-base
snapshot.

## 2026-05-25 20:10 PDT

Branch-base write-policy validation is green. `write_if_ref_readable` now uses
an effective branch relation: branch-local rows win, otherwise main rows are
visible if the branch has no shadow for that row. This keeps the cross-branch
leak closed while allowing pinned-base parents to authorize branch writes.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
branch_write_policy` passes, and full `cargo test -p mini-jazz-sqlite` passes
with 99 whole-system tests.

## 2026-05-25 20:10 PDT

Starting prior-row read-set coverage. Policy reads are recorded, but updates and
deletes also depend on the previously visible row version for validation and
causality. I want that represented explicitly in `jazz_tx_read`.

## 2026-05-25 20:12 PDT

Prior-row read-set coverage is green. Generic updates and transaction deletes
now record reason `2` reads for the row version they depend on, alongside reason
`1` policy reads. Added an inspection helper for previous-row reads.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
generic_update_records_previous_row_read_set` passes, and full
`cargo test -p mini-jazz-sqlite` passes with 100 whole-system tests.

## 2026-05-25 20:13 PDT

Recursive query tombstone export is green. A peer that had previously synced a
recursive root+child now receives the deleted child tombstone when the child
drops out of the recursive result, so it can converge to root-only. The
implementation currently covers deleted direct descendants of the visible
recursive result; fully recursive deleted subtrees remain a future sharpening.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
recursive_query_scope_sync_exports_deleted_descendant_tombstone` passes, and
full `cargo test -p mini-jazz-sqlite` passes with 101 whole-system tests.

## 2026-05-25 20:14 PDT

Starting explicit delete prior-row read-set coverage. The implementation records
reason `2` reads for transaction deletes; adding a focused test so this does not
remain only indirectly covered.

## 2026-05-25 20:14 PDT

Delete prior-row read-set coverage is green. Transaction deletes now have
explicit test coverage showing the deleted row is recorded as a reason `2`
read-set entry.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
generic_transaction_delete_records_previous_row_read_set` passes, and full
`cargo test -p mini-jazz-sqlite` passes with 102 whole-system tests.

## 2026-05-25 20:15 PDT

Starting accepted-after-remote-pending visibility coverage. A peer may keep a
remote pending update history-only because a durable row already exists, but
once a trusted tier accepts that same transaction, current projection must
advance to it.

## 2026-05-25 20:15 PDT

Accepted-after-remote-pending visibility is green without further code changes.
The previous remote-pending fix still allows the accepted fate update to replay
the same history record and advance current projection once the transaction
becomes globally accepted.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
accepted_remote_pending_update_repairs_peer_current_projection` passes, and full
`cargo test -p mini-jazz-sqlite` passes with 103 whole-system tests.

## 2026-05-25 20:16 PDT

Starting exclusive same-row conflict coverage. The prototype currently proves
exclusive transactions require global acceptance, but not that exclusive writes
to the same row actually force an authority decision.

## 2026-05-25 20:19 PDT

Exclusive same-row conflict coverage is green. Added a prototype authority-side
check that rejects an exclusive transaction if it tries to write a row already
written by an accepted exclusive transaction. While running the full suite, an
uncommitted fixture test also exposed that `open_todos` needed to use the
generic row reader to honor pinned-base branch overlays; that path is now green
too.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system
exclusive_transaction_rejects_same_row_conflict` passes, targeted
`fixture_open_todos_reads_pinned_base_with_sparse_overlay` passes, and full
`cargo test -p mini-jazz-sqlite` passes with 105 whole-system tests.

## 2026-05-25 20:16 PDT

Starting conflict-candidate policy coverage. `read_row_candidates` is the visible-conflict surface, so it must not leak candidate rows from source branches that the reader cannot read by policy. This is a small but product-shaped invariant: conflicts can be multiple visible candidates, not privileged hidden candidates.

## 2026-05-25 20:17 PDT

Candidate policy leakage was already covered in `branch_conflict_candidates_respect_effective_row_policy`, so I am pivoting to exclusive transaction conflicts. The API enforces exclusive transactions at global tier, but the prototype should also prove same-row exclusive writes get a deterministic accepted/rejected fate instead of both silently winning by last projection order.

## 2026-05-25 20:17 PDT

Exclusive conflict coverage also already exists. Next slice: remove divergence between fixture todo queries and generic branch semantics. The hardcoded `open_todos` path should see pinned-base rows plus sparse branch overlay just like `read_rows`, because otherwise examples can accidentally pass while the real generic runtime behaves differently.

## 2026-05-25 20:18 PDT

Fixture todo branch semantics are green. `open_todos` now reconstructs fixture views through the generic query path, so it inherits sparse branch base and overlay behavior instead of querying only branch-local current rows. This intentionally sacrifices the fixture-specific hand-written SQL path for semantic alignment with the generic runtime.

Test status: `cargo test -p mini-jazz-sqlite --test whole_system fixture_open_todos_reads_pinned_base_with_sparse_overlay` passes, and full `cargo test -p mini-jazz-sqlite` passes with 105 whole-system tests.

## 2026-05-25 20:19 PDT

Found the exclusive-conflict slice already present in the working tree rather than committed. The full suite confirms it: same-row exclusive transactions now reject the second accepted global write with `exclusive conflict`, while the earlier accepted value remains current. I am keeping the log honest and committing this together with the fixture query alignment.

Test status: full `cargo test -p mini-jazz-sqlite` passes with 105 whole-system tests.
