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
