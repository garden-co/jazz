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
