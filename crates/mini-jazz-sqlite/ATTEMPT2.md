# Attempt 2: Schema-Driven SQLite Engine

Started: 2026-05-25 11:10 PDT.

Goal: build a small working system around schemas, layouts, plans, effects, and
whole-system tests. The implementation should discover architecture, not only
feature behavior.

## Guardrails

- Product-shaped integration tests first.
- Detailed decision/discovery log while context is fresh.
- Native Rust SQLite via `rusqlite`.
- Mutable fate on `jazz_tx` as baseline.
- Per-column conflict metadata from the start.
- Keep attempt1 under `reference/attempt1` for comparison.

## Decisions And Discoveries

### 2026-05-25 11:10 PDT

Starting attempt2 from a clean prep commit.

First target: a schema-driven local engine slice with a public-ish Rust API:

- define `projects` and `todos`
- open a durable client store
- apply one write transaction through schema-derived plans
- query open todos with a required project include
- capture result and dependency scope
- rebuild current projections byte-for-byte
- reopen the same SQLite file and reproduce the query

This is deliberately bigger than "insert a todo" because it pressures the
first abstractions immediately: layouts, row codecs, query plans, scope, and
projection rebuilds.

### 2026-05-25 11:11 PDT

First red test failed before compile due to workspace SQLite linkage:

```text
rusqlite 0.37 -> libsqlite3-sys 0.35
jazz-tools uses rusqlite 0.34 -> libsqlite3-sys 0.32
```

Cargo only allows one crate with `links = "sqlite3"` in the workspace graph.
Decision: attempt2 uses `rusqlite 0.34` for now, matching the existing
workspace. This is a workspace hygiene constraint, not an architectural choice.

### 2026-05-25 11:14 PDT

First product-shaped local test is green:

- schema builder defines `projects` and `todos`
- schema-derived DDL creates history/current tables for both
- one write call creates one `jazz_tx` and two row history/current entries
- joined query reads `todos` with required `project`
- query returns result scope plus dependency scope
- current projections rebuild byte-for-byte from history
- durable reopen reproduces the query and scope

Discovery: the first useful boundary is not a component object. It is the
schema-derived table plan: field list, physical tables, generated DDL, row
codecs, current projection columns, and fingerprint/rebuild shape all want to
come from one data artifact.

Discovery: even the tiny DDL generator caught a real layout bug. Quoted table
identifiers cannot be used as string prefixes for index identifiers. Physical
names and quoted SQL identifiers need to stay separate in the layout layer.

Discovery: the public-ish API is already doing useful pressure work. The test
did not call `insert_todo` or `query_open_todos_with_projects`; fixture tables
are concrete, but the engine path is schema-driven.

### 2026-05-25 11:15 PDT

Starting subscription slice with a red joined-subscription test:

- subscribe to open todos with required project include
- update only the project row
- poll subscription
- expect an updated semantic result row, not an unchanged result

This targets the attempt1 lesson that subscription state must store dependency
payloads, not only result row ids/versions.

### 2026-05-25 11:17 PDT

Joined dependency-update subscription test is green.

Implementation shape:

- `Client::subscribe(query)` runs the query once and stores full previous
  `RowView`s.
- `Client::poll_subscription(id)` reruns the query and diffs by `$rowId` plus
  full row payload equality.
- `RowView` includes nested dependency values, so a project-only rename changes
  the semantic todo row even though the todo row id and tx id stay the same.

Discovery: for v0, a subscription can be almost embarrassingly simple if the
previous result stores decoded dependency payloads. The correctness boundary is
`run_query -> full semantic rows + scope`, not "watch this result table row".

Discovery: update support immediately made immutable creation metadata useful.
`update` preserves `j_created_at`, writes a new history row, updates
`j_updated_at`, and current rebuild still has enough data to stay deterministic.

### 2026-05-25 11:18 PDT

Adding the next subscription red test: required dependency deletion should
remove the parent joined result. This keeps pressure on the generic write path:
delete must be a history version, current projection state, and subscription
semantic diff, not a table-specific special case.

### 2026-05-25 11:19 PDT

Required dependency deletion subscription test is green.

Implementation shape:

- `WriteTx::delete(table, row_id)` writes a `delete` history version.
- The current projection keeps the row with `j_is_deleted = 1`.
- Required includes remain `INNER JOIN ... dep.j_is_deleted = 0`.
- Subscription rerun+diff reports the parent result as removed.

Discovery: keeping deleted rows in current projection with an explicit
`j_is_deleted` bit makes required-join semantics simple and matches the rebuild
shape. The query plan, not the write path, decides whether deleted rows
participate.

Subagent review highlighted architecture debt to address soon:

- DDL, query lowering, projection rebuild, and write execution still re-derive
  table layout locally from `TableDef` instead of using explicit
  `StorageLayout`/`TablePlan`/`WritePlan`/`QueryPlan` data.
- System-column mapping is duplicated between index creation and query lowering.
- The `main` branch is hard-coded in several execution paths.
- `include_required(alias, fk_column)` leaks relation layout into the query API.
- `RowView::get` only returns string values, which will get awkward for bools,
  numbers, and system columns.
- There is no explicit `EffectLog` yet; subscriptions poll by full rerun.

Decision: continue through the subscription slice, but do not let these harden.
Before sync/authority, carve out at least table/layout plans and centralized
system-column mapping so later phases compose through data artifacts.

### 2026-05-25 11:19 PDT

Adding optional-include subscription red test:

- subscribe to open todos with optional project include
- delete the project
- parent todo should remain in the result
- nested project should become absent/null
- subscription diff should report an updated row, not removed

This is the left-join counterpart to the required dependency deletion test.

### 2026-05-25 11:20 PDT

Optional dependency deletion subscription test is green.

Implementation shape:

- `include_optional` lowers to `LEFT JOIN`.
- Required and optional includes share the same schema relation resolution.
- If the dependency side is absent/deleted, the parent row remains and the
  nested include is omitted from `RowView`.
- Full-row diff reports an updated semantic row.

Discovery: optional nulling fits the same rerun+diff model with almost no
subscription-specific logic. The important distinction is all in the query
plan: `INNER JOIN` vs `LEFT JOIN`, and whether absent dependency columns decode
to no nested row.

Open debt: optional absence currently has no predicate/range scope. The result
semantics are correct locally, but sync/authority will need explicit absence
facts before this is a complete scope story.

### 2026-05-25 11:23 PDT

After slices 1-3, carved out the first explicit `TablePlan`.

It now owns:

- physical history/current table names
- user column list
- generated index names
- user/system column mapping for index/query lowering
- user column DDL fragments

This is intentionally modest, but it moved repeated physical layout decisions
out of DDL, query, write, rebuild, and fingerprint code. Existing tests stayed
green.

Discovery: this is the right kind of abstraction for attempt2: not a manager,
just data plus a few local derivation helpers. It reduces drift while keeping
the execution verbs plain.

Remaining architecture debt before sync/authority:

- `main` is still hard-coded as an execution assumption.
- write/query/rebuild are still large methods and should split into
  `WritePlan`, `QueryPlan`, and `ProjectionPlan` shapes as pressure increases.
- relation intent still leaks through `include_required(alias, fk_column)`.
- no explicit `EffectLog` yet.

### 2026-05-25 12:21 PDT

Added branch/sync coverage for exporting a branch query scope and importing it
into another store, then reading it back with `all_on_branch`.

Implementation shape:

- `QueryScopeBundle` now carries branch records alongside tx and history rows.
- `export_query_scope` expands scoped rows by their visible tx ids, so branch
  overlay history is included instead of only `main` history.
- `import_query_scope` upserts bundled branch metadata before inserting history
  and rebuilding current projections.

Discovery: branch sync needs two distinct facts, not just row history. The
branch-local tx supplies the overlay row, while `jazz_branch.base_global_epoch`
supplies the accepted main base that `all_on_branch` overlays against.

### 2026-05-25 11:24 PDT

Starting sync slice with a red query-scope bundle test:

- Alice writes joined todo/project data.
- Alice runs the joined query and gets result/dependency scope.
- Alice exports the scope.
- Bob imports it into an empty store with the same schema.
- Bob reproduces the joined query locally.

This is the first pressure on scope-to-bundle expansion and semantic import.
The intended first shape is full row history for scoped rows, deduped
transaction records, then projection rebuild on import.

### 2026-05-25 11:25 PDT

First query-scope sync test is green.

Implementation shape:

- `export_query_scope(scope)` deduplicates scoped `(table, row_id)` pairs.
- For each scoped row, it exports all `main` branch history versions for that
  row.
- Transaction records are deduped by `tx_id` and export stable `node_id`
  instead of local `node_num`.
- `import_query_scope(bundle)` hydrates `node_num`, upserts `jazz_tx`, inserts
  missing history rows, then rebuilds current projections.

Discovery: full-history scope is a natural first sync shape. It is not compact,
but it lets the receiver reproduce the current joined query without a separate
result payload and keeps semantic history available for later diff/time-travel
tests.

Discovery: `TablePlan` immediately helped the sync slice: export/import could
reuse physical history names and user columns instead of inventing another
table-specific path.

Open debt:

- Bundles are Rust structs only; no canonical wire encoding yet.
- Import does a broad projection rebuild.
- Only concrete row scope is handled; predicate/absence scope is still missing.
- Only `main` branch history is exported.

### 2026-05-25 11:26 PDT

Starting authority loop with a red local-to-global mapping test:

- Alice writes optimistically.
- Alice exports query scope to core.
- Core imports the same transaction/history.
- Core accepts the transaction at global epoch 1.
- Core exports the accepted scope back.
- Alice imports it and sees the same tx id enriched with accepted fate/global
  epoch.

This intentionally skips read-set validation at first. The target is the
identity/fate propagation shape.

### 2026-05-25 11:27 PDT

First authority acceptance loop is green.

Implementation shape:

- `Harness::authority` is currently role sugar over the same store type.
- Core imports Alice's scoped tx/history bundle.
- `accept_transaction(tx_id, global_epoch)` mutates `jazz_tx.status` and
  `jazz_tx.global_epoch`.
- Core exports the same query scope back.
- Alice imports it and upserts fate on the existing tx id.

Discovery: the mutable-fate baseline is enough for the first local-to-global
mapping flow. The public tx id remains stable; authority acceptance enriches
the existing transaction row.

Open debt:

- This is not yet authority validation. There are no read sets, policies, or
  constraints in the acceptance path.
- `authority` is not a distinct role yet.
- Proposal-vs-authority-observation is only conceptual; there is no separate
  observation payload beyond the updated tx record.

### 2026-05-25 11:27 PDT

Starting authority rejection repair test:

- Alice writes optimistically and sees the joined row locally.
- Core imports the same scoped bundle.
- Core rejects the tx with a machine-readable reason.
- Alice imports the rejected tx bundle.
- Alice repairs current projections and no longer sees the optimistic row.

This tests mutable fate plus import side effects in the negative direction.

### 2026-05-25 11:29 PDT

Authority rejection repair is green.

Implementation shape:

- `reject_transaction(tx_id, reason)` mutates `jazz_tx.status` to `rejected`
  and stores machine-readable reason JSON.
- `export_transaction(tx_id)` exports one tx record plus all history rows
  written by that tx across schema tables.
- `import_query_scope` upserts the rejected fate and rebuilds current
  projections.
- Visibility queries filter rejected txs through current projection rebuild.

Discovery: broad projection rebuild makes rejection repair almost trivial and
keeps the invariant obvious. This is absolutely not the final hot path, but it
is the right first semantics path.

Discovery: exporting by tx id is a distinct protocol primitive from exporting a
query scope. Query scope export is result/dependency-shaped; fate propagation
sometimes needs to send a transaction even after it no longer appears in a
query result.

Open debt: transaction export currently scans every schema table for rows with
that tx id. A generated write-set/table membership index should replace that.

### 2026-05-25 11:30 PDT

Starting first read-set validation test:

- Core creates and accepts a base todo.
- Alice and Bob both import the base.
- Alice updates the row and core accepts her transaction.
- Bob updates from the stale base.
- Core should reject Bob because his row read set points at the old visible tx.

This is the first real exclusive/global-consistent validation path. It should
force update writes to record previous visible row versions.

### 2026-05-25 11:34 PDT

Split the prototype before continuing the validation slice:

- `src/lib.rs` is now a thin public surface over `src/store.rs`.
- Authority tests moved to `tests/authority.rs`; local/query/subscription
  coverage remains in `tests/attempt2_local.rs` for now.

This is still a coarse split, but it stops the single-file prototype from
growing further while the architecture is becoming clearer.

Read-set validation is green.

Implementation shape:

- Write transactions collect row read-set entries when `update` or `delete`
  reads from the current projection.
- The read set is stored in `jazz_tx.metadata_json` and therefore moves through
  existing query-scope export/import without adding a new table yet.
- `accept_transaction_validating_reads(tx_id, global_epoch)` parses metadata
  and checks each observed row version against accepted authority history.
- Validation deliberately excludes the candidate tx from the authority lookup,
  because importing a proposal currently updates local current projections
  optimistically before the authority has accepted it.
- On mismatch, authority mutates the tx fate to `rejected`, records structured
  reason JSON, rebuilds current projections, and returns a `stale row read`
  error.

Discovery: validation should be based on authority history, not current
projection, unless the authority has a separate proposal quarantine. The
current projection can be polluted by pending imported proposals.

Open debt:

- Read sets only cover direct row reads from update/delete, not predicate/range
  reads from query execution.
- The read-set JSON shape is a prototype metadata payload. We still need to
  compare JSONB metadata against normalized read-set tables once query read
  sets exist.
- `accept_transaction_validating_reads` validates only accepted global history;
  mergeable/local-pending semantics are still outside this path.

### 2026-05-25 11:38 PDT

Corrected the coarse split after feedback.

The first split moved almost all implementation from `lib.rs` into a giant
`store.rs`, which did not solve the problem. The crate now has real top-level
modules:

- `error.rs`: shared `Error`/`Result`.
- `schema.rs`: schema DSL plus internal table/field/index model.
- `layout.rs`: physical SQLite table/column naming and value conversion.
- `query.rs`: query DSL, filters, include description, sort description, and
  filter SQL helper.
- `scope.rs`: query result/scope DTOs, row views, and subscription diffing.
- `store.rs`: runtime storage behavior, subscriptions, sync import/export,
  authority fate mutation, read-set validation, projection rebuild, and the
  write transaction path.

Discovery: even for prototype speed, module boundaries are useful once the
system starts to reveal verbs. The next worthwhile split is to move
transaction/write mechanics out of `store.rs` into `tx.rs`/`write.rs`, but this
pass already separates the type/plan/query vocabulary from runtime behavior.

Verified after the real split:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 11:41 PDT

Extracted write transaction mechanics too.

- `write.rs` now owns `WriteTx`, `RowRef`, row read recording, current-row
  reads for mutation, history append, and current projection writes.
- `store.rs` keeps transaction orchestration and read-set validation metadata
  for now, because authority validation/import/export still live there.

This brings `store.rs` under 1k lines. It is still the runtime coordinator, but
it no longer owns the mutation mechanics directly.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 12:21 PDT

Added a whole-system scenario test for the Alice/Bob/authority loop.

Red/green shape:

- Authority creates and accepts the base todo, then scoped-syncs it to Alice
  and Bob.
- Bob subscribes to the open todo query.
- Alice writes an optimistic update; Bob independently writes a stale
  optimistic update from the old base.
- Bob's local optimistic write updates his subscription immediately.
- Authority imports both scoped writes and exposes conflict candidate metadata
  while both txs are pending.
- Authority accepts Alice, rejects Bob via read-set validation, and Bob imports
  both fates.
- Bob's subscription updates to Alice's accepted value once Bob's rejected fate
  removes his optimistic projection.

Runtime adjustment:

- Query-scope and transaction exports now carry branch records required by the
  current `QueryScopeBundle` shape.
- Import invalidation compares current projections before and after sync so
  fate-only imports only rerun subscriptions when visible rows actually change.

Verified:

- `cargo test -p mini-jazz-sqlite --test whole_system -- --nocapture`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 12:21 PDT

Pushed three composition slices together because the worker outputs overlapped
cleanly with the projection-diff work.

Projection-diff effects:

- Red/green test: importing an older accepted transaction after the receiver
  already has a newer visible projection should not wake a subscription.
- `import_query_scope` now snapshots affected current rows before import,
  rebuilds projections, snapshots them again, and emits listener effects only
  for rows whose visible current projection changed.
- Discovery: inserted history and changed tx fate are too low-level as listener
  effect sources. Projection deltas are closer to what subscribers observe.

Branch sync:

- `QueryScopeBundle` now carries branch records.
- Branch-local rows can travel through scoped export/import and remain readable
  with `all_on_branch`.
- Discovery: branch provenance needs to be protocol payload, not only local
  catalog state, even in the query-only branch overlay model.

Whole-system composition:

- Added an integration flow for authority, Alice, and Bob:
  base sync -> local optimistic edits -> authority import -> conflict metadata
  -> accepted/rejected fate import -> subscription repair.
- Discovery: fate import order matters. Importing the accepted winner while the
  local stale candidate is still visible may not produce a semantic diff until
  the stale candidate's rejection arrives.

Verified:

- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 11:42 PDT

Added the first explicit predicate/absence scope fact.

Optional includes now record a `PredicateScope` when the referenced dependency
row is missing. The current shape is intentionally narrow:

- table
- row id
- reason: `OptionalIncludeMissing`

Discovery: this is a useful stepping stone before full predicate/range read
sets. It proves the query result can carry non-row-locator scope facts while
still keeping row result/dependency scope separate.

Open debt:

- Predicate scope is not yet exported in sync bundles.
- Predicate scope is not yet used for subscription invalidation.
- Required includes and ordinary filters still do not record range/predicate
  read facts.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 11:44 PDT

Split integration tests by behavior:

- `attempt2_local.rs`: local storage/query/reopen.
- `subscriptions.rs`: dependency-driven subscription diffs and optional
  absence scope.
- `sync.rs`: scoped bundle export/import reproduction.
- `authority.rs`: acceptance, rejection, and read-set validation.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 11:45 PDT

Made absence scope affect sync bundles.

Red test: Bob first imports a todo with an optional project include, then Alice
deletes the project and exports the same optional query. Without exporting the
predicate-scoped project row history, Bob kept rendering the stale project.

Fix: `export_query_scope` now follows `predicate_scopes` in addition to result
and dependency row locators. This sends the delete history needed for Bob to
reproduce the null optional include.

Discovery: predicate/absence scope is not just listener metadata; it is part of
the sync closure required to recreate query semantics.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 11:51 PDT

Added first filter predicate scope and made it affect sync closure.

Shape:

- Query filters now emit `PredicateScope { table, column, op, value }`.
- Filter predicate scopes currently have an empty `row_id`.
- Empty `row_id` means broad table predicate closure during export: include all
  row histories for that table.

Red/green discovery:

- A row entering a filtered result is already handled by result-row scope,
  because the row appears in the new result.
- A row leaving a filtered result is the important failure. The new result can
  be empty, so result-row scope alone sends no history and the receiver keeps a
  stale row.
- Broad table predicate closure fixes correctness for that case, at obvious
  overfetch cost.

Next optimization target: replace broad table predicate closure with indexed
old/new key ranges or a query-aware delta protocol, while preserving the
row-leaving-filter test.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 11:53 PDT

Added the first in-memory effect log for subscription invalidation.

Shape:

- `WriteTx` records touched row effects while appending history/current rows.
- `Client` stores an in-memory ordered effect log.
- Each subscription stores its last result rows, last query scope, last seen
  effect sequence, and a rerun counter.
- `poll_subscription` skips rerun when no new write effect overlaps the stored
  result/dependency/predicate scope.

Red/green test:

- Subscribe to open todos.
- Write an unrelated project row.
- Poll returns an empty diff and does not increment rerun count.
- Write a matching todo row.
- Poll reruns and reports one added row.

Discovery: the broad table predicate scope from filter reads makes simple
table-level invalidation correct but conservative. That is a good baseline:
indexed predicate/range scope can become a precision improvement without
changing the subscription verb shape.

Open debt:

- Effect log is not durable.
- Import/rejection/projection repair do not emit effects yet.
- Rerun counters are prototype test instrumentation, not a public API.
- Column masks and old/new index keys are not recorded yet.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 11:54 PDT

Made imported scope bundles emit effects.

Red test: Alice subscribes to an optimistic row, authority rejects the tx, Alice
imports the rejected tx bundle, and then polls the subscription. Before this
change, projection repair happened but the subscription skipped rerun because
import emitted no effects.

Fix: `import_query_scope` records write effects for bundled history rows after
rebuilding projections. This is broad and can over-invalidate, but it makes
remote sync/fate changes participate in the same subscription invalidation path
as local writes.

Discovery: projection repair and import are effectful verbs. Treating effects
as only local write output misses exactly the sync cases listeners care about.

Open debt:

- Imported effects should probably be based on projection deltas, not every
  bundled history row.
- Fate-only tx imports without bundled history still need a principled effect
  story.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 11:56 PDT

Added first pure-query historical snapshot read.

API:

- `all_at_global_epoch(query, global_epoch)`

Current limits:

- single table
- filters/order/limit
- no includes yet
- only accepted global transactions
- main branch only

Implementation shape:

- Read directly from the history table.
- Join `jazz_tx`.
- Keep only `global_durable_accepted` txs at or before the requested epoch.
- Use `NOT EXISTS` to select the latest visible history row per row id at that
  epoch.
- Exclude deletes.
- Reuse normal row result/scope and filter predicate scope construction.

Discovery: this validates the query-only snapshot path for basic current-vs-old
reads without any projection table. The SQL is simple enough for the prototype,
and the test protects the important invariant: current projection can move on
while a historical epoch still reconstructs the older accepted row version.

Open debt:

- Snapshot includes need the same history-backed lowering on both sides of the
  join.
- Branch snapshots need source-stack visibility, not just global epoch.
- Tie-breaking should eventually use the precise transaction coordinate rules,
  not just `(global_epoch, tx_id)`.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 11:59 PDT

Extended pure-query historical snapshots to required includes.

Red/green test:

- Insert a project and todo, accept them at global epochs 1 and 2.
- Update the project and accept at global epoch 3.
- Query joined todos at epoch 2 and epoch 3.
- The same todo sees the old project name at epoch 2 and the new project name
  at epoch 3.

Implementation shape:

- `all_at_global_epoch` now lowers a required include by joining the dependency
  history table through the same "latest accepted row at or before epoch"
  predicate used for the base row.
- Result scope and dependency scope are populated from history-backed rows.
- Optional historical includes remain untested, but the SQL path is already
  using `LEFT JOIN` when the query asks for optional.

Discovery: history-backed joins are mechanically straightforward but duplicate
the current-read lowering shape. This argues for extracting a reusable
"visible table expression" builder before branches, where the visibility
predicate becomes more complex.

Also fixed a stale-read validation bug found by the full suite: validation must
choose the latest accepted row by `global_epoch`, not by wall-clock update time.
Same-millisecond writes could otherwise make the authority compare against the
wrong accepted base.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 12:02 PDT

Started extracting visibility planning.

Change:

- Added `visibility.rs` with helpers for accepted-history visibility:
  - join a row version to an accepted transaction at/before an epoch
  - assert no newer accepted row version is visible at/before that epoch
- Rewired `all_at_global_epoch` to use those helpers for both base and joined
  dependency rows.

Discovery: even this tiny extraction immediately exposed parameter-order
coupling in generated SQL. A fuller query planner should return SQL fragments
plus ordered bind params together, not as separate strings and hand-maintained
parameter pushes.

Verified no behavior change:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 12:05 PDT

Added the first pure-query branch read.

API:

- `create_branch(branch_id, base_global_epoch)`
- `write_on_branch(branch_id, |tx| ...)`
- `all_on_branch(query, branch_id)`

Current limits:

- one source: main at a global epoch
- one table
- filters/order/limit
- no includes yet
- branch-local inserts/overrides are visible even before global acceptance

Implementation shape:

- `jazz_branch` stores branch id and main base epoch.
- `WriteTx` now carries a branch id; normal writes use `main`.
- Branch reads are query-only:
  - latest non-rejected row per row id from branch history
  - union with latest accepted main row at the branch base epoch
  - suppress main base rows when the branch has any non-rejected row for that
    row id

Discovery: the overlay query is straightforward for one source, but it already
points at the need for a shared visible-row expression builder. Branches,
snapshots, and future multi-source branch precedence are the same problem with
different source stacks.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 12:07 PDT

Improved imported bundle effects from broad history effects to actual import
delta effects.

Red/green test:

- Bob imports a scope bundle and subscribes to the query.
- Bob imports the same bundle again.
- Polling the subscription should not rerun, because the second import changed
  neither history nor tx fate.

Implementation shape:

- `import_query_scope` now detects tx fate changes before upsert.
- History row effects are emitted only when `INSERT OR IGNORE` actually inserts
  a row.
- If a tx fate changes, bundled history rows for that tx emit effects so
  rejection/acceptance projection repair still wakes listeners.

Discovery: projection effects should be tied to import deltas, not protocol
payload size. This keeps duplicate sync messages idempotent from the listener
perspective while preserving rejection invalidation.

Open debt: the best effect source is still probably projection diffing, because
inserted history does not always imply changed current visibility.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 12:08 PDT

Added a persistent write-set index.

Change:

- New system table: `jazz_tx_write(tx_id, table_name, row_id)`.
- Local writes populate it from `WriteTx` effects before commit.
- Imported history rows populate it during `import_query_scope`.
- `export_transaction(tx_id)` now reads distinct table names from
  `jazz_tx_write` instead of scanning every schema table.

Discovery: write effects, sync closure, and tx export want the same durable
write-set fact. The in-memory effect log is listener-oriented; `jazz_tx_write`
is protocol/runtime metadata and should survive restart.

Open debt:

- Backfill for older stores is not handled in the prototype.
- We still need column masks in the write-set.
- `export_transaction` should probably fail loudly if the tx has no write-set
  rows but does have history rows; that would catch index drift.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 12:10 PDT

Added first conflict candidate projection.

Red/green test:

- Core creates and accepts a base row.
- Alice and Bob both import the base and update the same row independently.
- Core imports both pending updates without validating/rejecting either.
- Query returns one resolved current row, while `$conflicts` contains both
  pending candidate tx ids.

Implementation shape:

- Current row views now expose `$conflicts` from `j_conflicts_json`.
- Current projection rebuild resets conflict metadata, then detects rows with
  multiple local-pending candidates on the same branch/row id.
- For those rows, current projection stores:
  `{ "candidates": [tx_id, ...] }`

Discovery: this is enough to prove the "resolved current value plus conflict
meta" shape, but the candidate detection is intentionally naive. It uses
multiple local-pending versions as the conflict signal, not true causality from
read/write sets.

Open debt:

- Conflict candidate detection should use causality/read-write sets.
- Conflict metadata should include per-column masks and candidate values, not
  only tx ids.
- Resolved value policy is currently whatever current projection selected
  deterministically; it is not a real merge resolver.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`

### 2026-05-25 12:12 PDT

Refined predicate invalidation with local write column masks.

Red/green test:

- Subscribe to `todos where done = false`.
- Keep a closed row outside the result.
- Update only the closed row's `title`.
- Polling the subscription should skip rerun, because neither result row scope
  nor the `done` predicate scope overlaps the changed column.

Implementation shape:

- `WriteEffect` now carries changed user columns.
- Inserts/deletes conservatively mark all user columns.
- Updates mark only patched columns.
- Predicate scope overlap checks table/row plus changed column:
  - exact result/dependency row still invalidates regardless of column
  - table-level predicate scope invalidates only when the predicate column was
    changed, or when the effect has unknown columns

Discovery: this keeps the broad table predicate closure correct while making it
less noisy for common non-result payload updates. It is still not true old/new
index-key range invalidation, but it is the first useful precision step.

Verified:

- `cargo fmt -p mini-jazz-sqlite`
- `cargo clippy -p mini-jazz-sqlite --tests --all-targets -- -D warnings`
- `cargo test -p mini-jazz-sqlite`
