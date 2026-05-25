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
