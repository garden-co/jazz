# Attempt 1: SQLite Core Speedrun

Started: 2026-05-24 22:10 PDT.

Goal: implement as much of the parity ladder as possible from the current spec,
prioritizing learning over polish. When the spec is fuzzy, make a local
decision, write it down here, and keep moving.

Timing note: headings after the initial entries were corrected against commit
timestamps, not intuition. The first burst from `Refine SQLite core transaction
model` at 22:10:42 to `Add conflict candidate spike` at 22:35:15 covered about
25 minutes of wall-clock time.

## Ground Rules

- Keep commits at meaningful checkpoints.
- Prefer executable semantics over microbenchmarks.
- Use the deterministic harness as the integration spine.
- Do not preserve compatibility with old Jazz internals.

## Decisions and Discoveries

### 2026-05-24 22:12 PDT

Starting with a minimal native Rust SQLite storage slice:

- one hard-coded `todos` table shape
- `jazz_node`, `jazz_tx`, `todos__schema_v1_history`,
  `todos__schema_v1_current`
- insert, current query, subscription-ish rerun/diff later
- inline JSONB columns represented as text JSON-ish placeholders at first

Decision: use `rusqlite` directly in the prototype. The spec wants a minimal
replaceable interface eventually, but a direct implementation should expose the
right seams faster.

Open fuzziness:

- SQLite JSONB construction from Rust values is not important for the first
  semantics slice; stable text payloads in BLOB/TEXT-shaped columns are enough
  to exercise the row flow.
- Conflict metadata starts as a single tx id array represented as a string.

### 2026-05-24 22:15 PDT

First concrete SQLite learning: bare `$systemColumn` identifiers fail because
SQLite treats `$name` as parameter syntax in many contexts. For implementation I
encoded system columns as snake_case (`tx_id`, `created_at`, etc.) while keeping
`$` names in the spec as semantic notation. Later lowering needs a single
identifier codec instead of hand-written names.

### 2026-05-24 22:18 PDT

Reached first vertical slice:

- deterministic harness skeleton
- pure model types for transaction status/vector visibility/read-write sets
- SQLite schema bootstrap
- insert/update/delete on hard-coded `todos`
- current query over user and system columns
- result-scope locators
- local subscription rerun+diff for added/updated/removed

Discovery: the hard-coded table is a good forcing function. Generic schema
lowering would be premature; the current useful pressure is on transaction
metadata, read/write sets, and deterministic projection rebuilding.

Subscription decision: callback-free polling API for tests. This avoids async
runtime choices and still exercises the semantic loop:

```text
write -> rerun SQL -> full-row diff -> update stored result
```

### 2026-05-24 22:20 PDT

Added a persistence/reopen check and byte-for-byte current projection rebuild.

Discovery: update and delete history rows must carry immutable creation
metadata (`created_at`, and eventually `created_by`) as well as updated
metadata. If history rows only store the new values, rebuilding the main current
projection from history can drift even when ordinary current reads look correct.

For now the prototype preserves `created_at` across update/delete rows. It still
uses the write actor as `created_by` on update/delete history rows because the
read model does not expose `created_by` yet; that should be corrected before
this becomes a real projection invariant.

### 2026-05-24 22:22 PDT

Added model-level acceptance mapping:

- local/edge durable transaction keeps its `tx_id`
- authority maps `(tx_id, node, local_epoch)` to `global_epoch`
- accepted transaction remains addressable by both old local coordinates and
  new global epoch

This supports the "txids first, epoch indexes later if useful" direction while
still making the local-to-global upgrade explicit.

Added first storage-level snapshot read:

- `query_todos_at_local_epoch(query, node_id, local_epoch)`
- implemented as a pure history query, not a projection table
- chooses the latest non-rejected version per row at or below the requested
  local epoch
- delete rows suppress the row at that snapshot

Fuzziness: this is intentionally only a same-node local snapshot. It does not
yet evaluate a full dotted version vector across global base, local bases, and
explicit tx includes. The SQL shape is useful, though: "latest visible version
per row" can be expressed with history joins and `NOT EXISTS`, and the next
step is to replace the same-node predicate with a visibility relation.

### 2026-05-24 22:23 PDT

Added storage-level global acceptance:

- `accept_tx(tx_id, global_epoch)` updates `jazz_tx`
- accepted transactions become `global_durable_accepted`
- global snapshot reads only include accepted transactions at or below the
  requested global epoch

Discovery: the current projection and historical snapshot queries now
deliberately answer different questions. `todos__schema_v1_current` shows the
local optimistic main-branch state, including local pending writes. A global
snapshot query is authority-shaped and only sees globally accepted writes. This
seems right, but the API needs to make the read mode explicit so callers do not
confuse "current local" with "globally durable at epoch N".

Open fuzziness:

- `accept_tx` currently mutates the transaction row directly. The real system
  may want an append-only authority receipt table, with `jazz_tx` holding the
  denormalized current acceptance state.

Added storage-level rejection:

- `reject_tx(tx_id, reason_json)` marks pending/edge transactions rejected
- rejected history remains stored
- local and global snapshot reads filter rejected transactions out

Discovery: this validates the "no vector excludes for rejected txs" instinct.
Rejected txs can remain in history and simply fail the visibility predicate.
The hard remaining problem is not historical visibility; it is repairing the
optimistic current projection after a local write is later rejected.

### 2026-05-24 22:25 PDT

Recommended next five rungs after the CRUD/query/subscription/snapshot basics,
ordered for learning value:

1. Full snapshot vector visibility over history.
   Replace the same-node local snapshot predicate with the spec's closed vector
   semantics: `globalBase`, sorted `localBases`, explicit `include` dots, and
   rejected-transaction filtering. This should be the next pressure test because
   branches, sync, reconnect, and read sets all depend on exactly the same
   visibility relation.

2. Two-node authority acceptance and fate propagation in the deterministic
   harness.
   Create local transactions on an "alice" node, forward them to an authority,
   assign global epochs, broadcast the mapping, and assert that `$txId` remains
   stable while compact coordinates become available. Include a rejected
   transaction case so current projections and snapshots learn to remove or
   ignore rejected local effects.

3. Branch metadata and branch-local reads/writes.
   Add `jazz_branch`/`jazz_branch_history`, create a branch from a closed vector,
   write rows on that branch, and prove those globally accepted rows stay hidden
   from `main` until a metadata-only merge updates the target branch head. This
   is the smallest branch slice that tests the core "global history is not main
   visibility" rule.

4. Two-table joins/includes with result dependency scope.
   Add a second hard-coded table, likely `projects`, and lower a realistic
   todos-with-project query. Capture both result locators and dependency
   locators, then make subscription diffs and sync-scope output prove that a
   reproduced result has every row version it needs.

5. Multi-write transactions plus per-column merge candidates.
   Let one sealed transaction touch multiple rows/tables, record row+column
   write sets, and run a small concurrent update scenario where `title` and
   `done` can merge independently. Store resolved current values with ordered
   conflict candidate tx ids so byte-for-byte projection rebuilds start covering
   the merge contract instead of only last-write projection.

Uncertainties to settle while implementing:

- Whether decoded snapshot vectors should be represented as temp tables first
  or compiled directly into generated predicates. Temp tables look slower but
  will make the visibility contract easier to test and reuse.
- What canonical ordering to use for mixed include dots before global mappings
  exist. `$txId`-only includes may be the most stable prototype choice even if
  not the final compact form.
- How rejected local transactions should be undone in `main` current without
  overfitting to the one-row CRUD path. A projection rebuild after rejection is
  semantically clean; incremental repair can follow.
- Whether branch merge metadata should be represented before app-level branch
  rows. For this prototype, system metadata first is enough to test visibility,
  but it leaves the permission-anchor story unexercised.
- How much of read/write-set JSON needs to be durable in this pass. The useful
  minimum is exact previous visible row version for writes plus column masks;
  range reads can wait until exclusive validation is being modeled.

### 2026-05-24 22:26 PDT

Added a first storage `SnapshotVector`:

- `global_base`
- `local_bases`
- `include_tx_ids`

and `query_todos_at_snapshot(query, snapshot)`, which first resolves visible
transaction ids, then queries history rows for the latest visible row versions.

Discovery: txid includes are a very simple first representation. They avoid
local-to-global coordinate rewrite inside serialized vectors and can be made to
work before compact epoch encodings exist.

Bad smell: the first implementation loops once per visible tx and uses dynamic
`IN (?, ?, ...)` predicates. This is acceptable as an executable spec, but the
real implementation should likely materialize the resolved visible tx set into a
temporary table or use a generated CTE so SQLite can plan the whole snapshot
query at once.

Open fuzziness: ordering concurrent visible versions of the same row is not
solved. The prototype only suppresses older versions from the same node. If two
visible transactions from different nodes both write the same row, this should
become conflict-candidate output rather than "pick one latest row".

### 2026-05-24 22:27 PDT

Added the smallest branch metadata slice:

- `jazz_branch`
- `jazz_branch_history`
- `create_branch`
- `insert_todo_in_branch`

Validated that a branch-local data transaction can be globally accepted while
remaining isolated from `main`. This matches the important rule: global history
is not the same as visibility on every branch.

Major semantic gap: branch reads do not yet inherit rows from their base
branches. A branch created at `main@globalBase=1` should see main rows at that
base plus branch-local rows. The current spike only proves branch id isolation
for branch-local writes. To model real branch reads, a query probably needs a
branch visibility source like:

```text
(source_branch_id, snapshot_vector)
```

for each base/provenance component, plus the branch's own head vector.

### 2026-05-24 22:28 PDT

Made rejected local inserts repair `main` current by rebuilding the current
projection from non-rejected history after `reject_tx`.

Discovery: full projection rebuild is the cleanest first implementation for
rejection repair. It is obviously too broad for a hot path, but it keeps the
semantic invariant simple:

```text
main current = fold(non-rejected main history rows in deterministic order)
```

That invariant is more valuable right now than incremental cleverness. Later,
rejection repair can be narrowed to affected rows using write sets.

### 2026-05-24 22:29 PDT

Closed the first branch-read gap crudely with `query_todos_on_branch`:

- reads base rows from `main` at the branch's stored `head_global_epoch`
- reads branch-local rows from the branch at the requested global epoch
- lets branch-local rows shadow same-row base rows

Discovery: this shape makes branch provenance matter immediately. A branch head
is not just a single vector; it is a set of source branch/vector components plus
local branch history. The current implementation hard-codes "one main base plus
branch-local rows" in Rust. The real lowering probably needs a SQL-visible
branch source relation before joins, pagination, and sync scopes can be correct.

### 2026-05-24 22:30 PDT

Added first sync bundle shape:

- `export_tx(tx_id)`
- `import_tx(bundle)`
- bundle includes one `jazz_tx` row with stable `node_id/local_epoch`
- bundle includes associated `todos` history rows

Validated a client-to-authority path:

```text
alice local write -> export bundle -> authority import -> authority accept -> global snapshot read
```

Discovery: storing `node_num` as a local surrogate is fine as long as every sync
boundary exports stable `node_id` and rehydrates the local surrogate on import.
This mirrors the likely production shape: compact local integer keys inside one
SQLite database, stable string identities on the wire.

Open fuzziness: authority acceptance is still a direct mutation of the imported
`jazz_tx` row. If acceptance receipts become append-only, import/export needs to
carry both proposed local transaction state and authority fate events.

### 2026-05-24 22:31 PDT

Added authority fate propagation back to a client:

```text
client exports local tx -> authority imports/accepts -> authority exports same tx
-> client imports fate -> client global snapshot sees accepted tx
```

Discovery: `import_tx` cannot be insert-only. It has to upsert transaction fate
fields (`status`, `global_epoch`, rejection reason, etc.) while keeping the
stable local identity. This is where "local versions turn into global ones"
starts to become concrete: the row version's public `$txid` stays fixed, but
its coordinate metadata is enriched when the authority response arrives.

### 2026-05-24 22:32 PDT

Added remote rejection propagation:

```text
client optimistic write -> authority imports/rejects -> authority exports same tx
-> client imports fate -> client repairs main current
```

Discovery: import needs side effects, not just row insertion. If an incoming
bundle marks an existing transaction rejected, the recipient has to repair any
derived projections that may have included that optimistic transaction. The
prototype uses full `main` rebuild again; write-set-driven repair remains the
obvious optimization path.

### 2026-05-24 22:34 PDT

Added a first multi-write transaction:

- one `jazz_tx`
- two `todos` history rows
- one exported/imported bundle
- one authority acceptance

Discovery: the schema shape is genuinely transaction-shaped enough for this.
Export/import naturally carries multiple history rows under one transaction.
This also reinforces that write sets should be transaction-level metadata, not
row metadata; row history can stay simple as long as the transaction row
contains the durable read/write contract.

### 2026-05-24 22:35 PDT

Added a tiny conflict-candidate spike:

- two updates both declare `tx-base` as their read/write base
- current row stores one resolved value
- current row also stores multiple visible conflict candidate tx ids
- the later transaction's read set still records `tx-base`, not the optimistic
  current value it overwrote

Discovery: read sets are not the same thing as parent pointers, but they are
enough to recover the important concurrency fact here: both writes depended on
the same previous row version. That supports the "no explicit parents for now"
decision. A real merge layer still needs per-column candidate metadata rather
than one row-level `conflict_tx_ids_jsonb` array.

### 2026-05-24 22:42 PDT

Added the first joined query:

- second hard-coded table: `projects`
- project insert path with history/current projection
- todo insert path with `project_id`
- `query_open_todos_with_projects`
- scope output contains result todo locators and dependency project locators

Discovery: result scope and dependency scope should be distinct in the public
contract. The joined row's todo version is the result identity, while the
project version is necessary to reproduce the displayed result and to send a
complete sync payload. This makes the "visited rows" idea concrete without
SQLite exposing visited rows natively.

### 2026-05-24 22:44 PDT

Added joined subscription invalidation for dependency rows:

- project update writes project history/current
- joined subscription reruns `todos JOIN projects`
- project-only changes produce an updated joined result even when the todo row
  version is unchanged

Discovery: dependency rows need subscription semantics equal to result rows for
rerun+diff correctness. The diff can still be full-row semantic diff, but the
stored subscription state has to include the dependency payload, not just the
result table's row id and version.

### 2026-05-24 22:45 PDT

Added a temp-table snapshot query variant:

- resolves the vector to `temp_visible_tx`
- runs one SQL query against history joined to the temp visible set
- verifies it matches the direct/looping snapshot implementation

Discovery: this feels much closer to the shape we want for real lowering. It
lets SQLite plan the history query as one query and makes the resolved
visibility relation inspectable. The tradeoff is lifecycle management for temp
tables during nested reads/subscriptions, but that seems more tractable than
generating huge `IN (...)` predicates for large read sets.

### 2026-05-24 22:47 PDT

Extended transaction bundles to carry `projects` history as well as `todos`
history, and made import update main current projections for imported
non-rejected rows.

Validated a scoped join sync path:

```text
alice joined query -> scope has todo tx + project tx
-> bob imports both scoped bundles -> bob can reproduce joined query
```

Discovery: scope expansion immediately forces bundles to be table-polymorphic.
The first bundle shape was accidentally `todos`-centric; joins make that wrong.
The sync sender should expand locators into all relevant history rows by table
and schema, while the receiver updates enough projections to answer ordinary
current queries after import.

### 2026-05-24 22:48 PDT

Added required dependency deletion behavior for joins:

- project delete writes project history/current
- joined subscription reports the todo-with-project row as removed
- fresh joined query returns no row

Discovery: the "required child filters parent" rule has a clean SQL
interpretation for inner joins: dependency disappearance is just membership
disappearance. For optional relations we will need a left-join/nulling variant,
and the scope contract will need to distinguish "dependency absent" from "not
visited".

### 2026-05-24 22:51 PDT

Added optional dependency behavior for joins:

- optional project query uses `LEFT JOIN`
- deleted/missing project becomes `None`
- todo remains in the result
- scope includes the todo result locator but no project dependency locator

Discovery: optional dependencies need a distinct scope shape. If the joined row
contains `project = None`, there is no project version to send, but there may
still be a semantic dependency on the absence of a project. That absence is not
captured by row-version locators, so correctness for optional relations will
eventually need range/predicate scope, not just visited row scope.

### 2026-05-24 22:52 PDT

Added first predicate scope for optional dependency absence:

- optional project query can return `(rows, row_scope, predicate_scope)`
- missing project produces a predicate dependency like
  `{"rowId":"missing-project","isDeleted":0}`
- row scope still only includes concrete row versions

Discovery: this is the missing piece behind optional relations and policy
dependencies. Row-version locators describe what was found. Predicate scope
describes the absence or range condition that must remain true for the result to
be reproducible. This should probably become a first-class read-set/sync-scope
shape rather than being smuggled through JSON on a query result.

### 2026-05-24 22:54 PDT

Added branch-local shadowing:

- branch created from `main@globalBase=1`
- branch writes a row with the same row id as a base row
- branch read returns only the branch-local row

Discovery: even the crude branch reader needs source precedence. The current
Rust combiner hard-codes "branch-local shadows base". A real SQL lowering
probably wants a source relation with `(source_branch_id, vector, precedence)`
so shadowing works uniformly for joins, pagination, and sync scope.

### 2026-05-24 22:54 PDT

Extended rejection repair to `projects`:

- `reject_tx` now rebuilds both todo and project current projections
- imported rejection fate also rebuilds both projections
- rejected project inserts disappear from required joined queries
- optional joined queries preserve the todo and emit absence predicate scope

Discovery: projection repair is table-polymorphic for the same reason sync
bundles are table-polymorphic. The first "rebuild main current" helper was
accidentally todo-specific. Once joins exist, every table participating in
derived results needs the same rejection/durability repair contract.

### 2026-05-24 22:56 PDT

Added model-level read-set validation for authority acceptance:

- `ReadSet::validate_against(AuthorityReadState)`
- row reads compare expected visible tx id to authority current state
- stale row reads produce a rejectable `ReadValidationError::StaleRow`
- range validation is explicitly unsupported for now

Discovery: this reinforces the "no explicit parents yet" direction. For the
exclusive/global-consistent path, a precise enough read set can tell the
authority whether the transaction was based on stale data. Parent pointers may
still be useful for graph traversal or debugging, but the correctness check can
start from read sets.

### 2026-05-24 22:57 PDT

Connected read-set validation to storage acceptance for `todos`:

- accepted base row establishes latest global visible version
- first update based on that version accepts
- second concurrent update with the same stale base rejects
- rejection repairs current back to the accepted update

Discovery: this is the first concrete exclusive/global-consistent validation
path. The implementation uses a brittle prototype parser for read-set JSON, but
the semantic flow is promising: validate declared row bases against authority
visible state, then either assign a global epoch or reject with a reason.

### 2026-05-24 22:59 PDT

Added byte-for-byte rebuild coverage for `projects` current projection.

Discovery: once a second table exists, every projection invariant needs to be
table-polymorphic too. The project rebuild test caught no new bug, but it makes
the earlier rejection-repair broadening much less hand-wavy: both result tables
can now prove current is derivable from non-rejected history.

### 2026-05-24 23:00 PDT

Added first branch data merge:

- branch-local todo row stays invisible on `main`
- merge transaction copies visible branch rows into `main`
- main current then exposes the merge transaction as the visible version

Discovery: this is intentionally a data-merge spike, not the final
metadata-only merge path. It shows the visibility boundary clearly: accepted
branch history is global history but still not main visibility until an explicit
merge transaction writes or exposes it on main.

### 2026-05-24 23:02 PDT

Added a tiny read-set codec module:

- typed `EncodedRowRead`
- `encode_row_read`
- `decode_first_row_read`
- storage write paths use the encoder
- authority validation uses the decoder instead of local substring parsing

Discovery: still not a real JSON codec, but moving the encoding boundary into
one module immediately improves the prototype. The storage layer can now depend
on a semantic read-set shape rather than scattered string templates. This should
be replaced by `serde_json`/SQLite JSONB-aware encoding once the shape settles.

### 2026-05-24 23:03 PDT

Added append-only transaction fate receipts:

- new `jazz_tx_fate`
- `accept_tx` records an `accepted` receipt
- `reject_tx` records a `rejected` receipt
- `jazz_tx` still carries denormalized current fate for query convenience

Discovery: the dual shape feels right for a prototype: append-only fate receipts
preserve the authority's decisions, while denormalized status/global epoch keeps
visibility queries simple. The next step would be exporting/importing fate
receipts explicitly instead of relying only on the mutated `jazz_tx` row.

### 2026-05-24 23:04 PDT

Extended transaction bundles to carry fate receipts:

- `TxBundle` now includes `jazz_tx_fate` rows
- import inserts fate receipts idempotently
- client acceptance/rejection propagation tests assert both current tx state and
  the received fate log

Discovery: this makes authority decisions feel like first-class sync data
rather than incidental column mutations. The denormalized `jazz_tx.status` still
drives query speed, but receipts are now available for audit, replay, and
eventual append-only authority semantics.

### 2026-05-24 23:05 PDT

Added a top-N joined query ordered by dependency data:

- query open todos joined to projects
- order by project name
- limit to first two rows
- rename a third project's name so it crosses into the first page

Discovery: pagination makes dependency invalidation nastier than simple joined
rerun+diff. A dependency row can change page membership without changing the
result row itself. Page subscriptions need to retain enough boundary scope to
know when off-page rows can enter the page.

### 2026-05-24 23:08 PDT

Added first `EXPLAIN QUERY PLAN` hook for the paginated joined query and an
index on `(branch_id, name, row_id)` for project current rows.

Discovery: this is only a smoke test so far; it proves the prototype can expose
planner evidence in tests, not that the plan is good. The next useful step is
to make plan assertions or measurements meaningful for realistic row counts and
page boundaries.

### 2026-05-24 23:08 PDT

Added a tiny snapshot-query ballpark test:

- 2,000 inserted rows
- 1,333 rows match `open_since(0)`
- current projection query: about 2.4 ms in this debug test run
- temp-table snapshot query: about 603 ms in this debug test run

Discovery: the naive temp-table snapshot query is semantically helpful but not
yet performant. The `NOT EXISTS` latest-visible-version shape is likely doing
too much work per candidate row. This is the clearest performance warning so
far: pure-query snapshots may be acceptable for small branch reads, but the
lowering needs serious planner attention before we rely on it for hot paths.

### 2026-05-24 23:09 PDT

Rewrote the temp-table snapshot query from correlated `NOT EXISTS` to a grouped
latest-visible-version CTE.

Same debug ballpark test:

- 2,000 inserted rows
- 1,333 matching open rows
- current projection query: about 2.3 ms
- temp-table snapshot query: about 17.2 ms
- previous temp-table snapshot shape: about 603 ms

Discovery: this is the first strong performance-positive result of the night.
The visibility relation can be SQL-shaped in ways the planner handles much
better. Pure-query snapshots are still slower than current projections, but the
difference between "obviously too slow" and "plausibly acceptable for cold
branches/snapshots" may be mostly query shape, not the whole SQLite approach.

### 2026-05-24 23:12 PDT

Added conflict metadata for joined dependency rows:

- projects now carry row-level conflict candidate tx ids
- concurrent project name updates from the same base produce multiple
  candidates
- joined todo/project query can expose resolved project value plus dependency
  conflict metadata

Discovery: joined conflict exposure should probably live on the nested
dependency object, not just the top-level result row. A todo can be perfectly
unconflicted while its displayed project is conflicted, and sync/listener
semantics need to preserve that distinction.

### 2026-05-24 23:15 PDT

Added a tiny current-projection registry:

- `CURRENT_PROJECTIONS` lists known main-branch current projections
- rejection and rejected-bundle import repair call
  `rebuild_all_current_projections`
- a regression test deletes both todo and project current rows, then proves one
  registry rebuild restores both ordinary current reads and joined reads

Discovery: this keeps the prototype honest as tables multiply. The registry is
still hard-coded, but it moves repair coverage from "remember every table at
every fate transition" to "register every current projection once." A real
schema lowerer could generate this list per schema version.

### 2026-05-24 23:16 PDT

Added model-level absence/range validation:

- range read sets can express a predicate such as
  `projects.rowId = X AND isDeleted = false`
- authority state can test whether any currently visible row now matches
- a matching row turns the read set into a `StaleRange` rejection reason

Discovery: this is enough to model the optional-join absence hazard without
inventing explicit parent pointers. The first implementation only understands a
tiny equality predicate over `rowId` and `isDeleted`, but the shape is useful:
predicate/range read sets are the correctness contract for "I observed that no
required row existed."

### 2026-05-24 23:18 PDT

Pushed absence/range validation through storage acceptance:

- added a tiny absence-read codec shape
- `accept_todo_tx_validating_reads` now checks whether a previously absent
  project is visible in `projects__schema_v1_current`
- if it is visible, the transaction is rejected with `stale_range` and current
  projections are repaired

Discovery: this makes predicate scope more than sync bookkeeping. The same
shape can become an authority-side acceptance guard for optional joins, policy
dependencies, and uniqueness-like checks. The prototype is still table-specific
and only recognizes project absence, but it proves where the validation hook
lives.

### 2026-05-24 23:20 PDT

Added a first query-scope bundle:

- row scope expands to normal transaction bundles
- predicate scope is carried alongside row bundles
- optional missing-project results can be synced to another store and still
  reproduce both `project = None` and the absence predicate scope

Discovery: predicate scope does not necessarily need a row bundle to be useful.
For the first protocol shape, it can ride alongside concrete history bundles as
declarative evidence/revalidation material. The receiver can reproduce the
result from row bundles, then retain or recompute the same predicate scope.

### 2026-05-24 23:21 PDT

Added a SQL-visible branch source relation spike:

- branch reads can materialize `temp_branch_todo_sources`
- the query uses source branch, source global epoch, and precedence in SQL
- branch-local rows shadow base rows without Rust post-combining

Discovery: this is the right direction for branch provenance. A source relation
makes branch reads inspectable and gives joins/pagination something concrete to
join against. The current version is still todos-only and uses a temp table, but
it removes one of the biggest branch-query hand waves.

### 2026-05-24 23:23 PDT

Added a top-N joined subscription variant:

- subscription state can be backed by `query_top_open_todos_by_project_name`
- project sort-key changes that move an off-page todo onto the page produce an
  added row and a removed row
- the result matches fresh query rerun semantics

Discovery: simple rerun+diff can produce the correct semantic diff for page
membership churn, but it does not yet explain why the page was invalidated or
which off-page boundary rows were watched. This keeps the correctness story
alive while pointing at the next performance/scope problem: page boundary
dependencies.

### 2026-05-24 23:24 PDT

Added explicit page-boundary scope for top-N joined queries:

- top joined pages now expose concrete todo/project row locators
- they also expose a boundary predicate like
  `done=false AND projectName <= lastVisibleProjectName LIMIT N`

Discovery: this is a useful but incomplete scope shape. It describes the
current page boundary well enough to reproduce/debug the page, but by itself it
does not catch a row whose old sort key was outside the boundary and whose new
sort key moves inside it. Efficient subscriptions probably need either index
maintenance that can invalidate on old/new sort keys or a broader ordered-index
watch primitive.

### 2026-05-24 23:25 PDT

Added the smallest old/new sort-key invalidation primitive:

- a top page boundary is crossed if either the old or new project name is inside
  the watched boundary
- this catches both rows leaving the page and rows entering from outside

Discovery: ordered-index subscriptions likely want change records that expose
both old and new index keys. A static predicate scope over the current page is
not enough by itself; the invalidator needs to classify movement across that
boundary.

## Next pressure points after joins

Once two-table joins/includes and explicit result scope are green, the next
risks are less about proving that SQLite can join rows and more about proving
that the joined answer is reproducible, subscribable, and syncable.

1. Scope representation must become a first-class output contract.
   The spec sketches `$resultScopeJson`/`$policyScopeJson`, but JSON hidden
   columns may be the wrong durable shape. Try three tiny variants for the same
   todos-with-project query: JSON column, second SQL result/side table, and Rust
   side-channel collection from projected locators. Judge by deterministic
   ordering, duplicate elimination, and how naturally the sender expands scope
   into history bundles.

2. Dependency rows need separate invalidation semantics from result rows.
   A project rename can update a joined todo result without changing todo
   membership; a project delete or authorization failure can remove it. Add a
   subscription test where only the joined project changes, then one where the
   todo's `project_id` changes, and require rerun+diff to report the same final
   answer as a fresh query.

3. Branch provenance will stress joins harder than single-table reads.
   The current branch query hard-codes one main base plus branch-local rows.
   A joined query should prove that parent and dependency rows are read from the
   same effective branch source set, including branch-local shadowing on either
   side of the join. If this gets awkward, materialize a SQL-visible
   `(source_branch_id, snapshot_vector, precedence)` relation before adding more
   relation features.

4. Scope must survive durability transitions and rejection repair.
   A joined result may include an optimistic todo and an accepted project, or
   vice versa. Exercise authority acceptance and rejection after a scoped join
   subscription is established, and assert that scope locators move from local
   coordinates to stable tx ids/global metadata without leaving stale dependency
   rows in the subscription state.

5. Policy dependencies should not be folded into result dependencies.
   The first join implementation will be tempting to reuse for inherited read
   policies. Keep a separate policy-scope experiment: Alice can read todos only
   through project ownership, then Bob changes ownership. The expected output is
   not just row membership; it is a changed authorization explanation and sync
   payload.

6. Pagination/order-by over joined dependencies is the first performance cliff.
   Current v0 can rerun SQL and diff full rows, but page scope is unstable when
   dependency rows change sort keys or filters. Add a top-N joined query and
   compare `EXPLAIN QUERY PLAN` before introducing generic lowering. The useful
   experiment is whether existing current-table indexes are enough or whether
   relation-specific serving indexes appear immediately.

7. Conflict candidates need an app-facing read shape before sync bakes it in.
   A joined row whose parent or dependency has multiple visible candidates
   should not silently pick one value forever. Before adding more sync protocol
   surface, create one test with concurrent project-name updates and decide
   whether query results expose conflict metadata, resolved values only, or both
   plus scope entries for every candidate tx.

## Semantic gaps spotted at 23:00

Concrete next experiments after the first speedrun:

1. Replace ad-hoc read/write-set JSON parsing with a tiny typed codec.
   The authority validation path is semantically important now, but it depends
   on brittle string extraction. Keep the schema JSON-shaped, add typed encode /
   decode helpers, then rerun the stale-read acceptance tests through the codec.

2. Validate range and absence reads at authority acceptance.
   Optional joins now emit predicate scope for missing dependencies, but
   `ReadSet::validate_against` rejects all ranges as unsupported. Add one
   transaction that depends on "project row absent" and prove a concurrent
   project insert rejects it.

3. Make branch source provenance SQL-visible.
   `query_todos_on_branch` still combines main-base and branch-local rows in
   Rust. Materialize a temporary source relation with branch id, vector, and
   precedence, then use it for one todo/project joined query with branch-local
   shadowing on both sides.

4. Exercise conflict candidates across joins, not just single todo rows.
   Create concurrent project name updates from the same base, join todos to the
   project, and decide whether the joined result exposes candidate tx ids on the
   dependency, the projected nested object, or both.

5. Prove scoped sync for optional absence.
   Current scoped sync expands concrete row locators into bundles, but an
   optional missing project has no row bundle to send. Add a sender/receiver
   scenario where Bob can reproduce `project = None` only if predicate scope is
   transmitted or intentionally revalidated.

6. Turn projection rebuild into a table registry experiment.
   Rejection and import repair now manually rebuild todos and projects. Add a
   tiny registry/list of projection rebuilders and require all fate transitions
   to call through it, so the next hard-coded table cannot silently skip repair.

7. Split durable transaction state from authority fate receipts.
   `accept_tx` and `reject_tx` mutate `jazz_tx` directly. Try an append-only
   `jazz_tx_fate` table plus a current fate view/projection, then export/import
   both proposal and fate events for the same stable tx id.

8. Add a top-N joined subscription with dependency sort-key churn.
   Required and optional joins rerun correctly, but pagination/order stability
   is still untouched. Query the first N open todos ordered by project name,
   rename a project across the page boundary, and compare fresh query output to
   subscription diff output.

## SQL-visible branch provenance at 23:21

Added a bounded todo-only spike for a temporary `temp_branch_todo_sources`
relation with `(source_branch_id, source_global_epoch, precedence)`.

What it showed:

1. The branch source stack can be exposed to SQLite directly. A single SQL query
   can read accepted history from each source branch at its own epoch boundary,
   then use `ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY precedence)` to let
   branch-local rows shadow base rows.

2. This matches the current Rust-combined branch query for the tested shape:
   main-base todo rows plus a branch-local row with the same `row_id`.

3. Scope can carry precise provenance naturally from SQL. The shadowed row has
   `branch_id = draft`; the untouched base row has `branch_id = main`.

Open follow-up:

- Generalize the temporary source relation from todo-only branch reads to joined
  todo/project reads, so both sides of a join share exactly the same branch
  source stack.
