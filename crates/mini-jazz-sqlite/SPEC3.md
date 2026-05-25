# SQLite Jazz Core SPEC3

Status: draft for Attempt 3.

Date: 2026-05-25.

Audience: database engineers and systems engineers who have not seen current
Jazz internals.

## 1. Summary

Jazz should be rebuilt as a local-first relational core on top of SQLite.

SQLite should provide:

- local ACID transactions
- durable storage
- B-tree indexes
- query planning
- joins, sorting, limits, and ordinary relational execution

Jazz should provide:

- stable public transaction and row identities
- local optimistic transactions
- authority-assigned global transaction fate
- append-only history
- current projections for hot main-branch reads
- query-scoped sync
- subscriptions and semantic diffs
- branch/source visibility
- historical snapshots
- conflict candidates and resolution metadata
- policy scope and validation
- schema-version lenses

Attempt 3 should start fresh rather than continue the Attempt 2 runtime. Attempt
2 produced valuable executable research, but its implementation grew around
text physical ids, string enums, and a central store coordinator. SPEC3 bakes in
the research results from the start.

The core idea:

```text
public API/protocol identities stay stable and readable
hot SQLite tables use compact physical ids and integer enums
history is source of truth
main current projection is a serving index
branch/snapshot reads are query-only correctness baselines
sync sends history/fate/scope, not result payloads
imports emit listener effects from projection diffs
```

## 2. Goals

SPEC3 is a design for the next executable attempt, not a final product spec.

The attempt should get close enough to the real system to pressure the main
architectural decisions:

- physical storage layout
- query lowering
- projection materialization
- transaction fate
- sync bundle shape
- branch/source visibility
- subscriptions
- row-granular conflict semantics
- read/write-set encoding
- planner-aware performance

The attempt should not try to reach every production feature.

It should preserve the high-level Jazz product shape, but it may break all
prototype internals and all Attempt 2 APIs.

## 3. Non-Goals For Attempt 3

Attempt 3 does not need to implement:

- browser/WASM SQLite packaging
- real networking
- production authentication
- full policy language
- schema lenses beyond one narrow spike, if any
- arbitrary multi-base branch merge UI
- compact reconnect protocol
- custom SQLite VFS/page compression
- final TypeScript DSL bindings

It should leave clean surfaces for those features.

## 4. Evidence From Attempt 2

Attempt 2 validated:

- schema-driven history/current tables
- one write call creates one sealed transaction
- current projections can rebuild deterministically from history plus fate
- required/optional includes lower naturally to SQL joins
- rerun-and-diff subscriptions are semantically correct
- query scope must include result rows, dependencies, predicates, and branch
  context
- query-scoped sync can reproduce results by importing history/fate
- authority accept/reject can mutate fate on the same public transaction id
- row read-set validation catches stale exclusive writes
- branch reads can be query-only overlays over a pinned main base
- branch sync needs explicit branch identity in scope
- import effects should be projection-diff-driven
- conflict metadata needs row semantics first, with column metadata as
  auxiliary information

Attempt 2 contradicted earlier assumptions:

- hot physical storage should not be stringly
- text enum labels should not be stored in hot rows
- fully indexed read/write-set side tables are not the default
- branch provenance cannot be inferred from visible row tx ids
- history deltas and listener effects are distinct facts

Performance experiments found:

- compact internal ids can dramatically reduce disk and peak memory
- `WITHOUT ROWID` is useful for composite-primary-key system tables
- index shape can dominate snapshot-read performance
- current projection reads are close to raw data with matching indexes
- compression has theoretical disk upside, but compact layout is lower risk
- row/tx public string ids should be kept at API/protocol edges, not repeated
  in every hot internal key

## 5. Terminology

**Node**

A local writer identity such as a device, process, or authority participant.
Nodes assign local epochs.

**Transaction**

The only write unit. A transaction is sealed when a write call completes.

**Fate**

Authority-observed status for a transaction: pending, accepted, rejected, and
possibly edge-tier states. Fate mutates on the transaction row in v0.

**History Row**

An append-only row version written by a transaction.

**Current Projection**

A serving table containing the currently visible main-branch row version per
logical row.

**Scope**

The set of rows, predicates, branch/source context, policy dependencies, and
other facts needed to reproduce or validate a query.

**Bundle**

The sync payload derived from scope. It contains transactions, fate,
branch/source metadata, predicate/range facts, and history rows.

**Public Id**

Stable identity visible across sync/API boundaries.

**Physical Id**

Compact SQLite-local integer identity used in hot tables and indexes.

## 6. Physical Storage Principles

Physical storage is not the public data model.

Public identities remain stable strings at API/protocol boundaries. Hot SQLite
tables use local integer surrogate ids.

Normative decisions for Attempt 3:

- Use integer enum discriminants for physical enum fields.
- Do not store text enum labels in hot tables.
- Use `WITHOUT ROWID` for composite-primary-key system tables unless a benchmark
  proves otherwise.
- Keep current projection for main branch only by default.
- Keep history append-only.
- Store public ids at API/protocol boundaries and in mapping tables.
- Generate covering and partial indexes from query/schema intent.
- Do not introduce custom page compression in Attempt 3.

Decision: Attempt 3 uses local integer surrogates for hot SQLite storage.

This applies to:

- nodes: `node_num`
- transactions: `tx_num`
- rows: `row_num`
- branches: `branch_num`
- tables/schemas/columns where they appear repeatedly in hot metadata

Stable public ids remain strings and are stored once at the boundary. Bundles
export public ids. Import hydrates public ids into local integer surrogates.

Fixed-width BLOB ids were promising in experiments, but they require committing
to a canonical binary public-id format. They are not the Attempt 3 baseline.

## 7. Physical Id Model

Every public id that appears in hot tables has a local integer surrogate.

Candidate surrogate tables:

```sql
CREATE TABLE jazz_node_id (
  node_num INTEGER PRIMARY KEY,
  node_id TEXT NOT NULL UNIQUE
);

CREATE TABLE jazz_tx_id (
  tx_num INTEGER PRIMARY KEY,
  tx_id TEXT NOT NULL UNIQUE
);

CREATE TABLE jazz_row_id (
  row_num INTEGER PRIMARY KEY,
  table_num INTEGER NOT NULL,
  row_id TEXT NOT NULL UNIQUE
);

CREATE TABLE jazz_branch_id (
  branch_num INTEGER PRIMARY KEY,
  branch_id TEXT NOT NULL UNIQUE
);
```

The codec must be centralized.

Required operations:

- hydrate public id to local integer id
- decode local integer id to public id for export/debugging
- hydrate public id on bundle import
- allocate local surrogate on first sight
- preserve stable public identity across local-to-global mapping

Risks:

- mapping tables increase insert cost and public-id lookup cost
- physical ids must not leak into public API semantics

## 8. Integer Enum Model

All hot physical enum fields use stable integers.

Examples:

```text
tx_kind:
  1 data
  2 branch_metadata
  3 schema_metadata
  4 permission_metadata

tx_status:
  1 local_pending
  2 edge_durable
  3 global_durable_accepted
  4 rejected

row_op:
  1 insert
  2 update
  3 delete
```

The discriminants are part of the durable format. They should be documented and
never silently reused.

Debug/protocol/API layers may map integers to names.

Risks:

- integer enums reduce inspectability in raw SQLite consoles
- migrations must preserve old discriminants
- accidental discriminant drift is severe

Mitigations:

- keep one checked enum codec
- include debug views or helper queries
- test roundtrips and unknown discriminant handling

## 9. Core System Tables

The exact table names are not final. The physical shape uses local integer
surrogates in hot paths and stores public ids at boundary tables.

```sql
CREATE TABLE jazz_node (
  node_num INTEGER PRIMARY KEY,
  node_id TEXT NOT NULL UNIQUE
);

CREATE TABLE jazz_tx (
  tx_num INTEGER PRIMARY KEY,
  tx_id TEXT NOT NULL UNIQUE,
  node_num INTEGER NOT NULL,
  local_epoch INTEGER NOT NULL,
  global_epoch INTEGER,
  kind INTEGER NOT NULL,
  status INTEGER NOT NULL,
  rejection_reason_json TEXT,
  created_at INTEGER NOT NULL,
  metadata_blob BLOB NOT NULL,
  UNIQUE (node_num, local_epoch),
  UNIQUE (global_epoch)
);

CREATE INDEX jazz_tx_status_global_epoch
  ON jazz_tx(status, global_epoch, tx_num);

CREATE TABLE jazz_branch (
  branch_num INTEGER PRIMARY KEY,
  branch_id TEXT NOT NULL UNIQUE,
  head_global_epoch INTEGER NOT NULL,
  head_vector_blob BLOB NOT NULL,
  precise_provenance_blob BLOB NOT NULL,
  flattened_sources_blob BLOB NOT NULL
);
```

Attempt 3 may keep `tx_id TEXT UNIQUE` in `jazz_tx` even when hot history tables
use `tx_num`, because `jazz_tx` is a boundary between public identity and hot
storage.

Open:

- whether `metadata_blob` is JSON text, SQLite JSONB, postcard/bincode-like
  bytes, or a custom canonical encoding
- exactly which public id mappings are embedded in system tables versus split
  into dedicated mapping tables
- whether branch source flattening should be stored as BLOB, normalized rows,
  or both

## 10. User Table Layout

Every structural schema version has its own physical history and current table
shape.

Example with integer physical ids:

```sql
CREATE TABLE todos_v1_history (
  row_num INTEGER NOT NULL,
  branch_num INTEGER NOT NULL,
  tx_num INTEGER NOT NULL,
  op INTEGER NOT NULL,

  title TEXT,
  done INTEGER,

  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  conflict_blob BLOB,
  edit_metadata_blob BLOB,

  PRIMARY KEY (row_num, branch_num, tx_num)
) WITHOUT ROWID;

CREATE TABLE todos_v1_current (
  row_num INTEGER NOT NULL,
  branch_num INTEGER NOT NULL,
  visible_tx_num INTEGER NOT NULL,
  is_deleted INTEGER NOT NULL,

  title TEXT,
  done INTEGER,

  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  conflict_blob BLOB,
  edit_metadata_blob BLOB,

  PRIMARY KEY (row_num, branch_num)
) WITHOUT ROWID;
```

History rows must carry every projection-affecting value needed for byte-stable
rebuilds:

- operation/delete state
- user columns
- immutable creation metadata
- updated metadata
- conflict metadata or explicit cleared conflict state
- edit metadata needed by API/sync

Current projection is derived. History plus transaction fate is source of
truth.

Attempt 3 should make current projection rebuild a generated table plan, not a
hand-coded method.

Risks:

- storing too much metadata per row inflates history
- storing too little breaks deterministic projection rebuild
- conflict metadata may be sparse; nullable/omitted metadata should be tested

## 11. Index Strategy

Indexes are part of the design, not an afterthought.

Baseline generated indexes:

```sql
CREATE INDEX todos_v1_current_done_created
  ON todos_v1_current(branch_num, done, created_at DESC, row_num);

CREATE INDEX todos_v1_history_visible_order
  ON todos_v1_history(branch_num, done, created_at DESC, row_num, tx_num);

CREATE INDEX todos_v1_history_row_tx
  ON todos_v1_history(branch_num, row_num, tx_num);
```

Schema/query declarations should be lowerable to covering and partial indexes.

Example:

```ts
todos.indexOnly(["done", "$createdAt"]);
```

May lower to:

```sql
CREATE INDEX todos_current_open_created
  ON todos_v1_current(created_at DESC, row_num, title)
  WHERE branch_num = 1 AND done = 0;
```

This is not a final syntax recommendation; it is a physical capability that the
planner should be able to choose.

Attempt 2 showed:

- covering current indexes can materially speed current page queries
- covering history indexes can materially speed snapshot queries
- partial current indexes can be especially good for common predicates
- generic system indexes alone are insufficient for performance claims

Attempt 3 should capture `EXPLAIN QUERY PLAN` output for risky lowerings and
keep query-plan assertions in perf tests.

Risks:

- over-indexing can dominate write cost and disk
- partial indexes are query-shape-specific
- generated indexes must stay compatible with schema lenses

## 12. Transactions And Fate

One write call creates one sealed transaction.

Transaction identity:

- public `tx_id`
- writer `node_num`
- `local_epoch`
- optional authority `global_epoch`

Fate mutates on the transaction row in Attempt 3:

```text
local_pending -> global_durable_accepted
local_pending -> rejected
local_pending -> edge_durable -> global_durable_accepted
edge_durable -> rejected
```

The direct `local_pending -> global_durable_accepted` path is allowed.

Rejections:

- keep history rows
- store machine-readable rejection reason
- are filtered out by visibility
- repair projections through projection-diff effects

Importing authority fate enriches or rejects an existing transaction identity.
It does not create a new public transaction id and does not rename references.

Open:

- whether append-only fate receipts are needed later for audit/debug/handoff
- exact protocol shape for authority observations
- edge-tier semantics

## 13. Write Path

The write path should be a verb over layout and write plans:

```text
allocate tx
begin SQLite transaction
append jazz_tx
append history rows
materialize/update current projection for affected main rows
record typed read/write set
record touched effects
commit SQLite transaction
publish local effects
```

For updates/deletes, the write path must read the previous visible row version
and record it in the read set as `write_base`.

The current projection may be updated incrementally for local main writes.
Projection rebuild must still be able to reproduce the same bytes from history.

Open:

- whether local branch writes update a branch current projection if one exists
- whether Attempt 3 should implement only main current incrementally and rebuild
  everything else
- exact allocation strategy for physical row ids

## 14. Read And Write Sets

Read/write sets should be typed in memory from the start.

Durable encoding starts inline on `jazz_tx.metadata_blob` unless a hot path
justifies a side index.

Read-set entry kinds:

```text
row
absence
range
policy
page_boundary
```

Required read entry fields:

```text
kind
table/schema id
branch/source context
predicate or row id
visible tx/version observed
reason
```

Write-set entry fields:

```text
table/schema id
row id
op
column mask metadata
```

The write conflict item for exclusive/global transactions is the row.

Column masks are auxiliary:

- mergeable transaction merging
- subscription invalidation
- conflict UI
- policy/error explanation

Column masks do not by themselves prove two same-row exclusive writes are safe.

Open:

- exact canonical binary/JSONB encoding
- how large read sets are chunked or summarized
- whether hot authority validation needs narrow side indexes
- how predicate/range reads map to SQLite index spans

## 15. Authority Validation

Exclusive transactions are validated by the authority before global acceptance.

Validation checks:

- every row read still observes the same authority-visible tx/version
- every absence read is still absent
- every range read is still valid
- every policy dependency still authorizes the operation
- constraints are still true

If validation fails, the authority rejects the transaction with structured
reason JSON.

Attempt 2 proved row read validation with stale update rejection. Attempt 3
should extend validation to at least one predicate/absence read.

Validation should read authority history/visibility, not optimistic current
projection polluted by imported pending proposals.

Open:

- validation performance for large read sets
- policy validation
- uniqueness-like constraints
- mergeable transaction acceptance rules

## 16. Visibility Relations

Attempt 3 should introduce a visibility relation early.

A visibility relation is the SQL-usable representation of "which transactions
are visible for this read."

For simple main current reads:

```text
current projection, branch_num = main
```

For global snapshot reads:

```text
accepted tx where global_epoch <= requested_epoch
```

For branch reads:

```text
source stack:
  branch local overlay
  main/base sources at pinned epochs
```

Attempt 2 implemented separate SQL paths for current, snapshot, and branch
reads. Attempt 3 should converge them around a reusable `VisibilityPlan` or
`VisibleRowsPlan`.

Possible lowering shapes:

- generated CTEs
- temp visibility/source tables
- query fragments with ordered bindings

The planner must own SQL text and bind parameters together to avoid the
parameter-order coupling discovered in Attempt 2.

Open:

- temp tables vs CTEs vs generated predicates
- vector decoding into visibility relations
- multi-base branch source precedence
- window functions vs grouped CTE vs `NOT EXISTS`

## 17. Queries

Queries lower to SQL plus decoders plus scope plans.

Attempt 3 public-ish Rust test API can remain simple:

```rust
query("todos")
    .filter(eq("done", false))
    .include_required("project", "project_id")
    .order_by("$createdAt", Desc)
    .limit(20)
```

But query plans should be data artifacts:

```text
SQL
bindings
row decoder
include decoder
scope collector
predicate/range scope
visibility/source plan
expected indexes
```

Query results include:

- decoded semantic rows
- result row locators
- dependency locators
- predicate/range facts
- policy facts
- branch/source context
- page boundary facts

Normal application code sees rows. Sync/subscription/authority paths see scope.

Open:

- exact API for relation inference so callers need not pass foreign-key column
  names forever
- typed non-string row value access
- duplicate scope explanation format
- policy scope collection

## 18. Includes

Required includes lower to inner joins. Missing dependency filters out parent.

Optional includes lower to left joins. Missing dependency keeps parent and
returns `null`/absent include.

Optional missing includes must produce absence scope.

Joined dependency payloads must be stored in previous subscription rows. A
dependency-only update can change the semantic result row.

Open:

- historical/branch optional include tests
- relation inference from schema metadata
- include pagination and nested includes
- authorization failure semantics for required vs optional includes

## 19. Scope And Sync Bundles

Sync is query-scoped.

The upstream executes a query and exports enough history/fate/scope for the
receiver to reproduce the query locally.

Scope categories:

- result rows
- include dependency rows
- policy dependency rows
- predicate/range/absence facts
- page boundary facts
- branch/source context

Bundle categories:

- transaction records/fate
- branch/source records
- predicate/range facts
- history rows
- schema/lens metadata when needed

Scope locators and bundles have different cardinality. Locators may repeat for
explanation; bundles dedupe concrete transactions and history rows.

Branch query scope must carry explicit branch identity even if every visible
row came from main-base history.

Predicate scope should ride alongside bundles even when it does not correspond
to any row bundle.

Attempt 3 should preserve these Attempt 2 regression scenarios:

- joined query scope reproduces result
- optional missing include reproduces null include
- row entering filter syncs
- row leaving filter syncs
- branch local rows sync
- branch base-only rows sync to a receiver that lacks the base
- duplicate import is idempotent for subscriptions

Open:

- compact predicate/range closure
- canonical wire encoding
- reconnect summary
- schema/lens scope

## 20. Import And Projection Effects

Import is semantic, not insert-only.

Import flow:

```text
hydrate public ids to physical ids
upsert tx/fate
upsert branch/source metadata
insert missing history rows
repair/materialize affected projections
diff affected visible projections
emit listener effects from projection deltas
```

Listener effects should be based on projection deltas, not raw history deltas.

This distinction matters:

- importing old hidden history may not change visible rows
- importing a rejection may remove an optimistic projection
- importing accepted fate may or may not change visible rows depending on local
  pending candidates

Open:

- efficient affected-row discovery
- projection diff representation
- durable effect log vs in-memory effect log
- branch projection effects if hot branch projections are added

## 21. Subscriptions

Baseline subscriptions rerun SQL and diff full semantic rows.

Subscription state:

- query AST/plan
- previous ordered rows with dependency payloads
- previous scope
- last seen effect sequence
- invalidation metadata

Invalidation starts coarse but correct:

- row effects overlap result/dependency rows
- predicate effects overlap predicate/range facts
- branch/source/fate effects overlap query context

Column masks are useful for invalidation precision. If a non-result row changes
only a column unrelated to the predicate/order/projection, a rerun can be
skipped.

Ordered page invalidation needs old/new order keys. Row-id cursors are not
enough internally.

Open:

- old/new index-key effect records
- durable subscription resume
- callback/async API
- page boundary scope format

## 22. Conflict Candidates And Resolution

Exclusive/global correctness uses row-granular write conflicts.

Conflict metadata should still explain column differences.

Current projection stores:

- resolved value
- conflict metadata, nullable/omitted when empty

Conflict metadata may include:

- candidate tx ids
- candidate values
- changed column masks
- causality/base information
- resolution metadata

Mergeable transactions may use column masks/field paths for automatic or
semi-automatic merge. Exclusive transactions should not treat disjoint columns
as automatically safe.

Conflict resolution is an ordinary transaction:

- reads conflicted row
- writes chosen value
- records resolved candidates
- clears conflict metadata

Open:

- candidate ordering
- per-column UI shape
- merge algorithms
- how multiple branch bases expose candidates

## 23. Branches

Branch visibility is source/provenance metadata, not database copying.

The simplest branch:

```text
draft = branch overlay over main@global_epoch
```

Attempt 3 should model branch reads through a source relation:

```text
source_branch
source_epoch/vector
precedence
```

Visible row selection:

```text
for each row id:
  choose highest-precedence source with a visible non-rejected row version
```

Branch scope must include branch/source context.

Branch metadata should preserve both:

- precise provenance for UI/debug/rebuild
- flattened effective source list for query execution

Open:

- precise provenance encoding
- multi-base conflict candidates
- metadata-only merge
- permissions on source changes
- hot branch projection heuristics
- joined branch queries over one shared source stack

Attempt 3 should implement:

- branch create from main epoch
- branch local write
- branch read pinned to base
- branch sync including base-only views

Attempt 3 may defer:

- multi-base merge
- branch permissions
- branch projections

## 24. Historical Snapshots And Version Vectors

Pure-query history reads are correctness baseline.

Attempt 3 should implement global-epoch snapshots early, but should not bake
global epochs into every visibility path.

The model should be ready for dotted version vectors:

```text
global base epoch
node local bases
included tx dots
```

No excludes in v0.

Rejected transactions are filtered by fate.

Open:

- exact vector encoding
- local coordinate upgrade after global acceptance
- compact reconnect summaries
- temp visibility relation performance

Attempt 3 should likely implement a minimal visibility relation even before
full vectors are done, so snapshot/branch/query paths share the same shape.

## 25. Policies

Policies must be SQL-lowerable in v0.

Policy scope is distinct from result/dependency scope. The same physical row
can appear for multiple reasons.

Attempt 3 should implement one narrow policy path if time allows:

- policy SQL lowering
- policy dependency scope
- authority validation
- rejection reason
- sync payload decision

Open:

- local vs authority policy evaluation
- recursive/inherited policies
- opaque proofs vs sending policy rows
- policy explanation API

## 26. Schema Versions And Lenses

Each structural schema version has its own physical table shape.

Lenses must be SQL-lowerable at first.

Attempt 3 probably should not attempt full schema lenses unless the core spine
stabilizes early.

If included, implement one rename lens:

```text
todos.title -> tasks.text
todos.done  -> tasks.completed
```

Open:

- write-forward constraints
- cross-schema conflict candidates
- serving indexes over lens unions
- schema metadata transactions

## 27. Architecture For Attempt 3

Attempt 3 should start fresh.

Recommended crate strategy:

- keep Attempt 2 code under reference or leave it as historical implementation
- create a new Attempt 3 module/crate/subtree
- copy tests and small helpers deliberately
- do not preserve Attempt 2 module graph

Implementation should be organized around data artifacts and verbs.

Artifacts:

- `SchemaDef`
- `PhysicalLayout`
- `IdCodec`
- `EnumCodec`
- `TablePlan`
- `ProjectionPlan`
- `VisibilityPlan`
- `QueryPlan`
- `ScopePlan`
- `WriteSet` / `ReadSet`
- `SyncBundle`
- `Effect`

Verbs:

- `lower_schema`
- `open_store`
- `apply_local_write`
- `run_query`
- `export_scope`
- `import_bundle`
- `validate_at_authority`
- `materialize_or_repair_projection`
- `diff_projection_effects`
- `poll_subscription`

Avoid manager-object taxonomy. Storage is a SQLite capability; Jazz semantics
should be explicit in plans, codecs, and verbs.

SQL fragments and bind parameters must travel together. Attempt 2 found
parameter-order coupling when fragments were assembled separately.

## 28. Test Architecture

Tests should be product-shaped and integration-heavy.

Canonical actors:

- Alice
- Bob
- Core authority
- optional edge tier later

Canonical fixture:

- `projects`
- `todos`

Add richer fixtures only when needed for subtle behavior.

Required semantic regression scenarios from Attempt 2:

1. schema-driven local write/query/reopen
2. projection rebuild byte-for-byte
3. joined required include
4. optional include nulling
5. joined subscription updates on dependency change
6. required dependency deletion removes parent
7. optional dependency deletion updates parent
8. scoped sync reproduces joined query
9. row entering filtered query syncs
10. row leaving filtered query syncs
11. authority accept enriches existing tx
12. authority reject repairs optimistic projection
13. stale row read rejects exclusive transaction
14. historical global epoch snapshot
15. branch pinned base
16. branch sync with branch-local rows
17. branch sync with only main-base rows
18. duplicate import does not rerun subscription
19. import old non-visible history does not rerun subscription
20. whole-system Alice/Bob/authority flow

Attempt 3 should add early tests for:

- local integer physical ids roundtrip through sync and hydrate from public ids
- integer enum codec rejects unknown discriminants
- projection-diff import effects
- row-granular same-row exclusive conflict despite disjoint column masks
- one predicate/absence authority validation case

Performance tests should remain runnable examples or benches:

- layout overhead
- id representation
- read/write-set storage
- query-plan/index shape
- memory representation

They should print enough context to compare runs but should not gate ordinary
CI until stabilized.

## 29. Recommended Attempt 3 Slice Order

1. Physical layout and id codec.

   Implement local integer surrogates, public-id mapping tables, integer enums,
   and `WITHOUT ROWID` DDL for one schema.

2. Local write/query/current projection.

   Recreate the basic projects/todos write/query/reopen slice.

3. Projection rebuild and projection-diff effects.

   Make rebuild/diff a first-class path before sync.

4. Query scope.

   Capture result/dependency/predicate/branch context in typed scope.

5. Subscriptions.

   Rerun+diff with effect overlap and full dependency payloads.

6. Sync import/export.

   Bundle tx/fate/history/branch/scope, reproduce joined and branch queries.

7. Authority validation.

   Mutable fate, row read sets, stale-row rejection, one absence/range case.

8. Branch visibility.

   Pinned main base, branch-local overlay, branch-base sync. Multi-base can wait.

9. Historical snapshots.

   Shared visibility relation with global epoch snapshots.

10. Conflict candidates.

Row-granular conflict semantics with column metadata for explanation.

11. Policies or lenses.

Pick one narrow vertical slice if the core spine holds.

## 30. Success Criteria For Attempt 3

Attempt 3 is successful if it proves a cleaner spine, not if it reaches final
product completeness.

Success means:

- local integer physical ids and integer enums are used from the beginning
- current main reads are fast and projection-backed
- history remains source of truth
- projection rebuild/diff is a central mechanism
- branch/source context is part of query scope
- sync reproduces scoped queries across stores
- authority validation uses typed read/write sets
- conflict correctness is row-granular
- column masks exist as auxiliary metadata
- tests read like product scenarios
- performance examples can compare physical layout choices

Failure modes to watch:

- the new code again collapses into one central store object
- visibility logic forks into unrelated current/snapshot/branch paths
- physical ids leak into public API semantics
- sync bundles become result payloads instead of history/fate/scope
- policy/lens ambitions overwhelm the core spine
- layout generics slow iteration more than they help

## 31. Possible Future Revisit: Fixed-Width Binary Ids

Attempt 3 should not branch over physical id representations. Its baseline is
local integer surrogates.

Fixed-width binary ids remain a possible future optimization if public Jazz ids
gain or already have a canonical compact binary form.

Why revisit later:

- BLOB id experiments showed strong disk savings versus long text ids.
- Inline binary ids avoid mapping-table joins for public-id lookup.
- A fixed binary representation may simplify some protocol/storage roundtrips.

Why not now:

- integer surrogates are the most SQLite-native hot key shape
- mapping/hydration semantics are straightforward and explicit
- choosing BLOB ids now would couple storage to a not-yet-final public id
  encoding
- Attempt 3 needs fewer branches, not more

If revisited, it should be a contained physical-layout experiment after the
Attempt 3 semantics spine is working.

## 32. Meta: How Serious Should Attempt 3 Be?

Attempt 3 should be more serious than Attempt 2, but still an attempt.

It should aim for a coherent mini-system that could plausibly become the real
core spine. But it should preserve the freedom to throw away implementation
again if the physical layout, visibility model, or sync shape proves wrong.

The best posture:

- be stricter about physical layout from the start
- be stricter about typed data artifacts
- keep tests high-level and executable
- keep detailed decision logs
- commit after green slices
- avoid polishing public API too early
- avoid preserving code just because it works

Attempt 2 made the system possible. Attempt 3 should find out whether the
compact, data-driven, SQLite-backed design can be made clean.
