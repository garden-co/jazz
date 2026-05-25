# SQLite Jazz Core Spec

## Purpose

Build a new Jazz core on top of SQLite.

SQLite provides:

- local ACID transactions
- durable storage
- indexes
- query planning
- ordinary relational joins and sorting

Jazz provides the distributed/local-first semantics above SQLite:

- transaction identity and fate
- append-only row history
- current projections
- version-vector and branch visibility
- query subscriptions
- query-scoped sync
- read/write-set validation
- conflict candidates and resolution
- policies
- schema-version lenses

The goal is not to preserve current Jazz internals. The goal is to preserve the
validated high-level Jazz API and rebuild the engine bottom-up around SQLite.

## Application Surface

Consumers mostly use typed schemas and query builders rather than SQL.

Example schema:

```ts
import { schema as s } from "jazz-tools";

const schema = {
  todos: s
    .table({
      title: s.string(),
      done: s.boolean(),
    })
    .indexOnly(["done", "$createdAt"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
```

Example usage:

```ts
await db.insert(app.todos, {
  title: "Write the SQLite lowering",
  done: false,
});

await db.all(
  app.todos
    .where({ done: false })
    .where({ $createdAt: { gt: yesterday } })
    .orderBy("$createdAt", "desc"),
);

db.subscribeAll(
  app.todos
    .where({ done: false })
    .where({ $createdAt: { gt: yesterday } })
    .orderBy("$createdAt", "desc"),
  ({ all }) => render(all),
);
```

The first implementation target is native Rust SQLite. Browser/WASM SQLite is a
separate packaging and startup-size risk, not the first semantics target.

## Core Storage Shape

Every logical user table has append-only history tables. History is the source
of truth.

Each structural schema version gets its own physical table shape, because
columns can differ across schema versions:

```text
todos__schema_v1_history
todos__schema_v1_current
tasks__schema_v2_history
tasks__schema_v2_current
```

The `main` branch gets a derived current projection for fast ordinary reads.
Non-main branches and arbitrary historical snapshots start as pure-query reads
over history plus branch/source metadata.

Per-branch projections, sparse branch overlays, query-specific serving indexes,
and hot-branch projections are optional serving indexes. They must not be
required for correctness.

Row ids are globally unique.

## System Tables

System tables are engine-only, so their columns use plain names.

Sketch:

```sql
CREATE TABLE jazz_node (
  node_num INTEGER PRIMARY KEY,
  node_id TEXT NOT NULL UNIQUE
);

CREATE TABLE jazz_tx (
  tx_id TEXT PRIMARY KEY,
  node_num INTEGER NOT NULL,
  local_epoch INTEGER NOT NULL,
  global_epoch INTEGER,
  kind TEXT NOT NULL,
  base_global_epoch INTEGER NOT NULL,
  base_local_jsonb BLOB NOT NULL,
  base_include_jsonb BLOB NOT NULL,
  read_set_jsonb BLOB NOT NULL,
  write_set_jsonb BLOB NOT NULL,
  status TEXT NOT NULL,
  rejection_reason_json TEXT,
  created_at INTEGER NOT NULL,
  sealed_at INTEGER,
  metadata_json TEXT NOT NULL,
  UNIQUE (node_num, local_epoch),
  UNIQUE (global_epoch)
);

CREATE INDEX jazz_tx_status_global_epoch
  ON jazz_tx(status, global_epoch, tx_id);

CREATE TABLE jazz_branch (
  branch_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  head_global_epoch INTEGER NOT NULL,
  head_local_jsonb BLOB NOT NULL,
  head_include_jsonb BLOB NOT NULL,
  base_provenance_jsonb BLOB NOT NULL
);

CREATE TABLE jazz_branch_history (
  branch_id TEXT NOT NULL,
  tx_id TEXT NOT NULL,
  op TEXT NOT NULL,
  head_global_epoch INTEGER NOT NULL,
  head_local_jsonb BLOB NOT NULL,
  head_include_jsonb BLOB NOT NULL,
  base_provenance_jsonb BLOB NOT NULL,
  metadata_json TEXT NOT NULL,
  PRIMARY KEY (branch_id, tx_id),
  FOREIGN KEY (tx_id) REFERENCES jazz_tx(tx_id)
);

CREATE TABLE jazz_branch_base (
  branch_id TEXT NOT NULL,
  source_branch_id TEXT NOT NULL,
  source_global_epoch INTEGER NOT NULL,
  precedence INTEGER NOT NULL,
  PRIMARY KEY (branch_id, source_branch_id, precedence),
  FOREIGN KEY (branch_id) REFERENCES jazz_branch(branch_id)
);

CREATE TABLE jazz_schema (
  schema_hash TEXT PRIMARY KEY,
  schema_json TEXT NOT NULL
);
```

Transaction kinds:

```text
data
branch_metadata
schema_metadata
permission_metadata
```

The first implementation should try mutable fate directly on `jazz_tx`:

```text
status
global_epoch
rejection_reason_json
```

The hope is that mutable fate on `jazz_tx`, plus durable transaction rows and
sync replay, is enough for replayability. Append-only fate receipts are a
possible later addition if debugging, audit, idempotency, or authority handoff
requires them. They are not baseline spec for the next implementation pass.

Even without a separate receipt table, the implementation should keep the
conceptual boundary clear: proposed transaction state and authority observations
are different facts. Importing an authority response enriches or rejects an
existing transaction; it does not create a new public transaction identity.

## User Tables

User row tables mix app columns and engine columns. Engine columns use `j_`;
app columns do not.

The lowerer needs one identifier codec. SQLite treats bare `$name` as parameter
syntax in common contexts, so user-facing `$` system fields must never be
hand-written as physical SQLite identifiers.

History table sketch:

```sql
CREATE TABLE todos__schema_v1_history (
  j_row_id TEXT NOT NULL,
  j_branch_id TEXT NOT NULL,
  j_tx_id TEXT NOT NULL,
  j_op TEXT NOT NULL,

  title TEXT,
  done INTEGER,

  j_conflict_tx_ids_jsonb BLOB NOT NULL,
  j_created_by TEXT,
  j_created_at INTEGER NOT NULL,
  j_updated_by TEXT,
  j_updated_at INTEGER NOT NULL,
  j_edit_metadata_json TEXT NOT NULL,

  PRIMARY KEY (j_row_id, j_branch_id, j_tx_id),
  FOREIGN KEY (j_tx_id) REFERENCES jazz_tx(tx_id)
);

CREATE INDEX todos__schema_v1_history_branch_row_updated
  ON todos__schema_v1_history(j_branch_id, j_row_id, j_updated_at DESC, j_tx_id);

CREATE INDEX todos__schema_v1_history_branch_tx
  ON todos__schema_v1_history(j_branch_id, j_tx_id, j_row_id);
```

Current projection sketch:

```sql
CREATE TABLE todos__schema_v1_current (
  j_row_id TEXT NOT NULL,
  j_branch_id TEXT NOT NULL,
  j_visible_tx_id TEXT NOT NULL,
  j_is_deleted INTEGER NOT NULL,

  title TEXT,
  done INTEGER,

  j_conflict_tx_ids_jsonb BLOB NOT NULL,
  j_created_by TEXT,
  j_created_at INTEGER NOT NULL,
  j_updated_by TEXT,
  j_updated_at INTEGER NOT NULL,
  j_edit_metadata_json TEXT NOT NULL,

  PRIMARY KEY (j_row_id, j_branch_id)
);

CREATE INDEX todos__schema_v1_current_done_created_at
  ON todos__schema_v1_current(j_branch_id, done, j_created_at DESC);
```

History rows for update/delete must carry immutable creation metadata as well
as updated metadata. Otherwise byte-for-byte current projection rebuilds can
drift.

More generally, history rows must carry every projection-affecting field needed
for deterministic rebuilds: operation/delete state, immutable creation metadata,
updated metadata, conflict candidates, and cleared conflict state.

Current projections must be rebuildable from history plus transaction fate.
Projection rebuilds should be registered/generated per schema table rather than
hand-coded at each rejection/import path.

The local `main` current projection is the optimistic current local view. It may
include local pending transactions. Global historical snapshots are different:
they only include transactions accepted by the authority at or below the
requested global epoch. APIs should make this read mode explicit.

## Transactions

The only write unit is a transaction. One write call creates one sealed
transaction. Multi-row writes use the same transaction abstraction.

Transaction identity has three coordinates:

- `$txId`: stable public identity, never rewritten
- `($nodeId, $localEpoch)`: assigned immediately by the writer
- `$globalEpoch`: assigned later by the authority

`$globalEpoch` is a simple logical number. The first version has one authority.
Future sharding should remain implicit below this logical model rather than
surfacing authority ids in vectors or queries.

SQLite databases may use local integer surrogates such as `node_num` for compact
joins and indexes. Sync boundaries must export stable node ids and hydrate local
surrogates on import.

When the authority accepts a transaction, it broadcasts a fate/mapping for the
existing transaction identity:

```text
TxAccepted {
  txId: "tx_alice_21",
  nodeId: "alice_device",
  localEpoch: 21,
  globalEpoch: 1057
}
```

Receivers update the existing `jazz_tx` row. References do not rename from local
to global; compact coordinates become additional addressability for the same
transaction.

Transaction state machine:

```text
local_pending -> edge_durable -> global_durable_accepted
local_pending -> global_durable_accepted
local_pending -> rejected
edge_durable -> rejected
```

Direct `local_pending -> global_durable_accepted` is allowed when there is no
edge tier.

Rejected transactions:

- remain in history
- keep machine-readable rejection reasons
- are filtered out by visibility predicates
- do not require version-vector excludes

If a rejected local transaction affected an optimistic current projection, the
recipient must repair derived projections. The first implementation can rebuild
affected current projections from non-rejected history; write-set-driven repair
is an optimization.

Accepted branch-local transactions receive normal global epochs. They are
globally known history, but they remain isolated to their branch until an
explicit merge transaction changes cross-branch visibility.

## Transaction Acceptance

Transactions can be:

- mergeable/eventually consistent
- exclusive/globally consistent

Mergeable transactions can be accepted independently and later reconciled.
Exclusive transactions are authority-validated before global acceptance.

Authority acceptance checks precise read sets:

- row reads must still point at the authority-visible row version
- absence/range reads must still be true
- mixed read sets must validate every entry
- writes carry row and column masks

If a transaction writes a row, its read set includes the exact previous visible
row version even when application code did not explicitly read that row first.

The prototype showed that precise read/write sets can replace explicit parent
pointers for v0 acceptance correctness. Parent pointers may still be useful
later for debugging or graph traversal, but they are not baseline.

## Read And Write Sets

Stored inline on `jazz_tx` as canonical JSONB/BLOB-shaped data for now.

Example:

```json
[
  {
    "kind": "row",
    "table": "todos",
    "rowId": "todo_1",
    "visibleTxId": "tx_base",
    "reason": "write_base"
  },
  {
    "kind": "range",
    "table": "projects",
    "index": "projects_by_row_id_deleted",
    "predicate": { "rowId": "project_missing", "isDeleted": false },
    "reason": "optional_dependency_absence"
  }
]
```

Read reasons include at least:

```text
direct
write_base
policy_dependency
optional_dependency_absence
page_boundary
```

Write-set entries include:

```json
{
  "table": "todos",
  "rowId": "todo_1",
  "op": "update",
  "columns": ["title", "$updatedAt"]
}
```

Read/write sets need typed internal representations even if the first durable
format is JSONB. They should be canonical for byte-for-byte rebuilds.

Read sets are allowed to over-approximate, but they must not omit any row
version, absence, range, or policy dependency that affected the transaction's
validity. Write sets are both row-granular and column-granular: rows support
coarse invalidation/sync, while column masks support merging, policies, and
subscription filtering.

The encoding boundary should be centralized early. Storage code should depend
on typed read/write-set values and a single codec, not ad-hoc JSON/string
templates scattered through write and acceptance paths.

Validation must decode and check every read-set entry. A partial decoder that
only validates the first row/range dependency is unsound.

Open: exact JSONB schema, compact encoding, and whether large read sets need
side tables.

## Version Vectors

Use compact additive dotted version vectors:

```ts
type TxRef = { txId: string } | { global: number } | { node: string; local: number };

type VersionVector = {
  globalBase: number;
  localBases?: Record<string, number>;
  include?: TxRef[];
};
```

Semantics:

- `globalBase` includes all globally accepted transactions through that epoch
- `localBases[node]` includes locally durable transactions for that node
- `include` contains sparse positive dots
- no `exclude` in v0
- vectors are closed, not pointers to other snapshots
- rejected transactions are filtered by transaction fate/status

There is no general snapshot table in the baseline. Vectors are stored directly
where owned:

- transactions
- branches
- subscriptions
- reconnect state
- named snapshots later, if needed

Representation tradeoff:

- `$txId` includes are long but stable
- global epochs are compact but only exist after acceptance
- node-local coordinates are compact locally but need global mapping awareness

First pass can use `$txId` includes for stability, then experiment with epoch
coordinates for compactness.
Until compact coordinate upgrades are proven, `$txId` includes are the preferred
prototype representation because they are stable before and after authority
mapping.

Vector JSONB/BLOB fields must be canonical. Local bases should sort by node
identity or local surrogate. Include dots need a canonical mixed-coordinate
sort. Branch provenance either preserves user/input order with explicit ordinals
or uses a canonical key; this remains a representation decision.

SQLite query execution should decode a vector once into bindings or temp
tables. It should not repeatedly parse JSONB per candidate history row.
Temp visibility tables are useful for testability too: they make the resolved
visibility relation inspectable, even if another representation wins for
performance.

Useful temp-table shape:

```sql
CREATE TEMP TABLE snapshot_node_base (
  node_num INTEGER PRIMARY KEY,
  local_base_epoch INTEGER NOT NULL
);

CREATE TEMP TABLE snapshot_include_tx (
  tx_id TEXT PRIMARY KEY
);
```

## Snapshot And Time-Travel Reads

Pure-query history reads are the correctness baseline.

Snapshot query shape:

1. Resolve visible transaction ids/coordinates from the vector.
2. Query history for latest visible version per row.
3. Filter deletes.
4. Apply user predicates/order.

The prototype found:

- naive correlated `NOT EXISTS` shape was very slow
- grouped latest-visible CTE was much faster
- 2,000 rows / 1,333 matching open rows:
  - current projection roughly 2.4 ms
  - optimized temp-table snapshot roughly 17 ms in debug test mode

This is acceptable for cold snapshots/branches in v0. Hot `main` reads should
use current projections. Hot branches may later get projections or serving
indexes.

The initial snapshot lowering candidate should be the grouped latest-visible
CTE shape, with visibility pre-decoded into temp tables or an equivalent
relation. Correlated `NOT EXISTS` remains a comparison point, not the default.

Open performance experiments:

- normalized history + join to `jazz_tx`
- denormalized transaction coordinates on history rows
- temp tables vs generated predicates for visible transaction sets
- grouped CTE vs window functions vs `NOT EXISTS`
- native history sort/index layouts
- target: pure-query branch/time-travel reads around tens of milliseconds at
  100k-row early datasets are acceptable for cold paths

## Branches

A branch's visible content is defined by branch provenance/source metadata, not
by copying the whole database.

There are two layers:

- user-visible branch rows for app metadata and permissions
- engine tables for effective visibility and provenance

Recommended app-level shape:

```ts
const branch = await db.insert(app.branches, {
  projectId,
  name: "Alice's draft",
  ownerId: session.user_id,
});

const draft = db.branch(branch.id);
```

The user-visible branch row is the natural permission anchor.
Branch reads require read permission on the branch row. Branch writes require
update permission on the branch row, plus ordinary table/row permissions for
data accessed through the branch.

Engine branch metadata:

- `jazz_branch`: current branch head/projection
- `jazz_branch_history`: append-only branch metadata history
- `jazz_branch_base`: precise source list usable by SQL

Branch reads use a source relation:

```text
(source_branch_id, source_global_epoch, precedence)
```

SQL chooses the highest-precedence visible row per row id. This represents:

```text
draft over main
branch_b over branch_a over main
```

The same effective source stack must be used for every table in one query. A
joined branch query cannot read the parent row from one source interpretation
and the dependency row from another.

Branches store both:

- precise provenance
- flattened/effective source list for querying

Precise provenance is for UI/debugging/rebuilds. The flattened source list is a
serving representation for SQL visibility. The two should be rebuildable from
branch metadata history.

Metadata-only branch merges are first-class:

- write a `branch_metadata` transaction
- update branch source/provenance
- do not copy user-table history

Data-copy merges remain possible when conflicts require explicit translated row
versions, but metadata-only merge better matches the desired isolation model.
Data-copy merge is a fallback/resolution strategy, not the default merge
semantics.

Open branch questions:

- exact `base_provenance_jsonb` shape
- deriving flattened sources from precise provenance
- conflicts between multiple bases as multiple visible candidates
- permissions on branch source changes
- hot branch projection heuristics

## Conflict Candidates And Resolution

Current projections store:

- resolved value
- conflict metadata

For v0, conflict metadata is candidate tx ids:

```text
j_conflict_tx_ids_jsonb
```

Conflict metadata belongs at the object that is conflicted. In joined results,
a todo can be unconflicted while its nested project is conflicted.

Conflict resolution is an ordinary data transaction:

- reads the conflicted current row
- writes the chosen value
- clears candidate metadata
- records which candidates were resolved in transaction metadata

Projection rebuild after conflict resolution must be byte-for-byte stable.
Resolution history rows must carry both the chosen value and the cleared
candidate metadata.
Transaction metadata records the candidate tx ids resolved by the transaction,
so rebuilds, listeners, and sync can explain why conflict metadata disappeared.

Open:

- per-column candidate representation
- merge algorithms beyond last-writer-wins
- conflict metadata API shape
- candidate ordering rules beyond `updatedAt` plus global epoch tie-breaker

## Queries

Current `main` reads use current projections:

```sql
SELECT j_row_id, title, done, j_created_at
FROM todos__schema_v1_current
WHERE j_branch_id = :branch_id
  AND j_is_deleted = 0
  AND done = 0
  AND j_created_at > :yesterday
ORDER BY j_created_at DESC;
```

Queries can also return engine-only scope data. The implementation may use:

- hidden columns
- side-channel collection
- temp tables
- a second result set
- Rust-side locator assembly

Do not expose scope internals to normal application code.

Scope representation is still a first implementation choice. The attempt1 spike
identified three plausible shapes to compare on the same joined query:

- hidden JSON/JSONB columns in the SQL result
- temp tables or a second result set populated by the lowered query
- Rust-side side-channel assembly from projected locators

The choice should be judged by deterministic ordering, duplicate handling, and
how naturally scope expands into history bundles.

## Includes And Joins

Required includes lower to inner joins. If the child/dependency is missing, the
parent row is filtered out.

Optional includes lower to left joins. If the child/dependency is missing, the
parent row remains and the included value is `null`.

Optional missing includes require predicate/absence scope, because there is no
concrete row locator for the absence.

Joined query scope distinguishes:

- parent/result row
- dependency row
- policy row
- absence/predicate

This is required for sync, subscriptions, and authority validation.

For subscriptions, required and optional dependency changes have different
semantic diffs: deleting a required dependency removes the parent result, while
deleting an optional dependency keeps the parent, nulls the child, and records
absence/predicate scope.

## Policies

Policies should lower to SQL in v0.

Policy dependencies are separate from result dependencies even when they point
at the same row. This avoids ambiguity about whether a row was needed to render
the result, enforce authorization, or both.
A scope entry's reason is part of its meaning; the same row may appear as both
result/dependency scope and policy scope without collapsing those roles.

Example policy shape:

```ts
export default s.definePermissions(app, ({ policy, session, allowedTo }) => [
  policy.projects.allowRead.where({ ownerId: session.user_id }),
  policy.todos.allowRead.where(allowedTo.read("projectId")),
]);
```

Policy dependencies may be sent to ordinary clients in v0. Opaque proofs are
future work.

Open:

- exact policy-scope output format
- authority vs local policy evaluation split
- inherited/recursive policy lowering
- policy explanation/error payloads

## Subscriptions

Baseline subscriptions rerun SQL and diff full result rows:

```text
write commits
applicator records touched tables/rows/columns
subscription manager reruns affected queries
diff previous ordered full rows vs next ordered full rows
emit semantic changes
update stored scope
```

No SQLite triggers should carry semantic machinery. The Jazz write applicator
has write-set information and should drive invalidation.

Subscription state should include the original query AST, compiled SQL, previous
ordered result rows, last result scope, last policy scope, and dependency
metadata for tables/columns/branches/schemas/transactions it may depend on.
For joined results, previous rows must include dependency payloads too; a
dependency-only update can change the semantic result without changing the
result row's id or visible transaction.

Rerun+diff is semantically correct for:

- simple current queries
- joined dependency updates
- required dependency deletion
- optional dependency nulling
- top-N page churn

Efficient invalidation is still open, especially for ordered/page queries.

Page scope can include a boundary predicate, such as:

```json
{
  "done": false,
  "projectNameLte": "Beehive",
  "limit": 20
}
```

But boundary predicates alone are insufficient. If an off-page row moves from
`"Zebra"` to `"Aardwolf"`, invalidation needs old and new sort keys to detect
the boundary crossing.

Likely need:

- old/new index-key change records
- ordered-index watch primitives
- or coarse invalidation for first version

Public pagination cursors may be row ids, but internal invalidation still needs
the resolved order key for the cursor row. A row-id cursor does not by itself
detect an off-page row whose old/new sort key crosses into the page.
The minimal precise rule is boundary crossing: rerun a page when either the old
or new ordered index key is inside the watched page boundary.

## Sync Scope

Sync remains query-scoped.

Upstream executes lowered SQL and sends enough data for the lower tier to
reproduce the query locally. The app-facing result is not the source of truth;
row history is.

Scope categories:

- result rows
- include/join dependency rows
- policy dependency rows
- predicate/range/absence facts
- page boundary facts

Concrete row scope expands to transaction bundles. Bundle export deduplicates
by tx id, even if locators mention the same dependency multiple times.
Scope locators and wire bundles intentionally have different cardinality:
locators may repeat to explain each result's dependencies, while wire bundles
should deduplicate concrete transactions.

Bundles are table/schema-polymorphic. A query involving `todos` and `projects`
must export/import history for both tables. Import is an upsert of transaction
fate plus history rows; importing a rejection can require projection repair.

Import has semantic side effects. It may hydrate node surrogates, update
transaction fate, update current projections for accepted imported rows, and
repair projections for imported rejections. It is not an insert-only operation.

For v0, sync can send full history of result/dependency rows. This supports:

- replay
- semantic diffs
- time-travel inspection of rows in the result set
- reproducing older snapshots for rows that were in scope

Predicate scope rides alongside row bundles. It may not correspond to any row
bundle.
The same predicate/range facts should serve three roles when possible: query
sync scope, subscription invalidation scope, and authority-side read-set
validation for optional dependencies, policy checks, and uniqueness-like
constraints.

Reconnect can start by replaying desired subscriptions and comparing known
transaction ids / vectors. More compact sync protocols can come later.

## Schemas And Lenses

Each structural schema version has its own history/current table shape.

Lenses must be SQL-lowerable at first.

Reads over newer schemas can union native rows with lens-translated rows from
older schema tables.

Example:

```text
v1 todos.title      -> v2 tasks.text
v1 todos.done       -> v2 tasks.completed
```

Read lowering sketch:

```sql
WITH native_v2 AS (
  SELECT j_row_id, j_branch_id, j_visible_tx_id, text, completed
  FROM tasks__schema_v2_current
  WHERE j_branch_id = :branch_id
    AND j_is_deleted = 0
),
translated_v1 AS (
  SELECT j_row_id, j_branch_id, j_visible_tx_id, title AS text, done AS completed
  FROM todos__schema_v1_current
  WHERE j_branch_id = :branch_id
    AND j_is_deleted = 0
)
SELECT * FROM native_v2
UNION ALL
SELECT * FROM translated_v1;
```

Writes through a lens create a new row version in the writer's current schema
version table.

Open:

- cross-schema same-row conflict resolution
- lens write translation constraints
- schema metadata as transactions
- serving indexes over lens unions

## Implementation Strategy

Continue with a deterministic multi-tier harness.

Model:

```text
client main
client worker
edge tier
core authority
durable storage
ephemeral storage
scriptable links
```

Harness capabilities:

- enqueue messages
- deliver in chosen orders
- drop/duplicate messages
- partition/reconnect links
- restart nodes
- inspect durable SQLite state

Each simulated node should run the same core state machine against pluggable
durable or ephemeral storage. The first harness does not need real networking;
explicit message delivery is more useful for making distributed semantics
testable.

The first local subscription API can be callback-free/polling in tests. That
keeps async/runtime choices out of the semantics while still exercising the
rerun+diff loop.

Prefer vertical executable slices:

1. Single-node CRUD/current projection/restart.
2. Local subscriptions through rerun+diff.
3. Authority acceptance/rejection and local-to-global mapping.
4. Query-scoped sync for result/dependency rows.
5. Predicate/absence scope and authority validation.
6. Branch creation/source reads/metadata merge.
7. Policies with separate policy scope.
8. Full-history scope import/export.
9. Conflict candidates and resolution.
10. Schema lenses.

Each slice should assert whole-system invariants, especially projection rebuild
stability, query reproduction after sync, subscription diff correctness, branch
visibility explainability, and idempotent reconnect/import behavior.

Projection rebuilders should be registered/generated per schema table. Fate
imports, rejections, and repair paths should call the registry rather than
remembering each current table at each transition.

Benchmarks should be promoted from harness scenarios after semantics are clear.
Use isolated SQLite microbenchmarks only when a scenario identifies a concrete
hot path.
Planner visibility is part of that work: add `EXPLAIN QUERY PLAN` hooks for
known risky lowerings before relying on them for performance claims.

## Attempt2 Architecture Pass

Attempt2 should get closer to a small working system, not another collection of
independent spikes. It is also an architecture-discovery pass: the goal is to
learn which component boundaries survive when CRUD, queries, subscriptions,
sync, authority validation, branches, and conflicts all run through them.

The test API does not need to reach the final TypeScript DSL yet, but it should
be semantically close enough that tests read like product usage rather than
storage-helper usage.

Example Rust-side shape:

```rust
let schema = Schema::new()
    .table("projects", |t| {
        t.text("name");
        t.index("by_name", ["name", "$createdAt"]);
    })
    .table("todos", |t| {
        t.text("title");
        t.bool("done");
        t.ref_("project_id", "projects");
        t.index("open_by_created", ["done", "$createdAt"]);
    });

let alice = harness.client("alice", schema.clone()).durable();
let core = harness.authority("core", schema.clone());

alice.write(|tx| {
    let project = tx.insert("projects", json!({ "name": "SQLite Jazz" }));
    tx.insert("todos", json!({
        "title": "Design attempt2",
        "done": false,
        "project_id": project.id()
    }));
})?;

let sub = alice.subscribe(
    query("todos")
        .filter(eq("done", false))
        .include_required("project")
        .order_by("$createdAt", Desc)
        .limit(20),
)?;
```

Tests should mostly use this public-ish engine surface: define schema, write,
query, subscribe, sync, restart. Direct storage helpers can exist, but they
should not become the main semantic test surface.

### Architectural Style

Attempt2 should prefer declarative data structures and small execution
pipelines over manager-style components. The durable architecture should be the
flow of data and effects, not a taxonomy of service objects.

Stable artifacts should mostly be data:

- `SchemaDef`: logical tables, fields, relations, indexes, policies, and schema
  versions.
- `StorageLayout`: physical system tables, user history/current tables,
  indexes, physical names, and user-column escaping.
- `TablePlan`: per-table DDL, row codecs, system-column mapping, scope locator
  shape, and bundle expansion shape.
- `WritePlan`: append transaction, append row history, update projections,
  record read/write sets, and emit touched facts.
- `ProjectionPlan`: current projection update/rebuild SQL plus deterministic
  rebuild invariants.
- `QueryPlan`: lowered SQL, row decoder, required temp relations, and scope
  plan.
- `ScopePlan`: result/dependency/policy/predicate/page locators and their
  cardinality/deduplication rules.
- `VisibilityPlan`: version-vector canonicalization and visibility-relation
  materialization.
- `BranchSourcePlan`: precise provenance to flattened source relation.
- `ValidationPlan`: authority-side read-set, policy, and constraint checks.
- `SyncBundlePlan`: transaction/history/fate/scope export and semantic import.
- `EffectLog`: touched rows, columns, tables, branches, schemas, tx fate, and
  old/new index keys for invalidation.

Runtime code should be a small set of verbs over those artifacts:

- `lower_schema(schema) -> StorageLayout`
- `derive_plans(layout) -> table/write/query/projection/sync plans`
- `apply_local_write(plan, input) -> effects`
- `run_query(plan, snapshot_or_current) -> rows + scope`
- `apply_import(bundle_plan, bundle) -> effects`
- `validate_at_authority(validation_plan, tx) -> fate`
- `repair_projections(projection_plans, effects)`
- `run_subscription_tick(query_plan, previous, effects) -> diff + scope`
- `export_scope(sync_plan, scope) -> bundle`

Storage remains a thin SQLite capability used by these verbs: execute SQL,
manage transactions, create temp relations, inspect plans, and persist bytes.
It should not accumulate Jazz semantics behind object boundaries.

This style is intentionally closer to data-driven app/game-engine design:
schema data generates plans, plans plus inputs produce effects, and effects
drive invalidation, sync, and repair. Some artifacts may later become plain
functions or SQL strings rather than structs. That is fine; the important part
is that semantics are explicit and testable as data flowing through phases.

The meaningful execution phases are:

```text
write:
  allocate tx -> append history -> update current -> record effects -> notify

import:
  hydrate ids -> upsert tx/fate -> append missing history -> repair -> notify

query:
  materialize visibility/source relations -> run SQL -> collect scope -> decode

authority:
  validate reads/policies/constraints -> decide fate -> emit observation

subscription:
  choose affected subscriptions -> rerun -> diff full rows -> publish

sync:
  expand scope -> dedupe bundles -> import -> reproduce query
```

Attempt2 vertical slices:

1. Schema-driven local engine: generated `projects`/`todos` history/current DDL,
   layouts, table plans, write plans, and projection plans.
2. Typed query compiler: current `main` filters over user/system columns,
   joins/includes, order/limit, query plans, result scope, and dependency scope.
3. Subscriptions: rerun+diff over compiled queries with previous result and
   dependency payloads, including required deletion, optional nulling, and page
   churn.
4. Sync between stores: joined query scope export/import, full-history mode,
   predicate absence scope, bundle plans, and deduped bundles.
5. Authority loop: optimistic client writes, export to authority, read-set
   validation plan, accept/reject, fate import, and projection repair.
6. Snapshot/vector reads: temp visibility relation, grouped latest-visible CTE,
   visibility plan, and tests for global/local/include visibility.
7. Branches: branch creation from sources, branch-local writes, shared
   branch source plan, metadata-only merge, and joined branch query.
8. Conflicts: concurrent writes from the same base, visible candidates, resolved
   value plus conflict metadata, and deterministic resolution rebuilds.

Attempt2 should make progress on open questions while implementing these slices,
but its primary output is architectural evidence: which abstractions simplify
the whole system, which leak, and where SQLite-specific assumptions need to be
contained.

Attempt2 guardrails:

- Start each vertical slice from product-shaped integration tests. Write the
  test first, watch it fail, then implement.
- Keep tests on the public-ish engine API wherever possible. Storage helpers
  can exist behind the scenes, but should not become the main semantic surface.
- Recreate `crates/mini-jazz-sqlite` as an active Rust crate root. Keep
  `reference/attempt1` inside the folder for comparison.
- Use native Rust SQLite via `rusqlite` for the next semantics pass.
- Keep a detailed `ATTEMPT2.md` decision/discovery log while work is in
  progress. It is cheap while context is fresh and can be summarized later.
- Use `projects` and `todos` as the canonical fixture, but add richer fixtures
  whenever subtle behavior needs them.
- Use mutable fate on `jazz_tx` as the baseline, while keeping
  proposal-vs-authority-observation explicit in tests and protocol shape.
- Model conflicts per-column from the start. This is intentionally more
  demanding than row-level conflict metadata, because it forces the storage,
  projection, query, sync, and listener shapes to expose the right seams.
- No table-specific storage paths after the first schema-driven slice. Fixture
  tables can be concrete; write/query/projection logic should flow through
  schema-derived layouts and plans.
- Commit after each green vertical slice so architectural turns are easy to
  inspect and backtrack.
- First daytime target: get slices 1-3 green, then inspect the architecture
  before pushing into sync and authority.

## Invariants

- Every visible row version references a non-rejected transaction.
- Rejected transactions may remain in history but are never visible.
- Current projections rebuild byte-for-byte from history and transaction fate.
- Projection repair is table/schema-polymorphic.
- Transaction ids never change after local creation.
- Local-to-global mapping enriches transaction coordinates; it does not rename
  transactions.
- Sync boundaries export stable node ids and rehydrate local node surrogates.
- Sync recipients can reproduce scoped query results locally.
- Subscription diffs match rerunning the query from scratch.
- Branch visibility is explainable by source/provenance metadata.
- Read-set validation checks every declared row/range dependency.
- Policy scope remains distinguishable from result/dependency scope.
- User-facing `$` semantics are independent of physical SQLite identifier
  encoding.

## Next Things To Derisk

1. **Generic lowering**
   Replace hard-coded `todos`/`projects` storage with generated descriptors for
   history/current DDL, projection rebuilds, import/export, and scope capture.
   Generated descriptors should also own projection registry entries, row
   bundle expansion, rejection repair, and rebuild invariants.

2. **Realtime invalidation**
   Prove a cheap invalidation path for non-trivial subscriptions, especially
   ordered/page/range queries with old/new index keys. Row-id-only public
   cursors should be tested against internal order-key invalidation.

3. **Version-vector compactness**
   Compare tx-id includes, global epoch dots, and node-local dots for storage,
   wire size, and upgrade behavior after authority mapping.

4. **Read/write-set encoding**
   Move from tiny prototype codec to a canonical typed JSONB shape. Measure
   when inline JSONB becomes too large and whether side tables are needed.

5. **Branch provenance**
   Specify exact precise provenance shape and deterministic flattening into
   queryable source lists, including multiple bases and conflicts.
   Prove joined branch queries use one shared effective source stack.

6. **Policy lowering**
   Implement one real policy path end-to-end: SQL lowering, policy scope,
   authority validation, rejection reason, and sync payload.

7. **Conflict model**
   Make conflicts per-column, not only row-level candidate tx ids. Define API
   shape for resolved value plus conflict metadata.

8. **Schema lenses**
   Implement one SQL-lowerable rename lens with read union and write-forward
   behavior, then test cross-schema conflicts.

9. **SQLite/WASM product risk**
   Measure binary size, startup time, persistence options, and feature
   availability for the chosen browser SQLite build, including JSONB support or
   required fallback encoding.

10. **Snapshot performance**
    Run larger realistic datasets and compare pure-query history reads,
    denormalized history coordinates, temp-table visibility, and optional hot
    branch projections.
    Include query-plan assertions or captured planner evidence for candidate
    lowerings.

## Open Questions

- Is mutable fate on `jazz_tx` sufficient for replay/debugging, or do we later
  need append-only authority fate receipts?
- If fate remains mutable on `jazz_tx`, what protocol shape preserves the
  distinction between proposed transaction state and authority observations?
- What is the exact escaping rule for user columns beginning with `j_` inside
  row tables?
- What exact JSONB shapes should vectors, read sets, write sets, conflict
  metadata, and branch provenance use?
- Is SQLite JSONB available in every target we care about, or do durable
  encodings need a non-JSONB fallback?
- Which version-vector coordinate form should be canonical on disk and wire?
- How do we compact local vector coordinates after global acceptance?
- What does a reconnect "known transactions/vectors" summary look like once
  vectors can represent compact ranges?
- How broad should predicate/range sync scope be for optional includes and
  policy checks?
- How should optional vs required includes interact with authorization failure:
  filter parent, null child, or return an authorization error?
- What is the first acceptable subscription invalidation strategy for ordered
  queries?
- Should subscription read/scope state become durable resume material, or is it
  initially reconstructed by replaying desired subscriptions?
- How should multiple base conflicts surface before resolution?
- What permissions are required to add/remove branch sources?
- Can policy dependency rows always be sent to clients in v0, or do some apps
  need opaque authorization material immediately?
- When do current projections need to exist for non-main branches?
- What exact acceptance flow distinguishes mergeable and exclusive
  transactions once policies, constraints, and read-set validation all run?
- Which parts of this can remain SQLite-specific, and where is the minimal
  replaceable embedded-database interface?
