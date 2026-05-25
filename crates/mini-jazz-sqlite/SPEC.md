# Distilled SQLite Core Spec

## Goal

Build a new Jazz core on top of SQLite. SQLite provides local ACID
transactions, persistence, indexes, and query planning. Jazz provides the
distributed/local-first semantics above it:

- append-only row history
- derived current projections
- transactions and branch snapshots
- query subscriptions
- query-scoped sync
- policies
- schema-version lenses

The first target is native Rust SQLite. The first implementation slice should
prove local CRUD, fast current reads on `main`, subscriptions, and the core
transaction/snapshot model before sync.

## Application Surface

Most consumers use a typed schema/query API rather than SQL directly. The
SQLite core must preserve this high-level shape while changing the storage
engine underneath it.

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

## Storage Model

Every logical user table has append-only history tables. History is the source
of truth.

Each schema version gets its own physical table shape because schema versions
can have different columns:

```text
todos__schema_v1_history
todos__schema_v1_current
tasks__schema_v2_history
tasks__schema_v2_current
```

The `main` branch gets a derived current projection for fast ordinary reads.
Non-main branches and arbitrary historical snapshots start as pure-query reads
over history. Per-branch projections, sparse branch overlays, and query-shaped
indexes are serving indexes only; they are not required for correctness.

`$rowId` is globally unique across all tables.

## System Tables

Sketch:

```sql
CREATE TABLE jazz_node (
  $nodeNum INTEGER PRIMARY KEY,
  $nodeId TEXT NOT NULL UNIQUE
);

CREATE TABLE jazz_tx (
  $txId TEXT PRIMARY KEY,
  $nodeNum INTEGER NOT NULL,
  $localEpoch INTEGER NOT NULL,
  $globalEpoch INTEGER,
  $baseGlobalEpoch INTEGER NOT NULL,
  $baseLocalJsonb BLOB NOT NULL,
  $baseIncludeJsonb BLOB NOT NULL,
  $status TEXT NOT NULL, -- local_pending | edge_durable | global_durable | rejected
  $createdAt INTEGER NOT NULL,
  $sealedAt INTEGER,
  $metadataJson TEXT NOT NULL,
  UNIQUE ($nodeNum, $localEpoch),
  UNIQUE ($globalEpoch)
);

CREATE TABLE jazz_branch (
  $branchId TEXT PRIMARY KEY,
  $name TEXT NOT NULL,
  $headGlobalEpoch INTEGER NOT NULL,
  $headLocalJsonb BLOB NOT NULL,
  $headIncludeJsonb BLOB NOT NULL,
  $baseProvenanceJsonb BLOB NOT NULL
);

CREATE TABLE jazz_schema (
  $schemaHash TEXT PRIMARY KEY,
  $schemaJson TEXT NOT NULL
);
```

Indexes:

```sql
CREATE INDEX jazz_tx_global_epoch_idx
ON jazz_tx($globalEpoch)
WHERE $globalEpoch IS NOT NULL;

CREATE INDEX jazz_tx_node_local_idx
ON jazz_tx($nodeNum, $localEpoch);

CREATE INDEX jazz_tx_status_idx
ON jazz_tx($status);
```

## User Tables

History table sketch:

```sql
CREATE TABLE todos__schema_v1_history (
  $rowId TEXT NOT NULL,
  $branchId TEXT NOT NULL,
  $txId TEXT NOT NULL,
  $op TEXT NOT NULL, -- insert | update | delete
  $parentTxIdsJson TEXT NOT NULL,

  title TEXT,
  done INTEGER,

  $createdBy TEXT,
  $createdAt INTEGER NOT NULL,
  $updatedBy TEXT,
  $updatedAt INTEGER NOT NULL,
  $editMetadataJson TEXT NOT NULL,

  PRIMARY KEY ($rowId, $branchId, $txId),
  FOREIGN KEY ($txId) REFERENCES jazz_tx($txId)
);
```

Main current projection sketch:

```sql
CREATE TABLE todos__schema_v1_current (
  $rowId TEXT NOT NULL,
  $branchId TEXT NOT NULL,
  $visibleTxId TEXT NOT NULL,
  $isDeleted INTEGER NOT NULL,

  title TEXT,
  done INTEGER,

  $createdBy TEXT,
  $createdAt INTEGER NOT NULL,
  $updatedBy TEXT,
  $updatedAt INTEGER NOT NULL,
  $editMetadataJson TEXT NOT NULL,

  PRIMARY KEY ($rowId, $branchId)
);

CREATE INDEX todos__schema_v1_current_done_created_at
  ON todos__schema_v1_current($branchId, done, $createdAt DESC);
```

System columns such as `$createdAt`, `$updatedAt`, `$createdBy`, and
`$updatedBy` are queryable anywhere ordinary user columns are queryable.

Physical column names prefixed with `$` are owned by the database engine. The
implementation may quote or encode those names differently for SQLite; the
prefix is the semantic marker in this spec.

## Transactions

The only write unit is a transaction. One write call creates one sealed
transaction. Multi-row writes use the same transaction abstraction.

A transaction can be:

- `mergeable / eventually consistent`: accepted independently, later merged per
  column
- `exclusive / globally consistent`: authority-serialized and globally
  validated; conflicts or constraint violations may reject it

Transaction identity has two layers:

- `$txId` is stable public identity.
- `($nodeId, $localEpoch)` is assigned immediately by the writer.
- `$globalEpoch` is assigned later by the authority when the transaction becomes
  globally known.

When the authority accepts a local transaction, it broadcasts a fate/mapping
event:

```text
TxAccepted {
  txId: <same id>,
  nodeNum: 7,
  nodeId: alice_device,
  localEpoch: 21,
  globalEpoch: 1057,
  fate: accepted,
}
```

Receivers update `jazz_tx.$globalEpoch` for the existing transaction row.
References do not need to rename from local to global; compact vectors can
simply prefer global coordinates after the mapping is known.

Rejected transactions are marked rejected. Visibility predicates must check
transaction fate/status so rejected transactions are not made visible merely
because they are below a local vector base.

For v0, merge resolution may be last-writer-wins, but it should already be
per-column. History may contain multiple visible candidates; current
projections store derived merge resolution for the branch/snapshot they serve.

Insert lowering sketch:

```sql
BEGIN IMMEDIATE;

INSERT INTO jazz_tx (
  $txId, $nodeNum, $localEpoch, $baseGlobalEpoch, $baseLocalJsonb,
  $baseIncludeJsonb, $status,
  $createdAt, $sealedAt, $metadataJson
) VALUES (
  :txId, :nodeNum, :localEpoch, :baseGlobalEpoch, :baseLocalJsonb,
  :baseIncludeJsonb, 'local_pending',
  :now, :now, :metadataJson
);

INSERT INTO todos__schema_v1_history (
  $rowId, $branchId, $txId, $op, $parentTxIdsJson,
  title, done,
  $createdBy, $createdAt, $updatedBy, $updatedAt, $editMetadataJson
) VALUES (
  :rowId, 'main', :txId, 'insert', '[]',
  :title, 0,
  :actorId, :now, :actorId, :now, :editMetadataJson
);

INSERT INTO todos__schema_v1_current (
  $rowId, $branchId, $visibleTxId, $isDeleted,
  title, done,
  $createdBy, $createdAt, $updatedBy, $updatedAt, $editMetadataJson
) VALUES (
  :rowId, 'main', :txId, 0,
  :title, 0,
  :actorId, :now, :actorId, :now, :editMetadataJson
);

COMMIT;
```

## Version Vectors

The snapshot/read-set representation is a compact, additive dotted version
vector:

```ts
type TxRef = { txId: string } | { global: number } | { node: string; local: number };

type VersionVector = {
  globalBase: number;
  localBases?: Record<string, number>;
  include?: TxRef[];
};
```

Semantics:

- `globalBase` includes all globally assigned transactions through that epoch.
- `localBases[node]` includes locally durable transactions for that node through
  that local epoch, whether or not they have global epochs yet.
- `include` contains sparse positive dots beyond the bases.
- There is no `exclude` in v0.
- Vectors are closed: they do not point to other vectors/snapshots.

Vectors are stored inline where they are owned:

- transactions store their read/base vector
- branches store their effective head vector
- subscriptions/resume state store their ack/read vectors

There is no general snapshot table in the baseline. Named snapshots can later
be metadata rows containing the same scalar + JSONB vector fields.

SQLite storage uses scalar columns for hot/common fields and SQLite JSONB BLOBs
for uncommon vector parts:

```text
local bases: [[nodeNum, localBaseEpoch], ...]
include dots: [{ "txId": "..." }, { "global": 45 }, { "node": 7, "local": 21 }]
```

The hot path should not run `json_each()` over vector JSONB for every candidate
history row. Query execution decodes the one snapshot vector once into scalar
bindings, generated `OR` predicates, or tiny temp tables:

```sql
CREATE TEMP TABLE snapshot_node_base (
  $nodeNum INTEGER PRIMARY KEY,
  $localBaseEpoch INTEGER NOT NULL
);

CREATE TEMP TABLE snapshot_include_tx (
  $txId TEXT PRIMARY KEY
);
```

Visibility predicate:

```sql
-- A transaction is visible in snapshot S if:
-- 1. it is globally durable with $globalEpoch <= S.$globalBaseEpoch, or
-- 2. it is local to a node with $localEpoch <= S.local_base(node), or
-- 3. it is explicitly included by S.
-- Rejected transactions are never visible.
```

Read sets use the same vector shape. They may over-approximate, but must not
omit any transaction whose visible row version or policy dependency affected
the result.

## Branches

A branch's visible content is defined by an effective closed version vector, not
by copying the whole database.

Accepted branch transactions become global history but remain isolated to their
branch. Global history is not equivalent to visibility on `main`; content
becomes visible across branches only through explicit merge-commit
transactions.

Branches may be created from multiple bases:

```text
createBranch feature_branch from:
  main@globalBase 42
  design_branch@head
  import_branch@snapshot abc
```

The stored branch keeps both:

- precise base provenance in `$baseProvenanceJsonb`
- a flattened effective head vector used for query-time visibility

Base conflicts are a to-be-tried decision: represent conflicts between bases as
multiple visible candidates until a later merge-resolution transaction, rather
than resolving them immediately during branch creation/flattening.

## Query Execution

Current `main` reads should use current projections:

```sql
SELECT $rowId, title, done, $createdAt
FROM todos__schema_v1_current
WHERE $branchId = :branchId
  AND $isDeleted = 0
  AND done = 0
  AND $createdAt > :yesterday
ORDER BY $createdAt DESC;
```

Queries used for subscriptions and upstream sync can add engine-only scope
outputs. Those outputs are not exposed to application code.

```sql
SELECT
  $rowId,
  title,
  done,
  $createdAt,

  json_array(
    json_object(
      'kind', 'result',
      'table', 'todos',
      'schema', 'schema_v1',
      'branch', $branchId,
      'rowId', $rowId,
      'txId', $visibleTxId
    )
  ) AS $resultScopeJson,

  json_array() AS $policyScopeJson
FROM todos__schema_v1_current
WHERE $branchId = :branchId
  AND $isDeleted = 0
  AND done = 0
  AND $createdAt > :yesterday
ORDER BY $createdAt DESC;
```

The JSON scope columns are illustrative. The implementation may use hidden
columns, a second result set, temporary tables, or a Rust side channel.

Pagination scope includes exactly the visible page in v0:

```sql
WITH page AS (
  SELECT $rowId, $branchId, $visibleTxId
  FROM todos__schema_v1_current
  WHERE $branchId = :branchId
    AND $isDeleted = 0
    AND done = 0
    AND $createdAt > :yesterday
  ORDER BY $createdAt DESC
  LIMIT 20
)
SELECT 'todos', 'schema_v1', $branchId, $rowId, $visibleTxId, 'result'
FROM page;
```

Live subscriptions can still notice rows entering or leaving the page because
the full SQL is rerun after relevant writes. Upstream sync can rerun and send
the new page scope after relevant upstream changes.

Includes and joins add result dependencies. A result containing a todo with its
project is not locally reproducible unless both rows are present:

```sql
SELECT
  t.$rowId AS todo_id,
  t.title,
  p.$rowId AS project_id,
  p.name AS project_name,

  json_array(
    json_object(
      'kind', 'result',
      'table', 'todos',
      'schema', 'schema_v1',
      'branch', t.$branchId,
      'rowId', t.$rowId,
      'txId', t.$visibleTxId
    ),
    json_object(
      'kind', 'result_dependency',
      'table', 'projects',
      'schema', 'schema_v1',
      'branch', p.$branchId,
      'rowId', p.$rowId,
      'txId', p.$visibleTxId
    )
  ) AS $resultScopeJson
FROM todos__schema_v1_current t
JOIN projects__schema_v1_current p
  ON p.$branchId = t.$branchId
 AND p.$rowId = t.project_id
 AND p.$isDeleted = 0
WHERE t.$branchId = :branchId
  AND t.$isDeleted = 0
  AND t.done = 0;
```

Policies add policy dependencies separately from result dependencies:

```sql
WITH candidate_todos AS (
  SELECT *
  FROM todos__schema_v1_current
  WHERE $branchId = :branchId
    AND $isDeleted = 0
    AND done = 0
),
authorized AS (
  SELECT
    t.*,
    p.$rowId AS $policyProjectRowId,
    p.$visibleTxId AS $policyProjectTxId
  FROM candidate_todos t
  JOIN projects__schema_v1_current p
    ON p.$branchId = t.$branchId
   AND p.$rowId = t.project_id
   AND p.$isDeleted = 0
  WHERE p.owner_id = :session_account_id
)
SELECT
  $rowId,
  title,
  done,
  json_array(
    json_object(
      'kind', 'policy_dependency',
      'table', 'projects',
      'schema', 'schema_v1',
      'branch', $branchId,
      'rowId', $policyProjectRowId,
      'txId', $policyProjectTxId,
      'operation', 'read'
    )
  ) AS $policyScopeJson
FROM authorized;
```

Branch/time-travel reads start as pure-query history reads with a snapshot
predicate. The exact SQL is still a sketch:

```sql
SELECT *
FROM todos__schema_v1_history h
JOIN jazz_tx tx ON tx.$txId = h.$txId
WHERE h.$branchId = :branchId
  AND jazz_tx_visible_in_snapshot(tx.$txId, :snapshot_vector)
  AND NOT EXISTS (
    SELECT 1
    FROM todos__schema_v1_history newer
    JOIN jazz_tx newer_tx ON newer_tx.$txId = newer.$txId
    WHERE newer.$rowId = h.$rowId
      AND newer.$branchId = h.$branchId
      AND jazz_tx_visible_in_snapshot(newer_tx.$txId, :snapshot_vector)
      AND jazz_tx_happens_after(newer.$txId, h.$txId)
  );
```

Pure-query history snapshots are the correctness baseline. Optional serving
indexes are reserved for hot branch/snapshot paths.

## Subscriptions

Subscriptions are part of the first local implementation slice.

The first engine reruns compiled SQL and diffs full result rows:

1. Store original query AST, compiled SQL, previous ordered rows, scope, and
   dependency metadata.
2. After a Jazz write commits in SQLite, the applicator records touched
   tables/rows/columns/branches/schemas/transactions.
3. The subscription manager reruns potentially affected SQL.
4. It diffs previous and new ordered full result rows.
5. It publishes added/updated/removed/moved rows and updates scope.

No SQLite triggers or hooks should be semantic machinery. The Jazz applicator
already has the necessary write-set information.

Subscription state should include at least:

```text
subscription_id
original query AST
compiled SQL
last result rows, in order
last result scope
last policy scope
tables/columns it may depend on
```

Full-row diffs are intentional: listeners should eventually be able to receive
semantic diffs, not only membership changes.

## Sync Scope

Sync remains query-scoped. Lower-tier nodes forward writes upward and await
durability acks. They also forward desired queries upward.

An upstream tier executes lowered SQL and sends enough data for the lower tier
to reproduce the result locally at the requested durability tier. It does not
need to send the app-facing result as source of truth.

Sync scope includes:

- rows in the result set
- joined/include dependency rows
- policy dependency rows
- any other rows needed to recreate the query result

For v0, sync can send the full history of result/dependency rows. Policy
dependency rows are sent to ordinary clients. Opaque authorization proofs are
future work.

For paginated queries, v0 sync scope includes only the current page. Live
subscriptions rerun on the upstream tier and send the new page scope when
relevant changes occur.

Reconnect can start by replaying desired subscriptions and comparing known
transaction ids.

For a simple current read, upstream sync scope can be represented as row-version
locators:

```sql
WITH visible_result AS (
  SELECT
    $rowId,
    $branchId,
    $visibleTxId
  FROM todos__schema_v1_current
  WHERE $branchId = :branchId
    AND $isDeleted = 0
    AND done = 0
    AND $createdAt > :yesterday
  ORDER BY $createdAt DESC
)
SELECT
  'todos' AS $tableName,
  'schema_v1' AS $schemaHash,
  $branchId,
  $rowId,
  $visibleTxId AS $txId,
  'result' AS $reason
FROM visible_result;
```

The sender expands `(table, schema, branch, $rowId, $txId)` into the history
and current-projection messages required by the sync protocol.

## Includes, Policies, and Constraints

Includes are read in the same consistency snapshot as their parent query.

If an included child is required by the type, a missing child filters out the
parent. If it is optional, the included value is `null`.

Policies lower into SQL. Policy dependencies are tracked separately from result
dependencies so sync can send rows needed to justify authorization decisions.

Example policy shape:

```ts
import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy, session, allowedTo }) => [
  policy.projects.allowRead.where({ ownerId: session.user_id }),
  policy.todos.allowRead.where(allowedTo.read("projectId")),
]);
```

Foreign-key constraints are enforced by the authority for exclusive/globally
consistent transactions. Mergeable transactions can be accepted and reconciled
without local SQLite FK enforcement.

## Schemas and Lenses

Each structural schema version gets its own SQLite history/current table shape.

Lenses must be SQL-lowerable in the first implementation. Reads over a newer
schema can union native rows with lens-translated rows from older schema tables.

Example rename lens shape:

```text
schema v1:
  todos.title
  todos.done

schema v2:
  tasks.text       -- renamed from todos.title
  tasks.completed  -- renamed from todos.done
```

Read lowering sketch:

```sql
WITH native_v2 AS (
  SELECT
    $rowId,
    $branchId,
    $visibleTxId,
    text,
    completed
  FROM tasks__schema_v2_current
  WHERE $branchId = :branchId
    AND $isDeleted = 0
),
translated_v1 AS (
  SELECT
    $rowId,
    $branchId,
    $visibleTxId,
    title AS text,
    done AS completed
  FROM todos__schema_v1_current
  WHERE $branchId = :branchId
    AND $isDeleted = 0
)
SELECT *
FROM native_v2
UNION ALL
SELECT *
FROM translated_v1;
```

Writes through a lens create a new row version in the writer's current schema
table rather than mutating the source schema table.

If versions of the same logical row exist in different schema tables:

- mergeable transactions merge with translation
- exclusive transactions are authority-decided; conflicting same-row updates can
  be rejected like same-schema exclusive conflicts

Schema, catalogue, permissions, and lens changes should ideally be represented
as transactions in the same history/sync system.

## Initial Parity Ladder

1. Single-table local CRUD, one branch, one schema, no policies.
2. One-shot current reads via SQLite, with result-scope capture.
3. Subscriptions via rerun+diff.
4. Two-table joins/includes with explicit result dependencies.
5. Local durable restart.
6. Upstream query forwarding with result sync scope.
7. Reconnect/replay from durable tx/history tables.
8. Simple policies with separate policy scope.
9. Pure-query branch/time-travel snapshots.
10. Schema lenses.
11. Multi-write transactions and conflict reconciliation.
12. Optional serving indexes for hot snapshot/branch reads.
13. Recursive/inherited policies and complex sync scopes.

## Performance Targets

Initial targets:

- current reads comfortably below 1 ms for early datasets
- pure-query branch/time-travel snapshot reads below roughly 50 ms at 100k rows
  are acceptable

## Implementation Strategy

The implementation should be driven by a deterministic multi-tier harness before
it grows production transport or storage adapters. The harness is the main tool
for making local-first/distributed behavior precise.

The harness should model:

- client main thread
- client worker
- edge tier
- core authority
- durable and ephemeral storage nodes
- fast reliable IPC-like links
- slower unreliable network links

Each simulated node should run the same core state machine against a pluggable
storage adapter:

```text
Node {
  nodeId
  role: client_main | client_worker | edge | core
  storage: durable_sqlite | ephemeral_sqlite | in_memory
  inbox
  outbox
  clock
}
```

Transport should be explicit and scriptable:

```text
Link {
  from
  to
  reliability
  latency_model
  partition_state
}
```

The first harness does not need real networking. It should let tests enqueue
messages, deliver them in chosen orders, drop them, duplicate them, partition
links, restart nodes, and inspect durable state.

Development should proceed as vertical slices through this harness:

1. Single durable node: schema registration, insert/update/delete, current
   projection, restart.
2. Local subscriptions on one node: rerun+diff after writes, including system
   column filters.
3. Two local nodes with explicit message delivery: local transaction creation,
   forwarding, authority acceptance, local-to-global mapping broadcast.
4. Query-scoped sync: lower-tier subscription forwarded upward, result rows and
   dependency rows sent downward, local reproduction asserted.
5. Reconnect: desired subscriptions replayed, known transaction/vector state
   compared, missing rows/fates repaired.
6. Branches: branch creation from a closed vector, branch-local writes,
   merge-commit visibility, multiple base provenance.
7. Policies/includes: required vs optional includes, result dependencies,
   policy dependencies.
8. Schema lenses: read through SQL-lowerable lenses, write new versions in the
   writer's schema version.

Each slice should include invariants over the whole simulated system:

- every visible row version has an accepted/non-rejected transaction
- current projections can be rebuilt from history
- a synced query result is reproducible from the receiver's local rows
- subscription diffs match rerunning the query from scratch
- branch visibility is explainable by the branch effective vector
- local-to-global transaction mappings never change `$txId`
- reconnect converges without duplicate visible effects

Benchmarks should come after these semantics are executable. The first
benchmarks should be harness scenarios promoted into measurement mode rather
than isolated microbenchmarks, with focused SQLite microbenchmarks added only
when a scenario identifies a concrete hot path.

## Open Questions and Underspecified Areas

### Version Vectors

- What are the exact canonical compaction rules for durable vectors?
- When can a local dot/base be rewritten or omitted after a global epoch mapping
  is known?
- Should `include` dots be normalized to `$txId`, global epoch, or node-local
  coordinate in storage and on the wire?
- Is `JSONB` acceptable for all targets, especially browser/WASM SQLite, or do
  we need a fallback representation?
- Can most visibility checks compile to indexed range predicates, or do we need
  custom SQLite functions or side tables?
- Do broadcasts need both `$nodeId/$localEpoch` and `$txId`, or can peers always
  resolve one from the other before applying the mapping?

### Snapshot Query Performance

- Should history rows denormalize `$nodeNum`, `$localEpoch`, `$globalEpoch`, and
  `$txStatus` to avoid joining `jazz_tx` for every candidate row?
- What is the best SQL shape for "latest visible version per row": `NOT EXISTS`,
  window functions, grouped max keys, or another shape?
- What native sort order/indexes should history tables use for snapshot reads?
- When do we introduce optional branch overlays or hot-branch projections?
- Should query-time decoded vectors use temp tables, generated `OR` clauses, or
  bound table-valued mechanisms?

### Branches

- How exactly do we flatten multiple base vectors into the smallest safe closed
  vector?
- What is the precise `$baseProvenanceJsonb` shape?
- How do multiple visible candidates flow through reads, subscriptions, and sync
  before a merge-resolution transaction exists?
- What does a merge-commit transaction contain when it makes content visible
  across branches?
- How does branch-local isolated global history interact with authority ordering
  and global epoch assignment?

### Transactions and Conflict Resolution

- What is the concrete data model for per-column merge candidates?
- How is last-writer-wins ordered: global epoch, local epoch, created time, or
  explicit merge metadata?
- What is the exact acceptance flow for mergeable vs exclusive transactions?
- How are rejected local transactions removed from current projections and
  subscription results?
- Can one sealed transaction touch multiple tables and schema versions in v0?

### Current Projections

- Is the main current projection one table per schema version only, or does it
  also need per-branch columns/rows for `main` sub-branches?
- How are current projections repaired/rebuilt after crash or corruption?
- Should current projections store only resolved visible rows, or also conflict
  candidate metadata?

### Sync Scope

- What exact representation replaces the example `$resultScopeJson` and
  `$policyScopeJson`: hidden columns, second result sets, temp tables, or a Rust
  side channel?
- For v0, is sending full history for result/dependency rows always acceptable?
- What does a reconnect "known transaction ids" summary look like when vectors
  can represent compact ranges?
- How much policy dependency history is required: current visible row only, or
  enough history to replay authorization at a requested snapshot?
- Do policy dependency rows need to be opaque in some environments sooner than
  planned?

### Subscriptions

- How broad can invalidation be before rerun+diff is too expensive?
- What dependency metadata should compiled subscriptions store?
- How are semantic diffs represented when rows have conflict candidates or lens
  translations?
- Should subscription read sets become durable resume tokens?

### Policies, Relations, and Constraints

- Which policy language subset is SQL-lowerable in v0?
- How are recursive/inherited policies represented without reintroducing a query
  graph?
- How should optional vs required includes interact with authorization failure
  versus missing data?
- Are FK constraints only authority-side for exclusive transactions, or do we
  still want local advisory checks for developer feedback?

### Schemas and Lenses

- What is the physical identity of a logical row across schema-version tables?
- How are duplicate logical row versions across schema tables detected and
  resolved efficiently?
- Can all v0 lenses be expressed as SQL projections/unions, or do we need
  restricted expression forms?
- Should schema/catalogue/lens/permission changes be user-visible transactions
  from day one or introduced later?

### Implementation and Testing

- What minimal Rust API should sit between Jazz semantics and SQLite so SQLite
  can later be replaced or augmented?
- How should the multi-tier test harness model client main thread, worker, edge,
  and core authorities?
- Which benchmarks are required before choosing between pure-query snapshots,
  denormalized history coordinates, and serving indexes?
- What SQLite version is required for JSONB across native and WASM targets?
