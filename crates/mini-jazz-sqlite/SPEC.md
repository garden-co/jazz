# Jazz on SQLite core

## Intro/goal

In this crate we'll try to implement from scratch a fully-functional version of jazz (as per the surrounding repo)
but using much less code.

The idea is that jazz implements a local-first/distributed, permissioned, syncing, real-time, branching, multi-schema
database by lowering its semantics into SQLite queries, maintaining subscriptions and peers and sending sync messages.

We also use this as an opportunity to heavily simplify jazz and its own semantics, given the organically grown mess
with lots of redundancies, overlapping concepts and performance traps the current codebase is.

## Mapping of existing concepts

### History and edit metadata

> Current Jazz stores each logical row as row-batch history plus a separate
> visible-row projection. A concrete history entry is identified by
> `(row_id, branch_name, batch_id)` and contains reserved `_jazz_*` engine
> fields, row state, parent batch ids, durability/tier metadata, delete markers,
> provenance/edit metadata, and user columns. Current reads usually do not scan
> history; they load a compact `VisibleRowEntry` for `(branch_name, row_id)`.
> History and visible entries are stored in schema-qualified raw table instances,
> and exact routing uses system locator tables such as
> `__visible_row_table_locator` and `__history_row_batch_table_locator`.
> Storage also maintains Jazz-owned secondary indices separately from the
> visible/history payloads.

The first layer we add on top of SQLite is that inserts, updates and deletes get converted into append-only
history tables in SQLite for every high-level jazz table.

What is currently a separate persisted current-state area of the raw storage arenas will instead become a
(close-to-covering) index.

### Distributed transactions and branching

> Current Jazz has two write lifecycles. Direct writes are treated as
> one-member batches that become optimistically visible immediately, then later
> receive authoritative batch fate. Transactional writes stage
> `StoredRowBatch` entries as `StagingPending`, are sealed explicitly, and only
> become visible if the authority accepts the whole batch. Branches are carried
> through composed branch names and branch-local visible/history keys rather
> than through a global MVCC snapshot model. Conflict handling happens per row by
> recomputing the visible entry from row history/frontiers, using schema-declared
> merge strategies such as `lww` and `counter`. Batch fate is durable and
> batch-scoped; successful fate applies to the whole sealed batch, and rejection
> removes staged/conflicted rows from ordinary visibility.

Transactions and branches are somewhat ill-defined and incomplete in Jazz currently. We'll use this opportunity to create
precise semantics based on dotted version vectors that allow us to implement global MVCC snapshots for both
transaction and branch startpoints. The idea is that row versions and transaction read/write sets as well as
branch start points usually consist of a global base epoch, plus select global or local individual epoch idxs (representing)
individual transactions.

Transaction and branch read isolation is then provided by augmenting queries with additional filters so they only
see stuff from their tx/branch.

We will need extra system tables for transactions (including id, snapshot base, global sync state/fate and edit metadata) and branches (including snapshot base and id)

### Sync and reconciliation

> Current Jazz sync is query-scoped. A client registers a desired query
> subscription; the upstream runtime records it, compiles a server-side query
> graph with the client's schema/session context, settles it against visible
> rows, sends the needed row batch entries, sends batch fate, and finally emits
> `QuerySettled` for the requested durability tier. The graph stays alive after
> the initial fill. Later local or remote row changes dirty relevant graph nodes;
> settling the graph computes which rows entered, changed, or left scope, and
> sync sends only affected rows/fate. Reconnect treats subscriptions as desired
> state: forwarded subscriptions are replayed upstream and scope is rebuilt.
> Transport code does not evaluate policies itself; the Query Manager owns
> filtering, ordering, policy checks, and sync-scope computation.

The concept of sync stays the same. Lower-tier nodes forward created rows upwards and await durability acks.
Lower-tier nodes also forward queries upwards, which get executed on each upward tier, which captures not the
result rows but all rows necessary to sync down so the client gets results as of the desired tier once all rows
are received and the client executes the query locally.

This necessitates a further modification/lowering of the queries on the higher tiers to capture sync scope.

This all should also work in the reconciliation sense, where sync is intelligently resumed after disconnects.

### Multi-schema and migration lenses

> Current Jazz identifies each structural schema by content hash. User row
> history and visible rows are stored in schema-qualified raw table instances,
> and runtime branch names are composed from environment, schema hash, and user
> branch. The Schema Manager keeps known schemas, live schema sets, and lens
> paths. Queries are issued against the client's current schema view; when older
> stored rows are reachable, lenses translate table/column/value shapes on read.
> Writes to older rows are intentionally copy-on-write into the current schema
> branch. Schemas, lenses, and permission bundles live in catalogue state rather
> than ordinary user history. Servers may learn schemas dynamically from clients
> and enforce only once the matching permissions head/bundle is available.

The idea is that each schema version becomes one underlying SQLite table, migration lenses are then implemented
by just lowering the translation work into queries when accessing them "as of" a specific schema version.

### Setup, development, testing and benchmarking

It would be nice to set up a much more principled development harness from the beginning, where we can
simulate complicated multi tier setups (client main thread ↔ client worker ↔ edge ↔ core ↔ edge ↔ client...)
with durable storage (almost everywhere) and ephemeral in memory storage (client main thread)
message exchange (fast reliable IPC-ish between client main thread and worker, unreliable slow networking between everywhere else)
and then have complex scenarios and assertions over the whole thing, Jepsen/prop-testing/FoundationDB style

## SQLite schema, index and query concept overview

TODO: this section should take an example Jazz schema with multiple tables, permissions, branches and schema versions and then spell out
which corresponding SQLite schema would need to be generated, how high-level jazz queries would translate to expanded SQLite queries, etc.

## Worked examples playground

This section is intentionally concrete and pseudocode-heavy. The goal is not to
specify the final implementation yet; it is to make the semantics visible enough
that we can edit examples until the model feels obvious.

Each example should spell out:

- the high-level Jazz schema/API call
- the generated SQLite tables/indexes
- the write lowering
- the read lowering
- the sync-scope lowering
- the subscription invalidation story
- the unresolved questions it exposes

### Example 1: one table, one branch, one schema, no policies

High-level schema:

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

High-level usage:

```ts
const todo = await db.insert(app.todos, {
  title: "Write the SQLite lowering",
  done: false,
});

const openTodos = await db.all(
  app.todos
    .where({ done: false })
    .where({ $createdAt: { gt: yesterday } })
    .orderBy("$createdAt", "desc"),
);

const unsubscribe = db.subscribeAll(
  app.todos
    .where({ done: false })
    .where({ $createdAt: { gt: yesterday } })
    .orderBy("$createdAt", "desc"),
  ({ all }) => {
    render(all);
  },
);
```

#### Generated SQLite shape

For the first example, assume:

- one schema hash: `schema_v1`
- one logical branch: `main`
- one local site id: `alice_device`
- one monotonically increasing local tx clock per site

In the SQLite sketches, any column prefixed with `$` is owned by Jazz rather
than the application schema. The final implementation may quote or encode those
identifiers differently for SQLite; the prefix is the semantic marker.

System tables:

```sql
CREATE TABLE jazz_site (
  $siteId TEXT PRIMARY KEY
);

CREATE TABLE jazz_tx (
  $txId TEXT PRIMARY KEY,
  $siteId TEXT NOT NULL,
  $siteTx INTEGER NOT NULL,
  $baseGlobalTx INTEGER,
  $status TEXT NOT NULL, -- local_pending | edge_durable | global_durable | rejected
  $createdAt INTEGER NOT NULL,
  $sealedAt INTEGER,
  $metadataJson TEXT NOT NULL,
  UNIQUE ($siteId, $siteTx)
);

CREATE TABLE jazz_branch (
  $branchId TEXT PRIMARY KEY,
  $name TEXT NOT NULL,
  $headVectorJson TEXT NOT NULL
);

CREATE TABLE jazz_schema (
  $schemaHash TEXT PRIMARY KEY,
  $schemaJson TEXT NOT NULL
);
```

User history table:

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

Optional current-state projection table:

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

The current table is a serving optimization, not a semantic requirement. The
simplest implementation can answer ordinary reads, branch reads, and time-travel
reads directly from history using snapshot predicates. Benchmarks suggest that
pure-query snapshot reads are acceptable for the row counts we expect early on.

We should therefore start with history as the only required row store. Current
tables, sparse branch overlays, and per-hot-branch projections can be introduced
later as serving indexes for hot paths without changing the logical model.

#### Insert lowering

High-level insert:

```ts
db.insert(app.todos, { title, done: false });
```

Lowers to one SQLite transaction:

```sql
BEGIN IMMEDIATE;

INSERT INTO jazz_tx (
  $txId, $siteId, $siteTx, $baseGlobalTx, $status,
  $createdAt, $sealedAt, $metadataJson
) VALUES (
  :txId, :siteId, :siteTx, :baseGlobalTx, 'local_pending',
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

The runtime also records an in-memory invalidation:

```text
changed table: todos
changed row: (main, $rowId)
changed columns: $rowId, title, done, $createdAt, $updatedAt
changed tx: $txId
```

Open question: should invalidation be derived from SQLite hooks/triggers, or
from the Jazz write path after the SQLite transaction commits?

#### One-shot read lowering

High-level read:

```ts
db.all(
  app.todos
    .where({ done: false })
    .where({ $createdAt: { gt: yesterday } })
    .orderBy("$createdAt", "desc"),
);
```

App-facing SQL:

```sql
SELECT
  $rowId,
  title,
  done,
  $createdAt
FROM todos__schema_v1_current
WHERE $branchId = :branchId
  AND $isDeleted = 0
  AND done = 0
  AND $createdAt > :yesterday
ORDER BY $createdAt DESC;
```

Sync/provenance-aware SQL:

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

The app never sees the `$resultScopeJson` or `$policyScopeJson` columns. They are for local
subscription diffing and upstream query-scope capture.

Open question: should scope be encoded as JSON for simplicity, or returned as a
second result set / temp table for speed?

#### Subscription lowering

Subscription registration stores:

```text
subscription_id
original Jazz query AST
compiled SQL
last result row ids, in order
last result scope
last policy scope
tables/columns it may depend on
```

Initial subscribe:

1. Run the provenance-aware SQL.
2. Strip hidden scope columns and publish rows.
3. Store ordered row ids and scope sets.

On local write:

1. Jazz write path commits SQLite transaction.
2. Runtime records changed table/row/columns.
3. Subscription manager finds subscriptions depending on `todos`.
4. Rerun compiled SQL.
5. Diff previous ordered result against new ordered result.
6. Publish added/updated/removed/moved rows.
7. Update sync scope.

This is intentionally less clever than the current query graph. The bet is that
SQLite's planner and indexes make rerun+diff fast enough for many workloads, and
we can later add query-specific invalidation shortcuts where needed.

Open question: do subscriptions diff full result rows, row ids plus visible tx
ids, or a stable result hash per row?

#### Upstream query sync lowering

When a lower tier forwards this query upward, the upper tier does not need to
send the app-facing result as the source of truth. It needs to send the rows
required for the lower tier to reproduce the result locally at the requested
durability tier.

For this example, result scope is enough:

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

The sender then expands `(table, schema, branch, $rowId, $txId)` into the
corresponding history/current messages required by the sync protocol.

Open question: does sync transmit the exact visible tx, all frontier txs, or the
minimal history slice needed to reconstruct the visible row?

### Example 2: same query with pagination

High-level query:

```ts
db.all(
  app.todos
    .where({ done: false })
    .where({ $createdAt: { gt: yesterday } })
    .orderBy("$createdAt", "desc")
    .limit(20),
);
```

App-facing SQL:

```sql
SELECT $rowId, title, done, $createdAt
FROM todos__schema_v1_current
WHERE $branchId = :branchId
  AND $isDeleted = 0
  AND done = 0
  AND $createdAt > :yesterday
ORDER BY $createdAt DESC
LIMIT 20;
```

Sync-scope SQL should usually include exactly the visible page, not every row
matching the unbounded predicate:

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

But live subscriptions need to know when rows just outside the page may enter
after a delete/update. The simple rerun+diff model handles that locally because
the full SQL is rerun. For sync, the upper tier can also rerun and send the new
page scope after each relevant upstream change.

Open question: should paginated sync scope include only the current page, or an
extra lookahead row/page to reduce churn?

### Example 3: two tables and explicit result provenance

Schema:

```ts
import { schema as s } from "jazz-tools";

const schema = {
  projects: s.table({
    name: s.string(),
  }),

  todos: s
    .table({
      projectId: s.ref("projects"),
      title: s.string(),
      done: s.boolean(),
    })
    .indexOnly(["projectId", "done", "$createdAt"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
```

High-level query:

```ts
db.all(app.todos.where({ done: false }).include({ project: true }));
```

Lowering:

```sql
SELECT
  t.$rowId AS todo_id,
  t.title,
  t.done,
  t.$createdAt,
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
  ) AS $resultScopeJson,

  json_array() AS $policyScopeJson
FROM todos__schema_v1_current t
JOIN projects__schema_v1_current p
  ON p.$branchId = t.$branchId
 AND p.$rowId = t.project_id
 AND p.$isDeleted = 0
WHERE t.$branchId = :branchId
  AND t.$isDeleted = 0
  AND t.done = 0;
```

This makes the sync contract explicit: the query result is not reproducible
unless both the todo rows and joined project rows are present locally.

Open question: for `include`, should missing joined rows filter out the parent,
or produce `null`/pending nested data while sync catches up?

### Example 4: simple row policy with separate policy scope

Schema:

```ts
import { schema as s } from "jazz-tools";

const schema = {
  projects: s.table({
    name: s.string(),
    ownerId: s.string(),
  }),

  todos: s.table({
    projectId: s.ref("projects"),
    title: s.string(),
    done: s.boolean(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
```

Policy:

```ts
import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy, session, allowedTo }) => [
  policy.projects.allowRead.where({ ownerId: session.user_id }),
  policy.todos.allowRead.where(allowedTo.read("projectId")),
]);
```

High-level query:

```ts
db.all(app.todos.where({ done: false }), { session: alice });
```

Lowering:

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
      'kind', 'result',
      'table', 'todos',
      'schema', 'schema_v1',
      'branch', $branchId,
      'rowId', $rowId,
      'txId', $visibleTxId
    )
  ) AS $resultScopeJson,

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

This distinguishes:

- result scope: rows needed to materialize the app result
- policy scope: rows needed to justify the authorization decision

For sync, both scopes may need to be sent down. A client cannot safely reproduce
the query at the requested tier unless it has the result rows and the policy
dependency rows that prove visibility.

Open question: should policy dependencies be synced to ordinary clients, or
should some clients receive opaque authorization proofs instead of policy rows?

### Example 5: schema v2 with a rename lens

Schema v1:

```ts
todos: {
  title: s.string(),
  done: s.boolean(),
}
```

Schema v2:

```ts
tasks: {
  text: s.string(),      // renamed from todos.title
  completed: s.boolean(), // renamed from todos.done
}
```

Physical tables:

```sql
todos__schema_v1_history
todos__schema_v1_current
tasks__schema_v2_history
tasks__schema_v2_current
```

A v2 read over data that may still live in v1 can lower to a union of native v2
rows and lens-translated v1 rows:

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

If a v2 client updates a translated v1 row, the write path should write a new
v2 history/current row rather than mutating the v1 table. That matches the
current intent: read old rows through a lens, write new versions in the current
schema.

Open question: how do we prevent duplicate logical rows from appearing in both
v1-translated and native-v2 branches after a row has been upgraded?

### Example 6: branch/snapshot filter sketch

This is deliberately less settled than the earlier examples.

Instead of treating `$branchId` as enough, a branch read eventually needs a
snapshot predicate:

```sql
SELECT *
FROM todos__schema_v1_history h
JOIN jazz_tx tx ON tx.$txId = h.$txId
WHERE h.$branchId = :branchId
  AND jazz_tx_visible_in_snapshot(tx.$txId, :snapshot_vector_json)
  AND NOT EXISTS (
    SELECT 1
    FROM todos__schema_v1_history newer
    JOIN jazz_tx newer_tx ON newer_tx.$txId = newer.$txId
    WHERE newer.$rowId = h.$rowId
      AND newer.$branchId = h.$branchId
      AND jazz_tx_visible_in_snapshot(newer_tx.$txId, :snapshot_vector_json)
      AND jazz_tx_happens_after(newer.$txId, h.$txId)
  );
```

For the first implementation, this pure-query shape is the intended baseline.
It keeps branch creation cheap, avoids per-branch projection management, and
lets the exact snapshot semantics stay visible in SQL. Early benchmarks over
100k base rows, 1k branches, and 200k total history versions put indexed
history snapshot reads in the tens of milliseconds. That is acceptable for the
row counts we are targeting initially.

Open question: can dotted-version-vector visibility be compiled into ordinary
range predicates over `($globalEpoch, $siteId, $siteTx)` often enough, or does it
require custom SQLite functions / side tables?

Later optimization options:

- query-shaped history indexes for common filters/orders
- sparse branch overlays: one shared base-current table plus per-branch changed
  rows
- per-hot-branch current projections for opened or server-hot branches

These should be treated as serving indexes. They must not be required for
correctness.

## Parity ladder

Rather than trying to match all of Jazz at once, use the examples as a ladder:

1. Single-table local CRUD, one branch, one schema, no policies.
2. One-shot reads via SQLite, with result-scope columns.
3. Subscriptions via rerun+diff.
4. Two-table joins/includes with explicit result dependencies.
5. Local durable restart.
6. Upstream query forwarding with result sync scope.
7. Reconnect/replay from durable tx/history tables.
8. Simple policies with separate policy scope.
9. Pure-query branch/time-travel snapshots.
10. Schema lenses.
11. Transactional batches and conflict reconciliation.
12. Optional serving indexes for hot snapshot/branch reads.
13. Recursive/inherited policies and complex sync scopes.
