# Embedded Database Lowering

## 26. Embedded Database Lowering

This section describes the selected lowering strategy for SQLite-like embedded
databases.

Physical storage baseline:

- local integer surrogates for hot keys
- integer enum discriminants, not text labels
- composite primary keys with `WITHOUT ROWID` where useful
- generated covering and partial indexes
- current projection for hot main reads
- columnar current projection tables
- JSONB-style user payloads for append-only history as the first storage
  experiment
- query-time visibility for historical and branch correctness baselines

Main-branch current projection is the recommended hot-read surface. It carries
real disk overhead, but gives predictable indexed reads for ordinary product
screens. Historical snapshots, arbitrary time travel, and pinned branch base
views may initially use slower query-time visibility unless measurements justify
promoting a derived projection or specialized historical index.

Sealed history blocks are a later storage optimization, not a separate semantic
history lane. When introduced, their lowering should sit beside the open history
tables with explicit block metadata, format version, covered transaction/epoch
ranges, covered rows, and integrity hashes. Query and export planning must be
able to combine open rows and sealed blocks for the same logical row scope.
Applying a sync payload that contains open history, sealed blocks, and sidecars
should go through one coherent delta application boundary.

### 26.1 Transaction Tables

Sketch:

```sql
CREATE TABLE jazz_tx (
  tx_num INTEGER PRIMARY KEY,
  tx_id TEXT NOT NULL UNIQUE,
  node_num INTEGER NOT NULL,
  local_epoch INTEGER NOT NULL,
  global_epoch INTEGER,
  kind INTEGER NOT NULL,
  conflict_mode INTEGER NOT NULL,
  outcome INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  metadata_blob BLOB NOT NULL,
  UNIQUE (node_num, local_epoch)
);

CREATE TABLE jazz_tx_receipt (
  tx_num INTEGER NOT NULL,
  tier INTEGER NOT NULL,
  observed_at INTEGER NOT NULL,
  authority_node_num INTEGER,
  receipt_blob BLOB,
  PRIMARY KEY (tx_num, tier)
) WITHOUT ROWID;

CREATE TABLE jazz_tx_rejection (
  tx_num INTEGER PRIMARY KEY,
  code INTEGER NOT NULL,
  detail_blob BLOB NOT NULL
);

CREATE TABLE jazz_tx_awaiting_dependency (
  tx_num INTEGER PRIMARY KEY,
  auth_user TEXT NOT NULL,
  detail_blob BLOB NOT NULL,
  updated_at INTEGER NOT NULL
);
```

This sketch encodes the v2 split between outcome, durability receipt, and
rejection detail. `jazz_tx_awaiting_dependency` is the selected prototype
lowering for `awaiting_deps`: the hot transaction outcome remains `pending`,
while the side table records the durable wait reason and the user context needed
to re-run authority policy validation after missing facts arrive.

`global_epoch` is intentionally not unique. Multiple transactions may share one
authority epoch. Indexes should support lookup/order by `(global_epoch, tx_num)`
or equivalent stable tie-breaker.

### 26.2 History And Current Tables

Sketch:

```sql
CREATE TABLE todos_v1_history (
  row_num INTEGER NOT NULL,
  branch_num INTEGER NOT NULL,
  tx_num INTEGER NOT NULL,
  op INTEGER NOT NULL,
  layout_num INTEGER NOT NULL,

  values_jsonb BLOB NOT NULL,

  j_created_at INTEGER NOT NULL,
  j_updated_at INTEGER NOT NULL,
  j_conflict_blob BLOB,
  j_edit_metadata_blob BLOB,

  PRIMARY KEY (row_num, branch_num, tx_num)
) WITHOUT ROWID;

CREATE TABLE todos_v1_current (
  row_num INTEGER NOT NULL,
  branch_num INTEGER NOT NULL,
  visible_tx_num INTEGER NOT NULL,
  is_deleted INTEGER NOT NULL,

  title TEXT,
  done INTEGER,
  project_row_num INTEGER,

  j_created_at INTEGER NOT NULL,
  j_updated_at INTEGER NOT NULL,
  j_conflict_blob BLOB,
  j_edit_metadata_blob BLOB,

  PRIMARY KEY (row_num, branch_num)
) WITHOUT ROWID;
```

History keeps system columns, identity columns, branch/transaction keys, and
ordering keys relational. User values are shown as `values_jsonb` to reflect the
first layout experiment: store cold history payloads as inspectable JSONB-style
data while keeping current projection columnar. SQLite may represent this as
JSON text, JSONB when available, or an equivalent binary payload; the product
contract is that history is append-only and semantically decodable through the
catalogue/lens graph.

Current projection tables keep user fields as real SQLite columns because they
serve hot reads, policy filters, subscriptions, explicit indexes, and common
query plans. Generated or side indexes over history payloads should be added
only when measurements show a hot historical query, conflict lookup, or
authority-validation path needs them.

Storage compression should target whole SQLite pages or larger ordered ranges,
not individual row payloads. Per-row history payload compression has too little
compression window for the expected complexity. History table ordering and
primary keys should therefore be chosen with compression locality in mind:
nearby pages should tend to contain related table/layout/row/history data so
redundant append-only history can compress well. Custom VFS/page compression is
a serious storage research target despite portability cost across browser,
native, and server runtimes.

### 26.3 Branch View Tables

Sketch:

```sql
CREATE TABLE jazz_branch (
  branch_num INTEGER PRIMARY KEY,
  branch_id TEXT NOT NULL UNIQUE,
  current_head_tx_num INTEGER,
  source_version INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE jazz_branch_history (
  branch_num INTEGER NOT NULL,
  tx_num INTEGER NOT NULL,
  op INTEGER NOT NULL,
  provenance_blob BLOB NOT NULL,
  PRIMARY KEY (branch_num, tx_num)
) WITHOUT ROWID;

CREATE TABLE jazz_branch_source (
  branch_num INTEGER NOT NULL,
  source_ordinal INTEGER NOT NULL,
  source_branch_num INTEGER NOT NULL,
  source_global_epoch INTEGER,
  source_vector_blob BLOB,
  precedence INTEGER NOT NULL,
  provenance_ref_blob BLOB,
  PRIMARY KEY (branch_num, source_ordinal)
) WITHOUT ROWID;
```

### 26.4 Identity Mapping

Logical mappings:

```sql
CREATE TABLE jazz_node (
  node_num INTEGER PRIMARY KEY,
  node_id TEXT NOT NULL UNIQUE
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

The implementation may combine identity mapping with hot tables when the
public/physical boundary remains clear.
Public row ids are globally unique across application tables. A row id may be
mentioned by an unresolved reference before the target row exists, but table
ownership is claimed only when history/current state for that row is inserted.
After a row id is owned by one table, incoming sync or local writes must reject
attempts to use the same public row id as an owned row in another table.

Identity and ordinal mapping updates must be crash-safe. A crash must not leave
torn public-id/physical-id, branch-id/branch-ordinal, or source-list mappings
that can later hydrate the same public identity to two different physical
identities or attach branch provenance to the wrong branch. SQLite transactions
should be used as the atomicity boundary for all such mapping updates.

### 26.5 Indexes

Indexes are part of the lowering plan, not handwritten per feature.

The planner should generate:

- point lookup indexes for row identity
- covering indexes for current queries
- history indexes for system keys needed by rebuild, sync, snapshots, branches,
  and conflict candidate lookup
- partial indexes for selective predicates
- authority-validation indexes when read sets become hot

Example:

```sql
CREATE INDEX todos_v1_current_open_created
  ON todos_v1_current(branch_num, done, j_created_at DESC, row_num);
```

Observable query ordering must use semantic tie-breakers. Physical row numbers
may appear in indexes and joins, but unordered reads and equal ordered-page keys
should tie-break by public row id or an equivalent semantic key so replicas that
apply the same history in different physical order converge on the same visible
ordering.

Performance tests should retain `EXPLAIN QUERY PLAN` output for risky lowerings.

Generated indexes must remain compatible with lenses. A covering index generated
for one structural schema may not directly serve another schema view.

Automatic user-field indexes should target current projection first. History
payloads are not the default query/index surface. If a historical field becomes
hot, the engine may add a generated SQLite expression index, maintained side
table, materialized historical projection, or copy-forward layout. Such derived
history indexes must be maintained in the same embedded-database transaction as
the history append or incoming-sync apply, and should be driven by explicit
schema/query intent or measured hot paths rather than generated for every
JSONB payload field.

Current-projection lowering should cover ordinary supported indexable query
forms, including equality, `IN`, selected semantic system-field predicates, and
ordered top-N pages. Historical and branch snapshot fallbacks may be slower, but
current reads should not silently degrade into full visible-row filtering when a
declared indexable predicate is available.

Performance risks:

- mapping tables add insert and boundary lookup cost
- inline transaction metadata may become expensive for authority validation
- broad projection repair may be too slow after sync application/rejection
- rerun-and-diff subscriptions may be too coarse for large result sets
- predicate/range scope may become too large
- generated indexes may overfit query shapes and inflate writes
