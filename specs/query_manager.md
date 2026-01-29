# QueryManager Architecture

## Overview

The QueryManager layer provides reactive SQL queries over Jazz2's object-based storage. Each row is a Jazz object; indices are local-only B-trees with page-based storage; queries compile to incremental computation graphs that emit row deltas.

```
┌─────────────────────────────────────────────────────────┐
│                    Application                          │
│              (Value for input/output)                   │
├─────────────────────────────────────────────────────────┤
│                   QueryManager                          │
│  ┌─────────────┐  ┌──────────┐  ┌───────────────────┐  │
│  │ Row Codec   │  │ Indices  │  │ Query Graph       │  │
│  │ encode at   │  │ (source  │  │ (binary rows      │  │
│  │ boundary    │  │ nodes)   │  │  throughout)      │  │
│  └─────────────┘  └──────────┘  └───────────────────┘  │
├─────────────────────────────────────────────────────────┤
│                   SyncManager                           │
├─────────────────────────────────────────────────────────┤
│                  ObjectManager                          │
│            (+ global object subscription)               │
└─────────────────────────────────────────────────────────┘
```

## Core Design Decisions

- **Row = Object**: Each row is a separate Jazz object; ObjectId = primary key
- **Row format**: Fixed fields first, then variable offsets, nullable 1-byte prefix
- **Binary throughout**: `Value` only at API boundary; internally everything is `&[u8]` with `RowDescriptor`
- **Index-first**: No table scans; every query uses an index as source (including "_id" for unfiltered)
- **Auto-index all columns**: Every column gets a single-column B-tree index (zero-config)
- **All indices persisted**: Every index is persisted as B-tree pages in storage (not as Jazz objects)
- **No index rebuild**: Indices are incrementally maintained; if missing on startup, that's an error state
- **Query graph**: Unified `TupleDelta` type with progressive materialization throughout
- **Branch-aware**: Indices are keyed by (table, column, branch); queries specify target branch(es)

## The "_id" Index IS the Row Manifest

The `_id` index for each table serves as the authoritative list of row ObjectIds. This eliminates the need for a separate manifest object.

**Key principle:** Durability of a new row requires persisting BOTH:
1. The row object itself
2. The `_id` index update (and all column index updates)

If any index is lost, that's an error state. We do NOT rebuild indices from rows.

**On cold start:**
1. Load `_id` index for each table → discover all row ObjectIds
2. Load all column indices for each table
3. Load row objects as needed (lazy, via query - NOT eagerly into a cache)

## Index Storage Model

Indices are stored as B-tree pages directly in storage (not as Jazz objects):
- **Much lower overhead**: ~20 bytes per page vs ~530 bytes per Object-wrapped node
- **Higher fanout**: ~64 entries per leaf page means fewer storage operations
- **Incremental updates**: Only modified pages are written
- **Lazy loading**: Pages loaded on demand as queries traverse the tree

### Storage Key Format

```
index:{table}:{column}:{branch}:meta      → IndexMeta (root page id, entry count)
index:{table}:{column}:{branch}:page:{id} → Page data
```

### Page Format (Binary)

**Internal node:**
```
[type: u8 = 0]
[key_count: u16]
[children: PageId × (key_count + 1)]  // PageId = u64
[keys: (len: u16, data: bytes) × key_count]
```

**Leaf node:**
```
[type: u8 = 1]
[entry_count: u16]
[entries × entry_count]:
  [key_len: u16][key: bytes]
  [row_count: u32][row_ids: 16 bytes × row_count]
```

## B-Tree Index Architecture

```rust
pub struct BTreeIndex {
    table: String,
    column: String,
    branch: String,

    meta: IndexMeta,
    meta_loaded: bool,
    meta_dirty: bool,

    pages: HashMap<PageId, PageState>,
    dirty_pages: HashSet<PageId>,
    deleted_pages: HashSet<PageId>,
    pending_requests: Vec<StorageRequest>,
}

impl BTreeIndex {
    pub fn new(table: &str, column: &str, branch: &str) -> Self;
    pub fn insert(&mut self, key: &[u8], row_id: ObjectId) -> Result<bool, IndexError>;
    pub fn remove(&mut self, key: &[u8], row_id: ObjectId) -> Result<(), IndexError>;
    pub fn lookup_exact(&self, key: &[u8]) -> Vec<ObjectId>;
    pub fn range_scan(&self, min: &Bound<Vec<u8>>, max: &Bound<Vec<u8>>) -> Vec<ObjectId>;
    pub fn scan_all(&self) -> Vec<ObjectId>;
    pub fn contains_row(&self, row_id: ObjectId) -> bool;
    pub fn take_storage_requests(&mut self) -> Vec<StorageRequest>;
    pub fn process_meta_load(&mut self, data: Option<Vec<u8>>);
    pub fn process_page_load(&mut self, page_id: PageId, data: Option<Vec<u8>>);
    pub fn reset_for_cold_start(&mut self);
}
```

## Branch-Aware Queries

Queries are branch-aware:
- Each branch maintains its own set of indices: `(table, column, branch)`
- Queries must specify branch(es) via `.branch("main")` or `.branches(&["main", "draft"])`
- Multi-branch queries combine results with LWW (last-writer-wins) merge for same ObjectId
- Subscriptions inherit branch scope from their query

```rust
// Single branch query
let query = qm.query("users").branch("main").build();

// Multi-branch query (union with LWW)
let query = qm.query("users").branches(&["main", "draft"]).build();
```

## Query Graph Architecture

The query graph uses a unified tuple model with progressive materialization. All nodes work with `TupleDelta`.

### Tuple Model

```rust
pub enum TupleElement {
    Id(ObjectId),
    Row { id: ObjectId, content: Vec<u8>, commit_id: CommitId },
}

pub struct Tuple(Vec<TupleElement>);  // Hash/Eq based on IDs only

pub struct TupleDelta {
    pub added: Vec<Tuple>,
    pub removed: Vec<Tuple>,
    pub updated: Vec<(Tuple, Tuple)>,  // (old, new) - same IDs, different content
    pub pending: bool,
}
```

### Node Traits

**Source nodes** (`SourceNode`) read from external state:
- `IndexScanNode` - scans indices, returns length-1 tuples with `TupleElement::Id`

**Transform nodes** (`TransformNode`) operate on tuple sets:
- `UnionNode` - merges tuple sets for OR conditions

**Row nodes** (`RowNode`) operate on `TupleDelta`:
- `MaterializeNode` - converts `TupleElement::Id` → `TupleElement::Row`
- `FilterNode` - filters tuples by predicate
- `SortNode` - orders tuples
- `LimitOffsetNode` - pagination
- `JoinNode` - equi-joins with hash indices
- `ArraySubqueryNode` - correlated subqueries producing array columns
- `OutputNode` - terminal node, delivers results

### Graph Pipeline

**Single-table query:**
```
IndexScan(table,branch) → [Union] → Materialize → [Filter] → [Sort] → [LimitOffset] → Output
     ↓                                  ↓              ↓          ↓           ↓            ↓
  TupleDelta                       TupleDelta      TupleDelta  TupleDelta  TupleDelta   TupleDelta
  mat: [false]                     mat: [true]     mat: [true] mat: [true] mat: [true]  mat: [true]
```

**Join query:**
```
IndexScan(users) → Materialize ──┐
                                 ├──→ JoinNode → Materialize({1}) → [Filter] → Output
         posts index (lookup) ───┘
                                    mat: [true,false]  mat: [true,true]
```

## Query API

```rust
// Single-table query
let query = qm.query("users")
    .branch("main")
    .filter_eq("status", Value::Text("active".into()))
    .order_by_desc("score")
    .limit(10)
    .build();

// With projection
let query = qm.query("users")
    .branch("main")
    .select(&["name", "email"])
    .build();

// Join query
let query = qm.query("users")
    .branch("main")
    .alias("u")
    .join("posts")
    .alias("p")
    .on("u.id", "p.author_id")
    .select(&["u.name", "p.title"])
    .build();

// Array subquery (correlated subquery producing array column)
let query = qm.query("users")
    .branch("main")
    .with_array("posts", |sub| {
        sub.from("posts")
           .correlate("author_id", "users.id")
           .select(&["id", "title"])
           .order_by_desc("created_at")
           .limit(10)
    })
    .build();

// Nested array subqueries
let query = qm.query("users")
    .branch("main")
    .with_array("posts", |sub| {
        sub.from("posts")
           .correlate("author_id", "users.id")
           .with_array("comments", |sub2| {
               sub2.from("comments")
                   .correlate("post_id", "posts.id")
           })
    })
    .build();
```

### Query Struct

```rust
pub struct Query {
    pub table: TableName,
    pub branches: Vec<String>,
    pub alias: Option<String>,
    pub joins: Vec<JoinSpec>,
    pub disjuncts: Vec<Conjunction>,
    pub order_by: Vec<(String, SortDirection)>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub include_deleted: bool,
    pub select_columns: Option<Vec<String>>,
    pub array_subqueries: Vec<ArraySubquerySpec>,
}
```

## Deletion Semantics

| Type | Content | Metadata | `_id_deleted` | Undeletable | Authoritative |
|------|---------|----------|---------------|-------------|---------------|
| Soft Delete | Preserved | `delete: soft` | Added | Yes | No |
| Hard Delete | Empty | `delete: hard` | Removed | No | Yes (always wins) |

- `_id` index: Live rows only
- `_id_deleted` index: Soft-deleted rows only (with preserved content)
- Hard-deleted rows appear in neither index
- `include_deleted()` queries return both live and soft-deleted rows

## Policy Evaluation (ReBAC)

Policies are evaluated asynchronously via PolicyGraphs for complex clauses (EXISTS, INHERITS).

```rust
// Simple policy (synchronous)
INSERT policy: owner_id = @user_id

// Complex policy with EXISTS (async)
SELECT policy: EXISTS(SELECT 1 FROM memberships WHERE team_id = documents.team_id AND user_id = @user_id)

// Complex policy with INHERITS (async)
SELECT policy: INHERITS SELECT VIA folder_id
```

Session propagates through multi-tier sync for permissioned queries.

## File Structure

```
crates/groove/src/query_manager/
├── mod.rs
├── manager.rs            # QueryManager struct and methods
├── manager_tests.rs      # Integration tests
├── types.rs              # ColumnType, RowDescriptor, Row, TupleElement, Tuple, TupleDelta
├── encoding.rs           # encode/decode (boundary), binary ops (internal)
├── query.rs              # Query, QueryBuilder, ArraySubqueryBuilder, Value
├── graph.rs              # Graph structure, dirty marking, settling
├── session.rs            # Session for ReBAC
├── policy.rs             # Policy evaluation
├── policy_graph.rs       # PolicyGraph for async policy evaluation
├── rebac_tests.rs        # ReBAC integration tests
├── index/
│   ├── mod.rs
│   ├── btree_index.rs    # B-tree index with page-based storage
│   └── btree_page.rs     # Page types and serialization
└── graph_nodes/
    ├── mod.rs            # SourceNode, TransformNode, RowNode traits
    ├── index_scan.rs     # Source node
    ├── union.rs          # OR conditions
    ├── materialize.rs    # Progressive materialization
    ├── filter.rs         # In-memory filter
    ├── sort.rs           # Sorting
    ├── limit_offset.rs   # Pagination
    ├── output.rs         # Terminal node
    ├── alias.rs          # Table aliasing
    ├── project.rs        # Column selection
    ├── join.rs           # Equi-join
    ├── subgraph.rs       # SubgraphTemplate for correlated subqueries
    ├── array_subquery.rs # Array expressions
    ├── policy_filter.rs  # Policy-based filtering
    └── exists_output.rs  # EXISTS clause evaluation
```

---

## Dynamic Schema Context

QueryManager supports dynamic schema activation without recreation. This preserves active subscriptions and indices when new schema versions become available.

### Initialization Pattern

```rust
// 1. Create QueryManager with empty context
let mut qm = QueryManager::new(sync_manager);

// 2. Set the current schema (only once)
qm.set_current_schema(schema, "dev", "main");

// 3. Add live schemas dynamically (can be called anytime)
qm.add_live_schema(old_schema);
qm.register_lens(lens);
```

### Key Behaviors

**Schema Context Always Present**
- `schema_context` is non-optional (starts empty, initialized by `set_current_schema`)
- All code paths use schema-aware branch handling
- `DEFAULT_ROW_BRANCH` has been removed; branches are always derived from schema context

**Subscription Recompilation**
- When `add_live_schema()` or `register_lens()` is called, subscriptions are marked for recompile
- On next `process()`, stale subscriptions rebuild their QueryGraph with updated branch lists
- Original `Query` is stored in subscription for recompilation
- Subscription IDs remain stable across recompilation

**Branch Name Composition**
- Branch names follow format: `"{env}-{hash8}-{user_branch}"`
- Example: `"dev-a1b2c3d4-main"` for env="dev", hash=a1b2..., user_branch="main"
- All live schemas get their own branch name
- Queries without explicit `.branch()` use schema context's branches automatically

**Pending Row Buffer**
- Rows arriving on unknown branches are buffered in `pending_row_updates`
- When a schema activates, buffered rows are retried via `retry_pending_row_updates()`
- This handles rows arriving before their schema is discovered via catalogue sync

### Branch Propagation Through Query Graphs

All query graph nodes receive explicit branch information. There is no implicit "main" default anywhere in production code.

**IndexScanNode**
- Always constructed with explicit branch: `IndexScanNode::new_with_branch(table, column, branch, condition, descriptor)`
- Index lookups use the composed key `(table, column, branch)`
- The `new()` method exists for tests only (uses "main")

**Joins**
- `compile_join()` receives branches from the outer query
- Join correlation lookups use the same branch list as the main query
- Inner table index scans use schema-aware branch names

**Array Subqueries**
- `SubgraphTemplate::instantiate()` copies branches from the base query
- This ensures correlated subqueries query the same branch set as their parent
- Nested subqueries inherit branches recursively

**PolicyGraph and PolicyFilter**
- PolicyGraph functions take explicit branch: `for_using_check(table, id, policy, session, schema, branch)`
- PolicyFilterNode stores branch for INHERITS evaluation: `new_with_branch(..., branch)`
- EXISTS clauses in policies use the row's source branch for index lookups

### Server Subscriptions

Server subscriptions (created via `subscribe_server_query`) handle remote clients:

```rust
struct ServerQuerySubscription {
    query: Query,
    graph: QueryGraph,
    session: Option<Session>,
    branches: Vec<String>,        // Resolved at creation time
    last_scope: HashSet<(ObjectId, BranchName)>,
    needs_recompile: bool,
}
```

**Branch Resolution**
- When `query.branches` is empty (common case), branches are resolved from schema context
- This resolved list is stored in the subscription and used for settling
- Recompilation updates the branch list from the current schema context

**Scope Tracking**
- `last_scope` tracks `(ObjectId, BranchName)` pairs for change detection
- Row updates emit deltas based on scope differences
- Branch-aware scope ensures correct delta emission across schema versions

### SchemaManager Integration

SchemaManager wraps QueryManager and provides the high-level API:

```rust
// SchemaManager::new() internally calls:
// - QueryManager::new(sync_manager)
// - qm.set_current_schema(schema, env, user_branch)

// add_live_schema() updates both context and QueryManager
sm.add_live_schema(old_schema)?;  // Auto-updates QueryManager

// process() handles catalogue sync and auto-activates schemas
sm.process();  // No more sync_context() needed
```

The old `sync_context()` method has been removed. Schema changes flow incrementally through `add_live_schema()` and `register_lens()`.

---

## Known Limitations & Future Work

### High Priority

- **UPDATE with INHERITS chains**: UPDATE may fail with PolicyDenied even when INHERITS chain should grant access. Needs investigation of PolicyGraph wiring for UPDATE USING.

- **ORDER BY + LIMIT subscriptions**: Subscriptions with `ORDER BY ... LIMIT N` don't receive incremental updates when new rows enter the top N. LimitOffsetNode may need rework or a dedicated TopN node.

### Medium Priority

- **Self-referential INHERITS**: Currently disallowed. Common pattern for hierarchical data (folders → parent_id) needs iterative settlement with depth limit.

- **UUID format mismatch**: Session claim comparison uses Debug format which may differ from JSON string format, causing false negatives in IN checks.

### Low Priority

- **project_row memcpy optimization**: Currently decodes to Value then re-encodes. Should memcpy bytes directly for fixed-size columns.

- **subscribe_full API**: Only delta-mode subscriptions exposed. OutputMode::Full exists but isn't wired to API.

- **NOT(complex_clause) semantics**: Meaning of NOT(INHERITS(...)) is unclear; needs documentation or semantic fix.
