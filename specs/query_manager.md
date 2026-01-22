# QueryManager Architecture

## Overview

The QueryManager layer provides reactive SQL queries over Jazz2's object-based storage. Each row is a Jazz object; indices are local-only skip lists; queries compile to incremental computation graphs that emit row deltas.

## Architecture

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
- **Auto-index all columns**: Every column gets a single-column skip list index (zero-config)
- **All indices persisted**: Every index (including column indices) is persisted as Jazz objects
- **No index rebuild**: Indices are incrementally maintained; if missing on startup, that's an error state
- **Indices**: Skip lists, node-per-object, `nosync: "true"` metadata, persisted locally only
- **Query graph**: Unified `TupleDelta` type with progressive materialization throughout

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

## Index Storage Model (Node-per-Object)

Each skip list node is stored as a separate Jazz object:
- **Root sentinel**: ObjectId = `index_root_id(table, column)` (deterministic via UUID v5)
- **Data nodes**: ObjectId = newly generated on creation
- **Forward pointers**: Stored as `Vec<Option<ObjectId>>` - natural fit for Jazz
- **Content**: Binary-encoded `SkipListNode`

### Index Object Metadata

All index objects have metadata marking them as local-only:
```rust
{
    "type": "index",
    "nosync": "true",
    "index_table": "users",
    "index_column": "email"  // or "_id" for primary index
}
```

The sync layer filters out objects with `nosync: "true"` when syncing to remotes.

## Zero-Copy Index Architecture

### Design Principles

1. **Single source of truth**: ObjectManager holds all index node data
2. **Zero-copy reads**: `SkipListNodeView` reads directly from `commit.content` without allocating
3. **Immediate persistence**: Mutations persist immediately to ObjectManager (no buffered node state)
4. **Queue insert intents**: When index not ready, queue `(key, row_id)` for replay later
5. **No global state machine**: QueryManager has no Setup/Ready states - operations work immediately
6. **Lazy loading**: Index data is read from ObjectManager on demand

### ObjectManager as Source of Truth

ObjectManager is designed to be a fast in-memory store with lazy loading from persistence. This means:

1. **No caches on top**: Components consuming ObjectManager should NOT create their own caches of object content. ObjectManager already handles in-memory storage efficiently.

2. **Handle not-yet-loaded state**: Consumers must handle the case where an object isn't loaded yet (ObjectState::Loading or missing). This is normal operation, not an error.

3. **Listen for load completion**: Use ObjectManager's subscription mechanisms (global subscription, object updates) to react when objects become available. Don't poll or eagerly load.

The row_loader closure passed to MaterializeNode should access ObjectManager directly, returning None for rows not yet loaded. The query graph already handles this gracefully.

### SkipListNodeView (Zero-Copy)

```rust
/// Zero-copy view into a skip list node's encoded data.
pub struct SkipListNodeView<'a> {
    data: &'a [u8],
    key_end: usize,       // Key is at 2..key_end
    row_count: u32,
    rows_start: usize,    // Row IDs start here
    forward_start: usize, // Forward pointers start here
    level: u8,
    forward_count: u8,
}

impl<'a> SkipListNodeView<'a> {
    /// Zero-copy key access.
    pub fn key(&self) -> &'a [u8];

    /// Iterate row IDs without allocating.
    pub fn row_ids(&self) -> impl Iterator<Item = ObjectId> + 'a;

    /// Get forward pointer at level (no allocation).
    pub fn forward(&self, level: usize) -> Option<ObjectId>;

    /// Convert to owned SkipListNode (for mutations).
    pub fn to_owned(&self) -> SkipListNode;
}
```

### IndexState

```rust
pub struct IndexState {
    pub root_id: ObjectId,
    pub table: String,
    pub column: String,
    pending_index_updates: Vec<(Vec<u8>, ObjectId)>,  // Queue of insert intents (key, row_id)
    current_level: usize,
}

impl IndexState {
    /// Get node as zero-copy view. Returns None if not in ObjectManager.
    fn get_node<'a>(&self, node_id: ObjectId, om: &'a ObjectManager) -> Option<SkipListNodeView<'a>>;

    /// Check if the index root exists in ObjectManager.
    pub fn root_exists(&self, om: &ObjectManager) -> bool;

    /// Take pending index updates (for replay when index becomes ready).
    pub fn take_pending_updates(&mut self) -> Vec<(Vec<u8>, ObjectId)>;

    /// Check if there are pending updates.
    pub fn has_pending_updates(&self) -> bool;

    /// Flush pending updates - replay queued inserts when index becomes ready.
    pub fn flush_pending(&mut self, om: &mut ObjectManager) -> Result<(), IndexError>;
}
```

### Mutation Flow

Mutations persist immediately to ObjectManager. If the index isn't ready (sentinel doesn't exist), the insert intent is queued for later replay:

```rust
impl IndexState {
    /// Insert a row into the index.
    /// Returns Ok(true) if inserted, Ok(false) if queued (index not ready).
    pub fn insert(&mut self, key: &[u8], row_id: ObjectId, om: &mut ObjectManager)
        -> Result<bool, IndexError>;

    /// Remove a row from the index. Persists immediately.
    pub fn remove(&mut self, key: &[u8], row_id: ObjectId, om: &mut ObjectManager)
        -> Result<(), IndexError>;
}
```

Read-only traversal methods take `&ObjectManager`:
- `lookup_exact(key, om)` - Exact match lookup
- `range_scan(min, max, om)` - Range scan
- `scan_all(om)` - Full index scan

### InsertHandle

```rust
/// Handle for tracking insert durability.
pub struct InsertHandle {
    pub row_id: ObjectId,
    pub row_commit_id: CommitId,
}

impl InsertHandle {
    /// Check if row object is persisted.
    pub fn is_complete(&self, qm: &QueryManager) -> bool;

    /// Check if the row appears in the table's _id index.
    pub fn is_indexed(&self, qm: &QueryManager, table: &str) -> bool;
}
```

### process() Method

```rust
impl QueryManager {
    /// Drive all async progress: object updates, pending index flushes, subscription settling.
    pub fn process(&mut self) {
        // 1. Process object updates from SyncManager
        // 2. Flush pending index updates for indices that became ready
        // 3. Settle subscriptions (row data loaded on-demand from ObjectManager)
    }

    /// Flush pending index updates for indices that became ready.
    fn flush_pending_index_updates(&mut self) {
        // For each index with pending updates where root now exists:
        //   - Take pending updates
        //   - Replay each (key, row_id) insert
    }
}
```

## File Structure

```
crates/groove/src/query_manager/
├── mod.rs
├── types.rs              # ColumnType, RowDescriptor, Row, TupleElement, Tuple, TupleDelta
├── encoding.rs           # encode/decode (boundary), binary ops (internal)
├── manager.rs            # QueryManager
├── query.rs              # Query, QueryBuilder, ArraySubqueryBuilder, Value (boundary type)
├── graph.rs              # Graph structure, dirty marking, settling
├── index/
│   ├── mod.rs
│   └── skip_list.rs
└── graph_nodes/
    ├── mod.rs            # SourceNode, TransformNode, RowNode traits, SourceContext
    ├── index_scan.rs     # Source node (impl SourceNode)
    ├── union.rs          # OR conditions (impl TransformNode)
    ├── materialize.rs    # Progressive materialization (TupleDelta → TupleDelta)
    ├── filter.rs         # In-memory filter (TupleDelta)
    ├── sort.rs           # Sorting (TupleDelta)
    ├── limit_offset.rs   # Pagination (TupleDelta)
    ├── output.rs         # Terminal node (TupleDelta)
    ├── alias.rs          # Table aliasing for self-joins (TupleDelta)
    ├── project.rs        # Column selection (TupleDelta)
    ├── join.rs           # Equi-join with hash indices (TupleDelta)
    ├── subgraph.rs       # SubgraphTemplate for correlated subqueries
    └── array_subquery.rs # ArraySubqueryNode for array expressions
```

## Node Trait Architecture

The query graph uses a unified tuple model with progressive materialization. All nodes work with `TupleDelta` - the difference is whether tuples contain just IDs or fully materialized row data.

### Tuple Model

```rust
/// A single element in a tuple - either just an ID or a fully loaded row.
pub enum TupleElement {
    Id(ObjectId),
    Row { id: ObjectId, content: Vec<u8>, commit_id: CommitId },
}

/// A tuple of elements. Identity based on IDs only (Hash/Eq ignore content).
/// Length corresponds to number of tables (1 for single-table, 2+ for joins).
pub struct Tuple(Vec<TupleElement>);

/// Delta with progressive materialization support.
pub struct TupleDelta {
    pub added: Vec<Tuple>,
    pub removed: Vec<Tuple>,
    pub updated: Vec<(Tuple, Tuple)>,  // (old, new) - same IDs, different content
    pub pending: bool,  // true if any elements still loading
}
```

### Node Traits

Three traits for different node roles:

**Source nodes** (`SourceNode`) read from external state:
- `IndexScanNode` - scans indices, returns length-1 tuples with `TupleElement::Id`
- Require external context (indices, ObjectManager) via `SourceContext`

**Transform nodes** (`TransformNode`) operate on tuple sets (before materialization):
- `UnionNode` - merges tuple sets for OR conditions
- Takes input tuple sets directly, no external state needed

**Row nodes** (`RowNode`) operate on `TupleDelta` (can work with materialized content):
- `MaterializeNode` - converts `TupleElement::Id` → `TupleElement::Row`
- `FilterNode` - filters tuples by predicate (requires materialized content)
- `SortNode` - orders tuples (requires materialized content)
- `LimitOffsetNode` - pagination
- `OutputNode` - terminal node, delivers results

All nodes expose `output_tuple_descriptor()` for descriptor chaining.

```rust
pub struct SourceContext<'a> {
    pub indices: &'a HashMap<(String, String), IndexState>,
    pub om: &'a ObjectManager,
}

pub trait SourceNode {
    fn scan(&mut self, ctx: &SourceContext) -> TupleDelta;
    fn current_tuples(&self) -> &HashSet<Tuple>;
    fn mark_dirty(&mut self);
    fn is_dirty(&self) -> bool;
}

pub trait TransformNode {
    fn process(&mut self, inputs: &[&HashSet<Tuple>]) -> TupleDelta;
    fn current_tuples(&self) -> &HashSet<Tuple>;
    fn mark_dirty(&mut self);
    fn is_dirty(&self) -> bool;
}

pub trait RowNode {
    fn output_descriptor(&self) -> &RowDescriptor;
    fn process(&mut self, input: TupleDelta) -> TupleDelta;
    fn current_tuples(&self) -> &HashSet<Tuple>;
    fn mark_dirty(&mut self);
    fn is_dirty(&self) -> bool;
}
```

### Graph Pipeline

**Single-table query:**
```
IndexScan(table) → [Union] → Materialize → [Filter] → [Sort] → [LimitOffset] → [Project] → Output
     ↓                           ↓              ↓          ↓           ↓            ↓           ↓
  TupleDelta               TupleDelta      TupleDelta  TupleDelta  TupleDelta   TupleDelta  TupleDelta
  mat: [false]             mat: [false]    mat: [true] mat: [true] mat: [true]  mat: [true] mat: [true]
```

**Join query:**
```
IndexScan(users) → Materialize ──┐
                                 ├──→ JoinNode → Materialize({1}) → [Filter] → Output
         posts index (lookup) ───┘
                                    mat: [true,false]  mat: [true,true]
```

Each node receives input descriptor(s), computes its output descriptor, and exposes it via `output_tuple_descriptor()`. Materialization requirements are validated at graph construction time.

The final output converts `TupleDelta` to `RowDelta` via `to_row_delta()` for the subscription API.

### Additional Nodes

**MaterializeNode** - Converts `TupleElement::Id` → `TupleElement::Row` with selective control:
```rust
// Materialize all elements
let mat = MaterializeNode::new_all(input_descriptor);

// Selectively materialize only specified elements
let mat = MaterializeNode::with_elements(input_descriptor, HashSet::from([1]));
```
Updates output descriptor's materialization state for specified elements.

**FilterNode** - Filters tuples with compile-time validation:
```rust
// Returns Err if predicate references unmaterialized elements
let filter = FilterNode::try_new(input_descriptor, predicate)?;
```
Uses `TupleDescriptor::resolve_column()` to find correct element for each predicate column.

**AliasNode** - Transforms table namespace without modifying row data:
```rust
pub struct AliasNode {
    original_table: String,
    alias: String,
    row_descriptor: RowDescriptor,
    combined_descriptor: CombinedRowDescriptor,
    // ...
}
```
Used for self-joins where the same table appears multiple times with different aliases.

**ProjectNode** - Selects a subset of columns:
```rust
pub struct ProjectNode {
    input_descriptor: RowDescriptor,
    output_descriptor: RowDescriptor,
    column_mapping: Vec<(usize, usize)>,  // (src_col, dst_col)
    // ...
}
```
Requires materialized tuples. Re-encodes row data with only selected columns.

**JoinNode** - Performs equi-joins with descriptor propagation:
```rust
pub struct JoinNode {
    output_descriptor: TupleDescriptor,  // Concatenated from left + right
    combined_descriptor: RowDescriptor,
    // Hash index on left for efficient lookup
    left_by_key: HashMap<Vec<u8>, HashSet<Tuple>>,
    // Provenance tracking for reactivity
    left_to_output: HashMap<Vec<ObjectId>, HashSet<Tuple>>,
    right_to_output: HashMap<Vec<ObjectId>, HashSet<Tuple>>,
    // ...
}
```

Constructors:
- `JoinNode::new(left_desc, right_desc, left_col, right_col)` - Takes `TupleDescriptor`s, validates left join column is materialized
- `JoinNode::from_row_descriptors(...)` - Convenience for single-table inputs

Output descriptor is `TupleDescriptor::concat(left, right)` - materialization state is concatenated from both sides. Maintains hash index on left for O(1) lookup. Tracks provenance so removals can efficiently find affected output tuples.

## Query API

### QueryBuilder

```rust
// Single-table query
let query = qm.query("users")
    .filter_eq("status", Value::Text("active".into()))
    .order_by_desc("score")
    .limit(10)
    .build();

// With projection
let query = qm.query("users")
    .select(&["name", "email"])
    .build();

// With alias (for self-joins)
let query = qm.query("users")
    .alias("u1")
    .build();

// Simple join
let query = qm.query("users")
    .join("posts")
    .on("users.id", "posts.author_id")
    .build();

// Join with aliases
let query = qm.query("users")
    .alias("u")
    .join("posts")
    .alias("p")
    .on("u.id", "p.author_id")
    .select(&["u.name", "p.title"])
    .build();

// Self-join (same table twice)
let query = qm.query("employees")
    .alias("e")
    .join("employees")
    .alias("m")
    .on("e.manager_id", "m.id")
    .build();

// Array subquery (correlated subquery producing array column)
let query = qm.query("users")
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
    .with_array("posts", |sub| {
        sub.from("posts")
           .correlate("author_id", "users.id")
           .with_array("comments", |sub2| {
               sub2.from("comments")
                   .correlate("post_id", "posts.id")
           })
    })
    .build();

// Multiple array columns
let query = qm.query("users")
    .with_array("posts", |sub| {
        sub.from("posts").correlate("author_id", "users.id")
    })
    .with_array("comments", |sub| {
        sub.from("comments").correlate("user_id", "users.id")
    })
    .build();

// Join inside array subquery
let query = qm.query("users")
    .with_array("post_comments", |sub| {
        sub.from("posts")
           .join("comments")
           .on("posts.id", "comments.post_id")
           .correlate("author_id", "users.id")
    })
    .build();
```

### Query Struct

```rust
pub struct Query {
    pub table: TableName,
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

pub struct JoinSpec {
    pub table: TableName,
    pub alias: Option<String>,
    pub on: Option<(String, String)>,
}

pub struct ArraySubquerySpec {
    pub column_name: String,
    pub table: TableName,
    pub joins: Vec<JoinSpec>,
    pub inner_column: String,
    pub outer_column: String,
    pub filters: Vec<Condition>,
    pub select_columns: Option<Vec<String>>,
    pub order_by: Vec<(String, SortDirection)>,
    pub limit: Option<usize>,
    pub nested_arrays: Vec<ArraySubquerySpec>,
}
```

## Implementation Status

### Completed
- [x] types.rs - ColumnType (incl. Array, Row), RowDescriptor, Row, TupleElement, Tuple, TupleDelta, Value (incl. Array, Row)
- [x] encoding.rs - binary format + operations (incl. Array/Row encoding)
- [x] Global subscription in object_manager.rs
- [x] skip_list.rs - index with binary keys
- [x] All graph nodes: index_scan, union, materialize, filter, sort, limit_offset, output, alias, project, join, array_subquery, subgraph
- [x] graph.rs - settling algorithm (single-table and join queries)
- [x] manager.rs + query.rs - QueryManager with Value boundary
- [x] Query API: alias(), select(), join(), on(), with_array() methods on QueryBuilder
- [x] Join graph compilation (multi-table queries with JoinNode)
- [x] `create_with_id()` in ObjectManager for deterministic root IDs
- [x] Zero-copy index architecture:
  - `SkipListNodeView` for zero-copy reads from ObjectManager
  - Mutations persist immediately to ObjectManager (no `pending_writes` buffer)
  - `pending_index_updates` queues insert intents when index not ready
  - `flush_pending()` replays queued inserts when index becomes ready
  - Removed `NodeRef` enum - only use `SkipListNodeView` directly
  - Removed `nodes` cache - ObjectManager is single source of truth
  - Removed `QueryManagerState` - no Setup/Ready state machine
  - All traversal methods take `&ObjectManager` parameter
  - Mutation methods take `&mut ObjectManager` and persist immediately
  - `InsertHandle` with `is_complete()` and `is_indexed()` methods
  - Lazy sentinel creation and persistence on first insert
- [x] ObjectManager as sole source of truth for row data:
  - Removed `row_cache` - no redundant caching layer
  - `row_loader` closures access ObjectManager directly
  - Removed `discover_rows_from_indices()` - no eager loading on process()
  - `get()` renamed to `test_get_row_if_loaded()` and gated with `#[cfg(test)]`
  - Production code must use queries to read data

**Zero-Copy Architecture Tests:**
| Test | Status | Location |
|------|--------|----------|
| Node view parses encoded data | ✓ | `skip_list::tests::node_view_parses_encoded_data` |
| Node view key is zero-copy | ✓ | `skip_list::tests::node_view_key_is_zero_copy` |
| Node view iterates row IDs | ✓ | `skip_list::tests::node_view_iterates_row_ids` |
| Node view reads forward pointers | ✓ | `skip_list::tests::node_view_reads_forward_pointers` |
| Insert creates and persists sentinel | ✓ | `skip_list::tests::insert_creates_and_persists_sentinel` |
| Insert returns true when inserted | ✓ | `skip_list::tests::insert_returns_true_when_inserted` |
| Pending updates queue and flush | ✓ | `skip_list::tests::pending_updates_queue_and_flush` |
| Cold start loads indices | ✓ | `manager::tests::cold_start_loads_persisted_indices_and_rows` |
| Cold start doesn't eagerly load rows | ✓ | `manager::tests::cold_start_only_loads_queried_rows` |
| Row is indexed after insert | ✓ | `manager::tests::row_is_indexed_after_insert` |

### Completed Followups

#### Followup 2: nosync Filtering in SyncManager ✓

Index objects have `nosync: "true"` metadata. SyncManager now filters them before syncing to peers:
- `queue_tips_to_server()` and `queue_tips_to_client()` check nosync before sending ObjectUpdated
- `forward_truncation_to_servers()` and `forward_truncation_to_clients_except()` check nosync before sending ObjectTruncated
- Tests in `sync_manager::tests::nosync_*` verify filtering behavior

#### Followup 3: Sync Integration for Row Updates ✓

Extended `AllObjectUpdate` with previous state to enable proper column index updates for synced commits:
- Added `previous_commit_ids: Vec<CommitId>` - previous tips before the update
- Added `old_content: Option<Vec<u8>>` - content of the "winning" tip (last by timestamp)
- `add_commit()` and `receive_commit()` capture previous state before mutation
- `handle_object_update()` uses `old_content` to compute index delta for updates
- First commit on branch (empty `previous_commit_ids`) treated as insert
- Tests: `local_update_updates_all_column_indices`, `synced_update_updates_column_indices`

**TODO:** Currently uses last-writer-wins (newest tip by timestamp). Future work: merge strategies for concurrent updates.

#### Followup 4: Async Row Materialization ✓

When rows are still loading, the system now holds back ALL query results until everything is available:

**Core changes:**
- `RowDelta` gains `pending: bool` field - true when any rows are still loading
- `MaterializeNode` tracks `pending_ids: HashSet<ObjectId>` for rows where loader returns `None`
- `check_pending()` method re-tries pending IDs and emits newly-loaded rows
- All downstream nodes (Filter, Sort, LimitOffset) propagate pending flag unchanged
- `OutputNode` holds back deltas when `pending=true`, emitting full snapshot when pending clears

**OutputNode state tracking:**
- `held_pending: bool` - true when holding back results
- `subscriber_initialized: bool` - true after first snapshot delivered
- `held_changes: RowDelta` - accumulates changes during subsequent pending periods

**Behavior:**
- Initial pending → clears: Emit full `current_rows` as snapshot
- Subsequent pending → clears: Emit only accumulated changes (not full state again)
- Normal (non-pending): Deliver deltas incrementally

**Integration:**
- `graph.rs`: `settle()` calls `check_pending()` before processing new IdDelta, merges results
- `manager.rs`: `mark_subscriptions_with_pending_dirty()` ensures subscriptions with pending IDs are re-settled on each `process()` call

**Tests:**
| Test | Location |
|------|----------|
| MaterializeNode tracks pending | `materialize::tests::materialize_tracks_pending_when_loader_returns_none` |
| Not pending when all loaded | `materialize::tests::materialize_not_pending_when_all_loaded` |
| check_pending emits newly loaded | `materialize::tests::check_pending_emits_newly_loaded_rows` |
| Remove clears from pending | `materialize::tests::remove_clears_from_pending` |
| OutputNode holds back when pending | `output::tests::output_holds_back_when_pending` |
| OutputNode emits full state on clear | `output::tests::output_emits_full_state_when_pending_clears` |
| Subsequent pending emits only changes | `output::tests::output_subsequent_pending_emits_only_new_changes` |

#### Followup 5: Row Content Update Propagation ✓

When a row's content changes (same ObjectId, new commit), subscriptions now receive update deltas:

**Problem solved:**
- `handle_object_update()` had the `object_id` but only marked table-level dirty
- `settle()` only processed `IdDelta` (added/removed) from index scans
- `MaterializeNode.check_update()` existed but was never called for content changes

**Core changes:**
- `MaterializeNode` gains `updated_ids: HashSet<ObjectId>` - IDs to check for content updates
- `mark_updated(id)` method marks an ID for checking (only if already tracked in `rows`)
- `check_updated_ids(loader)` method checks marked IDs and returns `RowDelta` with updates
- `materialize()` clears `updated_ids` for removed IDs

**QueryGraph changes:**
- `mark_row_updated(id)` marks the ID in all MaterializeNodes and propagates dirty marks downstream
- `mark_downstream_dirty(node_id)` helper recursively marks dependent nodes via `reverse_edges`
- `settle()` calls `check_updated_ids()` after `materialize()` and merges update deltas

**QueryManager integration:**
- `mark_row_updated_in_subscriptions(table, id)` calls `graph.mark_row_updated(id)` for matching subscriptions
- Wired into both `update()` (local updates) and `handle_object_update()` (synced updates)

**Filter interaction:**
- Row updated to fail filter → emits removal delta
- Row updated to pass filter → emits addition delta
- Row still passes filter → emits update delta with (old, new) pair

**Tests:**
| Test | Location |
|------|----------|
| Local update emits subscription delta | `manager::tests::local_update_emits_subscription_delta` |
| Synced update emits subscription delta | `manager::tests::synced_update_emits_subscription_delta` |
| Multiple updates same row single delta | `manager::tests::multiple_updates_same_row_single_delta` |
| Update fails filter emits removal | `manager::tests::update_fails_filter_emits_removal` |
| Update passes filter emits addition | `manager::tests::update_passes_filter_emits_addition` |
| Update still passes filter emits update | `manager::tests::update_still_passes_filter_emits_update` |
| Update to untracked row is silent | `manager::tests::update_to_untracked_row_is_silent` |
| Insert then update same cycle | `manager::tests::insert_then_update_same_cycle` |

#### Followup 8: Range Scan Boundary Semantics + FilterNode Elision ✓

Fixed range scan boundaries to use idiomatic Rust `std::ops::Bound` and added optimization to elide redundant FilterNodes.

**Problem solved:**
- `Lt`/`Gt` queries used inclusive bounds in index scan, returning extra rows that `FilterNode` had to remove
- FilterNode was always present even when index scan fully covered the query condition

**Core changes:**

1. **`ScanCondition` uses `Bound<Vec<u8>>`:**
```rust
pub enum ScanCondition {
    All,
    Eq(Vec<u8>),
    Range {
        min: Bound<Vec<u8>>,  // Included/Excluded/Unbounded
        max: Bound<Vec<u8>>,
    },
}
```

2. **`condition_to_scan()` maps correctly:**
   - `Lt` → `max: Bound::Excluded`
   - `Le` → `max: Bound::Included`
   - `Gt` → `min: Bound::Excluded`
   - `Ge` → `min: Bound::Included`
   - `Between` → both `Bound::Included`

3. **`range_scan()` respects exclusivity:**
   - `Bound::Excluded(key)` skips exact matches
   - `Bound::Included(key)` includes exact matches
   - `Bound::Unbounded` has no constraint

4. **FilterNode elision:**
   - `Conjunction::is_fully_covered_by_index(column)` checks if all conditions are indexable and on the index column
   - `build_remaining_predicate()` returns `Predicate::True` when all disjuncts are fully covered
   - Query compilation skips FilterNode when predicate is `Predicate::True`

**Behavior change:**

| Query | Before | After |
|-------|--------|-------|
| `WHERE score = 100` | IndexScan → Mat → **Filter** → Output | IndexScan → Mat → Output |
| `WHERE score < 50` | IndexScan(≤50) → Mat → **Filter** → Output | IndexScan(<50) → Mat → Output |
| `WHERE score < 50 AND name = 'Alice'` | IndexScan → Mat → Filter → Output | IndexScan → Mat → Filter → Output |

**Tests:**
| Test | Location |
|------|----------|
| Range scan exclusive min | `skip_list::tests::range_scan_exclusive_min` |
| Range scan exclusive max | `skip_list::tests::range_scan_exclusive_max` |
| Range scan both exclusive | `skip_list::tests::range_scan_both_exclusive` |
| Single Eq elides filter | `graph::tests::single_eq_condition_elides_filter` |
| Single Lt elides filter | `graph::tests::single_lt_condition_elides_filter` |
| Single Between elides filter | `graph::tests::single_between_condition_elides_filter` |
| Multiple conditions keeps filter | `graph::tests::multiple_conditions_different_columns_keeps_filter` |
| Non-indexable condition keeps filter | `graph::tests::non_indexable_condition_keeps_filter` |
| OR with single conditions elides filter | `graph::tests::or_with_single_conditions_elides_filter` |

#### Followup 10: Unified Tuple Model ✓

Refactored the entire query graph to use a unified tuple model with progressive materialization.

**Problem solved:**
- Separate `IdDelta`/`RowDelta` types required awkward boundary at MaterializeNode
- `IdNode` trait was confusing (didn't work with IDs after materialization)
- Architecture didn't support joins (which need multi-element tuples)

**Core changes:**

1. **`TupleElement` and `Tuple` types** for progressive materialization:
```rust
pub enum TupleElement {
    Id(ObjectId),
    Row { id: ObjectId, content: Vec<u8>, commit_id: CommitId },
}

pub struct Tuple(Vec<TupleElement>);  // Hash/Eq based on IDs only
```

2. **Unified `TupleDelta`** replaces both `IdDelta` and `RowDelta` internally:
```rust
pub struct TupleDelta {
    pub added: Vec<Tuple>,
    pub removed: Vec<Tuple>,
    pub updated: Vec<(Tuple, Tuple)>,
    pub pending: bool,
}
```

3. **Three clean traits** for different node roles:
   - `SourceNode` - reads from indices, returns unmaterialized tuples
   - `TransformNode` - operates on tuple sets (UnionNode for OR)
   - `RowNode` - processes `TupleDelta` (Filter, Sort, etc.)

4. **All nodes work with `TupleDelta`** throughout the pipeline:
   - IndexScanNode returns length-1 tuples with `TupleElement::Id`
   - MaterializeNode converts `Id` → `Row` elements
   - Filter/Sort/etc. work with materialized tuples
   - OutputNode maintains ordered results for deterministic output

5. **Final output conversion**:
   - `TupleDelta::to_row_delta()` converts to `RowDelta` at API boundary
   - Only needed for subscription deltas visible to users

#### Followup 9: End-to-End Sync Integration Tests ✓

Added tests verifying synced updates flow through to query subscription deltas via the full `push_inbox()` → `process_inbox()` → `process()` path.

**Tests:**
| Test | Description |
|------|-------------|
| `sync_inbox_insert_flows_to_subscription_delta` | New row via sync inbox → subscription delta |
| `sync_inbox_update_flows_to_subscription_delta` | Row update via sync inbox → update delta |
| `two_peer_sync_insert_reaches_subscription` | Full two-peer flow: Peer A inserts → sync payload → Peer B subscription |

These tests validate the integration between SyncManager and QueryManager, ensuring that:
1. `push_inbox()` + `process_inbox()` properly triggers `AllObjectUpdate` callbacks
2. QueryManager's `process()` picks up the updates and settles subscriptions
3. Subscription deltas are correctly emitted for both inserts and updates

#### Followup 11: Soft Deletes and Hard Deletes ✓

Implemented deletion semantics with two delete types:

**Delete Types:**

| Type | Content | Metadata | `_id_deleted` | Undeletable | Authoritative |
|------|---------|----------|---------------|-------------|---------------|
| Soft Delete | Preserved | `delete: soft` | Added | Yes | No |
| Hard Delete | Empty | `delete: hard` | Removed | No | Yes (always wins) |

**Key Design Decision:** Soft deletes preserve the row content (copied from previous tip). This allows:
- `include_deleted()` queries to return full row data for soft-deleted rows
- Soft-deleted rows to be materialized exactly like live rows
- The `delete: soft` metadata serves as a filter flag, not a content marker

**Index Infrastructure:**
- `_id` index: Live rows only
- `_id_deleted` index: Soft-deleted rows only (with preserved content)
- Hard-deleted rows appear in neither index (empty content = true tombstone)

**API:**

```rust
// Soft delete - preserves content, can be undone
let handle = qm.delete(row_id)?;

// Undelete - restore with new values
let handle = qm.undelete(row_id, &[value1, value2])?;

// Hard delete - permanent, authoritative, empties content
let handle = qm.hard_delete(row_id)?;

// Truncate - upgrade soft delete to hard delete
let handle = qm.truncate(row_id)?;

// Query including soft-deleted rows (returns full data)
let query = qm.query("users").include_deleted().build();
```

**Error Types:**
- `RowNotDeleted(id)` - Cannot undelete/truncate non-deleted row
- `RowAlreadyDeleted(id)` - Cannot delete already-deleted row
- `RowHardDeleted(id)` - Cannot operate on hard-deleted row

**Sync Conflict Resolution:**
- Hard delete is authoritative - always wins regardless of timestamp
- Incoming update for hard-deleted row is ignored
- Incoming hard delete discards any local updates

**Query Behavior:**
- Normal queries only scan `_id` index (live rows)
- `include_deleted()` also scans `_id_deleted` - returns full row data for soft-deleted rows
- Hard-deleted rows are invisible to all queries (empty content can't be materialized)

**Subscription Deltas:**
- Soft delete emits removal delta for subscribed rows
- Hard delete emits removal delta for subscribed rows
- Undelete emits addition delta

**Tests:**
| Test | Description |
|------|-------------|
| `soft_delete_removes_from_id_index` | Row removed from _id index |
| `soft_delete_adds_to_id_deleted_index` | Row added to _id_deleted index |
| `soft_deleted_row_not_in_query_results` | Deleted rows invisible to normal queries |
| `delete_already_deleted_row_fails` | Idempotency error |
| `undelete_adds_to_id_index` | Restored row back in _id |
| `undelete_removes_from_id_deleted_index` | Row removed from _id_deleted |
| `undelete_row_appears_in_query_results` | Restored row visible |
| `hard_delete_removes_from_id_index` | Row removed from _id |
| `hard_delete_removes_from_id_deleted_index` | Row removed from _id_deleted |
| `soft_then_hard_delete_removes_from_id_deleted` | Upgrade removes from _id_deleted |
| `include_deleted_query_returns_soft_deleted_rows` | Query returns both live and soft-deleted rows with full data |
| `soft_delete_emits_removal_delta` | Subscription notified |
| `hard_delete_emits_removal_delta` | Subscription notified |

#### Followup 12: Self-Describing Tuples with Per-Element Materialization ✓

Refactored the query graph so every node declares its output `TupleDescriptor` with per-element materialization state. This enables:
- Arbitrary graph composition (nodes chain descriptors)
- Lazy materialization (only load row content when needed)
- Compile-time validation of materialization requirements

**Problem solved:**
- FilterNode only looked at `tuple.get(0)` - couldn't filter on joined table columns
- No way to know at graph construction time if required elements were materialized
- Materialization was all-or-nothing, not per-element

**Core types added to `types.rs`:**

```rust
/// Per-element materialization tracking.
/// materialized[i] == true means element i has row content loaded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationState {
    materialized: Vec<bool>,
}

impl MaterializationState {
    pub fn all_ids(element_count: usize) -> Self;     // All false
    pub fn all_materialized(element_count: usize) -> Self;  // All true
    pub fn is_materialized(&self, element_index: usize) -> bool;
    pub fn with_materialized(self, element_index: usize) -> Self;
    pub fn concat(&self, other: &Self) -> Self;  // For joins
}

/// Describes structure and materialization state of tuples.
pub struct TupleDescriptor {
    elements: Vec<ElementDescriptor>,
    total_columns: usize,
    materialization: MaterializationState,  // Per-element state
}

impl TupleDescriptor {
    pub fn single(table: &str, descriptor: RowDescriptor) -> Self;  // [false]
    pub fn single_with_materialization(table: &str, desc: RowDescriptor, mat: bool) -> Self;
    pub fn concat(left: &Self, right: &Self) -> Self;  // For joins
    pub fn with_materialized(self, elements: &HashSet<usize>) -> Self;
    pub fn assert_materialized(&self, elements: &HashSet<usize>) -> Result<(), String>;
    pub fn resolve_column(global_index) -> Option<(element_index, local_index)>;
    pub fn elements_for_columns(cols) -> HashSet<usize>;
}
```

**Node changes - each node declares `output_tuple_descriptor()`:**

| Node | Constructor | Materialization |
|------|-------------|-----------------|
| `IndexScanNode` | `new(..., row_descriptor)` | `[false]` (IDs only) |
| `MaterializeNode` | `new_all(input_desc)` or `with_elements(desc, set)` | Marks specified elements true |
| `JoinNode` | `new(left_desc, right_desc, ...)` | Concatenates states |
| `FilterNode` | `try_new(desc, pred)` | Pass-through (validates required elements) |
| `SortNode` | `with_tuple_descriptor(desc, keys)` | Pass-through |
| `ProjectNode` | `with_tuple_descriptor(desc, cols)` | Pass-through |
| `OutputNode` | `with_tuple_descriptor(desc, mode)` | Pass-through |

**Compile-time validation:**
- `FilterNode::try_new()` returns `Err` if predicate references unmaterialized elements
- `JoinNode::new()` returns `None` if left join column isn't materialized
- Catches errors at graph construction, not runtime

**Graph compilation flow:**
```rust
// IndexScan outputs unmaterialized tuples
let scan = IndexScanNode::new("users", "_id", cond, row_desc);
// scan.output_tuple_descriptor().materialization = [false]

// Materialize loads row content
let mat = MaterializeNode::new_all(scan.output_tuple_descriptor().clone());
// mat.output_tuple_descriptor().materialization = [true]

// Filter validates and passes through
let filter = FilterNode::try_new(mat.output_tuple_descriptor().clone(), pred)?;
// Returns Err if predicate needs unmaterialized elements

// Join concatenates descriptors
let join = JoinNode::new(left_desc, right_desc, "users.id", "posts.author_id")?;
// join.output_tuple_descriptor().materialization = [true, false]
// Returns None if left join column not materialized
```

**Example: Join with filter on right table:**
```
Query: users JOIN posts ON users.id = posts.author_id WHERE posts.title = 'foo'

IndexScan(users)                 → materialization: [false]
MaterializeNode                  → materialization: [true]
JoinNode(left=users, right=posts)→ materialization: [true, false]
MaterializeNode(elements={1})    → materialization: [true, true]
FilterNode(posts.title='foo')    → validates element 1 is materialized ✓
OutputNode                       → flattens to RowDelta
```

**Tests:**
| Test | Description |
|------|-------------|
| `join_filter_on_joined_table_column` | Filter on posts.title in users JOIN posts |
| `filter_on_joined_table_column` | Unit test for multi-element tuple filtering |
| `filter_on_left_table_column_in_join` | Filter on users.name in join |
| `required_columns_*` | Predicate column extraction tests |
| `tuple_descriptor_*` | TupleDescriptor resolution tests |
| `materialization_state_*` | MaterializationState unit tests |

**Selective materialization (implemented):**
- `MaterializeNode::with_elements(desc, HashSet<usize>)` - only materialize specified elements
- Graph analysis can determine which elements filter/sort actually need
- Skip loading row data for elements not referenced by downstream nodes

#### Array Subqueries (Correlated Subqueries) ✓

Implemented inline array expressions that collect related rows into array columns.

**Core Types:**

```rust
// Value types for arrays and heterogeneous rows
pub enum ColumnType {
    // ... existing ...
    Array(Box<ColumnType>),      // Homogeneous array
    Row(Box<RowDescriptor>),     // Heterogeneous tuple (for array elements)
}

pub enum Value {
    // ... existing ...
    Array(Vec<Value>),           // Array of values
    Row(Vec<Value>),             // Heterogeneous tuple
}
```

**Architecture: Dynamic Graph Instances**

Each outer row gets its own subgraph evaluation ("recompile per binding"):

```
OuterScan → Materialize → ArraySubqueryNode
                              ↓
                    For each outer tuple:
                      1. Extract correlation value
                      2. Instantiate SubgraphTemplate
                      3. Settle subgraph
                      4. Collect results as Array<Row>
                              ↓
                    outer tuple + array column
```

This approach was chosen over shared hash indices to:
1. Support complex inner queries (filters, joins, nested arrays)
2. Explore subgraph patterns for future optimization
3. Keep implementation simple and correct

See `/specs/subgraph_sharing.md` for learnings and future optimization paths.

**Core Components:**

| Component | File | Purpose |
|-----------|------|---------|
| `SubgraphTemplate` | `subgraph.rs` | Parameterized query template |
| `SubgraphInstance` | `subgraph.rs` | Instantiated graph with bound correlation |
| `ArraySubqueryNode` | `array_subquery.rs` | Manages instances, emits deltas |
| `ArraySubquerySpec` | `query.rs` | Query-level specification |
| `ArraySubqueryBuilder` | `query.rs` | Fluent API for building specs |

**Subgraph Instantiation:**

```rust
impl SubgraphTemplate {
    /// Create instance with bound correlation value.
    /// Compiles fresh QueryGraph with correlation as equality filter.
    pub fn instantiate(&self, correlation_value: Value, schema: &Schema)
        -> Option<SubgraphInstance>;
}
```

The instantiated query includes:
- Correlation filter: `inner_column = correlation_value`
- All filters, order_by, limit from spec
- Joins within the subquery
- Nested array subqueries (recursive)

**Delta Reactivity:**

| Event | Handling |
|-------|----------|
| Outer row added | Evaluate subgraph, emit output with array |
| Outer row removed | Remove instance, emit removal |
| Outer row updated (correlation unchanged) | Keep existing array |
| Outer row updated (correlation changed) | Re-evaluate subgraph |
| Inner table changed | Mark `inner_dirty`, call `reevaluate_all()` |

Inner table changes trigger full re-evaluation of all instances:
```rust
impl ArraySubqueryNode {
    pub fn mark_inner_dirty(&mut self);
    pub fn reevaluate_all(&mut self, ...) -> TupleDelta;
}
```

**Graph Integration:**

`QueryGraph` tracks which tables affect array subquery nodes:
```rust
pub struct QueryGraph {
    // ... existing ...
    pub array_subquery_tables: Vec<(NodeId, String)>, // (node_id, inner_table)
}
```

`mark_dirty_for_table()` marks both IndexScanNodes and ArraySubqueryNodes dirty.

**Supported Features:**

| Feature | Status | Example |
|---------|--------|---------|
| Simple correlation | ✓ | `posts.author_id = users.id` |
| Filters inside | ✓ | `.filter_eq("published", true)` |
| Order by inside | ✓ | `.order_by_desc("created_at")` |
| Limit inside | ✓ | `.limit(10)` |
| Select columns | ✓ | `.select(&["id", "title"])` |
| Joins inside | ✓ | `.join("comments").on(...)` |
| Nested arrays | ✓ | `.with_array("comments", \|...\|)` |
| Multiple array columns | ✓ | Two `.with_array()` calls |
| Delta on inner change | ✓ | `reevaluate_all()` |
| Delta on outer change | ✓ | `process_with_context()` |

**Output Descriptor:**

Array elements are typed as `Array<Row<descriptor>>`:
```rust
// For users.with_array("posts", ...)
// Output: [id, name, posts]
// posts column type: Array(Row([id, title, author_id]))
```

**Tests:**

| Test | Description |
|------|-------------|
| `array_subquery_single_user_with_posts` | Basic correlation |
| `array_subquery_user_with_no_posts` | Empty array result |
| `array_subquery_multiple_users_correct_correlation` | Multiple outer rows |
| `array_subquery_delta_on_inner_insert` | Reactivity for inner changes |
| `array_subquery_delta_on_outer_insert` | Reactivity for outer changes |
| `array_subquery_with_order_by` | Order by in subquery |
| `array_subquery_with_limit` | Limit in subquery |
| `array_subquery_with_select_columns` | Column projection in subquery |
| `array_subquery_with_join` | Join inside subquery |
| `array_subquery_nested` | Nested array subqueries |
| `array_subquery_multiple_columns` | Multiple array columns |

### Pending Followups

#### Followup 6: `project_row` Should Use Memcpy (Low Priority)

Currently decodes to `Value` then re-encodes. Should memcpy bytes directly for fixed-size columns.

#### Followup 7: Add `subscribe_full` API (Low Priority)

Only delta-mode subscriptions exposed. `OutputMode::Full` exists but isn't wired up to API.
