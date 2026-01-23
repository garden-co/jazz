# QueryManager Architecture

## Overview

The QueryManager layer provides reactive SQL queries over Jazz2's object-based storage. Each row is a Jazz object; indices are local-only B-trees with page-based storage; queries compile to incremental computation graphs that emit row deltas.

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
- **Auto-index all columns**: Every column gets a single-column B-tree index (zero-config)
- **All indices persisted**: Every index is persisted as B-tree pages in storage (not as Jazz objects)
- **No index rebuild**: Indices are incrementally maintained; if missing on startup, that's an error state
- **Indices**: B-trees with page-based storage, local-only (no sync), loaded lazily
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

## Index Storage Model (B-Tree Pages)

Indices are stored as B-tree pages directly in storage (not as Jazz objects). This approach provides:
- **Much lower overhead**: ~20 bytes per page vs ~530 bytes per Object-wrapped node
- **Higher fanout**: ~64 entries per leaf page means fewer storage operations
- **Incremental updates**: Only modified pages are written
- **Lazy loading**: Pages loaded on demand as queries traverse the tree

### Storage Key Format

```
index:{table}:{column}:meta     → IndexMeta (root page id, entry count)
index:{table}:{column}:page:{id} → Page data
```

### Storage Requests

```rust
pub enum StorageRequest {
    // ... existing ...
    LoadIndexPage { table: String, column: String, page_id: u64 },
    StoreIndexPage { table: String, column: String, page_id: u64, data: Vec<u8> },
    DeleteIndexPage { table: String, column: String, page_id: u64 },
    LoadIndexMeta { table: String, column: String },
    StoreIndexMeta { table: String, column: String, data: Vec<u8> },
}
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

**Per-page overhead:** ~20 bytes (vs ~530 bytes for Object-wrapped skip list node)

## B-Tree Index Architecture

### Design Principles

1. **Page-based storage**: Each B-tree node is a storage page, not a Jazz object
2. **Lazy loading**: Pages loaded on demand as queries traverse the tree
3. **Dirty tracking**: Only modified pages are persisted
4. **Incremental updates**: Insert/remove only touch affected pages
5. **No ObjectManager involvement**: Indices don't use the object system

### BTreeIndex

```rust
pub struct BTreeIndex {
    table: String,
    column: String,

    /// Index metadata (root page, next page id, entry count).
    meta: IndexMeta,
    meta_loaded: bool,
    meta_dirty: bool,

    /// Loaded pages (lazy loading).
    pages: HashMap<PageId, PageState>,

    /// Pages that have been modified and need persistence.
    dirty_pages: HashSet<PageId>,

    /// Pages to delete on next persist.
    deleted_pages: HashSet<PageId>,

    /// Pending storage requests to emit.
    pending_requests: Vec<StorageRequest>,
}

impl BTreeIndex {
    /// Create a new B-tree index for a table/column.
    pub fn new(table: &str, column: &str) -> Self;

    /// Insert a row into the index.
    /// Returns Ok(true) if inserted, Ok(false) if pages need loading.
    pub fn insert(&mut self, key: &[u8], row_id: ObjectId) -> Result<bool, IndexError>;

    /// Remove a row from the index.
    pub fn remove(&mut self, key: &[u8], row_id: ObjectId) -> Result<(), IndexError>;

    /// Exact lookup - returns row IDs for the given key.
    pub fn lookup_exact(&self, key: &[u8]) -> Vec<ObjectId>;

    /// Range scan - returns row IDs for keys in range.
    pub fn range_scan(&self, min: &Bound<Vec<u8>>, max: &Bound<Vec<u8>>) -> Vec<ObjectId>;

    /// Full scan - returns all row IDs.
    pub fn scan_all(&self) -> Vec<ObjectId>;

    /// Check if a row exists in the index.
    pub fn contains_row(&self, row_id: ObjectId) -> bool;

    /// Take pending storage requests.
    pub fn take_storage_requests(&mut self) -> Vec<StorageRequest>;

    /// Process loaded metadata response.
    pub fn process_meta_load(&mut self, data: Option<Vec<u8>>);

    /// Process loaded page response.
    pub fn process_page_load(&mut self, page_id: PageId, data: Option<Vec<u8>>);

    /// Reset for cold start - request meta from storage.
    pub fn reset_for_cold_start(&mut self);

    /// Estimate memory size.
    pub fn memory_size(&self) -> usize;
}
```

### Page Types

```rust
pub struct PageId(pub u64);

pub enum BTreePage {
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<PageId>,
    },
    Leaf {
        entries: Vec<LeafEntry>,
    },
}

pub struct LeafEntry {
    pub key: Vec<u8>,
    pub row_ids: HashSet<ObjectId>,
}

pub struct IndexMeta {
    pub root_page_id: PageId,
    pub next_page_id: u64,
    pub entry_count: u64,
}
```

### Storage Flow

Indices generate `StorageRequest` variants for their pages. QueryManager collects these and routes through the storage layer:

```rust
impl QueryManager {
    /// Process storage through a real driver.
    pub fn process_storage_with_driver(&mut self, driver: &mut impl Driver);

    /// Load indices from storage for cold start.
    pub fn load_indices_from_driver(&mut self, driver: &mut impl Driver);
}
```

For tests without persistence, `drain_storage_noop()` generates success responses without actual storage.

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
│   ├── btree_index.rs    # B-tree index with page-based storage
│   ├── btree_page.rs     # Page types and serialization
│   └── skip_list.rs      # Legacy skip list (for reference)
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
    pub indices: &'a HashMap<(String, String), BTreeIndex>,
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
- [x] B-tree index with page-based storage (btree_index.rs, btree_page.rs)
- [x] All graph nodes: index_scan, union, materialize, filter, sort, limit_offset, output, alias, project, join, array_subquery, subgraph
- [x] graph.rs - settling algorithm (single-table and join queries)
- [x] manager.rs + query.rs - QueryManager with Value boundary
- [x] Query API: alias(), select(), join(), on(), with_array() methods on QueryBuilder
- [x] Join graph compilation (multi-table queries with JoinNode)
- [x] B-tree index architecture:
  - Page-based storage (not Object-wrapped)
  - Binary serialization (~20 bytes overhead vs ~530 for Object-wrapped)
  - Lazy loading of pages on demand
  - Dirty tracking for incremental persistence
  - Cold start support via `load_indices_from_driver()`
  - `InsertHandle` with `is_complete()` and `is_indexed()` methods
- [x] ObjectManager as sole source of truth for row data:
  - Removed `row_cache` - no redundant caching layer
  - `row_loader` closures access ObjectManager directly
  - Removed `discover_rows_from_indices()` - no eager loading on process()
  - `get()` renamed to `test_get_row_if_loaded()` and gated with `#[cfg(test)]`
  - Production code must use queries to read data

**B-Tree Index Tests:**
| Test | Status | Location |
|------|--------|----------|
| Insert and lookup | ✓ | `btree_index::tests::insert_and_lookup` |
| Range scan | ✓ | `btree_index::tests::range_scan` |
| Scan all | ✓ | `btree_index::tests::scan_all` |
| Page serialization | ✓ | `btree_page::tests::page_serialization_roundtrip` |
| Meta serialization | ✓ | `btree_page::tests::meta_serialization_roundtrip` |
| Cold start loads indices | ✓ | `manager::tests::cold_start_loads_persisted_indices_and_rows` |
| Cold start doesn't eagerly load rows | ✓ | `manager::tests::cold_start_only_loads_queried_rows` |
| Cold start sorted query | ✓ | `manager::tests::cold_start_with_sorted_query` |
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

#### Followup 13: ReBAC Policy Evaluation Gaps

The async permission evaluation system (ReBAC via Policy Graphs) is implemented but has several gaps:

**High Priority (Security Gaps):**

1. ~~**EXISTS clause evaluation not implemented** (`manager.rs:1398`)~~ ✅ DONE
   - EXISTS clauses now properly create and settle policy graphs
   - Test: `rebac_tests::rebac_exists_clause_denies_non_matching_insert`
   - Test: `rebac_tests::rebac_update_denied_by_using_exists_policy`

2. ~~**UPDATE USING check not implemented**~~ ✅ DONE
   - UPDATE now evaluates both USING (against old row) and WITH CHECK (against new row)
   - Implemented via `evaluate_update_permission()` in `manager.rs`
   - Complex clauses (INHERITS/EXISTS) in USING are properly evaluated
   - Test: `rebac_tests::rebac_update_denied_by_using_policy`

**Medium Priority:**

3. ~~**INHERITS in PolicyFilterNode stubbed**~~ ✅ DONE
   - PolicyFilterNode now properly evaluates INHERITS using PolicyGraph
   - Added `process_with_context()` method for INHERITS evaluation in settlement
   - Self-referential INHERITS disallowed (returns false, cycle detection at schema load)
   - Dirty tracking: PolicyFilter nodes marked dirty when INHERITS-referenced tables change
   - Cycle detection: `validate_no_inherits_cycles()` added to types.rs
   - Test: `rebac_tests::rebac_inherits_filters_select_query_results` (passing)
   - Test: `rebac_tests::rebac_inherits_cycle_detection` (passing)
   - Test: `rebac_tests::rebac_inherits_self_reference_detection` (passing)
   - Test: `rebac_tests::rebac_inherits_no_cycle_passes` (passing)

4. **UUID format mismatch in session claims** (`policy.rs:534-536`)
   - Uses `format!("{:?}", id)` (Debug format) for UUID comparison
   - Session claims are JSON strings which may use different format
   - May cause false negatives in `IN` checks with UUID arrays

5. **NOT(complex_clause) semantics unclear** (`policy.rs:734-746`)
   - When policy contains `NOT(INHERITS(...))`, the inversion logic is unclear
   - Returns complex clauses with `passed: true` but meaning of NOT inversion is ambiguous
   - Needs documentation or semantic fix

**Low Priority (Code Quality):**

6. ~~**Unused `table_name` field**~~ ✅ FIXED
   - `PolicyFilterNode.table_name` now used for self-INHERITS detection

#### Followup 14: Self-Referential INHERITS Support

Self-referential INHERITS (e.g., folders with `parent_id` referencing the same `folders` table) is currently disallowed for safety. This is a common pattern for hierarchical data with inherited permissions.

**Current Behavior:**
- `policy_filter.rs:270`: Returns `false` when INHERITS references same table
- `types.rs:validate_no_inherits_cycles()`: Detects self-reference as a cycle

**Implementation Approach:**

1. **Iterative Settlement with Depth Limit**
   - Walk up the parent chain iteratively (not recursively via PolicyGraph)
   - Check each parent row against the policy directly
   - Stop at depth limit (e.g., 32) or NULL parent_id
   - Track visited ObjectIds to detect runtime cycles

2. **Example Policy:**
   ```
   -- Folders: user can see folder if they own it OR can see parent folder
   SELECT policy: owner_id = @user_id OR INHERITS SELECT VIA parent_id
   ```

3. **Algorithm:**
   ```rust
   fn evaluate_self_inherits(row, depth) -> bool {
       if depth >= 32 { return false; }

       let parent_id = row.get("parent_id");
       if parent_id.is_null() { return true; }  // Root folder, chain ends

       let parent_row = load_row(parent_id)?;

       // Evaluate non-INHERITS parts of policy against parent
       if evaluate_simple_policy(parent_row) { return true; }

       // Recurse up the chain
       evaluate_self_inherits(parent_row, depth + 1)
   }
   ```

4. **Considerations:**
   - Runtime cycle detection via visited set (handles data cycles, not schema cycles)
   - Caching: Parent policy results could be memoized across sibling evaluations
   - Performance: Deep hierarchies may be slow; consider materialized path optimization

**Tests to Add:**
- `rebac_inherits_self_reference_allowed` - Basic self-referential INHERITS
- `rebac_inherits_deep_hierarchy` - Deep folder hierarchy (depth limit)
- `rebac_inherits_runtime_cycle` - Data cycle (A→B→A in parent_id)

#### Followup 15: UPDATE with INHERITS Policy Chain (High Priority)

**Problem:** UPDATE operations fail with `PolicyDenied` even when the INHERITS chain should grant access.

**Reproduction scenario:**
- User has access to a folder via INHERITS (folder → team → user membership)
- Documents in that folder are authored by a different user
- UPDATE policy: `USING: author_id = @user_id OR INHERITS SELECT VIA folder_id`
- Expected: User can update documents in accessible folders (via INHERITS)
- Actual: `PolicyDenied` error

**Root cause (likely):**
The UPDATE USING policy evaluation for INHERITS may not be properly wiring up the policy graph evaluation, or the INHERITS chain traversal isn't correctly checking the folder→team→user path during UPDATE operations.

**Discovered in:** `update_benchmark.rs::update_team_documents` benchmark

**To investigate:**
1. Trace the INHERITS evaluation path in `evaluate_update_permission()`
2. Verify PolicyGraph is correctly instantiated for UPDATE USING with INHERITS
3. Check if the issue is specific to UPDATE or also affects DELETE with INHERITS

#### Followup 16: ORDER BY + LIMIT Subscriptions Not Receiving Updates (High Priority)

**Problem:** Subscriptions with `ORDER BY ... LIMIT N` do not receive incremental updates when new rows are inserted that should appear in the top N results.

**Reproduction scenario:**
- Subscribe to `documents ORDER BY created_at DESC LIMIT 50`
- Insert a document with a timestamp that would place it in the top 50
- Expected: Subscription receives update delta with the new row (and possibly removal of row #51)
- Actual: No subscription update received

**Root cause (likely):**
The LimitOffsetNode may not be correctly handling deltas where:
1. A new row is added that displaces an existing row from the window
2. The node needs to emit both an addition (new row in window) and removal (row pushed out of window)

The current implementation may only track absolute position rather than relative ordering, causing it to miss updates that affect the bounded result set.

**Discovered in:** `subscription_benchmark.rs::complex_query_latency` benchmark

**To investigate:**
1. Check `LimitOffsetNode::process()` logic for handling additions to sorted, limited sets
2. Verify SortNode upstream is emitting correct deltas
3. Consider if a dedicated "TopN" node is needed for efficient bounded result tracking

#### Followup 17: B-Tree Range Scan Sibling Pointers (Medium Priority)

**Problem:** The B-tree `range_scan()` currently only scans a single leaf page. For queries that span multiple leaf pages, not all matching entries are returned.

**Current behavior:**
- `scan_all()` works correctly by iterating all loaded pages
- `range_scan()` finds the starting leaf via `find_leaf_for_key()` but only scans that one leaf
- Large range queries may miss entries in sibling leaves

**Implementation approach:**

1. **Add sibling pointers to leaf pages:**
   ```rust
   pub enum BTreePage {
       Leaf {
           entries: Vec<LeafEntry>,
           next_leaf: Option<PageId>,  // Sibling pointer for range scans
       },
       // ...
   }
   ```

2. **Update `split_leaf()` to maintain sibling chain:**
   - When splitting, set `left.next_leaf = Some(right_page_id)`
   - Preserve existing `next_leaf` on the right page

3. **Update `scan_leaves()` to follow sibling chain:**
   ```rust
   fn scan_leaves(&self, start: PageId, min: &Bound, max: &Bound) -> Vec<ObjectId> {
       let mut current = Some(start);
       let mut results = Vec::new();

       while let Some(page_id) = current {
           let page = self.get_page(page_id)?;
           if let BTreePage::Leaf { entries, next_leaf } = page {
               for entry in entries {
                   if past_max_bound(entry, max) { return results; }
                   if in_range(entry, min, max) { results.extend(&entry.row_ids); }
               }
               current = next_leaf;
           }
       }
       results
   }
   ```

4. **Lazy loading consideration:**
   - When `next_leaf` page isn't loaded, generate `LoadIndexPage` request
   - Return partial results with indication more pages are pending

**Tests to add:**
- `range_scan_spans_multiple_leaves` - Insert enough entries to cause splits, verify range scan
- `range_scan_with_lazy_loaded_siblings` - Test partial results when sibling not loaded

#### Followup 18: Cold Start Index Persistence ✓

**Problem solved:** Implemented B-tree index persistence for cold start scenarios.

**Implementation:**

1. **TestDriver now persists index data:**
   ```rust
   pub struct TestDriver {
       // ... existing ...
       pub index_pages: HashMap<(String, String, u64), Vec<u8>>,
       pub index_meta: HashMap<(String, String), Vec<u8>>,
   }
   ```

2. **QueryManager storage methods:**
   - `process_storage_with_driver()` - Routes both ObjectManager and index requests through a real driver
   - `load_indices_from_driver()` - Resets indices and loads from storage (for cold start)
   - `reset_indices_for_cold_start()` - Clears index state and queues meta load requests

3. **BTreeIndex methods:**
   - `reset_for_cold_start()` - Clears loaded state, queues LoadIndexMeta request
   - `process_meta_load()` now queues LoadIndexPage for root when loading persisted meta
   - Multiple storage rounds: meta load → root page load → ready

**Tests:**
| Test | Description |
|------|-------------|
| `cold_start_loads_persisted_indices_and_rows` | Full cold start with index persistence |
| `cold_start_only_loads_queried_rows` | Verifies lazy row loading |
| `cold_start_with_sorted_query` | Sorted queries work after cold start |
