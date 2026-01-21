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
- **Query graph**: Two delta types - `IdDelta` before materialization, `RowDelta` after

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
├── types.rs              # ColumnType, RowDescriptor, Row, IdDelta, RowDelta
├── encoding.rs           # encode/decode (boundary), binary ops (internal)
├── manager.rs            # QueryManager
├── query.rs              # Query, QueryBuilder, Value (boundary type)
├── graph.rs              # Graph structure, dirty marking, settling
├── index/
│   ├── mod.rs
│   └── skip_list.rs
└── graph_nodes/
    ├── mod.rs            # SourceNode, IdNode, RowNode traits, SourceContext
    ├── index_scan.rs     # Source node (impl SourceNode)
    ├── union.rs          # OR conditions (impl IdNode)
    ├── materialize.rs    # IdDelta → RowDelta boundary
    ├── filter.rs         # In-memory filter (RowDelta)
    ├── sort.rs           # (RowDelta)
    ├── limit_offset.rs   # (RowDelta)
    └── output.rs         # (RowDelta)
```

## Node Trait Architecture

The query graph distinguishes between two kinds of ID-level nodes:

**Source nodes** (`SourceNode` trait) read from external state:
- `IndexScanNode` - scans indices for matching ObjectIds
- Require external context (indices, ObjectManager) via `SourceContext`
- Have no input nodes in the graph

**Transform nodes** (`IdNode` trait) are pure dataflow:
- `UnionNode` - merges ID sets from multiple inputs
- `process()` takes input ID sets directly, no external state needed
- Combine/filter outputs from other nodes

```rust
/// Context for source nodes that need external data.
pub struct SourceContext<'a> {
    pub indices: &'a HashMap<(String, String), IndexState>,
    pub om: &'a ObjectManager,
}

/// Source nodes produce data from external state (no input nodes).
pub trait SourceNode {
    fn scan(&mut self, ctx: &SourceContext) -> IdDelta;
    fn current_ids(&self) -> &HashSet<ObjectId>;
    fn mark_dirty(&mut self);
    fn is_dirty(&self) -> bool;
}

/// Transform nodes combine/filter id sets from their inputs.
pub trait IdNode {
    fn process(&mut self, inputs: &[&HashSet<ObjectId>]) -> IdDelta;
    fn current_ids(&self) -> &HashSet<ObjectId>;
    fn mark_dirty(&mut self);
    fn is_dirty(&self) -> bool;
}
```

This separation enables `settle()` to use clean pattern matching rather than string-based dispatch.

## Implementation Status

### Completed
- [x] types.rs - ColumnType, RowDescriptor, Row, IdDelta, RowDelta
- [x] encoding.rs - binary format + operations
- [x] Global subscription in object_manager.rs
- [x] skip_list.rs - index with binary keys
- [x] All graph nodes: index_scan, union, materialize, filter, sort, limit_offset, output
- [x] graph.rs - settling algorithm
- [x] manager.rs + query.rs - QueryManager with Value boundary
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

#### Followup 10: Separate Source Nodes from Transform Nodes ✓

Refactored the ID-level node architecture to cleanly separate source nodes from transform nodes.

**Problem solved:**
- `IdNode::process()` was a no-op on IndexScanNode
- `settle()` used string-based dispatch to special-case each node type
- Two different computation patterns conflated in one trait

**Core changes:**

1. **New `SourceNode` trait** for nodes that read from external state:
```rust
pub trait SourceNode {
    fn scan(&mut self, ctx: &SourceContext) -> IdDelta;
    fn current_ids(&self) -> &HashSet<ObjectId>;
    fn mark_dirty(&mut self);
    fn is_dirty(&self) -> bool;
}
```

2. **`SourceContext` struct** bundles external dependencies:
```rust
pub struct SourceContext<'a> {
    pub indices: &'a HashMap<(String, String), IndexState>,
    pub om: &'a ObjectManager,
}
```

3. **Updated `IdNode::process()` signature** to take inputs directly:
```rust
fn process(&mut self, inputs: &[&HashSet<ObjectId>]) -> IdDelta;
```

4. **`IndexScanNode`** now implements `SourceNode` (not `IdNode`)
   - `scan()` looks up its index from `SourceContext` internally

5. **`UnionNode`** simplified:
   - Removed `pending_deltas` and `add_input()`
   - `process()` takes input sets directly
   - Removed redundant `process_inputs()` method

6. **`settle()` uses pattern matching** instead of string dispatch:
   - Creates `SourceContext` once at start
   - `collect_id_inputs()` helper gathers inputs for transform nodes

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

### Pending Followups

#### Followup 6: `project_row` Should Use Memcpy (Low Priority)

Currently decodes to `Value` then re-encodes. Should memcpy bytes directly for fixed-size columns.

#### Followup 7: Add `subscribe_full` API (Low Priority)

Only delta-mode subscriptions exposed. `OutputMode::Full` exists but isn't wired up to API.
