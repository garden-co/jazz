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
    ├── mod.rs            # IdNode, RowNode traits
    ├── index_scan.rs     # Source node (IdDelta)
    ├── union.rs          # OR conditions (IdDelta)
    ├── materialize.rs    # IdDelta → RowDelta boundary
    ├── filter.rs         # In-memory filter (RowDelta)
    ├── sort.rs           # (RowDelta)
    ├── limit_offset.rs   # (RowDelta)
    └── output.rs         # (RowDelta)
```

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

### Pending Followups

#### Followup 3: Sync Integration for Row Updates (Medium Priority)

**Current limitation:** When rows are updated via sync (not local `update()` call), column indices may become stale. The `_id` index remains correct, but column indices won't reflect the new values because we don't have the old data to compute the index delta.

**Design decision:** Require explicit `process()` calls (no auto-process).

**Core need:** A way to get (old_data, new_data) on any object update, enabling:
- Detection of which columns changed
- Removal of old value from index before inserting new value

**Implementation steps:**
1. First: Add test verifying local insert+update updates ALL column indices (not just `_id`)
2. Implement old_data/new_data tracking for object updates
3. Integration test: QueryManager1 on SyncManager1/ObjectManager1 exchanging messages with QueryManager2 on SyncManager2/ObjectManager2

#### Followup 4: Async Row Materialization (Medium Priority)

MaterializeNode loads rows synchronously. If loader returns `None` (not yet loaded), row is skipped. Need mechanism for rows to appear in results once they arrive from network.

**Options:** Return "pending" state in RowDelta, track pending IDs for re-emit, or async loader.

#### Followup 5: `project_row` Should Use Memcpy (Low Priority)

Currently decodes to `Value` then re-encodes. Should memcpy bytes directly for fixed-size columns.

#### Followup 6: Add `subscribe_full` API (Low Priority)

Only delta-mode subscriptions exposed. `OutputMode::Full` exists but isn't wired up to API.

#### Followup 7: Fix Range Scan Boundary Semantics (Low Priority)

Lt/Gt use inclusive bounds in index scan. Filter node corrects this, so correctness maintained but slightly inefficient.

#### Followup 8: End-to-End Sync Integration Tests (Medium Priority)

No tests verify synced updates flow through to query deltas. Need two-peer test with subscription verification.

#### Followup 9: IndexScanNode Process Method (Low Priority)

`IdNode::process()` is a no-op on IndexScanNode. Graph settling special-cases it. Works but violates trait contract.

#### Followup 10: Row Deletion (Medium Priority)

Implement `delete()` API for removing rows.
