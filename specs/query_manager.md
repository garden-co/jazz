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
3. Load row objects as needed (lazy, via query)

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
        // 3. Discover any new rows from _id indices
        // 4. Settle subscriptions
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
| Row is indexed after insert | ✓ | `manager::tests::row_is_indexed_after_insert` |

### Pending Followups

#### Followup 2: nosync Filtering in SyncManager (High Priority)

**Gap:** Index objects have `nosync: "true"` metadata but SyncManager doesn't filter them.

**Implementation needed:**
- When preparing to sync objects to a peer, filter out those with `nosync: "true"` metadata:
```rust
fn should_sync_object(&self, object: &Object) -> bool {
    object.metadata.get("nosync") != Some(&"true".to_string())
}
```

**Verification:**
- Create QueryManager, insert rows
- Verify index objects have nosync metadata
- Verify sync doesn't include index objects

#### Followup 3: Automatic Sync Integration (Medium Priority)

**Gap:** Changes arriving via sync don't automatically update indices unless `process()` is manually called.

**Current state:**
- Global subscription was added to ObjectManager and works correctly
- QueryManager only calls `take_all_object_updates()` in `process()` when explicitly invoked
- External changes from SyncManager (e.g., received from network) are NOT automatically processed

**Implementation needed:**
1. Design decision: auto-process on each API call, or require explicit `process()` calls
2. If auto-process: call `process()` at the end of `insert()`, `update()`, `execute()`, etc.
3. If explicit: document that users must call `process()` to see synced changes
4. Integration test: two QueryManagers sharing a SyncManager, verify changes from one appear in the other

#### Followup 4: Async Row Materialization (Medium Priority)

**Gap:** MaterializeNode loads rows synchronously, which could block if rows need to be fetched from network.

**Current state:**
- `MaterializeNode::materialize()` takes a synchronous `FnMut(ObjectId) -> Option<(Vec<u8>, CommitId)>` loader
- If the loader returns `None`, the row is simply skipped (not added to result)
- No mechanism to "wait" for pending rows or retry later

**Implementation options:**
- Option A: Return a "pending" state in RowDelta for rows not yet available
- Option B: MaterializeNode tracks pending IDs and re-emits them when data arrives
- Option C: Async loader with `Future` return type

**Verification:**
- Subscribe to query, sync brings in new object ID before content, verify row appears after content arrives

#### Followup 5: `project_row` Should Use Memcpy (Low Priority)

**Gap:** The `project_row` function decodes to `Value` then re-encodes, rather than copying bytes directly.

**Current state (encoding.rs):**
```rust
pub fn project_row(...) -> Result<Vec<u8>, EncodingError> {
    let mut dst_values = vec![Value::Null; dst_descriptor.columns.len()];
    for &(src_col, dst_col) in column_mapping {
        let value = decode_column(src_descriptor, src_data, src_col)?;
        dst_values[dst_col] = value;
    }
    encode_row(dst_descriptor, &dst_values)
}
```

**Implementation needed:**
1. For fixed-size columns: directly memcpy bytes from source to destination
2. For variable-length columns: copy byte ranges using offset table
3. Only use decode/encode as fallback for complex cases (e.g., nullable flag differences)
4. Add benchmark comparing current vs memcpy approach

#### Followup 6: Add `subscribe_full` API (Low Priority)

**Gap:** Only delta-mode subscriptions are exposed in the API.

**Current state:**
- Only `subscribe()` exists (delta mode)
- `OutputMode::Full` exists in OutputNode but isn't exposed
- `OutputNode::decode_current()` exists for getting full result set

**Implementation needed:**
1. Add `subscribe_full()` method to QueryManager
2. Store output mode in subscription metadata
3. In `take_updates()`, return full decoded result for full-mode subscriptions
4. Add test for full-mode subscription

#### Followup 7: Fix Range Scan Boundary Semantics (Low Priority)

**Gap:** Range conditions Lt/Gt use inclusive bounds in the index scan, potentially returning extra rows.

**Current state:** The skip list `range_scan` uses inclusive bounds (`<=` and `>=`), but:
- `Lt` (less than) should exclude the boundary value
- `Gt` (greater than) should exclude the boundary value
- `Le` (less than or equal) should include - correct
- `Ge` (greater than or equal) should include - correct

**Impact:** The filter node catches and correctly filters these rows, so correctness is maintained. But the index may return extra rows that are then filtered out, which is slightly inefficient.

**Implementation options:**
1. Add `Range { min, max, min_inclusive, max_inclusive }` to ScanCondition
2. Update skip list `range_scan` to accept inclusivity flags
3. Or: accept current behavior as "good enough" since filter corrects it

#### Followup 8: End-to-End Sync Integration Tests (Medium Priority)

**Gap:** No tests verify that synced updates from another peer flow through to query deltas.

**Implementation needed:**
1. Create test with two SyncManagers connected (simulating two peers)
2. Peer A: QueryManager with subscription
3. Peer B: Insert row
4. Simulate sync: Peer B sends object/commits to Peer A
5. Peer A: Call `process()`, verify subscription receives delta
6. Test both new row insertion and row update scenarios

#### Followup 9: IndexScanNode Process Method (Low Priority)

**Gap:** The `IdNode::process()` trait method on IndexScanNode is essentially a no-op.

**Current state:**
- `IndexScanNode` stores `(table, column)` strings, not the actual index reference
- `IdNode::process()` returns empty IdDelta
- The graph settling code special-cases IndexScanNode and calls `scan()` with the index passed in

**Impact:** The trait-based abstraction is partially broken. The code works but violates the trait contract.

**Implementation options:**
1. Accept current design: IndexScanNode is a "semi-source" that needs external index injection
2. Refactor: Give IndexScanNode a reference/Arc to IndexState so `process()` works standalone
3. Refactor: Change IdNode trait to accept context parameter: `process(&mut self, ctx: &QueryContext) -> IdDelta`
