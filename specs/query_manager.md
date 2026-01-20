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
3. **Copy-on-write**: Only mutated nodes are copied to `pending_writes`
4. **No global state machine**: QueryManager has no Setup/Ready states - operations work immediately
5. **Lazy loading**: Index data is read from ObjectManager on demand

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
}
```

### NodeRef (Unified Access)

```rust
/// Unified node access - either a view into ObjectManager or reference to pending write.
pub enum NodeRef<'a> {
    View(SkipListNodeView<'a>),
    Pending(&'a SkipListNode),
}
```

### IndexState

```rust
pub struct IndexState {
    pub root_id: ObjectId,
    pub table: String,
    pub column: String,
    pub pending_writes: HashMap<ObjectId, SkipListNode>,  // Mutations not yet persisted
    current_level: usize,
}

impl IndexState {
    /// Get node - checks pending_writes first, then ObjectManager.
    fn get_node<'a>(&'a self, node_id: ObjectId, om: &'a ObjectManager) -> Option<NodeRef<'a>>;

    /// For mutations, copy to pending_writes if not already there.
    fn get_node_mut(&mut self, node_id: ObjectId, om: &ObjectManager) -> Option<&mut SkipListNode>;

    /// Persist any pending_writes to ObjectManager.
    fn persist_pending(&mut self, om: &mut ObjectManager) -> Vec<(ObjectId, CommitId)>;
}
```

All traversal methods take `&ObjectManager`:
- `insert(key, row_id, om)` - Insert into index
- `remove(key, row_id, om)` - Remove from index
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
    /// Drive all async progress: persistence, subscription updates.
    pub fn process(&mut self) {
        // 1. Discover any new rows from _id indices
        // 2. Persist any pending index writes
        // 3. Settle subscriptions
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
- [x] Index persistence: `persist_node()`, `persist_pending()` in IndexState
- [x] `create_with_id()` in ObjectManager for deterministic root IDs
- [x] Zero-copy index architecture:
  - `SkipListNodeView` for zero-copy reads from ObjectManager
  - `NodeRef` enum for unified access (View or Pending)
  - `pending_writes` for copy-on-write mutations
  - Removed `nodes` cache - ObjectManager is single source of truth
  - Removed `QueryManagerState` - no Setup/Ready state machine
  - All traversal methods take `&ObjectManager` parameter
  - `InsertHandle` with `is_complete()` and `is_indexed()` methods
  - Lazy sentinel creation on first insert

**Zero-Copy Architecture Tests:**
| Test | Status | Location |
|------|--------|----------|
| Node view parses encoded data | ✓ | `skip_list::tests::node_view_parses_encoded_data` |
| Node view key is zero-copy | ✓ | `skip_list::tests::node_view_key_is_zero_copy` |
| Node view iterates row IDs | ✓ | `skip_list::tests::node_view_iterates_row_ids` |
| Node view reads forward pointers | ✓ | `skip_list::tests::node_view_reads_forward_pointers` |
| Get node reads from ObjectManager | ✓ | `skip_list::tests::get_node_reads_from_object_manager` |
| Pending write takes precedence | ✓ | `skip_list::tests::get_node_returns_pending_write_if_present` |
| Persist pending clears writes | ✓ | `skip_list::tests::persist_pending_clears_pending_writes` |
| Cold start loads indices | ✓ | `manager::tests::cold_start_loads_persisted_indices_and_rows` |
| Insert handle is_indexed works | ✓ | `manager::tests::insert_handle_is_indexed` |

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
