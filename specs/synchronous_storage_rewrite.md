# Synchronous Storage Architecture Rewrite

> **THIS IS A MASSIVE REWRITE.**
>
> We are throwing away the async storage assumption that permeates the entire codebase. This is not a refactor - it's a ground-up rearchitecture. Be aggressive. No backwards compatibility. No migration paths. No deprecation periods.
>
> **Priorities:**
> 1. Correctness and architectural clarity over speed
> 2. Thoroughness over incremental progress
> 3. Delete aggressively - less code is better
> 4. If something doesn't fit the new model, remove it entirely
>
> This will touch every layer: storage, object management, indices, query graphs, sync protocol, WASM bindings, and TypeScript client.

---

## Motivation

The current architecture assumes storage is asynchronous. This assumption infects every layer:

| Layer | Async Complexity |
|-------|------------------|
| **BTreeIndex** | `is_ready()`, `PageState::Loading`, `pending_inserts/deletes`, `IndexError::PageNotLoaded` |
| **ObjectManager** | `ObjectState::Loading`, `BlobState::Loading`, `Error::ObjectNotReady`, `Error::BranchNotLoaded` |
| **QueryManager** | `MaterializeNode.pending_ids`, `TupleDelta.pending`, retry loops |
| **RuntimeCore** | `park_storage_response()`, `IoHandler` trait, batched tick scheduling |

This complexity exists because WASM can't block the main thread on IndexedDB/OPFS async APIs.

**The insight**: OPFS provides synchronous I/O via `FileSystemSyncAccessHandle` - but only in Dedicated Web Workers. By running the persistent groove instance in a worker, we get sync storage without blocking the UI.

**The architecture**:
- **Main thread**: Groove with memory-only storage (always sync, always fast)
- **Worker**: Groove with OPFS storage (sync within worker), acts as upstream server
- **Native**: Groove with sync file I/O (single process, no worker needed)

### Key Decisions (Resolved)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **bf-tree integration** | Full key-value store | bf-tree has range queries - that's all we need for index scans. Simpler than maintaining our own B-tree. |
| **Index encoding** | Composite keys | `idx:{table}:{col}:{value}:{row_id}` - range scan on prefix gives index lookups naturally. |
| **Tab coordination** | Leader election | One tab's worker owns OPFS, others sync through it. Leader election on tab close. |
| **Leader failover** | Accept potential loss | Fire-and-forget means user accepted this. Lost writes are lost. Simplest. |
| **Native architecture** | Single process | No worker needed. Groove uses sync filesystem directly. Simpler, native-optimized. |
| **Durability default** | Fire-and-forget | Optimistic by default. Promise-based API (`await todo.persisted()`) for explicit durability. |
| **Persistence API** | Promise-based | `await db.todos.create(...).persisted()` returns Promise that resolves on worker ACK. |

---

## Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              BROWSER                                     │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                        MAIN THREAD                               │    │
│  │  ┌─────────────────────────────────────────────────────────┐    │    │
│  │  │  Groove (MemoryVfs)                                      │    │    │
│  │  │   - All operations sync                                  │    │    │
│  │  │   - No Loading states                                    │    │    │
│  │  │   - No pending tracking                                  │    │    │
│  │  │   - Acts as cache of worker state                        │    │    │
│  │  └─────────────────────────────────────────────────────────┘    │    │
│  │                              │                                   │    │
│  │                         SyncProtocol                             │    │
│  │                        (postMessage)                             │    │
│  │                              │                                   │    │
│  └──────────────────────────────┼──────────────────────────────────┘    │
│                                 │                                        │
│  ┌──────────────────────────────┼──────────────────────────────────┐    │
│  │                        DEDICATED WORKER                          │    │
│  │                              │                                   │    │
│  │  ┌─────────────────────────────────────────────────────────┐    │    │
│  │  │  Groove (OpfsVfs)                                        │    │    │
│  │  │   - All operations sync (OPFS SyncAccessHandle)          │    │    │
│  │  │   - Durable persistence                                  │    │    │
│  │  │   - Single connection to upstream server                 │    │    │
│  │  └─────────────────────────────────────────────────────────┘    │    │
│  │                              │                                   │    │
│  └──────────────────────────────┼──────────────────────────────────┘    │
│                                 │                                        │
└─────────────────────────────────┼────────────────────────────────────────┘
                                  │
                             WebSocket
                                  │
                                  ▼
                         ┌─────────────────┐
                         │  Edge Server    │
                         │  (Groove)       │
                         └─────────────────┘
```

---

## Phase Dependencies

```
Phase 1 (Sync IoHandler trait)
         │
         ▼
Phase 2 (Object mgmt) ──► Phase 3 (Delete B-tree) ──► Phase 4 (Query graphs)
         │                                                      │
         └──────────────────────────────────────────────────────┤
                                                                ▼
                                                         Phase 5 (Tests)
                                                                │
         ┌──────────────────────────────────────────────────────┤
         │                                                      │
         ▼                                                      ▼
Phase 7 (bf-tree impl)                                Phase 6 (Durability)
         │                                                      │
         └──────────────────────┬───────────────────────────────┘
                                │
                                ▼
                         Phase 8 (jazz-ts worker)
                                │
                                ▼
                         Phase 9 (E2E verification)
```

**Key insight**: bf-tree integration (Phase 7) can happen LATE. We define the sync `IoHandler` trait first with index methods built-in, implement a simple `MemoryIoHandler` for testing, and rewrite everything against that. bf-tree becomes just "implement the trait with persistence."

This allows us to:
1. Validate the sync architecture works with in-memory testing
2. Get all groove tests passing before touching bf-tree
3. Defer bf-tree complexity until we're confident in the design

---

## Phase 1: Synchronous IoHandler Trait

**Goal**: Replace the async `IoHandler` pattern with a synchronous trait that includes storage AND index operations.

### Current (Async)

```rust
pub trait IoHandler {
    /// Fire-and-forget storage request - response comes later
    fn send_storage_request(&mut self, request: StorageRequest);

    /// Sync message sending (already sync!)
    fn send_sync_message(&mut self, message: OutboxEntry);

    /// Schedule batched tick
    fn schedule_batched_tick(&self);

    /// Take pending responses (for sync drivers)
    fn take_pending_responses(&mut self) -> Vec<StorageResponse>;
}

// Caller:
io_handler.send_storage_request(StorageRequest::LoadObjectBranch { ... });
// ... later ...
fn park_storage_response(&mut self, response: StorageResponse) { ... }
```

### New (Sync)

The new `IoHandler` has **synchronous storage and index methods**. Index operations are built into the trait (not a separate abstraction):

```rust
/// Synchronous I/O handler for storage, indices, and sync messages.
pub trait IoHandler: Send + Sync {
    // ================================================================
    // Object storage (sync - returns immediately with result)
    // ================================================================

    fn create_object(&mut self, id: ObjectId, metadata: HashMap<String, String>) -> Result<(), StorageError>;
    fn load_object_metadata(&self, id: ObjectId) -> Result<HashMap<String, String>, StorageError>;
    fn load_branch(&self, object_id: ObjectId, branch: &BranchName) -> Result<LoadedBranch, StorageError>;
    fn append_commit(&mut self, object_id: ObjectId, branch: &BranchName, commit: Commit) -> Result<(), StorageError>;
    fn delete_commit(&mut self, object_id: ObjectId, branch: &BranchName, commit_id: CommitId) -> Result<(), StorageError>;
    fn set_branch_tails(&mut self, object_id: ObjectId, branch: &BranchName, tails: Option<HashSet<CommitId>>) -> Result<(), StorageError>;

    // ================================================================
    // Blob storage (sync)
    // ================================================================

    fn store_blob(&mut self, hash: ContentHash, data: &[u8]) -> Result<(), StorageError>;
    fn load_blob(&self, hash: ContentHash) -> Result<Vec<u8>, StorageError>;
    fn delete_blob(&mut self, hash: ContentHash) -> Result<(), StorageError>;

    // ================================================================
    // Index operations (sync - the key innovation)
    // ================================================================
    //
    // These replace our entire BTreeIndex implementation.
    // Implementations can use bf-tree, SQLite, or simple HashMaps.

    /// Insert an index entry.
    fn index_insert(&mut self, table: &str, column: &str, value: &[u8], row_id: ObjectId) -> Result<(), StorageError>;

    /// Remove an index entry.
    fn index_remove(&mut self, table: &str, column: &str, value: &[u8], row_id: ObjectId) -> Result<(), StorageError>;

    /// Lookup exact value - returns all row IDs with this value.
    fn index_lookup(&self, table: &str, column: &str, value: &[u8]) -> Vec<ObjectId>;

    /// Range scan - returns row IDs where start <= value < end.
    fn index_range(&self, table: &str, column: &str, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<ObjectId>;

    /// Full scan - returns all row IDs in this index.
    fn index_scan_all(&self, table: &str, column: &str) -> Vec<ObjectId>;

    // ================================================================
    // Sync messages (already sync in current design)
    // ================================================================

    fn send_sync_message(&mut self, message: OutboxEntry);

    // ================================================================
    // Scheduling (may still be needed for subscription batching)
    // ================================================================

    fn schedule_batched_tick(&self);
}
```

### Implementations

| Implementation | Use Case | Index Backing |
|----------------|----------|---------------|
| `MemoryIoHandler` | Tests, main thread | `HashMap<(table, col, value), Vec<ObjectId>>` |
| `BfTreeIoHandler` | Worker (OPFS), Native | bf-tree with composite keys |

**`MemoryIoHandler`** is simple and sufficient for:
- All groove unit tests
- All groove integration tests
- Main thread in browser (it's just a cache)

**`BfTreeIoHandler`** adds persistence and is only needed for:
- Worker with OPFS
- Native with filesystem

### What Gets Deleted

- `StorageRequest` enum
- `StorageResponse` enum
- `park_storage_response()` method
- `take_pending_responses()` method
- All async response handling
- `NullIoHandler` (replace with `MemoryIoHandler`)
- `TestIoHandler` (replace with `MemoryIoHandler`)
- `DelayedIoHandler` (no longer needed - everything is sync)

### What Gets Deleted

- `IoHandler` trait
- `StorageRequest` enum
- `StorageResponse` enum
- `NullIoHandler`, `TestIoHandler`, `DelayedIoHandler`
- `RuntimeCore.park_storage_response()`
- All `outbox`/`inbox` queues for storage

---

## Phase 2: Synchronous Object Management

**Goal**: Remove all `Loading` states and async error variants from ObjectManager.

### State Simplification

```rust
// Before: Three states
pub enum ObjectState {
    Creating(Object),
    Loading,           // DELETE
    Available(Object),
}

// After: Two states
pub enum ObjectState {
    Creating(Object),   // Local, not yet persisted
    Available(Object),  // Persisted (or loaded from storage)
}
```

```rust
// Before: Four states
enum BlobState {
    Available { data, stored_state },
    Loading,        // DELETE
    NotFound,
    PendingDelete,
}

// After: Three states
enum BlobState {
    Available { data, stored_state },
    NotFound,
    PendingDelete,
}
```

### Error Simplification

```rust
// Before
pub enum Error {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
    ParentNotFound(CommitId),
    ObjectNotReady(ObjectId),      // DELETE
    BranchNotLoaded(BranchName),   // DELETE
    BlobNotLoaded(ContentHash),    // DELETE
    BlobNotFound(ContentHash),
    StorageError(StorageError),
}

// After
pub enum Error {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
    ParentNotFound(CommitId),
    BlobNotFound(ContentHash),
    StorageError(StorageError),
}
```

### API Changes

```rust
// Before: Returns bool, might queue async load
pub fn request_load(&mut self, object_id: ObjectId, branch: BranchName) -> bool;

// After: Loads synchronously, returns the object
pub fn load(&mut self, object_id: ObjectId, branch: &BranchName) -> Result<&Object, Error>;
```

### What Gets Deleted

- `ObjectState::Loading`
- `BlobState::Loading`
- `Error::ObjectNotReady`
- `Error::BranchNotLoaded`
- `Error::BlobNotLoaded`
- `BranchLoadedState` enum (always fully loaded)
- `request_load()` method
- `is_loading()` method
- All "retry later" patterns

---

## Phase 3: Delete Our B-tree Implementation

**Goal**: Remove our entire B-tree index implementation - it's replaced by `IoHandler` index methods.

> **Note**: This does NOT require bf-tree. `MemoryIoHandler` implements the index methods with simple HashMaps. bf-tree (Phase 7) is just one persistent implementation of the same interface.

### What Gets Deleted

Our entire custom B-tree:
- `crates/groove/src/query_manager/index/btree_index.rs`
- `crates/groove/src/query_manager/index/btree_page.rs`
- `crates/groove/src/query_manager/index/mod.rs`

All related types:
- `PageId`, `PageState`, `BTreePage`, `IndexMeta`, `LeafEntry`
- `IndexError` enum
- `pending_inserts`, `pending_deletes`
- `is_ready()`, `ensure_meta_loading()`, `process_meta_load()`, etc.

### New Behavior

Index operations become calls to `IoHandler`:

```rust
// Before: Complex B-tree with async page loading
impl BTreeIndex {
    pub fn insert(&mut self, key: &[u8], row_id: ObjectId) -> Result<bool, IndexError> {
        if !self.is_ready() {
            self.pending_inserts.push((key.to_vec(), row_id));
            return Ok(false);
        }
        match self.insert_into_tree(key, row_id) {
            Ok(()) => { ... }
            Err(IndexError::PageNotLoaded(page_id)) => { ... }
        }
    }
}

// After: One-liner to IoHandler
impl QueryManager {
    pub fn index_insert(&mut self, table: &str, column: &str, value: &Value, row_id: ObjectId) {
        let encoded = encode_index_value(value);
        self.io.index_insert(table, column, &encoded, row_id).unwrap();
    }

    pub fn index_scan(&self, table: &str, column: &str, range: Range) -> Vec<ObjectId> {
        let (start, end) = encode_range(range);
        self.io.index_range(table, column, start.as_deref(), end.as_deref())
    }
}
```

### QueryManager Simplification

The `QueryManager` no longer manages index structures internally:

```rust
// Before
pub struct QueryManager {
    indices: HashMap<(String, String), BTreeIndex>,
    // ...
}

// After
pub struct QueryManager {
    io: Arc<dyn IoHandler>,  // Indices accessed via io.index_*() methods
    // ...
}
```

---

## Phase 4: Synchronous Query Graphs

**Goal**: Remove all pending tracking from query graph nodes.

### MaterializeNode Simplification

```rust
// Before
pub struct MaterializeNode {
    pending_ids: AHashMap<ObjectId, String>,  // DELETE
    // ...
}

impl MaterializeNode {
    pub fn has_pending(&self) -> bool;           // DELETE
    pub fn pending_ids(&self) -> impl Iterator;  // DELETE
    pub fn check_pending_tuples(&mut self, ...); // DELETE
}

// After
pub struct MaterializeNode {
    rows: AHashMap<ObjectId, Row>,
    current_tuples: AHashSet<Tuple>,
    // No pending tracking - materialize always succeeds
}
```

### TupleDelta Simplification

```rust
// Before
pub struct TupleDelta {
    pub added: Vec<Tuple>,
    pub removed: Vec<Tuple>,
    pub updated: Vec<(Tuple, Tuple)>,
    pub pending: bool,  // DELETE
}

// After
pub struct TupleDelta {
    pub added: Vec<Tuple>,
    pub removed: Vec<Tuple>,
    pub updated: Vec<(Tuple, Tuple)>,
    // No pending flag - deltas are always complete
}
```

### What Gets Deleted

- `MaterializeNode.pending_ids`
- `MaterializeNode.has_pending()`
- `MaterializeNode.check_pending_tuples()`
- `TupleDelta.pending` field
- All "pending" checks in graph traversal
- `IndexScanNode` pending machinery

---

## Phase 5: Test Adaptation

**Goal**: All existing groove tests pass with the new sync storage.

### Test Infrastructure Changes

```rust
// Before: DelayedIoHandler for simulating async
let handler = DelayedIoHandler::new();
let mut core = RuntimeCore::new(schema, handler);
// ... do stuff ...
handler.flush();  // Simulate async response

// After: Just use MemoryStorage
let storage = MemoryStorage::new();
let mut core = RuntimeCore::new(schema, storage);
// ... do stuff - everything is immediate
```

### Tests to Delete

All tests specifically testing async behavior:
- `delayed_io_tests` module (Phases 0-4 tests)
- Any test using `DelayedIoHandler`
- Tests for `pending` states

### Tests to Adapt

Tests that happen to use async infrastructure but test other behavior:
- Change `TestIoHandler` → `MemoryStorage`
- Remove `flush()` calls
- Remove "retry after response" patterns

---

## Phase 6: Durability Confirmation Protocol

**Goal**: Extend sync protocol with persistence acknowledgment and query settlement.

### New SyncPayload Variants

```rust
pub enum SyncPayload {
    // ... existing variants ...

    /// Confirms commits have been durably persisted at this tier.
    PersistenceAck {
        object_id: ObjectId,
        branch_name: BranchName,
        /// Commit IDs confirmed persisted.
        confirmed_commits: HashSet<CommitId>,
        /// Which tier is confirming (for multi-tier scenarios).
        tier: PersistenceTier,
    },

    /// Confirms all current data for a query has been sent.
    /// Sent after QueryRegistration when initial sync is complete.
    QuerySettled {
        query_id: QueryId,
        /// Which tier has settled (for multi-tier scenarios).
        tier: PersistenceTier,
    },
}

/// Identifies which tier is confirming persistence/settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PersistenceTier {
    /// Local worker (OPFS persistence).
    Worker,
    /// Edge server (regional persistence).
    EdgeServer,
    /// Global core server (global persistence).
    CoreServer,
}
```

### Durability API: Fire-and-Forget with Optional Persistence Promises

**Default: Fire-and-forget** - writes return immediately, persistence happens async.

```typescript
// Default: returns immediately, persists in background
const todo = db.todos.create({ title: "Buy milk" });
// todo is usable immediately (in memory)
// May be lost if tab closes before worker persists
```

**Optional: Wait for persistence** - Promise-based API for explicit durability.

```typescript
// Wait for worker (local OPFS) persistence
await todo.persisted();  // or: await todo.persisted('worker')

// Wait for edge server persistence
await todo.persisted('edge');

// Wait for core server persistence (strongest guarantee)
await todo.persisted('core');

// Can also chain
const todo = await db.todos.create({ title: "Buy milk" }).persisted('edge');
```

### Implementation

```typescript
interface PersistedPromise<T> extends Promise<T> {
    /** Wait for persistence at the specified tier. */
    persisted(tier?: PersistenceTier): Promise<T>;
}

class MutationResult<T> implements PersistedPromise<T> {
    private value: T;
    private commitId: CommitId;
    private bridge: WorkerBridge;

    constructor(value: T, commitId: CommitId, bridge: WorkerBridge) {
        this.value = value;
        this.commitId = commitId;
        this.bridge = bridge;
    }

    // Immediately resolves with the value (fire-and-forget)
    then<R>(onFulfilled: (value: T) => R): Promise<R> {
        return Promise.resolve(this.value).then(onFulfilled);
    }

    // Waits for persistence acknowledgment
    async persisted(tier: PersistenceTier = 'worker'): Promise<T> {
        await this.bridge.waitForPersistence(this.commitId, tier);
        return this.value;
    }
}
```

### Multi-Tier Acknowledgment Flow

```
Main Thread          Worker              Edge Server         Core Server
     │                  │                     │                   │
     │ ── create() ──►  │                     │                   │
     │ ◄── (immediate)  │                     │                   │
     │                  │                     │                   │
     │                  │ ── persist ──►      │                   │
     │                  │ ◄── (sync I/O)      │                   │
     │ ◄── PersistAck   │                     │                   │
     │    (tier:Worker) │                     │                   │
     │                  │                     │                   │
     │                  │ ── sync ──────────► │                   │
     │                  │ ◄── PersistAck ──── │                   │
     │ ◄── PersistAck   │    (tier:Edge)      │                   │
     │    (tier:Edge)   │                     │                   │
     │                  │                     │ ── sync ────────► │
     │                  │                     │ ◄── PersistAck ── │
     │ ◄── PersistAck   │                     │    (tier:Core)    │
     │    (tier:Core)   │                     │                   │
```

The `persisted()` Promise resolves when the requested tier's `PersistAck` arrives.

### Query Settlement Levels

Similarly for queries:

```rust
pub enum SettlementLevel {
    /// Settled on local data only.
    Local,
    /// Settled including worker data.
    Worker,
    /// Settled including edge server data.
    EdgeServer,
    /// Settled including core server data (authoritative).
    CoreServer,
}

// Usage in jazz-ts
const todos = await db.todos.findAll({
    settlement: SettlementLevel.EdgeServer
});
// Returns only after edge server confirms "end of initial results"
```

### Cold Start Hydration Flow

```
Main Thread                    Worker
     │                            │
     │  ──── Connect ────────►    │
     │                            │
     │  ◄─── QuerySettled ─────   │  (worker sends current state)
     │       (tier: Worker)       │
     │                            │
     │  [UI can now render]       │
     │                            │
```

---

## Phase 7: bf-tree-web Integration

**Goal**: Implement `BfTreeIoHandler` - a persistent `IoHandler` backed by bf-tree.

> **This phase can happen LATE.** All prior phases (1-6) work with `MemoryIoHandler`. bf-tree is only needed when we want actual persistence (worker with OPFS, native with filesystem).

### Why bf-tree-web?

1. **Synchronous OPFS**: Already has working `OpfsVfs` with `FileSystemSyncAccessHandle`
2. **Cross-platform VFS**: `MemoryVfs`, `StdVfs`, `OpfsVfs` all implement same trait
3. **Production-tested**: Based on Microsoft Research's bf-tree
4. **WAL support**: Write-ahead logging for crash recovery
5. **Range queries**: Built-in `scan()` - exactly what `IoHandler.index_range()` needs

### Integration Approach: Full Key-Value Store

Implement `IoHandler` trait using bf-tree. Store everything as key-value pairs with composite keys designed for range scans.

**bf-tree provides:**
- `insert(key, value)` - O(log n)
- `read(key)` - O(log n)
- `delete(key)` - O(log n)
- `scan(start_key, end_key)` - range queries!

**Note**: Our `BTreeIndex` was already deleted in Phase 3. bf-tree replaces it via the `IoHandler` index methods.

### Key Encoding Scheme

All data lives in a single bf-tree with carefully designed composite keys:

```rust
/// Key prefixes for different data types.
/// Keys are designed so lexicographic ordering enables efficient range scans.

// Object metadata
// Key:   "obj:{object_id}:meta"
// Value: JSON metadata

// Branch tips
// Key:   "obj:{object_id}:branch:{branch_name}:tips"
// Value: serialized HashSet<CommitId>

// Individual commits
// Key:   "obj:{object_id}:branch:{branch_name}:commit:{commit_id}"
// Value: serialized Commit

// Blobs (content-addressed)
// Key:   "blob:{content_hash}"
// Value: raw blob data

// Secondary index entries (THE KEY INSIGHT)
// Key:   "idx:{table}:{column}:{encoded_value}:{object_id}"
// Value: empty (presence is the information)
//
// Example: User with age=25 and id=abc123
// Key:   "idx:users:age:\x00\x00\x00\x19:abc123"
//        (age encoded as big-endian u32 for correct sort order)
//
// Range scan for age >= 20 AND age < 30:
// scan("idx:users:age:\x00\x00\x00\x14", "idx:users:age:\x00\x00\x00\x1e")
```

### Value Encoding for Sortable Keys

Index keys must sort correctly for range queries:

```rust
/// Encode a value for use in composite index keys.
/// Encoding preserves sort order for the value type.
fn encode_index_value(value: &Value) -> Vec<u8> {
    match value {
        // Integers: big-endian with sign bit flipped for correct ordering
        Value::Int(n) => {
            let mut bytes = (*n as i64 ^ i64::MIN).to_be_bytes().to_vec();
            bytes.insert(0, 0x01); // type tag
            bytes
        }
        // Strings: UTF-8 bytes, null-terminated
        Value::Text(s) => {
            let mut bytes = vec![0x02]; // type tag
            bytes.extend(s.as_bytes());
            bytes.push(0x00); // terminator
            bytes
        }
        // UUIDs: raw bytes (already sort correctly)
        Value::Uuid(id) => {
            let mut bytes = vec![0x03]; // type tag
            bytes.extend(id.as_bytes());
            bytes
        }
        // Null: sorts before all values
        Value::Null => vec![0x00],
        // ... other types
    }
}
```

### Storage Trait Implementation

```rust
pub struct BfTreeStorage {
    tree: BfTree,
}

impl Storage for BfTreeStorage {
    fn create_object(&mut self, id: ObjectId, metadata: HashMap<String, String>) -> Result<(), StorageError> {
        let key = format!("obj:{}:meta", id.uuid());
        let value = serde_json::to_vec(&metadata)?;
        self.tree.insert(key.as_bytes(), &value);
        Ok(())
    }

    fn append_commit(&mut self, object_id: ObjectId, branch: &BranchName, commit: Commit) -> Result<(), StorageError> {
        // Store commit
        let commit_key = format!("obj:{}:branch:{}:commit:{}",
            object_id.uuid(), branch.as_str(), commit.id());
        self.tree.insert(commit_key.as_bytes(), &commit.serialize());

        // Update tips (read-modify-write)
        let tips_key = format!("obj:{}:branch:{}:tips", object_id.uuid(), branch.as_str());
        let mut tips: HashSet<CommitId> = self.tree.read(tips_key.as_bytes())
            .map(|data| deserialize(&data))
            .unwrap_or_default();

        for parent in &commit.parents {
            tips.remove(parent);
        }
        tips.insert(commit.id());

        self.tree.insert(tips_key.as_bytes(), &serialize(&tips));
        Ok(())
    }

    // Index operations now use bf-tree's range queries directly
    fn index_insert(&mut self, table: &str, column: &str, value: &Value, row_id: ObjectId) -> Result<(), StorageError> {
        let key = format!("idx:{}:{}:{}:{}",
            table, column, hex::encode(encode_index_value(value)), row_id.uuid());
        self.tree.insert(key.as_bytes(), &[]); // Empty value - key presence is enough
        Ok(())
    }

    fn index_scan(&self, table: &str, column: &str, start: Option<&Value>, end: Option<&Value>) -> Vec<ObjectId> {
        let prefix = format!("idx:{}:{}:", table, column);
        let start_key = start
            .map(|v| format!("{}{}", prefix, hex::encode(encode_index_value(v))))
            .unwrap_or_else(|| prefix.clone());
        let end_key = end
            .map(|v| format!("{}{}", prefix, hex::encode(encode_index_value(v))))
            .unwrap_or_else(|| format!("{}~", prefix)); // ~ sorts after hex chars

        self.tree.scan(start_key.as_bytes(), end_key.as_bytes())
            .map(|(key, _)| {
                // Extract ObjectId from end of key
                let key_str = std::str::from_utf8(&key).unwrap();
                let id_str = key_str.rsplit(':').next().unwrap();
                ObjectId::from_uuid(Uuid::parse_str(id_str).unwrap())
            })
            .collect()
    }
}
```

---

## Phase 8: jazz-ts Worker Architecture

**Goal**: Implement the main thread ↔ worker architecture in TypeScript.

### Package Structure

```
packages/jazz-ts/
├── src/
│   ├── worker/
│   │   ├── groove-worker.ts    # Worker entry point
│   │   ├── storage-opfs.ts     # OPFS storage implementation
│   │   └── worker-protocol.ts  # Message types
│   ├── runtime/
│   │   ├── client.ts           # Main thread client (existing, modified)
│   │   ├── worker-bridge.ts    # Main thread ↔ worker communication
│   │   └── storage-memory.ts   # Memory storage for main thread
│   └── ...
```

### Worker Protocol (over postMessage)

```typescript
// Main thread → Worker
type MainToWorkerMessage =
    | { type: 'sync'; payload: SyncPayload }
    | { type: 'query-register'; queryId: number; queryJson: string }
    | { type: 'query-unregister'; queryId: number }
    | { type: 'connect-upstream'; url: string };

// Worker → Main thread
type WorkerToMainMessage =
    | { type: 'sync'; payload: SyncPayload }
    | { type: 'persistence-ack'; payload: PersistenceAck }
    | { type: 'query-settled'; queryId: number; tier: PersistenceTier }
    | { type: 'ready' };  // Worker initialized
```

### Initialization Flow

```typescript
// jazz-ts/src/runtime/client.ts

export async function createDb<S extends Schema>(options: DbOptions<S>): Promise<Db<S>> {
    // 1. Spawn worker
    const worker = new Worker(new URL('./worker/groove-worker.ts', import.meta.url));

    // 2. Wait for worker ready
    await waitForMessage(worker, 'ready');

    // 3. Create main-thread groove with MemoryStorage
    const mainGroove = new Groove(new MemoryStorage());

    // 4. Connect main groove to worker as "upstream server"
    const bridge = new WorkerBridge(worker, mainGroove);

    // 5. Register initial queries, wait for settlement
    await bridge.registerQuery(initialQuery, { settlement: 'worker' });

    // 6. Return Db interface
    return new Db(mainGroove, bridge);
}
```

### Tab Coordination: Leader Election

Multiple tabs share the same OPFS origin, but only ONE can hold the `FileSystemSyncAccessHandle` at a time. We use leader election:

```
Tab A (LEADER)                Tab B (FOLLOWER)           Tab C (FOLLOWER)
      │                              │                          │
      ▼                              ▼                          ▼
┌─────────────┐              ┌─────────────┐            ┌─────────────┐
│ Worker A    │◄────────────►│ Worker B    │◄──────────►│ Worker C    │
│ (has OPFS)  │  BroadcastCh │ (mem only)  │            │ (mem only)  │
│ + WebSocket │              │             │            │             │
└─────────────┘              └─────────────┘            └─────────────┘
      │
      ▼
 Edge Server
```

**Leader responsibilities:**
- Holds exclusive OPFS `SyncAccessHandle`
- Maintains WebSocket to upstream server
- Broadcasts changes to follower tabs via BroadcastChannel
- Persists data from all tabs

**Follower behavior:**
- Memory-only storage (like main thread)
- Sends writes to leader via BroadcastChannel
- Receives updates from leader
- No direct server connection

**Leader election protocol:**

```typescript
// On worker startup
const LEADER_KEY = 'jazz-leader';
const LEADER_HEARTBEAT_MS = 1000;
const LEADER_TIMEOUT_MS = 3000;

async function electLeader(): Promise<boolean> {
    const channel = new BroadcastChannel('jazz-leader-election');

    // Try to claim leadership
    const myId = crypto.randomUUID();
    const claim = { type: 'claim', id: myId, timestamp: Date.now() };

    // Listen for competing claims
    let isLeader = true;
    channel.onmessage = (e) => {
        if (e.data.type === 'claim' && e.data.timestamp < claim.timestamp) {
            isLeader = false; // Older claim wins
        }
        if (e.data.type === 'heartbeat' && e.data.id !== myId) {
            isLeader = false; // Someone else is leader
        }
    };

    channel.postMessage(claim);
    await sleep(100); // Wait for competing claims

    if (isLeader) {
        // Start heartbeat
        setInterval(() => {
            channel.postMessage({ type: 'heartbeat', id: myId });
        }, LEADER_HEARTBEAT_MS);

        // Open OPFS
        await openOpfsStorage();
    }

    return isLeader;
}
```

**Failover on leader tab close:**

When leader tab closes unexpectedly:
1. Heartbeat stops
2. After `LEADER_TIMEOUT_MS`, followers detect leader loss
3. Remaining tabs run election
4. New leader opens OPFS, becomes authoritative
5. **In-flight writes from old leader may be lost** (fire-and-forget semantics)

This is acceptable because:
- Fire-and-forget is the default durability level
- Users who need guarantees use `await write.persisted()`
- Simplest possible failover - no WAL replay complexity

---

## Phase 9: End-to-End Verification

**Goal**: All integration tests and example apps work.

### Test Levels

1. **Unit tests** (groove crate): Sync storage, no async
2. **Integration tests** (groove crate): Full query graphs, sync
3. **WASM tests** (groove-wasm): Worker + main thread
4. **E2E tests** (jazz-ts): Full stack including React bindings
5. **Example apps**: todo-ts-client, etc.

### What Should Work

- Fresh start: Empty database, create objects, query them
- Cold start: Reopen database, data persisted in OPFS
- Sync: Connect to server, receive updates
- Multi-tab: Changes in one tab appear in another
- Offline: Works without server connection
- Reconnect: Syncs accumulated changes when server reconnects

---

## Migration Notes

### Code Deletion Checklist

**groove crate - Async Storage Infrastructure:**
- [ ] `IoHandler` trait and all implementations
- [ ] `StorageRequest` / `StorageResponse` enums
- [ ] `NullIoHandler`, `TestIoHandler`, `DelayedIoHandler`
- [ ] `park_storage_response()` method
- [ ] `take_pending_responses()` method
- [ ] `schedule_batched_tick()` (review if still needed)
- [ ] All `delayed_io_tests` module (Phases 0-4)

**groove crate - Loading States:**
- [ ] `ObjectState::Loading`
- [ ] `BlobState::Loading`
- [ ] `PageState` enum entirely
- [ ] `BranchLoadedState` enum
- [ ] `Error::ObjectNotReady`, `BranchNotLoaded`, `BlobNotLoaded`
- [ ] `is_loading()` method
- [ ] `request_load()` method

**groove crate - Our B-tree Implementation (replaced by bf-tree):**
- [ ] `btree_index.rs` - entire file
- [ ] `btree_page.rs` - entire file
- [ ] `query_manager/index/mod.rs`
- [ ] `PageId`, `PageState`, `BTreePage`, `IndexMeta`, `LeafEntry` structs
- [ ] `IndexError` enum (or most of it)
- [ ] `pending_inserts` / `pending_deletes` vectors
- [ ] `is_ready()` method
- [ ] `ensure_meta_loading()` / `ensure_page_loading()`
- [ ] `process_meta_load()` / `process_page_load()`
- [ ] `take_storage_requests()` method

**groove crate - Query Graph Pending Machinery:**
- [ ] `MaterializeNode.pending_ids`
- [ ] `MaterializeNode.has_pending()`
- [ ] `MaterializeNode.check_pending_tuples()`
- [ ] `TupleDelta.pending` field
- [ ] All "pending" checks in graph traversal

**groove-wasm crate:**
- [ ] Async storage callback pattern
- [ ] `onStorageResponse()` method

**jazz-ts:**
- [ ] Current `JazzClient.connect()` async storage setup
- [ ] Storage driver abstraction (replaced by worker)

### New Code Checklist

**groove crate:**
- [ ] New sync `IoHandler` trait (with index methods)
- [ ] `MemoryIoHandler` implementation (for tests + main thread)
- [ ] Key encoding functions (`encode_index_value`, etc.)
- [ ] `PersistenceAck` and `QuerySettled` sync payload variants
- [ ] `PersistenceTier` enum
- [ ] `BfTreeIoHandler` implementation (Phase 7, for persistence)

**groove-wasm crate:**
- [ ] Worker entry point (`groove-worker.ts` or Rust-based)
- [ ] OPFS initialization (async open, sync operations after)
- [ ] Worker ↔ main thread postMessage protocol
- [ ] Leader election implementation

**jazz-ts:**
- [ ] `WorkerBridge` class (main thread side)
- [ ] Worker spawning and lifecycle management
- [ ] Leader election via BroadcastChannel
- [ ] `MutationResult` with `.persisted()` Promise API
- [ ] `PersistenceTier` types and API

---

## Resolved Decisions

| Question | Decision |
|----------|----------|
| bf-tree integration depth | Full key-value store with composite keys |
| Tab coordination mechanism | Leader election with BroadcastChannel |
| Native story | Single process with sync file I/O |
| Durability default | Fire-and-forget, Promise-based `.persisted()` API |
| Persistence API style | Promise-based |
| Leader failover | Accept potential loss (fire-and-forget semantics) |

## Open Questions

1. **Key encoding edge cases**: How to handle composite indices (multiple columns)? NULL ordering?

2. **bf-tree-web fork maintenance**: Do we maintain our own fork, or upstream changes?

3. **OPFS quota handling**: What happens when OPFS storage quota is exceeded?

4. **Worker bundling**: How to bundle the worker code for different build systems (Vite, webpack, etc.)?

---

## Success Criteria

### Code Quality
- [ ] No `Loading` states anywhere in codebase
- [ ] No `pending` fields anywhere in codebase
- [ ] No async storage patterns (fire-and-forget + callback)
- [ ] No `IoHandler`, `StorageRequest`, `StorageResponse`
- [ ] No `btree_index.rs`, `btree_page.rs` (replaced by bf-tree)
- [ ] `grep -r "PageNotLoaded\|ObjectNotReady\|BranchNotLoaded" src/` returns nothing

### Tests
- [ ] All groove unit tests pass
- [ ] All groove integration tests pass
- [ ] groove-wasm builds without errors
- [ ] jazz-ts builds without errors
- [ ] jazz-ts tests pass

### Browser Functionality
- [ ] Worker spawns and initializes
- [ ] Worker opens OPFS with sync access
- [ ] Main thread ↔ worker sync protocol works
- [ ] Cold start correctly hydrates from worker
- [ ] Data persists across page reloads
- [ ] Leader election works across tabs
- [ ] Leader failover works when leader tab closes
- [ ] `.persisted()` Promise resolves on worker ACK

### Server Sync
- [ ] Worker connects to upstream server
- [ ] Writes propagate: main thread → worker → server
- [ ] Updates propagate: server → worker → main thread
- [ ] Multi-tier `.persisted('edge')` works

### Example Apps
- [ ] todo-ts-client works end-to-end
- [ ] Fresh start creates data correctly
- [ ] Page reload preserves data
- [ ] Multi-tab shows same data
- [ ] Offline mode works (no server)
- [ ] Reconnection syncs correctly

### Performance (TBD benchmarks)
- [ ] Insert latency < X ms (fire-and-forget)
- [ ] Query latency < X ms
- [ ] Cold start < X ms
- [ ] Worker persistence overhead acceptable
