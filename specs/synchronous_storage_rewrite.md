# Synchronous Storage Architecture Rewrite

> **THIS IS A MASSIVE REWRITE.**
>
> We are throwing away the async storage assumption that permeates the entire codebase. This is not a refactor - it's a ground-up rearchitecture. Be aggressive. No backwards compatibility. No migration paths. No deprecation periods.
>
> **Priorities:**
>
> 1. Correctness and architectural clarity over speed
> 2. Thoroughness over incremental progress
> 3. Delete aggressively - less code is better
> 4. If something doesn't fit the new model, remove it entirely
>
> This will touch every layer: storage, object management, indices, query graphs, sync protocol, WASM bindings, and TypeScript client.

---

## Motivation

The current architecture assumes storage is asynchronous. This assumption infects every layer:

| Layer             | Async Complexity                                                                                |
| ----------------- | ----------------------------------------------------------------------------------------------- |
| **BTreeIndex**    | `is_ready()`, `PageState::Loading`, `pending_inserts/deletes`, `IndexError::PageNotLoaded`      |
| **ObjectManager** | `ObjectState::Loading`, `BlobState::Loading`, `Error::ObjectNotReady`, `Error::BranchNotLoaded` |
| **QueryManager**  | `MaterializeNode.pending_ids`, `TupleDelta.pending`, retry loops                                |
| **RuntimeCore**   | `park_storage_response()`, `IoHandler` trait, batched tick scheduling                           |

This complexity exists because WASM can't block the main thread on IndexedDB/OPFS async APIs.

**The insight**: OPFS provides synchronous I/O via `FileSystemSyncAccessHandle` - but only in Dedicated Web Workers. By running the persistent groove instance in a worker, we get sync storage without blocking the UI.

**The architecture**:

- **Main thread**: Groove with memory-only storage (always sync, always fast)
- **Worker**: Groove with OPFS storage (sync within worker), acts as upstream server
- **Native**: Groove with sync file I/O (single process, no worker needed)

### Key Decisions (Resolved)

| Decision                | Choice                | Rationale                                                                                                |
| ----------------------- | --------------------- | -------------------------------------------------------------------------------------------------------- |
| **bf-tree integration** | Full key-value store  | bf-tree has range queries - that's all we need for index scans. Simpler than maintaining our own B-tree. |
| **Index encoding**      | Composite keys        | `idx:{table}:{col}:{value}:{row_id}` - range scan on prefix gives index lookups naturally.               |
| **Tab coordination**    | Leader election       | One tab's worker owns OPFS, others sync through it. Leader election on tab close.                        |
| **Leader failover**     | Accept potential loss | Fire-and-forget means user accepted this. Lost writes are lost. Simplest.                                |
| **Native architecture** | Single process        | No worker needed. Groove uses sync filesystem directly. Simpler, native-optimized.                       |
| **Durability default**  | Fire-and-forget       | Optimistic by default. `_persisted` API variants for explicit durability.                                |
| **Persistence API**     | `_persisted` variants | `createPersisted()`/`insertPersisted()` return Promises that resolve on tier ACK.                        |

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
Phase 7 (bf-tree impl)                             Phase 6a (Write acks, Rust)
         │                                                      │
         │                                               Phase 6b (Query settlement tiers, Rust)
         │                                                      │
         │                                               Phase 6c (Durability API, TS)
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

The new `IoHandler` has **synchronous storage and index methods**. Index operations are built into the trait (not a separate abstraction).

**Design decision: Single-threaded only.** No `Send + Sync` bounds on the trait, no `RwLock` in implementations. This simplifies the design significantly - all Groove operations happen on a single thread (main thread or worker thread). Cross-thread communication uses message passing (sync protocol), not shared mutable state.

```rust
/// Synchronous I/O handler for storage, indices, and sync messages.
///
/// Single-threaded: no Send + Sync bounds. Each thread (main, worker)
/// has its own IoHandler instance. Cross-thread communication uses
/// the sync protocol over postMessage, not shared state.
pub trait IoHandler {
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
    //
    // NOTE: Branch is included to support multi-branch scenarios.
    // NOTE: Methods take `Value` not raw bytes - each implementation
    //       handles encoding internally (cleaner separation of concerns).

    /// Insert an index entry.
    fn index_insert(&mut self, table: &str, column: &str, branch: &str, value: &Value, row_id: ObjectId) -> Result<(), StorageError>;

    /// Remove an index entry.
    fn index_remove(&mut self, table: &str, column: &str, branch: &str, value: &Value, row_id: ObjectId) -> Result<(), StorageError>;

    /// Lookup exact value - returns all row IDs with this value.
    fn index_lookup(&self, table: &str, column: &str, branch: &str, value: &Value) -> Vec<ObjectId>;

    /// Range scan - returns row IDs matching the given bounds.
    fn index_range(&self, table: &str, column: &str, branch: &str, start: Bound<&Value>, end: Bound<&Value>) -> Vec<ObjectId>;

    /// Full scan - returns all row IDs in this index.
    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId>;

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

| Implementation  | Use Case              | Index Backing                                                               |
| --------------- | --------------------- | --------------------------------------------------------------------------- |
| `MemoryStorage` | Tests, main thread    | `HashMap<(table, col, branch), BTreeMap<encoded_value, HashSet<ObjectId>>>` |
| `BfTreeStorage` | Worker (OPFS), Native | bf-tree with composite keys                                                 |

**`MemoryStorage`** is simple and sufficient for:

- All groove unit tests
- All groove integration tests
- Main thread in browser (it's just a cache)

**Implementation note**: `MemoryStorage` uses simple `HashMap`/`BTreeMap` with `&mut self` for mutations. No `RwLock` needed since we're single-threaded. The `&self` methods (`load_*`, `index_lookup`, etc.) only need shared references.

**`BfTreeStorage`** adds persistence and is only needed for:

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

## Phase 3: Delete Our B-tree Implementation ✅

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

// After: No internal index structures - IoHandler provides index ops
pub struct QueryManager {
    // No BTreeIndex HashMap here anymore
    // Index operations receive &mut IoHandler from RuntimeCore
    // ...
}

impl QueryManager {
    // Methods that need indices take IoHandler as parameter
    pub fn process_delta(&mut self, io: &mut impl IoHandler, ...) { ... }
    pub fn index_scan(&self, io: &impl IoHandler, table: &str, ...) -> Vec<ObjectId> {
        io.index_lookup(table, column, branch, value)
    }
}
```

---

## Phase 4: Synchronous Query Graphs ✅

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

## Phase 5: Test Adaptation ✅

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

## Phase 6a: Write Persistence Acks (Rust) ✅

**Goal**: Add persistence acknowledgment messages to the sync protocol. Implement emission, routing, relay, and consumption of write acks. Verify with three-tier E2E tests using three groove instances (A ↔ B ↔ C).

**After Phase 6a**: `PersistenceAck` flows correctly through a multi-tier topology. Commits carry ack state. 558 tests pass.

### Key Design Decisions

#### Separation of concerns: ack state vs routing

Two distinct concerns, stored in different places:

1. **CommitAckState on Commit** — intrinsic property of the commit ("which tiers have confirmed persistence"). Lives on the `Commit` struct with `#[serde(skip)]` (not serialized over the wire, not hashed). **Persisted via IoHandler** — `store_ack_tier()` writes incrementally, `load_branch()` populates ack_state on each returned commit.

2. **Interest map on SyncManager** — transient routing table ("which clients need to hear about acks for this commit"). `HashMap<CommitId, HashSet<ClientId>>`. Connection-scoped, cleaned up on disconnect.

```rust
// On Commit (intrinsic):
pub struct Commit {
    // ... existing fields ...
    #[serde(skip, default)]
    pub ack_state: CommitAckState,
}

// On SyncManager (transient routing):
pub struct SyncManager {
    // ... existing fields ...
    my_tier: Option<PersistenceTier>,
    commit_interest: HashMap<CommitId, HashSet<ClientId>>,
}
```

#### Why no CommitSource on Commit

The same commit can arrive from multiple clients, or multiple times from the same client (network retries). Storing a single source on the commit doesn't work. The interest map handles the N:1 relationship naturally — multiple clients can be interested in the same commit's acks.

#### Interest map lifecycle

- **Add**: When `process_from_client` receives `ObjectUpdated`, add `client_id` to interest set for each commit ID.
- **Remove**: When a client disconnects, remove it from all interest sets. Clean up empty entries.
- **Idempotent**: Re-receiving a commit from the same client just re-adds to the HashSet (no-op). Node re-acks and re-relays any existing upstream acks.

#### Each groove instance knows its own tier

`SyncManager` gets a `my_tier: Option<PersistenceTier>` config field. When set, acks emitted by this instance carry this tier. When `None`, the instance doesn't emit acks (e.g., a memory-only client that doesn't persist).

#### Ack flow

Direct ack is always immediate — the receiver knows who sent the commit (from method params). Relay uses the interest map:

```
A (client) ──write──▸ B (server/client) ──forward──▸ C (server)
                                                       │
A ◂──relay(C's ack)── B ◂──────direct ack(tier=Core)──┘
     via interest map    │
A ◂──direct ack(tier=Worker)──┘
```

1. A sends commit to B. B records `interest[commit] = {A}`.
2. B persists → B acks A directly (tier=Worker). B forwards commit to C.
3. C persists → C acks B directly (tier=Core).
4. B receives C's ack. Looks up `interest[commit]` → {A}. Relays ack to A.
5. A now has acks from both Worker and Core tiers.

### Scope

**ADD**:

- `PersistenceTier` enum (with `Ord`: Worker < EdgeServer < CoreServer)
- `CommitAckState` struct (on Commit, `#[serde(skip)]`)
- `SyncPayload::PersistenceAck` variant
- `SyncManager.my_tier` config
- `SyncManager.commit_interest` routing table
- Interest map population in `process_from_client()`
- Direct ack emission in `apply_object_updated()` (after successful persist)
- Ack relay in PersistenceAck handler (via interest map)
- Ack state update on commits via IoHandler
- `IoHandler::store_ack_tier()` + MemoryIoHandler impl
- `load_branch()` populates ack_state from storage
- Client disconnect cleanup
- Three-tier E2E tests (write ack only)

**DON'T ADD** (later phases):

- `QuerySettled` (Phase 6b)
- `settled_tier` on subscriptions (Phase 6b)
- TypeScript API (Phase 6c)
- Worker bridge (Phase 6c)

### New Types

```rust
/// Identifies which tier is confirming persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum PersistenceTier {
    /// Local worker (OPFS persistence).
    Worker,
    /// Edge server (regional persistence).
    EdgeServer,
    /// Global core server (global persistence).
    CoreServer,
}

/// Ack state: which tiers have confirmed persistence of this commit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitAckState {
    pub confirmed_tiers: HashSet<PersistenceTier>,
}
```

```rust
pub enum SyncPayload {
    // ... existing variants ...

    /// Confirms commits have been durably persisted at this tier.
    PersistenceAck {
        object_id: ObjectId,
        branch_name: BranchName,
        confirmed_commits: HashSet<CommitId>,
        tier: PersistenceTier,
    },
}
```

### Implementation Steps

**Step 1: Types + IoHandler persistence**

Add `PersistenceTier`, `CommitAckState` to commit.rs. Add `ack_state: CommitAckState` field to `Commit` (`#[serde(skip, default)]`). Add `SyncPayload::PersistenceAck` variant. Add `store_ack_tier()` to IoHandler trait. Implement in MemoryIoHandler with `ack_tiers: HashMap<CommitId, HashSet<PersistenceTier>>`. Update `load_branch()` to populate ack_state from storage. Add `my_tier` and `commit_interest` fields to SyncManager.

**Step 2: Populate interest map**

In `process_from_client()`, when receiving `ObjectUpdated`, add `client_id` to interest set for each commit ID before calling `apply_object_updated()`.

**Step 3: Emit direct PersistenceAck**

Modify `apply_object_updated()` to accept a `source` parameter. After successfully persisting commits, if `self.my_tier` is set, emit `PersistenceAck` back to the source with the persisted commit IDs.

**Step 4: Handle incoming PersistenceAck + relay**

Add handling in `process_from_server()` and `process_from_client()`: persist ack state via `io.store_ack_tier()`, update in-memory commit ack_state, relay to interested clients via the interest map. Add `get_commit_mut()` to ObjectManager for in-memory ack state updates.

**Step 5: Client disconnect cleanup**

In `remove_client()` (or equivalent), remove the disconnected client from all interest sets. Clean up empty entries.

**Step 6: Three-tier E2E tests**

Create `pump_messages_3tier()` helper. Tests:

1. `persistence_ack_direct` — A writes to B, B acks A with B's tier
2. `persistence_ack_relay` — A writes through B to C, C acks B, B relays to A with C's tier
3. `persistence_ack_both_tiers` — A receives acks from both B and C tiers
4. `persistence_ack_idempotent` — duplicate commit delivery doesn't panic
5. `persistence_ack_cleanup_on_disconnect` — interest map cleaned on disconnect
6. `persistence_ack_survives_reload` — store_ack_tier → load_branch round-trip

### Files to Modify

| File                    | Change                                                                                                                                                    |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `commit.rs`             | Add `CommitAckState`, `ack_state` field on `Commit`                                                                                                       |
| `io_handler.rs`         | Add `store_ack_tier()` to IoHandler trait, implement in MemoryIoHandler (+ `ack_tiers` field), update `load_branch()` to populate ack_state               |
| `sync_manager.rs`       | Add `PersistenceTier`, `SyncPayload::PersistenceAck`, `my_tier` field, `commit_interest` map, interest population, ack emission/relay, disconnect cleanup |
| `object_manager.rs`     | Add `get_commit_mut()` method                                                                                                                             |
| `sync_manager_tests.rs` | Three-tier E2E tests, `pump_messages_3tier()` helper                                                                                                      |

### Verification

```bash
cargo check -p groove
cargo test -p groove
cargo clippy -p groove -- -D warnings
```

---

## Phase 6b: Query Settlement Tiers (Rust) ✅

**Goal**: Add tier-aware query settlement to the sync protocol. A subscriber can request that initial delivery be held until a specific persistence tier confirms settlement (e.g., "don't show results until EdgeServer has settled"). Implement `QuerySettled` message emission, relay, and tier-gated delivery in QueryManager. Verify with E2E tests.

**Depends on**: Phase 6a (PersistenceTier enum, SyncManager routing patterns).

**After Phase 6b**: Subscribers can specify `settled_tier` on subscriptions. `QuerySettled` flows through the sync topology. First delivery is held until the required tier settles, then delivers the full accumulated state. 558 tests pass.

### Key Design Decisions

#### settled_tier controls initial delivery timing

The `settled_tier` option on a subscription controls when the _first_ update is delivered:

- `None` (default): delivery is immediate on first local settle (current behavior preserved)
- `Some(PersistenceTier)`: delivery is held until `QuerySettled` arrives from that tier (or higher)

After the first delivery, all subsequent incremental updates are delivered immediately regardless of tier.

#### Data accumulates while waiting

While holding back delivery, the query graph continues settling and processing incoming data normally. When the required tier finally settles, the initial delivery contains ALL accumulated data — not just what arrived after the tier settled.

This is achieved by using `QueryGraph::current_result()` (already exists) to get the full current state at the moment of first delivery.

#### QuerySettled is a separate SyncPayload variant

```rust
SyncPayload::QuerySettled {
    query_id: QueryId,
    tier: PersistenceTier,
}
```

Emitted by a server when a client's forwarded query subscription settles for the first time. Relayed through intermediaries back to the originating client.

#### Query origin tracking for relay

SyncManager needs to know which client originated each forwarded query so it can relay `QuerySettled` back. New field: `query_origin: HashMap<QueryId, ClientId>`.

### Scope

**ADD**:

- `SyncPayload::QuerySettled` variant
- `settled_tier: Option<PersistenceTier>` on `QuerySubscription`
- `achieved_tiers: HashSet<PersistenceTier>` on `QuerySubscription`
- Delivery hold logic in `QueryManager::process()` — gate first delivery on tier satisfaction
- `notify_query_settled(sub_id, tier)` method on QueryManager
- `query_origin: HashMap<QueryId, ClientId>` on SyncManager
- QuerySettled emission on first server-side subscription settlement
- QuerySettled relay through intermediaries
- `settled_tier` parameter on RuntimeCore subscribe API
- E2E tests for QuerySettled flow

**DON'T ADD** (Phase 6c):

- TypeScript API
- Worker bridge integration

### Implementation Steps

**Step 1: Add QuerySettled SyncPayload variant**

```rust
SyncPayload::QuerySettled {
    query_id: QueryId,
    tier: PersistenceTier,
},
```

**Step 2: Add settled_tier to QuerySubscription**

In `query_manager/manager.rs`, add to `QuerySubscription`:

- `settled_tier: Option<PersistenceTier>` — required tier for initial delivery
- `achieved_tiers: HashSet<PersistenceTier>` — tiers that have confirmed settlement

Add `notify_query_settled(sub_id, tier)` method that adds tier to achieved_tiers and marks subscription dirty.

**Step 3: Modify delivery logic in QueryManager::process()**

```rust
let delta = subscription.graph.settle(io_ref, row_loader);

let tier_satisfied = match &subscription.settled_tier {
    None => true,
    Some(required) => subscription.achieved_tiers.iter().any(|t| t >= required),
};

if !tier_satisfied {
    // Waiting for required tier — don't deliver anything yet
} else if !subscription.settled_once {
    // First delivery — full current state from graph
    let current_rows = subscription.graph.current_result();
    let full_delta = RowDelta { added: current_rows, removed: vec![], updated: vec![] };
    subscription.settled_once = true;
    self.update_outbox.push(QueryUpdate { delta: full_delta, ... });
} else if !delta.is_empty() {
    // Incremental delivery (tier already satisfied)
    self.update_outbox.push(QueryUpdate { delta, ... });
}
```

**Step 4: Query origin tracking + relay in SyncManager**

Add `query_origin: HashMap<QueryId, ClientId>` to SyncManager. When SyncManager forwards a client's query upstream, record the mapping. When receiving QuerySettled from server, relay to originating client. When B's own ServerQuerySubscription settles for the first time, emit QuerySettled to the client.

**Step 5: Extend subscribe API**

Add `settled_tier: Option<PersistenceTier>` parameter to `RuntimeCore::subscribe()`, thread through to QuerySubscription.

**Step 6: E2E tests**

1. `query_settled_direct` — A subscribes on B with `settled_tier=Worker`, B settles, B sends QuerySettled. Assert: A gets initial update only after QuerySettled.
2. `query_settled_relay` — A subscribes with `settled_tier=EdgeServer` through B to C. B settles (Worker) → no update. C settles (EdgeServer) → QuerySettled relayed → A gets update.
3. `query_settled_no_tier_immediate` — `settled_tier=None` preserves immediate delivery (current behavior).
4. `query_settled_data_accumulates` — Data arrives while waiting for tier. Assert: initial update contains ALL accumulated data.

### Files to Modify

| File                       | Change                                                                                             |
| -------------------------- | -------------------------------------------------------------------------------------------------- |
| `sync_manager.rs`          | Add `SyncPayload::QuerySettled`, `query_origin` map, QuerySettled emission/relay                   |
| `query_manager/manager.rs` | Add `settled_tier`/`achieved_tiers` to subscription, delivery hold logic, `notify_query_settled()` |
| `runtime_core.rs`          | Add `settled_tier` parameter to subscribe API                                                      |
| `sync_manager_tests.rs`    | QuerySettled E2E tests                                                                             |

### Verification

```bash
cargo check -p groove
cargo test -p groove
cargo clippy -p groove -- -D warnings
```

---

## Phase 6c: Durability API (TypeScript) ✅

**Goal**: Expose PersistenceAck (6a) and QuerySettled (6b) to TypeScript. Two patterns: sync mutations (fire-and-forget) + async `_persisted` variants that resolve when a tier confirms; and optional `settled_tier` on queries/subscriptions.

**Depends on**: Phase 6a, Phase 6b. Does NOT depend on Phase 8 (worker) — tests use artificially connected RuntimeCore/WasmRuntime instances.

**After Phase 6c**: 571 groove tests + 3 groove-tokio tests pass. `pnpm build` succeeds (all 4 turbo tasks). 1 known-red test (`jazz-rs::test_persistence` — needs persistent storage from Phase 7).

### What was implemented

#### Rust: RuntimeCore API

```rust
// Fire-and-forget (existing, unchanged)
pub fn insert(&mut self, table, values, session) -> Result<ObjectId, RuntimeError>
pub fn update(&mut self, object_id, values, session) -> Result<(), RuntimeError>
pub fn delete(&mut self, object_id, session) -> Result<(), RuntimeError>

// Persisted variants (new) — return oneshot::Receiver that resolves on tier ack
pub fn insert_persisted(&mut self, table, values, session, tier) -> Result<(ObjectId, Receiver<()>), RuntimeError>
pub fn update_persisted(&mut self, object_id, values, session, tier) -> Result<Receiver<()>, RuntimeError>
pub fn delete_persisted(&mut self, object_id, session, tier) -> Result<Receiver<()>, RuntimeError>

// Query settlement (new parameter)
pub fn subscribe_with_settled_tier(query, callback, session, settled_tier: Option<PersistenceTier>)
pub fn query_with_settled_tier(query, session, settled_tier: Option<PersistenceTier>)

// Schema persistence (new — uses RuntimeCore's own io_handler)
pub fn persist_schema(&mut self) -> ObjectId
```

**Ack watcher mechanism**: `RuntimeCore` has `ack_watchers: HashMap<CommitId, Vec<(PersistenceTier, oneshot::Sender<()>)>>`. When a `PersistenceAck` arrives via sync, watchers for that commit are checked — a tier >= the requested tier satisfies the watcher (e.g., EdgeServer ack satisfies a Worker watcher).

#### Rust: WasmRuntime API

`WasmRuntime::new()` now takes an optional `tier: Option<String>` parameter to set the node's persistence tier at construction time.

New methods: `insertPersisted`, `updatePersisted`, `deletePersisted` (return `Promise`), `addClient`, `addClientWithFullSync`. `query` and `subscribe` accept optional `settled_tier` parameter.

#### Rust: TokioRuntime

Removed `Driver` parameter from `TokioRuntime::new()` — storage is now internal via `MemoryIoHandler`. Removed `load_indices()` (indices load lazily). Added `persist_schema()`.

#### TypeScript: JazzClient + Db

```typescript
// Fire-and-forget (existing)
client.create(table, values): string
db.insert(app.todos, data): string

// Persisted variants (new)
client.createPersisted(table, values, tier): Promise<string>
db.insertPersisted(app.todos, data, tier): Promise<string>
// Same for update/delete

// Query settlement (new optional parameter)
client.query(queryJson, settledTier?): Promise<Row[]>
client.subscribe(queryJson, callback, settledTier?): number
db.all(query, settledTier?): Promise<T[]>
db.subscribeAll(query, callback, settledTier?): () => void
```

`AppContext.tier` added — passed to `WasmRuntime` constructor. `AppContext.driver` made optional (storage is in-memory by default).

#### Cleanup performed

- **Removed `groove-rocksdb`** from workspace (will be replaced by bftree in Phase 7)
- **Removed `SqliteNodeDriver`** and **`IndexedDBDriver`** from jazz-ts (dead code from old async storage pattern)
- **Removed stale driver-dependent test files** (`client.test.ts`, `db.test.ts`, `codegen/e2e.test.ts`)
- **Removed stale storage types** from `drivers/types.ts` (`StorageRequest`, `StorageResponse`, `Commit`, `LoadedBranch`, `BlobAssociation`)
- **Fixed stale imports** in `jazz-rs`, `jazz-cli`, `groove-tokio`
- **Removed `load_indices()`** and `reset_indices_for_cold_start()` — indices now load lazily via IoHandler

#### Bug fix discovered during testing

`update_with_session` and `delete_with_session` in `query_manager/manager.rs` were missing `forward_update_to_servers()` calls — updates and deletes never synced to servers. Fixed by adding the calls after `add_commit()`. Covered by `rc_update_sync` and `rc_delete_sync` tests.

### E2E Tests (Rust, RuntimeCore level)

10 tests using a 3-tier harness (A=client, B=Worker, C=EdgeServer):

1. `rc_insert_returns_immediately` — sync insert returns ObjectId, data queryable locally
2. `rc_insert_data_syncs_to_server` — data syncs via pump to server B
3. `rc_update_sync` — update syncs to server (covers the bug fix)
4. `rc_delete_sync` — delete syncs to server (covers the bug fix)
5. `rc_insert_persisted_resolves_on_worker_ack` — receiver resolves when Worker acks
6. `rc_insert_persisted_holds_until_correct_tier` — Worker ack doesn't satisfy EdgeServer request; EdgeServer ack does
7. `rc_insert_persisted_higher_tier_satisfies_lower` — EdgeServer ack satisfies Worker request
8. `rc_update_persisted_resolves_on_ack` — update_persisted receiver resolves
9. `rc_delete_persisted_resolves_on_ack` — delete_persisted receiver resolves
10. `rc_multiple_persisted_inserts_independent` — two inserts get independent receivers
11. `rc_query_no_settled_tier_immediate` — query with `settled_tier=None` resolves immediately
12. `rc_query_settled_tier_holds` — query with `settled_tier=Worker` holds until QuerySettled arrives
13. `rc_subscribe_settled_tier` — subscribe with `settled_tier=Worker` holds callback until QuerySettled; first delivery contains accumulated data

### Known gaps (deferred)

- **TypeScript durability tests**: Discussed loopback transport design (wire WasmRuntime instances via `onSyncMessageToSend`/`onSyncMessageReceived`) but not yet implemented. Requires WASM module loading in test environment.

### Deferred to Phase 8 (worker architecture)

The original Phase 6c spec described `MutationResult.persisted()` chaining. This was dropped — the `_persisted` method variants (`insertPersisted`, `createPersisted`, etc.) that return Promises are the API. No chaining needed.

---

## Phase 7: bf-tree Persistence

**Goal**: Persistent `IoHandler` backed by [bf-tree-web](https://github.com/garden-co/bf-tree-web) (garden-co fork of Microsoft Research's bf-tree). Split into three sub-phases:

- **7a**: Subtree import + `BfTreeStorage` + native integration (jazz-rs, jazz-cli) ✅
- **7b**: `groove-napi` crate — NAPI addon for server-side TypeScript persistence ✅
- **7c-i**: bf-tree WASM persistence (snapshot + WAL recovery via VFS) ✅
- **7c-ii**: groove-wasm integration (BfTreeStorage in WasmRuntime, `open_persistent()`)

> All prior phases (1-6) work with `MemoryStorage`. bf-tree is only needed for actual persistence.

### Key Decisions

| Decision                 | Choice                            | Rationale                                                                                                                                   |
| ------------------------ | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| **Monorepo integration** | git subtree into `crates/bf-tree` | Self-contained, no auth issues for CI (private repo). Treated as a workspace member.                                                        |
| **Storage model**        | BfTree as primary store           | All reads/writes go through bf-tree directly. No separate in-memory cache layer. bf-tree has its own in-memory cache. Simpler architecture. |
| **Storage mutability**   | Keep `&mut self`                  | BfTree takes `&self` (handles concurrency internally), but `&mut self` on Storage is a harmless superset. Avoids touching the trait.        |
| **Native constructor**   | External construction             | jazz-rs creates `BfTreeStorage`, passes it to `TokioRuntime::new()`. TokioRuntime stays generic over `S: Storage`.                          |
| **Server-side TS**       | NAPI addon (Phase 7b)             | `groove-napi` wraps `RuntimeCore<BfTreeStorage>` via napi-rs. BfTree uses StdVfs natively.                                                  |
| **OPFS tests**           | wasm-pack test + headless browser | Reuse bf-tree-web's proven test pattern from `tests/wasm/`.                                                                                 |

### Why bf-tree-web?

1. **Synchronous OPFS**: Working `OpfsVfs` with `FileSystemSyncAccessHandle`
2. **Cross-platform VFS**: `MemoryVfs`, `StdVfs`, `OpfsVfs` all implement `VfsImpl` (pub(crate))
3. **Production-tested**: Based on MSR's bf-tree ([paper](https://badrish.net/papers/bftree-vldb2024.pdf))
4. **WAL support**: Write-ahead logging for crash recovery
5. **Range queries**: `scan_with_end_key()` — exactly what `IoHandler::index_range()` needs

### bf-tree API Surface

```rust
// Constructors
BfTree::new(path, cache_size_byte) -> Result<Self, ConfigError>  // StdVfs (native)
BfTree::new(":memory:", cache_size_byte)                          // MemoryVfs
BfTree::with_opfs_vfs(opfs_vfs, config)                          // OpfsVfs (WASM, single file)
BfTree::open_with_opfs(tree_vfs, wal_vfs, config)                // OpfsVfs (WASM, snapshot + WAL recovery)

// All methods take &self (not &mut self) — internal concurrency control
tree.insert(key: &[u8], value: &[u8]) -> LeafInsertResult
tree.read(key: &[u8], out_buffer: &mut [u8]) -> LeafReadResult
tree.delete(key: &[u8])
tree.scan_with_end_key(start: &[u8], end: &[u8], ...) -> ScanIter
tree.scan_with_count(start: &[u8], count: usize, ...) -> ScanIter
```

---

## Phase 7a: BfTreeStorage + Native Integration

**Goal**: Import bf-tree-web, implement `BfTreeStorage`, make `jazz-rs::test_persistence` pass.

**Depends on**: Phase 6c (Storage trait is stable).

**After 7a**: Native Rust applications (jazz-rs, jazz-cli) have real disk persistence. The currently-red `test_persistence` test goes green.

### Key Design: Lazy Loading via ObjectManager::get_or_load()

On cold start after reopening persistent storage, index scans find ObjectIds in Storage but the in-memory ObjectManager is empty. The solution is `ObjectManager::get_or_load()` — a lazy loader that checks in-memory first, then falls back to `storage.load_object_metadata()` + `storage.load_branch()` to populate the object on demand.

```rust
impl ObjectManager {
    /// Get an object, loading from storage if not in memory (lazy cold-start load).
    pub fn get_or_load(
        &mut self, id: ObjectId, storage: &dyn Storage, branches: &[String],
    ) -> Option<&Object> {
        if self.objects.contains_key(&id) {
            return self.objects.get(&id);
        }
        // Load metadata + branches from Storage, insert into self.objects
        // ...
    }
}
```

**Borrow design**: `QueryGraph::settle()` takes only `(storage, row_loader)` — no `&ObjectManager`. The `row_loader` closure captures `&mut ObjectManager` and calls `get_or_load()` internally. This avoids double-mut-borrow: the closure owns the `&mut om` reference, and `settle()` never needs direct ObjectManager access.

```rust
// In QueryManager::process():
let om = &mut self.sync_manager.object_manager;
let branches = vec![current_branch.clone()];
let mut row_loader = |id: ObjectId| -> Option<(Vec<u8>, CommitId)> {
    let obj = om.get_or_load(id, storage_ref, &branches)?;
    // extract content + commit_id from obj...
};
let delta = graph.settle(storage_ref, &mut row_loader);
```

PolicyFilterNode, ArraySubqueryNode, and PolicyGraph also take only `(storage, row_loader)` — no ObjectManager parameter anywhere in the settle chain.

### Step 1: Import bf-tree-web as subtree

```bash
git subtree add --prefix=crates/bf-tree \
  git@github.com:garden-co/bf-tree-web.git main --squash
```

Add to workspace `Cargo.toml`:

```toml
members = [
  "crates/bf-tree",
  # ... existing members
]
```

Verify: `cargo check -p bf-tree`

### Step 2: Key encoding module

New file: `crates/groove/src/io_handler/encoding.rs`

All data lives in a single bf-tree with composite keys designed for range scans:

```
Object metadata:    "obj:{object_id}:meta"         → JSON metadata
Branch tips:        "obj:{object_id}:br:{branch}:tips"  → serialized HashSet<CommitId>
Commits:            "obj:{object_id}:br:{branch}:c:{commit_id}" → serialized Commit
Blobs:              "blob:{content_hash}"           → raw blob data
Ack tiers:          "ack:{commit_id}"               → serialized PersistenceTier
Index entries:      "idx:{table}:{column}:{branch}:{encoded_value}:{object_id}" → empty
```

Value encoding for sortable index keys:

```rust
fn encode_index_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0x00],
        Value::Int(n) => {
            let mut bytes = vec![0x01];
            bytes.extend((*n as i64 ^ i64::MIN).to_be_bytes()); // sign-flip for sort order
            bytes
        }
        Value::Text(s) => {
            let mut bytes = vec![0x02];
            bytes.extend(s.as_bytes());
            bytes.push(0x00); // null terminator
            bytes
        }
        Value::Uuid(id) => {
            let mut bytes = vec![0x03];
            bytes.extend(id.0.as_bytes());
            bytes
        }
        Value::Boolean(b) => vec![0x04, if *b { 1 } else { 0 }],
        // ... other types
    }
}
```

### Step 3: BfTreeStorage

New file: `crates/groove/src/io_handler/bftree.rs` (behind a `bftree` feature flag, since bf-tree has heavy dependencies).

```rust
pub struct BfTreeStorage {
    tree: BfTree,
    outbox: Vec<OutboxEntry>,
}

impl BfTreeStorage {
    /// Open or create a persistent store at the given path.
    pub fn open(path: impl AsRef<Path>, cache_size_bytes: usize) -> Result<Self, ...> {
        let tree = BfTree::new(path, cache_size_bytes)?;
        Ok(Self { tree, outbox: Vec::new() })
    }

    /// Create an in-memory store (for tests).
    pub fn memory(cache_size_bytes: usize) -> Result<Self, ...> {
        let tree = BfTree::new(":memory:", cache_size_bytes)?;
        Ok(Self { tree, outbox: Vec::new() })
    }
}
```

All Storage methods go directly to bf-tree. No MemoryStorage cache layer.

Key implementation notes:

- `create_object` → `tree.insert("obj:{id}:meta", &serde_json::to_vec(&metadata))`
- `load_object_metadata` → `tree.read("obj:{id}:meta", &mut buf)` → deserialize
- `append_commit` → insert commit + read-modify-write tips
- `load_branch` → scan `"obj:{id}:br:{branch}:c:"` prefix for commits + read tips
- `index_insert` → `tree.insert("idx:...:encoded_value:row_id", &[])`
- `index_range` → `tree.scan_with_end_key(start, end)` → parse ObjectIds from keys
- `index_scan_all` → `tree.scan_with_end_key("idx:{table}:{col}:{branch}:", "idx:{table}:{col}:{branch}:~")`
- `send_sync_message` → push to outbox (same as MemoryStorage)
- `schedule_batched_tick` → no-op (same as MemoryStorage)

### Step 4: Integration with jazz-rs

Update `crates/jazz-rs/src/client.rs`:

- Create `BfTreeStorage::open(data_dir, cache_size)`
- Pass to `TokioRuntime::new(schema_manager, storage, sync_callback)`
- (TokioRuntime is generic over `S: Storage + Send`)

**Completed**: `RuntimeCore` is generic over `<S: Storage, Sched: Scheduler, Sync: SyncSender>`. `TokioRuntime` wraps `RuntimeCore<S, TokioScheduler, CallbackSyncSender>` for any `S: Storage + Send`.

### Step 5: Tests

1. **`jazz-rs::test_persistence`** (currently red) — goes green. Write data, shutdown, reopen from same path, query returns persisted data.
2. **`bftree_iohandler_crud`** — unit test in `io_handler/bftree.rs`. Insert object + commit, load it back, verify round-trip.
3. **`bftree_iohandler_index_ops`** — insert index entries, verify `index_lookup`, `index_range`, `index_scan_all` return correct results.
4. **`bftree_iohandler_persistence`** — open store, insert data, drop, reopen from same path, verify data survives.
5. **`bftree_iohandler_with_runtime_core`** — create `RuntimeCore<BfTreeStorage>`, insert/query/update/delete, verify end-to-end.

### Files to Modify/Create

| File                                     | Change                                                    |
| ---------------------------------------- | --------------------------------------------------------- |
| `Cargo.toml` (workspace)                 | Add `crates/bf-tree` to members                           |
| `crates/groove/Cargo.toml`               | Add `bf-tree` optional dependency behind `bftree` feature |
| `crates/groove/src/storage/encoding.rs`  | New: key encoding functions                               |
| `crates/groove/src/storage/bftree.rs`    | New: `BfTreeStorage`                                      |
| `crates/groove/src/storage/mod.rs`       | Re-export `BfTreeStorage` (behind feature)                |
| `crates/groove-tokio/src/lib.rs`         | `TokioRuntime` generic over `S: Storage + Send`           |
| `crates/jazz-rs/Cargo.toml`              | Add `groove/bftree` feature dependency                    |
| `crates/jazz-rs/src/client.rs`           | Use `BfTreeStorage::open()`                               |
| `crates/jazz-cli/Cargo.toml`             | Add `groove/bftree` feature dependency                    |
| `crates/jazz-cli/src/commands/server.rs` | Use `BfTreeStorage::open()`                               |

### Verification

```bash
cargo check -p bf-tree
cargo test -p groove --features bftree
cargo test -p jazz-rs  # test_persistence goes green
cargo test -p todo-server-rs
```

### Expected test status after 7a

| Test                                     | Status       | Notes                                           |
| ---------------------------------------- | ------------ | ----------------------------------------------- |
| `jazz-rs::test_crud_operations`          | GREEN        | Already passes (in-memory)                      |
| `jazz-rs::test_persistence`              | **GREEN** ✅ | Fixed by `get_or_load()` lazy loading           |
| `todo-server-rs::test_health_check`      | GREEN        | Already passes                                  |
| `todo-server-rs::test_crud_operations`   | GREEN        | Already passes                                  |
| `todo-server-rs::test_local_persistence` | **GREEN** ✅ | Fixed by `get_or_load()` lazy loading           |
| `todo-server-rs::test_server_resync`     | RED          | Pre-existing failure (unrelated to persistence) |
| `todo-server-rs::test_todos_live_sse`    | GREEN        | Already passes                                  |
| `groove::bftree_iohandler_*` (new)       | GREEN        | New unit + round-trip tests                     |

---

## Phase 7b: groove-napi (Server-side TypeScript) ✅

**Goal**: NAPI addon wrapping `RuntimeCore<BfTreeStorage>` for Node.js. Replaces groove-wasm for server-side TS.

**Depends on**: Phase 7a (BfTreeStorage exists).

**After 7b**: `jazz-ts` server applications (like `todo-server-ts`) use real disk persistence via a native Node.js addon instead of WASM + in-memory storage.

### Architecture

```
crates/groove-napi/
  ├── Cargo.toml                ← napi-rs + groove(bftree) + serde_json
  ├── build.rs                  ← napi-build script
  ├── src/lib.rs                ← #[napi] exports: NapiRuntime class + utilities
  ├── package.json              ← npm package "jazz-napi"
  ├── index.js                  ← CJS loader for .node binary
  └── index.d.ts                ← TypeScript types (auto-generated by napi-rs)

packages/jazz-ts/
  └── src/runtime/client.ts     ← Runtime interface, connectWithRuntime() factory
```

### Key Design

- `groove-napi` wraps `RuntimeCore<BfTreeStorage, NapiScheduler, NapiSyncSender>` using [napi-rs](https://napi.rs)
- `NapiScheduler`: `ThreadsafeFunction` + `Arc<AtomicBool>` debounce schedules `batched_tick()` on Node.js event loop
- `NapiSyncSender`: `ThreadsafeFunction<String>` bridges outbox to JS callback
- BfTree uses `StdVfs` natively (no WASM, no OPFS)
- Exposes same API surface as `WasmRuntime`: `insert`, `update`, `delete`, `query`, `subscribe`, persisted variants, sync bridge, `flush`
- `jazz-ts` provides `Runtime` interface + `JazzClient.connectWithRuntime()` — consumer chooses NAPI or WASM explicitly
- Subscribe callbacks pass native JS objects (not JSON strings) via `serde_json::Value` through TSFN
- Query returns `Promise<any>` via `napi::Deferred` + spawned thread blocking on `QueryFuture`

### Steps (all completed)

1. Created `crates/groove-napi` with napi-rs boilerplate (Cargo.toml, build.rs, package.json)
2. Wrapped `RuntimeCore<BfTreeStorage>` with `#[napi]` class (`NapiRuntime`)
3. Exposed: `insert`, `update`, `delete`, `query`, `subscribe`, `unsubscribe`, `insertPersisted`, `updatePersisted`, `deletePersisted`, `onSyncMessageReceived`, `onSyncMessageToSend`, `addServer`, `addClient`, `addClientWithFullSync`, `getSchema`, `flush`
4. Module-level: `generateId()`, `currentTimestamp()`, `parseSchema()`
5. Build system: `npx napi build --release` produces `.node` binary + `index.d.ts`
6. Added `Runtime` interface to `jazz-ts` + `JazzClient.connectWithRuntime()` factory
7. Updated `todo-server-ts` to use `NapiRuntime` + `connectWithRuntime()`
8. Added persistence/cold-start test (shutdown + reopen, data survives)
9. Added `pnpm-workspace.yaml` entry for `crates/groove-napi`

### Files Created

| File                              | Purpose                                              |
| --------------------------------- | ---------------------------------------------------- |
| `crates/groove-napi/Cargo.toml`   | napi-rs + groove(bftree) + serde_json deps           |
| `crates/groove-napi/build.rs`     | napi-build script                                    |
| `crates/groove-napi/src/lib.rs`   | NapiRuntime class + all #[napi] exports (~950 lines) |
| `crates/groove-napi/package.json` | npm package "jazz-napi"                              |
| `crates/groove-napi/index.js`     | CJS loader for .node binary                          |

### Files Modified

| File                                                | Change                                                                    |
| --------------------------------------------------- | ------------------------------------------------------------------------- |
| `Cargo.toml` (workspace)                            | Added `crates/groove-napi` to members                                     |
| `pnpm-workspace.yaml`                               | Added `crates/groove-napi` workspace entry                                |
| `packages/jazz-ts/src/runtime/client.ts`            | Added `Runtime` interface, `connectWithRuntime()`, changed `runtime` type |
| `packages/jazz-ts/src/runtime/index.ts`             | Exported `Runtime` type                                                   |
| `examples/todo-server-ts/package.json`              | Added `jazz-napi` dependency                                              |
| `examples/todo-server-ts/src/main.ts`               | Uses `NapiRuntime` + `connectWithRuntime()`, optional `dataPath` param    |
| `examples/todo-server-ts/tests/integration.test.ts` | Added persistence/cold-start test                                         |

### Verification

```bash
cargo build -p groove-napi                          # Rust build
cd crates/groove-napi && npx napi build --release   # native .node + index.d.ts
cargo clippy -p groove-napi --no-deps -- -D warnings # clean
cargo test -p groove --features bftree              # 577 pass
pnpm test --filter jazz-ts                          # 129 pass
pnpm test --filter todo-server-ts                   # 10 pass
```

### Test status after 7b

| Test                                        | Status | Notes                                    |
| ------------------------------------------- | ------ | ---------------------------------------- |
| `todo-server-ts::Health Check`              | GREEN  | Via NapiRuntime                          |
| `todo-server-ts::CRUD Operations` (5 tests) | GREEN  | insert/query/update/delete via NAPI      |
| `todo-server-ts::Error Handling` (2 tests)  | GREEN  |                                          |
| `todo-server-ts::Persistence / Cold Start`  | GREEN  | Shutdown + reopen, 2 todos survive       |
| `todo-server-ts::SSE Live Endpoint`         | GREEN  | Subscriptions via NAPI                   |
| `jazz-ts::*` unit tests (129)               | GREEN  | Runtime interface is backward-compatible |
| `groove` tests (577)                        | GREEN  | Unchanged                                |

---

## Phase 7c: OPFS Persistence (WASM Worker)

**Goal**: Make `BfTreeStorage` work with OPFS in a Web Worker with full persistence (snapshot + WAL recovery). Verify with wasm-pack tests in headless browser.

**Depends on**: Phase 7a (BfTreeStorage and key encoding exist).

**After 7c**: groove-wasm can persist data via OPFS when running in a Dedicated Web Worker. Data survives Worker termination via snapshot + WAL recovery. This is the storage layer for Phase 8's worker architecture.

### Key Design Decisions

| Decision                | Choice                                | Rationale                                                                                                                 |
| ----------------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **groove-wasm storage** | BfTreeStorage only (no MemoryStorage) | `BfTreeStorage::memory()` for main thread, `BfTreeStorage` with OPFS for worker. Same RuntimeCore type, no enum dispatch. |
| **Persistence model**   | Snapshot + WAL                        | Snapshot for clean shutdown/reopen. WAL for crash recovery (no data loss between snapshots).                              |
| **OPFS files**          | Two per database                      | `{db_name}.bftree` for tree data, `{db_name}.wal` for WAL. Separate `OpfsVfs` instances (both async to open, sync after). |
| **WasmRuntime API**     | `new()` + `open_persistent()`         | `new()` uses `BfTreeStorage::memory()` (main thread). `open_persistent()` async, opens OPFS + enables WAL (worker).       |

### Architecture

```
WASM Worker context:

  OpfsVfs::open("mydb.bftree").await  →  tree_vfs
  OpfsVfs::open("mydb.wal").await     →  wal_vfs
                    │                           │
                    ▼                           ▼
         BfTree (data pages,          WriteAheadLog (append-only
          inner nodes, metadata)       log entries, crash recovery)
                    │                           │
                    └───────────┬───────────────┘
                                │
                     BfTree::open_with_opfs()
                     - fresh: create empty tree + start WAL
                     - recovery: load snapshot + replay WAL

groove-wasm:
  WasmRuntime
    └── RuntimeCore<BfTreeStorage, WasmScheduler, JsSyncSender>
          └── BfTreeStorage
                └── BfTree (with OPFS or in-memory)
```

### Technical Discoveries (from planning)

#### bf-tree snapshot recovery gap

`snapshot()` writes metadata through VFS — works on OPFS. But `new_from_snapshot()` (snapshot.rs:118) reads metadata via `std::fs::File::open()` + `read_at()` — bypasses VFS entirely. Won't compile/run on WASM.

**Fix**: Add `new_from_snapshot_with_vfs()` that reads metadata from VFS (`vfs.read(0, &mut metadata)`) instead of `std::fs::File`. ~100 lines, structurally identical to existing function.

#### WAL panics on OPFS

`WriteAheadLog::new()` calls `make_vfs()` which **panics** for `StorageBackend::Opfs` (storage.rs:408): _"OPFS backend requires async initialization."_

**Fix**: Add `WriteAheadLog::new_with_vfs()` that accepts a pre-initialized `Arc<dyn VfsImpl>`, bypassing `make_vfs()`. Uses existing `make_vfs_from_opfs()` helper (storage.rs:415).

#### WAL reader gated out on WASM

`WalReader` is behind `#[cfg(not(target_arch = "wasm32"))]` — uses `std::fs::File` for reads. Recovery function (`recovery()` in snapshot.rs) depends on it.

**Fix**: Add `VfsWalReader` that reads segments via VFS (`vfs.read(offset, &mut buf)`) instead of `std::fs::File`.

#### SplitOp is dead code

`SplitOp` struct is empty, `LogEntry::Split` has `todo!()` in serialization and recovery. But SplitOp is **never written** to WAL — only `WriteOp` is logged during insert/delete. The `todo!()` is dead code.

**Fix**: Replace `todo!()` with skip/no-op in recovery.

#### `with_opfs_vfs()` doesn't accept Config

`BfTree::with_opfs_vfs(opfs_vfs, cache_size_byte)` creates `Config::default()` which doesn't set `BfTreeStorage`'s required constants (max_key_len=256, max_record_size=16000, leaf_page_size=32768, min_record_size=8).

**Fix**: Change signature to accept full `Config` parameter.

### Sub-phases

#### Phase 7c-i: bf-tree WASM Persistence ✅

All changes in `crates/bf-tree/`. Tested with wasm-pack at the bf-tree level.

**After 7c-i**: bf-tree's snapshot and WAL recovery work on WASM/OPFS. `open_with_opfs()` handles both fresh start and crash recovery. 11 WASM tests pass (headless Chrome) + 67 native tests + 9 doc-tests.

**Steps (all complete):**

1. ✅ Modify `BfTree::with_opfs_vfs()` to accept `Config` (not just `cache_size_byte`)
2. ✅ Add `new_from_snapshot_with_vfs()` — VFS-aware snapshot recovery
3. ✅ Add `WriteAheadLog::new_with_vfs()` — WAL with pre-initialized VFS
4. ✅ Add `VfsWalReader` — VFS-based WAL reader for recovery
5. ✅ Replace SplitOp `todo!()` with skip in recovery
6. ✅ Add `BfTree::open_with_opfs(tree_vfs, wal_vfs, config)` — integrated fresh/recovery constructor
7. ✅ Add `OpfsVfs::file_size()` method
8. ✅ Add `open_tree_with_opfs_persistent()` async WASM entry point
9. ✅ Tests: snapshot round-trip, WAL recovery, snapshot+WAL, fresh start, delete recovery, scan after recovery (6 tests, 12 total WASM tests pass)

**Technical discoveries during implementation:**

- **WASM32 `usize` is 4 bytes**: `LogHeader::from_slice` used `buffer[0..8]` for `usize` — fails on WASM32. Fixed to use `std::mem::size_of::<usize>()`. `lsn` and `page_offset` offsets remain `[8..16]` and `[16..24]` due to `#[repr(C)]` alignment padding.
- **BfTree writes `WriteOp` directly to WAL, not `LogEntry`**: `wal.append_and_wait(&write_op, ...)` writes raw `WriteOp` format (`[key_len:u16][val_len:u16][op_type:u8][key][value]`), not `LogEntry` which adds a tag byte. Recovery must use `WriteOp::read_from_buffer()`, not `LogEntry::read_from_buffer()`.
- **WAL segment size**: `WalConfig::new_for_wasm()` sets 64KB segments (vs 1GB native default) to avoid browser memory issues.
- **SplitOp**: Never written to WAL. Only `WriteOp` is logged during insert/delete. SplitOp `todo!()` replaced with no-op.

**Files:**

| File                                   | Change                                                                       |
| -------------------------------------- | ---------------------------------------------------------------------------- |
| `crates/bf-tree/src/tree.rs`           | Modify `with_opfs_vfs()` to accept Config; add `open_with_opfs()`            |
| `crates/bf-tree/src/snapshot.rs`       | Add `new_from_snapshot_with_vfs()`                                           |
| `crates/bf-tree/src/wal/mod.rs`        | Add `new_with_vfs()`, `VfsWalReader`, fix `LogHeader::from_slice` for WASM32 |
| `crates/bf-tree/src/wal/operations.rs` | Replace SplitOp `todo!()` with skip                                          |
| `crates/bf-tree/src/fs/opfs_vfs.rs`    | Add `file_size()` method                                                     |
| `crates/bf-tree/src/lib.rs`            | Add `open_tree_with_opfs_persistent()`, `flush_wal()`, `snapshot()`          |
| `crates/bf-tree/src/config.rs`         | Add `WalConfig::new_for_wasm()` (64KB segments)                              |
| `crates/bf-tree/tests/wasm/src/lib.rs` | 5 new OPFS persistence tests + cleanup helper                                |
| `crates/bf-tree/tests/wasm/Cargo.toml` | Add `[workspace]` for standalone compilation                                 |

#### Phase 7c-ii: groove-wasm Integration

Changes in `crates/groove/` and `crates/groove-wasm/`. Builds on 7c-i.

**Steps:**

1. Add `flush()` to `Storage` trait (default no-op; BfTreeStorage overrides with `snapshot()`)
2. Add `flush_storage()` to `RuntimeCore`
3. Add `BfTreeStorage::with_opfs(opfs_vfs, wal_vfs, cache_size_bytes)` constructor (behind `#[cfg(target_arch = "wasm32")]`)
4. Update `groove-wasm/Cargo.toml`: `groove` with `bftree` feature + direct `bf-tree` dependency
5. Switch `WasmRuntime` type alias: `RuntimeCore<BfTreeStorage, WasmScheduler, JsSyncSender>`
6. Update `WasmRuntime::new()`: use `BfTreeStorage::memory(DEFAULT_CACHE_SIZE)`
7. Add `WasmRuntime::open_persistent()` async constructor (OPFS + WAL)
8. Expose `flush()` on `WasmRuntime`
9. Tests: 4 wasm-pack tests through `WasmRuntime::open_persistent()`

**Files:**

| File                                  | Change                                                 |
| ------------------------------------- | ------------------------------------------------------ |
| `crates/groove/src/storage/mod.rs`    | Add `fn flush(&self) {}` to Storage trait              |
| `crates/groove/src/storage/bftree.rs` | Add `with_opfs()` constructor, impl `Storage::flush()` |
| `crates/groove/src/runtime_core.rs`   | Add `flush_storage()` method                           |
| `crates/groove-wasm/Cargo.toml`       | Add bf-tree dep, groove bftree feature                 |
| `crates/groove-wasm/src/runtime.rs`   | BfTreeStorage type, `open_persistent()`, `flush()`     |
| `crates/groove-wasm/tests/opfs.rs`    | New: 4 wasm-pack browser tests via WasmRuntime         |

### Test Plan

**bf-tree level (Phase 7c-i):**

1. **`test_opfs_snapshot_round_trip`** — Insert 100 keys, `snapshot()`, drop, reopen same db_name → all 100 readable.
2. **`test_opfs_wal_recovery`** — Insert 50 keys (no snapshot), drop → WAL replays all 50.
3. **`test_opfs_snapshot_plus_wal`** — Insert 50, snapshot, insert 50 more, drop → snapshot restores first 50, WAL replays next 50.
4. **`test_opfs_scan_after_recovery`** — Insert indexed keys, snapshot, reopen → `scan_with_end_key()` returns correct results.
5. **`test_opfs_fresh_start`** — New db_name, insert/read works (proves fresh detection).

**groove-wasm level (Phase 7c-ii):**

All through `WasmRuntime::open_persistent()`:

1. **`opfs_crud_round_trip`** — Insert row, query back, verify.
2. **`opfs_persistence_across_reopen`** — Insert, flush, drop, reopen same db_name → data survives.
3. **`opfs_index_operations`** — Insert rows with different values, query with filters (equality, range).
4. **`opfs_runtime_core_e2e`** — Full lifecycle: insert, query, update, delete.

**Deferred:** wasm-pack test for large value chunking (>16KB) over OPFS. Chunking works natively (covered by `bftree_iohandler_*` tests) but should be verified in OPFS context later.

### Verification

```bash
# bf-tree WASM tests (Phase 7c-i)
RUSTFLAGS='--cfg=web_sys_unstable_apis --cfg getrandom_backend="wasm_js"' \
  wasm-pack test --headless --chrome crates/bf-tree

# groove-wasm WASM tests (Phase 7c-ii)
RUSTFLAGS='--cfg=web_sys_unstable_apis --cfg getrandom_backend="wasm_js"' \
  wasm-pack test --headless --chrome crates/groove-wasm

# Native tests still pass
cargo test -p bf-tree
cargo test -p groove --features bftree
cargo check --workspace
```

### Risks (resolved after 7c-i)

1. ~~**bf-tree snapshot recovery with OPFS is uncharted**~~ — **Resolved.** `new_from_snapshot_with_vfs()` works correctly. VFS reads replace `std::fs::File` reads. Pointer reconstruction logic is identical.
2. ~~**WAL segment size**~~ — **Resolved.** `WalConfig::new_for_wasm()` uses 64KB segments.
3. ~~**Two OPFS file handles**~~ — **Resolved.** Two handles to different OPFS files work in Chrome headless (verified by all WAL tests).
4. **WASM binary size** — Bundling bf-tree increases groove-wasm binary. Acceptable for now. (Still applies to 7c-ii.)
5. ~~**Pointer-based serialization**~~ — **Resolved.** Snapshot round-trip tests confirm reconstruction works identically in VFS-aware version.

### What does NOT work after 7c (needs Phase 8)

- `examples/todo-ts-client` — browser app with OPFS persistence, leader election, tab sync
- `jazz-ts` main-thread ↔ worker bridge (postMessage protocol)
- Multi-tab coordination (BroadcastChannel leader election)
- Production browser deployment

These require the **Phase 8 worker architecture** which builds the JS bridge on top of 7c's proven OPFS layer.

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
  | { type: "sync"; payload: SyncPayload }
  | { type: "query-register"; queryId: number; queryJson: string }
  | { type: "query-unregister"; queryId: number }
  | { type: "connect-upstream"; url: string };

// Worker → Main thread
type WorkerToMainMessage =
  | { type: "sync"; payload: SyncPayload }
  | { type: "persistence-ack"; payload: PersistenceAck }
  | { type: "query-settled"; queryId: number; tier: PersistenceTier }
  | { type: "ready" }; // Worker initialized
```

### Initialization Flow

```typescript
// jazz-ts/src/runtime/client.ts

export async function createDb<S extends Schema>(options: DbOptions<S>): Promise<Db<S>> {
  // 1. Spawn worker
  const worker = new Worker(new URL("./worker/groove-worker.ts", import.meta.url));

  // 2. Wait for worker ready
  await waitForMessage(worker, "ready");

  // 3. Create main-thread groove with MemoryStorage
  const mainGroove = new Groove(new MemoryStorage());

  // 4. Connect main groove to worker as "upstream server"
  const bridge = new WorkerBridge(worker, mainGroove);

  // 5. Register initial queries, wait for settlement
  await bridge.registerQuery(initialQuery, { settlement: "worker" });

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
const LEADER_KEY = "jazz-leader";
const LEADER_HEARTBEAT_MS = 1000;
const LEADER_TIMEOUT_MS = 3000;

async function electLeader(): Promise<boolean> {
  const channel = new BroadcastChannel("jazz-leader-election");

  // Try to claim leadership
  const myId = crypto.randomUUID();
  const claim = { type: "claim", id: myId, timestamp: Date.now() };

  // Listen for competing claims
  let isLeader = true;
  channel.onmessage = (e) => {
    if (e.data.type === "claim" && e.data.timestamp < claim.timestamp) {
      isLeader = false; // Older claim wins
    }
    if (e.data.type === "heartbeat" && e.data.id !== myId) {
      isLeader = false; // Someone else is leader
    }
  };

  channel.postMessage(claim);
  await sleep(100); // Wait for competing claims

  if (isLeader) {
    // Start heartbeat
    setInterval(() => {
      channel.postMessage({ type: "heartbeat", id: myId });
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
- Users who need guarantees use `_persisted` variants
- Simplest possible failover - no WAL replay complexity

### Expected test status after Phase 8

| Test                                       | Status    | Notes                                                         |
| ------------------------------------------ | --------- | ------------------------------------------------------------- |
| `examples/todo-ts-client`                  | **GREEN** | Full browser app: OPFS persistence, leader election, tab sync |
| `jazz-ts::worker_bridge_*` (new)           | **GREEN** | Main thread ↔ worker postMessage protocol                     |
| `jazz-ts::persistence_across_reload` (new) | **GREEN** | Write data, "reload" (re-init), data survives via OPFS        |
| `jazz-ts::multi_tab_sync` (new)            | **GREEN** | Leader broadcasts to follower, both see same data             |
| `jazz-ts::leader_failover` (new)           | **GREEN** | Leader tab closes, follower takes over, OPFS still works      |
| `jazz-ts::persisted_variants` (new)        | **GREEN** | `createPersisted()` resolves on worker ack                    |

Only after Phase 8 does browser persistence work in a production-like setup (main thread ↔ worker bridge, OPFS, leader election). Phases 7a-7c prove each layer independently.

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
- [ ] `PersistenceAck` sync payload variant + `CommitAckState` (Phase 6a)
- [ ] `QuerySettled` sync payload variant + `settled_tier` on subscriptions (Phase 6b)
- [ ] `PersistenceTier` enum (Phase 6a)
- [ ] `BfTreeStorage` implementation (Phase 7, for persistence)

**groove-wasm crate:**

- [ ] Worker entry point (`groove-worker.ts` or Rust-based)
- [ ] OPFS initialization (async open, sync operations after)
- [ ] Worker ↔ main thread postMessage protocol
- [ ] Leader election implementation

**jazz-ts:**

- [ ] `WorkerBridge` class (main thread side)
- [ ] Worker spawning and lifecycle management
- [ ] Leader election via BroadcastChannel
- [ ] `_persisted` mutation variants (`insertPersisted`, `createPersisted`, etc.)
- [ ] `PersistenceTier` types and API

---

## Resolved Decisions

| Question                   | Decision                                                                                                                                                          |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| bf-tree integration depth  | Full key-value store with composite keys                                                                                                                          |
| Tab coordination mechanism | Leader election with BroadcastChannel                                                                                                                             |
| Native story               | Single process with sync file I/O                                                                                                                                 |
| Durability default         | Fire-and-forget, `_persisted` variant API for explicit durability                                                                                                 |
| Persistence API style      | Promise-based                                                                                                                                                     |
| Leader failover            | Accept potential loss (fire-and-forget semantics)                                                                                                                 |
| Index method branch param  | Include branch in all index methods (supports multi-branch)                                                                                                       |
| Index value encoding       | Methods take `Value`, not raw bytes - encoding inside IoHandler                                                                                                   |
| Thread safety              | **Single-threaded only.** No `Send + Sync` on `IoHandler`, no `RwLock` in `MemoryIoHandler`. Each thread has its own instance; cross-thread uses message passing. |

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
- [ ] `_persisted` variants resolve on worker ACK

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

---

## Phase 10: Cleanup & Ergonomics

**Goal**: Simplify the `&mut io` drilling pattern introduced in Phase 2.

### The Problem

Phase 2 added `io: &mut H` as a parameter to nearly every method in the call chain: `RuntimeCore` → `SchemaManager` → `QueryManager` → `SyncManager` → `ObjectManager`. This is architecturally correct (IoHandler is owned by RuntimeCore, passed down as needed) but ergonomically noisy—every test helper, every call site, every new method needs to thread it through.

### Approaches to Explore

1. **Store `&mut IoHandler` in managers during `process()`**: RuntimeCore could set a temporary reference before calling into SchemaManager, clearing it after. Avoids permanent ownership issues while reducing parameter drilling during the processing phase. Tricky with Rust lifetimes.

2. **RuntimeCore mediates all IO**: Instead of passing `io` down, managers return "intent" values (what they want to read/write), and RuntimeCore executes them. Clean separation but may be too indirect for sync operations that need immediate results.

3. **Accept the drilling**: The current pattern is explicit, Rust-idiomatic, and zero-cost. The verbosity is real but mechanical—it doesn't add conceptual complexity. If the other approaches add lifetime gymnastics or indirection, the cure may be worse than the disease.

### When

After Phase 3 (delete BTreeIndex) settles, since that phase will change the index call patterns significantly. No point optimizing ergonomics on a shape that's about to change.
