# Object Layer Architecture

The object layer provides a Git-like versioned data model for the distributed database. It manages objects containing branches of commits, with content-addressed blob storage and asynchronous persistence.

## Core Concepts

### Object

A top-level container identified by a UUIDv7. Objects hold metadata and named branches.

```
Object
├── id: ObjectId (UUIDv7)
├── metadata: HashMap<String, String>
└── branches: HashMap<BranchName, Branch>
```

### Branch

A named DAG of commits within an object. Tracks the current frontier (tips) and optional truncation boundary (tails).

When commits in a branch diverge (multiple children of the same parent), these divergent paths are called **twigs**. Twigs exist within a single branch and may later merge back together.

```
Branch
├── commits: HashMap<CommitId, Commit>
├── tips: HashSet<CommitId>      # Current frontier
├── tails: Option<HashSet<CommitId>>  # Truncation boundary (None = full history)
└── loaded_state: BranchLoadedState
```

### Commit

An immutable node in the branch DAG. Identified by BLAKE3 hash of its content.

```
Commit
├── parents: Vec<CommitId>       # Empty for root commits
├── content: Vec<u8>             # Application payload
├── timestamp: u64               # Microseconds since epoch
├── author: ObjectId             # Who created it
├── metadata: Option<BTreeMap<String, String>>
└── stored_state: StoredState    # Not included in hash
```

**CommitId** is computed by hashing: parents, content, timestamp, author, and metadata (but not stored_state).

### Blob

Content-addressed binary data. Identified by BLAKE3 hash. Deduplicated across all commits.

```
BlobId
├── object_id: ObjectId
├── branch_name: BranchName
├── commit_id: CommitId
└── content_hash: ContentHash (BLAKE3)
```

Blobs are associated with commits for garbage collection. A blob is deleted only when all associations are removed.

## Identifiers

| Type        | Format   | Generation               |
| ----------- | -------- | ------------------------ |
| ObjectId    | UUIDv7   | `Uuid::now_v7()`         |
| BranchName  | String   | User-defined             |
| CommitId    | [u8; 32] | BLAKE3 hash of commit    |
| ContentHash | [u8; 32] | BLAKE3 hash of blob data |

## State Machines

### ObjectState

```
Creating ──(CreateObject success)──► Available
    │                                    ▲
    └──(error)──► stays Creating         │
                                         │
Loading ──(LoadObjectBranch success)─────┘
```

- **Creating**: Locally created, persistence pending. Operations work immediately.
- **Loading**: Being loaded from storage. Operations must wait/poll.
- **Available**: Fully persisted/loaded. Operations work immediately.

### StoredState (Commits & Blobs)

```
Pending ──(success)──► Stored
    │
    └──(error)──► Errored(msg)

Stored/Pending ──(truncate)──► PendingDelete ──(confirmed)──► removed
```

### BlobState

```
Loading ──(found)──► Available { data, stored_state }
    │
    └──(not found)──► NotFound

Available ──(truncate)──► PendingDelete ──(confirmed)──► removed
```

### BranchLoadedState

Tracks how much of a branch has been loaded from storage:

```
NotLoaded < TipIdsOnly < TipsOnly < AllCommits
```

## ObjectManager

Central coordinator that maintains in-memory state and queues storage operations.

### Fields

```rust
ObjectManager {
    objects: HashMap<ObjectId, ObjectState>,
    outbox: Vec<StorageRequest>,      // Pending writes
    inbox: Vec<StorageResponse>,       // Responses to process
    subscriptions: ...,                // Branch watchers
    subscription_outbox: Vec<SubscriptionUpdate>,
    blobs: HashMap<ContentHash, BlobState>,
    blob_associations: HashMap<ContentHash, Vec<BlobAssociation>>,
    last_timestamp: u64,               // Monotonic clock
}
```

### Public API

#### Object Management

```rust
fn create(&mut self, metadata: Option<...>) -> ObjectId
fn get(&self, id: ObjectId) -> Option<&Object>
fn is_loading(&self, id: ObjectId) -> bool
fn start_loading(&mut self, object_id: ObjectId)
```

#### Commit Operations

```rust
fn add_commit(
    &mut self,
    object_id: ObjectId,
    branch_name: impl Into<BranchName>,
    parents: Vec<CommitId>,
    content: Vec<u8>,
    author: ObjectId,
    metadata: Option<BTreeMap<String, String>>,
) -> Result<CommitId, Error>
```

- Creates branch if `parents` is empty
- Validates parents exist and are not truncated
- Updates tips: removes parents, adds new commit
- Queues `AppendCommit` request

```rust
fn get_tip_ids(&mut self, ...) -> Result<&HashSet<CommitId>, Error>
fn get_tips(&mut self, ...) -> Result<HashMap<CommitId, &Commit>, Error>
fn get_commits(&mut self, ...) -> Result<&HashMap<CommitId, Commit>, Error>
```

#### Blob Operations

```rust
fn associate_blob(
    &mut self,
    object_id: ObjectId,
    branch_name: impl Into<BranchName>,
    commit_id: CommitId,
    data: Vec<u8>,
) -> BlobId
```

- Computes content hash
- Deduplicates: only stores if new
- Queues `StoreBlob` (if new) and `AssociateBlob`

```rust
fn load_blob(&mut self, blob_id: &BlobId) -> Result<&[u8], Error>
```

- Returns data if available
- Queues `LoadBlob` if not present
- Returns `BlobNotLoaded` or `BlobNotFound` errors

#### Branch Truncation

```rust
fn truncate_branch(
    &mut self,
    object_id: ObjectId,
    branch_name: impl Into<BranchName>,
    tail_ids: HashSet<CommitId>,
) -> TruncateResult
```

- Validates all tails exist
- Checks all tips are descendants of some tail
- Finds ancestors of tails (commits to delete)
- Queues: `SetBranchTails`, `DissociateAndMaybeDeleteBlob`, `DeleteCommit`
- Returns `Pending` or `Success { deleted_commits, deleted_blobs }`

#### Subscriptions

```rust
fn subscribe(
    &mut self,
    object_id: ObjectId,
    branch_name: impl Into<BranchName>,
    depth: LoadDepth,
) -> SubscriptionId

fn unsubscribe(&mut self, subscription_id: SubscriptionId)
fn take_subscription_updates(&mut self) -> Vec<SubscriptionUpdate>
```

Subscribers receive `SubscriptionUpdate` with current frontier (tips sorted by timestamp) on:

- Initial subscription (if data loaded)
- New commits added
- Branch loaded from storage

#### Storage Integration

```rust
fn take_requests(&mut self) -> Vec<StorageRequest>
fn push_response(&mut self, response: StorageResponse)
fn process_storage_responses(&mut self)
```

## Storage Protocol

The ObjectManager communicates with storage via request/response enums, enabling pluggable backends.

### Requests

| Request                        | Purpose                                   |
| ------------------------------ | ----------------------------------------- |
| `CreateObject`                 | Persist new object metadata               |
| `AppendCommit`                 | Store commit in branch                    |
| `LoadObjectBranch`             | Load branch at specified depth            |
| `StoreBlob`                    | Persist blob data                         |
| `LoadBlob`                     | Retrieve blob by hash                     |
| `AssociateBlob`                | Record commit→blob reference              |
| `LoadBlobAssociations`         | Get all references to a blob              |
| `DeleteCommit`                 | Remove commit from storage                |
| `DissociateAndMaybeDeleteBlob` | Remove reference; delete blob if orphaned |
| `SetBranchTails`               | Update truncation boundary                |

### Responses

Each request has a corresponding response with the operation result.

### Load Depths

```rust
enum LoadDepth {
    TipIdsOnly,   // Just CommitIds
    TipsOnly,     // Full Commit structs for tips
    AllCommits,   // Entire branch history
}
```

## DAG Topology

### Tips (Frontier)

The tips are commits with no children—the "current state" of a branch. When:

- A commit is added: its parents leave tips, it joins tips
- Twigs diverge: multiple tips exist (divergent paths within a branch)
- Twigs merge: merge commit becomes single tip

```
Linear:       root → c1 → c2 (tip)

Diverged:     root → a (tip)      # two twigs
                   → b (tip)

Merged:       root → a ─┬─► merge (tip)
                   → b ─┘
```

### Tails (Truncation Boundary)

Optional set marking where history was truncated. Commits before tails have been deleted. Used for:

- Garbage collection of old history
- Reducing storage/memory footprint
- Local-first sync (don't need full history)

Invariant: All tips must be descendants of (or equal to) some tail.

## Error Handling

```rust
enum Error {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
    ParentNotFound(CommitId),      // Also used for truncated parents
    ObjectNotReady(ObjectId),       // Loading state
    BranchNotLoaded(BranchName),    // Need to poll
    StorageError(StorageError),
    BlobNotLoaded(ContentHash),     // Need to poll
    BlobNotFound(ContentHash),      // Permanent
}

enum TruncateError {
    ObjectNotFound(ObjectId),
    BranchNotFound(BranchName),
    ObjectNotReady(ObjectId),
    TailNotFound(CommitId),
    TipBeforeTail(CommitId),        // Would orphan a tip
}
```

## Driver Interface

Storage backends implement the `Driver` trait:

```rust
trait Driver {
    fn process(&mut self, requests: Vec<StorageRequest>) -> Vec<StorageResponse>;
}
```

`TestDriver` provides an in-memory implementation for testing.

## Usage Pattern

```rust
// Create manager
let mut manager = ObjectManager::new();

// Create object
let obj_id = manager.create(Some(metadata));

// Add commits
let root = manager.add_commit(obj_id, "main", vec![], content, author, None)?;
let c1 = manager.add_commit(obj_id, "main", vec![root], content2, author, None)?;

// Associate blob
let blob_id = manager.associate_blob(obj_id, "main", c1, blob_data);

// Process with storage
let requests = manager.take_requests();
let responses = driver.process(requests);
for r in responses {
    manager.push_response(r);
}
manager.process_storage_responses();

// Subscribe to updates
let sub_id = manager.subscribe(obj_id, "main", LoadDepth::AllCommits);
let updates = manager.take_subscription_updates();

// Truncate old history
manager.truncate_branch(obj_id, "main", HashSet::from([c1]));
```

## Design Decisions

1. **Content-addressed commits**: Enables deduplication, integrity verification, and deterministic IDs.

2. **Asynchronous persistence**: Operations succeed immediately in memory; storage is eventual. Enables optimistic local-first operations.

3. **Explicit tip/tail tracking**: Tips track the frontier for sync; tails enable bounded history.

4. **Blob deduplication**: Content-addressed storage with reference counting via associations.

5. **Pluggable storage**: Request/response protocol decouples from storage implementation.

6. **Monotonic timestamps**: Guarantees causal ordering within a single manager instance.
