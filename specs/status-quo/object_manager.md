# Object Manager — Status Quo

The object layer is the foundation everything else builds on. Every piece of data in Jazz2 — table rows, schema definitions, lenses, index roots — is an object. This is what makes sync possible: replicating the system is just replicating objects.

The model is deliberately git-like. Each object contains branches of immutable commits forming a DAG (directed acyclic graph). Commits are content-addressed via BLAKE3, giving deduplication and integrity for free. When two clients edit the same object concurrently, the branches diverge naturally and can merge later — exactly like git branches.

The [Query Manager](query_manager.md) sits above this layer and interprets objects as SQL rows. The [Sync Manager](sync_manager.md) replicates objects between nodes. The [Storage](storage.md) trait persists them. The Object Manager itself is agnostic to all of these concerns — it just manages versioned, content-addressed data.

## Core Data Model

### Object

A top-level container identified by a UUIDv7. Objects hold metadata and named branches.

```
Object
├── id: ObjectId (UUIDv7)
├── metadata: HashMap<String, String>
└── branches: HashMap<BranchName, Branch>
```

> `crates/groove/src/object.rs:138-154`

### Branch

A named DAG of commits within an object. Tracks the current frontier (tips) and optional truncation boundary (tails).

When commits in a branch diverge (multiple children of the same parent), these divergent paths are called **twigs**. Twigs exist within a single branch and may later merge back together.

```
Branch
├── commits: HashMap<CommitId, Commit>
├── tips: SmolSet<[CommitId; 2]>       # Current frontier
├── tails: Option<HashSet<CommitId>>   # Truncation boundary (None = full history)
└── loaded_state: BranchLoadedState
```

> `crates/groove/src/object.rs:126-136`

### Commit

An immutable node in the branch DAG. Identified by BLAKE3 hash of its content (parents, content, timestamp, author, metadata — but NOT stored_state or ack_state).

> `crates/groove/src/commit.rs:33-48` (struct), `commit.rs:52-86` (CommitId computation)

### Blob

Content-addressed binary data. Identified by BLAKE3 hash. Deduplicated across all commits.

BlobId identifies a blob's association context: `(object_id, branch_name, commit_id, content_hash)`.

> `crates/groove/src/object_manager.rs:62-68`

## Identifiers

| Type        | Format   | Generation                  |
| ----------- | -------- | --------------------------- |
| ObjectId    | UUIDv7   | `Uuid::now_v7()` (interned) |
| BranchName  | String   | User-defined (interned)     |
| CommitId    | [u8; 32] | BLAKE3 hash of commit       |
| ContentHash | [u8; 32] | BLAKE3 hash of blob data    |

> `crates/groove/src/object.rs:10-59` (ObjectId), `object.rs:79-124` (BranchName)

## Storage Model

All persistence is **synchronous** via the `Storage` trait. There are no request/response queues, no async loading states, and no `Driver` trait.

- `Storage` trait: `crates/groove/src/storage/mod.rs:67-195`
- `MemoryStorage`: `crates/groove/src/storage/mod.rs:321-553`
- `BfTreeStorage`: `crates/groove/src/storage/bftree.rs` (persistent B-tree pages via OPFS/disk)

Key Storage methods: `append_commit()`, `store_blob()`, `load_blob()`, `delete_commit()`, `set_branch_tails()`, `index_insert()`, `index_remove()`, `index_lookup()`, `index_range()`, `index_scan_all()`.

All ObjectManager write operations call Storage synchronously — no eventual consistency within a single node.

## ObjectManager

Central coordinator that maintains in-memory state and writes synchronously to Storage.

> `crates/groove/src/object_manager.rs:124-142`

### Public API — Object Management

| Method             | Purpose                                                     |
| ------------------ | ----------------------------------------------------------- |
| `create()`         | Create object with auto-generated ObjectId, sync to Storage |
| `create_with_id()` | Create with deterministic ID (for index roots)              |
| `get()`            | Return `Option<&Object>` from memory                        |
| `get_or_load()`    | Lazy cold-start: load from Storage if not in memory         |
| `receive_object()` | Accept pre-built object from sync layer                     |

> `crates/groove/src/object_manager.rs:168-260` (create, get, get_or_load), `607-623` (receive_object)

### Public API — Commit Operations

| Method                                           | Purpose                                                                                            |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| `add_commit()`                                   | Create branch if parents empty, validate parents, update tips, sync to Storage, notify subscribers |
| `replace_content()`                              | Special case for derived data (indices) — clears all commits, does NOT call Storage                |
| `receive_commit()`                               | Accept pre-built commit from sync, idempotent, sync to Storage                                     |
| `get_tip_ids()` / `get_tips()` / `get_commits()` | Read branch state                                                                                  |

> `crates/groove/src/object_manager.rs:291-527` (add_commit through get_commits), `630-712` (receive_commit)

### Public API — Blob Operations

| Method             | Purpose                                                                  |
| ------------------ | ------------------------------------------------------------------------ |
| `associate_blob()` | Compute BLAKE3 hash, deduplicate, store synchronously, track association |
| `load_blob()`      | Return blob data from Storage, cache in memory                           |
| `put_blob()`       | Simpler interface using associate_blob                                   |
| `get_blob()`       | Return cached blob (does NOT load from Storage)                          |

> `crates/groove/src/object_manager.rs:533-600` (associate_blob, load_blob), `718-739` (put_blob, get_blob)

### Public API — Branch Truncation

`truncate_branch()` validates tails, checks all tips are descendants of some tail, finds ancestors for deletion, syncs to Storage. Returns `TruncateResult::Success { deleted_commits, deleted_blobs }` or `TruncateResult::PermanentError(TruncateError)`.

> `crates/groove/src/object_manager.rs:899-1006`

### Public API — Subscriptions

Two subscription levels:

1. **Per-branch**: `subscribe()` / `unsubscribe()` / `take_subscription_updates()` — watchers get `SubscriptionUpdate` with frontier (tips sorted by timestamp).
2. **Global**: `subscribe_all()` / `unsubscribe_all()` / `take_all_object_updates()` — `AllObjectUpdate` with `is_new_object`, `previous_commit_ids`, `old_content` for QueryManager index deltas.

> `crates/groove/src/object_manager.rs:745-821`

## DAG Topology

**Tips** (frontier): commits with no children — the "current state" of a branch. When a commit is added, its parents leave tips and it joins tips.

**Tails** (truncation boundary): optional set marking where history was truncated. Invariant: all tips must be descendants of (or equal to) some tail.

```
Linear:       root → c1 → c2 (tip)
Diverged:     root → a (tip)      # two twigs
                   → b (tip)
Merged:       root → a ─┬─► merge (tip)
                   → b ─┘
```

## Error Handling

```
Error: ObjectNotFound, BranchNotFound, ParentNotFound, StorageError, BlobNotFound
TruncateError: ObjectNotFound, BranchNotFound, TailNotFound, TipBeforeTail
```

> `crates/groove/src/object_manager.rs:80-109`

Note: the old async-era errors (`ObjectNotReady`, `BranchNotLoaded`, `BlobNotLoaded`) no longer exist.

## Design Decisions

1. **Content-addressed commits**: BLAKE3 hashing gives deduplication, integrity verification, and deterministic IDs. Two clients creating the same commit independently produce the same CommitId — sync can detect this and skip duplicates.
2. **Synchronous persistence**: All writes go to Storage immediately. Within a single node, there's no window where data is "committed but not persisted." This simplifies the query engine enormously — see [Storage](storage.md).
3. **Explicit tip/tail tracking**: Tips (the frontier) tell sync "what's new since last time." Tails let us bound history size without losing data integrity.
4. **Blob deduplication**: Large binary payloads stored once, referenced by content hash. Multiple commits sharing the same file content share one blob.
5. **Pluggable storage via trait**: `Storage` trait decouples ObjectManager from backend (MemoryStorage, BfTreeStorage). The same ObjectManager code runs in browser WASM and native Rust.
6. **Monotonic timestamps**: `next_timestamp()` guarantees causal ordering within a single manager instance. (`object_manager.rs:151-164`)

## Test Coverage

46+ unit tests in `object_manager.rs:1199-2287` using `MemoryStorage`.

Note: blob and truncation tests were rewritten for the sync Storage API. A TODO remains for additional blob test coverage.
