# New Jazz Implementation Plan

## Phase 1: Core Data Structures

### Objects with Commit Graph

**Approach**: Start with naive implementation using explicit IDs and uncompressed storage everywhere. Build comprehensive test suite first, then optimize later.

- [x] Implement Object with commit graph structure
- [x] Implement Commit with content, parents, metadata
- [x] Implement Branch with frontier tracking
- [x] Content hashing for CommitID (BLAKE3)
- [x] UUIDv7 generation for Object IDs
- [x] Tests for commit graph operations (add commit, find frontier, etc.)
- [x] Tests for branching (create branch, list branches)
- [x] Tests for merging scenarios (sequential, concurrent tips)
- [x] LCA (Lowest Common Ancestor) computation
- [x] MergeStrategy trait with LastWriterWins implementation
- [x] Branch merging (merge_branches)

**Future optimizations** (after tests are solid):
- [ ] Delta encoding for commits
- [ ] Relative pointers instead of explicit hashes
- [ ] Compression
- [ ] FastCDC for content-defined chunking (currently using fixed-size chunking)

### Storage Abstraction
- [x] ContentRef enum (Inline ≤1KB, Chunked >1KB)
- [x] ChunkHash type (BLAKE3)
- [x] ContentStore trait (async get/put/has chunk)
- [x] CommitStore trait (async commit operations, list_commits walks full history)
- [x] Environment trait (combines ContentStore + CommitStore + Send + Sync + Debug)
- [x] MemoryEnvironment (full in-memory implementation for testing)
- [x] MemoryContentStore (legacy content-only store for backwards compatibility)
- [x] Sync read/write methods (write_sync, read_sync) - require explicit branch
- [x] Async read/write methods (write, read) - require explicit branch
- [x] Streaming read/write methods (write_stream, read_stream) - require explicit branch

### Listeners (Local Subscriptions)
- [x] Synchronous callback system (replaced futures-signals)
- [x] ObjectListenerRegistry for managing subscriptions by (object_id, branch)
- [x] ObjectCallback type: `Box<dyn Fn(Arc<ObjectState>) + Send + Sync>`
- [x] ObjectState with previous tips and current tips
- [x] ListenerId for subscription management
- [x] subscribe() returns ListenerId, calls callback immediately with current state
- [x] unsubscribe() removes listener by ID
- [x] Helper functions: compute_change_ranges, ByteDiff, DiffRange
- [x] Callbacks fire synchronously during write operations (same call stack)

### LocalNode Architecture
- [x] LocalNode owns Arc<dyn Environment> for storage
- [x] LocalNode.in_memory() convenience constructor with MemoryEnvironment
- [x] All read/write methods on LocalNode use storage internally
- [x] ObjectListenerRegistry for synchronous callback management
- [x] write_sync/write/write_stream automatically notify listeners
- [x] No main branch convenience methods - all operations require explicit branch name

**Current module structure**:
- `commit.rs` - CommitId, Commit with ContentRef
- `branch.rs` - Branch with LCA, frontier tracking
- `merge.rs` - MergeStrategy trait, LastWriterWins
- `object.rs` - Object with branches, sync/async/streaming read/write, ContentStream
- `node.rs` - LocalNode (owns Environment), generate_object_id(), read/write/subscribe APIs
- `storage.rs` - ContentRef, ChunkHash, ContentStore, CommitStore, Environment, MemoryEnvironment
- `listener.rs` - ObjectListenerRegistry, ObjectCallback, ObjectState, ListenerId, ByteDiff

### Persistence Backends
- [ ] Implement RocksDB backend
- [ ] Implement SQLite fallback
- [ ] Implement IndexedDB for browser
- [ ] Tests for persistence roundtrip

## Phase 1.5: SQL Layer

### Step 1: Basic Storage
- [x] `ColumnType` and `ColumnDef` types
- [x] `TableSchema` type with serialization
- [x] Row binary encoding/decoding (length-prefix header + column values)
- [x] Nullable column handling (1-byte presence flag in content)
- [x] SQL parser: CREATE TABLE, INSERT, UPDATE, SELECT
- [x] `create_table` - store schema as Object
- [x] `insert` - create row Object with encoded data
- [x] `get` - fetch and decode row by ID
- [x] `update` - create new commit on row Object
- [x] `delete` - tombstone commit on row Object
- [x] `execute()` method for SQL strings

### Step 2: References and Queries
- [x] `Ref` column type with target schema validation
- [x] Index object creation per (source_table, source_column) Ref column
- [x] Synchronous index maintenance on insert/update/delete
- [x] `select` with scan-based where clause (`=` only)
- [x] `find_referencing` using index lookup
- [x] `reactive_query()` for callback-based reactive queries
- [x] ReactiveQuery with subscribe()/unsubscribe() for synchronous callbacks
- [x] ReactiveQueryRegistry for tracking active queries per table
- [x] Callbacks fire synchronously on insert/update/delete

### Syncing Objects
- [ ] Design sync protocol for commit graph reconciliation
- [ ] Implement client-side sync
- [ ] Implement server-side sync
- [ ] Tests for sync scenarios (sequential commits, concurrent commits, reconnection)

## Phase 2: SQL Interface

### Basic Tables
- [ ] Schema definition (code-gen build step)
- [ ] Create simple tables (maps to CoValue type)
- [ ] Basic CRUD via SQL subset

### References
- [ ] Allow referencing other tables (foreign keys to other CoValues)
- [ ] Maintain reverse pointer index over all objects (backlinks)

### Querying
- [ ] SQL parser for subset
- [ ] Brute-force scan implementation
- [ ] Historical queries via magic column filters
- [ ] Reactive subscriptions (default) with one-time load as special case

## Phase 3: Permissions & Identity

- [ ] Server-authenticated accounts
- [ ] ReBAC rules in schema
- [ ] Permission evaluation on server
- [ ] Creation rules

## Phase 4: Advanced Features

- [ ] Additional merge strategies (beyond LastWriterWins)
- [ ] Migration branches
- [ ] Opt-in E2EE for sensitive data
- [ ] Index optimization (beyond brute-force)

---

## Clarifications

**Reactive SQL**: Queries are still reactive (subscription as default, one-time load as special case). The data you subscribe to is determined by an SQL query - the query defines *what* to watch, and changes to matching data trigger updates.
