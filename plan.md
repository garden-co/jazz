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

### Signals (Local Subscriptions)
- [x] `futures-signals` integration
- [x] SignalState enum (Loading, Loaded, Error)
- [x] LoadedState with previous tips and current tips (raw data)
- [x] TipState for accessing individual tips
- [x] SignalRegistry for deduplication by (object_id, branch)
- [x] ObjectSignal handles with automatic cleanup when dropped
- [x] Helper functions: build_tip_states, merge_tips, compute_change_ranges
- [x] LoadedState.merge() - compute merge preview on demand
- [x] LoadedState.diff() - compute byte-level diff on demand
- [x] LoadedState.diff_raw() - diff without merge (for single-tip case)
- [x] notify_object_update for batch signal updates
- [x] Async content loading in LoadedState (merge_async, get_tip_content_async, load_all_tips)

### LocalNode Architecture
- [x] LocalNode owns Arc<dyn Environment> for storage
- [x] LocalNode.in_memory() convenience constructor with MemoryEnvironment
- [x] All read/write methods on LocalNode use storage internally
- [x] Signals hold environment reference for async content loading
- [x] SignalRegistry.get_or_create() requires environment parameter
- [x] No main branch convenience methods - all operations require explicit branch name

**Current module structure**:
- `commit.rs` - CommitId, Commit with ContentRef
- `branch.rs` - Branch with LCA, frontier tracking
- `merge.rs` - MergeStrategy trait, LastWriterWins
- `object.rs` - Object with branches, sync/async/streaming read/write, ContentStream
- `node.rs` - LocalNode (owns Environment), generate_object_id(), read/write/subscribe APIs
- `storage.rs` - ContentRef, ChunkHash, ContentStore, CommitStore, Environment, MemoryEnvironment
- `signal.rs` - SignalRegistry, ObjectSignal, SignalState, LoadedState (with env), TipState, LazyDiff

### Persistence Backends
- [ ] Implement RocksDB backend
- [ ] Implement SQLite fallback
- [ ] Implement IndexedDB for browser
- [ ] Tests for persistence roundtrip

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
