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

**Current module structure**:
- `commit.rs` - CommitId, Commit
- `branch.rs` - Branch with LCA, frontier tracking
- `merge.rs` - MergeStrategy trait, LastWriterWins
- `object.rs` - Object (CoValue) with branches
- `node.rs` - LocalNode, generate_object_id()

**Future optimizations** (after tests are solid):
- [ ] Delta encoding for commits
- [ ] Relative pointers instead of explicit hashes
- [ ] Compression

### Persistence
- [ ] Define KV store interface
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
