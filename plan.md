# New Jazz Implementation Plan

## Phase 1: Core Data Structures

### Objects with Commit Graph

**Approach**: Start with naive implementation using explicit IDs and uncompressed storage everywhere. Build comprehensive test suite first, then optimize later.

- [ ] Implement Object with commit graph structure
- [ ] Implement Commit with content, parents, metadata
- [ ] Implement Branch with frontier tracking
- [ ] Content hashing for CommitID (BLAKE3)
- [ ] Tests for commit graph operations (add commit, find frontier, etc.)
- [ ] Tests for branching (create branch, list branches)
- [ ] Tests for merging scenarios (sequential, concurrent tips)

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

- [ ] Merge strategies
- [ ] Migration branches
- [ ] Opt-in E2EE for sensitive data
- [ ] Index optimization (beyond brute-force)

---

## Clarifications

**Reactive SQL**: Queries are still reactive (subscription as default, one-time load as special case). The data you subscribe to is determined by an SQL query - the query defines *what* to watch, and changes to matching data trigger updates.
