# New Jazz

A distributed database that syncs across frontend, backend, and cloud. Data feels like local reactive state but syncs automatically.

## Core Principles

- **Local-first DX**: Data looks and feels like local JSON state
- **Trusted sync server**: Server stores data in plaintext by default
- **E2EE opt-in**: Encryption available for sensitive data, not mandatory
- **SQL everywhere**: Query language for both local and server
- **Git-style history**: Compressed snapshots with explicit merges

## Data Model

### Objects

Objects are the fundamental unit, identified by UUIDv7. Each object has:
- A commit graph (history as compressed snapshots with parent references)
- Optional branches (default: "main")
- Schema-defined structure

**Note**: We use "Object" rather than "CoValue" - this is a clean semantic break from legacy Jazz.

### Tables and Rows

The primary use case: each object represents a **table row**. Commit content is the row data (likely JSON format, but implementation detail).

- Schema defines tables (like SQL CREATE TABLE)
- Each row is an object with its own commit history
- References between rows are foreign keys to other objects

### Commits

Each commit contains:
- Snapshot of the object state (semantically full, but delta-encoded + compressed)
- Parent reference(s) - typically relative pointers, not full hashes
- Author and timestamp
- Optional metadata

Commit IDs are canonically the content hash of the commit.

**Content storage**:
- **Inline** (≤1KB): Content stored directly in commit
- **Chunked** (>1KB): Content split via FastCDC, stored separately

### Branches

First-class abstraction on the commit graph:
- Named branches (like git)
- Default "main" branch
- Most objects may never use branching (except for migration branches)
- Explicit merge operations when needed

## Identity

- Accounts are server-authenticated (via auth tokens)
- Cryptographic device identities can be tied to auth tokens (signed and stored in user object)
- Simpler than legacy model where crypto was required for everything

## Permissions (ReBAC)

Relationship-Based Access Control using SQL queries, similar to Postgres Row-Level Security:
- Defined in schema
- Evaluated on server (to enforce what can be synced)
- Client-server model validated first; P2P permissions explored later
- Flexible relationships, not fixed role hierarchies
- Decoupled from encryption
- No ownership concept - purely rule-based
- Creation rules possible, e.g., "can only create Chats that reference an Org where you're a writer"

## Querying

SQL (a parsed subset) as the query interface for API users:
- Runs on top of commit graph (latest state, or historical via magic column filters)
- Can query local state
- Can send queries to sync server to use indices and "warm up" syncing
- Server proactively pushes matching objects (saves roundtrips)
- **Reactive by default**: Queries are subscriptions; one-time load is the special case
- SQL determines *what* to watch; changes to matching data trigger updates

### Indices

- Stored as objects themselves (sync like any other object)
- Server builds indices over all objects it can see
- Client builds indices over objects it has locally (subset of server's view)
- First priority: reverse pointer lookup (backlinks) - accelerates joins like "find all Messages referencing this Chat"

## Sync Protocol

Custom protocol for commit graph reconciliation. Primarily client-server (P2P to be explored later). Separate from the SQL query interface.

Default sync behavior:
- **Frontier commits**: Full sync (metadata + content)
- **Historical commits**: Metadata only, content on demand

## Merging

Merges are only needed when a branch has multiple tips (concurrent unmerged commits). In practice, ~99% of commits are sequential (single parent) because sync is fast enough.

- **Write path**: Merge strategy applied when writing to resolve concurrent tips
- **Read path**: Read-only clients can render preliminary merge of unmerged branch frontiers
- **CRDT option**: Snapshots can be interpreted as mergeable edits using CRDT semantics, with result "baked" into a new commit

## Storage

Simple KV store interface:
- RocksDB (preferred where available)
- SQLite (fallback)
- IndexedDB (browser)

### Object Loading States

Objects can be in one of four states:
1. **Metadata only**: Just ID, type, frontier commit IDs
2. **Frontier loaded**: Latest state in memory, history on disk/remote
3. **Partial history**: Some commits in memory, rest on demand
4. **Fully loaded**: Everything in memory

Loading is implicit (access triggers load). Global memory budget with LRU eviction.

## Schema & Code Generation

Build step generates:
- Clean TypeScript types (with proper type aliases)
- Migration helpers (Prisma-style)

## Migrations

Modeled as branching operations:
- Each schema migration creates a branch
- Offline clients can remain on old branch
- Read-only clients can preview-apply migrations on unmigrated data
- Uses same mechanism as merge previews

---

## Implementation Status

Core commit graph and SQL layer implemented in Rust (`groove` crate):

### Core Layer
- `CommitId` - BLAKE3 content hash (256-bit)
- `Commit` - snapshot with parents, author, timestamp, metadata
- `Branch` - named branch with frontier tracking, LCA computation
- `Object` - with branches, default "main" branch
- `LocalNode` - manages objects with UUIDv7 IDs
- `MergeStrategy` trait with `LastWriterWins` implementation
- `ObjectListenerRegistry` - synchronous callback system for reactivity

### SQL Layer
- `ObjectId` - newtype with Crockford Base32 encoding (26 chars, case-insensitive)
- `Database` - CRUD operations on top of LocalNode
- `TableSchema` - schema definitions stored as Objects
- `Row` - compact binary encoding with length-prefix header
- `Value` - runtime value representation (Null, Bool, I64, F64, String, Bytes, Ref)
- `RefIndex` - reverse index for efficient backlink queries
- SQL parser - CREATE TABLE, INSERT, UPDATE, SELECT with JOIN
- `ReactiveQuery` - synchronous callback-based reactive queries
- Value coercion - String→Ref at execution time for Ref columns

### WASM Bindings
- `groove-wasm` crate with WasmDatabase and WasmQueryHandle
- JavaScript callback integration for reactive queries
- Base32 ObjectId strings in public API

**141 tests** covering commit graph, storage, listeners, SQL parsing, row encoding, and reactive queries.
