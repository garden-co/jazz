# New Jazz

A distributed database that syncs across frontend, backend, and cloud. Data feels like local reactive state but syncs automatically.

## Core Principles

- **Local-first DX**: Data looks and feels like local JSON state
- **Trusted sync server**: Server stores data in plaintext by default
- **E2EE opt-in**: Encryption available for sensitive data, not mandatory
- **SQL everywhere**: Query language for both local and server
- **Git-style history**: Compressed snapshots with explicit merges

## Data Model

### CoValues

Collaborative values identified by unique IDs. Each CoValue has:
- A commit graph (history as compressed snapshots with parent references)
- Optional branches
- Schema-defined structure

### Commits

Each commit contains:
- Snapshot of the CoValue state (semantically full, but delta-encoded + compressed in memory, on wire, and on disk)
- Parent reference(s) - typically relative pointers, not full hashes
- Author and timestamp
- Optional metadata

Commit IDs are canonically the content hash of the commit, but in most places explicit storage of hashes is avoided in favor of relative pointers.

### Branches

First-class abstraction on the commit graph:
- Named branches (like git)
- Default "main" branch
- Most CoValues may never use branching (except for migration branches)
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
- Server proactively pushes matching CoValues (saves roundtrips)

### Indices

- Stored as CoValues themselves (sync like any other object)
- Server builds indices over all CoValues it can see
- Client builds indices over CoValues it has locally (subset of server's view)
- First priority: reverse pointer lookup (backlinks) - accelerates joins like "find all Messages referencing this Chat"

## Sync Protocol

Custom protocol for commit graph reconciliation. Primarily client-server (P2P to be explored later). Separate from the SQL query interface.

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
