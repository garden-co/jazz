# From Legacy Jazz to New Jazz

## Motivation

The overarching issue: Jazz made life extremely hard by being **e2ee-default**, while few adopters actually care about it. What people care most about is Jazz's **developer experience**, brought about by its local-first properties (everything just looks like local state).

**New direction**: Keep the DX, change almost everything else, erring toward reusing existing tech and conventions instead of NIH.

## Changes

### Trust Model

| Legacy | New |
|--------|-----|
| E2EE by default, server is untrusted | Sync server is trusted with most data |
| All data encrypted with group keys | E2EE becomes opt-in for specific sensitive data |
| Complex key management | Simpler by default, encryption only where needed |

### Query Language & Sync

| Legacy | New |
|--------|-----|
| Reactive subscriptions with dependency loading | SQL (subset) as query interface for API users |
| Custom sync protocol messages | New custom sync protocol for commit graph reconciliation |
| References encrypted, can't query server | SQL queries can be sent to server for index use & sync warmup |
| Multiple roundtrips to resolve references | Server can pre-fetch referenced objects, saving roundtrips |

### Identity

| Legacy | New |
|--------|-----|
| Crypto-first: Account requires Agent (signing keypair) | Server-authenticated accounts (auth tokens) |
| All actions require cryptographic signatures | Crypto optional: device identities can sign tokens |

### Permissions

| Legacy | New |
|--------|-----|
| Groups: early-bound, rigid role hierarchy | ReBAC (Relationship-Based Access Control) |
| Coupled to encryption (key rotation on member changes) | SQL queries, similar to Postgres RLS |
| Fixed roles: admin > manager > writer > reader > writeOnly | Flexible, late-bound permission evaluation |
| CoValue "owned by" a Group | No ownership - purely rule-based |
| P2P capable | Client-server first, P2P later |
| | Creation rules via relationships (e.g., "create Chat if writer in referenced Org") |

### History & Merging

| Legacy | New |
|--------|-----|
| CRDTs replayed on each load | Git-style commit graph per CoValue |
| Easy writes, slow loads | Snapshots (semantically full, but delta-encoded + compressed) |
| Implicit merges (CRDT semantics) | Explicit merges only when branch has multiple tips |
| | 99% of commits are sequential (single parent) due to fast sync |
| | CRDTs can still interpret snapshot history for merge strategy |
| | Read-only clients can render preliminary merge of unmerged frontiers |

### Storage

| Legacy | New |
|--------|-----|
| Platform-specific (SQLite, IndexedDB, etc.) | Simple KV store interface |
| Complex storage adapters | RocksDB where possible, SQLite or IndexedDB as fallback |

### Schema & Types

| Legacy | New |
|--------|-----|
| Zod-like runtime schemas | Build step with code-gen |
| Unwieldy types (no type aliases) | Clean generated types |
| No migration tooling | Prisma-style auto-migrations possible |

### Branching

| Legacy | New |
|--------|-----|
| Implicit in CRDT session structure | First-class abstraction on commit graph |
| | Natural model for parallel development |

### Migrations

| Legacy | New |
|--------|-----|
| Difficult with offline clients | Migration = branching operation |
| | Read-only clients can preview-apply migrations on unmigrated data |
| | Uses same mechanism as merge previews |
