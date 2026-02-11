# Design: Migrate Server-Side Storage from SQLite to Fjall

## Overview

Replace the server-side SQLite storage backend (`better-sqlite3`) with [fjall](https://github.com/fjall-rs/fjall), a Rust-based, LSM-tree key-value storage engine. Fjall will be integrated into the existing `cojson-core-napi` crate, reusing the same binary distribution to avoid publishing additional platform-specific packages. The migration targets **server-side only** — browser (IndexedDB) and Cloudflare (Durable Objects) storage remain unchanged.

### Why Fjall?

- **LSM-tree architecture**: Optimized for write-heavy workloads (Jazz sync servers are write-intensive)
- **Native Rust**: Eliminates the `better-sqlite3` native dependency and its C compilation overhead
- **Built-in compression**: LZ4 by default, reducing disk usage for transaction logs
- **Thread-safe**: Internal synchronization eliminates need for TypeScript-level transaction queuing
- **Embeddable**: No external server process required
- **Stable disk format**: Migration path guaranteed across versions

### Performance Expectations

- **Writes**: Significantly faster due to LSM-tree append-only write path (no B-tree rebalancing)
- **Sequential reads**: Comparable or faster (LSM compaction keeps data sorted)
- **Point lookups**: Comparable (block cache + bloom filters)
- **Range scans**: Comparable (sorted runs)
- **Disk usage**: Likely smaller due to built-in LZ4 compression

## Architecture / Components

### Current Architecture

```
jazz-run (server)
  └── LocalNode.setStorage()
        └── StorageApiSync (TypeScript, unchanged across backends)
              └── SQLiteClient implements DBClientInterfaceSync (TypeScript)
                    └── BetterSqliteDriver implements SQLiteDatabaseDriver (TypeScript)
                          └── better-sqlite3 (native C addon)
```

### New Architecture

```
jazz-run (server)
  └── LocalNode.setStorage()
        └── StorageApiAsync (TypeScript, UNCHANGED)
              └── FjallClient implements DBClientInterfaceAsync (TypeScript, packages/cojson-storage-fjall)
                    └── FjallStorageNapi (NAPI AsyncTask bindings inside cojson-core-napi, Rust)
                          └── FjallStorage (crates/cojson-storage-fjall, Rust)
                                └── fjall (external Rust crate)
```

### Key Design Decisions

#### Interface Layer

The migration plugs in at the `DBClientInterfaceAsync` level. `StorageApiAsync` — which handles known-state caching, store queue, correction callbacks, cooperative eraser cancellation, and deletion scheduling — remains completely unchanged. We only replace the database client layer.

#### Async via libuv Thread Pool

All fjall storage operations are offloaded to the **libuv thread pool** via NAPI's `AsyncTask` trait. This means:

- The **Node.js event loop stays free** to process WebSocket messages while storage I/O runs in background threads
- **Fjall is thread-safe** internally (`Keyspace` and `PartitionHandle` are `Arc`-wrapped), so multiple libuv threads can safely perform concurrent reads
- The Rust `FjallStorage` core remains **synchronous and simple** — only the NAPI boundary introduces async
- On the TypeScript side, every NAPI method returns a `Promise`, naturally fitting `DBClientInterfaceAsync`

This gives the server significantly better throughput under load compared to the current synchronous SQLite approach, where every storage operation blocks the event loop.

#### Summary

- **No changes** to `StorageApiAsync`, `StorageAPI`, sync protocol, or `LocalNode`
- **New Rust crate** (`cojson-storage-fjall`) in `crates/` implementing the pure storage engine logic
- **NAPI bindings added to `cojson-core-napi`** — the fjall storage class is exported from the existing NAPI binary via `AsyncTask`, avoiding a separate platform-specific package distribution
- **New TypeScript package** (`cojson-storage-fjall`) in `packages/` providing the `FjallClient` wrapper that imports from `cojson-core-napi`
- **Minimal change** to `jazz-run` to use the new storage

### Component Diagram

```
crates/
├── cojson-storage-fjall/          # Pure Rust storage engine (new crate)
│   ├── Cargo.toml                 # Depends on fjall
│   └── src/
│       ├── lib.rs                 # FjallStorage struct
│       ├── keyspaces.rs           # Keyspace definitions + key encoding
│       └── migrations.rs          # Data version management
│
├── cojson-core-napi/              # EXISTING crate — add fjall bindings here
│   ├── Cargo.toml                 # Add cojson-storage-fjall dependency
│   └── src/
│       ├── lib.rs                 # Existing exports + new FjallStorageNapi
│       └── storage/               # New module for fjall NAPI bindings
│           └── mod.rs             # #[napi] FjallStorageNapi struct
│
packages/
├── cojson-storage-fjall/          # TypeScript wrapper (new package)
│   ├── package.json               # Depends on cojson-core-napi
│   └── src/
│       └── index.ts               # FjallClient + getFjallStorage()
```

This approach has a key advantage: **fjall ships inside the existing `cojson-core-napi` binary**. No additional platform-specific npm packages need to be published, and the existing CI pipeline for NAPI builds handles compilation for all platforms automatically. The fjall dependency only adds to the binary size of the NAPI addon — it is a pure Rust dependency with no additional native build requirements.

## Data Model

### SQLite Schema → Fjall Keyspace Mapping

The current SQLite schema uses 6 tables with auto-incrementing rowIDs and SQL indexes. Fjall uses keyspaces (similar to column families) with lexicographically-sorted byte keys.

#### Key Encoding

All composite keys use a length-prefixed encoding to ensure correct lexicographic ordering:

```rust
/// Encode a u64 as 8 big-endian bytes for correct lexicographic ordering
fn encode_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

/// Encode a composite key: u64 prefix + separator + suffix bytes
fn encode_composite_key(prefix: u64, suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + suffix.len());
    key.extend_from_slice(&encode_u64(prefix));
    key.extend_from_slice(suffix);
    key
}
```

#### Keyspace Definitions

| SQLite Table | Fjall Keyspace | Key | Value |
|---|---|---|---|
| `coValues` | `covalue_by_id` | `{coValueID}` (UTF-8 bytes) | `{rowID(u64-BE)}{header JSON}` |
| _(reverse)_ | `covalue_by_row` | `{rowID}` (u64-BE) | `{coValueID}` (UTF-8 bytes) |
| `sessions` | `session_by_cv_sid` | `{coValueRowID(u64-BE)}{sessionID}` | `{rowID(u64-BE)}{lastIdx(u32-BE)}{lastSignature}{bytesSinceLastSig(u32-BE)}` |
| _(reverse)_ | `session_by_row` | `{rowID}` (u64-BE) | `{coValueRowID(u64-BE)}{sessionID}` |
| `transactions` | `transactions` | `{sessionRowID(u64-BE)}{idx(u32-BE)}` | `{tx JSON}` (UTF-8 bytes) |
| `signatureAfter` | `signature_after` | `{sessionRowID(u64-BE)}{idx(u32-BE)}` | `{signature}` (UTF-8 bytes) |
| `unsynced_covalues` | `unsynced` | `{coValueID}\x00{peerID}` | `[]` (empty) |
| `deletedCoValues` | `deleted` | `{coValueID}` | `{status(u8)}` (0=Pending, 1=Done) |
| _(metadata)_ | `meta` | `"next_covalue_row_id"` | `{u64-BE}` |
| | | `"next_session_row_id"` | `{u64-BE}` |
| | | `"schema_version"` | `{u32-BE}` |

#### Auto-Incrementing Row IDs

SQLite provides auto-incrementing `rowID` values. In fjall, we maintain two monotonic counters stored in the `meta` keyspace:
- `next_covalue_row_id`: Atomically incremented when inserting a new CoValue
- `next_session_row_id`: Atomically incremented when inserting a new session

```rust
impl FjallStorage {
    fn next_covalue_row_id(&self) -> u64 {
        let key = b"next_covalue_row_id";
        let current = self.meta.get(key)
            .ok().flatten()
            .map(|v| u64::from_be_bytes(v[..8].try_into().unwrap()))
            .unwrap_or(1);
        let next = current + 1;
        self.meta.insert(key, &next.to_be_bytes()).unwrap();
        current
    }
}
```

### Rust Storage Implementation

Core struct:

```rust
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};

pub struct FjallStorage {
    db: Keyspace,
    covalue_by_id: PartitionHandle,
    covalue_by_row: PartitionHandle,
    session_by_cv_sid: PartitionHandle,
    session_by_row: PartitionHandle,
    transactions: PartitionHandle,
    signature_after: PartitionHandle,
    unsynced: PartitionHandle,
    deleted: PartitionHandle,
    meta: PartitionHandle,
}

impl FjallStorage {
    pub fn open(path: &str) -> Result<Self, fjall::Error> {
        let db = Config::new(path).open()?;
        let opts = PartitionCreateOptions::default();

        Ok(Self {
            covalue_by_id: db.open_partition("covalue_by_id", opts.clone())?,
            covalue_by_row: db.open_partition("covalue_by_row", opts.clone())?,
            session_by_cv_sid: db.open_partition("session_by_cv_sid", opts.clone())?,
            session_by_row: db.open_partition("session_by_row", opts.clone())?,
            transactions: db.open_partition("transactions", opts.clone())?,
            signature_after: db.open_partition("signature_after", opts.clone())?,
            unsynced: db.open_partition("unsynced", opts.clone())?,
            deleted: db.open_partition("deleted", opts.clone())?,
            meta: db.open_partition("meta", opts)?,
            db,
        })
    }
}
```

### NAPI Bindings (inside `cojson-core-napi`)

The NAPI bindings are added as a new module inside the existing `cojson-core-napi` crate. This means the `FjallStorageNapi` class is exported alongside `SessionMap`, crypto functions, and other existing exports from the same `.node` binary.

Each storage method uses the **`AsyncTask` trait** to offload work to the libuv thread pool. The `FjallStorage` is wrapped in `Arc` so it can be cheaply cloned into each task.

**Cargo.toml change** — add the dependency:
```toml
# crates/cojson-core-napi/Cargo.toml
[dependencies]
cojson-storage-fjall = { path = "../cojson-storage-fjall" }
```

**New module** — `crates/cojson-core-napi/src/storage/mod.rs`:
```rust
use std::sync::Arc;
use napi::bindgen_prelude::*;
use napi::Task;
use cojson_storage_fjall::FjallStorage;

// --- AsyncTask structs (one per operation) ---

/// Runs get_co_value on a libuv worker thread.
struct GetCoValueTask {
    storage: Arc<FjallStorage>,
    co_value_id: String,
}

#[napi(object)]
pub struct CoValueResult {
    pub row_id: u32,
    pub header_json: String,
}

impl Task for GetCoValueTask {
    type Output = Option<CoValueResult>;
    type JsValue = Option<CoValueResult>;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        // Runs on libuv worker thread — event loop stays free
        self.storage.get_co_value(&self.co_value_id)
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(output)
    }
}

/// Runs upsert_co_value on a libuv worker thread.
struct UpsertCoValueTask {
    storage: Arc<FjallStorage>,
    id: String,
    header_json: Option<String>,
}

impl Task for UpsertCoValueTask {
    type Output = Option<u32>;
    type JsValue = Option<u32>;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        self.storage.upsert_co_value(&self.id, self.header_json.as_deref())
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(output)
    }
}

// ... similar AsyncTask structs for each operation:
// GetCoValueSessionsTask, GetSingleCoValueSessionTask,
// GetNewTransactionInSessionTask, GetSignaturesTask,
// AddSessionUpdateTask, AddTransactionTask, AddSignatureAfterTask,
// MarkCoValueAsDeletedTask, EraseCoValueButKeepTombstoneTask,
// GetAllCoValuesWaitingForDeleteTask, TrackCoValuesSyncStateTask,
// GetUnsyncedCoValueIDsTask, StopTrackingSyncStateTask,
// GetCoValueKnownStateTask, TransactionTask

// --- NAPI class ---

#[napi]
pub struct FjallStorageNapi {
    inner: Arc<FjallStorage>,
}

#[napi]
impl FjallStorageNapi {
    #[napi(constructor)]
    pub fn new(path: String) -> napi::Result<Self> {
        let inner = FjallStorage::open(&path)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(Self { inner: Arc::new(inner) })
    }

    /// Returns Promise<CoValueResult | null> — resolved on libuv thread.
    #[napi]
    pub fn get_co_value(&self, co_value_id: String) -> AsyncTask<GetCoValueTask> {
        AsyncTask::new(GetCoValueTask {
            storage: Arc::clone(&self.inner),
            co_value_id,
        })
    }

    /// Returns Promise<number | null> — resolved on libuv thread.
    #[napi]
    pub fn upsert_co_value(&self, id: String, header_json: Option<String>) -> AsyncTask<UpsertCoValueTask> {
        AsyncTask::new(UpsertCoValueTask {
            storage: Arc::clone(&self.inner),
            id,
            header_json,
        })
    }

    // ... all other methods follow the same pattern:
    // each returns AsyncTask<XxxTask> which napi-rs converts to a Promise on the JS side

    /// Close the database, flushing pending writes.
    /// Constructor (sync) — no need for AsyncTask since open() is a one-time operation.
    #[napi]
    pub fn close(&self) -> napi::Result<()> {
        self.inner.close()
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}
```

**lib.rs change** — register the module:
```rust
// crates/cojson-core-napi/src/lib.rs
mod storage;
pub use storage::*;
```

The auto-generated `index.d.ts` will include `FjallStorageNapi` alongside all existing type definitions. On the TypeScript side, every method except `new()` and `close()` will appear as returning `Promise<T>`.

#### How libuv Thread Pool Works Here

```
    JS Main Thread (event loop)              libuv Worker Threads
    ┌──────────────────────────┐            ┌────────────────────┐
    │  client.getCoValue("id") │──dispatch──▶│ FjallStorage.get() │
    │  ↓ returns Promise       │            │ (disk I/O here)    │
    │  ... handles WebSocket   │            │ ↓ result ready     │
    │  ... processes messages   │◀─resolve───│                    │
    │  await result             │            └────────────────────┘
    └──────────────────────────┘
```

Multiple storage operations can run concurrently on different libuv threads, and fjall's internal locking ensures safety. The default libuv thread pool size is 4, which can be tuned via the `UV_THREADPOOL_SIZE` environment variable if needed.

### TypeScript Client

Thin wrapper that maps NAPI Promise results to the `DBClientInterfaceAsync` interface. Since every NAPI method returns a `Promise` (resolved on a libuv worker thread), this naturally fits the async interface:

```typescript
import { FjallStorageNapi } from "cojson-core-napi";  // Reuse existing NAPI package
import type {
  DBClientInterfaceAsync,
  DBTransactionInterfaceAsync,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
  SignatureAfterRow,
  SessionRow,
} from "cojson";
import { StorageApiAsync } from "cojson";
import type { CoValueHeader, Transaction } from "cojson";
import type { RawCoID, SessionID, Signature } from "cojson";
import type { PeerID } from "cojson";
import type { CoValueKnownState } from "cojson";

export class FjallClient implements DBClientInterfaceAsync, DBTransactionInterfaceAsync {
  private readonly napi: FjallStorageNapi;

  constructor(path: string) {
    this.napi = new FjallStorageNapi(path);
  }

  async getCoValue(coValueId: string): Promise<StoredCoValueRow | undefined> {
    const result = await this.napi.getCoValue(coValueId);
    if (!result) return undefined;
    return {
      rowID: result.rowId,
      id: coValueId as RawCoID,
      header: JSON.parse(result.headerJson),
    };
  }

  async upsertCoValue(id: string, header?: CoValueHeader): Promise<number | undefined> {
    return this.napi.upsertCoValue(id, header ? JSON.stringify(header) : undefined);
  }

  async getCoValueSessions(coValueRowId: number): Promise<StoredSessionRow[]> {
    return this.napi.getCoValueSessions(coValueRowId);
  }

  async getSingleCoValueSession(
    coValueRowId: number,
    sessionID: SessionID,
  ): Promise<StoredSessionRow | undefined> {
    return this.napi.getSingleCoValueSession(coValueRowId, sessionID);
  }

  async getNewTransactionInSession(
    sessionRowId: number,
    fromIdx: number,
    toIdx: number,
  ): Promise<TransactionRow[]> {
    const rawRows = await this.napi.getNewTransactionInSession(sessionRowId, fromIdx, toIdx);
    return rawRows.map((row) => ({
      ...row,
      tx: JSON.parse(row.tx) as Transaction,
    }));
  }

  async transaction(callback: (tx: DBTransactionInterfaceAsync) => Promise<unknown>): Promise<unknown> {
    // FjallClient itself serves as the transaction interface.
    // Fjall handles atomicity internally via write batches for
    // operations that need it (e.g. eraseCoValueButKeepTombstone).
    return callback(this);
  }

  async getCoValueKnownState(coValueId: string): Promise<CoValueKnownState | undefined> {
    return this.napi.getCoValueKnownState(coValueId);
  }

  // ... remaining methods follow the same async delegation pattern
}

export function getFjallStorage(path: string) {
  const client = new FjallClient(path);
  return new StorageApiAsync(client);
}
```

### Integration with jazz-run

Minimal change in `startSyncServer.ts`:

```typescript
// Before:
import { getBetterSqliteStorage } from "cojson-storage-sqlite";
const storage = getBetterSqliteStorage(db);

// After:
import { getFjallStorage } from "cojson-storage-fjall";
const storage = getFjallStorage(db);
```

The return type is the same (`StorageAPI`), so `LocalNode.setStorage()` works unchanged.

### Transaction Semantics

SQLite uses explicit `BEGIN/COMMIT` transactions. Fjall provides write batches for atomic multi-key writes. `StorageApiAsync` uses a `StoreQueue` to serialize store operations, and calls `transaction()` on the client for atomic session updates.

In the TypeScript `FjallClient`, `transaction()` passes `this` as the transaction interface (same pattern as `SQLiteClient`). For operations requiring multi-key atomicity (e.g., `eraseCoValueButKeepTombstone`), the Rust layer uses `fjall::Batch`:

```rust
/// Atomic multi-key write in Rust (used for deletion erasure).
pub fn erase_co_value_but_keep_tombstone(&self, co_value_id: &str) -> Result<(), fjall::Error> {
    let mut batch = self.db.batch();

    // Delete all non-tombstone sessions, their transactions and signatures
    // ... iterate and collect keys to remove ...

    for key in keys_to_remove {
        batch.remove(&self.transactions, key);
    }

    // Mark deletion as done
    batch.insert(&self.deleted, co_value_id.as_bytes(), &[1u8]); // Done status

    batch.commit()?;
    Ok(())
}
```

Note: `StorageApiAsync` serializes stores via its internal `StoreQueue` (one store at a time), which means concurrent write conflicts are already prevented at the TypeScript layer. Fjall's thread safety provides an additional safety net.

### Query Patterns → Fjall Operations

| SQLite Query | Fjall Operation |
|---|---|
| `SELECT * FROM coValues WHERE id = ?` | `covalue_by_id.get(id)` |
| `SELECT * FROM sessions WHERE coValue = ?` | `session_by_cv_sid.prefix(encode_u64(cv_row_id))` |
| `SELECT * FROM sessions WHERE coValue = ? AND sessionID = ?` | `session_by_cv_sid.get(encode_composite_key(cv_row_id, session_id))` |
| `SELECT * FROM transactions WHERE ses = ? AND idx >= ? AND idx <= ?` | `transactions.range(encode_key(ses, from)..=encode_key(ses, to))` |
| `SELECT * FROM signatureAfter WHERE ses = ? AND idx >= ?` | `signature_after.range(encode_key(ses, from)..)` (with prefix filter) |
| `INSERT ... ON CONFLICT DO NOTHING` | `if !partition.contains_key(k) { partition.insert(k, v) }` |
| `INSERT ... ON CONFLICT DO UPDATE` | `partition.insert(k, v)` (upsert by default) |
| `DELETE FROM ... WHERE ...` | Iterate prefix/range + `partition.remove(k)` |
| `SELECT DISTINCT co_value_id FROM unsynced_covalues` | `unsynced.prefix("")` with deduplication by prefix |

### Build System Integration

Since we're extending `cojson-core-napi` rather than creating a new NAPI crate:

1. Add `cojson-storage-fjall` to `crates/Cargo.toml` workspace members
2. Add `cojson-storage-fjall` as a path dependency in `cojson-core-napi/Cargo.toml`
3. The existing `build:napi` task and `napi.yml` CI workflow automatically pick up the new code — no new platform packages or CI workflows needed
4. Add `cojson-storage-fjall` TypeScript package to `packages/` with standard `build` task in `turbo.json`

## Testing Strategy

Testing focuses on proving the fjall backend is a drop-in replacement for SQLite, producing identical behavior.

### 1. Shared Storage Conformance Tests

Extract the existing `cojson-storage-sqlite` tests into a shared test suite that can run against any `DBClientInterfaceAsync` implementation. Both SQLite (async) and fjall backends must pass the same tests:

```typescript
// packages/cojson/src/storage/tests/storageConformance.test.ts
import { describe, it, expect } from "vitest";
import type { DBClientInterfaceAsync } from "../types.js";

export function runStorageConformanceTests(
  createClient: () => Promise<DBClientInterfaceAsync>,
) {
  describe("CoValue CRUD", () => {
    it("should upsert and retrieve a CoValue", async () => {
      const client = await createClient();
      const header = { type: "comap", ruleset: { type: "ownedByGroup" }, meta: null, createdAt: null, uniqueness: null };
      const rowId = await client.upsertCoValue("co_test123" as RawCoID, header);
      expect(rowId).toBeDefined();

      const stored = await client.getCoValue("co_test123");
      expect(stored?.header).toEqual(header);
    });
  });

  describe("Session operations", () => {
    it("should add and query sessions for a CoValue", async () => {
      // ...
    });
  });

  describe("Transaction storage", () => {
    it("should store and range-query transactions", async () => {
      // ...
    });
  });

  describe("Deletion workflow", () => {
    it("should mark, list, and erase deleted CoValues preserving tombstones", async () => {
      // ...
    });
  });

  describe("Sync tracking", () => {
    it("should track and query unsynced CoValues", async () => {
      // ...
    });
  });
}
```

Then each backend registers:

```typescript
// packages/cojson-storage-fjall/src/tests/conformance.test.ts
import { runStorageConformanceTests } from "cojson/storage/tests/storageConformance.test.js";
import { FjallClient } from "../index.js";

runStorageConformanceTests(async () => new FjallClient(tempDir()));
```

### 2. Integration Tests with Sync

Test the full stack: `LocalNode` → `StorageApiAsync` → `FjallClient` → fjall, verifying sync correctness:

```typescript
import { describe, it, expect } from "vitest";
import { createTestNode } from "cojson/tests/testUtils.js";
import { getFjallStorage } from "cojson-storage-fjall";

describe("Fjall storage integration", () => {
  it("should persist and reload CoValues through sync", async () => {
    const storage = getFjallStorage(tempDir());
    const node = createTestNode();
    node.setStorage(storage);

    // Create a CoMap, add data, verify it persists across node restarts
    // ...
  });

  it("should handle concurrent sync from multiple peers", async () => {
    // Spin up two nodes with fjall storage, sync through a third
    // Verify data consistency
  });
});
```

### 3. Rust Unit Tests

Test the core Rust storage implementation independently:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_covalue_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        let row_id = storage.upsert_co_value("co_test", Some(r#"{"type":"comap"}"#)).unwrap();
        assert!(row_id.is_some());

        let result = storage.get_co_value("co_test").unwrap();
        assert_eq!(result.unwrap().header_json, r#"{"type":"comap"}"#);
    }

    #[test]
    fn test_transaction_range_query() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        // Insert 100 transactions, query range [10, 50]
        // Verify exact results
    }
}
```

## Concurrency Model

### How It Works

```
  Event Loop (main thread)         libuv Thread Pool (UV_THREADPOOL_SIZE, default=4)
  ┌────────────────────────┐       ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
  │ WebSocket msg received │       │ Thread 1 │  │ Thread 2 │  │ Thread 3 │  │ Thread 4 │
  │ ↓                      │       │          │  │          │  │          │  │          │
  │ storage.load("co_abc") │──▶────│ load()   │  │          │  │          │  │          │
  │ ↓ returns Promise      │       │ (reading)│  │          │  │          │  │          │
  │                        │       │          │  │          │  │          │  │          │
  │ WebSocket msg received │       │          │  │          │  │          │  │          │
  │ ↓                      │       │          │  │          │  │          │  │          │
  │ storage.store(msg)     │──▶────│          │  │ store()  │  │          │  │          │
  │ ↓ returns Promise      │       │          │  │ (writing)│  │          │  │          │
  │                        │       │          │  │          │  │          │  │          │
  │ ... processes more WS  │       │          │  │          │  │          │  │          │
  │                        │       │          │  │          │  │          │  │          │
  │ ◀── load() resolved ───│───────│ ✓ done   │  │          │  │          │  │          │
  │ ◀── store() resolved ──│───────│          │  │ ✓ done   │  │          │  │          │
  └────────────────────────┘       └─────────┘  └─────────┘  └─────────┘  └─────────┘
```

### Concurrency Guarantees

- **Reads are concurrent**: Multiple `getCoValue`, `getCoValueSessions`, etc. can run simultaneously on different libuv threads. Fjall uses MVCC internally so reads don't block writes.
- **Writes are serialized by `StorageApiAsync`**: The `StoreQueue` in `StorageApiAsync` ensures only one `storeSingle` runs at a time. This prevents conflicting session updates without needing database-level locking.
- **Atomic multi-key writes**: Operations like `eraseCoValueButKeepTombstone` use `fjall::Batch` for atomicity within a single libuv thread.
- **Thread pool sizing**: The default libuv pool of 4 threads should be sufficient. For high-throughput servers, `UV_THREADPOOL_SIZE` can be increased (up to 1024) via environment variable.

### Comparison with Current SQLite Approach

| Aspect | SQLite (current) | Fjall (new) |
|---|---|---|
| Storage operations | Block event loop | Run on libuv threads |
| WebSocket handling | Paused during storage I/O | Continues in parallel |
| Concurrent reads | N/A (single-threaded) | Yes, via MVCC |
| Write serialization | Implicit (main thread) | `StoreQueue` + fjall internal |
| Crypto + storage overlap | Sequential | Can overlap (different threads) |

## Open Questions / Assumptions

1. **Migration from existing SQLite databases**: This design does NOT include a data migration tool for existing SQLite → fjall. Existing servers would start fresh with fjall storage and re-sync data from peers. A migration tool could be added later if needed.

2. **Durability mode**: Fjall defaults to flushing to OS buffers (not fsync). This matches the current `better-sqlite3` WAL mode behavior. We can expose `PersistMode` configuration later if needed.

3. **Memory configuration**: Fjall's block cache should be configured based on available memory. The default should be reasonable (e.g., 256MB) with an option to configure via environment variable or constructor parameter.

4. **Compaction**: Fjall handles compaction automatically in background threads. This should be transparent to the application, but we should monitor and tune if needed.

5. **Single-process constraint**: Like the current SQLite setup, fjall does not support concurrent access from multiple processes. This matches the existing deployment model (single `jazz-run` process).

6. **Libuv thread pool contention**: The NAPI crypto operations (`SessionMap`, ed25519, etc.) also run on the main thread currently. If we later move those to libuv threads too, we should monitor pool contention and consider increasing `UV_THREADPOOL_SIZE`.

7. **Future optimization — bypass `StoreQueue`**: `StorageApiAsync`'s `StoreQueue` serializes all writes because async SQLite needed it. Since fjall handles concurrent writes safely, a future `StorageApiFjall` could allow concurrent stores for even better throughput. This is out of scope for the initial migration.
