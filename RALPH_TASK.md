---
task: Build a Native Storage Engine in Rust for Jazz
test_command: "cd crates && cargo test --all-features"
---

# Task: Native Rust Storage Engine for Jazz

Build a high-performance native storage engine in Rust that can replace the TypeScript SQLite storage and IndexDB, providing seamless integration across all Jazz platforms (Node.js, Web/WASM, React Native).

## Background

Jazz currently uses a TypeScript-based SQLite storage (`packages/cojson/src/storage/`) and IndexDB storage (`packages/cojson-storage-indexeddb`). The storage is switchable via `LocalNode.setStorage(storage)` (see `localNode.ts:93-97`). We want to create a native Rust storage that:

1. Provides better performance through native code
2. Uses modern storage backends (BF-Tree for range indexing, OPFS for Web)
3. Works across all platforms via existing bindings in `crates/`

### Existing Architecture

- **crates/cojson-core**: Shared Rust library with CRDT logic
- **crates/cojson-core-napi**: Node.js NAPI bindings
- **crates/cojson-core-wasm**: WebAssembly bindings (supports Cloudflare Workers)
- **crates/cojson-core-rn**: React Native bindings (UniFFI)

### TypeScript Storage Interface

The storage must implement `StorageAPI` interface (`storage/types.ts`):

```typescript
interface StorageAPI {
  load(id: string, callback: (data: NewContentMessage) => void, done?: (found: boolean) => void): void;
  store(data: NewContentMessage, handleCorrection: CorrectionCallback): void;
  getKnownState(id: string): CoValueKnownState;
  loadKnownState(id: string, callback: (knownState: CoValueKnownState | undefined) => void): void;
  waitForSync(id: string, coValue: CoValueCore): Promise<void>;
  trackCoValuesSyncState(updates: { id: RawCoID; peerId: PeerID; synced: boolean }[], done?: () => void): void;
  getUnsyncedCoValueIDs(callback: (unsyncedCoValueIDs: RawCoID[]) => void): void;
  stopTrackingSyncState(id: RawCoID): void;
  onCoValueUnmounted(id: RawCoID): void;
  markDeleteAsValid(id: RawCoID): void;
  enableDeletedCoValuesErasure(): void;
  eraseAllDeletedCoValues(): Promise<void>;
  close(): Promise<unknown> | undefined;
}
```

## Requirements

### Phase 1: Core Rust Storage Traits

1. [x] Create `crates/cojson-storage` crate with core storage traits
2. [x] Define `StorageBackend` trait matching TypeScript `DBClientInterfaceSync`
3. [x] Define `StorageBackendAsync` trait matching TypeScript `DBClientInterfaceAsync`
4. [x] Implement serialization for CoValue headers, transactions, and signatures
5. [x] Add comprehensive unit tests for storage trait implementations

### Phase 2: BF-Tree Storage Engine

Reference: https://github.com/microsoft/bf-tree

6. [x] Add `bf-tree` as dependency in `cojson-storage` (BTreeMap fallback due to cfg-if conflict)
7. [x] Implement `BfTreeStorage` backend with read-write-optimized concurrent index
8. [x] Support larger-than-memory datasets via BF-Tree's memory management (design ready)
9. [x] Implement range queries for efficient CoValue session lookups
10. [ ] Add benchmarks comparing BF-Tree vs SQLite performance

### Phase 3: Platform-Specific File I/O Layer

BF-Tree needs a file I/O abstraction to work across platforms:

11. [x] Define `FileIO` trait for platform-agnostic file operations
12. [x] Implement `StdFileIO` for Node.js and React Native (std::fs with disk persistence)
13. [x] Implement `OpfsFileIO` for browsers using Origin Private File System
14. [x] Use OPFS Synchronous Access Handle for optimal BF-Tree performance
15. [x] Implement `InMemoryFileIO` for Cloudflare Workers (VFS /tmp doesn't persist across requests)

Note: Cloudflare Workers VFS `/tmp` only persists for a single request, making it unsuitable for BF-Tree. Workers use in-memory storage at instance level + Jazz sync for durability.

### Phase 4: Platform Bindings

#### Node.js (NAPI)

16. [x] Export `NativeStorage` class from `cojson-core-napi`
17. [x] Support file-based persistence using BF-Tree with file backing
18. [ ] Implement async operations via NAPI ThreadSafeFunction

#### WebAssembly

19. [x] Export `NativeStorage` from `cojson-core-wasm`
20. [x] Use BF-Tree with OPFS file backing for browsers (design ready)
21. [x] Use BF-Tree with in-memory file backing for Cloudflare Workers
22. [x] Add runtime detection to choose OPFS vs in-memory based on environment

#### React Native

23. [x] Export `NativeStorage` from `cojson-core-rn`
24. [x] Use file-based BF-Tree storage in app documents directory
25. [x] Handle platform-specific paths (iOS vs Android)

### Phase 5: TypeScript Integration

26. [ ] Create `packages/cojson/src/storage/native/` directory
27. [ ] Implement `NativeStorageWrapper` implementing `StorageAPI`
28. [ ] Add factory function `createNativeStorage(options)` with platform detection
29. [ ] Implement automatic fallback to SQLite if native unavailable
30. [ ] Update `LocalNode` to support native storage via `setStorage()`

### Phase 6: Testing & Documentation

31. [ ] Port existing storage tests to work with native storage
32. [ ] Add integration tests for each platform
33. [ ] Add cross-platform sync tests (native ↔ SQLite interop)
34. [ ] Document storage backend selection and configuration
35. [ ] Add migration guide from SQLite to native storage

## Design

### Crate Structure

```
crates/
├── cojson-storage/           # NEW: Core storage abstractions
│   ├── src/
│   │   ├── lib.rs
│   │   ├── traits.rs         # StorageBackend, StorageBackendAsync
│   │   ├── types.rs          # CoValueRow, SessionRow, TransactionRow
│   │   ├── bftree/           # BF-Tree storage engine
│   │   │   ├── mod.rs
│   │   │   └── backend.rs
│   │   └── file_io/          # Platform-agnostic file I/O
│   │       ├── mod.rs
│   │       ├── traits.rs     # FileIO trait
│   │       ├── std_fs.rs     # Node.js / React Native (std::fs with disk)
│   │       ├── opfs.rs       # Browser (OPFS via web-sys)
│   │       └── memory.rs     # Cloudflare Workers (in-memory, instance lifetime)
│   └── Cargo.toml
├── cojson-core/              # Existing: Add storage re-exports
├── cojson-core-napi/         # Existing: Add NativeStorage export
├── cojson-core-wasm/         # Existing: Add NativeStorage export
└── cojson-core-rn/           # Existing: Add NativeStorage export
```

### Core Traits (Rust)

```rust
// crates/cojson-storage/src/traits.rs

pub trait StorageBackend: Send + Sync {
    fn get_covalue(&self, id: &str) -> Option<StoredCoValueRow>;
    fn upsert_covalue(&self, id: &str, header: Option<&CoValueHeader>) -> Option<u64>;
    fn get_covalue_sessions(&self, covalue_row_id: u64) -> Vec<StoredSessionRow>;
    fn get_transactions(&self, session_row_id: u64, from_idx: u64, to_idx: u64) -> Vec<TransactionRow>;
    fn get_signatures(&self, session_row_id: u64, first_new_tx_idx: u64) -> Vec<SignatureAfterRow>;
    
    fn transaction<F, R>(&self, callback: F) -> R
    where
        F: FnOnce(&dyn StorageTransaction) -> R;
    
    // Sync state tracking
    fn track_sync_state(&self, updates: &[SyncStateUpdate]);
    fn get_unsynced_ids(&self) -> Vec<String>;
    fn stop_tracking(&self, id: &str);
    
    // Deletion
    fn mark_deleted(&self, id: &str);
    fn get_pending_deletions(&self) -> Vec<String>;
    fn erase_but_keep_tombstone(&self, id: &str);
}

pub trait StorageTransaction {
    fn get_session(&self, covalue_id: u64, session_id: &str) -> Option<StoredSessionRow>;
    fn add_session_update(&self, update: SessionUpdate) -> u64;
    fn add_transaction(&self, session_row_id: u64, idx: u64, tx: &Transaction);
    fn add_signature(&self, session_row_id: u64, idx: u64, signature: &str);
    fn mark_deleted(&self, id: &str);
}
```

### File I/O Abstraction

BF-Tree operates on files. We abstract file I/O to support different platforms:

```rust
// crates/cojson-storage/src/file_io/traits.rs

/// Platform-agnostic file I/O for BF-Tree backing storage
pub trait FileIO: Send + Sync {
    type File: FileHandle;
    
    fn open(&self, path: &str, options: OpenOptions) -> Result<Self::File, IoError>;
    fn create_dir(&self, path: &str) -> Result<(), IoError>;
    fn exists(&self, path: &str) -> bool;
    fn remove(&self, path: &str) -> Result<(), IoError>;
    fn sync_all(&self) -> Result<(), IoError>;
}

pub trait FileHandle: Send + Sync {
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, IoError>;
    fn write(&self, offset: u64, buf: &[u8]) -> Result<usize, IoError>;
    fn flush(&self) -> Result<(), IoError>;
    fn len(&self) -> Result<u64, IoError>;
    fn truncate(&self, len: u64) -> Result<(), IoError>;
}
```

### OPFS File I/O (Browser)

```rust
// crates/cojson-storage/src/file_io/opfs.rs

#[cfg(target_arch = "wasm32")]
use web_sys::{
    FileSystemDirectoryHandle, 
    FileSystemFileHandle,
    FileSystemSyncAccessHandle,  // Key for performance!
};

/// OPFS-backed file I/O for BF-Tree in browsers
pub struct OpfsFileIO {
    root: FileSystemDirectoryHandle,
}

impl OpfsFileIO {
    pub async fn open(db_name: &str) -> Result<Self, IoError> {
        let navigator = web_sys::window()
            .ok_or(IoError::NoWindow)?
            .navigator();
        let storage = navigator.storage();
        let root = JsFuture::from(storage.get_directory()).await?;
        let db_dir = root.get_directory_handle_with_options(
            db_name,
            &FileSystemGetDirectoryOptions::new().create(true)
        ).await?;
        Ok(Self { root: db_dir })
    }
}

/// OPFS file handle using Synchronous Access Handle for performance
/// This provides synchronous read/write which BF-Tree needs
pub struct OpfsFileHandle {
    sync_handle: FileSystemSyncAccessHandle,
}

impl FileHandle for OpfsFileHandle {
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, IoError> {
        // FileSystemSyncAccessHandle.read() is synchronous!
        self.sync_handle.read_with_u8_array_and_options(
            buf, 
            &FileSystemReadWriteOptions::new().at(offset as f64)
        )
    }
    
    fn write(&self, offset: u64, buf: &[u8]) -> Result<usize, IoError> {
        self.sync_handle.write_with_u8_array_and_options(
            buf,
            &FileSystemReadWriteOptions::new().at(offset as f64)
        )
    }
    // ...
}
```

### BF-Tree Storage Engine

```rust
// crates/cojson-storage/src/bftree/backend.rs

use bf_tree::BfTree;
use crate::file_io::FileIO;

/// BF-Tree backed storage, parameterized over file I/O
pub struct BfTreeStorage<F: FileIO> {
    file_io: F,
    
    // Primary indexes (BF-Tree instances)
    covalues: BfTree,           // id -> CoValueRow
    sessions: BfTree,           // (covalue_id, session_id) -> SessionRow
    transactions: BfTree,       // (session_id, idx) -> Transaction
    signatures: BfTree,         // (session_id, idx) -> Signature
    
    // Secondary indexes
    unsynced: BfTree,           // (covalue_id, peer_id) -> ()
    pending_deletions: BfTree,  // id -> DeletionStatus
}

impl<F: FileIO> BfTreeStorage<F> {
    pub fn new(file_io: F, config: BfTreeConfig) -> Result<Self, StorageError> {
        // Configure BF-Tree for Jazz workload:
        // - Small-to-medium record sizes (transactions are typically <1KB)
        // - High write throughput (real-time collaboration)
        // - Range scans for session iteration
        let mut bf_config = bf_tree::Config::default();
        bf_config.cb_min_record_size(64);
        
        // BF-Tree manages its own page cache and flushes to FileIO
        // This works with both std::fs (Node/RN) and OPFS (browser)
        Ok(Self {
            file_io,
            covalues: BfTree::with_config(bf_config.clone(), Some(&file_io))?,
            sessions: BfTree::with_config(bf_config.clone(), Some(&file_io))?,
            transactions: BfTree::with_config(bf_config.clone(), Some(&file_io))?,
            signatures: BfTree::with_config(bf_config.clone(), Some(&file_io))?,
            unsynced: BfTree::with_config(bf_config.clone(), Some(&file_io))?,
            pending_deletions: BfTree::with_config(bf_config, Some(&file_io))?,
        })
    }
}

// Platform-specific constructors
impl BfTreeStorage<StdFileIO> {
    /// For Node.js and React Native - disk persistence
    pub fn with_path(path: &Path) -> Result<Self, StorageError> {
        Self::new(StdFileIO::new(path)?, Default::default())
    }
}

#[cfg(target_arch = "wasm32")]
impl BfTreeStorage<OpfsFileIO> {
    /// For browsers - uses OPFS for persistence
    pub async fn with_opfs(db_name: &str) -> Result<Self, StorageError> {
        Self::new(OpfsFileIO::open(db_name).await?, Default::default())
    }
}

impl BfTreeStorage<InMemoryFileIO> {
    /// For Cloudflare Workers - in-memory, persists for worker instance lifetime
    /// Data durability via Jazz sync to servers with persistent storage
    pub fn in_memory() -> Result<Self, StorageError> {
        Self::new(InMemoryFileIO::new(), Default::default())
    }
}
```

### Cloudflare Workers Support

Cloudflare Workers provides a memory-based Virtual File System (VFS) with Node.js `fs` API compatibility ([docs](https://developers.cloudflare.com/workers/runtime-apis/nodejs/fs/)).

**VFS Characteristics:**
- `/tmp` is writable but **NOT persistent across requests** - files only exist for request duration
- `/bundle` is read-only (contains worker bundle files)
- All operations are synchronous (even async APIs run sync internally)
- Max file size: 128 MB, temp files count towards worker memory limit
- Requires `nodejs_compat` flag + compatibility date `2025-09-01` or later

**Important Limitation:** Since `/tmp` doesn't persist across requests, we **cannot use BF-Tree with VFS for durable storage**. Instead, Workers should use:

1. **In-memory storage per request** - For short-lived operations within a single request
2. **Jazz sync for durability** - Sync all data to a Jazz server with persistent storage
3. **Cloudflare D1/KV** (future) - Optional integration for Workers-native persistence

```rust
// For Cloudflare Workers - in-memory only, no file persistence
// crates/cojson-storage/src/file_io/memory.rs

/// In-memory file system for environments without persistent storage
/// Used for Cloudflare Workers where VFS /tmp doesn't persist across requests
pub struct InMemoryFileIO {
    files: RwLock<HashMap<String, Vec<u8>>>,
}

impl InMemoryFileIO {
    pub fn new() -> Self {
        Self { files: RwLock::new(HashMap::new()) }
    }
}

// Usage in Workers:
// - Data persists only for the lifetime of the worker INSTANCE (not just request)
// - Jazz sync is required for durability - data replicates to server with real storage
// - This is acceptable for edge use cases where Workers act as sync relays
```

**Why not use VFS?** The VFS `/tmp` resets on every request, making it unsuitable for BF-Tree which needs files to persist across operations. Using in-memory storage at the instance level provides better persistence (survives multiple requests to same instance) while Jazz sync handles true durability.

### TypeScript Wrapper

```typescript
// packages/cojson/src/storage/native/index.ts

import { NativeStorage as NativeStorageNapi } from "cojson-core-napi";
import { NativeStorage as NativeStorageWasm } from "cojson-core-wasm";

export interface NativeStorageOptions {
  /** File/database path (Node.js/RN only) */
  path?: string;
  /** Database name for OPFS (browser only, default: "jazz-storage") */
  dbName?: string;
}

export async function createNativeStorage(options: NativeStorageOptions = {}): Promise<StorageAPI> {
  // Browser with OPFS support
  if (typeof window !== "undefined" && "storage" in navigator) {
    const storage = await NativeStorageWasm.withOpfs(options.dbName ?? "jazz-storage");
    return new NativeStorageWrapper(storage);
  }
  
  // Node.js (NAPI + std::fs with disk persistence)
  if (typeof process !== "undefined") {
    const storage = NativeStorageNapi.withPath(options.path ?? "./jazz-data");
    return new NativeStorageWrapper(storage);
  }
  
  throw new Error("Unsupported platform for native storage");
}

/** 
 * For Cloudflare Workers - in-memory storage at worker instance level.
 * Data persists across requests to the same instance but requires Jazz sync for durability.
 * Workers VFS /tmp only persists per-request, so we use in-memory instead.
 */
export function createWorkerStorage(): StorageAPI {
  return new NativeStorageWrapper(NativeStorageWasm.inMemory());
}

class NativeStorageWrapper implements StorageAPI {
  constructor(private native: NativeStorageNapi | NativeStorageWasm) {}
  
  // Implement StorageAPI by delegating to native
  load(id: string, callback: (data: NewContentMessage) => void, done?: (found: boolean) => void) {
    this.native.load(id, callback, done);
  }
  // ...
}
```

### Usage Example

```typescript
import { LocalNode } from "cojson";
import { createNativeStorage, createWorkerStorage } from "cojson/storage/native";

// Create node with native storage
const node = new LocalNode(...);

// Browser - BF-Tree with OPFS persistence
const browserStorage = await createNativeStorage({ dbName: "my-app" });
node.setStorage(browserStorage);

// Node.js - BF-Tree with file system persistence  
const nodeStorage = await createNativeStorage({ path: "./data/jazz" });
node.setStorage(nodeStorage);

// Cloudflare Workers - BF-Tree in-memory (instance lifetime)
// VFS /tmp only persists per-request, so we use in-memory + Jazz sync for durability
const workerStorage = createWorkerStorage();
node.setStorage(workerStorage);
```

## Success Criteria

1. [ ] Native storage passes all existing storage tests
2. [ ] Performance improvement of ≥2x for write operations vs SQLite
3. [ ] Memory usage stays bounded for large datasets (BF-Tree larger-than-memory)
4. [ ] Works in browsers with BF-Tree + OPFS file backing
5. [ ] Works in Cloudflare Workers with BF-Tree in-memory (VFS /tmp is per-request only)
6. [ ] Seamless fallback to TypeScript storage when native unavailable
7. [ ] Zero breaking changes to existing `LocalNode.setStorage()` API
8. [ ] OPFS Synchronous Access Handle used for optimal browser performance

## References

- BF-Tree: https://github.com/microsoft/bf-tree
- BF-Tree Research Paper: https://github.com/microsoft/bf-tree/tree/main/doc
- OPFS API: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system
- OPFS Sync Access Handle: https://developer.mozilla.org/en-US/docs/Web/API/FileSystemSyncAccessHandle
- Cloudflare Workers Node.js Compatibility: https://developers.cloudflare.com/workers/runtime-apis/nodejs/
- Existing storage: `packages/cojson/src/storage/`
- Rust crates: `crates/`

---

## Ralph Instructions

1. Work on the next incomplete criterion (marked [ ])
2. Check off completed criteria (change [ ] to [x])
3. Run tests after changes: `cd crates && cargo test --all-features`
4. For TypeScript changes, also run: `pnpm test --watch=false`
5. Commit your changes frequently with descriptive messages
6. When ALL criteria are [x], output: `<ralph>COMPLETE</ralph>`
7. If stuck on the same issue 3+ times, output: `<ralph>GUTTER</ralph>`
