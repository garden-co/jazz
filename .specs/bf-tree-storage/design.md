# Design: BfTree Storage Backend

## Overview

Add a new browser storage backend for Jazz that uses **bf-tree-web** (a WASM-compiled B+ tree backed by OPFS) as an alternative to IndexedDB. The bf-tree runs inside a **dedicated Web Worker**, keeping all storage I/O off the main thread.

The bf-tree WASM bindings are added to the existing **`cojson-core-wasm`** crate (not a separate WASM package). This means a single WASM binary ships both the crypto primitives and the storage engine, avoiding a second WASM module load and simplifying the build pipeline.

The new package — `cojson-storage-bftree` — will implement `DBClientInterfaceAsync` and produce a `StorageAPI` via the existing `StorageApiAsync` wrapper, exactly like `cojson-storage-indexeddb` does today.

## Architecture / Components

```
                        Main Thread                              Worker Thread
┌──────────────────────────────────────────┐    ┌──────────────────────────────────────┐
│                                          │    │                                      │
│  LocalNode                               │    │  BfTreeWorkerBackend                 │
│    │                                     │    │    │                                  │
│    ▼                                     │    │    ▼                                  │
│  StorageApiAsync                         │    │  Key encoding (keys.ts)              │
│    │                                     │    │    │                                  │
│    ▼                                     │    │    ▼                                  │
│  BfTreeClient (DBClientInterfaceAsync)   │    │  cojson-core-wasm (single WASM)      │
│    │                                     │    │    ├─ crypto (existing)               │
│    │  postMessage(request) ──────────────┼────┼──▶ └─ BfTree bindings (new)          │
│    │                                     │    │        │                              │
│    │  ◀────────────────── postMessage(response)        │  insert / read / delete     │
│    │                                     │    │        ▼                              │
│                                          │    │     OPFS (FileSystemSyncAccessHandle) │
└──────────────────────────────────────────┘    └──────────────────────────────────────┘
```

### Components

1. **`cojson-core-wasm`** (extended) — existing crate under `crates/`
   - Add `bf-tree` as a Rust dependency
   - New `src/storage/` module exposing bf-tree operations to JavaScript via `wasm_bindgen`:
     `BfTreeStore` struct with `insert`, `read`, `delete`, `scan` methods, plus
     `open_bftree_opfs` and `create_bftree_memory` factory functions
   - The existing crypto bindings (`SessionMap`, `blake3`, etc.) remain unchanged
   - Single `wasm-pack build` produces one `.wasm` binary containing everything

2. **`cojson-storage-bftree`** — new TypeScript package under `packages/`
   - `src/index.ts` — exports `getBfTreeStorage()` factory
   - `src/client.ts` — `BfTreeClient` implementing `DBClientInterfaceAsync` as an RPC proxy
   - `src/worker.ts` — Worker entry point; imports `cojson-core-wasm`, initializes bf-tree, handles incoming commands
   - `src/workerBackend.ts` — `BfTreeWorkerBackend` with actual data access logic (key encoding, serialization, row ID management)
   - `src/protocol.ts` — shared request/response type definitions for the worker protocol
   - `src/keys.ts` — key encoding utilities (used inside the worker)

## Worker Communication Protocol

### Design

The main thread and worker communicate via structured `postMessage` calls using a request/response pattern. Each request carries a unique `reqId` so the main-thread proxy can match responses to their pending Promises.

```typescript
// src/protocol.ts

/** Every request sent from main thread → worker */
type WorkerRequest = {
  reqId: number;
  method: string;       // maps to a DBClientInterfaceAsync method
  args: unknown[];      // serializable arguments
};

/** Every response sent from worker → main thread */
type WorkerResponse = {
  reqId: number;
  result?: unknown;     // serializable return value
  error?: string;       // error message if the operation failed
};

/** Initialization message sent once on startup */
type WorkerInitRequest = {
  type: "init";
  dbName: string;
  cacheSizeBytes: number;
};

type WorkerInitResponse = {
  type: "ready";
} | {
  type: "error";
  message: string;
};
```

### Lifecycle

1. Main thread creates `new Worker("worker.js")`
2. Main thread sends `{ type: "init", dbName, cacheSizeBytes }`
3. Worker calls `cojson-core-wasm`'s `initialize()` (loads the single WASM binary), then calls `open_bftree_opfs()` to create the tree — responds `{ type: "ready" }`
4. Main thread resolves the `getBfTreeStorage()` Promise — storage is ready
5. All subsequent calls are `WorkerRequest` / `WorkerResponse` pairs
6. On `close()`, the main thread sends a `{ method: "close" }` request and terminates the worker

### Batching Optimization (future)

For high-throughput scenarios (e.g., initial sync), we can batch multiple requests into a single `postMessage` to amortize the structured-clone overhead. The protocol supports this by allowing an array of `WorkerRequest` objects. This is not required for v1 but the protocol design accommodates it.

## Data Models

### Key Schema

bf-tree is a sorted byte key-value store. We map the relational schema (coValues, sessions, transactions, signatures) to a flat key space using **prefixed composite keys** that enable efficient range scans.

All keys are UTF-8 encoded strings converted to bytes. The `|` separator is used between key components.

```
Key Format                                           Value (JSON-encoded)
──────────────────────────────────────────────────── ─────────────────────────
cv|{coValueId}                                       CoValueRow { id, header }
se|{coValueId}|{sessionID}                           SessionRow { lastIdx, lastSignature, bytesSinceLastSignature }
tx|{coValueId}|{sessionID}|{idx:padded}              Transaction (JSON)
si|{coValueId}|{sessionID}|{idx:padded}              Signature (string)
de|{coValueId}                                       DeletedCoValueDeletionStatus (0|1)
us|{coValueId}|{peerId}                              "" (presence key)
```

- `idx:padded` — transaction/signature index zero-padded to 10 digits (e.g., `0000000042`) to preserve sort order in the B+ tree.
- Prefixes (`cv`, `se`, `tx`, `si`, `de`, `us`) ensure logical grouping and efficient prefix scans.

### Row ID Mapping

The `DBClientInterfaceAsync` interface expects `number` row IDs from `upsertCoValue` and `addSessionUpdate`. Since bf-tree uses composite string keys, we maintain an in-memory counter map inside the worker that assigns numeric IDs and tracks the reverse mapping:

- `coValueIdToRowId: Map<string, number>` / `rowIdToCoValueId: Map<number, string>`
- `sessionKeyToRowId: Map<string, number>` / `rowIdToSessionKey: Map<number, string>`

These maps live entirely in the worker thread. The main-thread proxy receives and forwards row IDs as opaque numbers.

On worker restart (page reload), the maps are rebuilt by scanning the `cv|` and `se|` prefixes from the persisted bf-tree data.

### Size Constraints

bf-tree constraints (final Jazz config via `jazz_bftree_config()`):
- Max key length: **256 bytes** (default 16)
- Min record size: **8 bytes** (satisfies `leaf_page_size / min_record_size <= 4096`)
- Max record size (key + value): **15,360 bytes** (limited by leaf page geometry)
- Leaf page size: **32,768 bytes** (maximum allowed by bf-tree)

Jazz data characteristics:
- CoValue IDs: ~50 bytes (e.g., `co_z...`)
- Session IDs: ~80 bytes
- Our longest key: `tx|{coValueId}|{sessionID}|{idx}` ≈ 50 + 80 + 10 + 6 = ~146 bytes (fits in 256)
- Transaction values: typically < 4 KB, can exceed for file chunks → need chunking or config tuning

We'll configure bf-tree with:
```rust
config.cb_max_key_len(256);
config.cb_max_record_size(16384);
config.leaf_page_size(32768);
config.cb_size_byte(32 * 1024 * 1024); // 32 MB cache
```

For values exceeding the max record size, we split them into chunks stored under sequential sub-keys:
```
tx|{coValueId}|{sessionID}|{idx:padded}|c0    → chunk 0
tx|{coValueId}|{sessionID}|{idx:padded}|c1    → chunk 1
```

## Core Implementation

### Key Encoding (runs inside worker)

```typescript
// src/keys.ts
const SEP = "|";

function padIdx(idx: number): string {
  return idx.toString().padStart(10, "0");
}

export const Keys = {
  coValue(id: string): string {
    return `cv${SEP}${id}`;
  },
  session(coValueId: string, sessionID: string): string {
    return `se${SEP}${coValueId}${SEP}${sessionID}`;
  },
  transaction(coValueId: string, sessionID: string, idx: number): string {
    return `tx${SEP}${coValueId}${SEP}${sessionID}${SEP}${padIdx(idx)}`;
  },
  transactionPrefix(coValueId: string, sessionID: string): string {
    return `tx${SEP}${coValueId}${SEP}${sessionID}${SEP}`;
  },
  signature(coValueId: string, sessionID: string, idx: number): string {
    return `si${SEP}${coValueId}${SEP}${sessionID}${SEP}${padIdx(idx)}`;
  },
  signaturePrefix(coValueId: string, sessionID: string): string {
    return `si${SEP}${coValueId}${SEP}${sessionID}${SEP}`;
  },
  deleted(id: string): string {
    return `de${SEP}${id}`;
  },
  deletedPrefix(): string {
    return `de${SEP}`;
  },
  unsynced(coValueId: string, peerId: string): string {
    return `us${SEP}${coValueId}${SEP}${peerId}`;
  },
  unsyncedPrefix(coValueId: string): string {
    return `us${SEP}${coValueId}${SEP}`;
  },
  allUnsyncedPrefix(): string {
    return `us${SEP}`;
  },
} as const;
```

### BfTreeClient — Main Thread RPC Proxy

The client runs on the main thread. Every `DBClientInterfaceAsync` method serializes its arguments into a `WorkerRequest`, sends it via `postMessage`, and returns a Promise that resolves when the worker responds.

```typescript
// src/client.ts
import type {
  DBClientInterfaceAsync,
  DBTransactionInterfaceAsync,
  StoredCoValueRow,
  StoredSessionRow,
  TransactionRow,
  SignatureAfterRow,
} from "cojson";
import type { WorkerRequest, WorkerResponse } from "./protocol.js";

export class BfTreeClient implements DBClientInterfaceAsync {
  private worker: Worker;
  private nextReqId = 0;
  private pending = new Map<number, {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
  }>();

  constructor(worker: Worker) {
    this.worker = worker;
    this.worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      const { reqId, result, error } = event.data;
      const handler = this.pending.get(reqId);
      if (!handler) return;
      this.pending.delete(reqId);
      if (error) {
        handler.reject(new Error(error));
      } else {
        handler.resolve(result);
      }
    };
  }

  private call<T>(method: string, args: unknown[]): Promise<T> {
    const reqId = this.nextReqId++;
    return new Promise<T>((resolve, reject) => {
      this.pending.set(reqId, {
        resolve: resolve as (v: unknown) => void,
        reject,
      });
      this.worker.postMessage({ reqId, method, args } satisfies WorkerRequest);
    });
  }

  getCoValue(coValueId: string) {
    return this.call<StoredCoValueRow | undefined>("getCoValue", [coValueId]);
  }

  upsertCoValue(id: string, header?: CoValueHeader) {
    return this.call<number | undefined>("upsertCoValue", [id, header]);
  }

  getCoValueSessions(coValueRowId: number) {
    return this.call<StoredSessionRow[]>("getCoValueSessions", [coValueRowId]);
  }

  getNewTransactionInSession(sessionRowId: number, fromIdx: number, toIdx: number) {
    return this.call<TransactionRow[]>("getNewTransactionInSession", [sessionRowId, fromIdx, toIdx]);
  }

  getSignatures(sessionRowId: number, firstNewTxIdx: number) {
    return this.call<SignatureAfterRow[]>("getSignatures", [sessionRowId, firstNewTxIdx]);
  }

  /**
   * Transactions are executed entirely inside the worker.
   * We serialize the full transaction callback as a sequence of operations.
   *
   * The "transaction" method sends a `beginTransaction` command; the worker
   * creates a BfTreeTransaction context and executes the callback's operations
   * in sequence, then responds with the final result.
   *
   * In practice, since bf-tree has no multi-key ACID transactions anyway,
   * each sub-operation (getSingleCoValueSession, addSessionUpdate, etc.)
   * is an individual worker call. The `transaction()` wrapper groups them
   * logically on the main thread side.
   */
  async transaction(
    callback: (tx: DBTransactionInterfaceAsync) => Promise<unknown>,
  ): Promise<unknown> {
    // The transaction proxy forwards each tx method as a worker call
    const txProxy: DBTransactionInterfaceAsync = {
      getSingleCoValueSession: (coValueRowId, sessionID) =>
        this.call("tx.getSingleCoValueSession", [coValueRowId, sessionID]),
      markCoValueAsDeleted: (id) =>
        this.call("tx.markCoValueAsDeleted", [id]),
      addSessionUpdate: ({ sessionUpdate, sessionRow }) =>
        this.call("tx.addSessionUpdate", [{ sessionUpdate, sessionRow }]),
      addTransaction: (sessionRowID, idx, newTransaction) =>
        this.call("tx.addTransaction", [sessionRowID, idx, newTransaction]),
      addSignatureAfter: ({ sessionRowID, idx, signature }) =>
        this.call("tx.addSignatureAfter", [{ sessionRowID, idx, signature }]),
      deleteCoValueContent: (coValueRow) =>
        this.call("tx.deleteCoValueContent", [coValueRow]),
    };
    return callback(txProxy);
  }

  getAllCoValuesWaitingForDelete() {
    return this.call<RawCoID[]>("getAllCoValuesWaitingForDelete", []);
  }

  trackCoValuesSyncState(updates: { id: RawCoID; peerId: PeerID; synced: boolean }[]) {
    return this.call<void>("trackCoValuesSyncState", [updates]);
  }

  getUnsyncedCoValueIDs() {
    return this.call<RawCoID[]>("getUnsyncedCoValueIDs", []);
  }

  stopTrackingSyncState(id: RawCoID) {
    return this.call<void>("stopTrackingSyncState", [id]);
  }

  eraseCoValueButKeepTombstone(coValueID: RawCoID) {
    return this.call<void>("eraseCoValueButKeepTombstone", [coValueID]);
  }

  getCoValueKnownState(coValueId: string) {
    return this.call("getCoValueKnownState", [coValueId]);
  }
}
```

### Worker Entry Point

```typescript
// src/worker.ts
import { initialize, open_bftree_opfs } from "cojson-core-wasm";
import { BfTreeWorkerBackend } from "./workerBackend.js";
import type { WorkerRequest, WorkerInitRequest } from "./protocol.js";

let backend: BfTreeWorkerBackend | undefined;

self.onmessage = async (event: MessageEvent<WorkerInitRequest | WorkerRequest>) => {
  const msg = event.data;

  // Handle initialization
  if ("type" in msg && msg.type === "init") {
    try {
      // Load the single cojson-core WASM binary (crypto + storage in one module)
      await initialize();
      // Open bf-tree backed by OPFS (requires Web Worker context)
      const tree = await open_bftree_opfs(msg.dbName, msg.cacheSizeBytes);
      backend = new BfTreeWorkerBackend(tree);
      self.postMessage({ type: "ready" });
    } catch (e) {
      self.postMessage({ type: "error", message: String(e) });
    }
    return;
  }

  // Handle RPC calls
  const req = msg as WorkerRequest;
  if (!backend) {
    self.postMessage({ reqId: req.reqId, error: "Worker not initialized" });
    return;
  }

  try {
    const result = await backend.dispatch(req.method, req.args);
    self.postMessage({ reqId: req.reqId, result });
  } catch (e) {
    self.postMessage({ reqId: req.reqId, error: String(e) });
  }
};
```

### BfTreeWorkerBackend — Data Access Logic (runs inside worker)

This is where the actual bf-tree operations happen. It holds the WASM `BfTree` instance, manages row ID mappings, and implements all the storage methods.

```typescript
// src/workerBackend.ts
import { Keys } from "./keys.js";
import type { BfTreeStore } from "cojson-core-wasm";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export class BfTreeWorkerBackend {
  private tree: BfTreeStore;
  private rowIdCounter = 0;
  private coValueIdToRowId = new Map<string, number>();
  private rowIdToCoValueId = new Map<number, string>();
  private sessionKeyToRowId = new Map<string, number>();
  private rowIdToSessionKey = new Map<number, string>();

  constructor(tree: BfTreeStore) {
    this.tree = tree;
  }

  /** Route an RPC method name to the corresponding implementation */
  async dispatch(method: string, args: unknown[]): Promise<unknown> {
    switch (method) {
      case "getCoValue":
        return this.getCoValue(args[0] as string);
      case "upsertCoValue":
        return this.upsertCoValue(args[0] as string, args[1]);
      case "getCoValueSessions":
        return this.getCoValueSessions(args[0] as number);
      case "getNewTransactionInSession":
        return this.getNewTransactionInSession(args[0] as number, args[1] as number, args[2] as number);
      case "getSignatures":
        return this.getSignatures(args[0] as number, args[1] as number);
      // Transaction sub-operations
      case "tx.getSingleCoValueSession":
        return this.getSingleCoValueSession(args[0] as number, args[1] as string);
      case "tx.addSessionUpdate":
        return this.addSessionUpdate(args[0] as any);
      case "tx.addTransaction":
        return this.addTransaction(args[0] as number, args[1] as number, args[2]);
      case "tx.addSignatureAfter":
        return this.addSignatureAfter(args[0] as any);
      case "tx.markCoValueAsDeleted":
        return this.markCoValueAsDeleted(args[0] as string);
      case "tx.deleteCoValueContent":
        return this.deleteCoValueContent(args[0] as any);
      // Other methods
      case "getAllCoValuesWaitingForDelete":
        return this.getAllCoValuesWaitingForDelete();
      case "trackCoValuesSyncState":
        return this.trackCoValuesSyncState(args[0] as any);
      case "getUnsyncedCoValueIDs":
        return this.getUnsyncedCoValueIDs();
      case "stopTrackingSyncState":
        return this.stopTrackingSyncState(args[0] as string);
      case "eraseCoValueButKeepTombstone":
        return this.eraseCoValueButKeepTombstone(args[0] as string);
      case "getCoValueKnownState":
        return this.getCoValueKnownState(args[0] as string);
      default:
        throw new Error(`Unknown method: ${method}`);
    }
  }

  // --- Low-level bf-tree access ---

  private put(key: string, value: unknown): void {
    this.tree.insert(encoder.encode(key), encoder.encode(JSON.stringify(value)));
  }

  private get<T>(key: string): T | undefined {
    const result = this.tree.read(encoder.encode(key));
    if (!result) return undefined;
    return JSON.parse(decoder.decode(result)) as T;
  }

  private del(key: string): void {
    this.tree.delete(encoder.encode(key));
  }

  // --- Row ID management ---

  private assignCoValueRowId(id: string): number {
    let rowId = this.coValueIdToRowId.get(id);
    if (rowId !== undefined) return rowId;
    rowId = ++this.rowIdCounter;
    this.coValueIdToRowId.set(id, rowId);
    this.rowIdToCoValueId.set(rowId, id);
    return rowId;
  }

  private assignSessionRowId(coValueId: string, sessionID: string): number {
    const key = `${coValueId}|${sessionID}`;
    let rowId = this.sessionKeyToRowId.get(key);
    if (rowId !== undefined) return rowId;
    rowId = ++this.rowIdCounter;
    this.sessionKeyToRowId.set(key, rowId);
    this.rowIdToSessionKey.set(rowId, key);
    return rowId;
  }

  // --- DBClientInterfaceAsync methods ---

  getCoValue(coValueId: string) {
    const data = this.get<{ id: string; header: unknown }>(Keys.coValue(coValueId));
    if (!data) return undefined;
    return { ...data, rowID: this.assignCoValueRowId(coValueId) };
  }

  upsertCoValue(id: string, header?: unknown) {
    const existing = this.get(Keys.coValue(id));
    if (existing) return this.assignCoValueRowId(id);
    if (!header) return undefined;
    this.put(Keys.coValue(id), { id, header });
    return this.assignCoValueRowId(id);
  }

  getCoValueSessions(coValueRowId: number) {
    const coValueId = this.rowIdToCoValueId.get(coValueRowId);
    if (!coValueId) return [];
    // Use bf-tree's range scan to find all se|{coValueId}|* keys
    // (implementation relies on sorted key order + prefix matching)
    return this.scanSessions(coValueId, coValueRowId);
  }

  getNewTransactionInSession(sessionRowId: number, fromIdx: number, toIdx: number) {
    const sessionKey = this.rowIdToSessionKey.get(sessionRowId);
    if (!sessionKey) return [];
    const [coValueId, sessionID] = this.splitSessionKey(sessionKey);
    const results = [];
    for (let idx = fromIdx; idx <= toIdx; idx++) {
      const tx = this.get(Keys.transaction(coValueId, sessionID, idx));
      if (tx) results.push({ ses: sessionRowId, idx, tx });
    }
    return results;
  }

  // ... (remaining method implementations follow the same pattern)
}
```

### Factory Function

```typescript
// src/index.ts
import { StorageApiAsync } from "cojson";
import { BfTreeClient } from "./client.js";

export async function getBfTreeStorage(
  dbName = "jazz-bftree.db",
  cacheSizeBytes = 32 * 1024 * 1024,
): Promise<StorageAPI> {
  // The worker script is bundled alongside this package.
  // It imports cojson-core-wasm internally — no separate WASM package needed.
  const worker = new Worker(
    new URL("./worker.js", import.meta.url),
    { type: "module" },
  );

  // Wait for the worker to initialize cojson-core-wasm + open bf-tree on OPFS
  await new Promise<void>((resolve, reject) => {
    worker.onmessage = (event) => {
      if (event.data.type === "ready") resolve();
      else if (event.data.type === "error") reject(new Error(event.data.message));
    };
    worker.postMessage({ type: "init", dbName, cacheSizeBytes });
  });

  const client = new BfTreeClient(worker);
  return new StorageApiAsync(client);
}
```

### Usage (same pattern as IndexedDB)

```typescript
import { getBfTreeStorage } from "cojson-storage-bftree";

const node = new LocalNode(/* ... */);
node.setStorage(await getBfTreeStorage());
```

## WASM Bindings: Extending `cojson-core-wasm`

Rather than creating a separate WASM package for bf-tree, we extend the existing `cojson-core-wasm` crate. This produces a **single `.wasm` binary** that includes both the crypto primitives (ed25519, xsalsa20, blake3, etc.) and the bf-tree storage engine.

### Why a single WASM module

- **One load, one init**: the worker calls `initialize()` once and gets access to both crypto and storage — no second WASM fetch/compile
- **Simpler build pipeline**: `wasm-pack build` in `cojson-core-wasm` is the only WASM build step; no second pipeline to maintain
- **Smaller total size**: WASM modules share common dependencies (serde, getrandom, etc.) — a single binary deduplicates them
- **Existing infrastructure**: `cojson-core-wasm` already has `build.js`, `index.js`/`index.d.ts`, `edge-lite.js`, base64-inlined wasm for bundler compat — bf-tree gets all of this for free

### Rust changes to `cojson-core-wasm`

```rust
// crates/cojson-core-wasm/src/storage/mod.rs (new module)
use bf_tree::{BfTree as BfTreeInner, Config, LeafInsertResult, LeafReadResult};
use wasm_bindgen::prelude::*;

/// WASM wrapper around bf-tree for use from JavaScript.
#[wasm_bindgen]
pub struct BfTreeStore {
    inner: BfTreeInner,
}

#[wasm_bindgen]
impl BfTreeStore {
    pub fn insert(&self, key: &[u8], value: &[u8]) -> bool {
        matches!(self.inner.insert(key, value), LeafInsertResult::Success)
    }

    pub fn read(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut buf = vec![0u8; 65536];
        match self.inner.read(key, &mut buf) {
            LeafReadResult::Found(len) => { buf.truncate(len as usize); Some(buf) }
            _ => None,
        }
    }

    pub fn delete(&self, key: &[u8]) {
        self.inner.delete(key);
    }

    // Prefix scan for range queries (sessions, transactions, signatures)
    pub fn scan(&self, prefix: &[u8], limit: u32) -> Vec<JsValue> {
        // Uses bf-tree's ScanIter to collect matching key-value pairs
        // Returns array of [key, value] pairs as JsValue
        todo!("wire ScanIter to WASM")
    }
}

/// Open a bf-tree backed by OPFS. Must be called from a Web Worker.
#[wasm_bindgen]
pub async fn open_bftree_opfs(
    db_name: &str,
    cache_size_bytes: usize,
) -> Result<BfTreeStore, JsValue> {
    let opfs_vfs = bf_tree::OpfsVfs::open(db_name).await?;
    let mut config = Config::default();
    config.cb_max_key_len(256);
    config.cb_max_record_size(16384);
    config.leaf_page_size(32768);
    config.cb_size_byte(cache_size_bytes);
    let inner = BfTreeInner::with_opfs_vfs(opfs_vfs, cache_size_bytes)
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
    Ok(BfTreeStore { inner })
}

/// Create an in-memory bf-tree (no persistence, for testing).
#[wasm_bindgen]
pub fn create_bftree_memory(cache_size_bytes: usize) -> Result<BfTreeStore, JsValue> {
    let inner = BfTreeInner::new(":memory:", cache_size_bytes)
        .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
    Ok(BfTreeStore { inner })
}
```

### Cargo.toml change

```toml
# crates/cojson-core-wasm/Cargo.toml — add bf-tree dependency
[dependencies]
bf-tree = { path = "../bf-tree-web" }
```

### TypeScript exports

The new `BfTreeStore`, `open_bftree_opfs`, and `create_bftree_memory` are automatically available in the generated `cojson_core_wasm.js` / `.d.ts` files after `wasm-pack build`. They're re-exported through the existing `index.js`:

```typescript
// cojson-core-wasm/index.js (existing, no change needed)
export * from "./public/cojson_core_wasm.js";
// ↑ This already re-exports everything from the wasm-pack output,
//   so BfTreeStore, open_bftree_opfs, create_bftree_memory are included.
```

## Key Design Decisions

### 1. Extend `cojson-core-wasm` — single WASM binary

bf-tree bindings are added to the existing `cojson-core-wasm` crate rather than creating a separate WASM package. This produces one `.wasm` file containing both crypto and storage, avoiding double WASM loads and simplifying the build pipeline.

### 2. Dedicated Worker Thread

The bf-tree WASM module runs entirely inside a dedicated Web Worker. This is both a **requirement** (OPFS sync access is Worker-only) and a **performance win** (storage I/O never blocks the main thread). The main thread holds a thin RPC proxy (`BfTreeClient`) that communicates with the worker via `postMessage`.

### 3. Reuse `StorageApiAsync` — don't reimplement `StorageAPI`

The existing `StorageApiAsync` class handles all the complex logic: known state management, streaming queues, correction handling, deletion scheduling. We only need to implement `DBClientInterfaceAsync`, which is a much simpler CRUD interface. `StorageApiAsync` lives on the main thread; `BfTreeClient` (also main thread) proxies calls to the worker.

### 4. Flat key-value mapping instead of relational tables

bf-tree is a sorted key-value store, not a relational database. Rather than emulating IndexedDB's object stores and indexes, we use a flat key space with prefixed composite keys. This:
- Leverages bf-tree's sorted iteration for prefix scans
- Eliminates the need for secondary indexes
- Keeps the mapping layer thin

### 5. Transactions are "best-effort"

bf-tree doesn't provide multi-key ACID transactions. Individual key writes are atomic, but a batch of writes (e.g., storing a session + its transactions) is not. This is acceptable because:
- Jazz's CRDT model is append-only and idempotent — partial writes can be corrected on the next sync
- `StorageApiAsync` already handles corrections via the `correctionCallback` mechanism
- IndexedDB transactions in practice rarely roll back in the Jazz codebase

### 6. Row ID mapping lives in the worker

The `DBClientInterfaceAsync` interface uses numeric row IDs as foreign keys. We maintain the mapping (`string key ↔ number rowId`) entirely in the worker thread. The main thread treats row IDs as opaque numbers. On page reload, the worker rebuilds the mapping by scanning persisted `cv|` and `se|` prefixes.

### 7. Worker bundling

The worker script needs to be bundled and loadable via `new URL("./worker.js", import.meta.url)`. This pattern is supported by Vite, Webpack 5, and other modern bundlers. We'll also provide a pre-built worker bundle in the package for frameworks that don't handle worker bundling well.

## Testing Strategy

We prioritize integration testing by reusing the existing storage test suite from `cojson-storage-indexeddb`, since both backends must satisfy the same `StorageAPI` contract.

### Integration Test: Store and Load CoValue

```typescript
import { describe, it, expect } from "vitest";
import { getBfTreeStorage } from "../index.js";

describe("BfTree Storage", () => {
  it("should store and load a CoValue round-trip", async () => {
    const { node1, node2, connectToSyncServer, syncServer } = setupTestNodes();

    node1.setStorage(await getBfTreeStorage("test.db"));

    const group = node1.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Sync to server, then load on a second node
    connectToSyncServer(node1, syncServer);
    connectToSyncServer(node2, syncServer);

    node2.setStorage(await getBfTreeStorage("test2.db"));

    const map2 = await node2.load(map.id);
    expect(map2).not.toBe("unavailable");
    expect(map2.get("key")).toBe("value");
  });
});
```

### Integration Test: Worker Lifecycle

```typescript
it("should initialize and close the worker cleanly", async () => {
  const storage = await getBfTreeStorage("lifecycle-test.db");

  // Storage should be functional
  const node = createTestNode();
  node.setStorage(storage);

  const group = node.createGroup();
  const map = group.createMap();
  map.set("hello", "world");

  // Close should terminate the worker without errors
  await storage.close();
});
```
