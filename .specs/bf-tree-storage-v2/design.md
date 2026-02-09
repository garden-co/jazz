# Design: BfTree Storage v2 — StorageAPI as the Worker Boundary

## Overview

Redesign the `cojson-storage-bftree` worker communication layer so that the **`StorageAPI` interface** is the boundary between the main thread and the worker, instead of the lower-level `DBClientInterfaceAsync`.

### Problem with v1

In the current design, `StorageApiAsync` lives on the main thread and wraps a `BfTreeClient` (implementing `DBClientInterfaceAsync`). Every DB method call is a separate `postMessage` round-trip:

```
Main Thread                              Worker Thread
┌─────────────────────────────────┐    ┌──────────────────────────────┐
│ StorageApiAsync                 │    │ BfTreeWorkerBackend          │
│   ↓                             │    │                              │
│ BfTreeClient                    │    │                              │
│   (DBClientInterfaceAsync)      │    │                              │
│                                 │    │                              │
│   For a single store():         │    │                              │
│     1. upsertCoValue ──────────►│    │ dispatch("upsertCoValue")    │
│     ◄────── response ──────────┤    │                              │
│     2. tx.getSingleSession ────►│    │ dispatch("tx.getSession")    │
│     ◄────── response ──────────┤    │                              │
│     3. tx.addSessionUpdate ────►│    │ dispatch("tx.addSession")    │
│     ◄────── response ──────────┤    │                              │
│     4. tx.addTransaction ──────►│    │ dispatch("tx.addTx")  ×N    │
│     ◄────── response ──────────┤    │                              │
│     5. tx.addSignatureAfter ───►│    │ dispatch("tx.addSig")       │
│     ◄────── response ──────────┤    │                              │
│     (5-10+ round-trips)        │    │                              │
└─────────────────────────────────┘    └──────────────────────────────┘
```

A single `store()` call generates **5–10+ postMessage round-trips**, each with structured-clone serialization. A `load()` similarly generates many round-trips to fetch the CoValue, its sessions, transactions, and signatures.

### New Design: StorageAPI as the boundary

Move the heavy lifting into the worker. The worker's `BfTreeWorkerBackend` gets two new high-level methods (`storeContent` and `loadContent`) that combine all DB operations into a single synchronous call. The main thread has a thin `BfTreeStorageProxy` implementing `StorageAPI`.

```
Main Thread                              Worker Thread
┌─────────────────────────────────┐    ┌──────────────────────────────┐
│ BfTreeStorageProxy              │    │ BfTreeWorkerBackend          │
│   (implements StorageAPI)       │    │   (enhanced with high-level  │
│                                 │    │    storeContent/loadContent) │
│   For a single store():        │    │                              │
│     1. storeContent(msg) ──────►│    │ Runs ALL DB ops locally:    │
│                                 │    │   upsert + sessions + txs   │
│     ◄──── { knownState } ──────┤    │   + signatures              │
│     (1 round-trip!)             │    │   Returns knownState        │
│                                 │    │                              │
│   Store queue + corrections    │    │                              │
│   Known state cache             │    │                              │
│   Streaming queue               │    │                              │
└─────────────────────────────────┘    └──────────────────────────────┘
```

**Result: 1 postMessage round-trip per `store()` or `load()` instead of 5–10+.**

## Architecture / Components

```
                        Main Thread                              Worker Thread
┌──────────────────────────────────────────┐    ┌──────────────────────────────────────┐
│                                          │    │                                      │
│  LocalNode                               │    │  BfTreeWorkerBackend (enhanced)      │
│    │                                     │    │    │                                  │
│    ▼                                     │    │    ├─ storeContent(msg) → knownState  │
│  BfTreeStorageProxy (StorageAPI)         │    │    ├─ loadContent(id)  → messages[]   │
│    │                                     │    │    ├─ loadKnownState(id) → knownState │
│    ├── knownStates (StorageKnownState)   │    │    ├─ markDeleteAsValid(id)           │
│    ├── storeQueue (StoreQueue)           │    │    ├─ eraseAllDeletedCoValues()       │
│    ├── inMemoryCoValues (Set)            │    │    ├─ trackCoValuesSyncState(updates) │
│    ├── deletedValues (Set)               │    │    ├─ getUnsyncedCoValueIDs()         │
│    │                                     │    │    ├─ stopTrackingSyncState(id)        │
│    │  postMessage(request) ──────────────┼────┼──▶ └─ close()                         │
│    │                                     │    │                                       │
│    │  ◀────────────────── postMessage(response)   bf-tree WASM                       │
│    │                                     │    │     │                                 │
│                                          │    │     ▼                                 │
│                                          │    │  OPFS (FileSystemSyncAccessHandle)    │
└──────────────────────────────────────────┘    └──────────────────────────────────────┘
```

### Components

1. **`BfTreeWorkerBackend`** (enhanced) — runs inside the Worker
   - Keeps all existing low-level bf-tree CRUD methods (`get`, `put`, `del`, `scanByPrefix`)
   - Keeps all existing row-ID mapping logic
   - **New**: `storeContent(msg)` — performs upsert + session updates + transaction writes + signature writes in a single synchronous call, returns the resulting `CoValueKnownState`
   - **New**: `loadContent(id)` — loads the CoValue, all sessions, transactions, and signatures, assembles `NewContentMessage` objects, returns them with a `found` flag
   - **New**: `loadKnownState(id)` — lightweight known-state-only load

2. **`BfTreeStorageProxy`** — main thread, implements `StorageAPI`
   - Thin proxy that forwards operations to the worker via `postMessage`
   - Manages local state that must live on the main thread:
     - `StorageKnownState` cache (for synchronous `getKnownState()`)
     - `StoreQueue` (for serializing stores per CoValue + correction handling)
     - `inMemoryCoValues` set (tracks which CoValues are loaded)
     - `deletedValues` set (tracks valid deletes)
   - Handles the `CorrectionCallback` loop locally (since corrections need main-thread CoValue state)

3. **`src/worker.ts`** — Worker entry point (simplified)
   - Initializes WASM + opens bf-tree
   - Dispatches incoming messages directly to `BfTreeWorkerBackend` methods
   - No longer routes through `DBClientInterfaceAsync` dispatch table

4. **`src/protocol.ts`** — message types aligned with `StorageAPI` semantics

### What changes vs. v1

| Component | v1 | v2 |
|---|---|---|
| `StorageApiAsync` | Main thread, wraps `BfTreeClient` | **Removed** — its logic is split between `BfTreeStorageProxy` (main) and `BfTreeWorkerBackend` (worker) |
| `BfTreeClient` | Main thread RPC proxy for `DBClientInterfaceAsync` | **Removed** — replaced by `BfTreeStorageProxy` |
| `BfTreeWorkerBackend` | Low-level CRUD only | **Enhanced** with `storeContent`, `loadContent`, `loadKnownState` |
| Worker protocol | `DBClientInterfaceAsync` methods (many per operation) | `StorageAPI`-level methods (1 per operation) |
| Store queue | Inside `StorageApiAsync` on main thread | Inside `BfTreeStorageProxy` on main thread |
| Known state cache | Inside `StorageApiAsync` on main thread | Inside `BfTreeStorageProxy` on main thread |
| Correction handling | Inside `StorageApiAsync.storeSingle()` | Inside `BfTreeStorageProxy.store()` |

### What stays the same

- `keys.ts` — key encoding utilities (unchanged, runs in worker)
- `BfTreeWorkerBackend` low-level methods — all existing CRUD remains
- WASM bindings in `cojson-core-wasm` — no changes needed
- Test patterns — same test helpers, just adapted for new factory

## Worker Communication Protocol

### Design

The protocol is now based on `StorageAPI`-level operations. Each operation is at most **1 request + 1 response**, except for `load()` which may stream multiple data messages.

```typescript
// src/protocol.ts

/** Initialization message sent once on startup */
export type WorkerInitRequest = {
  type: "init";
  dbName: string;
  cacheSizeBytes: number;
};

export type WorkerInitResponse =
  | { type: "ready" }
  | { type: "error"; message: string };

/**
 * Request/response messages — each maps to a StorageAPI-level operation.
 * Fire-and-forget messages omit reqId.
 */
export type WorkerRequest =
  | { reqId: number; method: "load"; id: string }
  | { reqId: number; method: "store"; data: NewContentMessage; deletedCoValues: string[] }
  | { reqId: number; method: "loadKnownState"; id: string }
  | { reqId: number; method: "eraseAllDeletedCoValues" }
  | { reqId: number; method: "getUnsyncedCoValueIDs" }
  | { reqId: number; method: "waitForQueueDrain" }
  | { reqId: number; method: "close" }
  | { method: "markDeleteAsValid"; id: string }
  | { method: "enableDeletedCoValuesErasure" }
  | { method: "trackCoValuesSyncState"; updates: { id: string; peerId: string; synced: boolean }[] }
  | { method: "stopTrackingSyncState"; id: string }
  | { method: "onCoValueUnmounted"; id: string };

/**
 * Worker responses.
 *
 * For load(): the worker sends one or more "load:data" messages followed by a "load:done".
 * For store(): the worker sends a single "store:result" with the resulting knownState.
 * For other request/response methods: a single "result" message.
 */
export type WorkerResponse =
  | { reqId: number; type: "load:data"; data: NewContentMessage }
  | { reqId: number; type: "load:done"; found: boolean }
  | { reqId: number; type: "store:result"; knownState: CoValueKnownState; storedCoValueRowID: number | undefined }
  | { reqId: number; type: "result"; value: unknown }
  | { reqId: number; type: "error"; message: string };

/** All message types the worker can receive */
export type WorkerIncoming = WorkerInitRequest | WorkerRequest;
```

### Message Flow Examples

**`store()` — 1 round-trip (vs 5-10+ in v1):**
```
Main → Worker:  { reqId: 1, method: "store", data: NewContentMessage, deletedCoValues: ["co_z..."] }
Worker → Main:  { reqId: 1, type: "store:result", knownState: {...}, storedCoValueRowID: 42 }
                 (if correction needed, main sends another store — still just 1 more round-trip)
```

**`load()` — 1-2 messages (vs 5-10+ in v1):**
```
Main → Worker:  { reqId: 2, method: "load", id: "co_z..." }
Worker → Main:  { reqId: 2, type: "load:data", data: NewContentMessage }  (repeated for streaming)
Worker → Main:  { reqId: 2, type: "load:done", found: true }
```

**Fire-and-forget (no response):**
```
Main → Worker:  { method: "markDeleteAsValid", id: "co_z..." }
Main → Worker:  { method: "trackCoValuesSyncState", updates: [...] }
```

## Data Models

### Key Schema (unchanged from v1)

The bf-tree key-value mapping is identical:

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

### What crosses the postMessage boundary

The key efficiency gain: instead of individual rows (CoValueRow, SessionRow, TransactionRow, SignatureRow) crossing the boundary in separate messages, only high-level aggregate objects cross:

| Object | Direction | When |
|---|---|---|
| `NewContentMessage` | Main → Worker | `store()` |
| `CoValueKnownState` | Worker → Main | `store()` response |
| `NewContentMessage` (assembled) | Worker → Main | `load()` response |
| `CoValueKnownState` | Worker → Main | `loadKnownState()` response |
| Sync state updates | Main → Worker | `trackCoValuesSyncState()` |
| `RawCoID[]` | Worker → Main | `getUnsyncedCoValueIDs()` response |

## Core Implementation

### BfTreeWorkerBackend — New High-Level Methods

The existing low-level methods remain unchanged. Two new methods combine multiple operations:

```typescript
// src/workerBackend.ts (additions to existing class)

/**
 * Store a NewContentMessage in a single synchronous call.
 *
 * Performs: upsert CoValue + for each session: get/create session,
 * write transactions, write signatures, update session metadata.
 *
 * Returns the resulting CoValueKnownState after the store.
 *
 * This replaces the 5-10+ individual DBClientInterfaceAsync calls
 * that StorageApiAsync.storeSingle() previously made across postMessage.
 */
storeContent(
  msg: NewContentMessage,
  deletedCoValues: Set<string>,
): { knownState: CoValueKnownState; storedCoValueRowID: number | undefined } {
  const id = msg.id;

  // 1. Upsert the CoValue
  const storedCoValueRowID = this.upsertCoValue(id, msg.header);

  if (!storedCoValueRowID) {
    // No header and CoValue doesn't exist yet — return empty known state
    return {
      knownState: { id: id as RawCoID, header: false, sessions: {} },
      storedCoValueRowID: undefined,
    };
  }

  const knownState: CoValueKnownState = {
    id: id as RawCoID,
    header: true,
    sessions: {},
  };

  let invalidAssumptions = false;

  // 2. Process each session
  for (const sessionID of Object.keys(msg.new)) {
    const sessionRow = this.getSingleCoValueSession(
      storedCoValueRowID,
      sessionID,
    );

    // Handle delete markers
    if (deletedCoValues.has(id) && isDeleteSessionID(sessionID)) {
      this.markCoValueAsDeleted(id);
    }

    const lastIdx = sessionRow?.lastIdx || 0;
    const after = msg.new[sessionID]?.after || 0;

    if (sessionRow) {
      knownState.sessions[sessionID] = sessionRow.lastIdx;
    }

    if (lastIdx < after) {
      // Storage has less data than message assumes — need correction
      invalidAssumptions = true;
    } else {
      // 3. Write new transactions + signatures
      const newLastIdx = this.putNewTxs(
        msg,
        sessionID,
        sessionRow,
        storedCoValueRowID,
      );
      knownState.sessions[sessionID] = newLastIdx;
    }
  }

  return { knownState, storedCoValueRowID: invalidAssumptions ? undefined : storedCoValueRowID };
}

/**
 * Write new transactions and signatures for a session.
 * Synchronous — all bf-tree operations happen in the worker.
 */
private putNewTxs(
  msg: NewContentMessage,
  sessionID: string,
  sessionRow: StoredSessionRow | undefined,
  storedCoValueRowID: number,
): number {
  const newTransactions = msg.new[sessionID]?.newTransactions || [];
  const lastIdx = sessionRow?.lastIdx || 0;
  const actuallyNewOffset = lastIdx - (msg.new[sessionID]?.after || 0);
  const actuallyNewTransactions = newTransactions.slice(actuallyNewOffset);

  if (actuallyNewTransactions.length === 0) {
    return lastIdx;
  }

  let bytesSinceLastSignature = sessionRow?.bytesSinceLastSignature || 0;
  const newTransactionsSize = getNewTransactionsSize(actuallyNewTransactions);
  const newLastIdx = lastIdx + actuallyNewTransactions.length;

  let shouldWriteSignature = false;
  if (exceedsRecommendedSize(bytesSinceLastSignature, newTransactionsSize)) {
    shouldWriteSignature = true;
    bytesSinceLastSignature = 0;
  } else {
    bytesSinceLastSignature += newTransactionsSize;
  }

  if (!msg.new[sessionID]) throw new Error("Session ID not found");

  const sessionUpdate = {
    coValue: storedCoValueRowID,
    sessionID,
    lastIdx: newLastIdx,
    lastSignature: msg.new[sessionID].lastSignature,
    bytesSinceLastSignature,
  };

  const sessionRowID = this.addSessionUpdate({
    sessionUpdate,
    sessionRow,
  });

  if (shouldWriteSignature) {
    this.addSignatureAfter({
      sessionRowID,
      idx: newLastIdx - 1,
      signature: msg.new[sessionID].lastSignature,
    });
  }

  for (let i = 0; i < actuallyNewTransactions.length; i++) {
    this.addTransaction(sessionRowID, lastIdx + i, actuallyNewTransactions[i]!);
  }

  return newLastIdx;
}

/**
 * Load all content for a CoValue in a single synchronous call.
 *
 * Assembles complete NewContentMessage objects (including streamed chunks
 * for large CoValues with multiple signatures).
 *
 * Returns the messages and whether the CoValue was found.
 */
loadContent(id: string): { messages: NewContentMessage[]; found: boolean } {
  const coValueRow = this.getCoValue(id);

  if (!coValueRow) {
    return { messages: [], found: false };
  }

  const allSessions = this.getCoValueSessions(coValueRow.rowID);
  const messages: NewContentMessage[] = [];

  // Collect signatures per session
  const signaturesBySession = new Map<string, { idx: number; signature: string }[]>();
  let needsStreaming = false;

  for (const sessionRow of allSessions) {
    const signatures = this.getSignatures(sessionRow.rowID, 0);
    if (signatures.length > 0) {
      needsStreaming = true;
      signaturesBySession.set(sessionRow.sessionID, signatures);
    }
  }

  // Build known state for expectContentUntil
  const knownState: Record<string, number> = {};
  for (const sessionRow of allSessions) {
    knownState[sessionRow.sessionID] = sessionRow.lastIdx;
  }

  let contentMessage = createContentMessage(coValueRow.id, coValueRow.header);

  if (needsStreaming) {
    contentMessage.expectContentUntil = knownState;
  }

  for (const sessionRow of allSessions) {
    const signatures = signaturesBySession.get(sessionRow.sessionID) || [];
    let idx = 0;

    const lastSignature = signatures[signatures.length - 1];
    if (lastSignature?.signature !== sessionRow.lastSignature) {
      signatures.push({
        idx: sessionRow.lastIdx,
        signature: sessionRow.lastSignature,
      });
    }

    for (const signature of signatures) {
      const txRows = this.getNewTransactionInSession(
        sessionRow.rowID,
        idx,
        signature.idx,
      );

      collectNewTxs({
        newTxsInSession: txRows,
        contentMessage,
        sessionRow,
        firstNewTxIdx: idx,
        signature: signature.signature,
      });

      idx = signature.idx + 1;

      if (signatures.length > 1) {
        // Stream: push current chunk, start new message
        messages.push(contentMessage);
        contentMessage = createContentMessage(coValueRow.id, coValueRow.header);
      }
    }
  }

  const hasNewContent = Object.keys(contentMessage.new).length > 0;
  if (hasNewContent || !needsStreaming) {
    messages.push(contentMessage);
  }

  return { messages, found: true };
}
```

### BfTreeStorageProxy — Main Thread

```typescript
// src/proxy.ts

import type {
  CoValueCore,
  CorrectionCallback,
  RawCoID,
  StorageAPI,
} from "cojson";
import { StorageKnownState } from "cojson/storage/knownState";
import { StoreQueue } from "cojson/queue/StoreQueue";
import { NewContentMessage, PeerID } from "cojson";
import { CoValueKnownState, emptyKnownState } from "cojson";
import type { WorkerRequest, WorkerResponse } from "./protocol.js";

/**
 * Main-thread proxy implementing StorageAPI.
 *
 * Forwards operations to the BfTree Web Worker via postMessage.
 * Each StorageAPI method is 1 round-trip (or fire-and-forget),
 * compared to 5-10+ round-trips in the v1 DBClientInterfaceAsync design.
 */
export class BfTreeStorageProxy implements StorageAPI {
  private worker: Worker;
  private nextReqId = 0;
  private pending = new Map<number, {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
    /** For load(): accumulates data callbacks before done */
    onData?: (data: NewContentMessage) => void;
    onDone?: (found: boolean) => void;
  }>();

  // Main-thread state (mirrors what StorageApiAsync managed)
  private knownStates = new StorageKnownState();
  private storeQueue = new StoreQueue();
  private inMemoryCoValues = new Set<RawCoID>();
  private deletedValues = new Set<RawCoID>();

  private pendingKnownStateLoads = new Map<string, Promise<CoValueKnownState | undefined>>();

  constructor(worker: Worker) {
    this.worker = worker;
    this.worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      this.handleResponse(event.data);
    };
  }

  private handleResponse(resp: WorkerResponse) {
    const handler = this.pending.get(resp.reqId);
    if (!handler) return;

    switch (resp.type) {
      case "load:data":
        handler.onData?.(resp.data);
        break;

      case "load:done":
        this.pending.delete(resp.reqId);
        handler.onDone?.(resp.found);
        break;

      case "store:result":
        this.pending.delete(resp.reqId);
        handler.resolve(resp);
        break;

      case "result":
        this.pending.delete(resp.reqId);
        handler.resolve(resp.value);
        break;

      case "error":
        this.pending.delete(resp.reqId);
        handler.reject(new Error(resp.message));
        break;
    }
  }

  /** Send a request and return a Promise for the response */
  private call<T>(msg: WorkerRequest & { reqId: number }): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.pending.set(msg.reqId, {
        resolve: resolve as (v: unknown) => void,
        reject,
      });
      this.worker.postMessage(msg);
    });
  }

  /** Send a fire-and-forget message (no response expected) */
  private send(msg: WorkerRequest): void {
    this.worker.postMessage(msg);
  }

  // =========================================================================
  // StorageAPI implementation
  // =========================================================================

  getKnownState(id: string): CoValueKnownState {
    return this.knownStates.getKnownState(id);
  }

  loadKnownState(
    id: string,
    callback: (knownState: CoValueKnownState | undefined) => void,
  ): void {
    const cached = this.knownStates.getCachedKnownState(id);
    if (cached) {
      callback(cached);
      return;
    }

    const pending = this.pendingKnownStateLoads.get(id);
    if (pending) {
      pending.then(callback, () => callback(undefined));
      return;
    }

    const reqId = this.nextReqId++;
    const loadPromise = this.call<CoValueKnownState | undefined>({
      reqId,
      method: "loadKnownState",
      id,
    })
      .then((knownState) => {
        if (knownState) {
          this.knownStates.setKnownState(id, knownState);
        }
        return knownState;
      })
      .catch(() => undefined)
      .finally(() => {
        this.pendingKnownStateLoads.delete(id);
      });

    this.pendingKnownStateLoads.set(id, loadPromise);
    loadPromise.then(callback);
  }

  load(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ): void {
    const reqId = this.nextReqId++;

    // Register handlers for streaming response
    this.pending.set(reqId, {
      resolve: () => {},
      reject: (err) => { console.error("load error", err); done(false); },
      onData: (data) => {
        // Update known state from loaded data
        callback(data);
      },
      onDone: (found) => {
        if (found) {
          this.inMemoryCoValues.add(id as RawCoID);
        }
        done(found);
      },
    });

    this.worker.postMessage({ reqId, method: "load", id });
  }

  store(msg: NewContentMessage, correctionCallback: CorrectionCallback): void {
    this.storeQueue.push(msg, correctionCallback);

    this.storeQueue.processQueue(async (data, correctionCallback) => {
      return this.storeSingle(data, correctionCallback);
    });
  }

  /**
   * Store a single message by forwarding to the worker.
   * Handles corrections locally since they need main-thread CoValue state.
   */
  private async storeSingle(
    msg: NewContentMessage,
    correctionCallback: CorrectionCallback,
  ): Promise<boolean> {
    if (this.storeQueue.closed) {
      return false;
    }

    const reqId = this.nextReqId++;

    // Send to worker — 1 round-trip
    const response = await this.call<{
      knownState: CoValueKnownState;
      storedCoValueRowID: number | undefined;
    }>({
      reqId,
      method: "store",
      data: msg,
      deletedCoValues: Array.from(this.deletedValues),
    });

    const { knownState, storedCoValueRowID } = response;

    this.inMemoryCoValues.add(msg.id);
    this.knownStates.setKnownState(msg.id, knownState);
    this.knownStates.handleUpdate(msg.id, knownState);

    // If storedCoValueRowID is undefined, the store needs correction
    if (!storedCoValueRowID) {
      return this.handleCorrection(knownState, correctionCallback);
    }

    return true;
  }

  private async handleCorrection(
    knownState: CoValueKnownState,
    correctionCallback: CorrectionCallback,
  ): Promise<boolean> {
    const correction = correctionCallback(knownState);

    if (!correction) {
      return false;
    }

    for (const msg of correction) {
      const success = await this.storeSingle(msg, (_knownState) => {
        // Double corrections shouldn't happen
        return undefined;
      });

      if (!success) {
        return false;
      }
    }

    return true;
  }

  markDeleteAsValid(id: RawCoID): void {
    this.deletedValues.add(id);
    this.send({ method: "markDeleteAsValid", id });
  }

  enableDeletedCoValuesErasure(): void {
    this.send({ method: "enableDeletedCoValuesErasure" });
  }

  async eraseAllDeletedCoValues(): Promise<void> {
    const reqId = this.nextReqId++;
    await this.call({ reqId, method: "eraseAllDeletedCoValues" });
  }

  waitForSync(id: string, coValue: CoValueCore): Promise<void> {
    return this.knownStates.waitForSync(id, coValue);
  }

  trackCoValuesSyncState(
    updates: { id: RawCoID; peerId: PeerID; synced: boolean }[],
    done?: () => void,
  ): void {
    this.send({ method: "trackCoValuesSyncState", updates });
    // Fire-and-forget; call done immediately since the worker will process it
    done?.();
  }

  getUnsyncedCoValueIDs(
    callback: (unsyncedCoValueIDs: RawCoID[]) => void,
  ): void {
    const reqId = this.nextReqId++;
    this.call<RawCoID[]>({ reqId, method: "getUnsyncedCoValueIDs" }).then(callback);
  }

  stopTrackingSyncState(id: RawCoID): void {
    this.send({ method: "stopTrackingSyncState", id });
  }

  onCoValueUnmounted(id: RawCoID): void {
    this.inMemoryCoValues.delete(id);
    this.send({ method: "onCoValueUnmounted", id });
  }

  close(): Promise<unknown> | undefined {
    const queuePromise = this.storeQueue.close();

    const reqId = this.nextReqId++;
    const closePromise = this.call({ reqId, method: "close" }).then(() => {
      this.worker.terminate();
    });

    return Promise.all([queuePromise, closePromise].filter(Boolean));
  }
}
```

### Worker Entry Point (simplified)

```typescript
// src/worker.ts

import { initialize, open_bftree_opfs } from "cojson-core-wasm";
import { BfTreeWorkerBackend } from "./workerBackend.js";
import type { WorkerIncoming } from "./protocol.js";

let backend: BfTreeWorkerBackend | undefined;

const ctx = self as unknown as DedicatedWorkerGlobalScope;

ctx.onmessage = async (event: MessageEvent<WorkerIncoming>) => {
  const msg = event.data;

  // Handle initialization
  if ("type" in msg && msg.type === "init") {
    try {
      await initialize();
      const tree = await open_bftree_opfs(msg.dbName, msg.cacheSizeBytes);
      backend = new BfTreeWorkerBackend(tree);
      ctx.postMessage({ type: "ready" });
    } catch (e) {
      ctx.postMessage({ type: "error", message: String(e) });
    }
    return;
  }

  if (!backend) {
    if ("reqId" in msg) {
      ctx.postMessage({ reqId: msg.reqId, type: "error", message: "Worker not initialized" });
    }
    return;
  }

  try {
    // No more giant dispatch table — just match on the method
    switch (msg.method) {
      case "load": {
        const { messages, found } = backend.loadContent(msg.id);
        for (const data of messages) {
          ctx.postMessage({ reqId: msg.reqId, type: "load:data", data });
        }
        ctx.postMessage({ reqId: msg.reqId, type: "load:done", found });
        break;
      }

      case "store": {
        const result = backend.storeContent(msg.data, new Set(msg.deletedCoValues));
        ctx.postMessage({ reqId: msg.reqId, type: "store:result", ...result });
        break;
      }

      case "loadKnownState": {
        const knownState = backend.getCoValueKnownState(msg.id);
        ctx.postMessage({ reqId: msg.reqId, type: "result", value: knownState });
        break;
      }

      case "eraseAllDeletedCoValues": {
        backend.eraseAllDeletedCoValues();
        ctx.postMessage({ reqId: msg.reqId, type: "result", value: undefined });
        break;
      }

      case "getUnsyncedCoValueIDs": {
        const ids = backend.getUnsyncedCoValueIDs();
        ctx.postMessage({ reqId: msg.reqId, type: "result", value: ids });
        break;
      }

      case "close": {
        ctx.postMessage({ reqId: msg.reqId, type: "result", value: undefined });
        break;
      }

      // Fire-and-forget messages (no reqId)
      case "markDeleteAsValid":
        // No-op in worker for now (deletion validation tracked on main thread)
        break;

      case "enableDeletedCoValuesErasure":
        backend.enableDeletedCoValuesErasure();
        break;

      case "trackCoValuesSyncState":
        backend.trackCoValuesSyncState(msg.updates);
        break;

      case "stopTrackingSyncState":
        backend.stopTrackingSyncState(msg.id);
        break;

      case "onCoValueUnmounted":
        // Worker can clean up row-ID caches if desired
        break;
    }
  } catch (e) {
    if ("reqId" in msg) {
      ctx.postMessage({
        reqId: msg.reqId,
        type: "error",
        message: e instanceof Error ? e.message : String(e),
      });
    }
  }
};
```

### Factory Function (updated)

```typescript
// src/index.ts

import type { StorageAPI } from "cojson";
import { BfTreeStorageProxy } from "./proxy.js";

export async function getBfTreeStorage(
  dbName = "jazz-bftree.db",
  cacheSizeBytes = 32 * 1024 * 1024,
): Promise<StorageAPI> {
  const worker = new Worker(new URL("./worker.js", import.meta.url), {
    type: "module",
  });

  await new Promise<void>((resolve, reject) => {
    const onMessage = (event: MessageEvent) => {
      worker.removeEventListener("message", onMessage);
      if (event.data.type === "ready") resolve();
      else if (event.data.type === "error") reject(new Error(event.data.message));
      else reject(new Error("Unexpected worker response"));
    };
    worker.addEventListener("message", onMessage);
    worker.postMessage({ type: "init", dbName, cacheSizeBytes });
  });

  // Returns StorageAPI directly — no StorageApiAsync wrapper needed!
  return new BfTreeStorageProxy(worker);
}
```

## Key Design Decisions

### 1. StorageAPI as the communication boundary (not DBClientInterfaceAsync)

The v1 design faithfully mirrored IndexedDB's architecture: `StorageApiAsync` on the main thread, `DBClientInterfaceAsync` as the RPC layer. This made sense for IndexedDB (same-thread, no serialization cost), but for a worker, each RPC call incurs `postMessage` overhead.

By making `StorageAPI` the boundary, a single `store()` call does all DB work in the worker (upsert + sessions + transactions + signatures) and returns just the resulting `CoValueKnownState`. **5-10+ round-trips become 1.**

### 2. Correction handling stays on the main thread

The `CorrectionCallback` in `store()` needs access to the in-memory CoValue state (to produce `NewContentMessage` objects for missing data). This state lives on the main thread in `LocalNode`. Rather than attempting a complex multi-message correction protocol with the worker, the proxy handles corrections locally:

1. Send `store(data)` to worker → get back `knownState`
2. Call `correctionCallback(knownState)` on main thread
3. If corrections needed, send them as additional `store()` calls

This keeps the worker simple (stateless w.r.t. corrections) and avoids potential deadlocks.

### 3. StoreQueue lives on the main thread

The `StoreQueue` serializes stores per CoValue and ensures corrections are applied before processing the next store. Since corrections require main-thread state, the queue must live on the main thread. The worker processes store requests as they arrive (no internal queue needed since the main thread already serializes them).

### 4. No StorageApiAsync in the architecture

`StorageApiAsync` was the glue between `StorageAPI` and `DBClientInterfaceAsync`. In v2, its responsibilities are split:
- **Known state management** → `StorageKnownState` in the proxy (main thread)
- **Store queue + corrections** → `StoreQueue` in the proxy (main thread)
- **Load assembly** → `BfTreeWorkerBackend.loadContent()` (worker)
- **Store execution** → `BfTreeWorkerBackend.storeContent()` (worker)
- **Eraser scheduling** → Worker-side (the worker manages its own `DeletedCoValuesEraserScheduler`)

### 5. Worker manages its own erasure scheduling

The `enableDeletedCoValuesErasure()` and `eraseAllDeletedCoValues()` methods are delegated to the worker. The worker runs the `DeletedCoValuesEraserScheduler` internally, using `setTimeout` (available in workers) to schedule background cleanup. This avoids round-trip overhead for a background operation that doesn't need main-thread state.

### 6. Streamed load responses

For large CoValues with multiple signatures (file-like data), the worker sends multiple `load:data` messages followed by a `load:done`. This mirrors the streaming behavior of `StorageApiAsync.loadCoValue()` but happens entirely within the worker — no per-chunk round-trip.

### 7. Dependencies loaded in the worker

The existing `loadCoValue` in `StorageApiAsync` recursively loads dependencies (groups that a CoValue extends). In the v2 worker, `loadContent` can detect dependencies from the header and load them as part of the same worker call, sending their content as additional `load:data` messages before the requested CoValue's data. This eliminates the dependency-loading round-trips.

## Testing Strategy

Tests reuse the existing test infrastructure. The main change is that `createStorageFromBackend()` now returns a `BfTreeStorageProxy` wrapping a synchronous message channel (no actual `Worker` for unit tests).

### Test Adapter for Unit Tests

```typescript
// src/tests/testUtils.ts

/**
 * Direct adapter: wraps BfTreeWorkerBackend to simulate the worker protocol
 * without actual postMessage. Uses synchronous calls for testing.
 */
export function createStorageFromBackend(backend: BfTreeWorkerBackend): StorageAPI {
  return new DirectBfTreeStorageProxy(backend);
}

/**
 * A test-only StorageAPI that calls BfTreeWorkerBackend directly
 * (no Worker, no postMessage). Replicates BfTreeStorageProxy logic
 * but with synchronous backend calls.
 */
class DirectBfTreeStorageProxy implements StorageAPI {
  private knownStates = new StorageKnownState();
  private storeQueue = new StoreQueue();
  private inMemoryCoValues = new Set<RawCoID>();
  private deletedValues = new Set<RawCoID>();

  constructor(private backend: BfTreeWorkerBackend) {}

  load(id: string, callback: (data: NewContentMessage) => void, done: (found: boolean) => void) {
    const { messages, found } = this.backend.loadContent(id);
    for (const msg of messages) {
      callback(msg);
    }
    if (found) {
      this.inMemoryCoValues.add(id as RawCoID);
    }
    done(found);
  }

  store(msg: NewContentMessage, correctionCallback: CorrectionCallback) {
    this.storeQueue.push(msg, correctionCallback);
    this.storeQueue.processQueue(async (data, correctionCallback) => {
      const { knownState, storedCoValueRowID } = this.backend.storeContent(
        data,
        this.deletedValues,
      );
      this.inMemoryCoValues.add(data.id);
      this.knownStates.setKnownState(data.id, knownState);
      this.knownStates.handleUpdate(data.id, knownState);

      if (!storedCoValueRowID) {
        const correction = correctionCallback(knownState);
        if (correction) {
          for (const msg of correction) {
            const result = this.backend.storeContent(msg, this.deletedValues);
            this.knownStates.setKnownState(msg.id, result.knownState);
            this.knownStates.handleUpdate(msg.id, result.knownState);
          }
        }
      }
      return true;
    });
  }

  getKnownState(id: string) { return this.knownStates.getKnownState(id); }
  // ... remaining methods delegate directly to backend
}
```

### Integration Test: Store and Load Round-Trip

```typescript
test("store and load round-trip", async () => {
  const backend = createBfTreeBackend();

  const node1 = createTestNode();
  node1.setStorage(createStorageFromBackend(backend));

  const group = node1.createGroup();
  const map = group.createMap();
  map.set("hello", "world");

  await map.core.waitForSync();
  node1.gracefulShutdown();

  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(createStorageFromBackend(backend));

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") throw new Error("Map is unavailable");

  expect(map2.get("hello")).toBe("world");
});
```

### Integration Test: Corrections

```typescript
test("store handles corrections when storage is behind", async () => {
  const backend = createBfTreeBackend();
  const storage = createStorageFromBackend(backend);
  const node = createTestNode();
  node.setStorage(storage);

  const group = node.createGroup();
  const map = group.createMap();
  map.set("a", "1");
  map.set("b", "2");

  await map.core.waitForSync();

  // Verify both values persisted (even if correction was needed)
  node.gracefulShutdown();

  const node2 = createTestNode({ secret: node.agentSecret });
  node2.setStorage(createStorageFromBackend(backend));

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") throw new Error("Map is unavailable");

  expect(map2.get("a")).toBe("1");
  expect(map2.get("b")).toBe("2");
});
```
