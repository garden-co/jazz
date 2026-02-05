# Design: Atomic Transactions for CoValue Mutations

## Overview

This design implements atomic transactions for CoValue mutations, allowing multiple mutations to be persisted together to IndexedDB and synced together to servers in a single network message. The core principle is **optimistic in-memory updates with atomic persistence** - mutations are applied immediately to memory, but persistence and sync happen atomically, each with a single attempt per transaction.

The key flow:
1. User calls `account.withTransaction(callback)` with a **synchronous** callback
2. Callback runs to completion (no await); mutations inside are applied to memory immediately
3. All mutations are buffered in a `TransactionContext`
4. After callback returns, all mutations are:
   - Stored in a single IndexedDB transaction
   - Sent in a single `BatchMessage` to sync servers
5. If either network sync or local persistence fails, the failure is surfaced to the caller and no automatic retry is performed.

## Architecture / Components

### 1. Public API: `withTransaction()` Method

**Location:** `packages/jazz-tools/src/tools/coValues/account.ts`

Add method to `AccountJazzApi` class:

```typescript
class AccountJazzApi<A extends Account> extends CoValueJazzApi<A> {
  /**
   * Execute multiple mutations atomically.
   * All mutations are applied immediately to memory, but persisted and synced as a batch.
   *
   * @param callback - Synchronous function containing mutations (no async/await)
   * @returns Promise that resolves when all mutations are persisted and synced
   *
   * @example
   * await account.withTransaction(() => {
   *   map1.$jazz.set("key1", "value1");
   *   map2.$jazz.set("key2", "value2");
   *   list.$jazz.push(item);
   * });
   */
  async withTransaction<T>(callback: () => T): Promise<T> {
    return this.localNode.withTransaction(callback);
  }
}
```

### 2. Transaction Context

**New File:** `packages/cojson/src/transactionContext.ts`

```typescript
import type { NewContentMessage, RawCoID } from "./types.js";

export class TransactionContext {
  private pendingMessages: NewContentMessage[] = [];
  private pendingCoValues: Set<RawCoID> = new Set();

  bufferMessage(msg: NewContentMessage): void {
    this.pendingMessages.push(msg);
    this.pendingCoValues.add(msg.id);
  }

  getPendingMessages(): NewContentMessage[] {
    return this.pendingMessages;
  }

  getCoValueIds(): Set<RawCoID> {
    return this.pendingCoValues;
  }

  isActive(): boolean {
    return true;
  }

  clear(): void {
    this.pendingMessages = [];
    this.pendingCoValues.clear();
  }
}
```

Design notes:
- The presence of a `TransactionContext` should be treated as the "active" signal.
- The callback is **synchronous**; no other code can run until it returns, so there is
  no read/write concurrency during the transaction window.

### 3. LocalNode Integration

**Location:** `packages/cojson/src/localNode.ts`

Add transaction context support:

```typescript
export class LocalNode {
  // ... existing properties ...

  private transactionContext?: TransactionContext;

  async withTransaction<T>(callback: () => T): Promise<T> {
    // Check for nested transactions
    if (this.transactionContext) {
      throw new Error("Nested transactions are not supported");
    }

    // Create and activate transaction context
    this.transactionContext = new TransactionContext();

    try {
      // Execute user callback synchronously (no concurrency while transaction is open)
      const result = callback();

      // Get buffered messages
      const messages = this.transactionContext.getPendingMessages();

      if (messages.length > 0) {
        // Flush to storage and sync in parallel
        await this.syncManager.syncAtomicBatch(messages);
      }

      return result;
    } finally {
      // Always clean up transaction context
      this.transactionContext = undefined;
    }
  }

  getTransactionContext(): TransactionContext | undefined {
    return this.transactionContext;
  }
}
```

Design notes:
- Empty batches should be treated as a no-op (avoid storage transaction / network).
- If the callback throws, buffered messages are discarded; in-memory state diverges
  from persisted state until future successful persistence.

### 4. CoValueCore: Intercept makeTransaction

**Location:** `packages/cojson/src/coValueCore/coValueCore.ts`

Modify `makeTransaction()` (around line 1166):

```typescript
makeTransaction(changes: TransactionChanges): Transaction | undefined {
  // ... existing validation ...

  const transactionContext = this.node.getTransactionContext();

  // Create the transaction content message
  const content = this.newContentSince(knownStateBefore);

  if (transactionContext?.isActive()) {
    // Buffer the message instead of syncing immediately
    for (const msg of content) {
      transactionContext.bufferMessage(msg);
    }
    // Don't sync now - will be done when transaction completes
  } else {
    // Normal flow: sync immediately
    this.node.syncManager.syncLocalTransaction(this.verified, knownStateBefore);
  }

  // ... rest of method (notify updates, etc.) ...
}
```

### 5. New Message Type: BatchMessage

**Location:** `packages/cojson/src/sync.ts` (around line 32)

```typescript
export type BatchMessage = {
  action: "batch";
  messages: NewContentMessage[];
};

export type SyncMessage =
  | LoadMessage
  | KnownStateMessage
  | NewContentMessage
  | DoneMessage
  | BatchMessage; // Add this
```

### 6. SyncManager: Atomic Batch Sync

**Location:** `packages/cojson/src/sync.ts`

```typescript
export class SyncManager {
  /**
   * Sync multiple mutations atomically.
   * Stores all in IndexedDB in one transaction and sends as one BatchMessage to servers.
   */
  async syncAtomicBatch(messages: NewContentMessage[]): Promise<void> {
    const storage = this.local.storage;
    const coValueIds = new Set(messages.map(m => m.id));

    if (messages.length === 0) {
      return;
    }

    // Start both operations in parallel (single attempt for storage and network)
    const storagePromise = storage
      ? storage.storeAtomicBatch(messages)
      : Promise.resolve();

    const syncPromise = this.syncBatchToServers(messages, coValueIds);

    // Wait for both to complete
    await Promise.all([storagePromise, syncPromise]);
  }

  private async syncBatchToServers(
    messages: NewContentMessage[],
    coValueIds: Set<RawCoID>
  ): Promise<void> {
    // Create single batch message
    const batchMessage: BatchMessage = {
      action: "batch",
      messages: messages
    };

    // Send to all server peers
    for (const coValueId of coValueIds) {
      for (const peer of this.getPeers(coValueId)) {
        if (peer.role === "server") {
          this.trySendToPeer(peer, batchMessage);
        }
      }
    }

    // Wait for all CoValues to be synced
    await this.waitForBatchSynced(coValueIds);
  }

  private async waitForBatchSynced(coValueIds: Set<RawCoID>): Promise<void> {
    let attempt = 0;

    while (true) {
      // Check if all CoValues are synced to all server peers
      const allSynced = Array.from(coValueIds).every(id => {
        const peers = this.getPersistentServerPeers(id);
        return peers.length === 0 || peers.every(peer =>
          this.syncState.isSynced(peer, id)
        );
      });

      if (allSynced) {
        return; // Success
      }

      // Not synced yet, wait
      attempt++;
      const delay = 100;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}
```

Design notes:
- Both storage and network sync are attempted once per transaction; failures are surfaced to the caller.

### 7. Server-Side: Handle Batch Messages

**Location:** `packages/cojson/src/sync.ts`

Update `handleSyncMessage()` switch statement (around line 215):

```typescript
handleSyncMessage(msg: SyncMessage, peer: PeerState) {
  // ... existing validation ...

  switch (msg.action) {
    case "load":
      return this.handleLoad(msg, peer);
    case "known":
      if (msg.isCorrection) {
        return this.handleCorrection(msg, peer);
      } else {
        return this.handleKnownState(msg, peer);
      }
    case "content":
      return this.handleNewContent(msg, peer);
    case "batch":  // Add this
      return this.handleBatch(msg, peer);
    case "done":
      return;
    default:
      throw new Error(
        `Unknown message type ${(msg as { action: string }).action}`,
      );
  }
}

/**
 * Handle a batch of content messages.
 * Unpacks the batch and processes each message individually.
 */
handleBatch(msg: BatchMessage, peer: PeerState) {
  for (const message of msg.messages) {
    this.handleNewContent(message, peer); // Reuse existing logic!
  }
}
```

Design notes:
- `handleBatch` must preserve per-CoValue ordering as sent by the client.
- `handleNewContent` must update sync state per message so batch retries remain idempotent.

### 8. Storage API: Atomic Batch Method

**Location:** `packages/cojson/src/storage/storageAsync.ts`

```typescript
export class StorageApiAsync implements StorageAPI {
  /**
   * Store multiple messages atomically in a single IndexedDB transaction.
   * If any operation fails, the entire transaction is rolled back and retried.
   */
  async storeAtomicBatch(messages: NewContentMessage[]): Promise<void> {
    // Use the DB client's transaction method for atomicity
    await this.dbClient.transaction(async (tx) => {
      for (const msg of messages) {
        // Store each message in the same transaction
        await this.storeSingleInTransaction(msg, tx);
      }
    });
  }

  private async storeSingleInTransaction(
    msg: NewContentMessage,
    tx: IDBTransaction
  ): Promise<void> {
    // Store CoValue header
    const storedCoValueRowID = await tx.upsertCoValue(msg.id, msg.header);

    if (!storedCoValueRowID) {
      throw new Error(`Failed to upsert CoValue ${msg.id}`);
    }

    // Store all sessions and transactions
    for (const [sessionID, sessionContent] of Object.entries(msg.new)) {
      const session = await tx.addSessionUpdate(
        storedCoValueRowID,
        sessionID,
        sessionContent.after,
        sessionContent.lastSignature
      );

      // Store transactions
      for (let i = 0; i < sessionContent.newTransactions.length; i++) {
        const transaction = sessionContent.newTransactions[i];
        await tx.addTransaction(
          session.rowID,
          sessionContent.after + i + 1,
          transaction
        );
      }

      // Store signature if present
      if (sessionContent.newTransactions.length > 0) {
        await tx.addSignatureAfter(
          session.rowID,
          sessionContent.after + sessionContent.newTransactions.length,
          sessionContent.lastSignature
        );
      }
    }
  }
}
```

### 9. IndexedDB Client: Transaction Support

**Location:** `packages/cojson-storage-indexeddb/src/idbClient.ts`

The existing `transaction()` method (line 299) already supports this pattern:

```typescript
async transaction(
  operationsCallback: (tx: IDBTransaction) => Promise<unknown>,
  storeNames?: StoreName[],
) {
  const tx = new CoJsonIDBTransaction(this.db, storeNames);

  try {
    await operationsCallback(new IDBTransaction(tx));
    tx.commit(); // Commit atomically
  } catch (error) {
    tx.rollback(); // Rollback on error
    throw error;
  }
}
```

## Data Flow

### Complete Transaction Flow

```
User Code
    ↓
account.withTransaction(callback)
    ↓
LocalNode.withTransaction()
    ↓
    [Create TransactionContext]
    ↓
Execute callback
    ↓
    ├─> map1.$jazz.set()
    │   ↓
    │   CoValueCore.makeTransaction()
    │   ↓
    │   [Check transaction context active]
    │   ↓
    │   [Apply to memory immediately]
    │   ↓
    │   [Buffer NewContentMessage]
    │
    ├─> map2.$jazz.set() [same flow]
    └─> list.$jazz.push() [same flow]
    ↓
[Callback complete, N messages buffered]
    ↓
SyncManager.syncAtomicBatch(messages)
    ↓
Parallel execution:
    ├─> StorageApiAsync.storeAtomicBatch()
    │   ↓
    │   IDBClient.transaction()
    │   ↓
    │   [Store all messages in single transaction]
    │   ↓
    │   [Commit or rollback]
    │   ↓
    │   [Surface failure to caller if any]
    │
    └─> syncBatchToServers()
        ↓
        Create BatchMessage { action: "batch", messages: [...] }
        ↓
        peer.pushOutgoingMessage(batchMessage)
        ↓
        [Server receives ONE message]
        ↓
        Server.handleBatch()
        ↓
        For each message:
          handleNewContent(message)
        ↓
        [Server optionally stores in transaction]
        ↓
        waitForBatchSynced()
        ↓
        [Check syncState.isSynced() for all CoValues]
        ↓
        [Retry if not synced]
    ↓
[Both storage and sync succeed]
    ↓
Return to user
```

## Error Handling Strategy

### Core Principle: No Rollback, No Automatic Retry

In-memory mutations are permanent. If persistence or sync fails, the failure is surfaced to the caller and no automatic retry is performed by the framework.

**1. Error During Callback Execution**
- Mutations before the error remain in memory
- Mutations after the error are not executed
- No persistence or sync is attempted
- Transaction context is cleaned up

**2. IndexedDB Storage Failure**
- Mutations already in memory (visible to user)
- Storage operation fails atomically (no partial writes)
- No automatic retry is attempted

**3. Network Sync Failure**
- Mutations already in memory (and may already be persisted to IndexedDB)
- A single attempt is made to send the batch
- No automatic retry is attempted by the framework

**4. Partial Sync (Multiple Server Peers)**
- Sync state for each server peer may still be tracked for diagnostics, but the framework does not automatically retry failed deliveries

**5. Nested Transactions**
- Throw error immediately
- Alternative: flatten into outer transaction (not implemented)

## Testing Strategy

### Unit Tests

1. **TransactionContext:**
   - Test bufferMessage() adds messages
   - Test getPendingMessages() returns buffered messages
   - Test clear() empties buffer

2. **LocalNode.withTransaction():**
   - Test creates and cleans up transaction context
   - Test callback result is returned
   - Test errors in callback are propagated
   - Test nested transactions throw error

3. **CoValueCore.makeTransaction():**
   - Test buffers messages when transaction context active
   - Test syncs immediately when no transaction context
   - Test mutations are applied to memory in both cases

4. **SyncManager.syncAtomicBatch():**
   - Test calls storeAtomicBatch() and syncBatchToServers()
   - Test retries storage on failure
   - Test retries sync until all CoValues synced

5. **StorageApiAsync.storeAtomicBatch():**
   - Test stores all messages in single transaction
   - Test rolls back on failure
   - Test throws error for retry

### Integration Tests

1. **Successful Transaction:**
   - Execute multiple mutations in transaction
   - Verify all stored in IndexedDB atomically
   - Verify all sent in single BatchMessage
   - Verify all synced to server

2. **Transaction with Error:**
   - Execute mutations, throw error mid-callback
   - Verify partial mutations remain in memory
   - Verify no persistence or sync occurred

3. **Storage Failure Handling:**
   - Mock IndexedDB failure
   - Verify failure is surfaced to the caller without automatic retry

4. **Network Failure Handling:**
   - Mock network disconnection
   - Verify failure is surfaced to the caller without automatic retry

5. **Server Batch Processing:**
   - Send BatchMessage to server
   - Verify server unpacks and processes each message
   - Verify existing handleNewContent() logic works

### Performance Tests

1. Benchmark transaction with 1, 10, 100, 1000 mutations
2. Compare single transaction vs individual stores (IndexedDB)
3. Compare BatchMessage vs individual messages (network)
4. Memory usage with large transactions

## Backward Compatibility

- Servers that don't understand `BatchMessage` will ignore it (unknown action type)
- No breaking changes to existing APIs

## Constraints and Footguns

- The transaction callback is **synchronous**; the implementation must not accept or
  await an async callback, so that no other code can run until the callback returns
  and the batch is fixed.
- Batch size limits (count and/or payload size) must be enforced with explicit behavior
  (reject, split, or fallback).
- If storage succeeds but sync does not (or vice versa), the system surfaces the failure
  and does not automatically retry either side.
