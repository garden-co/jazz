# Full Storage Reconciliation

## Overview

Full Storage Reconciliation ensures all CoValue content in storage is uploaded to the server peers. It's implemented by adding a new `SyncManager.startStorageReconciliation()` function. This is separate from peer reconciliation and focuses on uploading local changes to servers, without subscribing to peer changes. It concerns only CoValues in storage, as in-memory CoValues are already expected to be synced.

The key mechanism is a new **"reconcile"** message. The client sends a batch of `[coValueId, sessionsHash]` pairs; the server checks for each whether it is missing that CoValue or has different `KnownStateSessions`. When it does, it responds with a "known" message (its known state for that CoValue), so that the client can upload content as needed. The server then sends a **"reconcile-ack"** message with the same batch `id` so the client knows when processing of that batch is complete.

## Reconcile Messages

**Reconcile (client → server):**

```ts
export type ReconcileMessage = {
  action: "reconcile";
  id: string;
  values: [coValue: RawCoID, sessionsHash: string][];
};
```

- **id**: Unique batch id for this reconcile message (e.g. UUID or monotonic string). Used to correlate the **reconcile-ack** response.
- **coValue**: CoValue ID (`RawCoID`).
- **sessionsHash**: Hash of that CoValue's **KnownStateSessions** (the `sessions` field of `CoValueKnownState`: `{ [sessionID]: number }`). Use `CryptoProvider.shortHash(sessions)` so client and server hashes are comparable.

**Reconcile-ack (server → client):**

```ts
export type ReconcileAckMessage = {
  action: "reconcile-ack";
  id: string;
};
```

- **id**: Same value as the `id` from the **reconcile** message that was just processed.

**Server behavior:**

- For each `[coValue, sessionsHash]` in `values`:
  - If the server does **not** have that CoValue, or its own hash of its known state's sessions differs from `sessionsHash`, the server sends back a **"known"** message with its known state for that CoValue (same format as today).
- After processing all entries in the batch, the server sends a **"reconcile-ack"** message with the same `id` as the reconcile message.
- No subscription is created; the server does not push new content for these CoValues.

**Client behavior:**

- `startStorageReconciliation` collects local known state, computes `sessionsHash` for each CoValue, and sends one or more "reconcile" messages (e.g. batched by 100), each with a unique `id`.
- The client tracks pending acks in `pendingReconciliationAck` (e.g. a set of `"batchId#peerId"`). When it sends a reconcile message with `id` to a peer, it adds `"id#peerId"` to the set; when it receives a **reconcile-ack** with that `id` from that peer, it removes `"id#peerId"` in the handler. This lets the client know when each batch has been fully acknowledged by each server peer.
- When the client receives a "known" message in response to a reconcile, it uses the existing `handleKnownState` logic: compare with local known state and upload content if the client is ahead.

## Implementation Steps

### 1. Enumerating CoValues (batched)

#### 1.1. Add Storage API Method

**File:** `packages/cojson/src/storage/types.ts`

Add a batched method to `StorageAPI`:

```typescript
/**
 * Get a batch of CoValue IDs from storage.
 * Used for full storage reconciliation. Call repeatedly with increasing offset
 * until the returned batch has length < limit (or 0) to enumerate all IDs.
 * @param limit - Max number of IDs to return (e.g. 100).
 * @param offset - Number of IDs to skip (0 for first batch).
 * @param callback - Called with the batch. Ordering must be stable (e.g. by id).
 */
getCoValueIDs(
  limit: number,
  offset: number,
  callback: (batch: { id: RawCoID }[]) => void,
): void;
```

Also add to `DBClientInterfaceSync` and `DBClientInterfaceAsync`:

```typescript
getCoValueIDs(limit: number, offset: number): { id: RawCoID }[];  // sync
getCoValueIDs(limit: number, offset: number): Promise<{ id: RawCoID }[]>;  // async
```

#### 1.2. Implement getCoValueIDs in Storage Backends

Implementations must use stable ordering (e.g. by `id`) and never load the full ID list into memory.

##### SQLite Sync Client

**File:** `packages/cojson/src/storage/sqlite/client.ts`

```typescript
getCoValueIDs(limit: number, offset: number): { id: RawCoID }[] {
  return this.db.query<{ id: RawCoID }>(
    "SELECT id FROM coValues ORDER BY rowID LIMIT ? OFFSET ?",
    [limit, offset],
  );
}
```

##### SQLite Async Client

**File:** `packages/cojson/src/storage/sqliteAsync/client.ts`

```typescript
async getCoValueIDs(limit: number, offset: number): Promise<{ id: RawCoID }[]> {
  return this.db.query<{ id: RawCoID }>(
    "SELECT id FROM coValues ORDER BY rowID LIMIT ? OFFSET ?",
    [limit, offset],
  );
}
```

##### Storage Wrappers

**Files:**
- `packages/cojson/src/storage/storageSync.ts`
- `packages/cojson/src/storage/storageAsync.ts`

```typescript
getCoValueIDs(
  limit: number,
  offset: number,
  callback: (batch: { id: RawCoID }[]) => void,
): void {
  const batch = this.dbClient.getCoValueIDs(limit, offset);
  callback(batch);
}
```

For async:
```typescript
getCoValueIDs(
  limit: number,
  offset: number,
  callback: (batch: { id: RawCoID }[]) => void,
): void {
  this.dbClient.getCoValueIDs(limit, offset).then(callback);
}
```

##### IndexedDB Client

**File:** `packages/cojson-storage-indexeddb/src/idbClient.ts`

```typescript
async getAllCoValueIDs(
  limit: number,
  offset: number
): Promise<{ id: RawCoID }[]> {
  return queryIndexedDbStore<StoredCoValueRow[]>(
    this.db,
    "coValues",
    // Include upper bound but not lower bound (offset starts at 0)
    (store) => store.getAll(IDBKeyRange.bound(offset, offset + limit, true, false)),
  );
}
```

### 2. Reconcile Message and Handler

#### 2.1. Add ReconcileMessage, ReconcileAckMessage types and hash helper

**File:** `packages/cojson/src/sync.ts` (or message types module)

Add the new message types:

```typescript
export type ReconcileMessage = {
  action: "reconcile";
  id: string;
  values: [coValue: RawCoID, sessionsHash: string][];
};

export type ReconcileAckMessage = {
  action: "reconcile-ack";
  id: string;
};

export type SyncMessage =
  | LoadMessage
  | KnownStateMessage
  | NewContentMessage
  | DoneMessage
  | ReconcileMessage
  | ReconcileAckMessage;
```

#### 2.2. Server: handle reconcile messages

**File:** `packages/cojson/src/sync.ts`

Add a handler for incoming "reconcile" messages (in the message dispatch that currently handles "load", "known", "content", etc.):

```typescript
handleReconcile(msg: ReconcileMessage, peer: PeerState): void {
  for (const [coValueId, clientSessionsHash] of msg.values) {
    const coValue = this.local.getCoValue(coValueId);
    const knownState = coValue.isAvailable()
      ? coValue.knownState()
      : await coValue.getKnownStateFromStorage(); // or load from storage and compute
    if (!knownState) {
      // Server doesn't have this CoValue: send "known" with empty/undefined so client can upload
      this.trySendToPeer(peer, { action: "known", id: coValueId, header: false, sessions: {} });
      continue;
    }
    const serverSessionsHash = this.local.crypto.shortHash(knownState.sessions);
    if (serverSessionsHash !== clientSessionsHash) {
      // Server has different state: send our known state
      peer.trackToldKnownState(coValueId);
      this.trySendToPeer(peer, { action: "known", ...knownState });
    }
  }
  // Signal that this batch is done so the client can clear the pending ack
  this.trySendToPeer(peer, { action: "reconcile-ack", id: msg.id });
}
```

#### 2.3. Wire reconcile and reconcile-ack into message dispatch

At the start of `handleSyncMessage`, handle both reconcile and reconcile-ack before the `msg.id` (CoValue id) check, since these messages use `id` for the batch, not a CoValue id:

- When `msg.action === "reconcile"`, call `handleReconcile(msg, peer)` and return.
- When `msg.action === "reconcile-ack"`, call `handleReconcileAck(msg, peer)` and return.

The existing "known" response is handled by the current `handleKnownState` on the client; no change there.

#### 2.4. Client: handle reconcile-ack messages

**File:** `packages/cojson/src/sync.ts`

Add a handler for incoming "reconcile-ack" messages. Remove the corresponding entry from `pendingReconciliationAck` so the client knows this batch has been acknowledged by this peer:

```typescript
handleReconcileAck(msg: ReconcileAckMessage, peer: PeerState): void {
  const key = `${msg.id}#${peer.id}`;
  this.pendingReconciliationAck.delete(key);
}
```

Wire this in the same message dispatch: when `msg.action === "reconcile-ack"`, call `handleReconcileAck(msg, peer)`.

### 3. Implement startStorageReconciliation (client sends reconcile)

**File:** `packages/cojson/src/sync.ts`

Add a field on `SyncManager` to track pending reconcile acks (e.g. per batch per peer), and add the method:

```typescript
// On SyncManager: track pending reconcile acks so we know when each batch is acked by each peer
pendingReconciliationAck: Set<string> = new Set(); // entries are "batchId#peerId"

const RECONCILIATION_BATCH_SIZE = 100;

/**
 * Ensures all CoValues in storage are synced to server peers.
 * Sends "reconcile" message(s) with [coValueId, sessionsHash] for each CoValue.
 * Server responds with "known" only where it is missing the CoValue or has different sessions,
 * so that client can send missing content.
 * Processes CoValues in batches of RECONCILIATION_BATCH_SIZE.
 */
startStorageReconciliation(): void {
  if (!this.local.storage) return;

  const serverPeers = Object.values(this.peers).filter(
    isPersistentServerPeer,
  );
  if (serverPeers.length === 0) return;

  const batchSize = STORAGE_RECONCILIATION_CONFIG.BATCH_SIZE;

  const sendReconcileBatch = (entries: [RawCoID, string][]) => {
    if (entries.length === 0) return;
    const batchId = crypto.randomUUID();
    const msg: ReconcileMessage = {
      action: "reconcile",
      id: batchId,
      values: entries,
    };
    for (const peer of serverPeers) {
      this.pendingReconciliationAck.add(`${batchId}#${peer.id}`);
      this.trySendToPeer(peer, msg);
    }
  };

  const processStorageBatch = (offset: number) => {
    this.local.storage!.getCoValueIDs(batchSize, offset, (batch) => {
      // Skip in-memory CoValues
      const pending = batch.filter(({ id }) => !this.local.hasCoValue(id));
      let done = 0;
      const entries: [RawCoID, string][] = [];
      const sendReconcileMessageWhenDone = () => {
        if (++done === pending.length) {
          sendReconcileBatch(entries);
          if (batch.length === batchSize) {
            processStorageBatch(offset + batch.length);
          }
        }
      };
      for (const { id } of pending) {
        const coValue = this.local.getCoValue(id);
        coValue.getKnownStateFromStorage((storageKnownState) => {
          if (storageKnownState) {
            entries.push([
              id,
              this.hashKnownStateSessions(storageKnownState.sessions),
            ]);
          }
          sendReconcileMessageWhenDone();
        });
      }
      if (pending.length === 0 && batch.length >= batchSize) {
        processStorageBatch(offset + batchSize);
      }
    });
  };
  processStorageBatch(0);
}
```

**Note:** Storage: fetch IDs in batches, load known state from storage for each, then send reconcile batch(es). No subscription is created; server responds with "known" where state differs or CoValue is missing, then with "reconcile-ack" for that batch. The client removes from `pendingReconciliationAck` in `handleReconcileAck`.

### 4. Handle "known" response from reconcile

**File:** `packages/cojson/src/sync.ts`

When the client receives a "known" message (in response to a reconcile), it compares with local known state and uploads content if the client is ahead. Since `startStorageReconciliation` only loads the CoValue known state (but not its content), we need to modify the `handleKnownState` method to load the CoValue if it's partially loaded.

## Design Considerations

1. **No Subscription**: The "reconcile" message does not create a subscription. The server does not add the client to its subscription list for the CoValues in the reconcile; it only sends back "known" where state differs or the CoValue is missing.

2. **Sessions Hash**: `sessionsHash` is `CryptoProvider.secureHash(sessions)` so client and server hashes are comparable without sending full known state unless needed.

3. **Known State Comparison**: When the client receives a "known" message (in response to reconcile), it uses existing `handleKnownState` logic to compare with local known state and upload content if the client is ahead.

4. **Loading Strategy**: For CoValues not in memory, we use `getKnownStateFromStorage` to get known state and compute sessions hash; we do not load full content until we need to upload after a "known" response.

5. **Batching**: CoValues are processed in batches of 100. Reconcile messages are sent per batch (one message per batch per peer). Storage never returns all IDs in one call.

6. **Completion signal**: The server sends a **reconcile-ack** with the same batch `id` after processing each reconcile message. The client tracks pending acks in `pendingReconciliationAck` (e.g. `"batchId#peerId"`) so it knows when each batch has been acknowledged by each server peer. Callers can use this to run logic after reconciliation (e.g. when `pendingReconciliationAck` is empty for a given run).

## Testing

- Test `startStorageReconciliation` with CoValues only in storage
- Test with CoValues already in memory
- Test with mixed scenarios (some synced, some not)
- Verify that client correctly uploads content when needed after receiving "known" messages
- Verify that server sends "reconcile-ack" with the same `id` after processing each "reconcile" batch
- Verify that client removes entries from `pendingReconciliationAck` when it receives "reconcile-ack" (e.g. one ack per peer per batch)
