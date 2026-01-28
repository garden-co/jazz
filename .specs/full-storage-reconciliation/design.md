# Full Storage Reconciliation

## Overview

Full Storage Reconciliation ensures all local CoValue content (both in memory and in storage) is uploaded to the server peers. It's implemented by adding a new `SyncManager.ensureCoValuesSync()` function. This is separate from peer reconciliation and focuses on uploading local changes to servers, without subscribing to peer changes.

The key mechanism is a new **"reconcile"** message. The client sends a batch of `[coValueId, sessionsHash]` pairs; the server checks for each whether it is missing that CoValue or has different `KnownStateSessions`. When it does, it responds with a "known" message (its known state for that CoValue), so that the client can upload content as needed.

## Reconcile Message

**Shape:**

```ts
{
  type: "reconcile",
  values: [coValue: string, sessionsHash: string][],
}
```

- **coValue**: CoValue ID (`RawCoID`).
- **sessionsHash**: Hash of that CoValue's **KnownStateSessions** (the `sessions` field of `CoValueKnownState`: `{ [sessionID]: number }`). Use `CryptoProvider.shortHash(sessions)` so client and server hashes are comparable.

**Server behavior:**

- For each `[coValue, sessionsHash]` in `values`:
  - If the server does **not** have that CoValue, or its own hash of its known state's sessions differs from `sessionsHash`, the server sends back a **"known"** message with its known state for that CoValue (same format as today).
- No subscription is created; the server does not push new content for these CoValues.

**Client behavior:**

- `ensureCoValuesSync` collects local known state (in memory and from storage in batches), computes `sessionsHash` for each CoValue, and sends one or more "reconcile" messages (e.g. batched by 100).
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
    "SELECT id FROM coValues ORDER BY id LIMIT ? OFFSET ?",
    [limit, offset],
  );
}
```

##### SQLite Async Client

**File:** `packages/cojson/src/storage/sqliteAsync/client.ts`

```typescript
async getCoValueIDs(limit: number, offset: number): Promise<{ id: RawCoID }[]> {
  return this.db.query<{ id: RawCoID }>(
    "SELECT id FROM coValues ORDER BY id LIMIT ? OFFSET ?",
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

#### 2.1. Add ReconcileMessage type and hash helper

**File:** `packages/cojson/src/sync.ts` (or message types module)

Add the new message type:

```typescript
export type ReconcileMessage = {
  type: "reconcile";
  values: [coValue: RawCoID, sessionsHash: string][];
};
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
}
```

#### 2.3. Wire reconcile into message dispatch

Ensure that when a message with `type: "reconcile"` (or `action: "reconcile"`) is received, `handleReconcile(msg, peer)` is called. The existing "known" response is handled by the current `handleKnownState` on the client; no change there.

### 3. Implement ensureCoValuesSync (client sends reconcile)

**File:** `packages/cojson/src/sync.ts`

Add new method to `SyncManager`:

```typescript
const RECONCILIATION_BATCH_SIZE = 100;

/**
 * Ensures all CoValues (both in memory and in storage) are synced to server peers.
 * Sends "reconcile" message(s) with [coValueId, sessionsHash] for each CoValue.
 * Server responds with "known" message only where it is missing the CoValue or has different sessions.
 * Processes CoValues in batches of RECONCILIATION_BATCH_SIZE.
 */
ensureCoValuesSync(): void {
  const serverPeers = Object.values(this.peers).filter(isPersistentServerPeer);
  if (serverPeers.length === 0) return;

  const batchSize = RECONCILIATION_BATCH_SIZE;
  const processedIds = new Set<RawCoID>();

  const sendReconcileBatch = (entries: [RawCoID, string][]) => {
    if (entries.length === 0) return;
    const msg: ReconcileMessage = { type: "reconcile", values: entries };
    for (const peer of serverPeers) {
      this.trySendToPeer(peer, msg);
    }
  };

  // 1. In-memory CoValues: collect [id, hash(sessions)] in batches
  const inMemory = Array.from(this.local.allCoValues()).filter((c) => c.isAvailable());
  for (let i = 0; i < inMemory.length; i += batchSize) {
    const batch = inMemory.slice(i, i + batchSize);
    const entries: [RawCoID, string][] = batch.map((c) => [
      c.id,
      hashKnownStateSessions(c.knownState().sessions, this.local.crypto),
    ]);
    sendReconcileBatch(entries);
    batch.forEach((c) => processedIds.add(c.id));
  }

  // 2. Storage CoValues: getCoValueIDs in batches, load known state, send reconcile
  if (!this.local.storage) return;

  const processStorageBatch = (offset: number) => {
    this.local.storage!.getCoValueIDs(batchSize, offset, (batch) => {
      const pending = batch.filter(({ id }) => !processedIds.has(id));
      let done = 0;
      const entries: [RawCoID, string][] = [];
      for (const { id } of pending) {
        const coValue = this.local.getCoValue(id);
        coValue.getKnownStateFromStorage((storageKnownState) => {
          if (!storageKnownState) {
            if (++done === pending.length && batch.length >= batchSize) processStorageBatch(offset + batchSize);
            return;
          }
          processedIds.add(id);
          entries.push([id, hashKnownStateSessions(storageKnownState.sessions, this.local.crypto)]);
          if (entries.length === pending.length) {
            sendReconcileBatch(entries);
            if (batch.length >= batchSize) processStorageBatch(offset + batch.length);
          }
        });
      }
      if (pending.length === 0 && batch.length >= batchSize) processStorageBatch(offset + batchSize);
    });
  };
  processStorageBatch(0);
}
```

**Note:** In-memory batch: build `[id, sessionsHash]` and send one "reconcile" per batch to each persistent server peer. Storage: fetch IDs in batches, load known state from storage for each, then send reconcile batch(es). No subscription is created; server only responds with "known" where state differs or CoValue is missing.

### 4. Handle "known" response from reconcile

**File:** `packages/cojson/src/sync.ts`

The existing `handleKnownState` method is used unchanged. When the client receives a "known" message (in response to a reconcile), it compares with local known state and uploads content if the client is ahead.

## Design Considerations

1. **No Subscription**: The "reconcile" message does not create a subscription. The server does not add the client to its subscription list for the CoValues in the reconcile; it only sends back "known" where state differs or the CoValue is missing.

2. **Sessions Hash**: `sessionsHash` is `CryptoProvider.secureHash(sessions)` so client and server hashes are comparable without sending full known state unless needed.

3. **Known State Comparison**: When the client receives a "known" message (in response to reconcile), it uses existing `handleKnownState` logic to compare with local known state and upload content if the client is ahead.

4. **Loading Strategy**: For CoValues not in memory, we use `getKnownStateFromStorage` to get known state and compute sessions hash; we do not load full content until we need to upload after a "known" response.

5. **Batching**: CoValues are processed in batches of 100. Reconcile messages are sent per batch (one message per batch per peer). Storage never returns all IDs in one call.

## Testing

- Test `ensureCoValuesSync` with CoValues only in storage
- Test with CoValues already in memory
- Test with mixed scenarios (some synced, some not)
- Verify that client correctly uploads content when needed after receiving "known" messages
