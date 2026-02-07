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
 * Ensures all CoValues in storage are synced to the given server peer.
 * Sends "reconcile" message(s) with [coValueId, sessionsHash] for each CoValue.
 * Server responds with "known" only where it is missing the CoValue or has different sessions,
 * so that client can send missing content.
 * Processes CoValues in batches of RECONCILIATION_BATCH_SIZE.
 */
startStorageReconciliation(peer: PeerState): void {
  if (!this.local.storage) return;
  if (!isPersistentServerPeer(peer)) return;

  const batchSize = STORAGE_RECONCILIATION_CONFIG.BATCH_SIZE;

  const sendReconcileBatch = (entries: [RawCoID, string][]) => {
    if (entries.length === 0) return;
    const batchId = crypto.randomUUID();
    const msg: ReconcileMessage = {
      action: "reconcile",
      id: batchId,
      values: entries,
    };
    this.pendingReconciliationAck.add(`${batchId}#${peer.id}`);
    this.trySendToPeer(peer, msg);
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

### 5. Scheduling: when to run storage reconciliation

Storage reconciliation runs per server peer, periodically (every 30 days), with single-tab/process execution per peer. If the process is interrupted, it is fine to restart when the app is reloaded. There is one lock and lastRun per peer, so reconciliation with different peers can run independently (or in parallel). We use the Storage API to schedule the run.

#### 5.1. Storage API additions

**File:** `packages/cojson/src/storage/types.ts`

Add to `StorageAPI`:

```typescript
export type StorageReconciliationAcquireResult =
  | { acquired: true }
  | { acquired: false; reason: "not_due" | "lock_held" };

// Constants (in sync.ts or a shared config):
// LOCK_TTL_MS = 24 * 60 * 60 * 1000 (1 day)
// RECONCILIATION_INTERVAL_MS = 30 * 24 * 60 * 60 * 1000 (30 days)

/**
 * Try to acquire the storage reconciliation lock for a given peer.
 * Atomically checks if reconciliation is due for this peer (lastRun older than 30 days or missing)
 * and if no other process/tab holds the lock for this peer, then acquires it.
 * @param sessionId - Unique ID for this process/tab (e.g. crypto.randomUUID())
 * @param peerId - The peer ID to reconcile with.
 * @param callback - Called with result. If acquired, caller must call releaseStorageReconciliationLock on completion.
 */
tryAcquireStorageReconciliationLock(
  sessionId: string,
  peerId: string,
  callback: (result: StorageReconciliationAcquireResult) => void
): void;

/**
 * Release the storage reconciliation lock for a peer and record completion. Only call on successful completion.
 * On failure/interrupt, do not call; the lock expires after LOCK_TTL_MS and another process can retry for this peer.
 * @param sessionId - The sessionId that acquired the lock.
 * @param peerId - The peer ID for this reconciliation run.
 * @param releasedAt - Timestamp when reconciliation completed / lock was released (e.g. Date.now()).
 */
releaseStorageReconciliationLock(
  sessionId: string,
  peerId: string,
  releasedAt: number,
): void;
```

**Behavior of `tryAcquireStorageReconciliationLock`** (must run in one storage transaction):

1. Read `lastRun` for this peerId. If it exists and `Date.now() - timestamp < RECONCILIATION_INTERVAL_MS`, return `{ acquired: false, reason: "not_due" }`.
2. Read `lock` for this peerId. If it exists and `expiresAt >= Date.now()`, return `{ acquired: false, reason: "lock_held" }`.
3. Otherwise: write `lock` for this peerId with `{ holderSessionId, acquiredAt, expiresAt: now + LOCK_TTL_MS }` and return `{ acquired: true }`.

#### 5.2. startStorageReconciliation takes a peer

**File:** `packages/cojson/src/sync.ts`

Change `startStorageReconciliation()` to accept a peer and only send reconcile messages to that peer:

```typescript
startStorageReconciliation(peer: PeerState): void;
```

Reconcile batches are sent only to the given peer. `pendingReconciliationAck` entries for a given run are only for that peer. Completion is detected when all acks for that peer's batches are received.

#### 5.3. Storage backend implementations

Add the two methods to `StorageAPI`; implementations delegate to the underlying DB client. Add corresponding methods to `DBClientInterfaceSync` and `DBClientInterfaceAsync` in `packages/cojson/src/storage/types.ts`, and implement in the storage wrappers (`storageSync.ts`, `storageAsync.ts`). Lock and lastRun are stored per peerId (e.g. key = `lock#${peerId}` and `lastRun#${peerId}`). The release method atomically removes the lock and updates lastRun for that peer in one transaction.

**IndexedDB** (`packages/cojson-storage-indexeddb`):

- Add object store `storageReconciliationLocks` with keyPath `"key"` (bump DB version).
- Rows: `{ key: "lock#<peerId>", holderSessionId, acquiredAt, expiresAt }`, `{ key: "lastRun#<peerId>", timestamp }`.
- Implement using IndexedDB transactions.

**SQLite** (`packages/cojson-storage-sqlite`):

- Add table `storageReconciliationLocks` with columns for peerId and lock/lastRun (e.g. composite key `(peerId, key)` or `peer_id`, `key`, `holder_session_id`, `acquired_at`, `expires_at`, `timestamp`).
- Implement using SQLite transactions.

#### 5.4. Integration point

**File:** `packages/cojson/src/sync.ts` (e.g. when adding a peer)

For each persistent server peer, check if full-storage reconciliation needs to be performed for that peer:

1. If `!this.local.storage`, skip.
2. Call `storage.tryAcquireStorageReconciliationLock(sessionId, peer.id, (result) => { ... })`.
3. If `result.acquired`:
   - Call `this.startStorageReconciliation(peer)`.
   - Wait for completion (e.g. poll `pendingReconciliationAck` for entries matching this peer, or add completion callback).
   - On success: call `storage.releaseStorageReconciliationLock(sessionId, peer.id, Date.now())`.
   - On failure/interrupt: do not call release; the lock expires after `LOCK_TTL_MS` and another process/tab can acquire it and retry for this peer.

Run this check when a persistent server peer is added (and at startup for already-connected peers).

## Design Considerations

1. **No Subscription**: The "reconcile" message does not create a subscription. The server does not add the client to its subscription list for the CoValues in the reconcile; it only sends back "known" where state differs or the CoValue is missing.

2. **Sessions Hash**: `sessionsHash` is `CryptoProvider.secureHash(sessions)` so client and server hashes are comparable without sending full known state unless needed.

3. **Known State Comparison**: When the client receives a "known" message (in response to reconcile), it uses existing `handleKnownState` logic to compare with local known state and upload content if the client is ahead.

4. **Loading Strategy**: For CoValues not in memory, we use `getKnownStateFromStorage` to get known state and compute sessions hash; we do not load full content until we need to upload after a "known" response.

5. **Batching**: CoValues are processed in batches of 100. Reconcile messages are sent per batch (one message per batch per peer). Storage never returns all IDs in one call.

6. **Completion signal**: The server sends a **reconcile-ack** with the same batch `id` after processing each reconcile message. The client tracks pending acks in `pendingReconciliationAck` (e.g. `"batchId#peerId"`) so it knows when each batch has been acknowledged by each server peer. Callers can use this to run logic after reconciliation (e.g. when `pendingReconciliationAck` is empty for a given peer's run).

7. **Scheduling**: Reconciliation runs at most every 30 days per peer, triggered when peers connect. Lock and lastRun are per peer; reconciliation with different peers can run independently. Storage backends persist lock and `lastRun` per peerId; the lock ensures only one tab/process runs reconciliation for a given peer at a time. Lock TTL is 1 day; if interrupted, another tab/process can acquire when it expires. `lastRun` is only updated on successful completion (via `releaseStorageReconciliationLock`), so interrupted runs do not block the next due run.

## Resuming Storage Reconciliation

If `startStorageReconciliation` is interrupted (tab close, crash, app restart), the run currently starts again from offset 0 on the next acquire. To avoid re-processing already-synced CoValues, we keep the **last processed offset** as part of the lock and resume from that offset when a new run acquires the lock (e.g. after app restart or lock TTL expiry).

### Strategy

1. **Persist progress in the lock row**  
   Extend the storage reconciliation lock row with a required `lastProcessedOffset` (number). Semantics:
   - `0`: start from the beginning (first batch).
   - `N`: all CoValues up to offset `N` (by the same stable ordering as `getCoValueIDs`) have been reconciled for this peer; the next run should call `getCoValueIDs(batchSize, N)` and continue from there.

2. **When to advance the offset (out-of-order acks)**  
   We advance only after the server has acknowledged a batch (client receives **reconcile-ack**).

3. **New storage API**  
   - **Acquire result**: When `tryAcquireStorageReconciliationLock` returns `{ acquired: true }`, also return the offset to resume from. Change the type to:
     - `{ acquired: true; lastProcessedOffset: number }` (0 for a fresh run, or the value from the previous lock row when resuming after an expired lock).
   - **Update progress**: Add a method so the client can persist progress after each acked batch:
     - `renewStorageReconciliationLock(peerId: PeerID, offset: number)` (sync/async as per existing storage style).
   - Implementation: when acquiring, if we take over an existing lock row (expired or due for a new run): if the previous row has `releasedAt` set (completed run), set `lastProcessedOffset` to 0 and return 0 (full run). If the previous row has no `releasedAt` (interrupted run) and we are acquiring because the lock expired, preserve its `lastProcessedOffset` in the new row and return it. When there is no previous row, set `lastProcessedOffset` to 0 and return 0.

4. **Lock row shape**  
   Add to `StorageReconciliationLockRow`: `lastProcessedOffset: number` (required). Backends store it in the same table (e.g. new column). On `putStorageReconciliationLock` when acquiring, the backend sets `lastProcessedOffset` from the previous row when resuming, or 0 when starting fresh. `renewStorageReconciliationLock` only updates `lastProcessedOffset` for the lock row key for that peer (and must only update if the current holder is the same session).

5. **Client flow**  
   - `maybeStartStorageReconciliationForPeer`: on `result.acquired`, read `result.lastProcessedOffset` (0 or from storage) and call `startStorageReconciliation(peer, onComplete, result.lastProcessedOffset)`.
   - `startStorageReconciliation(peer, onComplete?, initialOffset?)`: start the batch loop at `initialOffset ?? 0` (i.e. first call is `processStorageBatch(initialOffset ?? 0)`).
   - When sending a reconcile at offset `O` with length `L`, record `batchId → offset`. On **reconcile-ack** for `batchId`: mark that batch acked and call `renewStorageReconciliationLock(peerId, offset)`.
   - On successful completion, call `releaseStorageReconciliationLock`; the row gets `releasedAt` and `lastProcessedOffset` set to 0.

6. **Ordering and stability**  
   `getCoValueIDs(limit, offset)` uses stable ordering (e.g. `ORDER BY rowID`). Offsets are therefore stable across restarts as long as the ordering is unchanged. New CoValues get new rowIDs at the end, so the “already processed” prefix remains valid. Deletions (if any) are implementation-dependent; SQLite with a monotonic rowID does not reuse IDs, so the prefix remains well-defined.

7. **Completion vs resume**  
   When we call `releaseStorageReconciliationLock` (full completion), set `lastProcessedOffset` to 0 so that the next due run (after 30 days) is a full sweep from the start. Resuming is only for **interrupted** runs (no release called); in that case the row keeps `lastProcessedOffset` and no `releasedAt` (or an old one), and the next acquire after TTL will see the expired lock and return the stored `lastProcessedOffset`.

### Implementation checklist (resume)

- Extend `StorageReconciliationLockRow` with `lastProcessedOffset: number` (required).
- Extend `StorageReconciliationAcquireResult` for `acquired: true` with `lastProcessedOffset: number`.
- Add `renewStorageReconciliationLock(peerId, offset)` to `StorageAPI` and DB clients; implement in SQLite (sync/async) and IndexedDB (migration for new column/store).
- In `tryAcquireStorageReconciliationLock`: when taking over an existing row, return `lastProcessedOffset` from the previous row only when the previous run was interrupted (no `releasedAt`); when the previous run completed (`releasedAt` set), use 0. New row gets the same value so future progress updates are correct.
- In `startStorageReconciliation`: accept optional `initialOffset`, start at `processStorageBatch(initialOffset ?? 0)`.
- In reconcile-ack handler: track `batchId → offset` when sending; on ack, mark batch acked and call `renewStorageReconciliationLock(peerId, offset)`.
- In `releaseStorageReconciliationLock`: when writing the row with `releasedAt`, set `lastProcessedOffset` to 0 so the next due run starts from the beginning.
- Wire `maybeStartStorageReconciliationForPeer` to pass `result.lastProcessedOffset` into `startStorageReconciliation`.

### Testing (resume)

- Test that after an interrupted run (e.g. simulate by not calling release and expiring the lock), the next acquire returns `lastProcessedOffset` equal to the last acked batch’s next offset, and the next run only processes CoValues from that offset onward.
- Test that after a successful completion (release), the next run (when due) starts from offset 0.
- Test that progress is only updated after reconcile-ack for the batch, not just after sending.

## Testing

- Test `startStorageReconciliation(peer)` with CoValues only in storage (single peer)
- Test with CoValues already in memory
- Test with mixed scenarios (some synced, some not)
- Verify that client correctly uploads content when needed after receiving "known" messages
- Verify that server sends "reconcile-ack" with the same `id` after processing each "reconcile" batch
- Verify that client removes entries from `pendingReconciliationAck` when it receives "reconcile-ack" (e.g. one ack per peer per batch)
- **Scheduling / lock**: Test `tryAcquireStorageReconciliationLock` returns `not_due` when `lastRun` for that peer is recent
- **Scheduling / lock**: Test `tryAcquireStorageReconciliationLock` returns `lock_held` when another process holds a non-expired lock for that peer
- **Scheduling / lock**: Test `tryAcquireStorageReconciliationLock` returns `acquired: true` when due and no lock (or lock expired) for that peer
- **Scheduling / lock**: Test that locks are independent per peer (acquiring for peer A does not block peer B)
- **Scheduling / lock**: Test that `lastRun` is only updated on successful completion, not on acquisition
- **Scheduling / lock**: Test with both IndexedDB and SQLite backends
