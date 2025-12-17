# Design: Data Delete Flow for CoValues

## Overview

This design implements a secure, efficient data deletion flow for coValues that allows users to self-serve data deletion. The solution uses unencrypted delete transactions with specific metadata, immediate sync blocking, and eventual physical deletion through batch processing. Deleted coValues leave cryptographic tombstones that prevent data resurrection while enabling efficient cleanup.

**Important Constraint**: Account and Group coValues cannot be deleted. This is required to keep the tombstone permissions verifiable.

The design leverages existing transaction validation mechanisms, adding delete-specific checks in `tryAddTransactions`, and implements sync restrictions to prevent deleted content from propagating while allowing delete operations to sync.

## Backward compatibility (wire protocol + mixed-version peers)

This design is intended to be rolled out without introducing new message types or changing the shape of existing sync messages (`load`, `known`, `content`, `done`). Instead, we change *semantics* for deleted coValues while remaining compatible with older peers.

Key points:

- **No protocol versioning required**: delete is represented as an ordinary unencrypted (`trusting`) transaction with meta `{ deleted: true }`, and a dedicated session ID pattern (`{accountId}_deleted_{uniqueId}`).
- **Mixed-version safety**:
  - New peers must **not accept** or **re-propagate** historical content for a deleted coValue (they only accept/sync the tombstone).
  - Old peers may continue to send historical content. New peers handle this by **ignoring** non-delete sessions for deleted coValues (see “Sync message behavior” below). This avoids resurrection even when older peers don’t understand deletion.
- **Sync quenching (“poisoned knownState”)**: when interacting with peers that *do* speak the existing protocol (including older peers), we can prevent them from continuously trying to send historical sessions by replying with the peer optimistic known state plus the deleted session. This makes the sender believe we already have everything and therefore stops further uploads.

## Architecture / Components

### 1. Delete API Method

**Location**: `packages/cojson/src/coValueCore/coValueCore.ts`

**Changes**:
- Add method `deleteCoValue()` to `CoValueCore`:
  - Checks that the coValue is not an Account or Group (throws error if it is)
  - Checks that the current account has admin permissions on the coValue
  - Throws error if not admin
  - Creates delete transaction with meta `{ deleted: true }`
  - Uses session naming pattern: `{accountId}_deleted_{uniqueId}`

**Key Logic**:
```typescript
deleteCoValue(): void {
  if (!this.verified) {
    throw new Error("Cannot delete coValue without verified state");
  }

  const currentAccount = this.node.getCurrentAgent();
  if (!currentAccount) {
    throw new Error("No current account to perform delete");
  }

  // Check that coValue is not an Account or Group
  if (this.verified.header.ruleset.type === "group") {
    throw new Error("Cannot delete Group coValues");
  } else if (this.verified.header.ruleset.type === "unsafeAllowAll") {
    // For unsafeAllowAll, check if it's an account (should have been caught above)
    // This should not be reachable since accounts are blocked earlier
    throw new Error("Cannot delete coValue with unsafeAllowAll ruleset");
  }

  // Check admin permissions
  const group = this.safeGetGroup()
  
  if (group) {
    const role = group.getRoleOf(currentAccount.id);
    if (role !== "admin") {
      throw new Error("Only admins can delete coValues");
    }
  }

  // Generate unique session ID for delete transaction
  const uniqueId = this.crypto.newRandomSessionID(currentAccount.id).split('_session_z')[1];
  const deleteSessionID = `${currentAccount.id}_deleted_${uniqueId}` as SessionID;

  // Create unencrypted (trusting) transaction with delete meta
  // Pass deleteSessionID to makeTransaction to use the delete session
  this.makeTransaction(
    [], // Empty changes array
    "trusting", // Unencrypted
    { deleted: true }, // Delete metadata
    Date.now(),
    deleteSessionID // Use the delete session ID
  );
}
```

### 2. Delete Transaction Creation

**Location**: `packages/cojson/src/coValueCore/coValueCore.ts` - `makeTransaction()`

**Changes**:
- Modify `makeTransaction()` to accept optional `sessionID` parameter
- When creating delete transaction, use the delete session ID pattern
- Ensure delete transactions are always unencrypted (trusting)

**Rationale for Special Delete Session**:
The special session naming pattern (`{accountId}_deleted_{uniqueId}`) is used to provide a simple way to sync the delete marker without carrying extra data. By using a dedicated session for delete transactions, the system can:
- Easily identify delete operations by session ID pattern
- Sync only the delete marker without carrying any history
- Maintain a clean separation between regular transactions and delete operations

**Key Logic**:
```typescript
makeTransaction(
  changes: JsonValue[],
  privacy: "private" | "trusting",
  meta?: JsonObject,
  madeAt?: number,
  sessionID?: SessionID, // Optional override for delete sessions
): boolean {
  // ... existing validation ...

  const effectiveSessionID = sessionID || (
    this.verified.header.meta?.type === "account"
      ? (this.node.currentSessionID.replace(
          this.node.getCurrentAgent().id,
          this.node.getCurrentAgent().currentAgentID(),
        ) as SessionID)
      : this.node.currentSessionID
  );

  // ... rest of transaction creation ...
}
```

### 3. Delete Transaction Validation in tryAddTransactions

**Location**: `packages/cojson/src/coValueCore/coValueCore.ts` - `tryAddTransactions()`

**Changes**:
- After signature verification succeeds, check for delete transactions
- Only parse transaction meta if sessionID contains `_deleted_` (optimization)
- Validate that delete transaction author has admin permissions
- Mark coValue as deleted if valid delete transaction found
- Store delete state for sync blocking

**Key Logic**:
```typescript
tryAddTransactions(
  sessionID: SessionID,
  newTransactions: Transaction[],
  newSignature: Signature,
  skipVerify: boolean = false,
) {
  let isDeleteOperation = false;
  // Only check for delete transactions if sessionID contains '_deleted_'
  if (sessionID.includes('_deleted_') && !this.isGroup() && !this.isAccount()) {
    // Check for delete transactions after successful addition
    for (const tx of newTransactions) {
      if (tx.privacy !== "trusting" || !tx.meta) continue;
      const txMeta = JSON.parse(tx.meta)
      if (txMeta?.deleted === true) {
        // Validate admin permissions for delete
        if (!skipVerify) {
          const deleteAuthor = accountOrAgentIDfromSessionID(sessionID);
          const hasAdminPermission = this.validateDeletePermission(deleteAuthor);
          
          if (!hasAdminPermission) {
            // Mark transaction as invalid
            // This should be handled in determineValidTransactions
            return { type: "NotEnoughPermissions", id: this.id, error: new Error("Not enough permissions to delete the CoValue, transaction rejected") };
          }
        }
        
        // Mark coValue as deleted
        isDeleteOperation = true;
      }
    }
  }

  try {
    this.verified.tryAddTransactions(
      sessionID,
      signerID,
      newTransactions,
      newSignature,
      skipVerify,
    );

    if (isDeleteOperation) this.markAsDeleted(sessionID, tx);

    this.processNewTransactions();
    this.scheduleNotifyUpdate();
    this.invalidateDependants();
  } catch (e) {
    return { type: "InvalidSignature", id: this.id, error: e } as const;
  }
}
```

### 4. Delete Permission Validation

**Location**: `packages/cojson/src/coValueCore/coValueCore.ts` - `validateDeletePermission()`

**Changes**:
- Check that transaction author has admin permissions when meta contains `{ deleted: true }`
- Reject delete transactions from non-admin accounts

**Key Logic**:
```typescript
validateDeletePermission(sessionID: SessionID, tx: Transaction): void {
  const deleteAuthor = accountOrAgentIDfromSessionID(sessionID);
  const groupAtTime = this.safeGetGroup()?.atTime(tx.currentMadeAt);

  if (!groupAtTime) return false;

  return groupAtTime.roleOf(deleteAuthor) === "admin"
}
```

### 5. CoValue Deleted State Tracking

**Location**: `packages/cojson/src/coValueCore/coValueCore.ts` and `packages/cojson/src/coValueCore/verifiedState.ts`

**Changes**:
- Add `isDeleted: boolean` property to `CoValueCore`
- Add `deleteSessionID: SessionID | undefined` to track which session contains the delete transaction

**Key Logic**:
```typescript
// In CoValueCore
public isDeleted = false;
public deleteSessionID: SessionID | undefined;

markAsDeleted(sessionID: SessionID): void {
  this.isDeleted = true;
  this.deleteSessionID = sessionID;
}
```

### 6. Immediate Sync Blocking

**Location**: `packages/cojson/src/sync.ts` - `SyncManager.handleNewContent()` and `SyncManager.syncLocalTransaction()`

**Changes**:
- Before syncing content, check if coValue is deleted
- If deleted, only allow syncing the delete session/transaction (tombstone)
- Block all other content from syncing

**Key Logic**:
```typescript
// In handleNewContent
handleNewContent(
  msg: NewContentMessage,
  from: PeerState | "storage" | "import",
) {
  const coValue = this.node.getCoValue(msg.id);

  // If the coValue is deleted, only accept the delete session.
  // Any other session content must be ignored to prevent resurrection.
  if (coValue.isDeleted && coValue.deleteSessionID) {
    // Keep only the delete session from msg.new (and optionally msg.header).
    // Everything else is ignored.
  }

  // ... rest of handleNewContent ...
}
```

### 6.1 Sync message behavior for deleted CoValues (responses + quenching)

This section defines exactly how existing sync message handlers behave once a coValue is marked as deleted (i.e. `isDeleted === true` and `deleteSessionID` is set).

#### Terminology / constants

- **Delete session**: `deleteSessionID` with pattern `{RawAccountID}_deleted_{string}`.
- **Delete transaction count**: `deleteTxCount = knownState.sessions[deleteSessionID]` (typically `1`).
- **Poison counter**: We reply with the peer optimistic known state plus the deleted session.

#### `load` message

Requirement: for a deleted coValue, the `load` response must:

- report **`deleteSessionID`** with the amount of transactions in that session
- include a **poisoned knownState** (very high counters) to prevent further syncs of historical sessions

Behavior:

- On receiving `LoadMessage` for `id` where the local coValue is deleted and available:
  - Reply with a `KnownStateMessage`:
    - `header: true` (tombstone header is available)
    - `sessions`:
      - `{ [deleteSessionID]: deleteTxCount }`
      - For every session key present in the incoming `msg.sessions` **other than** `deleteSessionID`, send `{ [thatSessionID]: POISON_COUNTER }`
  - Then, sync **only** the delete session content if the requester is behind (normal `content` flow, but limited to delete session).

Rationale:

- The explicit delete session counter allows a peer that is missing the tombstone to request/receive it.
- The poisoned counters ensure a peer that still has historical sessions will not keep trying to upload them after deletion.

#### `content` message

- For deleted coValues, only `content.new[deleteSessionID]` is accepted/applied/stored.
- Any other `content.new` session entries MUST be ignored (and must not be stored or forwarded).
- If the incoming `content` contains `header`, it may be accepted if needed to complete the tombstone (header must remain available for verification).

#### `known` message

- For deleted coValues, known-state merging/tracking should be treated as **delete-session-only** for purposes of sync completion and progress tracking.
- (Optional optimization) peers may continue to exchange full knownStates, but wait/sync checks must only consider the delete session once it exists.

#### `done` message

No special behavior required. It remains a transport-level signal.

### 6.2 `waitForSync` semantics for deleted CoValues

Once `deleteSessionID` exists for a coValue, `waitForSync()` must only wait for:

- the tombstone/header (if applicable in the specific wait path), and
- the **delete session counter** to be fully uploaded/stored

It must **not** wait for historical sessions, because those are intentionally blocked from syncing after deletion (and may never be uploaded/stored on some peers).

### 7. DBClient Deleted CoValue Tracking

**Location**: `packages/cojson/src/storage/types.ts` and DBClient implementations

**Changes**:
- DBClient needs to track deleted coValues to enable quick scanning for batch delete operations
- When a delete transaction is stored, mark the coValue as deleted in the database
- This allows efficient querying of all deleted coValues without scanning all coValues

**Key Logic**:
```typescript
// In DBClient interface (types.ts)
export interface DBClientInterfaceAsync {
  // ... existing methods ...
  
  // Mark coValue as deleted when delete transaction is stored
  markCoValueAsDeleted(coValueRowID: number): Promise<void>;
  
  // Get all deleted coValue IDs for batch processing
  getAllDeletedCoValueIDs(): Promise<RawCoID[]>;
}

export interface DBClientInterfaceSync {
  // ... existing methods ...
  
  markCoValueAsDeleted(coValueRowID: number): void;
  getAllDeletedCoValueIDs(): RawCoID[];
}
```

**Implementation Notes (by adapter)**:

The repository currently has three DBClient implementations:

- SQLite (sync): `packages/cojson/src/storage/sqlite/client.ts` (`SQLiteClient`)
- SQLite (async): `packages/cojson/src/storage/sqliteAsync/client.ts` (`SQLiteClientAsync`)
- IndexedDB (async): `packages/cojson-storage-indexeddb/src/idbClient.ts` (`IDBClient`)

Additionally, these packages provide **SQLite drivers** that reuse the same SQLite storage implementation and therefore inherit the SQLite behavior below:

- `packages/cojson-storage-sqlite` (better-sqlite3 driver)
- `packages/cojson-storage-do-sqlite` (Cloudflare Durable Object SQLite driver)

#### 7.1 SQLite family (sync + async)

**Schema**

The SQLite schema is managed by `packages/cojson/src/storage/sqlite/sqliteMigrations.ts` (used by both `getSqliteStorage` and `getSqliteStorageAsync`).

Add a migration that introduces a separate table for deleted coValues:

```sql
CREATE TABLE IF NOT EXISTS deletedCoValues (
  coValueRowID INTEGER PRIMARY KEY
  -- Optional (if foreign keys are enabled in the embedding runtime):
  -- , FOREIGN KEY (coValueRowID) REFERENCES coValues(rowID) ON DELETE CASCADE
);
```

**Implementation: `markCoValueAsDeleted(coValueRowID)`**

SQLite implementation is an idempotent insert (works for both sync and async variants):

```sql
INSERT OR IGNORE INTO deletedCoValues (coValueRowID) VALUES (?);
```

Notes:
- the storage write path already has both `msg.id` and `storedCoValueRowID`; passing `storedCoValueRowID` is sufficient.

**Implementation: `getAllDeletedCoValueIDs()`**

```sql
SELECT c.id
FROM deletedCoValues d
JOIN coValues c ON c.rowID = d.coValueRowID;
```

#### 7.2 IndexedDB (cojson-storage-indexeddb)

**Schema**

The IndexedDB schema is defined in `packages/cojson-storage-indexeddb/src/idbNode.ts` (inside `onupgradeneeded`).

Add a new database version (bump from `indexedDB.open(name, 4)` to the next version) and introduce a new store:

- Create `deletedCoValues` store with `keyPath: "coValueRowID"` and values containing:
  - `coValueRowID: number` (the `coValues.rowID`)
  - `id: RawCoID` (duplicated for efficient reads)

**Implementation: `markCoValueAsDeleted(coValueRowID)`**

Because `IDBClient` uses a transactional wrapper (`CoJsonIDBTransaction`), implement `markCoValueAsDeleted` by:

1. `get` the `coValues` record by primary key (`rowID`) to retrieve the `id`
2. `put` `{ coValueRowID, id }` into the `deletedCoValues` store (idempotent via key)

This should run inside the same write transaction that stores the delete transaction so the marker is not lost if the write rolls back.

**Implementation: `getAllDeletedCoValueIDs()`**

Read `getAll()` from the `deletedCoValues` store and return the `id` field from each row.

### 8. Storage API Batch Delete

**Location**: `packages/cojson/src/storage/storageSync.ts` and `packages/cojson/src/storage/storageAsync.ts`

**Changes**:
- Storage API should expose a method to delete all deleted coValues
- This method uses the DBClient's `getAllDeletedCoValueIDs()` to get the list
- For each deleted coValue, perform physical deletion while preserving tombstone

**Key Logic**:
```typescript
// In StorageAPI interface
export interface StorageAPI {
  // ... existing methods ...
  
  // Delete all deleted coValues (batch operation)
  ereaseAllDeletedCoValues(): Promise<void>;
}

// Implementation in StorageApiAsync/StorageApiSync
async ereaseAllDeletedCoValues(): Promise<void> {
  const deletedCoValueIDs = await this.dbClient.getAllDeletedCoValueIDs();
  
  for (const coValueID of deletedCoValueIDs) {
    // Perform physical deletion while preserving tombstone
    await this.performPhysicalDelete(coValueID);
  }
}
```

### 9. DBDriver Deleted CoValue Extraction

**Location**: `packages/cojson/src/storage/sqlite/types.ts` and DBDriver implementations

**Changes**:
- DBDriver should have a method to extract all deleted coValue IDs
- This enables efficient batch processing without loading all coValues into memory
- The method should return only the IDs, not full coValue data

**Key Logic**:
```typescript
// In SQLiteDatabaseDriver interface
export interface SQLiteDatabaseDriver {
  // ... existing methods ...
  
  // Get all deleted coValue IDs
  getAllDeletedCoValueIDs(): RawCoID[];
}

export interface SQLiteDatabaseDriverAsync {
  // ... existing methods ...
  
  getAllDeletedCoValueIDs(): Promise<RawCoID[]>;
}

// Implementation example (SQL)
// SELECT coValueRowID FROM deletedCoValues
```

### 10. Delete Tombstone

**Location**: `packages/cojson/src/storage/storageSync.ts` and `packages/cojson/src/storage/storageAsync.ts`

**Changes**:
- When storing delete transaction, ensure tombstone is preserved
- Tombstone consists of: delete transaction + delete session + coValue header
- Even after physical deletion, tombstone remains

**Key Logic**:
```typescript
// When physical delete happens, preserve tombstone
async performPhysicalDelete(coValueID: RawCoID) {
  // Delete all sessions except delete session
  // Delete all transactions except delete transaction
  // Keep header and delete transaction (tombstone)
  await this.dbClient.deleteCoValueContent(coValueID);
}
```

### 11. Storage Shard Handling (skipVerify: true)

**Location**: `packages/cojson/src/storage/storageSync.ts` and `packages/cojson/src/storage/storageAsync.ts`

**Changes**:
- Storage shards with `skipVerify: true` don't verify delete transactions
- But they still delete the value and block sync

## Data Models

### Delete Transaction Metadata

```typescript
type DeleteTransactionMeta = {
  deleted: true;
};

type DeleteSessionID = `${RawAccountID}_deleted_${string}`;
```

### CoValue Deleted State

```typescript
type CoValueDeletedState = {
  isDeleted: boolean;
  deleteSessionID: SessionID | undefined;
};
```


## Error Handling / Testing Strategy

### Error Cases

1. **Account or Group Delete Attempt**:
   - If trying to delete an Account or Group coValue
   - **Handling**: Throw error immediately before creating transaction
   - **Test**: Attempt delete on Account or Group, verify error thrown

2. **Non-Admin Delete Attempt**:
   - If non-admin tries to call `deleteCoValue()`
   - **Handling**: Throw error immediately before creating transaction
   - **Test**: Attempt delete as non-admin, verify error thrown

3. **Invalid Delete Transaction**:
   - If delete transaction from non-admin is received
   - **Handling**: Mark transaction as invalid in `determineValidTransactions()`
   - **Test**: Send delete transaction from non-admin, verify rejection

4. **Delete on Already Deleted CoValue**:
   - If trying to delete an already deleted coValue
   - **Handling**: Allow (idempotent) or reject with error
   - **Test**: Attempt delete on already deleted coValue

5. **Sync Blocking Failure**:
   - If sync blocking doesn't work correctly
   - **Handling**: Ensure deleted coValues can only sync delete session
   - **Test**: Try to sync non-delete content from deleted coValue, verify blocking

### Testing Strategy

1. **Unit Tests**:
   - `deleteCoValue()` - verify Account/Group check, admin check and transaction creation
   - `tryAddTransactions()` - verify delete detection and state marking
   - `determineValidTransactions()` - verify delete permission validation
   - Sync blocking logic - verify only delete session syncs
   - Tombstone storage - verify tombstone preservation
   - `DBClient.markCoValueAsDeleted()` - verify deleted coValues are tracked
   - `DBClient.getAllDeletedCoValueIDs()` - verify correct IDs are returned
   - `StorageAPI.ereaseAllDeletedCoValues()` - verify batch deletion

2. **Integration Tests**:
   - End-to-end delete flow: Create coValue, delete it, verify state
   - Account deletion: Delete account, verify deletion state
   - Sync blocking: Delete coValue, try to sync, verify blocking
   - Batch delete: Multiple deletes, verify DBClient tracking and Storage API batch deletion
   - Tombstone: Delete coValue, physically delete, verify tombstone

3. **Edge Case Tests**:
   - Delete during active sync: Ensure no race conditions
   - Multiple delete transactions: Handle idempotency
   - Delete on large coValues: Verify performance
   - Storage shard deletes: Verify tombstone preservation
   - Concurrent deletes: Ensure consistency

4. **Stress Tests**:
   - Many coValues deleted simultaneously
   - Large accounts with many coValues
   - Rapid delete/sync operations
   - Storage performance with many tombstones

### Test Scenarios

**Scenario 1: Basic Delete Flow**
1. Create a coValue
2. Call `deleteCoValue()` as admin
3. Verify delete transaction created with correct meta
4. Verify coValue marked as deleted
5. Verify sync blocking works
6. Verify tombstone stored

**Scenario 2: Account Self-Deletion**
1. User clicks "Delete account" button
2. System identifies sensitive coValues
3. Delete operations performed on each
4. Account itself deleted
5. Verify account is marked as deleted

**Scenario 3: Batch Delete Processing**
1. Multiple coValues deleted (delete transactions stored)
2. DBClient tracks deleted coValues
3. Storage API calls `ereaseAllDeletedCoValues()`
4. DBDriver extracts all deleted coValue IDs via `getAllDeletedCoValueIDs()`
5. Physical deletion performed for each deleted coValue
6. Tombstones preserved

**Scenario 4: Storage Shard Delete**
1. Delete transaction sent to storage shard (skipVerify: true)
2. Shard doesn't verify but stores tombstone
3. Tombstone preserved after physical delete
4. CoValue marked as deleted

**Scenario 5: Sync Restriction**
1. CoValue deleted
2. Try to sync non-delete content
3. Verify sync blocked
4. Try to sync delete session
5. Verify delete session syncs successfully

