# Design: Data Delete Flow for CoValues

## Overview

This design implements a secure, efficient data deletion flow for coValues that allows users to self-serve data deletion. The solution uses unencrypted delete transactions with specific metadata, immediate sync blocking, and eventual physical deletion through batch processing. Deleted coValues leave cryptographic tombstones that prevent data resurrection while enabling efficient cleanup.

**Important Constraint**: Account and Group coValues cannot be deleted. This is required to keep the tombstone permissions verifiable.

The design leverages existing transaction validation mechanisms, adding delete-specific checks in `tryAddTransactions`, and implements sync restrictions to prevent deleted content from propagating while allowing delete operations to sync.

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
  // ... existing signature verification ...

  try {
    this.verified.tryAddTransactions(
      sessionID,
      signerID,
      newTransactions,
      newSignature,
      skipVerify,
    );

    // Only check for delete transactions if sessionID contains '_deleted_'
    if (sessionID.includes('_deleted_') && !this.isGroup() && !this.isAccount()) {
      // Check for delete transactions after successful addition
      for (const tx of newTransactions) {
        const txMeta = this.parseTransactionMeta(tx);
        if (txMeta?.deleted === true) {
          // Validate admin permissions for delete
          if (!skipVerify) {
            const deleteAuthor = accountOrAgentIDfromSessionID(sessionID);
            const hasAdminPermission = this.validateDeletePermission(deleteAuthor);
            
            if (!hasAdminPermission) {
              // Mark transaction as invalid
              // This should be handled in determineValidTransactions
              continue;
            }
          }
          
          // Mark coValue as deleted
          this.markAsDeleted(sessionID, tx);
        }
      }
    }

    this.processNewTransactions();
    this.scheduleNotifyUpdate();
    this.invalidateDependants();
  } catch (e) {
    return { type: "InvalidSignature", id: this.id, error: e } as const;
  }
}
```

### 4. Delete Permission Validation

**Location**: `packages/cojson/src/permissions.ts` - `determineValidTransactions()`

**Changes**:
- Add validation for delete transactions in `determineValidTransactions()`
- Check that transaction author has admin permissions when meta contains `{ deleted: true }`
- Reject delete transactions from non-admin accounts

**Key Logic**:
```typescript
export function determineValidTransactions(coValue: CoValueCore): void {
  // ... existing validation logic ...

  for (const tx of coValue.toValidateTransactions) {
    // Check for delete transactions
    if (tx.meta?.deleted === true) {
      // Only admins can delete
      if (transactorRoleAtTxTime !== "admin") {
        tx.markInvalid("Only admins can delete coValues", {
          transactor: tx.author,
          transactorRole: transactorRoleAtTxTime ?? "undefined",
        });
        continue;
      }
    }
    // ... existing transaction validation ...
  }
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
private _isDeleted = false;
private _deleteSessionID: SessionID | undefined;

get isDeleted(): boolean {
  return this._isDeleted;
}

markAsDeleted(sessionID: SessionID, transaction: Transaction): void {
  this._isDeleted = true;
  this._deleteSessionID = sessionID;
  this._deleteTransaction = transaction;
}
```

### 6. Immediate Sync Blocking

**Location**: `packages/cojson/src/sync.ts` - `SyncManager.handleNewContent()` and `SyncManager.syncLocalTransaction()`

**Changes**:
- Before syncing content, check if coValue is deleted
- If deleted, only allow syncing the delete session/transaction
- Block all other content from syncing

**Key Logic**:
```typescript
// In handleNewContent
handleNewContent(
  msg: NewContentMessage,
  from: PeerState | "storage" | "import",
) {
  const coValue = this.node.getCoValue(msg.id);
  
  // Check if coValue is deleted
  if (coValue.isDeleted && coValue.deleteSessionID) {
    return;
  }

  // ... rest of handleNewContent ...
}

// In syncLocalTransaction
syncLocalTransaction(
  coValue: VerifiedState,
  knownStateBefore: CoValueKnownState,
) {
  const core = this.node.getCoValueCore(coValue.id);
  
  if (core.isDeleted && core.deleteSessionID) {
    return;
  }

  // ... normal sync logic ...
}
```

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

**Implementation Notes**:
- When storing a delete transaction, call `markCoValueAsDeleted()` to update the database
- This can be done by adding a `deleted: boolean` column to the `coValues` table or a separate `deletedCoValues` table
- The tracking happens automatically when delete transactions are stored via normal storage flow

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
// SELECT id FROM coValues WHERE deleted = true;
// or
// SELECT coValueID FROM deletedCoValues;
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

