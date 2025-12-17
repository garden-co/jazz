# Requirements: Data Delete Flow for CoValues

## Introduction

Users need the ability to delete their data in Jazz applications. This requires a secure, efficient mechanism to mark coValues as deleted, prevent further synchronization of deleted data, and eventually remove the data physically while maintaining system integrity.

The delete operation must work across all coValue types (CoMap, CoList, CoStream, etc.) and handle large accounts efficiently. Account and Group coValues cannot be deleted. The system must immediately block synchronization of deleted data to the public while allowing the delete operation itself to propagate. Deleted coValues leave a cryptographic tombstone that prevents data resurrection while enabling eventual physical deletion in batch operations.

This specification defines the requirements for a data deletion flow that allows coValues to be deleted with proper permission checks, immediate sync blocking, and eventual physical deletion.

## User Stories and Acceptance Criteria

### US-1: Admin-Only Delete API

**As a** developer  
**I want to** provide a delete API method that only admins can use  
**So that** delete operations are properly permissioned

**Acceptance Criteria:**
- WHEN code calls `coValue.$jazz.raw.core.deleteCoValue()`
- THEN the method checks that the coValue is not an account or group coValue
- AND if the coValue is an account or group, the operation throws an error
- AND the method checks that the current account has admin permissions on the coValue
- AND if the account is not an admin, the operation throws an error
- AND if the account is an admin and the coValue is not an account or group, the delete operation proceeds

### US-2: Delete Transaction Creation

**As a** system  
**I want to** create delete transactions with specific metadata and session naming  
**So that** deleted coValues are properly marked and identifiable

**Acceptance Criteria:**
- WHEN a delete operation is performed on a coValue
- THEN an unencrypted (trusting) transaction is created
- AND the transaction has meta `{ deleted: true }`
- AND the transaction is created in a session named `{accountId}_session_z{uniqueId}_deleted`
- AND the session naming works for all coValue types
- AND the uniqueId ensures uniqueness across multiple delete operations

### US-3: Sync Server Delete Processing

**As a** sync server  
**I want to** process delete transactions in batches and verify permissions  
**So that** delete operations are efficiently handled and properly authorized

**Acceptance Criteria:**
- WHEN the sync server receives a transaction with meta `{ deleted: true }`
- THEN the server stores the delete operation in a batch store
- AND the server verifies that the transaction author has admin permissions on the coValue
- AND if permissions are invalid, the delete transaction is rejected
- AND if permissions are valid, the delete is queued for batch processing

### US-4: Immediate Sync Blocking

**As a** system  
**I want to** immediately block synchronization of deleted coValues to the public  
**So that** deleted data cannot be synced even before physical deletion

**Acceptance Criteria:**
- WHEN a coValue has a valid delete transaction
- THEN any sync attempts for that coValue are immediately blocked
- AND only the delete session/transaction can be synced
- AND all other content from the coValue is prevented from syncing
- AND this blocking happens immediately upon delete transaction creation, before physical deletion

### US-5: Delete Tombstone

**As a** system  
**I want to** leave a tombstone when a coValue is deleted  
**So that** the deletion is cryptographically verifiable and prevents data resurrection

**Acceptance Criteria:**
- WHEN a coValue is deleted
- THEN a tombstone is created containing:
  - The delete transaction
  - The coValue header
- AND the tombstone persists even after physical data deletion
- AND the tombstone provides cryptographic proof of deletion

### US-6: Storage Shard Delete Handling

**As a** storage shard (node with skipVerify: true)  
**I want to** handle delete transactions without verification but keep tombstones  
**So that** storage shards can efficiently process deletes while maintaining deletion records

**Acceptance Criteria:**
- WHEN a storage shard receives a delete transaction
- THEN it does not verify the delete transaction (due to skipVerify: true)
- AND it stores the tombstone (delete transaction + coValue header)
- AND the tombstone is preserved even though verification is skipped

### US-7: Sync Restriction for Deleted CoValues

**As a** system  
**I want to** restrict what can be synced for deleted coValues  
**So that** only deletion information propagates, not the deleted content

**Acceptance Criteria:**
- WHEN a coValue has a valid deleted session/transaction
- AND the coValue is marked as deleted
- THEN only the delete session/transaction can be synced
- AND all other sessions and transactions from that coValue are blocked from syncing
- AND this restriction is enforced on both clients and servers
