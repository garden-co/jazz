# Tasks: Data Delete Flow for CoValues

Numbered checklist of **coding tasks** only. Each task references the relevant requirements in `requirements.md` (US-1…US-7).

1. [ ] **Add admin-only delete API on CoValueCore** (Req: US-1)
   - Implement `CoValueCore.deleteCoValue()` in `packages/cojson/src/coValueCore/coValueCore.ts`.
   - Hard-block deletion for **Account** and **Group** coValues (throw).
   - Enforce **admin-only** permissions (throw if not admin).

2. [ ] **Create delete transaction with correct meta + session naming** (Req: US-2)
   - Ensure delete creates an **unencrypted/trusting** transaction with `meta: { deleted: true }`.
   - Generate a delete session ID with pattern `{accountId}_deleted_{uniqueId}` that is unique per delete.
   - Ensure the mechanism works across all coValue types.

3. [ ] **Allow overriding session ID for transaction creation (delete sessions)** (Req: US-2)
   - Update `makeTransaction(...)` in `packages/cojson/src/coValueCore/coValueCore.ts` to accept an optional `sessionID?: SessionID`.
   - Ensure delete uses the delete session ID and cannot accidentally fall back to the regular session.

4. [ ] **Detect delete transactions and validate delete permissions during ingestion** (Req: US-3)
   - In `packages/cojson/src/coValueCore/coValueCore.ts` (`tryAddTransactions` and/or the validity pipeline), detect delete transactions via:
     - session ID containing `_deleted_`, and
     - trusting tx with `meta.deleted === true`.
   - When not `skipVerify`, verify the delete author has **admin** permissions on the coValue.
   - Reject invalid delete transactions (non-admin) with an explicit error result.

5. [ ] **Track “deleted” state on CoValueCore and surface it for sync decisions** (Req: US-4, US-7)
   - Add and wire `isDeleted: boolean` and `deleteSessionID?: SessionID` on `CoValueCore` (and any necessary persistence/derivation via `verifiedState`).
   - Mark the coValue as deleted when a valid delete transaction is accepted.

6. [ ] **Client-side immediate sync blocking: only allow delete session/tx to sync** (Req: US-4, US-7)
   - In sync logic (per design: `packages/cojson/src/sync.ts`), ensure that once `isDeleted` is true:
     - inbound `content` ignores any non-delete sessions/transactions
     - outbound sync only uploads the delete session/transaction (tombstone) for that coValue
     - historical content is blocked from syncing immediately

7. [ ] **Implement “poisoned knownState” quenching for deleted coValues** (Req: US-4, US-7)
   - In `load` handling, when local coValue is deleted:
     - respond with `deleteSessionID` counter, and
     - poison counters for other sessions present in the requester’s `msg.sessions` to stop repeated uploads of historical content.
   - Ensure this remains wire-compatible with existing `load/known/content/done` shapes.

8. [ ] **Update `waitForSync` semantics for deleted coValues** (Req: US-7)
   - Once a delete session exists, `waitForSync()` must wait only for:
     - tombstone/header as applicable, and
     - the delete session counter to be uploaded/stored.
   - Ensure it does **not** wait for historical sessions for deleted values.

9. [ ] **Persist “deleted coValue” marker in storage when delete tx is stored** (Req: US-3, US-6)
   - Extend DB client interfaces in `packages/cojson/src/storage/types.ts`:
     - `markCoValueAsDeleted(...)`
     - `getAllDeletedCoValueIDs(...)`
   - Wire these calls into the normal storage path when a delete transaction is committed/stored.
   - Ensure storage shards (`skipVerify: true`) still persist the deleted marker and tombstone without doing permission verification.

10. [ ] **Expose batch erase API for physically deleting deleted coValues (preserve tombstones)** (Req: US-5, US-6)
   - Add a storage API method (per design) in:
     - `packages/cojson/src/storage/storageSync.ts`
     - `packages/cojson/src/storage/storageAsync.ts`
   - Implementation should:
     - enumerate deleted IDs via `getAllDeletedCoValueIDs()`
     - perform physical deletion per coValue while preserving tombstone (delete tx + header).

11. [ ] **Implement physical deletion primitive that preserves tombstone** (Req: US-5, US-6)
   - Add/extend storage deletion functions to remove:
     - all non-delete sessions and their transactions
   - Preserve:
     - header
     - delete session + delete transaction
   - Ensure post-delete sync behavior still advertises/serves the tombstone but never historical content.

12. [ ] **Implement “get all deleted IDs” efficiently in DB drivers/implementations** (Req: US-3, US-6)
   - Add driver-level support where appropriate (per design section 9), e.g. sqlite driver types + query.
   - Implement for each relevant storage backend in-repo (sqlite/indexeddb/etc.), keeping interface parity between sync + async DB clients.

13. [ ] **Add unit + integration tests for the full delete flow** (Req: US-1, US-2, US-3, US-4, US-5, US-6, US-7)
   - `deleteCoValue()`:
     - rejects Account/Group deletion
     - rejects non-admin
     - produces trusting tx with `{ deleted: true }` in `{accountId}_deleted_{uniqueId}` session
   - Transaction ingestion:
     - accepts valid admin delete
     - rejects non-admin delete (when verifying)
     - sets `isDeleted/deleteSessionID`
   - Sync behavior:
     - blocks non-delete sessions immediately
     - accepts/syncs delete session only
     - quenching via poisoned knownState works against peers sending historical sessions
     - `waitForSync` completes based on delete session only
   - Storage behavior:
     - deleted marker persisted
     - batch erase removes content but preserves tombstone
     - storage shard (`skipVerify`) keeps tombstone + deleted marker


