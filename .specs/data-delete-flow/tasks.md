# Tasks: Data Delete Flow for CoValues

Numbered checklist of **coding tasks** only. Each task references the relevant requirements in `requirements.md` (US-1…US-7).

1. [x] **Add admin-only delete API on CoValueCore** (Req: US-1)
   - Implement `CoValueCore.deleteCoValue()` in `packages/cojson/src/coValueCore/coValueCore.ts`.
   - Hard-block deletion for **Account** and **Group** coValues (throw).
   - Enforce **admin-only** permissions (throw if not admin).

2. [x] **Create delete transaction with correct meta + session naming** (Req: US-2)
   - Ensure delete creates an **unencrypted/trusting** transaction with `meta: { deleted: true }`.
   - Generate a delete session ID with pattern `{accountId}_session_z{uniqueId}_deleted` (i.e. `_deleted` suffix) that is unique per delete.
   - Ensure the mechanism works across all coValue types.

3. [x] **Allow overriding session ID for transaction creation (delete sessions)** (Req: US-2)
   - Update `makeTransaction(...)` in `packages/cojson/src/coValueCore/coValueCore.ts` to accept an optional `sessionID?: SessionID`.
   - Ensure delete uses the delete session ID and cannot accidentally fall back to the regular session.

4. [x] **Detect delete transactions and validate delete permissions during ingestion** (Req: US-3)
   - In `packages/cojson/src/coValueCore/coValueCore.ts` (`tryAddTransactions` and/or the validity pipeline), detect delete transactions via:
     - session ID ending with `_deleted`, and
     - trusting tx with `meta.deleted === true`.
   - When not `skipVerify`, verify the delete author has **admin** permissions on the coValue.
   - Reject invalid delete transactions (non-admin) with an explicit error result.

5. [x] **Track “deleted” state on CoValueCore and surface it for sync decisions** (Req: US-4, US-7)
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
   - Implement the marker as a **work queue** named `deletedCoValues`:
     - **SQLite family**: new `deletedCoValues(coValueRowID INTEGER PRIMARY KEY)` table via `packages/cojson/src/storage/sqlite/sqliteMigrations.ts` (no `deletedAt` column).
     - **IndexedDB**: new `deletedCoValues` object store with `keyPath: "coValueRowID"` and stored values `{ coValueRowID, id }` (no `deletedAt`).

10. [ ] **Expose batch erase API for physically deleting deleted coValues (preserve tombstones)** (Req: US-5, US-6)
   - Add a storage API method (per design) in:
     - `packages/cojson/src/storage/storageSync.ts`
     - `packages/cojson/src/storage/storageAsync.ts`
   - Implementation should:
     - enumerate deleted IDs via `getAllDeletedCoValueIDs()`
     - perform physical deletion per coValue while preserving tombstone (delete tx + header).
     - treat `deletedCoValues` as a **work queue**: remove each queue entry after successful physical deletion.

11. [ ] **Implement physical deletion primitive: erase all content but keep tombstone** (Req: US-5, US-6)
   - Implement a per-coValue primitive (run inside a single storage transaction) that:
     - deletes **all non-delete sessions** (`sessionID` not matching `*_deleted`) for the coValue
     - deletes their `transactions` and `signatureAfter` rows
     - preserves:
       - the `coValues` row (header)
       - all delete-session(s) (`*_deleted`) and their transactions/signatures (tombstone)
   - After erasure, delete the queue entry:
     - SQLite: `DELETE FROM deletedCoValues WHERE coValueRowID = ?;`
     - IndexedDB: `deletedCoValues.delete(coValueRowID)`
   - Ensure post-delete sync behavior still advertises/serves the tombstone but never historical content.

12. [ ] **Trigger batch erasure in the background (debounced + startup/resume)** (Req: US-5, US-6)
   - Ensure `ereaseAllDeletedCoValues()` is not run inline in latency-sensitive paths; instead:
     - schedule a debounced run **after storing** a delete transaction
     - run once on **startup/resume** to drain any queued entries
   - Add a simple re-entrancy guard:
     - sqlite: in-memory “currently erasing” flag (single-process assumption)
     - IndexedDB: in-memory flag + rely on `readwrite` transaction semantics
   - Enforce batching/time budget:
     - `maxCoValuesPerRun` (e.g. 50–500)
     - optional `maxDurationMs` budget (e.g. 100–300ms)

13. [ ] **Implement “get all deleted IDs” efficiently in DB drivers/implementations** (Req: US-3, US-6)
   - Implement `markCoValueAsDeleted(...)` + `getAllDeletedCoValueIDs()` for each in-repo DBClient:
     - `packages/cojson/src/storage/sqlite/client.ts` (SQLite sync)
     - `packages/cojson/src/storage/sqliteAsync/client.ts` (SQLite async)
     - `packages/cojson-storage-indexeddb/src/idbClient.ts` (IndexedDB async)
   - Add schema migrations/upgrades:
     - SQLite migration creating `deletedCoValues`
     - IndexedDB upgrade creating the `deletedCoValues` object store
   - Keep behavior consistent with the work-queue semantics (queue entry is removed after physical deletion).

14. [ ] **Add unit + integration tests for the full delete flow** (Req: US-1, US-2, US-3, US-4, US-5, US-6, US-7)
   - `deleteCoValue()`:
     - rejects Account/Group deletion
     - rejects non-admin
     - produces trusting tx with `{ deleted: true }` in `{accountId}_session_z{uniqueId}_deleted` session
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
     - batch erase removes the corresponding `deletedCoValues` queue entry after success


