# Atomic Transactions for CoValue Mutations

**Context**
Users need to guarantee that multiple CoValue mutations either all persist together or none of them do. This is critical for maintaining data consistency when changes span multiple CoValues or when a group of related changes must be atomic. Currently, each mutation is stored separately, which can lead to partial states if operations fail midway.

## User Stories

### US-1: Execute Multiple Mutations Atomically

> **As a** Jazz application developer
> **I want** to execute multiple CoValue mutations in a single atomic transaction
> **So that** all changes either succeed together or fail together, preventing partial states

**Acceptance Criteria:**
* The system shall provide a `withTransaction()` method on the account that accepts a **synchronous** callback function (no async, no await; avoids read/write concurrency while the transaction is open)
* When mutations are executed within the callback:
  * All mutations shall be applied immediately to in-memory state (optimistic updates)
  * All mutations shall be buffered and not immediately persisted
* When the callback completes successfully:
  * All mutations shall be persisted to IndexedDB in a single transaction
  * All mutations shall be sent to sync servers in a single network message
  * If either persistence or sync fails, the system shall retry until success
* When the callback throws an error:
  * Mutations executed before the error shall remain in memory
  * No mutations shall be persisted or synced
  * Buffered transaction messages shall be discarded on error
  * The discrepancy between in-memory state and persisted state shall be documented
* The method shall return a Promise that resolves when persistence and sync are complete
* The method shall preserve the per-CoValue mutation order when buffering and sending

---

### US-2: Atomic IndexedDB Storage

> **As a** Jazz application developer
> **I want** all mutations in a transaction to be stored in a single IndexedDB transaction
> **So that** local storage remains consistent even if storage operations fail

**Acceptance Criteria:**
* When storing a batch of mutations:
  * The system shall open a single IndexedDB transaction
  * All mutations shall be written within that transaction
  * The transaction shall either commit all changes or rollback all changes
* If the IndexedDB transaction fails:
  * No partial data shall be written to storage
  * The system shall retry the entire batch with exponential backoff
  * In-memory state shall remain unchanged
* The system shall continue retrying until the transaction succeeds
* Storage implementations used in production (IndexedDB, SQLite) shall provide
  equivalent atomic batch guarantees
* Empty batches shall be treated as a no-op without opening a storage transaction

---

### US-3: Atomic Network Sync

> **As a** Jazz application developer
> **I want** all mutations in a transaction to be sent to sync servers as a single message
> **So that** servers receive and process the entire batch together

**Acceptance Criteria:**
* When syncing a batch of mutations:
  * The system shall create a single `BatchMessage` containing all mutations
  * The message shall be sent as one network request per peer
* When the server receives a `BatchMessage`:
  * The system shall unpack the batch
  * Each mutation shall be processed with existing `handleNewContent()` logic
  * The server may optionally store all mutations in a single database transaction
  * The server shall preserve the order of mutations within the batch
* If network sync fails:
  * The system shall retry sending the entire `BatchMessage`
  * In-memory state and local storage shall remain unchanged
  * The system shall use existing sync state tracking to know when all CoValues are synced
* If a server does not support `BatchMessage`:
  * The client shall fall back to sending individual `NewContentMessage`s
  * The fallback shall preserve per-CoValue ordering
  * The client shall continue retrying until the server acknowledges receipt

---

### US-4: Eventual Consistency with Retry

> **As a** Jazz application developer
> **I want** failed transactions to automatically retry until they succeed
> **So that** I don't need to implement complex retry logic

**Acceptance Criteria:**
* When persistence or sync fails:
  * The system shall NOT rollback in-memory changes
  * The system shall retry with exponential backoff
  * The system shall continue retrying indefinitely until success
* Users shall be able to:
  * Await the transaction promise to wait for full persistence
  * Continue using the app with optimistic in-memory state
  * Optionally specify a timeout for the transaction
* If a timeout is reached:
  * The promise shall reject with a timeout error
  * Retry shall continue in the background until success

---

### US-5: Transaction Context Isolation

> **As a** Jazz application developer
> **I want** mutations outside of `withTransaction()` to behave normally
> **So that** the transaction API is opt-in and doesn't affect existing code

**Acceptance Criteria:**
* Mutations executed outside of `withTransaction()`:
  * Shall be applied immediately to memory
  * Shall be persisted and synced individually (existing behavior)
* Mutations executed inside `withTransaction()`:
  * Shall be buffered and processed as a batch
* The callback shall be synchronous; the system shall not await the callback (no async callbacks)
* Nested transactions shall be handled:
  * Either: flatten into the outer transaction
  * Or: throw an error indicating nesting is not supported

---

## Global Constraints and Edge Cases

* Batch size limits shall be defined (by message count and/or payload size),
  with explicit behavior when exceeded (reject, split, or fallback).
* Batching shall not weaken ordering guarantees for mutations within a CoValue.
* Sync state tracking shall treat batch delivery as individual message delivery.
