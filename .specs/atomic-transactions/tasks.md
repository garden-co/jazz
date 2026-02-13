# Implementation Tasks

## Phase 1: Core Infrastructure (US-5)

- [ ] **Task 1**: Create TransactionContext class
  - Create new file `packages/cojson/src/transactionContext.ts`
  - Implement `TransactionContext` class with methods:
    - `bufferMessage(msg: NewContentMessage): void`
    - `getPendingMessages(): NewContentMessage[]`
    - `getCoValueIds(): Set<RawCoID>`
    - `isActive(): boolean`
    - `clear(): void`
  - Add unit tests for all methods

- [ ] **Task 2**: Add transaction context to LocalNode
  - Add `transactionContext?: TransactionContext` property to `LocalNode` in `packages/cojson/src/localNode.ts`
  - Add `getTransactionContext(): TransactionContext | undefined` method
  - Export for use by CoValueCore

- [ ] **Task 3**: Implement LocalNode.withTransaction()
  - Add `withTransaction<T>(callback: () => T): Promise<T>` method to `LocalNode`
  - Callback must be **synchronous** (invoke with `callback()`, do not await)
  - Create `TransactionContext` before executing callback
  - Execute callback and capture result/errors
  - Get buffered messages after callback completes
  - Call `syncManager.syncAtomicBatch()` with messages
  - Clean up transaction context in `finally` block
  - Handle nested transactions (throw error)
  - Support optional timeout (reject promise, keep retries running)
  - Add unit tests

## Phase 2: Intercept Mutations (US-1, US-5)

- [ ] **Task 4**: Modify CoValueCore.makeTransaction()
  - Update `makeTransaction()` in `packages/cojson/src/coValueCore/coValueCore.ts` (line ~1166)
  - Check if `node.getTransactionContext()` is active
  - If active: buffer `NewContentMessage` instead of syncing
  - If not active: sync immediately (existing behavior)
  - Ensure mutations still applied to memory in both cases
  - Add unit tests for both paths

## Phase 3: Protocol Extension (US-3)

- [ ] **Task 5**: Add BatchMessage type
  - Add `BatchMessage` type to `packages/cojson/src/sync.ts` (around line 32):
    ```typescript
    export type BatchMessage = {
      action: "batch";
      messages: NewContentMessage[];
    };
    ```
  - Update `SyncMessage` union to include `BatchMessage`
  - Export type from `packages/cojson/src/exports.ts`

- [ ] **Task 6**: Add handleBatch() to SyncManager
  - Add `handleBatch(msg: BatchMessage, peer: PeerState)` method to `SyncManager` in `packages/cojson/src/sync.ts`
  - Loop through `msg.messages` and call `this.handleNewContent(message, peer)` for each
  - Update `handleSyncMessage()` switch statement to route `"batch"` action
  - Add unit tests

## Phase 4: Storage Layer (US-2)

- [ ] **Task 7**: Add storeAtomicBatch() to StorageAPI interface
  - Add method signature to `StorageAPI` interface in `packages/cojson/src/storage/types.ts`:
    ```typescript
    storeAtomicBatch(messages: NewContentMessage[]): Promise<void>;
    ```

- [ ] **Task 8**: Implement storeAtomicBatch() in StorageApiAsync
  - Implement method in `packages/cojson/src/storage/storageAsync.ts`
  - Use `dbClient.transaction()` to wrap all stores
  - Call `storeSingleInTransaction()` for each message
  - Throw error on failure so the caller can decide how to handle it
  - Add unit tests

- [ ] **Task 9**: Create storeSingleInTransaction() helper
  - Add private method `storeSingleInTransaction(msg: NewContentMessage, tx: IDBTransaction)` to `StorageApiAsync`
  - Extract logic from existing `storeSingle()` method
  - Adapt to work within an existing transaction context
  - Store CoValue header, sessions, transactions, and signatures

- [ ] **Task 10**: Implement storeAtomicBatch() in StorageApiSync
  - Implement method in `packages/cojson/src/storage/storageSync.ts`
  - Similar to async version but synchronous
  - Use SQLite transaction support
  - Add unit tests

## Phase 5: Sync Layer (US-3, US-4)

- [ ] **Task 11**: Implement SyncManager.syncAtomicBatch()
  - Add method to `SyncManager` in `packages/cojson/src/sync.ts`
  - Extract CoValue IDs from messages
  - Call `storage.storeAtomicBatch()` once for IndexedDB persistence (no retry loop)
  - Call `syncBatchToServers()` for network sync
  - Run both in parallel with `Promise.all()`
  - Short-circuit empty batches (no-op)
  - Return Promise that resolves when both complete

- [ ] **Task 13**: Implement syncBatchToServers()
  - Add private method to `SyncManager`
  - Create single `BatchMessage` containing all messages
  - Send to all server peers using `trySendToPeer()`
  - Detect peers that do not support batching and fall back to ordered
    `NewContentMessage`s
  - Enforce batch size limits (reject or split) with explicit behavior
  - Call `waitForBatchSynced()` to wait for completion
  - Add unit tests

// Task 14 removed: no automatic network retry or polling for batch sync

## Phase 6: Public API (US-1)

- [ ] **Task 15**: Add withTransaction() to AccountJazzApi
  - Add method to `AccountJazzApi` class in `packages/jazz-tools/src/tools/coValues/account.ts`
  - Delegate to `this.localNode.withTransaction(callback)`
  - Signature: sync callback `() => T`; document that callback must not be async
  - Accept and pass through optional timeout configuration
  - Add JSDoc documentation with example (sync callback)
  - Export for public use

## Phase 7: Testing

### Unit Tests

- [ ] **Task 16**: Test TransactionContext
  - Test bufferMessage() adds messages correctly
  - Test getPendingMessages() returns all buffered messages
  - Test getCoValueIds() returns unique CoValue IDs
  - Test clear() empties the buffer
  - Location: `packages/cojson/src/tests/transactionContext.test.ts`

- [ ] **Task 17**: Test LocalNode.withTransaction()
  - Test creates and cleans up transaction context
  - Test callback is invoked synchronously (no await before it completes)
  - Test callback result is returned
  - Test errors in callback are propagated
  - Test nested transactions throw error
  - Test calls syncAtomicBatch() with buffered messages
  - Location: `packages/cojson/src/tests/localNode.transactions.test.ts`

- [ ] **Task 18**: Test CoValueCore mutation buffering
  - Test makeTransaction() buffers when context active
  - Test makeTransaction() syncs immediately when context inactive
  - Test mutations applied to memory in both cases
  - Location: `packages/cojson/src/tests/coValueCore.transactions.test.ts`

- [ ] **Task 19**: Test SyncManager.handleBatch()
  - Test unpacks BatchMessage correctly
  - Test calls handleNewContent() for each message
  - Test with empty batch
  - Test with large batch
  - Test preserves per-CoValue ordering
  - Location: `packages/cojson/src/tests/sync.batch.test.ts`

- [ ] **Task 20**: Test StorageApiAsync.storeAtomicBatch()
  - Test stores all messages in single transaction
  - Test rolls back on failure
  - Test throws error for retry
  - Test with empty batch
  - Test with messages for different CoValues
  - Location: `packages/cojson/src/tests/storageAsync.batch.test.ts`

- [ ] **Task 21**: Test SyncManager batch sync logic
  - Test SyncManager.syncAtomicBatch() calls `storage.storeAtomicBatch()` once
  - Test `syncBatchToServers()` sends a `BatchMessage` to all relevant server peers
  - Ensure no automatic retry or backoff logic is present for failed storage or network operations
  - Location: `packages/cojson/src/tests/sync.atomicBatch.test.ts`

### Integration Tests

- [ ] **Task 22**: Test successful transaction end-to-end
  - Create account and CoValues
  - Execute multiple mutations in withTransaction()
  - Verify all mutations applied to memory
  - Verify all stored in IndexedDB
  - Verify BatchMessage sent to server
  - Verify server receives and processes batch
  - Location: `packages/jazz-tools/src/tools/tests/transactions.success.test.ts`

- [ ] **Task 23**: Test transaction with error
  - Execute mutations in withTransaction()
  - Throw error mid-callback
  - Verify mutations before error remain in memory
  - Verify no persistence occurred
  - Verify no sync occurred
  - Location: `packages/jazz-tools/src/tools/tests/transactions.error.test.ts`

- [ ] **Task 24**: Test storage failure (no automatic retry)
  - Mock IndexedDB to fail on `storeAtomicBatch()`
  - Execute transaction
  - Verify the failure is surfaced to the caller of `withTransaction()`
  - Verify mutations remain in memory despite the storage failure
  - Location: `packages/jazz-tools/src/tools/tests/transactions.storageRetry.test.ts`

- [ ] **Task 25**: Test network sync failure (no automatic retry)
  - Execute transaction with server disconnected
  - Verify a single send attempt is made
  - Verify failure is surfaced to the caller and not retried automatically
  - Location: `packages/jazz-tools/src/tools/tests/transactions.syncRetry.test.ts`

- [ ] **Task 26**: Test server batch processing
  - Client sends BatchMessage to server
  - Verify server receives single message
  - Verify server unpacks and processes each mutation
  - Verify all CoValues updated on server
  - Verify other clients receive updates
  - Location: `tests/e2e/transactions.server.test.ts`

- [ ] **Task 27**: Test nested transactions
  - Execute withTransaction()
  - Inside callback, call withTransaction() again
  - Verify error is thrown
  - Verify outer transaction is not affected
  - Location: `packages/jazz-tools/src/tools/tests/transactions.nested.test.ts`

- [ ] **Task 28**: Test sync callback semantics
  - Verify callback is run to completion before any persistence/sync
  - Verify no other code can run during callback (e.g. no microtask interleaving)
  - Optionally: reject or document behavior if callback returns a Promise
  - Location: `packages/jazz-tools/src/tools/tests/transactions.syncCallback.test.ts`

### Performance Tests

- [ ] **Task 29**: Benchmark transaction sizes
  - Test with 1, 10, 100, 1000 mutations
  - Measure time to complete
  - Measure memory usage
  - Compare with individual mutations
  - Location: `bench/jazz-tools/transactions.bench.ts`

- [ ] **Task 30**: Benchmark IndexedDB batching
  - Compare single transaction vs individual stores
  - Measure throughput (mutations/second)
  - Test with concurrent transactions
  - Location: `bench/cojson-storage-indexeddb/batch.bench.ts`

- [ ] **Task 31**: Benchmark network batching
  - Compare BatchMessage vs individual NewContentMessages
  - Measure network payload size
  - Measure server processing time
  - Location: `bench/cojson-transport-ws/batch.bench.ts`

## Phase 8: Documentation

- [ ] **Task 32**: Add API documentation
  - Document `withTransaction()` method
  - Document that callback must be synchronous (no async/await)
  - Add usage examples to docs site (sync callback)
  - Document atomicity guarantees
  - Document that the framework does not perform automatic retries; application code is responsible for any retry policy
  - Document error handling

- [ ] **Task 33**: Update migration guide
  - Explain when to use transactions
  - Show before/after code examples
  - Document performance considerations
  - Add troubleshooting section

- [ ] **Task 34**: Create example application
  - Build simple app demonstrating transactions
  - Show multi-CoValue updates
  - Show error handling
  - Add to `examples/` directory

## Phase 9: Compatibility and Safety

- [ ] **Task 35**: Define batch size limits
  - Decide limits by count and/or payload size
  - Implement explicit behavior when exceeded (reject, split, or fallback)
  - Add unit tests for limit handling

- [ ] **Task 36**: Add server batch capability detection
  - Negotiate capability during connection
  - Persist capability per peer
  - Use capability to choose batch vs individual messages

- [ ] **Task 37**: Add timeout option to transaction API
  - Extend API signature to accept timeout configuration
  - Implement rejection behavior without stopping retries
  - Add unit tests
