# Storage Content Streaming Queue - Tasks

## Implementation Checklist

### Phase 1: StorageStreamingQueue

- [x] **1. Create `StorageStreamingQueue` class** (US-1, US-2)
  - Create new file `packages/cojson/src/queue/StorageStreamingQueue.ts`
  - Implement priority-based queue using `LinkedList` pattern
  - Handles all priorities (HIGH, MEDIUM, LOW) with priority ordering: HIGH > MEDIUM > LOW
  - Implement `push(callback: ContentCallback, priority: CoValuePriority)` with separate params
  - Implement `pull()` returning `ContentCallback | undefined` directly
  - Implement `isEmpty()` method
  - Implement `setListener()` and `emit()` for triggering processing

- [x] **2. Add unit tests for `StorageStreamingQueue`** (US-1, US-2)
  - Test `push` and `pull` in priority order (HIGH > MEDIUM > LOW)
  - Test callback is not invoked until caller invokes it after pull
  - Test `isEmpty()` returns correct state
  - Test `setListener` and `emit` trigger correctly
  - Test HIGH priority is processed before MEDIUM and LOW

### Phase 2: StorageApiSync Modifications

- [x] **3. Add `streamingQueue` to `StorageApiSync`** (US-2, US-5)
  - Import `StorageStreamingQueue` in `storageSync.ts`
  - Add `readonly streamingQueue: StorageStreamingQueue` property
  - Initialize in constructor

- [x] **4. Implement priority-based routing in `loadCoValue`** (US-1, US-5)
  - Use `getPriorityFromHeader()` to determine priority
  - All priorities: push callbacks to `streamingQueue` with `push(callback, priority)`
  - First chunk is sent directly, subsequent chunks are queued
  - Call `streamingQueue.emit()` after pushing entries to trigger processing
  - Remove `await new Promise((resolve) => setTimeout(resolve))` yielding

- [x] **5. Update `StorageApiSync` tests** (US-1, US-2, US-5)
  - Update `packages/cojson/src/tests/StorageApiSync.test.ts`
  - Add test: HIGH priority content goes through queue first
  - Add test: MEDIUM priority content goes through queue
  - Add test: LOW priority content (binary) goes through queue

### Phase 3: SyncManager Unified Scheduling

- [x] **6. Add unified `processQueues` method to `SyncManager`** (US-3, US-4)
  - Add `private processing = false` flag
  - Implement `processQueues()` that coordinates both queues
  - Pull from `IncomingMessagesQueue` and storage `streamingQueue` alternately
  - Invoke `entry.pushContent()` callback when processing storage entries
  - Content flows to `handleNewContent(content, 'storage')` via callback chain
  - Implement time budget check using `SYNC_SCHEDULER_CONFIG.INCOMING_MESSAGES_TIME_BUDGET`
  - Yield with `setTimeout(0)` when budget exceeded

- [x] **7. Implement priority-aware work selection** (US-4)
  - Process messages and streaming entries in alternating fashion
  - Both queues are processed in priority order (HIGH > MEDIUM > LOW for storage)
  - All priorities go through the queue, HIGH is processed first

- [x] **8. Update `pushMessage` to use unified scheduling** (US-4)
  - `IncomingMessagesQueue` constructor takes `processQueues` callback
  - `IncomingMessagesQueue.push()` calls `processQueues()` after adding message
  - Ensure backward compatibility with existing behavior

- [x] **9. Add trigger for storage queue processing** (US-4, US-5)
  - `StorageStreamingQueue` has `setListener()` and `emit()` methods
  - `SyncManager.setStorage()` connects queue via `setListener(() => processQueues())`
  - `StorageApiSync.loadCoValue()` calls `emit()` after pushing entries

### Phase 4: IncomingMessagesQueue Simplification

- [x] **10. Remove `processQueue` from `IncomingMessagesQueue`** (US-4)
  - Remove `processing` property
  - Remove `processQueue` method
  - Keep `push`, `pull`, and queue management intact
  - Add `processQueues` callback parameter to constructor
  - Call `processQueues()` at end of `push()` method

- [x] **11. Update `IncomingMessagesQueue` tests** (US-4)
  - Update `packages/cojson/src/tests/IncomingMessagesQueue.test.ts`
  - Remove tests for `processQueue` method
  - Add mock `processQueues` callback to test setup
  - Keep tests for `push` and `pull` behavior

### Phase 5: Integration Tests

- [x] **12. Add integration tests for unified scheduling** (US-3, US-4)
  - Update `packages/cojson/src/tests/sync.storage.test.ts`
  - Test: large binary streaming interleaved with incoming messages
  - Test: HIGH priority content is processed first in the queue
  - Test: MEDIUM priority content is processed before LOW priority
  - Test: storage streaming and peer messages share the same scheduler

- [x] **13. Add integration tests for streaming lifecycle** (US-2)
  - Test: queue becomes empty after streaming completes
  - Test: multiple CoValues can stream concurrently with correct prioritization

### Phase 6: Cleanup and Documentation

- [ ] **14. Update config exports** (US-3)
  - Ensure `SYNC_SCHEDULER_CONFIG` time budget is used consistently
  - Add any new config options if needed for storage streaming

- [ ] **15. Add OpenTelemetry metrics** (US-2)
  - Add metrics for storage streaming queue (similar to existing queue metrics)
  - Track: queue length, push/pull counts
  - Follow patterns from `LinkedList.ts` `QueueMeter`

## Task Dependencies

```
1 ──► 2
      │
      ▼
3 ──► 4 ──► 5
      │
      ▼
6 ──► 7 ──► 8 ──► 9
                  │
                  ▼
            10 ──► 11
                   │
                   ▼
             12 ──► 13
                    │
                    ▼
              14 ──► 15
```

## Notes

- Tasks 1-2 can be done independently as the queue is self-contained
- Tasks 3-5 depend on the queue being implemented
- Tasks 6-9 are the core scheduling changes
- Tasks 10-11 clean up the old approach
- Tasks 12-15 validate and polish the implementation

