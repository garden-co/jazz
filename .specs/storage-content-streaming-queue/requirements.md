# Storage Content Streaming Queue

## Introduction

The current storage content streaming implementation in `storageSync.ts` uses `setTimeout` to yield the main thread during content streaming of large CoValues. This approach has several limitations:

1. **No coordination with incoming messages**: The storage streaming runs independently from `IncomingMessagesQueue`, which can lead to suboptimal scheduling when both are active simultaneously.
2. **No priority awareness**: All CoValues are streamed with equal priority, regardless of their importance (e.g., account/group CoValues vs. binary streams).
3. **No centralized queue management**: CoValues are not tracked while streaming, making it difficult to manage the streaming lifecycle or cancel/reprioritize in-flight operations.
4. **Unpredictable yielding**: The `setTimeout` approach yields after each chunk regardless of how much time has been spent, which can be either too aggressive or not aggressive enough.

This feature introduces a priority-based streaming queue that integrates with the existing sync scheduling infrastructure, providing better control over main thread usage and consistent prioritization across both incoming messages and storage operations.

## User Stories

### US-1: Priority-based storage streaming

**As a** sync system  
**I want** storage content streaming to respect CoValue priorities  
**So that** important CoValues (accounts, groups) are streamed before less important ones (binary streams)

**Acceptance Criteria:**
- When a CoValue is queued for streaming, it shall be assigned a priority based on its header type (HIGH for accounts/groups, MEDIUM for regular CoValues, LOW for binary streams)
- When pulling from the queue, the system shall always prefer higher-priority CoValues over lower-priority ones
- When CoValues of the same priority are queued, they shall be processed in FIFO order

### US-2: Queue lifecycle management

**As a** sync system  
**I want** CoValues to remain in the queue until their streaming is completed  
**So that** I can track which CoValues are currently being streamed and ensure completion

**Acceptance Criteria:**
- When a CoValue is added to the streaming queue, it shall remain tracked until all its content chunks have been streamed
- When streaming completes successfully for a CoValue, it shall be removed from the queue
- When a streaming operation fails or is cancelled, the CoValue shall be removed from the queue
- The queue shall provide a method to check if a CoValue is currently being streamed

### US-3: Time-budget-aware scheduling

**As a** sync system  
**I want** the streaming queue to yield the main thread when the time budget is exceeded  
**So that** the application remains responsive during heavy streaming operations

**Acceptance Criteria:**
- When the cumulative processing time exceeds the configured time budget (default 50ms), the system shall yield to the event loop
- When resuming after yielding, the system shall continue from where it left off
- The time budget shall be configurable via `SYNC_SCHEDULER_CONFIG`

### US-4: Unified scheduling with IncomingMessagesQueue

**As a** sync system  
**I want** storage streaming to be scheduled together with incoming message processing  
**So that** both operations share the same time budget and prioritization logic

**Acceptance Criteria:**
- When `processQueue` is called on the SyncManager, it shall process both incoming messages and storage streaming operations
- When determining what to process next, the system shall consider priorities from both queues
- When the time budget is exceeded, both queues shall pause together
- The scheduling logic shall be moved from `IncomingMessagesQueue` to `SyncManager`

### US-5: Pull-based streaming

**As a** SyncManager  
**I want** to pull storage content from the queue  
**So that** I have control over when and how content is processed

**Acceptance Criteria:**
- The SyncManager shall pull content chunks from the storage streaming queue
- The storage API shall not directly push content to callbacks during streaming
- The storage API shall add content to the queue and let the SyncManager handle the delivery

