# Storage Content Streaming Queue - Design

## Overview

This design introduces a priority-aware architecture for storage content streaming that integrates with the existing sync scheduling infrastructure. The key changes are:

1. **Content that doesn't require streaming** (single chunk) streams directly via callbacks — no queueing
2. **Content requiring streaming** (multiple chunks) uses a pull-based queue model where SyncManager controls the flow
3. **All priority levels go through the same queue** with priority ordering: HIGH > MEDIUM > LOW

The queue is only used when content requires streaming (has multiple signature chunks that would block the main thread). This ensures most CoValues load directly without queue overhead, while large binary streams are properly scheduled.

The design consists of three main changes:
1. A new `StorageStreamingQueue` class for priority-based content queuing
2. Modifications to `StorageApiSync` to use queue for all streaming content
3. Unified scheduling logic in `SyncManager` that coordinates both incoming messages and storage streaming

## Architecture / Components

### 1. StorageStreamingQueue

A new queue class that manages storage content streaming with priority-based scheduling.

```
packages/cojson/src/queue/StorageStreamingQueue.ts
```

**Responsibilities:**
- Queue callbacks to get content chunks (lazy evaluation)
- Track active streaming sessions per CoValue
- Provide content to SyncManager via pull interface
- Signal when a CoValue's streaming is complete

**Key Design Decision: Callbacks instead of Content**

The queue stores callbacks that produce content rather than the content itself. This is important because:
- Content can be large (binary streams can be megabytes)
- Lazily fetching content avoids memory pressure when many CoValues are queued
- Content is only loaded from the database when actually being processed

**Interface:**

```typescript
import { CoValuePriority, CO_VALUE_PRIORITY } from "../priority.js";
import { LinkedList } from "./LinkedList.js";

/**
 * A callback that pushes content when invoked.
 * Content is only fetched from the database when this callback is called.
 */
type ContentCallback = () => void;

export class StorageStreamingQueue {
  private queues: StreamingQueueTuple;
  private listener: (() => void) | undefined;
  
  constructor();
  
  /**
   * Push a content callback to the queue with explicit priority.
   * The callback will be invoked when the entry is pulled and processed.
   * 
   * @param entry - Callback that pushes content when invoked
   * @param priority - Priority for this entry (HIGH, MEDIUM, or LOW)
   */
  push(entry: ContentCallback, priority: CoValuePriority): void;
  
  /**
   * Pull the next callback from the queue.
   * Returns undefined if no entries are available.
   * Priority order: HIGH > MEDIUM > LOW.
   */
  pull(): ContentCallback | undefined;
  
  /**
   * Check if the queue is empty (no pending entries).
   */
  isEmpty(): boolean;
  
  /**
   * Set a listener to be called when emit() is invoked.
   * Used to connect the queue to SyncManager.processQueues().
   */
  setListener(listener: () => void): void;
  
  /**
   * Emit an event to trigger queue processing.
   * Calls the listener set via setListener().
   */
  emit(): void;
}
```

**Priority Handling:**
- Uses the same priority levels as `PriorityBasedMessageQueue` (HIGH, MEDIUM, LOW)
- Priority is provided explicitly on each `push()` call
- Caller (StorageApiSync) determines priority using `getPriorityFromHeader()`
- HIGH priority: accounts and groups — processed first
- MEDIUM priority: regular CoValues — processed after HIGH
- LOW priority: binary streams — processed last

### 2. StorageApiSync Modifications

Changes to `StorageApiSync` to support pull-based streaming.

**Current flow (push-based):**
```
load() → loadCoValue() → for each chunk → pushContentWithDependencies() → callback()
                                        → setTimeout() to yield
```

**New flow (streaming-aware):**
```
load() → loadCoValue() → check if streaming needed (multiple chunks)
                              ↓
              ┌───────────────┴───────────────┐
              ↓                               ↓
    No streaming needed              Streaming needed
              ↓                               ↓
    First chunk sent               queue.push(getContent, priority)
    directly                       SyncManager.processQueues()
    (no queueing)                  → queue.pull() (HIGH > MEDIUM > LOW)
                                   → entry.getContent()
                                   → handleNewContent()
```

**New interface additions:**

```typescript
export class StorageApiSync implements StorageAPI {
  // Existing members...
  
  /**
   * Queue for streaming content that will be pulled by SyncManager.
   * Exposed for SyncManager to access.
   */
  readonly streamingQueue: StorageStreamingQueue;
  
  /**
   * Start loading a CoValue. Content callbacks will be added to streamingQueue
   * and must be pulled by SyncManager.
   */
  load(
    id: string,
    callback: (data: NewContentMessage) => void,
    done: (found: boolean) => void,
  ): void;
}
```

**Key changes:**
- Remove `await new Promise((resolve) => setTimeout(resolve))` yielding
- Check if content requires streaming (multiple signature chunks)
- Determine priority using `getPriorityFromHeader()`
- All streaming content goes through the queue regardless of priority
- First chunk is sent directly, subsequent chunks are queued
- Dependencies are still loaded directly

**Example of streaming with queue check:**

```typescript
const priority = getPriorityFromHeader(coValueRow.header);

// Build streaming callbacks for all chunks after the first
const streamingQueue: ContentCallback[] = [];

for (const sessionRow of allCoValueSessions) {
  const signatures = signaturesBySession.get(sessionRow.sessionID);
  
  // First chunk is sent directly
  this.loadSessionTransactions(contentMessage, sessionRow, 0, firstSignature);
  
  // Subsequent chunks are queued as callbacks
  for (let i = 1; i < signatures.length; i++) {
    streamingQueue.push(() => {
      const contentMessage = createContentMessage(coValueRow.id, coValueRow.header);
      this.loadSessionTransactions(contentMessage, sessionRow, prevIdx, signature);
      
      if (Object.keys(contentMessage.new).length > 0) {
        this.pushContentWithDependencies(coValueRow, contentMessage, callback);
      }
    });
  }
}

// Send the first chunk directly
this.pushContentWithDependencies(coValueRow, contentMessage, callback);

// All priorities go through the queue (HIGH > MEDIUM > LOW)
for (const pushStreamingContent of streamingQueue) {
  this.streamingQueue.push(pushStreamingContent, priority);
}

// Trigger the queue to process the entries
if (streamingQueue.length > 0) {
  this.streamingQueue.emit();
}
```

### 3. SyncManager Unified Scheduling

Move `processQueue` logic from `IncomingMessagesQueue` to `SyncManager` and add storage streaming coordination.

**New method in SyncManager:**

```typescript
export class SyncManager {
  // Existing members...
  
  messagesQueue = new IncomingMessagesQueue(
    () => this.processQueues(),
  );
  private processing = false;
  
  /**
   * Process both incoming messages and storage streaming content
   * using a shared time budget.
   * 
   * Processing alternates between incoming messages and storage streaming,
   * with time budget checks to avoid blocking the main thread.
   */
  private async processQueues(): Promise<void> {
    if (this.processing) return;
    this.processing = true;
    
    let lastTimer = performance.now();
    
    while (true) {
      // First, try to pull from incoming messages queue
      const messageEntry = this.messagesQueue.pull();
      if (messageEntry) {
        this.handleSyncMessage(messageEntry.msg, messageEntry.peer);
      }
      
      // Then, try to pull from storage streaming queue
      const storageQueue = this.getStorageStreamingQueue();
      const streamingCallback = storageQueue?.pull();
      if (streamingCallback) {
        // Invoke the callback to lazily fetch content and push via callback chain
        streamingCallback();
      }
      
      // Exit if no work was done
      if (!messageEntry && !streamingEntry) {
        break;
      }
      
      // Check time budget and yield if exceeded
      const currentTimer = performance.now();
      if (currentTimer - lastTimer > SYNC_SCHEDULER_CONFIG.INCOMING_MESSAGES_TIME_BUDGET) {
        await new Promise(resolve => setTimeout(resolve, 0));
        lastTimer = performance.now();
      }
    }
    
    this.processing = false;
  }
  
  private getStorageStreamingQueue(): StorageStreamingQueue | undefined {
    // Access storage's streaming queue if available
  }
  
  setStorage(storage: StorageAPI) {
    // ... existing logic ...
    
    // Connect storage queue to processQueues
    const storageStreamingQueue = this.getStorageStreamingQueue();
    if (storageStreamingQueue) {
      storageStreamingQueue.setListener(() => {
        this.processQueues();
      });
    }
  }
}
```

**Scheduling algorithm:**

The unified scheduler alternates between:
1. Incoming messages from peers (round-robin across peers)
2. Storage streaming content (priority-based via `StorageStreamingQueue`)

Processing order per iteration:
1. Pull one message from `IncomingMessagesQueue` (if available)
2. Pull one entry from `StorageStreamingQueue` (if available, HIGH > MEDIUM > LOW)
3. Continue until both queues are empty

All streaming content goes through the queue regardless of priority, with HIGH priority entries processed before MEDIUM and LOW.

### 4. IncomingMessagesQueue Changes

Remove `processQueue` method since scheduling moves to SyncManager. Add callback-based trigger.

```typescript
export class IncomingMessagesQueue {
  // Remove: processing property
  // Remove: processQueue method
  
  // Keep: push, pull, queues management
  
  // Add: constructor takes processQueues callback
  constructor(private processQueues: () => void);
  
  // Modified: push now triggers processQueues after adding message
  public push(msg: SyncMessage, peer: PeerState) {
    // ... existing queue logic ...
    this.processQueues();
  }
}
```

## Data Models

### ContentCallback

A function that lazily produces and pushes content when invoked:

```typescript
type ContentCallback = () => void;
```

The callback is responsible for loading content from the database and calling the appropriate push callback to deliver it. This encapsulates the entire streaming step.

The queue stores these callbacks directly (not wrapped in an entry object). Priority is provided separately when pushing and is used to determine which internal queue to use.

### Queue Processing

The SyncManager processes both queues in a unified loop:

```typescript
// Incoming message entry from IncomingMessagesQueue.pull()
type MessageEntry = {
  msg: SyncMessage;
  peer: PeerState;
};

// Storage streaming callback from StorageStreamingQueue.pull()
type ContentCallback = () => void;
```

When processing a storage entry, SyncManager invokes the callback directly (`callback()`) which lazily fetches content from the database and pushes it via the callback chain.

## Sequence Diagrams

### Queued Streaming Path (All Priorities)

All streaming content goes through the priority-based queue (HIGH > MEDIUM > LOW):

```
┌─────────┐     ┌─────────────┐     ┌────────────────────┐     ┌─────────────┐
│ Storage │     │ StorageApi  │     │ StorageStreaming   │     │ SyncManager │
│   DB    │     │   Sync      │     │      Queue         │     │             │
└────┬────┘     └──────┬──────┘     └─────────┬──────────┘     └──────┬──────┘
     │                 │                      │                       │
     │  load(id)       │                      │                       │
     │ ◄───────────────│                      │                       │
     │                 │                      │                       │
     │  coValue data   │                      │                       │
     │ ────────────────►                      │                       │
     │                 │                      │                       │
     │                 │ getPriorityFromHeader() → priority            │
     │                 │                      │                       │
     │                 │ push({id, pushContent1, priority})            │
     │                 │ ─────────────────────►                       │
     │                 │                      │                       │
     │                 │ push({id, pushContentN, priority})           │
     │                 │ ─────────────────────►                       │
     │                 │                      │                       │
     │                 │ emit()               │                       │
     │                 │ ─────────────────────►                       │
     │                 │                      │                       │
     │                 │                      │  listener()           │
     │                 │                      │ ─────────────────────►│
     │                 │                      │                       │
     │                 │                      │  processQueues()      │
     │                 │                      │                       │
     │                 │                      │  pull()               │
     │                 │                      │ ◄─────────────────────│
     │                 │                      │                       │
     │                 │                      │  {id, pushContent1}   │
     │                 │                      │ ─────────────────────►│
     │                 │                      │                       │
     │                 │  pushContent1()      │                       │
     │  fetch chunk 1  │ ◄────────────────────┼───────────────────────│
     │ ────────────────►                      │                       │
     │                 │                      │  (callback invoked)   │
     │                 │                      │                       │
     │                 │                      │  (continues until     │
     │                 │                      │   queue is empty)     │
```

Note: Content is only fetched from the database when `pushContent()` is invoked by SyncManager, not when the entry is pushed to the queue. This lazy evaluation prevents memory issues when many CoValues are queued.

## Testing Strategy

### Unit Tests for StorageStreamingQueue

Follow the patterns from `PriorityBasedMessageQueue.test.ts`:

```typescript
describe("StorageStreamingQueue", () => {
  describe("push and pull", () => {
    test("should pull callbacks in priority order (HIGH > MEDIUM > LOW)", () => {
      const queue = new StorageStreamingQueue();
      
      const lowCallback = () => {};
      const mediumCallback = () => {};
      const highCallback = () => {};
      
      queue.push(lowCallback, CO_VALUE_PRIORITY.LOW);
      queue.push(mediumCallback, CO_VALUE_PRIORITY.MEDIUM);
      queue.push(highCallback, CO_VALUE_PRIORITY.HIGH);
      
      expect(queue.pull()).toBe(highCallback);
      expect(queue.pull()).toBe(mediumCallback);
      expect(queue.pull()).toBe(lowCallback);
    });

    test("callback is not invoked until caller invokes it", () => {
      const queue = new StorageStreamingQueue();
      
      const callback = vi.fn();
      queue.push(callback, CO_VALUE_PRIORITY.MEDIUM);
      
      // Callback not invoked yet
      expect(callback).not.toHaveBeenCalled();
      
      const pulled = queue.pull();
      // Still not invoked - SyncManager calls it
      expect(callback).not.toHaveBeenCalled();
      
      // SyncManager invokes the callback
      pulled?.();
      expect(callback).toHaveBeenCalledTimes(1);
    });
  });

  describe("isEmpty", () => {
    test("should return true for empty queue", () => {
      const queue = new StorageStreamingQueue();
      expect(queue.isEmpty()).toBe(true);
    });

    test("should return false when entries exist", () => {
      const queue = new StorageStreamingQueue();
      queue.push(() => {}, CO_VALUE_PRIORITY.MEDIUM);
      expect(queue.isEmpty()).toBe(false);
    });
  });

  describe("setListener and emit", () => {
    test("should call listener when emit is called", () => {
      const queue = new StorageStreamingQueue();
      const listener = vi.fn();
      
      queue.setListener(listener);
      queue.emit();
      
      expect(listener).toHaveBeenCalledTimes(1);
    });
  });
});
```

### Unit Tests for SyncManager.processQueues

Follow the patterns from `IncomingMessagesQueue.test.ts`:

```typescript
describe("SyncManager.processQueues", () => {
  test("should process both incoming messages and storage content", async () => {
    const { syncManager, storage } = setup();
    
    // Push incoming message
    syncManager.pushMessage(createMockLoadMessage("co_z1"), mockPeer);
    
    // Push storage content callback
    const callback = vi.fn();
    storage.streamingQueue.push(callback, CO_VALUE_PRIORITY.MEDIUM);
    storage.streamingQueue.emit();
    
    // Both should be processed
    expect(callback).toHaveBeenCalled();
  });

  test("should invoke callback when processing storage entries", async () => {
    const { syncManager, storage } = setup();
    
    const callback = vi.fn();
    storage.streamingQueue.push(callback, CO_VALUE_PRIORITY.MEDIUM);
    
    expect(callback).not.toHaveBeenCalled();
    
    storage.streamingQueue.emit();
    
    expect(callback).toHaveBeenCalledTimes(1);
  });

  test("IncomingMessagesQueue.push triggers processQueues", async () => {
    const processQueues = vi.fn();
    const queue = new IncomingMessagesQueue(processQueues);
    
    queue.push(createMockLoadMessage("co_z1"), mockPeer);
    
    expect(processQueues).toHaveBeenCalledTimes(1);
  });
});
```

### Integration Tests

Extend existing tests in `sync.storage.test.ts`:

```typescript
describe("storage streaming with unified scheduling", () => {
  test("large coValue streaming interleaved with incoming messages", async () => {
    const client = setupTestNode();
    client.connectToSyncServer();
    client.addStorage();

    // Create a large binary coValue that requires streaming
    const group = jazzCloud.node.createGroup();
    const binary = group.createBinaryStream();
    await fillBinaryWithLargeData(binary, 10 * 1024 * 1024); // 10MB

    // Simultaneously send other messages
    const map = group.createMap();
    map.set("key", "value", "trusting");

    // Load both - streaming should interleave with map loading
    const [binaryOnClient, mapOnClient] = await Promise.all([
      loadCoValueOrFail(client.node, binary.id),
      loadCoValueOrFail(client.node, map.id),
    ]);

    expect(binaryOnClient).toBeDefined();
    expect(mapOnClient.get("key")).toBe("value");
  });

  test("high-priority coValues are processed first in the queue", async () => {
    const client = setupTestNode();
    client.addStorage();

    // Start streaming a low-priority binary first
    const group = jazzCloud.node.createGroup();
    const binary = group.createBinaryStream();
    await fillBinaryWithLargeData(binary, 5 * 1024 * 1024);

    // Queue the binary load first (goes through queue - LOW priority)
    const binaryPromise = loadCoValueOrFail(client.node, binary.id);

    // Then request the group (HIGH priority - processed first in queue)
    const groupPromise = loadCoValueOrFail(client.node, group.id);

    // Group should complete first because HIGH priority is processed first
    const results = await Promise.race([
      groupPromise.then(() => "group"),
      binaryPromise.then(() => "binary"),
    ]);

    expect(results).toBe("group");
  });

  test("queue is empty after all streaming completes", async () => {
    const client = setupTestNode();
    const { storage } = client.addStorage();

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "reader");
    await group.core.waitForSync();

    // Load the group from storage
    await loadCoValueOrFail(client.node, group.id);

    // Queue should be empty after processing completes
    expect(storage.streamingQueue.isEmpty()).toBe(true);
  });
});
```

## Migration Notes

This change is internal to the cojson package and does not affect the public API. The `StorageAPI` interface remains unchanged - the `load` method signature is preserved, and the callback will still be called for each content chunk, just orchestrated through the queue.

The only observable difference should be improved performance characteristics:
- More consistent main thread yielding
- Better prioritization of important CoValues during heavy streaming
- Reduced latency for high-priority operations when streaming is active

