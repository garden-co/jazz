import {
  afterEach,
  beforeEach,
  describe,
  expect,
  onTestFinished,
  test,
  vi,
} from "vitest";
import {
  CO_VALUE_LOADING_CONFIG,
  setMaxInFlightLoadsPerPeer,
} from "../config.js";
import { OutgoingLoadQueue } from "../queue/OutgoingLoadQueue.js";
import type { PeerID } from "../sync.js";
import {
  createTestMetricReader,
  createTestNode,
  tearDownTestMetricReader,
} from "./testUtils.js";

const TEST_PEER_ID = "test-peer" as PeerID;

// Store original config values
let originalMaxInFlightLoads: number;
let originalTimeout: number;

beforeEach(() => {
  originalMaxInFlightLoads =
    CO_VALUE_LOADING_CONFIG.MAX_IN_FLIGHT_LOADS_PER_PEER;
  originalTimeout = CO_VALUE_LOADING_CONFIG.TIMEOUT;
});

afterEach(() => {
  // Restore original config
  setMaxInFlightLoadsPerPeer(originalMaxInFlightLoads);
  CO_VALUE_LOADING_CONFIG.TIMEOUT = originalTimeout;
  vi.useRealTimers();
});

describe("OutgoingLoadQueue", () => {
  describe("basic enqueue behavior", () => {
    test("should call sendCallback immediately when queue has capacity", () => {
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();
      const map = group.createMap();

      let callbackCalled = false;
      queue.enqueue(map.core, () => {
        callbackCalled = true;
      });

      expect(callbackCalled).toBe(true);
      expect(queue.inFlightCount).toBe(1);
    });

    test("should track sent request in inFlightLoads", () => {
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();
      const map = group.createMap();

      queue.enqueue(map.core, () => {});

      expect(queue.inFlightCount).toBe(1);
    });
  });

  describe("FIFO ordering", () => {
    test("should maintain FIFO order for pending requests", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue first
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      // Enqueue multiple CoValues
      const map1 = group.createMap();
      const map2 = group.createMap();
      const map3 = group.createMap();

      const order: string[] = [];

      queue.enqueue(map1.core, () => order.push("map1"));
      queue.enqueue(map2.core, () => order.push("map2"));
      queue.enqueue(map3.core, () => order.push("map3"));

      // Complete requests one by one
      queue.trackComplete(blockerMap.core);
      queue.trackComplete(map1.core);
      queue.trackComplete(map2.core);

      expect(order).toEqual(["map1", "map2", "map3"]);
    });
  });

  describe("throttling", () => {
    test("should queue requests when at capacity limit", () => {
      setMaxInFlightLoadsPerPeer(2);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map1 = group.createMap();
      const map2 = group.createMap();
      const map3 = group.createMap();

      let callback3Called = false;

      queue.enqueue(map1.core, () => {});
      queue.enqueue(map2.core, () => {});
      queue.enqueue(map3.core, () => {
        callback3Called = true;
      });

      // First two should be in flight
      expect(queue.inFlightCount).toBe(2);
      // Third should be pending
      expect(queue.pendingCount).toBe(1);
      expect(callback3Called).toBe(false);
    });

    test("should process queued requests when a slot becomes available", () => {
      setMaxInFlightLoadsPerPeer(2);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map1 = group.createMap();
      const map2 = group.createMap();
      const map3 = group.createMap();

      let callback3Called = false;

      queue.enqueue(map1.core, () => {});
      queue.enqueue(map2.core, () => {});
      queue.enqueue(map3.core, () => {
        callback3Called = true;
      });

      expect(callback3Called).toBe(false);

      // Complete one request
      queue.trackComplete(map1.core);

      // Third should now be processed
      expect(callback3Called).toBe(true);
      expect(queue.inFlightCount).toBe(2);
      expect(queue.pendingCount).toBe(0);
    });
  });

  describe("request deduplication", () => {
    test("should skip duplicate enqueue while pending", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const blockerMap = group.createMap();
      const targetMap = group.createMap();

      let targetCallbackCount = 0;
      let duplicateCallbackCount = 0;

      queue.enqueue(blockerMap.core, () => {});

      queue.enqueue(targetMap.core, () => {
        targetCallbackCount += 1;
      });
      queue.enqueue(targetMap.core, () => {
        duplicateCallbackCount += 1;
      });

      expect(queue.pendingCount).toBe(1);
      expect(targetCallbackCount).toBe(0);
      expect(duplicateCallbackCount).toBe(0);

      queue.trackComplete(blockerMap.core);

      expect(targetCallbackCount).toBe(1);
      expect(duplicateCallbackCount).toBe(0);
    });

    test("should skip duplicate enqueue while in-flight", () => {
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map = group.createMap();
      let duplicateCallbackCount = 0;

      queue.enqueue(map.core, () => {});
      queue.enqueue(map.core, () => {
        duplicateCallbackCount += 1;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(duplicateCallbackCount).toBe(0);
    });

    test("should skip duplicate enqueue for same CoValue ID while in-flight", () => {
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const otherNode = createTestNode();
      const group = node.createGroup();

      const map = group.createMap();
      const sameIdCoValue = otherNode.getCoValue(map.id);

      let duplicateCallbackCount = 0;

      queue.enqueue(map.core, () => {});
      queue.enqueue(sameIdCoValue, () => {
        duplicateCallbackCount += 1;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(duplicateCallbackCount).toBe(0);
    });

    test("should skip duplicate enqueue for same CoValue ID", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const otherNode = createTestNode();
      const group = node.createGroup();

      const availableMap = group.createMap();
      const sameIdCoValue = otherNode.getCoValue(availableMap.id);

      queue.enqueue(availableMap.core, () => {});
      queue.trackComplete(availableMap.core);

      // Block capacity so the next enqueue stays pending
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      let duplicateCallbackCount = 0;
      queue.enqueue(availableMap.core, () => {});
      queue.enqueue(sameIdCoValue, () => {
        duplicateCallbackCount += 1;
      });

      expect(queue.pendingCount).toBe(1);
      expect(duplicateCallbackCount).toBe(0);
    });
  });

  describe("timeout behavior", () => {
    test("should mark CoValue as not found in peer after timeout", async () => {
      vi.useFakeTimers();
      CO_VALUE_LOADING_CONFIG.TIMEOUT = 1000;

      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();

      // Get an unavailable CoValue
      const coValue = node.getCoValue("co_zTestTimeoutCoValue0001" as any);

      queue.enqueue(coValue, () => {});

      expect(queue.inFlightCount).toBe(1);

      // Advance time past the timeout
      await vi.advanceTimersByTimeAsync(1001);

      // Should be removed from in-flight
      expect(queue.inFlightCount).toBe(0);

      // Should be marked as not found
      expect(coValue.getLoadingStateForPeer(TEST_PEER_ID)).toBe("unavailable");
    });

    test("should free the queue slot and process pending requests on timeout", async () => {
      vi.useFakeTimers();
      CO_VALUE_LOADING_CONFIG.TIMEOUT = 1000;
      setMaxInFlightLoadsPerPeer(1);

      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const coValue1 = node.getCoValue("co_zTestTimeoutFree00000001" as any);
      const map2 = group.createMap();

      let callback2Called = false;

      queue.enqueue(coValue1, () => {});
      queue.enqueue(map2.core, () => {
        callback2Called = true;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(callback2Called).toBe(false);

      // Advance time past the timeout
      await vi.advanceTimersByTimeAsync(1001);

      // First should have timed out, second should be processed
      expect(callback2Called).toBe(true);
      expect(queue.inFlightCount).toBe(1);
    });

    test("should timeout each in-flight load independently", async () => {
      vi.useFakeTimers();
      CO_VALUE_LOADING_CONFIG.TIMEOUT = 1000;
      setMaxInFlightLoadsPerPeer(3);

      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();

      const coValue1 = node.getCoValue("co_zTestSingleTimer000001" as any);
      const coValue2 = node.getCoValue("co_zTestSingleTimer000002" as any);
      const coValue3 = node.getCoValue("co_zTestSingleTimer000003" as any);

      queue.enqueue(coValue1, () => {});
      await vi.advanceTimersByTimeAsync(100);
      queue.enqueue(coValue2, () => {});
      await vi.advanceTimersByTimeAsync(100);
      queue.enqueue(coValue3, () => {});

      expect(queue.inFlightCount).toBe(3);

      // Advance time past the timeout
      await vi.advanceTimersByTimeAsync(801);

      // All three should have timed out
      expect(queue.inFlightCount).toBe(2);

      await vi.advanceTimersByTimeAsync(101);
      expect(coValue1.getLoadingStateForPeer(TEST_PEER_ID)).toBe("unavailable");
      expect(queue.inFlightCount).toBe(1);
      await vi.advanceTimersByTimeAsync(101);
      expect(coValue2.getLoadingStateForPeer(TEST_PEER_ID)).toBe("unavailable");
      expect(queue.inFlightCount).toBe(0);
      await vi.advanceTimersByTimeAsync(101);
      expect(coValue3.getLoadingStateForPeer(TEST_PEER_ID)).toBe("unavailable");
      expect(queue.inFlightCount).toBe(0);
    });

    test("should allow re-enqueue after timeout", async () => {
      vi.useFakeTimers();
      CO_VALUE_LOADING_CONFIG.TIMEOUT = 1000;
      setMaxInFlightLoadsPerPeer(1);

      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();

      const coValue = node.getCoValue("co_zTestTimeoutReenqueue0001" as any);

      let secondCallbackCount = 0;

      queue.enqueue(coValue, () => {});
      await vi.advanceTimersByTimeAsync(1001);

      expect(queue.inFlightCount).toBe(0);
      expect(coValue.getLoadingStateForPeer(TEST_PEER_ID)).toBe("unavailable");

      queue.enqueue(coValue, () => {
        secondCallbackCount += 1;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(secondCallbackCount).toBe(1);
    });

    test("should warn but not mark as unavailable when streaming CoValue times out", async () => {
      vi.useFakeTimers();
      CO_VALUE_LOADING_CONFIG.TIMEOUT = 1000;
      setMaxInFlightLoadsPerPeer(1);

      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map = group.createMap();
      map.set("key", "value");

      // Mock isStreaming to return true (CoValue is available but still streaming)
      vi.spyOn(map.core, "isStreaming").mockReturnValue(true);

      queue.enqueue(map.core, () => {});

      expect(queue.inFlightCount).toBe(1);

      // Advance time past the timeout
      await vi.advanceTimersByTimeAsync(1001);

      // Should be removed from in-flight (timeout clears the slot)
      expect(queue.inFlightCount).toBe(0);

      // But should NOT be marked as unavailable since it's available (just streaming)
      expect(map.core.isAvailable()).toBe(true);
    });
  });

  describe("trackComplete", () => {
    test("should remove from inFlightLoads and allow next request to process", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map1 = group.createMap();
      const map2 = group.createMap();

      let callback2Called = false;

      queue.enqueue(map1.core, () => {});
      queue.enqueue(map2.core, () => {
        callback2Called = true;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(callback2Called).toBe(false);

      queue.trackComplete(map1.core);

      expect(callback2Called).toBe(true);
      expect(queue.inFlightCount).toBe(1);
    });

    test("should be a no-op for unknown CoValues", () => {
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map1 = group.createMap();
      const unknownCoValue = node.getCoValue(
        "co_zTestUnknownCoValue001" as any,
      );

      queue.enqueue(map1.core, () => {});

      expect(queue.inFlightCount).toBe(1);

      // trackComplete on unknown CoValue should be a no-op
      queue.trackComplete(unknownCoValue);

      expect(queue.inFlightCount).toBe(1);
    });

    test("should complete in-flight request by CoValue ID even with different instance", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const otherNode = createTestNode();
      const group = node.createGroup();

      const map = group.createMap();
      const nextMap = group.createMap();
      const sameIdCoValue = otherNode.getCoValue(map.id);

      let nextCallbackCalled = false;

      queue.enqueue(map.core, () => {});
      queue.enqueue(nextMap.core, () => {
        nextCallbackCalled = true;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(nextCallbackCalled).toBe(false);

      // Complete using a different CoValue instance with the same ID.
      queue.trackComplete(sameIdCoValue);

      expect(nextCallbackCalled).toBe(true);
      expect(queue.inFlightCount).toBe(1);
    });

    test("should allow re-enqueue after completion", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map = group.createMap();

      let firstCallbackCount = 0;
      let secondCallbackCount = 0;

      queue.enqueue(map.core, () => {
        firstCallbackCount += 1;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(firstCallbackCount).toBe(1);

      queue.trackComplete(map.core);

      queue.enqueue(map.core, () => {
        secondCallbackCount += 1;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(secondCallbackCount).toBe(1);
    });

    test("should not complete if CoValue is streaming for content updates", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map1 = group.createMap();
      const map2 = group.createMap();

      // Mock isStreaming to return true
      vi.spyOn(map1.core, "isStreaming").mockReturnValue(true);

      let callback2Called = false;

      queue.enqueue(map1.core, () => {});
      queue.enqueue(map2.core, () => {
        callback2Called = true;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(callback2Called).toBe(false);

      // trackComplete should not complete because map1 is streaming
      queue.trackComplete(map1.core, "content");

      // Should still be in-flight and pending should not be processed
      expect(queue.inFlightCount).toBe(1);
      expect(callback2Called).toBe(false);
      expect(queue.pendingCount).toBe(1);

      // Stop streaming
      vi.spyOn(map1.core, "isStreaming").mockReturnValue(false);

      // Now trackComplete should work
      queue.trackComplete(map1.core, "content");

      expect(queue.inFlightCount).toBe(1); // map2 is now in-flight
      expect(callback2Called).toBe(true);
      expect(queue.pendingCount).toBe(0);
    });

    test("should complete if CoValue is streaming for known-state updates", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map1 = group.createMap();
      const map2 = group.createMap();

      vi.spyOn(map1.core, "isStreaming").mockReturnValue(true);

      let callback2Called = false;

      queue.enqueue(map1.core, () => {});
      queue.enqueue(map2.core, () => {
        callback2Called = true;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(callback2Called).toBe(false);

      queue.trackComplete(map1.core, "known");

      expect(queue.inFlightCount).toBe(1); // map2 is now in-flight
      expect(callback2Called).toBe(true);
      expect(queue.pendingCount).toBe(0);
    });
  });

  describe("trackUpdate", () => {
    test("should refresh timeout for in-flight streaming CoValue", async () => {
      vi.useFakeTimers();
      CO_VALUE_LOADING_CONFIG.TIMEOUT = 1000;
      setMaxInFlightLoadsPerPeer(1);

      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map = group.createMap();
      map.set("key", "value");

      // Mock isStreaming to return true
      vi.spyOn(map.core, "isStreaming").mockReturnValue(true);

      queue.enqueue(map.core, () => {});

      expect(queue.inFlightCount).toBe(1);

      // Advance time to just before timeout
      await vi.advanceTimersByTimeAsync(900);

      // Refresh the timeout (simulating receiving a chunk)
      queue.trackUpdate(map.core);

      // Advance another 200ms - would have timed out without the refresh
      await vi.advanceTimersByTimeAsync(200);

      // Should still be in-flight because timeout was refreshed
      expect(queue.inFlightCount).toBe(1);

      // Now advance past the new timeout
      await vi.advanceTimersByTimeAsync(900);

      // Should have timed out now
      expect(queue.inFlightCount).toBe(0);
    });

    test("should be a no-op for unknown CoValues", () => {
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map = group.createMap();
      const unknownCoValue = node.getCoValue(
        "co_zTestTrackUpdateUnknown01" as any,
      );

      queue.enqueue(map.core, () => {});

      expect(queue.inFlightCount).toBe(1);

      // trackUpdate on unknown CoValue should be a no-op
      queue.trackUpdate(unknownCoValue);

      // Should still have the same in-flight count
      expect(queue.inFlightCount).toBe(1);
    });
  });

  describe("immediate mode", () => {
    test("should send immediately when mode is immediate even at capacity", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map1 = group.createMap();
      const map2 = group.createMap();

      let callback1Called = false;
      let callback2Called = false;

      // Fill the queue to capacity
      queue.enqueue(map1.core, () => {
        callback1Called = true;
      });

      expect(callback1Called).toBe(true);
      expect(queue.inFlightCount).toBe(1);

      // This should bypass the capacity limit with immediate mode
      queue.enqueue(
        map2.core,
        () => {
          callback2Called = true;
        },
        "immediate",
      );

      // Both should have been called even though capacity is 1
      expect(callback2Called).toBe(true);
      expect(queue.inFlightCount).toBe(2);
    });

    test("should still track immediate requests for timeout handling", async () => {
      vi.useFakeTimers();
      CO_VALUE_LOADING_CONFIG.TIMEOUT = 1000;
      setMaxInFlightLoadsPerPeer(1);

      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();

      const coValue = node.getCoValue("co_zTestOverflowTimeout01" as any);

      queue.enqueue(coValue, () => {}, "immediate");

      expect(queue.inFlightCount).toBe(1);

      // Advance time past the timeout
      await vi.advanceTimersByTimeAsync(1001);

      // Should be removed from in-flight and marked as not found
      expect(queue.inFlightCount).toBe(0);
      expect(coValue.getLoadingStateForPeer(TEST_PEER_ID)).toBe("unavailable");
    });

    test("should still deduplicate immediate requests", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map = group.createMap();

      let callbackCount = 0;

      queue.enqueue(
        map.core,
        () => {
          callbackCount++;
        },
        "immediate",
      );
      queue.enqueue(
        map.core,
        () => {
          callbackCount++;
        },
        "immediate",
      );

      // Should only be called once due to deduplication
      expect(callbackCount).toBe(1);
      expect(queue.inFlightCount).toBe(1);
    });

    test("should process pending requests when immediate request completes", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map1 = group.createMap();
      const immediateMap = group.createMap();
      const map2 = group.createMap();

      let callback2Called = false;

      // Fill the queue
      queue.enqueue(map1.core, () => {});

      // Add pending request
      queue.enqueue(map2.core, () => {
        callback2Called = true;
      });

      expect(callback2Called).toBe(false);
      expect(queue.pendingCount).toBe(1);

      // Add immediate request - this bypasses the queue but still counts as in-flight
      queue.enqueue(immediateMap.core, () => {}, "immediate");

      expect(queue.inFlightCount).toBe(2);

      // Complete the immediate request
      queue.trackComplete(immediateMap.core);

      // Still at capacity because map1 is still in-flight
      expect(callback2Called).toBe(false);
      expect(queue.inFlightCount).toBe(1);

      // Complete the regular request - now pending can be processed
      queue.trackComplete(map1.core);

      expect(callback2Called).toBe(true);
      expect(queue.inFlightCount).toBe(1); // map2 is now in-flight
    });
  });

  describe("clear", () => {
    test("should clear all in-flight loads and pending queues", () => {
      setMaxInFlightLoadsPerPeer(2);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map1 = group.createMap();
      const map2 = group.createMap();
      const map3 = group.createMap();

      queue.enqueue(map1.core, () => {});
      queue.enqueue(map2.core, () => {});
      queue.enqueue(map3.core, () => {});

      expect(queue.inFlightCount).toBe(2);
      expect(queue.pendingCount).toBe(1);

      queue.clear();

      expect(queue.inFlightCount).toBe(0);
      expect(queue.pendingCount).toBe(0);
    });

    test("should cancel any pending timeout", async () => {
      vi.useFakeTimers();
      CO_VALUE_LOADING_CONFIG.TIMEOUT = 1000;

      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();

      const coValue = node.getCoValue("co_zTestClearTimeout00001" as any);

      queue.enqueue(coValue, () => {});

      expect(queue.inFlightCount).toBe(1);

      // Clear before timeout
      queue.clear();

      expect(queue.inFlightCount).toBe(0);

      // Advance time past the timeout
      await vi.advanceTimersByTimeAsync(1001);

      // Should not have marked as not found since we cleared
      expect(coValue.getLoadingStateForPeer(TEST_PEER_ID)).not.toBe(
        "unavailable",
      );
    });

    test("should balance push/pull metrics when clearing pending items", async () => {
      const metricReader = createTestMetricReader();
      onTestFinished(tearDownTestMetricReader);

      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue first (this item is pushed then immediately shifted to go in-flight)
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      // Enqueue multiple items that will be pending
      const map1 = group.createMap();
      const map2 = group.createMap();
      const map3 = group.createMap();

      queue.enqueue(map1.core, () => {});
      queue.enqueue(map2.core, () => {});
      queue.enqueue(map3.core, () => {});

      expect(queue.pendingCount).toBe(3);

      // Get metrics before clear
      // 4 items pushed (blocker + 3 pending), 1 pulled (blocker was shifted to go in-flight)
      const pushedBefore = await metricReader.getMetricValue(
        "jazz.messagequeue.load-requests-queue.pushed",
        { priority: "high" },
      );
      const pulledBefore = await metricReader.getMetricValue(
        "jazz.messagequeue.load-requests-queue.pulled",
        { priority: "high" },
      );

      expect(pushedBefore).toBe(4);
      expect(pulledBefore).toBe(1);

      // Clear the queue - this should drain pending items, incrementing pulled counter
      queue.clear();

      // Get metrics after clear - pushed and pulled should now be equal
      const pushedAfter = await metricReader.getMetricValue(
        "jazz.messagequeue.load-requests-queue.pushed",
        { priority: "high" },
      );
      const pulledAfter = await metricReader.getMetricValue(
        "jazz.messagequeue.load-requests-queue.pulled",
        { priority: "high" },
      );

      // All 4 pushed items should now be pulled (balanced)
      expect(pushedAfter).toBe(4);
      expect(pulledAfter).toBe(4);
      expect(pushedAfter).toBe(pulledAfter);
    });

    test("should track in-flight loads with OpenTelemetry", async () => {
      const metricReader = createTestMetricReader();
      onTestFinished(tearDownTestMetricReader);

      setMaxInFlightLoadsPerPeer(2);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const map1 = group.createMap();
      const map2 = group.createMap();

      expect(
        await metricReader.getMetricValue("jazz.loadqueue.outgoing.inflight"),
      ).toBe(0);

      queue.enqueue(map1.core, () => {});
      expect(
        await metricReader.getMetricValue("jazz.loadqueue.outgoing.inflight"),
      ).toBe(1);

      queue.enqueue(map2.core, () => {});
      expect(
        await metricReader.getMetricValue("jazz.loadqueue.outgoing.inflight"),
      ).toBe(2);

      queue.trackComplete(map1.core);
      expect(
        await metricReader.getMetricValue("jazz.loadqueue.outgoing.inflight"),
      ).toBe(1);

      queue.clear();
      expect(
        await metricReader.getMetricValue("jazz.loadqueue.outgoing.inflight"),
      ).toBe(0);
    });

    test("should decrement in-flight metric when a load times out", async () => {
      CO_VALUE_LOADING_CONFIG.TIMEOUT = 5;

      const metricReader = createTestMetricReader();
      onTestFinished(tearDownTestMetricReader);

      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const coValue = node.getCoValue("co_zTestMetricTimeout00001" as any);

      queue.enqueue(coValue, () => {});
      expect(
        await metricReader.getMetricValue("jazz.loadqueue.outgoing.inflight"),
      ).toBe(1);

      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(
        await metricReader.getMetricValue("jazz.loadqueue.outgoing.inflight"),
      ).toBe(0);
    });

    test("should not change in-flight metric when completing unknown CoValue", async () => {
      const metricReader = createTestMetricReader();
      onTestFinished(tearDownTestMetricReader);

      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();
      const knownMap = group.createMap();
      const unknownCoValue = node.getCoValue(
        "co_zTestUnknownMetric0001" as any,
      );

      queue.enqueue(knownMap.core, () => {});
      expect(
        await metricReader.getMetricValue("jazz.loadqueue.outgoing.inflight"),
      ).toBe(1);

      queue.trackComplete(unknownCoValue);
      expect(
        await metricReader.getMetricValue("jazz.loadqueue.outgoing.inflight"),
      ).toBe(1);

      queue.trackComplete(knownMap.core);
      expect(
        await metricReader.getMetricValue("jazz.loadqueue.outgoing.inflight"),
      ).toBe(0);
    });
  });

  describe("priority queue", () => {
    test("should process high-priority requests before low-priority", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue first
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      // Enqueue low-priority requests
      const lowMap1 = group.createMap();
      const lowMap2 = group.createMap();

      // Enqueue high-priority request
      const highMap = group.createMap();

      const order: string[] = [];

      // Add low-priority first
      queue.enqueue(lowMap1.core, () => order.push("low1"), "low-priority");
      queue.enqueue(lowMap2.core, () => order.push("low2"), "low-priority");
      // Add high-priority after
      queue.enqueue(highMap.core, () => order.push("high"));

      expect(queue.pendingCount).toBe(3);
      expect(queue.highPriorityPendingCount).toBe(1);
      expect(queue.lowPriorityPendingCount).toBe(2);

      // Complete the blocker and process requests one by one
      queue.trackComplete(blockerMap.core);
      expect(order).toEqual(["high"]);

      queue.trackComplete(highMap.core);
      expect(order).toEqual(["high", "low1"]);

      queue.trackComplete(lowMap1.core);
      expect(order).toEqual(["high", "low1", "low2"]);
    });

    test("should add low-priority request to low-priority queue", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      const lowMap = group.createMap();
      queue.enqueue(lowMap.core, () => {}, "low-priority");

      expect(queue.pendingCount).toBe(1);
      expect(queue.lowPriorityPendingCount).toBe(1);
      expect(queue.highPriorityPendingCount).toBe(0);
    });

    test("should upgrade low-priority to high-priority when requested with normal priority", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      const targetMap = group.createMap();
      let callbackCount = 0;

      // First enqueue as low-priority
      queue.enqueue(
        targetMap.core,
        () => {
          callbackCount++;
        },
        "low-priority",
      );

      expect(queue.lowPriorityPendingCount).toBe(1);
      expect(queue.highPriorityPendingCount).toBe(0);

      // Upgrade by requesting with normal priority
      queue.enqueue(targetMap.core, () => {
        callbackCount += 10;
      });

      // Should have moved to high-priority queue
      expect(queue.lowPriorityPendingCount).toBe(0);
      expect(queue.highPriorityPendingCount).toBe(1);

      // Complete blocker and verify upgraded callback is used
      queue.trackComplete(blockerMap.core);
      expect(callbackCount).toBe(10); // Upgraded callback should be called
    });

    test("should not downgrade high-priority to low-priority", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      const targetMap = group.createMap();

      // First enqueue as high-priority
      queue.enqueue(targetMap.core, () => {});

      expect(queue.highPriorityPendingCount).toBe(1);
      expect(queue.lowPriorityPendingCount).toBe(0);

      // Try to enqueue as low-priority
      queue.enqueue(targetMap.core, () => {}, "low-priority");

      // Should still be in high-priority queue (no change)
      expect(queue.highPriorityPendingCount).toBe(1);
      expect(queue.lowPriorityPendingCount).toBe(0);
    });

    test("should process mixed priority requests in correct order", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue first
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      const order: string[] = [];

      // Interleave low and high priority
      const low1 = group.createMap();
      const high1 = group.createMap();
      const low2 = group.createMap();
      const high2 = group.createMap();
      const low3 = group.createMap();

      queue.enqueue(low1.core, () => order.push("low1"), "low-priority");
      queue.enqueue(high1.core, () => order.push("high1"));
      queue.enqueue(low2.core, () => order.push("low2"), "low-priority");
      queue.enqueue(high2.core, () => order.push("high2"));
      queue.enqueue(low3.core, () => order.push("low3"), "low-priority");

      expect(queue.highPriorityPendingCount).toBe(2);
      expect(queue.lowPriorityPendingCount).toBe(3);

      // Process all
      queue.trackComplete(blockerMap.core);
      queue.trackComplete(high1.core);
      queue.trackComplete(high2.core);
      queue.trackComplete(low1.core);
      queue.trackComplete(low2.core);

      // High priority should come first, then low priority
      expect(order).toEqual(["high1", "high2", "low1", "low2", "low3"]);
    });

    test("should handle upgrade when item is the only one in low-priority queue", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      const targetMap = group.createMap();

      // Add single low-priority item
      queue.enqueue(targetMap.core, () => {}, "low-priority");

      expect(queue.lowPriorityPendingCount).toBe(1);

      // Upgrade it
      queue.enqueue(targetMap.core, () => {});

      expect(queue.lowPriorityPendingCount).toBe(0);
      expect(queue.highPriorityPendingCount).toBe(1);
    });

    test("should handle upgrade when item is in the middle of low-priority queue", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      const order: string[] = [];
      const low1 = group.createMap();
      const low2 = group.createMap();
      const low3 = group.createMap();

      // Add three low-priority items
      queue.enqueue(low1.core, () => order.push("low1"), "low-priority");
      queue.enqueue(low2.core, () => order.push("low2"), "low-priority");
      queue.enqueue(low3.core, () => order.push("low3"), "low-priority");

      expect(queue.lowPriorityPendingCount).toBe(3);

      // Upgrade the middle one (uses new callback)
      queue.enqueue(low2.core, () => order.push("upgraded"));

      expect(queue.lowPriorityPendingCount).toBe(2);
      expect(queue.highPriorityPendingCount).toBe(1);

      // Process all
      queue.trackComplete(blockerMap.core);
      queue.trackComplete(low2.core);
      queue.trackComplete(low1.core);

      // upgraded should be first (high priority with new callback), then low1 and low3
      expect(order).toEqual(["upgraded", "low1", "low3"]);
    });

    test("should execute immediately when upgrading low-priority with immediate mode", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      const targetMap = group.createMap();
      let originalCallbackCalled = false;
      let immediateCallbackCalled = false;

      // Add low-priority item
      queue.enqueue(
        targetMap.core,
        () => {
          originalCallbackCalled = true;
        },
        "low-priority",
      );

      expect(queue.lowPriorityPendingCount).toBe(1);
      expect(originalCallbackCalled).toBe(false);

      // Upgrade with immediate mode - should execute immediately with new callback
      queue.enqueue(
        targetMap.core,
        () => {
          immediateCallbackCalled = true;
        },
        "immediate",
      );

      // Should have been executed immediately, not moved to high-priority queue
      expect(queue.lowPriorityPendingCount).toBe(0);
      expect(queue.highPriorityPendingCount).toBe(0);
      expect(originalCallbackCalled).toBe(false); // Original callback not called
      expect(immediateCallbackCalled).toBe(true); // New callback called
      // Should be in-flight now (2 because blocker is also in-flight)
      expect(queue.inFlightCount).toBe(2);
    });

    test("should execute immediately when upgrading high-priority with immediate mode", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      const targetMap = group.createMap();
      let originalCallbackCalled = false;
      let immediateCallbackCalled = false;

      // Add high-priority item (it will be queued since blocker is in-flight)
      queue.enqueue(targetMap.core, () => {
        originalCallbackCalled = true;
      });

      expect(queue.highPriorityPendingCount).toBe(1);
      expect(originalCallbackCalled).toBe(false);

      // Request with immediate mode - should execute immediately with new callback
      queue.enqueue(
        targetMap.core,
        () => {
          immediateCallbackCalled = true;
        },
        "immediate",
      );

      // Should have been executed immediately, removed from high-priority queue
      expect(queue.highPriorityPendingCount).toBe(0);
      expect(originalCallbackCalled).toBe(false); // Original callback not called
      expect(immediateCallbackCalled).toBe(true); // New callback called
      // Should be in-flight now (2 because blocker is also in-flight)
      expect(queue.inFlightCount).toBe(2);
    });

    test("should skip immediate request on an in-flight value", () => {
      setMaxInFlightLoadsPerPeer(10);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const targetMap = group.createMap();
      let firstCallbackCount = 0;
      let secondCallbackCount = 0;

      // First enqueue - should go in-flight immediately
      queue.enqueue(targetMap.core, () => {
        firstCallbackCount++;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(firstCallbackCount).toBe(1);

      // Immediate request on in-flight value - should be skipped
      queue.enqueue(
        targetMap.core,
        () => {
          secondCallbackCount++;
        },
        "immediate",
      );

      // Should not have sent again
      expect(queue.inFlightCount).toBe(1);
      expect(firstCallbackCount).toBe(1);
      expect(secondCallbackCount).toBe(0);
    });

    test("should skip low-priority request on an in-flight value", () => {
      setMaxInFlightLoadsPerPeer(10);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      const targetMap = group.createMap();
      let firstCallbackCount = 0;
      let secondCallbackCount = 0;

      // First enqueue - should go in-flight immediately
      queue.enqueue(targetMap.core, () => {
        firstCallbackCount++;
      });

      expect(queue.inFlightCount).toBe(1);
      expect(firstCallbackCount).toBe(1);

      // Low-priority request on in-flight value - should be skipped
      queue.enqueue(
        targetMap.core,
        () => {
          secondCallbackCount++;
        },
        "low-priority",
      );

      // Should not have sent again or queued
      expect(queue.inFlightCount).toBe(1);
      expect(queue.lowPriorityPendingCount).toBe(0);
      expect(firstCallbackCount).toBe(1);
      expect(secondCallbackCount).toBe(0);
    });

    test("should clear both priority queues on clear()", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      const highMap = group.createMap();
      const lowMap = group.createMap();

      queue.enqueue(highMap.core, () => {});
      queue.enqueue(lowMap.core, () => {}, "low-priority");

      expect(queue.highPriorityPendingCount).toBe(1);
      expect(queue.lowPriorityPendingCount).toBe(1);

      queue.clear();

      expect(queue.highPriorityPendingCount).toBe(0);
      expect(queue.lowPriorityPendingCount).toBe(0);
      expect(queue.pendingCount).toBe(0);
    });
  });
});
