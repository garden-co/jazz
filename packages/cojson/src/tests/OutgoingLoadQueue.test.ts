import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import {
  CO_VALUE_LOADING_CONFIG,
  setMaxInFlightLoadsPerPeer,
} from "../config.js";
import { OutgoingLoadQueue } from "../queue/OutgoingLoadQueue.js";
import type { PeerID } from "../sync.js";
import { createTestNode } from "./testUtils.js";

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

  describe("priority ordering", () => {
    test("should process unavailable CoValues before available ones", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Create an available CoValue
      const availableMap = group.createMap();

      // Get an unavailable CoValue (doesn't exist locally)
      const unavailableCoValue = node.getCoValue(
        "co_zTestUnavailableCoValueId1234" as any,
      );

      const order: string[] = [];

      // First enqueue available, then unavailable
      // But block processing by filling the queue first
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {
        order.push("blocker");
      });

      // Now queue is at capacity, enqueue available then unavailable
      queue.enqueue(availableMap.core, () => {
        order.push("available");
      });
      queue.enqueue(unavailableCoValue, () => {
        order.push("unavailable");
      });

      expect(queue.pendingHighCount).toBe(1); // unavailable
      expect(queue.pendingLowCount).toBe(1); // available

      // Complete the blocker to start processing
      queue.trackComplete(blockerMap.core);

      // Unavailable should be processed first (high priority)
      expect(order).toEqual(["blocker", "unavailable"]);
    });

    test("should maintain FIFO order within each priority tier", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const group = node.createGroup();

      // Block the queue first
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      // Enqueue multiple unavailable CoValues
      const unavailable1 = node.getCoValue(
        "co_zTestUnavailable00000001" as any,
      );
      const unavailable2 = node.getCoValue(
        "co_zTestUnavailable00000002" as any,
      );
      const unavailable3 = node.getCoValue(
        "co_zTestUnavailable00000003" as any,
      );

      const order: string[] = [];

      queue.enqueue(unavailable1, () => order.push("unavailable1"));
      queue.enqueue(unavailable2, () => order.push("unavailable2"));
      queue.enqueue(unavailable3, () => order.push("unavailable3"));

      // Complete requests one by one
      queue.trackComplete(blockerMap.core);
      queue.trackComplete(unavailable1);
      queue.trackComplete(unavailable2);

      expect(order).toEqual(["unavailable1", "unavailable2", "unavailable3"]);
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
      expect(queue.pendingLowCount).toBe(1);
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
      expect(queue.pendingLowCount).toBe(0);
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

      expect(queue.pendingLowCount).toBe(1);
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

    test("should skip duplicate enqueue across availability tiers", () => {
      setMaxInFlightLoadsPerPeer(1);
      const queue = new OutgoingLoadQueue(TEST_PEER_ID);
      const node = createTestNode();
      const otherNode = createTestNode();
      const group = node.createGroup();

      const availableMap = group.createMap();
      const unavailableCoValue = otherNode.getCoValue(availableMap.id);

      queue.enqueue(availableMap.core, () => {});
      queue.trackComplete(availableMap.core);

      // Block capacity so the next enqueue stays pending
      const blockerMap = group.createMap();
      queue.enqueue(blockerMap.core, () => {});

      let duplicateCallbackCount = 0;
      queue.enqueue(availableMap.core, () => {});
      queue.enqueue(unavailableCoValue, () => {
        duplicateCallbackCount += 1;
      });

      expect(queue.pendingLowCount + queue.pendingHighCount).toBe(1);
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
      expect(queue.pendingLowCount).toBe(1);

      queue.clear();

      expect(queue.inFlightCount).toBe(0);
      expect(queue.pendingLowCount).toBe(0);
      expect(queue.pendingHighCount).toBe(0);
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
  });
});
