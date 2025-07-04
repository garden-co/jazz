import { beforeEach, describe, expect, test, vi } from "vitest";
import { IncomingMessagesQueue } from "../IncomingMessagesQueue.js";
import { PeerState } from "../PeerState.js";
import { ConnectedPeerChannel } from "../streamUtils.js";
import { Peer, SyncMessage } from "../sync.js";

// Mock performance.now for consistent timing tests
const mockPerformanceNow = vi.fn();
Object.defineProperty(global, "performance", {
  value: {
    now: mockPerformanceNow,
  },
  writable: true,
});

function createMockPeer(id: string): Peer {
  return {
    id,
    role: "client",
    incoming: new ConnectedPeerChannel(),
    outgoing: new ConnectedPeerChannel(),
  };
}

function createMockPeerState(id: string): PeerState {
  const peer = createMockPeer(id);
  return new PeerState(peer, undefined);
}

function createMockSyncMessage(
  id: string,
  action: "load" | "known" | "content" | "done" = "load",
): SyncMessage {
  if (action === "load" || action === "known") {
    return {
      action,
      id: `co_z${id}`,
      header: false,
      sessions: {},
    };
  } else if (action === "content") {
    return {
      action: "content",
      id: `co_z${id}`,
      priority: 3, // MEDIUM priority
      new: {},
    };
  } else {
    return {
      action: "done",
      id: `co_z${id}`,
    };
  }
}

function setup() {
  const queue = new IncomingMessagesQueue();
  const peer1 = createMockPeerState("peer1");
  const peer2 = createMockPeerState("peer2");

  return { queue, peer1, peer2 };
}

beforeEach(() => {
  mockPerformanceNow.mockReturnValue(0);
});

describe("IncomingMessagesQueue", () => {
  describe("constructor", () => {
    test("should initialize with empty state", () => {
      const { queue } = setup();
      expect(queue["queues"]).toEqual([]);
      expect(queue.currentQueue).toBe(0);
      expect(queue.processing).toBe(false);
    });
  });

  describe("push", () => {
    test("should add message to new peer queue", () => {
      const { queue, peer1 } = setup();
      const msg = createMockSyncMessage("test");
      queue.push(msg, peer1);

      expect(queue["queues"].length).toBe(1);
      expect(queue["queues"][0]?.[1]).toBe(peer1);
      expect(queue["peerToQueue"].has(peer1)).toBe(true);
    });

    test("should add message to existing peer queue", () => {
      const { queue, peer1 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");

      queue.push(msg1, peer1);
      queue.push(msg2, peer1);

      expect(queue["queues"].length).toBe(1);
      const peerQueue = queue["peerToQueue"].get(peer1);
      expect(peerQueue).toBeDefined();
      expect(peerQueue?.length).toBe(2);
    });

    test("should handle multiple peers", () => {
      const { queue, peer1, peer2 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");

      queue.push(msg1, peer1);
      queue.push(msg2, peer2);

      expect(queue["queues"].length).toBe(2);
      expect(queue["peerToQueue"].has(peer1)).toBe(true);
      expect(queue["peerToQueue"].has(peer2)).toBe(true);
    });
  });

  describe("pull", () => {
    test("should return undefined for empty queue", () => {
      const { queue } = setup();
      expect(queue.pull()).toBeUndefined();
    });

    test("should pull message from first peer", () => {
      const { queue, peer1 } = setup();
      const msg = createMockSyncMessage("test");
      queue.push(msg, peer1);

      const result = queue.pull();
      expect(result).toEqual({ msg, peer: peer1 });
    });

    test("should pull messages in round-robin order", () => {
      const { queue, peer1, peer2 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");
      const msg3 = createMockSyncMessage("test3");

      queue.push(msg1, peer1);
      queue.push(msg2, peer1);
      queue.push(msg3, peer2);

      // First pull from peer1
      expect(queue.pull()).toEqual({ msg: msg1, peer: peer1 });
      // Second pull from peer2 (round-robin)
      expect(queue.pull()).toEqual({ msg: msg3, peer: peer2 });
      // Third pull from peer1 (back to first)
      expect(queue.pull()).toEqual({ msg: msg2, peer: peer1 });
    });

    test("should remove peer when their queue becomes empty", () => {
      const { queue, peer1 } = setup();
      const msg = createMockSyncMessage("test");
      queue.push(msg, peer1);

      queue.pull();

      expect(queue["queues"].length).toBe(0);
      expect(queue["peerToQueue"].has(peer1)).toBe(false);
    });

    test("should not advance currentQueue when only one peer has messages", () => {
      const { queue, peer1 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");

      queue.push(msg1, peer1);
      queue.push(msg2, peer1);

      queue.pull(); // Pull first message

      expect(queue.currentQueue).toBe(0); // Only one queue, so stays at 0
      expect(queue["queues"].length).toBe(1); // Peer still has messages
    });

    test("should reset currentQueue when it reaches queue length", () => {
      const { queue, peer1, peer2 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");

      queue.push(msg1, peer1);
      queue.push(msg2, peer2);

      queue.pull(); // Pull from peer1, currentQueue becomes 1
      queue.pull(); // Pull from peer2, currentQueue becomes 2, then resets to 0

      expect(queue.currentQueue).toBe(0);
    });

    test("should handle currentQueue reset when queues are removed", () => {
      const { queue, peer1, peer2 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");

      queue.push(msg1, peer1);
      queue.push(msg2, peer2);

      queue.pull(); // Pull from peer1, currentQueue becomes 1
      queue.pull(); // Pull from peer2, currentQueue becomes 2, then resets to 0

      // Add another message to peer1
      const msg3 = createMockSyncMessage("test3");
      queue.push(msg3, peer1);

      // Should pull from peer1 again (currentQueue is 0)
      expect(queue.pull()).toEqual({ msg: msg3, peer: peer1 });
    });
  });

  describe("processQueue", () => {
    test("should process all messages in queue", async () => {
      const { queue, peer1, peer2 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");
      const msg3 = createMockSyncMessage("test3");

      queue.push(msg1, peer1);
      queue.push(msg2, peer1);
      queue.push(msg3, peer2);

      const processedMessages: Array<{ msg: SyncMessage; peer: PeerState }> =
        [];

      await queue.processQueue((msg, peer) => {
        processedMessages.push({ msg, peer });
      });

      expect(processedMessages).toEqual([
        { msg: msg1, peer: peer1 },
        { msg: msg3, peer: peer2 },
        { msg: msg2, peer: peer1 },
      ]);
      expect(queue.processing).toBe(false);
    });

    test("should set processing flag during execution", async () => {
      const { queue, peer1 } = setup();
      const msg = createMockSyncMessage("test");
      queue.push(msg, peer1);

      let processingFlagDuringExecution = false;
      const processingPromise = queue.processQueue(() => {
        processingFlagDuringExecution = queue.processing;
      });

      await processingPromise;
      expect(processingFlagDuringExecution).toBe(true);
      expect(queue.processing).toBe(false);
    });

    test("should handle empty queue", async () => {
      const { queue } = setup();
      const callback = vi.fn();

      await queue.processQueue(callback);

      expect(callback).not.toHaveBeenCalled();
      expect(queue.processing).toBe(false);
    });

    test("should yield to event loop when processing takes too long", async () => {
      const { queue, peer1 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");

      queue.push(msg1, peer1);
      queue.push(msg2, peer1);

      // Mock timing to simulate long processing
      mockPerformanceNow
        .mockReturnValueOnce(0) // Initial time
        .mockReturnValueOnce(60); // After first message (60ms > 50ms threshold)

      const setTimeoutSpy = vi.spyOn(global, "setTimeout");

      await queue.processQueue(() => {
        // Simulate some processing time
      });

      expect(setTimeoutSpy).toHaveBeenCalledWith(expect.any(Function), 0);
    });

    test("should not yield to event loop when processing is fast", async () => {
      const { queue, peer1 } = setup();
      const msg = createMockSyncMessage("test");
      queue.push(msg, peer1);

      // Mock timing to simulate fast processing
      mockPerformanceNow
        .mockReturnValueOnce(0) // Initial time
        .mockReturnValueOnce(30); // After message (30ms < 50ms threshold)

      const setTimeoutSpy = vi.spyOn(global, "setTimeout");

      await queue.processQueue(() => {
        // Simulate some processing time
      });

      expect(setTimeoutSpy).not.toHaveBeenCalled();
    });

    test("should handle callback errors gracefully", async () => {
      const { queue, peer1 } = setup();
      const msg = createMockSyncMessage("test");
      queue.push(msg, peer1);

      const error = new Error("Callback error");

      await queue.processQueue(() => {
        throw error;
      });

      // The processing flag should be reset even when an error occurs
      expect(queue.processing).toBe(false);
    });

    test("should process messages in correct round-robin order", async () => {
      const { queue, peer1, peer2 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");
      const msg3 = createMockSyncMessage("test3");
      const msg4 = createMockSyncMessage("test4");

      queue.push(msg1, peer1);
      queue.push(msg2, peer1);
      queue.push(msg3, peer2);
      queue.push(msg4, peer2);

      const processedMessages: Array<{ msg: SyncMessage; peer: PeerState }> =
        [];

      await queue.processQueue((msg, peer) => {
        processedMessages.push({ msg, peer });
      });

      // Should process in round-robin: peer1, peer2, peer1, peer2
      expect(processedMessages).toEqual([
        { msg: msg1, peer: peer1 },
        { msg: msg3, peer: peer2 },
        { msg: msg2, peer: peer1 },
        { msg: msg4, peer: peer2 },
      ]);
    });
  });

  describe("edge cases", () => {
    test("should handle peer with multiple messages correctly", () => {
      const { queue, peer1 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");
      const msg3 = createMockSyncMessage("test3");

      queue.push(msg1, peer1);
      queue.push(msg2, peer1);
      queue.push(msg3, peer1);

      expect(queue.pull()).toEqual({ msg: msg1, peer: peer1 });
      expect(queue["queues"].length).toBe(1); // Peer still has messages
      expect(queue.currentQueue).toBe(0); // Only one queue, so stays at 0

      expect(queue.pull()).toEqual({ msg: msg2, peer: peer1 });
      expect(queue["queues"].length).toBe(1); // Peer still has messages
      expect(queue.currentQueue).toBe(0); // Only one queue, so stays at 0

      expect(queue.pull()).toEqual({ msg: msg3, peer: peer1 });
      expect(queue["queues"].length).toBe(0); // Peer queue is now empty
      expect(queue.currentQueue).toBe(0); // Reset to 0
    });

    test("should handle rapid push and pull operations", () => {
      const { queue, peer1 } = setup();
      const msg1 = createMockSyncMessage("test1");
      const msg2 = createMockSyncMessage("test2");

      queue.push(msg1, peer1);
      expect(queue.pull()).toEqual({ msg: msg1, peer: peer1 });

      queue.push(msg2, peer1);
      expect(queue.pull()).toEqual({ msg: msg2, peer: peer1 });

      expect(queue["queues"].length).toBe(0);
      expect(queue["peerToQueue"].has(peer1)).toBe(false);
    });

    test("should handle different message types", () => {
      const { queue, peer1, peer2 } = setup();
      const loadMsg = createMockSyncMessage("load", "load");
      const knownMsg = createMockSyncMessage("known", "known");
      const contentMsg = createMockSyncMessage("content", "content");
      const doneMsg = createMockSyncMessage("done", "done");

      queue.push(loadMsg, peer1);
      queue.push(knownMsg, peer1);
      queue.push(contentMsg, peer2);
      queue.push(doneMsg, peer2);

      expect(queue.pull()).toEqual({ msg: loadMsg, peer: peer1 });
      expect(queue.pull()).toEqual({ msg: contentMsg, peer: peer2 });
      expect(queue.pull()).toEqual({ msg: knownMsg, peer: peer1 });
      expect(queue.pull()).toEqual({ msg: doneMsg, peer: peer2 });
    });
  });

  describe("concurrent operations", () => {
    test("should prevent multiple concurrent processQueue calls", async () => {
      const { queue, peer1 } = setup();
      const msg = createMockSyncMessage("test");
      queue.push(msg, peer1);

      const firstProcessSpy = vi.fn();

      const firstProcess = queue.processQueue((msg, peer) => {
        firstProcessSpy(msg, peer);
      });

      const secondProcessSpy = vi.fn();

      // Second process should not interfere
      const secondProcess = queue.processQueue(() => {
        secondProcessSpy();
      });

      await firstProcess;
      await secondProcess;

      expect(firstProcessSpy).toHaveBeenCalled();
      expect(secondProcessSpy).not.toHaveBeenCalled();

      expect(queue.processing).toBe(false);
    });
  });
});
