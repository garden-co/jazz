import { describe, expect, test, vi } from "vitest";
import { StoreQueue } from "../queue/StoreQueue.js";
import { NewContentMessage } from "../sync.js";
import { CoValueKnownState } from "../knownState.js";

function createMockNewContentMessage(id: string): NewContentMessage {
  return {
    action: "content",
    id: id as any,
    priority: 0,
    new: {},
  };
}

function setup() {
  const storeQueue = new StoreQueue();
  const mockCallback = vi.fn();
  const mockCorrectionCallback = vi.fn();

  return { storeQueue, mockCallback, mockCorrectionCallback };
}

describe("StoreQueue", () => {
  describe("push and pull", () => {
    test("should push and pull entries in FIFO order", () => {
      const { storeQueue, mockCorrectionCallback } = setup();
      const data1 = createMockNewContentMessage("co1");
      const data2 = createMockNewContentMessage("co2");

      storeQueue.push(data1, mockCorrectionCallback);
      storeQueue.push(data2, mockCorrectionCallback);

      const entry1 = storeQueue.pull();
      const entry2 = storeQueue.pull();
      const entry3 = storeQueue.pull(); // Should be undefined

      expect(entry1).toEqual({
        data: data1,
        correctionCallback: mockCorrectionCallback,
      });
      expect(entry2).toEqual({
        data: data2,
        correctionCallback: mockCorrectionCallback,
      });
      expect(entry3).toBeUndefined();
    });

    test("should handle empty queue", () => {
      const { storeQueue } = setup();
      const entry = storeQueue.pull();
      expect(entry).toBeUndefined();
    });

    test("should handle single entry", () => {
      const { storeQueue, mockCorrectionCallback } = setup();
      const data = createMockNewContentMessage("co1");

      storeQueue.push(data, mockCorrectionCallback);
      const entry = storeQueue.pull();

      expect(entry).toEqual({
        data,
        correctionCallback: mockCorrectionCallback,
      });
      expect(storeQueue.pull()).toBeUndefined();
    });
  });

  describe("processQueue", () => {
    test("should process all entries in queue", async () => {
      const { storeQueue, mockCallback, mockCorrectionCallback } = setup();
      const data1 = createMockNewContentMessage("co1");
      const data2 = createMockNewContentMessage("co2");

      storeQueue.push(data1, mockCorrectionCallback);
      storeQueue.push(data2, mockCorrectionCallback);

      await storeQueue.processQueue(mockCallback);

      expect(mockCallback).toHaveBeenCalledTimes(2);
      expect(mockCallback).toHaveBeenNthCalledWith(
        1,
        data1,
        mockCorrectionCallback,
      );
      expect(mockCallback).toHaveBeenNthCalledWith(
        2,
        data2,
        mockCorrectionCallback,
      );
    });

    test("should not process if already processing", async () => {
      const { storeQueue, mockCallback, mockCorrectionCallback } = setup();
      const data = createMockNewContentMessage("co1");

      storeQueue.push(data, mockCorrectionCallback);

      // Start processing
      const processPromise1 = storeQueue.processQueue(mockCallback);

      // Try to process again while already processing
      const processPromise2 = storeQueue.processQueue(mockCallback);

      await Promise.all([processPromise1, processPromise2]);

      // Should only be called once
      expect(mockCallback).toHaveBeenCalledTimes(1);
    });

    test("should process entries pushed after a first processing round is done", async () => {
      const { storeQueue, mockCallback, mockCorrectionCallback } = setup();
      const data = createMockNewContentMessage("co1");

      storeQueue.push(data, mockCorrectionCallback);

      await storeQueue.processQueue(mockCallback);

      storeQueue.push(data, mockCorrectionCallback);

      await storeQueue.processQueue(mockCallback);

      expect(mockCallback).toHaveBeenCalledTimes(2);
    });

    test("should handle empty queue during processing", async () => {
      const { storeQueue, mockCallback } = setup();
      await storeQueue.processQueue(mockCallback);

      expect(mockCallback).not.toHaveBeenCalled();
      expect(storeQueue.processing).toBe(false);
    });

    test("should handle async callback that throws", async () => {
      const { storeQueue, mockCorrectionCallback } = setup();
      const data = createMockNewContentMessage("co1");

      storeQueue.push(data, mockCorrectionCallback);

      const errorCallback = vi.fn().mockRejectedValue(new Error("Test error"));

      await storeQueue.processQueue(errorCallback);
      expect(storeQueue.processing).toBe(false);
    });
  });

  describe("drain", () => {
    test("should remove all entries from queue", () => {
      const { storeQueue, mockCorrectionCallback } = setup();
      const data1 = createMockNewContentMessage("co1");
      const data2 = createMockNewContentMessage("co2");

      storeQueue.push(data1, mockCorrectionCallback);
      storeQueue.push(data2, mockCorrectionCallback);

      storeQueue.close();

      expect(storeQueue.pull()).toBeUndefined();
    });

    test("should handle empty queue", () => {
      const { storeQueue } = setup();
      expect(() => storeQueue.close()).not.toThrow();
      expect(storeQueue.pull()).toBeUndefined();
    });
  });

  describe("integration scenarios", () => {
    test("should handle complex workflow with multiple operations", async () => {
      const { storeQueue, mockCallback, mockCorrectionCallback } = setup();
      const data1 = createMockNewContentMessage("co1");
      const data2 = createMockNewContentMessage("co2");

      // Push entries
      storeQueue.push(data1, mockCorrectionCallback);
      storeQueue.push(data2, mockCorrectionCallback);

      // Process queue
      await storeQueue.processQueue(mockCallback);

      // Verify all entries were processed
      expect(mockCallback).toHaveBeenCalledTimes(2);
      expect(storeQueue.pull()).toBeUndefined();

      // Add more entries and process again
      const data3 = createMockNewContentMessage("co3");
      storeQueue.push(data3, mockCorrectionCallback);

      await storeQueue.processQueue(mockCallback);

      expect(mockCallback).toHaveBeenCalledTimes(3);
      expect(mockCallback).toHaveBeenLastCalledWith(
        data3,
        mockCorrectionCallback,
      );
    });

    test("should handle correction callback with known state", async () => {
      const { storeQueue, mockCorrectionCallback } = setup();
      const sessionId = "account1_session_z123" as any;
      const knownState: CoValueKnownState = {
        id: "co1" as any,
        header: true,
        sessions: { [sessionId]: 5 },
      };

      const data = createMockNewContentMessage("co1");

      storeQueue.push(data, mockCorrectionCallback);

      await storeQueue.processQueue(async (data, correctionCallback) => {
        // Simulate some processing
        await new Promise((resolve) => setTimeout(resolve, 10));
        // Call the correction callback
        correctionCallback(knownState);
      });

      expect(mockCorrectionCallback).toHaveBeenCalledWith(knownState);
    });

    test("should handle concurrent processing attempts", async () => {
      const { storeQueue, mockCallback, mockCorrectionCallback } = setup();
      const data = createMockNewContentMessage("co1");

      storeQueue.push(data, mockCorrectionCallback);

      // Start multiple processing attempts concurrently
      const promises = [
        storeQueue.processQueue(mockCallback),
        storeQueue.processQueue(mockCallback),
        storeQueue.processQueue(mockCallback),
      ];

      await Promise.all(promises);

      // Should only process once
      expect(mockCallback).toHaveBeenCalledTimes(1);
    });
  });

  describe("edge cases", () => {
    test("should handle null correction callback", () => {
      const { storeQueue } = setup();
      const data = createMockNewContentMessage("co1");

      const nullCallback = () => undefined;
      storeQueue.push(data, nullCallback);

      const entry = storeQueue.pull();
      expect(entry).toEqual({ data, correctionCallback: nullCallback });
    });

    test("should handle very large number of entries", () => {
      const { storeQueue, mockCorrectionCallback } = setup();
      const entries = 1000;

      for (let i = 0; i < entries; i++) {
        const data = createMockNewContentMessage(`co${i}`);
        storeQueue.push(data, mockCorrectionCallback);
      }

      let count = 0;
      while (storeQueue.pull()) {
        count++;
      }

      expect(count).toBe(entries);
    });
  });
});
