import { describe, expect, test, vi } from "vitest";
import { CO_VALUE_PRIORITY } from "../priority.js";
import { StorageStreamingQueue } from "../queue/StorageStreamingQueue.js";

describe("StorageStreamingQueue", () => {
  describe("constructor", () => {
    test("should initialize with empty queues", () => {
      const queue = new StorageStreamingQueue();
      expect(queue.isEmpty()).toBe(true);
    });
  });

  describe("push", () => {
    test("should add MEDIUM priority entry to queue", () => {
      const queue = new StorageStreamingQueue();

      queue.push(() => {}, CO_VALUE_PRIORITY.MEDIUM);

      expect(queue.isEmpty()).toBe(false);
    });

    test("should add LOW priority entry to queue", () => {
      const queue = new StorageStreamingQueue();

      queue.push(() => {}, CO_VALUE_PRIORITY.LOW);

      expect(queue.isEmpty()).toBe(false);
    });

    test("should throw when pushing HIGH priority entry", () => {
      const queue = new StorageStreamingQueue();

      expect(() => queue.push(() => {}, CO_VALUE_PRIORITY.HIGH)).toThrow(
        "HIGH priority content should bypass the queue and stream directly",
      );
    });

    test("should accept multiple entries", () => {
      const queue = new StorageStreamingQueue();
      const entry1 = () => {};
      const entry2 = () => {};

      queue.push(entry1, CO_VALUE_PRIORITY.MEDIUM);
      queue.push(entry2, CO_VALUE_PRIORITY.MEDIUM);

      expect(queue.pull()).toBe(entry1);
      expect(queue.pull()).toBe(entry2);
    });
  });

  describe("pull", () => {
    test("should return undefined for empty queue", () => {
      const queue = new StorageStreamingQueue();
      expect(queue.pull()).toBeUndefined();
    });

    test("should return and remove entry from queue", () => {
      const queue = new StorageStreamingQueue();
      const entry = () => {};

      queue.push(entry, CO_VALUE_PRIORITY.MEDIUM);
      const pulled = queue.pull();

      expect(pulled).toBe(entry);
      expect(queue.isEmpty()).toBe(true);
    });

    test("should pull MEDIUM priority before LOW priority", () => {
      const queue = new StorageStreamingQueue();
      const lowEntry = () => {};
      const mediumEntry = () => {};

      // Push LOW first, then MEDIUM
      queue.push(lowEntry, CO_VALUE_PRIORITY.LOW);
      queue.push(mediumEntry, CO_VALUE_PRIORITY.MEDIUM);

      // Should pull MEDIUM first
      expect(queue.pull()).toBe(mediumEntry);
      expect(queue.pull()).toBe(lowEntry);
    });

    test("should pull entries in FIFO order within same priority", () => {
      const queue = new StorageStreamingQueue();
      const entry1 = () => {};
      const entry2 = () => {};
      const entry3 = () => {};

      queue.push(entry1, CO_VALUE_PRIORITY.MEDIUM);
      queue.push(entry2, CO_VALUE_PRIORITY.MEDIUM);
      queue.push(entry3, CO_VALUE_PRIORITY.MEDIUM);

      expect(queue.pull()).toBe(entry1);
      expect(queue.pull()).toBe(entry2);
      expect(queue.pull()).toBe(entry3);
    });

    test("should handle interleaved priorities correctly", () => {
      const queue = new StorageStreamingQueue();
      const low1 = () => {};
      const medium1 = () => {};
      const low2 = () => {};
      const medium2 = () => {};

      queue.push(low1, CO_VALUE_PRIORITY.LOW);
      queue.push(medium1, CO_VALUE_PRIORITY.MEDIUM);
      queue.push(low2, CO_VALUE_PRIORITY.LOW);
      queue.push(medium2, CO_VALUE_PRIORITY.MEDIUM);

      // All MEDIUM should come first, in order
      expect(queue.pull()).toBe(medium1);
      expect(queue.pull()).toBe(medium2);
      // Then all LOW, in order
      expect(queue.pull()).toBe(low1);
      expect(queue.pull()).toBe(low2);
    });
  });

  describe("isEmpty", () => {
    test("should return true for empty queue", () => {
      const queue = new StorageStreamingQueue();
      expect(queue.isEmpty()).toBe(true);
    });

    test("should return false when MEDIUM queue has entries", () => {
      const queue = new StorageStreamingQueue();
      queue.push(() => {}, CO_VALUE_PRIORITY.MEDIUM);
      expect(queue.isEmpty()).toBe(false);
    });

    test("should return false when LOW queue has entries", () => {
      const queue = new StorageStreamingQueue();
      queue.push(() => {}, CO_VALUE_PRIORITY.LOW);
      expect(queue.isEmpty()).toBe(false);
    });

    test("should return true after all entries are pulled", () => {
      const queue = new StorageStreamingQueue();
      queue.push(() => {}, CO_VALUE_PRIORITY.MEDIUM);
      queue.push(() => {}, CO_VALUE_PRIORITY.LOW);

      queue.pull();
      queue.pull();

      expect(queue.isEmpty()).toBe(true);
    });
  });

  describe("callback invocation", () => {
    test("should not invoke callback when pushed", () => {
      const queue = new StorageStreamingQueue();
      const callback = vi.fn();

      queue.push(callback, CO_VALUE_PRIORITY.MEDIUM);

      expect(callback).not.toHaveBeenCalled();
    });

    test("should not invoke callback when pulled", () => {
      const queue = new StorageStreamingQueue();
      const callback = vi.fn();

      queue.push(callback, CO_VALUE_PRIORITY.MEDIUM);
      queue.pull();

      expect(callback).not.toHaveBeenCalled();
    });

    test("should allow caller to invoke callback after pull", () => {
      const queue = new StorageStreamingQueue();
      const callback = vi.fn();

      queue.push(callback, CO_VALUE_PRIORITY.MEDIUM);
      const pulled = queue.pull();

      expect(callback).not.toHaveBeenCalled();

      pulled?.();

      expect(callback).toHaveBeenCalledTimes(1);
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

    test("should not throw when emit is called without listener", () => {
      const queue = new StorageStreamingQueue();

      expect(() => queue.emit()).not.toThrow();
    });

    test("should call listener multiple times on multiple emits", () => {
      const queue = new StorageStreamingQueue();
      const listener = vi.fn();

      queue.setListener(listener);
      queue.emit();
      queue.emit();
      queue.emit();

      expect(listener).toHaveBeenCalledTimes(3);
    });

    test("should use latest listener when setListener is called multiple times", () => {
      const queue = new StorageStreamingQueue();
      const listener1 = vi.fn();
      const listener2 = vi.fn();

      queue.setListener(listener1);
      queue.setListener(listener2);
      queue.emit();

      expect(listener1).not.toHaveBeenCalled();
      expect(listener2).toHaveBeenCalledTimes(1);
    });
  });

  describe("edge cases", () => {
    test("should handle alternating push and pull operations", () => {
      const queue = new StorageStreamingQueue();

      const entry1 = () => {};
      const entry2 = () => {};
      const entry3 = () => {};

      queue.push(entry1, CO_VALUE_PRIORITY.MEDIUM);
      expect(queue.pull()).toBe(entry1);

      queue.push(entry2, CO_VALUE_PRIORITY.LOW);
      expect(queue.pull()).toBe(entry2);

      expect(queue.isEmpty()).toBe(true);

      queue.push(entry3, CO_VALUE_PRIORITY.MEDIUM);
      expect(queue.pull()).toBe(entry3);
    });
  });
});
