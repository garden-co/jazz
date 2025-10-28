import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { RawCoID } from "../ids";
import { StorageAPI } from "../storage/types";
import {
  createTestMetricReader,
  createTestNode,
  createUnloadedCoValue,
  tearDownTestMetricReader,
} from "./testUtils";

let metricReader: ReturnType<typeof createTestMetricReader>;

beforeEach(() => {
  metricReader = createTestMetricReader();
});

afterEach(() => {
  tearDownTestMetricReader();
});

function setup() {
  const node = createTestNode();

  const { coValue, id, header } = createUnloadedCoValue(node);

  return { node, state: coValue, id, header };
}

function createMockStorage(
  opts: {
    load?: (
      id: RawCoID,
      callback: (data: any) => void,
      done: (found: boolean) => void,
    ) => void;
    store?: (data: any, correctionCallback: any) => void;
    getKnownState?: (id: RawCoID) => any;
    waitForSync?: (id: string, coValue: any) => Promise<void>;
    close?: () => Promise<unknown> | undefined;
  } = {},
): StorageAPI {
  return {
    load: opts.load || vi.fn(),
    store: opts.store || vi.fn(),
    getKnownState: opts.getKnownState || vi.fn(),
    waitForSync: opts.waitForSync || vi.fn().mockResolvedValue(undefined),
    close: opts.close || vi.fn().mockResolvedValue(undefined),
  };
}

describe("CoValueCore.loadFromStorage", () => {
  describe("when storage is not configured", () => {
    test("should call done callback with false immediately", () => {
      const { state } = setup();
      const doneSpy = vi.fn();

      state.loadFromStorage(doneSpy);

      expect(doneSpy).toHaveBeenCalledTimes(1);
      expect(doneSpy).toHaveBeenCalledWith(false);
    });

    test("should not crash when done callback is not provided", () => {
      const { state } = setup();

      expect(() => state.loadFromStorage()).not.toThrow();
    });
  });

  describe("when current state is pending", () => {
    test("should return early when done callback is not provided", () => {
      const { state, node } = setup();
      const loadSpy = vi.fn();
      const storage = createMockStorage({ load: loadSpy });
      node.setStorage(storage);

      // Mark as pending
      state.markPending("storage");

      // Call without done callback
      state.loadFromStorage();

      // Should not call storage.load again
      expect(loadSpy).not.toHaveBeenCalled();
    });

    test("should wait for loading to complete and call done(true) when becomes available", async () => {
      const { state, node, header } = setup();
      let storageCallback: any;
      let storageDone: any;

      const storage = createMockStorage({
        load: (id, callback, done) => {
          storageCallback = callback;
          storageDone = done;
        },
      });
      node.setStorage(storage);

      // Start initial load (will mark as pending)
      state.loadFromStorage();

      // Now try to load again with a done callback while pending
      const doneSpy = vi.fn();
      state.loadFromStorage(doneSpy);

      // Should not call done yet
      expect(doneSpy).not.toHaveBeenCalled();

      // Simulate storage providing header and marking as found
      const previousState = state.loadingState;
      state.provideHeader(header);
      state.markFoundInPeer("storage", previousState);

      // Wait a tick for subscription to fire
      await new Promise((resolve) => setImmediate(resolve));

      expect(doneSpy).toHaveBeenCalledTimes(1);
      expect(doneSpy).toHaveBeenCalledWith(true);
    });

    test("should wait for loading to complete and call done(false) when becomes errored", async () => {
      const { state, node } = setup();
      const storage = createMockStorage({
        load: vi.fn(),
      });
      node.setStorage(storage);

      // Start initial load (will mark as pending)
      state.loadFromStorage();

      // Now try to load again with a done callback while pending
      const doneSpy = vi.fn();
      state.loadFromStorage(doneSpy);

      // Should not call done yet
      expect(doneSpy).not.toHaveBeenCalled();

      // Simulate error
      state.markErrored("storage", {} as any);

      // Wait a tick for subscription to fire
      await new Promise((resolve) => setImmediate(resolve));

      expect(doneSpy).toHaveBeenCalledTimes(1);
      expect(doneSpy).toHaveBeenCalledWith(false);
    });

    test("should wait for loading to complete and call done(false) when becomes unavailable", async () => {
      const { state, node } = setup();
      const storage = createMockStorage({
        load: vi.fn(),
      });
      node.setStorage(storage);

      // Start initial load (will mark as pending)
      state.loadFromStorage();

      // Now try to load again with a done callback while pending
      const doneSpy = vi.fn();
      state.loadFromStorage(doneSpy);

      // Should not call done yet
      expect(doneSpy).not.toHaveBeenCalled();

      // Simulate not found
      state.markNotFoundInPeer("storage");

      // Wait a tick for subscription to fire
      await new Promise((resolve) => setImmediate(resolve));

      expect(doneSpy).toHaveBeenCalledTimes(1);
      expect(doneSpy).toHaveBeenCalledWith(false);
    });

    test("should unsubscribe after receiving result", async () => {
      const { state, node, header } = setup();
      const storage = createMockStorage({
        load: vi.fn(),
      });
      node.setStorage(storage);

      // Start initial load (will mark as pending)
      state.loadFromStorage();

      // Now try to load again with a done callback while pending
      const doneSpy = vi.fn();
      state.loadFromStorage(doneSpy);

      // Simulate becoming available
      const previousState = state.loadingState;
      state.provideHeader(header);
      state.markFoundInPeer("storage", previousState);

      // Wait a tick for subscription to fire
      await new Promise((resolve) => setImmediate(resolve));

      expect(doneSpy).toHaveBeenCalledTimes(1);

      // Further state changes should not trigger the callback again
      state.markNotFoundInPeer("another_peer");
      await new Promise((resolve) => setImmediate(resolve));

      expect(doneSpy).toHaveBeenCalledTimes(1); // Still only called once
    });
  });

  describe("when current state is not unknown", () => {
    test("should call done(true) immediately when state is available", () => {
      const { state, node, header } = setup();
      const storage = createMockStorage();
      node.setStorage(storage);

      // Mark as available
      const previousState = state.loadingState;
      state.provideHeader(header);
      state.markFoundInPeer("storage", previousState);

      const doneSpy = vi.fn();
      state.loadFromStorage(doneSpy);

      expect(doneSpy).toHaveBeenCalledTimes(1);
      expect(doneSpy).toHaveBeenCalledWith(true);
    });

    test("should call done(false) immediately when state is unavailable", () => {
      const { state, node } = setup();
      const storage = createMockStorage();
      node.setStorage(storage);

      // Mark as unavailable
      state.markNotFoundInPeer("storage");

      const doneSpy = vi.fn();
      state.loadFromStorage(doneSpy);

      expect(doneSpy).toHaveBeenCalledTimes(1);
      expect(doneSpy).toHaveBeenCalledWith(false);
    });

    test("should call done(false) immediately when state is errored", () => {
      const { state, node } = setup();
      const storage = createMockStorage();
      node.setStorage(storage);

      // Mark as errored
      state.markErrored("storage", {} as any);

      const doneSpy = vi.fn();
      state.loadFromStorage(doneSpy);

      expect(doneSpy).toHaveBeenCalledTimes(1);
      expect(doneSpy).toHaveBeenCalledWith(false);
    });

    test("should not call storage.load when state is already known", () => {
      const { state, node, header } = setup();
      const loadSpy = vi.fn();
      const storage = createMockStorage({ load: loadSpy });
      node.setStorage(storage);

      // Mark as available
      const previousState = state.loadingState;
      state.provideHeader(header);
      state.markFoundInPeer("storage", previousState);

      state.loadFromStorage(vi.fn());

      expect(loadSpy).not.toHaveBeenCalled();
    });

    test("should handle missing done callback when state is available", () => {
      const { state, node, header } = setup();
      const storage = createMockStorage();
      node.setStorage(storage);

      // Mark as available
      const previousState = state.loadingState;
      state.provideHeader(header);
      state.markFoundInPeer("storage", previousState);

      expect(() => state.loadFromStorage()).not.toThrow();
    });
  });

  describe("when current state is unknown", () => {
    test("should mark as pending and call storage.load", () => {
      const { state, node, id } = setup();
      const loadSpy = vi.fn();
      const storage = createMockStorage({ load: loadSpy });
      node.setStorage(storage);

      state.loadFromStorage();

      expect(state.getLoadingStateForPeer("storage")).toBe("pending");
      expect(loadSpy).toHaveBeenCalledTimes(1);
      expect(loadSpy).toHaveBeenCalledWith(
        id,
        expect.any(Function),
        expect.any(Function),
      );
    });

    test("should call done(true) when storage finds the value", async () => {
      const { state, node, id, header } = setup();
      let storageCallback: any;
      let storageDone: any;

      const storage = createMockStorage({
        load: (id, callback, done) => {
          storageCallback = callback;
          storageDone = done;
        },
      });
      node.setStorage(storage);

      const doneSpy = vi.fn();
      state.loadFromStorage(doneSpy);

      // Simulate storage finding the value
      // First provide the content through callback
      state.provideHeader(header);

      // Then call done with true
      storageDone(true);

      expect(doneSpy).toHaveBeenCalledTimes(1);
      expect(doneSpy).toHaveBeenCalledWith(true);
    });

    test("should call done(false) and mark as not found when storage doesn't find the value", async () => {
      const { state, node } = setup();
      let storageDone: any;

      const storage = createMockStorage({
        load: (id, callback, done) => {
          storageDone = done;
        },
      });
      node.setStorage(storage);

      const doneSpy = vi.fn();
      state.loadFromStorage(doneSpy);

      // Simulate storage not finding the value
      storageDone(false);

      expect(doneSpy).toHaveBeenCalledTimes(1);
      expect(doneSpy).toHaveBeenCalledWith(false);
      expect(state.getLoadingStateForPeer("storage")).toBe("unavailable");
    });

    test("should pass content to syncManager when storage provides it", async () => {
      const { state, node } = setup();
      let storageCallback: any;

      const storage = createMockStorage({
        load: (id, callback, done) => {
          storageCallback = callback;
        },
      });
      node.setStorage(storage);

      const handleNewContentSpy = vi.spyOn(
        node.syncManager,
        "handleNewContent",
      );

      state.loadFromStorage();

      // Simulate storage providing content with proper format
      const mockData = {
        action: "content" as const,
        id: state.id,
        priority: 0,
        new: {},
      };
      storageCallback(mockData);

      expect(handleNewContentSpy).toHaveBeenCalledTimes(1);
      expect(handleNewContentSpy).toHaveBeenCalledWith(mockData, "storage");
    });

    test("should handle missing done callback when loading from storage", () => {
      const { state, node } = setup();
      let storageDone: any;

      const storage = createMockStorage({
        load: (id, callback, done) => {
          storageDone = done;
        },
      });
      node.setStorage(storage);

      expect(() => {
        state.loadFromStorage();
        storageDone(true);
      }).not.toThrow();
    });

    test("should not mark as not found when storage finds the value", async () => {
      const { state, node, header } = setup();
      let storageDone: any;

      const storage = createMockStorage({
        load: (id, callback, done) => {
          storageDone = done;
        },
      });
      node.setStorage(storage);

      state.loadFromStorage();

      // Provide header first
      state.provideHeader(header);
      const previousState = state.loadingState;
      state.markFoundInPeer("storage", previousState);

      // Call done with true
      storageDone(true);

      // State should be available, not unavailable
      expect(state.getLoadingStateForPeer("storage")).not.toBe("unavailable");
    });

    test("should handle multiple concurrent loadFromStorage calls", async () => {
      const { state, node, id } = setup();
      const loadSpy = vi.fn();
      const storage = createMockStorage({ load: loadSpy });
      node.setStorage(storage);

      const done1 = vi.fn();
      const done2 = vi.fn();
      const done3 = vi.fn();

      // All three calls should work together
      state.loadFromStorage(done1);
      state.loadFromStorage(done2);
      state.loadFromStorage(done3);

      // Storage.load should only be called once (first call)
      expect(loadSpy).toHaveBeenCalledTimes(1);

      // The other calls should be waiting (pending state)
      expect(done1).not.toHaveBeenCalled();
      expect(done2).not.toHaveBeenCalled();
      expect(done3).not.toHaveBeenCalled();
    });
  });

  describe("edge cases and integration", () => {
    test("should handle transition from unknown to pending to available", async () => {
      const { state, node, header } = setup();
      let storageCallback: any;
      let storageDone: any;

      const storage = createMockStorage({
        load: (id, callback, done) => {
          storageCallback = callback;
          storageDone = done;
        },
      });
      node.setStorage(storage);

      const doneSpy = vi.fn();

      // Start as unknown
      expect(state.getLoadingStateForPeer("storage")).toBe("unknown");

      // Load from storage
      state.loadFromStorage(doneSpy);

      // Should now be pending
      expect(state.getLoadingStateForPeer("storage")).toBe("pending");

      // Simulate storage providing the header
      state.provideHeader(header);
      const previousState = state.loadingState;
      state.markFoundInPeer("storage", previousState);

      // Call done
      storageDone(true);

      // Should be available
      expect(state.getLoadingStateForPeer("storage")).toBe("available");
      expect(doneSpy).toHaveBeenCalledWith(true);
    });

    test("should properly clean up subscriptions when state becomes available through isAvailable()", async () => {
      const { state, node, header } = setup();
      const storage = createMockStorage({
        load: vi.fn(),
      });
      node.setStorage(storage);

      // Start initial load (will mark as pending)
      state.loadFromStorage();

      // Now try to load again with a done callback while pending
      const doneSpy = vi.fn();
      state.loadFromStorage(doneSpy);

      // Make the whole state available (not just from storage peer)
      state.provideHeader(header);
      const previousState = state.loadingState;
      state.markFoundInPeer("some_other_peer", previousState);

      // Wait for subscription to process
      await new Promise((resolve) => setImmediate(resolve));

      // Should have called done(true) because isAvailable() is true
      expect(doneSpy).toHaveBeenCalledTimes(1);
      expect(doneSpy).toHaveBeenCalledWith(true);
    });

    test("should handle rapid state changes", async () => {
      const { state, node, header } = setup();
      const storage = createMockStorage({
        load: vi.fn(),
      });
      node.setStorage(storage);

      const doneSpy = vi.fn();

      // Start loading
      state.loadFromStorage(doneSpy);

      // Rapid state changes
      state.markPending("storage");
      state.markNotFoundInPeer("storage");

      const previousState = state.loadingState;
      state.provideHeader(header);
      state.markFoundInPeer("storage", previousState);

      // Should have the final state
      expect(state.getLoadingStateForPeer("storage")).toBe("available");
    });
  });
});
