import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { RawCoID } from "../ids";
import { PeerID } from "../sync";
import type {
  StorageAPI,
  StorageReconciliationAcquireResult,
} from "../storage/types";
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
    getCoValueIDs?: (
      limit: number,
      offset: number,
      callback: (batch: { id: RawCoID }[]) => void,
    ) => void;
    getCoValueCount?: (callback: (count: number) => void) => void;
    load?: (
      id: RawCoID,
      callback: (data: any) => void,
      done: (found: boolean) => void,
    ) => void;
    store?: (data: any, correctionCallback: any) => void;
    getKnownState?: (id: RawCoID) => any;
    loadKnownState?: (id: string, callback: (knownState: any) => void) => void;
    waitForSync?: (id: string, coValue: any) => Promise<void>;
    trackCoValuesSyncState?: (
      operations: Array<{ id: RawCoID; peerId: PeerID; synced: boolean }>,
    ) => void;
    getUnsyncedCoValueIDs?: (
      callback: (unsyncedCoValueIDs: RawCoID[]) => void,
    ) => void;
    stopTrackingSyncState?: (id: RawCoID) => void;
    onCoValueUnmounted?: (id: RawCoID) => void;
    close?: () => Promise<unknown> | undefined;
    markDeleteAsValid?: (id: RawCoID) => void;
    enableDeletedCoValuesErasure?: () => void;
    eraseAllDeletedCoValues?: () => Promise<void>;
    tryAcquireStorageReconciliationLock?: (
      sessionId: string,
      peerId: string,
      callback: (result: StorageReconciliationAcquireResult) => void,
    ) => void;
    renewStorageReconciliationLock?: (
      sessionId: string,
      peerId: string,
      offset: number,
    ) => void;
    releaseStorageReconciliationLock?: (
      sessionId: string,
      peerId: string,
      callback?: () => void,
    ) => void;
  } = {},
): StorageAPI {
  return {
    getCoValueIDs: opts.getCoValueIDs || vi.fn(),
    getCoValueCount: opts.getCoValueCount || vi.fn(),
    markDeleteAsValid: opts.markDeleteAsValid || vi.fn(),
    enableDeletedCoValuesErasure: opts.enableDeletedCoValuesErasure || vi.fn(),
    eraseAllDeletedCoValues: opts.eraseAllDeletedCoValues || vi.fn(),
    load: opts.load || vi.fn(),
    store: opts.store || vi.fn(),
    getKnownState: opts.getKnownState || vi.fn(),
    loadKnownState:
      opts.loadKnownState || vi.fn((id, callback) => callback(undefined)),
    waitForSync: opts.waitForSync || vi.fn().mockResolvedValue(undefined),
    trackCoValuesSyncState: opts.trackCoValuesSyncState || vi.fn(),
    getUnsyncedCoValueIDs: opts.getUnsyncedCoValueIDs || vi.fn(),
    stopTrackingSyncState: opts.stopTrackingSyncState || vi.fn(),
    onCoValueUnmounted: opts.onCoValueUnmounted || vi.fn(),
    close: opts.close || vi.fn().mockResolvedValue(undefined),
    tryAcquireStorageReconciliationLock:
      opts.tryAcquireStorageReconciliationLock ||
      vi.fn((_sessionId, _peerId, callback) =>
        callback({ acquired: false as const, reason: "not_due" as const }),
      ),
    renewStorageReconciliationLock:
      opts.renewStorageReconciliationLock || vi.fn(),
    releaseStorageReconciliationLock:
      opts.releaseStorageReconciliationLock || vi.fn(),
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

  describe("when state is garbageCollected", () => {
    test("should load from storage even if storage state is not unknown", () => {
      const { state, node, header, id } = setup();
      const loadSpy = vi.fn();
      const storage = createMockStorage({ load: loadSpy });
      node.setStorage(storage);

      // First, simulate that storage was previously accessed and marked available
      state.markFoundInPeer("storage", state.loadingState);

      // Then set the CoValue to garbageCollected state (simulating GC)
      // This is what happens when a GC'd CoValueCore shell is created
      state.setGarbageCollectedState({
        id,
        header: true,
        sessions: {},
      });

      // Verify we're in garbageCollected state
      expect(state.loadingState).toBe("garbageCollected");

      // Now call loadFromStorage - it should proceed to load
      state.loadFromStorage();

      // Should have called storage.load because we need full content
      expect(loadSpy).toHaveBeenCalledTimes(1);
    });

    test("should load from storage when garbageCollected and storage state is unknown", () => {
      const { state, node, id } = setup();
      const loadSpy = vi.fn();
      const storage = createMockStorage({ load: loadSpy });
      node.setStorage(storage);

      // Set the CoValue to garbageCollected state
      state.setGarbageCollectedState({
        id,
        header: true,
        sessions: {},
      });

      expect(state.loadingState).toBe("garbageCollected");
      expect(state.getLoadingStateForPeer("storage")).toBe("unknown");

      state.loadFromStorage();

      expect(loadSpy).toHaveBeenCalledTimes(1);
    });

    test("should keep garbageCollected loadingState even when a peer is pending", () => {
      const { state, node, id } = setup();
      const storage = createMockStorage();
      node.setStorage(storage);

      state.setGarbageCollectedState({
        id,
        header: true,
        sessions: {},
      });
      state.markPending("peer1");

      expect(state.getLoadingStateForPeer("peer1")).toBe("pending");
      expect(state.loadingState).toBe("garbageCollected");
    });
  });

  describe("when state is onlyKnownState", () => {
    test("should load from storage to get full content", () => {
      const { state, node, id } = setup();
      const loadSpy = vi.fn();
      const storage = createMockStorage({
        load: loadSpy,
        loadKnownState: (id, callback) => {
          // Simulate storage finding knownState
          callback({
            id,
            header: true,
            sessions: { session1: 5 },
          });
        },
      });
      node.setStorage(storage);

      // First, call getKnownStateFromStorage to set onlyKnownState
      state.getKnownStateFromStorage((knownState) => {
        expect(knownState).toBeDefined();
      });

      // Verify we're in onlyKnownState
      expect(state.loadingState).toBe("onlyKnownState");

      // Now call loadFromStorage - it should proceed to load full content
      state.loadFromStorage();

      expect(loadSpy).toHaveBeenCalledTimes(1);
    });

    test("should load from storage when onlyKnownState and storage state is unknown", () => {
      const { state, node, id } = setup();
      const loadSpy = vi.fn();
      const storage = createMockStorage({
        load: loadSpy,
        loadKnownState: (id, callback) => {
          callback({
            id,
            header: true,
            sessions: {},
          });
        },
      });
      node.setStorage(storage);

      // Set onlyKnownState via getKnownStateFromStorage
      state.getKnownStateFromStorage(() => {});

      expect(state.loadingState).toBe("onlyKnownState");
      expect(state.getLoadingStateForPeer("storage")).toBe("unknown");

      state.loadFromStorage();

      expect(loadSpy).toHaveBeenCalledTimes(1);
    });

    test("should keep onlyKnownState loadingState even when a peer is pending", () => {
      const { state, node, id } = setup();
      const storage = createMockStorage({
        loadKnownState: (id, callback) => {
          callback({
            id,
            header: true,
            sessions: { session1: 1 },
          });
        },
      });
      node.setStorage(storage);

      state.getKnownStateFromStorage(() => {});
      state.markPending("peer1");

      expect(state.getLoadingStateForPeer("peer1")).toBe("pending");
      expect(state.loadingState).toBe("onlyKnownState");
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
