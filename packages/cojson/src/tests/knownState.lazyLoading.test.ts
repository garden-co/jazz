import { beforeEach, describe, expect, test, vi } from "vitest";
import { RawCoID, SessionID } from "../ids";
import { PeerID } from "../sync";
import type {
  StorageAPI,
  StorageReconciliationAcquireResult,
} from "../storage/types";
import { CoValueKnownState, peerHasAllContent } from "../knownState";
import { createTestNode, createUnloadedCoValue } from "./testUtils";

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

describe("peerHasAllContent", () => {
  const storageKnownState: CoValueKnownState = {
    id: "co_test123" as RawCoID,
    header: true,
    sessions: {
      ["session1" as SessionID]: 5,
      ["session2" as SessionID]: 3,
    },
  };

  test("returns false when peerKnownState is undefined", () => {
    expect(peerHasAllContent(storageKnownState, undefined)).toBe(false);
  });

  test("returns false when peer does not have header but storage does", () => {
    const peerKnownState: CoValueKnownState = {
      id: "co_test123" as RawCoID,
      header: false,
      sessions: {
        ["session1" as SessionID]: 5,
        ["session2" as SessionID]: 3,
      },
    };
    expect(peerHasAllContent(storageKnownState, peerKnownState)).toBe(false);
  });

  test("returns false when peer has fewer transactions in a session", () => {
    const peerKnownState: CoValueKnownState = {
      id: "co_test123" as RawCoID,
      header: true,
      sessions: {
        ["session1" as SessionID]: 3, // Less than storage's 5
        ["session2" as SessionID]: 3,
      },
    };
    expect(peerHasAllContent(storageKnownState, peerKnownState)).toBe(false);
  });

  test("returns false when peer is missing a session", () => {
    const peerKnownState: CoValueKnownState = {
      id: "co_test123" as RawCoID,
      header: true,
      sessions: {
        ["session1" as SessionID]: 5,
        // session2 is missing
      },
    };
    expect(peerHasAllContent(storageKnownState, peerKnownState)).toBe(false);
  });

  test("returns true when peer has exactly the same content", () => {
    const peerKnownState: CoValueKnownState = {
      id: "co_test123" as RawCoID,
      header: true,
      sessions: {
        ["session1" as SessionID]: 5,
        ["session2" as SessionID]: 3,
      },
    };
    expect(peerHasAllContent(storageKnownState, peerKnownState)).toBe(true);
  });

  test("returns true when peer has more transactions than storage", () => {
    const peerKnownState: CoValueKnownState = {
      id: "co_test123" as RawCoID,
      header: true,
      sessions: {
        ["session1" as SessionID]: 10, // More than storage's 5
        ["session2" as SessionID]: 5, // More than storage's 3
      },
    };
    expect(peerHasAllContent(storageKnownState, peerKnownState)).toBe(true);
  });

  test("returns true when peer has additional sessions", () => {
    const peerKnownState: CoValueKnownState = {
      id: "co_test123" as RawCoID,
      header: true,
      sessions: {
        ["session1" as SessionID]: 5,
        ["session2" as SessionID]: 3,
        ["session3" as SessionID]: 2, // Extra session not in storage
      },
    };
    expect(peerHasAllContent(storageKnownState, peerKnownState)).toBe(true);
  });

  test("returns true when storage has empty sessions", () => {
    const emptyStorageKnownState: CoValueKnownState = {
      id: "co_test123" as RawCoID,
      header: true,
      sessions: {},
    };
    const peerKnownState: CoValueKnownState = {
      id: "co_test123" as RawCoID,
      header: true,
      sessions: {},
    };
    expect(peerHasAllContent(emptyStorageKnownState, peerKnownState)).toBe(
      true,
    );
  });
});

describe("CoValueCore.getKnownStateFromStorage", () => {
  function setup() {
    const node = createTestNode();
    const { coValue, id, header } = createUnloadedCoValue(node);
    return { node, coValue, id, header };
  }

  test("returns undefined when storage is not configured", () => {
    const { coValue } = setup();
    const doneSpy = vi.fn();

    coValue.getKnownStateFromStorage(doneSpy);

    expect(doneSpy).toHaveBeenCalledWith(undefined);
  });

  test("returns current knownState when CoValue is already available", () => {
    const { node, coValue, header } = setup();
    const storage = createMockStorage();
    node.setStorage(storage);

    // Make the CoValue available by providing header
    coValue.provideHeader(header, undefined, false);

    const doneSpy = vi.fn();
    coValue.getKnownStateFromStorage(doneSpy);

    expect(doneSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        header: true,
      }),
    );
  });

  test("calls storage.loadKnownState when CoValue not in memory", () => {
    const { node, coValue, id } = setup();
    const loadKnownStateSpy = vi.fn((id, callback) => {
      callback({
        id,
        header: true,
        sessions: { session1: 5 },
      });
    });
    const storage = createMockStorage({ loadKnownState: loadKnownStateSpy });
    node.setStorage(storage);

    const doneSpy = vi.fn();
    coValue.getKnownStateFromStorage(doneSpy);

    expect(loadKnownStateSpy).toHaveBeenCalledWith(id, expect.any(Function));
    expect(doneSpy).toHaveBeenCalledWith({
      id,
      header: true,
      sessions: { session1: 5 },
    });
  });

  test("returns undefined when storage does not have the CoValue", () => {
    const { node, coValue } = setup();
    const loadKnownStateSpy = vi.fn((id, callback) => {
      callback(undefined);
    });
    const storage = createMockStorage({ loadKnownState: loadKnownStateSpy });
    node.setStorage(storage);

    const doneSpy = vi.fn();
    coValue.getKnownStateFromStorage(doneSpy);

    expect(doneSpy).toHaveBeenCalledWith(undefined);
  });

  test("sets onlyKnownState and caches lastKnownState when storage returns data", () => {
    const { node, coValue, id } = setup();
    const storageKnownState = {
      id,
      header: true,
      sessions: { session1: 5 },
    };
    const loadKnownStateSpy = vi.fn((id, callback) => {
      callback(storageKnownState);
    });
    const storage = createMockStorage({ loadKnownState: loadKnownStateSpy });
    node.setStorage(storage);

    // Initially unknown
    expect(coValue.loadingState).toBe("unknown");

    const doneSpy = vi.fn();
    coValue.getKnownStateFromStorage(doneSpy);

    // After storage returns data, should be onlyKnownState
    expect(coValue.loadingState).toBe("onlyKnownState");

    // knownState() should return the cached lastKnownState
    expect(coValue.knownState()).toEqual(storageKnownState);
  });

  test("returns cached lastKnownState on subsequent calls without hitting storage", () => {
    const { node, coValue, id } = setup();
    const storageKnownState = {
      id,
      header: true,
      sessions: { session1: 5 },
    };
    const loadKnownStateSpy = vi.fn((id, callback) => {
      callback(storageKnownState);
    });
    const storage = createMockStorage({ loadKnownState: loadKnownStateSpy });
    node.setStorage(storage);

    // First call - hits storage
    const doneSpy1 = vi.fn();
    coValue.getKnownStateFromStorage(doneSpy1);
    expect(loadKnownStateSpy).toHaveBeenCalledTimes(1);
    expect(doneSpy1).toHaveBeenCalledWith(storageKnownState);

    // Second call - should use cached lastKnownState, not hit storage
    const doneSpy2 = vi.fn();
    coValue.getKnownStateFromStorage(doneSpy2);
    expect(loadKnownStateSpy).toHaveBeenCalledTimes(1); // Still 1, not 2
    expect(doneSpy2).toHaveBeenCalledWith(storageKnownState);
  });
});
