import { beforeEach, describe, expect, test, vi } from "vitest";
import { RawCoID, SessionID } from "../ids";
import { PeerID } from "../sync";
import { StorageAPI } from "../storage/types";
import { CoValueKnownState, peerHasAllContent } from "../knownState";
import { createTestNode, createUnloadedCoValue } from "./testUtils";

function createMockStorage(
  opts: {
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
    close?: () => Promise<unknown> | undefined;
  } = {},
): StorageAPI {
  return {
    load: opts.load || vi.fn(),
    store: opts.store || vi.fn(),
    getKnownState: opts.getKnownState || vi.fn(),
    loadKnownState:
      opts.loadKnownState || vi.fn((id, callback) => callback(undefined)),
    waitForSync: opts.waitForSync || vi.fn().mockResolvedValue(undefined),
    trackCoValuesSyncState: opts.trackCoValuesSyncState || vi.fn(),
    getUnsyncedCoValueIDs: opts.getUnsyncedCoValueIDs || vi.fn(),
    stopTrackingSyncState: opts.stopTrackingSyncState || vi.fn(),
    close: opts.close || vi.fn().mockResolvedValue(undefined),
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

describe("CoValueCore.lazyLoadFromStorage", () => {
  function setup() {
    const node = createTestNode();
    const { coValue, id, header } = createUnloadedCoValue(node);
    return { node, coValue, id, header };
  }

  test("returns undefined when storage is not configured", () => {
    const { coValue } = setup();
    const doneSpy = vi.fn();

    coValue.lazyLoadFromStorage(doneSpy);

    expect(doneSpy).toHaveBeenCalledWith(undefined);
  });

  test("returns current knownState when CoValue is already available", () => {
    const { node, coValue, header } = setup();
    const storage = createMockStorage();
    node.setStorage(storage);

    // Make the CoValue available by providing header
    coValue.provideHeader(header, undefined, false);

    const doneSpy = vi.fn();
    coValue.lazyLoadFromStorage(doneSpy);

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
    coValue.lazyLoadFromStorage(doneSpy);

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
    coValue.lazyLoadFromStorage(doneSpy);

    expect(doneSpy).toHaveBeenCalledWith(undefined);
  });
});

describe("CoValueCore.lazyLoad", () => {
  function setup() {
    const node = createTestNode();
    const { coValue, id, header } = createUnloadedCoValue(node);
    return { node, coValue, id, header };
  }

  test("calls onNeedsContent immediately when CoValue is already available", () => {
    const { node, coValue, header } = setup();
    const storage = createMockStorage();
    node.setStorage(storage);

    // Make the CoValue available
    coValue.provideHeader(header, undefined, false);

    const callbacks = {
      onNeedsContent: vi.fn(),
      onUpToDate: vi.fn(),
      onNotFound: vi.fn(),
    };

    coValue.lazyLoad(undefined, callbacks);

    expect(callbacks.onNeedsContent).toHaveBeenCalled();
    expect(callbacks.onUpToDate).not.toHaveBeenCalled();
    expect(callbacks.onNotFound).not.toHaveBeenCalled();
  });

  test("calls onNotFound when CoValue not in storage", () => {
    const { node, coValue } = setup();
    const loadKnownStateSpy = vi.fn((id, callback) => callback(undefined));
    const storage = createMockStorage({ loadKnownState: loadKnownStateSpy });
    node.setStorage(storage);

    const callbacks = {
      onNeedsContent: vi.fn(),
      onUpToDate: vi.fn(),
      onNotFound: vi.fn(),
    };

    coValue.lazyLoad(undefined, callbacks);

    expect(callbacks.onNotFound).toHaveBeenCalled();
    expect(callbacks.onNeedsContent).not.toHaveBeenCalled();
    expect(callbacks.onUpToDate).not.toHaveBeenCalled();
  });

  test("calls onUpToDate when peer already has all content", () => {
    const { node, coValue, id } = setup();
    const storageKnownState: CoValueKnownState = {
      id,
      header: true,
      sessions: { ["session1" as SessionID]: 5 },
    };
    const loadKnownStateSpy = vi.fn((id, callback) =>
      callback(storageKnownState),
    );
    const storage = createMockStorage({ loadKnownState: loadKnownStateSpy });
    node.setStorage(storage);

    const peerKnownState: CoValueKnownState = {
      id,
      header: true,
      sessions: { ["session1" as SessionID]: 5 }, // Same as storage
    };

    const callbacks = {
      onNeedsContent: vi.fn(),
      onUpToDate: vi.fn(),
      onNotFound: vi.fn(),
    };

    coValue.lazyLoad(peerKnownState, callbacks);

    expect(callbacks.onUpToDate).toHaveBeenCalledWith(storageKnownState);
    expect(callbacks.onNeedsContent).not.toHaveBeenCalled();
    expect(callbacks.onNotFound).not.toHaveBeenCalled();
  });

  test("triggers full load when peer needs new content", () => {
    const { node, coValue, id } = setup();
    const storageKnownState: CoValueKnownState = {
      id,
      header: true,
      sessions: { ["session1" as SessionID]: 5 },
    };
    const loadKnownStateSpy = vi.fn((_, callback) =>
      callback(storageKnownState),
    );
    // Track that load was called - don't complete it to avoid complexity
    const loadSpy = vi.fn();
    const storage = createMockStorage({
      loadKnownState: loadKnownStateSpy,
      load: loadSpy,
    });
    node.setStorage(storage);

    const peerKnownState: CoValueKnownState = {
      id,
      header: true,
      sessions: { ["session1" as SessionID]: 2 }, // Less than storage's 5
    };

    const callbacks = {
      onNeedsContent: vi.fn(),
      onUpToDate: vi.fn(),
      onNotFound: vi.fn(),
    };

    coValue.lazyLoad(peerKnownState, callbacks);

    // The key assertion: when peer needs content, full load IS triggered
    expect(loadSpy).toHaveBeenCalled();
    // onUpToDate should NOT be called since peer needs new content
    expect(callbacks.onUpToDate).not.toHaveBeenCalled();
  });

  test("skips full load when peer has more content than storage", () => {
    const { node, coValue, id } = setup();
    const storageKnownState: CoValueKnownState = {
      id,
      header: true,
      sessions: { ["session1" as SessionID]: 3 },
    };
    const loadKnownStateSpy = vi.fn((_, callback) =>
      callback(storageKnownState),
    );
    const loadSpy = vi.fn();
    const storage = createMockStorage({
      loadKnownState: loadKnownStateSpy,
      load: loadSpy,
    });
    node.setStorage(storage);

    const peerKnownState: CoValueKnownState = {
      id,
      header: true,
      sessions: { ["session1" as SessionID]: 10 }, // More than storage
    };

    const callbacks = {
      onNeedsContent: vi.fn(),
      onUpToDate: vi.fn(),
      onNotFound: vi.fn(),
    };

    coValue.lazyLoad(peerKnownState, callbacks);

    // Full load should NOT be called since peer already has all content
    expect(loadSpy).not.toHaveBeenCalled();
    expect(callbacks.onUpToDate).toHaveBeenCalledWith(storageKnownState);
  });
});
