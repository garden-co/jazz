import { describe, expect, test, vi, afterEach } from "vitest";
import { CoID, RawCoID, RawCoMap, logger } from "../exports.js";
import { CoValueCore } from "../exports.js";
import { NewContentMessage } from "../sync.js";
import { CoValueKnownState, emptyKnownState } from "../knownState.js";
import {
  createSyncStorage,
  getAllCoValuesWaitingForDelete,
  getCoValueStoredSessions,
  getDbPath,
} from "./testStorage.js";
import {
  fillCoMapWithLargeData,
  loadCoValueOrFail,
  setupTestNode,
} from "./testUtils.js";

/**
 * Helper function that gets new content since a known state, throwing if:
 * - The coValue is not verified
 * - There is no new content
 */
function getNewContentSince(
  coValue: CoValueCore,
  knownState: CoValueKnownState,
): NewContentMessage {
  if (!coValue.verified) {
    throw new Error(`CoValue ${coValue.id} is not verified`);
  }

  const contentMessage = coValue.newContentSince(knownState)?.[0];

  if (!contentMessage) {
    throw new Error(`No new content available for coValue ${coValue.id}`);
  }

  return contentMessage;
}

afterEach(() => {
  vi.useRealTimers();
});

describe("StorageApiSync", () => {
  describe("getKnownState", () => {
    test("should return empty known state for new coValue ID and cache the result", async () => {
      const fixtures = setupTestNode();
      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const id = fixtures.node.createGroup().id;
      const knownState = storage.getKnownState(id);

      expect(knownState).toEqual(emptyKnownState(id));
      expect(storage.getKnownState(id)).toBe(knownState); // Should return same instance
    });

    test("should return separate known state instances for different coValue IDs", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });
      const id1 = "test-id-1";
      const id2 = "test-id-2";

      const knownState1 = storage.getKnownState(id1);
      const knownState2 = storage.getKnownState(id2);

      expect(knownState1).not.toBe(knownState2);
    });
  });

  describe("load", () => {
    test("should fail gracefully when loading non-existent coValue and preserve known state", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });
      const id = "non-existent-id";
      const callback = vi.fn();
      const done = vi.fn();

      // Get initial known state
      const initialKnownState = storage.getKnownState(id);
      expect(initialKnownState).toEqual(emptyKnownState(id as `co_z${string}`));

      await storage.load(id, callback, done);

      expect(done).toHaveBeenCalledWith(false);
      expect(callback).not.toHaveBeenCalled();

      // Verify that storage known state is NOT updated when load fails
      const afterLoadKnownState = storage.getKnownState(id);
      expect(afterLoadKnownState).toEqual(initialKnownState);
    });

    test("should successfully load coValue with header and update known state", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });
      const callback = vi.fn((content) =>
        client.node.syncManager.handleNewContent(content, "storage"),
      );
      const done = vi.fn();

      // Create a real group and get its content message
      const group = fixtures.node.createGroup();
      await group.core.waitForSync();

      // Get initial known state
      const initialKnownState = storage.getKnownState(group.id);
      expect(initialKnownState).toEqual(emptyKnownState(group.id));

      await storage.load(group.id, callback, done);

      expect(callback).toHaveBeenCalledWith(
        expect.objectContaining({
          id: group.id,
          header: group.core.verified.header,
          new: expect.any(Object),
        }),
      );
      expect(done).toHaveBeenCalledWith(true);

      // Verify that storage known state is updated after load
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.knownState());

      const groupOnNode = await loadCoValueOrFail(client.node, group.id);

      expect(groupOnNode.core.verified.header).toEqual(
        group.core.verified.header,
      );
    });

    test("should successfully load coValue with transactions and update known state", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });
      const callback = vi.fn((content) =>
        client.node.syncManager.handleNewContent(content, "storage"),
      );
      const done = vi.fn();

      // Create a real group and add a member to create transactions
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      await group.core.waitForSync();

      // Get initial known state
      const initialKnownState = storage.getKnownState(group.id);
      expect(initialKnownState).toEqual(emptyKnownState(group.id));

      await storage.load(group.id, callback, done);

      expect(callback).toHaveBeenCalledWith(
        expect.objectContaining({
          id: group.id,
          header: group.core.verified.header,
          new: expect.objectContaining({
            [fixtures.node.currentSessionID]: expect.any(Object),
          }),
        }),
      );
      expect(done).toHaveBeenCalledWith(true);

      // Verify that storage known state is updated after load
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.knownState());

      const groupOnNode = await loadCoValueOrFail(client.node, group.id);
      expect(groupOnNode.get("everyone")).toEqual("reader");
    });
  });

  describe("store", () => {
    test("should successfully store new coValue with header and update known state", async () => {
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });
      // Create a real group and get its content message
      const group = fixtures.node.createGroup();
      const contentMessage = getNewContentSince(
        group.core,
        emptyKnownState(group.id),
      );
      const correctionCallback = vi.fn();

      // Get initial known state
      const initialKnownState = storage.getKnownState(group.id);
      expect(initialKnownState).toEqual(emptyKnownState(group.id));

      storage.store(contentMessage, correctionCallback);

      // Verify that storage known state is updated after store
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.knownState());

      client.addStorage({ storage });

      const groupOnNode = await loadCoValueOrFail(client.node, group.id);

      expect(groupOnNode.core.verified.header).toEqual(
        group.core.verified.header,
      );
    });

    test("should successfully store coValue with transactions and update known state", async () => {
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create a real group and add a member to create transactions
      const group = fixtures.node.createGroup();

      group.addMember("everyone", "reader");

      const contentMessage = getNewContentSince(
        group.core,
        emptyKnownState(group.id),
      );
      const correctionCallback = vi.fn();

      // Get initial known state
      const initialKnownState = storage.getKnownState(group.id);
      expect(initialKnownState).toEqual(emptyKnownState(group.id));

      storage.store(contentMessage, correctionCallback);

      // Verify that storage known state is updated after store
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.knownState());

      client.addStorage({ storage });

      const groupOnNode = await loadCoValueOrFail(client.node, group.id);
      expect(groupOnNode.get("everyone")).toEqual("reader");
    });

    test("should handle correction when header assumption is invalid", async () => {
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const group = fixtures.node.createGroup();
      const knownState = group.core.knownState();

      group.addMember("everyone", "reader");

      const contentMessage = getNewContentSince(group.core, knownState);
      const correctionCallback = vi.fn((known) => {
        expect(known).toEqual(emptyKnownState(group.id));
        return group.core.newContentSince(known);
      });

      // Get initial known state
      const initialKnownState = storage.getKnownState(group.id);
      expect(initialKnownState).toEqual(emptyKnownState(group.id));

      const result = storage.store(contentMessage, correctionCallback);

      expect(correctionCallback).toHaveBeenCalledTimes(1);
      expect(result).toBe(true);

      // Verify that storage known state is updated after store with correction
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.knownState());

      client.addStorage({ storage });
      const groupOnNode = await loadCoValueOrFail(client.node, group.id);

      expect(groupOnNode.get("everyone")).toEqual("reader");
    });

    test("should handle correction when new content assumption is invalid", async () => {
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const group = fixtures.node.createGroup();

      const initialContent = getNewContentSince(
        group.core,
        emptyKnownState(group.id),
      );

      const initialKnownState = group.core.knownState();

      group.addMember("everyone", "reader");

      const knownState = group.core.knownState();

      group.addMember("everyone", "writer");

      const contentMessage = getNewContentSince(group.core, knownState);
      const correctionCallback = vi.fn((known) => {
        expect(known).toEqual(initialKnownState);
        return group.core.newContentSince(known);
      });

      // Get initial storage known state
      const initialStorageKnownState = storage.getKnownState(group.id);
      expect(initialStorageKnownState).toEqual(emptyKnownState(group.id));

      storage.store(initialContent, correctionCallback);

      // Verify storage known state after first store
      const afterFirstStore = storage.getKnownState(group.id);
      expect(afterFirstStore).toEqual(initialKnownState);

      const result = storage.store(contentMessage, correctionCallback);
      expect(correctionCallback).toHaveBeenCalledTimes(1);

      expect(result).toBe(true);

      // Verify that storage known state is updated after store with correction
      const finalKnownState = storage.getKnownState(group.id);
      expect(finalKnownState).toEqual(group.core.knownState());

      client.addStorage({ storage });
      const groupOnNode = await loadCoValueOrFail(client.node, group.id);

      expect(groupOnNode.get("everyone")).toEqual("writer");
    });

    test("should log error and fail when correction callback returns undefined", async () => {
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const group = fixtures.node.createGroup();

      const knownState = group.core.knownState();
      group.addMember("everyone", "writer");

      const contentMessage = getNewContentSince(group.core, knownState);
      const correctionCallback = vi.fn((known) => {
        return undefined;
      });

      // Get initial known state
      const initialKnownState = storage.getKnownState(group.id);
      expect(initialKnownState).toEqual(emptyKnownState(group.id));

      const errorSpy = vi.spyOn(logger, "error").mockImplementation(() => {});
      const result = storage.store(contentMessage, correctionCallback);
      expect(correctionCallback).toHaveBeenCalledTimes(1);

      expect(result).toBe(false);

      // Verify that storage known state is NOT updated when store fails
      const afterStoreKnownState = storage.getKnownState(group.id);
      expect(afterStoreKnownState).toEqual(initialKnownState);

      expect(errorSpy).toHaveBeenCalledWith(
        "Correction callback returned undefined",
        {
          knownState: expect.any(Object),
        },
      );

      errorSpy.mockClear();
    });

    test("should log error and fail when correction callback returns invalid content message", async () => {
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const group = fixtures.node.createGroup();

      const knownState = group.core.knownState();
      group.addMember("everyone", "writer");

      const contentMessage = getNewContentSince(group.core, knownState);
      const correctionCallback = vi.fn(() => {
        return [contentMessage];
      });

      const errorSpy = vi.spyOn(logger, "error").mockImplementation(() => {});
      const result = storage.store(contentMessage, correctionCallback);
      expect(correctionCallback).toHaveBeenCalledTimes(1);

      expect(result).toBe(false);

      expect(errorSpy).toHaveBeenCalledWith(
        "Correction callback returned undefined",
        {
          knownState: expect.any(Object),
        },
      );

      expect(errorSpy).toHaveBeenCalledWith("Double correction requested", {
        knownState: expect.any(Object),
        msg: expect.any(Object),
      });

      errorSpy.mockClear();
    });

    test("should successfully store coValue with multiple sessions", async () => {
      const dbPath = getDbPath();

      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const fixture2 = setupTestNode();
      fixture2.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const coValue = fixtures.node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...fixtures.node.crypto.createdNowUnique(),
      });

      coValue.makeTransaction(
        [
          {
            count: 1,
          },
        ],
        "trusting",
      );

      await coValue.waitForSync();

      const mapOnNode2 = await loadCoValueOrFail(
        fixture2.node,
        coValue.id as CoID<RawCoMap>,
      );

      coValue.makeTransaction(
        [
          {
            count: 2,
          },
        ],
        "trusting",
      );

      const knownState = mapOnNode2.core.knownState();

      const contentMessage = getNewContentSince(
        mapOnNode2.core,
        emptyKnownState(mapOnNode2.id),
      );
      const correctionCallback = vi.fn();

      storage.store(contentMessage, correctionCallback);

      client.addStorage({ storage });

      const finalMap = await loadCoValueOrFail(client.node, mapOnNode2.id);
      expect(finalMap.core.knownState()).toEqual(knownState);
    });
  });

  describe("delete flow", () => {
    test("deleteCoValue enqueues the coValue for erasure", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const group = client.node.createGroup();
      const map = group.createMap();
      map.core.deleteCoValue();
      await map.core.waitForSync();

      const queued = await getAllCoValuesWaitingForDelete(storage);
      expect(queued).toContain(map.id);
    });

    test("background erasure doesn't run if not enabled", async () => {
      const dbPath = getDbPath();
      const node = setupTestNode();
      const { storage } = node.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const group = node.node.createGroup();
      const map = group.createMap();
      map.set("k", "v");
      await map.core.waitForSync();

      vi.useFakeTimers();

      map.core.deleteCoValue();
      await map.core.waitForSync();

      await vi.advanceTimersByTimeAsync(70_000);

      // Queue drained
      expect(await getAllCoValuesWaitingForDelete(storage)).toContain(map.id);
    });

    test("background erasure run if enabled", async () => {
      const dbPath = getDbPath();
      const node = setupTestNode();
      const { storage } = node.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      vi.useFakeTimers();

      node.node.enableDeletedCoValuesErasure();

      const group = node.node.createGroup();
      const map = group.createMap();
      map.set("k", "v");
      await map.core.waitForSync();

      map.core.deleteCoValue();
      await map.core.waitForSync();

      await vi.advanceTimersByTimeAsync(70_000);

      // Queue drained
      expect(await getAllCoValuesWaitingForDelete(storage)).not.toContain(
        map.id,
      );

      const sessionIDs = await getCoValueStoredSessions(storage, map.id);

      expect(sessionIDs).toHaveLength(1);
      expect(sessionIDs[0]).toMatch(/_session_d[1-9A-HJ-NP-Za-km-z]+\$$/); // Delete session format
    });

    test("eraseAllDeletedCoValues deletes history but preserves tombstone", async () => {
      const dbPath = getDbPath();

      const node = setupTestNode();
      const { storage } = node.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const group = node.node.createGroup();
      const map = group.createMap();
      map.set("k", "v");
      await map.core.waitForSync();

      map.core.deleteCoValue();
      await map.core.waitForSync();

      expect(await getAllCoValuesWaitingForDelete(storage)).toContain(map.id);

      await storage.eraseAllDeletedCoValues();

      // Queue drained
      expect(await getAllCoValuesWaitingForDelete(storage)).not.toContain(
        map.id,
      );

      // Tombstone preserved + history erased when loaded from storage
      const client = setupTestNode();
      const { storage: clientStorage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const loaded = await loadCoValueOrFail(
        client.node,
        map.id as CoID<RawCoMap>,
      );

      expect(loaded.core.isDeleted).toBe(true);
      expect(loaded.get("k")).toBeUndefined();

      const sessionIDs = await getCoValueStoredSessions(clientStorage, map.id);

      expect(sessionIDs).toHaveLength(1);
      expect(sessionIDs[0]).toMatch(/_session_d[1-9A-HJ-NP-Za-km-z]+\$$/); // Delete session format
    });

    test("eraseAllDeletedCoValues does not break when called while a coValue is streaming from storage", async () => {
      const dbPath = getDbPath();

      const node = setupTestNode();

      const storage = createSyncStorage({
        filename: dbPath,
        nodeName: "test",
        storageName: "test-storage",
      });
      node.addStorage({ storage });

      const group = node.node.createGroup();
      const map = group.createMap();
      fillCoMapWithLargeData(map);
      await map.core.waitForSync();
      map.core.deleteCoValue();
      await map.core.waitForSync();

      storage.close();

      const newStorage = createSyncStorage({
        filename: dbPath,
        nodeName: "test",
        storageName: "test-storage",
      });

      const callback = vi.fn();

      const loadPromise = new Promise((resolve) => {
        newStorage.load(map.id, callback, resolve);
      });
      await newStorage.eraseAllDeletedCoValues();

      expect(await loadPromise).toBe(true);
    });
  });

  describe("dependencies", () => {
    test("should load dependencies before dependent coValues and update all known states", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create a group and a map owned by that group to create dependencies
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      const map = group.createMap({ test: "value" });
      await group.core.waitForSync();
      await map.core.waitForSync();

      const callback = vi.fn((content) =>
        client.node.syncManager.handleNewContent(content, "storage"),
      );
      const done = vi.fn();

      // Get initial known states
      const initialGroupKnownState = storage.getKnownState(group.id);
      const initialMapKnownState = storage.getKnownState(map.id);
      expect(initialGroupKnownState).toEqual(emptyKnownState(group.id));
      expect(initialMapKnownState).toEqual(emptyKnownState(map.id));

      // Load the map, which should also load the group dependency first
      await storage.load(map.id, callback, done);

      expect(callback).toHaveBeenCalledTimes(2); // Group first, then map
      expect(callback).toHaveBeenNthCalledWith(
        1,
        expect.objectContaining({
          id: group.id,
        }),
      );
      expect(callback).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          id: map.id,
        }),
      );

      expect(done).toHaveBeenCalledWith(true);

      // Verify that storage known states are updated after load
      const updatedGroupKnownState = storage.getKnownState(group.id);
      const updatedMapKnownState = storage.getKnownState(map.id);
      expect(updatedGroupKnownState).toEqual(group.core.knownState());
      expect(updatedMapKnownState).toEqual(map.core.knownState());

      client.addStorage({ storage });
      const mapOnNode = await loadCoValueOrFail(client.node, map.id);
      expect(mapOnNode.get("test")).toEqual("value");
    });

    test("should skip loading already loaded dependencies", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create a group and a map owned by that group
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      const map = group.createMap({ test: "value" });
      await group.core.waitForSync();
      await map.core.waitForSync();

      const callback = vi.fn((content) =>
        client.node.syncManager.handleNewContent(content, "storage"),
      );
      const done = vi.fn();

      // Get initial known states
      const initialGroupKnownState = storage.getKnownState(group.id);
      const initialMapKnownState = storage.getKnownState(map.id);
      expect(initialGroupKnownState).toEqual(emptyKnownState(group.id));
      expect(initialMapKnownState).toEqual(emptyKnownState(map.id));

      // First load the group
      await storage.load(group.id, callback, done);
      callback.mockClear();
      done.mockClear();

      // Verify group known state is updated after first load
      const afterGroupLoad = storage.getKnownState(group.id);
      expect(afterGroupLoad).toEqual(group.core.knownState());

      // Then load the map - the group dependency should already be loaded
      await storage.load(map.id, callback, done);

      // Should only call callback once for the map since group is already loaded
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(
        expect.objectContaining({
          id: map.id,
        }),
      );

      expect(done).toHaveBeenCalledWith(true);

      // Verify map known state is updated after second load
      const finalMapKnownState = storage.getKnownState(map.id);
      expect(finalMapKnownState).toEqual(map.core.knownState());

      client.addStorage({ storage });
      const mapOnNode = await loadCoValueOrFail(client.node, map.id);
      expect(mapOnNode.get("test")).toEqual("value");
    });

    test("should load dependencies again if they were unmounted", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create a group and a map owned by that group
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      const map = group.createMap({ test: "value" });
      await group.core.waitForSync();
      await map.core.waitForSync();

      const callback = vi.fn((content) =>
        client.node.syncManager.handleNewContent(content, "storage"),
      );
      const done = vi.fn();

      // Load the map (and its group)
      await storage.load(map.id, callback, done);
      callback.mockClear();
      done.mockClear();

      // Unmount the map and its group
      storage.onCoValueUnmounted(map.id);
      storage.onCoValueUnmounted(group.id);

      // Load the map. The group dependency should be loaded again
      await storage.load(map.id, callback, done);

      expect(callback).toHaveBeenCalledTimes(2);
      expect(callback).toHaveBeenNthCalledWith(
        1,
        expect.objectContaining({
          id: group.id,
        }),
      );
      expect(callback).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({
          id: map.id,
        }),
      );

      expect(done).toHaveBeenCalledWith(true);

      client.addStorage({ storage });
      const mapOnNode = await loadCoValueOrFail(client.node, map.id);
      expect(mapOnNode.get("test")).toEqual("value");
    });
  });

  describe("waitForSync", () => {
    test("should resolve immediately when coValue is already synced", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create a group and add a member
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      await group.core.waitForSync();

      // Store the group in storage
      const contentMessage = getNewContentSince(
        group.core,
        emptyKnownState(group.id),
      );
      const correctionCallback = vi.fn();
      storage.store(contentMessage, correctionCallback);

      client.addStorage({ storage });

      // Load the group on the new node
      const groupOnNode = await loadCoValueOrFail(client.node, group.id);

      // Wait for sync should resolve immediately since the coValue is already synced
      await expect(
        storage.waitForSync(group.id, groupOnNode.core),
      ).resolves.toBeUndefined();

      expect(groupOnNode.get("everyone")).toEqual("reader");
    });
  });

  describe("close", () => {
    test("should close storage without throwing errors", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      expect(() => storage.close()).not.toThrow();
    });
  });

  describe("loadKnownState", () => {
    test("should return correct knownState structure for existing CoValue", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create a group to have data in the database
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      await group.core.waitForSync();

      const result = await new Promise<CoValueKnownState | undefined>(
        (resolve) => {
          storage.loadKnownState(group.id, resolve);
        },
      );

      expect(result).toBeDefined();
      expect(result?.id).toBe(group.id);
      expect(result?.header).toBe(true);
      expect(result?.sessions).toEqual(group.core.knownState().sessions);
    });

    test("should return undefined for non-existent CoValue", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const result = await new Promise<CoValueKnownState | undefined>(
        (resolve) => {
          storage.loadKnownState("co_nonexistent" as any, resolve);
        },
      );

      expect(result).toBeUndefined();
    });

    test("should handle CoValue with no sessions (header only)", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create a CoValue with just a header (no transactions yet)
      const coValue = fixtures.node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...fixtures.node.crypto.createdNowUnique(),
      });
      await coValue.waitForSync();

      const result = await new Promise<CoValueKnownState | undefined>(
        (resolve) => {
          storage.loadKnownState(coValue.id, resolve);
        },
      );

      expect(result).toBeDefined();
      expect(result?.id).toBe(coValue.id);
      expect(result?.header).toBe(true);
      // The sessions should have one entry with lastIdx = 0 (just header)
      expect(Object.keys(result?.sessions || {}).length).toBe(0);
    });

    test("should handle CoValue with multiple sessions", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const fixtures2 = setupTestNode();
      fixtures2.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create a CoValue and have two nodes make transactions
      const coValue = fixtures.node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...fixtures.node.crypto.createdNowUnique(),
      });

      coValue.makeTransaction([{ key1: "value1" }], "trusting");
      await coValue.waitForSync();

      const coValueOnNode2 = await loadCoValueOrFail(
        fixtures2.node,
        coValue.id as CoID<RawCoMap>,
      );

      coValueOnNode2.set("key2", "value2", "trusting");
      await coValueOnNode2.core.waitForSync();

      const result = await new Promise<CoValueKnownState | undefined>(
        (resolve) => {
          storage.loadKnownState(coValue.id, resolve);
        },
      );

      expect(result).toBeDefined();
      expect(result?.id).toBe(coValue.id);
      expect(result?.header).toBe(true);
      // Should have two sessions
      expect(Object.keys(result?.sessions || {}).length).toBe(2);
      // Verify sessions match the expected state
      expect(result?.sessions).toEqual(
        coValueOnNode2.core.knownState().sessions,
      );
    });

    test("should use cache when knownState is cached", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create a group to have data in the database
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      await group.core.waitForSync();

      // First call should hit the database and cache the result
      const result1 = await new Promise<CoValueKnownState | undefined>(
        (resolve) => {
          storage.loadKnownState(group.id, resolve);
        },
      );

      expect(result1).toBeDefined();
      expect(result1?.id).toBe(group.id);
      expect(result1?.header).toBe(true);

      // Second call should return from cache
      const result2 = await new Promise<CoValueKnownState | undefined>(
        (resolve) => {
          storage.loadKnownState(group.id, resolve);
        },
      );

      expect(result2).toEqual(result1);
    });
  });

  describe("getCoValueIDs", () => {
    test("should return empty array when storage has no CoValues", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const ids = await new Promise<{ id: RawCoID }[]>((resolve) => {
        storage.getCoValueIDs(100, 0, resolve);
      });

      expect(ids).toEqual([]);
    });

    test("should return CoValue IDs in batch after storing CoValues", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create CoValues and sync to storage
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      const map = group.createMap();
      map.set("key", "value", "trusting");
      await map.core.waitForSync();

      const ids = await new Promise<{ id: RawCoID }[]>((resolve) => {
        storage.getCoValueIDs(100, 0, resolve);
      });

      expect(ids.map((e) => e.id)).toContain(group.id);
      expect(ids.map((e) => e.id)).toContain(map.id);
      expect(ids.length).toEqual(2);
    });

    test("should paginate when there are more CoValues than the limit and return each ID only once", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      // Create more CoValues than the page size (1 group + 4 maps = 5 CoValues, limit = 2)
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      const expectedIds = new Set<RawCoID>([group.id]);
      const maps: ReturnType<typeof group.createMap>[] = [];
      for (let i = 0; i < 4; i++) {
        const map = group.createMap();
        map.set(`key${i}`, `value${i}`, "trusting");
        maps.push(map);
        expectedIds.add(map.id);
      }
      await maps[maps.length - 1]!.core.waitForSync();

      const limit = 2;
      const allIds: RawCoID[] = [];
      await new Promise<void>((resolve) => {
        const fetchBatch = (offset: number) => {
          storage.getCoValueIDs(limit, offset, (batch) => {
            for (const { id } of batch) {
              allIds.push(id);
            }
            if (batch.length >= limit) {
              fetchBatch(offset + batch.length);
            } else {
              resolve();
            }
          });
        };
        fetchBatch(0);
      });

      expect(allIds).toHaveLength(expectedIds.size);
      const seen = new Set<RawCoID>();
      for (const id of allIds) {
        expect(seen.has(id)).toBe(false);
        seen.add(id);
        expect(expectedIds.has(id)).toBe(true);
      }
    });
  });

  describe("getCoValueCount", () => {
    test("should return 0 when storage has no CoValues", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const count = await new Promise<number>((resolve) => {
        storage.getCoValueCount(resolve);
      });

      expect(count).toBe(0);
    });

    test("should return CoValue count after storing CoValues", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      fixtures.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const client = setupTestNode();
      const { storage } = client.addStorage({
        storage: createSyncStorage({
          filename: dbPath,
          nodeName: "test",
          storageName: "test-storage",
        }),
      });

      const countEmpty = await new Promise<number>((resolve) => {
        storage.getCoValueCount(resolve);
      });
      expect(countEmpty).toBe(0);

      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      await group.core.waitForSync();

      const countOne = await new Promise<number>((resolve) => {
        storage.getCoValueCount(resolve);
      });
      expect(countOne).toBe(1);

      const map = group.createMap();
      map.set("key", "value", "trusting");
      await map.core.waitForSync();

      const countTwo = await new Promise<number>((resolve) => {
        storage.getCoValueCount(resolve);
      });
      expect(countTwo).toBe(2);
    });
  });
});
