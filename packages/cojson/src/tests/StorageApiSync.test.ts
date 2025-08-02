import { randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, onTestFinished, test, vi } from "vitest";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { CoID, LocalNode, RawCoID, RawCoMap, logger } from "../exports.js";
import { CoValueCore } from "../exports.js";
import {
  CoValueKnownState,
  NewContentMessage,
  emptyKnownState,
} from "../sync.js";
import { createSyncStorage } from "./testStorage.js";
import { loadCoValueOrFail, randomAgentAndSessionID } from "./testUtils.js";

const crypto = await WasmCrypto.create();

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

  const contentMessage = coValue.verified.newContentSince(knownState)?.[0];

  if (!contentMessage) {
    throw new Error(`No new content available for coValue ${coValue.id}`);
  }

  return contentMessage;
}

async function createFixturesNode(customDbPath?: string) {
  const [admin, session] = randomAgentAndSessionID();
  const node = new LocalNode(admin.agentSecret, session, crypto);

  // Create a unique database file for each test
  const dbPath = customDbPath ?? join(tmpdir(), `test-${randomUUID()}.db`);
  const storage = createSyncStorage({
    filename: dbPath,
    nodeName: "test",
    storageName: "test-storage",
  });

  onTestFinished(() => {
    try {
      unlinkSync(dbPath);
    } catch {}
  });

  node.setStorage(storage);

  return {
    fixturesNode: node,
    dbPath,
  };
}

async function createTestNode(dbPath?: string) {
  const [admin, session] = randomAgentAndSessionID();
  const node = new LocalNode(admin.agentSecret, session, crypto);

  const storage = createSyncStorage({
    filename: dbPath,
    nodeName: "test",
    storageName: "test-storage",
  });

  return {
    node,
    storage,
  };
}

describe("StorageApiSync", () => {
  describe("getKnownState", () => {
    test("should return empty known state for new coValue ID and cache the result", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { storage } = await createTestNode();

      const id = fixturesNode.createGroup().id;
      const knownState = storage.getKnownState(id);

      expect(knownState).toEqual(emptyKnownState(id));
      expect(storage.getKnownState(id)).toBe(knownState); // Should return same instance
    });

    test("should return separate known state instances for different coValue IDs", async () => {
      const { storage } = await createTestNode();
      const id1 = "test-id-1";
      const id2 = "test-id-2";

      const knownState1 = storage.getKnownState(id1);
      const knownState2 = storage.getKnownState(id2);

      expect(knownState1).not.toBe(knownState2);
    });
  });

  describe("load", () => {
    test("should fail gracefully when loading non-existent coValue and preserve known state", async () => {
      const { storage } = await createTestNode();
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
      const { fixturesNode, dbPath } = await createFixturesNode();
      const { node, storage } = await createTestNode(dbPath);
      const callback = vi.fn((content) =>
        node.syncManager.handleNewContent(content, "storage"),
      );
      const done = vi.fn();

      // Create a real group and get its content message
      const group = fixturesNode.createGroup();
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
      expect(updatedKnownState).toEqual(group.core.verified.knownState());

      const groupOnNode = await loadCoValueOrFail(node, group.id);

      expect(groupOnNode.core.verified.header).toEqual(
        group.core.verified.header,
      );
    });

    test("should successfully load coValue with transactions and update known state", async () => {
      const { fixturesNode, dbPath } = await createFixturesNode();
      const { node, storage } = await createTestNode(dbPath);
      const callback = vi.fn((content) =>
        node.syncManager.handleNewContent(content, "storage"),
      );
      const done = vi.fn();

      // Create a real group and add a member to create transactions
      const group = fixturesNode.createGroup();
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
            [fixturesNode.currentSessionID]: expect.any(Object),
          }),
        }),
      );
      expect(done).toHaveBeenCalledWith(true);

      // Verify that storage known state is updated after load
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.verified.knownState());

      const groupOnNode = await loadCoValueOrFail(node, group.id);
      expect(groupOnNode.get("everyone")).toEqual("reader");
    });
  });

  describe("store", () => {
    test("should successfully store new coValue with header and update known state", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { node, storage } = await createTestNode();
      // Create a real group and get its content message
      const group = fixturesNode.createGroup();
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
      expect(updatedKnownState).toEqual(group.core.verified.knownState());

      node.setStorage(storage);

      const groupOnNode = await loadCoValueOrFail(node, group.id);

      expect(groupOnNode.core.verified.header).toEqual(
        group.core.verified.header,
      );
    });

    test("should successfully store coValue with transactions and update known state", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { node, storage } = await createTestNode();

      // Create a real group and add a member to create transactions
      const group = fixturesNode.createGroup();
      const knownState = group.core.verified.knownState();

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
      expect(updatedKnownState).toEqual(group.core.verified.knownState());

      node.setStorage(storage);

      const groupOnNode = await loadCoValueOrFail(node, group.id);
      expect(groupOnNode.get("everyone")).toEqual("reader");
    });

    test("should handle correction when header assumption is invalid", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { node, storage } = await createTestNode();

      const group = fixturesNode.createGroup();
      const knownState = group.core.verified.knownState();

      group.addMember("everyone", "reader");

      const contentMessage = getNewContentSince(group.core, knownState);
      const correctionCallback = vi.fn((known) => {
        expect(known).toEqual(emptyKnownState(group.id));
        return group.core.verified.newContentSince(known);
      });

      // Get initial known state
      const initialKnownState = storage.getKnownState(group.id);
      expect(initialKnownState).toEqual(emptyKnownState(group.id));

      const result = storage.store(contentMessage, correctionCallback);

      expect(correctionCallback).toHaveBeenCalledTimes(1);
      expect(result).toBe(true);

      // Verify that storage known state is updated after store with correction
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.verified.knownState());

      node.setStorage(storage);
      const groupOnNode = await loadCoValueOrFail(node, group.id);

      expect(groupOnNode.get("everyone")).toEqual("reader");
    });

    test("should handle correction when new content assumption is invalid", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { node, storage } = await createTestNode();

      const group = fixturesNode.createGroup();

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
        return group.core.verified.newContentSince(known);
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
      expect(finalKnownState).toEqual(group.core.verified.knownState());

      node.setStorage(storage);
      const groupOnNode = await loadCoValueOrFail(node, group.id);

      expect(groupOnNode.get("everyone")).toEqual("writer");
    });

    test("should log error and fail when correction callback returns undefined", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { storage } = await createTestNode();

      const group = fixturesNode.createGroup();

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
          correction: null,
        },
      );

      errorSpy.mockClear();
    });

    test("should log error and fail when correction callback returns invalid content message", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { storage } = await createTestNode();

      const group = fixturesNode.createGroup();

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
          correction: null,
        },
      );

      expect(errorSpy).toHaveBeenCalledWith("Double correction requested", {
        knownState: expect.any(Object),
        msg: expect.any(Object),
      });

      errorSpy.mockClear();
    });

    test("should successfully store coValue with multiple sessions", async () => {
      const { fixturesNode, dbPath } = await createFixturesNode();
      const { fixturesNode: fixtureNode2 } = await createFixturesNode(dbPath);
      const { node, storage } = await createTestNode();

      const coValue = fixturesNode.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...crypto.createdNowUnique(),
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
        fixtureNode2,
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

      node.setStorage(storage);

      const finalMap = await loadCoValueOrFail(node, mapOnNode2.id);
      expect(finalMap.core.knownState()).toEqual(knownState);
    });
  });

  describe("dependencies", () => {
    test("should load dependencies before dependent coValues and update all known states", async () => {
      const { fixturesNode, dbPath } = await createFixturesNode();
      const { node, storage } = await createTestNode(dbPath);

      // Create a group and a map owned by that group to create dependencies
      const group = fixturesNode.createGroup();
      group.addMember("everyone", "reader");
      const map = group.createMap({ test: "value" });
      await group.core.waitForSync();
      await map.core.waitForSync();

      const callback = vi.fn((content) =>
        node.syncManager.handleNewContent(content, "storage"),
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
      expect(updatedGroupKnownState).toEqual(group.core.verified.knownState());
      expect(updatedMapKnownState).toEqual(map.core.verified.knownState());

      node.setStorage(storage);
      const mapOnNode = await loadCoValueOrFail(node, map.id);
      expect(mapOnNode.get("test")).toEqual("value");
    });

    test("should skip loading already loaded dependencies", async () => {
      const { fixturesNode, dbPath } = await createFixturesNode();
      const { node, storage } = await createTestNode(dbPath);

      // Create a group and a map owned by that group
      const group = fixturesNode.createGroup();
      group.addMember("everyone", "reader");
      const map = group.createMap({ test: "value" });
      await group.core.waitForSync();
      await map.core.waitForSync();

      const callback = vi.fn((content) =>
        node.syncManager.handleNewContent(content, "storage"),
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
      expect(afterGroupLoad).toEqual(group.core.verified.knownState());

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
      expect(finalMapKnownState).toEqual(map.core.verified.knownState());

      node.setStorage(storage);
      const mapOnNode = await loadCoValueOrFail(node, map.id);
      expect(mapOnNode.get("test")).toEqual("value");
    });
  });

  describe("waitForSync", () => {
    test("should resolve immediately when coValue is already synced", async () => {
      const { fixturesNode, dbPath } = await createFixturesNode();
      const { node, storage } = await createTestNode(dbPath);

      // Create a group and add a member
      const group = fixturesNode.createGroup();
      group.addMember("everyone", "reader");
      await group.core.waitForSync();

      // Store the group in storage
      const contentMessage = getNewContentSince(
        group.core,
        emptyKnownState(group.id),
      );
      const correctionCallback = vi.fn();
      storage.store(contentMessage, correctionCallback);

      node.setStorage(storage);

      // Load the group on the new node
      const groupOnNode = await loadCoValueOrFail(node, group.id);

      // Wait for sync should resolve immediately since the coValue is already synced
      await expect(
        storage.waitForSync(group.id, groupOnNode.core),
      ).resolves.toBeUndefined();

      expect(groupOnNode.get("everyone")).toEqual("reader");
    });
  });

  describe("close", () => {
    test("should close storage without throwing errors", async () => {
      const { storage } = await createTestNode();

      expect(() => storage.close()).not.toThrow();
    });
  });
});
