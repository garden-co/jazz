import { randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, onTestFinished, test, vi } from "vitest";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { CoID, LocalNode, RawCoMap, logger } from "../exports.js";
import { CoValueCore } from "../exports.js";
import { NewContentMessage } from "../sync.js";
import { createAsyncStorage } from "./testStorage.js";
import {
  SyncMessagesLog,
  loadCoValueOrFail,
  randomAgentAndSessionID,
  waitFor,
} from "./testUtils.js";
import { CoValueKnownState, emptyKnownState } from "../knownState.js";

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
  const storage = await createAsyncStorage({
    filename: dbPath,
    nodeName: "test",
    storageName: "test-storage",
  });

  onTestFinished(() => {
    try {
      unlinkSync(dbPath);
    } catch {}
  });

  onTestFinished(async () => {
    await node.gracefulShutdown();
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

  const storage = await createAsyncStorage({
    filename: dbPath,
    nodeName: "test",
    storageName: "test-storage",
  });

  onTestFinished(async () => {
    node.gracefulShutdown();
    await storage.close();
  });

  return {
    node,
    storage,
  };
}

afterEach(() => {
  SyncMessagesLog.clear();
});

describe("StorageApiAsync", () => {
  describe("getKnownState", () => {
    test("should return known state for existing coValue ID", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { storage } = await createTestNode();

      const id = fixturesNode.createGroup().id;
      const knownState = storage.getKnownState(id);

      expect(knownState).toEqual(emptyKnownState(id));
      expect(storage.getKnownState(id)).toBe(knownState); // Should return same instance
    });

    test("should return different known states for different coValue IDs", async () => {
      const { storage } = await createTestNode();
      const id1 = "test-id-1";
      const id2 = "test-id-2";

      const knownState1 = storage.getKnownState(id1);
      const knownState2 = storage.getKnownState(id2);

      expect(knownState1).not.toBe(knownState2);
    });
  });

  describe("load", () => {
    test("should handle non-existent coValue gracefully", async () => {
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

    test("should load coValue with header only successfully", async () => {
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
      expect(updatedKnownState).toEqual(group.core.knownState());

      const groupOnNode = await loadCoValueOrFail(node, group.id);

      expect(groupOnNode.core.verified.header).toEqual(
        group.core.verified.header,
      );
    });

    test("should load coValue with sessions and transactions successfully", async () => {
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
      expect(updatedKnownState).toEqual(group.core.knownState());

      const groupOnNode = await loadCoValueOrFail(node, group.id);
      expect(groupOnNode.get("everyone")).toEqual("reader");
    });
  });

  describe("store", () => {
    test("should store new coValue with header successfully", async () => {
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

      await storage.store(contentMessage, correctionCallback);
      await storage.waitForSync(group.id, group.core);

      // Verify that storage known state is updated after store
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.knownState());

      node.setStorage(storage);

      const groupOnNode = await loadCoValueOrFail(node, group.id);

      expect(groupOnNode.core.verified.header).toEqual(
        group.core.verified.header,
      );
    });

    test("should store coValue with transactions successfully", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { node, storage } = await createTestNode();

      // Create a real group and add a member to create transactions
      const group = fixturesNode.createGroup();
      const knownState = group.core.knownState();

      group.addMember("everyone", "reader");

      const contentMessage = getNewContentSince(
        group.core,
        emptyKnownState(group.id),
      );
      const correctionCallback = vi.fn();

      // Get initial known state
      const initialKnownState = storage.getKnownState(group.id);
      expect(initialKnownState).toEqual(emptyKnownState(group.id));

      await storage.store(contentMessage, correctionCallback);
      await storage.waitForSync(group.id, group.core);

      // Verify that storage known state is updated after store
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.knownState());

      node.setStorage(storage);

      const groupOnNode = await loadCoValueOrFail(node, group.id);
      expect(groupOnNode.get("everyone")).toEqual("reader");
    });

    test("should handle invalid assumption on header presence with correction", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { node, storage } = await createTestNode();

      const group = fixturesNode.createGroup();
      const knownState = group.core.knownState();

      group.addMember("everyone", "reader");

      const contentMessage = getNewContentSince(group.core, knownState);
      const correctionCallback = vi.fn((known) => {
        expect(known).toEqual(emptyKnownState(group.id));
        return group.core.verified.newContentSince(known);
      });

      // Get initial known state
      const initialKnownState = storage.getKnownState(group.id);
      expect(initialKnownState).toEqual(emptyKnownState(group.id));

      await storage.store(contentMessage, correctionCallback);
      await storage.waitForSync(group.id, group.core);

      expect(correctionCallback).toHaveBeenCalledTimes(1);

      // Verify that storage known state is updated after store with correction
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.knownState());

      node.setStorage(storage);
      const groupOnNode = await loadCoValueOrFail(node, group.id);

      expect(groupOnNode.get("everyone")).toEqual("reader");
    });

    test("should handle invalid assumption on new content with correction", async () => {
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

      await storage.store(initialContent, correctionCallback);
      await storage.store(contentMessage, correctionCallback);

      await storage.waitForSync(group.id, group.core);

      expect(correctionCallback).toHaveBeenCalledTimes(1);

      // Verify that storage known state is updated after store with correction
      const finalKnownState = storage.getKnownState(group.id);
      expect(finalKnownState).toEqual(group.core.knownState());

      node.setStorage(storage);
      const groupOnNode = await loadCoValueOrFail(node, group.id);

      expect(groupOnNode.get("everyone")).toEqual("writer");
    });

    test("should log an error when the correction callback returns undefined", async () => {
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
      await storage.store(contentMessage, correctionCallback);

      await waitFor(() => {
        expect(correctionCallback).toHaveBeenCalledTimes(1);
      });

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

    test("should log an error when the correction callback returns an invalid content message", async () => {
      const { fixturesNode } = await createFixturesNode();
      const { storage } = await createTestNode();

      const group = fixturesNode.createGroup();

      const knownState = group.core.knownState();
      group.addMember("everyone", "writer");

      const contentMessage = getNewContentSince(group.core, knownState);
      const correctionCallback = vi.fn(() => {
        return [contentMessage];
      });

      // Get initial known state
      const initialKnownState = storage.getKnownState(group.id);
      expect(initialKnownState).toEqual(emptyKnownState(group.id));

      const errorSpy = vi.spyOn(logger, "error").mockImplementation(() => {});
      await storage.store(contentMessage, correctionCallback);

      await waitFor(() => {
        expect(correctionCallback).toHaveBeenCalledTimes(1);
      });

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

      expect(errorSpy).toHaveBeenCalledWith("Double correction requested", {
        knownState: expect.any(Object),
        msg: expect.any(Object),
      });

      errorSpy.mockClear();
    });

    test("should handle invalid assumption when pushing multiple transactions with correction", async () => {
      const { node, storage } = await createTestNode();

      const core = node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...crypto.createdNowUnique(),
      });

      core.makeTransaction([{ count: 1 }], "trusting");

      await core.waitForSync();

      // Add storage later
      node.setStorage(storage);

      core.makeTransaction([{ count: 2 }], "trusting");
      core.makeTransaction([{ count: 3 }], "trusting");

      await new Promise((resolve) => setTimeout(resolve, 10));

      core.makeTransaction([{ count: 4 }], "trusting");
      core.makeTransaction([{ count: 5 }], "trusting");

      await core.waitForSync();

      expect(storage.getKnownState(core.id)).toEqual(core.knownState());

      expect(
        SyncMessagesLog.getMessages({
          Core: core,
        }),
      ).toMatchInlineSnapshot(`
        [
          "test -> test-storage | CONTENT Core header: false new: After: 1 New: 2",
          "test-storage -> test | KNOWN CORRECTION Core sessions: empty",
          "test -> test-storage | CONTENT Core header: true new: After: 0 New: 3",
          "test -> test-storage | CONTENT Core header: false new: After: 3 New: 2",
        ]
      `);
    });

    test("should handle invalid assumption when pushing multiple transactions on different coValues with correction", async () => {
      const { node, storage } = await createTestNode();

      const core = node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...crypto.createdNowUnique(),
      });

      const core2 = node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...crypto.createdNowUnique(),
      });

      core.makeTransaction([{ count: 1 }], "trusting");
      core2.makeTransaction([{ count: 1 }], "trusting");

      await core.waitForSync();

      // Add storage later
      node.setStorage(storage);

      core.makeTransaction([{ count: 2 }], "trusting");
      core2.makeTransaction([{ count: 2 }], "trusting");
      core.makeTransaction([{ count: 3 }], "trusting");
      core2.makeTransaction([{ count: 3 }], "trusting");

      await new Promise((resolve) => setTimeout(resolve, 10));

      core.makeTransaction([{ count: 4 }], "trusting");
      core2.makeTransaction([{ count: 4 }], "trusting");
      core.makeTransaction([{ count: 5 }], "trusting");
      core2.makeTransaction([{ count: 5 }], "trusting");

      await core.waitForSync();

      expect(storage.getKnownState(core.id)).toEqual(core.knownState());

      expect(
        SyncMessagesLog.getMessages({
          Core: core,
          Core2: core2,
        }),
      ).toMatchInlineSnapshot(`
        [
          "test -> test-storage | CONTENT Core header: false new: After: 1 New: 1",
          "test -> test-storage | CONTENT Core2 header: false new: After: 1 New: 1",
          "test -> test-storage | CONTENT Core header: false new: After: 2 New: 1",
          "test -> test-storage | CONTENT Core2 header: false new: After: 2 New: 1",
          "test-storage -> test | KNOWN CORRECTION Core sessions: empty",
          "test -> test-storage | CONTENT Core header: true new: After: 0 New: 3",
          "test-storage -> test | KNOWN CORRECTION Core2 sessions: empty",
          "test -> test-storage | CONTENT Core2 header: true new: After: 0 New: 3",
          "test -> test-storage | CONTENT Core header: false new: After: 3 New: 1",
          "test -> test-storage | CONTENT Core2 header: false new: After: 3 New: 1",
          "test -> test-storage | CONTENT Core header: false new: After: 4 New: 1",
          "test -> test-storage | CONTENT Core2 header: false new: After: 4 New: 1",
        ]
      `);
    });

    test("should handle close while pushing multiple transactions on different coValues with an invalid assumption", async () => {
      const { node, storage } = await createTestNode();

      const core = node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...crypto.createdNowUnique(),
      });

      const core2 = node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...crypto.createdNowUnique(),
      });

      core.makeTransaction([{ count: 1 }], "trusting");
      core2.makeTransaction([{ count: 1 }], "trusting");

      await core.waitForSync();

      // Add storage later
      node.setStorage(storage);

      core.makeTransaction([{ count: 2 }], "trusting");
      core2.makeTransaction([{ count: 2 }], "trusting");
      core.makeTransaction([{ count: 3 }], "trusting");
      core2.makeTransaction([{ count: 3 }], "trusting");

      await new Promise<void>(queueMicrotask);

      await storage.close();
      const knownState = JSON.parse(
        JSON.stringify(storage.getKnownState(core.id)),
      );

      core.makeTransaction([{ count: 4 }], "trusting");
      core2.makeTransaction([{ count: 4 }], "trusting");
      core.makeTransaction([{ count: 5 }], "trusting");
      core2.makeTransaction([{ count: 5 }], "trusting");

      await new Promise<void>((resolve) => setTimeout(resolve, 10));

      expect(
        SyncMessagesLog.getMessages({
          Core: core,
          Core2: core2,
        }),
      ).toMatchInlineSnapshot(`
        [
          "test -> test-storage | CONTENT Core header: false new: After: 1 New: 1",
          "test -> test-storage | CONTENT Core2 header: false new: After: 1 New: 1",
          "test -> test-storage | CONTENT Core header: false new: After: 2 New: 1",
          "test -> test-storage | CONTENT Core2 header: false new: After: 2 New: 1",
          "test-storage -> test | KNOWN CORRECTION Core sessions: empty",
          "test -> test-storage | CONTENT Core header: true new: After: 0 New: 3",
          "test -> test-storage | CONTENT Core header: false new: After: 3 New: 1",
          "test -> test-storage | CONTENT Core2 header: false new: After: 3 New: 1",
          "test -> test-storage | CONTENT Core header: false new: After: 4 New: 1",
          "test -> test-storage | CONTENT Core2 header: false new: After: 4 New: 1",
        ]
      `);

      expect(storage.getKnownState(core.id)).toEqual(knownState);
    });

    test("should handle multiple sessions correctly", async () => {
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

      await storage.store(contentMessage, correctionCallback);
      await storage.waitForSync(mapOnNode2.id, mapOnNode2.core);

      node.setStorage(storage);

      const finalMap = await loadCoValueOrFail(node, mapOnNode2.id);
      expect(finalMap.core.knownState()).toEqual(knownState);
    });
  });

  describe("dependencies", () => {
    test("should push dependencies before the coValue", async () => {
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
      expect(updatedGroupKnownState).toEqual(group.core.knownState());
      expect(updatedMapKnownState).toEqual(map.core.knownState());

      node.setStorage(storage);
      const mapOnNode = await loadCoValueOrFail(node, map.id);
      expect(mapOnNode.get("test")).toEqual("value");
    });

    test("should handle dependencies that are already loaded correctly", async () => {
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

      node.setStorage(storage);
      const mapOnNode = await loadCoValueOrFail(node, map.id);
      expect(mapOnNode.get("test")).toEqual("value");
    });
  });

  describe("waitForSync", () => {
    test("should resolve when the coValue is already synced", async () => {
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
      await storage.store(contentMessage, correctionCallback);

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
    test("should close without throwing an error", async () => {
      const { storage } = await createTestNode();

      expect(() => storage.close()).not.toThrow();
    });
  });
});
