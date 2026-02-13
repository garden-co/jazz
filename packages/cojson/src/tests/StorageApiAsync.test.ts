import { afterEach, describe, expect, test, vi } from "vitest";
import { CoID, RawCoID, RawCoMap, logger } from "../exports.js";
import { CoValueCore } from "../exports.js";
import { NewContentMessage } from "../sync.js";
import {
  createAsyncStorage,
  getAllCoValuesWaitingForDelete,
  getCoValueStoredSessions,
  getDbPath,
} from "./testStorage.js";
import {
  SyncMessagesLog,
  fillCoMapWithLargeData,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils.js";
import { CoValueKnownState, emptyKnownState } from "../knownState.js";

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
  SyncMessagesLog.clear();
  vi.useRealTimers();
});

describe("StorageApiAsync", () => {
  describe("getKnownState", () => {
    test("should return known state for existing coValue ID", async () => {
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const id = fixtures.node.createGroup().id;
      const knownState = storage.getKnownState(id);

      expect(knownState).toEqual(emptyKnownState(id));
      expect(storage.getKnownState(id)).toBe(knownState); // Should return same instance
    });

    test("should return different known states for different coValue IDs", async () => {
      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });
      const id1 = "test-id-1";
      const id2 = "test-id-2";

      const knownState1 = storage.getKnownState(id1);
      const knownState2 = storage.getKnownState(id2);

      expect(knownState1).not.toBe(knownState2);
    });
  });

  describe("load", () => {
    test("should handle non-existent coValue gracefully", async () => {
      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
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

    test("should load coValue with header only successfully", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
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

    test("should load coValue with sessions and transactions successfully", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
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
    test("should store new coValue with header successfully", async () => {
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
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

      await storage.store(contentMessage, correctionCallback);
      await storage.waitForSync(group.id, group.core);

      // Verify that storage known state is updated after store
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.knownState());

      client.addStorage({ storage });

      const groupOnNode = await loadCoValueOrFail(client.node, group.id);

      expect(groupOnNode.core.verified.header).toEqual(
        group.core.verified.header,
      );
    });

    test("should store coValue with transactions successfully", async () => {
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
      });

      // Create a real group and add a member to create transactions
      const group = fixtures.node.createGroup();
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

      client.addStorage({ storage });

      const groupOnNode = await loadCoValueOrFail(client.node, group.id);
      expect(groupOnNode.get("everyone")).toEqual("reader");
    });

    test("should handle invalid assumption on header presence with correction", async () => {
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
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

      await storage.store(contentMessage, correctionCallback);
      await storage.waitForSync(group.id, group.core);

      expect(correctionCallback).toHaveBeenCalledTimes(1);

      // Verify that storage known state is updated after store with correction
      const updatedKnownState = storage.getKnownState(group.id);
      expect(updatedKnownState).toEqual(group.core.knownState());

      client.addStorage({ storage });
      const groupOnNode = await loadCoValueOrFail(client.node, group.id);

      expect(groupOnNode.get("everyone")).toEqual("reader");
    });

    test("should handle invalid assumption on new content with correction", async () => {
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
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

      await storage.store(initialContent, correctionCallback);
      await storage.store(contentMessage, correctionCallback);

      await storage.waitForSync(group.id, group.core);

      expect(correctionCallback).toHaveBeenCalledTimes(1);

      // Verify that storage known state is updated after store with correction
      const finalKnownState = storage.getKnownState(group.id);
      expect(finalKnownState).toEqual(group.core.knownState());

      client.addStorage({ storage });
      const groupOnNode = await loadCoValueOrFail(client.node, group.id);

      expect(groupOnNode.get("everyone")).toEqual("writer");
    });

    test("should log an error when the correction callback returns undefined", async () => {
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
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
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const group = fixtures.node.createGroup();

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
      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
      });

      const core = client.node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...client.node.crypto.createdNowUnique(),
      });

      core.makeTransaction([{ count: 1 }], "trusting");

      await core.waitForSync();

      // Add storage later
      client.addStorage({ storage });

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
      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
      });

      const core = client.node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...client.node.crypto.createdNowUnique(),
      });

      const core2 = client.node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...client.node.crypto.createdNowUnique(),
      });

      core.makeTransaction([{ count: 1 }], "trusting");
      core2.makeTransaction([{ count: 1 }], "trusting");

      await core.waitForSync();

      // Add storage later
      client.addStorage({ storage });

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
      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
      });

      const core = client.node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...client.node.crypto.createdNowUnique(),
      });

      const core2 = client.node.createCoValue({
        type: "comap",
        ruleset: { type: "unsafeAllowAll" },
        meta: null,
        ...client.node.crypto.createdNowUnique(),
      });

      core.makeTransaction([{ count: 1 }], "trusting");
      core2.makeTransaction([{ count: 1 }], "trusting");

      await core.waitForSync();

      // Add storage later
      client.addStorage({ storage });

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
      const dbPath = getDbPath();

      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const fixture2 = setupTestNode();
      await fixture2.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
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

      await storage.store(contentMessage, correctionCallback);
      await storage.waitForSync(mapOnNode2.id, mapOnNode2.core);

      client.addStorage({ storage });

      const finalMap = await loadCoValueOrFail(client.node, mapOnNode2.id);
      expect(finalMap.core.knownState()).toEqual(knownState);
    });
  });

  describe("delete flow", () => {
    test("deleteCoValue enqueues the coValue for erasure", async () => {
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
      });

      const client = setupTestNode();
      client.addStorage({ storage });

      const group = client.node.createGroup();
      const map = group.createMap();
      map.core.deleteCoValue();
      await map.core.waitForSync();

      expect(await getAllCoValuesWaitingForDelete(storage)).toContain(map.id);
    });

    test("background erasure doesn't run if not enabled", async () => {
      const dbPath = getDbPath();

      const node = setupTestNode();
      const { storage } = await node.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const group = node.node.createGroup();
      const map = group.createMap();
      map.set("k", "v");
      await map.core.waitForSync();

      vi.useFakeTimers();

      map.core.deleteCoValue();
      await map.core.waitForSync();

      await vi.runAllTimersAsync();

      expect(await getAllCoValuesWaitingForDelete(storage)).toContain(map.id);
    });

    test("background erasure runs if enabled", async () => {
      const dbPath = getDbPath();

      const node = setupTestNode();
      const { storage } = await node.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      vi.useFakeTimers();
      node.node.enableDeletedCoValuesErasure();

      const group = node.node.createGroup();
      const map = group.createMap();
      map.set("k", "v");
      await map.core.waitForSync();

      map.core.deleteCoValue();
      await map.core.waitForSync();

      await vi.runAllTimersAsync();

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
      const { storage } = await node.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const group = node.node.createGroup();
      const map = group.createMap();
      map.set("k", "v");
      await map.core.waitForSync();

      map.core.deleteCoValue();
      await map.core.waitForSync();

      await waitFor(async () => {
        const queued = await getAllCoValuesWaitingForDelete(storage);
        expect(queued).toContain(map.id);
        return true;
      });

      await storage.eraseAllDeletedCoValues();

      const queued = await getAllCoValuesWaitingForDelete(storage);
      expect(queued).not.toContain(map.id);

      // Tombstone preserved + history erased when loaded from storage
      const client = setupTestNode();
      const { storage: clientStorage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
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
      const { storage } = await node.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const group = node.node.createGroup();
      const map = group.createMap();
      fillCoMapWithLargeData(map);
      await map.core.waitForSync();
      map.core.deleteCoValue();
      await map.core.waitForSync();

      storage.close();

      const newSession = node.spawnNewSession();
      const { storage: newStorage } = await newSession.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const callback = vi.fn();

      const loadPromise = new Promise((resolve) => {
        newStorage.load(map.id, callback, resolve);
      });
      await newStorage.eraseAllDeletedCoValues();

      expect(await loadPromise).toBe(true);
    });

    test("load interrupts eraseAllDeletedCoValues mid-run (resolves early, leaves some queued)", async () => {
      const dbPath = getDbPath();

      const node = setupTestNode();
      const { storage } = await node.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const group = node.node.createGroup();

      const map1 = group.createMap();
      map1.set("k", "v");
      await map1.core.waitForSync();

      const map2 = group.createMap();
      map2.set("k", "v");
      await map2.core.waitForSync();

      map1.core.deleteCoValue();
      map2.core.deleteCoValue();
      await map1.core.waitForSync();
      await map2.core.waitForSync();

      await waitFor(async () => {
        const queued = await getAllCoValuesWaitingForDelete(storage);
        expect(queued).toEqual(expect.arrayContaining([map1.id, map2.id]));
        return true;
      });

      const { promise: barrier, resolve: releaseBarrier } =
        Promise.withResolvers<void>();
      const { promise: firstTxStarted, resolve: firstTxStartedResolve } =
        Promise.withResolvers<void>();

      // @ts-expect-error - dbClient is private
      const dbClient = storage.dbClient;
      const originalTransaction = dbClient.transaction.bind(dbClient);

      let txCalls = 0;
      const txSpy = vi
        .spyOn(dbClient, "transaction")
        .mockImplementation(async (callback) => {
          txCalls += 1;
          return originalTransaction(async (tx) => {
            if (txCalls === 1) {
              firstTxStartedResolve();
              await barrier;
            }
            return callback(tx);
          });
        });

      const erasePromise = storage.eraseAllDeletedCoValues();

      // Ensure the eraser is in-flight and inside its first transaction.
      await firstTxStarted;

      // Trigger interruption. We don't await the load immediately to avoid doing
      // DB reads while the transaction is being held open by the barrier.
      const loadDone = new Promise<boolean>((resolve) => {
        void storage.load("non-existent-id", () => {}, resolve);
      });

      releaseBarrier();

      await erasePromise;
      await loadDone;

      const queuedAfter = await getAllCoValuesWaitingForDelete(storage);
      expect(queuedAfter).toHaveLength(1);

      txSpy.mockRestore();
    });
  });

  describe("dependencies", () => {
    test("should push dependencies before the coValue", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
        filename: dbPath,
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

    test("should handle dependencies that are already loaded correctly", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
        filename: dbPath,
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
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
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
    test("should resolve when the coValue is already synced", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
        filename: dbPath,
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
      await storage.store(contentMessage, correctionCallback);

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
    test("should close without throwing an error", async () => {
      const storage = await createAsyncStorage({
        nodeName: "test",
        storageName: "test-storage",
      });

      expect(() => storage.close()).not.toThrow();
    });
  });

  describe("loadKnownState", () => {
    test("should return cached knownState if available", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
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

    test("should return undefined for non-existent CoValue", async () => {
      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const result = await new Promise<CoValueKnownState | undefined>(
        (resolve) => {
          storage.loadKnownState("co_nonexistent" as any, resolve);
        },
      );

      expect(result).toBeUndefined();
    });

    test("should deduplicate concurrent requests for the same ID", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      // Create a group to have data in the database
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      await group.core.waitForSync();

      // Clear the cache to force database access
      storage.knownStates.knownStates.clear();

      // Spy on the database client to track how many times it's called
      const dbClientSpy = vi.spyOn(
        (storage as any).dbClient,
        "getCoValueKnownState",
      );

      // Make multiple concurrent requests for the same ID
      const promises = [
        new Promise<CoValueKnownState | undefined>((resolve) => {
          storage.loadKnownState(group.id, resolve);
        }),
        new Promise<CoValueKnownState | undefined>((resolve) => {
          storage.loadKnownState(group.id, resolve);
        }),
        new Promise<CoValueKnownState | undefined>((resolve) => {
          storage.loadKnownState(group.id, resolve);
        }),
      ];

      const results = await Promise.all(promises);

      // All results should be the same
      expect(results[0]).toEqual(results[1]);
      expect(results[1]).toEqual(results[2]);
      expect(results[0]?.id).toBe(group.id);

      // Database should only be called once due to deduplication
      expect(dbClientSpy).toHaveBeenCalledTimes(1);
    });

    test("should use cache and not query database when cache is populated", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      // Create a group to have data in the database
      const group = fixtures.node.createGroup();
      group.addMember("everyone", "reader");
      await group.core.waitForSync();

      // Spy on the database client to track calls
      const dbClientSpy = vi.spyOn(
        (storage as any).dbClient,
        "getCoValueKnownState",
      );

      // First call - should hit the database
      const result1 = await new Promise<CoValueKnownState | undefined>(
        (resolve) => {
          storage.loadKnownState(group.id, resolve);
        },
      );

      expect(result1).toBeDefined();
      expect(dbClientSpy).toHaveBeenCalledTimes(1);

      // Clear the spy to reset call count
      dbClientSpy.mockClear();

      // Second call - should use cache, not database
      const result2 = await new Promise<CoValueKnownState | undefined>(
        (resolve) => {
          storage.loadKnownState(group.id, resolve);
        },
      );

      expect(result2).toEqual(result1);
      // Database should NOT be called since cache was hit
      expect(dbClientSpy).toHaveBeenCalledTimes(0);

      // Third call - also from cache
      const result3 = await new Promise<CoValueKnownState | undefined>(
        (resolve) => {
          storage.loadKnownState(group.id, resolve);
        },
      );

      expect(result3).toEqual(result1);
      expect(dbClientSpy).toHaveBeenCalledTimes(0);
    });
  });

  describe("getCoValueIDs", () => {
    test("should return empty array when storage has no CoValues", async () => {
      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const ids = await new Promise<{ id: RawCoID }[]>((resolve) => {
        storage.getCoValueIDs(100, 0, resolve);
      });

      expect(ids).toEqual([]);
    });

    test("should return CoValue IDs in batch after storing CoValues", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
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
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
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
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
      });

      const count = await new Promise<number>((resolve) => {
        storage.getCoValueCount(resolve);
      });

      expect(count).toBe(0);
    });

    test("should return CoValue count after storing CoValues", async () => {
      const dbPath = getDbPath();
      const fixtures = setupTestNode();
      await fixtures.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
      });

      const client = setupTestNode();
      const { storage } = await client.addAsyncStorage({
        ourName: "test",
        storageName: "test-storage",
        filename: dbPath,
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
