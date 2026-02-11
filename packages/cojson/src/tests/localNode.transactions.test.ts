import { describe, expect, test, beforeEach, vi } from "vitest";
import { SyncMessagesLog, setupTestNode } from "./testUtils.js";
import { BatchMessage, NewContentMessage } from "../sync.js";
import { SessionID } from "../exports.js";

describe("LocalNode.unstable_withTransaction", () => {
  beforeEach(() => {
    SyncMessagesLog.clear();
  });

  test("executes callback synchronously and returns result", async () => {
    const node = setupTestNode().node;

    const result = await node.unstable_withTransaction(() => {
      return "test-result";
    });

    expect(result).toBe("test-result");
  });

  test("throws error for nested transactions", async () => {
    const node = setupTestNode().node;

    await expect(
      node.unstable_withTransaction(() => {
        // Try to start a nested transaction
        return node.unstable_withTransaction(() => {
          return "nested";
        });
      }),
    ).rejects.toThrow("Nested transactions are not supported");
  });

  test("cleans up transaction context on success", async () => {
    const node = setupTestNode().node;

    await node.unstable_withTransaction(() => {
      // Transaction context should be active here
      expect(node.getTransactionContext()).toBeDefined();
    });

    // Transaction context should be cleaned up
    expect(node.getTransactionContext()).toBeUndefined();
  });

  test("cleans up transaction context on error", async () => {
    const node = setupTestNode().node;
    const error = new Error("Test error");

    try {
      await node.unstable_withTransaction(() => {
        throw error;
      });
    } catch (e) {
      expect(e).toBe(error);
    }

    // Transaction context should be cleaned up even on error
    expect(node.getTransactionContext()).toBeUndefined();
  });

  test("transaction context is undefined outside of transaction", () => {
    const node = setupTestNode().node;
    expect(node.getTransactionContext()).toBeUndefined();
  });

  test("empty transaction is a no-op", async () => {
    // Set up a sync server first
    setupTestNode({ isSyncServer: true });
    const { node, connectToSyncServer } = setupTestNode();
    connectToSyncServer();

    // Start capturing messages
    const messagesBefore = SyncMessagesLog.messages.length;

    await node.unstable_withTransaction(() => {
      // Empty transaction - no mutations
    });

    const messagesAfter = SyncMessagesLog.messages.length;

    expect(messagesAfter).toBe(messagesBefore);
  });

  test("buffers mutations and syncs them after callback completes", async () => {
    // Set up a sync server first
    setupTestNode({ isSyncServer: true });
    const { node, connectToSyncServer } = setupTestNode();
    const { peer } = connectToSyncServer();

    // Create a group to test mutations
    const group = node.createGroup();

    // Clear previous messages
    SyncMessagesLog.clear();

    // Create a CoMap within a transaction
    const result = await node.unstable_withTransaction(() => {
      const map = group.createMap({ test: "value" });
      map.set("key1", "value1", "trusting");
      map.set("key2", "value2", "trusting");

      return map.id;
    });

    // Mutations should have been synced as a batch
    const batchMessages = SyncMessagesLog.messages.filter(
      (m) => m.msg.action === "batch",
    );

    expect(batchMessages.length).toBe(1);
    expect(batchMessages).toStrictEqual([
      {
        from: "client",
        to: "server",
        msg: {
          action: "batch",
          messages: [
            // coMap creation
            {
              id: result,
              action: "content",
              header: undefined,
              priority: 3,
              new: expect.any(Object),
            },
            // key1
            {
              id: result,
              action: "content",
              header: undefined,
              priority: 3,
              new: expect.any(Object),
            },
            // key2
            {
              id: result,
              action: "content",
              header: undefined,
              priority: 3,
              new: expect.any(Object),
            },
          ],
        },
      },
    ]);

    const coMapCreationMessage = (batchMessages[0]!.msg as BatchMessage)
      .messages[0]!;

    const coMapCreationMessageSealedId = Object.keys(
      coMapCreationMessage.new,
    )[0] as SessionID;
    expect(
      coMapCreationMessage.new[coMapCreationMessageSealedId],
    ).toStrictEqual({
      after: 0,
      lastSignature: expect.any(String),
      newTransactions: [
        expect.objectContaining({
          privacy: "private",
        }),
      ],
    });

    const coMapSetKey1Message = (batchMessages[0]!.msg as BatchMessage)
      .messages[1]!;

    const coMapSetKey1MessageSealedId = Object.keys(
      coMapSetKey1Message.new,
    )[0] as SessionID;
    expect(coMapSetKey1Message.new[coMapSetKey1MessageSealedId]).toStrictEqual({
      after: 1,
      lastSignature: expect.any(String),
      newTransactions: [
        expect.objectContaining({
          privacy: "trusting",
          changes: '[{"op":"set","key":"key1","value":"value1"}]',
        }),
      ],
    });
  });

  test("emits a single batch SyncMessage for transaction mutations", async () => {
    // Set up a sync server first
    setupTestNode({ isSyncServer: true });
    const { node, connectToSyncServer } = setupTestNode();
    connectToSyncServer();

    const group = node.createGroup();
    const map = group.createMap();

    SyncMessagesLog.clear();

    await node.unstable_withTransaction(() => {
      map.set("k1", "v1", "trusting");
      map.set("k2", "v2", "trusting");
    });

    const simplified = SyncMessagesLog.getMessages({
      Group: group.core,
      Map: map.core,
    });

    expect(simplified).toMatchInlineSnapshot(`
      [
        "client -> server | BATCH [Map, Map]",
        "server -> client | KNOWN CORRECTION Map sessions: empty",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | KNOWN CORRECTION Map sessions: empty",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | LOAD Group sessions: empty",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | KNOWN Map sessions: header/2",
        "server -> client | KNOWN Map sessions: header/2",
        "server -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("mutations are applied to memory immediately", async () => {
    const node = setupTestNode().node;
    const group = node.createGroup();
    const map = group.createMap();

    await node.unstable_withTransaction(() => {
      map.set("key", "value", "trusting");
      // Value should be immediately visible in memory
      expect(map.get("key")).toBe("value");
    });

    // Value should still be visible after transaction
    expect(map.get("key")).toBe("value");
  });

  test("mutations before error are applied to memory", async () => {
    const node = setupTestNode().node;
    const group = node.createGroup();
    const map = group.createMap();

    try {
      await node.unstable_withTransaction(() => {
        map.set("key1", "value1", "trusting");
        expect(map.get("key1")).toBe("value1");

        throw new Error("Test error");

        // This line won't be reached
        // map.set("key2", "value2", "trusting");
      });
    } catch {
      // Expected error
    }

    // First mutation should be in memory
    expect(map.get("key1")).toBe("value1");
  });
});
