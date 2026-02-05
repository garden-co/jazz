import { assert, beforeEach, describe, expect, test } from "vitest";
import { expectMap } from "../coValue";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils";
import { isDeleteSessionID, SessionID } from "../ids";

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  // We want to simulate a real world communication that happens asynchronously
  TEST_NODE_CONFIG.withAsyncPeers = true;

  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("syncing deleted coValues", () => {
  test("client loads a deleted coValue from server (tombstone-only)", async () => {
    const { node: client } = setupTestNode({ connected: true });

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    // Delete on the server before the client loads.
    map.core.deleteCoValue();
    expect(map.core.isDeleted).toBe(true);

    const mapOnClient = await loadCoValueOrFail(client, map.id);
    const mapCoreOnClient = client.expectCoValueLoaded(map.id);

    expect(mapCoreOnClient.isDeleted).toBe(true);
    // Historical content should not be synced.
    expect(mapOnClient.get("hello")).toBeUndefined();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 4",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/4",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("inbound filtering: after deletion, non-delete sessions in the same content message are ignored", async () => {
    const client = setupTestNode({ connected: false });

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("k", "v", "trusting");

    const contentBeforeDelete = map.core.newContentSince(undefined)?.[0];
    assert(contentBeforeDelete);

    // Create a delete marker on the server, but also keep a historical session around.
    map.core.deleteCoValue();

    const content = map.core.newContentSince(undefined)?.[0];
    assert(content);

    const groupContent = group.core.newContentSince(undefined)?.[0];
    assert(groupContent);

    // We merge the content before delete with the content after delete to simulate an older peer that might send extra sessions in the same message
    Object.assign(contentBeforeDelete.new, content.new);

    client.node.syncManager.handleNewContent(groupContent, "import");
    client.node.syncManager.handleNewContent(content, "import");

    const coreOnClient = client.node.expectCoValueLoaded(map.id);
    expect(coreOnClient.isDeleted).toBe(true);

    const contentOnClient = expectMap(coreOnClient.getCurrentContent());
    expect(contentOnClient.get("k")).toBeUndefined();
  });

  test("should wait for the dependencies to be available before processing the deleted session/transaction", async () => {
    const client = setupTestNode({ connected: false });

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();

    // Create a delete marker on the server, but also keep a historical session around.
    map.core.deleteCoValue();

    const content = map.core.newContentSince(undefined)?.[0];
    assert(content);

    const groupContent = group.core.newContentSince(undefined)?.[0];
    assert(groupContent);

    client.node.syncManager.handleNewContent(content, "import");
    client.node.syncManager.handleNewContent(groupContent, "import");

    await waitFor(() => {
      expect(client.node.expectCoValueLoaded(map.id).isDeleted).toBe(true);
    });
  });

  test("outbound blocking: post-delete normal writes are ignored and do not produce content uploads", async () => {
    const client = setupTestNode({ connected: true });

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const map = group.createMap();
    map.set("a", 1, "trusting");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);

    // Delete on the server and wait for it to propagate.
    map.core.deleteCoValue();
    await waitFor(() => {
      expect(mapOnClient.core.isDeleted).toBe(true);
    });

    SyncMessagesLog.clear();

    mapOnClient.set("x", "y", "trusting");

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Ensure we didn't produce outgoing content uploads as a result of the rejected write.
    const messages = SyncMessagesLog.getMessages({
      Group: group.core,
      Map: map.core,
    });
    expect(messages.some((m) => m.includes("CONTENT Map"))).toBe(false);
  });

  test("delete should be propagated to client-to-client sync", async () => {
    const alice = setupTestNode();
    alice.connectToSyncServer({
      ourName: "alice",
    });
    const bob = setupTestNode();
    bob.connectToSyncServer({
      ourName: "bob",
    });

    const group = alice.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");
    map.core.deleteCoValue();

    await loadCoValueOrFail(bob.node, map.id);

    await waitFor(() => {
      expect(bob.node.expectCoValueLoaded(map.id).isDeleted).toBe(true);
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "bob -> server | LOAD Map sessions: empty",
        "alice -> server | CONTENT Group header: true new: After: 0 New: 4",
        "alice -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> bob | KNOWN Map sessions: empty",
        "server -> alice | KNOWN Group sessions: header/4",
        "server -> alice | KNOWN Map sessions: header/1",
        "server -> bob | CONTENT Group header: true new: After: 0 New: 4",
        "server -> bob | CONTENT Map header: true new: After: 0 New: 1",
        "bob -> server | KNOWN Group sessions: header/4",
        "bob -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("content synced after deletion should be ignored", async () => {
    const alice = setupTestNode({ connected: true });
    const bob = setupTestNode();

    const { peerState: bobConnection } = bob.connectToSyncServer({
      ourName: "bob",
    });

    const group = alice.node.createGroup();
    group.addMember("everyone", "writer");
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnBob = await loadCoValueOrFail(bob.node, map.id);

    SyncMessagesLog.clear();

    bobConnection.gracefulShutdown();

    map.core.deleteCoValue();

    await map.core.waitForSync();

    mapOnBob.set("hello", "updated", "trusting");

    bob.connectToSyncServer({
      ourName: "bob",
    });

    await mapOnBob.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Map header: false new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/2",
        "server -> bob | CONTENT Map header: false new: After: 0 New: 1",
        "bob -> server | LOAD Group sessions: header/6",
        "bob -> server | LOAD Map sessions: header/2",
        "bob -> server | CONTENT Map header: false new: After: 0 New: 1",
        "server -> bob | KNOWN Group sessions: header/6",
        "server -> bob | CONTENT Map header: false new: After: 0 New: 1",
        "server -> bob | KNOWN Map sessions: header/3",
        "bob -> server | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("should handle concurrent delete operations", async () => {
    const alice = await setupTestAccount();
    alice.connectToSyncServer({
      ourName: "alice",
    });
    const bob = await setupTestAccount();
    bob.connectToSyncServer({
      ourName: "bob",
    });

    const group = jazzCloud.node.createGroup();
    group.addMemberInternal(alice.account, "admin");
    group.addMemberInternal(bob.account, "admin");

    const map = group.createMap();
    map.set("counter", 0, "trusting");
    map.set("counter", 1, "trusting");

    const mapOnAlice = await loadCoValueOrFail(alice.node, map.id);
    const mapOnBob = await loadCoValueOrFail(bob.node, map.id);

    SyncMessagesLog.clear();

    mapOnAlice.core.deleteCoValue();
    mapOnBob.core.deleteCoValue();

    await mapOnAlice.core.waitForSync();
    await mapOnBob.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "alice -> server | CONTENT Map header: false new: After: 0 New: 1",
        "bob -> server | CONTENT Map header: false new: After: 0 New: 1",
        "server -> alice | KNOWN Map sessions: header/3",
        "server -> bob | CONTENT Map header: false new: After: 0 New: 1",
        "server -> bob | KNOWN Map sessions: header/4",
        "server -> alice | CONTENT Map header: false new: After: 0 New: 1",
        "bob -> server | KNOWN Map sessions: header/4",
      ]
    `);

    expect(map.core.isDeleted).toBe(true);

    const sessions = map.core.knownState().sessions;

    expect(Object.keys(sessions)).toHaveLength(2);
    expect(
      Object.keys(sessions).every((sessionID) =>
        isDeleteSessionID(sessionID as SessionID),
      ),
    ).toBe(true);
  });
});
