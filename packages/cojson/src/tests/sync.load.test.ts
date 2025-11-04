import { beforeEach, describe, expect, test, vi } from "vitest";
import { expectMap } from "../coValue";
import {
  CO_VALUE_LOADING_CONFIG,
  setCoValueLoadingRetryDelay,
} from "../config";
import { RawCoMap } from "../exports";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  blockMessageTypeOnOutgoingPeer,
  fillCoMapWithLargeData,
  getSyncServerConnectedPeer,
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils";

let jazzCloud: ReturnType<typeof setupTestNode>;

// Set a short timeout to make the tests on unavailable complete faster
setCoValueLoadingRetryDelay(100);

beforeEach(async () => {
  // We want to simulate a real world communication that happens asynchronously
  TEST_NODE_CONFIG.withAsyncPeers = true;

  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("loading coValues from server", () => {
  test("coValue loading", async () => {
    const { node: client } = setupTestNode({
      connected: true,
    });

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/3",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("coValue load throws on invalid id", async () => {
    const { node } = setupTestNode({
      connected: true,
    });

    await expect(async () => await node.load("test" as any)).rejects.toThrow(
      "Trying to load CoValue with invalid id test",
    );
    await expect(async () => await node.load(null as any)).rejects.toThrow(
      "Trying to load CoValue with invalid id null",
    );
    await expect(async () => await node.load(undefined as any)).rejects.toThrow(
      "Trying to load CoValue with invalid id undefined",
    );
    await expect(async () => await node.load(1 as any)).rejects.toThrow(
      "Trying to load CoValue with invalid id 1",
    );
    await expect(async () => await node.load({} as any)).rejects.toThrow(
      "Trying to load CoValue with invalid id [object Object]",
    );
    await expect(async () => await node.load([] as any)).rejects.toThrow(
      "Trying to load CoValue with invalid id []",
    );
    await expect(async () => await node.load(["test"] as any)).rejects.toThrow(
      'Trying to load CoValue with invalid id ["test"]',
    );
    await expect(
      async () => await node.load((() => {}) as any),
    ).rejects.toMatchInlineSnapshot(`
      [TypeError: Trying to load CoValue with invalid id () => {
            }]
    `);
    await expect(
      async () => await node.load(new Date() as any),
    ).rejects.toThrow();
  });

  test("unavailable coValue retry with skipRetry set to true", async () => {
    const client = setupTestNode();
    const client2 = setupTestNode();

    client2.connectToSyncServer({
      ourName: "client2",
    });

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const promise = client2.node.load(map.id, true);

    await new Promise((resolve) => setTimeout(resolve, 1));

    client.connectToSyncServer();

    const mapOnClient2 = await promise;

    expect(mapOnClient2).toBe("unavailable");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client2 -> server | LOAD Map sessions: empty",
        "server -> client2 | KNOWN Map sessions: empty",
        "client -> server | LOAD Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/1",
      ]
    `);
  });

  test("new dependencies coming from updates should be pushed", async () => {
    const client = setupTestNode({
      connected: true,
    });
    const client2 = setupTestNode();

    client2.connectToSyncServer({
      ourName: "client2",
    });

    const group = client.node.createGroup();
    group.addMember("everyone", "reader");
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    await loadCoValueOrFail(client2.node, map.id);

    const parentGroup = client.node.createGroup();
    group.extend(parentGroup);

    await group.core.waitForSync();
    await jazzCloud.node.syncManager.waitForAllCoValuesSync();

    const messagesLog = SyncMessagesLog.getMessages({
      ParentGroup: parentGroup.core,
      Group: group.core,
      Map: map.core,
    });

    expect(messagesLog).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> client | KNOWN Map sessions: header/1",
        "client2 -> server | LOAD Map sessions: empty",
        "server -> client2 | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client2 | CONTENT Map header: true new: After: 0 New: 1",
        "client2 -> server | KNOWN Group sessions: header/5",
        "client2 -> server | KNOWN Map sessions: header/1",
        "client -> server | CONTENT ParentGroup header: true new: After: 0 New: 3",
        "client -> server | CONTENT Group header: false new: After: 5 New: 2",
        "server -> client | KNOWN ParentGroup sessions: header/3",
        "server -> client | KNOWN Group sessions: header/7",
        "server -> client2 | CONTENT ParentGroup header: true new: After: 0 New: 3",
        "server -> client2 | CONTENT Group header: false new: After: 5 New: 2",
        "client2 -> server | KNOWN ParentGroup sessions: header/3",
        "client2 -> server | KNOWN Group sessions: header/7",
      ]
    `);

    expect(client2.node.expectCoValueLoaded(parentGroup.id)).toBeTruthy();
  });

  test("loading a branch", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const map = group.createMap();
    const branchName = "feature-branch";

    map.set("key1", "value1");
    map.set("key2", "value2");

    const branch = await jazzCloud.node.checkoutBranch(map.id, branchName);

    if (branch === "unavailable") {
      throw new Error("Branch is unavailable");
    }

    branch.set("branchKey", "branchValue");

    SyncMessagesLog.clear();

    const loadedBranch = await loadCoValueOrFail(client.node, branch.id);

    expect(branch.get("key1")).toBe("value1");
    expect(branch.get("key2")).toBe("value2");
    expect(branch.get("branchKey")).toBe("branchValue");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
        Branch: branch.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Branch sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 3",
        "server -> client | CONTENT Branch header: true new: After: 0 New: 2",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/3",
        "client -> server | KNOWN Branch sessions: header/2",
      ]
    `);
  });

  test("unavailable coValue retry with skipRetry set to false", async () => {
    const client = setupTestNode();
    const client2 = setupTestNode();

    client2.connectToSyncServer({
      ourName: "client2",
    });

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const promise = loadCoValueOrFail(client2.node, map.id, false);

    await new Promise((resolve) => setTimeout(resolve, 1));

    client.connectToSyncServer();

    const mapOnClient2 = await promise;

    expect(mapOnClient2.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client2 -> server | LOAD Map sessions: empty",
        "server -> client2 | KNOWN Map sessions: empty",
        "client -> server | LOAD Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> client2 | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client2 | CONTENT Map header: true new: After: 0 New: 1",
        "client2 -> server | KNOWN Group sessions: header/3",
        "client2 -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("unavailable coValue retry", async () => {
    const client = setupTestNode();
    const client2 = setupTestNode();

    client2.connectToSyncServer({
      ourName: "client2",
    });

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const promise = loadCoValueOrFail(client2.node, map.id);

    await new Promise((resolve) => setTimeout(resolve, 1));

    client.connectToSyncServer();

    const mapOnClient2 = await promise;

    expect(mapOnClient2.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client2 -> server | LOAD Map sessions: empty",
        "server -> client2 | KNOWN Map sessions: empty",
        "client -> server | LOAD Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> client2 | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client2 | CONTENT Map header: true new: After: 0 New: 1",
        "client2 -> server | KNOWN Group sessions: header/3",
        "client2 -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("coValue with parent groups loading", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = jazzCloud.node.createGroup();
    const parentGroup = jazzCloud.node.createGroup();
    parentGroup.addMember("everyone", "reader");

    group.extend(parentGroup);

    const map = group.createMap();
    map.set("hello", "world");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        ParentGroup: parentGroup.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT ParentGroup header: true new: After: 0 New: 5",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN ParentGroup sessions: header/5",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("updating a coValue while offline", async () => {
    const client = setupTestNode({
      connected: false,
    });

    const { peerState } = client.connectToSyncServer();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    peerState.gracefulShutdown();

    map.set("hello", "updated", "trusting");

    SyncMessagesLog.clear();
    client.connectToSyncServer();

    await map.core.waitForSync();

    expect(mapOnClient.get("hello")).toEqual("updated");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/2",
        "client -> server | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("updating a coValue on both sides while offline", async () => {
    const client = setupTestNode({});

    const { peerState } = client.connectToSyncServer();

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);

    peerState.gracefulShutdown();

    map.set("fromServer", "updated", "trusting");
    mapOnClient.set("fromClient", "updated", "trusting");

    SyncMessagesLog.clear();
    client.connectToSyncServer();

    await map.core.waitForSync();
    await mapOnClient.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/5",
        "client -> server | LOAD Map sessions: header/2",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | CONTENT Map header: false new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/3",
        "server -> client | KNOWN Map sessions: header/3",
        "client -> server | KNOWN Map sessions: header/3",
      ]
    `);

    expect(mapOnClient.get("fromServer")).toEqual("updated");
    expect(mapOnClient.get("fromClient")).toEqual("updated");
  });

  test("wrong optimistic known state should be corrected", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    // Load the coValue on the client
    await loadCoValueOrFail(client.node, map.id);

    // Forcefully delete the coValue from the client (simulating some data loss)
    client.node.internalDeleteCoValue(map.id);

    map.set("fromServer", "updated", "trusting");

    await waitFor(() => {
      const coValue = expectMap(
        client.node.expectCoValueLoaded(map.id).getCurrentContent(),
      );
      expect(coValue.get("fromServer")).toEqual("updated");
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN CORRECTION Map sessions: empty",
        "server -> client | CONTENT Map header: true new: After: 0 New: 2",
        "client -> server | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("unavailable coValue", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    // Makes the CoValues unavailable on the server
    jazzCloud.restart();

    const client = setupTestNode({
      connected: true,
    });

    // Load the coValue on the client
    const value = await client.node.load(map.id);
    expect(value).toEqual("unavailable");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | LOAD Map sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
      ]
    `);
  });

  test("large coValue streaming", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    fillCoMapWithLargeData(largeMap);

    const client = setupTestNode({
      connected: true,
    });

    const mapOnClient = await loadCoValueOrFail(client.node, largeMap.id);

    await mapOnClient.core.waitForFullStreaming();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 73 expectContentUntil: header/200",
        "server -> client | CONTENT Map header: false new: After: 73 New: 73",
        "server -> client | CONTENT Map header: false new: After: 146 New: 54",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/73",
        "client -> server | KNOWN Map sessions: header/146",
        "client -> server | KNOWN Map sessions: header/200",
      ]
    `);
  });

  test("streaming a large update", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    await largeMap.core.waitForSync();

    const client = setupTestNode({
      connected: true,
    });
    const mapOnClient = await loadCoValueOrFail(client.node, largeMap.id);

    // Generate a large amount of data (about 100MB)
    fillCoMapWithLargeData(largeMap);

    await waitFor(() => {
      expect(mapOnClient.core.knownState()).toEqual(largeMap.core.knownState());
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: ",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/0",
        "server -> client | CONTENT Map header: false new: After: 0 New: 73 expectContentUntil: header/200",
        "server -> client | CONTENT Map header: false new: After: 73 New: 73",
        "server -> client | CONTENT Map header: false new: After: 146 New: 54",
        "client -> server | KNOWN Map sessions: header/73",
        "client -> server | KNOWN Map sessions: header/146",
        "client -> server | KNOWN Map sessions: header/200",
      ]
    `);
  });

  test("should wait for a persistent peer to reconnect before marking the coValue as unavailable", async () => {
    const client = setupTestNode();
    const connection1 = client.connectToSyncServer({
      persistent: true,
    });
    connection1.peerState.gracefulShutdown();

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      test: "value",
    });
    const promise = client.node.load(map.id);

    await new Promise((resolve) => setTimeout(resolve, 10));

    client.connectToSyncServer();

    const coValue = await promise;

    expect(coValue).not.toBe("unavailable");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("should handle reconnections in the middle of a load with a persistent peer", async () => {
    TEST_NODE_CONFIG.withAsyncPeers = false; // To avoid flakiness

    const client = setupTestNode();
    const connection1 = client.connectToSyncServer({
      persistent: true,
    });

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      test: "value",
    });

    blockMessageTypeOnOutgoingPeer(connection1.peerOnServer, "content", {
      id: map.id,
      once: true,
    });

    const promise = client.node.load(map.id);

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Close the peer connection
    connection1.peerState.gracefulShutdown();

    client.connectToSyncServer();

    const coValue = await promise;

    expect(coValue).not.toBe("unavailable");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Group sessions: header/5",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Group sessions: header/5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Map sessions: header/1",
        "client -> server | LOAD Group sessions: header/5",
        "server -> client | KNOWN Group sessions: header/5",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("coValue with a delayed group loading", async () => {
    const client = setupTestNode();

    const { peerOnServer } = client.connectToSyncServer();

    const group = jazzCloud.node.createGroup();
    const parentGroup = jazzCloud.node.createGroup();
    parentGroup.addMember("everyone", "reader");

    const blocker = blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {
      id: group.id,
      once: true,
    });

    group.extend(parentGroup);

    const map = group.createMap();
    map.set("hello", "world");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        ParentGroup: parentGroup.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT ParentGroup header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN ParentGroup sessions: header/5",
        "client -> server | LOAD Group sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);

    blocker.unblock();
  });

  test("coValue with a delayed parent group loading", async () => {
    const client = setupTestNode();

    const { peerOnServer } = client.connectToSyncServer();

    const group = jazzCloud.node.createGroup();
    const parentGroup = jazzCloud.node.createGroup();
    parentGroup.addMember("everyone", "reader");

    const blocker = blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {
      id: parentGroup.id,
      once: true,
    });

    group.extend(parentGroup);

    const map = group.createMap();
    map.set("hello", "world");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        ParentGroup: parentGroup.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | LOAD ParentGroup sessions: empty",
        "client -> server | LOAD Group sessions: empty",
        "server -> client | CONTENT ParentGroup header: true new: After: 0 New: 5",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN ParentGroup sessions: header/5",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);

    blocker.unblock();
  });

  test("coValue with a delayed account loading (block once)", async () => {
    const client = setupTestNode();
    const syncServer = await setupTestAccount({ isSyncServer: true });

    const { peerOnServer } = client.connectToSyncServer({
      syncServer: syncServer.node,
    });

    const group = syncServer.node.createGroup();
    group.addMember("everyone", "writer");
    const blocker = blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {
      id: syncServer.accountID,
      once: true,
    });

    const account = syncServer.node.expectCurrentAccount(syncServer.accountID);

    const map = group.createMap();
    map.set("hello", "world");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    // ParentGroup sent twice, once because the server pushed it and another time because the client requested the missing dependency
    expect(
      SyncMessagesLog.getMessages({
        Account: account.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | LOAD Account sessions: empty",
        "client -> server | LOAD Group sessions: empty",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Account sessions: header/4",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);

    blocker.unblock();
  });

  test("coValue with a delayed account loading related to an update (block once)", async () => {
    const client = setupTestNode();
    const user = await setupTestAccount({
      connected: true,
    });

    const { peerOnServer } = client.connectToSyncServer();

    const account = user.node.expectCurrentAccount(user.accountID);
    await user.node.syncManager.waitForAllCoValuesSync();

    SyncMessagesLog.clear();

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const blocker = blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {
      id: user.accountID,
      once: true,
    });

    const map = group.createMap();
    map.set("hello", "world");

    const mapOnUser = await loadCoValueOrFail(user.node, map.id);
    mapOnUser.set("user", true);

    await mapOnUser.core.waitForSync();

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("user")).toEqual(true);

    // ParentGroup sent twice, once because the server pushed it and another time because the client requested the missing dependency
    expect(
      SyncMessagesLog.getMessages({
        Account: account.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
        "client -> server | CONTENT Map header: false new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/2",
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | LOAD Account sessions: empty",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
        "client -> server | KNOWN Account sessions: header/4",
        "client -> server | KNOWN Map sessions: header/2",
      ]
    `);

    blocker.unblock();
  });

  test("coValue with a delayed account loading (block for 100ms)", async () => {
    const client = setupTestNode();
    const syncServer = await setupTestAccount({ isSyncServer: true });

    const { peerOnServer } = client.connectToSyncServer({
      syncServer: syncServer.node,
    });

    const group = syncServer.node.createGroup();
    group.addMember("everyone", "writer");

    const blocker = blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {
      id: syncServer.accountID,
    });

    const account = syncServer.node.expectCurrentAccount(syncServer.accountID);

    const map = group.createMap();
    map.set("hello", "world");

    const core = client.node.getCoValue(map.id);
    const promise = client.node.loadCoValueCore(map.id);

    const spy = vi.fn();

    core.subscribe(spy);
    spy.mockClear(); // Reset the first call

    await new Promise((resolve) => setTimeout(resolve, 100));

    blocker.sendBlockedMessages();
    blocker.unblock();

    await promise;

    expect(spy).toHaveBeenCalled();
    expect(core.isAvailable()).toBe(true);

    const mapOnClient = expectMap(core.getCurrentContent());
    expect(mapOnClient.get("hello")).toEqual("world");

    // Account sent twice, once because the server pushed it and another time because the client requested the missing dependency
    expect(
      SyncMessagesLog.getMessages({
        Account: account.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | LOAD Account sessions: empty",
        "client -> server | LOAD Group sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
        "client -> server | KNOWN Account sessions: header/4",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("coValue with a delayed account loading with no group (block for 100ms)", async () => {
    const client = setupTestNode();
    const syncServer = await setupTestAccount({ isSyncServer: true });

    const { peerOnServer } = client.connectToSyncServer({
      syncServer: syncServer.node,
    });

    const blocker = blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {
      id: syncServer.accountID,
    });

    const account = syncServer.node.expectCurrentAccount(syncServer.accountID);

    const map = syncServer.node
      .createCoValue({
        type: "comap",
        ruleset: {
          type: "ownedByGroup",
          group: syncServer.accountID,
        },
        meta: null,
        ...syncServer.node.crypto.createdNowUnique(),
      })
      .getCurrentContent() as RawCoMap;

    map.set("hello", "world", "trusting");

    const core = client.node.getCoValue(map.id);
    const promise = client.node.loadCoValueCore(map.id);

    const spy = vi.fn();

    core.subscribe(spy);
    spy.mockClear(); // Reset the first call

    await new Promise((resolve) => setTimeout(resolve, 100));

    blocker.sendBlockedMessages();
    blocker.unblock();

    await promise;

    expect(spy).toHaveBeenCalled();
    expect(core.isAvailable()).toBe(true);

    const mapOnClient = expectMap(core.getCurrentContent());
    expect(mapOnClient.get("hello")).toEqual("world");

    // Account sent twice, once because the server pushed it and another time because the client requested the missing dependency
    expect(
      SyncMessagesLog.getMessages({
        Account: account.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | LOAD Account sessions: empty",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
        "client -> server | KNOWN Account sessions: header/4",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("edge servers should wait for all the dependencies to be available before sending the content", async () => {
    const coreServer = await setupTestAccount();
    coreServer.node.syncManager.disableTransactionVerification();

    const edgeServer = await setupTestNode({ isSyncServer: true });

    const { peerOnServer } = edgeServer.connectToSyncServer({
      syncServer: coreServer.node,
      ourName: "edge",
      syncServerName: "core",
    });

    const client = setupTestNode();

    client.connectToSyncServer({
      ourName: "client",
      syncServerName: "edge",
      syncServer: edgeServer.node,
    });

    const group = coreServer.node.createGroup();
    group.addMember("everyone", "writer");
    const accountBlocker = blockMessageTypeOnOutgoingPeer(
      peerOnServer,
      "content",
      {
        id: coreServer.accountID,
        once: true,
      },
    );
    const groupBlocker = blockMessageTypeOnOutgoingPeer(
      peerOnServer,
      "content",
      {
        id: group.id,
        once: true,
      },
    );

    const account = coreServer.node.expectCurrentAccount(coreServer.accountID);

    const map = group.createMap();
    map.set("hello", "world");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Account: account.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> edge | LOAD Map sessions: empty",
        "edge -> core | LOAD Map sessions: empty",
        "core -> edge | CONTENT Map header: true new: After: 0 New: 1",
        "edge -> core | LOAD Group sessions: empty",
        "edge -> core | LOAD Account sessions: empty",
        "core -> edge | CONTENT Group header: true new: After: 0 New: 5",
        "core -> edge | CONTENT Account header: true new: After: 0 New: 4",
        "edge -> core | KNOWN Account sessions: header/4",
        "edge -> core | KNOWN Group sessions: header/5",
        "edge -> core | KNOWN Map sessions: header/1",
        "edge -> client | CONTENT Account header: true new: After: 0 New: 4",
        "edge -> client | CONTENT Group header: true new: After: 0 New: 5",
        "edge -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge | KNOWN Account sessions: header/4",
        "client -> edge | KNOWN Group sessions: header/5",
        "client -> edge | KNOWN Map sessions: header/1",
      ]
    `);

    accountBlocker.unblock();
    groupBlocker.unblock();
  });

  test("edge servers should wait for all the dependencies to be available before sending an update", async () => {
    // Create a core -> edge -> client network
    const coreServer = setupTestNode({ isSyncServer: true });

    const edgeServer = setupTestNode({ isSyncServer: true });
    const { peerOnServer: coreToEdgePeer } = edgeServer.connectToSyncServer({
      syncServer: coreServer.node,
      ourName: "edge",
      syncServerName: "core",
    });

    const client = setupTestNode();
    client.connectToSyncServer({
      ourName: "client",
      syncServerName: "edge",
    });

    // Create the group on the client
    const group = client.node.createGroup();

    // Connect a new session and link it directly to the core
    const newSession = client.spawnNewSession();
    newSession.connectToSyncServer({
      syncServer: coreServer.node,
      ourName: "newSession",
      syncServerName: "core",
    });

    // Load the group on the new client
    const groupOnNewSession = await loadCoValueOrFail(
      newSession.node,
      group.id,
    );

    SyncMessagesLog.clear();

    const parentGroup = newSession.node.createGroup();
    groupOnNewSession.extend(parentGroup);

    // Block the content message from the core peer to simulate the situation where we won't push the dependency
    const blocker = blockMessageTypeOnOutgoingPeer(coreToEdgePeer, "content", {
      id: parentGroup.id,
      once: true,
    });

    // Wait for the parent group to be available on client
    await waitFor(() => {
      expect(client.node.getCoValue(parentGroup.id).isAvailable()).toBe(true);
    });

    // The edge server should wait for the parent group to be available before sending the group update
    expect(
      SyncMessagesLog.getMessages({
        ParentGroup: parentGroup.core,
        Group: group.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "newSession -> core | CONTENT ParentGroup header: true new: After: 0 New: 3",
        "newSession -> core | CONTENT Group header: false new: After: 0 New: 2",
        "core -> newSession | KNOWN ParentGroup sessions: header/3",
        "core -> newSession | KNOWN Group sessions: header/5",
        "core -> edge | CONTENT Group header: false new: After: 0 New: 2",
        "edge -> core | LOAD ParentGroup sessions: empty",
        "core -> edge | CONTENT ParentGroup header: true new: After: 0 New: 3",
        "edge -> core | KNOWN ParentGroup sessions: header/3",
        "edge -> core | KNOWN Group sessions: header/5",
        "edge -> client | CONTENT ParentGroup header: true new: After: 0 New: 3",
        "edge -> client | CONTENT Group header: false new: After: 0 New: 2",
        "client -> edge | KNOWN ParentGroup sessions: header/3",
        "client -> edge | KNOWN Group sessions: header/5",
      ]
    `);

    blocker.unblock();
  });

  test("edge servers should wait for all the session dependencies to be available before sending an update", async () => {
    // Create a core -> edge -> client network
    const coreServer = setupTestNode({ isSyncServer: true });

    const edgeServer = setupTestNode({ isSyncServer: true });
    const { peerOnServer: coreToEdgePeer } = edgeServer.connectToSyncServer({
      syncServer: coreServer.node,
      ourName: "edge",
      syncServerName: "core",
    });

    const client = setupTestNode();
    client.connectToSyncServer({
      ourName: "client",
      syncServerName: "edge",
    });

    // Create the map on the client
    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap();
    map.set("hello", "world");

    // Connect a new client that uses an account and link it directly to the core
    const newAccountClient = await setupTestAccount();
    newAccountClient.connectToSyncServer({
      syncServer: coreServer.node,
      ourName: "newAccountClient",
      syncServerName: "core",
    });
    await newAccountClient.node.syncManager.waitForAllCoValuesSync();

    // Load the map on the new client
    const mapOnNewAccountClient = await loadCoValueOrFail(
      newAccountClient.node,
      map.id,
    );
    const account = newAccountClient.node.expectCurrentAccount(
      newAccountClient.accountID,
    );

    SyncMessagesLog.clear();

    // Update the map on the new client, creating a new session
    mapOnNewAccountClient.set("newAccountClient", true);

    // Block the content message from the core peer to simulate the situation where we won't push the dependency
    const blocker = blockMessageTypeOnOutgoingPeer(coreToEdgePeer, "content", {
      id: account.id,
      once: true,
    });

    // Wait for the update to arrive on the initial client
    await waitFor(() => {
      const mapOnClient = expectMap(
        client.node.getCoValue(map.core.id).getCurrentContent(),
      );
      expect(mapOnClient.get("newAccountClient")).toBe(true);
    });

    // The edge server should wait for the new Account to be available before sending the Map update
    expect(
      SyncMessagesLog.getMessages({
        Account: account.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "newAccountClient -> core | CONTENT Map header: false new: After: 0 New: 1",
        "core -> newAccountClient | KNOWN Map sessions: header/2",
        "core -> edge | CONTENT Map header: false new: After: 0 New: 1",
        "edge -> core | LOAD Account sessions: empty",
        "core -> edge | CONTENT Account header: true new: After: 0 New: 4",
        "edge -> core | KNOWN Account sessions: header/4",
        "edge -> core | KNOWN Map sessions: header/2",
        "edge -> client | CONTENT Account header: true new: After: 0 New: 4",
        "edge -> client | CONTENT Map header: false new: After: 0 New: 1",
        "client -> edge | KNOWN Account sessions: header/4",
        "client -> edge | KNOWN Map sessions: header/2",
      ]
    `);

    blocker.unblock();
  });

  test("coValue with circular deps loading", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const parentGroup = client.node.createGroup();
    parentGroup.addMember("everyone", "reader");

    group.extend(parentGroup);

    // Disable the circular dependency check in the extend function
    vi.spyOn(parentGroup, "isSelfExtension").mockImplementation(() => false);

    parentGroup.extend(group);

    const map = group.createMap();
    map.set("hello", "world");

    await map.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        ParentGroup: parentGroup.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Group header: true new: After: 0 New: 3 expectContentUntil: header/5",
        "client -> server | CONTENT ParentGroup header: true new: After: 0 New: 5 expectContentUntil: header/7",
        "client -> server | CONTENT Group header: false new: After: 3 New: 2",
        "client -> server | CONTENT ParentGroup header: false new: After: 5 New: 2",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | KNOWN ParentGroup sessions: header/5",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> client | KNOWN ParentGroup sessions: header/7",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("should retry loading from a closed persistent peer after a timeout", async () => {
    vi.useFakeTimers();

    const client = setupTestNode();

    const connection1 = client.connectToSyncServer({
      persistent: true,
    });

    // Close the peer connection
    connection1.peerState.gracefulShutdown();

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "reader");

    const map = group.createMap();
    map.set("hello", "world");

    const promise = loadCoValueOrFail(client.node, map.id);

    await vi.advanceTimersByTimeAsync(
      CO_VALUE_LOADING_CONFIG.TIMEOUT +
        CO_VALUE_LOADING_CONFIG.RETRY_DELAY +
        10,
    );

    client.connectToSyncServer({
      persistent: true,
    });

    await vi.advanceTimersByTimeAsync(
      CO_VALUE_LOADING_CONFIG.TIMEOUT +
        CO_VALUE_LOADING_CONFIG.RETRY_DELAY +
        10,
    );

    const coValue = await promise;

    expect(coValue).not.toBe("unavailable");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);

    vi.useRealTimers();
  });

  test("should not request dependencies if transaction verification is disabled", async () => {
    // Create a disconnected client
    const { node: client, accountID } = await setupTestAccount({
      connected: false,
    });
    const account = client.expectCurrentAccount(accountID);

    // Prepare a group -- this will be a non-account dependency of a forthcoming map.
    const group = client.createGroup();
    group.addMember("everyone", "writer");

    // Create a sync server and disable transaction verification
    const syncServer = await setupTestAccount({ isSyncServer: true });
    syncServer.node.syncManager.disableTransactionVerification();

    // Connect the client, but don't setup syncing just yet...
    const { peer } = getSyncServerConnectedPeer({
      peerId: client.getCurrentAgent().id,
      syncServer: syncServer.node,
    });

    // Disable reconciliation while we setup syncing because we don't want the
    // server to know about our forthcoming map's dependencies (group + account).
    const blocker = blockMessageTypeOnOutgoingPeer(peer, "load", {});
    client.syncManager.addPeer(peer);
    blocker.unblock();

    // Create a map and set a value on it.
    // If transaction verification were enabled, this would trigger LOAD messages
    // from the server to the client asking for the group and account. However, we
    // don't expect to see those messages since we disabled transaction verification.
    const map = group.createMap();
    map.set("hello", "world");
    await map.core.waitForSync();

    const syncMessages = SyncMessagesLog.getMessages({
      Account: account.core,
      Group: group.core,
      Map: map.core,
    });
    expect(
      syncMessages.some(
        (msg) => msg.includes("LOAD Account") || msg.includes("LOAD Group"),
      ),
    ).toBe(false);

    // Verify the map is available on the server (transaction was accepted)
    const mapOnServerCore = await syncServer.node.loadCoValueCore(map.core.id);
    expect(mapOnServerCore.isAvailable()).toBe(true);
  });

  test("unknown coValues are ignored if ignoreUnrequestedCoValues is true", async () => {
    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const { node: shardedCoreNode } = setupTestNode({
      connected: true,
    });
    shardedCoreNode.syncManager.disableTransactionVerification();
    shardedCoreNode.syncManager.ignoreUnknownCoValuesFromServers();

    await shardedCoreNode.loadCoValueCore(map.id);

    expect(shardedCoreNode.hasCoValue(map.id)).toBe(true);
    expect(shardedCoreNode.hasCoValue(group.id)).toBe(false);
  });
});
