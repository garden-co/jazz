import { beforeEach, describe, expect, test, vi } from "vitest";
import {
  CO_VALUE_LOADING_CONFIG,
  setCoValueLoadingRetryDelay,
} from "../config";
import { expectMap } from "../coValue";
import { RawCoMap } from "../exports";
import {
  blockMessageTypeOnOutgoingPeer,
  loadCoValueOrFail,
  SyncMessagesLog,
  setupTestAccount,
  setupTestNode,
  TEST_NODE_CONFIG,
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
        "server -> client | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN ParentGroup sessions: header/6",
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

    // Generate a large amount of data (about 100MB)
    const dataSize = 1 * 1024 * 1024;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      largeMap.set(key, value, "trusting");
    }

    const client = setupTestNode({
      connected: true,
    });

    await loadCoValueOrFail(client.node, largeMap.id);

    await largeMap.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new:  expectContentUntil: header/1024",
        "server -> client | CONTENT Map header: false new: After: 0 New: 73",
        "server -> client | CONTENT Map header: false new: After: 73 New: 73",
        "server -> client | CONTENT Map header: false new: After: 146 New: 73",
        "server -> client | CONTENT Map header: false new: After: 219 New: 73",
        "server -> client | CONTENT Map header: false new: After: 292 New: 73",
        "server -> client | CONTENT Map header: false new: After: 365 New: 73",
        "server -> client | CONTENT Map header: false new: After: 438 New: 73",
        "server -> client | CONTENT Map header: false new: After: 511 New: 73",
        "server -> client | CONTENT Map header: false new: After: 584 New: 73",
        "server -> client | CONTENT Map header: false new: After: 657 New: 73",
        "server -> client | CONTENT Map header: false new: After: 730 New: 73",
        "server -> client | CONTENT Map header: false new: After: 803 New: 73",
        "server -> client | CONTENT Map header: false new: After: 876 New: 73",
        "server -> client | CONTENT Map header: false new: After: 949 New: 73",
        "server -> client | CONTENT Map header: false new: After: 1022 New: 2",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/0",
        "client -> server | KNOWN Map sessions: header/73",
        "client -> server | KNOWN Map sessions: header/146",
        "client -> server | KNOWN Map sessions: header/219",
        "client -> server | KNOWN Map sessions: header/292",
        "client -> server | KNOWN Map sessions: header/365",
        "client -> server | KNOWN Map sessions: header/438",
        "client -> server | KNOWN Map sessions: header/511",
        "client -> server | KNOWN Map sessions: header/584",
        "client -> server | KNOWN Map sessions: header/657",
        "client -> server | KNOWN Map sessions: header/730",
        "client -> server | KNOWN Map sessions: header/803",
        "client -> server | KNOWN Map sessions: header/876",
        "client -> server | KNOWN Map sessions: header/949",
        "client -> server | KNOWN Map sessions: header/1022",
        "client -> server | KNOWN Map sessions: header/1024",
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
        "server -> client | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN ParentGroup sessions: header/6",
        "client -> server | LOAD Group sessions: empty",
        "client -> server | KNOWN Map sessions: header/1",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Group sessions: header/5",
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
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
        "server -> client | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> server | KNOWN ParentGroup sessions: header/6",
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
        "client -> server | KNOWN Group sessions: header/0",
        "client -> server | KNOWN Map sessions: header/0",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
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
        "client -> server | KNOWN Map sessions: header/1",
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
        "client -> server | KNOWN Group sessions: header/0",
        "client -> server | KNOWN Map sessions: header/0",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
        "client -> server | KNOWN Account sessions: header/4",
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
        "client -> server | KNOWN Map sessions: header/0",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
        "server -> client | CONTENT Account header: true new: After: 0 New: 4",
        "client -> server | KNOWN Account sessions: header/4",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
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
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> server | CONTENT Group header: false new: After: 3 New: 3",
        "client -> server | CONTENT ParentGroup header: false new: After: 6 New: 2",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | KNOWN ParentGroup sessions: header/6",
        "server -> client | KNOWN Group sessions: header/6",
        "server -> client | KNOWN ParentGroup sessions: header/8",
        "server -> client | KNOWN Map sessions: header/1",
        "client -> server | CONTENT ParentGroup header: true new: ",
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
});
