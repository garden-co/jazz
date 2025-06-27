import { beforeEach, describe, expect, test } from "vitest";

import { SyncMessagesLog, loadCoValueOrFail, setupTestNode } from "./testUtils";

describe("client with storage syncs with server", () => {
  let jazzCloud = setupTestNode({
    isSyncServer: true,
  });

  beforeEach(async () => {
    SyncMessagesLog.clear();
    jazzCloud = setupTestNode({
      isSyncServer: true,
    });
  });

  test("coValue loading (empty storage)", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    await client.addAsyncStorage();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | KNOWN Map sessions: empty",
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | KNOWN Group sessions: header/3",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Map sessions: header/1",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("coValue loading (synced storage)", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    const { storage } = await client.addAsyncStorage();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const firstLoad = await loadCoValueOrFail(client.node, map.id);
    await firstLoad.core.waitForSync(); // Need to wait for sync with storage

    client.restart();

    client.connectToSyncServer();
    client.addStorage({
      ourName: "client",
      storage,
    });

    SyncMessagesLog.clear();

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | LOAD Group sessions: header/3",
        "server -> client | KNOWN Group sessions: header/3",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("coValue with parent groups loading", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    await client.addAsyncStorage();

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
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | KNOWN Map sessions: empty",
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> server | KNOWN ParentGroup sessions: header/6",
        "client -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Map sessions: header/1",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("updating a coValue while offline", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    await client.addAsyncStorage();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    client.node.syncManager.getPeers()[0]?.gracefulShutdown();

    SyncMessagesLog.clear();
    map.set("hello", "updated", "trusting");

    client.connectToSyncServer();

    await map.core.waitForSync();

    expect(mapOnClient.get("hello")).toEqual("updated");

    await mapOnClient.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/3",
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/2",
        "client -> storage | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/2",
        "client -> storage | CONTENT Map header: false new: After: 1 New: 1",
      ]
    `);
  });
});

describe("client syncs with a server with storage", () => {
  let jazzCloud = setupTestNode({
    isSyncServer: true,
  });

  beforeEach(async () => {
    SyncMessagesLog.clear();
    jazzCloud = setupTestNode({
      isSyncServer: true,
    });
    jazzCloud.addStorage({
      ourName: "server",
    });
  });

  test("coValue uploading", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    const mapOnServer = await loadCoValueOrFail(jazzCloud.node, map.id);
    expect(mapOnServer.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("large coValue streaming", async () => {
    const client = setupTestNode();

    client.connectToSyncServer({
      syncServer: jazzCloud.node,
    });

    const { storage } = await client.addAsyncStorage({
      ourName: "client",
    });

    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    // Generate a large amount of data (about 100MB)
    const dataSize = 1 * 200 * 1024;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      largeMap.set(key, value, "trusting");
    }

    await largeMap.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | CONTENT Group header: true new: After: 0 New: 5",
        "client -> storage | CONTENT Map header: true new: ",
        "client -> storage | CONTENT Map header: false new: After: 0 New: 73",
        "client -> storage | CONTENT Map header: false new: After: 73 New: 73",
        "client -> storage | CONTENT Map header: false new: After: 146 New: 54",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | CONTENT Map header: true new: ",
        "server -> client | KNOWN Map sessions: header/0",
        "server -> storage | CONTENT Map header: true new: ",
        "client -> server | CONTENT Map header: false new: After: 0 New: 73",
        "server -> client | KNOWN Map sessions: header/73",
        "server -> storage | CONTENT Map header: false new: After: 0 New: 73",
        "client -> server | CONTENT Map header: false new: After: 73 New: 73",
        "server -> client | KNOWN Map sessions: header/146",
        "server -> storage | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server | CONTENT Map header: false new: After: 146 New: 54",
        "server -> client | KNOWN Map sessions: header/200",
        "server -> storage | CONTENT Map header: false new: After: 146 New: 54",
      ]
    `);

    SyncMessagesLog.clear();

    client.restart();

    client.connectToSyncServer({
      ourName: "client",
      syncServer: jazzCloud.node,
    });

    client.addStorage({
      ourName: "client",
      storage,
    });

    const mapOnClient2 = await loadCoValueOrFail(client.node, largeMap.id);

    await mapOnClient2.core.waitForSync();

    // TODO: The client should wait for the full load from the storage peer before subscribing to the core server
    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | LOAD Group sessions: header/5",
        "server -> client | KNOWN Group sessions: header/5",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 73",
        "client -> server | LOAD Map sessions: header/73",
        "server -> client | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server | KNOWN Map sessions: header/146",
        "client -> storage | CONTENT Map header: false new: After: 73 New: 73",
        "server -> client | CONTENT Map header: false new: After: 146 New: 54",
        "client -> server | CONTENT Map header: true new: ",
        "client -> storage | CONTENT Map header: false new: After: 146 New: 54",
        "server -> client | KNOWN Map sessions: header/200",
        "server -> storage | CONTENT Map header: true new: ",
        "client -> server | KNOWN Map sessions: header/200",
        "storage -> client | CONTENT Map header: true new: After: 73 New: 73",
      ]
    `);
  });
});
