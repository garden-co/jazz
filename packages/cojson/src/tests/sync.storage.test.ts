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
    client.addStoragePeer();

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
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | KNOWN Map sessions: header/1",
        "storage -> client | KNOWN Group sessions: header/3",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "storage -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("coValue loading (synced storage)", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    const { storage } = client.addStoragePeer();

    storage.coValuesStore = jazzCloud.node.coValuesStore;

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
        "storage -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> storage | KNOWN Group sessions: header/3",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "client -> storage | KNOWN Map sessions: header/1",
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/1",
        "client -> server | KNOWN Group sessions: header/3",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("coValue with parent groups loading", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    client.addStoragePeer();

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
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> server | KNOWN Group sessions: header/5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "storage -> client | KNOWN ParentGroup sessions: header/6",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Map sessions: header/1",
        "storage -> client | KNOWN Group sessions: header/5",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "storage -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("updating a coValue while offline", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    client.addStoragePeer();

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
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> storage | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/2",
        "storage -> client | KNOWN Map sessions: header/2",
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
    jazzCloud.addStoragePeer({
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
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | KNOWN Map sessions: header/1",
        "storage -> server | KNOWN Group sessions: header/3",
        "server -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "storage -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test.skip("server restarts", async () => {
    const client = setupTestNode();

    client.addStoragePeer();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    jazzCloud.restart();

    SyncMessagesLog.clear();
    client.addStoragePeer();

    const mapOnServer = await loadCoValueOrFail(jazzCloud.node, map.id);
    expect(mapOnServer.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "server -> storage | LOAD Group sessions: header/3",
        "storage -> server | KNOWN Map sessions: header/1",
        "storage -> server | KNOWN Group sessions: empty",
        "server -> storage | LOAD Map sessions: header/1",
        "storage -> server | KNOWN Map sessions: empty",
      ]
    `);
  });
});
