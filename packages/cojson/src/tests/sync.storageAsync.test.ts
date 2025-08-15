import { assert, beforeEach, describe, expect, test, vi } from "vitest";

import { setMaxRecommendedTxSize } from "../config";
import { emptyKnownState } from "../exports";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils";
import { getDbPath } from "./testStorage";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

describe("client with storage syncs with server", () => {
  let jazzCloud: ReturnType<typeof setupTestNode>;

  beforeEach(async () => {
    vi.resetAllMocks();
    setMaxRecommendedTxSize(100 * 1024);
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
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/3",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 3",
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
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | LOAD Map sessions: header/1",
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
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN ParentGroup sessions: header/6",
        "client -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 5",
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
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/2",
        "client -> storage | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("the order of updates between CoValues should be preserved to ensure consistency in case of shutdown in the middle of sync", async () => {
    const client = setupTestNode();

    await client.addAsyncStorage();

    const group = client.node.createGroup();
    const initialMap = group.createMap();

    const child = group.createMap();
    child.set("parent", initialMap.id);
    initialMap.set("child", child.id);

    await initialMap.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        InitialMap: initialMap.core,
        ChildMap: child.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "client -> storage | CONTENT InitialMap header: true new: ",
        "client -> storage | CONTENT ChildMap header: true new: After: 0 New: 1",
        "client -> storage | CONTENT InitialMap header: false new: After: 0 New: 1",
      ]
    `);
  });
});

describe("client syncs with a server with storage", () => {
  let jazzCloud: ReturnType<typeof setupTestNode>;

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
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("large coValue streaming", async () => {
    setMaxRecommendedTxSize(1000);
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
    const chunks = 100;

    const value = "1".repeat(10);

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
        "client -> storage | CONTENT Map header: true new: After: 0 New: 20",
        "client -> server | CONTENT Map header: true new: After: 0 New: 20",
        "client -> storage | CONTENT Map header: false new: After: 20 New: 21",
        "client -> server | CONTENT Map header: false new: After: 20 New: 21",
        "client -> storage | CONTENT Map header: false new: After: 41 New: 21",
        "client -> server | CONTENT Map header: false new: After: 41 New: 21",
        "client -> storage | CONTENT Map header: false new: After: 62 New: 21",
        "client -> server | CONTENT Map header: false new: After: 62 New: 21",
        "client -> storage | CONTENT Map header: false new: After: 83 New: 17",
        "client -> server | CONTENT Map header: false new: After: 83 New: 17",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | KNOWN Map sessions: header/20",
        "server -> storage | CONTENT Map header: true new: After: 0 New: 20",
        "server -> client | KNOWN Map sessions: header/41",
        "server -> storage | CONTENT Map header: false new: After: 20 New: 21",
        "server -> client | KNOWN Map sessions: header/62",
        "server -> storage | CONTENT Map header: false new: After: 41 New: 21",
        "server -> client | KNOWN Map sessions: header/83",
        "server -> storage | CONTENT Map header: false new: After: 62 New: 21",
        "server -> client | KNOWN Map sessions: header/100",
        "server -> storage | CONTENT Map header: false new: After: 83 New: 17",
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
        "storage -> client | CONTENT Map header: true new: After: 0 New: 41 expectContentUntil: header/100",
        "client -> server | LOAD Map sessions: header/100",
        "storage -> client | CONTENT Map header: true new: After: 41 New: 21",
        "storage -> client | CONTENT Map header: true new: After: 62 New: 21",
        "storage -> client | CONTENT Map header: true new: After: 83 New: 17",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> client | KNOWN Map sessions: header/100",
      ]
    `);
  });

  test("storing stale data should not compromise the signatures", async () => {
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

    const newContentChunks = largeMap.core.verified.newContentSince(
      emptyKnownState(largeMap.id),
    );

    assert(newContentChunks);
    assert(newContentChunks.length > 1);

    const correctionSpy = vi.fn();

    client.node.storage?.store(newContentChunks[1]!, correctionSpy);

    // Wait for the content to be stored in the storage
    // We can't use waitForSync because we are trying to store stale data
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(correctionSpy).not.toHaveBeenCalled();

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

    await waitFor(async () => {
      expect(mapOnClient2.core.knownState()).toEqual(
        largeMap.core.knownState(),
      );
    });
  });

  test("large coValue streaming from cold server", async () => {
    const server = setupTestNode({
      isSyncServer: true,
    });
    const { storage: serverStorage } = server.addStorage({
      ourName: "server",
    });

    const client = setupTestNode();

    client.connectToSyncServer({
      syncServer: server.node,
    });

    const { storage } = await client.addAsyncStorage({
      ourName: "client",
    });

    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    // Generate a large amount of data
    const dataSize = 1 * 10 * 1024;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      largeMap.set(key, value, "trusting");
    }

    await largeMap.core.waitForSync();

    SyncMessagesLog.clear();

    server.restart();

    server.addStorage({
      ourName: "server",
      storage: serverStorage,
    });

    client.restart();

    client.connectToSyncServer({
      ourName: "client",
      syncServer: server.node,
    });

    client.addStorage({
      ourName: "client",
      storage,
    });

    const mapOnClient2 = await loadCoValueOrFail(client.node, largeMap.id);

    await mapOnClient2.core.waitForSync();

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
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1 expectContentUntil: header/10",
        "client -> server | LOAD Map sessions: header/10",
        "storage -> client | CONTENT Map header: true new: After: 1 New: 1",
        "storage -> client | CONTENT Map header: true new: After: 2 New: 1",
        "storage -> client | CONTENT Map header: true new: After: 3 New: 1",
        "storage -> client | CONTENT Map header: true new: After: 4 New: 1",
        "storage -> client | CONTENT Map header: true new: After: 5 New: 1",
        "storage -> client | CONTENT Map header: true new: After: 6 New: 1",
        "storage -> client | CONTENT Map header: true new: After: 7 New: 1",
        "storage -> client | CONTENT Map header: true new: After: 8 New: 1",
        "storage -> client | CONTENT Map header: true new: After: 9 New: 1",
        "storage -> client | CONTENT Map header: true new: After: 10 New: 0",
        "server -> storage | LOAD Group sessions: empty",
        "storage -> server | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> storage | LOAD Map sessions: empty",
        "storage -> server | CONTENT Map header: true new: After: 0 New: 1 expectContentUntil: header/10",
        "server -> client | KNOWN Map sessions: header/10",
      ]
    `);
  });

  test("two storage instances open on the same file should not conflict with each other", async () => {
    const client = setupTestNode();

    client.connectToSyncServer({
      syncServer: jazzCloud.node,
    });
    const dbPath = getDbPath();
    await client.addAsyncStorage({
      ourName: "client",
      filename: dbPath,
    });

    const client2 = setupTestNode();
    client2.connectToSyncServer({
      syncServer: jazzCloud.node,
    });
    await client2.addAsyncStorage({
      ourName: "client2",
      filename: dbPath,
    });

    for (let i = 0; i < 10; i++) {
      for (const node of [client.node, client2.node]) {
        const group = node.createGroup();
        const map = group.createMap();
        map.set("hello", "world", "trusting");
      }
    }

    await client.node.syncManager.waitForAllCoValuesSync();
    await client2.node.syncManager.waitForAllCoValuesSync();
  });
});
