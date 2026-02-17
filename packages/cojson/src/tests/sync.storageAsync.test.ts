import { assert, beforeEach, describe, expect, test, vi } from "vitest";

import { emptyKnownState } from "../exports";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  fillCoMapWithLargeData,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils";
import { getDbPath } from "./testStorage";
import { expectMap } from "../coValue";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

describe("client with storage syncs with server", () => {
  let jazzCloud: ReturnType<typeof setupTestNode>;

  beforeEach(async () => {
    vi.resetAllMocks();
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
        "server -> client | CONTENT Group header: true new: After: 0 New: 4",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/4",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 4",
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

    await client.restart();

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
        "storage -> client | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server | LOAD Group sessions: header/4",
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
        "server -> client | CONTENT Group header: true new: After: 0 New: 6",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN ParentGroup sessions: header/6",
        "client -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> server | KNOWN Group sessions: header/6",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 6",
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

    client.node.syncManager.getPeers(map.id)[0]?.gracefulShutdown();

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
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Group sessions: header/4",
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
        "client -> storage | CONTENT Group header: true new: After: 0 New: 4",
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
        "client -> server | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/4",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 4",
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

    fillCoMapWithLargeData(largeMap);

    await largeMap.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server | CONTENT Group header: true new: After: 0 New: 6",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> server | CONTENT Map header: true new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> storage | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server | CONTENT Map header: false new: After: 73 New: 73",
        "client -> storage | CONTENT Map header: false new: After: 146 New: 54",
        "client -> server | CONTENT Map header: false new: After: 146 New: 54",
        "server -> client | KNOWN Group sessions: header/6",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 6",
        "server -> client | KNOWN Map sessions: header/73",
        "server -> storage | CONTENT Map header: true new: After: 0 New: 73",
        "server -> client | KNOWN Map sessions: header/146",
        "server -> storage | CONTENT Map header: false new: After: 73 New: 73",
        "server -> client | KNOWN Map sessions: header/200",
        "server -> storage | CONTENT Map header: false new: After: 146 New: 54",
      ]
    `);

    SyncMessagesLog.clear();

    await client.restart();

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
        "storage -> client | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server | LOAD Group sessions: header/6",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> server | LOAD Map sessions: header/200",
        "storage -> client | CONTENT Map header: true new: After: 73 New: 73",
        "storage -> client | CONTENT Map header: true new: After: 146 New: 54",
        "server -> client | KNOWN Group sessions: header/6",
        "server -> client | KNOWN Map sessions: header/200",
      ]
    `);
  });

  test("streaming a large coValue with multiple sessions", async () => {
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

    fillCoMapWithLargeData(largeMap);

    await largeMap.core.waitForSync();

    // Fill the map with the data from 5 different sessions
    for (let i = 0; i < 5; i++) {
      const newSession = client.spawnNewSession();
      newSession.connectToSyncServer({
        syncServer: jazzCloud.node,
        ourName: "session " + i,
      });
      const mapInNewSession = await loadCoValueOrFail(
        newSession.node,
        largeMap.id,
      );

      // Alternate between small and large sessions
      if (i % 2 === 0) {
        mapInNewSession.set("hello", "world2", "trusting");
      } else {
        fillCoMapWithLargeData(mapInNewSession);
      }

      await mapInNewSession.core.waitForSync();
    }

    let largeMapOnServer = expectMap(
      jazzCloud.node.getCoValue(largeMap.id).getCurrentContent(),
    );
    await waitFor(async () => {
      expect(largeMapOnServer.core.knownState()).toEqual(
        largeMap.core.knownState(),
      );
    });

    SyncMessagesLog.clear();

    // We want to simulate the situation where the content was only partially synced
    // so we extract some chunks from the current coMap and import them into a new sync server
    const groupContent = group.core.newContentSince(undefined) ?? [];
    const largeMapContent =
      largeMap.core.newContentSince(undefined)?.slice(0, 4) ?? [];

    await client.restart();

    const newSyncServer = setupTestNode({
      isSyncServer: true,
    });

    for (const content of [...groupContent, ...largeMapContent]) {
      content.expectContentUntil = undefined; // Since the chunks are a partial, turn off the streaming infomation that would be invalid
      newSyncServer.node.syncManager.handleNewContent(content, "import");
    }

    const largeMapOnNewSyncServer = newSyncServer.node.getCoValue(largeMap.id);

    // We want to enforce that the new sync server has less content than the original sync server
    expect(largeMapOnNewSyncServer.knownState()).not.toEqual(
      largeMap.core.knownState(),
    );

    client.connectToSyncServer({
      ourName: "client",
      syncServer: newSyncServer.node,
    });

    client.addStorage({
      ourName: "client",
      storage,
    });

    const mapLoadedFromStorage = await loadCoValueOrFail(
      client.node,
      largeMap.id,
    );

    await mapLoadedFromStorage.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server | LOAD Group sessions: header/6",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 73 expectContentUntil: header/603",
        "client -> server | LOAD Map sessions: header/603",
        "storage -> client | CONTENT Map header: true new: After: 73 New: 73",
        "storage -> client | CONTENT Map header: true new: After: 146 New: 54",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 73",
        "storage -> client | CONTENT Map header: true new: After: 73 New: 73",
        "storage -> client | CONTENT Map header: true new: After: 146 New: 54",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 73",
        "storage -> client | CONTENT Map header: true new: After: 73 New: 73",
        "storage -> client | CONTENT Map header: true new: After: 146 New: 54",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/6",
        "server -> client | KNOWN Map sessions: header/292",
        "client -> server | CONTENT Map header: false new: After: 146 New: 54 | After: 0 New: 1 | After: 146 New: 54 expectContentUntil: header/603",
        "client -> server | CONTENT Map header: false new: After: 0 New: 73",
        "client -> server | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server | CONTENT Map header: false new: After: 0 New: 1 | After: 146 New: 54 | After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/401",
        "server -> client | KNOWN Map sessions: header/474",
        "server -> client | KNOWN Map sessions: header/547",
        "server -> client | KNOWN Map sessions: header/603",
      ]
    `);

    // The sync should be completed successfully and the map on the new sync server should have the same known state as the original map
    expect(largeMapOnNewSyncServer.knownState()).toEqual(
      largeMap.core.knownState(),
    );
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

    fillCoMapWithLargeData(largeMap);

    await largeMap.core.waitForSync();

    const newContentChunks = largeMap.core.newContentSince(
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

    await client.restart();

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

    fillCoMapWithLargeData(largeMap);

    await largeMap.core.waitForSync();

    await server.restart();

    server.addStorage({
      ourName: "server",
      storage: serverStorage,
    });

    await client.restart();

    client.connectToSyncServer({
      ourName: "client",
      syncServer: server.node,
    });

    client.addStorage({
      ourName: "client",
      storage,
    });

    SyncMessagesLog.clear();

    const mapOnClient2 = await loadCoValueOrFail(client.node, largeMap.id);
    await mapOnClient2.core.waitForFullStreaming();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server | LOAD Group sessions: header/6",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> server | LOAD Map sessions: header/200",
        "storage -> client | CONTENT Map header: true new: After: 73 New: 73",
        "storage -> client | CONTENT Map header: true new: After: 146 New: 54",
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

  test("large parent group streaming from storage", async () => {
    const syncServer = setupTestNode({
      isSyncServer: true,
    });
    const { storage } = await syncServer.addAsyncStorage({
      ourName: "syncServer",
    });

    const alice = setupTestNode();
    alice.connectToSyncServer({
      syncServer: syncServer.node,
    });

    const parentGroup = alice.node.createGroup();
    const group = alice.node.createGroup();
    group.extend(parentGroup);

    const map = group.createMap();

    fillCoMapWithLargeData(parentGroup);

    parentGroup.addMember("everyone", "reader");

    map.set("hello", "world");

    await map.core.waitForSync();
    await parentGroup.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        ParentGroup: parentGroup.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT ParentGroup header: true new: After: 0 New: 4",
        "client -> server | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server | CONTENT Map header: true new: ",
        "client -> server | CONTENT ParentGroup header: false new: After: 4 New: 73 expectContentUntil: header/206",
        "client -> server | CONTENT ParentGroup header: false new: After: 77 New: 73",
        "client -> server | CONTENT ParentGroup header: false new: After: 150 New: 56",
        "client -> server | CONTENT Map header: false new: After: 0 New: 1",
        "server -> client | KNOWN ParentGroup sessions: header/4",
        "syncServer -> storage | CONTENT ParentGroup header: true new: After: 0 New: 4",
        "server -> client | KNOWN Group sessions: header/6",
        "syncServer -> storage | CONTENT Group header: true new: After: 0 New: 6",
        "server -> client | KNOWN Map sessions: header/0",
        "syncServer -> storage | CONTENT Map header: true new: ",
        "server -> client | KNOWN ParentGroup sessions: header/77",
        "syncServer -> storage | CONTENT ParentGroup header: false new: After: 4 New: 73",
        "server -> client | KNOWN ParentGroup sessions: header/150",
        "syncServer -> storage | CONTENT ParentGroup header: false new: After: 77 New: 73",
        "server -> client | KNOWN ParentGroup sessions: header/206",
        "syncServer -> storage | CONTENT ParentGroup header: false new: After: 150 New: 56",
        "server -> client | KNOWN Map sessions: header/1",
        "syncServer -> storage | CONTENT Map header: false new: After: 0 New: 1",
      ]
    `);

    SyncMessagesLog.clear();

    await syncServer.restart();
    syncServer.addStorage({
      ourName: "syncServer",
      storage,
    });

    const bob = setupTestNode();
    bob.connectToSyncServer({
      syncServer: syncServer.node,
      ourName: "bob",
    });

    const mapOnBob = await loadCoValueOrFail(bob.node, map.id);

    expect(mapOnBob.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        ParentGroup: parentGroup.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "bob -> server | LOAD Map sessions: empty",
        "syncServer -> storage | LOAD Map sessions: empty",
        "storage -> syncServer | CONTENT ParentGroup header: true new: After: 0 New: 77 expectContentUntil: header/206",
        "storage -> syncServer | CONTENT ParentGroup header: true new: After: 77 New: 73",
        "storage -> syncServer | CONTENT ParentGroup header: true new: After: 150 New: 56",
        "storage -> syncServer | CONTENT Group header: true new: After: 0 New: 6",
        "storage -> syncServer | CONTENT Map header: true new: After: 0 New: 1",
        "server -> bob | CONTENT ParentGroup header: true new: After: 0 New: 77 expectContentUntil: header/206",
        "server -> bob | CONTENT ParentGroup header: false new: After: 77 New: 73",
        "server -> bob | CONTENT ParentGroup header: false new: After: 150 New: 56",
        "server -> bob | CONTENT Group header: true new: After: 0 New: 6",
        "server -> bob | CONTENT Map header: true new: After: 0 New: 1",
        "server -> bob | KNOWN Map sessions: header/1",
        "bob -> server | KNOWN ParentGroup sessions: header/77",
        "bob -> server | KNOWN ParentGroup sessions: header/150",
        "bob -> server | KNOWN ParentGroup sessions: header/206",
        "bob -> server | KNOWN Group sessions: header/6",
        "bob -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });
});
