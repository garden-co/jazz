import { beforeEach, describe, expect, test } from "vitest";

import { setGarbageCollectorMaxAge } from "../config";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  loadCoValueOrFail,
  setupTestNode,
} from "./testUtils";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

beforeEach(() => {
  // We want to test what happens when the garbage collector kicks in and removes a coValue
  // We set the max age to -1 to make it remove everything
  setGarbageCollectorMaxAge(-1);
});

describe("sync after the garbage collector has run", () => {
  let jazzCloud: ReturnType<typeof setupTestNode>;

  beforeEach(async () => {
    SyncMessagesLog.clear();
    jazzCloud = setupTestNode({
      isSyncServer: true,
    });
    jazzCloud.addStorage({
      ourName: "server",
    });
    jazzCloud.node.enableGarbageCollector();
  });

  test("loading a coValue from the sync server that was removed by the garbage collector", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    // force the garbage collector to run
    jazzCloud.node.garbageCollector?.collect();

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
        "client -> server | LOAD Map sessions: empty",
        "server -> storage | LOAD Map sessions: empty",
        "storage -> server | CONTENT Group header: true new: After: 0 New: 3",
        "storage -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/3",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("loading a coValue from the sync server that was removed by the garbage collector along with its owner", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    // force the garbage collector to run twice to remove the map and its group
    jazzCloud.node.garbageCollector?.collect();
    jazzCloud.node.garbageCollector?.collect();

    expect(jazzCloud.node.getCoValue(group.id).isAvailable()).toBe(false);
    expect(jazzCloud.node.getCoValue(map.id).isAvailable()).toBe(false);

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
        "client -> server | LOAD Map sessions: empty",
        "server -> storage | LOAD Map sessions: empty",
        "storage -> server | CONTENT Group header: true new: After: 0 New: 3",
        "storage -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/3",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("updating a coValue that was removed by the garbage collector", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    // force the garbage collector to run
    jazzCloud.node.garbageCollector?.collect();
    SyncMessagesLog.clear();

    mapOnClient.set("hello", "updated", "trusting");

    await mapOnClient.core.waitForSync();

    const mapOnServer = await loadCoValueOrFail(jazzCloud.node, map.id);

    expect(mapOnServer.get("hello")).toEqual("updated");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Map header: false new: After: 0 New: 1",
        "server -> storage | LOAD Map sessions: empty",
        "storage -> server | CONTENT Group header: true new: After: 0 New: 5",
        "storage -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/2",
        "server -> storage | CONTENT Map header: false new: After: 0 New: 1",
      ]
    `);
  });

  test("syncing a coValue that was removed by the garbage collector", async () => {
    const client = setupTestNode();
    client.addStorage({
      ourName: "client",
    });
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "updated", "trusting");

    // force the garbage collector to run before the transaction is synced
    client.node.garbageCollector?.collect();
    expect(client.node.getCoValue(map.id).isAvailable()).toBe(false);

    SyncMessagesLog.clear();

    client.connectToSyncServer();

    // Wait for unsynced coValues to be resumed and synced after connecting to server
    await client.node.syncManager.waitForAllCoValuesSync();

    // The storage should work even after the coValue is unmounted, so the load should be successful
    const mapOnServer = await loadCoValueOrFail(jazzCloud.node, map.id);
    expect(mapOnServer.get("hello")).toEqual("updated");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "client -> server | LOAD Group sessions: header/3",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> storage | LOAD Map sessions: empty",
        "storage -> server | KNOWN Map sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "server -> storage | GET_KNOWN_STATE Group",
        "storage -> server | GET_KNOWN_STATE_RESULT Group sessions: empty",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });
});
