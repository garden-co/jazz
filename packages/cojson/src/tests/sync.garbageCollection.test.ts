import { beforeEach, describe, expect, test } from "vitest";

import { expectMap } from "../coValue";
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
        "storage -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | CONTENT Group header: true new: After: 0 New: 4",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/1",
        "client -> server | KNOWN Group sessions: header/4",
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
        "storage -> server | CONTENT Group header: true new: After: 0 New: 4",
        "storage -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | CONTENT Group header: true new: After: 0 New: 4",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/1",
        "client -> server | KNOWN Group sessions: header/4",
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

    // With garbageCollected shells, client uses cached knownState (header/1)
    // which is more accurate than asking storage (which returns empty)
    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: header/1",
        "client -> server | LOAD Group sessions: header/4",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server | CONTENT Group header: true new: After: 0 New: 4",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> storage | GET_KNOWN_STATE Map",
        "storage -> server | GET_KNOWN_STATE_RESULT Map sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "server -> storage | GET_KNOWN_STATE Group",
        "storage -> server | GET_KNOWN_STATE_RESULT Group sessions: empty",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Group sessions: header/4",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 4",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("knownStateWithStreaming returns lastKnownState for garbageCollected CoValues", async () => {
    // This test verifies that knownStateWithStreaming() returns the cached lastKnownState
    // for garbage-collected CoValues, not an empty state. This is important for peer
    // reconciliation where we want to send the last known state to minimize data transfer.

    const client = setupTestNode();
    client.addStorage({ ourName: "client" });
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    // Sync to server
    client.connectToSyncServer();
    await client.node.syncManager.waitForAllCoValuesSync();

    // Capture known state before GC
    const originalKnownState = map.core.knownState();
    const originalKnownStateWithStreaming = map.core.knownStateWithStreaming();

    // For available CoValues, both should be equal (no streaming in progress)
    expect(originalKnownState).toEqual(originalKnownStateWithStreaming);
    expect(originalKnownState.header).toBe(true);
    expect(Object.values(originalKnownState.sessions)[0]).toBe(1);

    // Disconnect before GC
    client.disconnect();

    // Run GC to create garbageCollected shell
    client.node.garbageCollector?.collect();
    client.node.garbageCollector?.collect();

    const gcCoValue = client.node.getCoValue(map.id);
    expect(gcCoValue.loadingState).toBe("garbageCollected");

    // Key assertion: knownStateWithStreaming() should return lastKnownState, not empty state
    const gcKnownState = gcCoValue.knownState();
    const gcKnownStateWithStreaming = gcCoValue.knownStateWithStreaming();

    // Both should equal the original known state (the cached lastKnownState)
    expect(gcKnownState).toEqual(originalKnownState);
    expect(gcKnownStateWithStreaming).toEqual(originalKnownState);

    // Specifically verify it's NOT an empty state
    expect(gcKnownStateWithStreaming.header).toBe(true);
    expect(
      Object.keys(gcKnownStateWithStreaming.sessions).length,
    ).toBeGreaterThan(0);
  });

  test("garbageCollected CoValues read from verified content after reload", async () => {
    // This test verifies that after reloading a GC'd CoValue:
    // 1. lastKnownState is cleared
    // 2. knownState() returns data from verified content (not cached)
    // We prove this by adding a transaction after reload and verifying knownState() updates

    const client = setupTestNode();
    client.addStorage({ ourName: "client" });
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    // Sync to server
    client.connectToSyncServer();
    await client.node.syncManager.waitForAllCoValuesSync();

    // Capture known state before GC (has 1 transaction)
    const originalKnownState = map.core.knownState();
    const originalSessionCount = Object.values(originalKnownState.sessions)[0];
    expect(originalSessionCount).toBe(1);

    // Disconnect before GC
    client.disconnect();

    // Run GC to create garbageCollected shell
    client.node.garbageCollector?.collect();
    client.node.garbageCollector?.collect();

    const gcMap = client.node.getCoValue(map.id);
    expect(gcMap.loadingState).toBe("garbageCollected");

    // Verify knownState() returns lastKnownState (still shows 1 transaction)
    expect(gcMap.knownState()).toEqual(originalKnownState);

    // Reconnect and reload
    client.connectToSyncServer();
    const reloadedCore = await client.node.loadCoValueCore(map.id);

    // Verify CoValue is now available
    expect(reloadedCore.loadingState).toBe("available");
    expect(reloadedCore.isAvailable()).toBe(true);

    // At this point, knownState() should be reading from verified content
    // To prove this, we add a new transaction and verify knownState() updates
    const reloadedContent = expectMap(reloadedCore.getCurrentContent());
    reloadedContent.set("hello", "updated locally", "trusting");

    // Verify knownState() now shows 2 transactions
    // This proves we're reading from verified content, not cached lastKnownState
    const newKnownState = reloadedCore.knownState();
    const newSessionCount = Object.values(newKnownState.sessions)[0];

    expect(newSessionCount).toBe(2);
    expect(newKnownState).not.toEqual(originalKnownState);
  });
});
