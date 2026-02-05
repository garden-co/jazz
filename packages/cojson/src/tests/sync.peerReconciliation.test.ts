import { assert, beforeEach, describe, expect, test, vi } from "vitest";
import { expectMap } from "../coValue";
import { setGarbageCollectorMaxAge } from "../config";
import { RawCoMap } from "../exports";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
  // Set GC max age to -1 so items are collected immediately when needed
  setGarbageCollectorMaxAge(-1);
});

describe("peer reconciliation", () => {
  test("handle new peer connections", async () => {
    const client = setupTestNode();

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    client.connectToSyncServer();

    await map.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/1",
        "client -> server | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "server -> client | KNOWN Group sessions: header/4",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("handle peer reconnections", async () => {
    const client = setupTestNode();

    const group = client.node.createGroup();

    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const { peerState } = client.connectToSyncServer();

    await map.core.waitForSync();

    peerState.gracefulShutdown();

    map.set("hello", "updated", "trusting");

    SyncMessagesLog.clear();
    client.connectToSyncServer();

    await map.core.waitForSync();

    const mapOnSyncServer = jazzCloud.node.getCoValue(map.id);

    assert(mapOnSyncServer.isAvailable());

    expect(expectMap(mapOnSyncServer.getCurrentContent()).get("hello")).toEqual(
      "updated",
    );

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/2",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Group sessions: header/4",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("correctly handle concurrent peer reconnections", async () => {
    const client = setupTestNode();

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const { peerState } = client.connectToSyncServer();

    await map.core.waitForSync();

    peerState.gracefulShutdown();

    map.set("hello", "updated", "trusting");

    SyncMessagesLog.clear();
    const { peer } = client.connectToSyncServer();
    const { peer: latestPeer } = client.connectToSyncServer();

    await map.core.waitForSync();

    const mapOnSyncServer = jazzCloud.node.getCoValue(map.id);

    assert(mapOnSyncServer.isAvailable());

    expect(expectMap(mapOnSyncServer.getCurrentContent()).get("hello")).toEqual(
      "updated",
    );

    expect(peer.outgoing).toMatchObject({
      closed: true,
    });

    expect(latestPeer.outgoing).toMatchObject({
      closed: false,
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/2",
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/2",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Group sessions: header/4",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("correctly handle server restarts in the middle of a sync", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    await jazzCloud.restart();
    SyncMessagesLog.clear();
    client.connectToSyncServer();

    map.set("hello", "updated", "trusting");

    await new Promise((resolve) => setTimeout(resolve, 0));

    client.connectToSyncServer();

    await waitFor(() => {
      const mapOnSyncServer = jazzCloud.node.getCoValue(map.id);

      expect(mapOnSyncServer.loadingState).toBe("available");
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/1",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/2",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN CORRECTION Map sessions: empty",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | LOAD Group sessions: empty",
        "client -> server | CONTENT Group header: true new: After: 0 New: 4",
        "server -> client | KNOWN Group sessions: header/4",
        "server -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("correctly handle server restarts in the middle of a sync (2 - account)", async () => {
    const client = await setupTestAccount({
      connected: true,
    });

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    await jazzCloud.restart();
    SyncMessagesLog.clear();
    client.connectToSyncServer();

    map.set("hello", "updated", "trusting");

    await new Promise((resolve) => setTimeout(resolve, 0));

    client.connectToSyncServer();

    await waitFor(() => {
      const mapOnSyncServer = jazzCloud.node.getCoValue(map.id);

      expect(mapOnSyncServer.isAvailable()).toBe(true);
      const content = mapOnSyncServer.getCurrentContent() as RawCoMap;
      expect(content.get("hello")).toBe("updated");
    });

    expect(
      SyncMessagesLog.getMessages({
        Account: client.node.expectCurrentAccount("client account").core,
        Profile: client.node.expectProfileLoaded(client.accountID).core,
        ProfileGroup: client.node.expectProfileLoaded(client.accountID).group
          .core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Account sessions: header/4",
        "client -> server | LOAD ProfileGroup sessions: header/6",
        "client -> server | LOAD Profile sessions: header/1",
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/1",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Account sessions: empty",
        "server -> client | KNOWN ProfileGroup sessions: empty",
        "server -> client | KNOWN Profile sessions: empty",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | LOAD Account sessions: header/4",
        "client -> server | LOAD ProfileGroup sessions: header/6",
        "client -> server | LOAD Profile sessions: header/1",
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/2",
        "server -> client | KNOWN Account sessions: empty",
        "server -> client | KNOWN ProfileGroup sessions: empty",
        "server -> client | KNOWN Profile sessions: empty",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | CONTENT ProfileGroup header: true new: After: 0 New: 6",
        "client -> server | CONTENT Profile header: true new: After: 0 New: 1",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | LOAD Account sessions: empty",
        "server -> client | LOAD ProfileGroup sessions: empty",
        "client -> server | CONTENT Account header: true new: After: 0 New: 4",
        "client -> server | CONTENT ProfileGroup header: true new: After: 0 New: 6",
        "server -> client | KNOWN Account sessions: header/4",
        "server -> client | KNOWN ProfileGroup sessions: header/6",
        "server -> client | KNOWN Profile sessions: header/1",
        "server -> client | KNOWN CORRECTION Map sessions: empty",
        "server -> client | KNOWN ProfileGroup sessions: header/6",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | LOAD Group sessions: empty",
        "client -> server | CONTENT Group header: true new: After: 0 New: 4",
        "server -> client | KNOWN Group sessions: header/4",
        "server -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test.skip("handle peer reconnections with data loss", async () => {
    const client = setupTestNode();

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    client.connectToSyncServer();

    await map.core.waitForSync();

    await jazzCloud.restart();

    SyncMessagesLog.clear();
    client.connectToSyncServer();
    const mapOnSyncServer = jazzCloud.node.getCoValue(map.id);

    await waitFor(() => {
      expect(mapOnSyncServer.isAvailable()).toBe(true);
    });

    assert(mapOnSyncServer.isAvailable());

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Group sessions: header/3",
        "server -> KNOWN Group sessions: empty",
        "client -> LOAD Map sessions: header/1",
        "server -> KNOWN Map sessions: empty",
      ]
    `);

    expect(expectMap(mapOnSyncServer.getCurrentContent()).get("hello")).toEqual(
      "updated",
    );
  });
});

describe("peer reconciliation with garbageCollected CoValues", () => {
  test("sends cached known state for garbageCollected CoValues during reconciliation", async () => {
    const client = setupTestNode();
    client.addStorage({ ourName: "client" });
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    // Sync to server first
    client.connectToSyncServer();
    await client.node.syncManager.waitForAllCoValuesSync();

    // Capture the known state before GC
    const mapKnownState = map.core.knownState();
    const groupKnownState = group.core.knownState();

    // Disconnect first to avoid server reloading CoValues after GC
    client.disconnect();

    // Run GC to unmount the CoValues (creates garbageCollected shells)
    // GC works because there are no persistent server peers (disconnected)
    client.node.garbageCollector?.collect();
    client.node.garbageCollector?.collect(); // Second pass for dependencies

    // Verify CoValues are now garbageCollected
    const gcMap = client.node.getCoValue(map.id);
    const gcGroup = client.node.getCoValue(group.id);
    expect(gcMap.loadingState).toBe("garbageCollected");
    expect(gcGroup.loadingState).toBe("garbageCollected");

    // Verify knownState() returns the cached state (not empty)
    expect(gcMap.knownState()).toEqual(mapKnownState);
    expect(gcGroup.knownState()).toEqual(groupKnownState);

    // Reconnect to trigger peer reconciliation
    SyncMessagesLog.clear();
    client.connectToSyncServer();

    // Wait for messages to be exchanged
    await new Promise((resolve) => setTimeout(resolve, 100));

    // LOAD is sent with cached known state (no storage lookup needed)
    expect(
      SyncMessagesLog.getMessages({
        Group: gcGroup,
        Map: gcMap,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | KNOWN Group sessions: header/4",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("garbageCollected CoValues restore subscription with minimal data transfer", async () => {
    // Setup: both client and server have the same data
    const client = setupTestNode();
    client.addStorage({ ourName: "client" });
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    // Sync to server
    client.connectToSyncServer();
    await client.node.syncManager.waitForAllCoValuesSync();

    // Verify server has the data
    const serverMap = jazzCloud.node.getCoValue(map.id);
    expect(serverMap.isAvailable()).toBe(true);

    // Capture known states before GC
    const clientMapKnownState = map.core.knownState();
    const clientGroupKnownState = group.core.knownState();

    // Disconnect before GC to avoid server reloading CoValues
    client.disconnect();

    // Run GC
    client.node.garbageCollector?.collect();
    client.node.garbageCollector?.collect();

    const gcMap = client.node.getCoValue(map.id);
    const gcGroup = client.node.getCoValue(group.id);

    // Verify garbageCollected state
    expect(gcMap.loadingState).toBe("garbageCollected");
    expect(gcGroup.loadingState).toBe("garbageCollected");

    // Reconnect to trigger peer reconciliation
    SyncMessagesLog.clear();
    client.connectToSyncServer();

    // Wait for messages to be exchanged
    await new Promise((resolve) => setTimeout(resolve, 100));

    // LOAD is sent with cached known state
    // Server responds with KNOWN since client and server have the same data
    expect(
      SyncMessagesLog.getMessages({
        Group: gcGroup,
        Map: gcMap,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/4",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | KNOWN Group sessions: header/4",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("unknown CoValues return empty knownState during reconciliation", async () => {
    const client = setupTestNode();

    // Create a CoValue on another node that we'll hear about but not load
    const otherClient = setupTestNode();
    const group = otherClient.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    // Sync other client to server
    otherClient.connectToSyncServer();
    await otherClient.node.syncManager.waitForAllCoValuesSync();

    // Now client connects - it will hear about the CoValue IDs but not load them
    // Create a reference to the CoValue without loading it
    const unknownCoValue = client.node.getCoValue(map.id);

    // Verify it's in unknown state
    expect(unknownCoValue.loadingState).toBe("unknown");

    // Verify knownState() returns empty state for unknown CoValues
    const knownState = unknownCoValue.knownState();
    expect(knownState.header).toBe(false);
    expect(knownState.sessions).toEqual({});
  });

  test("unknown CoValues are skipped during peer reconciliation (no LOAD sent)", async () => {
    const client = setupTestNode();

    // Create a CoValue on another node
    const otherClient = setupTestNode();
    const group = otherClient.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    // Sync other client to server so the server knows about the CoValue
    otherClient.connectToSyncServer();
    await otherClient.node.syncManager.waitForAllCoValuesSync();

    // Client creates its own group (so we have something to compare against)
    const clientGroup = client.node.createGroup();
    const clientMap = clientGroup.createMap();
    clientMap.set("foo", "bar", "trusting");

    // Create a reference to the other client's CoValue WITHOUT loading it
    // This simulates "hearing about" a CoValue ID (e.g., from a reference)
    const unknownCoValue = client.node.getCoValue(map.id);
    expect(unknownCoValue.loadingState).toBe("unknown");

    // Connect client to server - this triggers peer reconciliation
    SyncMessagesLog.clear();
    client.connectToSyncServer();

    await client.node.syncManager.waitForAllCoValuesSync();

    // Verify: LOAD sent for client's own CoValues, but NOT for the unknown CoValue
    expect(
      SyncMessagesLog.getMessages({
        ClientGroup: clientGroup.core,
        ClientMap: clientMap.core,
        UnknownMap: unknownCoValue,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD ClientGroup sessions: header/4",
        "client -> server | LOAD ClientMap sessions: header/1",
        "client -> server | CONTENT ClientGroup header: true new: After: 0 New: 4",
        "client -> server | CONTENT ClientMap header: true new: After: 0 New: 1",
        "server -> client | KNOWN ClientGroup sessions: empty",
        "server -> client | KNOWN ClientMap sessions: empty",
        "server -> client | KNOWN ClientGroup sessions: header/4",
        "server -> client | KNOWN ClientMap sessions: header/1",
      ]
    `);
  });
});
