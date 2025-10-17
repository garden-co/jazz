import { beforeEach, describe, expect, onTestFinished, test, vi } from "vitest";
import {
  GlobalSyncStateListenerCallback,
  PeerSyncStateListenerCallback,
} from "../SyncStateManager.js";
import { connectedPeers } from "../streamUtils.js";
import { emptyKnownState } from "../exports.js";
import {
  SyncMessagesLog,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils.js";
import { TEST_NODE_CONFIG } from "./testUtils.js";

TEST_NODE_CONFIG.withAsyncPeers = true;

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("SyncStateManager", () => {
  test("subscribeToUpdates receives updates when peer state changes", async () => {
    // Setup nodes
    const client = setupTestNode({ connected: true });
    const { peerState } = client.connectToSyncServer();

    // Create test data
    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");

    const subscriptionManager = client.node.syncManager.syncState;

    const updateSpy: GlobalSyncStateListenerCallback = vi.fn();
    const unsubscribe = subscriptionManager.subscribeToUpdates(updateSpy);

    await waitFor(() => {
      return subscriptionManager.getCurrentSyncState(peerState.id, map.core.id)
        .uploaded;
    });

    expect(updateSpy).toHaveBeenCalledWith(
      peerState.id,
      client.node.syncManager.peers[peerState.id]!.knownStates.get(
        map.core.id,
      )!,
      { uploaded: true },
    );

    // Cleanup
    unsubscribe();
  });

  test("subscribeToPeerUpdates receives updates only for specific peer", async () => {
    // Setup nodes
    const client = setupTestNode({ connected: true });
    const { peerState } = client.connectToSyncServer();

    // Create test data
    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");

    const [serverPeer] = connectedPeers("serverPeer", "unusedPeer", {
      peer1role: "server",
      peer2role: "client",
    });

    client.node.syncManager.addPeer(serverPeer);

    const subscriptionManager = client.node.syncManager.syncState;

    const updateToJazzCloudSpy: PeerSyncStateListenerCallback = vi.fn();
    const updateToStorageSpy: PeerSyncStateListenerCallback = vi.fn();
    const unsubscribe1 = subscriptionManager.subscribeToPeerUpdates(
      peerState.id,
      updateToJazzCloudSpy,
    );
    const unsubscribe2 = subscriptionManager.subscribeToPeerUpdates(
      serverPeer.id,
      updateToStorageSpy,
    );

    onTestFinished(() => {
      unsubscribe1();
      unsubscribe2();
    });

    await waitFor(() => {
      return subscriptionManager.getCurrentSyncState(peerState.id, map.core.id)
        .uploaded;
    });

    expect(updateToJazzCloudSpy).toHaveBeenLastCalledWith(
      client.node.syncManager.peers[peerState.id]!.knownStates.get(
        map.core.id,
      )!,
      { uploaded: true },
    );

    expect(updateToStorageSpy).toHaveBeenCalledWith(
      emptyKnownState(group.core.id),
      { uploaded: false },
    );
  });

  test("getIsCoValueFullyUploadedIntoPeer returns correct status", async () => {
    // Setup nodes
    const client = setupTestNode({ connected: true });
    const { peerState } = client.connectToSyncServer();

    // Create test data
    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");

    const subscriptionManager = client.node.syncManager.syncState;

    expect(
      subscriptionManager.getCurrentSyncState(peerState.id, map.core.id)
        .uploaded,
    ).toBe(false);

    await waitFor(() => {
      return subscriptionManager.getCurrentSyncState(peerState.id, map.core.id)
        .uploaded;
    });

    expect(
      subscriptionManager.getCurrentSyncState(peerState.id, map.core.id)
        .uploaded,
    ).toBe(true);
  });

  test("unsubscribe stops receiving updates", async () => {
    // Setup nodes
    const client = setupTestNode({ connected: true });
    const { peerState } = client.connectToSyncServer();

    // Create test data
    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");

    const subscriptionManager = client.node.syncManager.syncState;
    const anyUpdateSpy = vi.fn();
    const unsubscribe1 = subscriptionManager.subscribeToUpdates(anyUpdateSpy);
    const unsubscribe2 = subscriptionManager.subscribeToPeerUpdates(
      peerState.id,
      anyUpdateSpy,
    );

    unsubscribe1();
    unsubscribe2();

    anyUpdateSpy.mockClear();

    await waitFor(() => {
      return subscriptionManager.getCurrentSyncState(peerState.id, map.core.id)
        .uploaded;
    });

    expect(anyUpdateSpy).not.toHaveBeenCalled();
  });

  test("getCurrentSyncState should return the correct state", async () => {
    // Setup nodes
    const client = setupTestNode({ connected: true });
    const serverNode = jazzCloud.node;
    const { peer, peerOnServer } = client.connectToSyncServer();

    // Create test data
    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");
    group.addMember("everyone", "writer");

    // Initially should not be synced
    expect(
      client.node.syncManager.syncState.getCurrentSyncState(
        peer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: false });

    // Wait for full sync
    await map.core.waitForSync();

    expect(
      client.node.syncManager.syncState.getCurrentSyncState(
        peer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: true });

    const mapOnServer = await loadCoValueOrFail(serverNode, map.id);

    mapOnServer.set("key2", "value2", "trusting");

    expect(
      client.node.syncManager.syncState.getCurrentSyncState(
        peer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: true });

    expect(
      serverNode.syncManager.syncState.getCurrentSyncState(
        peerOnServer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: false });

    await mapOnServer.core.waitForSync();

    expect(
      client.node.syncManager.syncState.getCurrentSyncState(
        peer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: true });

    expect(
      serverNode.syncManager.syncState.getCurrentSyncState(
        peerOnServer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: true });
  });

  test("should skip non-persistent closed peers", async () => {
    const client = setupTestNode();
    const { peerState } = client.connectToSyncServer({
      persistent: false,
    });

    peerState.gracefulShutdown();

    const group = client.node.createGroup();
    const map = group.createMap();

    await map.core.waitForSync();
  });

  test("should wait for persistent closed peers to reconnect", async () => {
    const client = setupTestNode();
    const { peerState } = client.connectToSyncServer({
      persistent: true,
    });

    peerState.gracefulShutdown();

    const group = client.node.createGroup();
    const map = group.createMap();

    const promise = map.core.waitForSync().then(() => "waitForSync");

    const result = await Promise.race([
      promise,
      new Promise((resolve) => {
        setTimeout(() => resolve("timeout"), 10);
      }),
    ]);

    expect(result).toBe("timeout");

    client.connectToSyncServer({
      persistent: true,
    });

    const result2 = await Promise.race([
      promise,
      new Promise((resolve) => {
        setTimeout(() => resolve("timeout"), 10);
      }),
    ]);

    expect(result2).toBe("waitForSync");
  });

  test("should skip client peers that are not subscribed to the coValue", async () => {
    const server = setupTestNode({ isSyncServer: true });
    const client = setupTestNode();

    client.connectToSyncServer({
      syncServer: server.node,
    });

    const group = server.node.createGroup();
    const map = group.createMap();

    await map.core.waitForSync();

    expect(client.node.getCoValue(map.id).isAvailable()).toBe(false);
  });

  test("should wait for client peers that are subscribed to the coValue", async () => {
    const server = setupTestNode({ isSyncServer: true });
    const client = setupTestNode();

    const { peerStateOnServer } = client.connectToSyncServer();

    const group = server.node.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");

    // Simulate the subscription to the coValue
    peerStateOnServer.setKnownState(map.core.id, {
      id: map.core.id,
      header: true,
      sessions: {},
    });

    await map.core.waitForSync();

    expect(client.node.getCoValue(map.id).hasVerifiedContent()).toBe(true);

    // Since only the map is subscribed, the dependencies are pushed after the client requests them
    await waitFor(() => {
      expect(client.node.getCoValue(map.id).isAvailable()).toBe(true);
    });

    expect(
      SyncMessagesLog.getMessages({
        Map: map.core,
        Group: group.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | LOAD Group sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | KNOWN Group sessions: header/3",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });
});
