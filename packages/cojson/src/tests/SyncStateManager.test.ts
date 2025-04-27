import { beforeEach, describe, expect, onTestFinished, test, vi } from "vitest";
import {
  GlobalSyncStateListenerCallback,
  PeerSyncStateListenerCallback,
} from "../SyncStateManager.js";
import { connectedPeers } from "../streamUtils.js";
import { emptyKnownState } from "../sync.js";
import {
  SyncMessagesLog,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils.js";

let jazzCloud = setupTestNode({ isSyncServer: true });

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

    await client.node.syncManager.syncCoValue(map.core);

    expect(updateSpy).toHaveBeenCalledWith(
      peerState.id,
      emptyKnownState(map.core.id),
      { uploaded: false },
    );

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

    const [clientStoragePeer] = connectedPeers("clientStorage", "unusedPeer", {
      peer1role: "client",
      peer2role: "server",
    });

    client.node.syncManager.addPeer(clientStoragePeer);

    const subscriptionManager = client.node.syncManager.syncState;

    const updateToJazzCloudSpy: PeerSyncStateListenerCallback = vi.fn();
    const updateToStorageSpy: PeerSyncStateListenerCallback = vi.fn();
    const unsubscribe1 = subscriptionManager.subscribeToPeerUpdates(
      peerState.id,
      updateToJazzCloudSpy,
    );
    const unsubscribe2 = subscriptionManager.subscribeToPeerUpdates(
      clientStoragePeer.id,
      updateToStorageSpy,
    );

    onTestFinished(() => {
      unsubscribe1();
      unsubscribe2();
    });

    await client.node.syncManager.syncCoValue(map.core);

    expect(updateToJazzCloudSpy).toHaveBeenCalledWith(
      emptyKnownState(map.core.id),
      { uploaded: false },
    );

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

    expect(updateToStorageSpy).toHaveBeenLastCalledWith(
      emptyKnownState(map.core.id),
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

    await client.node.syncManager.syncCoValue(map.core);

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

    await client.node.syncManager.syncCoValue(map.core);

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
});
