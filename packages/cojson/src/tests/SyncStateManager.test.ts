import { beforeEach, describe, expect, onTestFinished, test, vi } from "vitest";
import {
  GlobalSyncStateListenerCallback,
  PeerSyncStateListenerCallback,
} from "../SyncStateManager.js";
import { connectedPeers } from "../streamUtils.js";
import { emptyKnownState } from "../sync.js";
import {
  blockMessageTypeOnOutgoingPeer,
  connectNodeToSyncServer,
  createTestNode,
  loadCoValueOrFail,
  setupSyncServer,
  waitFor,
} from "./testUtils.js";

let jazzCloud = setupSyncServer();

beforeEach(async () => {
  jazzCloud = setupSyncServer();
});

describe("SyncStateManager", () => {
  test("subscribeToUpdates receives updates when peer state changes", async () => {
    // Setup nodes
    const client = createTestNode();
    const { nodeToServerPeer } = connectNodeToSyncServer(client);

    // Create test data
    const group = client.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");

    const subscriptionManager = client.syncManager.syncState;

    const updateSpy: GlobalSyncStateListenerCallback = vi.fn();
    const unsubscribe = subscriptionManager.subscribeToUpdates(updateSpy);

    await client.syncManager.actuallySyncCoValue(map.core);

    expect(updateSpy).toHaveBeenCalledWith(
      nodeToServerPeer.id,
      emptyKnownState(map.core.id),
      { uploaded: false },
    );

    await waitFor(() => {
      return subscriptionManager.getCurrentSyncState(
        nodeToServerPeer.id,
        map.core.id,
      ).uploaded;
    });

    expect(updateSpy).toHaveBeenCalledWith(
      nodeToServerPeer.id,
      client.syncManager.peers[nodeToServerPeer.id]!.knownStates.get(
        map.core.id,
      )!,
      { uploaded: true },
    );

    // Cleanup
    unsubscribe();
  });

  test("subscribeToPeerUpdates receives updates only for specific peer", async () => {
    // Setup nodes
    const client = createTestNode();
    const { nodeToServerPeer } = connectNodeToSyncServer(client);

    // Create test data
    const group = client.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");

    const [clientStoragePeer] = connectedPeers("clientStorage", "unusedPeer", {
      peer1role: "client",
      peer2role: "server",
    });

    client.syncManager.addPeer(clientStoragePeer);

    const subscriptionManager = client.syncManager.syncState;

    const updateToJazzCloudSpy: PeerSyncStateListenerCallback = vi.fn();
    const updateToStorageSpy: PeerSyncStateListenerCallback = vi.fn();
    const unsubscribe1 = subscriptionManager.subscribeToPeerUpdates(
      nodeToServerPeer.id,
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

    await client.syncManager.actuallySyncCoValue(map.core);

    expect(updateToJazzCloudSpy).toHaveBeenCalledWith(
      emptyKnownState(map.core.id),
      { uploaded: false },
    );

    await waitFor(() => {
      return subscriptionManager.getCurrentSyncState(
        nodeToServerPeer.id,
        map.core.id,
      ).uploaded;
    });

    expect(updateToJazzCloudSpy).toHaveBeenLastCalledWith(
      client.syncManager.peers[nodeToServerPeer.id]!.knownStates.get(
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
    const client = createTestNode();
    const { nodeToServerPeer } = connectNodeToSyncServer(client);

    // Create test data
    const group = client.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");

    await client.syncManager.actuallySyncCoValue(map.core);

    const subscriptionManager = client.syncManager.syncState;

    expect(
      subscriptionManager.getCurrentSyncState(nodeToServerPeer.id, map.core.id)
        .uploaded,
    ).toBe(false);

    await waitFor(() => {
      return subscriptionManager.getCurrentSyncState(
        nodeToServerPeer.id,
        map.core.id,
      ).uploaded;
    });

    expect(
      subscriptionManager.getCurrentSyncState(nodeToServerPeer.id, map.core.id)
        .uploaded,
    ).toBe(true);
  });

  test("unsubscribe stops receiving updates", async () => {
    // Setup nodes
    const client = createTestNode();
    const { nodeToServerPeer } = connectNodeToSyncServer(client);

    // Create test data
    const group = client.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");

    const subscriptionManager = client.syncManager.syncState;
    const anyUpdateSpy = vi.fn();
    const unsubscribe1 = subscriptionManager.subscribeToUpdates(anyUpdateSpy);
    const unsubscribe2 = subscriptionManager.subscribeToPeerUpdates(
      nodeToServerPeer.id,
      anyUpdateSpy,
    );

    unsubscribe1();
    unsubscribe2();

    await client.syncManager.actuallySyncCoValue(map.core);

    anyUpdateSpy.mockClear();

    await waitFor(() => {
      return subscriptionManager.getCurrentSyncState(
        nodeToServerPeer.id,
        map.core.id,
      ).uploaded;
    });

    expect(anyUpdateSpy).not.toHaveBeenCalled();
  });

  test("getCurrentSyncState should return the correct state", async () => {
    // Setup nodes
    const clientNode = createTestNode();
    const serverNode = jazzCloud.node;
    const {
      nodeToServerPeer: clientToServerPeer,
      serverToNodePeer: serverToClientPeer,
    } = connectNodeToSyncServer(clientNode);
    // Create test data
    const group = clientNode.createGroup();
    const map = group.createMap();
    map.set("key1", "value1", "trusting");
    group.addMember("everyone", "writer");

    // Initially should not be synced
    expect(
      clientNode.syncManager.syncState.getCurrentSyncState(
        clientToServerPeer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: false });

    // Wait for full sync
    await map.core.waitForSync();

    expect(
      clientNode.syncManager.syncState.getCurrentSyncState(
        clientToServerPeer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: true });

    const mapOnServer = await loadCoValueOrFail(serverNode, map.id);

    // Block the content messages so the client won't fully sync immediately
    const outgoing = blockMessageTypeOnOutgoingPeer(
      serverToClientPeer,
      "content",
    );

    mapOnServer.set("key2", "value2", "trusting");

    expect(
      clientNode.syncManager.syncState.getCurrentSyncState(
        clientToServerPeer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: true });

    expect(
      serverNode.syncManager.syncState.getCurrentSyncState(
        serverToClientPeer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: false });

    await outgoing.sendBlockedMessages();
    outgoing.unblock();

    await mapOnServer.core.waitForSync();

    expect(
      clientNode.syncManager.syncState.getCurrentSyncState(
        clientToServerPeer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: true });

    expect(
      serverNode.syncManager.syncState.getCurrentSyncState(
        serverToClientPeer.id,
        map.core.id,
      ),
    ).toEqual({ uploaded: true });
  });
});
