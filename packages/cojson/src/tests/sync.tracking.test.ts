import { beforeEach, describe, expect, test } from "vitest";
import { setSyncStateTrackingBatchDelay } from "../UnsyncedCoValuesTracker";
import {
  blockMessageTypeOnOutgoingPeer,
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  setupTestNode,
  waitFor,
} from "./testUtils";

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  // We want to simulate a real world communication that happens asynchronously
  TEST_NODE_CONFIG.withAsyncPeers = true;

  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("coValue sync state tracking", () => {
  test("coValues with unsynced local changes are tracked as unsynced", async () => {
    const { node: client } = setupTestNode({ connected: true });

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Wait for local transaction to trigger sync
    await new Promise<void>((resolve) => queueMicrotask(resolve));

    const unsyncedTracker = client.syncManager.unsyncedTracker;
    expect(unsyncedTracker.has(map.id)).toBe(true);
  });

  test("coValue is marked as synced when all persistent server peers have received the content", async () => {
    const { node: client } = setupTestNode({ connected: true });

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Wait for local transaction to trigger sync
    await new Promise<void>((resolve) => queueMicrotask(resolve));

    const unsyncedTracker = client.syncManager.unsyncedTracker;
    expect(unsyncedTracker.has(map.id)).toBe(true);

    const serverPeer =
      client.syncManager.peers[jazzCloud.node.currentSessionID]!;
    await waitFor(() =>
      client.syncManager.syncState.isSynced(serverPeer, map.id),
    );
    expect(unsyncedTracker.has(map.id)).toBe(false);
  });

  test("coValues are tracked as unsynced even if there are no persistent server peers", async () => {
    const { node: client } = setupTestNode({ connected: false });

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    await new Promise<void>((resolve) => queueMicrotask(resolve));

    const unsyncedTracker = client.syncManager.unsyncedTracker;
    expect(unsyncedTracker.has(map.id)).toBe(true);
  });

  test("only tracks sync state for persistent servers peers", async () => {
    const { node: client, connectToSyncServer } = setupTestNode({
      connected: true,
    });

    // Add a second server peer that is NOT persistent
    const server2 = setupTestNode({ isSyncServer: true });
    const { peer: server2PeerOnClient, peerState: server2PeerStateOnClient } =
      connectToSyncServer({
        syncServer: server2.node,
        syncServerName: "server2",
        persistent: false,
      });

    // Do not deliver new content messages to the second server peer
    blockMessageTypeOnOutgoingPeer(server2PeerOnClient, "content", {});

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    await new Promise<void>((resolve) => queueMicrotask(resolve));

    const unsyncedTracker = client.syncManager.unsyncedTracker;
    expect(unsyncedTracker.has(map.id)).toBe(true);

    const serverPeer =
      client.syncManager.peers[jazzCloud.node.currentSessionID]!;
    await waitFor(() =>
      client.syncManager.syncState.isSynced(serverPeer, map.id),
    );

    expect(
      client.syncManager.syncState.isSynced(server2PeerStateOnClient, map.id),
    ).toBe(false);
    expect(unsyncedTracker.has(map.id)).toBe(false);
  });

  test("coValues are not tracked as unsynced if sync is disabled", async () => {
    const { node: client } = setupTestNode({
      connected: false,
      syncWhen: "never",
    });

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    await new Promise<void>((resolve) => queueMicrotask(resolve));

    const unsyncedTracker = client.syncManager.unsyncedTracker;
    expect(unsyncedTracker.has(map.id)).toBe(false);
  });

  test("coValues modified by client peers are tracked as unsynced", async () => {
    const {
      node: edgeSyncServer,
      connectToSyncServer: edgeConnectToSyncServer,
    } = setupTestNode({ isSyncServer: true });
    const { peerState: coreServerPeerState } = edgeConnectToSyncServer({
      syncServer: jazzCloud.node,
      syncServerName: "core",
    });

    const { node: client, connectToSyncServer: clientConnectToSyncServer } =
      setupTestNode();
    clientConnectToSyncServer({
      syncServer: edgeSyncServer,
      syncServerName: "edge",
    });

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    await map.core.waitForSync();

    const unsyncedTracker = edgeSyncServer.syncManager.unsyncedTracker;
    expect(unsyncedTracker.has(map.id)).toBe(true);

    // Wait for the map to sync to jazzCloud (the core server)
    await waitFor(() =>
      edgeSyncServer.syncManager.syncState.isSynced(
        coreServerPeerState,
        map.id,
      ),
    );
    expect(unsyncedTracker.has(map.id)).toBe(false);
  });
});

describe("sync state persistence", () => {
  beforeEach(() => {
    setSyncStateTrackingBatchDelay(0);
  });

  afterEach(() => {
    setSyncStateTrackingBatchDelay(1000);
  });

  test("unsynced coValues are asynchronously persisted to storage", async () => {
    const { node: client, addStorage } = setupTestNode({ connected: false });
    addStorage();

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Wait for the unsynced coValues to be persisted to storage
    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    const unsyncedCoValueIDs = await new Promise((resolve) =>
      client.storage?.getUnsyncedCoValueIDs(resolve),
    );
    expect(unsyncedCoValueIDs).toHaveLength(2);
    expect(unsyncedCoValueIDs).toContain(map.id);
    expect(unsyncedCoValueIDs).toContain(group.id);
  });

  test("synced coValues are removed from storage", async () => {
    const { node: client, addStorage } = setupTestNode({ connected: true });
    addStorage();

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Wait enough time for the coValue to be synced
    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    const unsyncedCoValueIDs = await new Promise((resolve) =>
      client.storage?.getUnsyncedCoValueIDs(resolve),
    );
    expect(unsyncedCoValueIDs).toHaveLength(0);
    expect(client.syncManager.unsyncedTracker.has(map.id)).toBe(false);
  });
});
