import { afterEach, beforeEach, describe, expect, test } from "vitest";
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

  setSyncStateTrackingBatchDelay(0);
});

afterEach(() => {
  setSyncStateTrackingBatchDelay(1000);
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
});

describe("sync state persistence", () => {
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

describe("sync resumption", () => {
  test("unsynced coValues are resumed when the node is restarted", async () => {
    const client = setupTestNode({ connected: false });
    const { storage } = client.addStorage();

    const getUnsyncedCoValueIDsFromStorage = async () =>
      new Promise<string[]>((resolve) =>
        client.node.storage?.getUnsyncedCoValueIDs(resolve),
      );

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Wait for the unsynced coValues to be persisted to storage
    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    const unsyncedTracker = client.node.syncManager.unsyncedTracker;
    expect(unsyncedTracker.has(map.id)).toBe(true);
    expect(await getUnsyncedCoValueIDsFromStorage()).toHaveLength(2);

    client.restart();
    client.addStorage({ storage });
    const { peerState: serverPeerState } = client.connectToSyncServer();

    // Wait for sync to resume & complete
    await waitFor(
      async () => (await getUnsyncedCoValueIDsFromStorage()).length === 0,
    );
    expect(
      client.node.syncManager.syncState.isSynced(serverPeerState, map.id),
    ).toBe(true);
  });

  test("old peer entries are removed from storage when restarting with new peers", async () => {
    const client = setupTestNode();
    const { peer: serverPeer } = client.connectToSyncServer({
      persistent: true,
    });
    const { storage } = client.addStorage();

    // Do not deliver new content messages to the sync server
    blockMessageTypeOnOutgoingPeer(serverPeer, "content", {});

    const getUnsyncedCoValueIDsFromStorage = async () =>
      new Promise<string[]>((resolve) =>
        client.node.storage?.getUnsyncedCoValueIDs(resolve),
      );

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Wait for the unsynced coValues to be persisted to storage
    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    expect(await getUnsyncedCoValueIDsFromStorage()).toHaveLength(2);

    client.restart();
    client.addStorage({ storage });
    const newSyncServer = setupTestNode({ isSyncServer: true });
    const { peerState: newServerPeerState } = client.connectToSyncServer({
      syncServer: newSyncServer.node,
      persistent: true,
    });

    // Wait for sync to resume & complete
    await waitFor(
      async () => (await getUnsyncedCoValueIDsFromStorage()).length === 0,
    );
    expect(
      client.node.syncManager.syncState.isSynced(newServerPeerState, map.id),
    ).toBe(true);
  });
});
