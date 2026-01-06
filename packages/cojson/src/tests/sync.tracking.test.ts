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

  test("already synced coValues are not tracked as unsynced when trackSyncState is called", async () => {
    const { node: client } = setupTestNode({ connected: true });

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
    expect(unsyncedTracker.has(map.id)).toBe(false);

    // @ts-expect-error trackSyncState is private
    client.syncManager.trackSyncState(map.id);
    expect(unsyncedTracker.has(map.id)).toBe(false);
  });

  test("imported coValue content is tracked as unsynced", async () => {
    const { node: client } = setupTestNode({ connected: true });
    const { node: client2 } = setupTestNode({ connected: false });

    const group = client2.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Export the content from client2 to client
    const groupContent = group.core.newContentSince()![0]!;
    const mapContent = map.core.newContentSince()![0]!;
    client.syncManager.handleNewContent(groupContent, "import");
    client.syncManager.handleNewContent(mapContent, "import");

    const unsyncedTracker = client.syncManager.unsyncedTracker;

    // The imported coValue should be tracked as unsynced since it hasn't been synced to the server yet
    expect(unsyncedTracker.has(group.id)).toBe(true);
    expect(unsyncedTracker.has(map.id)).toBe(true);

    // Wait for the map to sync
    const serverPeer =
      client.syncManager.peers[jazzCloud.node.currentSessionID]!;
    await waitFor(() =>
      client.syncManager.syncState.isSynced(serverPeer, map.id),
    );
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

  test("lots of unsynced coValues are resumed in batches when the node is restarted", async () => {
    const client = setupTestNode({ connected: false });
    const { storage } = client.addStorage();

    const getUnsyncedCoValueIDsFromStorage = async () =>
      new Promise<string[]>((resolve) =>
        client.node.storage?.getUnsyncedCoValueIDs(resolve),
      );

    const group = client.node.createGroup();
    const maps = Array.from({ length: 100 }, () => {
      const map = group.createMap();
      map.set("key", "value");
      return map;
    });

    // Wait for the unsynced coValues to be persisted to storage
    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    const unsyncedTracker = client.node.syncManager.unsyncedTracker;
    for (const map of maps) {
      expect(unsyncedTracker.has(map.id)).toBe(true);
    }
    expect(await getUnsyncedCoValueIDsFromStorage()).toHaveLength(101);

    client.restart();
    client.addStorage({ storage });
    const { peerState: serverPeerState } = client.connectToSyncServer();

    // Wait for sync to resume & complete
    await waitFor(
      async () => (await getUnsyncedCoValueIDsFromStorage()).length === 0,
    );
    for (const map of maps) {
      expect(
        client.node.syncManager.syncState.isSynced(serverPeerState, map.id),
      ).toBe(true);
    }
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

  test("sync resumption is skipped when adding a peer that is not a persistent server", async () => {
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

    let unsyncedCoValueIDs = await getUnsyncedCoValueIDsFromStorage();
    expect(unsyncedCoValueIDs).toHaveLength(2);
    expect(unsyncedCoValueIDs).toContain(map.id);
    expect(unsyncedCoValueIDs).toContain(group.id);

    client.restart();
    client.addStorage({ storage });
    const newPeer = setupTestNode({ isSyncServer: true });
    client.connectToSyncServer({
      syncServer: newPeer.node,
      persistent: false,
    });

    // Wait to confirm sync is not resumed
    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    unsyncedCoValueIDs = await getUnsyncedCoValueIDsFromStorage();
    expect(unsyncedCoValueIDs).toHaveLength(2);
    expect(unsyncedCoValueIDs).toContain(map.id);
    expect(unsyncedCoValueIDs).toContain(group.id);
  });
});
