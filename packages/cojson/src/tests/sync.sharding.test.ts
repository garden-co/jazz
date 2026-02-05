import { beforeEach, describe, expect, test } from "vitest";
import { Peer } from "../exports";
import { hwrServerPeerSelector, ServerPeerSelector } from "../sync";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  connectedPeersWithMessagesTracking,
  getSyncServerConnectedPeer,
  setupTestAccount,
  setupTestNode,
} from "./testUtils";
import { PeerState } from "../PeerState";

beforeEach(async () => {
  // We want to simulate a real world communication that happens asynchronously
  TEST_NODE_CONFIG.withAsyncPeers = true;

  SyncMessagesLog.clear();
});

describe("sharding", () => {
  test("server peers are not filtered by default", async () => {
    const { node: client } = setupTestNode({
      connected: false,
    });
    const group = client.createGroup();

    const allPeers: Peer[] = [];
    for (let i = 0; i < 5; i++) {
      const syncServer = await setupTestAccount({ isSyncServer: true });

      const { peer } = getSyncServerConnectedPeer({
        peerId: client.getCurrentAgent().id,
        syncServer: syncServer.node,
      });

      client.syncManager.addPeer(peer);
      allPeers.push(peer);
    }

    const serverPeers = client.syncManager.getServerPeers(group.id);
    expect(serverPeers.map((p) => p.id)).toEqual(allPeers.map((p) => p.id));
  });

  test("server peers are filtered when a serverPeerSelector is set", async () => {
    const firstAlphabetical: ServerPeerSelector = (id, serverPeers) => {
      return serverPeers.sort((a, b) => a.id.localeCompare(b.id)).slice(0, 1);
    };
    const { node: client } = setupTestNode({
      connected: false,
    });
    client.syncManager.serverPeerSelector = firstAlphabetical;
    const group = client.createGroup();

    const allPeers: Peer[] = [];
    for (let i = 0; i < 5; i++) {
      const syncServer = await setupTestAccount({ isSyncServer: true });

      const { peer } = getSyncServerConnectedPeer({
        peerId: client.getCurrentAgent().id,
        syncServer: syncServer.node,
      });

      client.syncManager.addPeer(peer);
      allPeers.push(peer);
    }

    const serverPeers = client.syncManager.getServerPeers(group.id);
    expect(serverPeers.length).toBe(1);
    expect(serverPeers[0]!.id).toEqual(
      allPeers.sort((a, b) => a.id.localeCompare(b.id))[0]!.id,
    );
  });

  test("getPeers respects the serverPeerSelector", async () => {
    const firstPeer: ServerPeerSelector = (id, serverPeers) => {
      return serverPeers.slice(0, 1);
    };
    const { node: edge } = setupTestNode({
      connected: false,
    });
    edge.syncManager.serverPeerSelector = firstPeer;

    // Connect 2 clients to edge
    for (let i = 0; i < 2; i++) {
      const client = await setupTestAccount({});
      const { peer1, peer2 } = connectedPeersWithMessagesTracking({
        peer1: {
          id: client.node.getCurrentAgent().id,
          role: "client",
        },
        peer2: {
          id: edge.getCurrentAgent().id,
          role: "server",
        },
      });

      edge.syncManager.addPeer(peer1);
    }

    // Connect edge to 3 core nodes
    for (let i = 0; i < 3; i++) {
      const syncServer = await setupTestAccount({ isSyncServer: true });

      const { peer } = getSyncServerConnectedPeer({
        peerId: edge.getCurrentAgent().id,
        syncServer: syncServer.node,
      });

      edge.syncManager.addPeer(peer);
    }

    expect(edge.syncManager.getPeers("co_z12345").length).toBe(
      2 + 1, // 2 clients + 1 selected core
    );
  });

  describe("hwrServerPeerSelector", () => {
    const createMockPeer = (
      id: string,
      role: "server" | "client" = "server",
    ): Peer => ({
      id,
      role,
      incoming: {
        close: () => {},
        onMessage: () => {},
        onClose: () => {},
      },
      outgoing: {
        push: () => {},
        close: () => {},
        onClose: () => {},
      },
    });

    test("selects top n peers by hash weight", () => {
      const allPeers = Array.from(
        { length: 5 },
        (_, i) => new PeerState(createMockPeer(`peer_${i + 1}`), undefined),
      );

      // Create selector that picks top 2 peers
      const selector = hwrServerPeerSelector(2);
      const selectedPeers = selector("co_ztest123", allPeers);

      // Should return exactly 2 peers (n=2)
      expect(selectedPeers.length).toBe(2);

      // All returned peers should be from the original set
      expect(allPeers).toContain(selectedPeers[0]);
      expect(allPeers).toContain(selectedPeers[1]);

      // The two peers should be different
      expect(selectedPeers[0]).not.toBe(selectedPeers[1]);
    });

    test("returns all peers when n >= total peers", () => {
      const allPeers = Array.from(
        { length: 3 },
        (_, i) => new PeerState(createMockPeer(`peer_${i + 1}`), undefined),
      );

      // Create a selector that picks top 10 peers (more than we'll have)
      const selector = hwrServerPeerSelector(10);
      const selectedPeers = selector("co_ztest123", allPeers);

      // Should return all 3 peers since n=10 > 3
      expect(selectedPeers.length).toBe(3);
      expect(new Set(selectedPeers.map((p) => p.id))).toEqual(
        new Set(allPeers.map((p) => p.id)),
      );
    });

    test("returns consistent results for same CoValue ID", () => {
      const allPeers = Array.from(
        { length: 5 },
        (_, i) => new PeerState(createMockPeer(`peer_${i + 1}`), undefined),
      );

      const selector = hwrServerPeerSelector(2);

      // Call the selector multiple times with the same CoValue ID
      const result1 = selector("co_ztest123", allPeers);
      const result2 = selector("co_ztest123", allPeers);
      const result3 = selector("co_ztest123", allPeers);

      // Results should be consistent (same peers selected each time for the same CoValue ID)
      expect(new Set(result1.map((p) => p.id))).toEqual(
        new Set(result2.map((p) => p.id)),
      );
      expect(new Set(result2.map((p) => p.id))).toEqual(
        new Set(result3.map((p) => p.id)),
      );
    });

    test("returns different results for different CoValue IDs", () => {
      const allPeers = Array.from(
        { length: 6 },
        (_, i) => new PeerState(createMockPeer(`peer_${i + 1}`), undefined),
      );

      const selector = hwrServerPeerSelector(5);

      // Call the selector with different CoValue IDs
      const result1 = selector("co_ztest123", allPeers);
      const result2 = selector("co_ztest456", allPeers);
      const result3 = selector("co_ztest789", allPeers);

      expect(result1.length).toBe(5);
      expect(result2.length).toBe(5);
      expect(result3.length).toBe(5);

      // Results should all be in different orders
      expect(result1).not.toEqual(result2);
      expect(result1).not.toEqual(result3);
      expect(result2).not.toEqual(result3);
    });

    test("with n=0 throws an error", () => {
      expect(() => hwrServerPeerSelector(0)).toThrow(
        "n must be greater than 0",
      );
    });
  });

  test("peer reconciliation respects server sharding", async () => {
    const client = setupTestNode();
    const server1 = setupTestNode({ isSyncServer: true });
    const server2 = setupTestNode({ isSyncServer: true });
    const server3 = setupTestNode({ isSyncServer: true });
    server1.node.syncManager.disableTransactionVerification();
    server2.node.syncManager.disableTransactionVerification();
    server3.node.syncManager.disableTransactionVerification();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    // Custom server peer selector that routes the group to server1 and the map to server3
    client.node.syncManager.serverPeerSelector = (id, serverPeers) => {
      if (id === group.id) {
        return [
          serverPeers.find((p) => p.id === server1.node.currentSessionID)!,
        ];
      }
      if (id === map.id) {
        return [
          serverPeers.find((p) => p.id === server3.node.currentSessionID)!,
        ];
      }
      return [];
    };

    // Connect the client to all servers, but don't reconcile them yet
    client.connectToSyncServer({
      ourName: "client",
      syncServer: server1.node,
      syncServerName: "server1",
      skipReconciliation: true,
    });
    client.connectToSyncServer({
      ourName: "client",
      syncServer: server2.node,
      syncServerName: "server2",
      skipReconciliation: true,
    });
    client.connectToSyncServer({
      ourName: "client",
      syncServer: server3.node,
      syncServerName: "server3",
      skipReconciliation: true,
    });

    // Now that the full peer membership is known, we can reconcile
    client.node.syncManager.reconcileServerPeers();

    await map.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server1 | LOAD Group sessions: header/4",
        "client -> server3 | LOAD Map sessions: header/1",
        "client -> server1 | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server3 | CONTENT Map header: true new: After: 0 New: 1",
        "server1 -> client | KNOWN Group sessions: empty",
        "server3 -> client | KNOWN Map sessions: empty",
        "server1 -> client | KNOWN Group sessions: header/4",
        "server3 -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });
});
