import { describe, test, expect, beforeEach, assert } from "vitest";
import { CojsonMessageChannel } from "../CojsonMessageChannel";
import type { Peer } from "../sync.js";
import type { RawCoMap } from "../coValues/coMap.js";
import {
  setupTestNode,
  SyncMessagesLog,
  waitFor,
  createTrackedMessageChannel,
  createMockWorkerWithAccept,
  loadCoValueOrFail,
} from "./testUtils";

describe("CojsonMessageChannel", () => {
  beforeEach(() => {
    SyncMessagesLog.clear();
  });

  test("should sync data between two contexts via MessageChannel", async () => {
    // Create two nodes using setupTestNode (handles cleanup automatically)
    const { node: node1 } = setupTestNode();
    const { node: node2 } = setupTestNode();

    const mockWorker = createMockWorkerWithAccept(async (port) => {
      // This runs in the "worker" context
      const peer = await CojsonMessageChannel.acceptFromPort(port, {
        role: "server",
      });
      node2.syncManager.addPeer(peer);
    });

    // Host side: expose to the mock worker
    const peer1 = await CojsonMessageChannel.expose(mockWorker, {
      role: "client",
      messageChannel: createTrackedMessageChannel({
        port1Name: "client",
        port2Name: "server",
      }),
    });
    node1.syncManager.addPeer(peer1);

    // Create data on node1
    const group = node1.createGroup();
    group.addMember("everyone", "writer");
    const map = group.createMap();
    map.set("key", "value", "trusting");

    // Verify data synced
    const mapOnNode2 = await loadCoValueOrFail<RawCoMap>(node2, map.id);
    expect(mapOnNode2.get("key")).toBe("value");

    expect(
      SyncMessagesLog.getMessages({
        Map: map.core,
        Group: group.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "server -> client | LOAD Map sessions: empty",
        "client -> server | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/6",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("should handle disconnection correctly", async () => {
    const { node: node1 } = setupTestNode();
    const { node: node2 } = setupTestNode();

    const peerId = "disconnect-test-peer";
    let peer2: Peer | null = null;

    const mockWorker = createMockWorkerWithAccept(async (port) => {
      peer2 = await CojsonMessageChannel.acceptFromPort(port, {
        id: peerId,
        role: "server",
      });
      node2.syncManager.addPeer(peer2);
    });

    const peer1 = await CojsonMessageChannel.expose(mockWorker, {
      id: peerId,
      role: "client",
    });
    node1.syncManager.addPeer(peer1);

    // Verify peers are connected (same ID on both sides)
    expect(node1.syncManager.peers["disconnect-test-peer"]).toBeDefined();
    expect(node2.syncManager.peers["disconnect-test-peer"]).toBeDefined();

    peer1.outgoing.close();

    expect(node1.syncManager.peers["disconnect-test-peer"]).toBeUndefined();

    await waitFor(() => {
      expect(node2.syncManager.peers["disconnect-test-peer"]).toBeUndefined();
    });
  });

  test("should ignore mismatched peer IDs in waitForConnection() when id filter is provided", async () => {
    const { node } = setupTestNode();

    const hostPeerId = "host-peer-id";
    const wrongPeerId = "wrong-peer-id";

    let acceptPromiseResolved = false;

    // Mock worker that expects a different ID
    const mockWorker = createMockWorkerWithAccept(async (port) => {
      // This should not resolve because the ID doesn't match
      const acceptPromise = CojsonMessageChannel.acceptFromPort(port, {
        id: wrongPeerId, // Expecting a different ID
        role: "server",
      });

      // Set a timeout to detect if it's waiting
      const timeoutPromise = new Promise<null>((resolve) =>
        setTimeout(() => resolve(null), 100),
      );

      const result = await Promise.race([acceptPromise, timeoutPromise]);
      if (result !== null) {
        acceptPromiseResolved = true;
        node.syncManager.addPeer(result);
      }
    });

    // Expose with a different ID than what accept expects
    CojsonMessageChannel.expose(mockWorker, {
      id: hostPeerId,
      role: "client",
    });

    // Wait a bit to ensure the accept didn't resolve
    await new Promise((resolve) => setTimeout(resolve, 150));

    // The accept should not have resolved because IDs don't match
    expect(acceptPromiseResolved).toBe(false);
  });

  test("should sync data bidirectionally", async () => {
    const { node: node1 } = setupTestNode();
    const { node: node2 } = setupTestNode();

    const mockWorker = createMockWorkerWithAccept(async (port) => {
      const peer = await CojsonMessageChannel.acceptFromPort(port, {
        role: "server",
      });
      node2.syncManager.addPeer(peer);
    });

    const peer1 = await CojsonMessageChannel.expose(mockWorker, {
      role: "client",
    });
    node1.syncManager.addPeer(peer1);

    // Create data on node1
    const group1 = node1.createGroup();
    group1.addMember("everyone", "writer");
    const map1 = group1.createMap();
    map1.set("from", "node1", "trusting");

    // Create data on node2
    const group2 = node2.createGroup();
    group2.addMember("everyone", "writer");
    const map2 = group2.createMap();
    map2.set("from", "node2", "trusting");

    // Verify data synced in both directions
    const map1OnNode2 = await loadCoValueOrFail<RawCoMap>(node2, map1.id);
    expect(map1OnNode2.get("from")).toBe("node1");

    const map2OnNode1 = await loadCoValueOrFail<RawCoMap>(node1, map2.id);
    expect(map2OnNode1.get("from")).toBe("node2");
  });

  test("should invoke onClose callback when connection closes", async () => {
    const { node: node1 } = setupTestNode();
    const { node: node2 } = setupTestNode();

    let onCloseCalledOnHost = false;
    let onCloseCalledOnWorker = false;

    const mockWorker = createMockWorkerWithAccept(async (port) => {
      const peer = await CojsonMessageChannel.acceptFromPort(port, {
        role: "server",
        onClose: () => {
          onCloseCalledOnWorker = true;
        },
      });
      node2.syncManager.addPeer(peer);
    });

    const peer1 = await CojsonMessageChannel.expose(mockWorker, {
      role: "client",
      onClose: () => {
        onCloseCalledOnHost = true;
      },
    });
    node1.syncManager.addPeer(peer1);

    // Close the connection
    peer1.outgoing.close();

    // Wait for close to propagate
    await waitFor(() => {
      expect(onCloseCalledOnHost).toBe(true);
    });

    await waitFor(() => {
      expect(onCloseCalledOnWorker).toBe(true);
    });
  });

  test("should apply role configuration correctly", async () => {
    const { node: node1 } = setupTestNode();
    const { node: node2 } = setupTestNode();

    let peer2: Peer | null = null;

    const mockWorker = createMockWorkerWithAccept(async (port) => {
      peer2 = await CojsonMessageChannel.acceptFromPort(port, {
        role: "server",
      });
      node2.syncManager.addPeer(peer2);
    });

    const peer1 = await CojsonMessageChannel.expose(mockWorker, {
      role: "client",
    });
    node1.syncManager.addPeer(peer1);

    // Verify roles are correctly set
    expect(peer1.role).toBe("client");
    expect(peer2).not.toBeNull();
    expect(peer2!.role).toBe("server");
  });

  test("should generate and use the same peer ID on both sides when not provided", async () => {
    const { node: node1 } = setupTestNode();
    const { node: node2 } = setupTestNode();

    let peer2: Peer | null = null;

    const mockWorker = createMockWorkerWithAccept(async (port) => {
      peer2 = await CojsonMessageChannel.acceptFromPort(port, {
        role: "server",
      });
      node2.syncManager.addPeer(peer2);
    });

    // Don't provide an id - it should be auto-generated
    const peer1 = await CojsonMessageChannel.expose(mockWorker, {
      role: "client",
    });
    node1.syncManager.addPeer(peer1);

    // Verify peer1 has an auto-generated ID
    expect(peer1.id).toMatch(/^channel_/);

    // Verify both peers have the same ID
    expect(peer2).not.toBeNull();
    expect(peer2!.id).toBe(peer1.id);

    // Verify the peer is accessible in both sync managers with the same ID
    expect(node1.syncManager.peers[peer1.id]).toBeDefined();
    expect(node2.syncManager.peers[peer1.id]).toBeDefined();
  });

  test("should handle delayed addPeer on accept side", async () => {
    const { node: node1 } = setupTestNode();
    const { node: node2 } = setupTestNode();

    let peer2: Peer | null = null;

    const delay = new Promise((resolve) => setTimeout(resolve, 50));

    const mockWorker = createMockWorkerWithAccept(async (port) => {
      peer2 = await CojsonMessageChannel.acceptFromPort(port, {
        role: "server",
      });
      // Deliberately delay adding the peer
      await delay;
      node2.syncManager.addPeer(peer2);
    });

    const peer1 = await CojsonMessageChannel.expose(mockWorker, {
      role: "client",
    });
    node1.syncManager.addPeer(peer1);

    // Create data on node1 immediately (before node2 has added the peer)
    const group = node1.createGroup();
    group.addMember("everyone", "writer");
    const map = group.createMap();
    map.set("key", "value", "trusting");

    await delay;

    // Verify data synced despite the delay
    const mapOnNode2 = await loadCoValueOrFail<RawCoMap>(node2, map.id);
    expect(mapOnNode2.get("key")).toBe("value");
  });
});
