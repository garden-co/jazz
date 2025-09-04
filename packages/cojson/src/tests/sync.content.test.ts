import { assert, beforeEach, describe, expect, test, vi } from "vitest";

import { expectMap } from "../coValue";
import { setMaxRecommendedTxSize } from "../config";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  blockMessageTypeOnOutgoingPeer,
  connectedPeersWithMessagesTracking,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils";
import { stableStringify } from "../jsonStringify";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

function setupMesh() {
  const coreServer = setupTestNode();

  coreServer.addStorage({
    ourName: "core",
  });

  const edgeItaly = setupTestNode();
  edgeItaly.connectToSyncServer({
    ourName: "edge-italy",
    syncServerName: "core",
    syncServer: coreServer.node,
    persistent: true,
  });
  edgeItaly.addStorage({
    ourName: "edge-italy",
  });

  const edgeFrance = setupTestNode();
  edgeFrance.connectToSyncServer({
    ourName: "edge-france",
    syncServerName: "core",
    syncServer: coreServer.node,
    persistent: true,
  });
  edgeFrance.addStorage({
    ourName: "edge-france",
  });

  return { coreServer, edgeItaly, edgeFrance };
}

describe("handling content messages", () => {
  beforeEach(async () => {
    SyncMessagesLog.clear();
  });

  test("dependencies are included when syncing new content to clients", async () => {
    /*
     * Initial Setup:
     * core <-- edge <-- client1
     */
    const core = setupTestNode();

    const edge = setupTestNode();
    edge.connectToSyncServer({
      ourName: "edge",
      syncServerName: "core",
      syncServer: core.node,
    });

    const client1 = setupTestNode();
    client1.connectToSyncServer({
      ourName: "client1",
      syncServerName: "edge",
      syncServer: edge.node,
    });

    // Create a map with a dependency on the client.
    // Everythig will sync all the way back to core.
    const group = client1.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");
    await map.core.waitForSync();

    /*
     * Add another client:
     *
     *                  |-- client1
     * core <-- edge <--|
     *                  |-- client2
     *
     */
    const client2 = setupTestNode();
    const { peerStateOnServer: client2PeerStateOnEdge } =
      client2.connectToSyncServer({
        ourName: "client2",
        syncServerName: "edge",
        syncServer: edge.node,
      });

    client2PeerStateOnEdge.setOptimisticKnownState(map.core.id, {
      id: map.core.id,
      header: false,
      sessions: {},
    });

    // Update the map on client1.
    // This should sync the map AND its dependencies to the new client.
    map.set("hello2", "world2", "trusting");
    await map.core.waitForSync();

    const syncMessages = SyncMessagesLog.getMessages({
      Group: group.core,
      Map: map.core,
    });
    expect(
      syncMessages.some((msg) =>
        msg.startsWith("edge -> client2 | CONTENT Map"),
      ),
    ).toBe(true);
    expect(
      syncMessages.some((msg) =>
        msg.startsWith("edge -> client2 | CONTENT Group"),
      ),
    ).toBe(true);
  });

  test("dependencies are excluded when syncing new content to servers", async () => {
    /*
     * Initial Setup:
     * core1 <-- edge <-- client
     */
    const core1 = setupTestNode();

    const edge = setupTestNode();
    edge.connectToSyncServer({
      ourName: "edge",
      syncServerName: "core1",
      syncServer: core1.node,
    });

    const client = setupTestNode();
    client.connectToSyncServer({
      ourName: "client",
      syncServerName: "edge",
      syncServer: edge.node,
    });

    // Create a map with a dependency on the client.
    // Everythig will sync all the way back to core1.
    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");
    await map.core.waitForSync();

    /*
     * Add another core:
     *
     * core1 <--
     *         |
     *         |-- edge <-- client
     *         |
     * core2 <--
     */
    const core2 = setupTestNode();
    const { peerState: core2PeerStateOnEdge } = edge.connectToSyncServer({
      ourName: "edge",
      syncServerName: "core2",
      syncServer: core2.node,
      skipReconciliation: true,
    });

    core2PeerStateOnEdge.setOptimisticKnownState(map.core.id, {
      id: map.core.id,
      header: false,
      sessions: {},
    });

    // Update the map on the client.
    // This should sync ONLY the map back to both cores.
    map.set("hello2", "world2", "trusting");
    await map.core.waitForSync();

    const syncMessages = SyncMessagesLog.getMessages({
      Group: group.core,
      Map: map.core,
    });
    expect(
      syncMessages.some((msg) => msg.startsWith("edge -> core2 | CONTENT Map")),
    ).toBe(true);
    expect(
      syncMessages.some((msg) =>
        msg.startsWith("edge -> core2 | CONTENT Group"),
      ),
    ).toBe(false);
  });
});
