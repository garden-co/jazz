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
import { Stringified } from "../jsonStringify";
import { JsonValue } from "../jsonValue";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

beforeEach(() => {
  setMaxRecommendedTxSize(100 * 1024);
});

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

describe("multiple clients syncing with the a cloud-like server mesh", () => {
  let mesh: ReturnType<typeof setupMesh>;

  beforeEach(async () => {
    SyncMessagesLog.clear();
    mesh = setupMesh();
  });

  test("loading a coValue created on a different edge", async () => {
    const client = setupTestNode();

    client.connectToSyncServer({
      syncServerName: "edge-italy",
      syncServer: mesh.edgeItaly.node,
    });

    await client.addAsyncStorage({
      ourName: "client",
    });

    const group = mesh.edgeFrance.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "edge-france -> storage | CONTENT Group header: true new: After: 0 New: 4",
        "edge-france -> core | CONTENT Group header: true new: After: 0 New: 4",
        "edge-france -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "edge-france -> core | CONTENT Map header: true new: After: 0 New: 1",
        "core -> edge-france | KNOWN Group sessions: header/4",
        "core -> storage | CONTENT Group header: true new: After: 0 New: 4",
        "core -> edge-france | KNOWN Map sessions: header/1",
        "core -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | KNOWN Map sessions: empty",
        "client -> edge-italy | LOAD Map sessions: empty",
        "edge-italy -> storage | LOAD Map sessions: empty",
        "storage -> edge-italy | KNOWN Map sessions: empty",
        "edge-italy -> core | LOAD Map sessions: empty",
        "core -> edge-italy | CONTENT Group header: true new: After: 0 New: 4",
        "core -> edge-italy | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> core | KNOWN Group sessions: header/4",
        "edge-italy -> storage | CONTENT Group header: true new: After: 0 New: 4",
        "edge-italy -> core | KNOWN Map sessions: header/1",
        "edge-italy -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> client | CONTENT Group header: true new: After: 0 New: 4",
        "edge-italy -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge-italy | KNOWN Group sessions: header/4",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 4",
        "client -> edge-italy | KNOWN Map sessions: header/1",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("coValue created on a different edge with parent groups loading", async () => {
    const client = setupTestNode();

    client.connectToSyncServer({
      syncServerName: "edge-italy",
      syncServer: mesh.edgeItaly.node,
    });

    const group = mesh.edgeFrance.node.createGroup();
    const parentGroup = mesh.edgeFrance.node.createGroup();
    parentGroup.addMember("everyone", "reader");

    group.extend(parentGroup);

    // We wait for sync here to avoid flakiness on CI
    await parentGroup.core.waitForSync();
    await group.core.waitForSync();

    const map = group.createMap();
    map.set("hello", "world");

    await map.core.waitForSync();

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        ParentGroup: parentGroup.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "edge-france -> storage | CONTENT Group header: true new: After: 0 New: 4",
        "edge-france -> core | CONTENT Group header: true new: After: 0 New: 4",
        "edge-france -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "edge-france -> core | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "edge-france -> storage | CONTENT Group header: false new: After: 4 New: 2",
        "edge-france -> core | CONTENT Group header: false new: After: 4 New: 2",
        "core -> edge-france | KNOWN Group sessions: header/4",
        "core -> storage | CONTENT Group header: true new: After: 0 New: 4",
        "core -> edge-france | KNOWN ParentGroup sessions: header/6",
        "core -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "core -> edge-france | KNOWN Group sessions: header/6",
        "core -> storage | CONTENT Group header: false new: After: 4 New: 2",
        "edge-france -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "edge-france -> core | CONTENT Map header: true new: After: 0 New: 1",
        "core -> edge-france | KNOWN Map sessions: header/1",
        "core -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge-italy | LOAD Map sessions: empty",
        "edge-italy -> storage | LOAD Map sessions: empty",
        "storage -> edge-italy | KNOWN Map sessions: empty",
        "edge-italy -> core | LOAD Map sessions: empty",
        "core -> edge-italy | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "core -> edge-italy | CONTENT Group header: true new: After: 0 New: 6",
        "core -> edge-italy | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> core | KNOWN ParentGroup sessions: header/6",
        "edge-italy -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "edge-italy -> core | KNOWN Group sessions: header/6",
        "edge-italy -> storage | CONTENT Group header: true new: After: 0 New: 6",
        "edge-italy -> core | KNOWN Map sessions: header/1",
        "edge-italy -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> client | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "edge-italy -> client | CONTENT Group header: true new: After: 0 New: 6",
        "edge-italy -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge-italy | KNOWN ParentGroup sessions: header/6",
        "client -> edge-italy | KNOWN Group sessions: header/6",
        "client -> edge-italy | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("updating a coValue coming from a different edge", async () => {
    const client = setupTestNode();

    client.connectToSyncServer({
      syncServerName: "edge-italy",
      syncServer: mesh.edgeItaly.node,
    });

    const group = mesh.edgeFrance.node.createGroup();
    group.addMember("everyone", "writer");
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    SyncMessagesLog.clear(); // We want to focus on the sync messages happening from now
    mapOnClient.set("hello", "updated", "trusting");

    await waitFor(() => {
      expect(map.get("hello")).toEqual("updated");
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> edge-italy | CONTENT Map header: false new: After: 0 New: 1",
        "edge-italy -> client | KNOWN Map sessions: header/2",
        "edge-italy -> storage | CONTENT Map header: false new: After: 0 New: 1",
        "edge-italy -> core | CONTENT Map header: false new: After: 0 New: 1",
        "core -> edge-italy | KNOWN Map sessions: header/2",
        "core -> storage | CONTENT Map header: false new: After: 0 New: 1",
        "core -> edge-france | CONTENT Map header: false new: After: 0 New: 1",
        "edge-france -> core | KNOWN Map sessions: header/2",
        "edge-france -> storage | CONTENT Map header: false new: After: 0 New: 1",
      ]
    `);
  });

  test("syncs corrections from multiple peers", async () => {
    const client = setupTestNode();

    client.connectToSyncServer({
      syncServerName: "edge-italy",
      syncServer: mesh.edgeItaly.node,
    });

    const group = mesh.edgeItaly.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    // Load the coValue on the client
    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    const mapOnCoreServer = await loadCoValueOrFail(
      mesh.coreServer.node,
      map.id,
    );

    // Forcefully delete the coValue from the edge (simulating some data loss)
    mesh.edgeItaly.node.internalDeleteCoValue(map.id);
    mesh.edgeItaly.addStorage({
      ourName: "edge-italy",
    });

    mapOnClient.set("fromClient", "updated", "trusting");
    mapOnCoreServer.set("fromServer", "updated", "trusting");

    await waitFor(() => {
      const coValue = expectMap(
        mesh.edgeItaly.node.expectCoValueLoaded(map.id).getCurrentContent(),
      );
      expect(coValue.get("fromServer")).toEqual("updated");
      expect(coValue.get("fromClient")).toEqual("updated");
    });

    const syncLog = SyncMessagesLog.getMessages({
      Group: group.core,
      Map: map.core,
    });

    expect(syncLog).toContain(
      "edge-italy -> client | KNOWN CORRECTION Map sessions: empty",
    );
    expect(syncLog).toContain(
      "edge-italy -> core | KNOWN CORRECTION Map sessions: empty",
    );
  });

  test("sync of changes of a coValue with bad signatures should be blocked", async () => {
    const italianClient = setupTestNode();
    const frenchClient = setupTestNode();

    italianClient.connectToSyncServer({
      syncServerName: "edge-italy",
      syncServer: mesh.edgeItaly.node,
    });

    frenchClient.connectToSyncServer({
      syncServerName: "edge-france",
      syncServer: mesh.edgeFrance.node,
    });

    const group = mesh.edgeFrance.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const mapOnFrenchClient = await loadCoValueOrFail(
      frenchClient.node,
      map.id,
    );
    const mapOnItalianClient = await loadCoValueOrFail(
      italianClient.node,
      map.id,
    );

    expect(mapOnItalianClient.get("hello")).toEqual("world");
    expect(mapOnFrenchClient.get("hello")).toEqual("world");

    const msg = map.core.newContentSince(undefined)?.[0];
    assert(msg);

    msg.new[mesh.edgeFrance.node.currentSessionID]!.newTransactions.push({
      privacy: "trusting",
      changes: JSON.stringify([
        { op: "set", key: "hello", value: "updated" },
      ]) as Stringified<JsonValue[]>,
      madeAt: Date.now(),
    });

    mesh.edgeFrance.node.syncManager.handleNewContent(msg, "storage");

    await map.core.waitForSync();

    SyncMessagesLog.clear(); // We want to focus on the sync messages happening from now

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(mapOnFrenchClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`[]`);
  });

  test("load returns the coValue as soon as one of the peers return the content", async () => {
    const client = setupTestNode();
    const coreServer = setupTestNode({
      isSyncServer: true,
    });

    const { peerOnServer } = client.connectToSyncServer({
      syncServerName: "core",
    });

    const storage = setupTestNode();

    client.connectToSyncServer({
      syncServerName: "another-server",
      syncServer: storage.node,
    });

    const group = coreServer.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const { peerState } = storage.connectToSyncServer({
      ourName: "storage-of-client",
      syncServerName: "core",
    });

    await loadCoValueOrFail(storage.node, map.id);

    peerState.gracefulShutdown();

    SyncMessagesLog.clear();

    await new Promise((resolve) => setTimeout(resolve, 100));

    map.set("hello", "updated", "trusting");

    // Block the content message from the core peer to simulate the delay on response
    blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {});

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> core | LOAD Map sessions: empty",
        "client -> another-server | LOAD Map sessions: empty",
        "core -> storage-of-client | CONTENT Map header: false new: After: 1 New: 1",
        "another-server -> client | CONTENT Group header: true new: After: 0 New: 4",
        "another-server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> another-server | KNOWN Group sessions: header/4",
        "client -> core | LOAD Group sessions: header/4",
        "client -> another-server | KNOWN Map sessions: header/1",
      ]
    `);

    expect(mapOnClient.get("hello")).toEqual("world");
  });

  test("a stuck server peer should not block the load from other server peers", async () => {
    const client = setupTestNode();
    const coreServer = setupTestNode({
      isSyncServer: true,
    });

    const anotherServer = setupTestNode({});

    const { peer: peerToCoreServer } = client.connectToSyncServer({
      syncServerName: "core",
      syncServer: coreServer.node,
    });

    const { peer1, peer2 } = connectedPeersWithMessagesTracking({
      peer1: {
        id: anotherServer.node.getCurrentAgent().id,
        role: "server",
        name: "another-server",
      },
      peer2: {
        id: client.node.getCurrentAgent().id,
        role: "client",
        name: "client",
      },
    });

    blockMessageTypeOnOutgoingPeer(peerToCoreServer, "load", {});

    client.node.syncManager.addPeer(peer1);
    anotherServer.node.syncManager.addPeer(peer2);

    const group = anotherServer.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> another-server | LOAD Map sessions: empty",
        "another-server -> client | CONTENT Group header: true new: After: 0 New: 4",
        "another-server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> another-server | KNOWN Group sessions: header/4",
        "client -> another-server | KNOWN Map sessions: header/1",
      ]
    `);

    expect(mapOnClient.get("hello")).toEqual("world");
  });

  test("large coValue streaming from an edge to the core server and a client at the same time", async () => {
    setMaxRecommendedTxSize(1000);
    const edge = setupTestNode();

    const { storage } = edge.addStorage({
      ourName: "edge",
    });

    const group = edge.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    const chunks = 100;

    const value = "1".repeat(10);

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      largeMap.set(key, value, "trusting");
    }

    await largeMap.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "edge -> storage | CONTENT Group header: true new: After: 0 New: 6",
        "edge -> storage | CONTENT Map header: true new: After: 0 New: 21 expectContentUntil: header/100",
        "edge -> storage | CONTENT Map header: false new: After: 21 New: 21",
        "edge -> storage | CONTENT Map header: false new: After: 42 New: 21",
        "edge -> storage | CONTENT Map header: false new: After: 63 New: 21",
        "edge -> storage | CONTENT Map header: false new: After: 84 New: 16",
      ]
    `);

    await edge.restart();

    edge.connectToSyncServer({
      syncServerName: "core",
      ourName: "edge",
      syncServer: mesh.coreServer.node,
    });
    edge.addStorage({
      storage,
    });

    SyncMessagesLog.clear();

    const client = setupTestNode();

    client.connectToSyncServer({
      syncServerName: "edge",
      syncServer: edge.node,
    });

    client.addStorage({
      ourName: "client",
    });

    const mapOnClient = await loadCoValueOrFail(client.node, largeMap.id);

    await waitFor(() => {
      expect(mapOnClient.core.knownState()).toEqual(largeMap.core.knownState());
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | KNOWN Map sessions: empty",
        "client -> edge | LOAD Map sessions: empty",
        "edge -> storage | LOAD Map sessions: empty",
        "storage -> edge | CONTENT Group header: true new: After: 0 New: 6",
        "edge -> core | LOAD Group sessions: header/6",
        "storage -> edge | CONTENT Map header: true new: After: 0 New: 21 expectContentUntil: header/100",
        "edge -> core | LOAD Map sessions: header/100",
        "edge -> client | CONTENT Group header: true new: After: 0 New: 6",
        "edge -> client | CONTENT Map header: true new: After: 0 New: 21 expectContentUntil: header/100",
        "edge -> client | KNOWN Map sessions: header/100",
        "storage -> edge | CONTENT Map header: true new: After: 21 New: 21",
        "edge -> client | CONTENT Map header: false new: After: 21 New: 21 expectContentUntil: header/100",
        "storage -> edge | CONTENT Map header: true new: After: 42 New: 21",
        "edge -> client | CONTENT Map header: false new: After: 42 New: 21 expectContentUntil: header/100",
        "storage -> edge | CONTENT Map header: true new: After: 63 New: 21",
        "edge -> client | CONTENT Map header: false new: After: 63 New: 21 expectContentUntil: header/100",
        "storage -> edge | CONTENT Map header: true new: After: 84 New: 16",
        "edge -> client | CONTENT Map header: false new: After: 84 New: 16",
        "core -> storage | GET_KNOWN_STATE Group",
        "storage -> core | GET_KNOWN_STATE_RESULT Group sessions: empty",
        "core -> edge | KNOWN Group sessions: empty",
        "core -> storage | GET_KNOWN_STATE Map",
        "storage -> core | GET_KNOWN_STATE_RESULT Map sessions: empty",
        "core -> edge | KNOWN Map sessions: empty",
        "client -> edge | KNOWN Group sessions: header/6",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 6",
        "client -> edge | KNOWN Map sessions: header/21",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 21",
        "client -> edge | KNOWN Map sessions: header/42",
        "client -> storage | CONTENT Map header: false new: After: 21 New: 21",
        "client -> edge | KNOWN Map sessions: header/63",
        "client -> storage | CONTENT Map header: false new: After: 42 New: 21",
        "client -> edge | KNOWN Map sessions: header/84",
        "client -> storage | CONTENT Map header: false new: After: 63 New: 21",
        "client -> edge | KNOWN Map sessions: header/100",
        "client -> storage | CONTENT Map header: false new: After: 84 New: 16",
        "edge -> core | CONTENT Group header: true new: After: 0 New: 6",
        "edge -> core | CONTENT Map header: true new: After: 0 New: 21 expectContentUntil: header/100",
        "edge -> core | CONTENT Map header: false new: After: 21 New: 21",
        "edge -> core | CONTENT Map header: false new: After: 42 New: 21",
        "edge -> core | CONTENT Map header: false new: After: 63 New: 21",
        "edge -> core | CONTENT Map header: false new: After: 84 New: 16",
        "core -> edge | KNOWN Group sessions: header/6",
        "core -> storage | CONTENT Group header: true new: After: 0 New: 6",
        "core -> edge | KNOWN Map sessions: header/21",
        "core -> storage | CONTENT Map header: true new: After: 0 New: 21",
        "core -> edge | KNOWN Map sessions: header/42",
        "core -> storage | CONTENT Map header: false new: After: 21 New: 21",
        "core -> edge | KNOWN Map sessions: header/63",
        "core -> storage | CONTENT Map header: false new: After: 42 New: 21",
        "core -> edge | KNOWN Map sessions: header/84",
        "core -> storage | CONTENT Map header: false new: After: 63 New: 21",
        "core -> edge | KNOWN Map sessions: header/100",
        "core -> storage | CONTENT Map header: false new: After: 84 New: 16",
      ]
    `);

    expect(mapOnClient.core.knownState()).toEqual(largeMap.core.knownState());
  });

  test("edge must subscribe to core when handling client LOAD with lazy loading", async () => {
    // Topology: client1 -> edge -> core <- client2
    //
    // When edge uses lazy loading (getKnownStateFromStorage) to respond
    // to a client's LOAD request with KNOWN, it MUST still subscribe to
    // core by sending a LOAD request. Otherwise, edge won't receive
    // future updates from core for that covalue.
    //
    // This test verifies that updates from client2 (connected to core)
    // properly propagate to client1 (connected to edge).

    // Setup: core server with storage
    const core = setupTestNode();
    core.addStorage({ ourName: "core" });

    // Setup: edge server with storage, connected to core
    // NOTE: Using persistent: false so that when edge reconnects after restart,
    // core doesn't preserve the old subscription state (known states).
    // This simulates a fresh connection where edge must explicitly subscribe.
    const edge = setupTestNode();
    edge.addStorage({ ourName: "edge" });
    edge.connectToSyncServer({
      ourName: "edge",
      syncServerName: "core",
      syncServer: core.node,
      persistent: false,
    });

    // Setup: client1 connected to edge
    const client1 = setupTestNode();
    client1.connectToSyncServer({
      ourName: "client1",
      syncServerName: "edge",
      syncServer: edge.node,
    });

    // Create covalue on core and sync to edge and client1
    const group = core.node.createGroup();
    group.addMember("everyone", "writer"); // Allow anyone to write
    const map = group.createMap();
    map.set("hello", "world", "trusting");
    await map.core.waitForSync();

    // Client1 loads the covalue (syncs to edge storage)
    const mapOnClient1 = await loadCoValueOrFail(client1.node, map.id);
    expect(mapOnClient1.get("hello")).toEqual("world");

    // Verify the states match
    const client1KnownState = mapOnClient1.core.knownState();
    const edgeStorageKnownState = edge.node.storage!.getKnownState(map.id);
    expect(client1KnownState).toEqual(edgeStorageKnownState);

    // Disconnect client1 (keeps data in memory)
    client1.disconnect();

    // Restart edge with a NEW session to ensure it's a completely fresh peer
    // This is necessary because core tracks subscriptions by peer ID (agent+session),
    // and we need edge to appear as a new peer that has never subscribed to the coValues.
    const edgeStorage = edge.node.storage!;

    // Disconnect old edge from core first
    edge.disconnect();

    const newEdge = edge.spawnNewSession();
    newEdge.node.setStorage(edgeStorage);

    newEdge.connectToSyncServer({
      ourName: "edge2",
      syncServerName: "core",
      syncServer: core.node,
      persistent: false,
    });

    // Replace edge reference for the rest of the test
    const edge2 = newEdge;

    SyncMessagesLog.clear();

    // Client1 reconnects with existing data (same known state as edge's storage)
    // This triggers the lazy loading path in handleLoad
    client1.connectToSyncServer({
      ourName: "client1",
      syncServerName: "edge2",
      syncServer: edge2.node,
    });

    await client1.node.syncManager.waitForAllCoValuesSync();

    // Verify edge2 used lazy loading AND subscribed to core
    // Key messages:
    // - edge -> storage | GET_KNOWN_STATE (lazy loading)
    // - edge2 -> client1 | KNOWN (responds with KNOWN, not CONTENT)
    // - edge2 -> core | LOAD (subscribes to core for future updates)
    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client1 -> edge2 | LOAD Group sessions: header/6",
        "client1 -> edge2 | LOAD Map sessions: header/1",
        "edge -> storage | GET_KNOWN_STATE Group",
        "storage -> edge | GET_KNOWN_STATE_RESULT Group sessions: header/6",
        "edge2 -> client1 | KNOWN Group sessions: header/6",
        "edge2 -> core | LOAD Group sessions: header/6",
        "edge -> storage | GET_KNOWN_STATE Map",
        "storage -> edge | GET_KNOWN_STATE_RESULT Map sessions: header/1",
        "edge2 -> client1 | KNOWN Map sessions: header/1",
        "edge2 -> core | LOAD Map sessions: header/1",
        "core -> edge2 | KNOWN Group sessions: header/6",
      ]
    `);

    // IMPORTANT: Verify the coValue is NOT loaded into memory on edge yet.
    // The lazy loading optimization means edge should only have the coValue
    // in storage, not in memory, until actual content arrives.
    const mapOnEdge = edge2.node.getCoValue(map.id);
    expect(mapOnEdge.isAvailable()).toBe(false);

    SyncMessagesLog.clear();

    // Now client2 connects directly to core and makes an update
    const client2 = setupTestNode();
    client2.connectToSyncServer({
      ourName: "client2",
      syncServerName: "core",
      syncServer: core.node,
    });

    const mapOnClient2 = await loadCoValueOrFail(client2.node, map.id);
    mapOnClient2.set("hello", "updated by client2", "trusting");
    await mapOnClient2.core.waitForSync();

    // Wait for propagation: core -> edge2 -> client1
    // Without the fix, this will timeout because edge2 never subscribed to core
    await waitFor(() => mapOnClient1.get("hello") === "updated by client2");

    // Verify edge2 now has the coValue in memory (received from core)
    expect(mapOnEdge.isAvailable()).toBe(true);
    expect(mapOnClient1.get("hello")).toEqual("updated by client2");
  });
});
