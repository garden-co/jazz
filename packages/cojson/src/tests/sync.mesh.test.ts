import { beforeEach, describe, expect, test, vi } from "vitest";

import { expectMap } from "../coValue";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  blockMessageTypeOnOutgoingPeer,
  connectedPeersWithMessagesTracking,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils";

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
        "edge-france -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "edge-france -> core | CONTENT Group header: true new: After: 0 New: 3",
        "edge-france -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "edge-france -> core | CONTENT Map header: true new: After: 0 New: 1",
        "core -> edge-france | KNOWN Group sessions: header/3",
        "core -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "core -> edge-france | KNOWN Map sessions: header/1",
        "core -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | KNOWN Map sessions: empty",
        "client -> edge-italy | LOAD Map sessions: empty",
        "edge-italy -> storage | LOAD Map sessions: empty",
        "storage -> edge-italy | KNOWN Map sessions: empty",
        "edge-italy -> core | LOAD Map sessions: empty",
        "core -> edge-italy | CONTENT Group header: true new: After: 0 New: 3",
        "core -> edge-italy | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> core | KNOWN Group sessions: header/3",
        "edge-italy -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "edge-italy -> core | KNOWN Map sessions: header/1",
        "edge-italy -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> client | CONTENT Group header: true new: After: 0 New: 3",
        "edge-italy -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge-italy | KNOWN Group sessions: header/3",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 3",
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
        "edge-france -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "edge-france -> core | CONTENT Group header: true new: After: 0 New: 3",
        "edge-france -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "edge-france -> core | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "edge-france -> storage | CONTENT Group header: false new: After: 3 New: 2",
        "edge-france -> core | CONTENT Group header: false new: After: 3 New: 2",
        "edge-france -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "edge-france -> core | CONTENT Map header: true new: After: 0 New: 1",
        "core -> edge-france | KNOWN Group sessions: header/3",
        "core -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "core -> edge-france | KNOWN ParentGroup sessions: header/6",
        "core -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "core -> edge-france | KNOWN Group sessions: header/5",
        "core -> storage | CONTENT Group header: false new: After: 3 New: 2",
        "core -> edge-france | KNOWN Map sessions: header/1",
        "core -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "edge-france -> core | CONTENT ParentGroup header: true new: ",
        "client -> edge-italy | LOAD Map sessions: empty",
        "core -> edge-france | KNOWN ParentGroup sessions: header/6",
        "core -> storage | CONTENT ParentGroup header: true new: ",
        "edge-italy -> storage | LOAD Map sessions: empty",
        "storage -> edge-italy | KNOWN Map sessions: empty",
        "edge-italy -> core | LOAD Map sessions: empty",
        "core -> edge-italy | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "core -> edge-italy | CONTENT Group header: true new: After: 0 New: 5",
        "core -> edge-italy | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> core | KNOWN ParentGroup sessions: header/6",
        "edge-italy -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "edge-italy -> core | KNOWN Group sessions: header/5",
        "edge-italy -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "edge-italy -> core | KNOWN Map sessions: header/1",
        "edge-italy -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> client | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "edge-italy -> client | CONTENT Group header: true new: After: 0 New: 5",
        "edge-italy -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge-italy | KNOWN ParentGroup sessions: header/6",
        "client -> edge-italy | KNOWN Group sessions: header/5",
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

    mapOnClient.set("fromClient", "updated", "trusting");
    mapOnCoreServer.set("fromServer", "updated", "trusting");

    await waitFor(() => {
      const coValue = expectMap(
        mesh.edgeItaly.node.expectCoValueLoaded(map.id).getCurrentContent(),
      );
      expect(coValue.get("fromServer")).toEqual("updated");
      expect(coValue.get("fromClient")).toEqual("updated");
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> edge-italy | LOAD Map sessions: empty",
        "edge-italy -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "edge-italy -> core | CONTENT Group header: true new: After: 0 New: 5",
        "edge-italy -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> core | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> client | CONTENT Group header: true new: After: 0 New: 5",
        "edge-italy -> client | CONTENT Map header: true new: After: 0 New: 1",
        "core -> edge-italy | KNOWN Group sessions: header/5",
        "core -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "core -> edge-italy | KNOWN Map sessions: header/1",
        "core -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge-italy | KNOWN Group sessions: header/5",
        "client -> edge-italy | KNOWN Map sessions: header/1",
        "client -> edge-italy | CONTENT Map header: false new: After: 0 New: 1",
        "core -> storage | CONTENT Map header: false new: After: 0 New: 1",
        "core -> edge-italy | CONTENT Map header: false new: After: 0 New: 1",
        "edge-italy -> client | KNOWN CORRECTION Map sessions: empty",
        "edge-italy -> core | KNOWN CORRECTION Map sessions: empty",
        "client -> edge-italy | CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
        "core -> edge-italy | CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
        "edge-italy -> client | KNOWN Map sessions: header/2",
        "edge-italy -> storage | CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
        "edge-italy -> core | CONTENT Map header: false new: After: 0 New: 1",
        "edge-italy -> core | KNOWN Map sessions: header/3",
        "edge-italy -> storage | CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
        "edge-italy -> client | CONTENT Map header: false new: After: 0 New: 1",
        "core -> edge-italy | KNOWN Map sessions: header/3",
        "core -> storage | CONTENT Map header: false new: After: 0 New: 1",
        "client -> edge-italy | KNOWN Map sessions: header/3",
      ]
    `);
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

    // Return an invalid signature on the next transaction
    vi.spyOn(italianClient.node.crypto, "sign").mockReturnValueOnce(
      "signature_z2jYFqH6hey3Yy8EdgjFxDtD7MJWnNMkhBx5snKsBdFRNJgtPSNK73LrAyCjzMjH5f2nsssT5MbYm8r6tKJJGWDEB",
    );

    SyncMessagesLog.clear(); // We want to focus on the sync messages happening from now
    mapOnItalianClient.set("hello", "updated", "trusting");

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(mapOnFrenchClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> edge-italy | CONTENT Map header: false new: After: 0 New: 1",
        "edge-italy -> client | KNOWN Map sessions: header/1",
        "edge-italy -> storage | CONTENT Map header: false new: After: 0 New: 1",
      ]
    `);
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
        "another-server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "another-server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> another-server | KNOWN Group sessions: header/3",
        "client -> core | LOAD Group sessions: header/3",
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
        "another-server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "another-server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> another-server | KNOWN Group sessions: header/3",
        "client -> another-server | KNOWN Map sessions: header/1",
      ]
    `);

    expect(mapOnClient.get("hello")).toEqual("world");
  });

  test("large coValue streaming from an edge to the core server and a client at the same time", async () => {
    const edge = setupTestNode();

    const { storage } = edge.addStorage({
      ourName: "edge",
    });

    const group = edge.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    // Generate a large amount of data (about 100MB)
    const dataSize = 1 * 200 * 1024;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

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
        "edge -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "edge -> storage | CONTENT Map header: true new: After: 0 New: 73",
        "edge -> storage | CONTENT Map header: false new: After: 73 New: 73",
        "edge -> storage | CONTENT Map header: false new: After: 146 New: 54",
      ]
    `);

    edge.restart();

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
        "storage -> edge | CONTENT Group header: true new: After: 0 New: 5",
        "edge -> core | LOAD Group sessions: header/5",
        "storage -> edge | CONTENT Map header: true new: After: 0 New: 73 expectContentUntil: header/200",
        "edge -> core | LOAD Map sessions: header/200",
        "edge -> client | CONTENT Group header: true new: After: 0 New: 5",
        "edge -> client | CONTENT Map header: true new:  expectContentUntil: header/200",
        "edge -> client | CONTENT Map header: false new: After: 0 New: 73",
        "core -> storage | LOAD Group sessions: empty",
        "storage -> core | KNOWN Group sessions: empty",
        "core -> edge | KNOWN Group sessions: empty",
        "core -> storage | LOAD Map sessions: empty",
        "storage -> core | KNOWN Map sessions: empty",
        "core -> edge | KNOWN Map sessions: empty",
        "client -> edge | KNOWN Group sessions: header/5",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "client -> edge | KNOWN Map sessions: header/0",
        "client -> storage | CONTENT Map header: true new:  expectContentUntil: header/200",
        "client -> edge | KNOWN Map sessions: header/73",
        "client -> storage | CONTENT Map header: false new: After: 0 New: 73",
        "storage -> edge | CONTENT Map header: true new: After: 73 New: 73",
        "edge -> client | CONTENT Map header: false new: After: 73 New: 73",
        "edge -> core | CONTENT Group header: true new: After: 0 New: 5",
        "edge -> core | CONTENT Map header: true new:  expectContentUntil: header/200",
        "edge -> core | CONTENT Map header: false new: After: 0 New: 73",
        "edge -> core | CONTENT Map header: false new: After: 73 New: 73",
        "client -> edge | KNOWN Map sessions: header/146",
        "client -> storage | CONTENT Map header: false new: After: 73 New: 73",
        "storage -> edge | CONTENT Map header: true new: After: 146 New: 54",
        "edge -> core | CONTENT Map header: false new: After: 146 New: 54",
        "edge -> client | CONTENT Map header: false new: After: 146 New: 54",
        "core -> edge | KNOWN Group sessions: header/5",
        "core -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "core -> edge | KNOWN Map sessions: header/0",
        "core -> storage | CONTENT Map header: true new:  expectContentUntil: header/200",
        "core -> edge | KNOWN Map sessions: header/73",
        "core -> storage | CONTENT Map header: false new: After: 0 New: 73",
        "core -> edge | KNOWN Map sessions: header/146",
        "core -> storage | CONTENT Map header: false new: After: 73 New: 73",
        "core -> edge | KNOWN Map sessions: header/200",
        "core -> storage | CONTENT Map header: false new: After: 146 New: 54",
        "client -> edge | KNOWN Map sessions: header/200",
        "client -> storage | CONTENT Map header: false new: After: 146 New: 54",
      ]
    `);

    expect(mapOnClient.core.knownState()).toEqual(largeMap.core.knownState());
  });
});
