import { beforeEach, describe, expect, test, vi } from "vitest";

import { expectMap } from "../coValue";
import {
  SyncMessagesLog,
  blockMessageTypeOnOutgoingPeer,
  connectedPeersWithMessagesTracking,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils";

function setupMesh() {
  const coreServer = setupTestNode();

  coreServer.addStoragePeer({
    ourName: "core",
  });

  const edgeItaly = setupTestNode();
  edgeItaly.connectToSyncServer({
    ourName: "edge-italy",
    syncServerName: "core",
    syncServer: coreServer.node,
  });

  const edgeFrance = setupTestNode();
  edgeFrance.connectToSyncServer({
    ourName: "edge-france",
    syncServerName: "core",
    syncServer: coreServer.node,
  });

  return { coreServer, edgeItaly, edgeFrance };
}

describe("multiple clients syncing with the a cloud-like server mesh", () => {
  let mesh = setupMesh();

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
        "edge-france -> core | CONTENT Group header: true new: After: 0 New: 3",
        "core -> edge-france | KNOWN Group sessions: header/3",
        "core -> storage | LOAD Group sessions: header/3",
        "edge-france -> core | CONTENT Map header: true new: After: 0 New: 1",
        "storage -> core | KNOWN Group sessions: empty",
        "core -> edge-france | KNOWN Map sessions: header/1",
        "core -> storage | LOAD Map sessions: header/1",
        "storage -> core | KNOWN Map sessions: empty",
        "core -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "storage -> core | KNOWN Group sessions: header/3",
        "core -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge-italy | LOAD Map sessions: empty",
        "storage -> core | KNOWN Map sessions: header/1",
        "edge-italy -> core | LOAD Map sessions: empty",
        "core -> edge-italy | CONTENT Group header: true new: After: 0 New: 3",
        "edge-italy -> core | KNOWN Group sessions: header/3",
        "core -> edge-italy | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> core | KNOWN Map sessions: header/1",
        "edge-italy -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> edge-italy | KNOWN Group sessions: header/3",
        "edge-italy -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge-italy | KNOWN Map sessions: header/1",
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
        "edge-france -> core | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "core -> edge-france | KNOWN ParentGroup sessions: header/6",
        "core -> storage | LOAD ParentGroup sessions: header/6",
        "edge-france -> core | CONTENT Group header: true new: After: 0 New: 5",
        "storage -> core | KNOWN ParentGroup sessions: empty",
        "core -> edge-france | KNOWN Group sessions: header/5",
        "core -> storage | LOAD Group sessions: header/5",
        "edge-france -> core | CONTENT Map header: true new: After: 0 New: 1",
        "storage -> core | KNOWN Group sessions: empty",
        "core -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "core -> edge-france | KNOWN Map sessions: header/1",
        "storage -> core | KNOWN ParentGroup sessions: header/6",
        "core -> storage | LOAD Map sessions: header/1",
        "storage -> core | KNOWN Map sessions: empty",
        "core -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "storage -> core | KNOWN Group sessions: header/5",
        "core -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge-italy | LOAD Map sessions: empty",
        "storage -> core | KNOWN Map sessions: header/1",
        "edge-italy -> core | LOAD Map sessions: empty",
        "core -> edge-italy | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "edge-italy -> core | KNOWN ParentGroup sessions: header/6",
        "core -> edge-italy | CONTENT Group header: true new: After: 0 New: 5",
        "edge-italy -> core | KNOWN Group sessions: header/5",
        "core -> edge-italy | CONTENT Map header: true new: After: 0 New: 1",
        "edge-italy -> core | KNOWN Map sessions: header/1",
        "edge-italy -> client | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> edge-italy | KNOWN ParentGroup sessions: header/6",
        "edge-italy -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> edge-italy | KNOWN Group sessions: header/5",
        "edge-italy -> client | CONTENT Map header: true new: After: 0 New: 1",
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
        "edge-italy -> core | CONTENT Map header: false new: After: 0 New: 1",
        "core -> edge-italy | KNOWN Map sessions: header/2",
        "core -> storage | CONTENT Map header: false new: After: 0 New: 1",
        "core -> edge-france | CONTENT Map header: false new: After: 0 New: 1",
        "storage -> core | KNOWN Map sessions: header/2",
        "edge-france -> core | KNOWN Map sessions: header/2",
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
        "edge-italy -> core | CONTENT Group header: true new: After: 0 New: 5",
        "edge-italy -> client | CONTENT Group header: true new: After: 0 New: 5",
        "core -> edge-italy | KNOWN Group sessions: header/5",
        "core -> storage | LOAD Group sessions: header/5",
        "edge-italy -> core | CONTENT Map header: true new: After: 0 New: 1",
        "client -> edge-italy | KNOWN Group sessions: header/5",
        "edge-italy -> client | CONTENT Map header: true new: After: 0 New: 1",
        "storage -> core | KNOWN Group sessions: empty",
        "core -> edge-italy | KNOWN Map sessions: header/3",
        "core -> storage | LOAD Map sessions: header/3",
        "client -> edge-italy | KNOWN Map sessions: header/1",
        "storage -> core | KNOWN Map sessions: empty",
        "core -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "storage -> core | KNOWN Group sessions: header/5",
        "core -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "storage -> core | KNOWN Map sessions: header/1",
        "client -> edge-italy | CONTENT Map header: false new: After: 0 New: 1",
        "core -> storage | CONTENT Map header: false new: After: 0 New: 1",
        "core -> edge-italy | CONTENT Map header: false new: After: 0 New: 1",
        "edge-italy -> client | KNOWN CORRECTION Map sessions: empty",
        "storage -> core | KNOWN Map sessions: header/2",
        "edge-italy -> core | KNOWN CORRECTION Map sessions: empty",
        "client -> edge-italy | CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
        "core -> edge-italy | CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
        "edge-italy -> client | KNOWN Map sessions: header/2",
        "edge-italy -> core | CONTENT Map header: false new: After: 0 New: 1",
        "edge-italy -> client | CONTENT Map header: false new: After: 0 New: 1",
        "core -> edge-italy | KNOWN Map sessions: header/3",
        "core -> storage | CONTENT Map header: false new: After: 0 New: 1",
        "edge-italy -> core | KNOWN Map sessions: header/3",
        "client -> edge-italy | KNOWN Map sessions: header/3",
        "storage -> core | KNOWN Map sessions: header/3",
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

    const { peer: storagePeer } = client.connectToSyncServer({
      syncServerName: "storage",
      syncServer: storage.node,
    });

    storagePeer.role = "storage";
    storagePeer.priority = 100;

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
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> storage | KNOWN Group sessions: header/3",
        "client -> core | LOAD Group sessions: header/3",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
        "core -> client | KNOWN Group sessions: header/3",
        "client -> storage | KNOWN Map sessions: header/1",
        "client -> core | LOAD Map sessions: header/1",
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
        "client -> another-server | KNOWN Group sessions: header/3",
        "another-server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> another-server | KNOWN Map sessions: header/1",
      ]
    `);

    expect(mapOnClient.get("hello")).toEqual("world");
  });
});
