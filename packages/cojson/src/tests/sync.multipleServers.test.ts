import { assert, beforeEach, describe, expect, test } from "vitest";

import { expectList, expectMap } from "../coValue";
import { WasmCrypto } from "../crypto/WasmCrypto";
import { CO_VALUE_LOADING_CONFIG, setCoValueLoadingTimeout } from "../config";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  blockMessageTypeOnOutgoingPeer,
  fillCoMapWithLargeData,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils";
import { RawCoMap } from "../coValues/coMap";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

const Crypto = await WasmCrypto.create();
let server1: ReturnType<typeof setupTestNode>;
let server2: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  SyncMessagesLog.clear();
  server1 = setupTestNode();
  server2 = setupTestNode();
});

function connectServers(client: ReturnType<typeof setupTestNode>) {
  const server1Connection = client.connectToSyncServer({
    ourName: "client",
    syncServerName: "server1",
    syncServer: server1.node,
  });
  const server2Connection = client.connectToSyncServer({
    ourName: "client",
    syncServerName: "server2",
    syncServer: server2.node,
  });

  return { server1Connection, server2Connection };
}

describe("multiple servers peers", () => {
  test("coValue uploading", async () => {
    const client = setupTestNode();
    connectServers(client);

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server1 | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server2 | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server1 | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server2 | CONTENT Map header: true new: After: 0 New: 1",
        "server1 -> client | KNOWN Group sessions: header/4",
        "server2 -> client | KNOWN Group sessions: header/4",
        "server1 -> client | KNOWN Map sessions: header/1",
        "server2 -> client | KNOWN Map sessions: header/1",
      ]
    `);

    const mapOnServer1 = server1.node.getCoValue(map.id);
    const mapOnServer2 = server2.node.getCoValue(map.id);

    expect(mapOnServer1.knownState()).toEqual(map.core.knownState());
    expect(mapOnServer2.knownState()).toEqual(map.core.knownState());
  });

  test("coValue sync across clients", async () => {
    const client = setupTestNode();
    connectServers(client);

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("count", 1, "trusting");

    const session2 = client.spawnNewSession();
    connectServers(session2);

    const mapOnSession2 = await loadCoValueOrFail(session2.node, map.id);
    mapOnSession2.set("count", 2, "trusting");

    map.set("count", 3, "trusting");

    await new Promise((resolve) => setTimeout(resolve, 10));

    mapOnSession2.set("count", 4, "trusting");

    await waitFor(() => {
      expect(map.get("count")).toEqual(4);
      expect(mapOnSession2.get("count")).toEqual(4);
    });

    const mapOnServer1 = server1.node.getCoValue(map.id);
    const mapOnServer2 = server2.node.getCoValue(map.id);

    expect(mapOnServer1.knownState()).toEqual(map.core.knownState());
    expect(mapOnServer2.knownState()).toEqual(map.core.knownState());
  });

  test("coValue with parent groups uploading", async () => {
    const client = setupTestNode();
    connectServers(client);

    const group = client.node.createGroup();
    const parentGroup = client.node.createGroup();
    parentGroup.addMember("everyone", "reader");

    group.extend(parentGroup);

    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        ParentGroup: parentGroup.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server1 | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server2 | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server1 | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> server2 | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> server1 | CONTENT Group header: false new: After: 4 New: 2",
        "client -> server2 | CONTENT Group header: false new: After: 4 New: 2",
        "client -> server1 | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server2 | CONTENT Map header: true new: After: 0 New: 1",
        "server1 -> client | KNOWN Group sessions: header/4",
        "server2 -> client | KNOWN Group sessions: header/4",
        "server1 -> client | KNOWN ParentGroup sessions: header/6",
        "server2 -> client | KNOWN ParentGroup sessions: header/6",
        "server1 -> client | KNOWN Group sessions: header/6",
        "server2 -> client | KNOWN Group sessions: header/6",
        "server1 -> client | KNOWN Map sessions: header/1",
        "server2 -> client | KNOWN Map sessions: header/1",
      ]
    `);

    const mapOnServer1 = server1.node.getCoValue(map.id);
    const mapOnServer2 = server2.node.getCoValue(map.id);

    expect(mapOnServer1.knownState()).toEqual(map.core.knownState());
    expect(mapOnServer2.knownState()).toEqual(map.core.knownState());
  });

  test("wrong optimistic known state should be corrected", async () => {
    const client = setupTestNode();
    connectServers(client);

    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    // Load the coValue on the client
    await map.core.waitForSync();

    // Forcefully delete the coValue from server1 (simulating some data loss)
    server1.node.internalDeleteCoValue(map.id);

    map.set("fromClient", "updated", "trusting");

    await waitFor(() => {
      const mapOnServer1 = server1.node.getCoValue(map.id);
      const mapOnServer2 = server2.node.getCoValue(map.id);

      expect(mapOnServer1.knownState()).toEqual(map.core.knownState());
      expect(mapOnServer2.knownState()).toEqual(map.core.knownState());
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server1 | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server2 | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server1 | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server2 | CONTENT Map header: true new: After: 0 New: 1",
        "server1 -> client | KNOWN Group sessions: header/6",
        "server2 -> client | KNOWN Group sessions: header/6",
        "server1 -> client | KNOWN Map sessions: header/1",
        "server2 -> client | KNOWN Map sessions: header/1",
        "client -> server1 | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server2 | CONTENT Map header: false new: After: 1 New: 1",
        "server1 -> client | KNOWN CORRECTION Map sessions: empty",
        "server2 -> client | KNOWN Map sessions: header/2",
        "client -> server1 | CONTENT Map header: true new: After: 0 New: 2",
        "server1 -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("local updates batching", async () => {
    const client = setupTestNode();
    connectServers(client);

    const group = client.node.createGroup();
    const initialMap = group.createMap();

    const child = group.createMap();
    child.set("parent", initialMap.id);
    initialMap.set("child", child.id);

    await initialMap.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        InitialMap: initialMap.core,
        ChildMap: child.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server1 | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server2 | CONTENT Group header: true new: After: 0 New: 4",
        "client -> server1 | CONTENT InitialMap header: true new: ",
        "client -> server2 | CONTENT InitialMap header: true new: ",
        "client -> server1 | CONTENT ChildMap header: true new: After: 0 New: 1",
        "client -> server2 | CONTENT ChildMap header: true new: After: 0 New: 1",
        "client -> server1 | CONTENT InitialMap header: false new: After: 0 New: 1",
        "client -> server2 | CONTENT InitialMap header: false new: After: 0 New: 1",
        "server1 -> client | KNOWN Group sessions: header/4",
        "server2 -> client | KNOWN Group sessions: header/4",
        "server1 -> client | KNOWN InitialMap sessions: header/0",
        "server2 -> client | KNOWN InitialMap sessions: header/0",
        "server1 -> client | KNOWN ChildMap sessions: header/1",
        "server2 -> client | KNOWN ChildMap sessions: header/1",
        "server1 -> client | KNOWN InitialMap sessions: header/1",
        "server2 -> client | KNOWN InitialMap sessions: header/1",
      ]
    `);
  });

  test("large coValue upload streaming", async () => {
    const client = setupTestNode();
    connectServers(client);

    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    fillCoMapWithLargeData(largeMap);

    await largeMap.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server1 | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server2 | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server1 | CONTENT Map header: true new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> server2 | CONTENT Map header: true new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> server1 | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server2 | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server1 | CONTENT Map header: false new: After: 146 New: 54",
        "client -> server2 | CONTENT Map header: false new: After: 146 New: 54",
        "server1 -> client | KNOWN Group sessions: header/6",
        "server2 -> client | KNOWN Group sessions: header/6",
        "server1 -> client | KNOWN Map sessions: header/73",
        "server2 -> client | KNOWN Map sessions: header/73",
        "server1 -> client | KNOWN Map sessions: header/146",
        "server2 -> client | KNOWN Map sessions: header/146",
        "server1 -> client | KNOWN Map sessions: header/200",
        "server2 -> client | KNOWN Map sessions: header/200",
      ]
    `);
  });

  test("uploading a large update", async () => {
    const client = setupTestNode();
    connectServers(client);

    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    await largeMap.core.waitForSync();

    fillCoMapWithLargeData(largeMap);

    await largeMap.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server1 | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server2 | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server1 | CONTENT Map header: true new: ",
        "client -> server2 | CONTENT Map header: true new: ",
        "client -> server1 | CONTENT Map header: false new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> server2 | CONTENT Map header: false new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> server1 | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server2 | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server1 | CONTENT Map header: false new: After: 146 New: 54",
        "client -> server2 | CONTENT Map header: false new: After: 146 New: 54",
        "server1 -> client | KNOWN Group sessions: header/6",
        "server2 -> client | KNOWN Group sessions: header/6",
        "server1 -> client | KNOWN Map sessions: header/0",
        "server2 -> client | KNOWN Map sessions: header/0",
        "server1 -> client | KNOWN Map sessions: header/73",
        "server2 -> client | KNOWN Map sessions: header/73",
        "server1 -> client | KNOWN Map sessions: header/146",
        "server2 -> client | KNOWN Map sessions: header/146",
        "server1 -> client | KNOWN Map sessions: header/200",
        "server2 -> client | KNOWN Map sessions: header/200",
      ]
    `);
  });

  test("uploading a large update between two clients", async () => {
    const client = setupTestNode();
    connectServers(client);
    const client2 = setupTestNode();
    connectServers(client2);

    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();
    const largeMapOnClient2 = await loadCoValueOrFail(
      client2.node,
      largeMap.id,
    );

    fillCoMapWithLargeData(largeMap);

    await waitFor(() => {
      expect(largeMapOnClient2.core.knownState()).toEqual(
        largeMap.core.knownState(),
      );
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server1 | LOAD Map sessions: empty",
        "client -> server2 | LOAD Map sessions: empty",
        "client -> server1 | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server2 | CONTENT Group header: true new: After: 0 New: 6",
        "client -> server1 | CONTENT Map header: true new: ",
        "client -> server2 | CONTENT Map header: true new: ",
        "server1 -> client | KNOWN Map sessions: empty",
        "server2 -> client | KNOWN Map sessions: empty",
        "server1 -> client | KNOWN Group sessions: header/6",
        "server2 -> client | KNOWN Group sessions: header/6",
        "server1 -> client | KNOWN Map sessions: header/0",
        "server1 -> client | CONTENT Group header: true new: After: 0 New: 6",
        "server1 -> client | CONTENT Map header: true new: ",
        "server2 -> client | KNOWN Map sessions: header/0",
        "server2 -> client | CONTENT Group header: true new: After: 0 New: 6",
        "server2 -> client | CONTENT Map header: true new: ",
        "client -> server1 | KNOWN Group sessions: header/6",
        "client -> server2 | LOAD Group sessions: header/6",
        "client -> server1 | KNOWN Map sessions: header/0",
        "client -> server2 | CONTENT Map header: true new: ",
        "client -> server1 | CONTENT Map header: false new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> server2 | CONTENT Map header: false new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> server1 | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server2 | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server1 | CONTENT Map header: false new: After: 146 New: 54",
        "client -> server2 | CONTENT Map header: false new: After: 146 New: 54",
        "client -> server2 | KNOWN Group sessions: header/6",
        "client -> server2 | CONTENT Group header: false new: After: 0 New: 6",
        "client -> server2 | KNOWN Map sessions: header/0",
        "server2 -> client | KNOWN Group sessions: header/6",
        "server2 -> client | KNOWN Map sessions: header/0",
        "server1 -> client | KNOWN Map sessions: header/73",
        "server1 -> client | CONTENT Map header: false new: After: 0 New: 73",
        "server2 -> client | KNOWN Map sessions: header/73",
        "server2 -> client | CONTENT Map header: false new: After: 0 New: 73",
        "server1 -> client | KNOWN Map sessions: header/146",
        "server1 -> client | CONTENT Map header: false new: After: 73 New: 73",
        "server2 -> client | KNOWN Map sessions: header/146",
        "server2 -> client | CONTENT Map header: false new: After: 73 New: 73",
        "server1 -> client | KNOWN Map sessions: header/200",
        "server1 -> client | CONTENT Map header: false new: After: 146 New: 54",
        "server2 -> client | KNOWN Map sessions: header/200",
        "server2 -> client | CONTENT Map header: false new: After: 146 New: 54",
        "server2 -> client | KNOWN Group sessions: header/6",
        "client -> server1 | KNOWN Map sessions: header/73",
        "client -> server2 | CONTENT Map header: false new: After: 0 New: 73",
        "client -> server2 | KNOWN Map sessions: header/73",
        "client -> server1 | KNOWN Map sessions: header/146",
        "client -> server2 | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server2 | KNOWN Map sessions: header/146",
        "client -> server1 | KNOWN Map sessions: header/200",
        "client -> server2 | CONTENT Map header: false new: After: 146 New: 54",
        "client -> server2 | KNOWN Map sessions: header/200",
        "server2 -> client | KNOWN Map sessions: header/200",
        "server2 -> client | KNOWN Map sessions: header/200",
        "server2 -> client | KNOWN Map sessions: header/200",
      ]
    `);
  });

  test("coValue loading times out on both servers", async () => {
    const previousTimeout = CO_VALUE_LOADING_CONFIG.TIMEOUT;
    setCoValueLoadingTimeout(20);

    try {
      const creator = setupTestNode();
      connectServers(creator);

      const group = creator.node.createGroup();
      const map = group.createMap();
      map.set("hello", "world", "trusting");
      await map.core.waitForSync();

      const client = setupTestNode();
      const { server1Connection, server2Connection } = connectServers(client);

      blockMessageTypeOnOutgoingPeer(
        server1Connection.peerOnServer,
        "content",
        {
          id: map.id,
        },
      );
      blockMessageTypeOnOutgoingPeer(
        server2Connection.peerOnServer,
        "content",
        {
          id: map.id,
        },
      );

      const loadedMap = await client.node.load(map.id, true);

      expect(loadedMap).toBe("unavailable");
    } finally {
      setCoValueLoadingTimeout(previousTimeout);
    }
  });

  test("coValue loading is unavailable on both servers", async () => {
    const disconnectedNode = setupTestNode();
    const group = disconnectedNode.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const client = setupTestNode();
    connectServers(client);

    const loadedMap = await client.node.load(map.id, true);

    expect(loadedMap).toBe("unavailable");
  });
});
