import { assert, beforeEach, describe, expect, test } from "vitest";
import { expectMap } from "../coValue";
import { WasmCrypto } from "../crypto/WasmCrypto";
import { CoValueCore, RawCoMap } from "../exports";
import { LocalNode } from "../localNode";
import { toSimplifiedMessages } from "./messagesTestUtils";
import {
  SyncMessagesLog,
  createTestNode,
  randomAnonymousAccountAndSessionID,
  setupTestNode,
  waitFor,
} from "./testUtils";

const Crypto = await WasmCrypto.create();

let jazzCloud = setupTestNode({ isSyncServer: true });

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("peer reconciliation", () => {
  test("handle new peer connections", async () => {
    const client = setupTestNode();

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    client.connectToSyncServer();

    await new Promise((resolve) => setTimeout(resolve, 100));

    await map.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/3",
        "server -> client | KNOWN Group sessions: empty",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("handle peer reconnections", async () => {
    const client = setupTestNode();

    const group = client.node.createGroup();

    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const { peerState } = client.connectToSyncServer();

    await map.core.waitForSync();

    peerState.gracefulShutdown();

    map.set("hello", "updated", "trusting");

    SyncMessagesLog.clear();
    client.connectToSyncServer();

    await map.core.waitForSync();

    const mapOnSyncServer = jazzCloud.node.coValuesStore.get(map.id);

    assert(mapOnSyncServer.isAvailable());

    expect(
      expectMap(mapOnSyncServer.core.getCurrentContent()).get("hello"),
    ).toEqual("updated");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/3",
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/2",
        "server -> client | KNOWN Map sessions: header/1",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("correctly handle concurrent peer reconnections", async () => {
    const client = setupTestNode();

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const { peerState } = client.connectToSyncServer();

    await map.core.waitForSync();

    peerState.gracefulShutdown();

    map.set("hello", "updated", "trusting");

    SyncMessagesLog.clear();
    const { peer } = client.connectToSyncServer();
    const { peer: latestPeer } = client.connectToSyncServer();

    await map.core.waitForSync();

    const mapOnSyncServer = jazzCloud.node.coValuesStore.get(map.id);

    assert(mapOnSyncServer.isAvailable());

    expect(
      expectMap(mapOnSyncServer.core.getCurrentContent()).get("hello"),
    ).toEqual("updated");

    expect(peer.outgoing).toMatchObject({
      closed: true,
    });

    expect(latestPeer.outgoing).toMatchObject({
      closed: false,
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/3",
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/2",
        "server -> client | KNOWN Map sessions: header/1",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("correctly handle server restarts in the middle of a sync", async () => {
    const client = setupTestNode();

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    jazzCloud.restart();
    SyncMessagesLog.clear();
    client.connectToSyncServer();

    map.set("hello", "updated", "trusting");

    await new Promise((resolve) => setTimeout(resolve, 0));

    client.connectToSyncServer();

    await waitFor(() => {
      const mapOnSyncServer = jazzCloud.node.coValuesStore.get(map.id);

      expect(mapOnSyncServer.highLevelState).toBe("available");
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/3",
        "server -> client | KNOWN Group sessions: empty",
        "client -> server | LOAD Map sessions: header/2",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | KNOWN Map sessions: header/2",
        "client -> server | LOAD Group sessions: header/3",
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/2",
        "server -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test.skip("handle peer reconnections with data loss", async () => {
    const client = setupTestNode();

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    client.connectToSyncServer();

    await map.core.waitForSync();

    jazzCloud.restart();

    SyncMessagesLog.clear();
    client.connectToSyncServer();
    const mapOnSyncServer = jazzCloud.node.coValuesStore.get(map.id);

    await waitFor(() => {
      expect(mapOnSyncServer.isAvailable()).toBe(true);
    });

    assert(mapOnSyncServer.isAvailable());

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Group sessions: header/3",
        "server -> KNOWN Group sessions: empty",
        "client -> LOAD Map sessions: header/1",
        "server -> KNOWN Map sessions: empty",
      ]
    `);

    expect(
      expectMap(mapOnSyncServer.core.getCurrentContent()).get("hello"),
    ).toEqual("updated");
  });
});
