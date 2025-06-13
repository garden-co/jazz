import { assert, beforeEach, describe, expect, test } from "vitest";
import { expectMap } from "../coValue";
import { CO_VALUE_LOADING_CONFIG } from "../coValueCore/coValueCore";
import { WasmCrypto } from "../crypto/WasmCrypto";
import {
  SyncMessagesLog,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils";

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

    const mapOnSyncServer = jazzCloud.node.getCoValue(map.id);

    assert(mapOnSyncServer.isAvailable());

    expect(expectMap(mapOnSyncServer.getCurrentContent()).get("hello")).toEqual(
      "updated",
    );

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

    const mapOnSyncServer = jazzCloud.node.getCoValue(map.id);

    assert(mapOnSyncServer.isAvailable());

    expect(expectMap(mapOnSyncServer.getCurrentContent()).get("hello")).toEqual(
      "updated",
    );

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
    const client = setupTestNode({
      connected: true,
    });

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
      const mapOnSyncServer = jazzCloud.node.getCoValue(map.id);

      expect(mapOnSyncServer.loadingState).toBe("available");
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
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN CORRECTION Map sessions: empty",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | LOAD Group sessions: header/3",
        "server -> client | KNOWN Group sessions: empty",
        "client -> server | LOAD Map sessions: header/2",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | KNOWN Map sessions: empty",
      ]
    `);
  });

  test("correctly handle server restarts in the middle of a sync (2 - account)", async () => {
    const client = await setupTestAccount({
      connected: true,
    });

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
      const mapOnSyncServer = jazzCloud.node.getCoValue(map.id);

      expect(mapOnSyncServer.loadingState).toBe("available");
    });

    expect(
      SyncMessagesLog.getMessages({
        Account: client.node.expectCurrentAccount("client account").core,
        Profile: client.node.expectProfileLoaded(client.accountID).core,
        ProfileGroup: client.node.expectProfileLoaded(client.accountID).group
          .core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Account sessions: header/4",
        "server -> client | KNOWN Account sessions: empty",
        "client -> server | LOAD ProfileGroup sessions: header/5",
        "server -> client | KNOWN ProfileGroup sessions: empty",
        "client -> server | LOAD Profile sessions: header/1",
        "server -> client | KNOWN Profile sessions: empty",
        "client -> server | LOAD Group sessions: header/3",
        "server -> client | KNOWN Group sessions: empty",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN CORRECTION Map sessions: empty",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | KNOWN CORRECTION Map sessions: empty",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | LOAD Account sessions: header/4",
        "server -> client | KNOWN Account sessions: empty",
        "client -> server | LOAD ProfileGroup sessions: header/5",
        "server -> client | KNOWN ProfileGroup sessions: empty",
        "client -> server | LOAD Profile sessions: header/1",
        "server -> client | KNOWN Profile sessions: empty",
        "client -> server | LOAD Group sessions: header/3",
        "server -> client | KNOWN Group sessions: empty",
        "client -> server | LOAD Map sessions: header/2",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | KNOWN Map sessions: empty",
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
    const mapOnSyncServer = jazzCloud.node.getCoValue(map.id);

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

    expect(expectMap(mapOnSyncServer.getCurrentContent()).get("hello")).toEqual(
      "updated",
    );
  });
});
