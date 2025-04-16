import { assert, beforeEach, describe, expect, test } from "vitest";
import { expectMap } from "../coValue";
import { WasmCrypto } from "../crypto/WasmCrypto";
import { LocalNode } from "../localNode";
import { toSimplifiedMessages } from "./messagesTestUtils";
import {
  connectNodeToSyncServer,
  createTestNode,
  randomAnonymousAccountAndSessionID,
  setupSyncServer,
  waitFor,
} from "./testUtils";

const Crypto = await WasmCrypto.create();

let jazzCloud = setupSyncServer();

beforeEach(async () => {
  jazzCloud = setupSyncServer();
});

describe("peer reconciliation", () => {
  test("handle new peer connections", async () => {
    const node = createTestNode();

    const group = node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const { messages } = connectNodeToSyncServer(node, true);

    await new Promise((resolve) => setTimeout(resolve, 100));

    await map.core.waitForSync();

    expect(
      toSimplifiedMessages(
        {
          Group: group.core,
          Map: map.core,
        },
        messages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Group sessions: header/3",
        "server -> KNOWN Group sessions: empty",
        "client -> CONTENT Group header: true new: After: 0 New: 3",
        "server -> KNOWN Group sessions: header/3",
        "client -> LOAD Map sessions: header/1",
        "server -> KNOWN Map sessions: empty",
        "client -> CONTENT Map header: true new: After: 0 New: 1",
        "server -> KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("handle peer reconnections", async () => {
    const node = createTestNode();

    const group = node.createGroup();

    const map = group.createMap();

    map.set("hello", "world", "trusting");

    connectNodeToSyncServer(node, true);

    await map.core.waitForSync();

    node.syncManager.getPeers()[0]?.gracefulShutdown();

    map.set("hello", "updated", "trusting");

    const { messages } = connectNodeToSyncServer(node, true);

    await map.core.waitForSync();

    const mapOnSyncServer = jazzCloud.node.coValuesStore.get(map.id);

    assert(mapOnSyncServer.state.type === "available");

    expect(
      expectMap(mapOnSyncServer.state.coValue.getCurrentContent()).get("hello"),
    ).toEqual("updated");

    expect(
      toSimplifiedMessages(
        {
          Group: group.core,
          Map: map.core,
        },
        messages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Group sessions: header/3",
        "server -> KNOWN Group sessions: header/3",
        "client -> LOAD Map sessions: header/2",
        "server -> KNOWN Map sessions: header/1",
        "client -> CONTENT Map header: false new: After: 1 New: 1",
        "server -> KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("correctly handle concurrent peer reconnections", async () => {
    const [admin, session] = randomAnonymousAccountAndSessionID();
    const node = new LocalNode(admin, session, Crypto);

    const group = node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    connectNodeToSyncServer(node, true);

    await map.core.waitForSync();

    node.syncManager.getPeers()[0]?.gracefulShutdown();

    map.set("hello", "updated", "trusting");

    const { nodeToServerPeer: stalePeer, messages: stalePeerMessages } =
      connectNodeToSyncServer(node, true);
    const { nodeToServerPeer: latestPeer, messages: latestPeerMessages } =
      connectNodeToSyncServer(node, true);

    await map.core.waitForSync();

    const mapOnSyncServer = jazzCloud.node.coValuesStore.get(map.id);

    assert(mapOnSyncServer.state.type === "available");

    expect(
      expectMap(mapOnSyncServer.state.coValue.getCurrentContent()).get("hello"),
    ).toEqual("updated");

    expect(stalePeer.outgoing).toMatchObject({
      closed: true,
    });

    expect(latestPeer.outgoing).toMatchObject({
      closed: false,
    });

    expect(stalePeerMessages).toEqual([]);

    expect(
      toSimplifiedMessages(
        {
          Group: group.core,
          Map: map.core,
        },
        latestPeerMessages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Group sessions: header/3",
        "server -> KNOWN Group sessions: header/3",
        "client -> LOAD Map sessions: header/2",
        "server -> KNOWN Map sessions: header/1",
        "client -> CONTENT Map header: false new: After: 1 New: 1",
        "server -> KNOWN Map sessions: header/2",
      ]
    `);
  });

  test.skip("handle peer reconnections with data loss", async () => {
    const node = createTestNode();

    const group = node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    connectNodeToSyncServer(node, true);

    await map.core.waitForSync();

    jazzCloud.restart();

    const { messages } = connectNodeToSyncServer(node, true);
    const mapOnSyncServer = jazzCloud.node.coValuesStore.get(map.id);

    await waitFor(() => {
      expect(mapOnSyncServer.state.type).toBe("available");
    });

    assert(mapOnSyncServer.state.type === "available");

    expect(
      toSimplifiedMessages(
        {
          Group: group.core,
          Map: map.core,
        },
        messages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Group sessions: header/3",
        "server -> KNOWN Group sessions: empty",
        "client -> LOAD Map sessions: header/1",
        "server -> KNOWN Map sessions: empty",
      ]
    `);

    expect(
      expectMap(mapOnSyncServer.state.coValue.getCurrentContent()).get("hello"),
    ).toEqual("updated");
  });
});
