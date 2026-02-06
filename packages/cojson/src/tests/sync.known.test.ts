import { beforeEach, describe, expect, test } from "vitest";
import { setCoValueLoadingRetryDelay } from "../config";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  blockMessageTypeOnOutgoingPeer,
  getSyncServerConnectedPeer,
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
} from "./testUtils";

let jazzCloud: ReturnType<typeof setupTestNode>;

// Set a short timeout to make the tests on unavailable complete faster
setCoValueLoadingRetryDelay(100);

beforeEach(async () => {
  // We want to simulate a real world communication that happens asynchronously
  TEST_NODE_CONFIG.withAsyncPeers = true;

  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("sending known coValues", () => {
  test("dependencies are included when responding to a client", async () => {
    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const { node: client } = setupTestNode({
      connected: true,
    });

    map.set("hello2", "world2", "trusting");
    await map.core.waitForSync();

    const mapOnClient = await loadCoValueOrFail(client, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");
    expect(mapOnClient.get("hello2")).toEqual("world2");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 4",
        "server -> client | CONTENT Map header: true new: After: 0 New: 2",
        "client -> server | KNOWN Group sessions: header/4",
        "client -> server | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("dependencies are excluded when responding to a server", async () => {
    // Create a disconnected client
    const { node: client, accountID } = await setupTestAccount({
      connected: false,
    });
    const account = client.expectCurrentAccount(accountID);

    // Prepare a group -- this will be a non-account dependency of a forthcoming map.
    const group = client.createGroup();
    group.addMember("everyone", "writer");

    // Let the queue drain
    await new Promise((resolve) => setTimeout(resolve, 1));

    // Disable transaction verification on the server so it doesn't ask for dependencies.
    jazzCloud.node.syncManager.disableTransactionVerification();

    // Connect the client, but don't setup syncing just yet...
    const { peer } = getSyncServerConnectedPeer({
      peerId: client.getCurrentAgent().id,
      syncServer: jazzCloud.node,
    });

    // Disable reconciliation while we setup syncing because we don't want the
    // server to know about our forthcoming map's dependencies (group + account).
    const blocker = blockMessageTypeOnOutgoingPeer(peer, "load", {});
    client.syncManager.addPeer(peer);
    blocker.unblock();

    // Create a map and set a value on it, this will trigger:
    //   - CONTENT from client to server
    //   - KNOWN from server to client
    //
    // We don't expect any more messages to be sent from client to server in this
    // case because clients shouldn't greedily send dependencies to a server.
    const map = group.createMap();
    await map.core.waitForSync();

    const syncMessages = SyncMessagesLog.getMessages({
      Account: account.core,
      Group: group.core,
      Map: map.core,
    });
    expect(
      syncMessages.some(
        (msg) =>
          msg.includes("CONTENT Account") || msg.includes("CONTENT Group"),
      ),
    ).toBe(false);
  });
});
