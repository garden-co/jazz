import { assert, beforeEach, describe, expect, test } from "vitest";

import { WasmCrypto } from "../crypto/WasmCrypto";
import { LocalNode } from "../localNode";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  getSyncServerConnectedPeer,
  setupTestNode,
} from "./testUtils";

const Crypto = await WasmCrypto.create();
let jazzCloud: ReturnType<typeof setupTestNode>;

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("LocalNode auth sync", () => {
  test("create a new account", async () => {
    const { peer } = getSyncServerConnectedPeer({
      peerId: "new-account",
    });

    const { node } = await LocalNode.withNewlyCreatedAccount({
      creationProps: {
        name: "new-account",
      },
      peers: [peer],
      crypto: Crypto,
    });

    const account = node.expectCurrentAccount("after login");
    await account.core.waitForSync();

    const profileID = account.get("profile")!;

    const profileCoreOnSyncServer = jazzCloud.node.getCoValue(profileID);

    expect(profileCoreOnSyncServer.isAvailable()).toBe(true);

    assert(profileCoreOnSyncServer.isAvailable());

    expect(
      SyncMessagesLog.getMessages({
        Account: account.core,
        Profile: profileCoreOnSyncServer,
        ProfileGroup: profileCoreOnSyncServer.getGroup().core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Account header: true new: After: 0 New: 3",
        "client -> server | CONTENT ProfileGroup header: true new: After: 0 New: 6",
        "client -> server | CONTENT Profile header: true new: After: 0 New: 1",
        "client -> server | CONTENT Account header: false new: After: 3 New: 1",
        "server -> client | KNOWN Account sessions: header/3",
        "server -> client | KNOWN ProfileGroup sessions: header/6",
        "server -> client | KNOWN Profile sessions: header/1",
        "server -> client | KNOWN Account sessions: header/4",
      ]
    `);
  });

  test("create a new account with a migration", async () => {
    const { peer } = getSyncServerConnectedPeer({
      peerId: "new-account",
    });

    const { node } = await LocalNode.withNewlyCreatedAccount({
      creationProps: {
        name: "new-account",
      },
      peers: [peer],
      crypto: Crypto,
      async migration(account) {
        const root = account.createMap();
        const profile = account.createMap();

        root.set("hello", "world");
        profile.set("name", "new-account");

        account.set("root", root.id);
        account.set("profile", profile.id);

        await root.core.waitForSync();
        await profile.core.waitForSync();
      },
    });

    const account = node.expectCurrentAccount("after login");
    await account.core.waitForSync();

    const rootID = account.get("root")!;
    const profileID = account.get("profile")!;

    const rootCoreOnSyncServer = jazzCloud.node.getCoValue(rootID);
    expect(rootCoreOnSyncServer.isAvailable()).toBe(true);

    const profileCoreOnSyncServer = jazzCloud.node.getCoValue(profileID);

    expect(profileCoreOnSyncServer.isAvailable()).toBe(true);

    assert(profileCoreOnSyncServer.isAvailable());
    assert(rootCoreOnSyncServer.isAvailable());

    expect(
      SyncMessagesLog.getMessages({
        Account: account.core,
        Root: rootCoreOnSyncServer,
        Profile: profileCoreOnSyncServer,
        ProfileGroup: profileCoreOnSyncServer.getGroup().core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Account header: true new: After: 0 New: 3",
        "client -> server | CONTENT Root header: true new: ",
        "client -> server | CONTENT Profile header: true new: ",
        "client -> server | CONTENT Root header: false new: After: 0 New: 1",
        "client -> server | CONTENT Profile header: false new: After: 0 New: 1",
        "client -> server | CONTENT Account header: false new: After: 3 New: 2",
        "server -> client | KNOWN Account sessions: header/3",
        "server -> client | KNOWN Root sessions: header/0",
        "server -> client | KNOWN Profile sessions: header/0",
        "server -> client | KNOWN Root sessions: header/1",
        "server -> client | KNOWN Profile sessions: header/1",
        "server -> client | KNOWN Account sessions: header/5",
      ]
    `);
  });

  test("authenticate to an existing account", async () => {
    const { peer: newAccountPeer } = getSyncServerConnectedPeer({
      peerId: "new-account",
      ourName: "creation-node",
    });

    const { accountID, accountSecret } =
      await LocalNode.withNewlyCreatedAccount({
        creationProps: {
          name: "new-account",
        },
        peers: [newAccountPeer],
        crypto: Crypto,
      });

    const { peer: existingAccountPeer } = getSyncServerConnectedPeer({
      peerId: "existing-account",
      ourName: "auth-node",
    });

    const node = await LocalNode.withLoadedAccount({
      accountID,
      accountSecret,
      peers: [existingAccountPeer],
      sessionID: undefined,
      crypto: Crypto,
    });

    const account = node.expectCurrentAccount("after login");
    const profile = node.getCoValue(account.get("profile")!);

    assert(profile.isAvailable());

    expect(account.id).toBe(accountID);
    expect(node.agentSecret).toBe(accountSecret);

    expect(
      SyncMessagesLog.getMessages({
        Account: account.core,
        Profile: profile,
        ProfileGroup: profile.getGroup().core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "creation-node -> server | CONTENT Account header: true new: After: 0 New: 3",
        "creation-node -> server | CONTENT ProfileGroup header: true new: After: 0 New: 6",
        "creation-node -> server | CONTENT Profile header: true new: After: 0 New: 1",
        "creation-node -> server | CONTENT Account header: false new: After: 3 New: 1",
        "auth-node -> server | LOAD Account sessions: empty",
        "server -> creation-node | KNOWN Account sessions: header/3",
        "server -> creation-node | KNOWN ProfileGroup sessions: header/6",
        "server -> creation-node | KNOWN Profile sessions: header/1",
        "server -> creation-node | KNOWN Account sessions: header/4",
        "server -> auth-node | CONTENT Account header: true new: After: 0 New: 4",
        "auth-node -> server | KNOWN Account sessions: header/4",
        "auth-node -> server | LOAD Profile sessions: empty",
        "server -> auth-node | CONTENT ProfileGroup header: true new: After: 0 New: 6",
        "server -> auth-node | CONTENT Profile header: true new: After: 0 New: 1",
        "auth-node -> server | KNOWN ProfileGroup sessions: header/6",
        "auth-node -> server | KNOWN Profile sessions: header/1",
      ]
    `);
  });

  test("authenticate to an existing account after immediately close the creation node", async () => {
    const { peer: newAccountPeer } = getSyncServerConnectedPeer({
      peerId: "new-account",
      ourName: "creation-node",
    });

    const {
      accountID,
      accountSecret,
      node: creationNode,
    } = await LocalNode.withNewlyCreatedAccount({
      creationProps: {
        name: "new-account",
      },
      peers: [newAccountPeer],
      crypto: Crypto,
    });

    await creationNode.syncManager.waitForAllCoValuesSync();
    creationNode.gracefulShutdown();

    const { peer: existingAccountPeer } = getSyncServerConnectedPeer({
      peerId: "existing-account",
      ourName: "auth-node",
    });

    const node = await LocalNode.withLoadedAccount({
      accountID,
      accountSecret,
      peers: [existingAccountPeer],
      sessionID: undefined,
      crypto: Crypto,
    });

    const account = node.expectCurrentAccount("after login");
    const profile = creationNode.getCoValue(account.get("profile")!);

    assert(profile.isAvailable());

    expect(account.id).toBe(accountID);
    expect(node.agentSecret).toBe(accountSecret);

    expect(
      SyncMessagesLog.getMessages({
        Account: account.core,
        Profile: profile,
        ProfileGroup: profile.getGroup().core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "creation-node -> server | CONTENT Account header: true new: After: 0 New: 3",
        "creation-node -> server | CONTENT ProfileGroup header: true new: After: 0 New: 6",
        "creation-node -> server | CONTENT Profile header: true new: After: 0 New: 1",
        "creation-node -> server | CONTENT Account header: false new: After: 3 New: 1",
        "server -> creation-node | KNOWN Account sessions: header/3",
        "server -> creation-node | KNOWN ProfileGroup sessions: header/6",
        "server -> creation-node | KNOWN Profile sessions: header/1",
        "server -> creation-node | KNOWN Account sessions: header/4",
        "auth-node -> server | LOAD Account sessions: empty",
        "server -> auth-node | CONTENT Account header: true new: After: 0 New: 4",
        "auth-node -> server | KNOWN Account sessions: header/4",
        "auth-node -> server | LOAD Profile sessions: empty",
        "server -> auth-node | CONTENT ProfileGroup header: true new: After: 0 New: 6",
        "server -> auth-node | CONTENT Profile header: true new: After: 0 New: 1",
        "auth-node -> server | KNOWN ProfileGroup sessions: header/6",
        "auth-node -> server | KNOWN Profile sessions: header/1",
      ]
    `);
  });
});
