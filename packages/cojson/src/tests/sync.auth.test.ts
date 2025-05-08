import { assert, beforeEach, describe, expect, test } from "vitest";

import { WasmCrypto } from "../crypto/WasmCrypto";
import { LocalNode } from "../localNode";
import {
  SyncMessagesLog,
  getSyncServerConnectedPeer,
  setupTestNode,
} from "./testUtils";

const Crypto = await WasmCrypto.create();
let jazzCloud = setupTestNode({ isSyncServer: true });

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
      peersToLoadFrom: [peer],
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
        "client -> server | CONTENT Account header: true new: After: 0 New: 4",
        "server -> client | KNOWN Account sessions: header/4",
        "client -> server | CONTENT ProfileGroup header: true new: After: 0 New: 5",
        "server -> client | KNOWN ProfileGroup sessions: header/5",
        "client -> server | CONTENT Profile header: true new: After: 0 New: 1",
        "server -> client | KNOWN Profile sessions: header/1",
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
      peersToLoadFrom: [peer],
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
        "client -> server | CONTENT Account header: true new: After: 0 New: 5",
        "server -> client | KNOWN Account sessions: header/5",
        "client -> server | CONTENT Root header: true new: After: 0 New: 1",
        "server -> client | KNOWN Root sessions: header/1",
        "client -> server | CONTENT Profile header: true new: After: 0 New: 1",
        "server -> client | KNOWN Profile sessions: header/1",
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
        peersToLoadFrom: [newAccountPeer],
        crypto: Crypto,
      });

    const { peer: existingAccountPeer } = getSyncServerConnectedPeer({
      peerId: "existing-account",
      ourName: "auth-node",
    });

    const node = await LocalNode.withLoadedAccount({
      accountID,
      accountSecret,
      peersToLoadFrom: [existingAccountPeer],
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
        "creation-node -> server | CONTENT Account header: true new: After: 0 New: 4",
        "auth-node -> server | LOAD Account sessions: empty",
        "server -> creation-node | KNOWN Account sessions: header/4",
        "creation-node -> server | CONTENT ProfileGroup header: true new: After: 0 New: 5",
        "server -> auth-node | CONTENT Account header: true new: After: 0 New: 4",
        "server -> creation-node | KNOWN ProfileGroup sessions: header/5",
        "creation-node -> server | CONTENT Profile header: true new: After: 0 New: 1",
        "auth-node -> server | KNOWN Account sessions: header/4",
        "server -> creation-node | KNOWN Profile sessions: header/1",
        "auth-node -> server | LOAD Profile sessions: empty",
        "server -> auth-node | CONTENT ProfileGroup header: true new: After: 0 New: 5",
        "auth-node -> server | KNOWN ProfileGroup sessions: header/5",
        "server -> auth-node | CONTENT Profile header: true new: After: 0 New: 1",
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
      peersToLoadFrom: [newAccountPeer],
      crypto: Crypto,
    });

    creationNode.gracefulShutdown();

    const { peer: existingAccountPeer } = getSyncServerConnectedPeer({
      peerId: "existing-account",
      ourName: "auth-node",
    });

    const node = await LocalNode.withLoadedAccount({
      accountID,
      accountSecret,
      peersToLoadFrom: [existingAccountPeer],
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
        "creation-node -> server | CONTENT Account header: true new: After: 0 New: 4",
        "auth-node -> server | LOAD Account sessions: empty",
        "server -> creation-node | KNOWN Account sessions: header/4",
        "server -> auth-node | CONTENT Account header: true new: After: 0 New: 4",
        "auth-node -> server | KNOWN Account sessions: header/4",
        "auth-node -> server | LOAD Profile sessions: empty",
        "server -> auth-node | KNOWN Profile sessions: empty",
        "auth-node -> server | LOAD Profile sessions: empty",
        "server -> auth-node | KNOWN Profile sessions: empty",
      ]
    `);
  });
});
