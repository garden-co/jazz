import { expect, test } from "vitest";
import { expectAccount } from "../coValues/account.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { LocalNode } from "../localNode.js";
import { connectedPeers } from "../streamUtils.js";
import { createAsyncStorage } from "./testStorage.js";

const Crypto = await WasmCrypto.create();

test("Can create a node while creating a new account with profile", async () => {
  const { node, accountID, accountSecret, sessionID } =
    await LocalNode.withNewlyCreatedAccount({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

  expect(node).not.toBeNull();
  expect(accountID).not.toBeNull();
  expect(accountSecret).not.toBeNull();
  expect(sessionID).not.toBeNull();

  expect(node.expectProfileLoaded(accountID).get("name")).toEqual(
    "Hermes Puggington",
  );
});

test("A node with an account can create groups and and objects within them", async () => {
  const { node, accountID } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const group = await node.createGroup();
  expect(group).not.toBeNull();

  const map = group.createMap();
  map.set("foo", "bar", "private");
  expect(map.get("foo")).toEqual("bar");
  expect(map.lastEditAt("foo")?.by).toEqual(accountID);
});

test("Can create account with one node, and then load it on another", async () => {
  const { node, accountID, accountSecret } =
    await LocalNode.withNewlyCreatedAccount({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

  const group = await node.createGroup();
  expect(group).not.toBeNull();

  const map = group.createMap();
  map.set("foo", "bar", "private");
  expect(map.get("foo")).toEqual("bar");

  const [node1asPeer, node2asPeer] = connectedPeers("node1", "node2", {
    peer1role: "server",
    peer2role: "client",
  });

  node.syncManager.addPeer(node2asPeer);

  const node2 = await LocalNode.withLoadedAccount({
    accountID,
    accountSecret,
    sessionID: Crypto.newRandomSessionID(accountID),
    peers: [node1asPeer],
    crypto: Crypto,
  });

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") throw new Error("Map unavailable");

  expect(map2.get("foo")).toEqual("bar");
});

test("Should migrate the root from private to trusting", async () => {
  const { node, accountID, accountSecret } =
    await LocalNode.withNewlyCreatedAccount({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

  const group = await node.createGroup();
  expect(group).not.toBeNull();

  const map = group.createMap();
  map.set("foo", "bar", "private");
  expect(map.get("foo")).toEqual("bar");

  const peers1 = connectedPeers("node1", "node2", {
    peer1role: "server",
    peer2role: "client",
  });

  const account = await node.load(accountID);
  if (account === "unavailable") throw new Error("Account unavailable");

  account.set("root", map.id, "private");

  node.syncManager.addPeer(peers1[1]);

  const node2 = await LocalNode.withLoadedAccount({
    accountID,
    accountSecret,
    sessionID: Crypto.newRandomSessionID(accountID),
    peers: [peers1[0]],
    crypto: Crypto,
  });

  const account2 = await node2.load(accountID);
  if (account2 === "unavailable") throw new Error("Account unavailable");

  expect(account2.getRaw("root")?.trusting).toEqual(true);

  node2.gracefulShutdown(); // Stop getting updates from node1

  const peers2 = connectedPeers("node2", "node3", {
    peer1role: "server",
    peer2role: "client",
  });

  node.syncManager.addPeer(peers2[1]);

  const node3 = await LocalNode.withLoadedAccount({
    accountID,
    accountSecret,
    sessionID: Crypto.newRandomSessionID(accountID),
    peers: [peers2[0]],
    crypto: Crypto,
  });

  const account3 = await node3.load(accountID);
  if (account3 === "unavailable") throw new Error("Account unavailable");

  expect(account3.getRaw("root")?.trusting).toEqual(true);
  expect(account3.ops).toEqual(account2.ops); // No new transactions were made
});

test("myRole returns 'admin' for the current account", async () => {
  const { node, accountID } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const account = await node.load(accountID);
  if (account === "unavailable") throw new Error("Account unavailable");

  expect(account.myRole()).toEqual("admin");
});

test("roleOf returns 'admin' when the accountID is the same as the receiver account", async () => {
  const { node, accountID } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const account = await node.load(accountID);
  if (account === "unavailable") throw new Error("Account unavailable");

  expect(account.roleOf(accountID)).toEqual("admin");
});

test("throws an error if the user tried to add a member to an account", async () => {
  const { node, accountID } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const account = await node.load(accountID);
  if (account === "unavailable") throw new Error("Account unavailable");

  expect(() => account.addMember("everyone", "admin")).toThrow(
    "Cannot add a member to an account",
  );
});

test("throws an error if the user tried to remove a member from an account", async () => {
  const { node, accountID } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const account = await node.load(accountID);
  if (account === "unavailable") throw new Error("Account unavailable");

  expect(() => account.removeMember("everyone")).toThrow(
    "Cannot remove a member from an account",
  );
});

test("throws an error if the user tried to extend an account", async () => {
  const { node, accountID } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const account = await node.load(accountID);
  if (account === "unavailable") throw new Error("Account unavailable");

  expect(() => account.extend(node.createGroup())).toThrow(
    "Cannot extend an account",
  );
});

test("throws an error if the user tried to revoke extend from an account", async () => {
  const { node, accountID } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const account = await node.load(accountID);
  if (account === "unavailable") throw new Error("Account unavailable");

  expect(() => account.revokeExtend(node.createGroup())).toThrow(
    "Cannot unextend an account",
  );
});

test("throws an error if the user tried to create an invite from an account", async () => {
  const { node, accountID } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const account = await node.load(accountID);
  if (account === "unavailable") throw new Error("Account unavailable");

  expect(() => account.createInvite("admin")).toThrow(
    "Cannot create invite from an account",
  );
});

test("wait for storage sync before resolving withNewlyCreatedAccount", async () => {
  const storage = await createAsyncStorage({
    nodeName: "account-node",
    storageName: "storage",
  });

  const { accountID, node } = await LocalNode.withNewlyCreatedAccount({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
    storage,
  });

  expect(storage.getKnownState(accountID).header).toBe(true);

  const profileId = expectAccount(
    node.getCoValue(accountID).getCurrentContent(),
  ).get("profile")!;

  expect(storage.getKnownState(profileId).header).toBe(true);
});
