import { assert, beforeEach, expect, test } from "vitest";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { type SessionID, isDeletedSessionID } from "../ids.js";
import type { CoValueCore } from "../exports.js";
import {
  fillCoMapWithLargeData,
  importContentIntoNode,
  setupTestAccount,
  setupTestNode,
  loadCoValueOrFail,
  nodeWithRandomAgentAndSessionID,
  waitFor,
} from "./testUtils.js";
import { CO_VALUE_PRIORITY } from "../priority.js";

const Crypto = await WasmCrypto.create();

function makeDeleteMarkerTransaction(
  core: CoValueCore,
  deleteSessionID: SessionID,
  madeAt: number,
) {
  core.makeTransaction(
    [],
    "trusting",
    { deleted: true },
    madeAt,
    deleteSessionID,
  );
  const log = core.verified?.sessions.get(deleteSessionID);
  assert(log?.lastSignature);
  const tx = log.transactions.at(-1);
  assert(tx);
  return { tx, signature: log.lastSignature };
}

let jazzCloud: ReturnType<typeof setupTestNode>;
beforeEach(() => {
  jazzCloud = setupTestNode({ isSyncServer: true });
});

test("deleteCoValue is blocked for Account and Group CoValues", async () => {
  const client = await setupTestAccount();

  const account = client.node.expectCurrentAccount("to test deleteCoValue");
  expect(() => account.core.deleteCoValue()).toThrow(
    /Cannot delete Group or Account coValues/,
  );

  const group = client.node.createGroup();
  expect(() => group.core.deleteCoValue()).toThrow(
    /Cannot delete Group or Account coValues/,
  );
});

test("deleteCoValue throws when called by a non-admin on a group-owned CoValue", async () => {
  const alice = await setupTestAccount({ connected: true });
  const bob = await setupTestAccount({ connected: true });

  const bobAccountOnAlice = await loadCoValueOrFail(alice.node, bob.accountID);

  const group = alice.node.createGroup();
  group.addMember(bobAccountOnAlice, "writer");

  const map = group.createMap();

  // Give sync a moment to propagate the group ownership + membership
  const mapOnBob = await loadCoValueOrFail(bob.node, map.id);

  await waitFor(() => {
    expect(mapOnBob.core.safeGetGroup()?.myRole()).toBe("writer");
  });

  expect(() => mapOnBob.core.deleteCoValue()).toThrow(
    /Only admins can delete coValues/,
  );
});

test("deleteCoValue creates a trusting {deleted:true} tombstone tx, marks the session, and flips core.isDeleted", async () => {
  const alice = await setupTestAccount({ connected: true });

  const group = alice.node.createGroup();
  const map = group.createMap();

  expect(map.core.isDeleted).toBe(false);

  map.core.deleteCoValue();

  expect(map.core.isDeleted).toBe(true);

  const txs = map.core.getValidSortedTransactions();
  const last = txs.at(-1);
  expect(last).toBeTruthy();

  expect(last!.tx.privacy).toBe("trusting");
  expect(last!.changes).toEqual([]);
  expect(last!.meta).toMatchObject({ deleted: true });
  expect(last!.txID.sessionID.endsWith("_deleted")).toBe(true);
});

test("rejects delete marker ingestion from non-admin (ownedByGroup, skipVerify=false)", async () => {
  const alice = await setupTestAccount({ connected: true });
  const bob = await setupTestAccount({ connected: true });

  const bobAccount = await loadCoValueOrFail(alice.node, bob.accountID);
  await loadCoValueOrFail(bob.node, alice.accountID);

  const group = alice.node.createGroup();
  group.addMember(bobAccount, "writer");
  await group.core.waitForSync();

  const map = group.createMap();
  const mapOnBob = await loadCoValueOrFail(bob.node, map.id);

  const deleteSessionID = bob.node.crypto.newDeleteSessionID(
    bob.node.getCurrentAccountOrAgentID(),
  );
  const { tx, signature } = makeDeleteMarkerTransaction(
    mapOnBob.core,
    deleteSessionID,
    Date.now(),
  );

  const error = map.core.tryAddTransactions(
    deleteSessionID,
    [tx],
    signature,
    false,
  );

  expect(error).toMatchObject({
    type: "DeleteTransactionRejected",
    reason: "NotAdmin",
  });
  expect(map.core.isDeleted).toBe(false);
  expect(map.core.verified?.sessions.get(deleteSessionID)).toBeUndefined();
});

test("accepts delete marker ingestion from admin (ownedByGroup, skipVerify=false) and marks deleted", async () => {
  const alice = await setupTestAccount({ connected: true });
  const bob = await setupTestAccount({ connected: true });

  await loadCoValueOrFail(alice.node, bob.accountID);
  await loadCoValueOrFail(bob.node, alice.accountID);

  const group = alice.node.createGroup();
  const bobAccount = await loadCoValueOrFail(alice.node, bob.accountID);
  group.addMember(bobAccount, "writer");
  await group.core.waitForSync();

  const map = group.createMap();
  await loadCoValueOrFail(bob.node, group.id);
  const mapOnBob = await loadCoValueOrFail(bob.node, map.id);

  const deleteSessionID = alice.node.crypto.newDeleteSessionID(
    alice.node.getCurrentAccountOrAgentID(),
  );
  const { tx, signature } = makeDeleteMarkerTransaction(
    map.core,
    deleteSessionID,
    Date.now(),
  );

  const error = mapOnBob.core.tryAddTransactions(
    deleteSessionID,
    [tx],
    signature,
    false,
  );

  expect(error).toBeUndefined();
  expect(mapOnBob.core.isDeleted).toBe(true);
});

test("skipVerify=true ingestion marks deleted even for non-admin delete marker", async () => {
  const alice = await setupTestAccount({ connected: true });
  const bob = await setupTestAccount({ connected: true });

  const bobAccount = await loadCoValueOrFail(alice.node, bob.accountID);
  await loadCoValueOrFail(bob.node, alice.accountID);

  const group = alice.node.createGroup();
  group.addMember(bobAccount, "writer");
  await group.core.waitForSync();

  const map = group.createMap();
  const mapOnBob = await loadCoValueOrFail(bob.node, map.id);

  const deleteSessionID = bob.node.crypto.newDeleteSessionID(
    bob.node.getCurrentAccountOrAgentID(),
  );
  const { tx, signature } = makeDeleteMarkerTransaction(
    mapOnBob.core,
    deleteSessionID,
    Date.now(),
  );

  const error = map.core.tryAddTransactions(
    deleteSessionID,
    [tx],
    signature,
    true,
  );

  expect(error).toBeUndefined();
  expect(map.core.isDeleted).toBe(true);
});

test("rejects delete marker ingestion for non-owned covalue when verifying (skipVerify=false)", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "costream",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const deleteSessionID = node.crypto.newDeleteSessionID(
    node.getCurrentAccountOrAgentID(),
  );
  const { tx, signature } = makeDeleteMarkerTransaction(
    coValue,
    deleteSessionID,
    Date.now(),
  );

  node.internalDeleteCoValue(coValue.id);
  node.syncManager.handleNewContent(
    {
      action: "content",
      id: coValue.id,
      header: coValue.verified!.header,
      priority: CO_VALUE_PRIORITY.LOW,
      new: {},
    },
    "import",
  );

  const newEntry = node.getCoValue(coValue.id);
  const error = newEntry.tryAddTransactions(
    deleteSessionID,
    [tx],
    signature,
    false,
  );

  expect(error).toMatchObject({
    type: "DeleteTransactionRejected",
    reason: "CannotVerifyPermissions",
  });
  expect(newEntry.isDeleted).toBe(false);
});

test("deleted coValues return only the deleted session/transaction on the knownState", async () => {
  const client = setupTestNode({
    connected: true,
  });
  const group = client.node.createGroup();
  const map = group.createMap();
  map.set("hello", "world", "trusting");
  map.core.deleteCoValue();

  const knownState = map.core.knownState();
  expect(
    Object.keys(knownState.sessions).every((sessionID) =>
      isDeletedSessionID(sessionID as SessionID),
    ),
  ).toBe(true);
  expect(Object.keys(knownState.sessions)).toHaveLength(1);
});

test("deleted coValues return only the deleted session/transaction on the knownStateWithStreaming", async () => {
  const streamingClient = setupTestNode();
  const client = await setupTestAccount({ connected: true });
  const group = streamingClient.node.createGroup();

  group.addMemberInternal(client.account, "admin");

  // Import the group content into the client
  importContentIntoNode(group.core, client.node);

  const map = group.createMap();
  fillCoMapWithLargeData(map);

  // Import only partially the map content into the client, to keep it in streaming state
  importContentIntoNode(map.core, client.node, 1);

  const mapOnClient = await loadCoValueOrFail(client.node, map.id);

  expect(mapOnClient.core.isStreaming()).toBe(true);

  mapOnClient.core.deleteCoValue();

  const streamingSessions = mapOnClient.core.knownStateWithStreaming().sessions;

  expect(
    Object.keys(streamingSessions).every((sessionID) =>
      isDeletedSessionID(sessionID as SessionID),
    ),
  ).toBe(true);
  expect(Object.keys(streamingSessions)).toHaveLength(1);
});

test("waitForSync should wait only for the delete session/transaction", async () => {
  const client = setupTestNode({
    connected: true,
  });
  const group = client.node.createGroup();
  const map = group.createMap();
  map.set("hello", "world", "trusting");
  map.core.deleteCoValue();

  await map.core.waitForSync();

  expect(jazzCloud.node.expectCoValueLoaded(map.id).isDeleted).toBe(true);
});

test("waitForSync should wait only for the delete session/transaction even if the coValue loading was in streaming", async () => {
  const streamingClient = setupTestNode();
  const client = await setupTestAccount({ connected: true });
  const group = streamingClient.node.createGroup();

  group.addMemberInternal(client.account, "admin");

  // Import the group content into the client
  importContentIntoNode(group.core, client.node);

  const map = group.createMap();
  fillCoMapWithLargeData(map);

  // Import only partially the map content into the client, to keep it in streaming state
  importContentIntoNode(map.core, client.node, 1);

  const mapOnClient = await loadCoValueOrFail(client.node, map.id);

  expect(mapOnClient.core.isStreaming()).toBe(true);

  mapOnClient.core.deleteCoValue();

  await mapOnClient.core.waitForSync();

  const mapOnSyncServer = jazzCloud.node.expectCoValueLoaded(map.id);

  expect(
    Object.keys(mapOnSyncServer.knownState().sessions).every((sessionID) =>
      isDeletedSessionID(sessionID as SessionID),
    ),
  ).toBe(true);
  expect(jazzCloud.node.expectCoValueLoaded(map.id).isDeleted).toBe(true);
});
