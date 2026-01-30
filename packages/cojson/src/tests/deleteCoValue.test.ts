import { assert, beforeEach, expect, test } from "vitest";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { type SessionID, isDeleteSessionID } from "../ids.js";
import type { CoValueCore } from "../exports.js";
import {
  fillCoMapWithLargeData,
  importContentIntoNode,
  setupTestAccount,
  setupTestNode,
  loadCoValueOrFail,
  nodeWithRandomAgentAndSessionID,
  hotSleep,
  waitFor,
} from "./testUtils.js";
import { CO_VALUE_PRIORITY } from "../priority.js";

const Crypto = await WasmCrypto.create();

function makeDeleteMarkerTransaction(core: CoValueCore, madeAt?: number) {
  core.makeTransaction([], "trusting", { deleted: core.id }, madeAt);
  const deleteSessionID = Object.keys(core.knownState().sessions).find(
    (sessionID) => isDeleteSessionID(sessionID as SessionID),
  ) as SessionID;
  assert(deleteSessionID);
  const log = core.verified?.getSession(deleteSessionID);
  assert(log?.lastSignature);
  const tx = log.transactions.at(-1);
  assert(tx);
  return { tx, signature: log.lastSignature, deleteSessionID };
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
    /The current account lacks admin permissions to delete this coValue/,
  );
});

test("deleteCoValue creates a trusting {deleted:id} tombstone tx, marks the session, and flips core.isDeleted", async () => {
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
  expect(last!.meta).toMatchObject({ deleted: map.id });
  expect(last!.txID.sessionID).toMatch(/_session_d[1-9A-HJ-NP-Za-km-z]+\$$/); // Delete session format
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

  const { tx, signature, deleteSessionID } = makeDeleteMarkerTransaction(
    mapOnBob.core,
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
  expect(map.core.verified?.getSession(deleteSessionID)).toBeUndefined();
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

  const { tx, signature, deleteSessionID } = makeDeleteMarkerTransaction(
    map.core,
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

test("rejects delete session ingestion when attempting to append a second transaction (txCount > 0)", async () => {
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

  const { tx, signature, deleteSessionID } = makeDeleteMarkerTransaction(
    map.core,
  );

  const first = mapOnBob.core.tryAddTransactions(
    deleteSessionID,
    [tx],
    signature,
    false,
  );

  expect(first).toBeUndefined();
  expect(mapOnBob.core.isDeleted).toBe(true);
  expect(
    mapOnBob.core.verified?.getSession(deleteSessionID)?.transactions,
  ).toHaveLength(1);

  const second = mapOnBob.core.tryAddTransactions(
    deleteSessionID,
    [tx],
    signature,
    false,
  );

  expect(second).toMatchObject({
    type: "DeleteTransactionRejected",
    reason: "InvalidDeleteTransaction",
  });
  expect(second && "error" in second).toBe(true);
  const secondErr = (second as { error: unknown }).error;
  expect(secondErr).toBeInstanceOf(Error);
  if (secondErr instanceof Error) {
    expect(secondErr.message).toMatch(
      /Delete transaction must be the only transaction in the session/,
    );
  }
  expect(
    mapOnBob.core.verified?.getSession(deleteSessionID)?.transactions,
  ).toHaveLength(1);
});

test("rejects delete session ingestion when attempting to add multiple delete transactions", async () => {
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

  const { tx, signature, deleteSessionID } = makeDeleteMarkerTransaction(
    map.core,
  );

  const first = mapOnBob.core.tryAddTransactions(
    deleteSessionID,
    [tx, tx],
    signature,
    false,
  );

  expect(first).toMatchObject({
    type: "DeleteTransactionRejected",
    reason: "InvalidDeleteTransaction",
  });
  expect(first && "error" in first).toBe(true);
  const err = (first as { error: unknown }).error;
  expect(err).toBeInstanceOf(Error);
  if (err instanceof Error) {
    expect(err.message).toMatch(
      /Delete transaction must be the only transaction in the session/,
    );
  }
  expect(mapOnBob.core.verified?.getSession(deleteSessionID)).toBeUndefined();
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

  const { tx, signature, deleteSessionID } = makeDeleteMarkerTransaction(
    mapOnBob.core,
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

test("rejects delete marker ingestion when tx.madeAt predates admin rights (time travel permission check)", async () => {
  const alice = await setupTestAccount({ connected: true });
  const bob = await setupTestAccount({ connected: true });

  const bobAccount = await loadCoValueOrFail(alice.node, bob.accountID);
  await loadCoValueOrFail(bob.node, alice.accountID);

  const group = alice.node.createGroup();
  group.addMember(bobAccount, "writer");
  await group.core.waitForSync();

  const map = group.createMap();
  const mapOnBob = await loadCoValueOrFail(bob.node, map.id);

  // Ensure Bob is still a writer at tx creation time
  await waitFor(() => {
    expect(mapOnBob.core.safeGetGroup()?.myRole()).toBe("writer");
  });

  await new Promise((resolve) => setTimeout(resolve, 10));

  const { tx, signature, deleteSessionID } = makeDeleteMarkerTransaction(
    mapOnBob.core,
  );

  // Later, Bob gets admin rights...
  await new Promise((resolve) => setTimeout(resolve, 10));

  group.addMember(bobAccount, "admin");
  await group.core.waitForSync();

  // ...but ingestion should still validate permissions at tx.madeAt (writer), not "now" (admin).
  expect(group.roleOf(bob.accountID)).toBe("admin");

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
});

test("rejects delete marker ingestion for non-owned covalue when verifying (skipVerify=false)", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "costream",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const { tx, signature, deleteSessionID } =
    makeDeleteMarkerTransaction(coValue);

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
      isDeleteSessionID(sessionID as SessionID),
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
      isDeleteSessionID(sessionID as SessionID),
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
      isDeleteSessionID(sessionID as SessionID),
    ),
  ).toBe(true);
  expect(jazzCloud.node.expectCoValueLoaded(map.id).isDeleted).toBe(true);
});

test("rejects delete transaction with mismatched coValueId", async () => {
  const alice = await setupTestAccount({ connected: true });
  const bob = await setupTestAccount({ connected: true });

  await loadCoValueOrFail(alice.node, bob.accountID);
  await loadCoValueOrFail(bob.node, alice.accountID);

  const group = alice.node.createGroup();
  const bobAccount = await loadCoValueOrFail(alice.node, bob.accountID);
  group.addMember(bobAccount, "admin");
  await group.core.waitForSync();

  // Create two maps owned by the same group
  const mapA = group.createMap();
  const mapB = group.createMap();

  await loadCoValueOrFail(bob.node, group.id);
  const mapAOnBob = await loadCoValueOrFail(bob.node, mapA.id);
  const mapBOnBob = await loadCoValueOrFail(bob.node, mapB.id);

  // Create a delete transaction for mapA
  const { tx, signature, deleteSessionID } = makeDeleteMarkerTransaction(
    mapA.core,
  );

  // Try to apply mapA's delete transaction to mapB - should be rejected due to ID mismatch
  const error = mapBOnBob.core.tryAddTransactions(
    deleteSessionID,
    [tx],
    signature,
    false,
  );

  expect(error).toMatchObject({
    type: "DeleteTransactionRejected",
    reason: "InvalidDeleteTransaction",
  });
  expect(error && "error" in error).toBe(true);
  const err = (error as { error: unknown }).error;
  expect(err).toBeInstanceOf(Error);
  if (err instanceof Error) {
    expect(err.message).toMatch(/Delete transaction ID mismatch/);
  }
  expect(mapBOnBob.core.isDeleted).toBe(false);

  // Verify mapA's delete transaction can still be applied correctly to mapA
  const successError = mapAOnBob.core.tryAddTransactions(
    deleteSessionID,
    [tx],
    signature,
    false,
  );
  expect(successError).toBeUndefined();
  expect(mapAOnBob.core.isDeleted).toBe(true);
});
