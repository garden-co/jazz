import { beforeEach, expect, test } from "vitest";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import {
  setupTestAccount,
  setupTestNode,
  loadCoValueOrFail,
  waitFor,
} from "./testUtils.js";

const Crypto = await WasmCrypto.create();

beforeEach(() => {
  setupTestNode({ isSyncServer: true });
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
  expect(last!.txID.sessionID).toContain("_deleted_");
});
