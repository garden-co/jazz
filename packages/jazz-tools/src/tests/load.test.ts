import { cojsonInternals } from "cojson";
import { beforeEach, expect, test } from "vitest";
import { Account, Group, co, z } from "../exports.js";
import {
  createJazzTestAccount,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing.js";

beforeEach(async () => {
  await setupJazzTestSync();
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

test("load a value", async () => {
  const Person = co.map({
    name: z.string(),
  });

  const group = Group.create();
  const map = Person.create({ name: "John" }, group);
  group.addMember("everyone", "reader");

  const alice = await createJazzTestAccount();

  const john = await Person.load(map.id, { loadAs: alice });
  expect(john).not.toBeNull();
  expect(john?.name).toBe("John");
});

test("retry an unavailable a value", async () => {
  const Person = co.map({
    name: z.string(),
  });

  const currentAccount = Account.getMe();

  // Disconnect the current account
  currentAccount._raw.core.node.syncManager.getPeers().forEach((peer) => {
    peer.gracefulShutdown();
  });

  const group = Group.create();
  const map = Person.create({ name: "John" }, group);
  group.addMember("everyone", "reader");

  const alice = await createJazzTestAccount();

  let resolved = false;
  const promise = Person.load(map.id, { loadAs: alice });
  promise.then(() => {
    resolved = true;
  });

  await new Promise((resolve) =>
    setTimeout(
      resolve,
      cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY - 100,
    ),
  );

  expect(resolved).toBe(false);

  // Reconnect the current account
  currentAccount._raw.core.node.syncManager.addPeer(
    getPeerConnectedToTestSyncServer(),
  );

  const john = await promise;
  expect(john).not.toBeNull();
  expect(john?.name).toBe("John");
});

test("returns null if the value is unavailable after retries", async () => {
  const Person = co.map({
    name: z.string(),
  });

  const currentAccount = Account.getMe();

  // Disconnect the current account
  currentAccount._raw.core.node.syncManager.getPeers().forEach((peer) => {
    peer.gracefulShutdown();
  });

  const group = Group.create();
  const map = Person.create({ name: "John" }, group);
  group.addMember("everyone", "reader");

  const alice = await createJazzTestAccount();

  let resolved = false;
  const promise = Person.load(map.id, { loadAs: alice });
  promise.then(() => {
    resolved = true;
  });

  await new Promise((resolve) =>
    setTimeout(
      resolve,
      cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY + 100,
    ),
  );

  expect(resolved).toBe(true);

  // Reconnect the current account
  currentAccount._raw.core.node.syncManager.addPeer(
    getPeerConnectedToTestSyncServer(),
  );

  const john = await promise;
  expect(john).toBeNull();
});
