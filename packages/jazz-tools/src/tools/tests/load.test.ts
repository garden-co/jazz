import { waitFor } from "@testing-library/dom";
import { cojsonInternals, emptyKnownState } from "cojson";
import { assert, beforeEach, expect, test } from "vitest";
import { Account, Group, co, z } from "../exports.js";
import {
  createJazzTestAccount,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing.js";
import { assertLoaded } from "./utils.js";

cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 10;

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

  const john = await Person.load(map.$jazz.id, { loadAs: alice });
  assertLoaded(john);
  expect(john?.name).toBe("John");
});

test("return null if id is invalid", async () => {
  const Person = co.map({
    name: z.string(),
  });

  const john = await Person.load("test");
  expect(john).toBeNull();
});

test("load a missing optional value (co.optional)", async () => {
  const Dog = co.map({
    name: z.string(),
  });

  const Person = co.map({
    name: z.string(),
    dog: co.optional(Dog),
  });

  const group = Group.create();
  const map = Person.create({ name: "John" }, group);
  group.addMember("everyone", "reader");

  const alice = await createJazzTestAccount();

  const john = await Person.load(map.$jazz.id, {
    loadAs: alice,
    resolve: { dog: true },
  });

  assertLoaded(john);
  expect(john.name).toBe("John");
  expect(john.dog).toBeUndefined();
});

test("load a missing optional value (Schema.optional)", async () => {
  const Dog = co.map({
    name: z.string(),
  });

  const Person = co.map({
    name: z.string(),
    dog: Dog.optional(),
  });

  const group = Group.create();
  const map = Person.create({ name: "John" }, group);
  group.addMember("everyone", "reader");

  const alice = await createJazzTestAccount();

  const john = await Person.load(map.$jazz.id, {
    loadAs: alice,
    resolve: { dog: true },
  });

  assertLoaded(john);
  expect(john.name).toBe("John");
  expect(john.dog).toBeUndefined();
});

test("load a missing optional value (optional discrminatedUnion)", async () => {
  const Dog = co.map({
    type: z.literal("dog"),
    name: z.string(),
  });

  const Cat = co.map({
    type: z.literal("cat"),
    name: z.string(),
  });

  const Person = co.map({
    name: z.string(),
    pet: co.discriminatedUnion("type", [Dog, Cat]).optional(),
  });

  const group = Group.create();
  const map = Person.create({ name: "John" }, group);
  group.addMember("everyone", "reader");

  const alice = await createJazzTestAccount();

  const john = await Person.load(map.$jazz.id, {
    loadAs: alice,
    resolve: { pet: true },
  });

  assertLoaded(john);
  expect(john.name).toBe("John");
  expect(john.pet).toBeUndefined();
});

test("retry an unavailable value", async () => {
  const Person = co.map({
    name: z.string(),
  });

  const currentAccount = Account.getMe();

  // Disconnect the current account
  currentAccount.$jazz.localNode.syncManager
    .getClientPeers()
    .forEach((peer) => {
      peer.gracefulShutdown();
    });

  const group = Group.create();
  const map = Person.create({ name: "John" }, group);
  group.addMember("everyone", "reader");

  const alice = await createJazzTestAccount();

  const promise = Person.load(map.$jazz.id, { loadAs: alice });

  await new Promise((resolve) => setTimeout(resolve));

  // Reconnect the current account
  currentAccount.$jazz.localNode.syncManager.addPeer(
    getPeerConnectedToTestSyncServer(),
  );

  const john = await promise;
  assertLoaded(john);
  expect(john.name).toBe("John");
});

test("returns null if the value is unavailable after retries", async () => {
  const Person = co.map({
    name: z.string(),
  });

  const currentAccount = Account.getMe();

  // Disconnect the current account
  currentAccount.$jazz.localNode.syncManager
    .getServerPeers(currentAccount.$jazz.raw.id)
    .forEach((peer) => {
      peer.gracefulShutdown();
    });

  const group = Group.create();
  const map = Person.create({ name: "John" }, group);
  group.addMember("everyone", "reader");

  const alice = await createJazzTestAccount();

  const john = await Person.load(map.$jazz.id, { loadAs: alice });

  expect(john).toBeNull();
});

test("load works even when the coValue access is granted after the creation", async () => {
  const alice = await createJazzTestAccount();
  const bob = await createJazzTestAccount();

  const Person = co.map({
    name: z.string(),
  });

  const group = Group.create(alice);
  const map = Person.create({ name: "John" }, group);

  group.addMember("everyone", "reader");

  const mapOnBob = await Person.load(map.$jazz.id, { loadAs: bob });

  assertLoaded(mapOnBob);
  expect(mapOnBob.name).toBe("John");
});

test("load a large coValue", async () => {
  const syncServer = await setupJazzTestSync({ asyncPeers: true });

  const Data = co.list(z.string());
  const LargeDataset = co.map({
    metadata: z.object({
      name: z.string(),
      description: z.string(),
      createdAt: z.number(),
    }),
    data: Data,
  });

  const group = Group.create(syncServer);
  const largeMap = LargeDataset.create(
    {
      metadata: {
        name: "Large Dataset",
        description:
          "A dataset with many entries for testing large coValue loading",
        createdAt: Date.now(),
      },
      data: Data.create([], group),
    },
    group,
  );
  group.addMember("everyone", "reader");

  const dataSize = 100 * 1024;
  const chunkSize = 1024;
  const chunks = dataSize / chunkSize;

  const value = "x".repeat(chunkSize);

  for (let i = 0; i < chunks; i++) {
    largeMap.data.$jazz.push(value);
  }

  // Wait for the large coValue to be fully synced
  await largeMap.data.$jazz.raw.core.waitForSync();

  const alice = await createJazzTestAccount();

  // Test loading the large coValue
  const loadedDataset = await LargeDataset.load(largeMap.$jazz.id, {
    loadAs: alice,
    resolve: {
      data: true,
    },
  });

  assertLoaded(loadedDataset);
  expect(loadedDataset.metadata.name).toBe("Large Dataset");
  expect(loadedDataset.metadata.description).toBe(
    "A dataset with many entries for testing large coValue loading",
  );

  expect(loadedDataset.data.length).toBe(chunks);
  expect(loadedDataset.data.$jazz.raw.core.knownState()).toEqual(
    largeMap.data.$jazz.raw.core.knownState(),
  );
});
