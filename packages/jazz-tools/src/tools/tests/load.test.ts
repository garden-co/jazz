import { cojsonInternals, emptyKnownState } from "cojson";
import { assert, beforeEach, expect, test } from "vitest";
import {
  Account,
  Group,
  co,
  exportCoValue,
  jazzConfig,
  z,
} from "../exports.js";
import { CoValueLoadingState } from "../internal.js";
import {
  createJazzTestAccount,
  disableJazzTestSync,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing.js";
import { assertLoaded, waitFor } from "./utils.js";

cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 10;

let lastError: Error | undefined;
beforeEach(() => {
  lastError = undefined;
  jazzConfig.setCustomErrorReporter((error) => {
    lastError = error;
  });
});
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

test("return 'unavailable' if id is invalid", async () => {
  const Person = co.map({
    name: z.string(),
  });

  const john = await Person.load("test");
  expect(john.$jazz.loadingState).toBe(CoValueLoadingState.UNAVAILABLE);
  expect(lastError?.message).toBe(
    "Jazz Unavailable Error: unable to load test",
  );
});

test("return 'unavailable' with sync hint when syncWhen is 'never'", async () => {
  const Person = co.map({
    name: z.string(),
  });

  const account = Account.getMe();
  (account.$jazz.localNode as { syncWhen: string }).syncWhen = "never";

  const john = await Person.load("test");
  expect(john.$jazz.loadingState).toBe(CoValueLoadingState.UNAVAILABLE);
  expect(lastError?.message).toBe(
    'Jazz Unavailable Error: unable to load test. Sync is disabled (when: "never"), so this CoValue can only be loaded from local storage.',
  );
});

test("return 'unavailable' with sync hint when syncWhen is 'signedUp'", async () => {
  const Person = co.map({
    name: z.string(),
  });

  const account = Account.getMe();
  (account.$jazz.localNode as { syncWhen: string }).syncWhen = "signedUp";

  const john = await Person.load("test");
  expect(john.$jazz.loadingState).toBe(CoValueLoadingState.UNAVAILABLE);
  expect(lastError?.message).toBe(
    "Jazz Unavailable Error: unable to load test. Sync is set to when: \"signedUp\" — if the user hasn't signed up, the CoValue can't be loaded from the server.",
  );
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

test("returns 'unavailable' if the value is unavailable after retries", async () => {
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

  expect(john.$jazz.loadingState).toBe(CoValueLoadingState.UNAVAILABLE);
  expect(lastError?.message).toBe(
    "Jazz Unavailable Error: unable to load " + map.$jazz.id,
  );
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

test("should wait for the full streaming of the group", async () => {
  disableJazzTestSync();

  const alice = await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });

  const Person = co.map({
    name: z.string(),
    update: z.number(),
  });

  const group = Group.create();

  const person = Person.create(
    {
      name: "Bob",
      update: 1,
    },
    group,
  );

  // Make the group to grow big enough to trigger the streaming
  for (let i = 0; i <= 300; i++) {
    group.$jazz.raw.rotateReadKey();
  }

  group.addMember("everyone", "reader");

  const bob = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  const personContent = await exportCoValue(Person, person.$jazz.id, {
    loadAs: alice,
  });
  assert(personContent);

  const lastGroupPiece = personContent.findLast(
    (content) => content.id === group.$jazz.id,
  );
  assert(lastGroupPiece);

  for (const content of personContent.filter(
    (content) => content !== lastGroupPiece,
  )) {
    bob.$jazz.localNode.syncManager.handleNewContent(content, "import");
  }

  // Simulate the streaming delay on the last piece of the group
  setTimeout(() => {
    bob.$jazz.localNode.syncManager.handleNewContent(lastGroupPiece, "import");
  }, 10);

  // Load the value and expect the migration to run only once
  const loadedPerson = await Person.load(person.$jazz.id, { loadAs: bob });

  assertLoaded(loadedPerson);
  expect(loadedPerson.$jazz.owner.$jazz.raw.core.verified.isStreaming()).toBe(
    false,
  );
});

test("should wait for the full streaming of the parent groups", async () => {
  disableJazzTestSync();

  const alice = await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });

  const Person = co.map({
    name: z.string(),
    update: z.number(),
  });

  const parentGroup = Group.create();
  const group = Group.create();

  const person = Person.create(
    {
      name: "Bob",
      update: 1,
    },
    group,
  );

  // Make the parent group to grow big enough to trigger the streaming
  for (let i = 0; i <= 300; i++) {
    parentGroup.$jazz.raw.rotateReadKey();
  }

  group.addMember(parentGroup);
  parentGroup.addMember("everyone", "reader");

  const bob = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  const personContent = await exportCoValue(Person, person.$jazz.id, {
    loadAs: alice,
  });
  assert(personContent);

  const lastParentGroupPiece = personContent.findLast(
    (content) => content.id === parentGroup.$jazz.id,
  );
  assert(lastParentGroupPiece);

  for (const content of personContent.filter(
    (content) => content !== lastParentGroupPiece,
  )) {
    bob.$jazz.localNode.syncManager.handleNewContent(content, "import");
  }

  // Simulate the streaming delay on the last piece of the parent group
  setTimeout(() => {
    bob.$jazz.localNode.syncManager.handleNewContent(
      lastParentGroupPiece,
      "import",
    );
  }, 10);

  // Load the value and expect the migration to run only once
  const loadedPerson = await Person.load(person.$jazz.id, { loadAs: bob });

  assertLoaded(loadedPerson);
  expect(loadedPerson.$jazz.owner.$jazz.raw.core.verified.isStreaming()).toBe(
    false,
  );
});

test("should correctly reject the load if after the group streaming the account has no access", async () => {
  disableJazzTestSync();

  const alice = await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });

  const Person = co.map({
    name: z.string(),
    update: z.number(),
  });

  const group = Group.create();

  const person = Person.create(
    {
      name: "Bob",
      update: 1,
    },
    group,
  );

  group.addMember("everyone", "reader");

  for (let i = 0; i <= 150; i++) {
    group.$jazz.raw.rotateReadKey();
  }

  group.removeMember("everyone");

  const bob = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  const personContent = await exportCoValue(Person, person.$jazz.id, {
    loadAs: alice,
  });
  assert(personContent);

  const lastGroupPiece = personContent.findLast(
    (content) => content.id === group.$jazz.id,
  );
  assert(lastGroupPiece);

  for (const content of personContent.filter(
    (content) => content !== lastGroupPiece,
  )) {
    bob.$jazz.localNode.syncManager.handleNewContent(content, "import");
  }

  // Simulate the streaming delay on the last piece of the group
  setTimeout(() => {
    bob.$jazz.localNode.syncManager.handleNewContent(lastGroupPiece, "import");
  }, 10);

  // Load the value and expect the migration to run only once
  const loadedPerson = await Person.load(person.$jazz.id, { loadAs: bob });
  expect(loadedPerson.$isLoaded).toEqual(false);
  expect(loadedPerson.$jazz.loadingState).toEqual(
    CoValueLoadingState.UNAVAILABLE,
  );

  await waitFor(async () => {
    const loadedPerson = await Person.load(person.$jazz.id, { loadAs: bob });
    expect(loadedPerson.$isLoaded).toEqual(false);
    expect(loadedPerson.$jazz.loadingState).toEqual(
      CoValueLoadingState.UNAUTHORIZED,
    );
  });
});
