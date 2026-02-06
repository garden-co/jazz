import { cojsonInternals } from "cojson";
import { beforeEach, describe, expect, test, vi } from "vitest";
import {
  assertLoaded,
  setupJazzTestSync,
  linkAccounts,
  createJazzTestAccount,
  runWithoutActiveAccount,
} from "../testing";
import {
  CoValueLoadingState,
  Group,
  co,
  activeAccountContext,
  deriveChildUniqueness,
} from "../internal";
import { z } from "../exports";
import { waitFor } from "./utils";

beforeEach(async () => {
  cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 1000;

  await setupJazzTestSync({
    asyncPeers: true,
  });

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });
});

describe("CoMap.getOrCreateUnique", () => {
  test("creates a new CoMap when none exists", async () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
    });
    const group = Group.create();

    const person = await Person.getOrCreateUnique({
      value: { name: "Alice", age: 30 },
      unique: "alice-unique",
      owner: group,
    });

    assertLoaded(person);
    expect(person.name).toBe("Alice");
    expect(person.age).toBe(30);
  });

  test("returns existing CoMap without modification", async () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
    });
    const group = Group.create();

    // Create initial value
    const original = Person.create(
      { name: "Alice", age: 30 },
      { owner: group, unique: "alice-no-update" },
    );

    // Try to getOrCreate with different data
    const result = await Person.getOrCreateUnique({
      value: { name: "Bob", age: 25 },
      unique: "alice-no-update",
      owner: group,
    });

    assertLoaded(result);
    // Should return the original, NOT updated with new values
    expect(result.$jazz.id).toBe(original.$jazz.id);
    expect(result.name).toBe("Alice"); // NOT "Bob"
    expect(result.age).toBe(30); // NOT 25
  });

  test("works with resolve options", async () => {
    const Project = co.map({
      name: z.string(),
    });
    const Organisation = co.map({
      name: z.string(),
      projects: co.list(Project),
    });
    const group = Group.create();

    const projectList = co
      .list(Project)
      .create([Project.create({ name: "My project" }, group)], group);

    const org = await Organisation.getOrCreateUnique({
      value: {
        name: "My organisation",
        projects: projectList,
      },
      unique: { name: "My organisation" },
      owner: group,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });

    assertLoaded(org);
    expect(org.name).toBe("My organisation");
    expect(org.projects.length).toBe(1);
    expect(org.projects[0]?.name).toBe("My project");
  });

  test("works without an active account", async () => {
    const account = activeAccountContext.get();

    const Event = co.map({
      title: z.string(),
      identifier: z.string(),
    });

    const event = await runWithoutActiveAccount(() => {
      return Event.getOrCreateUnique({
        value: {
          title: "Test Event",
          identifier: "test-id",
        },
        unique: "no-active-account-test",
        owner: account,
      });
    });

    assertLoaded(event);
    expect(event.title).toBe("Test Event");
    expect(event.$jazz.owner).toEqual(account);
  });

  test("concurrent getOrCreateUnique returns same instance", async () => {
    const Counter = co.map({
      value: z.number(),
    });
    const group = Group.create().makePublic("writer");

    const bob = await createJazzTestAccount();
    const alice = await createJazzTestAccount();

    const bobGroup = await Group.load(group.$jazz.id, {
      loadAs: bob,
    });
    const aliceGroup = await Group.load(group.$jazz.id, {
      loadAs: alice,
    });

    assertLoaded(bobGroup);
    assertLoaded(aliceGroup);

    const [bobCounter, aliceCounter] = await Promise.all([
      Counter.getOrCreateUnique({
        value: { value: 0 },
        unique: "concurrent-counter",
        owner: bobGroup,
      }),
      Counter.getOrCreateUnique({
        value: { value: 0 },
        unique: "concurrent-counter",
        owner: aliceGroup,
      }),
    ]);

    assertLoaded(bobCounter);
    assertLoaded(aliceCounter);

    expect(bobCounter.$jazz.id).toBe(aliceCounter.$jazz.id);
    expect(bobCounter.value).toBe(0);
    expect(aliceCounter.value).toBe(0);
  });

  test("handles unauthorized access gracefully", async () => {
    const Secret = co.map({
      data: z.string(),
    });
    const group = Group.create();

    // Create initial value
    await Secret.getOrCreateUnique({
      value: { data: "secret" },
      unique: "secret-data",
      owner: group,
    });

    // Create another account without access
    const alice = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const aliceGroup = await Group.load(group.$jazz.id, {
      loadAs: alice,
    });
    assertLoaded(aliceGroup);

    const result = await Secret.getOrCreateUnique({
      value: { data: "alice-secret" },
      unique: "secret-data",
      owner: aliceGroup,
    });

    expect(result.$isLoaded).toBe(false);
    expect(result.$jazz.loadingState).toBe(CoValueLoadingState.UNAUTHORIZED);
  });

  test("unauthorized user offline creates unique value, transaction invalidated after reconnect", async () => {
    // Helper to disconnect an account from sync server (simulate going offline)
    function goOffline(
      account: InstanceType<typeof import("../internal").Account>,
    ) {
      Object.values(account.$jazz.localNode.syncManager.peers).forEach(
        (peer) => {
          peer.gracefulShutdown();
        },
      );
    }

    const Document = co.map({
      title: z.string(),
      content: z.string(),
    });
    const owner = activeAccountContext.get();

    // Create Bob and add him as a writer
    const bob = await createJazzTestAccount();
    const group = Group.create();
    group.addMember(bob, "writer");

    // Bob loads the group while online (he has write access)
    const groupOnBob = await Group.load(group.$jazz.id, { loadAs: bob });
    assertLoaded(groupOnBob);

    // Both go offline to isolate their states
    goOffline(owner);
    goOffline(bob);

    group.addMember(bob, "reader");

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Bob creates a unique value while offline using direct create
    // His local node thinks he has write access (hasn't seen the removal)
    const bobDoc = await Document.getOrCreateUnique({
      value: { title: "Bob's Doc", content: "Bob's content" },
      unique: "offline-unauthorized-doc",
      owner: groupOnBob,
    });

    assertLoaded(bobDoc);
    // From Bob's local perspective, it succeeds
    expect(bobDoc.title).toBe("Bob's Doc");

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Owner creates the same unique value (owner has actual write access)
    const ownerDoc = await Document.getOrCreateUnique({
      value: { title: "Owner's Doc", content: "Owner's content" },
      unique: "offline-unauthorized-doc",
      owner: group,
    });
    assertLoaded(ownerDoc);
    expect(ownerDoc.title).toBe("Owner's Doc");

    // Both documents have the same ID (derived from uniqueness)
    expect(bobDoc.$jazz.id).toBe(ownerDoc.$jazz.id);

    // Reconnect by linking accounts
    await linkAccounts(owner, bob);

    // After sync, Bob's transaction should be invalidated because he's no longer authorized
    await waitFor(() => {
      expect(bobDoc.$jazz.raw.core.knownState()).toEqual(
        ownerDoc.$jazz.raw.core.knownState(),
      );
    });

    // The owner's version should be the one that persists
    // (Bob's init transaction should be invalidated due to permission)
    expect(ownerDoc.title).toBe("Owner's Doc");
    expect(ownerDoc.content).toBe("Owner's content");
    expect(bobDoc.title).toBe("Owner's Doc");
    expect(bobDoc.content).toBe("Owner's content");
  });

  test("reader permission can load but not create", async () => {
    const Document = co.map({
      title: z.string(),
      content: z.string(),
    });
    const group = Group.create();

    // Create initial document as owner
    const original = await Document.getOrCreateUnique({
      value: { title: "Original", content: "Original content" },
      unique: "reader-test-doc",
      owner: group,
    });
    assertLoaded(original);

    // Create another account with reader access only
    const reader = await createJazzTestAccount();
    group.addMember(reader, "reader");

    const readerGroup = await Group.load(group.$jazz.id, {
      loadAs: reader,
    });
    assertLoaded(readerGroup);

    // Reader should be able to load the existing document
    const loadedByReader = await Document.getOrCreateUnique({
      value: { title: "Reader Attempt", content: "Reader content" },
      unique: "reader-test-doc",
      owner: readerGroup,
    });

    assertLoaded(loadedByReader);
    // Should return the original document, not create a new one
    expect(loadedByReader.$jazz.id).toBe(original.$jazz.id);
    expect(loadedByReader.title).toBe("Original");
    expect(loadedByReader.content).toBe("Original content");

    // Reader should not be able to create a new document with a new unique key
    const attemptCreate = await Document.getOrCreateUnique({
      value: { title: "New Doc", content: "New content" },
      unique: "reader-new-doc",
      owner: readerGroup,
    });

    // Should fail because reader cannot create in the group
    expect(attemptCreate.$isLoaded).toBe(false);
    expect(attemptCreate.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
  });

  test.skip("concurrent getOrCreateUnique with nested map returns same nested values", async () => {
    const Address = co.map({
      street: z.string(),
      city: z.string(),
    });
    const Person = co.map({
      name: z.string(),
      address: Address,
    });
    const group = Group.create().makePublic("writer");

    const bob = await createJazzTestAccount();
    const alice = await createJazzTestAccount();

    const bobGroup = await Group.load(group.$jazz.id, {
      loadAs: bob,
    });
    const aliceGroup = await Group.load(group.$jazz.id, {
      loadAs: alice,
    });

    assertLoaded(bobGroup);
    assertLoaded(aliceGroup);

    // Both users concurrently try to create a Person with a nested Address
    const [bobPerson, alicePerson] = await Promise.all([
      Person.getOrCreateUnique({
        value: {
          name: "Shared Person",
          address: Address.create(
            { street: "Bob Street", city: "Bob City" },
            bobGroup,
          ),
        },
        unique: "concurrent-nested-person",
        owner: bobGroup,
        resolve: { address: true },
      }),
      Person.getOrCreateUnique({
        value: {
          name: "Shared Person",
          address: Address.create(
            { street: "Alice Street", city: "Alice City" },
            aliceGroup,
          ),
        },
        unique: "concurrent-nested-person",
        owner: aliceGroup,
        resolve: { address: true },
      }),
    ]);

    assertLoaded(bobPerson);
    assertLoaded(alicePerson);

    // Both should have the same Person ID
    expect(bobPerson.$jazz.id).toBe(alicePerson.$jazz.id);

    await waitFor(() => {
      // Both users should end up with the same address values (one wins)
      expect(bobPerson.address.$jazz.id).toBe(alicePerson.address.$jazz.id);
    });
  });
});

describe("CoList.getOrCreateUnique", () => {
  test("creates a new CoList when none exists", async () => {
    const ItemList = co.list(z.string());
    const group = Group.create();

    const list = await ItemList.getOrCreateUnique({
      value: ["item1", "item2", "item3"],
      unique: "new-list",
      owner: group,
    });

    assertLoaded(list);
    expect(list.length).toBe(3);
    expect(list[0]).toBe("item1");
    expect(list[1]).toBe("item2");
    expect(list[2]).toBe("item3");
  });

  test("returns existing CoList without modification", async () => {
    const ItemList = co.list(z.string());
    const group = Group.create();

    // Create initial list
    const original = ItemList.create(["original1", "original2"], {
      owner: group,
      unique: "list-no-update",
    });

    // Try to getOrCreate with different data
    const result = await ItemList.getOrCreateUnique({
      value: ["new1", "new2", "new3"],
      unique: "list-no-update",
      owner: group,
    });

    assertLoaded(result);
    // Should return the original, NOT updated
    expect(result.$jazz.id).toBe(original.$jazz.id);
    expect(result.length).toBe(2); // NOT 3
    expect(result[0]).toBe("original1"); // NOT "new1"
    expect(result[1]).toBe("original2"); // NOT "new2"
  });

  test("works with CoValue items and resolve", async () => {
    const Item = co.map({
      name: z.string(),
      value: z.number(),
    });
    const ItemList = co.list(Item);
    const group = Group.create();

    const items = [
      Item.create({ name: "First", value: 1 }, group),
      Item.create({ name: "Second", value: 2 }, group),
    ];

    const result = await ItemList.getOrCreateUnique({
      value: items,
      unique: "item-list",
      owner: group,
      resolve: { $each: true },
    });

    assertLoaded(result);
    expect(result.length).toBe(2);
    expect(result[0]?.name).toBe("First");
    expect(result[1]?.name).toBe("Second");
  });

  test("works without an active account", async () => {
    const account = activeAccountContext.get();
    const ItemList = co.list(z.string());

    const list = await runWithoutActiveAccount(() => {
      return ItemList.getOrCreateUnique({
        value: ["item1", "item2"],
        unique: "no-active-account-list",
        owner: account,
      });
    });

    assertLoaded(list);
    expect(list.length).toBe(2);
    expect(list.$jazz.owner).toEqual(account);
  });

  test("concurrent getOrCreateUnique returns same instance", async () => {
    const ItemList = co.list(z.number());
    const group = Group.create().makePublic("writer");

    const bob = await createJazzTestAccount();
    const alice = await createJazzTestAccount();

    const bobGroup = await Group.load(group.$jazz.id, {
      loadAs: bob,
    });
    const aliceGroup = await Group.load(group.$jazz.id, {
      loadAs: alice,
    });

    assertLoaded(bobGroup);
    assertLoaded(aliceGroup);

    const [bobList, aliceList] = await Promise.all([
      ItemList.getOrCreateUnique({
        value: [1],
        unique: "concurrent-list",
        owner: bobGroup,
      }),
      ItemList.getOrCreateUnique({
        value: [1],
        unique: "concurrent-list",
        owner: aliceGroup,
      }),
    ]);

    assertLoaded(bobList);
    assertLoaded(aliceList);

    expect(bobList.$jazz.id).toBe(aliceList.$jazz.id);

    await waitFor(() => {
      expect(bobList.$jazz.raw.core.knownState()).toEqual(
        aliceList.$jazz.raw.core.knownState(),
      );
    });

    expect(bobList).toEqual([1]);
    expect(aliceList).toEqual([1]);
  });

  test.skip("concurrent getOrCreateUnique with nested map results in list with one of the maps", async () => {
    const Item = co.map({
      name: z.string(),
      createdBy: z.string(),
    });
    const ItemList = co.list(Item);
    const group = Group.create().makePublic("writer");

    const bob = await createJazzTestAccount();
    const alice = await createJazzTestAccount();

    const bobGroup = await Group.load(group.$jazz.id, {
      loadAs: bob,
    });
    const aliceGroup = await Group.load(group.$jazz.id, {
      loadAs: alice,
    });

    assertLoaded(bobGroup);
    assertLoaded(aliceGroup);

    // Both users concurrently try to create a list with a nested Item
    const [bobList, aliceList] = await Promise.all([
      ItemList.getOrCreateUnique({
        value: [
          Item.create({ name: "Bob's Item", createdBy: "bob" }, bobGroup),
        ],
        unique: "concurrent-nested-list",
        owner: bobGroup,
        resolve: { $each: true },
      }),
      ItemList.getOrCreateUnique({
        value: [
          Item.create({ name: "Alice's Item", createdBy: "alice" }, aliceGroup),
        ],
        unique: "concurrent-nested-list",
        owner: aliceGroup,
        resolve: { $each: true },
      }),
    ]);

    assertLoaded(bobList);
    assertLoaded(aliceList);

    // Both should have the same list ID
    expect(bobList.$jazz.id).toBe(aliceList.$jazz.id);

    // Wait for sync
    await waitFor(() => {
      expect(bobList.$jazz.raw.core.knownState()).toEqual(
        aliceList.$jazz.raw.core.knownState(),
      );
    });

    // Verify that the items are the same
    await waitFor(() => {
      expect(bobList.$jazz.refs[0]).toEqual(aliceList.$jazz.refs[0]);
    });

    // The list should contain both items (one from each user)
    expect(bobList.length).toBe(1);
    expect(aliceList.length).toBe(1);
  });
});

describe("CoFeed.getOrCreateUnique", () => {
  test("creates a new CoFeed when none exists", async () => {
    const MessageFeed = co.feed(z.string());
    const group = Group.create();

    const feed = await MessageFeed.getOrCreateUnique({
      value: ["message1", "message2"],
      unique: "new-feed",
      owner: group,
    });

    assertLoaded(feed);
    // CoFeed stores items, verify it was created
    expect(feed.$jazz.id).toBeDefined();
  });

  test("returns existing CoFeed without modification", async () => {
    const MessageFeed = co.feed(z.string());
    const group = Group.create();

    // Create initial feed
    const original = MessageFeed.create(["original"], {
      owner: group,
      unique: "feed-no-update",
    });
    const originalId = original.$jazz.id;

    // Try to getOrCreate with different data
    const result = await MessageFeed.getOrCreateUnique({
      value: ["new1", "new2", "new3"],
      unique: "feed-no-update",
      owner: group,
    });

    assertLoaded(result);
    // Should return the original, NOT updated
    expect(result.$jazz.id).toBe(originalId);
  });

  test("works without an active account", async () => {
    const account = activeAccountContext.get();
    const MessageFeed = co.feed(z.string());

    const feed = await runWithoutActiveAccount(() => {
      return MessageFeed.getOrCreateUnique({
        value: ["message"],
        unique: "no-active-account-feed",
        owner: account,
      });
    });

    assertLoaded(feed);
    expect(feed.$jazz.owner).toEqual(account);
  });
});

describe("getOrCreateUnique offline scenarios", () => {
  // Helper to disconnect an account from sync server (simulate going offline)
  function goOffline(
    account: InstanceType<typeof import("../internal").Account>,
  ) {
    Object.values(account.$jazz.localNode.syncManager.peers).forEach((peer) => {
      peer.gracefulShutdown();
    });
  }

  test("two accounts offline create unique values with nested children - all values have same IDs", async () => {
    const Address = co.map({
      street: z.string(),
      city: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      address: Address.withPermissions({
        onInlineCreate: "sameAsContainer",
      }),
    });

    const group = Group.create().makePublic("writer");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    const aliceGroup = await Group.load(group.$jazz.id, { loadAs: alice });
    const bobGroup = await Group.load(group.$jazz.id, { loadAs: bob });

    assertLoaded(aliceGroup);
    assertLoaded(bobGroup);

    // Simulate going offline
    goOffline(alice);
    goOffline(bob);

    // Both users create the same unique Person with nested Address while offline
    const alicePerson = await Person.getOrCreateUnique({
      value: {
        name: "Shared Person",
        address: { street: "Alice Street", city: "Alice City" },
      },
      unique: "offline-nested-person",
      owner: aliceGroup,
      resolve: { address: true },
    });

    const bobPerson = await Person.getOrCreateUnique({
      value: {
        name: "Shared Person",
        address: { street: "Bob Street", city: "Bob City" },
      },
      unique: "offline-nested-person",
      owner: bobGroup,
      resolve: { address: true },
    });

    assertLoaded(alicePerson);
    assertLoaded(bobPerson);

    // Both should have the same Person ID (derived from uniqueness)
    expect(alicePerson.$jazz.id).toBe(bobPerson.$jazz.id);

    // Both should have the same Address ID (derived from parent uniqueness + field name)
    expect(alicePerson.address.$jazz.id).toBe(bobPerson.address.$jazz.id);
  });

  test("two accounts offline create unique values with nested children, set different fields, updates merge after sync", async () => {
    const Settings = co.map({
      theme: z.string(),
      languages: co.list(z.string()).withPermissions({
        onInlineCreate: "sameAsContainer",
      }),
      fontSize: z.number().optional(),
      fontFamily: z.string().optional(),
    });

    const UserProfile = co.map({
      name: z.string(),
      settings: Settings.withPermissions({
        onInlineCreate: "sameAsContainer",
      }),
    });

    const group = Group.create().makePublic("writer");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    const aliceGroup = await Group.load(group.$jazz.id, { loadAs: alice });
    const bobGroup = await Group.load(group.$jazz.id, { loadAs: bob });

    assertLoaded(aliceGroup);
    assertLoaded(bobGroup);

    // Simulate going offline
    goOffline(alice);
    goOffline(bob);

    // Alice creates the profile with initial settings
    const aliceProfile = await UserProfile.getOrCreateUnique({
      value: {
        name: "Shared User",
        settings: { theme: "light", languages: ["en"] },
      },
      unique: "offline-nested-merge",
      owner: aliceGroup,
      resolve: { settings: { languages: true } },
    });

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Bob creates the same profile (will have same IDs)
    const bobProfile = await UserProfile.getOrCreateUnique({
      value: {
        name: "Shared User",
        settings: { theme: "dark", languages: ["it"] },
      },
      unique: "offline-nested-merge",
      owner: bobGroup,
      resolve: { settings: { languages: true } },
    });

    assertLoaded(aliceProfile);
    assertLoaded(bobProfile);

    // Verify same IDs
    expect(aliceProfile.$jazz.id).toBe(bobProfile.$jazz.id);
    expect(aliceProfile.settings.$jazz.id).toBe(bobProfile.settings.$jazz.id);

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Alice sets fontSize on the nested settings
    aliceProfile.settings.$jazz.set("fontSize", 16);

    // Bob sets fontFamily on the nested settings
    bobProfile.settings.$jazz.set("fontFamily", "Arial");
    bobProfile.settings.languages.$jazz.push("es");

    // Simulate reconnection by linking accounts directly
    await linkAccounts(alice, bob);

    // First init should win, and updates should merge
    expect(aliceProfile.settings.languages).toEqual(["en", "es"]);
    expect(bobProfile.settings.languages).toEqual(["en", "es"]);
    expect(aliceProfile.settings.theme).toBe("light");
    expect(bobProfile.settings.theme).toBe("light");

    // Both should see the merged updates
    expect(aliceProfile.settings.fontSize).toBe(16);
    expect(aliceProfile.settings.fontFamily).toBe("Arial");
    expect(bobProfile.settings.fontSize).toBe(16);
    expect(bobProfile.settings.fontFamily).toBe("Arial");
  });

  test("two accounts offline create unique values and do updates, updates merge after sync", async () => {
    const Counter = co.map({
      value: z.number(),
      lastUpdatedBy: z.string().optional(),
      notes: z.string().optional(),
    });

    const group = Group.create().makePublic("writer");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    const aliceGroup = await Group.load(group.$jazz.id, { loadAs: alice });
    const bobGroup = await Group.load(group.$jazz.id, { loadAs: bob });

    assertLoaded(aliceGroup);
    assertLoaded(bobGroup);

    // Simulate going offline
    goOffline(alice);
    goOffline(bob);

    // Alice creates the counter while offline
    const aliceCounter = await Counter.getOrCreateUnique({
      value: { value: 0 },
      unique: "offline-counter-merge",
      owner: aliceGroup,
    });

    // Bob creates the same counter while offline
    const bobCounter = await Counter.getOrCreateUnique({
      value: { value: 0 },
      unique: "offline-counter-merge",
      owner: bobGroup,
    });

    assertLoaded(aliceCounter);
    assertLoaded(bobCounter);

    // Verify same IDs
    expect(aliceCounter.$jazz.id).toBe(bobCounter.$jazz.id);

    // Alice updates the counter with non-conflicting field
    aliceCounter.$jazz.set("lastUpdatedBy", "alice");

    // Bob updates a different non-conflicting field
    bobCounter.$jazz.set("notes", "Bob's note");

    // Simulate reconnection by linking accounts directly
    await linkAccounts(alice, bob);

    // Non-conflicting fields should be merged on both sides
    expect(aliceCounter.lastUpdatedBy).toBe("alice");
    expect(aliceCounter.notes).toBe("Bob's note");
    expect(bobCounter.lastUpdatedBy).toBe("alice");
    expect(bobCounter.notes).toBe("Bob's note");
  });

  test("inline CoValue without sameAsContainer permission logs warning", async () => {
    const InlineSettings = co.map({
      theme: z.string(),
    });

    // Use a custom ref with specific owner (not sameAsContainer)
    const Profile = co.map({
      name: z.string(),
      settings: InlineSettings.withPermissions({
        onInlineCreate: "extendsContainer",
      }),
    });

    const ownerGroup = Group.create();

    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    try {
      // Create with unique - this should trigger a warning because settings uses a different owner
      await Profile.getOrCreateUnique({
        value: {
          name: "Test",
          settings: { theme: "dark" },
        },
        unique: "profile-different-owner",
        owner: ownerGroup,
      });

      // Verify warning was logged
      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining(
          'Inline CoValue at field "settings" has a different owner than its unique parent',
        ),
      );
      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining('Consider using "sameAsContainer" permission'),
      );
    } finally {
      warnSpy.mockRestore();
    }
  });
});

describe("CoList.getOrCreateUnique offline scenarios", () => {
  // Helper to disconnect an account from sync server (simulate going offline)
  function goOffline(
    account: InstanceType<typeof import("../internal").Account>,
  ) {
    Object.values(account.$jazz.localNode.syncManager.peers).forEach((peer) => {
      peer.gracefulShutdown();
    });
  }

  test("two accounts offline create unique list - same IDs", async () => {
    const ItemList = co.list(z.string());
    const group = Group.create().makePublic("writer");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    const aliceGroup = await Group.load(group.$jazz.id, { loadAs: alice });
    const bobGroup = await Group.load(group.$jazz.id, { loadAs: bob });

    assertLoaded(aliceGroup);
    assertLoaded(bobGroup);

    // Simulate going offline
    goOffline(alice);
    goOffline(bob);

    // Both users create the same unique list while offline
    const aliceList = await ItemList.getOrCreateUnique({
      value: ["alice-item-1", "alice-item-2"],
      unique: "offline-list",
      owner: aliceGroup,
    });

    const bobList = await ItemList.getOrCreateUnique({
      value: ["bob-item-1", "bob-item-2"],
      unique: "offline-list",
      owner: bobGroup,
    });

    assertLoaded(aliceList);
    assertLoaded(bobList);

    // Both should have the same list ID (derived from uniqueness)
    expect(aliceList.$jazz.id).toBe(bobList.$jazz.id);
  });

  test("two accounts offline create unique list with nested CoMaps - all values have same IDs", async () => {
    const Item = co.map({
      name: z.string(),
      value: z.number(),
    });

    const ItemList = co
      .list(
        Item.withPermissions({
          onInlineCreate: "sameAsContainer",
        }),
      )
      .withPermissions({ onInlineCreate: "sameAsContainer" });

    const group = Group.create().makePublic("writer");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    const aliceGroup = await Group.load(group.$jazz.id, { loadAs: alice });
    const bobGroup = await Group.load(group.$jazz.id, { loadAs: bob });

    assertLoaded(aliceGroup);
    assertLoaded(bobGroup);

    // Simulate going offline
    goOffline(alice);
    goOffline(bob);

    // Both users create the same unique list with nested items while offline
    const aliceList = await ItemList.getOrCreateUnique({
      value: [{ name: "Alice Item", value: 100 }],
      unique: "offline-nested-list",
      owner: aliceGroup,
      resolve: { $each: true },
    });

    const bobList = await ItemList.getOrCreateUnique({
      value: [{ name: "Bob Item", value: 200 }],
      unique: "offline-nested-list",
      owner: bobGroup,
      resolve: { $each: true },
    });

    assertLoaded(aliceList);
    assertLoaded(bobList);

    // Both should have the same list ID (derived from uniqueness)
    expect(aliceList.$jazz.id).toBe(bobList.$jazz.id);

    // The nested items at the same index should also have the same ID
    expect(aliceList[0]?.$jazz.id).toBe(bobList[0]?.$jazz.id);
  });

  test("two accounts offline create unique list and do updates, updates merge after sync", async () => {
    const ItemList = co.list(z.string());
    const group = Group.create().makePublic("writer");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    const aliceGroup = await Group.load(group.$jazz.id, { loadAs: alice });
    const bobGroup = await Group.load(group.$jazz.id, { loadAs: bob });

    assertLoaded(aliceGroup);
    assertLoaded(bobGroup);

    // Simulate going offline
    goOffline(alice);
    goOffline(bob);

    // Both users create the same unique list while offline
    const aliceList = await ItemList.getOrCreateUnique({
      value: ["initial"],
      unique: "offline-list-merge",
      owner: aliceGroup,
    });

    const bobList = await ItemList.getOrCreateUnique({
      value: ["initial"],
      unique: "offline-list-merge",
      owner: bobGroup,
    });

    assertLoaded(aliceList);
    assertLoaded(bobList);

    // Verify same IDs
    expect(aliceList.$jazz.id).toBe(bobList.$jazz.id);

    // Alice pushes an item
    aliceList.$jazz.push("alice-added");

    // Bob pushes a different item
    bobList.$jazz.push("bob-added");

    // Simulate reconnection by linking accounts directly
    await linkAccounts(alice, bob);

    // Both lists should now contain items from both users (merged)
    expect(aliceList.length).toBe(3);
    expect(bobList.length).toBe(3);
    expect(aliceList).toContain("initial");
    expect(aliceList).toContain("alice-added");
    expect(aliceList).toContain("bob-added");
    expect(bobList).toContain("initial");
    expect(bobList).toContain("alice-added");
    expect(bobList).toContain("bob-added");
  });
});

describe("CoFeed.getOrCreateUnique offline scenarios", () => {
  // Helper to disconnect an account from sync server (simulate going offline)
  function goOffline(
    account: InstanceType<typeof import("../internal").Account>,
  ) {
    Object.values(account.$jazz.localNode.syncManager.peers).forEach((peer) => {
      peer.gracefulShutdown();
    });
  }

  test("two accounts offline create unique feed - same IDs", async () => {
    const MessageFeed = co.feed(z.string());
    const group = Group.create().makePublic("writer");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    const aliceGroup = await Group.load(group.$jazz.id, { loadAs: alice });
    const bobGroup = await Group.load(group.$jazz.id, { loadAs: bob });

    assertLoaded(aliceGroup);
    assertLoaded(bobGroup);

    // Simulate going offline
    goOffline(alice);
    goOffline(bob);

    // Both users create the same unique feed while offline
    const aliceFeed = await MessageFeed.getOrCreateUnique({
      value: ["alice-message"],
      unique: "offline-feed",
      owner: aliceGroup,
    });

    const bobFeed = await MessageFeed.getOrCreateUnique({
      value: ["bob-message"],
      unique: "offline-feed",
      owner: bobGroup,
    });

    assertLoaded(aliceFeed);
    assertLoaded(bobFeed);

    // Both should have the same feed ID (derived from uniqueness)
    expect(aliceFeed.$jazz.id).toBe(bobFeed.$jazz.id);
  });

  test("two accounts offline create unique feed with CoMap items - same IDs", async () => {
    const Message = co.map({
      text: z.string(),
      author: z.string(),
    });

    const MessageFeed = co.feed(
      Message.withPermissions({
        onInlineCreate: "sameAsContainer",
      }),
    );

    const group = Group.create().makePublic("writer");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    const aliceGroup = await Group.load(group.$jazz.id, { loadAs: alice });
    const bobGroup = await Group.load(group.$jazz.id, { loadAs: bob });

    assertLoaded(aliceGroup);
    assertLoaded(bobGroup);

    // Simulate going offline
    goOffline(alice);
    goOffline(bob);

    // Both users create the same unique feed with nested messages while offline
    const aliceFeed = await MessageFeed.getOrCreateUnique({
      value: [{ text: "Hello from Alice", author: "alice" }],
      unique: "offline-nested-feed",
      owner: aliceGroup,
    });

    const bobFeed = await MessageFeed.getOrCreateUnique({
      value: [{ text: "Hello from Bob", author: "bob" }],
      unique: "offline-nested-feed",
      owner: bobGroup,
    });

    assertLoaded(aliceFeed);
    assertLoaded(bobFeed);

    // Both should have the same feed ID (derived from uniqueness)
    expect(aliceFeed.$jazz.id).toBe(bobFeed.$jazz.id);
  });

  test("two accounts offline create unique feed", async () => {
    const MessageFeed = co.feed(z.string());
    const group = Group.create().makePublic("writer");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    const aliceGroup = await Group.load(group.$jazz.id, { loadAs: alice });
    const bobGroup = await Group.load(group.$jazz.id, { loadAs: bob });

    assertLoaded(aliceGroup);
    assertLoaded(bobGroup);

    // Simulate going offline
    goOffline(alice);
    goOffline(bob);

    // Both users create the same unique feed while offline
    const aliceFeed = await MessageFeed.getOrCreateUnique({
      value: ["initial"],
      unique: "offline-feed-merge",
      owner: aliceGroup,
    });

    await new Promise((resolve) => setTimeout(resolve, 10));

    const bobFeed = await MessageFeed.getOrCreateUnique({
      value: ["initial"],
      unique: "offline-feed-merge",
      owner: bobGroup,
    });

    assertLoaded(aliceFeed);
    assertLoaded(bobFeed);

    // Verify same IDs
    expect(aliceFeed.$jazz.id).toBe(bobFeed.$jazz.id);

    // Simulate reconnection by linking accounts directly
    await linkAccounts(alice, bob);

    // CoFeed has per-session entries, so each user's messages appear in their own "lane"
    // After sync, both feeds should be synced with the same underlying data
    await waitFor(() => {
      expect(aliceFeed.$jazz.raw.core.knownState()).toEqual(
        bobFeed.$jazz.raw.core.knownState(),
      );
    });

    // Alice init wins
    expect(aliceFeed.perAccount[alice.$jazz.id]?.value).toEqual("initial");
    expect(bobFeed.perAccount[bob.$jazz.id]?.value).toEqual(undefined);
  });

  test("two accounts offline create unique feed and push updates, updates merge after sync", async () => {
    const MessageFeed = co.feed(z.string());
    const group = Group.create().makePublic("writer");

    const alice = await createJazzTestAccount();
    const bob = await createJazzTestAccount();

    const aliceGroup = await Group.load(group.$jazz.id, { loadAs: alice });
    const bobGroup = await Group.load(group.$jazz.id, { loadAs: bob });

    assertLoaded(aliceGroup);
    assertLoaded(bobGroup);

    // Simulate going offline
    goOffline(alice);
    goOffline(bob);

    // Both users create the same unique feed while offline
    const aliceFeed = await MessageFeed.getOrCreateUnique({
      value: ["initial"],
      unique: "offline-feed-merge",
      owner: aliceGroup,
    });

    await new Promise((resolve) => setTimeout(resolve, 10));

    const bobFeed = await MessageFeed.getOrCreateUnique({
      value: ["initial"],
      unique: "offline-feed-merge",
      owner: bobGroup,
    });

    assertLoaded(aliceFeed);
    assertLoaded(bobFeed);

    // Verify same IDs
    expect(aliceFeed.$jazz.id).toBe(bobFeed.$jazz.id);

    // Alice pushes a message (CoFeed uses push method)
    aliceFeed.$jazz.push("alice-message");

    // Bob pushes a different message
    bobFeed.$jazz.push("bob-message");

    // Simulate reconnection by linking accounts directly
    await linkAccounts(alice, bob);

    // CoFeed has per-session entries, so each user's messages appear in their own "lane"
    // After sync, both feeds should be synced with the same underlying data
    await waitFor(() => {
      expect(aliceFeed.$jazz.raw.core.knownState()).toEqual(
        bobFeed.$jazz.raw.core.knownState(),
      );
    });

    expect(aliceFeed.perAccount[alice.$jazz.id]?.value).toEqual(
      "alice-message",
    );
    expect(bobFeed.perAccount[bob.$jazz.id]?.value).toEqual("bob-message");
  });
});

describe("upsertUnique permissions", () => {
  test("reader permission can load but not update via upsertUnique", async () => {
    const Document = co.map({
      title: z.string(),
      content: z.string(),
    });
    const group = Group.create();

    // Create initial document as owner
    const original = await Document.upsertUnique({
      value: { title: "Original", content: "Original content" },
      unique: "reader-upsert-test-doc",
      owner: group,
    });
    assertLoaded(original);
    expect(original.title).toBe("Original");
    expect(original.content).toBe("Original content");

    // Create another account with reader access only
    const reader = await createJazzTestAccount();
    group.addMember(reader, "reader");

    const readerGroup = await Group.load(group.$jazz.id, {
      loadAs: reader,
    });
    assertLoaded(readerGroup);

    // Reader tries to upsertUnique - should load but NOT update
    const readerAttempt = await Document.upsertUnique({
      value: { title: "Reader Attempt", content: "Reader content" },
      unique: "reader-upsert-test-doc",
      owner: readerGroup,
    });

    assertLoaded(readerAttempt);
    // Should return the original document values, NOT the updated ones
    expect(readerAttempt.$jazz.id).toBe(original.$jazz.id);
    expect(readerAttempt.title).toBe("Original");
    expect(readerAttempt.content).toBe("Original content");

    // Verify the original wasn't modified either
    expect(original.title).toBe("Original");
    expect(original.content).toBe("Original content");
  });
});

describe("getOrCreateUnique vs upsertUnique behavior comparison", () => {
  test("upsertUnique updates existing values, getOrCreateUnique does not", async () => {
    const Settings = co.map({
      theme: z.string(),
      language: z.string(),
    });
    const group = Group.create();

    // Create initial settings for upsertUnique test
    Settings.create(
      { theme: "light", language: "en" },
      { owner: group, unique: "settings-upsert" },
    );

    // upsertUnique should update the values
    const upserted = await Settings.upsertUnique({
      value: { theme: "dark", language: "fr" },
      unique: "settings-upsert",
      owner: group,
    });

    assertLoaded(upserted);
    expect(upserted.theme).toBe("dark"); // Updated
    expect(upserted.language).toBe("fr"); // Updated

    // Create separate initial settings for getOrCreateUnique test
    Settings.create(
      { theme: "light", language: "en" },
      { owner: group, unique: "settings-getorcreate" },
    );

    // getOrCreateUnique should NOT update the values
    const gotOrCreated = await Settings.getOrCreateUnique({
      value: { theme: "dark", language: "fr" },
      unique: "settings-getorcreate",
      owner: group,
    });

    assertLoaded(gotOrCreated);
    expect(gotOrCreated.theme).toBe("light"); // NOT updated
    expect(gotOrCreated.language).toBe("en"); // NOT updated
  });
});

describe("deriveChildUniqueness", () => {
  test("derives child uniqueness from a string parent using @@ separator", () => {
    expect(deriveChildUniqueness("parent-unique", "fieldName")).toBe(
      "parent-unique@@fieldName",
    );
  });

  test("derives child uniqueness from an object parent with existing _field", () => {
    expect(deriveChildUniqueness({ _field: "a" }, "b")).toEqual({
      _field: "a/b",
    });
  });

  test("derives child uniqueness from an object parent without _field", () => {
    expect(deriveChildUniqueness({}, "field")).toEqual({ _field: "field" });
  });

  test("preserves extra properties on object uniqueness", () => {
    expect(
      deriveChildUniqueness({ _field: "parent", other: "value" }, "child"),
    ).toEqual({ _field: "parent/child", other: "value" });
  });

  test("returns non-derivable inputs unchanged", () => {
    expect(deriveChildUniqueness(true, "field")).toBe(true);
    expect(deriveChildUniqueness(null as any, "field")).toBe(null);
  });
});
