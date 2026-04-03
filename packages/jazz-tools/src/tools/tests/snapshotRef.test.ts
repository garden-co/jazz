import { cojsonInternals } from "cojson";
import { beforeEach, describe, expect, expectTypeOf, test } from "vitest";
import { Group, co, z, ResolveQuery, isControlledAccount } from "../exports.js";
import { ControlledAccount, CoValueLoadingState } from "../internal.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { assertLoaded, setupTwoNodes } from "./utils.js";

const Pet = co.map({
  nickname: z.string(),
});

const Person = co.map({
  name: z.string(),
  pet: Pet,
});

const PersonSnapshot = co.snapshotRef(Person, {
  cursorResolve: { pet: true },
});

const Toy = co.map({
  label: z.string(),
});

const DeepPet = co.map({
  nickname: z.string(),
  favoriteToy: Toy,
});

const DeepPerson = co.map({
  name: z.string(),
  pet: DeepPet,
});

const deepPersonResolve = {
  pet: {
    favoriteToy: true,
  },
} satisfies ResolveQuery<typeof DeepPerson>;

const DeepPersonSnapshot = co.snapshotRef(DeepPerson, {
  cursorResolve: deepPersonResolve,
});

describe("SnapshotRef", () => {
  let me: ControlledAccount;

  beforeEach(async () => {
    cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 1000;

    await setupJazzTestSync();

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });

    if (!isControlledAccount(account)) {
      throw new Error("account is not a controlled account");
    }

    me = account;
  });

  describe("creation", () => {
    test("creates snapshot from created CoValue", async () => {
      const person = Person.create({ name: "John", pet: { nickname: "Rex" } });

      const snapshot = await PersonSnapshot.create(person);

      assertLoaded(snapshot);
      expect(snapshot.cursor).toMatch(/^cursor_z/);
      expect(snapshot.ref.$jazz.id).toBe(person.$jazz.id);
    });

    test("creates snapshot from loaded CoValue", async () => {
      const person = Person.create({ name: "John", pet: { nickname: "Rex" } });
      const loadedPerson = await Person.load(person.$jazz.id);

      assertLoaded(loadedPerson);

      const snapshot = await PersonSnapshot.create(loadedPerson);

      assertLoaded(snapshot);
      expect(snapshot.cursor).toMatch(/^cursor_z/);
      expect(snapshot.ref.$jazz.id).toBe(person.$jazz.id);
    });

    test("creates snapshot from shallow-loaded CoValue", async () => {
      const person = Person.create({
        name: "Bob",
        pet: { nickname: "Fluffy" },
      });

      const shallowLoaded = await Person.load(person.$jazz.id);
      assertLoaded(shallowLoaded);

      const snapshot = await PersonSnapshot.create(shallowLoaded);

      assertLoaded(snapshot);
      expect(snapshot.ref.$jazz.id).toBe(person.$jazz.id);
      expect(snapshot.ref.pet.$isLoaded).toBe(false);
    });

    test("create snapshot of coList", async () => {
      const PetList = co.list(Pet);
      const PetListSnapshot = co.snapshotRef(PetList, {
        cursorResolve: { $each: true },
      });
      const petList = PetList.create([
        { nickname: "Rex" },
        { nickname: "Puff" },
      ]);

      const snapshot = await PetListSnapshot.create(petList);

      assertLoaded(snapshot);
      expect(snapshot.cursor).toMatch(/^cursor_z/);
      expect(snapshot.ref.$jazz.id).toBe(petList.$jazz.id);
      expect(snapshot.ref[0]?.$jazz.id).toBe(petList[0]?.$jazz.id);
    });

    test("create snapshot of coFeed", async () => {
      const PetFeed = co.feed(Pet);
      const PetFeedSnapshot = co.snapshotRef(PetFeed, {
        cursorResolve: { $each: true },
      });
      const petFeed = PetFeed.create([{ nickname: "Rex" }]);

      const snapshot = await PetFeedSnapshot.create(petFeed);

      assertLoaded(snapshot);
      expect(snapshot.cursor).toMatch(/^cursor_z/);
      expect(snapshot.ref.$jazz.id).toBe(petFeed.$jazz.id);
    });

    test("create snapshot of coPlainText", async () => {
      const PlainText = co.plainText();
      const PlainTextSnapshot = co.snapshotRef(PlainText);
      const text = PlainText.create("hello world");

      const snapshot = await PlainTextSnapshot.create(text);

      assertLoaded(snapshot);
      expect(snapshot.cursor).toMatch(/^cursor_z/);
      expect(snapshot.ref.$jazz.id).toBe(text.$jazz.id);
      expect(snapshot.ref.toString()).toBe("hello world");
    });
  });

  describe("ownership and deduplication", () => {
    test("use explicit owner when creating snapshot", async () => {
      const owner = Group.create({ owner: me });
      const person = Person.create({ name: "Mina", pet: { nickname: "Pico" } });

      const snapshot = await PersonSnapshot.create(person, { owner });

      assertLoaded(snapshot);
      expect(snapshot.$jazz.owner.$jazz.id).toBe(owner.$jazz.id);
    });

    test("deduplicate snapshots by default if no owner is set", async () => {
      const person = Person.create({
        name: "Alice",
        pet: { nickname: "Spot" },
      });

      const snapshot1 = await PersonSnapshot.create(person);
      const snapshot2 = await PersonSnapshot.create(person);

      assertLoaded(snapshot1);
      assertLoaded(snapshot2);
      expect(snapshot1.$jazz.id).toBe(snapshot2.$jazz.id);
    });

    test("cannot deduplicate snapshots without write access on referenced value", async () => {
      const alice = await createJazzTestAccount();
      const secretGroup = Group.create({ owner: alice }).makePublic("reader");

      const person = Person.create(
        { name: "Alice", pet: { nickname: "Spot" } },
        { owner: secretGroup },
      );

      const personLoadedAsMe = await Person.load(person.$jazz.id);
      assertLoaded(personLoadedAsMe);

      const snapshot1 = await PersonSnapshot.create(personLoadedAsMe);
      const snapshot2 = await PersonSnapshot.create(personLoadedAsMe);

      assertLoaded(snapshot1);
      assertLoaded(snapshot2);
      expect(snapshot1.$jazz.id).not.toBe(snapshot2.$jazz.id);
    });

    test("deduplicate snapshots if same explicit owner is passed", async () => {
      const owner = Group.create({ owner: me });
      const person = Person.create({
        name: "Alice",
        pet: { nickname: "Spot" },
      });

      const snapshot1 = await PersonSnapshot.create(person, owner);
      const snapshot2 = await PersonSnapshot.create(person, owner);

      assertLoaded(snapshot1);
      assertLoaded(snapshot2);
      expect(snapshot1.$jazz.id).toBe(snapshot2.$jazz.id);
    });

    test("skips withPermissions default group if account has write access on value", async () => {
      const owner = Group.create({ owner: me });
      const SnapshotWithOwner = PersonSnapshot.withPermissions({
        default: () => owner,
      });
      const person = Person.create({ name: "Mina", pet: { nickname: "Pico" } });

      const snapshot = await SnapshotWithOwner.create(person);

      assertLoaded(snapshot);
      expect(snapshot.$jazz.owner.$jazz.id).not.toBe(owner.$jazz.id);
      expect(snapshot.$jazz.owner.$jazz.id).toBe(person.$jazz.owner.$jazz.id);
    });

    test("uses default group from withPermissions if no write access on value", async () => {
      const alice = await createJazzTestAccount();

      const meGroup = Group.create({ owner: me });
      const readOnlyGroup = Group.create({ owner: alice }).makePublic("reader");

      const person = Person.create(
        { name: "Mina", pet: { nickname: "Pico" } },
        readOnlyGroup,
      );
      const personLoadedAsMe = await Person.load(person.$jazz.id);
      assertLoaded(personLoadedAsMe);

      const SnapshotWithOwner = PersonSnapshot.withPermissions({
        default: () => meGroup,
      });
      const snapshot = await SnapshotWithOwner.create(personLoadedAsMe);

      assertLoaded(snapshot);
      expect(snapshot.$jazz.owner.$jazz.id).toBe(meGroup.$jazz.id);
    });
  });

  describe("SnapshotRef.load()", () => {
    test("load snapshotRef using .load()", async () => {
      const person = Person.create({ name: "John", pet: { nickname: "Rex" } });
      const snapshot = await PersonSnapshot.create(person);
      assertLoaded(snapshot);

      const loadedSnapshot = await PersonSnapshot.load(snapshot.$jazz.id);

      assertLoaded(loadedSnapshot);
      expect(loadedSnapshot.$jazz.id).toBe(snapshot.$jazz.id);
      expect(loadedSnapshot.ref.$jazz.id).toBe(person.$jazz.id);
      expect(loadedSnapshot.cursor).toBe(snapshot.cursor);
    });

    test("load non-existent snapshotRef", async () => {
      const loadedSnapshot = await PersonSnapshot.load("co_zNonExistent", {
        skipRetry: true,
      });

      expect(loadedSnapshot.$isLoaded).toBe(false);
      expect(loadedSnapshot.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
    });

    test("load() then ensureLoaded() with no resolve", async () => {
      const person = Person.create({ name: "John", pet: { nickname: "Rex" } });
      const snapshot = await PersonSnapshot.create(person);
      const loadedSnapshot = await PersonSnapshot.load(snapshot.$jazz.id);

      assertLoaded(loadedSnapshot);

      const resolvedSnapshot = await loadedSnapshot.$jazz.ensureLoaded({
        resolve: true,
      });

      assertLoaded(resolvedSnapshot);
      expectTypeOf(resolvedSnapshot.ref.$isLoaded).toEqualTypeOf<boolean>();
    });

    test("load() then ensureLoaded() with resolve", async () => {
      const person = Person.create({ name: "John", pet: { nickname: "Rex" } });
      const snapshot = await PersonSnapshot.create(person);
      const loadedSnapshot = await PersonSnapshot.load(snapshot.$jazz.id);

      assertLoaded(loadedSnapshot);

      const resolvedSnapshot = await loadedSnapshot.$jazz.ensureLoaded({
        resolve: {
          ref: true,
        },
      });

      assertLoaded(resolvedSnapshot);
      expectTypeOf(resolvedSnapshot.ref.$isLoaded).toEqualTypeOf<true>();
      expectTypeOf(resolvedSnapshot.ref.pet.$isLoaded).toEqualTypeOf<boolean>();
      expect(resolvedSnapshot.ref.name).toBe("John");
    });

    test("load() then ensureLoaded() with with subset resolve", async () => {
      const person = DeepPerson.create({
        name: "John",
        pet: {
          nickname: "Rex",
          favoriteToy: { label: "Ball" },
        },
      });
      const snapshot = await DeepPersonSnapshot.create(person);
      const loadedSnapshot = await DeepPersonSnapshot.load(snapshot.$jazz.id);

      assertLoaded(loadedSnapshot);

      const loadedValue = await loadedSnapshot.$jazz.ensureLoaded({
        resolve: { ref: { pet: true } },
      });

      expect(loadedValue.ref.name).toBe("John");
      expect(loadedValue.ref.pet.nickname).toBe("Rex");
      expect(loadedValue.ref.pet.favoriteToy.$isLoaded).toBe(false);
    });
  });

  describe("composition", () => {
    test("snapshot as CoMap field", async () => {
      const Comment = co.map({
        text: z.string(),
        author: PersonSnapshot,
      });
      const person = Person.create({ name: "John", pet: { nickname: "Rex" } });
      const personSnapshot = await PersonSnapshot.create(person);
      assertLoaded(personSnapshot);

      const comment = Comment.create({
        text: "hello",
        author: personSnapshot,
      });

      const loadedComment = await Comment.load(comment.$jazz.id, {
        resolve: { author: { ref: { pet: true } } },
      });

      assertLoaded(loadedComment);
      expect(loadedComment.author.ref.name).toBe("John");
      expect(loadedComment.author.ref.pet.nickname).toBe("Rex");
    });

    test("setting snapshot in CoMap field", async () => {
      const Comment = co.map({
        text: z.string(),
        author: PersonSnapshot,
      });
      const person = Person.create({ name: "John", pet: { nickname: "Rex" } });
      const personSnapshot = await PersonSnapshot.create(person);
      assertLoaded(personSnapshot);

      const comment = Comment.create({
        text: "hello",
        author: personSnapshot,
      });

      person.$jazz.applyDiff({
        name: "Jane",
        pet: { nickname: "Puff" },
      });

      const newSnapshot = await PersonSnapshot.create(person);
      assertLoaded(newSnapshot);

      comment.$jazz.set("author", newSnapshot);

      const loadedComment = await Comment.load(comment.$jazz.id, {
        resolve: { author: { ref: { pet: true } } },
      });

      assertLoaded(loadedComment);
      expect(loadedComment.author.ref.name).toBe("Jane");
      expect(loadedComment.author.ref.pet.nickname).toBe("Puff");
    });

    test("setting snapshot in CoMap field from another deeply loaded snapshot", async () => {
      const Comment = co.map({
        text: z.string(),
        author: PersonSnapshot,
      });
      const person = Person.create({ name: "John", pet: { nickname: "Rex" } });
      const personSnapshot = await PersonSnapshot.create(person);
      assertLoaded(personSnapshot);

      const comment = Comment.create({
        text: "hello",
        author: personSnapshot,
      });

      const loadedComment = await Comment.load(comment.$jazz.id, {
        resolve: { author: true },
      });
      assertLoaded(loadedComment);

      const anotherComment = Comment.create({
        text: "World",
        author: loadedComment.author,
      });

      const loadedSecondComment = await Comment.load(anotherComment.$jazz.id, {
        resolve: { author: true },
      });
      assertLoaded(loadedSecondComment);

      expect(loadedComment.author.$jazz.id).toBe(
        loadedSecondComment.author.$jazz.id,
      );
      expect(loadedComment.author.ref.$jazz.id).toBe(
        loadedSecondComment.author.ref.$jazz.id,
      );
    });

    test("snapshot in CoList", async () => {
      const SnapshotList = co.list(PersonSnapshot);
      const person = Person.create({ name: "Ava", pet: { nickname: "Puff" } });
      const snapshot = await PersonSnapshot.create(person);
      assertLoaded(snapshot);

      const list = SnapshotList.create([snapshot]);

      const loadedList = await SnapshotList.load(list.$jazz.id, {
        resolve: {
          $each: { ref: { pet: true } },
        },
      });

      assertLoaded(loadedList);
      assertLoaded(loadedList[0]!);

      expect(loadedList[0]!.ref.name).toBe("Ava");
      expect(loadedList[0]!.ref.pet.nickname).toBe("Puff");
    });

    test("deep loading through composition", async () => {
      const Comment = co.map({
        body: z.string(),
        author: PersonSnapshot,
      });
      const Thread = co.map({
        title: z.string(),
        comments: co.list(Comment),
      });
      const person = Person.create({
        name: "Iris",
        pet: { nickname: "Mochi" },
      });
      const snapshot = await PersonSnapshot.create(person);
      assertLoaded(snapshot);

      const thread = Thread.create({
        title: "Snapshots",
        comments: [{ body: "first", author: snapshot }],
      });

      const loadedThread = await Thread.load(thread.$jazz.id, {
        resolve: {
          comments: {
            $each: {
              author: { ref: { pet: true } },
            },
          },
        },
      });

      assertLoaded(loadedThread);
      expect(loadedThread.comments[0]?.author.ref.name).toBe("Iris");
      expect(loadedThread.comments[0]?.author.ref.pet.nickname).toBe("Mochi");
    });
  });

  describe("edge cases", () => {
    test("optional inner refs", async () => {
      const OptionalPerson = co.map({
        name: z.string(),
        pet: co.optional(Pet),
      });
      const OptionalPersonSnapshot = co.snapshotRef(OptionalPerson, {
        cursorResolve: { pet: true },
      });

      const person = OptionalPerson.create({ name: "Nia" });
      const snapshot = await OptionalPersonSnapshot.create(person);
      assertLoaded(snapshot);

      person.$jazz.set("pet", Pet.create({ nickname: "Later" }));

      const result = await OptionalPersonSnapshot.load(snapshot.$jazz.id, {
        resolve: { ref: { pet: true } },
      });

      const latest = await OptionalPerson.load(person.$jazz.id, {
        resolve: { pet: true },
      });

      assertLoaded(result);
      assertLoaded(latest);

      expect(result.ref.pet).toBeUndefined();
      expect(latest.pet?.nickname).toBe("Later");
    });

    test("$onError catch", async () => {
      const alice = await createJazzTestAccount({
        creationProps: { name: "Alice" },
      });
      const publicOwner = Group.create({ owner: alice }).makePublic("reader");
      const secretOwner = Group.create({ owner: alice });
      const person = Person.create(
        {
          name: "Casey",
          pet: Pet.create({ nickname: "Shadow" }, { owner: secretOwner }),
        },
        { owner: publicOwner },
      );

      const snapshot = await PersonSnapshot.create(person, {
        owner: publicOwner,
      });

      const result = await PersonSnapshot.load(snapshot.$jazz.id, {
        loadAs: me,
        resolve: {
          ref: {
            $onError: "catch",
            pet: true,
          },
        },
      });
      assertLoaded(result);

      expect(result.$isLoaded).toBe(true);
      expect(result.ref.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });

    test("inner $onError catch", async () => {
      const Family = co.map({
        sibling: PersonSnapshot,
      });

      const alice = await createJazzTestAccount({
        creationProps: { name: "Alice" },
      });
      const publicOwner = Group.create({ owner: alice }).makePublic("reader");
      const secretOwner = Group.create({ owner: alice });
      const person = Person.create(
        {
          name: "Casey",
          pet: Pet.create({ nickname: "Shadow" }, { owner: secretOwner }),
        },
        { owner: publicOwner },
      );

      const snapshot = await PersonSnapshot.create(person, {
        owner: publicOwner,
      });
      assertLoaded(snapshot);

      const family = Family.create({
        sibling: snapshot,
      });

      const result = await Family.load(family.$jazz.id, {
        loadAs: me,
        resolve: {
          sibling: {
            ref: {
              pet: {
                $onError: "catch",
              },
            },
          },
        },
      });

      assertLoaded(result);
      expect(result.$isLoaded).toBe(true);
      expect(result.sibling.ref.$isLoaded).toBe(true);
      expect(result.sibling.ref.pet.$isLoaded).toBe(false);
      expect(result.sibling.ref.pet.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });

    test("$onError catch deleted snapshot referenced value", async () => {
      const Family = co.map({
        sibling: PersonSnapshot,
      });

      const person = Person.create({ name: "Jane", pet: { nickname: "Rex" } });
      const personSnapshot = await PersonSnapshot.create(person);
      assertLoaded(personSnapshot);

      const family = Family.create({ sibling: personSnapshot });

      person.$jazz.raw.core.deleteCoValue();

      const loadedFamily = await Family.load(family.$jazz.id, {
        resolve: {
          sibling: {
            ref: {
              pet: true,
              $onError: "catch",
            },
          },
        },
      });

      assertLoaded(loadedFamily);
      expect(loadedFamily.$isLoaded).toBe(true);
      expect(loadedFamily.sibling.ref.$isLoaded).toBe(false);
      expect(loadedFamily.sibling.ref.$jazz.loadingState).toBe(
        CoValueLoadingState.DELETED,
      );
    });

    test("sync between peers", async () => {
      const { clientNode, clientAccount, serverAccount } =
        await setupTwoNodes();
      const owner = Group.create(clientAccount).makePublic("reader");
      const person = Person.create(
        {
          name: "Rae",
          pet: { nickname: "Pip" },
        },
        { owner },
      );

      const snapshot = await PersonSnapshot.create(person, {
        owner,
      });

      await clientAccount.$jazz.waitForAllCoValuesSync({ timeout: 1000 });
      await clientNode.gracefulShutdown();

      const result = await PersonSnapshot.load(snapshot.$jazz.id, {
        loadAs: serverAccount,
        resolve: { ref: { pet: true } },
      });

      assertLoaded(result);
      expect(result.ref.name).toBe("Rae");
      expect(result.ref.pet.nickname).toBe("Pip");
    });

    test("snapshot of deleted CoValue", async () => {
      const owner = Group.create(me).makePublic("reader");
      const person = Person.create(
        {
          name: "Mara",
          pet: { nickname: "Ghost" },
        },
        { owner },
      );

      const snapshot = await PersonSnapshot.create(person, { owner });

      person.$jazz.raw.core.deleteCoValue();
      await person.$jazz.raw.core.waitForSync();

      const viewer = await createJazzTestAccount({
        creationProps: { name: "Viewer" },
      });
      const result = await PersonSnapshot.load(snapshot.$jazz.id, {
        loadAs: viewer,
        skipRetry: true,
        resolve: {
          ref: true,
        },
      });

      expect(result.$isLoaded).toBe(false);
      expect(result.$jazz.loadingState).toBe(CoValueLoadingState.DELETED);
    });
  });
});
