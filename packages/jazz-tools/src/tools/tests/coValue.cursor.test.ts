import { cojsonInternals } from "cojson";
import { beforeEach, describe, expect, test } from "vitest";
import { Group, co, z, ResolveQuery } from "../exports.js";
import { CoValueLoadingState } from "../internal.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { assertLoaded } from "./utils.js";
import { decodeAndValidateCursor } from "../subscribe/cursor";

describe("Creating and loading CoValues with cursors", () => {
  let me: Awaited<ReturnType<typeof createJazzTestAccount>>;

  beforeEach(async () => {
    cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 1000;

    await setupJazzTestSync();

    me = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  describe("createCursor", () => {
    test("returns a non-empty opaque string for a loaded root", async () => {
      const cursor = me.$jazz.createCursor();
      expect(cursor).toBeTypeOf("string");
      expect(cursor).toMatch(/^cursor_z/);
    });

    test("root with resolved descendants encodes frontiers", async () => {
      const Person = co.map({
        name: z.string(),
        get friends() {
          return co.list(Person);
        },
      });

      const john = Person.create({
        name: "John",
        friends: [
          {
            name: "Jane",
            friends: [
              {
                name: "Bob",
                friends: [],
              },
            ],
          },
        ],
      });

      const resolve = {
        friends: {
          $each: {
            friends: {
              $each: true,
            },
          },
        },
      } satisfies ResolveQuery<typeof Person>;

      const loadedJohn = await john.$jazz.ensureLoaded({
        resolve,
      });

      const cursor = loadedJohn.$jazz.createCursor();
      const decodedCursor = decodeAndValidateCursor({
        cursor,
        rootId: loadedJohn.$jazz.id,
        resolve,
      });

      expect(decodedCursor.frontiers).toEqual({
        [john.$jazz.id]: expect.anything(),
        [john.friends.$jazz.id]: expect.anything(),
        [john.friends[0]!.$jazz.id]: expect.anything(),
        [john.friends[0]!.friends.$jazz.id]: expect.anything(),
        [john.friends[0]!.friends[0]!.$jazz.id]: expect.anything(),
      });
    });

    test("CoList with no `resolve` encodes only the root frontier", async () => {
      const TodoList = co.list(co.map({ title: z.string() }));
      const todos = TodoList.create([{ title: "Todo 1" }]);
      const loadedTodos = await todos.$jazz.ensureLoaded({
        resolve: true,
      });
      const cursor = loadedTodos.$jazz.createCursor();
      const decodedCursor = decodeAndValidateCursor({
        cursor,
        rootId: todos.$jazz.id,
        resolve: {},
      });

      expect(decodedCursor.frontiers).toEqual({
        [todos.$jazz.id]: expect.anything(),
      });
    });

    test("CoMap with no `resolve` encodes only the root frontier", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        pet: co.map({ name: z.string() }),
      });

      const person = Person.create({
        name: "John",
        age: 30,
        pet: { name: "Rex" },
      });

      const loadedPerson = await person.$jazz.ensureLoaded({
        resolve: true,
      });

      // triggering load on `.owner` so  trigger a child subscription
      // and then ensure it does not get added to frontier
      expect(loadedPerson.$jazz.owner).toBeDefined();

      const cursor = loadedPerson.$jazz.createCursor();
      const decodedCursor = decodeAndValidateCursor({
        cursor,
        rootId: person.$jazz.id,
        resolve: {},
      });

      expect(decodedCursor.frontiers).toEqual({
        [person.$jazz.id]: expect.anything(),
      });
    });

    test("CoFeed with `resolve` encodes frontiers for all entries", async () => {
      const EventFeed = co.feed(co.map({ name: z.string() }));

      const eventFeed = EventFeed.create([{ name: "event-1" }]);
      const loadedEventFeed = await eventFeed.$jazz.ensureLoaded({
        resolve: { $each: true },
      });
      const cursor = loadedEventFeed.$jazz.createCursor();

      const decodedCursor = decodeAndValidateCursor({
        cursor,
        rootId: eventFeed.$jazz.id,
        resolve: { $each: true },
      });

      expect(decodedCursor.frontiers).toEqual({
        [loadedEventFeed.$jazz.id]: expect.anything(),
        [loadedEventFeed.byMe!.value.$jazz.id]: expect.anything(),
      });
    });

    test("CoMap with no `resolve` does not encode autoloaded children frontiers", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        pet: co.map({ name: z.string() }),
      });

      const person = Person.create({
        name: "John",
        age: 30,
        pet: { name: "Rex" },
      });

      const loadedPerson = await person.$jazz.ensureLoaded({
        resolve: true,
      });

      // trigger autoload on pet
      const petLoadingState = loadedPerson.pet.$jazz.loadingState;
      expect(petLoadingState).toBeOneOf([
        CoValueLoadingState.LOADING,
        CoValueLoadingState.LOADED,
      ]);

      const cursor = loadedPerson.$jazz.createCursor();

      const decodedCursor = decodeAndValidateCursor({
        cursor,
        rootId: person.$jazz.id,
        resolve: true,
      });

      expect(decodedCursor.frontiers).toEqual({
        [person.$jazz.id]: expect.anything(),
      });
    });

    test("CoFeed returns a valid cursor", async () => {
      const EventFeed = co.feed(z.string());
      const eventFeed = EventFeed.create(["event-1", "event-2"]);
      const cursor = eventFeed.$jazz.createCursor();
      expect(cursor).toMatch(/^cursor_z/);
    });

    test("CoPlainText returns a valid cursor", async () => {
      const text = co.plainText().create("Hello world");
      const cursor = text.$jazz.createCursor();
      expect(cursor).toMatch(/^cursor_z/);
    });

    test("multi-level `resolve` (root → list → map → text) encodes all frontiers", async () => {
      const Project = co.map({
        name: z.string(),
        tasks: co.list(
          co.map({
            title: z.string(),
            description: co.plainText(),
          }),
        ),
      });

      const project = Project.create({
        name: "Project 1",
        tasks: [
          {
            title: "Task 1",
            description: "A task",
          },
        ],
      });

      const resolve = {
        tasks: {
          $each: {
            description: true,
          },
        },
      } satisfies ResolveQuery<typeof Project>;

      const loadedProject = await project.$jazz.ensureLoaded({
        resolve,
      });

      const cursor = loadedProject.$jazz.createCursor();
      const decodedCursor = decodeAndValidateCursor({
        cursor,
        rootId: project.$jazz.id,
        resolve,
      });

      expect(decodedCursor.frontiers).toEqual({
        [project.$jazz.id]: expect.anything(),
        [project.tasks.$jazz.id]: expect.anything(),
        [project.tasks[0]!.$jazz.id]: expect.anything(),
        [project.tasks[0]!.description.$jazz.id]: expect.anything(),
      });
    });

    test("returns the same cursor when called multiple times without mutations", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const person = Person.create({ name: "John", age: 30 });

      const cursor1 = person.$jazz.createCursor();
      const cursor2 = person.$jazz.createCursor();
      expect(cursor1).toBe(cursor2);
    });

    test("returns a different cursor after a mutation", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const person = Person.create({ name: "John", age: 30 });

      const cursor1 = person.$jazz.createCursor();
      person.$jazz.set("name", "Jane");
      const cursor2 = person.$jazz.createCursor();
      expect(cursor1).not.toBe(cursor2);
    });
  });

  describe("load() with cursor", () => {
    test("loads original snapshot of CoMap after mutation", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const person = Person.create({ name: "John", age: 30 });
      const personSnapshot = await person.$jazz.ensureLoaded({
        resolve: true,
        cursor: {
          useCurrentCursor: true,
        },
      });

      person.$jazz.set("name", "Jane");

      expect(personSnapshot.name).toBe("John");

      const loadedSnapshot = await Person.load(person.$jazz.id, {
        cursor: personSnapshot.$jazz.cursor,
      });
      assertLoaded(loadedSnapshot);
      expect(loadedSnapshot.name).toBe("John");
    });

    test("loads original snapshot of CoList after mutation", async () => {
      const TodoList = co.list(z.string());

      const todoList = TodoList.create(["Buy groceries", "Walk the dog"]);
      const todoListSnapshot = await todoList.$jazz.ensureLoaded({
        resolve: true,
        cursor: {
          useCurrentCursor: true,
        },
      });

      todoList.$jazz.push("Call mom");

      const loadedSnapshot = await TodoList.load(todoList.$jazz.id, {
        cursor: todoListSnapshot.$jazz.cursor,
      });
      assertLoaded(loadedSnapshot);
      expect(loadedSnapshot).toEqual(["Buy groceries", "Walk the dog"]);
    });

    test("loads original snapshot of CoFeed after mutation", async () => {
      const EventFeed = co.feed(z.string());

      const eventFeed = EventFeed.create(["event-1", "event-2"]);
      const eventFeedSnapshot = await eventFeed.$jazz.ensureLoaded({
        resolve: true,
        cursor: {
          useCurrentCursor: true,
        },
      });

      eventFeed.$jazz.push("event-3");

      const loadedSnapshot = await EventFeed.load(eventFeed.$jazz.id, {
        cursor: eventFeedSnapshot.$jazz.cursor,
      });
      assertLoaded(loadedSnapshot);
      expect(loadedSnapshot.perAccount[me.$jazz.id]?.value).toBe("event-2");
    });

    test("loads original snapshot of CoPlainText after mutation", async () => {
      const text = co.plainText().create("Hello world");
      const textSnapshot = await co.plainText().load(text.$jazz.id, {
        loadAs: me,
        cursor: {
          useCurrentCursor: true,
        },
      });
      assertLoaded(textSnapshot);

      text.$jazz.applyDiff("Hello Jazz");

      const loadedSnapshot = await co.plainText().load(text.$jazz.id, {
        loadAs: me,
        cursor: textSnapshot.$jazz.cursor,
      });
      assertLoaded(loadedSnapshot);
      expect(loadedSnapshot.toString()).toBe("Hello world");
    });

    test("loads original snapshot of branched CoMap after mutation", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const person = Person.create({ name: "John", age: 30 });
      const branchName = "cursor-branch";
      const branchedPerson = await Person.load(person.$jazz.id, {
        unstable_branch: { name: branchName },
      });
      assertLoaded(branchedPerson);

      branchedPerson.$jazz.applyDiff({ name: "Jane", age: 31 });

      const branchedPersonSnapshot = await branchedPerson.$jazz.ensureLoaded({
        resolve: true,
        unstable_branch: { name: branchName },
        cursor: {
          useCurrentCursor: true,
        },
      });

      branchedPerson.$jazz.applyDiff({ name: "Bob", age: 32 });

      const loadedSnapshot = await Person.load(person.$jazz.id, {
        unstable_branch: { name: branchName },
        cursor: branchedPersonSnapshot.$jazz.cursor,
      });
      assertLoaded(loadedSnapshot);
      expect(loadedSnapshot.name).toBe("Jane");
      expect(loadedSnapshot.age).toBe(31);
    });

    test("loads original snapshot of multi-level value with mutations at all levels", async () => {
      const Organization = co.map({
        name: z.string(),
        description: co.plainText(),
        teams: co.list(
          co.map({
            name: z.string(),
            members: co.list(
              co.map({
                name: z.string(),
                role: z.string(),
              }),
            ),
          }),
        ),
        projects: co.list(
          co.map({
            name: z.string(),
            tasks: co.list(
              co.map({
                title: z.string(),
                description: z.string(),
                status: z.string(),
                comments: co.feed(
                  co.map({
                    text: z.string(),
                    author: z.string(),
                  }),
                ),
              }),
            ),
          }),
        ),
      });

      const original = Organization.create({
        name: "Acme Corporation",
        description: "A company that makes things.",
        teams: [
          {
            name: "Engineering",
            members: [
              { name: "John", role: "Lead" },
              { name: "Jane", role: "Developer" },
            ],
          },
        ],
        projects: [
          {
            name: "Project 1",
            tasks: [
              {
                title: "Task 1",
                description: "A task",
                status: "todo",
                comments: [
                  { text: "A comment", author: "John" },
                  { text: "Another comment", author: "Jane" },
                ],
              },
            ],
          },
        ],
      });

      const originalJson = original.toJSON();

      const resolve = {
        description: true,
        teams: { $each: { members: { $each: true } } },
        projects: {
          $each: { tasks: { $each: { comments: { $each: true } } } },
        },
      } satisfies ResolveQuery<typeof Organization>;

      const originalSnapshot = await Organization.load(original.$jazz.id, {
        resolve,
        cursor: {
          useCurrentCursor: true,
        },
      });
      assertLoaded(originalSnapshot);

      original.$jazz.applyDiff({
        name: "Acme + Garden",
        description: "A company that makes things and gardens.",
      });

      original.teams[0]?.$jazz.set("name", "Engineering + Design");
      original.teams.$jazz.push({
        name: "Marketing",
        members: [],
      });
      original.projects[0]?.$jazz.set("name", "Project 1 + Marketing");
      original.projects[0]?.tasks[0]?.$jazz.set("title", "Task 1 + Marketing");
      original.projects[0]?.tasks[0]?.comments.$jazz.push({
        text: "A comment",
        author: "Wile E. Coyote",
      });
      original.projects[0]?.tasks.$jazz.push({
        title: "Task 2",
        description: "A task",
        status: "todo",
        comments: [],
      });

      const loadedSnapshot = await Organization.load(original.$jazz.id, {
        resolve,
        cursor: originalSnapshot.$jazz.cursor,
      });
      assertLoaded(loadedSnapshot);

      expect(loadedSnapshot.toJSON()).toEqual(originalJson);
    });

    test("snapshot loaded with cursor does not autoload non-resolved children", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        pet: co.map({ name: z.string() }),
      });

      const person = Person.create({
        name: "John",
        age: 30,
        pet: { name: "Rex" },
      });

      const personSnapshot = await Person.load(person.$jazz.id, {
        cursor: {
          useCurrentCursor: true,
        },
      });

      assertLoaded(personSnapshot);

      expect(personSnapshot.pet.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
    });

    test("CoMap loaded with cursor cannot be mutated", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const person = Person.create({
        name: "John",
        age: 30,
      });

      const personSnapshot = await person.$jazz.ensureLoaded({
        resolve: true,
        cursor: {
          useCurrentCursor: true,
        },
      });

      expect(() => {
        personSnapshot.$jazz.set("name", "Jane");
      }).toThrowError("Cannot set value on a time travel entity");
    });

    test("CoList loaded with cursor cannot be mutated", async () => {
      const TodoList = co.list(z.string());

      const todoList = TodoList.create(["Buy groceries", "Walk the dog"]);

      const todoListSnapshot = await todoList.$jazz.ensureLoaded({
        resolve: true,
        cursor: {
          useCurrentCursor: true,
        },
      });

      expect(() => {
        todoListSnapshot.$jazz.push("Call mom");
      }).toThrowError("Cannot mutate a time travel entity");
    });

    test("CoFeed loaded with cursor cannot be mutated", async () => {
      const EventFeed = co.feed(z.string());

      const eventFeed = EventFeed.create(["event-1", "event-2"]);

      const eventFeedSnapshot = await eventFeed.$jazz.ensureLoaded({
        resolve: true,
        cursor: {
          useCurrentCursor: true,
        },
      });

      expect(() => {
        eventFeedSnapshot.$jazz.push("event-3");
      }).toThrowError("Cannot mutate a time travel entity");
    });
  });

  test("ensureLoaded() with cursor", async () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
      pet: co.map({ name: z.string() }),
    });

    const person = Person.create({
      name: "John",
      age: 30,
      pet: { name: "Rex" },
    });

    const personSnapshot = await person.$jazz.ensureLoaded({
      resolve: { pet: true },
      cursor: {
        useCurrentCursor: true,
      },
    });

    person.$jazz.set("name", "Jane");
    person.pet.$jazz.set("name", "Fido");

    const loadedSnapshot = await person.$jazz.ensureLoaded({
      resolve: { pet: true },
      cursor: personSnapshot.$jazz.cursor,
    });

    expect(loadedSnapshot.name).toBe("John");
    expect(loadedSnapshot.pet.name).toBe("Rex");
  });

  describe("load() with cursor and errored descendants", () => {
    test("keeps missing nested optional values with cursor", async () => {
      const Pet = co.map({ name: z.string() });
      const Person = co.map({
        name: z.string(),
        pet: co.optional(Pet),
      });

      const original = Person.create({ name: "John" });

      const loadedOriginal = await Person.load(original.$jazz.id, {
        resolve: { pet: true },
      });
      assertLoaded(loadedOriginal);

      const cursor = loadedOriginal.$jazz.createCursor();

      original.$jazz.applyDiff({
        name: "John Smith",
        pet: Pet.create({ name: "Rex" }),
      });

      const snapshot = await Person.load(original.$jazz.id, {
        resolve: { pet: true },
        cursor: cursor,
      });
      assertLoaded(snapshot);

      expect(snapshot.pet).toBeUndefined();

      const latest = await Person.load(original.$jazz.id, {
        resolve: { pet: true },
      });
      assertLoaded(latest);
      expect(latest.pet?.name).toBe("Rex");
    });

    test("handles nested values without access permissions", async () => {
      const Secret = co.map({ value: z.string() });
      const Document = co.map({
        title: z.string(),
        secret: co.optional(Secret),
      });

      const alice = await createJazzTestAccount({
        creationProps: { name: "Alice" },
      });

      const secretGroup = Group.create({ owner: alice });
      const secret = Secret.create(
        { value: "classified" },
        { owner: secretGroup },
      );
      const original = Document.create({
        title: "Doc",
        secret,
      });

      const loadedOriginal = await Document.load(original.$jazz.id, {
        resolve: { secret: { $onError: "catch" } },
      });
      assertLoaded(loadedOriginal);
      expect(loadedOriginal.secret?.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );

      const cursor = loadedOriginal.$jazz.createCursor();

      const snapshot = await Document.load(original.$jazz.id, {
        resolve: { secret: { $onError: "catch" } },
        cursor: cursor,
      });
      assertLoaded(snapshot);

      expect(snapshot.secret?.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
    });

    test("handles nested values when access is granted after cursor creation", async () => {
      const Secret = co.map({ value: z.string() });
      const Document = co.map({
        title: z.string(),
        secret: co.optional(Secret),
      });

      const alice = await createJazzTestAccount({
        creationProps: { name: "Alice" },
      });

      const secretGroup = Group.create({ owner: alice });
      const secret = Secret.create(
        { value: "classified" },
        { owner: secretGroup },
      );
      const original = Document.create({
        title: "Doc",
        secret,
      });

      const loadedOriginal = await Document.load(original.$jazz.id, {
        resolve: { secret: { $onError: "catch" } },
      });
      assertLoaded(loadedOriginal);
      expect(loadedOriginal.secret?.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );

      const cursor = loadedOriginal.$jazz.createCursor();

      secretGroup.addMember(me, "reader");

      const snapshot = await Document.load(original.$jazz.id, {
        resolve: { secret: { $onError: "catch" } },
        cursor: cursor,
      });
      assertLoaded(snapshot);

      expect(snapshot.secret?.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );

      const latest = await Document.load(original.$jazz.id, {
        resolve: { secret: { $onError: "catch" } },
      });
      assertLoaded(latest);
      expect(latest.secret?.$jazz.loadingState).toBe(
        CoValueLoadingState.LOADED,
      );
    });

    test("handles nested values when access is removed after cursor creation", async () => {
      const Secret = co.map({ value: z.string() });
      const Document = co.map({
        title: z.string(),
        secret: co.optional(Secret),
      });

      const alice = await createJazzTestAccount({
        creationProps: { name: "Alice" },
      });

      const secretGroup = Group.create({ owner: alice });
      secretGroup.addMember(me, "reader");

      const secret = Secret.create(
        { value: "classified" },
        { owner: secretGroup },
      );
      const original = Document.create({
        title: "Doc",
        secret,
      });

      const loadedOriginal = await Document.load(original.$jazz.id, {
        resolve: { secret: { $onError: "catch" } },
      });
      assertLoaded(loadedOriginal);
      expect(loadedOriginal.secret?.$jazz.loadingState).toBe(
        CoValueLoadingState.LOADED,
      );

      const cursor = loadedOriginal.$jazz.createCursor();

      secretGroup.removeMember(me);

      const snapshot = await Document.load(original.$jazz.id, {
        resolve: { secret: { $onError: "catch" } },
        cursor: cursor,
      });
      assertLoaded(snapshot);

      expect(snapshot.secret?.$jazz.loadingState).toBe(
        CoValueLoadingState.LOADED,
      );

      const latest = await Document.load(original.$jazz.id, {
        resolve: { secret: { $onError: "catch" } },
      });
      assertLoaded(latest);
      expect(latest.secret?.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });
  });
});
