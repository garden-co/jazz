import { cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import {
  assert,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  it,
  test,
  vi,
} from "vitest";
import { Group, co, subscribeToCoValue, z } from "../exports.js";
import { Account } from "../index.js";
import {
  Loaded,
  TypeSym,
  activeAccountContext,
  coValueClassFromCoValueClassOrSchema,
  CoValueLoadingState,
  exportCoValue,
  MaybeLoaded,
} from "../internal.js";
import {
  createJazzTestAccount,
  disableJazzTestSync,
  getPeerConnectedToTestSyncServer,
  runWithoutActiveAccount,
  setActiveAccount,
  setupJazzTestSync,
} from "../testing.js";
import {
  assertLoaded,
  expectValidationError,
  setupTwoNodes,
  waitFor,
} from "./utils.js";
import { setDefaultValidationMode } from "../implementation/zodSchema/validationSettings.js";

const Crypto = await WasmCrypto.create();

beforeEach(async () => {
  cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 1000;

  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });
});

describe("CoMap", async () => {
  describe("init", () => {
    test("create a CoMap with basic property access", () => {
      const Person = co.map({
        color: z.string(),
        _height: z.number(),
        birthday: z.date(),
        name: z.string(),
        enum: z.enum(["a", "b", "c"]),
        enumMap: z.enum({ a: 1, b: 2, c: 3 }),
        // nullable: z.optional.encoded<string | undefined>({
        //   encode: (value: string | undefined) => value || null,
        //   decode: (value: unknown) => (value as string) || undefined,
        // })
        optionalDate: z.date().optional(),
      });

      const birthday = new Date("1989-11-27");

      const john = Person.create({
        color: "red",
        _height: 10,
        birthday,
        name: "John",
        enum: "a",
        enumMap: 1,
      });

      expect(john.color).toEqual("red");
      expect(john._height).toEqual(10);
      expect(john.birthday).toEqual(birthday);
      expect(john.$jazz.raw.get("birthday")).toEqual(birthday.toISOString());
      expect(Object.keys(john)).toEqual([
        "color",
        "_height",
        "birthday",
        "name",
        "enum",
        "enumMap",
      ]);
      expect(john.enum).toEqual("a");
      expect(john.enumMap).toEqual(1);
    });

    test("property existence", () => {
      const Person = co.map({
        name: z.string(),
      });

      const john = Person.create({ name: "John" });

      expect("name" in john).toEqual(true);
      // "age" is not in the schema, so `in` returns false
      expect("age" in john).toEqual(false);
    });

    test("internal properties are not enumerable", () => {
      const Person = co.map({
        name: z.string(),
      });

      const person = Person.create({ name: "John" });

      expect(Object.keys(person)).toEqual(["name"]);
      expect(person).toEqual({ name: "John" });
    });

    test("create a CoMap with an account as owner", () => {
      const Person = co.map({
        name: z.string(),
      });

      const john = Person.create({ name: "John" }, Account.getMe());

      expect(john.name).toEqual("John");
      expect(john.$jazz.raw.get("name")).toEqual("John");
    });

    test("create a CoMap with a group as owner", () => {
      const Person = co.map({
        name: z.string(),
      });

      const john = Person.create({ name: "John" }, Group.create());

      expect(john.name).toEqual("John");
      expect(john.$jazz.raw.get("name")).toEqual("John");
    });

    test("Empty schema", () => {
      const emptyMap = co.map({}).create({});

      // @ts-expect-error
      expect(emptyMap.color).toEqual(undefined);
    });

    test("create CoMap with reference using CoValue", () => {
      const Dog = co.map({
        name: z.string(),
      });

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        dog: Dog,
      });

      const person = Person.create({
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex" }),
      });

      expect(person.dog?.name).toEqual("Rex");
    });

    test("assign a child by only passing the id", () => {
      const Dog = co.map({
        name: z.string(),
      });

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        dog: Dog,
      });

      const dog = Dog.create({ name: "Rex" });

      const person = Person.create(
        {
          name: "John",
          age: 20,
          // @ts-expect-error - This is an hack to test the behavior
          dog: { $jazz: { id: dog.$jazz.id } },
        },
        { validation: "loose" },
      );

      expect(person.dog?.name).toEqual("Rex");
    });

    describe("create CoMap with references using JSON", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        name: co.plainText(),
        bio: co.richText(),
        dog: Dog,
        get friends() {
          return co.list(Person);
        },
        reactions: co.feed(co.plainText()),
        pet: Pet,
        pets: co.record(z.string(), Pet),
      });

      let person: ReturnType<typeof Person.create>;

      beforeEach(() => {
        person = Person.create({
          name: "John",
          bio: "I am a software engineer",
          dog: { type: "dog", name: "Rex" },
          friends: [
            {
              name: "Jane",
              bio: "I am a mechanical engineer",
              dog: { type: "dog", name: "Fido" },
              friends: [],
              reactions: [],
              pet: { type: "dog", name: "Fido" },
              pets: {
                dog: { type: "dog", name: "Fido" },
              },
            },
          ],
          reactions: ["👎", "👍"],
          pet: { type: "cat", name: "Whiskers" },
          pets: {
            dog: { type: "dog", name: "Rex" },
            cat: { type: "cat", name: "Whiskers" },
          },
        });
      });

      it("automatically creates CoValues for each CoValue reference", () => {
        expect(person.name.toString()).toEqual("John");
        expect(person.bio.toString()).toEqual("I am a software engineer");
        expect(person.dog?.name).toEqual("Rex");
        expect(person.friends.length).toEqual(1);
        expect(person.friends[0]?.name.toString()).toEqual("Jane");
        expect(person.friends[0]?.bio.toString()).toEqual(
          "I am a mechanical engineer",
        );
        expect(person.friends[0]?.dog.name).toEqual("Fido");
        expect(person.friends[0]?.friends.length).toEqual(0);
        expect(person.reactions.byMe?.value?.toString()).toEqual("👍");
        expect(person.pet.name).toEqual("Whiskers");
        expect(person.pets.dog?.name).toEqual("Rex");
        expect(person.pets.cat?.name).toEqual("Whiskers");
      });

      it("creates a group for each new CoValue that is a child of the referencing CoValue's owner", () => {
        for (const value of Object.values(person)) {
          expect(
            value.$jazz.owner
              .getParentGroups()
              .map((group: Group) => group.$jazz.id),
          ).toContain(person.$jazz.owner.$jazz.id);
        }
        const friend = person.friends[0]!;
        for (const value of Object.values(friend)) {
          expect(
            value.$jazz.owner
              .getParentGroups()
              .map((group: Group) => group.$jazz.id),
          ).toContain(friend.$jazz.owner.$jazz.id);
        }
      });

      it("can create a coPlainText from an empty string", () => {
        const Schema = co.map({ text: co.plainText() });
        const map = Schema.create({ text: "" });
        expect(map.text.toString()).toBe("");
      });

      it("creates a group for the new CoValue when there is no active account", () => {
        const Schema = co.map({ text: co.plainText() });

        const parentGroup = Group.create();
        runWithoutActiveAccount(() => {
          const map = Schema.create({ text: "Hello" }, parentGroup);

          expect(
            map.text.$jazz.owner
              .getParentGroups()
              .map((group: Group) => group.$jazz.id),
          ).toContain(parentGroup.$jazz.id);
        });
      });
    });

    test("CoMap with self reference", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        get friend() {
          return co.optional(Person);
        },
      });

      const person = Person.create({
        name: "John",
        age: 20,
        friend: Person.create({ name: "Jane", age: 21 }),
      });

      expect(person.friend?.name).toEqual("Jane");
      expect(person.friend?.age).toEqual(21);
    });

    test("JSON.stringify should include user-defined properties + $jazz.id", () => {
      const Person = co.map({
        name: z.string(),
      });

      const person = Person.create({ name: "John" });

      expect(JSON.stringify(person)).toEqual(
        `{"$jazz":{"id":"${person.$jazz.id}"},"name":"John"}`,
      );
    });

    test("toJSON should not fail when there is a key in the raw value not represented in the schema", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const person = Person.create({ name: "John", age: 20 });

      person.$jazz.raw.set("extra", "extra");

      expect(person.toJSON()).toEqual({
        $jazz: { id: person.$jazz.id },
        name: "John",
        age: 20,
      });
    });

    test("toJSON should handle references", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        get friend(): co.Optional<typeof Person> {
          return co.optional(Person);
        },
      });

      const person = Person.create({
        name: "John",
        age: 20,
        friend: Person.create({ name: "Jane", age: 21 }),
      });

      expect(person.toJSON()).toEqual({
        $jazz: { id: person.$jazz.id },
        name: "John",
        age: 20,
        friend: {
          $jazz: { id: person.friend?.$jazz.id },
          name: "Jane",
          age: 21,
        },
      });
    });

    test("toJSON should handle circular references", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        get friend() {
          return co.optional(Person);
        },
      });

      const person = Person.create({
        name: "John",
        age: 20,
      });

      person.$jazz.set("friend", person);

      expect(person.toJSON()).toEqual({
        $jazz: { id: person.$jazz.id },
        name: "John",
        age: 20,
        friend: {
          _circular: person.$jazz.id,
        },
      });
    });

    test("testing toJSON on a CoMap with a Date field", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        birthday: z.date(),
      });

      const birthday = new Date();

      const john = Person.create({
        name: "John",
        age: 20,
        birthday,
      });

      expect(john.toJSON()).toMatchObject({
        $jazz: { id: john.$jazz.id },
        name: "John",
        age: 20,
        birthday: birthday.toISOString(),
      });
    });

    test("co.map with nested co.record should toJSON correctly", async () => {
      const Chat = co.map({
        title: z.string(),
      });

      const ChatRoot = co.map({
        chats: co.record(z.string(), Chat),
      });

      const chat = Chat.create({ title: "General" });
      const root = ChatRoot.create({
        chats: {
          general: chat,
        },
      });

      // Simulate loading in another context/component
      const loadedRoot = await ChatRoot.load(root.$jazz.id, {
        resolve: {
          chats: true,
        },
      });

      if (!loadedRoot || !loadedRoot.$isLoaded)
        throw new Error("Failed to load root");

      expect(loadedRoot.toJSON()).toEqual(
        expect.objectContaining({
          chats: expect.objectContaining({
            general: expect.objectContaining({
              title: "General",
            }),
          }),
        }),
      );
    });

    test("setting optional date as undefined should not throw", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        birthday: z.date().optional(),
      });

      const john = Person.create({
        name: "John",
        age: 20,
      });

      expect(john.toJSON()).toMatchObject({
        name: "John",
        age: 20,
      });
    });

    it("should disallow extra properties", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create(
        // @ts-expect-error - x is not a valid property
        { name: "John", age: 30, x: 1 },
        { validation: "loose" },
      );

      expect(john.toJSON()).toEqual({
        $jazz: { id: john.$jazz.id },
        name: "John",
        age: 30,
      });
    });

    it("should allow extra properties when catchall is provided", () => {
      const Person = co
        .map({
          name: z.string(),
          age: z.number(),
        })
        .catchall(z.string());

      const person = Person.create({ name: "John", age: 20 });
      expect(person.name).toEqual("John");
      expect(person.age).toEqual(20);
      expect(person.extra).toBeUndefined();

      person.$jazz.set("name", "Jane");
      person.$jazz.set("age", 28);
      person.$jazz.set("extra", "extra");

      expect(person.name).toEqual("Jane");
      expect(person.age).toEqual(28);
      expect(person.extra).toEqual("extra");
    });

    test("CoMap with reference can be created with a shallowly resolved reference", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        pet: Dog,
        get friend() {
          return Person.optional();
        },
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const pet = Dog.create({ name: "Rex", breed: "Labrador" }, group);
      const personA = Person.create(
        {
          name: "John",
          age: 20,
          pet,
        },
        { owner: group },
      );

      const userB = await createJazzTestAccount();
      const loadedPersonA = await Person.load(personA.$jazz.id, {
        resolve: true,
        loadAs: userB,
      });

      expect(loadedPersonA).not.toBeNull();
      assertLoaded(loadedPersonA);

      const personB = Person.create({
        name: "Jane",
        age: 28,
        pet,
        friend: loadedPersonA,
      });

      expect(personB.friend?.pet.name).toEqual("Rex");
    });

    it("should throw when creating with invalid properties", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      expectValidationError(() =>
        Person.create({
          name: "John",
          // @ts-expect-error - age should be a number
          age: "20",
        }),
      );
    });

    it("should not throw when creating with invalid properties with loose validation", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      expect(() =>
        Person.create(
          {
            name: "John",
            // @ts-expect-error - age should be a number
            age: "20",
          },
          { validation: "loose" },
        ),
      ).not.toThrow();
    });

    it("should throw when creating with extra properties", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      expectValidationError(() =>
        Person.create({
          name: "John",
          age: 20,
          // @ts-expect-error - extra is not a valid property
          extra: "extra",
        }),
      );
    });

    it("should validate Group schemas", async () => {
      const Person = co.map({
        group: co.group(),
        group2: Group,
      });

      expect(() =>
        Person.create({ group: Group.create(), group2: Group.create() }),
      ).not.toThrow();
      expect(() =>
        // @ts-expect-error - group should be a Group
        Person.create({ group: "Test", group2: Group.create() }),
      ).toThrow();
      expect(() =>
        // @ts-expect-error - group should be a Group
        Person.create({ group: Group.create(), group2: "Test" }),
      ).toThrow();
    });

    // .default() is not supported yet
    it.fails("should use zod defaults for plain items", async () => {
      const Person = co.map({
        name: z.string().default("John"),
        age: z.number().default(20),
      });

      // @ts-expect-error - name and age are required but have defaults
      const person = Person.create({});
      expect(person.name).toEqual("John");
      expect(person.age).toEqual(20);
    });

    test("CoMap validation should never validate a coValue instance as a plain object", () => {
      const Dog = co.list(z.string());

      const Person = co.map({
        pet: co.map({
          name: z.string(),
        }),
      });

      const dog = Dog.create(["Rex"]);

      expectValidationError(
        () =>
          Person.create({
            // @ts-expect-error - pet should be a CoMap
            pet: dog,
          }),
        [
          {
            code: "custom",
            message: "Expected a CoMap when providing a CoValue instance",
            path: ["pet"],
          },
        ],
      );
    });
  });

  describe("Mutation", () => {
    test("change a primitive value", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 20 });

      john.$jazz.set("name", "Jane");

      expect(john.name).toEqual("Jane");
      expect(john.age).toEqual(20);
    });

    test("change a primitive value should be validated", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 20 });

      expectValidationError(() =>
        john.$jazz.set("age", "21" as unknown as number),
      );

      expect(john.age).toEqual(20);
    });

    test("change a primitive value should not throw if validation is loose", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 20 });

      expect(() =>
        john.$jazz.set(
          "age",
          // @ts-expect-error - age should be a number
          "21",
          { validation: "loose" },
        ),
      ).not.toThrow();

      expect(john.age).toEqual("21");
    });

    test("delete an optional value by setting it to undefined", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      const john = Person.create({ name: "John", age: 20 });

      john.$jazz.set("age", undefined);

      expect(john.name).toEqual("John");
      expect(john.age).toEqual(undefined);

      expect(john.toJSON()).toEqual({
        $jazz: { id: john.$jazz.id },
        name: "John",
      });
      // `in` returns true for schema-defined keys even when value is undefined,
      // to satisfy proxy invariant consistency with ownKeys/getOwnPropertyDescriptor.
      expect("age" in john).toEqual(true);
      // $jazz.has() returns true because age was explicitly set (even to undefined)
      expect(john.$jazz.has("age")).toEqual(true);
      // The key still exists, since age === undefined
      expect(Object.keys(john)).toEqual(["name", "age"]);
    });

    test("delete optional properties using $jazz.delete", () => {
      const Dog = co.map({
        name: z.string(),
      });

      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
        pet: Dog.optional(),
      });

      const john = Person.create({
        name: "John",
        age: 20,
        pet: { name: "Rex" },
      });

      john.$jazz.delete("age");
      john.$jazz.delete("pet");

      expect(john.age).not.toBeDefined();
      expect(john.pet).not.toBeDefined();
      expect(john.toJSON()).toEqual({
        $jazz: { id: john.$jazz.id },
        name: "John",
      });
      // `in` returns true for schema-defined keys even after deletion.
      // Use $jazz.has() to check if a key has a set value.
      expect("age" in john).toEqual(true);
      expect("pet" in john).toEqual(true);
      expect(john.$jazz.has("age")).toEqual(false);
      expect(john.$jazz.has("pet")).toEqual(false);
      expect(Object.keys(john)).toEqual(["name"]);
    });

    test("cannot delete required properties using $jazz.delete", () => {
      const Dog = co.map({
        name: z.string(),
      });
      const Person = co.map({
        name: z.string(),
        pet: Dog,
      });

      const john = Person.create({ name: "John", pet: { name: "Rex" } });

      // @ts-expect-error - should not allow deleting required primitive properties
      john.$jazz.delete("name");
      // @ts-expect-error - should not allow deleting required reference properties
      john.$jazz.delete("pet");
    });

    test("update a reference using a CoValue", () => {
      const Dog = co.map({
        name: z.string(),
      });

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        dog: Dog,
      });

      const john = Person.create({
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex" }),
      });

      john.$jazz.set("dog", Dog.create({ name: "Fido" }));

      expect(john.dog?.name).toEqual("Fido");
    });

    describe("update a reference using a JSON object", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
      });
      const Pet = co.discriminatedUnion("type", [Dog, Cat]);
      const Person = co.map({
        name: co.plainText(),
        bio: co.richText().optional(),
        dog: Dog,
        get friends() {
          return co.list(Person);
        },
        reactions: co.feed(co.plainText()),
        pet: Pet,
        pets: co.record(z.string(), Pet),
      });

      let person: ReturnType<typeof Person.create>;

      beforeEach(() => {
        person = Person.create({
          name: "John",
          bio: "I am a software engineer",
          dog: { type: "dog", name: "Rex" },
          friends: [
            {
              name: "Jane",
              bio: "I am a mechanical engineer",
              dog: { type: "dog", name: "Fido" },
              friends: [],
              reactions: [],
              pet: { type: "dog", name: "Fido" },
              pets: {},
            },
          ],
          reactions: ["👎", "👍"],
          pet: { type: "cat", name: "Whiskers" },
          pets: {
            dog: { type: "dog", name: "Rex" },
            cat: { type: "cat", name: "Whiskers" },
          },
        });
      });

      test("automatically creates CoValues for plain text reference", () => {
        person.$jazz.set("name", "Jack");
        expect(person.name.toString()).toEqual("Jack");
      });

      test("automatically creates CoValues for rich text reference", () => {
        person.$jazz.set("bio", "I am a lawyer");
        expect(person.bio!.toString()).toEqual("I am a lawyer");
      });

      test("automatically creates CoValues for CoMap reference", () => {
        person.$jazz.set("dog", { type: "dog", name: "Fido" });
        expect(person.dog.name).toEqual("Fido");
      });

      test("automatically creates CoValues for CoRecord reference", () => {
        person.$jazz.set("pets", {
          dog: { type: "dog", name: "Fido" },
        });
        expect(person.pets.dog?.name).toEqual("Fido");
      });

      test("automatically creates CoValues for CoList reference", () => {
        person.$jazz.set("friends", [
          {
            name: "Jane",
            bio: "I am a mechanical engineer",
            dog: { type: "dog", name: "Firulais" },
            friends: [],
            reactions: [],
            pet: { type: "cat", name: "Nala" },
            pets: {},
          },
        ]);
        expect(person.friends[0]!.name.toString()).toEqual("Jane");
        expect(person.friends[0]!.dog.name).toEqual("Firulais");
        expect(person.friends[0]!.pet.name).toEqual("Nala");
      });

      test("automatically creates CoValues for CoFeed reference", () => {
        person.$jazz.set("reactions", ["🧑‍🔬"]);
        expect(person.reactions.byMe?.value?.toString()).toEqual("🧑‍🔬");
      });

      test("automatically creates CoValues for discriminated union reference", () => {
        person.$jazz.set("pet", { type: "cat", name: "Salem" });
        expect(person.pet.name).toEqual("Salem");
      });

      test("undefined properties can be ommited", () => {
        person.$jazz.set("friends", [
          {
            name: "Jane",
            // bio is omitted
            dog: { type: "dog", name: "Firulais" },
            friends: [],
            reactions: [],
            pet: { type: "cat", name: "Nala" },
            pets: {},
          },
        ]);

        expect(person.friends[0]!.name.toString()).toEqual("Jane");
        expect(person.friends[0]!.bio).toBeUndefined();
      });
    });

    test("changes should be listed in getEdits()", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 20 });

      const me = Account.getMe();

      john.$jazz.set("age", 21);

      expect(john.$jazz.getEdits().age?.all).toEqual([
        expect.objectContaining({
          value: 20,
          key: "age",
          ref: undefined,
          madeAt: expect.any(Date),
        }),
        expect.objectContaining({
          value: 21,
          key: "age",
          ref: undefined,
          madeAt: expect.any(Date),
        }),
      ]);
      expect(john.$jazz.getEdits().age?.all[0]?.by).toMatchObject({
        [TypeSym]: "Account",
        $jazz: expect.objectContaining({
          id: me.$jazz.id,
        }),
      });
      expect(john.$jazz.getEdits().age?.all[1]?.by).toMatchObject({
        [TypeSym]: "Account",
        $jazz: expect.objectContaining({
          id: me.$jazz.id,
        }),
      });
    });
  });

  describe("has", () => {
    test("should return true if the key is defined", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      const person = Person.create({ name: "John", age: 20 });

      expect(person.$jazz.has("name")).toBe(true);
      expect(person.$jazz.has("age")).toBe(true);
    });

    test("should return true if the key was set to undefined", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      const person = Person.create({ name: "John" });

      person.$jazz.set("age", undefined);

      expect(person.$jazz.has("age")).toBe(true);
    });

    test("should return false if the key is not defined", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      const person = Person.create({ name: "John" });

      expect(person.$jazz.has("age")).toBe(false);
    });

    test("should return false if the key was deleted", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      const person = Person.create({ name: "John", age: 20 });

      person.$jazz.delete("age");

      expect(person.$jazz.has("age")).toBe(false);
    });

    test("should not load the referenced CoValue", async () => {
      const Person = co.map({
        name: co.plainText(),
      });

      const { clientAccount, serverAccount } = await setupTwoNodes();

      const person = Person.create(
        {
          name: "John",
        },
        { owner: Group.create(serverAccount).makePublic() },
      );

      const loadedPerson = await Person.load(person.$jazz.id, {
        resolve: true,
        loadAs: clientAccount,
      });

      assertLoaded(loadedPerson);
      expect(loadedPerson.$jazz.has("name")).toBe(true);
      expect(loadedPerson.name.$jazz.loadingState).toBe(
        CoValueLoadingState.LOADING,
      );
    });

    test("should return true even if the viewer doesn't have access to the referenced CoValue", async () => {
      const Person = co.map({
        name: co.plainText(),
      });

      const person = Person.create(
        // UserB has no access to name
        { name: co.plainText().create("John", Group.create()) },
        // UserB has access to person
        { owner: Group.create().makePublic() },
      );

      const userB = await createJazzTestAccount();

      const loadedPerson = await Person.load(person.$jazz.id, {
        resolve: true,
        loadAs: userB,
      });

      assertLoaded(loadedPerson);
      expect(loadedPerson.$jazz.has("name")).toBe(true);
      expect(loadedPerson.name.$jazz.loadingState).toBe(
        CoValueLoadingState.LOADING,
      );
    });
  });

  describe("proxy invariant consistency", () => {
    test("has trap is consistent with ownKeys for schema-defined keys", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      const person = Person.create({ name: "John" });

      // ownKeys includes "age" via the schema even though it was never set
      const ownKeys = Object.keys(person);

      // `in` must return true for all keys reported by ownKeys
      for (const key of ownKeys) {
        expect(key in person).toBe(true);
      }

      // "age" is schema-defined, so `in` returns true even when unset
      expect("age" in person).toBe(true);
      // but $jazz.has() correctly reports it as unset
      expect(person.$jazz.has("age")).toBe(false);
    });

    test("has trap is consistent with getOwnPropertyDescriptor", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      const person = Person.create({ name: "John" });

      // getOwnPropertyDescriptor returns a descriptor for schema-defined keys
      const ageDescriptor = Object.getOwnPropertyDescriptor(person, "age");
      expect(ageDescriptor).toBeDefined();

      // has trap must agree — if a descriptor exists, `in` must return true
      expect("age" in person).toBe(true);
    });

    test("has trap returns false for keys not in schema", () => {
      const Person = co.map({
        name: z.string(),
      });

      const person = Person.create({ name: "John" });

      expect("nonExistent" in person).toBe(false);
    });

    test("internal properties are configurable", () => {
      const Person = co.map({
        name: z.string(),
      });

      const person = Person.create({ name: "John" });

      // $isLoaded must be configurable to satisfy proxy invariants
      const isLoadedDesc = Object.getOwnPropertyDescriptor(person, "$isLoaded");
      expect(isLoadedDesc).toBeDefined();
      expect(isLoadedDesc!.configurable).toBe(true);
    });

    test("$jazz.has() is unaffected by has trap changes", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
        bio: z.string().optional(),
      });

      const person = Person.create({ name: "John", age: 20 });

      // $jazz.has() returns true only for keys with set values
      expect(person.$jazz.has("name")).toBe(true);
      expect(person.$jazz.has("age")).toBe(true);
      expect(person.$jazz.has("bio")).toBe(false);

      // `in` returns true for all schema-defined keys
      expect("name" in person).toBe(true);
      expect("age" in person).toBe(true);
      expect("bio" in person).toBe(true);

      // Delete age — $jazz.has() reflects deletion, `in` does not
      person.$jazz.delete("age");
      expect(person.$jazz.has("age")).toBe(false);
      expect("age" in person).toBe(true);
    });
  });

  test("Enum of maps", () => {
    const ChildA = co.map({
      type: z.literal("a"),
      value: z.number(),
    });

    const ChildB = co.map({
      type: z.literal("b"),
      value: z.string(),
    });

    const MapWithEnumOfMaps = co.map({
      name: z.string(),
      child: co.discriminatedUnion("type", [ChildA, ChildB]),
    });

    const mapWithEnum = MapWithEnumOfMaps.create({
      name: "enum",
      child: ChildA.create({
        type: "a",
        value: 5,
      }),
    });

    expect(mapWithEnum.name).toEqual("enum");
    expect(mapWithEnum.child?.type).toEqual("a");
    expect(mapWithEnum.child?.value).toEqual(5);
    expect(mapWithEnum.child?.$jazz.id).toBeDefined();

    // TODO: properly support narrowing once we get rid of the coField marker
    // if (mapWithEnum.child?.type === "a") {
    //   expectTypeOf(mapWithEnum.child).toEqualTypeOf<Loaded<typeof ChildA>>();
    // }
  });
});

describe("CoMap resolution", async () => {
  test("loading a locally available map with deep resolve", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const person = Person.create({
      name: "John",
      age: 20,
      dog: Dog.create({ name: "Rex", breed: "Labrador" }),
    });

    const loadedPerson = await Person.load(person.$jazz.id, {
      resolve: {
        dog: true,
      },
    });

    assertLoaded(loadedPerson);
    expect(loadedPerson.dog.name).toEqual("Rex");
  });

  test("loading a locally available map using autoload for the refs", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const person = Person.create({
      name: "John",
      age: 20,
      dog: Dog.create({ name: "Rex", breed: "Labrador" }),
    });

    const loadedPerson = await Person.load(person.$jazz.id);

    assertLoaded(loadedPerson);
    assertLoaded(loadedPerson.dog);
    expect(loadedPerson.dog.name).toEqual("Rex");
  });

  test("loading a remotely available map with deep resolve", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const group = Group.create();
    group.addMember("everyone", "writer");

    const person = Person.create(
      {
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex", breed: "Labrador" }, group),
      },
      group,
    );

    const userB = await createJazzTestAccount();

    const loadedPerson = await Person.load(person.$jazz.id, {
      resolve: {
        dog: true,
      },
      loadAs: userB,
    });

    assertLoaded(loadedPerson);
    expect(loadedPerson.dog.name).toEqual("Rex");
  });

  test("loading a remotely available map using autoload for the refs", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const group = Group.create();
    group.addMember("everyone", "writer");

    const person = Person.create(
      {
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex", breed: "Labrador" }, group),
      },
      group,
    );

    const userB = await createJazzTestAccount();
    const loadedPerson = await Person.load(person.$jazz.id, {
      loadAs: userB,
    });

    assertLoaded(loadedPerson);

    await waitFor(() => {
      assertLoaded(loadedPerson.dog);
      expect(loadedPerson.dog.name).toEqual("Rex");
    });
  });

  test("loading a remotely available map with skipRetry set to true", async () => {
    // Make the retry delay extra long to ensure that it's not used
    cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 100_000_000;

    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const currentAccount = Account.getMe();

    // Disconnect the current account
    currentAccount.$jazz.localNode.syncManager
      .getServerPeers(currentAccount.$jazz.raw.id)
      .forEach((peer) => {
        peer.gracefulShutdown();
      });

    const group = Group.create();
    group.addMember("everyone", "writer");

    const person = Person.create(
      {
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex", breed: "Labrador" }, group),
      },
      group,
    );

    const userB = await createJazzTestAccount();

    // We expect that the test doesn't hang here and immediately returns null
    const loadedPerson = await Person.load(person.$jazz.id, {
      loadAs: userB,
      skipRetry: true,
    });

    expect(loadedPerson.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAVAILABLE,
    );
  });

  test("loading a remotely available map with skipRetry set to false", async () => {
    disableJazzTestSync();
    // Make the retry delay extra long to avoid flakyness in the resolved checks
    cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 100_000_000;

    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const currentAccount = Account.getMe();

    const group = Group.create();
    group.addMember("everyone", "writer");

    const person = Person.create(
      {
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex", breed: "Labrador" }, group),
      },
      group,
    );

    await setupJazzTestSync({
      asyncPeers: true,
    });

    const userB = await createJazzTestAccount();
    let resolved = false;
    const promise = Person.load(person.$jazz.id, {
      loadAs: userB,
      skipRetry: false,
    });
    promise.then(() => {
      resolved = true;
    });

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(resolved).toBe(false);

    // Reconnect the current account
    currentAccount.$jazz.localNode.syncManager.addPeer(
      getPeerConnectedToTestSyncServer(),
    );

    const loadedPerson = await promise;

    expect(resolved).toBe(true);
    assertLoaded(loadedPerson);

    await waitFor(() => {
      assertLoaded(loadedPerson.dog);
      expect(loadedPerson.dog.name).toEqual("Rex");
    });
  });

  test("obtaining coMap refs", async () => {
    const Dog = co.map({
      name: z.string().optional(),
      breed: z.string(),
      owner: co.plainText(),
      get parent() {
        return co.optional(Dog);
      },
    });

    const dog = Dog.create({
      name: "Rex",
      breed: "Labrador",
      owner: "John",
      parent: { name: "Fido", breed: "Labrador", owner: "Jane" },
    });

    const refs = dog.$jazz.refs;

    expect(Object.keys(refs)).toEqual(["owner", "parent"]);
    expect(refs.owner.id).toEqual(dog.owner.$jazz.id);
    expect(refs.parent?.id).toEqual(dog.parent!.$jazz.id);
  });

  test("accessing the value refs", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const group = Group.create();
    group.addMember("everyone", "writer");

    const person = Person.create(
      {
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex", breed: "Labrador" }, group),
      },
      group,
    );

    const userB = await createJazzTestAccount();
    const loadedPerson = await Person.load(person.$jazz.id, {
      loadAs: userB,
    });

    assertLoaded(loadedPerson);

    expect(loadedPerson.$jazz.refs.dog.id).toBe(person.dog.$jazz.id);

    const dog = await loadedPerson.$jazz.refs.dog.load();

    assertLoaded(dog);

    expect(dog.name).toEqual("Rex");
  });

  test("subscription on a locally available map with deep resolve", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const person = Person.create({
      name: "John",
      age: 20,
      dog: Dog.create({ name: "Rex", breed: "Labrador" }),
    });

    const updates: Loaded<typeof Person, { dog: true }>[] = [];
    const spy = vi.fn((person) => updates.push(person));

    Person.subscribe(
      person.$jazz.id,
      {
        resolve: {
          dog: true,
        },
      },
      spy,
    );

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.dog.name).toEqual("Rex");

    person.dog!.$jazz.set("name", "Fido");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.dog.name).toEqual("Fido");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("subscription on a locally available map with autoload", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const person = Person.create({
      name: "John",
      age: 20,
      dog: Dog.create({ name: "Rex", breed: "Labrador" }),
    });

    const updates: Loaded<typeof Person>[] = [];
    const spy = vi.fn((person) => updates.push(person));

    Person.subscribe(person.$jazz.id, {}, spy);

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    assert(updates[0]);
    assertLoaded(updates[0].dog);
    expect(updates[0].dog.name).toEqual("Rex");

    person.dog.$jazz.set("name", "Fido");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    assert(updates[1]);
    assertLoaded(updates[1].dog);
    expect(updates[1].dog.name).toEqual("Fido");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("subscription on a locally available map with syncResolution", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const person = Person.create({
      name: "John",
      age: 20,
      dog: Dog.create({ name: "Rex", breed: "Labrador" }),
    });

    const updates: Loaded<typeof Person>[] = [];
    const spy = vi.fn((person) => updates.push(person));

    subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(Person), // TODO: we should get rid of the conversion in the future
      person.$jazz.id,
      {
        syncResolution: true,
        loadAs: Account.getMe(),
      },
      spy,
    );

    expect(spy).toHaveBeenCalled();
    expect(spy).toHaveBeenCalledTimes(1);

    assert(updates[0]);
    assertLoaded(updates[0].dog);
    expect(updates[0].dog.name).toEqual("Rex");

    expect(spy).toHaveBeenCalledTimes(1);

    person.dog.$jazz.set("name", "Fido");

    expect(spy).toHaveBeenCalledTimes(2);

    assert(updates[1]);
    assertLoaded(updates[1].dog);
    expect(updates[1].dog.name).toEqual("Fido");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("subscription on a remotely available map with deep resolve", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const group = Group.create();
    group.addMember("everyone", "writer");

    const person = Person.create(
      {
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex", breed: "Labrador" }, group),
      },
      group,
    );

    const userB = await createJazzTestAccount();

    const updates: Loaded<typeof Person, { dog: true }>[] = [];
    const spy = vi.fn((person) => updates.push(person));

    Person.subscribe(
      person.$jazz.id,
      {
        resolve: {
          dog: true,
        },
        loadAs: userB,
      },
      spy,
    );

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.dog.name).toEqual("Rex");

    person.dog!.$jazz.set("name", "Fido");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.dog.name).toEqual("Fido");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("subscription on a remotely available map with autoload", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const group = Group.create();
    group.addMember("everyone", "writer");

    const person = Person.create(
      {
        name: "John",
        age: 20,
        dog: Dog.create({ name: "Rex", breed: "Labrador" }, group),
      },
      group,
    );

    const updates: Loaded<typeof Person>[] = [];
    const spy = vi.fn((person) => updates.push(person));

    const userB = await createJazzTestAccount();

    Person.subscribe(
      person.$jazz.id,
      {
        loadAs: userB,
      },
      spy,
    );

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    await waitFor(() => {
      assert(updates[0]);
      assertLoaded(updates[0].dog);
      expect(updates[0].dog.name).toEqual("Rex");
    });

    person.dog.$jazz.set("name", "Fido");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(3));

    assert(updates[1]);
    assertLoaded(updates[1].dog);
    expect(updates[1].dog.name).toEqual("Fido");

    expect(spy).toHaveBeenCalledTimes(3);
  });

  test("replacing nested object triggers updates", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      age: z.number(),
      dog: Dog,
    });

    const person = Person.create({
      name: "John",
      age: 20,
      dog: Dog.create({ name: "Rex", breed: "Labrador" }),
    });

    const updates: Loaded<typeof Person, { dog: true }>[] = [];
    const spy = vi.fn((person) => updates.push(person));

    Person.subscribe(
      person.$jazz.id,
      {
        resolve: {
          dog: true,
        },
      },
      spy,
    );

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.dog.name).toEqual("Rex");

    person.dog!.$jazz.set("name", "Fido");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.dog.name).toEqual("Fido");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("loading a locally available map with invalid data", async () => {
    const Person1 = co.map({
      name: z.string(),
      age: z.number(),
    });

    const Person2 = co.map({
      name: z.string(),
      age: z.string(),
    });

    const person1 = Person1.create({ name: "John", age: 20 });
    person1.$jazz.waitForSync();

    const person2 = await Person2.load(person1.$jazz.id);

    assertLoaded(person2);
    expect(person2.age).toStrictEqual(20);
  });

  test("loaded CoMap keeps schema validation", async () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
    });

    const person1 = Person.create({ name: "John", age: 20 });
    // person1.$jazz.waitForSync();

    const person2 = await Person.load(person1.$jazz.id);

    assertLoaded(person2);
    expectValidationError(() =>
      person2.$jazz.set("age", "20" as unknown as number),
    );
  });
});

describe("CoMap applyDiff", async () => {
  const me = await Account.create({
    creationProps: { name: "Tester McTesterson" },
    crypto: Crypto,
  });

  const NestedMap = co.map({
    value: z.string(),
  });

  const TestMap = co.map({
    name: z.string(),
    age: z.number(),
    isActive: z.boolean(),
    birthday: z.date(),
    nested: NestedMap,
    optionalField: z.string().optional(),
    optionalNested: co.optional(NestedMap),
  });

  test("Basic applyDiff", () => {
    const map = TestMap.create(
      {
        name: "Alice",
        age: 30,
        isActive: true,
        birthday: new Date("1990-01-01"),
        nested: NestedMap.create({ value: "original" }, { owner: me }),
      },
      { owner: me },
    );

    const newValues = {
      name: "Bob",
      age: 35,
      isActive: false,
    };

    map.$jazz.applyDiff(newValues);

    expect(map.name).toEqual("Bob");
    expect(map.age).toEqual(35);
    expect(map.isActive).toEqual(false);
    expect(map.birthday).toEqual(new Date("1990-01-01"));
    expect(map.nested?.value).toEqual("original");
  });

  test("Basic applyDiff should validate", () => {
    const map = TestMap.create(
      {
        name: "Alice",
        age: 30,
        isActive: true,
        birthday: new Date("1990-01-01"),
        nested: NestedMap.create({ value: "original" }, { owner: me }),
      },
      { owner: me },
    );

    const newValues = {
      age: "35",
    };

    // @ts-expect-error - age should be a number
    expect(() => map.$jazz.applyDiff(newValues)).toThrow();

    expect(map.name).toEqual("Alice");
    expect(map.age).toEqual(30);
    expect(map.isActive).toEqual(true);
    expect(map.birthday).toEqual(new Date("1990-01-01"));
    expect(map.nested?.value).toEqual("original");
  });

  test("Basic applyDiff should not validate if validation is loose", () => {
    const map = TestMap.create(
      {
        name: "Alice",
        age: 30,
        isActive: true,
        birthday: new Date("1990-01-01"),
        nested: NestedMap.create({ value: "original" }, { owner: me }),
      },
      { owner: me },
    );

    const newValues = {
      age: "35",
    };

    expect(() =>
      // @ts-expect-error - age should be a number
      map.$jazz.applyDiff(newValues, { validation: "loose" }),
    ).not.toThrow();

    expect(map.age).toEqual("35");
  });

  test("applyDiff with nested changes", () => {
    const originalNestedMap = NestedMap.create(
      { value: "original" },
      { owner: me },
    );
    const map = TestMap.create(
      {
        name: "Charlie",
        age: 25,
        isActive: true,
        birthday: new Date("1995-01-01"),
        nested: originalNestedMap,
      },
      { owner: me },
    );

    const newValues = {
      name: "David",
      nested: NestedMap.create({ value: "updated" }, { owner: me }),
    };

    map.$jazz.applyDiff(newValues);

    expect(map.name).toEqual("David");
    expect(map.age).toEqual(25);
    expect(map.nested?.value).toEqual("updated");
    // A new nested CoMap is created
    expect(map.nested.$jazz.id).not.toBe(originalNestedMap.$jazz.id);
  });

  test("applyDiff with encoded fields", () => {
    const map = TestMap.create(
      {
        name: "Eve",
        age: 28,
        isActive: true,
        birthday: new Date("1993-01-01"),
        nested: NestedMap.create({ value: "original" }, { owner: me }),
      },
      { owner: me },
    );

    const newValues = {
      birthday: new Date("1993-06-15"),
    };

    map.$jazz.applyDiff(newValues);

    expect(map.birthday).toEqual(new Date("1993-06-15"));
  });

  test("applyDiff with optional fields", () => {
    const map = TestMap.create(
      {
        name: "Frank",
        age: 40,
        isActive: true,
        birthday: new Date("1980-01-01"),
        nested: NestedMap.create({ value: "original" }, { owner: me }),
      },
      { owner: me },
    );

    const newValues = {
      optionalField: "New optional value",
    };

    map.$jazz.applyDiff(newValues);

    expect(map.optionalField).toEqual("New optional value");

    map.$jazz.applyDiff({ optionalField: undefined });

    expect(map.optionalField).toBeUndefined();
  });

  test("applyDiff with no changes", () => {
    const map = TestMap.create(
      {
        name: "Grace",
        age: 35,
        isActive: true,
        birthday: new Date("1985-01-01"),
        nested: NestedMap.create({ value: "original" }, { owner: me }),
      },
      { owner: me },
    );

    const originalJSON = map.toJSON();

    map.$jazz.applyDiff({});

    expect(map.toJSON()).toEqual(originalJSON);
  });

  test("applyDiff with invalid field", () => {
    const map = TestMap.create(
      {
        name: "Henry",
        age: 45,
        isActive: false,
        birthday: new Date("1975-01-01"),
        nested: NestedMap.create({ value: "original" }, { owner: me }),
      },
      { owner: me },
    );

    const newValues = {
      name: "Ian",
      invalidField: "This should be ignored",
    };
    map.$jazz.applyDiff(newValues);

    expect(map.name).toEqual("Ian");
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((map as any).invalidField).toBeUndefined();
  });

  test("applyDiff with optional reference set to undefined", () => {
    const map = TestMap.create(
      {
        name: "Jack",
        age: 50,
        isActive: true,
        birthday: new Date("1970-01-01"),
        nested: NestedMap.create({ value: "original" }, { owner: me }),
        optionalNested: NestedMap.create({ value: "optional" }, { owner: me }),
      },
      { owner: me },
    );

    const newValues = {
      optionalNested: undefined,
    };

    map.$jazz.applyDiff(newValues);

    expect(map.optionalNested).toBeUndefined();
  });

  test("applyDiff with required reference set to undefined should throw", () => {
    const map = TestMap.create(
      {
        name: "Kate",
        age: 55,
        isActive: true,
        birthday: new Date("1965-01-01"),
        nested: NestedMap.create({ value: "original" }, { owner: me }),
      },
      { owner: me },
    );

    const newValues = {
      nested: undefined,
    };

    expect(() => map.$jazz.applyDiff(newValues)).toThrow();
  });

  test("applyDiff from JSON", () => {
    const map = TestMap.create({
      name: "Alice",
      age: 30,
      isActive: true,
      birthday: new Date("1990-01-01"),
      nested: NestedMap.create({ value: "original" }),
    });
    const originalNestedMap = map.nested;

    const newValues = {
      nested: { value: "updated" },
    };

    map.$jazz.applyDiff(newValues);

    expect(map.nested?.value).toEqual("updated");
    // A new nested CoMap is created
    expect(map.nested.$jazz.id).not.toBe(originalNestedMap.$jazz.id);
  });
});

describe("CoMap Typescript validation", async () => {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  test("Is not ok to pass null into a required ref", () => {
    const NestedMap = co.map({
      value: z.string(),
    });

    const TestMap = co.map({
      required: NestedMap,
      optional: NestedMap.optional(),
    });

    expectTypeOf<typeof TestMap.create>().toBeCallableWith(
      // @ts-expect-error null can't be passed to a non-optional field
      {
        optional: NestedMap.create({ value: "" }, { owner: me }),
        required: null,
      },
      { owner: me },
    );
  });

  test("Is not ok if a required ref is omitted", () => {
    const NestedMap = co.map({
      value: z.string(),
    });

    const TestMap = co.map({
      required: NestedMap,
      optional: NestedMap.optional(),
    });

    expectTypeOf<typeof TestMap.create>().toBeCallableWith(
      // @ts-expect-error non-optional fields can't be omitted
      {},
      { owner: me },
    );
  });

  test("Is ok to omit optional fields", () => {
    const NestedMap = co.map({
      value: z.string(),
    });

    const TestMap = co.map({
      required: NestedMap,
      optional: NestedMap.optional(),
    });

    expectTypeOf<typeof TestMap.create>().toBeCallableWith(
      {
        required: NestedMap.create({ value: "" }, { owner: me }),
      },
      { owner: me },
    );

    expectTypeOf<typeof TestMap.create>().toBeCallableWith(
      {
        required: NestedMap.create({ value: "" }, { owner: me }),
        optional: undefined, // TODO: should we allow null here? zod is stricter about this than we were before
      },
      { owner: me },
    );
  });

  test("waitForSync should resolve when the value is uploaded", async () => {
    const TestMap = co.map({
      name: z.string(),
    });

    const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

    const map = TestMap.create(
      {
        name: "Alice",
      },
      { owner: clientAccount },
    );

    await map.$jazz.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the map from it
    clientNode.gracefulShutdown();

    const loadedMap = await serverNode.load(map.$jazz.raw.id);

    expect(loadedMap).not.toBe(CoValueLoadingState.UNAVAILABLE);
  });

  test("complex discriminated union", () => {
    const StringTag = co.map({
      type: z.literal("string"),
      stringValue: z.string(),
    });

    const DateTag = co.map({
      type: z.literal("date"),
      dateValue: z.date(),
      repeat: z.optional(
        z.literal("daily").or(z.literal("weekly")).or(z.literal("monthly")),
      ),
    });

    const StringAttributeValue = co.map({
      type: z.literal(["somethingElse", "string"]),
      stringValue: z.string(),
    });

    const NumberAttributeValue = co.map({
      type: z.literal("number"),
      numberValue: z.number(),
    });

    const DateAttributeValue = co.map({
      type: z.literal("date"),
      dateValue: z.date(),
    });

    const AttributeValue = co.discriminatedUnion("type", [
      StringAttributeValue,
      NumberAttributeValue,
      DateAttributeValue,
    ]);

    const AttributeTagKey = co.map({
      key: z.string(),
    });

    const AttributeTag = co.map({
      type: z.literal("attribute"),
      key: AttributeTagKey, // this is a covalue so that it can be referenced uniquely by other tags
      attributeValue: AttributeValue,
    });

    const Tag = co.discriminatedUnion("type", [
      AttributeTag,
      StringTag,
      DateTag,
    ]);

    const Wrapper = co.map({
      tag: Tag,
    });

    const wrapper = Wrapper.create({
      tag: AttributeTag.create({
        type: "attribute",
        key: AttributeTagKey.create({ key: "name" }),
        attributeValue: StringAttributeValue.create({
          type: "string",
          stringValue: "Alice",
        }),
      }),
    });

    if (wrapper.tag.type === "attribute") {
      expect(wrapper.tag.key.key).toEqual("name");
      if (wrapper.tag.attributeValue.type === "string") {
        expect(wrapper.tag.attributeValue.stringValue).toEqual("Alice");
      }
    }
  });

  test("complex discriminated union with numeric discriminator value", () => {
    const HttpError = co.map({
      code: z.number(),
      message: z.string(),
    });

    const ClientError = co.map({
      type: z.literal(400),
      error: HttpError,
    });

    const ServerError = co.map({
      type: z.literal(500),
      error: HttpError,
    });

    const NetworkError = co.map({
      type: z.literal(0),
      error: HttpError,
    });

    const ErrorResponse = co.discriminatedUnion("type", [
      ClientError,
      ServerError,
      NetworkError,
    ]);

    const ErrorWrapper = co.map({
      response: ErrorResponse,
    });

    const wrapper = ErrorWrapper.create({
      response: ClientError.create({
        type: 400,
        error: HttpError.create({
          code: 400,
          message: "Bad Request",
        }),
      }),
    });

    if (wrapper.response.type === 400) {
      expect(wrapper.response.error.code).toEqual(400);
      expect(wrapper.response.error.message).toEqual("Bad Request");
    }

    const serverErrorWrapper = ErrorWrapper.create({
      response: ServerError.create({
        type: 500,
        error: HttpError.create({
          code: 500,
          message: "Internal Server Error",
        }),
      }),
    });

    if (serverErrorWrapper.response.type === 500) {
      expect(serverErrorWrapper.response.error.code).toEqual(500);
      expect(serverErrorWrapper.response.error.message).toEqual(
        "Internal Server Error",
      );
    }

    const networkErrorWrapper = ErrorWrapper.create({
      response: NetworkError.create({
        type: 0,
        error: HttpError.create({
          code: 0,
          message: "Network Error",
        }),
      }),
    });

    if (networkErrorWrapper.response.type === 0) {
      expect(networkErrorWrapper.response.error.code).toEqual(0);
      expect(networkErrorWrapper.response.error.message).toEqual(
        "Network Error",
      );
    }
  });
});

describe("CoMap migration", () => {
  test("should run on load", async () => {
    const PersonV1 = co.map({
      name: z.string(),
      version: z.literal(1),
    });

    const Person = co
      .map({
        name: z.string(),
        age: z.number(),
        version: z.literal([1, 2]),
      })
      .withMigration((person) => {
        if (person.version === 1) {
          person.$jazz.set("age", 20);
          person.$jazz.set("version", 2);
        }
      });

    const person = PersonV1.create({
      name: "Bob",
      version: 1,
    });

    expect(person?.name).toEqual("Bob");
    expect(person?.version).toEqual(1);

    const loadedPerson = await Person.load(person.$jazz.id);

    assertLoaded(loadedPerson);
    expect(loadedPerson.name).toEqual("Bob");
    expect(loadedPerson.age).toEqual(20);
    expect(loadedPerson.version).toEqual(2);
  });

  test("should handle group updates", async () => {
    const Person = co
      .map({
        name: z.string(),
        version: z.literal([1, 2]),
      })
      .withMigration((person) => {
        if (person.version === 1) {
          person.$jazz.set("version", 2);

          person.$jazz.owner.addMember("everyone", "reader");
        }
      });

    const person = Person.create({
      name: "Bob",
      version: 1,
    });

    expect(person?.name).toEqual("Bob");
    expect(person?.version).toEqual(1);

    const loadedPerson = await Person.load(person.$jazz.id);

    assertLoaded(loadedPerson);
    expect(loadedPerson.name).toEqual("Bob");
    expect(loadedPerson.version).toEqual(2);

    const anotherAccount = await createJazzTestAccount();

    const loadedPersonFromAnotherAccount = await Person.load(person.$jazz.id, {
      loadAs: anotherAccount,
    });

    assertLoaded(loadedPersonFromAnotherAccount);
    expect(loadedPersonFromAnotherAccount.name).toEqual("Bob");
  });

  test("should return unavailable if a migration is async", async () => {
    const Person = co
      .map({
        name: z.string(),
        version: z.number(),
      })
      // @ts-expect-error async function
      .withMigration(async () => {});

    const person = Person.create({
      name: "Bob",
      version: 1,
    });

    const loaded = await Person.load(person.$jazz.id);
    expect(loaded.$jazz.loadingState).toBe(CoValueLoadingState.UNAVAILABLE);
  });

  test("should return unavailable when migration tries to create content as reader", async () => {
    const Extra = co.map({
      value: z.string(),
    });

    const Person = co
      .map({
        name: z.string(),
        version: z.literal([1, 2]),
        extra: Extra.optional(),
      })
      .withMigration((person) => {
        if (person.version === 1) {
          person.$jazz.set(
            "extra",
            Extra.create(
              {
                value: "created in migration",
              },
              person.$jazz.owner,
            ),
          );
          person.$jazz.set("version", 2);
        }
      });

    const group = Group.create();
    const person = Person.create(
      {
        name: "Bob",
        version: 1,
      },
      group,
    );

    group.addMember("everyone", "reader");
    const reader = await createJazzTestAccount();
    const loaded = await Person.load(person.$jazz.id, {
      loadAs: reader,
    });
    expect(loaded.$jazz.loadingState).toBe(CoValueLoadingState.UNAVAILABLE);
  });

  test("should run only once", async () => {
    const spy = vi.fn();
    const Person = co
      .map({
        name: z.string(),
        version: z.number(),
      })
      .withMigration((person) => {
        spy(person);
      });

    const person = Person.create({
      name: "Bob",
      version: 1,
    });

    await Person.load(person.$jazz.id);
    await Person.load(person.$jazz.id);
    expect(spy).toHaveBeenCalledTimes(1);
  });

  test("should not break recursive schemas", async () => {
    const PersonV1 = co.map({
      name: z.string(),
      version: z.literal(1),
      get friend() {
        return PersonV1.optional();
      },
    });

    const Person = co
      .map({
        name: z.string(),
        age: z.number(),
        get friend() {
          return Person.optional();
        },
        version: z.literal([1, 2]),
      })
      .withMigration((person) => {
        if (person.version === 1) {
          person.$jazz.set("age", 20);
          person.$jazz.set("version", 2);
        }
      });

    const charlie = PersonV1.create({
      name: "Charlie",
      version: 1,
    });

    const bob = PersonV1.create({
      name: "Bob",
      version: 1,
      friend: charlie,
    });

    const loaded = await Person.load(bob.$jazz.id, {
      resolve: {
        friend: true,
      },
    });

    // Migration should run on both the person and their friend
    assertLoaded(loaded);
    expect(loaded.name).toEqual("Bob");
    expect(loaded.age).toEqual(20);
    expect(loaded.version).toEqual(2);
    expect(loaded.friend?.name).toEqual("Charlie");
    expect(loaded.friend?.version).toEqual(2);
  });

  test("should wait for the full streaming before running the migration", async () => {
    disableJazzTestSync();
    const alice = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });

    const migration = vi.fn();
    const Person = co
      .map({
        name: z.string(),
        update: z.number(),
      })
      .withMigration((person) => {
        migration(person.update);
      });

    const person = Person.create({
      name: "Bob",
      update: 1,
    });

    person.$jazz.owner.addMember("everyone", "reader");

    // Pump the value to reach streaming
    for (let i = 0; i <= 300; i++) {
      person.$jazz.raw.assign({
        name: "1".repeat(1024),
        update: i,
      });
    }

    const bob = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const personContent = await exportCoValue(Person, person.$jazz.id, {
      loadAs: alice,
    });
    assert(personContent);

    migration.mockClear();

    const lastPiece = personContent.pop();
    assert(lastPiece);

    for (const content of personContent) {
      bob.$jazz.localNode.syncManager.handleNewContent(content, "storage");
    }

    // Simulate the streaming delay on the last piece
    setTimeout(() => {
      bob.$jazz.localNode.syncManager.handleNewContent(lastPiece, "storage");
    }, 10);

    // Load the value and expect the migration to run only once
    const loadedPerson = await Person.load(person.$jazz.id, { loadAs: bob });
    assert(loadedPerson);
    expect(migration).toHaveBeenCalledTimes(1);
    expect(migration).toHaveBeenCalledWith(300);
  });

  test("should run only when the group is fully loaded", async () => {
    disableJazzTestSync();

    const alice = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
    const migration = vi.fn();

    const Person = co
      .map({
        name: z.string(),
        update: z.number(),
      })
      .withMigration((person) => {
        migration({
          groupStreaming:
            person.$jazz.owner.$jazz.raw.core.verified.isStreaming(),
        });
      });

    const group = Group.create();

    const person = Person.create(
      {
        name: "Bob",
        update: 1,
      },
      group,
    );

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

    // Upload and unmount, to force the streaming download
    migration.mockClear();

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
      bob.$jazz.localNode.syncManager.handleNewContent(
        lastGroupPiece,
        "import",
      );
    }, 10);

    // Load the value and expect the migration to run only once
    const loadedPerson = await Person.load(person.$jazz.id, { loadAs: bob });
    assert(loadedPerson);
    expect(migration).toHaveBeenCalledTimes(1);
    expect(migration).toHaveBeenCalledWith({ groupStreaming: false });
  });
});

describe("createdAt, lastUpdatedAt, createdBy", () => {
  test("empty map created time", () => {
    const emptyMap = co.map({}).create({});

    expect(emptyMap.$jazz.createdAt).toEqual(
      new Date(emptyMap.$jazz.raw.core.verified.header.createdAt!).getTime(),
    );

    const lastTx = emptyMap.$jazz.raw.core.verifiedTransactions.at(-1);
    expect(emptyMap.$jazz.lastUpdatedAt).toEqual(lastTx!.madeAt);
  });

  test("empty map created by", () => {
    const emptyMap = co.map({}).create({});
    const me = Account.getMe();
    expect(emptyMap.$jazz.createdBy).toEqual(me.$jazz.id);
  });

  test("created time and last updated time", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const me = Account.getMe();

    const person = Person.create({ name: "John" });

    const createdAt = person.$jazz.createdAt;
    const setNameJohnTx = person.$jazz.raw.core.verifiedTransactions[0];
    expect(person.$jazz.createdAt).toEqual(createdAt);
    expect(person.$jazz.lastUpdatedAt).toEqual(setNameJohnTx!.madeAt);

    const createdBy = person.$jazz.createdBy;
    expect(createdBy).toEqual(me.$jazz.id);

    await new Promise((r) => setTimeout(r, 10));
    person.$jazz.set("name", "Jane");

    const setNameJaneTx = person.$jazz.raw.core.verifiedTransactions[1];
    expect(person.$jazz.createdAt).toEqual(createdAt);
    expect(person.$jazz.lastUpdatedAt).toEqual(setNameJaneTx!.madeAt);

    // Double check after update.
    expect(createdBy).toEqual(me.$jazz.id);
  });

  test("createdBy does not change when updated", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const me = Account.getMe();

    const person = Person.create({ name: "John" });

    const createdBy = person.$jazz.createdBy;
    expect(createdBy).toEqual(me.$jazz.id);

    await new Promise((r) => setTimeout(r, 10));
    person.$jazz.set("name", "Jane");

    // Double check after update.
    expect(createdBy).toEqual(me.$jazz.id);
  });

  test("createdBy is after key rotation", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const me = Account.getMe();

    // Create person
    const person = Person.create({ name: "John" });

    // True created by
    const createdBy = person.$jazz.createdBy;

    // Create a user, grant access, then kick to trigger key rotation.
    const newUser = await createJazzTestAccount();

    person.$jazz.owner.addMember(newUser, "reader");

    // This should trigger read key rotation
    person.$jazz.owner.removeMember(newUser);

    // Now create a new user and grant access
    const newUser2 = await createJazzTestAccount();
    person.$jazz.owner.addMember(newUser2, "reader");

    // Load the CoValue as the new user:
    setActiveAccount(newUser2);

    const personLoadedAsUser2 = await Person.load(person.$jazz.id);
    assertLoaded(personLoadedAsUser2);
    const createdByPerUser2 = personLoadedAsUser2.$jazz.createdBy;
    // Double check after update.
    expect(createdBy).toEqual(createdByPerUser2);
  });
});

describe("co.map schema", () => {
  test("can access the inner schemas of a co.map", () => {
    const Person = co.map({
      name: co.plainText(),
    });

    const person = Person.create({
      name: Person.shape["name"].create("John"),
    });

    expect(person.name.toString()).toEqual("John");
  });

  describe("pick()", () => {
    test("creates a new CoMap schema by picking fields of another CoMap schema", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const PersonWithName = Person.pick({
        name: true,
      });

      const person = PersonWithName.create({
        name: "John",
      });

      expect(person.name).toEqual("John");
    });

    test("the new schema does not include catchall properties", () => {
      const Person = co
        .map({
          name: z.string(),
          age: z.number(),
        })
        .catchall(z.string());

      const PersonWithName = Person.pick({
        name: true,
      });

      expect(PersonWithName.catchAll).toBeUndefined();

      const person = PersonWithName.create({
        name: "John",
      });
      // @ts-expect-error - property `extraField` does not exist in person
      expect(person.extraField).toBeUndefined();
    });
  });

  describe("partial()", () => {
    test("creates a new CoMap schema by making all properties optional", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        pet: Dog,
      });

      const DraftPerson = Person.partial();

      const draftPerson = DraftPerson.create({});

      expect(draftPerson.name).toBeUndefined();
      expect(draftPerson.age).toBeUndefined();
      expect(draftPerson.pet).toBeUndefined();

      draftPerson.$jazz.set("name", "John");
      draftPerson.$jazz.set("age", 20);
      const rex = Dog.create({ name: "Rex", breed: "Labrador" });
      draftPerson.$jazz.set("pet", rex);

      expect(draftPerson.name).toEqual("John");
      expect(draftPerson.age).toEqual(20);
      expect(draftPerson.pet).toEqual(rex);
    });

    test("creates a new CoMap schema by making some properties optional", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        pet: Dog,
      });

      const DraftPerson = Person.partial({
        pet: true,
      });

      const draftPerson = DraftPerson.create({
        name: "John",
        age: 20,
      });

      expect(draftPerson.$jazz.has("pet")).toBe(false);

      const rex = Dog.create({ name: "Rex", breed: "Labrador" });
      draftPerson.$jazz.set("pet", rex);

      expect(draftPerson.pet).toEqual(rex);

      expect(draftPerson.$jazz.has("pet")).toBe(true);

      draftPerson.$jazz.delete("pet");

      expect(draftPerson.$jazz.has("pet")).toBe(false);

      // @ts-expect-error - should not allow deleting required properties
      draftPerson.$jazz.delete("age");
    });

    test("the new schema includes catchall properties", () => {
      const Person = co
        .map({
          name: z.string(),
          age: z.number(),
        })
        .catchall(z.string());

      const DraftPerson = Person.partial();

      const draftPerson = DraftPerson.create({});
      draftPerson.$jazz.set("extraField", "extra");

      expect(draftPerson.extraField).toEqual("extra");
    });
  });

  test("co.map() should throw an error if passed a CoValue schema", () => {
    expect(() => co.map(co.map({}))).toThrow(
      "co.map() expects an object as its argument, not a CoValue schema",
    );
  });

  test("co.map() should throw an error if its shape does not contain valid schemas", () => {
    expect(() =>
      co.map({
        field: "a string is not a valid schema",
      }),
    ).toThrow("co.map() supports only Zod v4 schemas and CoValue schemas");
  });
});

describe("Updating a nested reference", () => {
  test("should assign a resolved optional reference and expect value is not null", async () => {
    // Define the schema similar to the server-worker-http example
    const PlaySelection = co.map({
      value: z.literal(["rock", "paper", "scissors"]),
      group: Group,
    });

    const Player = co.map({
      account: co.account(),
      playSelection: PlaySelection.optional(),
    });

    const Game = co.map({
      player1: Player,
      player2: Player,
      outcome: z.literal(["player1", "player2", "draw"]).optional(),
      player1Score: z.number(),
      player2Score: z.number(),
    });

    // Create accounts for the players
    const player1Account = await createJazzTestAccount({
      creationProps: { name: "Player 1" },
    });
    const player2Account = await createJazzTestAccount({
      creationProps: { name: "Player 2" },
    });

    // Create a game
    const game = Game.create({
      player1: Player.create({
        account: player1Account,
      }),
      player2: Player.create({
        account: player2Account,
      }),
      player1Score: 0,
      player2Score: 0,
    });

    // Create a group for the play selection (similar to the route logic)
    const group = Group.create({ owner: Account.getMe() });
    group.addMember(player1Account, "reader");

    // Load the game to verify the assignment worked
    const loadedGame = await Game.load(game.$jazz.id, {
      resolve: {
        player1: {
          account: true,
          playSelection: true,
        },
        player2: {
          account: true,
          playSelection: true,
        },
      },
    });

    assertLoaded(loadedGame);

    // Create a play selection
    const playSelection = PlaySelection.create({ value: "rock", group }, group);

    // Assign the play selection to player1 (similar to the route logic)
    loadedGame.player1.$jazz.set("playSelection", playSelection);

    // Verify that the playSelection is not null and has the expected value
    expect(loadedGame.player1.playSelection).not.toBeNull();
    expect(loadedGame.player1.playSelection).toBeDefined();
  });

  test("should assign a resolved reference and expect value to update", async () => {
    // Define the schema similar to the server-worker-http example
    const PlaySelection = co.map({
      value: z.literal(["rock", "paper", "scissors"]),
    });

    const Player = co.map({
      account: co.account(),
      playSelection: PlaySelection,
    });

    const Game = co.map({
      player1: Player,
      player2: Player,
      outcome: z.literal(["player1", "player2", "draw"]).optional(),
      player1Score: z.number(),
      player2Score: z.number(),
    });

    // Create accounts for the players
    const player1Account = await createJazzTestAccount({
      creationProps: { name: "Player 1" },
    });
    const player2Account = await createJazzTestAccount({
      creationProps: { name: "Player 2" },
    });

    // Create a game
    const game = Game.create({
      player1: Player.create({
        account: player1Account,
        playSelection: PlaySelection.create({ value: "rock" }),
      }),
      player2: Player.create({
        account: player2Account,
        playSelection: PlaySelection.create({ value: "paper" }),
      }),
      player1Score: 0,
      player2Score: 0,
    });

    // Load the game to verify the assignment worked
    const loadedGame = await Game.load(game.$jazz.id, {
      resolve: {
        player1: {
          account: true,
          playSelection: true,
        },
        player2: {
          account: true,
          playSelection: true,
        },
      },
    });

    assertLoaded(loadedGame);

    // Create a play selection
    const playSelection = PlaySelection.create({ value: "scissors" });

    // Assign the play selection to player1 (similar to the route logic)
    loadedGame.player1.$jazz.set("playSelection", playSelection);

    // Verify that the playSelection is not null and has the expected value
    expect(loadedGame.player1.playSelection.$jazz.id).toBe(
      playSelection.$jazz.id,
    );
    expect(loadedGame.player1.playSelection.value).toEqual("scissors");
  });
});

describe("nested CoValue validation mode propagation", () => {
  test("create with nested CoValue - loose validation should not throw", () => {
    const Dog = co.map({
      age: z.number(),
    });
    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    // Should throw with default strict validation when age is a string
    expectValidationError(() =>
      Person.create({
        name: "john",
        dog: { age: "12" as unknown as number },
      }),
    );

    // Should not throw with loose validation even though age is invalid
    expect(() =>
      Person.create(
        {
          name: "john",
          dog: { age: "12" as unknown as number },
        },
        { validation: "loose" },
      ),
    ).not.toThrow();

    const person = Person.create(
      {
        name: "john",
        dog: { age: "12" as unknown as number },
      },
      { validation: "loose" },
    );

    // Verify the nested CoValue was created with invalid data
    expect(person.name).toBe("john");
    expect(person.dog).toBeDefined();
    expect(person.dog.age).toBe("12");
  });

  test("set with nested CoValue - loose validation should not throw", () => {
    const Dog = co.map({
      age: z.number(),
    });
    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    const person = Person.create({
      name: "john",
      dog: { age: 5 },
    });

    // Should throw with default strict validation
    expectValidationError(() =>
      person.$jazz.set("dog", {
        age: "invalid" as unknown as number,
      }),
    );

    // Should not throw with loose validation
    expect(() =>
      person.$jazz.set(
        "dog",
        {
          age: "invalid" as unknown as number,
        },
        { validation: "loose" },
      ),
    ).not.toThrow();

    // Verify the nested CoValue was created with invalid data
    expect(person.dog.age).toBe("invalid");
  });

  test("applyDiff with nested CoValue - loose validation should not throw", () => {
    const Dog = co.map({
      age: z.number(),
    });
    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    const person = Person.create({
      name: "john",
      dog: { age: 5 },
    });

    // Should throw with default strict validation
    expectValidationError(() =>
      person.$jazz.applyDiff({
        dog: { age: "string" as unknown as number },
      }),
    );

    // Should not throw with loose validation
    expect(() =>
      person.$jazz.applyDiff(
        {
          dog: { age: "string" as unknown as number },
        },
        { validation: "loose" },
      ),
    ).not.toThrow();

    // Verify the nested CoValue was updated with invalid data
    expect(person.dog.age).toBe("string");
  });

  test("create with deeply nested CoValues - loose validation should not throw", () => {
    const Collar = co.map({
      size: z.number(),
    });
    const Dog = co.map({
      age: z.number(),
      collar: Collar,
    });
    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    // Should throw with strict validation when any nested field is invalid
    expectValidationError(() =>
      Person.create({
        name: "john",
        dog: {
          age: "12" as unknown as number,
          collar: { size: 10 },
        },
      }),
    );

    expectValidationError(() =>
      Person.create({
        name: "john",
        dog: {
          age: 12,
          // @ts-expect-error - size should be number
          collar: { size: "large" },
        },
      }),
    );

    // Should not throw with loose validation at any level
    expect(() =>
      Person.create(
        {
          name: "john",
          dog: {
            age: "12" as unknown as number,
            collar: { size: "large" as unknown as number },
          },
        },
        { validation: "loose" },
      ),
    ).not.toThrow();

    const person = Person.create(
      {
        name: "john",
        dog: {
          age: "12" as unknown as number,
          collar: { size: "large" as unknown as number },
        },
      },
      { validation: "loose" },
    );

    // Verify all levels were created with invalid data
    expect(person.name).toBe("john");
    expect(person.dog.age).toBe("12");
    expect(person.dog.collar.size).toBe("large");
  });

  test("create with nested CoValue - strict validation explicitly set should throw", () => {
    const Dog = co.map({
      age: z.number(),
    });
    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    // Explicitly setting validation to strict should throw
    expectValidationError(() =>
      Person.create(
        {
          name: "john",
          dog: { age: "12" as unknown as number },
        },
        { validation: "strict" },
      ),
    );
  });

  test("global loose validation mode propagates to nested CoValues in all mutations", () => {
    const Collar = co.map({
      size: z.number(),
    });
    const Dog = co.map({
      age: z.number(),
      collar: Collar,
    });
    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    // Set global validation mode to loose
    setDefaultValidationMode("loose");

    try {
      // Test 1: Create with deeply nested invalid data
      const person = Person.create({
        name: "john",
        dog: {
          age: "12" as unknown as number,
          collar: { size: "large" as unknown as number },
        },
      });

      // Verify all nested levels were created with invalid data
      expect(person.name).toBe("john");
      expect(person.dog.age).toBe("12");
      expect(person.dog.collar.size).toBe("large");

      // Test 2: Set with nested invalid data
      person.$jazz.set("dog", {
        age: "15" as unknown as number,
        collar: { size: "medium" as unknown as number },
      });

      expect(person.dog.age).toBe("15");
      expect(person.dog.collar.size).toBe("medium");

      // Test 3: ApplyDiff with nested invalid data
      person.$jazz.applyDiff({
        dog: {
          age: "20" as unknown as number,
          collar: { size: "small" as unknown as number },
        },
      });

      expect(person.dog.age).toBe("20");
      expect(person.dog.collar.size).toBe("small");
    } finally {
      // Reset to strict mode
      setDefaultValidationMode("strict");
    }
  });
});
