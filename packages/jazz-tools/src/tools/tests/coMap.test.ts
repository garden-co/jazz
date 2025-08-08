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
import { co, Group, subscribeToCoValue, z } from "../exports.js";
import { Account } from "../index.js";
import { coValueClassFromCoValueClassOrSchema, Loaded } from "../internal.js";
import {
  createJazzTestAccount,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing.js";
import { setupTwoNodes, waitFor } from "./utils.js";

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
      expect(john._raw.get("birthday")).toEqual(birthday.toISOString());
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
      expect("age" in john).toEqual(false);
    });

    test("create a CoMap with an account as owner", () => {
      const Person = co.map({
        name: z.string(),
      });

      const john = Person.create({ name: "John" }, Account.getMe());

      expect(john.name).toEqual("John");
      expect(john._raw.get("name")).toEqual("John");
    });

    test("create a CoMap with a group as owner", () => {
      const Person = co.map({
        name: z.string(),
      });

      const john = Person.create({ name: "John" }, Group.create());

      expect(john.name).toEqual("John");
      expect(john._raw.get("name")).toEqual("John");
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

    describe("create CoMap with references using JSON", () => {
      const Dog = co.map({
        type: z.literal("dog"),
        name: z.string(),
      });
      const Cat = co.map({
        type: z.literal("cat"),
        name: z.string(),
      });

      const Person = co.map({
        name: co.plainText(),
        bio: co.richText(),
        dog: Dog,
        get friends() {
          return co.list(Person);
        },
        reactions: co.feed(co.plainText()),
        pet: co.discriminatedUnion("type", [Dog, Cat]),
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
            },
          ],
          reactions: ["👎", "👍"],
          pet: { type: "cat", name: "Whiskers" },
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
      });

      it("creates a group for each new CoValue that is a child of the referencing CoValue's owner", () => {
        for (const value of Object.values(person)) {
          expect(
            value._owner.getParentGroups().map((group: Group) => group.id),
          ).toContain(person._owner.id);
        }
        const friend = person.friends[0]!;
        for (const value of Object.values(friend)) {
          expect(
            value._owner.getParentGroups().map((group: Group) => group.id),
          ).toContain(friend._owner.id);
        }
      });

      it("can create a coPlainText from an empty string", () => {
        const Schema = co.map({ text: co.plainText() });
        const map = Schema.create({ text: "" });
        expect(map.text.toString()).toBe("");
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

    test("toJSON should not fail when there is a key in the raw value not represented in the schema", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const person = Person.create({ name: "John", age: 20 });

      person._raw.set("extra", "extra");

      expect(person.toJSON()).toEqual({
        _type: "CoMap",
        id: person.id,
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
        _type: "CoMap",
        id: person.id,
        name: "John",
        age: 20,
        friend: {
          _type: "CoMap",
          id: person.friend?.id,
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

      person.friend = person;

      expect(person.toJSON()).toEqual({
        _type: "CoMap",
        id: person.id,
        name: "John",
        age: 20,
        friend: {
          _circular: person.id,
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
        name: "John",
        age: 20,
        birthday: birthday.toISOString(),
        _type: "CoMap",
        id: john.id,
      });
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

      // @ts-expect-error - x is not a valid property
      const john = Person.create({ name: "John", age: 30, x: 1 });

      expect(john.toJSON()).toEqual({
        _type: "CoMap",
        id: john.id,
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

      person.name = "Jane";
      person.age = 28;
      person.extra = "extra";

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
      const loadedPersonA = await Person.load(personA.id, {
        resolve: true,
        loadAs: userB,
      });

      expect(loadedPersonA).not.toBeNull();
      assert(loadedPersonA);

      const personB = Person.create({
        name: "Jane",
        age: 28,
        pet,
        friend: loadedPersonA,
      });

      expect(personB.friend?.pet.name).toEqual("Rex");
    });
  });

  describe("Mutation", () => {
    test("change a primitive value", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 20 });

      john.name = "Jane";

      expect(john.name).toEqual("Jane");
      expect(john.age).toEqual(20);
    });

    test("delete an optional value", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      const john = Person.create({ name: "John", age: 20 });

      delete john.age;

      expect(john.name).toEqual("John");
      expect(john.age).toEqual(undefined);

      expect(john.toJSON()).toEqual({
        _type: "CoMap",
        id: john.id,
        name: "John",
      });
    });

    test("update a reference", () => {
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

      john.dog = Dog.create({ name: "Fido" });

      expect(john.dog?.name).toEqual("Fido");
    });

    test("changes should be listed in _edits", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 20 });

      const me = Account.getMe();

      john.age = 21;

      expect(john._edits.age?.all).toEqual([
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
      expect(john._edits.age?.all[0]?.by).toMatchObject({
        _type: "Account",
        id: me.id,
      });
      expect(john._edits.age?.all[1]?.by).toMatchObject({
        _type: "Account",
        id: me.id,
      });
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
    expect(mapWithEnum.child?.id).toBeDefined();

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

    const loadedPerson = await Person.load(person.id, {
      resolve: {
        dog: true,
      },
    });

    assert(loadedPerson);
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

    const loadedPerson = await Person.load(person.id);

    assert(loadedPerson);
    expect(loadedPerson.dog?.name).toEqual("Rex");
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

    const loadedPerson = await Person.load(person.id, {
      resolve: {
        dog: true,
      },
      loadAs: userB,
    });

    assert(loadedPerson);
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
    const loadedPerson = await Person.load(person.id, {
      loadAs: userB,
    });

    assert(loadedPerson);
    expect(loadedPerson.dog?.name).toEqual("Rex");
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
    currentAccount._raw.core.node.syncManager.getPeers().forEach((peer) => {
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
    const loadedPerson = await Person.load(person.id, {
      loadAs: userB,
      skipRetry: true,
    });

    expect(loadedPerson).toBeNull();
  });

  test("loading a remotely available map with skipRetry set to false", async () => {
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

    // Disconnect the current account
    currentAccount._raw.core.node.syncManager.getPeers().forEach((peer) => {
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
    let resolved = false;
    const promise = Person.load(person.id, {
      loadAs: userB,
      skipRetry: false,
    });
    promise.then(() => {
      resolved = true;
    });

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(resolved).toBe(false);

    // Reconnect the current account
    currentAccount._raw.core.node.syncManager.addPeer(
      getPeerConnectedToTestSyncServer(),
    );

    const loadedPerson = await promise;

    expect(resolved).toBe(true);
    assert(loadedPerson);
    expect(loadedPerson.dog?.name).toEqual("Rex");
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
    const loadedPerson = await Person.load(person.id, {
      loadAs: userB,
    });

    assert(loadedPerson);

    expect(loadedPerson._refs.dog?.id).toBe(person.dog!.id);

    const dog = await loadedPerson._refs.dog?.load();

    assert(dog);

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
      person.id,
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

    person.dog!.name = "Fido";

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

    Person.subscribe(person.id, {}, spy);

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.dog?.name).toEqual("Rex");

    person.dog!.name = "Fido";

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.dog?.name).toEqual("Fido");

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
      person.id,
      {
        syncResolution: true,
        loadAs: Account.getMe(),
      },
      spy,
    );

    expect(spy).toHaveBeenCalled();
    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.dog?.name).toEqual("Rex");

    expect(spy).toHaveBeenCalledTimes(1);

    person.dog!.name = "Fido";

    expect(spy).toHaveBeenCalledTimes(2);

    expect(updates[1]?.dog?.name).toEqual("Fido");

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
      person.id,
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

    person.dog!.name = "Fido";

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
      person.id,
      {
        loadAs: userB,
      },
      spy,
    );

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.dog?.name).toEqual("Rex");

    person.dog!.name = "Fido";

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.dog?.name).toEqual("Fido");

    expect(spy).toHaveBeenCalledTimes(2);
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
      person.id,
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

    person.dog!.name = "Fido";

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.dog.name).toEqual("Fido");

    expect(spy).toHaveBeenCalledTimes(2);
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

    map.applyDiff(newValues);

    expect(map.name).toEqual("Bob");
    expect(map.age).toEqual(35);
    expect(map.isActive).toEqual(false);
    expect(map.birthday).toEqual(new Date("1990-01-01"));
    expect(map.nested?.value).toEqual("original");
  });

  test("applyDiff with nested changes", () => {
    const map = TestMap.create(
      {
        name: "Charlie",
        age: 25,
        isActive: true,
        birthday: new Date("1995-01-01"),
        nested: NestedMap.create({ value: "original" }, { owner: me }),
      },
      { owner: me },
    );

    const newValues = {
      name: "David",
      nested: NestedMap.create({ value: "updated" }, { owner: me }),
    };

    map.applyDiff(newValues);

    expect(map.name).toEqual("David");
    expect(map.age).toEqual(25);
    expect(map.nested?.value).toEqual("updated");
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

    map.applyDiff(newValues);

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

    map.applyDiff(newValues);

    expect(map.optionalField).toEqual("New optional value");

    map.applyDiff({ optionalField: undefined });

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

    map.applyDiff({});

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
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    map.applyDiff(newValues as any);

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

    map.applyDiff(newValues);

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

    expect(() => map.applyDiff(newValues)).toThrowError(
      "Cannot set required reference nested to undefined",
    );
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
      {
        optional: NestedMap.create({ value: "" }, { owner: me }),
        // @ts-expect-error null can't be passed to a non-optional field
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

    await map.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the map from it
    clientNode.gracefulShutdown();

    const loadedMap = await serverNode.load(map._raw.id);

    expect(loadedMap).not.toBe("unavailable");
  });
});

describe("Creating and finding unique CoMaps", async () => {
  test("Creating and finding unique CoMaps", async () => {
    const group = Group.create();

    const Person = co.map({
      name: z.string(),
      _height: z.number(),
      birthday: z.date(),
      color: z.string(),
    });

    const alice = Person.create(
      {
        name: "Alice",
        _height: 100,
        birthday: new Date("1990-01-01"),
        color: "red",
      },
      { owner: group, unique: { name: "Alice" } },
    );

    const foundAlice = await Person.loadUnique({ name: "Alice" }, group.id);
    expect(foundAlice).toEqual(alice);
  });

  test("manual upserting pattern", async () => {
    // Schema
    const Event = co.map({
      title: z.string(),
      identifier: z.string(),
      external_id: z.string(),
    });

    // Data
    const sourceData = {
      title: "Test Event Title",
      identifier: "test-event-identifier",
      _id: "test-event-external-id",
    };
    const workspace = Group.create();

    // Pattern
    let activeEvent = await Event.loadUnique(
      { identifier: sourceData.identifier },
      workspace.id,
    );
    if (!activeEvent) {
      activeEvent = Event.create(
        {
          title: sourceData.title,
          identifier: sourceData.identifier,
          external_id: sourceData._id,
        },
        workspace,
      );
    } else {
      activeEvent.applyDiff({
        title: sourceData.title,
        identifier: sourceData.identifier,
        external_id: sourceData._id,
      });
    }
    expect(activeEvent).toEqual({
      title: sourceData.title,
      identifier: sourceData.identifier,
      external_id: sourceData._id,
    });
  });

  test("upserting a non-existent value", async () => {
    // Schema
    const Event = co.map({
      title: z.string(),
      identifier: z.string(),
      external_id: z.string(),
    });

    // Data
    const sourceData = {
      title: "Test Event Title",
      identifier: "test-event-identifier",
      _id: "test-event-external-id",
    };
    const workspace = Group.create();

    // Upserting
    const activeEvent = await Event.upsertUnique({
      value: {
        title: sourceData.title,
        identifier: sourceData.identifier,
        external_id: sourceData._id,
      },
      unique: sourceData.identifier,
      owner: workspace,
    });
    expect(activeEvent).toEqual({
      title: sourceData.title,
      identifier: sourceData.identifier,
      external_id: sourceData._id,
    });
  });

  test("upserting an existing value", async () => {
    // Schema
    const Event = co.map({
      title: z.string(),
      identifier: z.string(),
      external_id: z.string(),
    });

    // Data
    const oldSourceData = {
      title: "Old Event Title",
      identifier: "test-event-identifier",
      _id: "test-event-external-id",
    };
    const newSourceData = {
      title: "New Event Title",
      identifier: "test-event-identifier",
      _id: "test-event-external-id",
    };
    expect(oldSourceData.identifier).toEqual(newSourceData.identifier);
    const workspace = Group.create();
    const oldActiveEvent = Event.create(
      {
        title: oldSourceData.title,
        identifier: oldSourceData.identifier,
        external_id: oldSourceData._id,
      },
      workspace,
    );

    // Upserting
    const activeEvent = await Event.upsertUnique({
      value: {
        title: newSourceData.title,
        identifier: newSourceData.identifier,
        external_id: newSourceData._id,
      },
      unique: newSourceData.identifier,
      owner: workspace,
    });
    expect(activeEvent).toEqual({
      title: newSourceData.title,
      identifier: newSourceData.identifier,
      external_id: newSourceData._id,
    });
    expect(activeEvent).not.toEqual(oldActiveEvent);
  });

  test("upserting a non-existent value with resolve", async () => {
    const Project = co.map({
      name: z.string(),
    });
    const Organisation = co.map({
      name: z.string(),
      projects: co.list(Project),
    });
    const workspace = Group.create();

    const myOrg = await Organisation.upsertUnique({
      value: {
        name: "My organisation",
        projects: co.list(Project).create(
          [
            Project.create(
              {
                name: "My project",
              },
              workspace,
            ),
          ],
          workspace,
        ),
      },
      unique: { name: "My organisation" },
      owner: workspace,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });
    assert(myOrg);
    expect(myOrg).not.toBeNull();
    expect(myOrg.name).toEqual("My organisation");
    expect(myOrg.projects.length).toBe(1);
    expect(myOrg.projects[0]).toMatchObject({
      name: "My project",
    });
  });

  test("upserting an existing value with resolve", async () => {
    const Project = co.map({
      name: z.string(),
    });
    const Organisation = co.map({
      name: z.string(),
      projects: co.list(Project),
    });
    const workspace = Group.create();
    const initialProject = await Project.upsertUnique({
      value: {
        name: "My project",
      },
      unique: { unique: "First project" },
      owner: workspace,
    });
    assert(initialProject);
    expect(initialProject).not.toBeNull();
    expect(initialProject.name).toEqual("My project");

    const myOrg = await Organisation.upsertUnique({
      value: {
        name: "My organisation",
        projects: co.list(Project).create([initialProject], workspace),
      },
      unique: { name: "My organisation" },
      owner: workspace,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });
    assert(myOrg);
    expect(myOrg).not.toBeNull();
    expect(myOrg.name).toEqual("My organisation");
    expect(myOrg.projects.length).toBe(1);
    expect(myOrg.projects.at(0)?.name).toEqual("My project");

    const updatedProject = await Project.upsertUnique({
      value: {
        name: "My updated project",
      },
      unique: { unique: "First project" },
      owner: workspace,
    });

    assert(updatedProject);
    expect(updatedProject).not.toBeNull();
    expect(updatedProject).toEqual(initialProject);
    expect(updatedProject.name).toEqual("My updated project");
    expect(myOrg.projects.length).toBe(1);
    expect(myOrg.projects.at(0)?.name).toEqual("My updated project");
  });

  test("upserting a partially loaded value on an new value with resolve", async () => {
    const Project = co.map({
      name: z.string(),
    });
    const Organisation = co.map({
      name: z.string(),
      projects: co.list(Project),
    });
    const publicAccess = Group.create();
    publicAccess.addMember("everyone", "writer");

    const initialProject = await Project.upsertUnique({
      value: {
        name: "My project",
      },
      unique: { unique: "First project" },
      owner: publicAccess,
    });
    assert(initialProject);
    expect(initialProject).not.toBeNull();
    expect(initialProject.name).toEqual("My project");

    const fullProjectList = co
      .list(Project)
      .create([initialProject], publicAccess);

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const shallowProjectList = await co.list(Project).load(fullProjectList.id, {
      loadAs: account,
    });
    assert(shallowProjectList);

    const publicAccessAsNewAccount = await Group.load(publicAccess.id, {
      loadAs: account,
    });
    assert(publicAccessAsNewAccount);

    const updatedOrg = await Organisation.upsertUnique({
      value: {
        name: "My organisation",
        projects: shallowProjectList,
      },
      unique: { name: "My organisation" },
      owner: publicAccessAsNewAccount,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });

    assert(updatedOrg);

    expect(updatedOrg.projects.id).toEqual(fullProjectList.id);
    expect(updatedOrg.projects.length).toBe(1);
    expect(updatedOrg.projects.at(0)?.name).toEqual("My project");
  });

  test("upserting a partially loaded value on an existing value with resolve", async () => {
    const Project = co.map({
      name: z.string(),
    });
    const Organisation = co.map({
      name: z.string(),
      projects: co.list(Project),
    });
    const publicAccess = Group.create();
    publicAccess.addMember("everyone", "writer");

    const initialProject = await Project.upsertUnique({
      value: {
        name: "My project",
      },
      unique: { unique: "First project" },
      owner: publicAccess,
    });
    assert(initialProject);
    expect(initialProject).not.toBeNull();
    expect(initialProject.name).toEqual("My project");

    const myOrg = await Organisation.upsertUnique({
      value: {
        name: "My organisation",
        projects: co.list(Project).create([], publicAccess),
      },
      unique: { name: "My organisation" },
      owner: publicAccess,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });
    assert(myOrg);

    const fullProjectList = co
      .list(Project)
      .create([initialProject], publicAccess);

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const shallowProjectList = await co.list(Project).load(fullProjectList.id, {
      loadAs: account,
    });
    assert(shallowProjectList);

    const publicAccessAsNewAccount = await Group.load(publicAccess.id, {
      loadAs: account,
    });
    assert(publicAccessAsNewAccount);

    const updatedOrg = await Organisation.upsertUnique({
      value: {
        name: "My organisation",
        projects: shallowProjectList,
      },
      unique: { name: "My organisation" },
      owner: publicAccessAsNewAccount,
      resolve: {
        projects: {
          $each: true,
        },
      },
    });

    assert(updatedOrg);

    expect(updatedOrg.projects.id).toEqual(fullProjectList.id);
    expect(updatedOrg.projects.length).toBe(1);
    expect(updatedOrg.projects.at(0)?.name).toEqual("My project");
    expect(updatedOrg.id).toEqual(myOrg.id);
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

describe("castAs", () => {
  test("should cast a co.map type", () => {
    const Person = co.map({
      name: z.string(),
    });

    const PersonWithAge = co.map({
      name: z.string(),
      age: z.number().optional(),
    });

    const person = Person.create({
      name: "Alice",
    });

    const personWithAge = person.castAs(PersonWithAge);

    personWithAge.age = 20;

    expect(personWithAge.age).toEqual(20);
  });

  test("should still be able to autoload in-memory deps", () => {
    const Dog = co.map({
      name: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    const PersonWithAge = co.map({
      name: z.string(),
      age: z.number().optional(),
      dog: Dog,
    });

    const person = Person.create({
      name: "Alice",
      dog: Dog.create({ name: "Rex" }),
    });

    const personWithAge = person.castAs(PersonWithAge);

    personWithAge.age = 20;

    expect(personWithAge.age).toEqual(20);
    expect(personWithAge.dog?.name).toEqual("Rex");
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
          person.age = 20;
          person.version = 2;
        }
      });

    const person = PersonV1.create({
      name: "Bob",
      version: 1,
    });

    expect(person?.name).toEqual("Bob");
    expect(person?.version).toEqual(1);

    const loadedPerson = await Person.load(person.id);

    expect(loadedPerson?.name).toEqual("Bob");
    expect(loadedPerson?.age).toEqual(20);
    expect(loadedPerson?.version).toEqual(2);
  });

  test("should handle group updates", async () => {
    const Person = co
      .map({
        name: z.string(),
        version: z.literal([1, 2]),
      })
      .withMigration((person) => {
        if (person.version === 1) {
          person.version = 2;

          person._owner.castAs(Group).addMember("everyone", "reader");
        }
      });

    const person = Person.create({
      name: "Bob",
      version: 1,
    });

    expect(person?.name).toEqual("Bob");
    expect(person?.version).toEqual(1);

    const loadedPerson = await Person.load(person.id);

    expect(loadedPerson?.name).toEqual("Bob");
    expect(loadedPerson?.version).toEqual(2);

    const anotherAccount = await createJazzTestAccount();

    const loadedPersonFromAnotherAccount = await Person.load(person.id, {
      loadAs: anotherAccount,
    });

    expect(loadedPersonFromAnotherAccount?.name).toEqual("Bob");
  });

  test("should throw an error if a migration is async", async () => {
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

    await expect(Person.load(person.id)).rejects.toThrow(
      "Migration function cannot be async",
    );
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

    await Person.load(person.id);
    await Person.load(person.id);
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
          person.age = 20;
          person.version = 2;
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

    const loaded = await Person.load(bob.id, {
      resolve: {
        friend: true,
      },
    });

    // Migration should run on both the person and their friend
    expect(loaded?.name).toEqual("Bob");
    expect(loaded?.age).toEqual(20);
    expect(loaded?.version).toEqual(2);
    expect(loaded?.friend?.name).toEqual("Charlie");
    expect(loaded?.friend?.version).toEqual(2);
  });
});

describe("createdAt & lastUpdatedAt", () => {
  test("empty map created time", () => {
    const emptyMap = co.map({}).create({});

    expect(emptyMap._lastUpdatedAt).toEqual(emptyMap._createdAt);
  });

  test("created time and last updated time", async () => {
    const Person = co.map({
      name: z.string(),
    });

    const person = Person.create({ name: "John" });

    const createdAt = person._createdAt;
    expect(person._lastUpdatedAt).toEqual(createdAt);

    await new Promise((r) => setTimeout(r, 10));
    person.name = "Jane";

    expect(person._createdAt).toEqual(createdAt);
    expect(person._lastUpdatedAt).not.toEqual(createdAt);
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

      draftPerson.name = "John";
      draftPerson.age = 20;
      const rex = Dog.create({ name: "Rex", breed: "Labrador" });
      draftPerson.pet = rex;

      expect(draftPerson.name).toEqual("John");
      expect(draftPerson.age).toEqual(20);
      expect(draftPerson.pet).toEqual(rex);
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
      draftPerson.extraField = "extra";

      expect(draftPerson.extraField).toEqual("extra");
    });
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
    const loadedGame = await Game.load(game.id, {
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

    assert(loadedGame);

    // Create a play selection
    const playSelection = PlaySelection.create({ value: "rock", group }, group);

    // Assign the play selection to player1 (similar to the route logic)
    loadedGame.player1.playSelection = playSelection;

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
    const loadedGame = await Game.load(game.id, {
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

    assert(loadedGame);

    // Create a play selection
    const playSelection = PlaySelection.create({ value: "scissors" });

    // Assign the play selection to player1 (similar to the route logic)
    loadedGame.player1.playSelection = playSelection;

    // Verify that the playSelection is not null and has the expected value
    expect(loadedGame.player1.playSelection.id).toBe(playSelection.id);
    expect(loadedGame.player1.playSelection.value).toEqual("scissors");
  });
});
