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
import { CoKeys, Loaded, zodSchemaToCoSchema } from "../internal.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { setupTwoNodes, waitFor } from "./utils.js";

const Crypto = await WasmCrypto.create();

beforeEach(async () => {
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

    test("setting date as undefined should throw", () => {
      const Person = co.map({
        color: z.string(),
        _height: z.number(),
        birthday: z.date(),
        name: z.string(),
        // nullable: z.optional.encoded<string | undefined>({
        //   encode: (value: string | undefined) => value || null,
        //   decode: (value: unknown) => (value as string) || undefined,
        // });
        optionalDate: z.date().optional(),
      });

      expect(() =>
        Person.create({
          color: "red",
          _height: 10,
          name: "John",
          birthday: undefined!,
        }),
      ).toThrow();
    });

    test("CoMap with reference", () => {
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

      expect(person.dog?.name).toEqual("Rex");
      expect(person.dog?.breed).toEqual("Labrador");
    });

    test("CoMap with self reference", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        // TODO: would be nice if this didn't need a type annotation
        get friend(): z.ZodOptional<typeof Person> {
          return z.optional(Person);
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
        get friend(): z.ZodOptional<typeof Person> {
          return z.optional(Person);
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
        get friend(): z.ZodOptional<typeof Person> {
          return z.optional(Person);
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
      child: z.discriminatedUnion([ChildA, ChildB]),
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
    expect(loadedPerson.dog).toBe(null);

    await waitFor(() => expect(loadedPerson.dog).toBeTruthy());

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
      zodSchemaToCoSchema(Person), // TODO: we should get rid of the conversion in the future
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
    optionalNested: z.optional(NestedMap),
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

    const foundAlice = Person.findUnique({ name: "Alice" }, group.id);
    expect(foundAlice).toEqual(alice.id);
  });
});
