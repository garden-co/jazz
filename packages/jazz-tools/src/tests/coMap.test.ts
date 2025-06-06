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
import { Loaded, zodSchemaToCoSchema } from "../internal.js";
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
      child: z.discriminatedUnion("type", [ChildA, ChildB]),
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

    const AttributeValue = z.discriminatedUnion("type", [
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

    const Tag = z.discriminatedUnion("type", [
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

    const ErrorResponse = z.discriminatedUnion("type", [
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
  describe("Time", () => {
    test("empty map created time", () => {
      const currentTimestampInSeconds = Math.floor(Date.now() / 1000);
      const emptyMap = co.map({}).create({});
      const createdAtInSeconds = Math.floor(emptyMap._createdAt / 1000);

      expect(createdAtInSeconds).toEqual(currentTimestampInSeconds);
      expect(emptyMap._lastUpdatedAt).toEqual(emptyMap._createdAt);
    });

    test("created time and last updated time", async () => {
      const Person = co.map({
        name: z.string(),
      });

      let currentTimestampInSeconds = Math.floor(Date.now() / 1000);
      const person = Person.create({ name: "John" });

      const createdAt = person._createdAt;
      const createdAtInSeconds = Math.floor(createdAt / 1000);
      expect(createdAtInSeconds).toEqual(currentTimestampInSeconds);
      expect(person._lastUpdatedAt).toEqual(createdAt);

      await new Promise((r) => setTimeout(r, 1000));
      currentTimestampInSeconds = Math.floor(Date.now() / 1000);
      person.name = "Jane";

      const lastUpdatedAtInSeconds = Math.floor(person._lastUpdatedAt / 1000);
      expect(lastUpdatedAtInSeconds).toEqual(currentTimestampInSeconds);
      expect(person._createdAt).toEqual(createdAt);
      expect(person._lastUpdatedAt).not.toEqual(createdAt);
    });

    test("comap with custom uniqueness", () => {
      const Person = co.map({
        name: z.string(),
      });

      let currentTimestampInSeconds = Math.floor(Date.now() / 1000);
      const person = Person.create(
        { name: "John" },
        { unique: "name", owner: Account.getMe() },
      );

      const createdAt = person._createdAt;
      const createdAtInSeconds = Math.floor(createdAt / 1000);
      expect(createdAtInSeconds).toEqual(currentTimestampInSeconds);
    });

    test("empty comap with custom uniqueness", () => {
      const Person = co.map({
        name: z.optional(z.string()),
      });

      let currentTimestampInSeconds = Math.floor(Date.now() / 1000);
      const person = Person.create(
        {},
        { unique: "name", owner: Account.getMe() },
      );

      const createdAt = person._createdAt;
      const createdAtInSeconds = Math.floor(createdAt / 1000);
      expect(createdAtInSeconds).toEqual(currentTimestampInSeconds);
    });
  });
});
