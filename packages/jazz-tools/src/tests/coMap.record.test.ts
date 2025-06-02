import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import {
  assert,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
} from "vitest";
import { Group, co, z } from "../exports.js";
import { InstanceOrPrimitiveOfSchema } from "../implementation/zodSchema/typeConverters/InstanceOrPrimitiveOfSchema.js";
import { Loaded } from "../implementation/zodSchema/zodSchema.js";
import { Account } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { waitFor } from "./utils.js";

const Crypto = await WasmCrypto.create();

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });
});

describe("CoMap.Record", async () => {
  describe("init", () => {
    test("create a Record with basic property access", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({
        name: "John",
        age: "20",
      });

      expect(person.name).toEqual("John");
      expect(person.age).toEqual("20");
      expect(Object.keys(person)).toEqual(["name", "age"]);
    });

    test("create a Record with enum value", () => {
      const Person = co.record(z.string(), z.enum(["a", "b", "c"]));

      const person = Person.create({
        age: "a",
      });

      expect(person.age).toEqual("a");
      expect(Object.keys(person)).toEqual(["age"]);
    });

    test("property existence", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" });

      expect("name" in person).toEqual(true);
      expect("age" in person).toEqual(false);
    });

    test("create a Record with an account as owner", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" }, Account.getMe());

      expect(person.name).toEqual("John");
      expect(person._raw.get("name")).toEqual("John");
    });

    test("create a Record with a group as owner", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" }, Group.create());

      expect(person.name).toEqual("John");
      expect(person._raw.get("name")).toEqual("John");
    });

    test("Empty schema", () => {
      const EmptyRecord = co.record(z.string(), z.string());
      const emptyRecord = EmptyRecord.create({});

      expect(Object.keys(emptyRecord)).toEqual([]);
    });

    test("Record with reference", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.record(z.string(), Dog);

      const person = Person.create({
        pet1: Dog.create({ name: "Rex", breed: "Labrador" }),
        pet2: Dog.create({ name: "Fido", breed: "Poodle" }),
      });

      expect(person.pet1?.name).toEqual("Rex");
      expect(person.pet1?.breed).toEqual("Labrador");
      expect(person.pet2?.name).toEqual("Fido");
      expect(person.pet2?.breed).toEqual("Poodle");
    });
  });

  describe("Mutation", () => {
    test("change a primitive value", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" });

      person.name = "Jane";

      expect(person.name).toEqual("Jane");
    });

    test("delete a value", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John", age: "20" });

      delete person.age;

      expect(person.name).toEqual("John");
      expect("age" in person).toEqual(false);

      expect(person.toJSON()).toEqual({
        _type: "CoMap",
        id: person.id,
        name: "John",
      });
    });

    test("update a reference", () => {
      const Dog = co.map({
        name: z.string(),
      });

      const Person = co.record(z.string(), Dog);

      const person = Person.create({
        pet1: Dog.create({ name: "Rex" }),
      });

      person.pet1 = Dog.create({ name: "Fido" });

      expect(person.pet1?.name).toEqual("Fido");
    });

    test("changes should be listed in _edits", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" });

      const me = Account.getMe();

      person.name = "Jane";

      const edits = person._edits.name?.all;
      expect(edits).toEqual([
        expect.objectContaining({
          value: "John",
          key: "name",
          ref: undefined,
          madeAt: expect.any(Date),
        }),
        expect.objectContaining({
          value: "Jane",
          key: "name",
          ref: undefined,
          madeAt: expect.any(Date),
        }),
      ]);
      expect(edits?.[0]?.by).toMatchObject({ _type: "Account", id: me.id });
      expect(edits?.[1]?.by).toMatchObject({ _type: "Account", id: me.id });
    });
  });

  describe("Record resolution", async () => {
    test("loading a locally available record with deep resolve", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.record(z.string(), Dog);

      const person = Person.create({
        pet1: Dog.create({ name: "Rex", breed: "Labrador" }),
        pet2: Dog.create({ name: "Fido", breed: "Poodle" }),
      });

      const loadedPerson = await Person.load(person.id, {
        resolve: {
          $each: true,
        },
      });

      assert(loadedPerson);
      expect(loadedPerson.pet1?.name).toEqual("Rex");
      expect(loadedPerson.pet2?.name).toEqual("Fido");
    });

    test("loading a locally available record using autoload for the refs", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.record(z.string(), Dog);

      const person = Person.create({
        pet1: Dog.create({ name: "Rex", breed: "Labrador" }),
        pet2: Dog.create({ name: "Fido", breed: "Poodle" }),
      });

      const loadedPerson = await Person.load(person.id);

      assert(loadedPerson);
      expect(loadedPerson.pet1?.name).toEqual("Rex");
      expect(loadedPerson.pet2?.name).toEqual("Fido");
    });

    test("subscription on a locally available record with deep resolve", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.record(z.string(), Dog);

      const person = Person.create({
        pet1: Dog.create({ name: "Rex", breed: "Labrador" }),
      });

      type V = (typeof Person)["_zod"]["def"]["valueType"];
      type T = InstanceOrPrimitiveOfSchema<typeof Person>;

      const updates: Loaded<typeof Person, { $each: true }>[] = [];
      const spy = vi.fn((person) => updates.push(person));

      Person.subscribe(
        person.id,
        {
          resolve: {
            $each: true,
          },
        },
        spy,
      );

      expect(spy).not.toHaveBeenCalled();

      await waitFor(() => expect(spy).toHaveBeenCalled());

      expect(spy).toHaveBeenCalledTimes(1);

      expect(updates[0]?.pet1?.name).toEqual("Rex");

      person.pet1 = Dog.create({ name: "Fido", breed: "Poodle" });

      await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

      expect(updates[1]?.pet1?.name).toEqual("Fido");

      expect(spy).toHaveBeenCalledTimes(2);
    });
  });

  describe("Record Typescript validation", async () => {
    const me = await Account.create({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

    const NestedRecord = co.map({
      value: z.string(),
    });

    test("Is ok to omit optional fields", () => {
      const TestRecord = co.record(z.string(), z.optional(NestedRecord));

      expectTypeOf<typeof TestRecord.create>().toBeCallableWith(
        {
          key1: NestedRecord.create({ value: "" }, { owner: me }),
        },
        { owner: me },
      );

      expectTypeOf<typeof TestRecord.create>().toBeCallableWith(
        {
          key1: NestedRecord.create({ value: "" }, { owner: me }),
          key2: undefined,
        },
        { owner: me },
      );
    });
  });

  // Covers https://github.com/garden-co/jazz/issues/2385
  test("create a Record with a discriminated union containing a co.map that uses withHelpers", () => {
    const Base = co.map({
      type: z.literal("base"),
      name: z.string(),
    });

    const IssueRepro = co.map({
      type: z.literal("repro"),
      catchall: co.map({}).withHelpers((self) => self),
      name: z.string(),
    });

    const PersonRecord = co.record(
      z.string(),
      z.discriminatedUnion("type", [Base, IssueRepro]),
    );

    const person = IssueRepro.create({
      type: "repro",
      catchall: IssueRepro.def.shape.catchall.create({}),
      name: "John",
    });

    const record = PersonRecord.create({
      john: person,
    });

    if (record.john?.type === "repro") {
      expect(record.john.catchall).toEqual({});
      expect(record.john.name).toEqual("John");
      expect(record.john.type).toEqual("repro");
    }
  });

  // Covers https://github.com/garden-co/jazz/issues/2385
  test("create a Record with a discriminated union containing a co.map that uses catchall", () => {
    const Base = co.map({
      type: z.literal("base"),
      name: z.string(),
    });

    const IssueRepro = co.map({
      type: z.literal("repro"),
      catchall: co.map({}).catchall(z.string()),
      name: z.string(),
    });

    const PersonRecord = co.record(
      z.string(),
      z.discriminatedUnion("type", [Base, IssueRepro]),
    );

    const person = IssueRepro.create({
      type: "repro",
      catchall: IssueRepro.def.shape.catchall.create({}),
      name: "John",
    });

    const record = PersonRecord.create({
      john: person,
    });

    if (record.john?.type === "repro") {
      expect(record.john.catchall).toEqual({});
      expect(record.john.name).toEqual("John");
      expect(record.john.type).toEqual("repro");
    }
  });

  // Covers https://github.com/garden-co/jazz/issues/2385
  test("create a Record with a discriminated union containing a co.image", () => {
    const Base = co.map({
      type: z.literal("base"),
      name: z.string(),
    });

    const IssueRepro = co.map({
      type: z.literal("repro"),
      image: co.image().optional(),
      name: z.string(),
    });

    const PersonRecord = co.record(
      z.string(),
      z.discriminatedUnion("type", [Base, IssueRepro]),
    );

    const person = IssueRepro.create({
      type: "repro",
      image: undefined,
      name: "John",
    });

    const record = PersonRecord.create({
      john: person,
    });

    if (record.john?.type === "repro") {
      expect(record.john.image).toEqual(undefined);
      expect(record.john.name).toEqual("John");
      expect(record.john.type).toEqual("repro");
    }
  });
});
