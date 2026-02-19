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
import { FileStream, Group, co, z } from "../exports.js";
import { Loaded } from "../implementation/zodSchema/zodSchema.js";
import { Account } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { assertLoaded, expectValidationError, waitFor } from "./utils.js";
import { CoValueLoadingState, TypeSym } from "../internal.js";
import { setDefaultValidationMode } from "../implementation/zodSchema/validationSettings.js";

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

    test("create a Record with nullable values", () => {
      const Person = co.record(z.string(), z.string().nullable());
      const person = Person.create({ name: "John", age: null });
      person.$jazz.set("bio", null);
      expect(person.name).toEqual("John");
      expect(person.age).toEqual(null);
      expect(person.bio).toEqual(null);
    });

    test("property existence", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" });

      expect("name" in person).toEqual(true);
      // CoRecords accept any string key, so `in` returns true for all strings.
      // Use $jazz.has() to check if a key has a set value.
      expect("age" in person).toEqual(true);
      expect(person.$jazz.has("age")).toEqual(false);
    });

    test("create a Record with an account as owner", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" }, Account.getMe());

      expect(person.name).toEqual("John");
      expect(person.$jazz.raw.get("name")).toEqual("John");
    });

    test("create a Record with a group as owner", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" }, Group.create());

      expect(person.name).toEqual("John");
      expect(person.$jazz.raw.get("name")).toEqual("John");
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

      person.$jazz.set("name", "Jane");

      expect(person.name).toEqual("Jane");
    });

    test("delete a value", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John", age: "20" });

      // Deleting a non-existent property does nothing
      person.$jazz.delete("nonExistentProperty");

      person.$jazz.delete("age");

      expect(person.name).toEqual("John");
      // `in` returns true for all string keys on CoRecords.
      // Use $jazz.has() to check if a key has a set value.
      expect("age" in person).toEqual(true);
      expect(person.$jazz.has("age")).toEqual(false);

      expect(person.toJSON()).toEqual({
        $jazz: { id: person.$jazz.id },
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

      person.$jazz.set("pet1", Dog.create({ name: "Fido" }));

      expect(person.pet1?.name).toEqual("Fido");
    });

    test("changes should be listed in getEdits()", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" });

      const me = Account.getMe();

      person.$jazz.set("name", "Jane");

      const edits = person.$jazz.getEdits().name?.all;
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
      expect(edits?.[0]?.by).toMatchObject({
        [TypeSym]: "Account",
        $jazz: expect.objectContaining({ id: me.$jazz.id }),
      });
      expect(edits?.[1]?.by).toMatchObject({
        [TypeSym]: "Account",
        $jazz: expect.objectContaining({ id: me.$jazz.id }),
      });
    });

    test("getEdits() keys should return deleted keys", () => {
      const me = Account.getMe();

      const Person = co.record(z.string(), z.string());
      const person = Person.create({ name: "John" });
      person.$jazz.set("name", "Jane");
      person.$jazz.delete("name");

      expect(Object.keys(person.$jazz.getEdits())).toEqual(["name"]);

      const edits = person.$jazz.getEdits().name?.all;

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
        expect.objectContaining({
          value: undefined,
          key: "name",
          ref: undefined,
          madeAt: expect.any(Date),
        }),
      ]);

      expect(edits?.[0]?.by).toMatchObject({
        [TypeSym]: "Account",
        $jazz: expect.objectContaining({ id: me.$jazz.id }),
      });
      expect(edits?.[1]?.by).toMatchObject({
        [TypeSym]: "Account",
        $jazz: expect.objectContaining({ id: me.$jazz.id }),
      });
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

      const loadedPerson = await Person.load(person.$jazz.id, {
        resolve: {
          $each: true,
        },
      });

      assertLoaded(loadedPerson);
      expect(loadedPerson.pet1?.name).toEqual("Rex");
      expect(loadedPerson.pet2?.name).toEqual("Fido");
    });

    test("loading a locally available record with single resolve", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.record(z.string(), Dog);

      const person = Person.create({
        pet1: Dog.create({ name: "Rex", breed: "Labrador" }),
        pet2: Dog.create({ name: "Fido", breed: "Poodle" }),
      });

      const loadedPerson = await Person.load(person.$jazz.id, {
        resolve: {
          pet1: true,
        },
      });

      assertLoaded(loadedPerson);
      expect(loadedPerson.pet1?.name).toEqual("Rex");
    });

    test("loading a locally available record with unavailable single resolve", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.record(z.string(), Dog);

      const person = Person.create({
        pet1: Dog.create({ name: "Rex", breed: "Labrador" }),
        pet2: Dog.create({ name: "Fido", breed: "Poodle" }),
      });

      const loadedPerson = await Person.load(person.$jazz.id, {
        resolve: {
          pet3: true,
        },
      });

      expect(loadedPerson.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
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

      const loadedPerson = await Person.load(person.$jazz.id);

      assertLoaded(loadedPerson);
      assert(loadedPerson.pet1);
      assertLoaded(loadedPerson.pet1);
      expect(loadedPerson.pet1?.name).toEqual("Rex");
      assert(loadedPerson.pet2);
      assertLoaded(loadedPerson.pet2);
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

      const updates: Loaded<typeof Person, { $each: true }>[] = [];
      const spy = vi.fn((person) => updates.push(person));

      Person.subscribe(
        person.$jazz.id,
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

      person.$jazz.set("pet1", Dog.create({ name: "Fido", breed: "Poodle" }));

      await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

      expect(updates[1]?.pet1?.name).toEqual("Fido");

      expect(spy).toHaveBeenCalledTimes(2);
    });
  });

  describe("nested CoValue validation mode propagation", () => {
    test("create with nested CoValue - loose validation should not throw", () => {
      const Dog = co.map({
        age: z.number(),
      });
      const PersonRecord = co.record(z.string(), Dog);

      // Should throw with default strict validation when age is a string
      expectValidationError(() =>
        PersonRecord.create({
          john: { age: "12" as unknown as number },
        }),
      );

      // Should not throw with loose validation even though age is invalid
      expect(() =>
        PersonRecord.create(
          {
            john: { age: "12" as unknown as number },
          },
          { validation: "loose" },
        ),
      ).not.toThrow();

      const personRecord = PersonRecord.create(
        {
          john: { age: "12" as unknown as number },
        },
        { validation: "loose" },
      );

      // Verify the nested CoValue was created with invalid data
      expect(personRecord.john).toBeDefined();
      expect(personRecord.john?.age).toBe("12");
    });

    test("set with nested CoValue - loose validation should not throw", () => {
      const Dog = co.map({
        age: z.number(),
      });
      const PersonRecord = co.record(z.string(), Dog);

      const personRecord = PersonRecord.create({
        john: { age: 5 },
      });

      // Should throw with default strict validation
      expectValidationError(() =>
        personRecord.$jazz.set("john", {
          age: "invalid" as unknown as number,
        }),
      );

      // Should not throw with loose validation
      expect(() =>
        personRecord.$jazz.set(
          "john",
          {
            age: "invalid" as unknown as number,
          },
          { validation: "loose" },
        ),
      ).not.toThrow();

      // Verify the nested CoValue was created with invalid data
      expect(personRecord.john?.age).toBe("invalid");
    });

    test("applyDiff with nested CoValue - loose validation should not throw", () => {
      const Dog = co.map({
        age: z.number(),
      });
      const PersonRecord = co.record(z.string(), Dog);

      const personRecord = PersonRecord.create({
        john: { age: 5 },
      });

      // Should throw with default strict validation
      expectValidationError(() =>
        personRecord.$jazz.applyDiff({
          john: { age: "string" as unknown as number },
        }),
      );

      // Should not throw with loose validation
      expect(() =>
        personRecord.$jazz.applyDiff(
          {
            john: { age: "string" as unknown as number },
          },
          { validation: "loose" },
        ),
      ).not.toThrow();

      // Verify the nested CoValue was updated with invalid data
      expect(personRecord.john?.age).toBe("string");
    });

    test("create with deeply nested CoValues - loose validation should not throw", () => {
      const Collar = co.map({
        size: z.number(),
      });
      const Dog = co.map({
        age: z.number(),
        collar: Collar,
      });
      const PersonRecord = co.record(z.string(), Dog);

      // Should throw with strict validation when any nested field is invalid
      expectValidationError(() =>
        PersonRecord.create({
          john: {
            age: "12" as unknown as number,
            collar: { size: 10 },
          },
        }),
      );

      expectValidationError(() =>
        PersonRecord.create({
          john: {
            age: 12,
            // @ts-expect-error - size should be number
            collar: { size: "large" },
          },
        }),
      );

      // Should not throw with loose validation at any level
      expect(() =>
        PersonRecord.create(
          {
            john: {
              age: "12" as unknown as number,
              collar: { size: "large" as unknown as number },
            },
          },
          { validation: "loose" },
        ),
      ).not.toThrow();

      const personRecord = PersonRecord.create(
        {
          john: {
            age: "12" as unknown as number,
            collar: { size: "large" as unknown as number },
          },
        },
        { validation: "loose" },
      );

      // Verify all levels were created with invalid data
      expect(personRecord.john).toBeDefined();
      expect(personRecord.john?.age).toBe("12");
      expect(personRecord.john?.collar.size).toBe("large");
    });

    test("create with nested CoValue - strict validation explicitly set should throw", () => {
      const Dog = co.map({
        age: z.number(),
      });
      const PersonRecord = co.record(z.string(), Dog);

      // Explicitly setting validation to strict should throw
      expectValidationError(() =>
        PersonRecord.create(
          {
            john: { age: "12" as unknown as number },
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
      const PersonRecord = co.record(z.string(), Dog);

      // Set global validation mode to loose
      setDefaultValidationMode("loose");

      try {
        // Test 1: Create with deeply nested invalid data
        const personRecord = PersonRecord.create({
          john: {
            age: "12" as unknown as number,
            collar: { size: "large" as unknown as number },
          },
        });

        // Verify all nested levels were created with invalid data
        expect(personRecord.john).toBeDefined();
        expect(personRecord.john?.age).toBe("12");
        expect(personRecord.john?.collar.size).toBe("large");

        // Test 2: Set with nested invalid data
        personRecord.$jazz.set("john", {
          age: "15" as unknown as number,
          collar: { size: "medium" as unknown as number },
        });

        expect(personRecord.john?.age).toBe("15");
        expect(personRecord.john?.collar.size).toBe("medium");

        // Test 3: ApplyDiff with nested invalid data
        personRecord.$jazz.applyDiff({
          john: {
            age: "20" as unknown as number,
            collar: { size: "small" as unknown as number },
          },
        });

        expect(personRecord.john?.age).toBe("20");
        expect(personRecord.john?.collar.size).toBe("small");
      } finally {
        // Reset to strict mode
        setDefaultValidationMode("strict");
      }
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
      const TestRecord = co.record(z.string(), co.optional(NestedRecord));

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
  test("create a Record with a discriminated union containing a co.image", () => {
    const Base = co.map({
      type: z.literal("base"),
      name: z.string(),
    });

    const IssueRepro = co.map({
      type: z.literal("repro"),
      name: z.string(),
      image: co.image(),
    });

    const PersonRecord = co.record(
      z.string(),
      co.discriminatedUnion("type", [Base, IssueRepro]),
    );

    const person = IssueRepro.create({
      type: "repro",
      name: "John",
      image: co.image().create({
        original: FileStream.create(),
        progressive: false,
        originalSize: [1920, 1080],
      }),
    });

    const record = PersonRecord.create({
      john: person,
    });

    if (record.john?.type === "repro") {
      expect(record.john.image.originalSize).toEqual([1920, 1080]);
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

    const Catchall = co.map({}).catchall(z.string());
    const IssueRepro = co.map({
      type: z.literal("repro"),
      catchall: Catchall,
      name: z.string(),
    });

    const PersonRecord = co.record(
      z.string(),
      co.discriminatedUnion("type", [Base, IssueRepro]),
    );

    const person = IssueRepro.create({
      type: "repro",
      catchall: Catchall.create({}),
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
      co.discriminatedUnion("type", [Base, IssueRepro]),
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

describe("CoRecord unique methods", () => {
  test("loadUnique returns existing record", async () => {
    const ItemRecord = co.record(z.string(), z.number());
    const group = Group.create();

    const originalRecord = ItemRecord.create(
      { item1: 1, item2: 2, item3: 3 },
      { owner: group, unique: "test-record" },
    );

    const foundRecord = await ItemRecord.loadUnique(
      "test-record",
      group.$jazz.id,
    );
    assertLoaded(foundRecord);
    expect(foundRecord).toEqual(originalRecord);
    expect(foundRecord.item1).toBe(1);
    expect(foundRecord.item2).toBe(2);
  });

  test("loadUnique returns 'unavailable' for non-existent record", async () => {
    const ItemRecord = co.record(z.string(), z.number());
    const group = Group.create();

    const foundRecord = await ItemRecord.loadUnique(
      "non-existent",
      group.$jazz.id,
    );
    expect(foundRecord.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAVAILABLE,
    );
  });

  test("upsertUnique creates new record when none exists", async () => {
    const ItemRecord = co.record(z.string(), z.number());
    const group = Group.create();

    const sourceData = { item1: 1, item2: 2, item3: 3 };

    const result = await ItemRecord.upsertUnique({
      value: sourceData,
      unique: "new-record",
      owner: group,
    });

    assertLoaded(result);
    expect(result.item1).toBe(1);
    expect(result.item2).toBe(2);
    expect(result.item3).toBe(3);
  });

  test("upsertUnique updates existing record", async () => {
    const ItemRecord = co.record(z.string(), z.number());
    const group = Group.create();

    // Create initial record
    const originalRecord = ItemRecord.create(
      { original1: 1, original2: 2 },
      { owner: group, unique: "update-record" },
    );

    // Upsert with new data
    const updatedRecord = await ItemRecord.upsertUnique({
      value: { updated1: 10, updated2: 20, updated3: 30 },
      unique: "update-record",
      owner: group,
    });

    assertLoaded(updatedRecord);
    expect(updatedRecord).toEqual(originalRecord); // Should be the same instance
    expect(updatedRecord.updated1).toBe(10);
    expect(updatedRecord.updated2).toBe(20);
    expect(updatedRecord.updated3).toBe(30);
  });

  test("upsertUnique with CoValue items", async () => {
    const Item = co.map({
      name: z.string(),
      value: z.number(),
    });
    const ItemRecord = co.record(z.string(), Item);
    const group = Group.create();

    const items = {
      first: Item.create({ name: "First", value: 1 }, group),
      second: Item.create({ name: "Second", value: 2 }, group),
    };

    const result = await ItemRecord.upsertUnique({
      value: items,
      unique: "item-record",
      owner: group,
      resolve: { first: true, second: true },
    });

    assertLoaded(result);
    expect(result.first?.name).toBe("First");
    expect(result.second?.name).toBe("Second");
  });

  test("findUnique returns correct ID", async () => {
    const ItemRecord = co.record(z.string(), z.string());
    const group = Group.create();

    const originalRecord = ItemRecord.create(
      { test: "value" },
      { owner: group, unique: "find-test" },
    );

    const foundId = ItemRecord.findUnique("find-test", group.$jazz.id);
    expect(foundId).toBe(originalRecord.$jazz.id);
  });

  test("co.record should toJSON correctly", () => {
    const Item = co.map({ val: z.number() });
    const RecordMap = co.record(z.string(), Item);

    const record = RecordMap.create({
      key1: Item.create({ val: 1 }),
    });

    expect(record.toJSON()).toEqual(
      expect.objectContaining({
        key1: expect.objectContaining({
          val: 1,
        }),
      }),
    );
  });
});
