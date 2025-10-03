import { describe, expectTypeOf, test } from "vitest";
import { Group, co, z } from "../exports.js";
import { Account } from "../index.js";
import { Loaded, MaybeLoaded, LoadedAndRequired } from "../internal.js";
import { assertLoaded } from "./utils.js";

describe("CoMap.Record", () => {
  describe("init", () => {
    test("create a Record with basic property access", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({
        name: "John",
        age: "20",
      });

      type ExpectedType = {
        [key: string]: string;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(person);
    });

    test("co.input returns the type for the init payload", () => {
      const Person = co.record(
        z.string(),
        co.map({
          name: z.string(),
          age: z.number(),
          address: co.map({
            street: z.string(),
            city: z.string(),
          }),
        }),
      );

      const init: co.input<typeof Person> = {};

      Person.create(init);
    });

    test("has the owner property", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" }, Account.getMe());

      expectTypeOf(person.$jazz.owner).toEqualTypeOf<Group>();
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

      type ExpectedType = {
        [key: string]: Loaded<typeof Dog>;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(person);
    });

    test("Record with optional reference", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.record(z.string(), Dog.optional());

      const person = Person.create({
        pet1: Dog.create({ name: "Rex", breed: "Labrador" }),
        pet2: undefined,
      });

      type ExpectedType = {
        [key: string]: Loaded<typeof Dog> | undefined;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(person);
    });

    test("Record create with partially loaded, reference and optional", () => {
      const Breed = co.map({ type: z.literal("labrador"), value: z.string() });
      const Dog = co.map({
        name: z.string(),
        breed: Breed,
      });
      type Dog = co.loaded<typeof Dog>;

      const DogRecord = co.record(z.string(), Dog.optional());

      const dog = Dog.create({
        name: "Rex",
        breed: Breed.create({
          type: "labrador",
          value: "Labrador",
        }),
      });

      const record = DogRecord.create({
        pet1: dog,
        pet2: undefined,
      });

      type ExpectedType = {
        [key: string]: Loaded<typeof Dog> | undefined;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(record);
    });

    test("Record with recursive reference", () => {
      const Dog = co.map({
        name: z.string(),
        get owner() {
          return Person.optional();
        },
      });

      const Person = co.record(z.string(), Dog);

      const person = Person.create({
        pet1: Dog.create({ name: "Rex" }),
      });

      person.pet1!.$jazz.set("owner", person);

      type ExpectedType = {
        [key: string]: Loaded<typeof Dog> | undefined;
      };

      function matches(value: ExpectedType) {
        return value;
      }

      matches(person);
    });
  });

  describe("Record resolution", () => {
    test("loading a record with deep resolve", async () => {
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

      type ExpectedType = MaybeLoaded<{
        [key: string]: Loaded<typeof Dog>;
      }>;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(loadedPerson);
    });

    test("loading a record with property resolve", async () => {
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

      type Expect = LoadedAndRequired<typeof loadedPerson> extends never
        ? "error: is never"
        : "ok";

      expectTypeOf("ok" as const).toEqualTypeOf<Expect>();

      assertLoaded(loadedPerson);
      expectTypeOf(loadedPerson.pet1).toEqualTypeOf<Loaded<typeof Dog>>();
      expectTypeOf(loadedPerson.pet3).branded.toEqualTypeOf<
        MaybeLoaded<Loaded<typeof Dog>> | undefined
      >();
    });

    test("loading a record with generic string resolve", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.record(z.string(), Dog);

      const person = Person.create({
        pet1: Dog.create({ name: "Rex", breed: "Labrador" }),
        pet2: Dog.create({ name: "Fido", breed: "Poodle" }),
      });

      const userId: string = "pet1";
      const userId2: string = "pet3";

      const loadedPerson = await Person.load(person.$jazz.id, {
        resolve: {
          [userId]: true,
          pet2: true,
          [userId2]: {
            $onError: null,
          },
        },
      });

      type Expect = LoadedAndRequired<typeof loadedPerson> extends never
        ? "error: is never"
        : "ok";

      expectTypeOf("ok" as const).toEqualTypeOf<Expect>();

      assertLoaded(loadedPerson);
      expectTypeOf(loadedPerson.pet1).branded.toEqualTypeOf<
        MaybeLoaded<Loaded<typeof Dog>> | undefined
      >();
      expectTypeOf(loadedPerson.pet2).branded.toEqualTypeOf<
        MaybeLoaded<Loaded<typeof Dog>> | undefined
      >();
      expectTypeOf(loadedPerson.pet3).branded.toEqualTypeOf<
        MaybeLoaded<Loaded<typeof Dog>> | undefined
      >();
    });

    test("loading a record with empty resolve", async () => {
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

      type Expect = LoadedAndRequired<typeof loadedPerson> extends never
        ? "error: is never"
        : "ok";

      expectTypeOf("ok" as const).toEqualTypeOf<Expect>();

      assertLoaded(loadedPerson);
      expectTypeOf(loadedPerson.pet1).branded.toEqualTypeOf<
        MaybeLoaded<Loaded<typeof Dog>> | undefined
      >();
      expectTypeOf(loadedPerson.pet3).branded.toEqualTypeOf<
        MaybeLoaded<Loaded<typeof Dog>> | undefined
      >();
    });

    test("loading a record with $onError", async () => {
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
          $each: { $onError: null },
        },
      });

      type ExpectedType = MaybeLoaded<{
        [key: string]: Loaded<typeof Dog> | null;
      }>;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(loadedPerson);
    });
  });
});
