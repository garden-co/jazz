import { describe, expectTypeOf, test } from "vitest";
import { Group, co, z } from "../exports.js";
import { Account } from "../index.js";
import { Loaded } from "../internal.js";

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

    test("has the _owner property", () => {
      const Person = co.record(z.string(), z.string());

      const person = Person.create({ name: "John" }, Account.getMe());

      expectTypeOf(person._owner).toEqualTypeOf<Account | Group>();
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
      const Dog = co.map({
        name: z.string(),
        breed: co.map({ type: z.literal("labrador"), value: z.string() }),
      });
      type Dog = co.loaded<typeof Dog>;

      const DogRecord = co.record(z.string(), Dog.optional());

      const dog = Dog.create({
        name: "Rex",
        breed: Dog.def.shape.breed.create({
          type: "labrador",
          value: "Labrador",
        }),
      }) as Dog;

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

      const loadedPerson = await Person.load(person.id, {
        resolve: {
          $each: true,
        },
      });

      type ExpectedType = {
        [key: string]: Loaded<typeof Dog>;
      } | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(loadedPerson);
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

      const loadedPerson = await Person.load(person.id, {
        resolve: {
          $each: { $onError: null },
        },
      });

      type ExpectedType = {
        [key: string]:
          | (Loaded<typeof Dog> & {
              $onError: never;
            })
          | null;
      } | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(loadedPerson);
    });
  });
});
