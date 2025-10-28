import { assert, describe, expectTypeOf, test } from "vitest";
import { Group, co, z } from "../exports.js";
import { Account } from "../index.js";
import { CoList, CoMap, Loaded } from "../internal.js";

describe("CoList", () => {
  describe("init", () => {
    test("create a CoList with basic property access", () => {
      const StringList = co.list(z.string());

      const list = StringList.create(["a", "b", "c"]);

      type ExpectedType = ReadonlyArray<string>;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(list);
    });

    test("co.input returns the type for the init payload", () => {
      const ListSchema = co.list(
        co.map({
          name: z.string(),
          age: z.number(),
          address: co.map({
            street: z.string(),
            city: z.string(),
          }),
        }),
      );

      const init: co.input<typeof ListSchema> = [];

      ListSchema.create(init);
    });

    test("has the owner property", () => {
      const StringList = co.list(z.string());

      const list = StringList.create(["a", "b", "c"], Account.getMe());

      expectTypeOf(list.$jazz.owner).toEqualTypeOf<Group>();
    });

    test("CoList with reference", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const DogList = co.list(Dog);

      const list = DogList.create([
        Dog.create({ name: "Rex", breed: "Labrador" }),
        Dog.create({ name: "Fido", breed: "Poodle" }),
        // @ts-expect-error - undefined is not a valid argument
        undefined,
      ]);

      type ExpectedType = ReadonlyArray<Loaded<typeof Dog>>;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(list);
    });

    test("CoList with optional reference", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const DogList = co.list(Dog.optional());

      const list = DogList.create([
        Dog.create({ name: "Rex", breed: "Labrador" }),
        undefined,
      ]);

      type ExpectedType = ReadonlyArray<Loaded<typeof Dog> | undefined>;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(list);
    });

    test("CoList create with partially loaded, reference and optional", () => {
      const Breed = co.map({ type: z.literal("labrador"), value: z.string() });
      const Dog = co.map({
        name: z.string(),
        breed: Breed,
      });
      type Dog = co.loaded<typeof Dog>;

      const DogList = co.list(Dog.optional());

      const dog = Dog.create({
        name: "Rex",
        breed: Breed.create({
          type: "labrador",
          value: "Labrador",
        }),
      });

      const list = DogList.create([dog, undefined]);

      type ExpectedType = ReadonlyArray<Loaded<typeof Dog> | undefined>;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(list);
    });

    test("CoList with recursive reference", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
        get friends() {
          return DogList.optional();
        },
      });

      const DogList = co.list(Dog);

      const rex = Dog.create({
        name: "Rex",
        breed: "Labrador",
        friends: DogList.create([]),
      });

      const list = DogList.create([rex]);

      type ExpectedType = ReadonlyArray<Loaded<typeof Dog>>;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(list);
    });

    test("CoList with nested lists", () => {
      const NestedList = co.list(co.list(z.string()));

      const list = NestedList.create([
        co.list(z.string()).create(["a", "b"]),
        co.list(z.string()).create(["c", "d"]),
      ]);

      type ExpectedType = ReadonlyArray<ReadonlyArray<string>>;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(list);
    });

    test("CoList with enum type", () => {
      const EnumList = co.list(z.enum(["a", "b", "c"]));

      const list = EnumList.create(["a", "b", "c"]);

      type ExpectedType = ReadonlyArray<"a" | "b" | "c">;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(list);
    });
  });

  describe("CoList resolution", () => {
    test("loading a list with deep resolve", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const DogList = co.list(Dog);

      const list = DogList.create([
        Dog.create({ name: "Rex", breed: "Labrador" }),
        Dog.create({ name: "Fido", breed: "Poodle" }),
      ]);

      const loadedList = await DogList.load(list.$jazz.id, {
        resolve: {
          $each: true,
        },
      });

      type ExpectedType = ReadonlyArray<Loaded<typeof Dog>> | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(loadedList);

      assert(loadedList);
      const firstDog = loadedList[0];
      assert(firstDog);
      expectTypeOf(firstDog.name).toEqualTypeOf<string>();
    });

    test("loading a list with $onError", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const DogList = co.list(Dog);

      const list = DogList.create([
        Dog.create({ name: "Rex", breed: "Labrador" }),
        Dog.create({ name: "Fido", breed: "Poodle" }),
      ]);

      const loadedList = await DogList.load(list.$jazz.id, {
        resolve: {
          $each: { $onError: null },
        },
      });

      type ExpectedType = ReadonlyArray<Loaded<typeof Dog> | null> | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(loadedList);
    });

    test("loading a nested list with deep resolve", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const DogList = co.list(Dog);
      const NestedList = co.list(DogList);

      const list = NestedList.create([
        DogList.create([
          Dog.create({ name: "Rex", breed: "Labrador" }),
          Dog.create({ name: "Fido", breed: "Poodle" }),
        ]),
      ]);

      const loadedList = await NestedList.load(list.$jazz.id, {
        resolve: {
          $each: {
            $each: true,
          },
        },
      });

      type ExpectedType = ReadonlyArray<
        ReadonlyArray<Loaded<typeof Dog>>
      > | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(loadedList);

      assert(loadedList);
      const firstList = loadedList[0];
      assert(firstList);
      const firstDog = firstList[0];
      assert(firstDog);
      expectTypeOf(firstDog.name).toEqualTypeOf<string>();
    });

    test("loading a nested list with deep resolve and $onError", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        dogs: co.list(Dog),
      });

      const person = Person.create({
        name: "John",
        age: 20,
        dogs: [
          { name: "Rex", breed: "Labrador" },
          { name: "Fido", breed: "Poodle" },
        ],
      });

      const loadedPerson = await Person.load(person.$jazz.id, {
        resolve: { dogs: { $onError: null } },
      });

      type ExpectedType =
        | ({
            name: string;
            age: number;
            dogs: CoList<
              | ({
                  name: string;
                  breed: string;
                } & CoMap)
              | null
            > | null;
          } & CoMap)
        | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(loadedPerson);
    });
  });
});
