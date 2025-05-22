import { assert, describe, expectTypeOf, test } from "vitest";
import { Group, co, z } from "../exports.js";
import { Account } from "../index.js";
import { Loaded } from "../internal.js";

describe("CoList", () => {
  describe("init", () => {
    test("create a CoList with basic property access", () => {
      const StringList = co.list(z.string());

      const list = StringList.create(["a", "b", "c"]);

      type ExpectedType = string[];

      function matches(value: ExpectedType) {
        return value;
      }

      matches(list);
    });

    test("has the _owner property", () => {
      const StringList = co.list(z.string());

      const list = StringList.create(["a", "b", "c"], Account.getMe());

      expectTypeOf(list._owner).toEqualTypeOf<Account | Group>();
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

      type ExpectedType = Loaded<typeof Dog>[];

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

      type ExpectedType = (Loaded<typeof Dog> | undefined)[];

      function matches(value: ExpectedType) {
        return value;
      }

      matches(list);
    });

    test("CoList create with partially loaded, reference and optional", () => {
      const Dog = co.map({
        name: z.string(),
        breed: co.map({ type: z.literal("labrador"), value: z.string() }),
      });
      type Dog = co.loaded<typeof Dog>;

      const DogList = co.list(Dog.optional());

      const dog = Dog.create({
        name: "Rex",
        breed: Dog.def.shape.breed.create({
          type: "labrador",
          value: "Labrador",
        }),
      }) as Dog;

      const list = DogList.create([dog, undefined]);

      type ExpectedType = (Loaded<typeof Dog> | undefined)[];

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

      type ExpectedType = string[][];

      function matches(value: ExpectedType) {
        return value;
      }

      matches(list);
    });

    test("CoList with enum type", () => {
      const EnumList = co.list(z.enum(["a", "b", "c"]));

      const list = EnumList.create(["a", "b", "c"]);

      type ExpectedType = ("a" | "b" | "c")[];

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

      const loadedList = await DogList.load(list.id, {
        resolve: {
          $each: true,
        },
      });

      type ExpectedType = Loaded<typeof Dog>[] | null;

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

      const loadedList = await DogList.load(list.id, {
        resolve: {
          $each: { $onError: null },
        },
      });

      type ExpectedType =
        | (
            | (Loaded<typeof Dog> & {
                $onError: never;
              })
            | null
          )[]
        | null;

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

      const loadedList = await NestedList.load(list.id, {
        resolve: {
          $each: {
            $each: true,
          },
        },
      });

      type ExpectedType = Loaded<typeof Dog>[][] | null;

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
  });
});
