import { assert, describe, expectTypeOf, test } from "vitest";
import { Group, co, z } from "../exports.js";
import { Account } from "../index.js";
import { CoFeed, FileStream, Loaded } from "../internal.js";

describe("CoFeed", () => {
  describe("init", () => {
    test("create a CoFeed with basic property access", () => {
      const StringFeed = co.feed(z.string());

      const feed = StringFeed.create(["milk"]);

      type ExpectedType = string;

      function matches(value: ExpectedType | undefined) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().id]!.value);
    });

    test("has the _owner property", () => {
      const StringFeed = co.feed(z.string());

      const feed = StringFeed.create(["milk"], Account.getMe());

      expectTypeOf(feed._owner).toEqualTypeOf<Account | Group>();
    });

    test("CoFeed with reference", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const DogFeed = co.feed(Dog);

      const feed = DogFeed.create([
        Dog.create({ name: "Rex", breed: "Labrador" }),
      ]);

      type ExpectedType = Loaded<typeof Dog> | undefined | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().id]!.value);
    });

    test("CoFeed with optional reference", () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const DogFeed = co.feed(Dog.optional());

      const feed = DogFeed.create([
        Dog.create({ name: "Rex", breed: "Labrador" }),
      ]);

      type ExpectedType = Loaded<typeof Dog> | undefined | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().id]?.value);
    });

    test("CoFeed create with partially loaded, reference and optional", () => {
      const Dog = co.map({
        name: z.string(),
        breed: co.map({ type: z.literal("labrador"), value: z.string() }),
      });
      type Dog = co.loaded<typeof Dog>;

      const DogFeed = co.feed(Dog.optional());

      const dog = Dog.create({
        name: "Rex",
        breed: Dog.def.shape.breed.create({
          type: "labrador",
          value: "Labrador",
        }),
      }) as Dog;

      const feed = DogFeed.create([dog, undefined]);

      type ExpectedType = Loaded<typeof Dog> | undefined | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().id]!.value);
    });

    test("CoFeed with nested feeds", () => {
      const NestedFeed = co.feed(co.feed(z.string()));

      const feed = NestedFeed.create([co.feed(z.string()).create(["milk"])]);

      type ExpectedType = CoFeed<string> | undefined | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().id]?.value);
    });

    test("CoFeed with enum type", () => {
      const EnumFeed = co.feed(z.enum(["a", "b", "c"]));

      const feed = EnumFeed.create(["a"]);

      type ExpectedType = "a" | "b" | "c" | undefined | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().id]?.value);
    });
  });

  describe("CoFeed resolution", () => {
    test("loading a feed with deep resolve", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const DogFeed = co.feed(Dog);

      const feed = DogFeed.create([
        Dog.create({ name: "Rex", breed: "Labrador" }),
      ]);

      const loadedFeed = await DogFeed.load(feed.id, {
        resolve: true,
      });

      type ExpectedType = Loaded<typeof Dog> | null | undefined;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(loadedFeed?.perAccount[Account.getMe().id]?.value);

      assert(loadedFeed);
      const dog = loadedFeed.perAccount[Account.getMe().id]?.value;
      assert(dog);
      expectTypeOf(dog.name).toEqualTypeOf<string>();
    });

    test("loading a feed with $onError", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const DogFeed = co.feed(Dog);

      const feed = DogFeed.create([
        Dog.create({ name: "Rex", breed: "Labrador" }),
      ]);

      const loadedFeed = await DogFeed.load(feed.id, {
        resolve: { $each: { $onError: null } },
      });

      type ExpectedType = Loaded<typeof Dog> | null | undefined;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(loadedFeed?.perAccount[Account.getMe().id]?.value);
    });

    test("loading a nested feed with deep resolve", async () => {
      const Dog = co.map({
        name: z.string(),
        breed: z.string(),
      });

      const DogFeed = co.feed(Dog);
      const NestedFeed = co.feed(DogFeed);

      const feed = NestedFeed.create([
        DogFeed.create([Dog.create({ name: "Rex", breed: "Labrador" })]),
      ]);

      const loadedFeed = await NestedFeed.load(feed.id, {
        resolve: {
          $each: true,
        },
      });

      type ExpectedType = Loaded<typeof Dog> | null | undefined;

      function matches(value: ExpectedType) {
        return value;
      }

      const nestedFeed = loadedFeed?.perAccount[Account.getMe().id]?.value;
      assert(nestedFeed);
      matches(nestedFeed.perAccount[Account.getMe().id]?.value);

      assert(loadedFeed);
      const dog = nestedFeed.perAccount[Account.getMe().id]?.value;
      assert(dog);
      expectTypeOf(dog.name).toEqualTypeOf<string>();
    });
  });
});

describe("co.fileStream", () => {
  test("create function type", () => {
    const FileStreamFeed = co.fileStream();

    const feed = FileStreamFeed.create({ owner: Account.getMe() });

    type ExpectedType = FileStream;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(feed);
  });

  test("createFromBlob function type", async () => {
    const FileStreamFeed = co.fileStream();
    const blob = new Blob(["test"], { type: "text/plain" });

    const feed = await FileStreamFeed.createFromBlob(blob, {
      owner: Account.getMe(),
      onProgress: (progress: number) => {
        console.log(`Progress: ${progress}`);
      },
    });

    type ExpectedType = FileStream;

    function matches(value: ExpectedType) {
      return value;
    }

    matches(feed);
  });
});
