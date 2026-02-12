import { assert, describe, expectTypeOf, test } from "vitest";
import { Group, co, z } from "../exports.js";
import { Account } from "../index.js";
import { CoFeed, FileStream, Loaded, MaybeLoaded } from "../internal.js";
import { assertLoaded } from "./utils.js";

describe("CoFeed", () => {
  describe("init", () => {
    test("create a CoFeed with basic property access", () => {
      const StringFeed = co.feed(z.string());

      const feed = StringFeed.create(["milk"]);

      type ExpectedType = string;

      function matches(value: ExpectedType | undefined) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().$jazz.id]!.value);
    });

    test("has the owner property", () => {
      const StringFeed = co.feed(z.string());

      const feed = StringFeed.create(["milk"], Account.getMe());

      expectTypeOf(feed.$jazz.owner).toEqualTypeOf<Group>();
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

      type ExpectedType = MaybeLoaded<Loaded<typeof Dog>> | undefined;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().$jazz.id]!.value);
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

      type ExpectedType = MaybeLoaded<Loaded<typeof Dog>> | undefined;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().$jazz.id]?.value);
    });

    test("CoFeed create with partially loaded, reference and optional", () => {
      const Breed = co.map({ type: z.literal("labrador"), value: z.string() });
      const Dog = co.map({
        name: z.string(),
        breed: Breed,
      });
      type Dog = co.loaded<typeof Dog>;

      const DogFeed = co.feed(Dog.optional());

      const dog = Dog.create({
        name: "Rex",
        breed: Breed.create({
          type: "labrador",
          value: "Labrador",
        }),
      });

      const feed = DogFeed.create([dog, undefined]);

      type ExpectedType = MaybeLoaded<Loaded<typeof Dog>> | undefined;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().$jazz.id]!.value);
    });

    test("CoFeed with nested feeds", () => {
      const NestedFeed = co.feed(co.feed(z.string()));

      const feed = NestedFeed.create([co.feed(z.string()).create(["milk"])]);

      type ExpectedType = MaybeLoaded<CoFeed<string>> | undefined;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().$jazz.id]?.value);
    });

    test("CoFeed with enum type", () => {
      const EnumFeed = co.feed(z.enum(["a", "b", "c"]));

      const feed = EnumFeed.create(["a"]);

      type ExpectedType = "a" | "b" | "c" | undefined | null;

      function matches(value: ExpectedType) {
        return value;
      }

      matches(feed.perAccount[Account.getMe().$jazz.id]?.value);
    });

    test("create options", () => {
      const StringFeed = co.feed(z.string());

      StringFeed.create(["a"]);
      StringFeed.create(["a"], Group.create());
      StringFeed.create(["a"], {});
      StringFeed.create(["a"], { owner: Group.create() });
      StringFeed.create(["a"], { owner: Group.create(), unique: "test" });

      // @ts-expect-error - owner is required if unique is provided
      StringFeed.create(["a"], { unique: "test" });

      // this is deprecated but valid
      StringFeed.create(["a"], Account.getMe());
      StringFeed.create(["a"], { owner: Account.getMe(), unique: "test" });

      StringFeed.create(["a"], { validation: "loose" });
      StringFeed.create(["a"], { owner: Group.create(), validation: "loose" });
      // @ts-expect-error - owner is required if unique is provided
      StringFeed.create(["a"], { unique: "test", validation: "loose" });
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

      const loadedFeed = await DogFeed.load(feed.$jazz.id, {
        resolve: true,
      });

      type ExpectedType = MaybeLoaded<Loaded<typeof Dog>> | undefined;

      function matches(value: ExpectedType) {
        return value;
      }

      assertLoaded(loadedFeed);
      matches(loadedFeed?.perAccount[Account.getMe().$jazz.id]?.value);

      assert(loadedFeed);
      const dog = loadedFeed.perAccount[Account.getMe().$jazz.id]?.value;
      assert(dog);
      assertLoaded(dog);
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

      const loadedFeed = await DogFeed.load(feed.$jazz.id, {
        resolve: { $each: { $onError: "catch" } },
      });

      type ExpectedType = MaybeLoaded<Loaded<typeof Dog>> | undefined;

      function matches(value: ExpectedType) {
        return value;
      }

      assertLoaded(loadedFeed);
      matches(loadedFeed.perAccount[Account.getMe().$jazz.id]?.value);
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

      const loadedFeed = await NestedFeed.load(feed.$jazz.id, {
        resolve: {
          $each: true,
        },
      });

      type ExpectedType = MaybeLoaded<Loaded<typeof Dog>> | undefined;

      function matches(value: ExpectedType) {
        return value;
      }

      assertLoaded(loadedFeed);
      const nestedFeed =
        loadedFeed?.perAccount[Account.getMe().$jazz.id]?.value;
      assert(nestedFeed);
      assertLoaded(nestedFeed);
      matches(nestedFeed.perAccount[Account.getMe().$jazz.id]?.value);

      assert(loadedFeed);
      const dog = nestedFeed.perAccount[Account.getMe().$jazz.id]?.value;
      assert(dog);
      assertLoaded(dog);
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
