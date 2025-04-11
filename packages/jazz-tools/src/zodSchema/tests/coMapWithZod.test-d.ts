import { assert, describe, expectTypeOf, it } from "vitest";
import { createJazzTestAccount } from "../../testing.js";
import { CoMapSchemaClass } from "../coMap/schema.js";
import { LazySchema } from "../coValue/lazy.js";
import { Optional } from "../coValue/optional.js";
import { MaybeLoaded, Unloaded } from "../coValue/types.js";
import { Loaded, co, z } from "../schema.js";
import { loadCoValue, subscribeToCoValue } from "../subscribe.js";

describe("CoMap - test types", () => {
  describe("init", () => {
    it("should create a CoMap with basic property access", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 30 });

      expectTypeOf<typeof john.name>().toEqualTypeOf<string>();
      expectTypeOf<typeof john.age>().toEqualTypeOf<number>();

      expectTypeOf<typeof john>().toEqualTypeOf<Loaded<typeof Person>>();
    });

    it("should disallow value mutations", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 30 });

      // @ts-expect-error - name is read-only
      john.name = "Jane";
      // @ts-expect-error - age is read-only
      john.age = 31;
    });

    it("should disallow extra properties", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      // @ts-expect-error - x is not a valid property
      Person.create({ name: "John", age: 30, x: 1 });
    });

    it("should throw an error if a required field is missing", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      // @ts-expect-error - age is required
      Person.create({ name: "John" });
    });

    it("should not throw an error if a optional field is missing", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      const john = Person.create({ name: "John" });

      expectTypeOf<typeof john.age>().toEqualTypeOf<number | undefined>();
      expectTypeOf<typeof john.name>().toEqualTypeOf<string>();
    });

    it("should create a CoMap with nested values", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      expectTypeOf<typeof john.address.street>().toEqualTypeOf<string>();
      expectTypeOf<typeof john.address>().toEqualTypeOf<
        Loaded<typeof Person.shape.address>
      >();
      expectTypeOf<typeof john>().toEqualTypeOf<
        Loaded<typeof Person, { address: true }>
      >();
    });

    it("should be compatible with the loaded type", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      function isLoaded(value: Loaded<typeof Person, { address: true }>) {
        return value;
      }

      isLoaded(john);
    });

    it("should create a CoMap with child values using the create method", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: Person.shape.address.create({ street: "123 Main St" }),
      });

      expectTypeOf<typeof john.address.street>().toEqualTypeOf<string>();
      expectTypeOf<typeof john.address>().toEqualTypeOf<
        Loaded<typeof Person.shape.address>
      >();
      expectTypeOf<typeof john>().toEqualTypeOf<
        Loaded<typeof Person, { address: true }>
      >();
    });

    it("should throw an error if a required ref is missing", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      // @ts-expect-error - address is required
      Person.create({ name: "John", age: 30 });
    });

    it("should not throw an error if a required ref is missing", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co
          .map({
            street: z.string(),
          })
          .optional(),
      });

      const john = Person.create({ name: "John", age: 30 });

      expectTypeOf<typeof john.address>().toEqualTypeOf<undefined>();
    });

    it("should create a CoMap with self references", () => {
      const personBaseProps = {
        name: z.string(),
        age: z.number(),
      };
      const Person: CoMapSchemaClass<
        typeof personBaseProps & {
          friend: LazySchema<Optional<typeof Person>>;
        },
        undefined,
        false
      > = co.map({
        ...personBaseProps,
        friend: co.lazy(() => Person.optional()),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        friend: {
          name: "Jane",
          age: 20,
          friend: Person.create({ name: "Bob", age: 20 }),
        },
      });

      expectTypeOf<typeof john.friend.friend.name>().toEqualTypeOf<string>();
      expectTypeOf<typeof john.friend.name>().toEqualTypeOf<string>();
      expectTypeOf<
        typeof john.friend.friend.friend
      >().toEqualTypeOf<undefined>();

      function isValidLoaded(value: Loaded<typeof Person, { friend: true }>) {
        return value;
      }

      isValidLoaded(john.friend);
    });

    it("should create mutually recursive CoMaps", () => {
      const personBaseProps = {
        name: z.string(),
        age: z.number(),
      };

      const Person: CoMapSchemaClass<
        typeof personBaseProps & {
          friends: LazySchema<Optional<typeof PeopleByNickname>>;
        },
        undefined,
        false
      > = co.map({
        ...personBaseProps,
        friends: co.lazy(() => PeopleByNickname.optional()),
      });

      const PeopleByNickname = co.record(z.string(), Person);

      const joey = Person.create({
        name: "joey",
        age: 20,
      });

      const john = Person.create({
        name: "John",
        age: 30,
        friends: {
          joey: joey,
          jj: { name: "jane", age: 20, friends: { joey: joey } },
        },
      });

      expectTypeOf<typeof john.friends.jj.name>().toEqualTypeOf<string>();
      expectTypeOf<
        typeof john.friends.jj.friends.joey.name
      >().toEqualTypeOf<string>();

      function isValidLoaded(
        value: Loaded<
          typeof Person,
          {
            friends: {
              joey: true;
            };
          }
        >,
      ) {
        return value;
      }

      isValidLoaded(john.friends.jj);
    });

    it("should return a loaded type when a self reference is passed on create", () => {
      const personBaseProps = {
        name: z.string(),
        age: z.number(),
      };

      const Person: CoMapSchemaClass<
        typeof personBaseProps & {
          friend: LazySchema<Optional<typeof Person>>;
        },
        undefined,
        false
      > = co.map({
        ...personBaseProps,
        friend: co.lazy(() => Person.optional()),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        friend: {
          name: "Jane",
          age: 20,
          friend: { name: "Bob", age: 20 },
        },
      });

      expectTypeOf<typeof john.friend.friend.name>().toEqualTypeOf<string>();
      expectTypeOf<
        typeof john.friend.friend.friend
      >().toEqualTypeOf<undefined>();

      function isValidLoaded(value: Loaded<typeof Person, { friend: true }>) {
        return value;
      }

      isValidLoaded(john.friend);
    });

    it("should not throw an error if an optional self reference is missing", () => {
      const personBaseProps = {
        name: z.string(),
        age: z.number(),
      };

      const Person: CoMapSchemaClass<
        typeof personBaseProps & {
          friend: LazySchema<Optional<typeof Person>>;
        },
        undefined,
        false
      > = co.map({
        ...personBaseProps,
        friend: co.lazy(() => Person.optional()),
      });

      const john = Person.create({
        name: "John",
        age: 30,
      });

      expectTypeOf<typeof john.friend>().toEqualTypeOf<undefined>();

      function isValidLoaded(value: Loaded<typeof Person>) {
        return value;
      }

      isValidLoaded(john);
    });

    it("should accept extra properties when catchall is used", () => {
      const Person = co
        .map({
          name: z.string(),
          age: z.number(),
        })
        .catchall(z.union([z.string(), z.number()]));

      const john = Person.create({
        name: "John",
        age: 30,
        extra: "extra",
      });

      // TODO: This is not correct, it should be string | undefined
      expectTypeOf<typeof john.extra>().toEqualTypeOf<
        string | number | undefined
      >();

      type LoadedPerson = Loaded<typeof Person, true>;

      function isValidLoaded(value: LoadedPerson) {
        return value;
      }

      isValidLoaded(john);
    });

    it("should accept extra relations when catchall is used", () => {
      const Person = co.map({}).catchall(
        co.map({
          extra: z.string(),
        }),
      );

      const john = Person.create({
        extra: { extra: "extra" },
      });

      expectTypeOf<typeof john.extra.extra>().toEqualTypeOf<string>();

      type LoadedPerson = Loaded<
        typeof Person,
        {
          extra: true;
        }
      >;

      function isValidLoaded(value: LoadedPerson) {
        return value;
      }

      isValidLoaded(john);
    });

    it("should accept relations when co.record is used", () => {
      const Friends = co.record(
        z.string().max(3),
        co.map({
          name: z.string(),
        }),
      );

      const friends = Friends.create({
        joe: { name: "joe" },
      });

      expectTypeOf<typeof friends.joe.name>().toEqualTypeOf<string>();

      type LoadedFriends = Loaded<typeof Friends, { joe: true }>;

      function isValidLoaded(value: LoadedFriends) {
        return value;
      }

      isValidLoaded(friends);
    });
  });

  describe("updates", () => {
    it("should not change original property after calling $jazz.set", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 30 });

      const jane = john.$jazz.set("name", "Jane");

      expectTypeOf(jane).toEqualTypeOf<Loaded<typeof Person>>();
    });

    it("should update nested values", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      const johnAfterMoving = john.$jazz.set(
        "address",
        Person.shape.address.create({
          street: "456 Main St",
        }),
      );

      expectTypeOf<
        typeof johnAfterMoving.address.street
      >().toEqualTypeOf<string>();
      expectTypeOf<typeof johnAfterMoving>().toEqualTypeOf<
        Loaded<typeof Person, { address: true }>
      >();
    });

    it("should update nested values with JSON data", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      const johnAfterMoving = john.$jazz.set("address", {
        street: "456 Main St",
      });

      expectTypeOf<
        typeof johnAfterMoving.address.street
      >().toEqualTypeOf<string>();
      expectTypeOf<typeof johnAfterMoving.address.$jazz.owner>().toEqualTypeOf<
        typeof john.$jazz.owner
      >();
      expectTypeOf<typeof johnAfterMoving>().toEqualTypeOf<
        Loaded<typeof Person, { address: true }>
      >();
    });

    it("should update catchall properties", () => {
      const Friends = co.map({}).catchall(z.string());

      const friends = Friends.create({
        first: "John",
      });

      const friendsAfterAddingJane = friends.$jazz.set("second", "Jane");

      expectTypeOf<typeof friends.first>().toEqualTypeOf<string | undefined>();

      expectTypeOf<typeof friendsAfterAddingJane.second>().toEqualTypeOf<
        string | undefined
      >();
    });

    it("should update nested values with catchall relations", () => {
      const Friends = co.map({}).catchall(
        co.map({
          name: z.string(),
        }),
      );

      const friends = Friends.create({
        john: {
          name: "John",
        },
      });

      const friendsAfterAddingJane = friends.$jazz.set("jane", {
        name: "Jane",
      });

      expectTypeOf<typeof friends.john.name>().toEqualTypeOf<string>();

      expectTypeOf<typeof friends.jane>().toEqualTypeOf<
        undefined | MaybeLoaded<typeof Friends.record.value>
      >();

      expectTypeOf<typeof friendsAfterAddingJane.jane>().toEqualTypeOf<
        undefined | MaybeLoaded<typeof Friends.record.value>
      >();
    });

    it("should update nested values with self references", () => {
      const personBaseProps = {
        name: z.string(),
        age: z.number(),
      };

      const Person: CoMapSchemaClass<
        typeof personBaseProps & {
          friend: LazySchema<Optional<typeof Person>>;
        },
        undefined,
        false
      > = co.map({
        ...personBaseProps,
        friend: co.lazy(() => Person.optional()),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        friend: {
          name: "Jane",
          age: 20,
        },
      });

      const johnWithANewFriend = john.$jazz.set("friend", {
        name: "Bob",
        age: 20,
      });

      expectTypeOf<typeof john.friend.name>().toEqualTypeOf<string>();

      const name = johnWithANewFriend.friend?.name;

      // TODO: It would be interesting to keep the Loaded type as non-nullable when returning updated values
      // because based on the set value we know for sure that the value is or is not undefined
      expectTypeOf<typeof name>().toEqualTypeOf<string | undefined>();

      type LoadedPerson = Loaded<typeof Person, { friend: true }>;

      function isValidLoaded(value: LoadedPerson) {
        return value;
      }

      isValidLoaded(johnWithANewFriend);
      isValidLoaded(john);
    });
  });

  describe("record methods", () => {
    it("should return correct values with $jazz.values()", () => {
      const Friends = co.record(
        z.string(),
        co.map({
          name: z.string(),
        }),
      );

      const friends = Friends.create({
        joe: { name: "joe" },
        bob: { name: "bob" },
      });

      const values = friends.$jazz.values();

      function isValid(
        values: Array<MaybeLoaded<typeof Friends.record.value> | undefined>,
      ) {
        return values;
      }

      isValid(values);
    });

    it("should return correct entries with $jazz.entries()", () => {
      const Friends = co.record(
        z.string(),
        co.map({
          name: z.string(),
        }),
      );

      const friends = Friends.create({
        joe: { name: "joe" },
        bob: { name: "bob" },
      });

      const entries = friends.$jazz.entries();

      function isValid(
        entries: [
          string,
          MaybeLoaded<typeof Friends.record.value> | undefined,
        ][],
      ) {
        return entries;
      }

      isValid(entries);
    });

    it("should return correct keys with $jazz.keys()", () => {
      const Friends = co.record(
        z.string().max(3),
        co.map({
          name: z.string(),
        }),
      );

      const friends = Friends.create({
        joe: { name: "joe" },
        bob: { name: "bob" },
      });

      const keys = friends.$jazz.keys();
      expectTypeOf(keys).toEqualTypeOf<Array<string>>();
    });
  });

  describe("load", () => {
    it("should return valid Loaded type from the load function", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: {
          address: true,
        },
      });

      function isValidLoaded(
        value: MaybeLoaded<typeof Person, { address: true }>,
      ) {
        return value;
      }

      isValidLoaded(loaded);
    });

    it("should disallow extra properties in the resolve", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: {
          // @ts-expect-error - extra is not a valid relation
          extra: true,
        },
      });
    });

    it("should return valid Loaded type from the load function (catchall)", async () => {
      const Person = co.map({}).catchall(z.string());

      const john = Person.create({
        first: "John",
      });

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: true,
      });

      function isValidLoaded(value: MaybeLoaded<typeof Person>) {
        return value;
      }

      isValidLoaded(loaded);

      assert(loaded.$jazzState === "loaded");

      expectTypeOf(loaded).toEqualTypeOf<Loaded<typeof Person>>();
    });

    it("should disallow resolving extra properties in the load function (catchall)", async () => {
      const Person = co.map({}).catchall(z.string());

      const john = Person.create({
        first: "John",
      });

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        // @ts-expect-error - extra is not a valid relation
        resolve: {
          extra: true,
        },
      });
    });

    it("should load a CoMap with a catchall relation", async () => {
      const Person = co.map({}).catchall(
        co.map({
          prop: z.string(),
        }),
      );

      const john = Person.create({
        first: {
          prop: "prop1",
        },
      });

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: true,
      });

      function isValidLoaded(value: MaybeLoaded<typeof Person>) {
        return value;
      }

      isValidLoaded(loaded);

      assert(loaded.$jazzState === "loaded");

      expectTypeOf(loaded.first).toEqualTypeOf<
        MaybeLoaded<typeof Person.record.value> | undefined
      >();
    });

    it("should load all the relations on co.record when using $each", async () => {
      const Friends = co.record(
        z.string(),
        co.map({
          name: z.string(),
          address: co.map({
            street: z.string(),
          }),
        }),
      );

      const friends = Friends.create({
        joe: {
          name: "joe",
          address: {
            street: "123 Main St",
          },
        },
        bob: {
          name: "bob",
          address: {
            street: "456 Main St",
          },
        },
      });

      const loaded = await loadCoValue(Friends, friends.$jazz.id, {
        resolve: { $each: true },
      });

      function isValidLoaded(
        value: MaybeLoaded<typeof Friends, { $each: true }>,
      ) {
        return value;
      }

      isValidLoaded(loaded);

      assert(loaded.$jazzState === "loaded");

      expectTypeOf(loaded.joe).toEqualTypeOf<
        Loaded<typeof Friends.record.value, true> | undefined
      >();

      expectTypeOf(loaded.joe?.address).toEqualTypeOf<
        MaybeLoaded<typeof Friends.record.value.shape.address> | undefined
      >();

      const values = loaded.$jazz.values();

      function isValid(values: Loaded<typeof Friends.record.value, true>[]) {
        return values;
      }

      isValid(values);
    });

    it("should deeply load all the relations on co.record when using $each", async () => {
      const Friends = co.record(
        z.string(),
        co.map({
          name: z.string(),
          address: co.map({
            street: z.string(),
          }),
        }),
      );

      const friends = Friends.create({
        joe: {
          name: "joe",
          address: {
            street: "123 Main St",
          },
        },
        bob: {
          name: "bob",
          address: {
            street: "456 Main St",
          },
        },
      });

      const loaded = await loadCoValue(Friends, friends.$jazz.id, {
        resolve: { $each: { address: true } },
      });

      function isValidLoaded(
        value: MaybeLoaded<typeof Friends, { $each: { address: true } }>,
      ) {
        return value;
      }

      isValidLoaded(loaded);

      assert(loaded.$jazzState === "loaded");

      expectTypeOf(loaded.joe).toEqualTypeOf<
        Loaded<typeof Friends.record.value, { address: true }> | undefined
      >();

      const values = loaded.$jazz.values();

      function isValid(
        values: Loaded<typeof Friends.record.value, { address: true }>[],
      ) {
        return values;
      }

      isValid(values);
    });

    it("should load a CoMap with self references", async () => {
      const personBaseProps = {
        name: z.string(),
        age: z.number(),
      };

      const Person: CoMapSchemaClass<
        typeof personBaseProps & {
          friend: LazySchema<Optional<typeof Person>>;
        },
        undefined,
        false
      > = co.map({
        ...personBaseProps,
        friend: co.lazy(() => Person.optional()),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        friend: {
          name: "Jane",
          age: 20,
        },
      });

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { friend: true },
      });

      function isValidLoaded(
        value: MaybeLoaded<typeof Person, { friend: true }>,
      ) {
        return value;
      }

      isValidLoaded(loaded);

      assert(loaded.$jazzState === "loaded");

      expectTypeOf(loaded.friend).toEqualTypeOf<
        Loaded<typeof Person> | undefined
      >();
    });
  });

  describe("ensureLoaded", () => {
    it("should load the nested values", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      const result = await john.$jazz.ensureLoaded({
        resolve: { address: true },
      });

      expectTypeOf(result).toEqualTypeOf<
        Loaded<typeof Person, { address: true }>
      >();
    });
  });

  describe("subscribe", () => {
    it("should invoke the callback with the loaded value", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      subscribeToCoValue(Person, john.$jazz.id, { resolve: true }, (value) => {
        expectTypeOf(value).toEqualTypeOf<
          Loaded<typeof Person, true> | Unloaded<typeof Person>
        >();
      });
    });
  });

  describe("Loaded", () => {
    it("deep loaded maps should be complatible non-deep maps values", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const deepLoaded = {} as Loaded<typeof Person, { address: true }>;

      function isValid(value: Loaded<typeof Person>) {
        return value;
      }

      isValid(deepLoaded);
    });

    it("$each loaded records should be complatible non-deep loaded values", async () => {
      const People = co.record(
        z.string(),
        co.map({
          name: z.string(),
          age: z.number(),
          address: co.map({
            street: z.string(),
          }),
        }),
      );

      const deepLoaded = {} as Loaded<typeof People, { $each: true }>;

      function isValid(value: Loaded<typeof People>) {
        return value;
      }

      isValid(deepLoaded);
    });

    it("deep loaded records should be complatible non-deep loaded values", async () => {
      const People = co.record(
        z.string(),
        co.map({
          name: z.string(),
          age: z.number(),
          address: co.map({
            street: z.string(),
          }),
        }),
      );

      const deepLoaded = {} as Loaded<typeof People, { joe: true }>;

      function isValid(value: Loaded<typeof People>) {
        return value;
      }

      isValid(deepLoaded);
    });
  });
});
