import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { createJazzTestAccount } from "../testing.js";
import {
  CoMapInit,
  CoMapSchema,
  CoMapSchemaClass,
  RefProps,
  ResolveQueryForCoMapInit,
  UnwrapRecordReference,
  UnwrapReference,
} from "./coMap/schema.js";
import { LazySchema } from "./coValue/lazy.js";
import { Optional } from "./coValue/optional.js";
import { flatten } from "./coValue/typeUtils.js";
import { MaybeLoaded, Unloaded } from "./coValue/types.js";
import { Loaded, co, z } from "./schema.js";

beforeEach(async () => {
  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("CoMap - with zod based schema", () => {
  describe("init", () => {
    it("should create a CoMap with basic property access", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 30 });

      expect(john.name).toBe("John");
      expect(john.age).toBe(30);
    });

    it("should disallow value mutations", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 30 });

      // @ts-expect-error - name is read-only
      expect(() => (john.name = "Jane")).toThrow();
      // @ts-expect-error - age is read-only
      expect(() => (john.age = 31)).toThrow();
    });

    it("should disallow extra properties", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });
      // @ts-expect-error - x is not a valid property
      const john = Person.create({ name: "John", age: 30, x: 1 });

      expect(john).toEqual({
        name: "John",
        age: 30,
      });
    });

    it("should generate the right CoMapInit type", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      expectTypeOf<CoMapInit<typeof Person>>().toMatchTypeOf<{
        name: string;
        age: number;
      }>();
    });

    it("should throw an error if a required field is missing", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      // @ts-expect-error - age is required
      expect(() => Person.create({ name: "John" })).toThrow(
        /^Failed to parse field age/,
      );
    });

    it("should not throw an error if a optional field is missing", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number().optional(),
      });

      const john = Person.create({ name: "John" });

      expect(john.age).toBeUndefined();
      expect(john.name).toBe("John");
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

      expect(john.name).toBe("John");
      expect(john.age).toBe(30);
      expect(john.address.street).toBe("123 Main St");
      expect(john.address.$jazz.owner).toBe(john.$jazz.owner);
    });

    it("should retrurn a loaded type when a reference is passed on create", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.optional(
          co.map({
            street: z.string(),
          }),
        ),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      const johnWithoutAddress = Person.create({
        name: "John",
        age: 30,
      });

      expectTypeOf<typeof john.address>().toMatchTypeOf<
        Loaded<typeof Person.shape.address>
      >();

      expectTypeOf<
        typeof johnWithoutAddress.address
      >().toMatchTypeOf<undefined>();
    });

    it("should be possible to reference a nested map schema to split group creation", () => {
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

      expect(john.name).toBe("John");
      expect(john.age).toBe(30);
      expect(john.address.street).toBe("123 Main St");
      expect(john.address.$jazz.owner).not.toBe(john.$jazz.owner);
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
      expect(() => Person.create({ name: "John", age: 30 })).toThrow(
        /^Field address is required/,
      );
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

      expect(john.address).toBeUndefined();
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
          friend: { name: "Bob", age: 20 },
        },
      });

      expect(john.friend.friend.name).toBe("Bob");
      expect(john.friend.name).toBe("Jane");
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

      type RQ = ResolveQueryForCoMapInit<
        typeof Person,
        {
          name: string;
          age: number;
        }
      >;

      type L = Loaded<typeof Person, RQ>;

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

      expect(john.friends.jj.name).toBe("jane");
      expect(john.friends.jj.friends.joey.name).toBe("joey");
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

      expectTypeOf<typeof john.friend.friend.friend>().toEqualTypeOf<
        MaybeLoaded<Optional<typeof Person>>
      >();
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

      expect(john.friend).toBeUndefined();
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

      expect(john.extra).toBe("extra");
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

      expect(john.extra.extra).toBe("extra");
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

      expect(friends.joe.name).toBe("joe");
    });

    // TODO: Does this make sense?
    it("should accept support a self reference as a catchall", () => {
      const Person: CoMapSchemaClass<
        {},
        { key: z.ZodString; value: LazySchema<Optional<typeof Person>> },
        false
      > = co.map({}).catchall(co.lazy(() => Person.optional()));

      const john = Person.create({
        extra: { extra: {} },
      });

      expect(john).toEqual({
        extra: { extra: {} },
      });
    });
  });

  describe("json format", () => {
    it("should properly serialize to JSON", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 30 });

      expect(JSON.stringify(john)).toMatchInlineSnapshot(
        `"{"name":"John","age":30}"`,
      );
    });

    it("should properly serialize nested values to JSON", () => {
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

      expect(JSON.stringify(john)).toMatchInlineSnapshot(
        `"{"name":"John","age":30,"address":{"street":"123 Main St"}}"`,
      );
    });

    it("should work with Object.entries", () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
      });

      const john = Person.create({ name: "John", age: 30 });

      expect(Object.entries(john)).toEqual([
        ["name", "John"],
        ["age", 30],
      ]);
    });

    it("should work on equality checks", () => {
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

      expect(john).toEqual({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });
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

      expect(john.name).toBe("John");
      expect(jane.name).toBe("Jane");
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

      expect(john.address.street).toBe("123 Main St");
      expect(johnAfterMoving.address.street).toBe("456 Main St");
      expect(johnAfterMoving.address.$jazz.owner).not.toBe(john.$jazz.owner);
      expect(john.$jazz.updated().address).toBe(johnAfterMoving.address);
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

      expect(john.address.street).toBe("123 Main St");
      expect(johnAfterMoving.address.street).toBe("456 Main St");
      expect(john.$jazz.updated().address).toBe(johnAfterMoving.address);
      expect(johnAfterMoving.address.$jazz.owner).toBe(john.$jazz.owner);
    });

    it("should update catchall properties", () => {
      const Friends = co.map({}).catchall(z.string());

      const friends = Friends.create({
        first: "John",
      });

      const friendsAfterAddingJane = friends.$jazz.set("second", "Jane");

      expect(friends).toEqual({
        first: "John",
      });

      expect(friendsAfterAddingJane.second).toBe("Jane");

      expect(friendsAfterAddingJane).toEqual({
        first: "John",
        second: "Jane",
      });
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

      expect(friends).toEqual({
        john: {
          name: "John",
        },
      });

      expect(friendsAfterAddingJane).toEqual({
        john: { name: "John" },
        jane: { name: "Jane" },
      });
    });

    it("should accept new relations when co.record is used", () => {
      const Friends = co.record(
        z.string().max(3),
        co.map({
          name: z.string(),
        }),
      );

      const friends = Friends.create({
        joe: { name: "joe" },
      });

      const friendsAfterAddingBob = friends.$jazz.set("bob", {
        name: "bob",
      });

      expect(friends.joe.name).toBe("joe");

      // @ts-expect-error - Loaded is not updated on set yet
      expect(friendsAfterAddingBob.bob.name).toBe("bob");

      expect(friendsAfterAddingBob).toEqual({
        joe: { name: "joe" },
        bob: { name: "bob" },
      });
    });

    it("should do nothing when assigning a relation on a non-valid key", () => {
      const Friends = co.record(
        z.string().max(3),
        co.map({
          name: z.string(),
        }),
      );

      const friends = Friends.create({
        joe: { name: "joe" },
      });

      const friendsAfterAddingJohnDoe = friends.$jazz.set("johndoe", {
        name: "johndoe",
      });

      expect(friendsAfterAddingJohnDoe).toBe(friends);
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

      expect(john.friend.name).toBe("Jane");
      expect(johnWithANewFriend.friend?.name).toBe("Bob");
      expect(john.$jazz.updated().friend).toBe(johnWithANewFriend.friend);

      const name = johnWithANewFriend.friend?.name;

      // TODO: It would be interesting to keep the Loaded type as non-nullable when returning updated values
      // because based on the set value we know for sure that the value is or is not undefined
      expectTypeOf<typeof name>().toMatchTypeOf<string | undefined>();
    });

    it("should return the same instance on $updated if there are no changes", () => {
      const MyCoMap = co.map({
        name: z.string(),
        age: z.number(),
      });

      const myCoMap = MyCoMap.create({ name: "John", age: 30 });

      expect(myCoMap.$jazz.updated()).toBe(myCoMap);
    });
  });
});
