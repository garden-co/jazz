import { cojsonInternals } from "cojson";
import { assert, beforeEach, describe, expect, it, vi } from "vitest";
import { ZodError } from "zod";
import { Group } from "../../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../../testing.js";
import { waitFor } from "../../tests/utils.js";
import { CoMapSchemaClass } from "../coMap/schema.js";
import { LazySchema } from "../coValue/lazy.js";
import { Optional } from "../coValue/optional.js";
import { Loaded, co, z } from "../schema.js";
import { loadCoValue, subscribeToCoValue } from "../subscribe.js";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

beforeEach(() => {
  cojsonInternals.CO_VALUE_LOADING_CONFIG.MAX_RETRIES = 1;
  cojsonInternals.CO_VALUE_LOADING_CONFIG.TIMEOUT = 1;
});

describe("CoMap with Zod", () => {
  describe("load", () => {
    it("should load a CoMap without nested values", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address: { street: "123 Main St" },
        },
        group,
      );

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: true,
      });

      assert(loaded.$jazzState === "loaded");

      expect(loaded.name).toBe("John");
      expect(loaded.age).toBe(30);
      expect(loaded.address).toMatchObject({
        $jazzState: "unloaded",
        $jazz: {
          schema: Person.shape.address,
          id: john.address.$jazz.id,
        },
      });
    });

    it("should load a CoMap with nested values", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address: { street: "123 Main St" },
        },
        group,
      );

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { address: true },
      });

      assert(loaded.$jazzState === "loaded");

      expect(loaded.name).toBe("John");
      expect(loaded.age).toBe(30);
      expect(loaded.address).toEqual({ street: "123 Main St" });
    });

    it("should load a CoMap with a primitive catchall property", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({}).catchall(z.string());

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          extra: "extra",
        },
        group,
      );

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: true,
      });

      assert(loaded.$jazzState === "loaded");

      expect(loaded.name).toBe("John");
      expect(loaded.extra).toBe("extra");
    });

    it("should load a CoMap with a catchall relation", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({}).catchall(
        co.map({
          prop: z.string(),
        }),
      );

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          extra1: {
            prop: "prop1",
          },
          extra2: {
            prop: "prop2",
          },
        },
        group,
      );

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { extra1: true },
      });

      assert(loaded.$jazzState === "loaded");

      expect(loaded.extra1.prop).toBe("prop1");
      expect("extra2" in loaded).toBe(true);
      expect(loaded.extra2?.$jazzState).toBe("unloaded");
    });

    it("should load all the relations on co.record when using $each", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Friends = co.record(
        z.string(),
        co.map({
          name: z.string(),
        }),
      );

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const friends = Friends.create(
        {
          joe: {
            name: "joe",
          },
          bob: {
            name: "bob",
          },
        },
        group,
      );

      const loaded = await loadCoValue(Friends, friends.$jazz.id, {
        resolve: { $each: true },
      });

      assert(loaded.$jazzState === "loaded");

      expect(loaded.joe?.name).toBe("joe");
      expect(loaded.bob?.name).toBe("bob");
    });

    it("should deeply load all the relations on co.record when using $each", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Friends = co.record(
        z.string(),
        co.map({
          name: z.string(),
          address: co.map({
            street: z.string(),
          }),
        }),
      );

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const friends = Friends.create(
        {
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
        },
        group,
      );

      const loaded = await loadCoValue(Friends, friends.$jazz.id, {
        resolve: { $each: { address: true } },
      });

      assert(loaded.$jazzState === "loaded");

      expect(loaded.joe?.name).toBe("joe");
      expect(loaded.joe?.address.street).toBe("123 Main St");
      expect(loaded.bob?.name).toBe("bob");
      expect(loaded.bob?.address.street).toBe("456 Main St");
    });

    it("should load a CoMap with self references", async () => {
      const anotherAccount = await createJazzTestAccount();

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

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          friend: {
            name: "Jane",
            age: 20,
          },
        },
        group,
      );

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { friend: true },
      });

      assert(loaded.$jazzState === "loaded");

      expect(loaded.name).toBe("John");
      expect(loaded.age).toBe(30);
      expect(loaded.friend).toEqual({
        name: "Jane",
        age: 20,
      });
    });

    it("should load a CoMap with nested values if an optional nested value is missing", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co
          .map({
            street: z.string(),
          })
          .optional(),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
        },
        group,
      );

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { address: true },
      });

      assert(loaded.$jazzState === "loaded");

      expect(loaded.name).toBe("John");
      expect(loaded.age).toBe(30);
      expect(loaded.address).toBeUndefined();
    });

    it("should return unloaded if the value is not available", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const loaded = await loadCoValue(Person, "co_z1" as any, {
        resolve: { address: true },
      });

      expect(loaded.$jazzState).toBe("unavailable");
      expect(loaded.$jazz.error).toMatchInlineSnapshot(`
        [ZodError: [
          {
            "code": "custom",
            "message": "The value is unavailable",
            "params": {
              "id": "co_z1"
            },
            "path": []
          }
        ]]
      `);
      expect(loaded.$jazz.id).toBe("co_z1");
    });

    it("should return unloaded if one of the nested values is not available", async () => {
      const anotherAccount = await createJazzTestAccount();
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address: { street: "123 Main St" },
        },
        group,
      );

      john.$jazz.raw.set("address", "co_z1");

      await john.$jazz.waitForSync();

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { address: true },
      });

      expect(loaded.$jazzState).toBe("unavailable");
      expect(loaded.$jazz.error).toMatchInlineSnapshot(`
        [ZodError: [
          {
            "code": "custom",
            "message": "The value is unavailable",
            "params": {
              "id": "co_z1"
            },
            "path": [
              "address"
            ]
          }
        ]]
      `);
      expect(loaded.$jazz.id).toBe(john.$jazz.id);
    });

    it("should return unloaded if one of the optional nested values is not available", async () => {
      const anotherAccount = await createJazzTestAccount();
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co
          .map({
            street: z.string(),
          })
          .optional(),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address: { street: "123 Main St" },
        },
        group,
      );

      john.$jazz.raw.set("address", "co_z1");

      await john.$jazz.waitForSync();

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { address: true },
      });

      expect(loaded.$jazzState).toBe("unavailable");
      expect(loaded.$jazz.error).toMatchInlineSnapshot(`
        [ZodError: [
          {
            "code": "custom",
            "message": "The value is unavailable",
            "params": {
              "id": "co_z1"
            },
            "path": [
              "address"
            ]
          }
        ]]
      `);
      expect(loaded.$jazz.id).toBe(john.$jazz.id);
    });

    it("should return unloaded if the value is not accessible", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address: { street: "123 Main St" },
        },
        anotherAccount,
      );

      await john.$jazz.waitForSync();

      const loaded = await loadCoValue(Person, john.$jazz.id as any, {
        resolve: { address: true },
      });

      expect(loaded.$jazzState).toBe("unauthorized");
      expect(loaded.$jazz.error).toMatchObject({
        issues: [
          {
            code: "custom",
            message: "The current user is not authorized to access this value",
            params: {
              id: john.$jazz.id,
            },
            path: [],
          },
        ],
      });
      expect(loaded.$jazz.id).toBe(john.$jazz.id);
    });

    it("should return unloaded if one of the nested values is not accessible", async () => {
      const anotherAccount = await createJazzTestAccount();
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address: Person.shape.address.create(
            { street: "123 Main St" },
            anotherAccount,
          ),
        },
        group,
      );

      await john.$jazz.waitForSync();

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { address: true },
      });

      expect(loaded.$jazzState).toBe("unauthorized");
      expect(loaded.$jazz.error).toMatchObject({
        issues: [
          {
            code: "custom",
            message: "The current user is not authorized to access this value",
            params: {
              id: john.address.$jazz.id,
            },
            path: ["address"],
          },
        ],
      });
      expect(loaded.$jazz.id).toBe(john.$jazz.id);
    });

    it("should return unauthorized if one of the optional nested values is not accessible", async () => {
      const anotherAccount = await createJazzTestAccount();
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co
          .map({
            street: z.string(),
          })
          .optional(),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address: Person.shape.address.create(
            { street: "123 Main St" },
            anotherAccount,
          ),
        },
        group,
      );

      await john.$jazz.waitForSync();

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { address: true },
      });

      expect(loaded.$jazzState).toBe("unauthorized");
      expect(loaded.$jazz.error).toMatchObject({
        issues: [
          {
            code: "custom",
            message: "The current user is not authorized to access this value",
            params: {
              id: john.address.$jazz.id,
            },
            path: ["address"],
          },
        ],
      });
    });

    it("should return validationError if the value is not valid", async () => {
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

      john.$jazz.raw.set("age", "not a number");

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { address: true },
      });

      expect(loaded.$jazzState).toBe("validationError");
      expect(loaded.$jazz.error).toMatchInlineSnapshot(`
        [ZodError: [
          {
            "code": "invalid_type",
            "expected": "number",
            "received": "string",
            "path": [
              "age"
            ],
            "message": "Expected number, received string"
          }
        ]]
      `);
    });

    it("should return validationError if the nested value is not valid", async () => {
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

      john.address.$jazz.raw.set("street", 123);

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { address: true },
      });

      expect(loaded.$jazzState).toBe("validationError");
      expect(loaded.$jazz.error).toMatchInlineSnapshot(`
        [ZodError: [
          {
            "code": "invalid_type",
            "expected": "string",
            "received": "number",
            "path": [
              "address",
              "street"
            ],
            "message": "Expected string, received number"
          }
        ]]
      `);
    });

    it("should aggregate the errors from the nested values", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address1: co.map({
          street: z.string(),
        }),
        address2: co.map({
          street: z.string(),
        }),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address1: { street: "123 Main St" },
          address2: Person.shape.address2.create(
            { street: "426" },
            anotherAccount,
          ),
        },
        group,
      );

      john.address1.$jazz.raw.set("street", 123);

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { address1: true, address2: true },
      });

      expect(loaded.$jazzState).toBe("unauthorized"); // Returns the last error
      expect(loaded.$jazz.error).toMatchObject({
        issues: [
          {
            code: "invalid_type",
            expected: "string",
            received: "number",
            path: ["address1", "street"],
            message: "Expected string, received number",
          },
          {
            code: "custom",
            message: "The current user is not authorized to access this value",
            params: {
              id: john.address2.$jazz.id,
            },
            path: ["address2"],
          },
        ],
      });
    });
  });

  describe("ensureLoaded", () => {
    it("should load the nested values", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address: { street: "123 Main St" },
        },
        group,
      );

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: true,
      });

      assert(loaded.$jazzState === "loaded");

      const { address } = await loaded.$jazz.ensureLoaded({
        resolve: { address: true },
      });

      expect(address).toEqual(john.address);
    });

    it("should throw if one of the nested values is not available", async () => {
      const anotherAccount = await createJazzTestAccount();
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address: { street: "123 Main St" },
        },
        group,
      );

      john.$jazz.raw.set("address", "co_z1");

      await john.$jazz.waitForSync();

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: true,
      });

      assert(loaded.$jazzState === "loaded");

      await expect(
        loaded.$jazz.ensureLoaded({
          resolve: { address: true },
        }),
      ).rejects.toThrow();
    });

    it("should throw if one of the nested values is not accessible", async () => {
      const anotherAccount = await createJazzTestAccount();
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address: Person.shape.address.create(
            { street: "123 Main St" },
            anotherAccount,
          ),
        },
        group,
      );

      await john.$jazz.waitForSync();

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: true,
      });

      assert(loaded.$jazzState === "loaded");

      await expect(
        loaded.$jazz.ensureLoaded({
          resolve: { address: true },
        }),
      ).rejects.toThrow();
    });
  });

  describe("subscribe", () => {
    it("should syncronously load a locally available CoMap without nested values", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
      });

      const spy = vi.fn();

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      let result: any;

      subscribeToCoValue(Person, john.$jazz.id, { resolve: true }, (value) => {
        result = value;
        spy();
      });

      const johnAfterSubscribe = result as Loaded<typeof Person, true>;

      expect(johnAfterSubscribe.name).toBe("John");
      expect(johnAfterSubscribe.age).toBe(30);
      expect(johnAfterSubscribe.address).toMatchObject({
        $jazzState: "unloaded",
        $jazz: {
          schema: Person.shape.address,
          id: john.address.$jazz.id,
        },
      });

      expect(spy).toHaveBeenCalledOnce();
    });

    it("should syncronously load a locally available CoMap with nested values", async () => {
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

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(
        Person,
        john.$jazz.id,
        { resolve: { address: true } },
        (value) => {
          result = value;
          spy();
        },
      );

      expect(result).toEqual({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      expect(spy).toHaveBeenCalledOnce();
    });

    it("should emit on updates", async () => {
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

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(
        Person,
        john.$jazz.id,
        { resolve: { address: true } },
        (value) => {
          result = value;
          spy();
        },
      );

      const resultBeforeSet = result;

      john.$jazz.set("name", "Jane");

      expect(result).toEqual({
        name: "Jane",
        age: 30,
        address: { street: "123 Main St" },
      });

      expect(spy).toHaveBeenCalledTimes(2);
      expect(resultBeforeSet).not.toBe(result);
    });

    it("should emit on nested values updates", async () => {
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

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(
        Person,
        john.$jazz.id,
        { resolve: { address: true } },
        (value) => {
          result = value;
          spy(value);
        },
      );

      const resultBeforeSet = result;

      john.$jazz.set("address", { street: "456 Main St" });

      expect(result).toEqual({
        name: "John",
        age: 30,
        address: { street: "456 Main St" },
      });

      expect(resultBeforeSet).not.toBe(result);

      expect(spy).toHaveBeenCalledTimes(2);
    });

    it("should emit when a catchall relation is updated", async () => {
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

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(
        Friends,
        friends.$jazz.id,
        { resolve: { john: true } },
        (value) => {
          result = value;
          spy();
        },
      );

      const resultBeforeSet = result;

      friends.$jazz.set("john", {
        name: "John Doe",
      });

      expect(result).toEqual({
        john: {
          name: "John Doe",
        },
      });

      expect(resultBeforeSet).not.toBe(result);

      expect(spy).toHaveBeenCalledTimes(2);
    });

    it("should emit when a catchall relation outside the resolve is added", async () => {
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

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(
        Friends,
        friends.$jazz.id,
        { resolve: { john: true } },
        (value) => {
          result = value;
          spy();
        },
      );

      const resultBeforeSet = result;

      const { jane } = friends.$jazz.set("jane", {
        name: "Jane",
      });

      expect(result).toEqual({
        john: {
          name: "John",
        },
        jane: {
          $jazzState: "unloaded",
        },
      });

      expect(result.jane.$jazz).toEqual({
        schema: Friends.record.value,
        id: jane?.$jazz.id,
      });

      expect(jane).toEqual({
        name: "Jane",
      });

      expect(resultBeforeSet).not.toBe(result);

      expect(spy).toHaveBeenCalledTimes(2);
    });

    it("should not emit updates if a stale nested value is updated", async () => {
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

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(
        Person,
        john.$jazz.id,
        { resolve: { address: true } },
        (value) => {
          result = value;
          spy(value);
        },
      );

      john.$jazz.set("address", { street: "456 Main St" });

      spy.mockReset();

      john.address.$jazz.set("street", "updated");

      expect(result).toEqual({
        name: "John",
        age: 30,
        address: { street: "456 Main St" },
      });

      expect(spy).not.toHaveBeenCalled();
    });

    it("should emit when an optional nested value becomes missing", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co
          .map({
            street: z.string(),
          })
          .optional(),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(
        Person,
        john.$jazz.id,
        { resolve: { address: true } },
        (value) => {
          result = value;
          spy();
        },
      );

      const resultBeforeSet = result;

      john.$jazz.set("address", undefined);

      expect(result).toEqual({
        name: "John",
        age: 30,
      });

      expect(resultBeforeSet).not.toBe(result);

      expect(spy).toHaveBeenCalledTimes(2);
    });

    it("should set the property as unloaded if we have the value but it is not requested", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co
          .map({
            street: z.string(),
          })
          .optional(),
      });

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(Person, john.$jazz.id, { resolve: true }, (value) => {
        result = value;
        spy();
      });

      expect(result.address).toMatchObject({
        $jazzState: "unloaded",
        $jazz: {
          schema: Person.shape.address,
          id: john.address.$jazz.id,
        },
      });

      expect(spy).toHaveBeenCalledTimes(1);
    });

    it("should set the property as undefined if we have the nested coValue is optional and missing", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address: co
          .map({
            street: z.string(),
          })
          .optional(),
      });

      const john = Person.create({
        name: "John",
        age: 30,
      });

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(Person, john.$jazz.id, { resolve: true }, (value) => {
        result = value;
        spy();
      });

      expect(result.address).toBe(undefined);

      expect(spy).toHaveBeenCalledTimes(1);
    });

    it("should emit the update only when the nested coValue is loaded", async () => {
      const anotherAccount = await createJazzTestAccount();

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

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(
        Person,
        john.$jazz.id,
        { resolve: { address: true } },
        (value) => {
          result = value;
          spy();
        },
      );

      const firstResult = result as Loaded<typeof Person, { address: true }>;

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const newAddress = Person.shape.address.create(
        { street: "456 Main St" },
        group,
      );

      john.$jazz.set("address", newAddress);

      expect(result).toBe(firstResult);

      await waitFor(() => {
        expect(result).not.toBe(firstResult);
      });

      expect(result).toEqual({
        name: "John",
        age: 30,
        address: { street: "456 Main St" },
      });

      expect(spy).toHaveBeenCalledTimes(2);
    });

    it("should preserve references of unchanged nested values", async () => {
      const personBaseProps = {
        name: z.string(),
        age: z.number(),
        address: co.map({
          street: z.string(),
        }),
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
        address: { street: "123 Main St" },
        friend: {
          name: "Jane",
          age: 20,
          address: { street: "456 Main St" },
        },
      });

      let result: any;

      const spy = vi.fn();

      subscribeToCoValue(
        Person,
        john.$jazz.id,
        { resolve: { address: true, friend: { address: true } } },
        (value) => {
          result = value;
          spy();
        },
      );

      expect(result).toEqual(john);

      const resultBeforeSet = result;

      john.address.$jazz.set("street", "456 Main St");

      expect(result).toEqual({
        name: "John",
        age: 30,
        address: { street: "456 Main St" },
        friend: {
          name: "Jane",
          age: 20,
          address: { street: "456 Main St" },
        },
      });

      expect(spy).toHaveBeenCalledTimes(2);

      expect(resultBeforeSet).not.toBe(result);
      expect(resultBeforeSet.friend).toBe(result.friend);
      expect(resultBeforeSet.address).not.toBe(result.address);
    });

    it("should send an update when all the errors from nested values are resolved", async () => {
      const anotherAccount = await createJazzTestAccount();
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address1: co.map({
          street: z.string(),
        }),
        address2: co.map({
          street: z.string(),
        }),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const groupAddress1 = Group.create(anotherAccount);
      const groupAddress2 = Group.create(anotherAccount);

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address1: Person.shape.address1.create(
            { street: "123 Main St" },
            groupAddress1,
          ),
          address2: Person.shape.address2.create(
            { street: "456 Main St" },
            groupAddress2,
          ),
        },
        group,
      );

      await john.$jazz.waitForSync();

      let result: any;
      const spy = vi.fn();

      subscribeToCoValue(
        Person,
        john.$jazz.id,
        { resolve: { address1: true, address2: true } },
        (value) => {
          result = value;
          spy();
        },
      );

      await waitFor(() => {
        expect(result.$jazzState).toBe("unauthorized");
      });

      expect(result.$jazz.error).toMatchObject({
        issues: [
          {
            code: "custom",
            message: "The current user is not authorized to access this value",
            params: { id: john.address1.$jazz.id },
            path: ["address1"],
          },
          {
            code: "custom",
            message: "The current user is not authorized to access this value",
            params: { id: john.address2.$jazz.id },
            path: ["address2"],
          },
        ],
      });

      // Resolve first address
      groupAddress1.addMember("everyone", "reader");
      await john.$jazz.waitForSync();

      await waitFor(() => {
        expect(result.$jazz.error.issues).toHaveLength(1);
      });

      expect(result.$jazz.error).toMatchObject({
        issues: [
          {
            code: "custom",
            message: "The current user is not authorized to access this value",
            params: { id: john.address2.$jazz.id },
            path: ["address2"],
          },
        ],
      });

      // Resolve second address
      groupAddress2.addMember("everyone", "reader");
      await john.$jazz.waitForSync();

      await waitFor(() => {
        expect(result.$jazzState).toBe("loaded");
      });

      expect(result).toEqual({
        name: "John",
        age: 30,
        address1: { street: "123 Main St" },
        address2: { street: "456 Main St" },
      });

      expect(spy).toHaveBeenCalledTimes(3);
    });

    it("should send an update when all the errors from nested values are resolved (errors are from values with the same id)", async () => {
      const anotherAccount = await createJazzTestAccount();
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        address1: co.map({
          street: z.string(),
        }),
        address2: co.map({
          street: z.string(),
        }),
      });

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const groupAddress = Group.create(anotherAccount);
      const address = Person.shape.address1.create(
        { street: "123 Main St" },
        groupAddress,
      );

      const john = Person.create(
        {
          name: "John",
          age: 30,
          address1: address,
          address2: address,
        },
        group,
      );

      await john.$jazz.waitForSync();

      let result: any;
      const spy = vi.fn();

      subscribeToCoValue(
        Person,
        john.$jazz.id,
        { resolve: { address1: true, address2: true } },
        (value) => {
          result = value;
          spy();
        },
      );

      await waitFor(() => {
        expect(result.$jazzState).toBe("unauthorized");
      });

      expect(result.$jazz.error).toMatchObject({
        issues: [
          {
            code: "custom",
            message: "The current user is not authorized to access this value",
            params: { id: john.address1.$jazz.id },
            path: ["address1"],
          },
          {
            code: "custom",
            message: "The current user is not authorized to access this value",
            params: { id: john.address2.$jazz.id },
            path: ["address2"],
          },
        ],
      });

      // Resolve first address
      groupAddress.addMember("everyone", "reader");
      await john.$jazz.waitForSync();

      await waitFor(() => {
        expect(result.$jazzState).toBe("loaded");
      });

      expect(result).toEqual({
        name: "John",
        age: 30,
        address1: { street: "123 Main St" },
        address2: { street: "123 Main St" },
      });

      expect(spy).toHaveBeenCalledTimes(3);
    });
  });
});
