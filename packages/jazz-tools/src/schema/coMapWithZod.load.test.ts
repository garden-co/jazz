import {
  assert,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  it,
  vi,
} from "vitest";
import { Group } from "../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { waitFor } from "../tests/utils.js";
import { Loaded, co, z } from "./schema.js";
import { loadCoValue, subscribeToCoValue } from "./subscribe.js";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
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

      assert(loaded);

      expect(loaded.name).toBe("John");
      expect(loaded.age).toBe(30);
      expect(loaded.address).toBe(null);

      expectTypeOf(loaded.address).toEqualTypeOf<null>();
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

      assert(loaded);

      expect(loaded.name).toBe("John");
      expect(loaded.age).toBe(30);
      expect(loaded.address).toEqual({ street: "123 Main St" });

      expectTypeOf(loaded.address).toEqualTypeOf<
        Loaded<typeof Person.shape.address, true>
      >();
    });

    it("should load a CoMap with a catchall static property", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({}).catchall(z.string());

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: "John",
          catchall: "catchall",
        },
        group,
      );

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { address: true },
      });

      assert(loaded);

      expect(loaded.name).toBe("John");
      expect(loaded.catchall).toBe("catchall");
    });

    it("should load a CoMap with a catchall relation", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({}).catchall(
        co.map({
          catchall: z.string(),
        }),
      );

      const group = Group.create(anotherAccount);
      group.addMember("everyone", "reader");

      const john = Person.create(
        {
          name: {
            catchall: "catchall",
          },
          catchall: {
            catchall: "catchall",
          },
        },
        group,
      );

      const loaded = await loadCoValue(Person, john.$jazz.id, {
        resolve: { catchall: true },
      });

      assert(loaded);

      expect(loaded.catchall?.catchall).toBe("catchall");
      expect(loaded.name).toBe(null);
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

      assert(loaded);

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

      assert(loaded);

      expect(loaded.joe?.name).toBe("joe");
      expect(loaded.joe?.address.street).toBe("123 Main St");
      expect(loaded.bob?.name).toBe("bob");
      expect(loaded.bob?.address.street).toBe("456 Main St");
    });

    it("should load a CoMap with self references", async () => {
      const anotherAccount = await createJazzTestAccount();

      const Person = co.map({
        name: z.string(),
        age: z.number(),
        friend: co.self(),
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

      assert(loaded);

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

      assert(loaded);

      expect(loaded.name).toBe("John");
      expect(loaded.age).toBe(30);
      expect(loaded.address).toBeUndefined();

      expectTypeOf(loaded.address).toEqualTypeOf<
        Loaded<typeof Person.shape.address, true> | undefined | null
      >();
    });

    it.todo(
      "should return undefined if the value is not available",
      async () => {
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

        const loaded = await loadCoValue(Person, (john.$jazz.id + "1") as any, {
          resolve: { address: true },
        });

        expect(loaded).toBeUndefined();
      },
    );

    it.todo(
      "should return undefined if one of the nested values is not available",
      async () => {
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

        expect(loaded).toBeUndefined();
      },
    );

    it.todo(
      "should return undefined if the value is not accessible",
      async () => {
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

        expect(loaded).toBeUndefined();
      },
    );

    it.todo(
      "should return undefined if one of the nested values is not accessible",
      async () => {
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
            address: Person.shape.address.create({ street: "123 Main St" }),
          },
          group,
        );

        await john.$jazz.waitForSync();

        const loaded = await loadCoValue(Person, john.$jazz.id, {
          resolve: { address: true },
        });

        expect(loaded).toBeUndefined();
      },
    );
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

      assert(loaded);

      const { address } = await loaded.$jazz.ensureLoaded({
        resolve: { address: true },
      });

      expect(address).toEqual(john.address);
      expectTypeOf(address).toEqualTypeOf<
        Loaded<typeof Person.shape.address, true>
      >();
    });

    it.todo(
      "should throw if one of the nested values is not available",
      async () => {
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

        assert(loaded);

        await expect(
          loaded.$jazz.ensureLoaded({
            resolve: { address: true },
          }),
        ).rejects.toThrow();
      },
    );

    it.todo(
      "should throw if one of the nested values is not accessible",
      async () => {
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
            address: Person.shape.address.create({ street: "123 Main St" }),
          },
          group,
        );

        await john.$jazz.waitForSync();

        const loaded = await loadCoValue(Person, john.$jazz.id, {
          resolve: true,
        });

        assert(loaded);

        await expect(
          loaded.$jazz.ensureLoaded({
            resolve: { address: true },
          }),
        ).rejects.toThrow();
      },
    );
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

      const john = Person.create({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });

      let result: any;

      subscribeToCoValue(Person, john.$jazz.id, { resolve: true }, (value) => {
        result = value;
      });

      const johnAfterSubscribe = result as Loaded<typeof Person, true>;

      expect(johnAfterSubscribe.name).toBe("John");
      expect(johnAfterSubscribe.age).toBe(30);
      expect(johnAfterSubscribe.address).toBe(null);
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

      subscribeToCoValue(
        Person,
        john.$jazz.id,
        { resolve: { address: true } },
        (value) => {
          result = value;
        },
      );

      expect(result).toEqual({
        name: "John",
        age: 30,
        address: { street: "123 Main St" },
      });
    });
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

    subscribeToCoValue(
      Person,
      john.$jazz.id,
      { resolve: { address: true } },
      (value) => {
        result = value;
      },
    );

    const resultBeforeSet = result;

    john.$jazz.set("name", "Jane");

    expect(result).toEqual({
      name: "Jane",
      age: 30,
      address: { street: "123 Main St" },
    });

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

    subscribeToCoValue(
      Person,
      john.$jazz.id,
      { resolve: { address: true } },
      (value) => {
        result = value;
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

    subscribeToCoValue(
      Friends,
      friends.$jazz.id,
      { resolve: { john: true } },
      (value) => {
        result = value;
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

    subscribeToCoValue(
      Friends,
      friends.$jazz.id,
      { resolve: { john: true } },
      (value) => {
        result = value;
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
      jane: null,
    });

    expect(jane).toEqual({
      name: "Jane",
    });

    expect(resultBeforeSet).not.toBe(result);
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

    subscribeToCoValue(
      Person,
      john.$jazz.id,
      { resolve: { address: true } },
      (value) => {
        result = value;
      },
    );

    const resultBeforeSet = result;

    john.$jazz.set("address", undefined);

    expect(result).toEqual({
      name: "John",
      age: 30,
    });

    expect(resultBeforeSet).not.toBe(result);
  });

  it("should set the property as null if we have the value but it is not requested", async () => {
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

    subscribeToCoValue(Person, john.$jazz.id, { resolve: true }, (value) => {
      result = value;
    });

    expect(result.address).toBe(null);
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

    subscribeToCoValue(Person, john.$jazz.id, { resolve: true }, (value) => {
      result = value;
    });

    expect(result.address).toBe(undefined);
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

    subscribeToCoValue(
      Person,
      john.$jazz.id,
      { resolve: { address: true } },
      (value) => {
        result = value;
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
  });

  it("should preserve references of unchanged nested values", async () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
      address: co.map({
        street: z.string(),
      }),
      friend: co.self(),
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

    subscribeToCoValue(
      Person,
      john.$jazz.id,
      { resolve: { address: true, friend: { address: true } } },
      (value) => {
        result = value;
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

    expect(resultBeforeSet).not.toBe(result);
    expect(resultBeforeSet.friend).toBe(result.friend);
    expect(resultBeforeSet.address).not.toBe(result.address);
  });
});
