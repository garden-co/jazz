import { assert, describe, expectTypeOf, it } from "vitest";
import { createJazzTestAccount } from "../../testing.js";
import { LoadedCoListJazzProps } from "../coList/instance.js";
import {
  CoListSchemaClass,
  ResolveQueryForCoListInit,
} from "../coList/schema.js";
import { CoMapSchemaClass } from "../coMap/schema.js";
import { LazySchema } from "../coValue/lazy.js";
import { Optional } from "../coValue/optional.js";
import { ResolveQueryOf, SchemaOf } from "../coValue/typeUtils.js";
import { MaybeLoaded, Unloaded } from "../coValue/types.js";
import { Loaded, co, z } from "../schema.js";
import { loadCoValue, subscribeToCoValue } from "../subscribe.js";
describe("CoList - test types", () => {
  describe("init", () => {
    it("should create a CoList with basic property access", () => {
      const ShoppingList = co.list(z.string());

      const shoppingList = ShoppingList.create(["banana", "apple"]);

      expectTypeOf<(typeof shoppingList)[0]>().toEqualTypeOf<string>();
      expectTypeOf<(typeof shoppingList)[1]>().toEqualTypeOf<string>();

      function isValid(value: Loaded<typeof ShoppingList>) {
        return value;
      }

      isValid(shoppingList);
    });

    it("we can't disallow value mutations", () => {
      const ShoppingList = co.list(z.string());

      const shoppingList = ShoppingList.create(["banana", "apple"]);

      shoppingList[0] = "orange";
    });

    it("should disallow invalid properties", () => {
      const ShoppingList = co.list(z.string());

      // @ts-expect-error - 1 is not a string
      ShoppingList.create([1]);
    });

    it("should not throw an error if a optional field is missing", () => {
      const ShoppingList = co.list(z.string().optional());

      const shoppingList = ShoppingList.create(["banana", undefined]);

      expectTypeOf<(typeof shoppingList)[0]>().toEqualTypeOf<
        string | undefined
      >();
      expectTypeOf<(typeof shoppingList)[1]>().toEqualTypeOf<
        string | undefined
      >();
    });

    it("should create a CoList with nested values", () => {
      const ShoppingList = co.list(
        co.map({
          name: z.string(),
          price: z.number(),
        }),
      );

      const shoppingList = ShoppingList.create([
        {
          name: "banana",
          price: 1,
        },
        {
          name: "apple",
          price: 2,
        },
      ]);

      expectTypeOf<(typeof shoppingList)[0]>().toEqualTypeOf<
        Loaded<typeof ShoppingList.items>
      >();
      expectTypeOf<(typeof shoppingList)[1]>().toEqualTypeOf<
        Loaded<typeof ShoppingList.items>
      >();

      function isValid(value: Loaded<typeof ShoppingList, { $each: true }>) {
        return value;
      }

      isValid(shoppingList);
    });

    it("should create a CoMap with child values using the create method", () => {
      const ShoppingList = co.list(
        co.map({
          name: z.string(),
          price: z.number(),
        }),
      );

      const shoppingList = ShoppingList.create([
        ShoppingList.items.create({
          name: "banana",
          price: 1,
        }),
        ShoppingList.items.create({
          name: "apple",
          price: 2,
        }),
      ]);

      expectTypeOf<(typeof shoppingList)[0]>().toEqualTypeOf<
        Loaded<typeof ShoppingList.items>
      >();
      expectTypeOf<(typeof shoppingList)[1]>().toEqualTypeOf<
        Loaded<typeof ShoppingList.items>
      >();

      function isValid(value: Loaded<typeof ShoppingList, { $each: true }>) {
        return value;
      }

      isValid(shoppingList);
    });

    it("should create a list inside a CoMap", () => {
      const Person = co.map({
        name: z.string(),
        tags: co.list(z.string()),
      });

      const person = Person.create({
        name: "John",
        tags: ["fun", "cool"],
      });

      expectTypeOf<(typeof person.tags)[0]>().toEqualTypeOf<string>();
      expectTypeOf<typeof person>().toEqualTypeOf<
        Loaded<
          typeof Person,
          {
            tags: true;
          }
        >
      >();
    });

    it("should create a list with a nested CoMap inside a CoMap", () => {
      const Person = co.map({
        name: z.string(),
        tags: co.list(
          co.map({
            name: z.string(),
            priority: z.number(),
          }),
        ),
      });

      const person = Person.create({
        name: "John",
        tags: [
          {
            name: "fun",
            priority: 1,
          },
          {
            name: "cool",
            priority: 2,
          },
        ],
      });

      expectTypeOf<(typeof person.tags)[0]>().toEqualTypeOf<
        Loaded<typeof Person.shape.tags.items>
      >();
      expectTypeOf<typeof person>().toEqualTypeOf<
        Loaded<
          typeof Person,
          {
            tags: {
              $each: true;
            };
          }
        >
      >();
    });

    it("should throw an error if a required ref is undefined", () => {
      const ShoppingList = co.list(
        co.map({
          name: z.string(),
          price: z.number(),
        }),
      );

      ShoppingList.create([
        // @ts-expect-error - items is not optional
        undefined,
        ShoppingList.items.create({ name: "apple", price: 2 }),
      ]);
    });

    it("should not throw an error if a required ref is missing", () => {
      const ShoppingList = co.list(
        co
          .map({
            name: z.string(),
            price: z.number(),
          })
          .optional(),
      );

      const shoppingList = ShoppingList.create([
        undefined,
        ShoppingList.items.create({ name: "apple", price: 2 }),
      ]);

      function isNullish(value: undefined) {
        return value === undefined;
      }

      if (!shoppingList[0]) {
        isNullish(shoppingList[0]);
      }
    });

    it("should create mutually recursive CoMaps", () => {
      const productBaseProps = {
        name: z.string(),
        price: z.number(),
      };

      const Product: CoMapSchemaClass<
        typeof productBaseProps & {
          list: CoListSchemaClass<LazySchema<typeof Product>, false>;
        },
        undefined,
        false
      > = co.map({
        ...productBaseProps,
        list: co.list(co.lazy(() => Product)),
      });

      const ShoppingList = co.list(Product);

      const list = ShoppingList.create([
        {
          name: "banana",
          price: 20,
          list: [],
        },
      ]);

      const value = list[0]?.list?.[0]?.name;

      expectTypeOf<typeof value>().toEqualTypeOf<string | undefined>();

      function isValidLoaded(
        value: Loaded<
          typeof ShoppingList,
          {
            $each: {
              list: {
                $each: true;
              };
            };
          }
        >,
      ) {
        return value;
      }

      isValidLoaded(list);
    });

    describe.todo("updates", () => {});

    describe("load", () => {
      it("should return valid Loaded type from the load function", async () => {
        const ShoppingList = co.list(
          co.map({
            name: z.string(),
            price: z.number(),
          }),
        );

        const shoppingList = ShoppingList.create([
          {
            name: "banana",
            price: 1,
          },
        ]);

        const loaded = await loadCoValue(ShoppingList, shoppingList.$jazz.id, {
          resolve: true,
        });

        function isValidLoaded(value: MaybeLoaded<typeof ShoppingList>) {
          return value;
        }

        isValidLoaded(loaded);
      });

      it("should return valid Loaded type from the load function", async () => {
        const ShoppingList = co.list(
          co.map({
            name: z.string(),
            price: z.number(),
          }),
        );

        const shoppingList = ShoppingList.create([
          {
            name: "banana",
            price: 1,
          },
        ]);

        const loaded = await loadCoValue(ShoppingList, shoppingList.$jazz.id, {
          resolve: {
            $each: true,
          },
        });

        function isValidLoaded(
          value: MaybeLoaded<typeof ShoppingList, { $each: true }>,
        ) {
          return value;
        }

        isValidLoaded(loaded);
      });

      it("should disallow extra properties in the resolve", async () => {
        const ShoppingList = co.list(
          co.map({
            name: z.string(),
            price: z.number(),
          }),
        );

        const shoppingList = ShoppingList.create([
          {
            name: "banana",
            price: 1,
          },
        ]);

        await loadCoValue(ShoppingList, shoppingList.$jazz.id, {
          resolve: {
            // @ts-expect-error - extra is not a valid relation
            extra: true,
          },
        });
      });

      it("should load a CoMap with an inner list", async () => {
        const Person = co.map({
          name: z.string(),
          age: z.number(),
          tags: co.list(
            co.map({
              name: z.string(),
              priority: z.number(),
            }),
          ),
        });

        const john = Person.create({
          name: "John",
          age: 30,
          tags: [
            {
              name: "fun",
              priority: 1,
            },
          ],
        });

        const loaded = await loadCoValue(Person, john.$jazz.id, {
          resolve: {
            tags: {
              $each: true,
            },
          },
        });

        function isValidLoaded(
          value: MaybeLoaded<typeof Person, { tags: { $each: true } }>,
        ) {
          return value;
        }

        isValidLoaded(loaded);

        assert(loaded.$jazzState === "loaded");

        expectTypeOf(loaded.tags).toEqualTypeOf<
          Loaded<typeof Person.shape.tags, { $each: true }>
        >();
      });

      it("should load all the relations on co.record when using $each", async () => {
        const Friends = co.record(
          z.string(),
          co.list(
            co.map({
              name: z.string(),
            }),
          ),
        );

        const friends = Friends.create({
          joe: [
            {
              name: "joe",
            },
          ],
          bob: [
            {
              name: "bob",
            },
          ],
        });

        const loaded = await loadCoValue(Friends, friends.$jazz.id, {
          resolve: {
            $each: {
              $each: true,
            },
          },
        });

        function isValidLoaded(
          value: MaybeLoaded<
            typeof Friends,
            {
              $each: {
                $each: true;
              };
            }
          >,
        ) {
          return value;
        }

        isValidLoaded(loaded);

        assert(loaded.$jazzState === "loaded");

        expectTypeOf(loaded.joe).toEqualTypeOf<
          Loaded<typeof Friends.record.value, { $each: true }> | undefined
        >();
      });
    });

    describe("ensureLoaded", () => {
      it("should load the nested values", async () => {
        const ShoppingList = co.list(
          co.map({
            name: z.string(),
            price: z.number(),
          }),
        );

        const shoppingList = ShoppingList.create([
          {
            name: "banana",
            price: 1,
          },
        ]);

        const result = await shoppingList.$jazz.ensureLoaded({
          resolve: { $each: true },
        });

        expectTypeOf(result).toEqualTypeOf<
          Loaded<typeof ShoppingList, { $each: true }>
        >();
      });
    });

    describe("subscribe", () => {
      it("should invoke the callback with the loaded value", async () => {
        const ShoppingList = co.list(
          co.map({
            name: z.string(),
            price: z.number(),
          }),
        );

        const shoppingList = ShoppingList.create([
          {
            name: "banana",
            price: 1,
          },
        ]);

        subscribeToCoValue(
          ShoppingList,
          shoppingList.$jazz.id,
          { resolve: { $each: true } },
          (value) => {
            expectTypeOf(value).toEqualTypeOf<
              | Loaded<typeof ShoppingList, { $each: true }>
              | Unloaded<typeof ShoppingList>
            >();
          },
        );
      });
    });

    describe("Loaded", () => {
      it("deep loaded maps should be complatible non-deep maps values", async () => {
        const ShoppingList = co.list(
          co.map({
            name: z.string(),
            price: z.number(),
          }),
        );

        const deepLoaded = {} as Loaded<typeof ShoppingList, { $each: true }>;

        function isValid(value: Loaded<typeof ShoppingList>) {
          return value;
        }

        isValid(deepLoaded);
      });

      it("$each loaded records should be complatible non-deep loaded values", async () => {
        const People = co.record(
          z.string(),
          co.list(
            co.map({
              name: z.string(),
              price: z.number(),
            }),
          ),
        );

        const deepLoaded = {} as Loaded<
          typeof People,
          { $each: { $each: true } }
        >;

        function isValid(value: Loaded<typeof People>) {
          return value;
        }

        isValid(deepLoaded);
      });

      it("deep loaded records should be complatible non-deep loaded values", async () => {
        const People = co.record(
          z.string(),
          co.list(
            co.map({
              name: z.string(),
              price: z.number(),
            }),
          ),
        );

        const deepLoaded = {} as Loaded<typeof People, { joe: true }>;

        function isValid(value: Loaded<typeof People>) {
          return value;
        }

        isValid(deepLoaded);
      });
    });
  });
});
