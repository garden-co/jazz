import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, beforeEach, describe, expect, test, vi } from "vitest";
import { Account, Group, subscribeToCoValue, z } from "../index.js";
import {
  Loaded,
  co,
  coValueClassFromCoValueClassOrSchema,
} from "../internal.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { setupTwoNodes, waitFor } from "./utils.js";

const Crypto = await WasmCrypto.create();

let me = await Account.create({
  creationProps: { name: "Hermes Puggington" },
  crypto: Crypto,
});

beforeEach(async () => {
  await setupJazzTestSync();

  me = await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });
});

describe("Simple CoList operations", async () => {
  const TestList = co.list(z.string());

  const list = TestList.create(["bread", "butter", "onion"], { owner: me });

  test("Construction", () => {
    expect(list[0]).toBe("bread");
    expect(list[1]).toBe("butter");
    expect(list[2]).toBe("onion");
    expect(list.$jazz.raw.asArray()).toEqual(["bread", "butter", "onion"]);
    expect(list.length).toBe(3);
    expect(list.map((item) => item.toUpperCase())).toEqual([
      "BREAD",
      "BUTTER",
      "ONION",
    ]);
  });

  test("list with enum type", () => {
    const List = co.list(z.enum(["a", "b", "c"]));
    const list = List.create(["a", "b", "c"]);
    expect(list.length).toBe(3);
    expect(list[0]).toBe("a");
    expect(list[1]).toBe("b");
    expect(list[2]).toBe("c");
  });

  test("create CoList with reference using CoValue", () => {
    const Dog = co.map({
      name: z.string(),
    });
    const Person = co.map({
      pets: co.list(Dog),
    });

    const person = Person.create({
      pets: [Dog.create({ name: "Rex" }), Dog.create({ name: "Fido" })],
    });

    expect(person.pets.length).toEqual(2);
    expect(person.pets[0]?.name).toEqual("Rex");
    expect(person.pets[1]?.name).toEqual("Fido");
  });

  describe("create CoList with reference using JSON", () => {
    test("automatically creates CoValues for nested objects", () => {
      const Dog = co.map({
        name: z.string(),
      });
      const Person = co.map({
        pets: co.list(Dog),
      });

      const person = Person.create({
        pets: [{ name: "Rex" }, { name: "Fido" }],
      });

      expect(person.pets.length).toEqual(2);
      expect(person.pets[0]?.name).toEqual("Rex");
      expect(person.pets[1]?.name).toEqual("Fido");
    });

    test("can create a coPlainText from an empty string", () => {
      const Schema = co.list(co.plainText());
      const list = Schema.create([""]);
      expect(list[0]?.toString()).toBe("");
    });
  });

  test("list with nullable content", () => {
    const List = co.list(z.string().nullable());
    const list = List.create(["a", "b", "c", null]);
    expect(list.length).toBe(4);
    expect(list[0]).toBe("a");
    expect(list[1]).toBe("b");
    expect(list[2]).toBe("c");
    expect(list[3]).toBeNull();
  });

  test("Construction with an Account", () => {
    const list = TestList.create(["milk"], me);

    expect(list[0]).toEqual("milk");
  });

  test("Construction with a Group", () => {
    const group = Group.create(me);
    const list = TestList.create(["milk"], group);

    expect(list[0]).toEqual("milk");
  });

  describe("Mutation", () => {
    test("assignment", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });
      list.$jazz.set(1, "margarine");
      expect(list.$jazz.raw.asArray()).toEqual(["bread", "margarine", "onion"]);
      expect(list[1]).toBe("margarine");
    });

    test("assignment with ref", () => {
      const Ingredient = co.map({
        name: z.string(),
      });

      const Recipe = co.list(Ingredient);

      const recipe = Recipe.create(
        [
          Ingredient.create({ name: "bread" }, me),
          Ingredient.create({ name: "butter" }, me),
          Ingredient.create({ name: "onion" }, me),
        ],
        { owner: me },
      );

      recipe.$jazz.set(1, Ingredient.create({ name: "margarine" }, me));
      expect(recipe[1]?.name).toBe("margarine");
    });

    test("assign undefined on a required ref", () => {
      const Ingredient = co.map({
        name: z.string(),
      });

      const Recipe = co.list(Ingredient);

      const recipe = Recipe.create(
        [
          Ingredient.create({ name: "bread" }, me),
          Ingredient.create({ name: "butter" }, me),
          Ingredient.create({ name: "onion" }, me),
        ],
        { owner: me },
      );

      expect(() => {
        recipe.$jazz.set(1, undefined as unknown as Loaded<typeof Ingredient>);
      }).toThrow("Cannot set required reference 1 to undefined");

      expect(recipe[1]?.name).toBe("butter");
    });

    test("assign undefined on an optional ref", () => {
      const Ingredient = co.map({
        name: z.string(),
      });

      const Recipe = co.list(co.optional(Ingredient));

      const recipe = Recipe.create(
        [
          Ingredient.create({ name: "bread" }, me),
          Ingredient.create({ name: "butter" }, me),
          Ingredient.create({ name: "onion" }, me),
        ],
        { owner: me },
      );

      recipe.$jazz.set(1, undefined);
      expect(recipe[1]).toBe(undefined);
    });

    test("push", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });
      list.$jazz.push("cheese");
      expect(list[3]).toBe("cheese");
      expect(list.$jazz.raw.asArray()).toEqual([
        "bread",
        "butter",
        "onion",
        "cheese",
      ]);
    });

    test("unshift", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });
      list.$jazz.unshift("lettuce");
      expect(list[0]).toBe("lettuce");
      expect(list.$jazz.raw.asArray()).toEqual([
        "lettuce",
        "bread",
        "butter",
        "onion",
      ]);
    });

    test("pop", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });
      expect(list.$jazz.pop()).toBe("onion");
      expect(list.length).toBe(2);
      expect(list.$jazz.raw.asArray()).toEqual(["bread", "butter"]);
    });

    test("shift", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });
      expect(list.$jazz.shift()).toBe("bread");
      expect(list.length).toBe(2);
      expect(list.$jazz.raw.asArray()).toEqual(["butter", "onion"]);
    });

    describe("splice", () => {
      test("insert after 1st item with 1 item removed", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });
        list.$jazz.splice(1, 1, "salt", "pepper");
        expect(list.length).toBe(4);
        expect(list.$jazz.raw.asArray()).toEqual([
          "bread",
          "salt",
          "pepper",
          "onion",
        ]);
      });

      test("insert before 1st item", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });
        list.$jazz.splice(0, 0, "salt", "pepper");
        expect(list.length).toBe(5);
        expect(list.$jazz.raw.asArray()).toEqual([
          "salt",
          "pepper",
          "bread",
          "butter",
          "onion",
        ]);
      });

      test("insert after 1st item", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });
        list.$jazz.splice(1, 0, "salt", "pepper");
        expect(list.length).toBe(5);
        expect(list.$jazz.raw.asArray()).toEqual([
          "bread",
          "salt",
          "pepper",
          "butter",
          "onion",
        ]);
      });

      test("insert after 2nd item", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });
        list.$jazz.splice(2, 0, "salt", "pepper");
        expect(list.length).toBe(5);
        expect(list.$jazz.raw.asArray()).toEqual([
          "bread",
          "butter",
          "salt",
          "pepper",
          "onion",
        ]);
      });
    });

    test("applyDiff", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });
      // replace
      list.$jazz.applyDiff(["bread", "margarine", "onion"]);
      expect(list.$jazz.raw.asArray()).toEqual(["bread", "margarine", "onion"]);
      // delete
      list.$jazz.applyDiff(["bread", "onion"]);
      expect(list.$jazz.raw.asArray()).toEqual(["bread", "onion"]);
      // insert multiple
      list.$jazz.applyDiff(["bread", "margarine", "onion", "cheese"]);
      expect(list.$jazz.raw.asArray()).toEqual([
        "bread",
        "margarine",
        "onion",
        "cheese",
      ]);
    });

    test("filter + assign to coMap", () => {
      const TestMap = co.map({
        list: TestList,
      });

      const map = TestMap.create(
        {
          list: TestList.create(["bread", "butter", "onion"], {
            owner: me,
          }),
        },
        { owner: me },
      );

      expect(() => {
        map.$jazz.set(
          "list",
          // @ts-expect-error
          map.list?.filter((item) => item !== "butter"),
        );
      }).toThrow("Cannot set reference list to a non-CoValue. Got bread,onion");

      expect(map.list?.$jazz.raw.asArray()).toEqual([
        "bread",
        "butter",
        "onion",
      ]);
    });

    test("filter + assign to CoList", () => {
      const TestListOfLists = co.list(TestList);

      const list = TestListOfLists.create(
        [
          TestList.create(["bread", "butter", "onion"], {
            owner: me,
          }),
        ],
        { owner: me },
      );

      list.$jazz.set(
        0,
        // @ts-expect-error
        list[0]?.filter((item) => item !== "butter"),
      );

      expect(list[0]?.$jazz.raw.asArray()).toEqual(["bread", "onion"]);
    });
  });
});

describe("CoList applyDiff operations", async () => {
  test("applyDiff with primitive values", () => {
    const StringList = co.list(z.string());
    const list = StringList.create(["a", "b", "c"], { owner: me });

    // Test adding items
    list.$jazz.applyDiff(["a", "b", "c", "d", "e"]);
    expect(list.$jazz.raw.asArray()).toEqual(["a", "b", "c", "d", "e"]);

    // Test removing items
    list.$jazz.applyDiff(["a", "c", "e"]);
    expect(list.$jazz.raw.asArray()).toEqual(["a", "c", "e"]);

    // Test replacing items
    list.$jazz.applyDiff(["x", "y", "z"]);
    expect(list.$jazz.raw.asArray()).toEqual(["x", "y", "z"]);

    // Test empty list
    list.$jazz.applyDiff([]);
    expect(list.$jazz.raw.asArray()).toEqual([]);
  });

  test("applyDiff with reference values", () => {
    const NestedItem = co.list(z.string());
    const RefList = co.list(NestedItem);

    const item1 = NestedItem.create(["item1"], { owner: me });
    const item2 = NestedItem.create(["item2"], { owner: me });
    const item3 = NestedItem.create(["item3"], { owner: me });
    const item4 = NestedItem.create(["item4"], { owner: me });

    const list = RefList.create([item1, item2], { owner: me });

    // Test adding reference items
    list.$jazz.applyDiff([item1, item2, item3]);
    expect(list.length).toBe(3);
    expect(list[2]?.[0]).toBe("item3");

    // Test removing reference items
    list.$jazz.applyDiff([item1, item3]);
    expect(list.length).toBe(2);
    expect(list[0]?.[0]).toBe("item1");
    expect(list[1]?.[0]).toBe("item3");

    // Test replacing reference items
    list.$jazz.applyDiff([item4]);
    expect(list.length).toBe(1);
    expect(list[0]?.[0]).toBe("item4");

    // Test empty list
    list.$jazz.applyDiff([]);
    expect(list.$jazz.raw.asArray()).toEqual([]);
  });

  test("applyDiff with refs + filter", () => {
    const TestMap = co.map({
      type: z.string(),
    });

    const TestList = co.list(TestMap);

    const bread = TestMap.create({ type: "bread" }, me);
    const butter = TestMap.create({ type: "butter" }, me);
    const onion = TestMap.create({ type: "onion" }, me);

    const list = TestList.create([bread, butter, onion], me);

    list.$jazz.applyDiff(list.filter((item) => item?.type !== "butter"));

    expect(list.$jazz.raw.asArray()).toEqual([bread.$jazz.id, onion.$jazz.id]);
  });

  test("applyDiff with mixed operations", () => {
    const StringList = co.list(z.string());
    const list = StringList.create(["a", "b", "c", "d", "e"], { owner: me });

    // Test multiple operations at once
    list.$jazz.applyDiff(["a", "x", "c", "y", "e"]);
    expect(list.$jazz.raw.asArray()).toEqual(["a", "x", "c", "y", "e"]);

    // Test reordering
    list.$jazz.applyDiff(["e", "c", "a", "y", "x"]);
    expect(list.$jazz.raw.asArray()).toEqual(["e", "c", "a", "y", "x"]);

    // Test partial update
    list.$jazz.applyDiff(["e", "c", "new", "y", "x"]);
    expect(list.$jazz.raw.asArray()).toEqual(["e", "c", "new", "y", "x"]);
  });
});

describe("CoList resolution", async () => {
  const TwiceNestedList = co.list(z.string());

  const NestedList = co.list(TwiceNestedList);

  const TestList = co.list(NestedList);

  const initNodeAndList = async () => {
    const me = await Account.create({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
    });

    const list = TestList.create(
      [
        NestedList.create([TwiceNestedList.create(["a", "b"], { owner: me })], {
          owner: me,
        }),
        NestedList.create([TwiceNestedList.create(["c", "d"], { owner: me })], {
          owner: me,
        }),
      ],
      { owner: me },
    );

    return { me, list };
  };

  test("Construction", async () => {
    const { list } = await initNodeAndList();

    expect(list[0]?.[0]?.[0]).toBe("a");
    expect(list[0]?.[0]?.join(",")).toBe("a,b");
    expect(list[0]?.[0]?.$jazz.id).toBeDefined();
    expect(list[1]?.[0]?.[0]).toBe("c");
  });

  test("accessing the value refs", async () => {
    const Dog = co.map({
      name: z.string(),
      breed: z.string(),
    });

    const Pets = co.list(Dog);

    const group = Group.create();
    group.addMember("everyone", "writer");

    const pets = Pets.create([{ name: "Rex", breed: "Labrador" }], group);

    const userB = await createJazzTestAccount();
    const loadedPets = await Pets.load(pets.$jazz.id, {
      loadAs: userB,
    });

    assert(loadedPets);

    const petReference = loadedPets.$jazz.refs[0];
    expect(petReference).toBeDefined();
    expect(petReference?.id).toBe(pets[0]?.$jazz.id);

    const dog = await petReference?.load();

    assert(dog);

    expect(dog.name).toEqual("Rex");
  });

  test("waitForSync should resolve when the value is uploaded", async () => {
    const TestList = co.list(z.number());

    const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

    const list = TestList.create([1, 2, 3], { owner: clientAccount });

    await list.$jazz.waitForSync({ timeout: 1000 });

    // Killing the client node so the serverNode can't load the list from it
    clientNode.gracefulShutdown();

    const loadedMap = await serverNode.load(list.$jazz.raw.id);

    expect(loadedMap).not.toBe("unavailable");
  });
});

describe("CoList subscription", async () => {
  test("subscription on a locally available list with deep resolve", async () => {
    const Item = co.map({
      name: z.string(),
    });

    const TestList = co.list(Item);

    const list = TestList.create(
      [Item.create({ name: "Item 1" }), Item.create({ name: "Item 2" })],
      { owner: me },
    );

    const updates: Loaded<typeof TestList, { $each: true }>[] = [];
    const spy = vi.fn((list) => updates.push(list));

    TestList.subscribe(
      list.$jazz.id,
      {
        resolve: {
          $each: true,
        },
      },
      spy,
    );

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.[0]?.name).toEqual("Item 1");
    expect(updates[0]?.[1]?.name).toEqual("Item 2");

    list[0]!.$jazz.set("name", "Updated Item 1");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.[0]?.name).toEqual("Updated Item 1");
    expect(updates[1]?.[1]?.name).toEqual("Item 2");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("subscription on a locally available list with autoload", async () => {
    const Item = co.map({
      name: z.string(),
    });

    const TestList = co.list(Item);

    const list = TestList.create(
      [Item.create({ name: "Item 1" }), Item.create({ name: "Item 2" })],
      { owner: me },
    );

    const updates: Loaded<typeof TestList>[] = [];
    const spy = vi.fn((list) => updates.push(list));

    TestList.subscribe(list.$jazz.id, {}, spy);

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.[0]?.name).toEqual("Item 1");
    expect(updates[0]?.[1]?.name).toEqual("Item 2");

    list[0]!.$jazz.set("name", "Updated Item 1");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.[0]?.name).toEqual("Updated Item 1");
    expect(updates[1]?.[1]?.name).toEqual("Item 2");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("subscription on a locally available list with syncResolution", async () => {
    const Item = co.map({
      name: z.string(),
    });

    const TestList = co.list(Item);

    const list = TestList.create(
      [Item.create({ name: "Item 1" }), Item.create({ name: "Item 2" })],
      { owner: me },
    );

    const updates: Loaded<typeof TestList>[] = [];
    const spy = vi.fn((list) => updates.push(list));

    subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(TestList),
      list.$jazz.id,
      {
        syncResolution: true,
        loadAs: Account.getMe(),
      },
      spy,
    );

    expect(spy).toHaveBeenCalled();
    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.[0]?.name).toEqual("Item 1");
    expect(updates[0]?.[1]?.name).toEqual("Item 2");

    expect(spy).toHaveBeenCalledTimes(1);

    list[0]!.$jazz.set("name", "Updated Item 1");

    expect(spy).toHaveBeenCalledTimes(2);

    expect(updates[1]?.[0]?.name).toEqual("Updated Item 1");
    expect(updates[1]?.[1]?.name).toEqual("Item 2");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("subscription on a remotely available list with deep resolve", async () => {
    const Item = co.map({
      name: z.string(),
    });

    const TestList = co.list(Item);

    const group = Group.create();
    group.addMember("everyone", "writer");

    const list = TestList.create(
      [
        Item.create({ name: "Item 1" }, group),
        Item.create({ name: "Item 2" }, group),
      ],
      group,
    );

    const userB = await createJazzTestAccount();

    const updates: Loaded<typeof TestList, { $each: true }>[] = [];
    const spy = vi.fn((list) => updates.push(list));

    TestList.subscribe(
      list.$jazz.id,
      {
        resolve: {
          $each: true,
        },
        loadAs: userB,
      },
      spy,
    );

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.[0]?.name).toEqual("Item 1");
    expect(updates[0]?.[1]?.name).toEqual("Item 2");

    list[0]!.$jazz.set("name", "Updated Item 1");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.[0]?.name).toEqual("Updated Item 1");
    expect(updates[1]?.[1]?.name).toEqual("Item 2");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("subscription on a remotely available list with autoload", async () => {
    const Item = co.map({
      name: z.string(),
    });

    const TestList = co.list(Item);

    const group = Group.create();
    group.addMember("everyone", "writer");

    const list = TestList.create(
      [
        Item.create({ name: "Item 1" }, group),
        Item.create({ name: "Item 2" }, group),
      ],
      group,
    );

    const updates: Loaded<typeof TestList>[] = [];
    const spy = vi.fn((list) => updates.push(list));

    const userB = await createJazzTestAccount();

    TestList.subscribe(
      list.$jazz.id,
      {
        loadAs: userB,
      },
      spy,
    );

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.[0]?.name).toEqual("Item 1");
    expect(updates[0]?.[1]?.name).toEqual("Item 2");

    list[0]!.$jazz.set("name", "Updated Item 1");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.[0]?.name).toEqual("Updated Item 1");
    expect(updates[1]?.[1]?.name).toEqual("Item 2");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("replacing list items triggers updates", async () => {
    const Item = co.map({
      name: z.string(),
    });

    const TestList = co.list(Item);

    const list = TestList.create(
      [Item.create({ name: "Item 1" }), Item.create({ name: "Item 2" })],
      { owner: me },
    );

    const updates: Loaded<typeof TestList, { $each: true }>[] = [];
    const spy = vi.fn((list) => updates.push(list));

    TestList.subscribe(
      list.$jazz.id,
      {
        resolve: {
          $each: true,
        },
      },
      spy,
    );

    expect(spy).not.toHaveBeenCalled();

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    expect(updates[0]?.[0]?.name).toEqual("Item 1");
    expect(updates[0]?.[1]?.name).toEqual("Item 2");

    list.$jazz.set(0, Item.create({ name: "New Item 1" }));

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(updates[1]?.[0]?.name).toEqual("New Item 1");
    expect(updates[1]?.[1]?.name).toEqual("Item 2");

    expect(spy).toHaveBeenCalledTimes(2);
  });

  test("pushing a new item triggers updates correctly", async () => {
    const Item = co.map({
      name: z.string(),
    });

    const TestList = co.list(Item);

    const group = Group.create();
    group.addMember("everyone", "writer");

    const list = TestList.create(
      [
        Item.create({ name: "Item 1" }, group),
        Item.create({ name: "Item 2" }, group),
      ],
      group,
    );

    const updates: Loaded<typeof TestList, { $each: true }>[] = [];
    const spy = vi.fn((list) => updates.push(list));

    const userB = await createJazzTestAccount();

    TestList.subscribe(
      list.$jazz.id,
      {
        loadAs: userB,
        resolve: {
          $each: true,
        },
      },
      (update) => {
        spy(update);

        // The update should be triggered only when the new item is loaded
        for (const item of update) {
          expect(item).toBeDefined();
        }
      },
    );

    await waitFor(() => expect(spy).toHaveBeenCalled());

    expect(spy).toHaveBeenCalledTimes(1);

    list.$jazz.push(Item.create({ name: "Item 3" }, group));

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    expect(spy).toHaveBeenCalledTimes(2);
  });
});

describe("co.list schema", () => {
  test("can access the inner schema of a co.list", () => {
    const Keywords = co.list(co.plainText());

    const keywords = Keywords.create([
      Keywords.element.create("hello"),
      Keywords.element.create("world"),
    ]);

    expect(keywords[0]?.toString()).toEqual("hello");
    expect(keywords[1]?.toString()).toEqual("world");
  });
});
