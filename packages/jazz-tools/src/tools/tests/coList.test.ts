import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, beforeEach, describe, expect, test, vi } from "vitest";
import { Account, Group, subscribeToCoValue, z } from "../index.js";
import {
  Loaded,
  activeAccountContext,
  co,
  coValueClassFromCoValueClassOrSchema,
  CoValueLoadingState,
} from "../internal.js";
import {
  createJazzTestAccount,
  runWithoutActiveAccount,
  setupJazzTestSync,
} from "../testing.js";
import {
  assertLoaded,
  expectValidationError,
  setupTwoNodes,
  waitFor,
} from "./utils.js";
import { setDefaultValidationMode } from "../implementation/zodSchema/validationSettings.js";

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
    expect(list.$jazz.createdAt).not.toBe(Number.MAX_SAFE_INTEGER);
  });

  test("construction with empty list", () => {
    const List = co.list(z.string());
    const list = List.create([]);
    expect(list.length).toBe(0);

    expect(list.$jazz.createdAt).not.toBe(Number.MAX_SAFE_INTEGER);
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

  test("create CoList with validation errors", () => {
    const List = co.list(z.string());
    expect(
      // @ts-expect-error - number is not a string
      () => List.create([2]),
    ).toThrow();
  });

  test("create CoList with validation errors with loose validation", () => {
    const List = co.list(z.string());
    expect(
      // @ts-expect-error - number is not a string
      () => List.create([2], { validation: "loose" }),
    ).not.toThrow();
  });

  test("CoList's items keep schema validation", () => {
    const Person = co.map({
      name: z.string(),
    });
    const List = co.list(Person);

    expectValidationError(
      // @ts-expect-error - number is not a string
      () => List.create([{ name: 2 }]),
    );

    const list = List.create([{ name: "John" }]);
    expect(list[0]?.name).toBe("John");

    expectValidationError(
      // @ts-expect-error - number is not a string
      () => list[0]?.$jazz.set("name", 2),
      [
        expect.objectContaining({
          message: "Invalid input: expected string, received number",
        }),
      ],
    );
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

  test("CoList keys can be iterated over just like an array's", () => {
    const TestList = co.list(z.string());
    const list = ["a", "b", "c", "d", "e"];
    const coList = TestList.create(list);
    const keys = [];
    for (const key in coList) {
      keys.push(key);
    }
    expect(keys).toEqual(Object.keys(list));
    expect(Object.keys(coList)).toEqual(Object.keys(list));
  });

  test("a CoList is structurally equal to an array", () => {
    const TestList = co.list(z.string());
    const list = ["a", "b", "c", "d", "e"];
    const coList = TestList.create(list);
    expect(coList).toEqual(list);
  });

  test("loaded CoList keeps schema validation", async () => {
    const Person = co.map({
      name: z.string(),
    });
    const List = co.list(Person);
    const list = List.create([{ name: "John" }]);

    const loadedList = await List.load(list.$jazz.id, {
      resolve: { $each: true },
    });
    assertLoaded(loadedList);
    expect(loadedList[0]?.name).toBe("John");

    expectValidationError(
      // @ts-expect-error - number is not a string
      () => loadedList[0]?.$jazz.set("name", 2),
      [
        expect.objectContaining({
          message: "Invalid input: expected string, received number",
        }),
      ],
    );
  });

  test("loaded CoList keeps schema validation", async () => {
    const Map = co.map({
      list: co.list(
        co.map({
          name: z.string(),
        }),
      ),
    });

    const map = Map.create({
      list: [{ name: "John" }, { name: "Jane" }],
    });

    const loadedMap = await Map.load(map.$jazz.id, {
      resolve: { list: { $each: true } },
    });
    assertLoaded(loadedMap);
    expect(loadedMap.list).toEqual([{ name: "John" }, { name: "Jane" }]);

    expectValidationError(
      // @ts-expect-error - number is not a person
      () => loadedMap.list.$jazz.push(2),
    );

    expectValidationError(
      // @ts-expect-error - number is not a string
      () => loadedMap.list[0]?.$jazz.set("name", 2),
    );
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

    test("assignment with validation errors", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });

      expectValidationError(
        // @ts-expect-error - number is not a string
        () => list.$jazz.set(1, 2),
        [
          expect.objectContaining({
            message: "Invalid input: expected string, received number",
          }),
        ],
      );
    });

    test("assignment with validation errors with loose validation", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });

      list.$jazz.set(
        1,
        // @ts-expect-error - number is not a string
        2,
        { validation: "loose" },
      );

      expect(list[1]).toBe(2);
    });

    test("assignment with ref using CoValue", () => {
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
      const originalIngredient = recipe[1];

      recipe.$jazz.set(1, Ingredient.create({ name: "margarine" }, me));
      expect(recipe[1]?.name).toBe("margarine");
      expect(recipe[1]?.$jazz.id).not.toBe(originalIngredient?.$jazz.id);
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
        recipe.$jazz.set(1, undefined as unknown as Loaded<typeof Ingredient>, {
          validation: "loose",
        });
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

    test("assignment with ref using JSON", () => {
      const Ingredient = co.map({
        name: z.string(),
      });

      const Recipe = co.list(Ingredient);

      const recipe = Recipe.create(
        [{ name: "bread" }, { name: "butter" }, { name: "onion" }],
        { owner: me },
      );
      const originalIngredient = recipe[1];

      recipe.$jazz.set(1, { name: "margarine" });
      expect(recipe[1]?.name).toBe("margarine");
      expect(recipe[1]?.$jazz.id).not.toBe(originalIngredient?.$jazz.id);
    });

    describe("push", () => {
      test("push into CoList of non-collaborative values", () => {
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

      test("push with validation errors", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });
        expectValidationError(
          // @ts-expect-error - number is not a string
          () => list.$jazz.push(2),
          [
            expect.objectContaining({
              message: "Invalid input: expected string, received number",
            }),
          ],
        );

        expectValidationError(
          // @ts-expect-error - number is not a string
          () => list.$jazz.push("test", 2),
        );

        expect(list).toEqual(["bread", "butter", "onion"]);
      });

      test("push with validation errors with loose validation", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });

        // @ts-expect-error - number is not a string
        list.$jazz.pushLoose(2);

        // @ts-expect-error - number is not a string
        list.$jazz.pushLoose("test", 2);

        expect(list).toEqual(["bread", "butter", "onion", 2, "test", 2]);
      });

      test("push CoValue into list of CoValues", () => {
        const Schema = co.list(co.plainText());
        const list = Schema.create(["bread", "butter", "onion"]);
        list.$jazz.push(Schema.element.create("cheese"));
        expect(list[3]?.toString()).toBe("cheese");
      });

      test("push JSON into list of CoValues", () => {
        const Schema = co.list(co.plainText());
        const list = Schema.create(["bread", "butter", "onion"]);
        list.$jazz.push("cheese");
        expect(list[3]?.toString()).toBe("cheese");
      });

      test("cannot push a shallowly-loaded CoValue into a deeply-loaded CoList", async () => {
        const Task = co.map({ title: co.plainText() });
        const TaskList = co.list(Task);

        const task = Task.create({ title: "Do the dishes" });
        const taskList = TaskList.create([]);

        const loadedTask = await Task.load(task.$jazz.id);
        const loadedTaskList = await TaskList.load(taskList.$jazz.id, {
          resolve: { $each: { title: true } },
        });

        assertLoaded(loadedTask);
        assertLoaded(loadedTaskList);
        // @ts-expect-error loadedTask may not have its `title` loaded
        loadedTaskList.$jazz.push(loadedTask);
        // In this case the title is loaded, so the assertion passes
        expect(loadedTaskList.at(-1)?.title.toString()).toBe("Do the dishes");
      });
    });

    describe("unshift", () => {
      test("add non-collaborative element at the beginning of the list", () => {
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

      test("add CoValue at the beginning of a CoValue CoList", () => {
        const Schema = co.list(co.plainText());
        const list = Schema.create(["bread", "butter", "onion"]);
        list.$jazz.unshift(Schema.element.create("lettuce"));
        expect(list[0]?.toString()).toBe("lettuce");
      });

      test("add JSON at the beginning of a CoValue CoList", () => {
        const Schema = co.list(co.plainText());
        const list = Schema.create(["bread", "butter", "onion"]);
        list.$jazz.unshift("lettuce");
        expect(list[0]?.toString()).toBe("lettuce");
      });

      test("unshift with validation errors", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });
        expectValidationError(
          // @ts-expect-error - number is not a string
          () => list.$jazz.unshift(2),
          [
            expect.objectContaining({
              message: "Invalid input: expected string, received number",
            }),
          ],
        );

        expectValidationError(
          // @ts-expect-error - number is not a string
          () => list.$jazz.unshift("test", 2),
        );

        expect(list).toEqual(["bread", "butter", "onion"]);
      });

      test("unshift with validation errors with loose validation", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });

        // @ts-expect-error - number is not a string
        list.$jazz.unshiftLoose(2);

        // @ts-expect-error - number is not a string
        list.$jazz.unshiftLoose("test", 2);

        expect(list).toEqual([2, "test", 2, "bread", "butter", "onion"]);
      });
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

      test("insert CoValue into a CoValue CoList", () => {
        const Schema = co.list(co.plainText());
        const list = Schema.create(["bread", "butter", "onion"]);
        list.$jazz.splice(1, 0, Schema.element.create("lettuce"));
        expect(list[1]?.toString()).toBe("lettuce");
      });

      test("insert JSON into a CoValue CoList", () => {
        const Schema = co.list(co.plainText());
        const list = Schema.create(["bread", "butter", "onion"]);
        list.$jazz.splice(1, 0, "lettuce");
        expect(list[1]?.toString()).toBe("lettuce");
      });

      test("splice with validation errors", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });
        expectValidationError(
          // @ts-expect-error - number is not a string
          () => list.$jazz.splice(1, 0, 2),
          [
            expect.objectContaining({
              message: "Invalid input: expected string, received number",
            }),
          ],
        );

        expectValidationError(
          // @ts-expect-error - number is not a string
          () => list.$jazz.splice(0, 1, "test", 2),
        );

        expect(list).toEqual(["bread", "butter", "onion"]);
      });

      test("spliceLoose removes and returns deleted items", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });

        const deleted = list.$jazz.spliceLoose(1, 1);

        expect(deleted).toEqual(["butter"]);
        expect(list.$jazz.raw.asArray()).toEqual(["bread", "onion"]);
      });

      test("spliceLoose with validation errors with loose validation", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });

        // @ts-expect-error - number is not a string
        list.$jazz.spliceLoose(1, 0, 2);

        // @ts-expect-error - number is not a string
        list.$jazz.spliceLoose(0, 1, "test", 2);

        expect(list).toEqual(["test", 2, 2, "butter", "onion"]);
      });
    });

    describe("remove", () => {
      describe("remove by index", () => {
        test("remove one item", () => {
          const list = TestList.create(["bread", "butter", "onion"]);

          expect(list.$jazz.remove(1)).toEqual(["butter"]);
          expect(list.$jazz.raw.asArray()).toEqual(["bread", "onion"]);
        });

        test("remove multiple items", () => {
          const list = TestList.create(["bread", "butter", "onion"]);

          expect(list.$jazz.remove(0, 2)).toEqual(["bread", "onion"]);
          expect(list.$jazz.raw.asArray()).toEqual(["butter"]);
        });

        test("ignores out-of-bound indices", () => {
          const list = TestList.create(["bread", "butter", "onion"]);

          expect(list.$jazz.remove(4, -1, 1)).toEqual(["butter"]);
          expect(list.$jazz.raw.asArray()).toEqual(["bread", "onion"]);
        });
      });

      describe("remove by predicate", () => {
        test("removes elements matching the predicate", () => {
          const list = TestList.create(["bread", "butter", "onion"]);

          expect(list.$jazz.remove((item) => item === "butter")).toEqual([
            "butter",
          ]);
          expect(list.$jazz.raw.asArray()).toEqual(["bread", "onion"]);
        });

        test("the predicate is called with the item, index and the coList", () => {
          const list = TestList.create(["bread", "butter", "onion"]);

          expect(
            list.$jazz.remove(
              (item, index, coList) => index > 0 && index < coList.length - 1,
            ),
          ).toEqual(["butter"]);
          expect(list.$jazz.raw.asArray()).toEqual(["bread", "onion"]);
        });
      });

      // CoListItem's type was being incorrectly inferred as nullable when using ensureLoaded
      // on loaded CoLists. Keeping this test to ensure it doesn't regress.
      test("removes elements from loaded CoLists", async () => {
        const NestedList = co.list(co.map({ title: z.string() }));
        const list = NestedList.create([
          { title: "bread" },
          { title: "butter" },
          { title: "onion" },
        ]);
        const bread = list[0];
        const butter = list[1];
        const onion = list[2];

        const shallowlyLoadedList = await NestedList.load(list.$jazz.id);
        assertLoaded(shallowlyLoadedList);

        const loadedList = await shallowlyLoadedList.$jazz.ensureLoaded({
          resolve: { $each: true },
        });

        expect(
          loadedList.$jazz.remove((item) => item.title === "butter"),
        ).toEqual([butter]);
        expect(shallowlyLoadedList[0]).toEqual(bread);
        expect(shallowlyLoadedList[1]).toEqual(onion);
      });
    });

    describe("retain", () => {
      test("retains elements matching the predicate", () => {
        const list = TestList.create(["bread", "butter", "onion"]);

        expect(list.$jazz.retain((item) => item === "butter")).toEqual([
          "bread",
          "onion",
        ]);
        expect(list.$jazz.raw.asArray()).toEqual(["butter"]);
      });

      test("the predicate is called with the item, index and the coList", () => {
        const list = TestList.create(["bread", "butter", "onion"]);

        expect(
          list.$jazz.retain(
            (item, index, coList) => index > 0 && index < coList.length - 1,
          ),
        ).toEqual(["bread", "onion"]);
        expect(list.$jazz.raw.asArray()).toEqual(["butter"]);
      });
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

      map.$jazz.set(
        "list",
        map.list?.filter((item) => item !== "butter"),
      );

      expect(map.list?.$jazz.raw.asArray()).toEqual(["bread", "onion"]);
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

      list.$jazz.set(0, list[0]?.filter((item) => item !== "butter") ?? []);

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

  test("applyDiff with reference values using CoValues", () => {
    const TicTacToeRow = co.list(z.string());
    const TicTacToeBoard = co.list(TicTacToeRow);

    const row1 = TicTacToeRow.create(["X", "O", ""], { owner: me });
    const row2 = TicTacToeRow.create(["", "X", "O"], { owner: me });
    const row3 = TicTacToeRow.create(["O", "O", ""], { owner: me });
    const winningRow = TicTacToeRow.create(["O", "O", "X"], { owner: me });

    const list = TicTacToeBoard.create([row1, row2], { owner: me });

    // Test adding reference items
    list.$jazz.applyDiff([row1, row2, row3]);
    expect(list.length).toBe(3);
    expect(list[2]?.toJSON()).toEqual(["O", "O", ""]);

    // Test replacing reference items
    list.$jazz.applyDiff([row1, row2, winningRow]);
    expect(list.length).toBe(3);
    expect(list[2]?.toJSON()).toEqual(["O", "O", "X"]);
    // elements with different $jazz.id are replaced
    expect(list[0]?.$jazz.id).toBe(row1?.$jazz.id);
    expect(list[1]?.$jazz.id).toBe(row2?.$jazz.id);
    expect(list[2]?.$jazz.id).not.toBe(row3?.$jazz.id);

    // Test removing reference items
    list.$jazz.applyDiff([row1, row3]);
    expect(list.length).toBe(2);
    expect(list[0]?.toJSON()).toEqual(["X", "O", ""]);
    expect(list[0]?.$jazz.id).toBe(row1?.$jazz.id);
    expect(list[1]?.toJSON()).toEqual(["O", "O", ""]);
    expect(list[1]?.$jazz.id).not.toBe(row2?.$jazz.id);

    // Test empty list
    list.$jazz.applyDiff([]);
    expect(list.$jazz.raw.asArray()).toEqual([]);
  });

  test("applyDiff with reference values using JSON", () => {
    const TicTacToeRow = co.list(z.string());
    const TicTacToeBoard = co.list(TicTacToeRow);

    const row1 = ["X", "O", ""];
    const row2 = ["", "X", "O"];
    const row3 = ["O", "O", ""];
    const winningRow = ["O", "O", "X"];

    const list = TicTacToeBoard.create([row1, row2], { owner: me });
    const originalRow1 = list[0];
    const originalRow2 = list[1];
    const originalRow3 = list[2];

    // Test adding reference items
    list.$jazz.applyDiff([row1, row2, row3]);
    expect(list.length).toBe(3);
    expect(list[2]?.toJSON()).toEqual(["O", "O", ""]);

    // Test replacing reference items
    list.$jazz.applyDiff([row1, row2, winningRow]);
    expect(list.length).toBe(3);
    expect(list[2]?.toJSON()).toEqual(["O", "O", "X"]);
    // All elements are replaced because new JSON values are set
    expect(list[0]?.$jazz.id).not.toBe(originalRow1?.$jazz.id);
    expect(list[1]?.$jazz.id).not.toBe(originalRow2?.$jazz.id);
    expect(list[2]?.$jazz.id).not.toBe(originalRow3?.$jazz.id);

    // Test removing reference items
    list.$jazz.applyDiff([row1, row3]);
    expect(list.length).toBe(2);
    expect(list[0]?.toJSON()).toEqual(["X", "O", ""]);
    expect(list[0]?.$jazz.id).not.toBe(originalRow1?.$jazz.id);
    expect(list[1]?.toJSON()).toEqual(["O", "O", ""]);
    expect(list[1]?.$jazz.id).not.toBe(originalRow2?.$jazz.id);

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

  test("applyDiff respects schema validation in strict mode", () => {
    const Person = co.map({
      name: z.string(),
    });
    const PersonList = co.list(Person);

    const list = PersonList.create([{ name: "John" }], { owner: me });

    expect(list.length).toBe(1);

    expectValidationError(() =>
      list.$jazz.applyDiff([
        { name: "John" },
        { name: 123 as unknown as string },
      ]),
    );

    // The list should remain unchanged after failed validation
    expect(list.length).toBe(1);
    expect(list[0]?.name).toBe("John");
  });

  test("applyDiff respects local loose validation mode", () => {
    const Person = co.map({
      name: z.string(),
    });
    const PersonList = co.list(Person);

    const list = PersonList.create([{ name: "John" }], { owner: me });

    list.$jazz.applyDiff(
      [
        { name: "John" },
        {
          name: 123 as unknown as string,
        },
      ],
      { validation: "loose" },
    );

    // Invalid data is accepted when validation is globally loose
    expect(list.length).toBe(2);
    expect(list[0]?.name).toBe("John");
    expect(list[1]?.name).toBe(123 as unknown as string);
  });

  test("applyDiff respects global loose validation mode", () => {
    const Person = co.map({
      name: z.string(),
    });
    const PersonList = co.list(Person);

    const list = PersonList.create([{ name: "John" }], { owner: me });

    setDefaultValidationMode("loose");

    try {
      list.$jazz.applyDiff([
        { name: "John" },
        {
          name: 123 as unknown as string,
        },
      ]);

      // Invalid data is accepted when validation is globally loose
      expect(list.length).toBe(2);
      expect(list[0]?.name).toBe("John");
      expect(list[1]?.name).toBe(123 as unknown as string);
    } finally {
      setDefaultValidationMode("strict");
    }
  });

  test("applyDiff should emit a single update", () => {
    const TestMap = co.map({
      type: z.string(),
    });

    const TestList = co.list(TestMap);

    const bread = TestMap.create({ type: "bread" }, me);
    const butter = TestMap.create({ type: "butter" }, me);
    const onion = TestMap.create({ type: "onion" }, me);

    const list = TestList.create([bread, butter, onion], me);

    const updateFn = vi.fn();

    const unsubscribe = TestList.subscribe(
      list.$jazz.id,
      {
        resolve: {
          $each: true,
        },
      },
      updateFn,
    );

    updateFn.mockClear();

    list.$jazz.applyDiff([bread]);

    expect(updateFn).toHaveBeenCalledTimes(1);

    unsubscribe();
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

    assertLoaded(loadedPets);

    const petReference = loadedPets.$jazz.refs[0];
    assert(petReference);
    expect(petReference.id).toBe(pets[0]?.$jazz.id);

    const dog = await petReference.load();

    assertLoaded(dog);
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

    expect(loadedMap).not.toBe(CoValueLoadingState.UNAVAILABLE);
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

    assert(updates[0]?.[0]);
    assertLoaded(updates[0][0]);
    expect(updates[0][0].name).toEqual("Item 1");
    assert(updates[0]?.[1]);
    assertLoaded(updates[0][1]);
    expect(updates[0]?.[1]?.name).toEqual("Item 2");

    list[0]!.$jazz.set("name", "Updated Item 1");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

    assert(updates[1]?.[0]);
    assertLoaded(updates[1][0]);
    expect(updates[1][0].name).toEqual("Updated Item 1");
    assert(updates[1]?.[1]);
    assertLoaded(updates[1][1]);
    expect(updates[1][1].name).toEqual("Item 2");

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

    assert(updates[0]?.[0]);
    assertLoaded(updates[0][0]);
    expect(updates[0][0].name).toEqual("Item 1");
    assert(updates[0]?.[1]);
    assertLoaded(updates[0][1]);
    expect(updates[0][1].name).toEqual("Item 2");

    expect(spy).toHaveBeenCalledTimes(1);

    list[0]!.$jazz.set("name", "Updated Item 1");

    expect(spy).toHaveBeenCalledTimes(2);

    assert(updates[1]?.[0]);
    assertLoaded(updates[1][0]);
    expect(updates[1][0].name).toEqual("Updated Item 1");
    assert(updates[1]?.[1]);
    assertLoaded(updates[1][1]);
    expect(updates[1][1].name).toEqual("Item 2");

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

    await waitFor(() => {
      assert(updates[0]?.[0]);
      assertLoaded(updates[0][0]);
      expect(updates[0][0].name).toEqual("Item 1");
      assert(updates[0]?.[1]);
      assertLoaded(updates[0][1]);
      expect(updates[0][1].name).toEqual("Item 2");
    });

    list[0]!.$jazz.set("name", "Updated Item 1");

    await waitFor(() => expect(spy).toHaveBeenCalledTimes(4));

    assert(updates[1]?.[0]);
    assertLoaded(updates[1][0]);
    expect(updates[1][0].name).toEqual("Updated Item 1");
    assert(updates[1]?.[1]);
    assertLoaded(updates[1][1]);
    expect(updates[1][1].name).toEqual("Item 2");

    expect(spy).toHaveBeenCalledTimes(4);
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

        // The update should be triggered when the new item is loaded
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

    const person = Person.create(
      {
        name: "John",
        age: 20,
        dogs: Person.shape.dogs.create([
          { name: "Rex", breed: "Labrador" },
          { name: "Fido", breed: "Poodle" },
        ]),
      },
      Group.create().makePublic(),
    );

    const bob = await createJazzTestAccount();

    const loadedPerson = await Person.load(person.$jazz.id, {
      resolve: { dogs: { $onError: "catch" } },
      loadAs: bob,
    });

    assertLoaded(loadedPerson);
    expect(loadedPerson.name).toBe("John");
    expect(loadedPerson.dogs.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAUTHORIZED,
    );
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

  test("supports writer: 'appendOnly' in schema definition and writes it to header", () => {
    const RestrictedList = co
      .list(z.string())
      .withPermissions({ writer: "appendOnly" });
    const list = RestrictedList.create(["seed"]);

    expect(list.$jazz.raw.core.verified.header.ruleset).toMatchObject({
      type: "ownedByGroup",
      restrictDeletion: true,
    });
  });

  test("writers can append but cannot delete or replace when writer: 'appendOnly' is enabled", async () => {
    const { clientAccount: alice, serverAccount: bob } = await setupTwoNodes();

    const ownerGroup = Group.create(alice);
    ownerGroup.addMember(bob, "writer");

    const RestrictedList = co
      .list(z.string())
      .withPermissions({ writer: "appendOnly" });
    const list = RestrictedList.create(["seed"], { owner: ownerGroup });
    const loadedAsBob = await RestrictedList.load(list.$jazz.id, {
      loadAs: bob,
    });

    assertLoaded(loadedAsBob);

    loadedAsBob.$jazz.push("writer-append");
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["seed", "writer-append"]);
    });

    loadedAsBob.$jazz.remove(0);
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["seed", "writer-append"]);
    });

    loadedAsBob.$jazz.set(1, "writer-replace-attempt");
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["seed", "writer-append"]);
    });
  });

  test("managers can always remove and mutate when writer: 'appendOnly' is enabled", async () => {
    const { clientAccount: alice, serverAccount: bob } = await setupTwoNodes();

    const ownerGroup = Group.create(alice);
    ownerGroup.addMember(bob, "manager");

    const RestrictedList = co
      .list(z.string())
      .withPermissions({ writer: "appendOnly" });
    const list = RestrictedList.create(["seed", "second"], {
      owner: ownerGroup,
    });
    const loadedAsBob = await RestrictedList.load(list.$jazz.id, {
      loadAs: bob,
    });

    assertLoaded(loadedAsBob);

    loadedAsBob.$jazz.remove(0);
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["second"]);
    });

    loadedAsBob.$jazz.set(0, "manager-updated");
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["manager-updated"]);
    });
  });

  test("admins can always remove and mutate when writer: 'appendOnly' is enabled", async () => {
    const { clientAccount: alice, serverAccount: bob } = await setupTwoNodes();

    const ownerGroup = Group.create(alice);
    ownerGroup.addMember(bob, "admin");

    const RestrictedList = co
      .list(z.string())
      .withPermissions({ writer: "appendOnly" });
    const list = RestrictedList.create(["seed", "second"], {
      owner: ownerGroup,
    });
    const loadedAsBob = await RestrictedList.load(list.$jazz.id, {
      loadAs: bob,
    });

    assertLoaded(loadedAsBob);

    loadedAsBob.$jazz.remove(0);
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["second"]);
    });

    loadedAsBob.$jazz.set(0, "admin-updated");
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["admin-updated"]);
    });
  });
});

describe("lastUpdatedAt", () => {
  test("empty list last updated time", () => {
    const emptyList = co.list(z.number()).create([]);

    expect(emptyList.$jazz.lastUpdatedAt).not.toEqual(0);
    expect(emptyList.$jazz.lastUpdatedAt).toEqual(emptyList.$jazz.createdAt);
  });

  test("last update should change on push", async () => {
    const list = co.list(z.string()).create(["John"]);

    expect(list.$jazz.lastUpdatedAt).not.toEqual(0);

    const updatedAt = list.$jazz.lastUpdatedAt;

    await new Promise((r) => setTimeout(r, 10));
    list.$jazz.push("Jane");

    expect(list.$jazz.lastUpdatedAt).not.toEqual(updatedAt);
  });
});

describe("CoList proxy traps", () => {
  test(".values() returns the same values as Object.values()", () => {
    const TestList = co.list(z.string());
    const list = TestList.create([]);
    list.$jazz.push("bread");
    list.$jazz.push("butter");
    list.$jazz.push("onion");

    const valuesFromMethod = [...list.values()];
    const valuesFromObject = Object.values(list);
    expect(valuesFromMethod).toEqual(valuesFromObject);
    expect(valuesFromMethod).toEqual(["bread", "butter", "onion"]);
  });

  test(".values().map() returns the same values as .map()", () => {
    const TestList = co.list(z.string());
    const list = TestList.create([]);
    list.$jazz.push("bread");
    list.$jazz.push("butter");
    list.$jazz.push("onion");

    const valuesFromMethod = [...list.values().map((v) => v.toUpperCase())];
    const valuesFromObject = list.map((v) => v.toUpperCase());

    expect(valuesFromMethod).toEqual(valuesFromObject);
    expect(valuesFromMethod).toEqual(["BREAD", "BUTTER", "ONION"]);
  });

  test(".keys() returns numeric indices", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["bread", "butter", "onion"]);

    const keys = [...list.keys()];

    expect(keys).toEqual([0, 1, 2]);
  });

  test(".entries() returns index-value pairs", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["bread", "butter", "onion"]);

    const entries = [...list.entries()];

    expect(entries).toEqual([
      [0, "bread"],
      [1, "butter"],
      [2, "onion"],
    ]);
  });

  test("for...of iteration works correctly", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["bread", "butter", "onion"]);

    const items: string[] = [];
    for (const item of list) {
      items.push(item);
    }

    expect(items).toEqual(["bread", "butter", "onion"]);
  });

  test("spread operator works correctly", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["bread", "butter", "onion"]);

    const items = [...list];

    expect(items).toEqual(["bread", "butter", "onion"]);
  });

  test("Array.from works correctly", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["bread", "butter", "onion"]);

    const items = Array.from(list);

    expect(items).toEqual(["bread", "butter", "onion"]);
  });

  test(".values() works with CoValue references", () => {
    const Dog = co.map({
      name: z.string(),
    });
    const DogList = co.list(Dog);

    const list = DogList.create([
      { name: "Rex" },
      { name: "Fido" },
      { name: "Buddy" },
    ]);

    const valuesFromMethod = [...list.values()];
    const valuesFromObject = Object.values(list);

    expect(valuesFromMethod.length).toBe(3);
    expect(valuesFromMethod.map((d) => d?.name)).toEqual([
      "Rex",
      "Fido",
      "Buddy",
    ]);
    expect(valuesFromMethod).toEqual(valuesFromObject);
  });

  test(".values() works after ensureLoaded", async () => {
    const Task = co.map({
      title: z.string(),
    });
    const TaskList = co.list(Task);

    const list = TaskList.create([
      { title: "Task 1" },
      { title: "Task 2" },
      { title: "Task 3" },
    ]);

    const loadedList = await list.$jazz.ensureLoaded({
      resolve: { $each: true },
    });

    const valuesFromMethod = [...loadedList.values()];
    const valuesFromObject = Object.values(loadedList);

    expect(valuesFromMethod.length).toBe(3);
    expect(valuesFromMethod.map((t) => t.title)).toEqual([
      "Task 1",
      "Task 2",
      "Task 3",
    ]);
    expect(valuesFromMethod.map((t) => t.$jazz.id)).toEqual(
      valuesFromObject.map((t) => t.$jazz.id),
    );
  });

  test(".values() works on remotely loaded list", async () => {
    const Task = co.map({
      title: z.string(),
    });
    const TaskList = co.list(Task);

    const group = Group.create();
    group.addMember("everyone", "writer");

    const list = TaskList.create(
      [{ title: "Task 1" }, { title: "Task 2" }, { title: "Task 3" }],
      group,
    );

    const userB = await createJazzTestAccount();

    const loadedList = await TaskList.load(list.$jazz.id, {
      resolve: { $each: true },
      loadAs: userB,
    });

    assertLoaded(loadedList);

    const valuesFromMethod = [...loadedList.values()];
    const valuesFromObject = Object.values(loadedList);

    expect(valuesFromMethod.length).toBe(3);
    expect(valuesFromMethod.map((t) => t.title)).toEqual([
      "Task 1",
      "Task 2",
      "Task 3",
    ]);
    expect(valuesFromMethod.map((t) => t.$jazz.id)).toEqual(
      valuesFromObject.map((t) => t.$jazz.id),
    );
  });

  test("iterator methods work on empty list", () => {
    const TestList = co.list(z.string());
    const list = TestList.create([]);

    expect([...list.values()]).toEqual([]);
    expect([...list.keys()]).toEqual([]);
    expect([...list.entries()]).toEqual([]);
    expect([...list]).toEqual([]);
  });

  test("Object.getOwnPropertyDescriptors returns correct descriptors", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["a", "b", "c"]);

    const descriptors = Object.getOwnPropertyDescriptors(list);

    // Check numeric index descriptors
    expect(descriptors["0"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: "a",
    });
    expect(descriptors["1"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: "b",
    });
    expect(descriptors["2"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: "c",
    });

    // Check length descriptor
    expect(descriptors["length"]).toEqual({
      enumerable: false,
      configurable: false,
      writable: true,
      value: 3,
    });

    // Verify only expected enumerable keys
    const enumerableKeys = Object.keys(descriptors).filter(
      (key) => descriptors[key]?.enumerable,
    );
    expect(enumerableKeys.sort()).toEqual(["0", "1", "2"]);
  });

  test("Object.getOwnPropertyDescriptors returns the resolved references", () => {
    const TestList = co.list(co.map({ name: z.string() }));
    const list = TestList.create([{ name: "a" }, { name: "b" }, { name: "c" }]);

    const descriptors = Object.getOwnPropertyDescriptors(list);

    // Check numeric index descriptors
    expect(descriptors["0"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: list[0],
    });
    expect(descriptors["1"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: list[1],
    });
    expect(descriptors["2"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: list[2],
    });
  });

  test("setting the lenght to 0 has no effect", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["a", "b", "c"]);

    list.length = 0;

    expect(list.length).toBe(3);
    expect(list[0]).toBe("a");
    expect(list[1]).toBe("b");
    expect(list[2]).toBe("c");
  });
});

describe("CoList proxy traps", () => {
  test(".values() returns the same values as Object.values()", () => {
    const TestList = co.list(z.string());
    const list = TestList.create([]);
    list.$jazz.push("bread");
    list.$jazz.push("butter");
    list.$jazz.push("onion");

    const valuesFromMethod = [...list.values()];
    const valuesFromObject = Object.values(list);
    expect(valuesFromMethod).toEqual(valuesFromObject);
    expect(valuesFromMethod).toEqual(["bread", "butter", "onion"]);
  });

  test(".values().map() returns the same values as .map()", () => {
    const TestList = co.list(z.string());
    const list = TestList.create([]);
    list.$jazz.push("bread");
    list.$jazz.push("butter");
    list.$jazz.push("onion");

    const valuesFromMethod = [...list.values().map((v) => v.toUpperCase())];
    const valuesFromObject = list.map((v) => v.toUpperCase());

    expect(valuesFromMethod).toEqual(valuesFromObject);
    expect(valuesFromMethod).toEqual(["BREAD", "BUTTER", "ONION"]);
  });

  test(".keys() returns numeric indices", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["bread", "butter", "onion"]);

    const keys = [...list.keys()];

    expect(keys).toEqual([0, 1, 2]);
  });

  test(".entries() returns index-value pairs", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["bread", "butter", "onion"]);

    const entries = [...list.entries()];

    expect(entries).toEqual([
      [0, "bread"],
      [1, "butter"],
      [2, "onion"],
    ]);
  });

  test("for...of iteration works correctly", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["bread", "butter", "onion"]);

    const items: string[] = [];
    for (const item of list) {
      items.push(item);
    }

    expect(items).toEqual(["bread", "butter", "onion"]);
  });

  test("spread operator works correctly", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["bread", "butter", "onion"]);

    const items = [...list];

    expect(items).toEqual(["bread", "butter", "onion"]);
  });

  test("Array.from works correctly", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["bread", "butter", "onion"]);

    const items = Array.from(list);

    expect(items).toEqual(["bread", "butter", "onion"]);
  });

  test(".values() works with CoValue references", () => {
    const Dog = co.map({
      name: z.string(),
    });
    const DogList = co.list(Dog);

    const list = DogList.create([
      { name: "Rex" },
      { name: "Fido" },
      { name: "Buddy" },
    ]);

    const valuesFromMethod = [...list.values()];
    const valuesFromObject = Object.values(list);

    expect(valuesFromMethod.length).toBe(3);
    expect(valuesFromMethod.map((d) => d?.name)).toEqual([
      "Rex",
      "Fido",
      "Buddy",
    ]);
    expect(valuesFromMethod).toEqual(valuesFromObject);
  });

  test(".values() works after ensureLoaded", async () => {
    const Task = co.map({
      title: z.string(),
    });
    const TaskList = co.list(Task);

    const list = TaskList.create([
      { title: "Task 1" },
      { title: "Task 2" },
      { title: "Task 3" },
    ]);

    const loadedList = await list.$jazz.ensureLoaded({
      resolve: { $each: true },
    });

    const valuesFromMethod = [...loadedList.values()];
    const valuesFromObject = Object.values(loadedList);

    expect(valuesFromMethod.length).toBe(3);
    expect(valuesFromMethod.map((t) => t.title)).toEqual([
      "Task 1",
      "Task 2",
      "Task 3",
    ]);
    expect(valuesFromMethod.map((t) => t.$jazz.id)).toEqual(
      valuesFromObject.map((t) => t.$jazz.id),
    );
  });

  test(".values() works on remotely loaded list", async () => {
    const Task = co.map({
      title: z.string(),
    });
    const TaskList = co.list(Task);

    const group = Group.create();
    group.addMember("everyone", "writer");

    const list = TaskList.create(
      [{ title: "Task 1" }, { title: "Task 2" }, { title: "Task 3" }],
      group,
    );

    const userB = await createJazzTestAccount();

    const loadedList = await TaskList.load(list.$jazz.id, {
      resolve: { $each: true },
      loadAs: userB,
    });

    assertLoaded(loadedList);

    const valuesFromMethod = [...loadedList.values()];
    const valuesFromObject = Object.values(loadedList);

    expect(valuesFromMethod.length).toBe(3);
    expect(valuesFromMethod.map((t) => t.title)).toEqual([
      "Task 1",
      "Task 2",
      "Task 3",
    ]);
    expect(valuesFromMethod.map((t) => t.$jazz.id)).toEqual(
      valuesFromObject.map((t) => t.$jazz.id),
    );
  });

  test("iterator methods work on empty list", () => {
    const TestList = co.list(z.string());
    const list = TestList.create([]);

    expect([...list.values()]).toEqual([]);
    expect([...list.keys()]).toEqual([]);
    expect([...list.entries()]).toEqual([]);
    expect([...list]).toEqual([]);
  });

  test("Object.getOwnPropertyDescriptors returns correct descriptors", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["a", "b", "c"]);

    const descriptors = Object.getOwnPropertyDescriptors(list);

    // Check numeric index descriptors
    expect(descriptors["0"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: "a",
    });
    expect(descriptors["1"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: "b",
    });
    expect(descriptors["2"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: "c",
    });

    // Check length descriptor
    expect(descriptors["length"]).toEqual({
      enumerable: false,
      configurable: false,
      writable: true,
      value: 3,
    });

    // Verify only expected enumerable keys
    const enumerableKeys = Object.keys(descriptors).filter(
      (key) => descriptors[key]?.enumerable,
    );
    expect(enumerableKeys.sort()).toEqual(["0", "1", "2"]);
  });

  test("Object.getOwnPropertyDescriptors returns the resolved references", () => {
    const TestList = co.list(co.map({ name: z.string() }));
    const list = TestList.create([{ name: "a" }, { name: "b" }, { name: "c" }]);

    const descriptors = Object.getOwnPropertyDescriptors(list);

    // Check numeric index descriptors
    expect(descriptors["0"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: list[0],
    });
    expect(descriptors["1"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: list[1],
    });
    expect(descriptors["2"]).toEqual({
      enumerable: true,
      configurable: true,
      writable: false,
      value: list[2],
    });
  });

  test("setting the lenght to 0 has no effect", () => {
    const TestList = co.list(z.string());
    const list = TestList.create(["a", "b", "c"]);

    list.length = 0;

    expect(list.length).toBe(3);
    expect(list[0]).toBe("a");
    expect(list[1]).toBe("b");
    expect(list[2]).toBe("c");
  });
});
