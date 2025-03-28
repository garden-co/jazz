import { cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { describe, expect, test } from "vitest";
import {
  Account,
  CoList,
  CoMap,
  Group,
  co,
  createJazzContextFromExistingCredentials,
  isControlledAccount,
} from "../index.js";
import { randomSessionProvider } from "../internal.js";

const connectedPeers = cojsonInternals.connectedPeers;

const Crypto = await WasmCrypto.create();

describe("Simple CoList operations", async () => {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  class TestList extends CoList.Of(co.string) {}

  const list = TestList.create(["bread", "butter", "onion"], { owner: me });

  test("Construction", () => {
    expect(list[0]).toBe("bread");
    expect(list[1]).toBe("butter");
    expect(list[2]).toBe("onion");
    expect(list._raw.asArray()).toEqual(["bread", "butter", "onion"]);
    expect(list.length).toBe(3);
    expect(list.map((item) => item.toUpperCase())).toEqual([
      "BREAD",
      "BUTTER",
      "ONION",
    ]);
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
      list[1] = "margarine";
      expect(list._raw.asArray()).toEqual(["bread", "margarine", "onion"]);
      expect(list[1]).toBe("margarine");
    });

    test("assignment with ref", () => {
      class Ingredient extends CoMap {
        name = co.string;
      }

      class Recipe extends CoList.Of(co.ref(Ingredient)) {}

      const recipe = Recipe.create(
        [
          Ingredient.create({ name: "bread" }, me),
          Ingredient.create({ name: "butter" }, me),
          Ingredient.create({ name: "onion" }, me),
        ],
        { owner: me },
      );

      recipe[1] = Ingredient.create({ name: "margarine" }, me);
      expect(recipe[1]?.name).toBe("margarine");
    });

    test("assign null on a required ref", () => {
      class Ingredient extends CoMap {
        name = co.string;
      }

      class Recipe extends CoList.Of(co.ref(Ingredient)) {}

      const recipe = Recipe.create(
        [
          Ingredient.create({ name: "bread" }, me),
          Ingredient.create({ name: "butter" }, me),
          Ingredient.create({ name: "onion" }, me),
        ],
        { owner: me },
      );

      expect(() => {
        recipe[1] = null;
      }).toThrow("Cannot set required reference 1 to null");

      expect(recipe[1]?.name).toBe("butter");
    });

    test("assign null on an optional ref", () => {
      class Ingredient extends CoMap {
        name = co.string;
      }

      class Recipe extends CoList.Of(co.optional.ref(Ingredient)) {}

      const recipe = Recipe.create(
        [
          Ingredient.create({ name: "bread" }, me),
          Ingredient.create({ name: "butter" }, me),
          Ingredient.create({ name: "onion" }, me),
        ],
        { owner: me },
      );

      recipe[1] = null;
      expect(recipe[1]).toBe(null);
    });

    test("push", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });
      list.push("cheese");
      expect(list[3]).toBe("cheese");
      expect(list._raw.asArray()).toEqual([
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
      list.unshift("lettuce");
      expect(list[0]).toBe("lettuce");
      expect(list._raw.asArray()).toEqual([
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
      expect(list.pop()).toBe("onion");
      expect(list.length).toBe(2);
      expect(list._raw.asArray()).toEqual(["bread", "butter"]);
    });

    test("shift", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });
      expect(list.shift()).toBe("bread");
      expect(list.length).toBe(2);
      expect(list._raw.asArray()).toEqual(["butter", "onion"]);
    });

    describe("splice", () => {
      test("insert after 1st item with 1 item removed", () => {
        const list = TestList.create(["bread", "butter", "onion"], {
          owner: me,
        });
        list.splice(1, 1, "salt", "pepper");
        expect(list.length).toBe(4);
        expect(list._raw.asArray()).toEqual([
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
        list.splice(0, 0, "salt", "pepper");
        expect(list.length).toBe(5);
        expect(list._raw.asArray()).toEqual([
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
        list.splice(1, 0, "salt", "pepper");
        expect(list.length).toBe(5);
        expect(list._raw.asArray()).toEqual([
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
        list.splice(2, 0, "salt", "pepper");
        expect(list.length).toBe(5);
        expect(list._raw.asArray()).toEqual([
          "bread",
          "butter",
          "salt",
          "pepper",
          "onion",
        ]);
      });
    });

    test("sort", () => {
      const list = TestList.create(
        ["hedgehog", "giraffe", "iguana", "flamingo"],
        { owner: me },
      );

      list.sort();
      expect(list._raw.asArray()).toEqual([
        "flamingo",
        "giraffe",
        "hedgehog",
        "iguana",
      ]);

      list.sort((a, b) => b.localeCompare(a));
      expect(list._raw.asArray()).toEqual([
        "iguana",
        "hedgehog",
        "giraffe",
        "flamingo",
      ]);
    });

    test("sort list of refs", async () => {
      class Message extends CoMap {
        text = co.string;
      }

      class Chat extends CoList.Of(co.ref(Message)) {}

      const chat = Chat.create(
        [
          Message.create({ text: "world" }, { owner: me }),
          Message.create({ text: "hello" }, { owner: me }),
        ],
        { owner: me },
      );

      chat.sort((a, b) => a!.text.localeCompare(b!.text));
      expect(chat.map((m) => m!.text)).toEqual(["hello", "world"]);

      chat.push(Message.create({ text: "beans on toast" }, { owner: me }));
      chat.sort((a, b) => a!.text.localeCompare(b!.text));
      expect(chat.map((m) => m!.text)).toEqual([
        "beans on toast",
        "hello",
        "world",
      ]);
    });

    test("applyDiff", () => {
      const list = TestList.create(["bread", "butter", "onion"], {
        owner: me,
      });
      // replace
      list.applyDiff(["bread", "margarine", "onion"]);
      expect(list._raw.asArray()).toEqual(["bread", "margarine", "onion"]);
      // delete
      list.applyDiff(["bread", "onion"]);
      expect(list._raw.asArray()).toEqual(["bread", "onion"]);
      // insert multiple
      list.applyDiff(["bread", "margarine", "onion", "cheese"]);
      expect(list._raw.asArray()).toEqual([
        "bread",
        "margarine",
        "onion",
        "cheese",
      ]);
    });

    test("filter + assign to coMap", () => {
      class TestMap extends CoMap {
        list = co.ref(TestList);
      }

      const map = TestMap.create(
        {
          list: TestList.create(["bread", "butter", "onion"], {
            owner: me,
          }),
        },
        { owner: me },
      );

      expect(() => {
        // @ts-expect-error
        map.list = map.list?.filter((item) => item !== "butter");
      }).toThrow("Cannot set reference list to a non-CoValue. Got bread,onion");

      expect(map.list?._raw.asArray()).toEqual(["bread", "butter", "onion"]);
    });

    test("filter + assign to CoList", () => {
      class TestListOfLists extends CoList.Of(co.ref(TestList)) {}

      const list = TestListOfLists.create(
        [
          TestList.create(["bread", "butter", "onion"], {
            owner: me,
          }),
        ],
        { owner: me },
      );

      expect(() => {
        // @ts-expect-error
        list[0] = list[0]?.filter((item) => item !== "butter");
      }).toThrow("Cannot set reference 0 to a non-CoValue. Got bread,onion");

      expect(list[0]?._raw.asArray()).toEqual(["bread", "butter", "onion"]);
    });
  });
});

describe("CoList applyDiff operations", async () => {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  test("applyDiff with primitive values", () => {
    class StringList extends CoList.Of(co.string) {}
    const list = StringList.create(["a", "b", "c"], { owner: me });

    // Test adding items
    list.applyDiff(["a", "b", "c", "d", "e"]);
    expect(list._raw.asArray()).toEqual(["a", "b", "c", "d", "e"]);

    // Test removing items
    list.applyDiff(["a", "c", "e"]);
    expect(list._raw.asArray()).toEqual(["a", "c", "e"]);

    // Test replacing items
    list.applyDiff(["x", "y", "z"]);
    expect(list._raw.asArray()).toEqual(["x", "y", "z"]);

    // Test empty list
    list.applyDiff([]);
    expect(list._raw.asArray()).toEqual([]);
  });

  test("applyDiff with reference values", () => {
    class NestedItem extends CoList.Of(co.string) {
      get value() {
        return this[0];
      }
    }
    class RefList extends CoList.Of(co.ref(NestedItem)) {}

    const item1 = NestedItem.create(["item1"], { owner: me });
    const item2 = NestedItem.create(["item2"], { owner: me });
    const item3 = NestedItem.create(["item3"], { owner: me });
    const item4 = NestedItem.create(["item4"], { owner: me });

    const list = RefList.create([item1, item2], { owner: me });

    // Test adding reference items
    list.applyDiff([item1, item2, item3]);
    expect(list.length).toBe(3);
    expect(list[2]?.value).toBe("item3");

    // Test removing reference items
    list.applyDiff([item1, item3]);
    expect(list.length).toBe(2);
    expect(list[0]?.value).toBe("item1");
    expect(list[1]?.value).toBe("item3");

    // Test replacing reference items
    list.applyDiff([item4]);
    expect(list.length).toBe(1);
    expect(list[0]?.value).toBe("item4");

    // Test empty list
    list.applyDiff([]);
    expect(list._raw.asArray()).toEqual([]);
  });

  test("applyDiff with refs + filter", () => {
    class TestMap extends CoMap {
      type = co.string;
    }

    class TestList extends CoList.Of(co.ref(TestMap)) {}

    const bread = TestMap.create({ type: "bread" }, me);
    const butter = TestMap.create({ type: "butter" }, me);
    const onion = TestMap.create({ type: "onion" }, me);

    const list = TestList.create([bread, butter, onion], me);

    list.applyDiff(list.filter((item) => item?.type !== "butter"));

    expect(list._raw.asArray()).toEqual([bread.id, onion.id]);
  });

  test("applyDiff with mixed operations", () => {
    class StringList extends CoList.Of(co.string) {}
    const list = StringList.create(["a", "b", "c", "d", "e"], { owner: me });

    // Test multiple operations at once
    list.applyDiff(["a", "x", "c", "y", "e"]);
    expect(list._raw.asArray()).toEqual(["a", "x", "c", "y", "e"]);

    // Test reordering
    list.applyDiff(["e", "c", "a", "y", "x"]);
    expect(list._raw.asArray()).toEqual(["e", "c", "a", "y", "x"]);

    // Test partial update
    list.applyDiff(["e", "c", "new", "y", "x"]);
    expect(list._raw.asArray()).toEqual(["e", "c", "new", "y", "x"]);
  });
});

describe("CoList resolution", async () => {
  class TwiceNestedList extends CoList.Of(co.string) {
    joined() {
      return this.join(",");
    }
  }

  class NestedList extends CoList.Of(co.ref(TwiceNestedList)) {}

  class TestList extends CoList.Of(co.ref(NestedList)) {}

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
    expect(list[0]?.[0]?.joined()).toBe("a,b");
    expect(list[0]?.[0]?.id).toBeDefined();
    expect(list[1]?.[0]?.[0]).toBe("c");
  });

  test("Loading and availability", async () => {
    const { me, list } = await initNodeAndList();

    const [initialAsPeer, secondPeer] = connectedPeers("initial", "second", {
      peer1role: "server",
      peer2role: "client",
    });
    if (!isControlledAccount(me)) {
      throw "me is not a controlled account";
    }
    me._raw.core.node.syncManager.addPeer(secondPeer);
    const { account: meOnSecondPeer } =
      await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: me.id,
          secret: me._raw.agentSecret,
        },
        sessionProvider: randomSessionProvider,
        peersToLoadFrom: [initialAsPeer],
        crypto: Crypto,
      });

    const loadedList = await TestList.load(list.id, { loadAs: meOnSecondPeer });

    expect(loadedList?.[0]).toBe(null);
    expect(loadedList?._refs[0]?.id).toEqual(list[0]!.id);

    const loadedNestedList = await NestedList.load(list[0]!.id, {
      loadAs: meOnSecondPeer,
    });

    expect(loadedList?.[0]).toBeDefined();
    expect(loadedList?.[0]?.[0]).toBe(null);
    expect(loadedList?.[0]?._refs[0]?.id).toEqual(list[0]![0]!.id);
    // TODO: this should be ref equal
    // expect(loadedList?._refs[0]?.value).toEqual(loadedNestedList);
    expect(loadedList?._refs[0]?.value?.toJSON()).toEqual(
      loadedNestedList?.toJSON(),
    );

    const loadedTwiceNestedList = await TwiceNestedList.load(list[0]![0]!.id, {
      loadAs: meOnSecondPeer,
    });

    expect(loadedList?.[0]?.[0]).toBeDefined();
    expect(loadedList?.[0]?.[0]?.[0]).toBe("a");
    expect(loadedList?.[0]?.[0]?.joined()).toBe("a,b");
    expect(loadedList?.[0]?._refs[0]?.id).toEqual(list[0]?.[0]?.id);
    // TODO: this should be ref equal
    // expect(loadedList?.[0]?._refs[0]?.value).toEqual(loadedTwiceNestedList);
    expect(loadedList?.[0]?._refs[0]?.value?.toJSON()).toEqual(
      loadedTwiceNestedList?.toJSON(),
    );

    const otherNestedList = NestedList.create(
      [TwiceNestedList.create(["e", "f"], { owner: meOnSecondPeer })],
      { owner: meOnSecondPeer },
    );

    loadedList![0] = otherNestedList;
    // TODO: this should be ref equal
    // expect(loadedList?.[0]).toEqual(otherNestedList);
    expect(loadedList?._refs[0]?.value?.toJSON()).toEqual(
      otherNestedList.toJSON(),
    );
    expect(loadedList?._refs[0]?.id).toEqual(otherNestedList.id);
  });

  test("Subscription & auto-resolution", async () => {
    const { me, list } = await initNodeAndList();

    const [initialAsPeer, secondPeer] = connectedPeers("initial", "second", {
      peer1role: "server",
      peer2role: "client",
    });
    if (!isControlledAccount(me)) {
      throw "me is not a controlled account";
    }
    me._raw.core.node.syncManager.addPeer(secondPeer);
    const { account: meOnSecondPeer } =
      await createJazzContextFromExistingCredentials({
        credentials: {
          accountID: me.id,
          secret: me._raw.agentSecret,
        },
        sessionProvider: randomSessionProvider,
        peersToLoadFrom: [initialAsPeer],
        crypto: Crypto,
      });

    const queue = new cojsonInternals.Channel();

    TestList.subscribe(
      list.id,
      { loadAs: meOnSecondPeer },
      (subscribedList) => {
        console.log(
          "subscribedList?.[0]?.[0]?.[0]",
          subscribedList?.[0]?.[0]?.[0],
        );
        void queue.push(subscribedList);
      },
    );

    const update1 = (await queue.next()).value;
    expect(update1?.[0]).toBe(null);

    const update2 = (await queue.next()).value;
    expect(update2?.[0]).toBeDefined();
    expect(update2?.[0]?.[0]).toBe(null);

    const update3 = (await queue.next()).value;
    expect(update3?.[0]?.[0]).toBeDefined();
    expect(update3?.[0]?.[0]?.[0]).toBe("a");
    expect(update3?.[0]?.[0]?.joined()).toBe("a,b");

    update3[0]![0]![0] = "x";

    const update4 = (await queue.next()).value;
    expect(update4?.[0]?.[0]?.[0]).toBe("x");

    // When assigning a new nested value, we get an update

    const newTwiceNestedList = TwiceNestedList.create(["y", "z"], {
      owner: meOnSecondPeer,
    });

    const newNestedList = NestedList.create([newTwiceNestedList], {
      owner: meOnSecondPeer,
    });

    update4[0] = newNestedList;

    const update5 = (await queue.next()).value;
    expect(update5?.[0]?.[0]?.[0]).toBe("y");
    expect(update5?.[0]?.[0]?.joined()).toBe("y,z");

    // we get updates when the new nested value changes
    newTwiceNestedList[0] = "w";
    const update6 = (await queue.next()).value;
    expect(update6?.[0]?.[0]?.[0]).toBe("w");
  });
});
