import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, beforeEach, describe, expect, test, vi } from "vitest";
import { Account, Group, subscribeToCoValue, z } from "../index.js";
import {
  Loaded,
  activeAccountContext,
  co,
  coValueClassFromCoValueClassOrSchema,
} from "../internal.js";
import {
  createJazzTestAccount,
  runWithoutActiveAccount,
  setupJazzTestSync,
} from "../testing.js";
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

describe("CoList unique methods", () => {
  test("loadUnique returns existing list", async () => {
    const ItemList = co.list(z.string());
    const group = Group.create();

    const originalList = ItemList.create(["item1", "item2", "item3"], {
      owner: group,
      unique: "test-list",
    });

    const foundList = await ItemList.loadUnique("test-list", group.$jazz.id);
    expect(foundList).toEqual(originalList);
    expect(foundList?.length).toBe(3);
    expect(foundList?.[0]).toBe("item1");
  });

  test("loadUnique returns null for non-existent list", async () => {
    const ItemList = co.list(z.string());
    const group = Group.create();

    const foundList = await ItemList.loadUnique("non-existent", group.$jazz.id);
    expect(foundList).toBeNull();
  });

  test("upsertUnique creates new list when none exists", async () => {
    const ItemList = co.list(z.string());
    const group = Group.create();

    const sourceData = ["item1", "item2", "item3"];

    const result = await ItemList.upsertUnique({
      value: sourceData,
      unique: "new-list",
      owner: group,
    });

    expect(result).not.toBeNull();
    expect(result?.length).toBe(3);
    expect(result?.[0]).toBe("item1");
    expect(result?.[1]).toBe("item2");
    expect(result?.[2]).toBe("item3");
  });

  test("upsertUnique without an active account", async () => {
    const account = activeAccountContext.get();
    const ItemList = co.list(z.string());

    const sourceData = ["item1", "item2", "item3"];

    const result = await runWithoutActiveAccount(() => {
      return ItemList.upsertUnique({
        value: sourceData,
        unique: "new-list",
        owner: account,
      });
    });

    expect(result).not.toBeNull();
    expect(result?.length).toBe(3);
    expect(result?.[0]).toBe("item1");
    expect(result?.[1]).toBe("item2");
    expect(result?.[2]).toBe("item3");

    expect(result?.$jazz.owner).toEqual(account);
  });

  test("upsertUnique updates existing list", async () => {
    const ItemList = co.list(z.string());
    const group = Group.create();

    // Create initial list
    const originalList = ItemList.create(["original1", "original2"], {
      owner: group,
      unique: "update-list",
    });

    // Upsert with new data
    const updatedList = await ItemList.upsertUnique({
      value: ["updated1", "updated2", "updated3"],
      unique: "update-list",
      owner: group,
    });

    expect(updatedList).toEqual(originalList); // Should be the same instance
    expect(updatedList?.length).toBe(3);
    expect(updatedList?.[0]).toBe("updated1");
    expect(updatedList?.[1]).toBe("updated2");
    expect(updatedList?.[2]).toBe("updated3");
  });

  test("upsertUnique with CoValue items", async () => {
    const Item = co.map({
      name: z.string(),
      value: z.number(),
    });
    const ItemList = co.list(Item);
    const group = Group.create();

    const items = [
      Item.create({ name: "First", value: 1 }, group),
      Item.create({ name: "Second", value: 2 }, group),
    ];

    const result = await ItemList.upsertUnique({
      value: items,
      unique: "item-list",
      owner: group,
      resolve: { $each: true },
    });

    expect(result).not.toBeNull();
    expect(result?.length).toBe(2);
    expect(result?.[0]?.name).toBe("First");
    expect(result?.[1]?.name).toBe("Second");
  });

  test("upsertUnique updates list with CoValue items", async () => {
    const Item = co.map({
      name: z.string(),
      value: z.number(),
    });
    const ItemList = co.list(Item);
    const group = Group.create();

    // Create initial list
    const initialItems = [Item.create({ name: "Initial", value: 0 }, group)];
    const originalList = ItemList.create(initialItems, {
      owner: group,
      unique: "updateable-item-list",
    });

    // Upsert with new items
    const newItems = [
      Item.create({ name: "Updated", value: 1 }, group),
      Item.create({ name: "Added", value: 2 }, group),
    ];

    const updatedList = await ItemList.upsertUnique({
      value: newItems,
      unique: "updateable-item-list",
      owner: group,
      resolve: { $each: true },
    });

    expect(updatedList).toEqual(originalList); // Should be the same instance
    expect(updatedList?.length).toBe(2);
    expect(updatedList?.[0]?.name).toBe("Updated");
    expect(updatedList?.[1]?.name).toBe("Added");
  });

  test("findUnique returns correct ID", async () => {
    const ItemList = co.list(z.string());
    const group = Group.create();

    const originalList = ItemList.create(["test"], {
      owner: group,
      unique: "find-test",
    });

    const foundId = ItemList.findUnique("find-test", group.$jazz.id);
    expect(foundId).toBe(originalList.$jazz.id);
  });

  test("upsertUnique with resolve options", async () => {
    const Category = co.map({ title: z.string() });
    const Item = co.map({
      name: z.string(),
      category: Category,
    });
    const ItemList = co.list(Item);
    const group = Group.create();

    const category = Category.create({ title: "Category 1" }, group);

    const items = [Item.create({ name: "Item 1", category }, group)];

    const result = await ItemList.upsertUnique({
      value: items,
      unique: "resolved-list",
      owner: group,
      resolve: { $each: { category: true } },
    });

    expect(result).not.toBeNull();
    expect(result?.length).toBe(1);
    expect(result?.[0]?.name).toBe("Item 1");
    expect(result?.[0]?.category?.title).toBe("Category 1");
  });

  test("concurrently upserting the same value", async () => {
    const ItemList = co.list(z.string());

    const owner = Group.create();

    const promises = Array.from({ length: 3 }, (_, i) =>
      ItemList.upsertUnique({
        owner,
        unique: "concurrent",
        value: [`Item ${i}`, `Second ${i}`],
      }),
    );

    await Promise.all(promises);

    const result = await ItemList.loadUnique("concurrent", owner.$jazz.id);
    assert(result);

    expect(result.length).toBe(2);
    expect(result[0]).toBe(`Item 2`);
    expect(result[1]).toBe(`Second 2`);
  });
});
