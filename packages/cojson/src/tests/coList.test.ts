import { beforeEach, describe, expect, test } from "vitest";
import { expectList } from "../coValue.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import {
  hotSleep,
  loadCoValueOrFail,
  nodeWithRandomAgentAndSessionID,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils.js";

const Crypto = await WasmCrypto.create();

beforeEach(async () => {
  setupTestNode({ isSyncServer: true });
});

test("Empty CoList works", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  expect(content.type).toEqual("colist");
  expect(content.toJSON()).toEqual([]);
});

test("Can append, prepend, delete and replace items in CoList", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.append("hello", 0, "trusting");
  expect(content.toJSON()).toEqual(["hello"]);
  content.append("world", 0, "trusting");
  expect(content.toJSON()).toEqual(["hello", "world"]);
  content.prepend("beautiful", 1, "trusting");
  expect(content.toJSON()).toEqual(["hello", "beautiful", "world"]);
  content.prepend("hooray", 3, "trusting");
  expect(content.toJSON()).toEqual(["hello", "beautiful", "world", "hooray"]);
  content.replace(2, "universe", "trusting");
  expect(content.toJSON()).toEqual([
    "hello",
    "beautiful",
    "universe",
    "hooray",
  ]);
  content.delete(2, "trusting");
  expect(content.toJSON()).toEqual(["hello", "beautiful", "hooray"]);
});

test("Push is equivalent to append after last item", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  expect(content.type).toEqual("colist");

  content.append("hello", 0, "trusting");
  expect(content.toJSON()).toEqual(["hello"]);
  content.append("world", undefined, "trusting");
  expect(content.toJSON()).toEqual(["hello", "world"]);
  content.append("hooray", undefined, "trusting");
  expect(content.toJSON()).toEqual(["hello", "world", "hooray"]);
});

test("appendItems add an array of items at the end of the list", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  expect(content.type).toEqual("colist");

  content.append("hello", 0, "trusting");
  expect(content.toJSON()).toEqual(["hello"]);
  content.appendItems(["world", "hooray", "universe"], undefined, "trusting");
  expect(content.toJSON()).toEqual(["hello", "world", "hooray", "universe"]);
});

test("appendItems at index", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.append("first", 0, "trusting");
  content.append("second", 0, "trusting");
  expect(content.toJSON()).toEqual(["first", "second"]);

  content.appendItems(["third", "fourth"], 1, "trusting");
  expect(content.toJSON()).toEqual(["first", "second", "third", "fourth"]);

  content.appendItems(["hello", "world"], 0, "trusting");
  expect(content.toJSON()).toEqual([
    "first",
    "hello",
    "world",
    "second",
    "third",
    "fourth",
  ]);
});

test("appendItems at index", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.append("first", 0, "trusting");
  expect(content.toJSON()).toEqual(["first"]);

  content.appendItems(["second"], 0, "trusting");
  expect(content.toJSON()).toEqual(["first", "second"]);

  content.appendItems(["third"], 1, "trusting");
  expect(content.toJSON()).toEqual(["first", "second", "third"]);
});

test("appendItems with negative index", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.append("hello", 0, "trusting");
  expect(content.toJSON()).toEqual(["hello"]);
  content.appendItems(["world", "hooray", "universe"], -1, "trusting");
  expect(content.toJSON()).toEqual(["hello", "world", "hooray", "universe"]);
});

test("Can push into empty list", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  expect(content.type).toEqual("colist");

  content.append("hello", undefined, "trusting");
  expect(content.toJSON()).toEqual(["hello"]);
});

test("init the list correctly", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const group = node.createGroup();

  const content = group.createList(["hello", "world", "hooray", "universe"]);

  expect(content.type).toEqual("colist");
  expect(content.toJSON()).toEqual(["hello", "world", "hooray", "universe"]);

  content.append("hello", content.toJSON().length - 1, "trusting");
  expect(content.toJSON()).toEqual([
    "hello",
    "world",
    "hooray",
    "universe",
    "hello",
  ]);
  expect(content.core.verified.header.createdAt).toBeDefined();
});

test("Items prepended to start appear with latest first", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.prepend("first", 0, "trusting");
  content.prepend("second", 0, "trusting");
  content.prepend("third", 0, "trusting");

  expect(content.toJSON()).toEqual(["third", "second", "first"]);
});

test("mixing prepend and append", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const list = expectList(coValue.getCurrentContent());

  list.append(2, undefined, "trusting");
  list.prepend(1, undefined, "trusting");
  list.append(3, undefined, "trusting");

  expect(list.toJSON()).toEqual([1, 2, 3]);
});

test("Items appended to start", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const content = expectList(coValue.getCurrentContent());

  content.append("first", 0, "trusting");
  content.append("second", 0, "trusting");
  content.append("third", 0, "trusting");

  // This result is correct because "third" is appended after "first"
  // Using the Array methods this would be the same as doing content.splice(1, 0, "third")
  expect(content.toJSON()).toEqual(["first", "third", "second"]);
});

test("syncing appends with an older timestamp", async () => {
  const client = setupTestNode({
    connected: true,
  });
  const otherClient = setupTestNode({});

  const otherClientConnection = otherClient.connectToSyncServer({
    ourName: "otherClient",
  });

  const coValue = client.node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const list = expectList(coValue.getCurrentContent());

  list.append(1, undefined, "trusting");
  list.append(2, undefined, "trusting");

  const listOnOtherClient = await loadCoValueOrFail(otherClient.node, list.id);

  otherClientConnection.peerState.gracefulShutdown();

  listOnOtherClient.append(3, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  list.append(4, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  listOnOtherClient.append(5, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  list.append(6, undefined, "trusting");

  otherClient.connectToSyncServer({
    ourName: "otherClient",
  });

  await waitFor(() => {
    expect(list.toJSON()).toEqual([1, 2, 4, 6, 3, 5]);
  });

  expect(listOnOtherClient.toJSON()).toEqual(list.toJSON());
});

test("syncing prepends with an older timestamp", async () => {
  const client = setupTestNode({
    connected: true,
  });
  const otherClient = setupTestNode({});

  const otherClientConnection = otherClient.connectToSyncServer();

  const coValue = client.node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const list = expectList(coValue.getCurrentContent());

  list.prepend(1, undefined, "trusting");
  list.prepend(2, undefined, "trusting");

  const listOnOtherClient = await loadCoValueOrFail(otherClient.node, list.id);

  otherClientConnection.peerState.gracefulShutdown();

  listOnOtherClient.prepend(3, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  list.prepend(4, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  listOnOtherClient.prepend(5, undefined, "trusting");

  await new Promise((resolve) => setTimeout(resolve, 50));

  list.prepend(6, undefined, "trusting");

  otherClient.connectToSyncServer();

  await waitFor(() => {
    expect(list.toJSON()).toEqual([6, 4, 5, 3, 2, 1]);
  });

  expect(listOnOtherClient.toJSON()).toEqual(list.toJSON());
});

test("totalValidTransactions should return the number of valid transactions processed", async () => {
  const client = setupTestNode({
    connected: true,
  });
  const otherClient = setupTestNode({});

  const otherClientConnection = otherClient.connectToSyncServer();

  const group = client.node.createGroup();
  group.addMember("everyone", "reader");

  const list = group.createList([1, 2]);

  const listOnOtherClient = await loadCoValueOrFail(otherClient.node, list.id);

  otherClientConnection.peerState.gracefulShutdown();

  group.addMember("everyone", "writer");

  await new Promise((resolve) => setTimeout(resolve, 50));

  listOnOtherClient.append(3, undefined, "trusting");

  expect(listOnOtherClient.toJSON()).toEqual([1, 2]);
  expect(listOnOtherClient.totalValidTransactions).toEqual(1);

  otherClient.connectToSyncServer();

  await waitFor(() => {
    expect(listOnOtherClient.core.getCurrentContent().toJSON()).toEqual([
      1, 2, 3,
    ]);
  });

  expect(
    listOnOtherClient.core.getCurrentContent().totalValidTransactions,
  ).toEqual(2);
});

test("Should ignore unknown meta transactions", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const coValue = node.createCoValue({
    type: "colist",
    ruleset: { type: "unsafeAllowAll" },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  coValue.makeTransaction([], "trusting", { unknownMeta: 1 });

  const content = expectList(coValue.getCurrentContent());

  content.append("first", 0, "trusting");

  expect(content.toJSON()).toEqual(["first"]);
});

describe("CoList Time Travel", () => {
  test("atTime should return a time travel entity", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const coValue = node.createCoValue({
      type: "colist",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const content = expectList(coValue.getCurrentContent());

    const beforeA = hotSleep(10);
    content.append("first", 0, "trusting");

    const beforeB = hotSleep(10);
    content.append("second", 0, "trusting");

    const beforeC = hotSleep(10);
    content.delete(0, "trusting");

    const beforeD = hotSleep(10);
    content.prepend("third", 0, "trusting");

    expect(content.toJSON()).toEqual(["third", "second"]);

    expect(content.atTime(0).toJSON()).toEqual([]);
    expect(content.atTime(beforeA).toJSON()).toEqual([]);
    expect(content.atTime(beforeB).toJSON()).toEqual(["first"]);
    expect(content.atTime(beforeC).toJSON()).toEqual(["first", "second"]);
    expect(content.atTime(beforeD).toJSON()).toEqual(["second"]);
    expect(content.atTime(Date.now()).toJSON()).toEqual(["third", "second"]);
  });
});

describe("CoList Branching", () => {
  test("should handle concurrent appends from multiple branches", async () => {
    const client1 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk"]);

    // Create branches for each client
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-branch", group.id)
        .getCurrentContent(),
    );
    const bobBranch = expectList(
      groceryList.core.createBranch("bob-branch", group.id).getCurrentContent(),
    );

    // Both branches append grocery items
    aliceBranch.append("bread", undefined, "trusting");
    aliceBranch.append("butter", undefined, "trusting");
    bobBranch.append("eggs", undefined, "trusting");

    // Merge both branches back to source
    aliceBranch.core.mergeBranch();
    bobBranch.core.mergeBranch();

    // Wait for sync
    await groceryList.core.waitForSync();

    // Source list should contain all items from both branches
    expect(groceryList.toJSON()).toMatchInlineSnapshot(`
      [
        "milk",
        "eggs",
        "bread",
        "butter",
      ]
    `);
  });

  test("should handle concurrent prepends from multiple branches", async () => {
    const client1 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk"]);

    // Create branches for each client
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-branch", group.id)
        .getCurrentContent(),
    );
    const bobBranch = expectList(
      groceryList.core.createBranch("bob-branch", group.id).getCurrentContent(),
    );

    // Both branches prepend grocery items
    aliceBranch.prepend("bread", undefined, "trusting");
    aliceBranch.prepend("butter", undefined, "trusting");
    bobBranch.prepend("eggs", undefined, "trusting");

    // Merge both branches back to source
    aliceBranch.core.mergeBranch();
    bobBranch.core.mergeBranch();

    // Wait for sync
    await groceryList.core.waitForSync();

    // Source list should contain all items from both branches
    expect(groceryList.toJSON()).toMatchInlineSnapshot(`
      [
        "eggs",
        "butter",
        "bread",
        "milk",
      ]
    `);
  });

  test("should handle concurrent deletes from multiple branches", async () => {
    const client1 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk", "bread", "eggs", "cheese"]);

    // Create branches for each client
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-branch", group.id)
        .getCurrentContent(),
    );
    const bobBranch = expectList(
      groceryList.core.createBranch("bob-branch", group.id).getCurrentContent(),
    );

    // Both branches delete different grocery items
    aliceBranch.delete(aliceBranch.asArray().indexOf("bread"), "trusting");
    bobBranch.delete(bobBranch.asArray().indexOf("eggs"), "trusting");

    // Merge both branches back to source
    aliceBranch.core.mergeBranch();
    bobBranch.core.mergeBranch();

    // Wait for sync
    await groceryList.core.waitForSync();

    // Source list should reflect both deletions
    expect(groceryList.toJSON()).toEqual(["milk", "cheese"]);
  });

  test("should handle concurrent insertions at different positions from branches", async () => {
    const client1 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk", "cheese", "butter"]);

    // Create branches for each client
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-branch", group.id)
        .getCurrentContent(),
    );
    const bobBranch = expectList(
      groceryList.core.createBranch("bob-branch", group.id).getCurrentContent(),
    );

    // Both branches insert grocery items at position 1
    aliceBranch.append("bread", 0, "trusting");
    bobBranch.append("eggs", 0, "trusting");

    // Merge both branches back to source
    aliceBranch.core.mergeBranch();
    bobBranch.core.mergeBranch();

    // Wait for sync
    await groceryList.core.waitForSync();

    // Source list should contain both insertions
    expect(groceryList.toJSON()).toMatchInlineSnapshot(`
      [
        "milk",
        "eggs",
        "bread",
        "cheese",
        "butter",
      ]
    `);
  });

  test("should handle multiple branches modifying different list items", async () => {
    const client1 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk", "bread", "eggs"]);

    // Create branches for each client
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-branch", group.id)
        .getCurrentContent(),
    );
    const bobBranch = expectList(
      groceryList.core.createBranch("bob-branch", group.id).getCurrentContent(),
    );
    const charlieBranch = expectList(
      groceryList.core
        .createBranch("charlie-branch", group.id)
        .getCurrentContent(),
    );

    // Each branch modifies different grocery items
    aliceBranch.replace(0, "organic milk", "trusting");
    bobBranch.replace(1, "whole wheat bread", "trusting");
    charlieBranch.replace(2, "free-range eggs", "trusting");

    // Merge all branches back to source
    aliceBranch.core.mergeBranch();
    bobBranch.core.mergeBranch();
    charlieBranch.core.mergeBranch();

    // Wait for sync
    await groceryList.core.waitForSync();

    // Source list should contain all modifications
    const expected = ["organic milk", "whole wheat bread", "free-range eggs"];
    expect(groceryList.toJSON()).toEqual(expected);
  });

  test("should handle appendItems from multiple branches", async () => {
    const client1 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk"]);

    // Create branches for each client
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-branch", group.id)
        .getCurrentContent(),
    );
    const bobBranch = expectList(
      groceryList.core.createBranch("bob-branch", group.id).getCurrentContent(),
    );

    // Both branches append multiple grocery items
    aliceBranch.appendItems(["bread", "butter"], undefined, "trusting");
    bobBranch.appendItems(["eggs", "cheese"], undefined, "trusting");

    // Merge both branches back to source
    aliceBranch.core.mergeBranch();
    bobBranch.core.mergeBranch();

    // Wait for sync
    await groceryList.core.waitForSync();

    // Source list should contain all items
    expect(groceryList.toJSON()).toMatchInlineSnapshot(`
      [
        "milk",
        "eggs",
        "cheese",
        "bread",
        "butter",
      ]
    `);
  });

  test("should handle complex operations from multiple branches", async () => {
    const client1 = setupTestNode({
      connected: true,
    });
    const client2 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk", "bread", "eggs"]);

    // Create branches for each client
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-branch", group.id)
        .getCurrentContent(),
    );
    const bobBranch = expectList(
      groceryList.core.createBranch("bob-branch", group.id).getCurrentContent(),
    );

    // Alice performs multiple operations
    aliceBranch.delete(1, "trusting"); // Remove "bread"
    aliceBranch.append("cheese", undefined, "trusting");
    aliceBranch.prepend("yogurt", undefined, "trusting");

    // Bob performs different operations
    bobBranch.replace(0, "organic milk", "trusting");
    bobBranch.append("butter", undefined, "trusting");
    bobBranch.delete(2, "trusting"); // Remove "eggs"

    // Merge both branches back to source
    aliceBranch.core.mergeBranch();
    bobBranch.core.mergeBranch();

    // Wait for sync
    await groceryList.core.waitForSync();

    // Source list should contain the combined result
    expect(groceryList.toJSON()).toMatchInlineSnapshot(`
      [
        "yogurt",
        "organic milk",
        "butter",
        "cheese",
      ]
    `);
  });

  test("should handle multiple branch merges with incremental changes", async () => {
    const client1 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk"]);

    // Create branches for each client
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-branch", group.id)
        .getCurrentContent(),
    );
    const bobBranch = expectList(
      groceryList.core.createBranch("bob-branch", group.id).getCurrentContent(),
    );
    const charlieBranch = expectList(
      groceryList.core
        .createBranch("charlie-branch", group.id)
        .getCurrentContent(),
    );

    // Each branch adds grocery items
    aliceBranch.append("bread", undefined, "trusting");
    bobBranch.append("eggs", undefined, "trusting");
    charlieBranch.append("cheese", undefined, "trusting");

    // Merge branches one by one
    aliceBranch.core.mergeBranch();
    await groceryList.core.waitForSync();
    expect(groceryList.toJSON()).toEqual(["milk", "bread"]);

    bobBranch.core.mergeBranch();
    await groceryList.core.waitForSync();
    expect(groceryList.toJSON()).toEqual(["milk", "eggs", "bread"]);

    charlieBranch.core.mergeBranch();
    await groceryList.core.waitForSync();
    expect(groceryList.toJSON()).toEqual(["milk", "cheese", "eggs", "bread"]);
  });

  test("should resolve conflicts when multiple branches modify the same position", async () => {
    const client1 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk", "bread", "eggs"]);

    // Create branches for each client
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-branch", group.id)
        .getCurrentContent(),
    );
    const bobBranch = expectList(
      groceryList.core.createBranch("bob-branch", group.id).getCurrentContent(),
    );

    // Both branches try to replace the same item at position 1
    aliceBranch.replace(1, "whole wheat bread", "trusting");
    bobBranch.replace(1, "sourdough bread", "trusting");

    // Merge both branches back to source
    aliceBranch.core.mergeBranch();
    bobBranch.core.mergeBranch();

    // Wait for sync
    await groceryList.core.waitForSync();

    // Source list should have a consistent state after conflict resolution
    expect(groceryList.toJSON()).toContain("whole wheat bread");
    expect(groceryList.toJSON()).toContain("sourdough bread");
    expect(groceryList.toJSON()).toMatchInlineSnapshot(`
      [
        "milk",
        "sourdough bread",
        "whole wheat bread",
        "eggs",
      ]
    `);
  });

  test("should handle concurrent deletions at overlapping positions across branches", async () => {
    const client1 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const list = group.createList(["milk", "bread", "eggs"]);

    // Create branches for each client
    const mondayShoppingBranch = expectList(
      list.core.createBranch("monday-shopping", group.id).getCurrentContent(),
    );
    const tuesdayShoppingBranch = expectList(
      list.core.createBranch("tuesday-shopping", group.id).getCurrentContent(),
    );

    mondayShoppingBranch.delete(list.asArray().indexOf("milk"), "trusting");
    tuesdayShoppingBranch.delete(list.asArray().indexOf("eggs"), "trusting");

    // Merge both branches back to source
    mondayShoppingBranch.core.mergeBranch();
    tuesdayShoppingBranch.core.mergeBranch();

    // Source list should reflect all deletions
    expect(list.toJSON()).toEqual(["bread"]);
  });

  test("should handle branching from different sessions and merging back", async () => {
    const client1 = setupTestNode({
      connected: true,
    });
    const client2 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk", "bread"]);

    // Client1 creates a branch
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-shopping-branch", group.id)
        .getCurrentContent(),
    );

    // Client1 adds items to the branch
    aliceBranch.append("eggs", undefined, "trusting");

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Client2 loads the branch from a different session
    const branchOnClient2 = await loadCoValueOrFail(
      client2.node,
      aliceBranch.id,
    );

    // Client2 adds more items and removes some existing ones
    branchOnClient2.append("cheese", undefined, "trusting");
    branchOnClient2.delete(
      branchOnClient2.asArray().indexOf("bread"),
      "trusting",
    );

    branchOnClient2.core.mergeBranch();

    // Wait for all coValues to sync on both nodes
    await client2.node.syncManager.waitForAllCoValuesSync();
    await client1.node.syncManager.waitForAllCoValuesSync();

    // Source list should contain the final state
    expect(groceryList.toJSON()).toEqual(["milk", "eggs", "cheese"]);
  });

  test("should handle multiple branches from different sessions with complex operations", async () => {
    const client1 = setupTestNode({
      connected: true,
    });
    const client2 = setupTestNode({
      connected: true,
    });

    const group = client1.node.createGroup();
    group.addMember("everyone", "writer");

    const groceryList = group.createList(["milk"]);

    // Client1 creates first branch
    const aliceBranch = expectList(
      groceryList.core
        .createBranch("alice-shopping-branch", group.id)
        .getCurrentContent(),
    );

    // Client1 adds items to first branch
    aliceBranch.append("bread", undefined, "trusting");

    // Client2 creates second branch
    const bobBranch = expectList(
      groceryList.core
        .createBranch("bob-shopping-branch", group.id)
        .getCurrentContent(),
    );

    // Client2 adds different items to second branch
    bobBranch.append("eggs", undefined, "trusting");

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Client2 loads first branch and modifies it
    const aliceBranchOnClient2 = await loadCoValueOrFail(
      client2.node,
      aliceBranch.id,
    );
    aliceBranchOnClient2.append("butter", undefined, "trusting");

    // Merge all branches back to source
    aliceBranchOnClient2.core.mergeBranch();

    // Make the second merge happen later than the previous one, so the ordering is not based on the random sessionIDs
    await new Promise((resolve) => setTimeout(resolve, 1));

    bobBranch.core.mergeBranch();

    // Wait for all coValues to sync on both nodes
    await client2.node.syncManager.waitForAllCoValuesSync();
    await client1.node.syncManager.waitForAllCoValuesSync();

    // Source list should contain all changes
    expect(groceryList.toJSON()).toMatchInlineSnapshot(`
      [
        "milk",
        "eggs",
        "bread",
        "butter",
      ]
    `);
  });
});

test("the list should rebuild when the group permissions change", async () => {
  const alice = setupTestNode({
    connected: true,
  });
  const group = alice.node.createGroup();
  const list = group.createList();

  list.append("fromAdmin");

  const bob = setupTestNode({
    connected: true,
  });

  const listOnBob = await loadCoValueOrFail(bob.node, list.id);

  expect(listOnBob.get(0)).toEqual(undefined);
  expect(listOnBob.totalValidTransactions).toEqual(0);

  group.addMember("everyone", "reader");

  await waitFor(() => {
    expect(listOnBob.get(0)).toEqual("fromAdmin");
  });

  expect(listOnBob.version).toEqual(1);
  expect(listOnBob.totalValidTransactions).toEqual(1);
});

describe("CoList restricted deletion", () => {
  test("default ownedByGroup behavior is unchanged (writers can delete)", async () => {
    const alice = await setupTestAccount({ connected: true });
    const bob = await setupTestAccount({ connected: true });

    const group = alice.node.createGroup();
    group.addMember(bob.account, "writer");

    const listCore = alice.node.createCoValue({
      type: "colist",
      ruleset: { type: "ownedByGroup", group: group.id },
      meta: null,
      ...Crypto.createdNowUnique(),
    });
    const list = expectList(listCore.getCurrentContent());
    list.append("item");

    const listOnBob = await loadCoValueOrFail(bob.node, list.id);
    listOnBob.delete(0);

    await waitFor(() => {
      expect(list.toJSON()).toEqual([]);
    });
  });

  test("with restrictDeletion enabled, writers can append but cannot delete or replace", async () => {
    const alice = await setupTestAccount({ connected: true });
    const bob = await setupTestAccount({ connected: true });

    const group = alice.node.createGroup();
    group.addMember(bob.account, "writer");

    const listCore = alice.node.createCoValue({
      type: "colist",
      ruleset: {
        type: "ownedByGroup",
        group: group.id,
        restrictDeletion: true,
      },
      meta: null,
      ...Crypto.createdNowUnique(),
    });
    const list = expectList(listCore.getCurrentContent());
    list.append("seed");

    const listOnBob = await loadCoValueOrFail(bob.node, list.id);

    listOnBob.append("writer-append");
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["seed", "writer-append"]);
    });

    listOnBob.delete(0);
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["seed", "writer-append"]);
    });

    listOnBob.replace(1, "writer-replace-attempt");
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["seed", "writer-append"]);
    });
  });

  test("with restrictDeletion enabled, managers and admins can delete", async () => {
    const alice = await setupTestAccount({ connected: true });
    const bob = await setupTestAccount({ connected: true });

    const group = alice.node.createGroup();
    group.addMember(bob.account, "manager");

    const listCore = alice.node.createCoValue({
      type: "colist",
      ruleset: {
        type: "ownedByGroup",
        group: group.id,
        restrictDeletion: true,
      },
      meta: null,
      ...Crypto.createdNowUnique(),
    });
    const list = expectList(listCore.getCurrentContent());
    list.appendItems(["first", "second", "third"]);

    const listOnBob = await loadCoValueOrFail(bob.node, list.id);
    listOnBob.delete(1);

    await waitFor(() => {
      expect(list.toJSON()).toEqual(["first", "third"]);
    });

    list.delete(0);
    await waitFor(() => {
      expect(list.toJSON()).toEqual(["third"]);
    });
  });

  test("deletions made while writer stay blocked after promotion", async () => {
    const alice = await setupTestAccount({ connected: true });
    const bob = await setupTestAccount({ connected: true });

    const group = alice.node.createGroup();
    group.addMember(bob.account, "writer");

    const listCore = alice.node.createCoValue({
      type: "colist",
      ruleset: {
        type: "ownedByGroup",
        group: group.id,
        restrictDeletion: true,
      },
      meta: null,
      ...Crypto.createdNowUnique(),
    });
    const list = expectList(listCore.getCurrentContent());
    list.append("item");

    const listOnBob = await loadCoValueOrFail(bob.node, list.id);
    listOnBob.delete(0);

    await waitFor(() => {
      expect(list.toJSON()).toEqual(["item"]);
    });

    group.addMember(bob.account, "manager");

    await waitFor(() => {
      expect(list.toJSON()).toEqual(["item"]);
      expect(listOnBob.toJSON()).toEqual(["item"]);
    });

    listOnBob.delete(0);
    await waitFor(() => {
      expect(list.toJSON()).toEqual([]);
    });
  });
});

test("items appended after a losing init transaction are preserved", async () => {
  const alice = setupTestNode({ connected: true });
  const bob = setupTestNode({ connected: true });

  const group = alice.node.createGroup();
  group.addMember("everyone", "writer");

  const list = group.createList(
    ["alice-init"],
    undefined,
    "trusting",
    undefined,
    { fww: "init" },
  );

  await new Promise((resolve) => setTimeout(resolve, 5));

  const listOnBob = await loadCoValueOrFail(bob.node, list.id);

  listOnBob.appendItems(["bob-init"], undefined, "trusting", { fww: "init" });
  listOnBob.appendItems(["bob-update"], undefined, "trusting");

  await waitFor(() => {
    expect(listOnBob.toJSON()).toEqual(["alice-init", "bob-update"]);
    expect(list.toJSON()).toEqual(["alice-init", "bob-update"]);
  });
});
