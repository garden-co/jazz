import { assert, beforeEach, describe, expect, test } from "vitest";
import {
  createTestNode,
  setupTestNode,
  loadCoValueOrFail,
} from "./testUtils.js";
import { expectList, expectMap, expectPlainText } from "../coValue.js";
import { RawCoMap } from "../exports.js";

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("Branching Logic", () => {
  describe("Branch Operations with Transactions", () => {
    test("should maintain separate transaction histories between original and branch", async () => {
      const node = createTestNode();
      const group = node.createGroup();
      const originalMap = group.createMap();
      const branchName = "feature-branch";

      // Add initial transactions to original map
      originalMap.set("originalKey1", "value1", "trusting");
      originalMap.set("originalKey2", "value2", "trusting");

      const branch = expectMap(
        originalMap.core.createBranch(branchName, group.id).getCurrentContent(),
      );

      // Add new transactions to branch
      branch.set("branchKey1", "branchValue1", "trusting");
      branch.set("branchKey2", "branchValue2", "trusting");

      // Verify original map doesn't have branch transactions
      expect(originalMap.get("branchKey1")).toBe(undefined);
      expect(originalMap.get("branchKey2")).toBe(undefined);

      // Verify branch has both original and new transactions
      expect(branch.get("branchKey2")).toBe("branchValue2");
      expect(branch.get("branchKey1")).toBe("branchValue1");
      expect(branch.get("originalKey1")).toBe("value1");
      expect(branch.get("originalKey2")).toBe("value2");
    });
  });

  describe("Branch Merging", () => {
    test("should merge branch transactions back to source map", () => {
      const node = createTestNode();
      const group = node.createGroup();
      const originalMap = group.createMap();
      const branchName = "feature-branch";

      // Add initial transactions to original map
      originalMap.set("key1", "value1", "trusting");
      originalMap.set("key2", "value2", "trusting");

      // Create branch from original map
      const branch = expectMap(
        originalMap.core.createBranch(branchName, group.id).getCurrentContent(),
      );

      // Add new transaction to branch
      branch.set("key1", "branchValue1", "trusting");

      // Merge branch back to source
      const result = expectMap(branch.core.mergeBranch().getCurrentContent());

      // Verify source now contains branch transactions
      expect(result.get("key1")).toBe("branchValue1");
      expect(result.get("key2")).toBe("value2");
    });

    test("should not create duplicate merge commits when merging already merged branch", () => {
      const node = createTestNode();
      const group = node.createGroup();
      const originalMap = group.createMap();
      const branchName = "feature-branch";

      // Add initial transactions to original map
      originalMap.set("key1", "value1", "trusting");
      originalMap.set("key2", "value2", "trusting");

      // Create branch from original map
      const branch = expectMap(
        originalMap.core.createBranch(branchName, group.id).getCurrentContent(),
      );

      // Add new transaction to branch
      branch.set("key1", "branchValue1", "trusting");

      // Merge branch twice - second merge should not create new commit
      expectMap(branch.core.mergeBranch().getCurrentContent());
      const result = expectMap(branch.core.mergeBranch().getCurrentContent());

      // Verify only one merge commit was created
      expect(branch.core.mergeCommits.length).toBe(1);

      // Verify source contains branch transactions
      expect(result.get("key1")).toBe("branchValue1");
      expect(result.get("key2")).toBe("value2");
    });

    test("should not create merge commit when merging empty branch", () => {
      const node = createTestNode();
      const group = node.createGroup();
      const originalMap = group.createMap();
      const branchName = "feature-branch";

      // Add initial transactions to original map
      originalMap.set("key1", "value1", "trusting");
      originalMap.set("key2", "value2", "trusting");

      // Create branch from original map (no changes made)
      const branch = expectMap(
        originalMap.core.createBranch(branchName, group.id).getCurrentContent(),
      );

      // Merge empty branch
      const result = expectMap(branch.core.mergeBranch().getCurrentContent());

      // Verify no merge commit was created
      expect(result.core.mergeCommits.length).toBe(0);
    });

    test("should merge only new changes from branch after previous merge", () => {
      const node = createTestNode();
      const group = node.createGroup();
      const originalMap = group.createMap();
      const branchName = "feature-branch";

      // Add initial transactions to original map
      originalMap.set("key1", "value1", "trusting");
      originalMap.set("key2", "value2", "trusting");

      const branch = expectMap(
        originalMap.core.createBranch(branchName, group.id).getCurrentContent(),
      );

      // Add first change to branch
      branch.set("key1", "branchValue1", "trusting");

      // Merge first change
      branch.core.mergeBranch();

      // Verify first change was merged
      expect(originalMap.get("key1")).toBe("branchValue1");
      expect(originalMap.get("key2")).toBe("value2");

      // Add second change to branch
      branch.set("key2", "branchValue2", "trusting");

      // Merge second change
      branch.core.mergeBranch();

      // Verify two merge commits exist
      expect(branch.core.mergeCommits.length).toBe(2);

      // Verify both changes are now in original map
      expect(originalMap.get("key1")).toBe("branchValue1");
      expect(originalMap.get("key2")).toBe("branchValue2");
    });

    test("should work with co.list", () => {
      const node = createTestNode();
      const group = node.createGroup();
      const list = group.createList();

      // Create a shopping list with grocery items
      list.appendItems(["bread", "milk", "eggs"]);

      // Remove milk from the list
      list.delete(list.asArray().indexOf("milk"));

      const branch = expectList(
        list.core.createBranch("feature-branch", group.id).getCurrentContent(),
      );

      // Add more items to the branch
      branch.appendItems(["cheese", "yogurt", "bananas"]);

      // Remove yogurt from the branch
      branch.delete(branch.asArray().indexOf("yogurt"));

      const result = expectList(branch.core.mergeBranch().getCurrentContent());

      expect(result.toJSON()).toEqual(["bread", "eggs", "cheese", "bananas"]);
    });

    test("should work with co.list when branching from different session", async () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = client.node.createGroup();
      const list = group.createList();

      // Create a grocery list with initial items
      list.appendItems(["bread", "milk"]);

      const branch1 = expectList(
        list.core.createBranch("feature-branch", group.id).getCurrentContent(),
      );

      // Add new items to first branch
      branch1.appendItems(["cheese"]);

      const branch2 = expectList(
        list.core
          .createBranch("feature-branch-2", group.id)
          .getCurrentContent(),
      );

      await new Promise((resolve) => setTimeout(resolve, 5));

      // Add different items to second branch
      branch2.appendItems(["apples", "oranges", "carrots"]);

      const anotherSession = client.spawnNewSession();

      const loadedBranch2 = await loadCoValueOrFail(
        anotherSession.node,
        branch2.id,
      );

      // Add more items and remove some existing ones
      loadedBranch2.appendItems(["tomatoes", "lettuce", "cucumber"]);
      loadedBranch2.delete(loadedBranch2.asArray().indexOf("lettuce"));
      loadedBranch2.delete(loadedBranch2.asArray().indexOf("milk"));

      loadedBranch2.core.mergeBranch();

      await loadedBranch2.core.waitForSync();

      branch1.core.mergeBranch();

      expect(list.toJSON()).toEqual([
        "bread",
        "apples",
        "oranges",
        "carrots",
        "tomatoes",
        "cucumber",
        "cheese",
      ]);
    });

    test("should work with co.plainText when merging the same branch twice on different sessions", async () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = client.node.createGroup();
      const plainText = group.createPlainText();

      plainText.insertAfter(0, "hello");

      const branch = expectPlainText(
        plainText.core
          .createBranch("feature-branch", group.id)
          .getCurrentContent(),
      );

      branch.insertAfter("hello".length, " world");

      const anotherSession = client.spawnNewSession();

      const loadedBranch = await loadCoValueOrFail(
        anotherSession.node,
        branch.id,
      );
      assert(loadedBranch);

      anotherSession.connectToSyncServer().peerState.gracefulShutdown();

      // Add more items to the branch
      loadedBranch.insertAfter("hello world".length, " people");

      branch.core.mergeBranch();
      const loadedBranchMergeResult = loadedBranch.core.mergeBranch();

      anotherSession.connectToSyncServer();
      await loadedBranchMergeResult.waitForSync();

      expect(plainText.toString()).toEqual("hello world people");
    });
  });

  describe("Branch Loading and Checkout", () => {
    test("should create new branch when checking out non-existent branch", async () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = jazzCloud.node.createGroup();
      group.addMember("everyone", "writer");

      const originalMap = group.createMap();
      const branchName = "feature-branch";

      // Add initial data to original map
      originalMap.set("key1", "value1");
      originalMap.set("key2", "value2");

      // Checkout non-existent branch - should create new one
      const branch = await client.node.checkoutBranch(
        originalMap.id,
        branchName,
      );

      expect(branch).not.toBe("unavailable");

      if (branch !== "unavailable") {
        // Verify branch inherits original data
        expect(branch.get("key1")).toBe("value1");
        expect(branch.get("key2")).toBe("value2");

        // Add new data to branch
        branch.set("branchKey", "branchValue");

        await branch.core.waitForSync();

        // Verify original map doesn't have branch data
        expect(originalMap.get("branchKey")).toBe(undefined);
        expect(branch.get("branchKey")).toBe("branchValue");
      }
    });

    test("should load existing branch when checking out created branch", async () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = jazzCloud.node.createGroup();
      group.addMember("everyone", "writer");

      const originalMap = group.createMap();
      const branchName = "feature-branch";

      // Add initial data to original map
      originalMap.set("key1", "value1");
      originalMap.set("key2", "value2");

      // Create branch with some data
      const originalBranch = expectMap(
        originalMap.core.createBranch(branchName, group.id).getCurrentContent(),
      );

      originalBranch.set("branchKey", "branchValue");

      // Checkout existing branch - should return created branch
      const branch = await client.node.checkoutBranch(
        originalMap.id,
        branchName,
      );

      expect(branch).not.toBe("unavailable");

      if (branch !== "unavailable") {
        // Verify branch contains both original and new data
        expect(branch.get("key1")).toBe("value1");
        expect(branch.get("key2")).toBe("value2");
        expect(branch.get("branchKey")).toBe("branchValue");
      }
    });

    test("should successfully load existing branch via node.load", async () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = jazzCloud.node.createGroup();
      group.addMember("everyone", "writer");

      const originalMap = group.createMap();
      const branchName = "feature-branch";

      // Add initial data to original map
      originalMap.set("key1", "value1");
      originalMap.set("key2", "value2");

      // Create branch with some data
      const originalBranch = expectMap(
        originalMap.core.createBranch(branchName, group.id).getCurrentContent(),
      );

      originalBranch.set("branchKey", "branchValue");

      // Load existing branch via node.load
      const branch = await client.node.load(originalBranch.id);

      expect(branch).not.toBe("unavailable");

      if (branch !== "unavailable") {
        // Verify branch contains all expected data
        expect(branch.get("key1")).toBe("value1");
        expect(branch.get("key2")).toBe("value2");
        expect(branch.get("branchKey")).toBe("branchValue");
      }
    });

    test("should create branch with different group owner when specified", async () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = jazzCloud.node.createGroup();
      group.addMember("everyone", "writer");

      const originalMap = group.createMap();
      const branchName = "feature-branch";

      // Add initial data to original map
      originalMap.set("key1", "value1");
      originalMap.set("key2", "value2");

      // Create different group to own the branch
      const branchGroup = client.node.createGroup();

      // Checkout branch with different group as owner
      const branch = await client.node.checkoutBranch(
        originalMap.id,
        branchName,
        branchGroup.id,
      );

      expect(branch).not.toBe("unavailable");

      if (branch !== "unavailable") {
        // Verify branch inherits original data
        expect(branch.get("key1")).toBe("value1");
        expect(branch.get("key2")).toBe("value2");

        // Add new data to branch
        branch.set("branchKey", "branchValue");

        await branch.core.waitForSync();

        // Verify original map doesn't have branch data
        expect(originalMap.get("branchKey")).toBe(undefined);
        expect(branch.get("branchKey")).toBe("branchValue");
      }

      // Verify that the sync server can't read the branch content
      const branchOnTheServer = await jazzCloud.node.checkoutBranch(
        originalMap.id,
        branchName,
        branchGroup.id,
      );

      expect(branchOnTheServer).not.toBe("unavailable");

      if (branchOnTheServer !== "unavailable") {
        expect(branchOnTheServer.get("branchKey")).toBe(undefined);
        expect(branchOnTheServer.get("key1")).toBe(undefined);
        expect(branchOnTheServer.get("key2")).toBe(undefined);
      }
    });

    test("should return the source value when trying to checkout branch from group", async () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = jazzCloud.node.createGroup();
      group.addMember("everyone", "writer");

      // Try to checkout branch from group (groups don't support branching)
      const branch = await client.node.checkoutBranch(
        group.id,
        "feature-branch",
      );

      expect(branch).not.toBe("unavailable");

      if (branch !== "unavailable") {
        expect(branch.id).toBe(group.id);
      }
    });

    test("should return unavailable when source value is unreachable", async () => {
      // Create client without sync server connection
      const client = setupTestNode();

      const group = jazzCloud.node.createGroup();
      group.addMember("everyone", "writer");

      // Create map on sync server (unreachable from client)
      const originalMap = group.createMap();
      originalMap.set("key1", "value1");

      // Try to checkout branch on unreachable map
      const branch = await client.node.checkoutBranch(
        originalMap.id,
        "feature-branch",
      );

      // Should return unavailable since checkout failed
      expect(branch).toBe("unavailable");
    });
  });

  describe("Branch Conflict Resolution", () => {
    test("should successfully handle concurrent branch creation on different nodes", async () => {
      const bob = setupTestNode();
      const { peer: bobPeer } = bob.connectToSyncServer();
      const alice = setupTestNode({
        connected: true,
      });

      const client = setupTestNode();
      const group = jazzCloud.node.createGroup();
      group.addMember("everyone", "writer");

      // Create map without any branches
      const originalMap = group.createMap();

      const originalMapOnBob = await loadCoValueOrFail(
        bob.node,
        originalMap.id,
      );

      // Disconnect bob from sync server to create isolation
      bobPeer.outgoing.close();

      // Create branches on different nodes
      const aliceBranch = await alice.node.checkoutBranch(
        originalMap.id,
        "feature-branch",
      );
      const bobBranch = expectMap(
        originalMapOnBob.core
          .createBranch("feature-branch", group.id)
          .getCurrentContent(),
      );

      if (aliceBranch === "unavailable") {
        throw new Error("Alice branch is unavailable");
      }

      // Add different data to each branch
      bobBranch.set("bob", true);
      aliceBranch.set("alice", true);

      // Reconnect bob to sync server
      bob.connectToSyncServer();

      // Wait for sync to complete
      await bobBranch.core.waitForSync();
      await aliceBranch.core.waitForSync();

      // Verify both branches now contain data from the other
      expect(bobBranch.get("alice")).toBe(true);
      expect(aliceBranch.get("bob")).toBe(true);
    });
  });

  test("should alias the txID when a transaction comes from a merge", async () => {
    const client = setupTestNode({
      connected: true,
    });
    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("key", "value");

    const branch = map.core
      .createBranch("feature-branch", group.id)
      .getCurrentContent() as RawCoMap;
    branch.set("branchKey", "branchValue");

    const originalTxID = branch.core
      .getValidTransactions({
        skipBranchSource: true,
        ignorePrivateTransactions: false,
      })
      .at(-1)?.txID;

    branch.core.mergeBranch();

    map.set("key2", "value2");

    const validSortedTransactions = map.core.getValidSortedTransactions();

    // Only the merged transaction should have the txId changed
    const mergedTransactionIdx = validSortedTransactions.findIndex(
      (tx) => tx.txID.branch,
    );

    expect(validSortedTransactions[mergedTransactionIdx - 1]?.txID.branch).toBe(
      undefined,
    );
    expect(validSortedTransactions[mergedTransactionIdx]?.txID).toEqual(
      originalTxID,
    );
    expect(validSortedTransactions[mergedTransactionIdx + 1]?.txID.branch).toBe(
      undefined,
    );
  });

  describe("hasBranch", () => {
    test("should work when the branch owner is the source owner", () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = client.node.createGroup();
      const map = group.createMap();

      map.set("key", "value");

      const branch = map.core.createBranch("feature-branch", group.id);

      expect(map.core.hasBranch("feature-branch")).toBe(true);
      expect(map.core.hasBranch("feature-branch", group.id)).toBe(true);
      expect(branch.hasBranch("feature-branch")).toBe(false);
    });

    test("should work when the branch onwer is implicit", () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = client.node.createGroup();
      const map = group.createMap();

      map.set("key", "value");

      const branch = map.core.createBranch("feature-branch");

      expect(map.core.hasBranch("feature-branch")).toBe(true);
      expect(map.core.hasBranch("feature-branch", group.id)).toBe(true);
      expect(branch.hasBranch("feature-branch")).toBe(false);
    });

    test("should return false for non-existent branch name", () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = client.node.createGroup();
      const map = group.createMap();

      map.set("key", "value");

      expect(map.core.hasBranch("non-existent-branch")).toBe(false);
    });

    test("should work with explicit ownerId parameter", () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = client.node.createGroup();
      const map = group.createMap();

      map.set("key", "value");

      const differentGroup = client.node.createGroup();

      map.core.createBranch("feature-branch", differentGroup.id);

      // Test with explicit ownerId
      expect(map.core.hasBranch("feature-branch", differentGroup.id)).toBe(
        true,
      );
      expect(map.core.hasBranch("feature-branch")).toBe(false);
    });

    test("should work when the transactions have not been parsed yet", async () => {
      const client = setupTestNode({
        connected: true,
      });
      const group = client.node.createGroup();
      const map = group.createMap();

      map.set("key", "value");

      map.core.createBranch("feature-branch", group.id);

      await map.core.waitForSync();

      const newSession = client.spawnNewSession();
      const loadedMapCore = await newSession.node.loadCoValueCore(map.core.id);

      expect(loadedMapCore.hasBranch("feature-branch", group.id)).toBe(true);
    });
  });
});
