import { cojsonInternals } from "cojson";
import { assert, beforeEach, describe, expect, test, vi } from "vitest";
import { Group, co, z } from "../exports.js";

import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { assertLoaded, waitFor } from "./utils.js";

beforeEach(async () => {
  cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 1000;

  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });
});

describe("CoFeed Branching", async () => {
  let me: any;

  beforeEach(async () => {
    me = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Test User" },
    });
  });

  describe("basic branch operations", () => {
    test("create a branch on a single CoFeed, edit and merge", async () => {
      const TestStream = co.feed(z.string());

      // Create the original CoFeed
      const originalFeed = TestStream.create(["milk", "bread", "butter"], {
        owner: me,
      });

      // Create a branch
      const branchFeed = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "feature-branch" },
      });

      assertLoaded(branchFeed);

      expect(branchFeed.$jazz.branchName).toBe("feature-branch");
      expect(branchFeed.$jazz.isBranched).toBe(true);

      // Edit the branch
      branchFeed.$jazz.push("jam");
      branchFeed.$jazz.push("cheese");

      // Verify the original is unchanged
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("butter");
      expect(originalFeed.perSession[me.$jazz.sessionID]?.value).toEqual(
        "butter",
      );

      // Verify the branch has the changes
      expect(branchFeed.perAccount[me.$jazz.id]?.value).toEqual("cheese");
      expect(branchFeed.perSession[me.$jazz.sessionID]?.value).toEqual(
        "cheese",
      );

      // Merge the branch back
      branchFeed.$jazz.unstable_merge();

      // Verify the original now has the merged changes
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("cheese");
      expect(originalFeed.perSession[me.$jazz.sessionID]?.value).toEqual(
        "cheese",
      );
    });

    test("CoFeed.unstable_merge static method", async () => {
      const TestStream = co.feed(z.string());

      // Create the original CoFeed
      const originalFeed = TestStream.create(["milk", "bread", "butter"], {
        owner: me,
      });

      // Create a branch
      const branchFeed = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "feature-branch" },
      });

      assertLoaded(branchFeed);

      expect(branchFeed.$jazz.branchName).toBe("feature-branch");
      expect(branchFeed.$jazz.isBranched).toBe(true);

      // Edit the branch
      branchFeed.$jazz.push("jam");
      branchFeed.$jazz.push("cheese");

      // Verify the original is unchanged
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("butter");
      expect(originalFeed.perSession[me.$jazz.sessionID]?.value).toEqual(
        "butter",
      );

      // Verify the branch has the changes
      expect(branchFeed.perAccount[me.$jazz.id]?.value).toEqual("cheese");
      expect(branchFeed.perSession[me.$jazz.sessionID]?.value).toEqual(
        "cheese",
      );

      // Merge the branch back
      await TestStream.unstable_merge(originalFeed.$jazz.id, {
        branch: { name: "feature-branch" },
      });

      // Verify the original now has the merged changes
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("cheese");
      expect(originalFeed.perSession[me.$jazz.sessionID]?.value).toEqual(
        "cheese",
      );
    });

    test("create branch and merge without doing any changes", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(["milk", "bread", "butter"], {
        owner: me,
      });

      // Create a branch but don't make any changes
      const branchFeed = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "no-changes-branch" },
      });

      assertLoaded(branchFeed);

      expect(branchFeed.$jazz.branchName).toBe("no-changes-branch");
      expect(branchFeed.$jazz.isBranched).toBe(true);

      // Verify branch has same values as original
      expect(branchFeed.perAccount[me.$jazz.id]?.value).toEqual("butter");
      expect(branchFeed.perSession[me.$jazz.sessionID]?.value).toEqual(
        "butter",
      );

      // Merge the branch without changes
      branchFeed.$jazz.unstable_merge();

      // Verify original is still the same (no changes to merge)
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("butter");
      expect(originalFeed.perSession[me.$jazz.sessionID]?.value).toEqual(
        "butter",
      );
    });

    test("the same user creates the same branch with different starting points", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(["milk", "bread", "butter"]);

      const branch1 = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
      });

      assertLoaded(branch1);

      branch1.$jazz.push("jam");

      const branch2 = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
      });

      assertLoaded(branch2);

      expect(branch1.perAccount[me.$jazz.id]?.value).toEqual("jam");
      expect(branch2.perAccount[me.$jazz.id]?.value).toEqual("jam");

      branch2.$jazz.push("cheese");

      expect(branch1.perAccount[me.$jazz.id]?.value).toEqual("cheese");
      expect(branch2.perAccount[me.$jazz.id]?.value).toEqual("cheese");

      branch1.$jazz.unstable_merge();

      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("cheese");
      expect(originalFeed.perSession[me.$jazz.sessionID]?.value).toEqual(
        "cheese",
      );
    });

    test("two users create the same branch with different starting points", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(
        ["milk", "bread", "butter"],
        Group.create(me).makePublic("writer"),
      );

      const alice = await createJazzTestAccount();
      const bob = await createJazzTestAccount();

      // User 1 creates branch and makes changes
      const branch1 = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
        loadAs: alice,
      });

      assertLoaded(branch1);

      originalFeed.$jazz.push("jam");

      // User 2 creates the same branch (should get the same branch)
      const branch2 = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
        loadAs: bob,
      });

      assertLoaded(branch2);

      // Both branches should have the same changes
      expect(branch1.perAccount[me.$jazz.id]?.value).toEqual("butter");
      expect(branch2.perAccount[me.$jazz.id]?.value).toEqual("jam");

      branch2.$jazz.push("cheese");

      // User 2 makes additional changes
      branch2.$jazz.push("honey");

      // Both branches should now have all changes
      expect(branch1.perAccount[me.$jazz.id]?.value).toEqual("butter");
      expect(branch2.perAccount[bob.$jazz.id]?.value).toEqual("honey");

      // Merge the branch
      branch1.$jazz.unstable_merge();
      branch2.$jazz.unstable_merge();

      await alice.$jazz.waitForAllCoValuesSync();
      await bob.$jazz.waitForAllCoValuesSync();

      // Verify original has all changes
      expect(originalFeed.perAccount[bob.$jazz.id]?.value).toEqual("honey");
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("jam");
    });

    test("a branch is merged twice by the same user", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(
        ["milk", "bread", "butter"],
        Group.create(me).makePublic("writer"),
      );

      const branch = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "double-merge-branch" },
      });

      assertLoaded(branch);

      branch.$jazz.push("jam");

      // First merge
      branch.$jazz.unstable_merge();

      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("jam");

      // Make more changes to the branch
      branch.$jazz.push("cheese");

      // Second merge
      branch.$jazz.unstable_merge();

      // Verify all changes are applied
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("cheese");
      expect(originalFeed.perSession[me.$jazz.sessionID]?.value).toEqual(
        "cheese",
      );
    });

    test("two users merge different branches with different edits", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(
        ["milk", "bread", "butter"],
        Group.create(me).makePublic("writer"),
      );

      const alice = await createJazzTestAccount();
      const bob = await createJazzTestAccount();

      // User 1 creates branch and makes changes
      const branch1 = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "collaborative-branch", owner: alice },
        loadAs: alice,
      });

      assertLoaded(branch1);

      branch1.$jazz.push("jam");

      // User 2 gets the same branch and makes different changes
      const branch2 = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "collaborative-branch", owner: bob },
        loadAs: bob,
      });

      assertLoaded(branch2);

      branch2.$jazz.push("cheese");
      branch2.$jazz.push("honey");

      // Both branches should have all changes
      expect(branch1.perAccount[alice.$jazz.id]?.value).toEqual("jam");
      expect(branch2.perAccount[bob.$jazz.id]?.value).toEqual("honey");
      expect(branch1.perAccount[me.$jazz.id]?.value).toEqual("butter");
      expect(branch2.perAccount[me.$jazz.id]?.value).toEqual("butter");

      // User 1 merges first
      branch1.$jazz.unstable_merge();

      await alice.$jazz.waitForAllCoValuesSync();

      expect(originalFeed.perAccount[alice.$jazz.id]?.value).toEqual("jam");

      // User 2 merges (should be idempotent)
      branch2.$jazz.unstable_merge();

      await bob.$jazz.waitForAllCoValuesSync();

      // Should still have the same values
      expect(originalFeed.perAccount[bob.$jazz.id]?.value).toEqual("honey");
    });

    test("the id of a branch is the source id", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(
        ["milk", "bread", "butter"],
        Group.create(me).makePublic("writer"),
      );

      // Create a branch
      const branch = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "load-by-id-branch" },
      });

      assertLoaded(branch);
      expect(branch.$jazz.id).toBe(originalFeed.$jazz.id);
    });

    test("merge with conflicts resolution", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(["milk", "bread", "butter"], {
        owner: me,
      });

      // User 1 creates a branch and makes changes
      const branch = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "conflict-branch" },
      });

      assertLoaded(branch);

      branch.$jazz.push("jam");
      branch.$jazz.push("cheese");

      await new Promise((resolve) => setTimeout(resolve, 10));

      // Apply conflicting changes to the main branch
      originalFeed.$jazz.push("honey");
      originalFeed.$jazz.push("olive oil");

      await new Promise((resolve) => setTimeout(resolve, 10));

      // Merge the branch
      branch.$jazz.unstable_merge();

      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("olive oil");
    });

    test("the branch always starts from the same point", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(
        ["milk", "bread", "butter"],
        Group.create(me).makePublic("writer"),
      );

      // Create a branch
      const branch = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "align-branch" },
      });

      assertLoaded(branch);

      branch.$jazz.push("jam");

      // Make changes to main while branch exists
      originalFeed.$jazz.push("cheese");
      originalFeed.$jazz.push("honey");

      // Branch should still have its changes
      expect(branch.perAccount[me.$jazz.id]?.value).toEqual("jam");
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("honey");

      const loadedBranch = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "align-branch" },
      });

      assertLoaded(loadedBranch);

      expect(loadedBranch.perAccount[me.$jazz.id]?.value).toEqual("jam");
    });

    test("branching & merging nested coValues", async () => {
      const Text = co.plainText();
      const TextFeed = co.feed(Text);

      // Create a feed with text values
      const textFeed = TextFeed.create(
        ["milk", "bread", "butter"],
        Group.create(me).makePublic("writer"),
      );

      // Create a branch for editing
      const branch = await TextFeed.load(textFeed.$jazz.id, {
        resolve: {
          $each: true,
        },
        unstable_branch: { name: "text-feed-edit" },
      });

      assertLoaded(branch);

      // Make extensive changes to the branch
      branch.$jazz.push(Text.create("jam"));
      branch.$jazz.push(Text.create("cheese"));

      const loadedTextFeed = await TextFeed.load(textFeed.$jazz.id, {
        resolve: {
          $each: true,
        },
      });

      assertLoaded(loadedTextFeed);

      // Verify original is unchanged
      expect(loadedTextFeed.perAccount[me.$jazz.id]?.value?.toString()).toBe(
        "butter",
      );

      // Verify branch has changes
      expect(branch.perAccount[me.$jazz.id]?.value?.toString()).toBe("cheese");

      // Merge the text feed
      branch.$jazz.unstable_merge();

      // Verify all changes are merged
      expect(loadedTextFeed.perAccount[me.$jazz.id]?.value?.toString()).toBe(
        "cheese",
      );
    });
  });

  describe("subscription & loading", () => {
    test("should carry the selected branch when calling value.subscribe, unless specified otherwise", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(["milk", "bread", "butter"], {
        owner: me,
      });

      // Create a branch and make changes
      const branch = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "subscribe-branch" },
      });

      assertLoaded(branch);

      const spy = vi.fn();
      const unsubscribe = branch.$jazz.subscribe((feed, unsubscribe) => {
        expect(feed.$jazz.branchName).not.toBe("subscribe-branch");
        expect(feed.$jazz.isBranched).toBe(false);
        spy();
        unsubscribe();
      });

      branch.$jazz.subscribe(
        {
          unstable_branch: { name: "subscribe-branch" },
        },
        (feed, unsubscribe) => {
          expect(feed.$jazz.branchName).toBe("subscribe-branch");
          expect(feed.$jazz.isBranched).toBe(true);
          spy();
          unsubscribe();
        },
      );

      originalFeed.$jazz.push("jam");
      branch.$jazz.push("cheese");

      // Wait for initial subscription
      await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));

      unsubscribe();
    });

    test("should not carry the selected branch when calling value.ensureLoaded, unless specified otherwise", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(["milk", "bread", "butter"], {
        owner: me,
      });

      // Create a branch and make changes
      const branch = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "ensure-loaded-branch" },
      });

      assertLoaded(branch);

      branch.$jazz.push("jam");
      branch.$jazz.push("cheese");

      // Load the branch using ensureLoaded
      const loadedFeed = await branch.$jazz.ensureLoaded();

      expect(loadedFeed.$jazz.branchName).not.toBe("ensure-loaded-branch");
      expect(loadedFeed.$jazz.isBranched).toBe(false);

      // Load the branch using ensureLoaded
      const withBranch = await branch.$jazz.ensureLoaded({
        unstable_branch: { name: "ensure-loaded-branch" },
      });

      // Verify we get the branch data, not the original data
      expect(withBranch.$jazz.branchName).toBe("ensure-loaded-branch");
      expect(withBranch.$jazz.isBranched).toBe(true);
      expect(withBranch.perAccount[me.$jazz.id]?.value).toBe("cheese");
    });

    test("should checkout the branch when calling Schema.subscribe", async () => {
      const TestStream = co.feed(z.string());

      const originalFeed = TestStream.create(["milk", "bread", "butter"], {
        owner: me,
      });

      // Create a branch and make changes
      const branch = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "schema-subscribe-branch" },
      });

      assertLoaded(branch);

      branch.$jazz.push("jam");
      branch.$jazz.push("cheese");

      // Subscribe using Schema.subscribe with branch
      const updates: any[] = [];
      const unsubscribe = TestStream.subscribe(
        originalFeed.$jazz.id,
        {
          unstable_branch: { name: "schema-subscribe-branch" },
        },
        (feed) => {
          expect(feed.$jazz.branchName).toBe("schema-subscribe-branch");
          expect(feed.$jazz.isBranched).toBe(true);
          updates.push(feed);
        },
      );

      await waitFor(() => expect(updates).toHaveLength(1));
      expect(updates[0]?.perAccount[me.$jazz.id]?.value).toBe("cheese");

      // Make additional changes to the branch
      branch.$jazz.push("honey");

      // Verify we get the updated branch data
      expect(updates[1]?.perAccount[me.$jazz.id]?.value).toBe("honey");

      // Verify original is still unchanged
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toBe("butter");

      unsubscribe();
    });
  });

  describe("CoFeed with nullable values branching", () => {
    test("create a branch on a CoFeed with nullable values", async () => {
      const NullableTestStream = co.feed(z.string().nullable());

      const originalFeed = NullableTestStream.create(["milk", null, "butter"], {
        owner: me,
      });

      // Create a branch
      const branch = await NullableTestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "nullable-branch" },
      });

      assertLoaded(branch);

      // Edit the branch
      branch.$jazz.push("jam");
      branch.$jazz.push(null);

      // Verify the original is unchanged
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual("butter");

      // Verify the branch has the changes
      expect(branch.perAccount[me.$jazz.id]?.value).toEqual(null);

      // Merge the branch back
      branch.$jazz.unstable_merge();

      // Verify the original now has the merged changes
      expect(originalFeed.perAccount[me.$jazz.id]?.value).toEqual(null);
    });
  });

  describe("CoFeed resolution with branching", () => {
    test("branching with nested CoFeeds", async () => {
      const TwiceNestedStream = co.feed(z.string());
      const NestedStream = co.feed(TwiceNestedStream);
      const TestStream = co.feed(NestedStream);

      const originalFeed = TestStream.create(
        [
          NestedStream.create(
            [TwiceNestedStream.create(["milk"], { owner: me })],
            { owner: me },
          ),
        ],
        { owner: me },
      );

      // Create a branch
      const branch = await TestStream.load(originalFeed.$jazz.id, {
        unstable_branch: { name: "nested-feed-branch" },
      });

      assertLoaded(branch);

      // Make changes to the branch
      const newTwiceNested = TwiceNestedStream.create(["bread"], {
        owner: me,
      });
      const newNested = NestedStream.create([newTwiceNested], {
        owner: me,
      });

      branch.$jazz.push(newNested);

      // Verify the original is unchanged
      const myTopLevelStream = originalFeed.perAccount[me.$jazz.id];
      assert(myTopLevelStream);
      assertLoaded(myTopLevelStream.value);
      const myNestedStream = myTopLevelStream.value.perAccount[me.$jazz.id];
      assert(myNestedStream);
      assertLoaded(myNestedStream.value);
      expect(myNestedStream.value.perAccount[me.$jazz.id]?.value).toEqual(
        "milk",
      );

      // Verify the branch has the changes
      const myBranchedTopLevelStream = branch.perAccount[me.$jazz.id];
      assert(myBranchedTopLevelStream);
      assertLoaded(myBranchedTopLevelStream.value);
      const myBranchedNestedStream =
        myBranchedTopLevelStream.value.perAccount[me.$jazz.id];
      assert(myBranchedNestedStream);
      assertLoaded(myBranchedNestedStream.value);
      expect(
        myBranchedNestedStream.value.perAccount[me.$jazz.id]?.value,
      ).toEqual("bread");

      // Merge the branch
      branch.$jazz.unstable_merge();

      // Verify the original now has the merged changes
      const myOriginalTopLevelStream = originalFeed.perAccount[me.$jazz.id];
      assert(myOriginalTopLevelStream);
      assertLoaded(myOriginalTopLevelStream.value);
      const myOriginalNestedStream =
        myOriginalTopLevelStream.value.perAccount[me.$jazz.id];
      assert(myOriginalNestedStream);
      assertLoaded(myOriginalNestedStream.value);
      expect(
        myOriginalNestedStream.value.perAccount[me.$jazz.id]?.value,
      ).toEqual("bread");
    });
  });
});
