import { cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, beforeEach, describe, expect, test } from "vitest";
import { Group, co, subscribeToCoValue, z } from "../exports.js";

import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";

beforeEach(async () => {
  cojsonInternals.CO_VALUE_LOADING_CONFIG.RETRY_DELAY = 1000;

  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
    creationProps: { name: "Hermes Puggington" },
  });
});

describe("CoList Branching", async () => {
  describe("basic branch operations", () => {
    test("create a branch on a single CoValue, edit and merge", async () => {
      const TodoList = co.list(z.string());

      // Create a group to own the CoList
      const group = Group.create();
      group.addMember("everyone", "writer");

      // Create the original CoList
      const originalList = TodoList.create(
        ["Buy groceries", "Walk the dog", "Finish project"],
        group,
      );

      // Create a branch
      const branchList = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "feature-branch" },
      });

      assert(branchList);

      // Edit the branch
      branchList.$jazz.set(0, "Buy organic groceries");
      branchList.$jazz.push("Call mom");
      branchList.$jazz.splice(1, 1); // Remove "Walk the dog"

      // Verify the original is unchanged
      expect(originalList[0]).toBe("Buy groceries");
      expect(originalList[1]).toBe("Walk the dog");
      expect(originalList[2]).toBe("Finish project");
      expect(originalList.length).toBe(3);

      // Verify the branch has the changes
      expect(branchList[0]).toBe("Buy organic groceries");
      expect(branchList[1]).toBe("Finish project");
      expect(branchList[2]).toBe("Call mom");
      expect(branchList.length).toBe(3);

      // Merge the branch back
      branchList.$jazz.unstable_merge();

      // Verify the original now has the merged changes
      expect(originalList[0]).toBe("Buy organic groceries");
      expect(originalList[1]).toBe("Finish project");
      expect(originalList[2]).toBe("Call mom");
      expect(originalList.length).toBe(3);
    });

    test("create branch and merge without doing any changes", async () => {
      const TodoList = co.list(z.string());

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalList = TodoList.create(
        ["Buy groceries", "Walk the dog", "Finish project"],
        group,
      );

      // Create a branch but don't make any changes
      const branchList = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "no-changes-branch" },
      });

      assert(branchList);

      // Verify branch has same values as original
      expect(branchList[0]).toBe("Buy groceries");
      expect(branchList[1]).toBe("Walk the dog");
      expect(branchList[2]).toBe("Finish project");
      expect(branchList.length).toBe(3);

      // Merge the branch without changes
      branchList.$jazz.unstable_merge();

      // Verify original is still the same (no changes to merge)
      expect(originalList[0]).toBe("Buy groceries");
      expect(originalList[1]).toBe("Walk the dog");
      expect(originalList[2]).toBe("Finish project");
      expect(originalList.length).toBe(3);
    });

    test("the same user creates the same branch with different starting points", async () => {
      const TodoList = co.list(z.string());

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalList = TodoList.create(
        ["Buy groceries", "Walk the dog", "Finish project"],
        group,
      );

      // User 1 creates branch and makes changes
      const branch1 = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
      });

      assert(branch1);

      branch1.$jazz.set(0, "Buy organic groceries");
      branch1.$jazz.push("Call mom");

      // User 2 creates the same branch (should get the same branch)
      const branch2 = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
      });

      assert(branch2);

      // Both branches should have the same changes
      expect(branch1[0]).toBe("Buy organic groceries");
      expect(branch1[3]).toBe("Call mom");
      expect(branch2[0]).toBe("Buy organic groceries");
      expect(branch2[3]).toBe("Call mom");

      // User 2 makes additional changes
      branch2.$jazz.set(1, "Walk the cat");

      // Both branches should now have all changes
      expect(branch1[1]).toBe("Walk the cat");
      expect(branch2[1]).toBe("Walk the cat");

      // Merge the branch
      branch1.$jazz.unstable_merge();

      // Verify original has all changes
      expect(originalList[0]).toBe("Buy organic groceries");
      expect(originalList[1]).toBe("Walk the cat");
      expect(originalList[2]).toBe("Finish project");
      expect(originalList[3]).toBe("Call mom");
    });

    test("two users create the same branch with different starting points", async () => {
      const TodoList = co.list(z.string());

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalList = TodoList.create(
        ["Buy groceries", "Walk the dog", "Finish project"],
        group,
      );

      const alice = await createJazzTestAccount();
      const bob = await createJazzTestAccount();

      // User 1 creates branch and makes changes
      const branch1 = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
        loadAs: alice,
      });

      assert(branch1);

      originalList.$jazz.set(0, "Buy organic groceries");
      originalList.$jazz.push("Call mom");

      // User 2 creates the same branch (should get the same branch)
      const branch2 = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
        loadAs: bob,
      });

      assert(branch2);

      // Both branches should have the same changes
      expect(branch1[0]).toBe("Buy groceries");
      expect(branch1[1]).toBe("Walk the dog");
      expect(branch1[2]).toBe("Finish project");
      expect(branch2[0]).toBe("Buy groceries");
      expect(branch2[1]).toBe("Walk the dog");
      expect(branch2[2]).toBe("Finish project");

      branch2.$jazz.set(1, "Walk the cat");

      // User 2 makes additional changes
      branch2.$jazz.push("Schedule dentist");

      // Both branches should now have all changes
      expect(branch1[1]).toBe("Walk the dog");
      expect(branch2[1]).toBe("Walk the cat");
      expect(branch2[3]).toBe("Schedule dentist");

      // Merge the branch
      branch1.$jazz.unstable_merge();
      branch2.$jazz.unstable_merge();

      await alice.$jazz.waitForAllCoValuesSync();
      await bob.$jazz.waitForAllCoValuesSync();

      // Verify original has all changes
      expect(originalList[0]).toBe("Buy organic groceries");
      expect(originalList[1]).toBe("Walk the cat");
      expect(originalList[2]).toBe("Finish project");
      expect(originalList[3]).toBe("Schedule dentist");
      expect(originalList[4]).toBe("Call mom");
    });

    test("a branch is merged twice by the same user", async () => {
      const TodoList = co.list(z.string());

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalList = TodoList.create(
        ["Buy groceries", "Walk the dog", "Finish project"],
        group,
      );

      const branch = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "double-merge-branch" },
      });

      assert(branch);

      branch.$jazz.set(0, "Buy organic groceries");
      branch.$jazz.push("Call mom");

      // First merge
      branch.$jazz.unstable_merge();

      expect(originalList[0]).toBe("Buy organic groceries");
      expect(originalList[3]).toBe("Call mom");

      // Make more changes to the branch
      branch.$jazz.set(1, "Walk the cat");

      // Second merge
      branch.$jazz.unstable_merge();

      // Verify all changes are applied
      expect(originalList[0]).toBe("Buy organic groceries");
      expect(originalList[1]).toBe("Walk the cat");
      expect(originalList[2]).toBe("Finish project");
      expect(originalList[3]).toBe("Call mom");
    });

    test("two users merge different branches with different edits", async () => {
      const TodoList = co.list(z.string());

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalList = TodoList.create(
        ["Buy groceries", "Walk the dog", "Finish project"],
        group,
      );

      const alice = await createJazzTestAccount();
      const bob = await createJazzTestAccount();

      // User 1 creates branch and makes changes
      const branch1 = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "collaborative-branch", owner: alice },
        loadAs: alice,
      });

      assert(branch1);

      branch1.$jazz.set(0, "Buy organic groceries");

      // User 2 gets the same branch and makes different changes
      const branch2 = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "collaborative-branch", owner: bob },
        loadAs: bob,
      });

      assert(branch2);

      branch2.$jazz.set(1, "Walk the cat");
      branch2.$jazz.push("Schedule dentist");

      // Both branches should have all changes
      expect(branch1[0]).toBe("Buy organic groceries");
      expect(branch1[1]).toBe("Walk the dog");
      expect(branch1[2]).toBe("Finish project");

      expect(branch2[0]).toBe("Buy groceries");
      expect(branch2[1]).toBe("Walk the cat");
      expect(branch2[3]).toBe("Schedule dentist");

      // User 1 merges first
      branch1.$jazz.unstable_merge();

      await alice.$jazz.waitForAllCoValuesSync();

      expect(originalList[0]).toBe("Buy organic groceries");
      expect(originalList[1]).toBe("Walk the dog");
      expect(originalList[2]).toBe("Finish project");

      // User 2 merges (should be idempotent)
      branch2.$jazz.unstable_merge();

      await bob.$jazz.waitForAllCoValuesSync();

      // Should still have the same values
      expect(originalList[0]).toBe("Buy organic groceries");
      expect(originalList[1]).toBe("Walk the cat");
      expect(originalList[2]).toBe("Finish project");
      expect(originalList[3]).toBe("Schedule dentist");
    });

    test("the id of a branch is the source id", async () => {
      const TodoList = co.list(z.string());

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalList = TodoList.create(
        ["Buy groceries", "Walk the dog", "Finish project"],
        group,
      );

      // Create a branch
      const branch = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "load-by-id-branch" },
      });

      assert(branch);

      expect(branch.$jazz.id).toBe(originalList.$jazz.id);
    });

    test("merge with conflicts resolution", async () => {
      const TodoList = co.list(z.string());

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalList = TodoList.create(
        ["Buy groceries", "Walk the dog", "Finish project"],
        group,
      );

      // User 1 creates a branch and makes changes
      const branch = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "conflict-branch" },
      });

      assert(branch);

      branch.$jazz.set(0, "Buy organic groceries");
      branch.$jazz.set(1, "Walk the cat");

      await new Promise((resolve) => setTimeout(resolve, 10));

      // Apply conflicting changes to the main branch
      originalList.$jazz.set(1, "Walk the hamster");
      originalList.$jazz.push("Call mom");

      await new Promise((resolve) => setTimeout(resolve, 10));

      // Merge the branch
      branch.$jazz.unstable_merge();

      expect(originalList[0]).toBe("Buy organic groceries");
      expect(originalList[1]).toBe("Walk the hamster");
      expect(originalList[2]).toBe("Walk the cat");
      expect(originalList[3]).toBe("Finish project");
      expect(originalList[4]).toBe("Call mom");
    });

    test("the branch always starts from the same point", async () => {
      const TodoList = co.list(z.string());

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalList = TodoList.create(
        ["Buy groceries", "Walk the dog", "Finish project"],
        group,
      );

      // Create a branch
      const branch = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "align-branch" },
      });

      assert(branch);

      branch.$jazz.set(0, "Buy organic groceries");

      // Make changes to main while branch exists
      originalList.$jazz.set(1, "Walk the cat");
      originalList.$jazz.push("Call mom");

      // Branch should still have its changes
      expect(branch[0]).toBe("Buy organic groceries");
      expect(branch[1]).toBe("Walk the dog"); // original value
      expect(branch[2]).toBe("Finish project"); // original value
      expect(branch.length).toBe(3); // original length

      const loadedBranch = await TodoList.load(originalList.$jazz.id, {
        unstable_branch: { name: "align-branch" },
      });

      assert(loadedBranch);

      expect(loadedBranch[0]).toBe("Buy organic groceries");
      expect(loadedBranch[1]).toBe("Walk the dog");
      expect(loadedBranch[2]).toBe("Finish project");
      expect(loadedBranch.length).toBe(3);
    });

    test("branching & merging nested coValues", async () => {
      const Task = co.map({
        title: z.string(),
        completed: z.boolean(),
        priority: z.enum(["low", "medium", "high"]),
      });

      const TaskList = co.list(Task);

      const group = Group.create();
      group.addMember("everyone", "writer");

      // Create a task list with many tasks
      const taskList = TaskList.create(
        [
          { title: "Buy groceries", completed: false, priority: "medium" },
          { title: "Walk the dog", completed: false, priority: "high" },
          { title: "Finish project", completed: true, priority: "high" },
          { title: "Call mom", completed: false, priority: "low" },
          { title: "Schedule dentist", completed: false, priority: "medium" },
        ],
        group,
      );

      // Create a branch for editing
      const branch = await TaskList.load(taskList.$jazz.id, {
        resolve: {
          $each: true,
        },
        unstable_branch: { name: "task-list-edit" },
      });

      assert(branch);

      // Make extensive changes to the branch
      branch.$jazz.set(0, {
        title: "Buy organic groceries",
        completed: false,
        priority: "high",
      });
      branch.$jazz.splice(1, 1); // Remove "Walk the dog"
      branch.$jazz.push({
        title: "Review code",
        completed: false,
        priority: "medium",
      });
      branch.$jazz.set(2, {
        title: "Call mom",
        completed: true,
        priority: "low",
      }); // Mark as completed

      const loadedTaskList = await TaskList.load(taskList.$jazz.id, {
        resolve: {
          $each: true,
        },
      });

      assert(loadedTaskList);

      // Verify original is unchanged
      expect(loadedTaskList[0]!.title).toBe("Buy groceries");
      expect(loadedTaskList[0]!.priority).toBe("medium");
      expect(loadedTaskList[1]!.title).toBe("Walk the dog");
      expect(loadedTaskList.length).toBe(5);

      // Verify branch has changes
      expect(branch[0]!.title).toBe("Buy organic groceries");
      expect(branch[0]!.priority).toBe("high");
      expect(branch[1]!.title).toBe("Finish project");
      expect(branch[2]!.title).toBe("Call mom");
      expect(branch[2]!.completed).toBe(true);
      expect(branch[3]!.title).toBe("Schedule dentist");
      expect(branch[4]!.title).toBe("Review code");
      expect(branch.length).toBe(5);

      // Merge the task list
      branch.$jazz.unstable_merge();

      // Verify all changes are merged
      expect(loadedTaskList[0]!.title).toBe("Buy organic groceries");
      expect(loadedTaskList[0]!.priority).toBe("high");
      expect(loadedTaskList[1]!.title).toBe("Finish project");
      expect(loadedTaskList[2]!.title).toBe("Call mom");
      expect(loadedTaskList[2]!.completed).toBe(true);
      expect(loadedTaskList[3]!.title).toBe("Schedule dentist");
      expect(loadedTaskList[4]!.title).toBe("Review code");
      expect(loadedTaskList.length).toBe(5);
    });
  });
});
