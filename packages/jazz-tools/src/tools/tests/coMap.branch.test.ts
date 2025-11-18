import { cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, beforeEach, describe, expect, test, vi } from "vitest";
import { Group, co, subscribeToCoValue, z } from "../exports.js";

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

describe("CoMap Branching", async () => {
  describe("basic branch operations", () => {
    test("create a branch on a single CoValue, edit and merge", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
      });

      // Create a group to own the CoMap
      const group = Group.create();
      group.addMember("everyone", "writer");

      // Create the original CoMap
      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
        },
        group,
      );

      // Create a branch
      const branchPerson = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "feature-branch" },
      });

      assertLoaded(branchPerson);

      // Edit the branch
      branchPerson.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
        email: "john.smith@example.com",
      });

      // Verify the original is unchanged
      expect(originalPerson.name).toBe("John Doe");
      expect(originalPerson.age).toBe(30);
      expect(originalPerson.email).toBe("john@example.com");

      // Verify the branch has the changes
      expect(branchPerson.name).toBe("John Smith");
      expect(branchPerson.age).toBe(31);
      expect(branchPerson.email).toBe("john.smith@example.com");

      // Merge the branch back
      branchPerson.$jazz.unstable_merge();

      // Verify the original now has the merged changes
      expect(originalPerson.name).toBe("John Smith");
      expect(originalPerson.age).toBe(31);
      expect(originalPerson.email).toBe("john.smith@example.com");
    });

    test("CoMap.unstable_merge static method", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
        dog: co.map({
          name: z.string(),
          breed: z.string(),
        }),
      });

      // Create a group to own the CoMap
      const group = Group.create();
      group.addMember("everyone", "writer");

      // Create the original CoMap
      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
          dog: { name: "Rex", breed: "Labrador" },
        },
        group,
      );

      // Create a branch
      const branchPerson = await Person.load(originalPerson.$jazz.id, {
        resolve: {
          dog: true,
        },
        unstable_branch: { name: "feature-branch" },
      });

      assertLoaded(branchPerson);

      // Edit the branch
      branchPerson.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
        email: "john.smith@example.com",
      });

      branchPerson.dog.$jazz.applyDiff({
        name: "Giggino",
        breed: "Border Collie",
      });

      // Merge the branch back
      await Person.unstable_merge(originalPerson.$jazz.id, {
        resolve: {
          dog: true,
        },
        branch: { name: "feature-branch" },
      });

      // Verify the original now has the merged changes
      expect(originalPerson.name).toBe("John Smith");
      expect(originalPerson.age).toBe(31);
      expect(originalPerson.email).toBe("john.smith@example.com");
      expect(originalPerson.dog.name).toBe("Giggino");
      expect(originalPerson.dog.breed).toBe("Border Collie");
    });

    test("create branch and merge without doing any changes", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
        },
        group,
      );

      // Create a branch but don't make any changes
      const branchPerson = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "no-changes-branch" },
      });

      assertLoaded(branchPerson);

      // Verify branch has same values as original
      expect(branchPerson.name).toBe("John Doe");
      expect(branchPerson.age).toBe(30);
      expect(branchPerson.email).toBe("john@example.com");

      // Merge the branch without changes
      branchPerson.$jazz.unstable_merge();

      // Verify original is still the same (no changes to merge)
      expect(originalPerson.name).toBe("John Doe");
      expect(originalPerson.age).toBe(30);
      expect(originalPerson.email).toBe("john@example.com");
    });

    test("the same user creates the same branch with different starting points", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
        },
        group,
      );

      // User 1 creates branch and makes changes
      const branch1 = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
      });

      assertLoaded(branch1);

      branch1.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
      });

      // User 2 creates the same branch (should get the same branch)
      const branch2 = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
      });

      assertLoaded(branch2);

      // Both branches should have the same changes
      expect(branch1.name).toBe("John Smith");
      expect(branch1.age).toBe(31);
      expect(branch2.name).toBe("John Smith");
      expect(branch2.age).toBe(31);

      // User 2 makes additional changes
      branch2.$jazz.applyDiff({
        email: "john.smith@newdomain.com",
      });

      // Both branches should now have all changes
      expect(branch1.email).toBe("john.smith@newdomain.com");
      expect(branch2.email).toBe("john.smith@newdomain.com");

      // Merge the branch
      branch1.$jazz.unstable_merge();

      // Verify original has all changes
      expect(originalPerson.name).toBe("John Smith");
      expect(originalPerson.age).toBe(31);
      expect(originalPerson.email).toBe("john.smith@newdomain.com");
    });

    test("two users create the same branch with different starting points", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
        },
        group,
      );

      const alice = await createJazzTestAccount();
      const bob = await createJazzTestAccount();

      // User 1 creates branch and makes changes
      const branch1 = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
        loadAs: alice,
      });

      assertLoaded(branch1);

      originalPerson.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
      });

      // User 2 creates the same branch (should get the same branch)
      const branch2 = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "shared-branch" },
        loadAs: bob,
      });

      assertLoaded(branch2);

      // Both branches should have the same changes
      expect(branch1.name).toBe("John Doe");
      expect(branch1.age).toBe(30);
      expect(branch2.name).toBe("John Smith");
      expect(branch2.age).toBe(30);

      branch2.$jazz.applyDiff({
        age: 32,
      });

      // User 2 makes additional changes
      branch2.$jazz.applyDiff({
        email: "john.smith@newdomain.com",
      });

      // Both branches should now have all changes
      expect(branch1.email).toBe("john@example.com");
      expect(branch2.email).toBe("john.smith@newdomain.com");

      // Merge the branch
      branch1.$jazz.unstable_merge();
      branch2.$jazz.unstable_merge();

      await alice.$jazz.waitForAllCoValuesSync();
      await bob.$jazz.waitForAllCoValuesSync();

      // Verify original has all changes
      expect(originalPerson.name).toBe("John Smith");
      expect(originalPerson.age).toBe(32);
      expect(originalPerson.email).toBe("john.smith@newdomain.com");
    });

    test("a branch is merged twice by the same user", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
        },
        group,
      );

      const branch = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "double-merge-branch" },
      });

      assertLoaded(branch);

      branch.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
      });

      // First merge
      branch.$jazz.unstable_merge();

      expect(originalPerson.name).toBe("John Smith");
      expect(originalPerson.age).toBe(31);

      // Make more changes to the branch
      branch.$jazz.applyDiff({
        email: "john.smith@newdomain.com",
      });

      // Second merge
      branch.$jazz.unstable_merge();

      // Verify all changes are applied
      expect(originalPerson.name).toBe("John Smith");
      expect(originalPerson.age).toBe(31);
      expect(originalPerson.email).toBe("john.smith@newdomain.com");
    });

    test("two users merge different branches with different edits", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
        },
        group,
      );

      const alice = await createJazzTestAccount();
      const bob = await createJazzTestAccount();

      // User 1 creates branch and makes changes
      const branch1 = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "collaborative-branch", owner: alice },
        loadAs: alice,
      });

      assertLoaded(branch1);

      branch1.$jazz.applyDiff({
        name: "John Smith",
      });

      // User 2 gets the same branch and makes different changes
      const branch2 = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "collaborative-branch", owner: bob },
        loadAs: bob,
      });

      assertLoaded(branch2);

      branch2.$jazz.applyDiff({
        age: 31,
        email: "john.smith@newdomain.com",
      });

      // Both branches should have all changes
      expect(branch1.name).toBe("John Smith");
      expect(branch1.age).toBe(30);
      expect(branch1.email).toBe("john@example.com");

      expect(branch2.name).toBe("John Doe");
      expect(branch2.age).toBe(31);
      expect(branch2.email).toBe("john.smith@newdomain.com");

      // User 1 merges first
      branch1.$jazz.unstable_merge();

      await alice.$jazz.waitForAllCoValuesSync();

      expect(originalPerson.name).toBe("John Smith");
      expect(originalPerson.age).toBe(30);
      expect(originalPerson.email).toBe("john@example.com");

      // User 2 merges (should be idempotent)
      branch2.$jazz.unstable_merge();

      await bob.$jazz.waitForAllCoValuesSync();

      // Should still have the same values
      expect(originalPerson.name).toBe("John Smith");
      expect(originalPerson.age).toBe(31);
      expect(originalPerson.email).toBe("john.smith@newdomain.com");
    });

    test("the id of a branch is the source id", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
        },
        group,
      );

      // Create a branch
      const branch = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "load-by-id-branch" },
      });

      assertLoaded(branch);
      expect(branch.$jazz.id).toBe(originalPerson.$jazz.id);
    });

    test("merge with conflicts resolution", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
        },
        group,
      );

      const branch = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "conflict-branch" },
      });

      assertLoaded(branch);

      // User 1 creates a branch and makes changes
      branch.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
      });

      // Wait some time to make the output deterministic and not based on the random sessionIDs
      await new Promise((resolve) => setTimeout(resolve, 10));

      // The same field is modified after the branch on main
      originalPerson.$jazz.applyDiff({
        email: "john.doe@company.com",
        age: 32,
      });

      await new Promise((resolve) => setTimeout(resolve, 10));

      // Merge the branch
      branch.$jazz.unstable_merge();

      expect(originalPerson.name).toBe("John Smith");
      expect(originalPerson.email).toBe("john.doe@company.com");
      // Age conflict: branch had 31, main had 32 - last writer wins so main wins
      expect(originalPerson.age).toBe(32);
    });

    test("the branch always starts from the same point", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
        },
        group,
      );

      // Create a branch
      const branch = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "align-branch" },
      });

      assertLoaded(branch);

      branch.$jazz.applyDiff({
        name: "John Smith",
      });

      // Make changes to main while branch exists
      originalPerson.$jazz.applyDiff({
        age: 31,
        email: "john.doe@company.com",
      });

      // Branch should still have its changes
      expect(branch.name).toBe("John Smith");
      expect(branch.age).toBe(30); // original value
      expect(branch.email).toBe("john@example.com"); // original value

      const loadedBranch = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "align-branch" },
      });

      assertLoaded(loadedBranch);

      expect(loadedBranch.name).toBe("John Smith");
      expect(loadedBranch.age).toBe(30);
      expect(loadedBranch.email).toBe("john@example.com");
    });

    test("branching & merging nested coValues", async () => {
      const LargeDocument = co.map({
        title: z.string(),
        content: z.string(),
        metadata: co.record(z.string(), z.string()),
        tags: co.list(z.string()),
        version: z.number(),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      // Create a large document with many properties
      const largeDoc = LargeDocument.create(
        {
          title: "Large Document",
          content:
            "This is a very long content that represents a large document with lots of text and information that would be typical in a real-world application.",
          metadata: {
            author: "John Doe",
            created: "2024-01-01",
            modified: "2024-01-01",
            category: "Technical",
            department: "Engineering",
            priority: "High",
            status: "Draft",
            reviewer: "Alice Smith",
            deadline: "2024-02-01",
            budget: "10000",
          },
          tags: [
            "technical",
            "documentation",
            "engineering",
            "draft",
            "high-priority",
          ],
          version: 1,
        },
        group,
      );

      // Create a branch for editing
      const branch = await LargeDocument.load(largeDoc.$jazz.id, {
        resolve: {
          metadata: true,
          tags: true,
        },
        unstable_branch: { name: "large-doc-edit" },
      });

      assertLoaded(branch);

      // Make extensive changes to the branch
      branch.$jazz.applyDiff({
        title: "Large Document - Updated",
        content:
          "This is an updated version of the very long content that represents a large document with lots of text and information that would be typical in a real-world application. It now includes additional information and improvements.",
        metadata: {
          modified: "2024-01-15",
          status: "In Review",
          reviewer: "Bob Johnson",
        },
        tags: [...branch.tags, "updated", "in-review"],
        version: 2,
      });

      const loadedLargeDoc = await LargeDocument.load(largeDoc.$jazz.id, {
        resolve: {
          metadata: true,
          tags: true,
        },
      });

      assertLoaded(loadedLargeDoc);

      // Verify original is unchanged
      expect(loadedLargeDoc.title).toBe("Large Document");
      expect(loadedLargeDoc.version).toBe(1);
      expect(loadedLargeDoc.metadata.status).toBe("Draft");

      // Verify branch has changes
      expect(branch.title).toBe("Large Document - Updated");
      expect(branch.version).toBe(2);
      expect(branch.metadata.status).toBe("In Review");
      expect(branch.tags).toContain("updated");

      // Merge the large document
      branch.$jazz.unstable_merge();

      // Verify all changes are merged
      expect(loadedLargeDoc.title).toBe("Large Document - Updated");
      expect(loadedLargeDoc.version).toBe(2);
      expect(loadedLargeDoc.metadata.status).toBe("In Review");
      expect(loadedLargeDoc.metadata.reviewer).toBe("Bob Johnson");
      expect(loadedLargeDoc.tags).toContain("updated");
      expect(loadedLargeDoc.tags).toContain("in-review");
      expect(loadedLargeDoc.content).toContain("updated version");
    });
  });

  describe("subscription & loading", () => {
    test("should not carry the selected branch when calling value.subscribe, unless specified otherwise", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
        dog: co.map({
          name: z.string(),
          breed: z.string(),
        }),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
          dog: { name: "Rex", breed: "Labrador" },
        },
        group,
      );

      // Create a branch and make changes
      const branch = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "subscribe-branch" },
      });

      assertLoaded(branch);

      const spy = vi.fn();
      branch.$jazz.subscribe(
        { resolve: { dog: true } },
        (person, unsubscribe) => {
          expect(person.$jazz.branchName).not.toBe("subscribe-branch");
          expect(person.$jazz.isBranched).toBe(false);
          expect(person.dog.$jazz.branchName).not.toBe("subscribe-branch");
          expect(person.dog.$jazz.isBranched).toBe(false);
          spy();
          unsubscribe();
        },
      );

      branch.$jazz.subscribe(
        {
          resolve: { dog: true },
          unstable_branch: { name: "subscribe-branch" },
        },
        (person, unsubscribe) => {
          expect(person.$jazz.branchName).toBe("subscribe-branch");
          expect(person.$jazz.isBranched).toBe(true);
          expect(person.dog.$jazz.branchName).toBe("subscribe-branch");
          expect(person.dog.$jazz.isBranched).toBe(true);
          spy();
          unsubscribe();
        },
      );

      originalPerson.$jazz.applyDiff({
        age: 31,
      });

      branch.$jazz.applyDiff({
        name: "John Smith",
      });

      // Wait for initial subscription
      await waitFor(() => expect(spy).toHaveBeenCalledTimes(2));
    });

    test("should not carry the selected branch when calling value.ensureLoaded, unless specified otherwise", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
        dog: co.map({
          name: z.string(),
          breed: z.string(),
        }),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
          dog: { name: "Rex", breed: "Labrador" },
        },
        group,
      );

      // Create a branch and make changes
      const branch = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "ensure-loaded-branch" },
      });

      assertLoaded(branch);

      branch.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
        email: "john.smith@example.com",
      });

      // Load the branch using ensureLoaded
      const loadedPerson = await branch.$jazz.ensureLoaded({
        resolve: { dog: true },
      });

      expect(loadedPerson.$jazz.branchName).not.toBe("ensure-loaded-branch");
      expect(loadedPerson.$jazz.isBranched).toBe(false);
      expect(loadedPerson.dog.$jazz.branchName).not.toBe(
        "ensure-loaded-branch",
      );
      expect(loadedPerson.dog.$jazz.isBranched).toBe(false);

      const withBranch = await branch.$jazz.ensureLoaded({
        resolve: { dog: true },
        unstable_branch: { name: "ensure-loaded-branch" },
      });

      expect(withBranch.$jazz.branchName).toBe("ensure-loaded-branch");
      expect(withBranch.$jazz.isBranched).toBe(true);
      expect(withBranch.dog.$jazz.branchName).toBe("ensure-loaded-branch");
      expect(withBranch.dog.$jazz.isBranched).toBe(true);

      // Verify we get the branch data, not the original data
      expect(withBranch.name).toBe("John Smith");
      expect(withBranch.age).toBe(31);
      expect(withBranch.email).toBe("john.smith@example.com");
    });

    test("should checkout the branch when calling Schema.subscribe", async () => {
      const Person = co.map({
        name: z.string(),
        age: z.number(),
        email: z.string(),
      });

      const group = Group.create();
      group.addMember("everyone", "writer");

      const originalPerson = Person.create(
        {
          name: "John Doe",
          age: 30,
          email: "john@example.com",
        },
        group,
      );

      // Create a branch and make changes
      const branch = await Person.load(originalPerson.$jazz.id, {
        unstable_branch: { name: "schema-subscribe-branch" },
      });

      assertLoaded(branch);

      branch.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
        email: "john.smith@example.com",
      });

      // Subscribe using Schema.subscribe with branch
      const updates: co.loaded<typeof Person, true>[] = [];
      const unsubscribe = Person.subscribe(
        originalPerson.$jazz.id,
        {
          unstable_branch: { name: "schema-subscribe-branch" },
        },
        (person) => {
          expect(person.$jazz.branchName).toBe("schema-subscribe-branch");
          expect(person.$jazz.isBranched).toBe(true);
          updates.push(person);
        },
      );

      await waitFor(() => expect(updates).toHaveLength(1));
      expect(updates[0]?.name).toBe("John Smith");
      expect(updates[0]?.age).toBe(31);
      expect(updates[0]?.email).toBe("john.smith@example.com");

      // Make additional changes to the branch
      branch.$jazz.applyDiff({
        name: "John Updated",
      });

      // Verify we get the updated branch data
      expect(updates).toHaveLength(2);
      expect(updates[1]?.name).toBe("John Updated");
      expect(updates[1]?.age).toBe(31);
      expect(updates[1]?.email).toBe("john.smith@example.com");

      // Verify original is still unchanged
      expect(originalPerson.name).toBe("John Doe");
      expect(originalPerson.age).toBe(30);
      expect(originalPerson.email).toBe("john@example.com");

      unsubscribe();
    });
  });
});
