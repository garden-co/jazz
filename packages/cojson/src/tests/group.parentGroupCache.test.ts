import { beforeEach, describe, expect, test } from "vitest";
import {
  setupTestAccount,
  setupTestNode,
  hotSleep,
  loadCoValueOrFail,
} from "./testUtils";

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("Parent Group Cache", () => {
  describe("Property 1: Parent group cache update correctness", () => {
    test("cache contains correct entries after processing parent group reference transactions", async () => {
      const account = await setupTestAccount({
        connected: true,
      });
      const parentGroup = account.node.createGroup();
      const childGroup = account.node.createGroup();

      // Initially no parent groups
      expect(childGroup.getParentGroups()).toEqual([]);

      // Extend parent group
      childGroup.extend(parentGroup);
      expect(childGroup.getParentGroups()).toEqual([parentGroup]);
    });

    test("cache handles multiple updates to same parent group", async () => {
      const account = await setupTestAccount({
        connected: true,
      });
      const parentGroup = account.node.createGroup();
      const childGroup = account.node.createGroup();

      childGroup.extend(parentGroup);
      expect(childGroup.getParentGroups()).toEqual([parentGroup]);

      // Revoke and re-extend
      childGroup.revokeExtend(parentGroup);
      expect(childGroup.getParentGroups()).toEqual([]);

      childGroup.extend(parentGroup, "admin");
      expect(childGroup.getParentGroups()).toEqual([parentGroup]);
    });

    test("cache correctly handles empty state (no parent groups)", async () => {
      const account = await setupTestAccount({
        connected: true,
      });
      const group = account.node.createGroup();

      expect(group.getParentGroups()).toEqual([]);
    });

    test("cache only contains parent groups that have reference transactions", async () => {
      const account = await setupTestAccount({
        connected: true,
      });
      const parentGroup1 = account.node.createGroup();
      const parentGroup2 = account.node.createGroup();
      const childGroup = account.node.createGroup();

      // Only extend one parent group
      childGroup.extend(parentGroup1);

      const parentGroups = childGroup.getParentGroups();
      expect(parentGroups).toHaveLength(1);
      expect(parentGroups[0]?.id).toEqual(parentGroup1.id);
    });

    test("cache handles multiple parent groups", async () => {
      const account = await setupTestAccount({
        connected: true,
      });
      const parentGroup1 = account.node.createGroup();
      const parentGroup2 = account.node.createGroup();
      const parentGroup3 = account.node.createGroup();
      const childGroup = account.node.createGroup();

      childGroup.extend(parentGroup1);
      childGroup.extend(parentGroup2);
      childGroup.extend(parentGroup3);

      const parentGroups = childGroup.getParentGroups();
      expect(parentGroups).toHaveLength(3);
      expect(parentGroups.map((g) => g.id).sort()).toEqual(
        [parentGroup1.id, parentGroup2.id, parentGroup3.id].sort(),
      );
    });
  });

  describe("Property 2: Parent group cache chronological ordering", () => {
    test("multiple parent group changes are stored in chronological order", async () => {
      const account = await setupTestAccount({
        connected: true,
      });
      const parentGroup = account.node.createGroup();
      const childGroup = account.node.createGroup();

      const t1 = hotSleep(10);
      childGroup.extend(parentGroup, "reader");
      const t2 = hotSleep(10);
      childGroup.extend(parentGroup, "writer");
      const t3 = hotSleep(10);
      childGroup.extend(parentGroup, "admin");

      // Check time travel queries return correct historical states
      expect(childGroup.atTime(t1).getParentGroups()).toEqual([]);
      expect(childGroup.atTime(t2).getParentGroups()).toEqual([
        parentGroup.atTime(t2),
      ]);
      expect(childGroup.atTime(t3).getParentGroups()).toEqual([
        parentGroup.atTime(t3),
      ]);

      // Current state should have admin role
      expect(childGroup.getParentGroups()).toEqual([parentGroup]);
    });

    test("out-of-order transaction processing maintains chronological order", async () => {
      const account = await setupTestAccount({
        connected: true,
      });
      const parentGroup = account.node.createGroup();
      const childGroup = account.node.createGroup();

      // Create transactions with different timestamps
      const t1 = hotSleep(10);
      const t2 = hotSleep(10);

      childGroup.core.makeTransaction(
        [
          {
            op: "set",
            key: `parent_${parentGroup.id}`,
            value: "revoked",
          },
        ],
        "trusting",
        undefined,
        t2,
      );

      childGroup.core.makeTransaction(
        [
          {
            op: "set",
            key: `parent_${parentGroup.id}`,
            value: "extend",
          },
        ],
        "trusting",
        undefined,
        t1,
      );
      // Verify chronological ordering through time travel
      const groupAtT1 = childGroup.atTime(t1);
      const groupAtT2 = childGroup.atTime(t2);

      expect(groupAtT1.getParentGroups()).toEqual([parentGroup.atTime(t1)]);
      expect(groupAtT2.getParentGroups()).toEqual([]);
    });

    test("chronological ordering after rebuild", async () => {
      const account = await setupTestAccount({
        connected: true,
      });
      const parentGroup = account.node.createGroup();
      const childGroup = account.node.createGroup();

      const t1 = hotSleep(10);
      childGroup.extend(parentGroup, "reader");
      const t2 = hotSleep(10);
      childGroup.extend(parentGroup, "writer");

      // Rebuild should maintain chronological order
      childGroup.rebuildFromCore();

      const groupAtT1 = childGroup.atTime(t1);
      const groupAtT2 = childGroup.atTime(t2);

      expect(groupAtT1.getParentGroups()).toEqual([]);
      expect(groupAtT2.getParentGroups()).toEqual([parentGroup.atTime(t2)]);
      expect(childGroup.getParentGroups()).toEqual([parentGroup]);
    });
  });

  describe("Property 3: Rebuild round-trip", () => {
    test("cache is cleared after rebuildFromCore", async () => {
      const account = await setupTestAccount({
        connected: true,
      });
      const parentGroup = account.node.createGroup();
      const childGroup = account.node.createGroup();

      childGroup.extend(parentGroup);
      expect(childGroup.getParentGroups()).toEqual([parentGroup]);

      // Rebuild should clear cache and rebuild it
      childGroup.rebuildFromCore();

      // Cache should be rebuilt and still work
      expect(childGroup.getParentGroups()).toEqual([parentGroup]);
    });

    test("cache is rebuilt correctly after clear", async () => {
      const account = await setupTestAccount({
        connected: true,
      });
      const parentGroup1 = account.node.createGroup();
      const parentGroup2 = account.node.createGroup();
      const childGroup = account.node.createGroup();

      childGroup.extend(parentGroup1);
      childGroup.extend(parentGroup2);

      expect(childGroup.getParentGroups()).toHaveLength(2);

      // Rebuild
      childGroup.rebuildFromCore();

      // Cache should be rebuilt with all parent groups
      const parentGroups = childGroup.getParentGroups();
      expect(parentGroups).toHaveLength(2);
      expect(parentGroups.map((g) => g.id).sort()).toEqual(
        [parentGroup1.id, parentGroup2.id].sort(),
      );
    });

    test("parent group lookups work correctly after rebuild", async () => {
      const alice = await setupTestAccount({
        connected: true,
      });
      const bob = await setupTestAccount({
        connected: true,
      });
      const parentGroup = alice.node.createGroup();
      const childGroup = alice.node.createGroup();

      parentGroup.addMember(
        await loadCoValueOrFail(alice.node, bob.accountID),
        "writer",
      );

      childGroup.extend(parentGroup);

      // Check role inheritance before rebuild
      expect(childGroup.roleOf(bob.accountID)).toEqual("writer");

      // Rebuild
      childGroup.rebuildFromCore();

      // Role inheritance should still work after rebuild
      expect(childGroup.roleOf(bob.accountID)).toEqual("writer");
      expect(childGroup.getParentGroups()).toEqual([parentGroup]);
    });

    test("rebuild maintains chronological ordering", async () => {
      const alice = await setupTestAccount({
        connected: true,
      });
      const bob = await setupTestAccount({
        connected: true,
      });
      const parentGroup = alice.node.createGroup();
      const childGroup = alice.node.createGroup();

      parentGroup.addMember(
        await loadCoValueOrFail(alice.node, bob.accountID),
        "writer",
      );

      const t1 = hotSleep(10);
      childGroup.extend(parentGroup, "reader");
      const t2 = hotSleep(10);
      childGroup.extend(parentGroup, "writer");

      // Rebuild
      childGroup.rebuildFromCore();

      // Chronological ordering should be maintained
      expect(childGroup.atTime(t1).roleOf(bob.accountID)).toEqual(undefined);
      expect(childGroup.atTime(t2).roleOf(bob.accountID)).toEqual("reader");
      expect(childGroup.atTime(Date.now()).roleOf(bob.accountID)).toEqual(
        "writer",
      );
    });
  });
});
