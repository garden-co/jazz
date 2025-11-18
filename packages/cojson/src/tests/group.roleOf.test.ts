import { describe, expect, test } from "vitest";
import { RawAccountID } from "../exports";
import {
  createThreeConnectedNodes,
  createTwoConnectedNodes,
  loadCoValueOrFail,
  nodeWithRandomAgentAndSessionID,
  randomAgentAndSessionID,
  waitFor,
} from "./testUtils";
import { expectMap } from "../coValue";

describe("roleOf", () => {
  test("returns direct role assignments", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const group = node.createGroup();
    const [agent2] = randomAgentAndSessionID();

    group.addMember(agent2, "writer");
    expect(group.roleOfInternal(agent2.id)).toEqual("writer");
  });

  test("returns undefined for non-members", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const group = node.createGroup();
    const [agent2] = randomAgentAndSessionID();

    expect(group.roleOfInternal(agent2.id)).toEqual(undefined);
  });

  test("revoked roles return undefined", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const group = node.createGroup();
    const [agent2] = randomAgentAndSessionID();

    group.addMember(agent2, "writer");
    group.removeMember(agent2);
    expect(group.roleOfInternal(agent2.id)).toEqual(undefined);
  });

  test("everyone role applies to all accounts", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const group = node.createGroup();
    const [agent2, sessionID2] = randomAgentAndSessionID();

    group.addMemberInternal("everyone", "reader");
    expect(group.roleOfInternal(agent2.id)).toEqual("reader");
  });

  test("account role overrides everyone role", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const group = node.createGroup();
    const [agent2, sessionID2] = randomAgentAndSessionID();

    group.addMemberInternal("everyone", "writer");
    group.addMember(agent2, "reader");
    expect(group.roleOfInternal(agent2.id)).toEqual("reader");
  });

  test("Revoking access on everyone role should not affect existing members", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const group = node.createGroup();
    const [agent2, sessionID2] = randomAgentAndSessionID();

    group.addMemberInternal("everyone", "reader");
    group.addMember(agent2, "writer");
    group.removeMember("everyone");
    expect(group.roleOfInternal(agent2.id)).toEqual("writer");
    expect(group.roleOfInternal("123" as RawAccountID)).toEqual(undefined);
  });

  test("Everyone role is inherited following the most permissive algorithm", () => {
    const node = nodeWithRandomAgentAndSessionID();
    const group = node.createGroup();
    const [agent2, sessionID2] = randomAgentAndSessionID();

    const parentGroup = node.createGroup();
    parentGroup.addMemberInternal("everyone", "writer");

    group.extend(parentGroup);
    group.addMember(agent2, "reader");

    expect(group.roleOfInternal(agent2.id)).toEqual("writer");
  });
  test("roleOf should prioritize explicit account role over everyone role in same group", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    const account2 = await loadCoValueOrFail(node1.node, node2.accountID);

    // Add both everyone and specific account
    group.addMember("everyone", "reader");
    group.addMember(account2, "writer");

    // Should return the explicit role, not everyone's role
    expect(group.roleOf(node2.accountID)).toEqual("writer");

    // Change everyone's role
    group.addMember("everyone", "writer");

    // Should still return the explicit role
    expect(group.roleOf(node2.accountID)).toEqual("writer");
  });

  test("roleOf should prioritize inherited everyone role over explicit account role", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const parentGroup = node1.node.createGroup();
    const childGroup = node1.node.createGroup();
    const account2 = await loadCoValueOrFail(node1.node, node2.accountID);

    // Set up inheritance
    childGroup.extend(parentGroup);

    // Add everyone to parent and account to child
    parentGroup.addMember("everyone", "writer");
    childGroup.addMember(account2, "reader");

    // Should return the explicit role from child, not inherited everyone role
    expect(childGroup.roleOf(node2.accountID)).toEqual("writer");
  });

  test("roleOf should use everyone role when no explicit role exists", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();

    // Add only everyone role
    group.addMember("everyone", "reader");

    // Should return everyone's role when no explicit role exists
    expect(group.roleOf(node2.accountID)).toEqual("reader");
  });

  test("roleOf should inherit everyone role from parent when no explicit roles exist", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const parentGroup = node1.node.createGroup();
    const childGroup = node1.node.createGroup();

    // Set up inheritance
    childGroup.extend(parentGroup);

    // Add everyone to parent only
    parentGroup.addMember("everyone", "reader");

    // Should inherit everyone's role from parent
    expect(childGroup.roleOf(node2.accountID)).toEqual("reader");
  });

  test("roleOf should handle everyone role inheritance through multiple levels", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const grandParentGroup = node1.node.createGroup();
    const parentGroup = node1.node.createGroup();
    const childGroup = node1.node.createGroup();

    const childGroupOnNode2 = await loadCoValueOrFail(
      node2.node,
      childGroup.id,
    );

    const account2 = await loadCoValueOrFail(node1.node, node2.accountID);

    // Set up inheritance chain
    parentGroup.extend(grandParentGroup);
    childGroup.extend(parentGroup);

    // Add everyone to grandparent
    grandParentGroup.addMember("everyone", "writer");

    // Should inherit everyone's role from grandparent
    expect(childGroup.roleOf(node2.accountID)).toEqual("writer");

    // Add explicit role in parent
    parentGroup.addMember(account2, "reader");

    // Should use parent's explicit role instead of grandparent's everyone role
    expect(childGroup.roleOf(node2.accountID)).toEqual("writer");

    // Add explicit role in child
    childGroup.addMember(account2, "admin");
    await childGroup.core.waitForSync();

    // Should use child's explicit role
    expect(childGroup.roleOf(node2.accountID)).toEqual("admin");

    // Remove child's explicit role
    await childGroupOnNode2.removeMember(account2);
    await childGroupOnNode2.core.waitForSync();

    // Should fall back to parent's explicit role
    expect(childGroup.roleOf(node2.accountID)).toEqual("writer");

    // Remove parent's explicit role
    await parentGroup.removeMember(account2);
    await childGroup.core.waitForSync();

    // Should fall back to grandparent's everyone role
    expect(childGroup.roleOf(node2.accountID)).toEqual("writer");
  });

  describe("writeOnly can be used as a role for everyone", () => {
    test("writeOnly can be used as a role for everyone", async () => {
      const { node1, node2 } = await createTwoConnectedNodes(
        "server",
        "server",
      );

      const group = node1.node.createGroup();

      group.addMember("everyone", "writeOnly");

      const map = group.createMap();

      const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

      expect(groupOnNode2.myRole()).toEqual("writeOnly");

      const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

      mapOnNode2.set("test", "Written from everyone");

      await waitFor(async () => {
        const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
        expect(mapOnNode1.get("test")).toEqual("Written from everyone");
      });
    });

    test("switching from everyone reader to writeOnly", async () => {
      const { node1, node2 } = await createTwoConnectedNodes(
        "server",
        "server",
      );

      const group = node1.node.createGroup();

      group.addMember("everyone", "reader");

      group.addMember("everyone", "writeOnly");

      const map = group.createMap();

      map.set("test", "Written from admin");

      const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

      expect(groupOnNode2.myRole()).toEqual("writeOnly");

      const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

      expect(mapOnNode2.get("test")).toEqual(undefined);

      mapOnNode2.set("test", "Written from everyone");

      await waitFor(async () => {
        const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
        expect(mapOnNode1.get("test")).toEqual("Written from everyone");
      });
    });

    test("switching from everyone writer to writeOnly", async () => {
      const { node1, node2 } = await createTwoConnectedNodes(
        "server",
        "server",
      );

      const group = node1.node.createGroup();

      group.addMember("everyone", "writer");

      group.addMember("everyone", "writeOnly");

      const map = group.createMap();

      map.set("test", "Written from admin");

      const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

      expect(groupOnNode2.myRole()).toEqual("writeOnly");

      const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

      expect(mapOnNode2.get("test")).toEqual(undefined);

      mapOnNode2.set("test", "Written from everyone");

      await waitFor(async () => {
        const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
        expect(mapOnNode1.get("test")).toEqual("Written from everyone");
      });
    });

    test("switching from everyone writeOnly to reader", async () => {
      const { node1, node2 } = await createTwoConnectedNodes(
        "server",
        "server",
      );

      const group = node1.node.createGroup();

      group.addMember("everyone", "writeOnly");

      const map = group.createMap();

      map.set("fromAdmin", "Written from admin");

      const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

      expect(groupOnNode2.myRole()).toEqual("writeOnly");

      const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

      expect(mapOnNode2.get("test")).toEqual(undefined);

      mapOnNode2.set("test", "Written from everyone");

      await waitFor(async () => {
        const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
        expect(mapOnNode1.get("test")).toEqual("Written from everyone");
      });

      group.addMember("everyone", "reader");

      await group.core.waitForSync();

      mapOnNode2.set("test", "Updated after the downgrade");

      expect(mapOnNode2.get("test")).toEqual("Written from everyone");

      await waitFor(async () => {
        expect(mapOnNode2.get("fromAdmin")).toEqual("Written from admin");
      });
    });

    test("switching from everyone writeOnly to writer", async () => {
      const { node1, node2 } = await createTwoConnectedNodes(
        "server",
        "server",
      );

      const group = node1.node.createGroup();

      group.addMember("everyone", "writeOnly");

      const map = group.createMap();

      map.set("fromAdmin", "Written from admin");

      const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

      expect(groupOnNode2.myRole()).toEqual("writeOnly");

      let mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

      expect(mapOnNode2.get("test")).toEqual(undefined);

      mapOnNode2.set("test", "Written from everyone");

      await waitFor(async () => {
        const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
        expect(mapOnNode1.get("test")).toEqual("Written from everyone");
      });

      group.addMember("everyone", "writer");

      await waitFor(async () => {
        expect(groupOnNode2.core.knownState()).toEqual(group.core.knownState());
      });

      mapOnNode2.set("test", "Updated after the upgrade");

      expect(mapOnNode2.get("test")).toEqual("Updated after the upgrade");

      await waitFor(async () => {
        // Get the new content after the invalidation caused by group update
        expect(mapOnNode2.get("fromAdmin")).toEqual("Written from admin");
      });
    });

    test("adding a reader member after writeOnly", async () => {
      const { node1, node2, node3 } = await createThreeConnectedNodes(
        "server",
        "server",
        "server",
      );

      const group = node1.node.createGroup();

      group.addMember("everyone", "writeOnly");

      const map = group.createMap();

      map.set("fromAdmin", "Written from admin");

      const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

      expect(groupOnNode2.myRole()).toEqual("writeOnly");

      const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

      expect(mapOnNode2.get("test")).toEqual(undefined);

      mapOnNode2.set("test", "Written from everyone");

      await waitFor(async () => {
        const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
        expect(mapOnNode1.get("test")).toEqual("Written from everyone");
      });

      const account3 = await loadCoValueOrFail(node1.node, node3.accountID);

      group.addMember(account3, "reader");

      await group.core.waitForSync();

      const mapOnNode3 = await loadCoValueOrFail(node3.node, map.id);

      expect(mapOnNode3.get("test")).toEqual("Written from everyone");
    });

    test.skip("adding a reader member while creating the writeOnly keys", async () => {
      const { node1, node2, node3 } = await createThreeConnectedNodes(
        "server",
        "server",
        "server",
      );
      const account3 = await loadCoValueOrFail(node1.node, node3.accountID);
      const group = node1.node.createGroup();

      group.addMember("everyone", "writeOnly");

      const map = group.createMap();

      map.set("fromAdmin", "Written from admin");

      const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

      expect(groupOnNode2.myRole()).toEqual("writeOnly");

      const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
      group.addMember(account3, "reader");

      expect(mapOnNode2.get("test")).toEqual(undefined);

      mapOnNode2.set("test", "Written from everyone");

      await waitFor(async () => {
        const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
        expect(mapOnNode1.get("test")).toEqual("Written from everyone");
      });

      await group.core.waitForSync();

      const mapOnNode3 = await loadCoValueOrFail(node3.node, map.id);

      expect(mapOnNode3.get("test")).toEqual("Written from everyone");
    });
  });
});
