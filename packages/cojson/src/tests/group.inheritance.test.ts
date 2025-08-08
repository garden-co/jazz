import { beforeEach, describe, expect, test } from "vitest";
import {
  createThreeConnectedNodes,
  createTwoConnectedNodes,
  loadCoValueOrFail,
  SyncMessagesLog,
  setupTestNode,
} from "./testUtils";

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("extend", () => {
  test("inherited writer roles should work correctly", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writer",
    );

    const childGroup = node1.node.createGroup();
    childGroup.extend(group);
    childGroup.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writeOnly",
    );

    const map = childGroup.createMap();
    map.set("test", "Written from the admin");

    await map.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    // The writer role should be able to see the edits from the admin
    expect(mapOnNode2.get("test")).toEqual("Written from the admin");
  });

  test("a user should be able to extend a group when his role on the parent group is writer", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writer",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

    const childGroup = node2.node.createGroup();
    childGroup.extend(groupOnNode2);

    const map = childGroup.createMap();
    map.set("test", "Written from node2");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    expect(mapOnNode2.get("test")).toEqual("Written from node2");
  });

  test("a user should be able to extend a group when his role on the parent group is reader", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "reader",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

    const childGroup = node2.node.createGroup();
    childGroup.extend(groupOnNode2);

    const map = childGroup.createMap();
    map.set("test", "Written from node2");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    expect(mapOnNode2.get("test")).toEqual("Written from node2");
  });

  test("a user should be able to extend a group when his role on the parent group is writeOnly", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writeOnly",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

    const childGroup = node2.node.createGroup();
    childGroup.extend(groupOnNode2);

    const map = childGroup.createMap();
    map.set("test", "Written from node2");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    expect(mapOnNode2.get("test")).toEqual("Written from node2");
  });

  test("self-extend a group should not break anything", async () => {
    const { node1 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.extend(group);

    const map = group.createMap();
    map.set("test", "Hello!");

    expect(map.get("test")).toEqual("Hello!");
  });

  test("should not break when introducing extend cycles", async () => {
    const { node1 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    const group2 = node1.node.createGroup();
    const group3 = node1.node.createGroup();

    group.extend(group2);
    group2.extend(group3);
    group3.extend(group);

    const map = group.createMap();
    map.set("test", "Hello!");

    expect(map.get("test")).toEqual("Hello!");
  });

  test("should not break when checking for cycles on a loaded group", async () => {
    const clientSession1 = setupTestNode({
      connected: true,
    });
    const clientSession2 = clientSession1.spawnNewSession();

    const group = clientSession1.node.createGroup();
    const childGroup = clientSession1.node.createGroup();
    const group2 = clientSession1.node.createGroup();
    const group3 = clientSession1.node.createGroup();

    childGroup.extend(group);
    group.extend(group2);
    group2.extend(group3);

    await group.core.waitForSync();
    await childGroup.core.waitForSync();
    await group2.core.waitForSync();
    await group3.core.waitForSync();

    const groupOnClientSession2 = await loadCoValueOrFail(
      clientSession2.node,
      group.id,
    );
    const group3OnClientSession2 = await loadCoValueOrFail(
      clientSession2.node,
      group3.id,
    );

    expect(group3OnClientSession2.isSelfExtension(groupOnClientSession2)).toBe(
      true,
    );

    // Child groups are not loaded as dependencies, and we want to make sure having a missing child doesn't break the extension
    expect(clientSession2.node.getCoValue(childGroup.id).isAvailable()).toEqual(
      false,
    );

    group3OnClientSession2.extend(groupOnClientSession2);

    expect(group3OnClientSession2.getParentGroups()).toEqual([]);

    const map = group3OnClientSession2.createMap();
    map.set("test", "Hello!");

    expect(map.get("test")).toEqual("Hello!");
  });

  test("should extend groups when loaded from a different session", async () => {
    const clientSession1 = setupTestNode({
      connected: true,
    });
    const clientSession2 = clientSession1.spawnNewSession();

    const group = clientSession1.node.createGroup();
    const group2 = clientSession1.node.createGroup();

    await group.core.waitForSync();
    await group2.core.waitForSync();

    const groupOnClientSession2 = await loadCoValueOrFail(
      clientSession2.node,
      group.id,
    );
    const group2OnClientSession2 = await loadCoValueOrFail(
      clientSession2.node,
      group2.id,
    );

    group2OnClientSession2.extend(groupOnClientSession2);

    expect(group2OnClientSession2.getParentGroups()).toEqual([
      groupOnClientSession2,
    ]);

    const map = group2OnClientSession2.createMap();
    map.set("test", "Hello!");

    expect(map.get("test")).toEqual("Hello!");
  });

  test("should extend groups when there is a cycle in the parent groups", async () => {
    const clientSession1 = setupTestNode({
      connected: true,
    });
    const clientSession2 = clientSession1.spawnNewSession();

    const group = clientSession1.node.createGroup();
    const group2 = clientSession1.node.createGroup();

    await group.core.waitForSync();
    await group2.core.waitForSync();

    const groupOnClientSession2 = await loadCoValueOrFail(
      clientSession2.node,
      group.id,
    );
    const group2OnClientSession2 = await loadCoValueOrFail(
      clientSession2.node,
      group2.id,
    );

    group.extend(group2);
    group2OnClientSession2.extend(groupOnClientSession2);

    expect(group.getParentGroups()).toEqual([group2]);

    expect(group2OnClientSession2.getParentGroups()).toEqual([
      groupOnClientSession2,
    ]);

    await group.core.waitForSync();
    await group2OnClientSession2.core.waitForSync();

    const group3 = clientSession1.node.createGroup();

    group3.extend(group2);

    expect(group3.getParentGroups()).toEqual([group2]);

    const map = group3.createMap();
    map.set("test", "Hello!");

    expect(map.get("test")).toEqual("Hello!");
  });

  test("a writerInvite role should not be inherited", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writerInvite",
    );

    const childGroup = node1.node.createGroup();
    childGroup.extend(group);

    expect(childGroup.roleOf(node2.accountID)).toEqual(undefined);
  });

  test("should be possible to extend a group without having membership in the parent group", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const parentGroup = node1.node.createGroup();
    const childGroup = node2.node.createGroup();

    const alice = await loadCoValueOrFail(node1.node, node3.accountID);
    parentGroup.addMember(alice, "writer");

    const parentGroupOnNode2 = await loadCoValueOrFail(
      node2.node,
      parentGroup.id,
    );

    childGroup.extend(parentGroupOnNode2);

    expect(childGroup.roleOf(alice.id)).toBe("writer");
  });
});

describe("unextend", () => {
  test("should revoke roles", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // `parentGroup` has `alice` as a writer
    const parentGroup = node1.node.createGroup();
    const alice = await loadCoValueOrFail(node1.node, node2.accountID);
    parentGroup.addMember(alice, "writer");
    // `alice`'s role in `parentGroup` is `"writer"`
    expect(parentGroup.roleOf(alice.id)).toBe("writer");

    // `childGroup` has `bob` as a reader
    const childGroup = node1.node.createGroup();
    const bob = await loadCoValueOrFail(node1.node, node3.accountID);
    childGroup.addMember(bob, "reader");
    // `bob`'s role in `childGroup` is `"reader"`
    expect(childGroup.roleOf(bob.id)).toBe("reader");

    // `childGroup` has `parentGroup`'s members (in this case, `alice` as a writer)
    childGroup.extend(parentGroup);
    expect(childGroup.roleOf(alice.id)).toBe("writer");

    // `childGroup` no longer has `parentGroup`'s members
    await childGroup.revokeExtend(parentGroup);
    expect(childGroup.roleOf(bob.id)).toBe("reader");
    expect(childGroup.roleOf(alice.id)).toBe(undefined);
  });

  test("should work when the account has no access to the parent group but owns the writeKey", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const parentGroup = node1.node.createGroup();
    const childGroup = node2.node.createGroup();

    const alice = await loadCoValueOrFail(node1.node, node3.accountID);
    parentGroup.addMember(alice, "writer");

    const parentGroupOnNode2 = await loadCoValueOrFail(
      node2.node,
      parentGroup.id,
    );

    childGroup.extend(parentGroupOnNode2);

    expect(childGroup.roleOf(alice.id)).toBe("writer");

    // `childGroup` no longer has `parentGroup`'s members
    await childGroup.revokeExtend(parentGroup);
    expect(childGroup.roleOf(alice.id)).toBe(undefined);

    const map = childGroup.createMap();
    map.set("test", "Hello!");

    const mapOnAlice = await loadCoValueOrFail(node3.node, map.id);

    expect(mapOnAlice.get("test")).toEqual(undefined);
  });

  test("should work when the account has no access to the parent group and not owns the writeKey", async () => {
    const {
      node1: bobNode,
      node2: johnNode,
      node3: aliceNode,
    } = await createThreeConnectedNodes("server", "server", "server");

    const parentGroup = bobNode.node.createGroup();
    const childGroup = johnNode.node.createGroup();

    const parentGroupOnJohn = await loadCoValueOrFail(
      johnNode.node,
      parentGroup.id,
    );

    childGroup.extend(parentGroupOnJohn);

    const bob = await loadCoValueOrFail(johnNode.node, bobNode.accountID);
    const alice = await loadCoValueOrFail(johnNode.node, aliceNode.accountID);
    childGroup.addMember(alice, "admin");

    const childGroupOnAlice = await loadCoValueOrFail(
      aliceNode.node,
      childGroup.id,
    );

    // `childGroup` no longer has `parentGroup`'s members
    await childGroupOnAlice.revokeExtend(parentGroup);
    expect(childGroupOnAlice.roleOf(bob.id)).toBe(undefined);

    const map = childGroupOnAlice.createMap();
    map.set("test", "Hello!");

    const mapOnBob = await loadCoValueOrFail(bobNode.node, map.id);

    expect(mapOnBob.get("test")).toEqual(undefined);
  });

  test("should do nothing if applied to a group that is not extended", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const parentGroup = node1.node.createGroup();
    const alice = await loadCoValueOrFail(node1.node, node2.accountID);
    parentGroup.addMember(alice, "writer");
    const childGroup = node1.node.createGroup();
    const bob = await loadCoValueOrFail(node1.node, node3.accountID);
    childGroup.addMember(bob, "reader");
    await childGroup.revokeExtend(parentGroup);
    expect(childGroup.roleOf(bob.id)).toBe("reader");
    expect(childGroup.roleOf(alice.id)).toBe(undefined);
  });

  test("should not throw if the revokeExtend is called twice", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const parentGroup = node1.node.createGroup();
    const alice = await loadCoValueOrFail(node1.node, node2.accountID);
    parentGroup.addMember(alice, "writer");
    const childGroup = node1.node.createGroup();
    const bob = await loadCoValueOrFail(node1.node, node3.accountID);
    childGroup.addMember(bob, "reader");

    childGroup.extend(parentGroup);

    await childGroup.revokeExtend(parentGroup);
    await childGroup.revokeExtend(parentGroup);
    expect(childGroup.roleOf(bob.id)).toBe("reader");
    expect(childGroup.roleOf(alice.id)).toBe(undefined);
  });
});

describe("extend with role mapping", () => {
  test("mapping to writer should add the ability to write", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "reader",
    );

    const childGroup = node1.node.createGroup();
    childGroup.extend(group, "writer");

    expect(childGroup.roleOf(node2.accountID)).toEqual("writer");

    const map = childGroup.createMap();
    map.set("test", "Written from the admin");

    await map.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    expect(mapOnNode2.get("test")).toEqual("Written from the admin");
    mapOnNode2.set("test", "Written from the inherited role");
    expect(mapOnNode2.get("test")).toEqual("Written from the inherited role");

    await mapOnNode2.core.waitForSync();

    expect(map.get("test")).toEqual("Written from the inherited role");
  });

  test("mapping to reader should remove the ability to write", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writer",
    );

    const childGroup = node1.node.createGroup();
    childGroup.extend(group, "reader");

    expect(childGroup.roleOf(node2.accountID)).toEqual("reader");

    const map = childGroup.createMap();
    map.set("test", "Written from the admin");

    await map.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    expect(mapOnNode2.get("test")).toEqual("Written from the admin");

    mapOnNode2.set("test", "Should not be visible");

    await mapOnNode2.core.waitForSync();

    expect(map.get("test")).toEqual("Written from the admin");
    expect(mapOnNode2.get("test")).toEqual("Written from the admin");
  });

  test("mapping to admin should add the ability to add members", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "reader",
    );

    const childGroup = node1.node.createGroup();
    childGroup.extend(group, "admin");

    expect(childGroup.roleOf(node2.accountID)).toEqual("admin");

    await childGroup.core.waitForSync();

    const childGroupOnNode2 = await loadCoValueOrFail(
      node2.node,
      childGroup.id,
    );

    childGroupOnNode2.addMember(
      await loadCoValueOrFail(node2.node, node3.accountID),
      "reader",
    );

    expect(childGroupOnNode2.roleOf(node3.accountID)).toEqual("reader");
  });

  test("mapping to reader should remove the ability to add members", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "admin",
    );

    const childGroup = node1.node.createGroup();
    childGroup.extend(group, "reader");

    expect(childGroup.roleOf(node2.accountID)).toEqual("reader");

    await childGroup.core.waitForSync();

    const childGroupOnNode2 = await loadCoValueOrFail(
      node2.node,
      childGroup.id,
    );

    const accountToAdd = await loadCoValueOrFail(node2.node, node3.accountID);

    expect(() => {
      childGroupOnNode2.addMember(accountToAdd, "reader");
    }).toThrow();

    expect(childGroupOnNode2.roleOf(node3.accountID)).toEqual(undefined);
  });

  test("non-inheritable roles should not give access to the child group when role mapping is used", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writeOnly",
    );

    const childGroup = node1.node.createGroup();
    childGroup.extend(group, "reader");

    expect(childGroup.roleOf(node2.accountID)).toEqual(undefined);

    const map = childGroup.createMap();
    map.set("test", "Written from the admin");

    await map.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    expect(mapOnNode2.get("test")).toEqual(undefined);
  });

  test("invite roles should not give write access to the child group when role mapping is used", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writerInvite",
    );

    const childGroup = node1.node.createGroup();
    childGroup.extend(group, "writer");

    expect(childGroup.roleOf(node2.accountID)).toEqual(undefined);

    const map = childGroup.createMap();
    map.set("test", "Written from the admin");

    await map.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    expect(mapOnNode2.get("test")).toEqual("Written from the admin"); // The invite roles have access to the readKey hence can read the values on inherited groups

    mapOnNode2.set("test", "Should not be visible");

    await mapOnNode2.core.waitForSync();

    expect(map.get("test")).toEqual("Written from the admin");
    expect(mapOnNode2.get("test")).toEqual("Written from the admin");
  });
});
