import { describe, expect, test } from "vitest";
import { CoValueState } from "../coValueState.js";
import { RawCoList } from "../coValues/coList.js";
import { RawCoMap } from "../coValues/coMap.js";
import { RawCoStream } from "../coValues/coStream.js";
import { RawBinaryCoStream } from "../coValues/coStream.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { RawAccountID } from "../exports.js";
import { LocalNode } from "../localNode.js";
import {
  createThreeConnectedNodes,
  createTwoConnectedNodes,
  loadCoValueOrFail,
  randomAnonymousAccountAndSessionID,
  waitFor,
} from "./testUtils.js";

const Crypto = await WasmCrypto.create();

test("Can create a RawCoMap in a group", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const group = node.createGroup();

  const map = group.createMap();

  expect(map.core.getCurrentContent().type).toEqual("comap");
  expect(map instanceof RawCoMap).toEqual(true);
});

test("Can create a CoList in a group", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const group = node.createGroup();

  const list = group.createList();

  expect(list.core.getCurrentContent().type).toEqual("colist");
  expect(list instanceof RawCoList).toEqual(true);
});

test("Can create a CoStream in a group", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const group = node.createGroup();

  const stream = group.createStream();

  expect(stream.core.getCurrentContent().type).toEqual("costream");
  expect(stream instanceof RawCoStream).toEqual(true);
});

test("Can create a FileStream in a group", () => {
  const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);

  const group = node.createGroup();

  const stream = group.createBinaryStream();

  expect(stream.core.getCurrentContent().type).toEqual("costream");
  expect(stream.headerMeta.type).toEqual("binary");
  expect(stream instanceof RawBinaryCoStream).toEqual(true);
});

test("Remove a member from a group where the admin role is inherited", async () => {
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
  group.addMember(
    await loadCoValueOrFail(node1.node, node3.accountID),
    "reader",
  );

  await group.core.waitForSync();

  const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

  // The account of node2 create a child group and extend the initial group
  // This way the node1 account should become "admin" of the child group
  // by inheriting the admin role from the initial group
  const childGroup = node2.node.createGroup();
  childGroup.extend(groupOnNode2);

  const map = childGroup.createMap();
  map.set("test", "Available to everyone");

  const mapOnNode3 = await loadCoValueOrFail(node3.node, map.id);

  // Check that the sync between node2 and node3 worked
  expect(mapOnNode3.get("test")).toEqual("Available to everyone");

  // The node1 account removes the reader from the group
  // The reader should be automatically kicked out of the child group
  await group.removeMember(node3.node.account);

  await group.core.waitForSync();

  // Update the map to check that node3 can't read updates anymore
  map.set("test", "Hidden to node3");

  await map.core.waitForSync();

  // Check that the value has not been updated on node3
  expect(mapOnNode3.get("test")).toEqual("Available to everyone");

  const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);

  expect(mapOnNode1.get("test")).toEqual("Hidden to node3");
});

test("An admin should be able to rotate the readKey on child groups and keep access to new coValues", async () => {
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
  group.addMember(
    await loadCoValueOrFail(node1.node, node3.accountID),
    "reader",
  );

  await group.core.waitForSync();

  const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

  // The account of node2 create a child group and extend the initial group
  // This way the node1 account should become "admin" of the child group
  // by inheriting the admin role from the initial group
  const childGroup = node2.node.createGroup();
  childGroup.extend(groupOnNode2);

  await childGroup.core.waitForSync();

  // The node1 account removes the reader from the group
  // In this case we want to ensure that node1 is still able to read new coValues
  // Even if some childs are not available when the readKey is rotated
  await group.removeMember(node3.node.account);
  await group.core.waitForSync();

  const map = childGroup.createMap();
  map.set("test", "Available to node1");

  const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
  expect(mapOnNode1.get("test")).toEqual("Available to node1");
});

test("An admin should be able to rotate the readKey on child groups even if it was unavailable when kicking out a member from a parent group", async () => {
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
  group.addMember(
    await loadCoValueOrFail(node1.node, node3.accountID),
    "reader",
  );

  await group.core.waitForSync();

  const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

  // The account of node2 create a child group and extend the initial group
  // This way the node1 account should become "admin" of the child group
  // by inheriting the admin role from the initial group
  const childGroup = node2.node.createGroup();
  childGroup.extend(groupOnNode2);

  // The node1 account removes the reader from the group
  // In this case we want to ensure that node1 is still able to read new coValues
  // Even if some childs are not available when the readKey is rotated
  await group.removeMember(node3.node.account);
  await group.core.waitForSync();

  const map = childGroup.createMap();
  map.set("test", "Available to node1");

  const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
  expect(mapOnNode1.get("test")).toEqual("Available to node1");
});

test("An admin should be able to rotate the readKey on child groups even if it was unavailable when kicking out a member from a parent group (grandChild)", async () => {
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
  group.addMember(
    await loadCoValueOrFail(node1.node, node3.accountID),
    "reader",
  );

  await group.core.waitForSync();

  const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

  // The account of node2 create a child group and extend the initial group
  // This way the node1 account should become "admin" of the child group
  // by inheriting the admin role from the initial group
  const childGroup = node2.node.createGroup();
  childGroup.extend(groupOnNode2);
  const grandChildGroup = node2.node.createGroup();
  grandChildGroup.extend(childGroup);

  // The node1 account removes the reader from the group
  // In this case we want to ensure that node1 is still able to read new coValues
  // Even if some childs are not available when the readKey is rotated
  await group.removeMember(node3.node.account);
  await group.core.waitForSync();

  const map = childGroup.createMap();
  map.set("test", "Available to node1");

  const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);

  expect(mapOnNode1.get("test")).toEqual("Available to node1");
});

test("A user add after a key rotation should have access to the old transactions", async () => {
  const { node1, node2, node3 } = await createThreeConnectedNodes(
    "server",
    "server",
    "server",
  );

  const group = node1.node.createGroup();

  group.addMember(
    await loadCoValueOrFail(node1.node, node2.accountID),
    "writer",
  );

  await group.core.waitForSync();

  const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

  const map = groupOnNode2.createMap();
  map.set("test", "Written from node2");

  await map.core.waitForSync();

  await group.removeMember(node3.node.account);
  group.addMember(
    await loadCoValueOrFail(node1.node, node3.accountID),
    "reader",
  );

  await group.core.waitForSync();

  const mapOnNode3 = await loadCoValueOrFail(node3.node, map.id);
  expect(mapOnNode3.get("test")).toEqual("Written from node2");
});

test("Invites should have access to the new keys", async () => {
  const { node1, node2, node3 } = await createThreeConnectedNodes(
    "server",
    "server",
    "server",
  );

  const group = node1.node.createGroup();
  group.addMember(
    await loadCoValueOrFail(node1.node, node3.accountID),
    "reader",
  );

  const invite = group.createInvite("admin");

  await group.removeMember(node3.node.account);

  const map = group.createMap();
  map.set("test", "Written from node1");

  await map.core.waitForSync();

  await node2.node.acceptInvite(group.id, invite);

  const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
  expect(mapOnNode2.get("test")).toEqual("Written from node1");
});

describe("writeOnly", () => {
  test("Admins can invite writeOnly members", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();

    const invite = group.createInvite("writeOnly");

    await node2.node.acceptInvite(group.id, invite);

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);
    expect(groupOnNode2.myRole()).toEqual("writeOnly");
  });

  test("writeOnly roles are not inherited", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writeOnly",
    );

    const childGroup = node1.node.createGroup();
    childGroup.extend(group);
    expect(childGroup.roleOf(node2.accountID)).toEqual(undefined);
  });

  test("writeOnly roles are not overridded by reader roles", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "reader",
    );

    const childGroup = node1.node.createGroup();
    childGroup.extend(group);
    childGroup.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writeOnly",
    );

    expect(childGroup.roleOf(node2.accountID)).toEqual("writeOnly");
  });

  test("writeOnly roles are overridded by writer roles", async () => {
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

    expect(childGroup.roleOf(node2.accountID)).toEqual("writer");
  });

  test("Edits by writeOnly members are visible to other members", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const group = node1.node.createGroup();

    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writeOnly",
    );
    group.addMember(
      await loadCoValueOrFail(node1.node, node3.accountID),
      "reader",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);
    const map = groupOnNode2.createMap();

    map.set("test", "Written from a writeOnly member");
    expect(map.get("test")).toEqual("Written from a writeOnly member");

    await map.core.waitForSync();

    const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
    expect(mapOnNode1.get("test")).toEqual("Written from a writeOnly member");

    const mapOnNode3 = await loadCoValueOrFail(node3.node, map.id);
    expect(mapOnNode3.get("test")).toEqual("Written from a writeOnly member");
  });

  test("Edits by other members are not visible to writeOnly members", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();

    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writeOnly",
    );
    const map = group.createMap();
    map.set("test", "Written from the admin");

    await map.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
    expect(mapOnNode2.get("test")).toEqual(undefined);
  });

  test("Write only member keys are rotated when a member is kicked out", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const group = node1.node.createGroup();

    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writeOnly",
    );
    group.addMember(
      await loadCoValueOrFail(node1.node, node3.accountID),
      "reader",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);
    const map = groupOnNode2.createMap();

    map.set("test", "Written from a writeOnly member");

    await map.core.waitForSync();

    await group.removeMember(node3.node.account);

    await group.core.waitForSync();

    map.set("test", "Updated after key rotation");

    await map.core.waitForSync();

    const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
    expect(mapOnNode1.get("test")).toEqual("Updated after key rotation");

    const mapOnNode3 = await loadCoValueOrFail(node3.node, map.id);
    expect(mapOnNode3.get("test")).toEqual("Written from a writeOnly member");
  });

  test("upgrade to writer roles should work correctly", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writeOnly",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);
    const map = groupOnNode2.createMap();
    map.set("test", "Written from the writeOnly member");

    await map.core.waitForSync();

    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writer",
    );

    node2.node.coValuesStore.coValues.delete(map.id);
    expect(node2.node.coValuesStore.get(map.id)?.highLevelState).toBe(
      "unknown",
    );

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    // The writer role should be able to see the edits from the admin
    expect(mapOnNode2.get("test")).toEqual("Written from the writeOnly member");
  });
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

describe("roleOf", () => {
  test("returns direct role assignments", () => {
    const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);
    const group = node.createGroup();
    const account = new LocalNode(
      ...randomAnonymousAccountAndSessionID(),
      Crypto,
    ).account;

    group.addMember(account, "writer");
    expect(group.roleOf(account.id as RawAccountID)).toEqual("writer");
  });

  test("returns undefined for non-members", () => {
    const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);
    const group = node.createGroup();
    const account = new LocalNode(
      ...randomAnonymousAccountAndSessionID(),
      Crypto,
    ).account;

    expect(group.roleOf(account.id as RawAccountID)).toEqual(undefined);
  });

  test("revoked roles return undefined", () => {
    const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);
    const group = node.createGroup();
    const account = new LocalNode(
      ...randomAnonymousAccountAndSessionID(),
      Crypto,
    ).account;

    group.addMember(account, "writer");
    group.removeMemberInternal(account);
    expect(group.roleOf(account.id as RawAccountID)).toEqual(undefined);
  });

  test("everyone role applies to all accounts", () => {
    const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);
    const group = node.createGroup();
    const account = new LocalNode(
      ...randomAnonymousAccountAndSessionID(),
      Crypto,
    ).account;

    group.addMemberInternal("everyone", "reader");
    expect(group.roleOf(account.id as RawAccountID)).toEqual("reader");
  });

  test("account role overrides everyone role", () => {
    const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);
    const group = node.createGroup();
    const account = new LocalNode(
      ...randomAnonymousAccountAndSessionID(),
      Crypto,
    ).account;

    group.addMemberInternal("everyone", "writer");
    group.addMember(account, "reader");
    expect(group.roleOf(account.id as RawAccountID)).toEqual("reader");
  });

  test("Revoking access on everyone role should not affect existing members", () => {
    const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);
    const group = node.createGroup();
    const account = new LocalNode(
      ...randomAnonymousAccountAndSessionID(),
      Crypto,
    ).account;

    group.addMemberInternal("everyone", "reader");
    group.addMember(account, "writer");
    group.removeMemberInternal("everyone");
    expect(group.roleOf(account.id as RawAccountID)).toEqual("writer");
    expect(group.roleOf("123" as RawAccountID)).toEqual(undefined);
  });

  test("Everyone role is inherited following the most permissive algorithm", () => {
    const node = new LocalNode(...randomAnonymousAccountAndSessionID(), Crypto);
    const group = node.createGroup();
    const account = new LocalNode(
      ...randomAnonymousAccountAndSessionID(),
      Crypto,
    ).account;

    const parentGroup = node.createGroup();
    parentGroup.addMemberInternal("everyone", "writer");

    group.extend(parentGroup);
    group.addMember(account, "reader");

    expect(group.roleOf(account.id as RawAccountID)).toEqual("writer");
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
      expect(mapOnNode2.get("fromAdmin")).toEqual("Written from admin");
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

      const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

      expect(mapOnNode2.get("test")).toEqual(undefined);

      mapOnNode2.set("test", "Written from everyone");

      await waitFor(async () => {
        const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
        expect(mapOnNode1.get("test")).toEqual("Written from everyone");
      });

      group.addMember("everyone", "writer");

      await group.core.waitForSync();

      mapOnNode2.set("test", "Updated after the upgrade");

      expect(mapOnNode2.get("test")).toEqual("Updated after the upgrade");
      expect(mapOnNode2.get("fromAdmin")).toEqual("Written from admin");
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
