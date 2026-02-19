import { describe, expect, test } from "vitest";
import { RawCoList } from "../coValues/coList.js";
import { RawCoMap } from "../coValues/coMap.js";
import { RawCoStream } from "../coValues/coStream.js";
import { RawBinaryCoStream } from "../coValues/binaryCoStream.js";
import type { RawCoValue, RawGroup } from "../exports.js";
import type { NewContentMessage } from "../sync.js";
import {
  createThreeConnectedNodes,
  createTwoConnectedNodes,
  loadCoValueOrFail,
  nodeWithRandomAgentAndSessionID,
  setupTestNode,
} from "./testUtils.js";

function expectGroup(content: RawCoValue): RawGroup {
  if (content.type !== "comap") {
    throw new Error("Expected group");
  }

  if (content.core.verified.header.ruleset.type !== "group") {
    throw new Error("Expected group ruleset in group");
  }

  return content as RawGroup;
}

test("Can create a RawCoMap in a group", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const group = node.createGroup();

  const map = group.createMap();

  expect(map.core.getCurrentContent().type).toEqual("comap");
  expect(map instanceof RawCoMap).toEqual(true);
});

test("Can create a CoList in a group", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const group = node.createGroup();

  const list = group.createList();

  expect(list.core.getCurrentContent().type).toEqual("colist");
  expect(list instanceof RawCoList).toEqual(true);
});

test("Can create a CoStream in a group", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const group = node.createGroup();

  const stream = group.createStream();

  expect(stream.core.getCurrentContent().type).toEqual("costream");
  expect(stream instanceof RawCoStream).toEqual(true);
});

test("Can create a FileStream in a group", () => {
  const node = nodeWithRandomAgentAndSessionID();

  const group = node.createGroup();

  const stream = group.createBinaryStream();

  expect(stream.core.getCurrentContent().type).toEqual("costream");
  expect(stream.headerMeta.type).toEqual("binary");
  expect(stream instanceof RawBinaryCoStream).toEqual(true);
});

test("Group without name has name undefined", () => {
  const node = nodeWithRandomAgentAndSessionID();
  const group = node.createGroup();
  expect(group.name).toBeUndefined();
});

test("Group created with name returns that name", () => {
  const node = nodeWithRandomAgentAndSessionID();
  const group = node.createGroup(undefined, { name: "My Group" });
  expect(group.name).toBe("My Group");
});

test("Group created with empty name has name undefined (meta not set)", () => {
  const node = nodeWithRandomAgentAndSessionID();
  const group = node.createGroup(undefined, { name: "" });
  expect(group.name).toBeUndefined();
});

test("Group with name persists after sync and load", async () => {
  const { node1, node2 } = await createTwoConnectedNodes("server", "server");
  const group = node1.node.createGroup(undefined, { name: "Synced Group" });
  expect(group.name).toBe("Synced Group");
  await group.core.waitForSync();

  const loaded = expectGroup(await loadCoValueOrFail(node2.node, group.id));
  expect(loaded.name).toBe("Synced Group");
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
  await group.removeMember(node3.node.expectCurrentAccount("node3"));

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
  await group.removeMember(node3.node.expectCurrentAccount("node3"));
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
  await group.removeMember(node3.node.expectCurrentAccount("node3"));
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
  await group.removeMember(node3.node.expectCurrentAccount("node3"));
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

  await group.removeMember(node3.node.expectCurrentAccount("node3"));
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

  await group.removeMember(node3.node.expectCurrentAccount("node3"));

  const map = group.createMap();
  map.set("test", "Written from node1");

  await map.core.waitForSync();

  await node2.node.acceptInvite(group.id, invite);

  const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
  expect(mapOnNode2.get("test")).toEqual("Written from node1");
});

test("Should heal the missing key_for_everyone", async () => {
  const client = setupTestNode({
    secret:
      "sealerSecret_zBTPp7U58Fzq9o7EvJpu4KEziepi8QVf2Xaxuy5xmmXFx/signerSecret_z62DuviZdXCjz4EZWofvr9vaLYFXDeTaC9KWhoQiQjzKk",
  });

  const brokenGroupContent = {
    action: "content",
    id: "co_zW7F36Nnop9A7Er4gUzBcUXnZCK",
    header: {
      type: "comap",
      ruleset: {
        type: "group",
        initialAdmin:
          "sealer_z12QDazYB3ygPZtBV7sMm7iYKMRnNZ6Aaj1dfLXR7LSBm/signer_z2AskZQbc82qxo7iA3oiXoNExHLsAEXC2pHbwJzRnATWv",
      },
      meta: null,
      createdAt: "2025-08-06T10:14:39.617Z",
      uniqueness: "z3LJjnuPiPJaf5Qb9A",
    },
    priority: 0,
    new: {
      "sealer_z12QDazYB3ygPZtBV7sMm7iYKMRnNZ6Aaj1dfLXR7LSBm/signer_z2AskZQbc82qxo7iA3oiXoNExHLsAEXC2pHbwJzRnATWv_session_zYLsz2CiW9pW":
        {
          after: 0,
          newTransactions: [
            {
              privacy: "trusting",
              madeAt: 1754475279619,
              changes:
                '[{"key":"sealer_z12QDazYB3ygPZtBV7sMm7iYKMRnNZ6Aaj1dfLXR7LSBm/signer_z2AskZQbc82qxo7iA3oiXoNExHLsAEXC2pHbwJzRnATWv","op":"set","value":"admin"}]',
            },
            {
              privacy: "trusting",
              madeAt: 1754475279621,
              changes:
                '[{"key":"key_z5CVahfMkEWPj1B3zH_for_sealer_z12QDazYB3ygPZtBV7sMm7iYKMRnNZ6Aaj1dfLXR7LSBm/signer_z2AskZQbc82qxo7iA3oiXoNExHLsAEXC2pHbwJzRnATWv","op":"set","value":"sealed_UCg4UkytXF-W8PaIvaDffO3pZ3d9hdXUuNkQQEikPTAuOD9us92Pqb5Vgu7lx1Fpb0X8V5BJ2yxz6_D5WOzK3qjWBSsc7J1xDJA=="}]',
            },
            {
              privacy: "trusting",
              madeAt: 1754475279621,
              changes:
                '[{"key":"readKey","op":"set","value":"key_z5CVahfMkEWPj1B3zH"}]',
            },
            {
              privacy: "trusting",
              madeAt: 1754475279622,
              changes: '[{"key":"everyone","op":"set","value":"reader"}]',
            },
            {
              privacy: "trusting",
              madeAt: 1754475279623,
              changes:
                '[{"key":"key_z5CVahfMkEWPj1B3zH_for_everyone","op":"set","value":"keySecret_z9U9gzkahQXCxDoSw7isiUnbobXwuLdcSkL9Ci6ZEEkaL"}]',
            },
            {
              privacy: "trusting",
              madeAt: 1754475279623,
              changes:
                '[{"key":"key_z4Fi7hZNBx7XoVAKkP_for_sealer_z12QDazYB3ygPZtBV7sMm7iYKMRnNZ6Aaj1dfLXR7LSBm/signer_z2AskZQbc82qxo7iA3oiXoNExHLsAEXC2pHbwJzRnATWv","op":"set","value":"sealed_UuCBBfZkTnRTrGraqWWlzm9JE-VFduhsfu7WaZjpCbJYOTXpPhSNOnzGeS8qVuIsG6dORbME22lc5ltLxPjRqofQdDCNGQehCeQ=="}]',
            },
            {
              privacy: "trusting",
              madeAt: 1754475279624,
              changes:
                '[{"key":"key_z5CVahfMkEWPj1B3zH_for_key_z4Fi7hZNBx7XoVAKkP","op":"set","value":"encrypted_USTrBuobwTCORwy5yHxy4sFZ7swfrafP6k5ZwcTf76f0MBu9Ie-JmsX3mNXad4mluI47gvGXzi8I_"}]',
            },
            {
              privacy: "trusting",
              madeAt: 1754475279624,
              changes:
                '[{"key":"readKey","op":"set","value":"key_z4Fi7hZNBx7XoVAKkP"}]',
            },
          ],
          lastSignature:
            "signature_z3tsE7U1JaeNeUmZ4EY3Xq5uQ9jq9jDi6Rkhdt7T7b7z4NCnpMgB4bo8TwLXYVCrRdBm6PoyyPdK8fYFzHJUh5EzA",
        },
    },
  } as unknown as NewContentMessage;

  client.node.syncManager.handleNewContent(brokenGroupContent, "import");

  const group = expectGroup(
    client.node.getCoValue(brokenGroupContent.id).getCurrentContent(),
  );

  expect(group.get(`${group.get("readKey")!}_for_everyone`)).toBe(
    group.core.getCurrentReadKey()?.secret,
  );
});

describe("reader cannot create content", () => {
  test("createMap throws for reader", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "reader",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);
    expect(groupOnNode2.myRole()).toBe("reader");
    expect(() => groupOnNode2.createMap()).toThrow(
      "does not have write permissions",
    );
  });

  test("createList throws for reader", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "reader",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);
    expect(() => groupOnNode2.createList()).toThrow(
      "does not have write permissions",
    );
  });

  test("createStream throws for reader", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "reader",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);
    expect(() => groupOnNode2.createStream()).toThrow(
      "does not have write permissions",
    );
  });

  test("createBinaryStream throws for reader", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "reader",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);
    expect(() => groupOnNode2.createBinaryStream()).toThrow(
      "does not have write permissions",
    );
  });

  test("writer can still create content", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(node1.node, node2.accountID),
      "writer",
    );

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);
    const map = groupOnNode2.createMap();
    expect(map.type).toBe("comap");
  });
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

    await group.removeMember(node3.node.expectCurrentAccount("node3"));

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

    node2.node.internalDeleteCoValue(map.id);
    expect(node2.node.getCoValue(map.id)?.loadingState).toBe("unknown");

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    // The writer role should be able to see the edits from the admin
    expect(mapOnNode2.get("test")).toEqual("Written from the writeOnly member");
  });
});
