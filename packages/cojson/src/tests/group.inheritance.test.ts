import { beforeEach, describe, expect, test } from "vitest";
import type { CoID, RawGroup } from "../exports";
import { NewContentMessage } from "../sync";
import {
  SyncMessagesLog,
  createNConnectedNodes,
  createThreeConnectedNodes,
  createTwoConnectedNodes,
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
} from "./testUtils";
import { expectMap } from "../coValue.js";

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("extend", () => {
  test("inherited writer roles should work correctly", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    const node2OnNode1 = await loadCoValueOrFail(node1.node, node2.accountID);
    group.addMember(node2OnNode1, "writer");

    const childGroup = node1.node.createGroup();
    childGroup.extend(group);
    childGroup.addMember(node2OnNode1, "writeOnly");

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

  test("inherited everyone roles should work correctly", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember("everyone", "writer");

    const childGroup = node1.node.createGroup();
    childGroup.extend(group);

    expect(childGroup.roleOf("everyone")).toEqual("writer");

    const map = childGroup.createMap();
    map.set("test", "Written from the admin");

    await map.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    // The writer role should be able to see the edits from the admin
    expect(mapOnNode2.get("test")).toEqual("Written from the admin");

    mapOnNode2.set("hello", "from node 2");

    expect(mapOnNode2.get("hello")).toEqual("from node 2");
  });

  test("existing parent groups have access to new writeOnly keys in the child group", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "admin");

    const childGroup = node1.node.createGroup();
    const account3OnNode1 = await loadCoValueOrFail(
      node1.node,
      node3.accountID,
    );
    childGroup.extend(parentGroup);
    // The existing parent group should have access to content written by
    // an account with writeOnly permission
    childGroup.addMember(account3OnNode1, "writeOnly");

    const map = childGroup.createMap();

    const mapOnNode3 = await loadCoValueOrFail(node3.node, map.id);
    mapOnNode3.set("test", "Written by writeOnly member");

    expect(mapOnNode3.get("test")).toEqual("Written by writeOnly member");
    await mapOnNode3.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
    expect(mapOnNode2.get("test")).toEqual("Written by writeOnly member");
  });

  test("new parent groups have access to existing writeOnly keys in the child group", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "admin");

    const childGroup = node1.node.createGroup();
    const account3OnNode1 = await loadCoValueOrFail(
      node1.node,
      node3.accountID,
    );
    childGroup.addMember(account3OnNode1, "writeOnly");
    // The new parent group should have access to content written by
    // an account with writeOnly permission
    childGroup.extend(parentGroup);

    const map = childGroup.createMap();

    const mapOnNode3 = await loadCoValueOrFail(node3.node, map.id);
    mapOnNode3.set("test", "Written by writeOnly member");

    expect(mapOnNode3.get("test")).toEqual("Written by writeOnly member");
    await mapOnNode3.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
    expect(mapOnNode2.get("test")).toEqual("Written by writeOnly member");
  });

  test("writeOnly keys are rotated for parent groups", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "admin");

    const childGroup = node1.node.createGroup();
    const account3OnNode1 = await loadCoValueOrFail(
      node1.node,
      node3.accountID,
    );
    childGroup.addMember(account3OnNode1, "writeOnly");
    childGroup.extend(parentGroup);

    childGroup.rotateReadKey();

    const childGroupOnNode3 = await loadCoValueOrFail(
      node3.node,
      childGroup.id,
    );
    const map = childGroupOnNode3.createMap();
    map.set("test", "Written by writeOnly member");

    await map.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
    expect(mapOnNode2.get("test")).toEqual("Written by writeOnly member");
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

  test("group inheritance should work for groups extended without having membership in the parent group", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "admin");

    const childGroup = node1.node.createGroup();
    childGroup.extend(parentGroup, "admin");

    // Wait for childGroup to sync so node3 sees the groupSealer field
    await childGroup.core.waitForSync();

    const sharedGroup = node3.node.createGroup();

    // Account3 does not have permissions over the childGroup being extended
    const childGroupOnNode3 = await loadCoValueOrFail(
      node3.node,
      childGroup.id,
    );
    sharedGroup.extend(childGroupOnNode3, "admin");

    expect(sharedGroup.roleOf(node1.accountID)).toEqual("admin");
    expect(sharedGroup.roleOf(node2.accountID)).toEqual("admin");
    expect(sharedGroup.roleOf(node3.accountID)).toEqual("admin");

    // Create a map owned by sharedGroup
    const testMap = sharedGroup.createMap();
    testMap.set("name", "Test");

    // Wait for sharedGroup to sync so other nodes can resolve keys via groupSealer
    await sharedGroup.core.waitForSync();

    // node1 should be able to access the map because it is admin of the childGroup
    const testMapOnNode1 = expectMap(
      await loadCoValueOrFail(node1.node, testMap.id),
    );
    expect(testMapOnNode1.get("name")).toEqual("Test");

    // node2 should also be able to access the map because it is admin of parentGroup
    const testMapOnNode2 = expectMap(
      await loadCoValueOrFail(node2.node, testMap.id),
    );
    expect(testMapOnNode2.get("name")).toEqual("Test");
  });

  test("adding new group members should work for groups extended without having membership in the parent group", async () => {
    const nodes = await createNConnectedNodes(
      "server",
      "server",
      "server",
      "server",
    );
    const node1 = nodes[0]!;
    const node2 = nodes[1]!;
    const node3 = nodes[2]!;
    const node4 = nodes[3]!;

    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "admin");

    const childGroup = node1.node.createGroup();
    childGroup.extend(parentGroup, "admin");

    // Wait for childGroup to sync so node3 sees the groupSealer field
    await childGroup.core.waitForSync();

    const sharedGroup = node3.node.createGroup();

    // Account3 does not have permissions over the childGroup being extended
    const childGroupOnNode3 = await loadCoValueOrFail(
      node3.node,
      childGroup.id,
    );
    sharedGroup.extend(childGroupOnNode3, "admin");

    // Add a new parent group to the previously extended child group
    const newParentGroup = node1.node.createGroup();
    const account4OnNode1 = await loadCoValueOrFail(
      node1.node,
      node4.accountID,
    );
    newParentGroup.addMember(account4OnNode1, "admin");
    childGroup.extend(newParentGroup);

    // Wait for sharedGroup and childGroup updates to sync
    await sharedGroup.core.waitForSync();
    await childGroup.core.waitForSync();

    // Create a map owned by sharedGroup
    const testMap = sharedGroup.createMap();
    testMap.set("name", "Test");

    // Wait for map to sync
    await testMap.core.waitForSync();

    // Account4 should be able to access the map because it is admin of newParentGroup
    const testMapOnNode4 = expectMap(
      await loadCoValueOrFail(node4.node, testMap.id),
    );
    expect(testMapOnNode4.get("name")).toEqual("Test");
  });

  test("should be possible to extend a group after getting revoked from the parent group", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const parentGroup = node1.node.createGroup();

    const alice = await loadCoValueOrFail(node1.node, node3.accountID);
    const bob = await loadCoValueOrFail(node1.node, node2.accountID);
    parentGroup.addMember(alice, "writer");
    parentGroup.addMember(bob, "reader");
    parentGroup.removeMember(bob);

    const parentGroupOnNode2 = await loadCoValueOrFail(
      node2.node,
      parentGroup.id,
    );

    const childGroup = node2.node.createGroup();
    childGroup.extend(parentGroupOnNode2);

    expect(childGroup.roleOf(alice.id)).toBe("writer");
  });

  test("should be possible to extend when access is everyone reader and the account is revoked from the parent group", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    const parentGroup = node1.node.createGroup();
    parentGroup.addMember("everyone", "reader");
    const alice = await loadCoValueOrFail(node1.node, node3.accountID);
    const bob = await loadCoValueOrFail(node1.node, node2.accountID);
    parentGroup.addMember(alice, "writer");
    parentGroup.addMember(bob, "reader");
    parentGroup.removeMember(bob);

    const parentGroupOnNode2 = await loadCoValueOrFail(
      node2.node,
      parentGroup.id,
    );

    const childGroup = node2.node.createGroup();
    childGroup.extend(parentGroupOnNode2);

    expect(childGroup.roleOf(alice.id)).toBe("writer");
  });

  test("should be able to extend when the last read key is healed", async () => {
    const clientWithAccess = setupTestNode({
      secret:
        "sealerSecret_zBTPp7U58Fzq9o7EvJpu4KEziepi8QVf2Xaxuy5xmmXFx/signerSecret_z62DuviZdXCjz4EZWofvr9vaLYFXDeTaC9KWhoQiQjzKk",
      connected: true,
    });
    const clientWithoutAccess = setupTestNode({
      connected: true,
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

    clientWithAccess.node.syncManager.handleNewContent(
      brokenGroupContent,
      "import",
    );

    // Load the CoValue to recover the key_for_everyone
    await loadCoValueOrFail(
      clientWithAccess.node,
      brokenGroupContent.id as CoID<RawGroup>,
    );

    const group = await loadCoValueOrFail(
      clientWithoutAccess.node,
      brokenGroupContent.id as CoID<RawGroup>,
    );
    const childGroup = clientWithoutAccess.node.createGroup();
    childGroup.extend(group);

    expect(childGroup.getParentGroups()).toEqual([group]);
  });

  test("should be able to extend when the last read key is missing", async () => {
    const clientWithoutAccess = setupTestNode({
      connected: true,
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

    clientWithoutAccess.node.syncManager.handleNewContent(
      brokenGroupContent,
      "import",
    );

    const group = await loadCoValueOrFail(
      clientWithoutAccess.node,
      brokenGroupContent.id as CoID<RawGroup>,
    );
    const childGroup = clientWithoutAccess.node.createGroup();
    childGroup.extend(group);

    expect(childGroup.getParentGroups()).toEqual([group]);
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

  test("mapping to manager should add the ability to add members", async () => {
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
    childGroup.extend(group, "manager");

    expect(childGroup.roleOf(node2.accountID)).toEqual("manager");

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

  test("if an account is revoked on the child but still a member of the parent, transactions should be considered valid", async () => {
    const alice = await setupTestAccount({
      connected: true,
    });
    const bob = await setupTestAccount({
      connected: true,
    });
    const charlie = await setupTestAccount({
      connected: true,
    });

    const group = alice.node.createGroup();
    const parentGroup = alice.node.createGroup();

    const bobInAlice = await loadCoValueOrFail(alice.node, bob.accountID);
    group.addMember(bobInAlice, "admin");
    group.extend(parentGroup, "admin");
    parentGroup.addMember(bobInAlice, "admin");

    const groupInBob = await loadCoValueOrFail(bob.node, group.id);
    groupInBob.removeMember(bob.node.getCurrentAgent());

    const charlieInBob = await loadCoValueOrFail(bob.node, charlie.accountID);
    groupInBob.addMember(charlieInBob, "reader");

    expect(groupInBob.roleOf(charlie.accountID)).toBe("reader");
  });

  test("if an account is revoked on the parent, their old transactions on the child should stay valid", async () => {
    const alice = await setupTestAccount({
      connected: true,
    });
    const bob = await setupTestAccount({
      connected: true,
    });
    const charlie = await setupTestAccount({
      connected: true,
    });

    const group = alice.node.createGroup();
    const parentGroup = alice.node.createGroup();

    const bobInAlice = await loadCoValueOrFail(alice.node, bob.accountID);
    group.extend(parentGroup, "admin");
    parentGroup.addMember(bobInAlice, "admin");

    const groupInBob = await loadCoValueOrFail(bob.node, group.id);
    const parentGroupInBob = await loadCoValueOrFail(bob.node, parentGroup.id);
    const charlieInBob = await loadCoValueOrFail(bob.node, charlie.accountID);
    groupInBob.addMember(charlieInBob, "reader");

    await new Promise((r) => setTimeout(r, 10));

    parentGroupInBob.removeMember(bob.node.getCurrentAgent());

    const groupInCharlie = await loadCoValueOrFail(charlie.node, group.id);

    expect(groupInCharlie.roleOf(charlie.accountID)).toBe("reader");
  });
});
