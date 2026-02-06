import { assert, beforeEach, describe, expect, test } from "vitest";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import { SessionID } from "../ids.js";
import { expectGroup } from "../typeUtils/expectGroup.js";
import { LocalNode } from "../localNode.js";
import { RawGroup } from "../coValues/group.js";
import {
  SyncMessagesLog,
  createNConnectedNodes,
  createTwoConnectedNodes,
  createThreeConnectedNodes,
  loadCoValueOrFail,
  setupTestNode,
} from "./testUtils.js";

const crypto = await WasmCrypto.create();

/**
 * Creates a group without the groupSealer field, simulating a legacy group
 * created before the groupSealer feature was introduced.
 */
function createLegacyGroup(node: LocalNode): RawGroup {
  const account = node.getCurrentAgent();

  const groupCoValue = node.createCoValue({
    type: "comap",
    ruleset: { type: "group", initialAdmin: account.id },
    meta: null,
    ...node.crypto.createdNowUnique(),
  });

  const group = expectGroup(groupCoValue.getCurrentContent());

  group.set(account.id, "admin", "trusting");

  const readKey = node.crypto.newRandomKeySecret();

  group.set(
    `${readKey.id}_for_${account.id}`,
    node.crypto.seal({
      message: readKey.secret,
      from: account.currentSealerSecret(),
      to: account.currentSealerID(),
      nOnceMaterial: {
        in: groupCoValue.id,
        tx: groupCoValue.nextTransactionID(),
      },
    }),
    "trusting",
  );

  group.set("readKey", readKey.id, "trusting");

  // Intentionally NOT setting groupSealer to simulate a pre-feature group

  return group;
}

// ============================================================================
// Unit tests for sealForGroup / unsealForGroup crypto operations (Task 26)
// ============================================================================

describe("sealForGroup / unsealForGroup crypto operations", () => {
  test("sealForGroup round-trips with correct sealer secret", () => {
    const data = { secret: "hello world", nested: { value: 123 } };
    const sealer = crypto.newRandomSealer();
    const sealerID = crypto.getSealerID(sealer);

    const nOnceMaterial = {
      in: "co_zTEST" as const,
      tx: { sessionID: "co_zTEST_session_zTEST" as SessionID, txIndex: 0 },
    };

    const sealed = crypto.sealForGroup({
      message: data,
      to: sealerID,
      nOnceMaterial,
    });

    expect(sealed).toMatch(/^sealedForGroup_U/);

    const unsealed = crypto.unsealForGroup(sealed, sealer, nOnceMaterial);
    expect(unsealed).toEqual(data);
  });

  test("sealForGroup fails to unseal with wrong sealer secret", () => {
    const data = { secret: "sensitive data" };
    const sealer = crypto.newRandomSealer();
    const wrongSealer = crypto.newRandomSealer();
    const sealerID = crypto.getSealerID(sealer);

    const nOnceMaterial = {
      in: "co_zTEST" as const,
      tx: { sessionID: "co_zTEST_session_zTEST" as SessionID, txIndex: 0 },
    };

    const sealed = crypto.sealForGroup({
      message: data,
      to: sealerID,
      nOnceMaterial,
    });

    // Wrong sealer should fail to unseal
    const result = crypto.unsealForGroup(sealed, wrongSealer, nOnceMaterial);
    expect(result).toBeUndefined();
  });

  test("sealForGroup fails to unseal with wrong nonce material", () => {
    const data = { secret: "sensitive data" };
    const sealer = crypto.newRandomSealer();
    const sealerID = crypto.getSealerID(sealer);

    const nOnceMaterial = {
      in: "co_zTEST" as const,
      tx: { sessionID: "co_zTEST_session_zTEST" as SessionID, txIndex: 0 },
    };

    const wrongNOnceMaterial = {
      in: "co_zDIFFERENT" as const,
      tx: { sessionID: "co_zDIFFERENT_session_zTEST" as SessionID, txIndex: 0 },
    };

    const sealed = crypto.sealForGroup({
      message: data,
      to: sealerID,
      nOnceMaterial,
    });

    // Wrong nonce material should fail to unseal
    const result = crypto.unsealForGroup(sealed, sealer, wrongNOnceMaterial);
    expect(result).toBeUndefined();
  });

  test("sealForGroup uses ephemeral keys (different ciphertext each time)", () => {
    const data = { message: "same message" };
    const sealer = crypto.newRandomSealer();
    const sealerID = crypto.getSealerID(sealer);

    const nOnceMaterial = {
      in: "co_zTEST" as const,
      tx: { sessionID: "co_zTEST_session_zTEST" as SessionID, txIndex: 0 },
    };

    const sealed1 = crypto.sealForGroup({
      message: data,
      to: sealerID,
      nOnceMaterial,
    });

    const sealed2 = crypto.sealForGroup({
      message: data,
      to: sealerID,
      nOnceMaterial,
    });

    // Same message should produce different ciphertext due to ephemeral keys
    expect(sealed1).not.toEqual(sealed2);

    // But both should decrypt to the same value
    expect(crypto.unsealForGroup(sealed1, sealer, nOnceMaterial)).toEqual(data);
    expect(crypto.unsealForGroup(sealed2, sealer, nOnceMaterial)).toEqual(data);
  });

  test("groupSealerFromReadKey is deterministic", () => {
    const readKey = crypto.newRandomKeySecret();

    const sealer1 = crypto.groupSealerFromReadKey(readKey.secret);
    const sealer2 = crypto.groupSealerFromReadKey(readKey.secret);

    expect(sealer1.publicKey).toEqual(sealer2.publicKey);
    expect(sealer1.secret).toEqual(sealer2.secret);
  });

  test("groupSealerFromReadKey produces different sealers for different keys", () => {
    const readKey1 = crypto.newRandomKeySecret();
    const readKey2 = crypto.newRandomKeySecret();

    const sealer1 = crypto.groupSealerFromReadKey(readKey1.secret);
    const sealer2 = crypto.groupSealerFromReadKey(readKey2.secret);

    expect(sealer1.publicKey).not.toEqual(sealer2.publicKey);
    expect(sealer1.secret).not.toEqual(sealer2.secret);
  });

  test("derived sealer works with sealForGroup/unsealForGroup", () => {
    const readKey = crypto.newRandomKeySecret();
    const sealer = crypto.groupSealerFromReadKey(readKey.secret);

    const data = { key: "value", num: 42 };
    const nOnceMaterial = {
      in: "co_zTEST" as const,
      tx: { sessionID: "co_zTEST_session_zTEST" as SessionID, txIndex: 0 },
    };

    const sealed = crypto.sealForGroup({
      message: data,
      to: sealer.publicKey,
      nOnceMaterial,
    });

    const unsealed = crypto.unsealForGroup(
      sealed,
      sealer.secret,
      nOnceMaterial,
    );
    expect(unsealed).toEqual(data);
  });
});

// ============================================================================
// Integration tests for groupSealer (Tasks 27-36)
// ============================================================================

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("groups created with groupSealer", () => {
  test("new groups are created with a groupSealer field (Task 27)", async () => {
    const { node1 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();

    const groupSealer = group.get("groupSealer");
    expect(groupSealer).toBeDefined();
    // New composite format: "key_z...@sealer_z..."
    expect(groupSealer).toMatch(/^key_z.+@sealer_z/);
  });

  test("groupSealer is derived from readKey deterministically (Task 28)", async () => {
    const { node1 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();

    const groupSealer = group.get("groupSealer") as string;
    const readKey = group.getCurrentReadKey();

    expect(readKey.secret).toBeDefined();
    if (!readKey.secret) {
      throw new Error("Expected read key secret");
    }

    const derivedSealer = crypto.groupSealerFromReadKey(readKey.secret);
    // Composite format: "readKeyID@sealerID"
    expect(groupSealer).toEqual(`${readKey.id}@${derivedSealer.publicKey}`);
  });

  test("getGroupSealerSecret derives the correct secret", async () => {
    const { node1 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();

    const groupSealerSecret = group.getGroupSealerSecret();
    expect(groupSealerSecret).toBeDefined();

    const groupSealerValue = group.get("groupSealer") as string;
    expect(groupSealerValue).toBeDefined();

    // Verify the secret corresponds to the SealerID embedded in the composite value
    assert(groupSealerSecret, "Expected groupSealerSecret");
    const derivedID = crypto.getSealerID(groupSealerSecret);
    expect(groupSealerValue).toContain(derivedID);
  });
});

describe("non-member extending child to parent via groupSealer", () => {
  test("parent member can read child group content via extension", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group and adds node2 as member
    const parentGroup = node1.node.createGroup();
    await parentGroup.core.waitForSync();

    // Node2 creates child group and extends parent (node2 is a member of parent)
    const parentOnNode2 = await loadCoValueOrFail(node2.node, parentGroup.id);
    const childGroup = node2.node.createGroup();
    childGroup.extend(parentOnNode2);

    // Add node3 as writeOnly to child (node3 is NOT a member of parent)
    const account3OnNode2 = await loadCoValueOrFail(
      node2.node,
      node3.accountID,
    );
    childGroup.addMember(account3OnNode2, "writeOnly");

    const map = childGroup.createMap();
    map.set("test", "Written by node2");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node1 (parent member) should be able to read content from child
    const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
    expect(mapOnNode1.get("test")).toEqual("Written by node2");

    // Verify that the child group has the groupSealer set
    const childGroupOnNode1 = await loadCoValueOrFail(
      node1.node,
      childGroup.id,
    );
    const childGroupSealer = childGroupOnNode1.get("groupSealer");
    expect(childGroupSealer).toBeDefined();
    // New composite format: "key_z...@sealer_z..."
    expect(childGroupSealer).toMatch(/^key_z.+@sealer_z/);
  });

  test("writeOnly member uses groupSealer for key revelation", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group and adds node2 as admin
    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "admin");

    await parentGroup.core.waitForSync();

    // Node2 creates child group and extends parent
    const parentOnNode2 = await loadCoValueOrFail(node2.node, parentGroup.id);
    const childGroup = node2.node.createGroup();
    childGroup.extend(parentOnNode2);

    // Add node3 as writeOnly to child (node3 is NOT a member of parent)
    const account3OnNode2 = await loadCoValueOrFail(
      node2.node,
      node3.accountID,
    );
    childGroup.addMember(account3OnNode2, "writeOnly");

    await childGroup.core.waitForSync();

    // Node3 now has writeOnly access to child but no access to parent
    // When node3 writes, the child needs to reveal keys to parent via groupSealer
    const childGroupOnNode3 = await loadCoValueOrFail(
      node3.node,
      childGroup.id,
    );

    const map = childGroupOnNode3.createMap();
    map.set("test", "Written by node3 (writeOnly)");

    await map.core.waitForSync();

    // Node1 (parent admin) should be able to read content written by node3
    const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
    expect(mapOnNode1.get("test")).toEqual("Written by node3 (writeOnly)");
  });

  test("no writeOnly key created when parent has groupSealer", async () => {
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
    parentGroup.addMember(account2OnNode1, "writer");

    await parentGroup.core.waitForSync();

    const parentOnNode2 = await loadCoValueOrFail(node2.node, parentGroup.id);
    const childGroup = node2.node.createGroup();
    childGroup.extend(parentOnNode2);

    // Add node3 as writeOnly (this should use groupSealer, not writeOnly key)
    const account3OnNode2 = await loadCoValueOrFail(
      node2.node,
      node3.accountID,
    );
    childGroup.addMember(account3OnNode2, "writeOnly");

    await childGroup.core.waitForSync();

    // Check that no writeKeyFor_ entry exists for the parent group
    // This verifies we're using groupSealer instead of writeOnly keys
    const childGroupOnNode1 = await loadCoValueOrFail(
      node1.node,
      childGroup.id,
    );

    // The parent group ID should not have a writeKeyFor entry in the child
    const writeKeyForParent = childGroupOnNode1.get(
      `writeKeyFor_${parentGroup.id}` as any,
    );
    expect(writeKeyForParent).toBeUndefined();
  });

  // Role variation tests
  test("reader in parent can read child content via groupSealer extension", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group and adds node2 as reader (not admin/writer)
    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "reader");

    await parentGroup.core.waitForSync();

    // Node3 creates child group and extends parent (node3 is NOT a member of parent)
    const parentOnNode3 = await loadCoValueOrFail(node3.node, parentGroup.id);
    const childGroup = node3.node.createGroup();
    childGroup.extend(parentOnNode3);

    const map = childGroup.createMap();
    map.set("test", "Written by non-member node3");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node2 (reader in parent) should be able to read content from child
    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
    expect(mapOnNode2.get("test")).toEqual("Written by non-member node3");
  });

  test("writer in parent can read child content via groupSealer extension", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group and adds node2 as writer
    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "writer");

    await parentGroup.core.waitForSync();

    // Node3 creates child group and extends parent (node3 is NOT a member of parent)
    const parentOnNode3 = await loadCoValueOrFail(node3.node, parentGroup.id);
    const childGroup = node3.node.createGroup();
    childGroup.extend(parentOnNode3);

    const map = childGroup.createMap();
    map.set("test", "Written by non-member node3");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node2 (writer in parent) should be able to read content from child
    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
    expect(mapOnNode2.get("test")).toEqual("Written by non-member node3");
  });

  test("manager in parent can read child content via groupSealer extension", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group and adds node2 as manager
    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "manager");

    await parentGroup.core.waitForSync();

    // Node3 creates child group and extends parent (node3 is NOT a member of parent)
    const parentOnNode3 = await loadCoValueOrFail(node3.node, parentGroup.id);
    const childGroup = node3.node.createGroup();
    childGroup.extend(parentOnNode3);

    const map = childGroup.createMap();
    map.set("test", "Written by non-member node3");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node2 (manager in parent) should be able to read content from child
    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
    expect(mapOnNode2.get("test")).toEqual("Written by non-member node3");
  });

  // Multi-level hierarchy tests
  test("three-level extension chain: grandparent can read grandchild content", async () => {
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

    // Node1 creates grandparent group and adds node2 as admin
    const grandparentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    grandparentGroup.addMember(account2OnNode1, "admin");

    await grandparentGroup.core.waitForSync();

    // Node2 creates parent group and extends grandparent
    const grandparentOnNode2 = await loadCoValueOrFail(
      node2.node,
      grandparentGroup.id,
    );
    const parentGroup = node2.node.createGroup();
    parentGroup.extend(grandparentOnNode2);

    await parentGroup.core.waitForSync();

    // Node3 (NOT a member of grandparent or parent) creates child and extends parent
    const parentOnNode3 = await loadCoValueOrFail(node3.node, parentGroup.id);
    const childGroup = node3.node.createGroup();
    childGroup.extend(parentOnNode3);

    // Node4 (NOT a member) creates grandchild and extends child
    const childOnNode4 = await loadCoValueOrFail(node4.node, childGroup.id);
    const grandchildGroup = node4.node.createGroup();
    grandchildGroup.extend(childOnNode4);

    const map = grandchildGroup.createMap();
    map.set("test", "Written in grandchild by node4");

    await map.core.waitForSync();
    await grandchildGroup.core.waitForSync();
    await childGroup.core.waitForSync();
    await parentGroup.core.waitForSync();

    // Node1 (grandparent admin) should be able to read content from grandchild
    const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
    expect(mapOnNode1.get("test")).toEqual("Written in grandchild by node4");
  });

  test("parallel extensions: two non-members extend to same parent", async () => {
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

    // Node1 creates parent group
    const parentGroup = node1.node.createGroup();

    await parentGroup.core.waitForSync();

    // Node2 (NOT a member of parent) creates child1 and extends parent
    const parentOnNode2 = await loadCoValueOrFail(node2.node, parentGroup.id);
    const childGroup1 = node2.node.createGroup();
    childGroup1.extend(parentOnNode2);

    const map1 = childGroup1.createMap();
    map1.set("test", "Written by node2 in child1");

    await map1.core.waitForSync();
    await childGroup1.core.waitForSync();

    // Node3 (NOT a member of parent) creates child2 and extends parent
    const parentOnNode3 = await loadCoValueOrFail(node3.node, parentGroup.id);
    const childGroup2 = node3.node.createGroup();
    childGroup2.extend(parentOnNode3);

    const map2 = childGroup2.createMap();
    map2.set("test", "Written by node3 in child2");

    await map2.core.waitForSync();
    await childGroup2.core.waitForSync();

    // Node4 (NOT a member of parent) creates child3 and extends parent
    const parentOnNode4 = await loadCoValueOrFail(node4.node, parentGroup.id);
    const childGroup3 = node4.node.createGroup();
    childGroup3.extend(parentOnNode4);

    const map3 = childGroup3.createMap();
    map3.set("test", "Written by node4 in child3");

    await map3.core.waitForSync();
    await childGroup3.core.waitForSync();

    // Node1 (parent admin) should be able to read all three child contents
    const map1OnNode1 = await loadCoValueOrFail(node1.node, map1.id);
    expect(map1OnNode1.get("test")).toEqual("Written by node2 in child1");

    const map2OnNode1 = await loadCoValueOrFail(node1.node, map2.id);
    expect(map2OnNode1.get("test")).toEqual("Written by node3 in child2");

    const map3OnNode1 = await loadCoValueOrFail(node1.node, map3.id);
    expect(map3OnNode1.get("test")).toEqual("Written by node4 in child3");
  });

  // Key rotation scenarios
  test("extension after parent key rotation uses new groupSealer", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group
    const parentGroup = node1.node.createGroup();
    const originalGroupSealer = parentGroup.get("groupSealer");

    await parentGroup.core.waitForSync();

    // Node1 rotates the parent group's read key (and thus groupSealer)
    parentGroup.rotateReadKey();
    const newGroupSealer = parentGroup.get("groupSealer");

    expect(newGroupSealer).not.toEqual(originalGroupSealer);

    await parentGroup.core.waitForSync();

    // Node2 (NOT a member of parent) creates child and extends parent after rotation
    const parentOnNode2 = await loadCoValueOrFail(node2.node, parentGroup.id);

    // Verify node2 sees the new groupSealer
    expect(parentOnNode2.get("groupSealer")).toEqual(newGroupSealer);

    const childGroup = node2.node.createGroup();
    childGroup.extend(parentOnNode2);

    const map = childGroup.createMap();
    map.set("test", "Written after parent key rotation");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node1 (parent admin) should be able to read content using the new groupSealer
    const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
    expect(mapOnNode1.get("test")).toEqual("Written after parent key rotation");
  });

  test("old content remains readable after child key rotation", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group
    const parentGroup = node1.node.createGroup();

    await parentGroup.core.waitForSync();

    // Node2 (NOT a member of parent) creates child and extends parent
    const parentOnNode2 = await loadCoValueOrFail(node2.node, parentGroup.id);
    const childGroup = node2.node.createGroup();
    childGroup.extend(parentOnNode2);

    // Create content before key rotation
    const mapBefore = childGroup.createMap();
    mapBefore.set("test", "Written before child key rotation");

    await mapBefore.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node1 verifies it can read content before rotation
    const mapBeforeOnNode1 = await loadCoValueOrFail(node1.node, mapBefore.id);
    expect(mapBeforeOnNode1.get("test")).toEqual(
      "Written before child key rotation",
    );

    // Node2 rotates child group's key
    // Note: For non-members, the new key cannot be revealed to parent via groupSealer
    // in the current implementation (see GitHub issue #1979)
    childGroup.rotateReadKey();

    await childGroup.core.waitForSync();

    // Node1 should still be able to read OLD content created before rotation
    // This verifies the historical sealer mechanism works correctly
    const mapBeforeOnNode1Again = await loadCoValueOrFail(
      node1.node,
      mapBefore.id,
    );
    expect(mapBeforeOnNode1Again.get("test")).toEqual(
      "Written before child key rotation",
    );
  });

  test("member of parent can read child content after child key rotation", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group and adds node2 as admin
    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "admin");

    await parentGroup.core.waitForSync();

    // Node2 (member of parent) creates child and extends parent
    const parentOnNode2 = await loadCoValueOrFail(node2.node, parentGroup.id);
    const childGroup = node2.node.createGroup();
    childGroup.extend(parentOnNode2);

    // Create content before key rotation
    const mapBefore = childGroup.createMap();
    mapBefore.set("test", "Written before rotation");

    await mapBefore.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node2 rotates child group's key (as a member, they have access to parent readKey)
    childGroup.rotateReadKey();

    // Create content after key rotation
    const mapAfter = childGroup.createMap();
    mapAfter.set("test", "Written after rotation");

    await mapAfter.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node1 should be able to read both old and new content
    const mapBeforeOnNode1 = await loadCoValueOrFail(node1.node, mapBefore.id);
    expect(mapBeforeOnNode1.get("test")).toEqual("Written before rotation");

    const mapAfterOnNode1 = await loadCoValueOrFail(node1.node, mapAfter.id);
    expect(mapAfterOnNode1.get("test")).toEqual("Written after rotation");
  });

  test("old content visible after 3 key rotations via historical groupSealer", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group and adds node2 as admin
    const parentGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "admin");

    await parentGroup.core.waitForSync();

    // Node2 (member of parent) creates child and extends parent
    const parentOnNode2 = await loadCoValueOrFail(node2.node, parentGroup.id);
    const childGroup = node2.node.createGroup();
    childGroup.extend(parentOnNode2);

    // Create content with the initial key
    const map0 = childGroup.createMap();
    map0.set("test", "Before any rotation");

    await map0.core.waitForSync();
    await childGroup.core.waitForSync();

    // Rotation 1
    childGroup.rotateReadKey();

    const map1 = childGroup.createMap();
    map1.set("test", "After rotation 1");

    await map1.core.waitForSync();
    await childGroup.core.waitForSync();

    // Rotation 2
    childGroup.rotateReadKey();

    const map2 = childGroup.createMap();
    map2.set("test", "After rotation 2");

    await map2.core.waitForSync();
    await childGroup.core.waitForSync();

    // Rotation 3
    childGroup.rotateReadKey();

    const map3 = childGroup.createMap();
    map3.set("test", "After rotation 3");

    await map3.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node1 (parent admin) should be able to read content from all key generations
    const map0OnNode1 = await loadCoValueOrFail(node1.node, map0.id);
    expect(map0OnNode1.get("test")).toEqual("Before any rotation");

    const map1OnNode1 = await loadCoValueOrFail(node1.node, map1.id);
    expect(map1OnNode1.get("test")).toEqual("After rotation 1");

    const map2OnNode1 = await loadCoValueOrFail(node1.node, map2.id);
    expect(map2OnNode1.get("test")).toEqual("After rotation 2");

    const map3OnNode1 = await loadCoValueOrFail(node1.node, map3.id);
    expect(map3OnNode1.get("test")).toEqual("After rotation 3");
  });

  // Edge case tests
  test("non-member can create multiple CoValues readable by parent", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group
    const parentGroup = node1.node.createGroup();

    await parentGroup.core.waitForSync();

    // Node2 (NOT a member of parent) creates child and extends parent
    const parentOnNode2 = await loadCoValueOrFail(node2.node, parentGroup.id);
    const childGroup = node2.node.createGroup();
    childGroup.extend(parentOnNode2);

    // Create multiple maps
    const map1 = childGroup.createMap();
    map1.set("name", "Map 1");

    const map2 = childGroup.createMap();
    map2.set("name", "Map 2");

    const map3 = childGroup.createMap();
    map3.set("name", "Map 3");

    const map4 = childGroup.createMap();
    map4.set("name", "Map 4");

    const map5 = childGroup.createMap();
    map5.set("name", "Map 5");

    await Promise.all([
      map1.core.waitForSync(),
      map2.core.waitForSync(),
      map3.core.waitForSync(),
      map4.core.waitForSync(),
      map5.core.waitForSync(),
    ]);
    await childGroup.core.waitForSync();

    // Node1 (parent admin) should be able to read all maps
    const map1OnNode1 = await loadCoValueOrFail(node1.node, map1.id);
    expect(map1OnNode1.get("name")).toEqual("Map 1");

    const map2OnNode1 = await loadCoValueOrFail(node1.node, map2.id);
    expect(map2OnNode1.get("name")).toEqual("Map 2");

    const map3OnNode1 = await loadCoValueOrFail(node1.node, map3.id);
    expect(map3OnNode1.get("name")).toEqual("Map 3");

    const map4OnNode1 = await loadCoValueOrFail(node1.node, map4.id);
    expect(map4OnNode1.get("name")).toEqual("Map 4");

    const map5OnNode1 = await loadCoValueOrFail(node1.node, map5.id);
    expect(map5OnNode1.get("name")).toEqual("Map 5");
  });

  test("content created before extension is accessible after extension", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group
    const parentGroup = node1.node.createGroup();

    await parentGroup.core.waitForSync();

    // Node2 creates child group and content BEFORE extending
    const childGroup = node2.node.createGroup();

    const mapBefore = childGroup.createMap();
    mapBefore.set("test", "Created before extension");

    await mapBefore.core.waitForSync();
    await childGroup.core.waitForSync();

    // Now extend the child to parent
    const parentOnNode2 = await loadCoValueOrFail(node2.node, parentGroup.id);
    childGroup.extend(parentOnNode2);

    // Create content after extension
    const mapAfter = childGroup.createMap();
    mapAfter.set("test", "Created after extension");

    await mapAfter.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node1 should be able to read content created both before and after extension
    const mapBeforeOnNode1 = await loadCoValueOrFail(node1.node, mapBefore.id);
    expect(mapBeforeOnNode1.get("test")).toEqual("Created before extension");

    const mapAfterOnNode1 = await loadCoValueOrFail(node1.node, mapAfter.id);
    expect(mapAfterOnNode1.get("test")).toEqual("Created after extension");
  });

  test("nested group membership: members of member-group can read via groupSealer", async () => {
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

    // Node1 creates an inner group and adds node2 as admin
    const innerGroup = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    innerGroup.addMember(account2OnNode1, "admin");

    await innerGroup.core.waitForSync();

    // Node1 creates parent group and adds innerGroup as member (nested group membership)
    // Using extend() to add innerGroup's members to parentGroup
    const parentGroup = node1.node.createGroup();
    parentGroup.extend(innerGroup, "writer");

    await parentGroup.core.waitForSync();

    // Node3 (NOT a member of parent or innerGroup) creates child and extends parent
    const parentOnNode3 = await loadCoValueOrFail(node3.node, parentGroup.id);
    const childGroup = node3.node.createGroup();
    childGroup.extend(parentOnNode3);

    const map = childGroup.createMap();
    map.set("test", "Written by non-member node3");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node2 (member of innerGroup, which is extended by parent) should be able to read
    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
    expect(mapOnNode2.get("test")).toEqual("Written by non-member node3");

    // Node1 (admin of both groups) should also be able to read
    const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
    expect(mapOnNode1.get("test")).toEqual("Written by non-member node3");
  });
});

describe("key rotation updates groupSealer", () => {
  test("rotating readKey also rotates groupSealer", async () => {
    const { node1 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    const originalGroupSealer = group.get("groupSealer");

    // Force key rotation
    group.rotateReadKey();

    const newGroupSealer = group.get("groupSealer") as string;

    expect(newGroupSealer).toBeDefined();
    expect(newGroupSealer).not.toEqual(originalGroupSealer);

    // Verify the new sealer is derived from the new read key
    const newReadKey = group.getCurrentReadKey();
    if (!newReadKey.secret) {
      throw new Error("Expected read key secret after rotation");
    }

    const derivedSealer = crypto.groupSealerFromReadKey(newReadKey.secret);
    // Composite format includes the readKeyID
    expect(newGroupSealer).toEqual(
      `${newReadKey.id}@${derivedSealer.publicKey}`,
    );
  });
});

describe("concurrent group sealer initialization", () => {
  test("concurrent group sealer initialization produces same result", () => {
    // Since groupSealer is derived deterministically from readKey,
    // multiple calls with the same readKey will always produce the same result
    const readKey = crypto.newRandomKeySecret();

    // Simulate concurrent derivations
    const results = Array.from({ length: 10 }, () =>
      crypto.groupSealerFromReadKey(readKey.secret),
    );

    // All results should be identical
    const firstResult = results[0];
    for (const result of results) {
      expect(result.publicKey).toEqual(firstResult!.publicKey);
      expect(result.secret).toEqual(firstResult!.secret);
    }
  });
});

describe("groupSealer composite format (readKeyID association)", () => {
  test("groupSealer stores readKeyID in composite format", async () => {
    const { node1 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    const groupSealerValue = group.get("groupSealer") as string;

    // Should be in composite format: "key_z...@sealer_z..."
    expect(groupSealerValue).toMatch(/^key_z.+@sealer_z/);

    // The readKeyID portion should match the current readKey
    const readKeyId = group.getCurrentReadKeyId();
    expect(groupSealerValue.startsWith(readKeyId!)).toBe(true);
  });

  test("concurrent key rotation and migration produce correct readKey association", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Create a legacy group on node1, add node2 and node3 as admins
    const legacyGroup = createLegacyGroup(node1.node);
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    const account3OnNode1 = await loadCoValueOrFail(
      node1.node,
      node3.accountID,
    );
    legacyGroup.addMember(account2OnNode1, "admin");
    legacyGroup.addMember(account3OnNode1, "admin");

    await legacyGroup.core.waitForSync();

    // Node2 loads the group and triggers migration (sets groupSealer from key1)
    const groupOnNode2 = await loadCoValueOrFail(node2.node, legacyGroup.id);
    await groupOnNode2.core.waitForSync();

    // Verify groupSealer was set with composite format
    const sealerOnNode2 = groupOnNode2.get("groupSealer") as string;
    expect(sealerOnNode2).toMatch(/^key_z.+@sealer_z/);

    // Now node2 rotates the read key (simulating concurrent rotation)
    groupOnNode2.rotateReadKey();
    await groupOnNode2.core.waitForSync();

    // The groupSealer should now reference the NEW readKey, not the old one
    const newSealerOnNode2 = groupOnNode2.get("groupSealer") as string;
    const newReadKeyId = groupOnNode2.getCurrentReadKeyId();
    expect(newSealerOnNode2.startsWith(newReadKeyId!)).toBe(true);

    // The new sealer should be different from the old one
    expect(newSealerOnNode2).not.toEqual(sealerOnNode2);
  });

  test("child group extended via groupSealer is readable after parent key rotation + migration race", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Node1 creates parent group with both node2 and node3 as admins
    const parentGroup = createLegacyGroup(node1.node);
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    parentGroup.addMember(account2OnNode1, "admin");

    await parentGroup.core.waitForSync();

    // Node2 sees the parent group
    parentGroup.rotateReadKey();
    const parentGroupOnNode2 = await loadCoValueOrFail(
      node2.node,
      parentGroup.id,
    );

    await Promise.all([
      parentGroup.core.waitForSync(),
      parentGroupOnNode2.core.waitForSync(),
    ]);

    // Node3 (non-member) creates child and extends parent using the current groupSealer
    const parentOnNode3 = await loadCoValueOrFail(node3.node, parentGroup.id);
    const childGroup = node3.node.createGroup();
    childGroup.extend(parentOnNode3);

    const map = childGroup.createMap();
    map.set("test", "Written by non-member");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    // Both node1 and node2 should be able to read the child content
    const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
    expect(mapOnNode1.get("test")).toEqual("Written by non-member");

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);
    expect(mapOnNode2.get("test")).toEqual("Written by non-member");
  });
});

describe("permission validation for groupSealer", () => {
  test("non-admin cannot set groupSealer", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );

    // Add node2 as reader (not admin)
    group.addMember(account2OnNode1, "reader");

    await group.core.waitForSync();

    const groupOnNode2 = await loadCoValueOrFail(node2.node, group.id);

    const originalSealer = groupOnNode2.get("groupSealer");

    // Attempt to set groupSealer as a reader
    const fakeSealer = crypto.newRandomSealer();
    const fakeSealerID = crypto.getSealerID(fakeSealer);

    groupOnNode2.set(
      "groupSealer",
      `${group.getCurrentReadKeyId()!}@${fakeSealerID}`,
      "trusting",
    );

    // The change should be rejected (sealer should remain original)
    // Wait for sync to ensure changes are processed
    await groupOnNode2.core.waitForSync();

    // Re-load group on node1 to check if the invalid change was rejected
    // The original sealer should be preserved because readers can't set groupSealer
    expect(group.get("groupSealer")).toEqual(originalSealer);
  });

  test("admin can set groupSealer", async () => {
    const { node1 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    const originalSealer = group.get("groupSealer");

    // As admin, rotate the read key which will update groupSealer
    group.rotateReadKey();

    const newSealer = group.get("groupSealer");
    expect(newSealer).toBeDefined();
    expect(newSealer).not.toEqual(originalSealer);
  });
});

describe("groupSealer migration for legacy groups", () => {
  test("legacy group without groupSealer gets migrated when loaded by admin on another node", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    // Create a legacy group (without groupSealer) on node1, add node2 as admin
    const legacyGroup = createLegacyGroup(node1.node);
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    legacyGroup.addMember(account2OnNode1, "admin");

    expect(legacyGroup.get("groupSealer")).toBeUndefined();

    await legacyGroup.core.waitForSync();

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Node2 (admin) loads the group - migration should add the groupSealer
    const groupOnNode2 = await loadCoValueOrFail(node2.node, legacyGroup.id);
    expect(groupOnNode2.get("groupSealer")).toBeDefined();
    // Migrated groups use the new composite format
    expect(groupOnNode2.get("groupSealer")).toMatch(/^key_z.+@sealer_z/);

    // Verify it's derived from the current read key
    const readKey = groupOnNode2.getCurrentReadKey();
    expect(readKey.secret).toBeDefined();
    const expectedSealer = crypto.groupSealerFromReadKey(readKey.secret!);
    expect(groupOnNode2.get("groupSealer")).toEqual(
      `${readKey.id}@${expectedSealer.publicKey}`,
    );
  });

  test("migration is idempotent - loading on two admin nodes produces same groupSealer", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Create a legacy group on node1, add node2 and node3 as admins
    const legacyGroup = createLegacyGroup(node1.node);
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    const account3OnNode1 = await loadCoValueOrFail(
      node1.node,
      node3.accountID,
    );
    legacyGroup.addMember(account2OnNode1, "admin");
    legacyGroup.addMember(account3OnNode1, "admin");

    expect(legacyGroup.get("groupSealer")).toBeUndefined();

    await legacyGroup.core.waitForSync();

    // Node2 loads and migrates
    const groupOnNode2 = await loadCoValueOrFail(node2.node, legacyGroup.id);
    await groupOnNode2.core.waitForSync();

    const sealerFromNode2 = groupOnNode2.get("groupSealer");
    expect(sealerFromNode2).toBeDefined();

    // Record transaction count after node2's migration has synced
    const transactionsAfterNode2 =
      groupOnNode2.core.getValidSortedTransactions();

    // Node3 loads - groupSealer is already set via sync from node2,
    // so no new migration should be applied
    const groupOnNode3 = await loadCoValueOrFail(node3.node, legacyGroup.id);
    await groupOnNode3.core.waitForSync();

    const sealerFromNode3 = groupOnNode3.get("groupSealer");
    expect(sealerFromNode3).toBeDefined();

    // Both should have the same groupSealer (deterministic from readKey)
    expect(sealerFromNode3).toEqual(sealerFromNode2);

    // Verify no redundant migration was applied — transaction count should be unchanged
    expect(groupOnNode3.core.getValidSortedTransactions()).toHaveLength(
      transactionsAfterNode2.length,
    );
  });

  test("parallel migrations from different accounts produce same groupSealer", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Create a legacy group on node1, add node2 and node3 as admins
    const legacyGroup = createLegacyGroup(node1.node);
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    const account3OnNode1 = await loadCoValueOrFail(
      node1.node,
      node3.accountID,
    );
    legacyGroup.addMember(account2OnNode1, "admin");
    legacyGroup.addMember(account3OnNode1, "admin");

    expect(legacyGroup.get("groupSealer")).toBeUndefined();

    await legacyGroup.core.waitForSync();

    // Both node2 and node3 load the group concurrently, triggering parallel migrations
    const [groupOnNode2, groupOnNode3] = await Promise.all([
      loadCoValueOrFail(node2.node, legacyGroup.id),
      loadCoValueOrFail(node3.node, legacyGroup.id),
    ]);

    // Both should have a groupSealer set
    const sealerOnNode2 = groupOnNode2.get("groupSealer");
    const sealerOnNode3 = groupOnNode3.get("groupSealer");

    expect(sealerOnNode2).toBeDefined();
    expect(sealerOnNode3).toBeDefined();

    // Both should derive the same groupSealer since it's deterministic from readKey
    expect(sealerOnNode2).toEqual(sealerOnNode3);

    // Wait for sync and verify convergence
    await groupOnNode2.core.waitForSync();
    await groupOnNode3.core.waitForSync();

    // After sync, both should still agree
    expect(groupOnNode2.get("groupSealer")).toEqual(
      groupOnNode3.get("groupSealer"),
    );
  });

  test("non-admin member does not trigger migration", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Create a legacy group on node1, add node2 as reader only
    const legacyGroup = createLegacyGroup(node1.node);
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    legacyGroup.addMember(account2OnNode1, "reader");

    expect(legacyGroup.get("groupSealer")).toBeUndefined();

    await legacyGroup.core.waitForSync();

    const transactions = legacyGroup.core.getValidSortedTransactions();

    // Node2 (reader) loads the group - should NOT trigger migration
    const groupOnNode2 = await loadCoValueOrFail(node2.node, legacyGroup.id);

    // The groupSealer should still be undefined because node2 is only a reader
    // and cannot set the groupSealer field
    expect(groupOnNode2.get("groupSealer")).toBeUndefined();
    expect(groupOnNode2.core.getValidSortedTransactions()).toHaveLength(
      transactions.length,
    );
  });

  test("migrated legacy group works with non-member extension via groupSealer", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Create a legacy group on node1 (no groupSealer), add node2 as admin
    const legacyGroup = createLegacyGroup(node1.node);
    const account2OnNode1 = await loadCoValueOrFail(
      node1.node,
      node2.accountID,
    );
    legacyGroup.addMember(account2OnNode1, "admin");

    expect(legacyGroup.get("groupSealer")).toBeUndefined();

    await legacyGroup.core.waitForSync();

    // Node2 (admin) loads the group, triggering migration
    const parentGroup = await loadCoValueOrFail(node2.node, legacyGroup.id);

    // Wait for async migration to complete (runs via waitFor when not fully downloaded)
    await parentGroup.core.waitForSync();

    // Verify migration happened
    expect(parentGroup.get("groupSealer")).toBeDefined();

    await parentGroup.core.waitForSync();

    // Node3 (NOT a member of parent) creates a child group and extends parent
    const parentOnNode3 = await loadCoValueOrFail(node3.node, legacyGroup.id);
    const childGroup = node3.node.createGroup();
    childGroup.extend(parentOnNode3);

    const map = childGroup.createMap();
    map.set("test", "Written by non-member after migration");

    await map.core.waitForSync();
    await childGroup.core.waitForSync();

    // Node1 (original creator/admin) should be able to read content via migrated groupSealer
    // First, sync to pick up the migrated groupSealer
    const parentOnNode1 = await loadCoValueOrFail(node1.node, legacyGroup.id);
    await parentOnNode1.core.waitForSync();

    const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
    expect(mapOnNode1.get("test")).toEqual(
      "Written by non-member after migration",
    );
  });

  test("legacy fallback: parent without groupSealer uses writeOnly key for non-member extension", async () => {
    const { node1, node2, node3 } = await createThreeConnectedNodes(
      "server",
      "server",
      "server",
    );

    // Create a legacy parent group (without groupSealer) on node1
    const legacyParent = createLegacyGroup(node1.node);
    expect(legacyParent.get("groupSealer")).toBeUndefined();

    await legacyParent.core.waitForSync();

    // Node2 (NOT a member of parent) creates child and extends legacy parent
    const legacyParentOnNode2 = await loadCoValueOrFail(
      node2.node,
      legacyParent.id,
    );
    const childGroup = node2.node.createGroup();
    childGroup.extend(legacyParentOnNode2);

    await childGroup.core.waitForSync();

    // Verify the legacy fallback was used: a writeKeyFor_ entry should exist
    // in the parent group for the extending account
    const legacyParentUpdated = await loadCoValueOrFail(
      node1.node,
      legacyParent.id,
    );

    const writeKeyForNode2 = legacyParentUpdated.get(
      `writeKeyFor_${node2.node.getCurrentAgent().id}` as any,
    );
    expect(writeKeyForNode2).toBeDefined();

    // Verify NO _sealedFor_ entries exist (groupSealer path was NOT used)
    const sealedForKeys = legacyParentUpdated
      .keys()
      .filter((key) => key.includes("_sealedFor_"));
    expect(sealedForKeys).toHaveLength(0);

    // Node3 is added as writeOnly to child by node2
    const account3OnNode2 = await loadCoValueOrFail(
      node2.node,
      node3.accountID,
    );
    childGroup.addMember(account3OnNode2, "writeOnly");

    await childGroup.core.waitForSync();

    // Node3 writes content
    const childGroupOnNode3 = await loadCoValueOrFail(
      node3.node,
      childGroup.id,
    );
    const map = childGroupOnNode3.createMap();
    map.set("test", "Written via legacy writeOnly fallback");

    await map.core.waitForSync();

    // Node1 (parent admin) should be able to read via the writeOnly key
    const mapOnNode1 = await loadCoValueOrFail(node1.node, map.id);
    expect(mapOnNode1.get("test")).toEqual(
      "Written via legacy writeOnly fallback",
    );
  });
});
