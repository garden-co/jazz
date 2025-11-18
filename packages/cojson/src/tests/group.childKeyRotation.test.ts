import { assert, beforeEach, describe, expect, test } from "vitest";
import { setCoValueLoadingRetryDelay } from "../config.js";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils.js";
import { expectGroup } from "../typeUtils/expectGroup.js";
import { RawAccount } from "../exports.js";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

setCoValueLoadingRetryDelay(10);

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("Group.childKeyRotation", () => {
  let admin: Awaited<ReturnType<typeof setupTestAccount>>;
  let bob: Awaited<ReturnType<typeof setupTestAccount>>;
  let alice: Awaited<ReturnType<typeof setupTestAccount>>;
  let charlie: Awaited<ReturnType<typeof setupTestAccount>>;
  let aliceOnAdminNode: RawAccount;
  let bobOnAdminNode: RawAccount;
  let charlieOnAdminNode: RawAccount;

  beforeEach(async () => {
    admin = await setupTestAccount({
      connected: true,
    });
    alice = await setupTestAccount({
      connected: true,
    });
    bob = await setupTestAccount({
      connected: true,
    });
    charlie = await setupTestAccount({
      connected: true,
    });
    aliceOnAdminNode = await loadCoValueOrFail(admin.node, alice.accountID);
    bobOnAdminNode = await loadCoValueOrFail(admin.node, bob.accountID);
    charlieOnAdminNode = await loadCoValueOrFail(admin.node, charlie.accountID);
  });

  test("removing a member should rotate the readKey on available child groups", async () => {
    const group = admin.node.createGroup();
    const childGroup = admin.node.createGroup();
    group.addMember(aliceOnAdminNode, "reader");

    childGroup.extend(group);

    group.removeMember(aliceOnAdminNode);

    // ReadKey rotated, now alice can't read the map
    const map = childGroup.createMap();
    map.set("test", "Not readable by alice");

    await map.core.waitForSync();

    const mapOnAliceNode = await loadCoValueOrFail(alice.node, map.id);
    expect(mapOnAliceNode.get("test")).toBeUndefined();
  });

  test.skip("removing a member should rotate the readKey on unloaded child groups", async () => {
    const group = admin.node.createGroup();

    let childGroup = bob.node.createGroup();
    group.addMember(bobOnAdminNode, "reader");
    group.addMember(aliceOnAdminNode, "reader");

    const groupOnBobNode = await loadCoValueOrFail(bob.node, group.id);

    childGroup.extend(groupOnBobNode);

    await childGroup.core.waitForSync();

    group.removeMember(aliceOnAdminNode);

    // Spinning a new session for bob, to be sure to trigger the group migration
    // that handles the key rotation
    const newBobSession = await bob.spawnNewSession();

    const childGroupOnNewBobNode = await loadCoValueOrFail(
      newBobSession.node,
      childGroup.id,
    );

    // The key should be rotated at this point, so alice can't read the map
    const map = childGroupOnNewBobNode.createMap();
    map.set("test", "Not readable by alice");

    await map.core.waitForSync();

    const mapOnAliceNode = await loadCoValueOrFail(alice.node, map.id);
    expect(mapOnAliceNode.get("test")).toBeUndefined();
  });

  test.skip("removing a member on a large group should rotate the readKey on unloaded child group", async () => {
    const group = admin.node.createGroup();

    const childGroup = bob.node.createGroup();
    group.addMember(bobOnAdminNode, "reader");
    group.addMember(aliceOnAdminNode, "reader");

    const groupOnBobNode = await loadCoValueOrFail(bob.node, group.id);

    childGroup.extend(groupOnBobNode);

    await childGroup.core.waitForSync();

    // Disconnect the admin node to sync the content manually and simulate the delay of the last chunk
    admin.disconnect();

    // Make the group to become large enough to require multiple messages to be synced
    for (let i = 0; i < 100; i++) {
      // @ts-expect-error - test property is not part of the group shape
      group.set("test", "1".repeat(1024));
    }

    expect(group.core.verified.newContentSince(undefined)?.length).toBe(2);

    group.removeMember(aliceOnAdminNode);

    const content = group.core.verified.newContentSince(undefined);
    assert(content);
    const lastChunk = content.pop();
    assert(lastChunk);

    // Spinning a new session for bob, to be sure to trigger the group migration
    const newBobSession = await bob.spawnNewSession();

    for (const chunk of content) {
      newBobSession.node.syncManager.handleNewContent(chunk, "import");
    }

    const childGroupOnNewBobNode = await loadCoValueOrFail(
      newBobSession.node,
      childGroup.id,
    );

    newBobSession.node.syncManager.handleNewContent(lastChunk, "import");

    // The migration waits for the group to be completely downloaded
    await childGroupOnNewBobNode.core.waitForAsync((core) =>
      core.isCompletelyDownloaded(),
    );

    // The key should be rotated at this point, so alice can't read the map
    const map = childGroupOnNewBobNode.createMap();
    map.set("test", "Not readable by alice");

    await map.core.waitForSync();

    const mapOnAliceNode = await loadCoValueOrFail(alice.node, map.id);
    expect(mapOnAliceNode.get("test")).toBeUndefined();
  });

  test.skip("removing a member on a large parent group should rotate the readKey on unloaded grandChild group", async () => {
    const parentGroup = admin.node.createGroup();

    const group = bob.node.createGroup();
    const childGroup = bob.node.createGroup();
    parentGroup.addMember(bobOnAdminNode, "reader");
    parentGroup.addMember(aliceOnAdminNode, "reader");

    const parentGroupOnBobNode = await loadCoValueOrFail(
      bob.node,
      parentGroup.id,
    );

    group.extend(parentGroupOnBobNode);
    childGroup.extend(group);

    await childGroup.core.waitForSync();

    // Disconnect the admin node to sync the content manually and simulate the delay of the last chunk
    admin.disconnect();

    // Make the parent group to become large enough to require multiple messages to be synced
    for (let i = 0; i < 200; i++) {
      // @ts-expect-error - test property is not part of the group shape
      parentGroup.set("test", "1".repeat(1024));
    }

    expect(parentGroup.core.verified.newContentSince(undefined)?.length).toBe(
      3,
    );

    parentGroup.removeMember(aliceOnAdminNode);

    const content = parentGroup.core.verified.newContentSince(undefined);
    assert(content);
    const lastChunk = content.pop();
    assert(lastChunk);

    // Spinning a new session for bob, to be sure to trigger the group migration
    const newBobSession = await bob.spawnNewSession();

    for (const chunk of content) {
      newBobSession.node.syncManager.handleNewContent(chunk, "import");
    }

    const childGroupOnNewBobNode = await loadCoValueOrFail(
      newBobSession.node,
      childGroup.id,
    );

    newBobSession.node.syncManager.handleNewContent(lastChunk, "import");

    // The migration waits for the group to be completely downloaded, this includes full streaming of the parent group
    await childGroupOnNewBobNode.core.waitForAsync((core) =>
      core.isCompletelyDownloaded(),
    );

    // The key should be rotated at this point, so alice can't read the map
    const map = childGroupOnNewBobNode.createMap();
    map.set("test", "Not readable by alice");

    await map.core.waitForSync();

    const mapOnAliceNode = await loadCoValueOrFail(alice.node, map.id);
    expect(mapOnAliceNode.get("test")).toBeUndefined();
  });

  test.skip("non-admin accounts can't trigger the unloaded child group key rotation", async () => {
    const group = admin.node.createGroup();
    const childGroup = bob.node.createGroup();

    group.addMember(bobOnAdminNode, "writer");
    group.addMember(aliceOnAdminNode, "writer");
    group.addMember(charlieOnAdminNode, "writer");

    await group.core.waitForSync();

    const groupOnBobNode = await loadCoValueOrFail(bob.node, group.id);

    childGroup.extend(groupOnBobNode);

    await childGroup.core.waitForSync();
    await groupOnBobNode.core.waitForSync();

    group.removeMember(charlieOnAdminNode);

    await group.core.waitForSync();

    // Alice only have writer access to the child group, so she can't rotate the readKey
    const childGroupOnAliceNode = await loadCoValueOrFail(
      alice.node,
      childGroup.id,
    );

    const map = childGroupOnAliceNode.createMap();
    map.set("test", "Readable by charlie");

    await map.core.waitForSync();

    // This means that Charlie can read what Alice wrote
    const mapOnCharlieNode = await loadCoValueOrFail(charlie.node, map.id);
    expect(mapOnCharlieNode.get("test")).toBe("Readable by charlie");

    // Instead Bob is an admin, so when loading the child group he can rotate the readKey
    const newBobSession = await bob.spawnNewSession();
    const mapOnNewBobNode = await loadCoValueOrFail(newBobSession.node, map.id);

    mapOnNewBobNode.set("test", "Not readable by charlie");

    await mapOnNewBobNode.core.waitForSync();

    const updatedMapOnCharlieNode = await loadCoValueOrFail(
      charlie.node,
      map.id,
    );

    // Ensure that the map is fully synced
    await waitFor(async () => {
      expect(updatedMapOnCharlieNode.core.knownState()).toEqual(
        mapOnNewBobNode.core.knownState(),
      );
    });

    // Charlie should not be able to read what Bob wrote
    expect(updatedMapOnCharlieNode.get("test")).toBe("Readable by charlie");
  });

  test.skip("direct manager account can trigger the unloaded child group key rotation", async () => {
    const group = admin.node.createGroup();
    const childGroup = bob.node.createGroup();

    group.addMember(bobOnAdminNode, "writer");
    group.addMember(aliceOnAdminNode, "reader");

    await group.core.waitForSync();

    const groupOnBobNode = await loadCoValueOrFail(bob.node, group.id);

    childGroup.extend(groupOnBobNode);

    const charlieOnBobNode = await loadCoValueOrFail(
      bob.node,
      charlie.accountID,
    );

    childGroup.addMember(charlieOnBobNode, "manager");

    await childGroup.core.waitForSync();
    await groupOnBobNode.core.waitForSync();

    group.removeMember(aliceOnAdminNode);

    await group.core.waitForSync();

    const childGroupOnCharlieNode = await loadCoValueOrFail(
      charlie.node,
      childGroup.id,
    );

    const map = childGroupOnCharlieNode.createMap();
    map.set("test", "Not readable by alice");

    await map.core.waitForSync();

    const mapOnAliceNode = await loadCoValueOrFail(alice.node, map.id);

    // Ensure that the map is fully synced
    await waitFor(async () => {
      expect(mapOnAliceNode.core.knownState()).toEqual(map.core.knownState());
    });

    // Charlie should not be able to read what Bob wrote because key was rotated
    expect(mapOnAliceNode.get("test")).toBeUndefined();
  });

  test.skip("inherited admin account triggers the unloaded child group key rotation", async () => {
    const group = admin.node.createGroup();
    const childGroup = bob.node.createGroup();

    // Alice is admin on parent group (will inherit to child), but not directly on child
    group.addMember(aliceOnAdminNode, "admin");
    group.addMember(bobOnAdminNode, "writer");
    group.addMember(charlieOnAdminNode, "reader");

    await group.core.waitForSync();

    const groupOnBobNode = await loadCoValueOrFail(bob.node, group.id);

    childGroup.extend(groupOnBobNode);

    await childGroup.core.waitForSync();
    await groupOnBobNode.core.waitForSync();

    group.removeMember(charlieOnAdminNode);

    await group.core.waitForSync();

    // Alice has inherited admin access but is not a direct admin on the child group
    // So she can't trigger key rotation
    const childGroupOnNewAliceNode = await loadCoValueOrFail(
      alice.node,
      childGroup.id,
    );

    const map = childGroupOnNewAliceNode.createMap();
    map.set("test", "Readable by charlie");

    await map.core.waitForSync();

    // Charlie should be able to read what Alice wrote because key wasn't rotated
    const mapOnCharlieNode = await loadCoValueOrFail(charlie.node, map.id);
    expect(mapOnCharlieNode.get("test")).toBe(undefined);
  });

  // TODO: In this case the child can't detect the parent group rotation, because it doesn't have access to the parent group readKey
  // We need to replace the writeOnlyKey with an asymmetric key sealing mechanism to cover this case
  test.skip("removing a member should rotate the writeOnlyKey on child group", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const bob = await setupTestAccount({
      connected: true,
    });

    const alice = await setupTestAccount({
      connected: true,
    });

    const aliceOnAdminNode = await loadCoValueOrFail(
      admin.node,
      alice.accountID,
    );

    const group = admin.node.createGroup();
    const childGroup = bob.node.createGroup();

    group.addMember(aliceOnAdminNode, "reader");

    await group.core.waitForSync();

    const groupOnBobNode = await loadCoValueOrFail(bob.node, group.id);

    childGroup.extend(groupOnBobNode);

    await childGroup.core.waitForSync();

    group.removeMember(aliceOnAdminNode);

    await group.core.waitForSync();

    const newBobSession = await bob.spawnNewSession();
    const childGroupOnNewBobNode = await loadCoValueOrFail(
      newBobSession.node,
      childGroup.id,
    );

    const map = childGroupOnNewBobNode.createMap();
    map.set("test", "Not readable by alice");

    await map.core.waitForSync();

    const mapOnAliceNode = await loadCoValueOrFail(alice.node, map.id);
    expect(mapOnAliceNode.get("test")).toBeUndefined();
  });
});
