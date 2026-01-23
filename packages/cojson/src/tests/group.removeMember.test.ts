import { beforeEach, describe, expect, test } from "vitest";
import { setCoValueLoadingRetryDelay } from "../config.js";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  blockMessageTypeOnOutgoingPeer,
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils.js";
import { expectGroup } from "../typeUtils/expectGroup.js";

setCoValueLoadingRetryDelay(10);

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("Group.removeMember", () => {
  test("revoking a member access should not affect everyone access", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const alice = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    group.addMember("everyone", "writer");

    const aliceOnAdminNode = await loadCoValueOrFail(
      admin.node,
      alice.accountID,
    );
    group.addMember(aliceOnAdminNode, "writer");
    group.removeMember(aliceOnAdminNode);

    const groupOnAliceNode = await loadCoValueOrFail(alice.node, group.id);
    expect(groupOnAliceNode.myRole()).toEqual("writer");

    const map = groupOnAliceNode.createMap();

    map.set("test", "test");
    expect(map.get("test")).toEqual("test");
  });

  test("revoking a member access should not affect everyone access when everyone access is gained through a group extension", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const alice = await setupTestAccount({
      connected: true,
    });

    const parentGroup = admin.node.createGroup();
    const group = admin.node.createGroup();
    parentGroup.addMember("everyone", "reader");
    group.extend(parentGroup);

    const aliceOnAdminNode = await loadCoValueOrFail(
      admin.node,
      alice.accountID,
    );
    group.addMember(aliceOnAdminNode, "writer");
    group.removeMember(aliceOnAdminNode);

    const map = group.createMap();
    map.set("test", "test");

    const groupOnAliceNode = await loadCoValueOrFail(alice.node, group.id);
    expect(groupOnAliceNode.myRole()).toEqual("reader");

    const mapOnAliceNode = await loadCoValueOrFail(alice.node, map.id);
    expect(mapOnAliceNode.get("test")).toEqual("test");
  });

  for (const member of [
    "writer",
    "reader",
    "writeOnly",
    "manager",
    "admin",
  ] as const) {
    test(`${member} member should be able to revoke themselves`, async () => {
      const superAdmin = await setupTestAccount({
        connected: true,
      });

      const client = await setupTestAccount({
        connected: true,
      });

      const group = superAdmin.node.createGroup();
      group.addMember(
        await loadCoValueOrFail(superAdmin.node, client.accountID),
        member,
      );

      await group.core.waitForSync();

      const loadedGroup = await loadCoValueOrFail(client.node, group.id);
      expect(loadedGroup.myRole()).toEqual(member);

      loadedGroup.removeMember(client.node.expectCurrentAccount(member));

      expect(loadedGroup.myRole()).toEqual(undefined);

      await loadedGroup.core.waitForSync();
      await waitFor(() => {
        expect(group.roleOf(client.accountID)).toEqual(undefined);
      });
    });
  }

  for (const member of ["writer", "reader", "writeOnly"] as const) {
    test(`${member} member cannot remove other accounts`, async () => {
      const admin = await setupTestAccount({
        connected: true,
      });

      const manager = await setupTestAccount({
        connected: true,
      });

      const writer = await setupTestAccount({
        connected: true,
      });

      const writeOnly = await setupTestAccount({
        connected: true,
      });

      const reader = await setupTestAccount({
        connected: true,
      });

      const client = await setupTestAccount({
        connected: true,
      });

      const group = admin.node.createGroup();
      group.addMember(
        await loadCoValueOrFail(admin.node, manager.accountID),
        "manager",
      );
      group.addMember(
        await loadCoValueOrFail(admin.node, writer.accountID),
        "writer",
      );
      group.addMember(
        await loadCoValueOrFail(admin.node, reader.accountID),
        "reader",
      );
      group.addMember(
        await loadCoValueOrFail(admin.node, writeOnly.accountID),
        "writeOnly",
      );
      group.addMember(
        await loadCoValueOrFail(admin.node, client.accountID),
        member,
      );

      const loadedGroup = await loadCoValueOrFail(client.node, group.id);

      expect(async () => {
        loadedGroup.removeMember(
          await loadCoValueOrFail(client.node, reader.accountID),
        );
      }).rejects.toThrow(
        `Failed to revoke role to ${reader.accountID} (role of current account is ${member})`,
      );

      expect(async () => {
        loadedGroup.removeMember(
          await loadCoValueOrFail(client.node, writeOnly.accountID),
        );
      }).rejects.toThrow(
        `Failed to revoke role to ${writeOnly.accountID} (role of current account is ${member})`,
      );

      expect(async () => {
        loadedGroup.removeMember(
          await loadCoValueOrFail(client.node, writer.accountID),
        );
      }).rejects.toThrow(
        `Failed to revoke role to ${writer.accountID} (role of current account is ${member})`,
      );

      expect(async () => {
        loadedGroup.removeMember(
          await loadCoValueOrFail(client.node, admin.accountID),
        );
      }).rejects.toThrow(
        `Failed to revoke role to ${admin.accountID} (role of current account is ${member})`,
      );

      expect(async () => {
        loadedGroup.removeMember(
          await loadCoValueOrFail(client.node, manager.accountID),
        );
      }).rejects.toThrow(
        `Failed to revoke role to ${manager.accountID} (role of current account is ${member})`,
      );
      expect(loadedGroup.roleOf(reader.accountID)).toEqual("reader");
      expect(loadedGroup.roleOf(writer.accountID)).toEqual("writer");
      expect(loadedGroup.roleOf(writeOnly.accountID)).toEqual("writeOnly");
      expect(loadedGroup.roleOf(admin.accountID)).toEqual("admin");
      expect(loadedGroup.roleOf(manager.accountID)).toEqual("manager");

      await loadedGroup.core.waitForSync();

      expect((await loadCoValueOrFail(reader.node, group.id)).myRole()).toEqual(
        "reader",
      );
      expect((await loadCoValueOrFail(writer.node, group.id)).myRole()).toEqual(
        "writer",
      );
      expect(
        (await loadCoValueOrFail(writeOnly.node, group.id)).myRole(),
      ).toEqual("writeOnly");
      expect((await loadCoValueOrFail(admin.node, group.id)).myRole()).toEqual(
        "admin",
      );
      expect(
        (await loadCoValueOrFail(manager.node, group.id)).myRole(),
      ).toEqual("manager");
    });
  }

  test(`admin member cannot remove other admins`, async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const client = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    group.addMember(
      await loadCoValueOrFail(admin.node, client.accountID),
      "admin",
    );

    const loadedGroup = await loadCoValueOrFail(client.node, group.id);
    const adminOnClientNode = await loadCoValueOrFail(
      client.node,
      admin.accountID,
    );

    expect(() => {
      loadedGroup.removeMember(adminOnClientNode);
    }).toThrow(
      `Failed to revoke role to ${admin.accountID} (role of current account is admin)`,
    );

    expect(loadedGroup.roleOf(admin.accountID)).toEqual("admin");

    await loadedGroup.core.waitForSync();

    expect((await loadCoValueOrFail(admin.node, group.id)).myRole()).toEqual(
      "admin",
    );
  });

  test(`managers can remove other managers`, async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const client = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    // downgrade admin to manager
    group.addMember(
      await loadCoValueOrFail(admin.node, admin.accountID),
      "manager",
    );
    group.addMember(
      await loadCoValueOrFail(admin.node, client.accountID),
      "manager",
    );

    const loadedGroup = await loadCoValueOrFail(client.node, group.id);

    const adminOnClientNode = await loadCoValueOrFail(
      client.node,
      admin.accountID,
    );

    loadedGroup.removeMember(adminOnClientNode);

    expect(loadedGroup.roleOf(admin.accountID)).toEqual(undefined);
  });

  test("removing a member when inheriting a group where the user lacks read rights", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const childAdmin = await setupTestAccount({
      connected: true,
    });

    const reader = await setupTestAccount({
      connected: true,
    });

    const childAdminOnAdminNode = await loadCoValueOrFail(
      admin.node,
      childAdmin.accountID,
    );

    const readerOnAdminNode = await loadCoValueOrFail(
      admin.node,
      reader.accountID,
    );

    const group = admin.node.createGroup();

    const childGroup = admin.node.createGroup();
    childGroup.addMember(childAdminOnAdminNode, "admin");
    childGroup.addMember(readerOnAdminNode, "reader");

    childGroup.extend(group);

    const readerOnChildAdminNode = await loadCoValueOrFail(
      childAdmin.node,
      reader.accountID,
    );

    const childGroupOnChildAdminNode = await loadCoValueOrFail(
      childAdmin.node,
      childGroup.id,
    );

    await childGroupOnChildAdminNode.removeMember(readerOnChildAdminNode);

    expect(childGroupOnChildAdminNode.roleOf(reader.accountID)).toEqual(
      undefined,
    );
  });
});
