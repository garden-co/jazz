import { beforeEach, describe, expect, test } from "vitest";
import {
  SyncMessagesLog,
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
} from "./testUtils.js";
import { Role } from "../permissions.js";

beforeEach(async () => {
  SyncMessagesLog.clear();
  setupTestNode({ isSyncServer: true });
});

// [sourceRole, from, to, canGrant]
const GRANT_MATRIX: [Role, Role | undefined, Role, boolean][] = [
  // Admin can grant any role to anyone
  ["admin", undefined, "admin", true],
  ["admin", undefined, "manager", true],
  ["admin", undefined, "writer", true],
  ["admin", undefined, "reader", true],
  ["admin", undefined, "writeOnly", true],

  ["admin", "manager", "admin", true],
  ["admin", "manager", "manager", true],
  ["admin", "manager", "writer", true],
  ["admin", "manager", "reader", true],
  ["admin", "manager", "writeOnly", true],

  ["admin", "admin", "admin", true],
  ["admin", "admin", "manager", false],
  ["admin", "admin", "writer", false],
  ["admin", "admin", "reader", false],
  ["admin", "admin", "writeOnly", false],

  ["admin", "writer", "admin", true],
  ["admin", "writer", "manager", true],
  ["admin", "writer", "writer", true],
  ["admin", "writer", "reader", true],
  ["admin", "writer", "writeOnly", true],

  ["admin", "reader", "admin", true],
  ["admin", "reader", "manager", true],
  ["admin", "reader", "writer", true],
  ["admin", "reader", "reader", true],
  ["admin", "reader", "writeOnly", true],

  ["admin", "writeOnly", "admin", true],
  ["admin", "writeOnly", "manager", true],
  ["admin", "writeOnly", "writer", true],
  ["admin", "writeOnly", "reader", true],
  ["admin", "writeOnly", "writeOnly", true],

  // Manager can grant any role except admin
  // Manager can downgrade any role except admin
  ["manager", undefined, "admin", false],
  ["manager", undefined, "manager", true],
  ["manager", undefined, "writer", true],
  ["manager", undefined, "reader", true],
  ["manager", undefined, "writeOnly", true],

  ["manager", "manager", "admin", false],
  ["manager", "manager", "manager", true],
  ["manager", "manager", "writer", true],
  ["manager", "manager", "reader", true],
  ["manager", "manager", "writeOnly", true],

  ["manager", "admin", "admin", true],
  ["manager", "admin", "manager", false],
  ["manager", "admin", "writer", false],
  ["manager", "admin", "reader", false],
  ["manager", "admin", "writeOnly", false],

  ["manager", "writer", "admin", false],
  ["manager", "writer", "manager", true],
  ["manager", "writer", "writer", true],
  ["manager", "writer", "reader", true],
  ["manager", "writer", "writeOnly", true],

  ["manager", "reader", "admin", false],
  ["manager", "reader", "manager", true],
  ["manager", "reader", "writer", true],
  ["manager", "reader", "reader", true],
  ["manager", "reader", "writeOnly", true],

  ["manager", "writeOnly", "admin", false],
  ["manager", "writeOnly", "manager", true],
  ["manager", "writeOnly", "writer", true],
  ["manager", "writeOnly", "reader", true],
  ["manager", "writeOnly", "writeOnly", true],

  // Writer cannot grant any roles to anyone
  ["writer", undefined, "admin", false],
  ["writer", undefined, "manager", false],
  ["writer", undefined, "writer", false],
  ["writer", undefined, "reader", false],
  ["writer", undefined, "writeOnly", false],

  ["writer", "manager", "admin", false],
  ["writer", "manager", "manager", false],
  ["writer", "manager", "writer", false],
  ["writer", "manager", "reader", false],
  ["writer", "manager", "writeOnly", false],

  ["writer", "admin", "admin", false],
  ["writer", "admin", "manager", false],
  ["writer", "admin", "writer", false],
  ["writer", "admin", "reader", false],
  ["writer", "admin", "writeOnly", false],

  ["writer", "writer", "admin", false],
  ["writer", "writer", "manager", false],
  ["writer", "writer", "writer", false],
  ["writer", "writer", "reader", false],
  ["writer", "writer", "writeOnly", false],

  ["writer", "reader", "admin", false],
  ["writer", "reader", "manager", false],
  ["writer", "reader", "writer", false],
  ["writer", "reader", "reader", false],
  ["writer", "reader", "writeOnly", false],

  ["writer", "writeOnly", "manager", false],
  ["writer", "writeOnly", "admin", false],
  ["writer", "writeOnly", "writer", false],
  ["writer", "writeOnly", "reader", false],
  ["writer", "writeOnly", "writeOnly", false],

  // Reader cannot grant any roles to anyone
  ["reader", undefined, "admin", false],
  ["reader", undefined, "manager", false],
  ["reader", undefined, "writer", false],
  ["reader", undefined, "reader", false],
  ["reader", undefined, "writeOnly", false],

  ["reader", "manager", "admin", false],
  ["reader", "manager", "manager", false],
  ["reader", "manager", "writer", false],
  ["reader", "manager", "reader", false],
  ["reader", "manager", "writeOnly", false],

  ["reader", "admin", "admin", false],
  ["reader", "admin", "manager", false],
  ["reader", "admin", "writer", false],
  ["reader", "admin", "reader", false],
  ["reader", "admin", "writeOnly", false],

  ["reader", "writer", "admin", false],
  ["reader", "writer", "manager", false],
  ["reader", "writer", "writer", false],
  ["reader", "writer", "reader", false],
  ["reader", "writer", "writeOnly", false],

  ["reader", "reader", "admin", false],
  ["reader", "reader", "manager", false],
  ["reader", "reader", "writer", false],
  ["reader", "reader", "reader", false],
  ["reader", "reader", "writeOnly", false],

  ["reader", "writeOnly", "admin", false],
  ["reader", "writeOnly", "manager", false],
  ["reader", "writeOnly", "writer", false],
  ["reader", "writeOnly", "reader", false],
  ["reader", "writeOnly", "writeOnly", false],

  // WriteOnly cannot grant any roles to anyone, so spell all writeOnly combinations out:
  ["writeOnly", undefined, "admin", false],
  ["writeOnly", undefined, "manager", false],
  ["writeOnly", undefined, "writer", false],
  ["writeOnly", undefined, "reader", false],
  ["writeOnly", undefined, "writeOnly", false],

  ["writeOnly", "manager", "admin", false],
  ["writeOnly", "manager", "manager", false],
  ["writeOnly", "manager", "writer", false],
  ["writeOnly", "manager", "reader", false],
  ["writeOnly", "manager", "writeOnly", false],

  ["writeOnly", "admin", "admin", false],
  ["writeOnly", "admin", "manager", false],
  ["writeOnly", "admin", "writer", false],
  ["writeOnly", "admin", "reader", false],
  ["writeOnly", "admin", "writeOnly", false],

  ["writeOnly", "writer", "admin", false],
  ["writeOnly", "writer", "manager", false],
  ["writeOnly", "writer", "writer", false],
  ["writeOnly", "writer", "reader", false],
  ["writeOnly", "writer", "writeOnly", false],

  ["writeOnly", "reader", "admin", false],
  ["writeOnly", "reader", "manager", false],
  ["writeOnly", "reader", "writer", false],
  ["writeOnly", "reader", "reader", false],
  ["writeOnly", "reader", "writeOnly", false],

  ["writeOnly", "writeOnly", "admin", false],
  ["writeOnly", "writeOnly", "manager", false],
  ["writeOnly", "writeOnly", "writer", false],
  ["writeOnly", "writeOnly", "reader", false],
  ["writeOnly", "writeOnly", "writeOnly", false],
];

describe("Group.addMember", () => {
  for (const [sourceRole, from, to, canGrant] of GRANT_MATRIX) {
    test(`${sourceRole} should ${canGrant ? "be able" : "not be able"} to grant ${from !== undefined ? `${from} -> ` : ""}${to} role`, async () => {
      const source = await setupTestAccount({
        connected: true,
      });

      const member = await setupTestAccount({
        connected: true,
      });

      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        member.accountID,
      );

      const group = source.node.createGroup();

      // setup initial role for member
      if (from !== undefined) {
        group.addMember(memberOnSourceNode, from);
      }

      group.addMember(
        await loadCoValueOrFail(source.node, source.accountID),
        sourceRole as Role,
      );

      expect(group.roleOf(source.accountID)).toEqual(sourceRole);

      if (canGrant || from === to) {
        group.addMember(memberOnSourceNode, to);

        expect(group.roleOf(member.accountID)).toEqual(to);
      } else {
        expect(() => {
          group.addMember(memberOnSourceNode, to);
        }).toThrow(
          `Failed to set role ${to} to ${member.accountID} (role of current account is ${sourceRole})`,
        );

        expect(group.roleOf(member.accountID)).toEqual(from);
      }
    });
  }

  test.each(["admin", "manager", "writer", "reader", "writeOnly"] as Role[])(
    "admin should be able to self downgrade to %s role",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup();

      group.addMember(memberOnSourceNode, role);

      expect(group.roleOf(source.accountID)).toEqual(role);
    },
  );

  test.each(["admin", "manager", "reader", "writeOnly"] as Role[])(
    "writer should not be able to self upgrade to %s role",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup();

      group.addMember(memberOnSourceNode, "writer");

      expect(group.roleOf(source.accountID)).toEqual("writer");

      expect(() => {
        group.addMember(memberOnSourceNode, role);
      }).toThrow(
        `Failed to set role ${role} to ${source.accountID} (role of current account is writer)`,
      );
    },
  );

  test.each(["admin", "manager", "writer", "writeOnly"] as Role[])(
    "reader should not be able to self upgrade to %s role",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup();

      group.addMember(memberOnSourceNode, "reader");

      expect(group.roleOf(source.accountID)).toEqual("reader");

      expect(() => {
        group.addMember(memberOnSourceNode, role);
      }).toThrow(
        `Failed to set role ${role} to ${source.accountID} (role of current account is reader)`,
      );
    },
  );

  test.each(["admin", "superAdmin", "writer", "reader"] as Role[])(
    "writeOnly should not be able to self upgrade to %s role",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup();

      group.addMember(memberOnSourceNode, "writeOnly");

      expect(group.roleOf(source.accountID)).toEqual("writeOnly");

      expect(() => {
        group.addMember(memberOnSourceNode, role);
      }).toThrow(
        `Failed to set role ${role} to ${source.accountID} (role of current account is writeOnly)`,
      );
    },
  );

  test.each(["admin"] as Role[])(
    "%s should be able to set writer role to EVERYONE",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup();

      group.addMember(memberOnSourceNode, role);
      expect(group.roleOf(source.accountID)).toEqual(role);

      group.addMember("everyone", "writer");
      expect(group.roleOf("everyone")).toEqual("writer");
    },
  );

  test.each(["admin"] as Role[])(
    "%s should be able to set writeOnly role to EVERYONE",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup();

      group.addMember(memberOnSourceNode, role);
      expect(group.roleOf(source.accountID)).toEqual(role);

      group.addMember("everyone", "writeOnly");
      expect(group.roleOf("everyone")).toEqual("writeOnly");
    },
  );

  test.each(["admin", "manager"] as Role[])(
    "%s should be able to set reader role to EVERYONE",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup();

      group.addMember(memberOnSourceNode, role);
      expect(group.roleOf(source.accountID)).toEqual(role);

      group.addMember("everyone", "reader");
      expect(group.roleOf("everyone")).toEqual("reader");
    },
  );

  test.each(["writer", "reader", "writeOnly"] as Role[])(
    "%s should not be able to set any role to EVERYONE",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup();

      group.addMember(memberOnSourceNode, role);
      expect(group.roleOf(source.accountID)).toEqual(role);

      expect(() => {
        group.addMember("everyone", "writer");
      }).toThrow(
        `Failed to set role writer to everyone (role of current account is ${role})`,
      );

      expect(group.roleOf("everyone")).toEqual(undefined);

      expect(() => {
        group.addMember("everyone", "reader");
      }).toThrow(
        `Failed to set role reader to everyone (role of current account is ${role})`,
      );

      expect(group.roleOf("everyone")).toEqual(undefined);

      expect(() => {
        group.addMember("everyone", "writeOnly");
      }).toThrow(
        `Failed to set role writeOnly to everyone (role of current account is ${role})`,
      );

      expect(group.roleOf("everyone")).toEqual(undefined);
    },
  );

  test("an admin should be able downgrade a reader to writeOnly", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const reader = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();

    const readerOnAdminNode = await loadCoValueOrFail(
      admin.node,
      reader.accountID,
    );
    group.addMember(readerOnAdminNode, "reader");
    group.addMember(readerOnAdminNode, "writeOnly");

    expect(group.roleOf(reader.accountID)).toEqual("writeOnly");

    const person = group.createMap({
      name: "John Doe",
    });

    // Verify reader can read
    const personOnReaderNode = await loadCoValueOrFail(reader.node, person.id);

    expect(personOnReaderNode.get("name")).toEqual(undefined);
  });
});
