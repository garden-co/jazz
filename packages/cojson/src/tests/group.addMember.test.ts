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
  // Super admin can grant any role to anyone
  ["superAdmin", undefined, "superAdmin", true],
  ["superAdmin", undefined, "admin", true],
  ["superAdmin", undefined, "writer", true],
  ["superAdmin", undefined, "reader", true],
  ["superAdmin", undefined, "writeOnly", true],

  // Super admin can grant any role to anyone
  ["superAdmin", "superAdmin", "superAdmin", true],
  ["superAdmin", "superAdmin", "admin", true],
  ["superAdmin", "superAdmin", "writer", true],
  ["superAdmin", "superAdmin", "reader", true],
  ["superAdmin", "superAdmin", "writeOnly", true],
  ["superAdmin", "admin", "superAdmin", true],
  ["superAdmin", "admin", "admin", true],
  ["superAdmin", "admin", "writer", true],
  ["superAdmin", "admin", "reader", true],
  ["superAdmin", "admin", "writeOnly", true],

  ["superAdmin", "writer", "superAdmin", true],
  ["superAdmin", "writer", "admin", true],
  ["superAdmin", "writer", "writer", true],
  ["superAdmin", "writer", "reader", true],
  ["superAdmin", "writer", "writeOnly", true],

  ["superAdmin", "reader", "superAdmin", true],
  ["superAdmin", "reader", "admin", true],
  ["superAdmin", "reader", "writer", true],
  ["superAdmin", "reader", "reader", true],
  ["superAdmin", "reader", "writeOnly", true],

  ["superAdmin", "writeOnly", "superAdmin", true],
  ["superAdmin", "writeOnly", "admin", true],
  ["superAdmin", "writeOnly", "writer", true],
  ["superAdmin", "writeOnly", "reader", true],
  ["superAdmin", "writeOnly", "writeOnly", true],

  // Admin can grant any role to anyone except super-admin
  ["admin", undefined, "superAdmin", false],
  ["admin", undefined, "admin", true],
  ["admin", undefined, "writer", true],
  ["admin", undefined, "reader", true],
  ["admin", undefined, "writeOnly", true],

  // Admin can't change other admins or super-admins
  ["admin", "superAdmin", "superAdmin", true],
  ["admin", "superAdmin", "admin", false],
  ["admin", "superAdmin", "writer", false],
  ["admin", "superAdmin", "reader", false],
  ["admin", "superAdmin", "writeOnly", false],

  ["admin", "admin", "superAdmin", false],
  ["admin", "admin", "admin", true],
  ["admin", "admin", "writer", false],
  ["admin", "admin", "reader", false],
  ["admin", "admin", "writeOnly", false],

  ["admin", "writer", "superAdmin", false],
  ["admin", "writer", "admin", true],
  ["admin", "writer", "writer", true],
  ["admin", "writer", "reader", true],
  ["admin", "writer", "writeOnly", true],

  ["admin", "reader", "superAdmin", false],
  ["admin", "reader", "admin", true],
  ["admin", "reader", "writer", true],
  ["admin", "reader", "reader", true],
  ["admin", "reader", "writeOnly", true],

  ["admin", "writeOnly", "superAdmin", false],
  ["admin", "writeOnly", "admin", true],
  ["admin", "writeOnly", "writer", true],
  ["admin", "writeOnly", "reader", true],
  ["admin", "writeOnly", "writeOnly", true],

  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writer", undefined, role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writer", "superAdmin", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writer", "admin", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writer", "writer", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writer", "reader", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writer", "writeOnly", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),

  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["reader", undefined, role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["reader", "superAdmin", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["reader", "admin", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["reader", "writer", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["reader", "reader", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["reader", "writeOnly", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),

  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writeOnly", undefined, role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writeOnly", "superAdmin", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writeOnly", "admin", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writeOnly", "writer", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writeOnly", "reader", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
  ...["superAdmin", "admin", "writer", "reader", "writeOnly"].map(
    (role) =>
      ["writeOnly", "writeOnly", role, false] as [
        Role,
        Role | undefined,
        Role,
        boolean,
      ],
  ),
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

      const group = source.node.createGroup(undefined, "superAdmin");

      // setup initial role for member
      if (from !== undefined) {
        group.addMember(memberOnSourceNode, from);
      }

      // downgrade super-admin if needed
      if (sourceRole !== "superAdmin") {
        group.addMember(
          await loadCoValueOrFail(source.node, source.accountID),
          sourceRole as Role,
        );

        expect(group.roleOf(source.accountID)).toEqual(sourceRole);
      }

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

  test.each(["superAdmin", "admin", "writer", "reader", "writeOnly"] as Role[])(
    "superAdmin should be able to self downgrade to %s role",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup(undefined, "superAdmin");

      group.addMember(memberOnSourceNode, role);

      expect(group.roleOf(source.accountID)).toEqual(role);
    },
  );

  test.each(["admin", "writer", "reader", "writeOnly"] as Role[])(
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

  test("admin should not be able to self upgrade to super-admin role", async () => {
    const source = await setupTestAccount({
      connected: true,
    });
    const memberOnSourceNode = await loadCoValueOrFail(
      source.node,
      source.accountID,
    );

    const group = source.node.createGroup();

    group.addMember(memberOnSourceNode, "admin");

    expect(group.roleOf(source.accountID)).toEqual("admin");

    expect(() => {
      group.addMember(memberOnSourceNode, "superAdmin");
    }).toThrow(
      `Failed to set role superAdmin to ${source.accountID} (role of current account is admin)`,
    );
  });

  test.each(["admin", "superAdmin", "reader", "writeOnly"] as Role[])(
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

  test.each(["admin", "superAdmin", "writer", "writeOnly"] as Role[])(
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

  test.each(["admin", "superAdmin"] as Role[])(
    "%s should be able to set writer role to EVERYONE",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup(undefined, "superAdmin");

      group.addMember(memberOnSourceNode, role);
      expect(group.roleOf(source.accountID)).toEqual(role);

      group.addMember("everyone", "writer");
      expect(group.roleOf("everyone")).toEqual("writer");
    },
  );

  test.each(["admin", "superAdmin"] as Role[])(
    "%s should be able to set writeOnly role to EVERYONE",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup(undefined, "superAdmin");

      group.addMember(memberOnSourceNode, role);
      expect(group.roleOf(source.accountID)).toEqual(role);

      group.addMember("everyone", "writeOnly");
      expect(group.roleOf("everyone")).toEqual("writeOnly");
    },
  );

  test.each(["admin", "superAdmin"] as Role[])(
    "%s should be able to set reader role to EVERYONE",
    async (role) => {
      const source = await setupTestAccount({
        connected: true,
      });
      const memberOnSourceNode = await loadCoValueOrFail(
        source.node,
        source.accountID,
      );

      const group = source.node.createGroup(undefined, "superAdmin");

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
