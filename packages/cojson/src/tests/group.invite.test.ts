import { beforeEach, describe, expect, test } from "vitest";
import { expectMap } from "../coValue.js";
import { LogLevel, logger } from "../logger.js";
import {
  SyncMessagesLog,
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils.js";
import { AccountRole } from "../permissions.js";

beforeEach(async () => {
  SyncMessagesLog.clear();
  setupTestNode({ isSyncServer: true });
});

// [fromRole, toRole, canCreateInvite]
const CAN_CREATE_INVITE_MATRIX: [AccountRole, AccountRole, boolean][] = [
  ["admin", "admin", true],
  ["admin", "manager", true],
  ["admin", "writer", true],
  ["admin", "reader", true],
  ["admin", "writeOnly", true],

  ["manager", "admin", false],
  ["manager", "manager", false],
  ["manager", "writer", true],
  ["manager", "reader", true],
  ["manager", "writeOnly", true],

  ["writer", "admin", false],
  ["writer", "manager", false],
  ["writer", "writer", false],
  ["writer", "reader", false],
  ["writer", "writeOnly", false],

  ["reader", "admin", false],
  ["reader", "manager", false],
  ["reader", "writer", false],
  ["reader", "reader", false],
  ["reader", "writeOnly", false],

  ["writeOnly", "admin", false],
  ["writeOnly", "manager", false],
  ["writeOnly", "writer", false],
  ["writeOnly", "reader", false],
  ["writeOnly", "writeOnly", false],
];

describe("Group invites", () => {
  for (const [fromRole, toRole, canCreateInvite] of CAN_CREATE_INVITE_MATRIX) {
    test(`${fromRole} should ${canCreateInvite ? "be able" : "not be able"} to create an invite for ${toRole}`, async () => {
      const admin = await setupTestAccount({
        connected: true,
      });

      const group = admin.node.createGroup();

      const adminOnAdminNode = await loadCoValueOrFail(
        admin.node,
        admin.accountID,
      );

      group.addMember(adminOnAdminNode, fromRole);

      if (canCreateInvite) {
        expect(group.createInvite(toRole)).toBeDefined();
      } else {
        expect(() => {
          group.createInvite(toRole);
        }).toThrow(`Failed to set role ${toRole}Invite to`);
      }
    });
  }

  test("should be able to accept a reader invite", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const newMember = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const inviteSecret = group.createInvite("reader");

    const personOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      person.id,
    );
    expect(personOnNewMemberNode.get("name")).toEqual(undefined);

    await newMember.node.acceptInvite(group.id, inviteSecret);

    await waitFor(() => {
      expect(
        expectMap(personOnNewMemberNode.core.getCurrentContent()).get("name"),
      ).toEqual("John Doe");
    });

    const groupOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      group.id,
    );

    expect(groupOnNewMemberNode.roleOf(newMember.accountID)).toEqual("reader");
  });

  test("should be able to accept a writer invite", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const newMember = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const inviteSecret = group.createInvite("writer");

    const personOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      person.id,
    );
    expect(personOnNewMemberNode.get("name")).toEqual(undefined);

    await newMember.node.acceptInvite(group.id, inviteSecret);

    await waitFor(() => {
      expect(
        expectMap(personOnNewMemberNode.core.getCurrentContent()).get("name"),
      ).toEqual("John Doe");
    });

    const groupOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      group.id,
    );

    expect(groupOnNewMemberNode.roleOf(newMember.accountID)).toEqual("writer");

    // Verify write access
    personOnNewMemberNode.set("name", "Jane Doe");
    expect(personOnNewMemberNode.get("name")).toEqual("Jane Doe");
  });

  test("should be able to accept a writeOnly invite", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const newMember = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const inviteSecret = group.createInvite("writeOnly");

    const personOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      person.id,
    );
    expect(personOnNewMemberNode.get("name")).toEqual(undefined);

    await newMember.node.acceptInvite(group.id, inviteSecret);

    const groupOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      group.id,
    );

    expect(groupOnNewMemberNode.roleOf(newMember.accountID)).toEqual(
      "writeOnly",
    );

    // Should not be able to read
    expect(personOnNewMemberNode.get("name")).toEqual(undefined);

    // Should be able to write
    personOnNewMemberNode.set("name", "Jane Doe");
    expect(personOnNewMemberNode.get("name")).toEqual("Jane Doe");
  });

  test("should be able to accept an admin invite", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const newMember = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    const inviteSecret = group.createInvite("admin");

    const personOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      person.id,
    );
    expect(personOnNewMemberNode.get("name")).toEqual(undefined);

    await newMember.node.acceptInvite(group.id, inviteSecret);

    await waitFor(() => {
      expect(
        expectMap(personOnNewMemberNode.core.getCurrentContent()).get("name"),
      ).toEqual("John Doe");
    });

    const groupOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      group.id,
    );

    expect(groupOnNewMemberNode.roleOf(newMember.accountID)).toEqual("admin");

    // Verify admin access by adding another member
    const reader = await setupTestAccount({
      connected: true,
    });

    const readerOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      reader.accountID,
    );
    groupOnNewMemberNode.addMember(readerOnNewMemberNode, "reader");

    const personOnReaderNode = await loadCoValueOrFail(reader.node, person.id);

    await waitFor(() => {
      expect(
        expectMap(personOnReaderNode.core.getCurrentContent()).get("name"),
      ).toEqual("John Doe");
    });
  });

  test("should be able to accept a manager invite", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const newMember = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();

    const person = group.createMap({
      name: "John Doe",
    });

    const inviteSecret = group.createInvite("manager");

    const personOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      person.id,
    );
    expect(personOnNewMemberNode.get("name")).toEqual(undefined);

    await newMember.node.acceptInvite(group.id, inviteSecret);

    await waitFor(() => {
      expect(
        expectMap(personOnNewMemberNode.core.getCurrentContent()).get("name"),
      ).toEqual("John Doe");
    });

    const groupOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      group.id,
    );

    expect(groupOnNewMemberNode.roleOf(newMember.accountID)).toEqual("manager");

    // Verify admin access by adding another member
    const reader = await setupTestAccount({
      connected: true,
    });

    const readerOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      reader.accountID,
    );
    groupOnNewMemberNode.addMember(readerOnNewMemberNode, "reader");

    const personOnReaderNode = await loadCoValueOrFail(reader.node, person.id);

    await waitFor(() => {
      expect(
        expectMap(personOnReaderNode.core.getCurrentContent()).get("name"),
      ).toEqual("John Doe");
    });
  });

  test("should not be able to accept an invite twice", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const newMember = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const inviteSecret = group.createInvite("reader");

    await newMember.node.acceptInvite(group.id, inviteSecret);

    const groupOnNewMemberNode = await loadCoValueOrFail(
      newMember.node,
      group.id,
    );

    expect(groupOnNewMemberNode.roleOf(newMember.accountID)).toEqual("reader");

    // Try to accept the same invite again
    await newMember.node.acceptInvite(group.id, inviteSecret);
    expect(groupOnNewMemberNode.roleOf(newMember.accountID)).toEqual("reader");
  });

  test("invites should not downgrade the role of an existing member", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const member = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    // First add member as admin
    const memberAccount = await loadCoValueOrFail(
      member.node,
      member.accountID,
    );
    group.addMember(memberAccount, "admin");

    // Create a reader invite
    const inviteSecret = group.createInvite("reader");

    // Try to accept the lower-privilege invite
    await member.node.acceptInvite(group.id, inviteSecret);

    const groupOnMemberNode = await loadCoValueOrFail(member.node, group.id);
    expect(groupOnMemberNode.roleOf(member.accountID)).toEqual("admin");
  });

  logger.setLevel(LogLevel.DEBUG);

  test("invites should be able to upgrade the role of an existing member", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const member = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();

    // First add member as reader
    const memberAccount = await loadCoValueOrFail(
      member.node,
      member.accountID,
    );
    group.addMember(memberAccount, "reader");

    // Create an admin invite
    const inviteSecret = group.createInvite("admin");

    const groupOnMemberNode = await loadCoValueOrFail(member.node, group.id);

    // Accept the higher-privilege invite
    await member.node.acceptInvite(groupOnMemberNode.id, inviteSecret);

    expect(groupOnMemberNode.roleOf(member.accountID)).toEqual("admin");

    // Verify admin access by adding another member
    const reader = await setupTestAccount({
      connected: true,
    });
    const readerAccount = await loadCoValueOrFail(
      member.node,
      reader.accountID,
    );
    groupOnMemberNode.addMember(readerAccount, "reader");
  });

  test("invites should work on revoked members", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const member = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const person = group.createMap({
      name: "John Doe",
    });

    // First add member as reader
    const memberAccount = await loadCoValueOrFail(
      member.node,
      member.accountID,
    );
    group.addMember(memberAccount, "reader");
    await group.removeMember(memberAccount);

    // Create a new reader invite
    const inviteSecret = group.createInvite("reader");

    const groupOnMemberNode = await loadCoValueOrFail(member.node, group.id);

    // Accept the invite after being revoked
    await member.node.acceptInvite(groupOnMemberNode.id, inviteSecret);

    expect(groupOnMemberNode.roleOf(member.accountID)).toEqual("reader");

    await waitFor(() => {
      expect(group.roleOf(member.accountID)).toEqual("reader");
    });

    // Verify read access is restored
    const personOnMemberNode = await loadCoValueOrFail(member.node, person.id);
    expect(personOnMemberNode.get("name")).toEqual("John Doe");
  });

  test("should not be able to accept an invalid invite", async () => {
    const admin = await setupTestAccount({
      connected: true,
    });

    const newMember = await setupTestAccount({
      connected: true,
    });

    const group = admin.node.createGroup();
    const invalidInvite = "inviteSecret_zinvalid";

    try {
      await newMember.node.acceptInvite(group.id, invalidInvite);
      throw new Error("Should not be able to accept invalid invite");
    } catch (e) {
      expect(e).toBeDefined();
    }
  });
});
