import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { Account, co, Group, Loaded, Ref } from "../internal";
import { createJazzTestAccount, setupJazzTestSync } from "../testing";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("Group", () => {
  it("should create a group", () => {
    const group = co.group().create();
    expect(group).toBeDefined();

    // Group methods are available , testing only for a few ones
    expect(group.addMember).toBeDefined();
    expect(group.removeMember).toBeDefined();
    expect(group.getRoleOf).toBeDefined();
    expect(group.makePublic).toBeDefined();
  });

  it("should make a group public", () => {
    const group = co.group().create();
    expect(group.getRoleOf("everyone")).toBeUndefined();

    group.makePublic();
    expect(group.getRoleOf("everyone")).toBe("reader");
  });

  describe("Invitations", () => {
    it("should create invitations as an instance method", () => {
      const group = co.group().create();
      const invite = group.$jazz.createInvite();
      expect(invite.startsWith("inviteSecret_")).toBeTruthy();
    });

    it("should create invitations as an static method", async () => {
      const group = co.group().create();
      const groupId = group.$jazz.id;
      const invite = await Group.createInvite(groupId);
      expect(invite.startsWith("inviteSecret_")).toBeTruthy();
    });

    it("should correctly create invitations for users of different roles", async () => {
      const currentUser = Account.getMe();
      const group = co.group().create();
      const invites = {
        reader: group.$jazz.createInvite("reader"),
        writeOnly: group.$jazz.createInvite("writeOnly"),
        writer: group.$jazz.createInvite("writer"),
        admin: group.$jazz.createInvite("admin"),
      };

      expect(group.getRoleOf(currentUser.$jazz.id)).toBe("admin");

      for (const [role, inviteSecret] of Object.entries(invites)) {
        const newUser = await createJazzTestAccount({
          isCurrentActiveAccount: true,
        });
        expect(group.getRoleOf(newUser.$jazz.id)).toBeUndefined();
        await newUser.acceptInvite(group.$jazz.id, inviteSecret);
        expect(group.getRoleOf(newUser.$jazz.id)).toBe(role);
      }
    });

    it("should create invitations with loadAs option", async () => {
      const group = co.group().create();
      const groupId = group.$jazz.id;

      const otherAccount = await createJazzTestAccount();
      group.addMember(otherAccount, "admin");

      const invite = await Group.createInvite(groupId, {
        role: "writer",
        loadAs: otherAccount,
      });

      expect(invite.startsWith("inviteSecret_")).toBeTruthy();
    });

    it("should create invitations via co.group() schema wrapper", async () => {
      const group = co.group().create();
      const groupId = group.$jazz.id;
      const invite = await co.group().createInvite(groupId, { role: "writer" });
      expect(invite.startsWith("inviteSecret_")).toBeTruthy();
    });
  });

  describe("TypeScript", () => {
    it("should correctly type the resolve query", async () => {
      const group = co.group().create();
      co.group().load(group.$jazz.id, {
        resolve: {},
      });
      co.group().load(group.$jazz.id, {
        resolve: true,
      });

      await expect(
        co.group().load(group.$jazz.id, {
          resolve: {
            // @ts-expect-error - members is not a valid resolve query
            members: {
              $each: true,
            },
          },
        }),
      ).rejects.toThrow();
    });

    it("should correctly type the create function", () => {
      const g = co.group();

      expectTypeOf(g.create).toBeCallableWith({ owner: Account.getMe() });
      expectTypeOf(g.create).toBeCallableWith(Account.getMe());
      expectTypeOf(g.create).toBeCallableWith(undefined);
    });

    it("should allow optional group fields in schemas", () => {
      const Schema = co.map({
        group: co.group().optional(),
      });

      const SchemaWithRequiredGroup = co.map({
        group: co.group(),
      });

      expectTypeOf(Schema.create).toBeCallableWith({});
      // @ts-expect-error - the group field is required
      expectTypeOf(SchemaWithRequiredGroup.create).toBeCallableWith({});
    });
  });
});
