import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { Account, co, Group, Loaded, Ref } from "../internal";
import { z } from "../exports";
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

  describe("reader cannot create content", () => {
    it("should throw when a reader tries to create a CoMap", () => {
      const me = Account.getMe();
      const group = Group.create();
      group.addMember(me, "reader");
      expect(group.myRole()).toBe("reader");

      const TestMap = co.map({ val: z.string() });
      expect(() => {
        TestMap.create({ val: "test" }, { owner: group });
      }).toThrow("does not have write permissions");
    });

    it("should throw when a reader tries to create a CoList", () => {
      const me = Account.getMe();
      const group = Group.create();
      group.addMember(me, "reader");

      const TestList = co.list(z.string());
      expect(() => {
        TestList.create(["test"], { owner: group });
      }).toThrow("does not have write permissions");
    });

    it("should throw when a reader tries to create a CoFeed", () => {
      const me = Account.getMe();
      const group = Group.create();
      group.addMember(me, "reader");

      const TestFeed = co.feed(z.string());
      expect(() => {
        TestFeed.create(["test"], { owner: group });
      }).toThrow("does not have write permissions");
    });

    it("should throw when a reader tries to create nested inline CoMaps", () => {
      const me = Account.getMe();
      const group = Group.create();
      group.addMember(me, "reader");

      const Inner = co.map({ val: z.string() });
      const Outer = co.map({ inner: Inner });

      expect(() => {
        Outer.create({ inner: { val: "test" } }, { owner: group });
      }).toThrow("does not have write permissions");
    });

    it("should throw when a reader creates a CoMap referencing a pre-created value", () => {
      const me = Account.getMe();
      const readerGroup = Group.create();
      readerGroup.addMember(me, "reader");

      const writerGroup = Group.create();
      const Inner = co.map({ val: z.string() });
      const Outer = co.map({ inner: Inner });

      const inner = Inner.create({ val: "test" }, { owner: writerGroup });
      expect(() => {
        Outer.create({ inner }, { owner: readerGroup });
      }).toThrow("does not have write permissions");
    });

    it("should allow a writer to create content", async () => {
      const admin = await createJazzTestAccount();
      const me = Account.getMe();

      const group = Group.create({ owner: admin });
      group.addMember(me, "writer");

      const TestMap = co.map({ val: z.string() });
      const map = TestMap.create({ val: "test" }, { owner: group });
      expect(map.val).toBe("test");
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
