import { beforeAll, describe, expect, test } from "vitest";
import { Account, co, z, Group } from "../exports";
import { createJazzTestAccount, setupJazzTestSync } from "../testing";

describe("Schema.withPermissions()", () => {
  beforeAll(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  test("can define permissions on all CoValue schemas for concrete CoValue types", () => {
    const AllSchemas = [
      // co.plainText(),
      // co.richText(),
      // co.fileStream(),
      // co.vector(1),
      // co.list(co.plainText()),
      // co.feed(co.plainText()),
      co.map({ text: co.plainText() }),
      // co.record(z.string(), co.plainText()),
    ];

    for (const Schema of AllSchemas) {
      const SchemaWithPermissions = Schema.withPermissions({
        onInlineCreate: "extendsContainer",
      });
      expect(SchemaWithPermissions.permissions).toEqual({
        onInlineCreate: "extendsContainer",
      });
    }
  });

  test("cannot define permissions on co.group()", () => {
    expect(() =>
      co
        .group()
        // @ts-expect-error: Groups do not have permissions
        .withPermissions({ onInlineCreate: "extendsContainer" }),
    ).toThrow();
  });

  test("cannot define permissions on co.optional()", () => {
    expect(() =>
      co
        .optional(co.plainText())
        // @ts-expect-error: cannot create CoValues with co.optional schema
        .withPermissions({ onInlineCreate: "extendsContainer" }),
    ).toThrow();
  });

  test("cannot define permissions on co.discriminatedUnion()", () => {
    expect(() =>
      co
        .discriminatedUnion("type", [
          co.map({ type: z.literal("a") }),
          co.map({ type: z.literal("b") }),
        ])
        // @ts-expect-error: cannot create CoValues with co.discriminatedUnion schema
        .withPermissions({ onInlineCreate: "extendsContainer" }),
    ).toThrow();
  });

  describe("onInlineCreate defines how the owner is obtained when creating CoValues from JSON", () => {
    test("defaults to 'extendsContainer'", () => {
      const Schema = co.map({ name: co.plainText() });
      expect(Schema.permissions.onInlineCreate).toEqual("extendsContainer");
    });

    describe("extendsContainer", () => {
      test("creates a new group that includes the container CoValue's owner as a member", async () => {
        const TestMap = co
          .map({ name: co.plainText() })
          .withPermissions({ onInlineCreate: "extendsContainer" });
        const ParentMap = co.map({ child: TestMap });
        const me = Account.getMe();
        const anotherAccount = await Account.createAs(me, {
          creationProps: { name: "Another Account" },
        });

        const parentOwner = Group.create({ owner: me });
        parentOwner.addMember(anotherAccount, "writer");
        const parentMap = ParentMap.create(
          {
            child: { name: "Hello" },
          },
          { owner: parentOwner },
        );

        const childOwner = parentMap.child.$jazz.owner;
        expect(
          childOwner.getParentGroups().map((group) => group.$jazz.id),
        ).toContain(parentOwner.$jazz.id);
        expect(childOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual("writer");
      });

      test("allows overriding the role of the container CoValue's owner", async () => {
        const TestMap = co.map({ name: co.plainText() }).withPermissions({
          onInlineCreate: { extendsContainer: "reader" },
        });
        const ParentMap = co.map({ child: TestMap });
        const me = Account.getMe();
        const anotherAccount = await Account.createAs(me, {
          creationProps: { name: "Another Account" },
        });

        const parentOwner = Group.create({ owner: me });
        parentOwner.addMember(anotherAccount, "writer");
        const parentMap = ParentMap.create(
          {
            child: { name: "Hello" },
          },
          { owner: parentOwner },
        );

        const childOwner = parentMap.child.$jazz.owner;
        expect(
          childOwner.getParentGroups().map((group) => group.$jazz.id),
        ).toContain(parentOwner.$jazz.id);
        expect(parentOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual(
          "writer",
        );
        expect(childOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual("reader");
      });
    });

    describe("newGroup", () => {
      test("creates a new group with the active account as the admin", async () => {
        const TestMap = co
          .map({ name: co.plainText() })
          .withPermissions({ onInlineCreate: "newGroup" });
        const ParentMap = co.map({ child: TestMap });
        const me = Account.getMe();
        const anotherAccount = await Account.createAs(me, {
          creationProps: { name: "Another Account" },
        });

        const parentOwner = Group.create({ owner: me });
        parentOwner.addMember(anotherAccount, "writer");
        const parentMap = ParentMap.create(
          {
            child: { name: "Hello" },
          },
          { owner: parentOwner },
        );

        const childOwner = parentMap.child.$jazz.owner;
        expect(parentOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual(
          "writer",
        );
        expect(childOwner.getRoleOf(anotherAccount.$jazz.id)).not.toBeDefined();
        expect(childOwner.members.map((member) => member.account)).toEqual([
          co.account().getMe(),
        ]);
      });
    });

    describe("equalsContainer", () => {
      test("uses the container CoValue's owner as the new CoValue's owner", async () => {
        const TestMap = co
          .map({ name: co.plainText() })
          .withPermissions({ onInlineCreate: "equalsContainer" });
        const ParentMap = co.map({ child: TestMap });
        const me = Account.getMe();
        const anotherAccount = await Account.createAs(me, {
          creationProps: { name: "Another Account" },
        });

        const parentOwner = Group.create({ owner: me });
        parentOwner.addMember(anotherAccount, "writer");
        const parentMap = ParentMap.create(
          {
            child: { name: "Hello" },
          },
          { owner: parentOwner },
        );

        const childOwner = parentMap.child.$jazz.owner;
        expect(childOwner.$jazz.id).toEqual(parentOwner.$jazz.id);
      });
    });

    describe("group configuration callback", () => {
      test("creates a new group and configures it according to the callback", async () => {
        const me = Account.getMe();
        const anotherAccount = await Account.createAs(me, {
          creationProps: { name: "Another Account" },
        });
        const TestMap = co.map({ name: co.plainText() }).withPermissions({
          onInlineCreate(newGroup) {
            newGroup.addMember(anotherAccount, "writer");
          },
        });
        const ParentMap = co.map({ child: TestMap });

        const parentOwner = Group.create({ owner: me });
        const parentMap = ParentMap.create(
          {
            child: { name: "Hello" },
          },
          { owner: parentOwner },
        );

        const childOwner = parentMap.child.$jazz.owner;
        expect(childOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual("writer");
        expect(
          parentOwner.getRoleOf(anotherAccount.$jazz.id),
        ).not.toBeDefined();
      });

      test("can access the container's owner inside the callback", async () => {
        const me = Account.getMe();
        const anotherAccount = await Account.createAs(me, {
          creationProps: { name: "Another Account" },
        });
        const TestMap = co.map({ name: co.plainText() }).withPermissions({
          onInlineCreate(newGroup, { containerOwner }) {
            containerOwner.addMember(anotherAccount, "writer");
            newGroup.addMember(containerOwner);
          },
        });
        const ParentMap = co.map({ child: TestMap });

        const parentOwner = Group.create({ owner: me });
        const parentMap = ParentMap.create(
          {
            child: { name: "Hello" },
          },
          { owner: parentOwner },
        );

        const childOwner = parentMap.child.$jazz.owner;
        expect(childOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual("writer");
        expect(parentOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual(
          "writer",
        );
      });
    });
  });

  test("withPermissions() does not override previous schema configuration", () => {
    const TestMap = co.map({ name: co.plainText() }).resolved({ name: true });
    const TestMapWithName = TestMap.withPermissions({
      onInlineCreate: "extendsContainer",
    });
    expect(TestMapWithName.permissions).toEqual({
      onInlineCreate: "extendsContainer",
    });
  });
});
