import { assert, beforeAll, describe, expect, test } from "vitest";
import { Account, co, CoValueLoadingState, Group, z } from "../exports";
import {
  assertLoaded,
  createJazzTestAccount,
  setupJazzTestSync,
} from "../testing";

describe("Schema.withPermissions()", () => {
  let me: Account;
  let anotherAccount: Account;
  beforeAll(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });

    me = Account.getMe();
    anotherAccount = await Account.createAs(Account.getMe(), {
      creationProps: { name: "Another Account" },
    });
  });

  test("can define permissions on all CoValue schemas for concrete CoValue types", () => {
    const AllSchemas = [
      co.plainText(),
      co.richText(),
      co.fileStream(),
      co.vector(1),
      co.list(co.plainText()),
      co.feed(co.plainText()),
      co.map({ text: co.plainText() }),
      co.record(z.string(), co.plainText()),
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

  test("cannot define permissions on co.account()", () => {
    expect(() =>
      co
        .account()
        // @ts-expect-error: Accounts do not have permissions
        .withPermissions({ onInlineCreate: "extendsContainer" }),
    ).toThrow();
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

  describe("onCreate", () => {
    describe("allows configuring a CoValue's group when using .create() without providing an explicit owner", () => {
      test("for CoMap", async () => {
        const TestMap = co.map({ name: co.plainText() }).withPermissions({
          onCreate(newGroup) {
            newGroup.makePublic();
          },
        });

        const map = TestMap.create({ name: "Hi!" });

        const loadedMap = await TestMap.load(map.$jazz.id, {
          resolve: { name: true },
          loadAs: anotherAccount,
        });
        assertLoaded(loadedMap);
        expect(loadedMap.name.toString()).toEqual("Hi!");
      });

      test("for CoList", async () => {
        const TestList = co.list(z.string()).withPermissions({
          onCreate(newGroup) {
            newGroup.makePublic();
          },
        });

        const list = TestList.create(["a", "b", "c"]);

        const loadedList = await TestList.load(list.$jazz.id, {
          loadAs: anotherAccount,
        });
        assertLoaded(loadedList);
        expect(loadedList).toEqual(["a", "b", "c"]);
      });

      test("for CoFeed", async () => {
        const TestFeed = co.feed(z.string()).withPermissions({
          onCreate(newGroup) {
            newGroup.makePublic();
          },
        });

        const feed = TestFeed.create(["a", "b", "c"]);

        const loadedFeed = await TestFeed.load(feed.$jazz.id, {
          loadAs: anotherAccount,
        });
        assertLoaded(loadedFeed);
        expect(loadedFeed.perAccount[me.$jazz.id]?.value).toEqual("c");
      });

      test("for CoPlainText", async () => {
        const TestPlainText = co.plainText().withPermissions({
          onCreate(newGroup) {
            newGroup.makePublic();
          },
        });

        const plainText = TestPlainText.create("Hello");

        const loadedPlainText = await TestPlainText.load(plainText.$jazz.id, {
          loadAs: anotherAccount,
        });
        assertLoaded(loadedPlainText);
        expect(loadedPlainText.toString()).toEqual("Hello");
      });

      test("for CoRichText", async () => {
        const TestRichText = co.richText().withPermissions({
          onCreate(newGroup) {
            newGroup.makePublic();
          },
        });

        const richText = TestRichText.create("Hello");

        const loadedRichText = await TestRichText.load(richText.$jazz.id, {
          loadAs: anotherAccount,
        });
        assertLoaded(loadedRichText);
        expect(loadedRichText.toString()).toEqual("Hello");
      });

      describe("for FileStream", async () => {
        test(".create()", async () => {
          const TestFileStream = co.fileStream().withPermissions({
            onCreate(newGroup) {
              newGroup.makePublic();
            },
          });

          const fileStream = TestFileStream.create();
          fileStream.start({ mimeType: "text/plain" });
          fileStream.end();

          const loadedFileStream = await TestFileStream.load(
            fileStream.$jazz.id,
            {
              loadAs: anotherAccount,
            },
          );
          assertLoaded(loadedFileStream);
          expect(loadedFileStream.getMetadata()).toEqual({
            mimeType: "text/plain",
          });
        });

        test(".createFromBlob()", async () => {
          const TestFileStream = co.fileStream().withPermissions({
            onCreate(newGroup) {
              newGroup.makePublic();
            },
          });

          const blob = new Blob(["test"], { type: "text/plain" });
          const fileStream = await TestFileStream.createFromBlob(blob);

          const loadedFileStream = await TestFileStream.load(
            fileStream.$jazz.id,
            {
              loadAs: anotherAccount,
            },
          );
          assertLoaded(loadedFileStream);
          expect(loadedFileStream.getMetadata()).toEqual({
            mimeType: "text/plain",
            totalSizeBytes: 4,
          });
        });

        test(".createFromArrayBuffer()", async () => {
          const TestFileStream = co.fileStream().withPermissions({
            onCreate(newGroup) {
              newGroup.makePublic();
            },
          });

          const arrayBuffer = new TextEncoder().encode("test").buffer;
          const fileStream = await TestFileStream.createFromArrayBuffer(
            arrayBuffer,
            "text/plain",
            "filename",
          );

          const loadedFileStream = await TestFileStream.load(
            fileStream.$jazz.id,
            {
              loadAs: anotherAccount,
            },
          );
          assertLoaded(loadedFileStream);
          expect(loadedFileStream.getMetadata()).toEqual({
            mimeType: "text/plain",
            totalSizeBytes: 4,
            fileName: "filename",
          });
        });
      });

      test("for CoVector", async () => {
        const TestVector = co.vector(1).withPermissions({
          onCreate(newGroup) {
            newGroup.makePublic();
          },
        });

        const vector = TestVector.create([1]);

        const loadedVector = await TestVector.load(vector.$jazz.id, {
          loadAs: anotherAccount,
        });
        assertLoaded(loadedVector);
        expect(loadedVector.toJSON()).toEqual([1]);
      });
    });

    test("configuration callback is not run when providing an explicit owner", async () => {
      const TestMap = co.map({ name: co.plainText() }).withPermissions({
        onCreate(newGroup) {
          newGroup.makePublic();
        },
      });
      const map = TestMap.create({ name: "Hi!" }, { owner: Group.create() });

      const loadedMap = await TestMap.load(map.$jazz.id, {
        resolve: { name: true },
        loadAs: anotherAccount,
      });
      expect(loadedMap.$isLoaded).toBe(false);
      expect(loadedMap.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });
  });

  describe("onInlineCreate defines how the owner is obtained when creating CoValues from JSON", () => {
    test("defaults to 'extendsContainer'", () => {
      const Schema = co.map({ name: co.plainText() });
      expect(Schema.permissions.onInlineCreate).toEqual("extendsContainer");
    });

    describe("extendsContainer", () => {
      test("creates a new group that includes the container CoValue's owner as a member", async () => {
        const TestMap = co.map({
          name: co
            .plainText()
            .withPermissions({ onInlineCreate: "extendsContainer" }),
        });

        const parentOwner = Group.create({ owner: me });
        parentOwner.addMember(anotherAccount, "writer");
        const map = TestMap.create({ name: "Hello" }, { owner: parentOwner });

        const childOwner = map.name.$jazz.owner;
        expect(
          childOwner.getParentGroups().map((group) => group.$jazz.id),
        ).toContain(parentOwner.$jazz.id);
        expect(childOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual("writer");
      });

      test("allows overriding the role of the container CoValue's owner", async () => {
        const TestMap = co.map({
          name: co.plainText().withPermissions({
            onInlineCreate: { extendsContainer: "reader" },
          }),
        });

        const parentOwner = Group.create({ owner: me });
        parentOwner.addMember(anotherAccount, "writer");
        const map = TestMap.create({ name: "Hello" }, { owner: parentOwner });

        const childOwner = map.name.$jazz.owner;
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
        const TestMap = co.map({
          name: co.plainText().withPermissions({ onInlineCreate: "newGroup" }),
        });

        const parentOwner = Group.create({ owner: me });
        parentOwner.addMember(anotherAccount, "writer");
        const map = TestMap.create({ name: "Hello" }, { owner: parentOwner });

        const childOwner = map.name.$jazz.owner;
        expect(parentOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual(
          "writer",
        );
        expect(childOwner.getRoleOf(anotherAccount.$jazz.id)).not.toBeDefined();
        expect(childOwner.members.map((member) => member.account)).toEqual([
          co.account().getMe(),
        ]);
      });
    });

    describe("sameAsContainer", () => {
      test("uses the container CoValue's owner as the new CoValue's owner", async () => {
        const TestMap = co.map({
          name: co
            .plainText()
            .withPermissions({ onInlineCreate: "sameAsContainer" }),
        });

        const parentOwner = Group.create({ owner: me });
        const map = TestMap.create({ name: "Hello" }, { owner: parentOwner });
        const childOwner = map.name.$jazz.owner;
        expect(childOwner.$jazz.id).toEqual(parentOwner.$jazz.id);
      });
    });

    describe("group configuration callback", () => {
      test("creates a new group and configures it according to the callback", async () => {
        const TestMap = co.map({
          name: co.plainText().withPermissions({
            onInlineCreate(newGroup) {
              newGroup.addMember(anotherAccount, "writer");
            },
          }),
        });

        const parentOwner = Group.create({ owner: me });
        const map = TestMap.create({ name: "Hello" }, { owner: parentOwner });

        const childOwner = map.name.$jazz.owner;
        expect(childOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual("writer");
        expect(
          parentOwner.getRoleOf(anotherAccount.$jazz.id),
        ).not.toBeDefined();
      });

      test("can access the container's owner inside the callback", async () => {
        const TestMap = co.map({
          name: co.plainText().withPermissions({
            onInlineCreate(newGroup, { containerOwner }) {
              containerOwner.addMember(anotherAccount, "writer");
              newGroup.addMember(containerOwner);
            },
          }),
        });

        const parentOwner = Group.create({ owner: me });
        const map = TestMap.create({ name: "Hello" }, { owner: parentOwner });

        const childOwner = map.name.$jazz.owner;
        expect(childOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual("writer");
        expect(parentOwner.getRoleOf(anotherAccount.$jazz.id)).toEqual(
          "writer",
        );
      });
    });

    test("when setting new properties on a CoValue", async () => {
      const TestMap = co.map({
        name: co
          .plainText()
          .withPermissions({ onInlineCreate: "sameAsContainer" }),
      });

      const parentOwner = Group.create({ owner: me });
      const map = TestMap.create({ name: "Hello" }, { owner: parentOwner });
      map.$jazz.set("name", "Hi!");

      const childOwner = map.name.$jazz.owner;
      expect(childOwner.$jazz.id).toEqual(parentOwner.$jazz.id);
    });

    test("for CoList container", async () => {
      const TestList = co.list(
        co.plainText().withPermissions({
          onInlineCreate: "sameAsContainer",
        }),
      );

      const parentOwner = Group.create({ owner: me });
      parentOwner.addMember(anotherAccount, "writer");
      const list = TestList.create(["Hello"], {
        owner: parentOwner,
      });

      const childOwner = list[0]?.$jazz.owner;
      expect(childOwner?.$jazz.id).toEqual(parentOwner.$jazz.id);
    });

    test("for CoFeed container", async () => {
      const TestFeed = co.feed(
        co.plainText().withPermissions({
          onInlineCreate: "sameAsContainer",
        }),
      );

      const parentOwner = Group.create({ owner: me });
      parentOwner.addMember(anotherAccount, "writer");
      const feed = TestFeed.create(["Hello"], { owner: parentOwner });

      const childCoValue = feed.inCurrentSession?.value;
      assert(childCoValue);
      assertLoaded(childCoValue);
      const childOwner = childCoValue.$jazz.owner;
      expect(childOwner?.$jazz.id).toEqual(parentOwner.$jazz.id);
    });
  });

  test("withPermissions() can be used with recursive schemas", () => {
    const Person = co.map({
      name: z.string(),
      get friend(): co.List<typeof Person> {
        return Friends;
      },
    });
    const Friends = co.list(Person).withPermissions({
      onInlineCreate: "sameAsContainer",
    });
    const person = Person.create({ name: "John", friend: [] });

    expect(person.friend.$jazz.owner).toEqual(person.$jazz.owner);
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
