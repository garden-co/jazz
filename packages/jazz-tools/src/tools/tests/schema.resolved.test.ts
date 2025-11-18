import {
  assert,
  beforeAll,
  describe,
  expect,
  expectTypeOf,
  test,
} from "vitest";
import {
  Account,
  co,
  CoPlainText,
  CoValueLoadingState,
  Group,
  z,
} from "../exports";
import { createJazzTestAccount, setupJazzTestSync } from "../testing";
import { assertLoaded, setupTwoNodes, waitFor } from "./utils";

describe("Schema.resolved()", () => {
  beforeAll(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  test("all CoValue schemas have a `true` resolve query by default", () => {
    const AllSchemas = [
      co.plainText(),
      co.richText(),
      co.fileStream(),
      co.vector(1),
      co.group(),
      co.list(co.plainText()),
      co.feed(co.plainText()),
      co.map({ text: co.plainText() }),
      co.record(z.string(), co.plainText()),
      co.optional(co.plainText()),
      co.discriminatedUnion("type", [
        co.map({ type: z.literal("a") }),
        co.map({ type: z.literal("b") }),
      ]),
    ];

    for (const Schema of AllSchemas) {
      expect(Schema.resolveQuery).toBe(true);
    }
  });

  describe("allows adding a default resolve query", () => {
    test("to a CoMap schema", () => {
      const TestMap = co.map({
        name: co.plainText(),
      });

      const TestMapWithName = TestMap.resolved({
        name: true,
      });

      expect(TestMapWithName.resolveQuery).toEqual({
        name: true,
      });
    });

    test("to a CoRecord schema", () => {
      const TestRecord = co.record(z.string(), co.plainText());

      const TestRecordWithName = TestRecord.resolved({
        name: true,
      });

      expect(TestRecordWithName.resolveQuery).toEqual({
        name: true,
      });
    });

    test("to a CoList schema", () => {
      const TestList = co.list(co.plainText());

      const TestListWithItems = TestList.resolved({
        $each: true,
      });

      expect(TestListWithItems.resolveQuery).toEqual({
        $each: true,
      });
    });

    test("to a CoFeed schema", () => {
      const TestFeed = co.feed(co.plainText());

      const TestFeedWithItems = TestFeed.resolved({
        $each: true,
      });

      expect(TestFeedWithItems.resolveQuery).toEqual({
        $each: true,
      });
    });

    test("to an Account schema", () => {
      const TestAccount = co.account();

      const TestAccountWithName = TestAccount.resolved({
        profile: true,
      });

      expect(TestAccountWithName.resolveQuery).toEqual({
        profile: true,
      });
    });

    test("to a DiscriminatedUnion schema", () => {
      const Pet = co.discriminatedUnion("type", [
        co.map({
          type: z.literal("dog"),
          name: co.plainText(),
        }),
        co.map({
          type: z.literal("cat"),
          name: co.plainText(),
        }),
      ]);

      const PetWithName = Pet.resolved({
        name: true,
      });

      expect(PetWithName.resolveQuery).toEqual({
        name: true,
      });
    });
  });

  describe("the schema's resolve query is used when loading CoValues", () => {
    let clientAccount: Account;
    let serverAccount: Account;
    let publicGroup: Group;

    beforeAll(async () => {
      ({ clientAccount, serverAccount } = await setupTwoNodes());
      publicGroup = Group.create(serverAccount).makePublic("writer");
    });

    describe("on load()", () => {
      test("for CoMap", async () => {
        const TestMap = co.map({
          name: co.plainText(),
        });

        const TestMapWithName = TestMap.resolved({
          name: true,
        });

        const map = TestMapWithName.create(
          {
            name: "Test",
          },
          publicGroup,
        );

        const loadedMap = await TestMapWithName.load(map.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(loadedMap);
        expect(loadedMap.name.$isLoaded).toBe(true);
        expect(loadedMap.name.toUpperCase()).toBe("TEST");
      });

      test("for CoRecord", async () => {
        const TestRecord = co.record(z.string(), co.plainText());

        const TestRecordWithName = TestRecord.resolved({
          name: true,
        });

        const record = TestRecordWithName.create(
          {
            name: "Test",
          },
          publicGroup,
        );

        const loadedRecord = await TestRecordWithName.load(record.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(loadedRecord);
        expect(loadedRecord.name?.$isLoaded).toBe(true);
        expect(loadedRecord.name?.toUpperCase()).toBe("TEST");
      });

      test("for CoList", async () => {
        const TestList = co.list(co.plainText());

        const TestListWithItems = TestList.resolved({
          $each: true,
        });

        const list = TestListWithItems.create(["Test", "Test2"], publicGroup);

        const loadedList = await TestListWithItems.load(list.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(loadedList);
        expect(loadedList.length).toBe(2);
        expect(loadedList[0]?.$isLoaded).toBe(true);
        expect(loadedList[0]?.toUpperCase()).toBe("TEST");
      });

      // TODO fix - `$each` does not load nested CoValues when providing an explicit resolve query either:
      // TestFeed.load(feed.$jazz.id, {
      //   loadAs: clientAccount,
      //   resolve: {
      //     $each: true,
      //   },
      // })
      test.skip("for CoFeed", async () => {
        const TestFeed = co.feed(co.plainText());

        const TestFeedWithItems = TestFeed.resolved({
          $each: true,
        });

        const feed = TestFeedWithItems.create(["Test"], publicGroup);

        const loadedFeed = await TestFeedWithItems.load(feed.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(loadedFeed);
        expect(loadedFeed.inCurrentSession?.value.$jazz.loadingState).toBe(
          CoValueLoadingState.LOADED,
        );
        expect(loadedFeed.inCurrentSession?.value.toUpperCase()).toEqual(
          "HELLO",
        );
      });

      test("for Account", async () => {
        const AccountWithProfile = co.account().resolved({
          profile: true,
        });

        const account = await AccountWithProfile.createAs(serverAccount, {
          creationProps: { name: "Hermes Puggington" },
        });
        account.$jazz.set(
          "profile",
          co.profile().create({ name: "Hermes Puggington" }, publicGroup),
        );

        const loadedAccount = await AccountWithProfile.load(account.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(loadedAccount);
        expect(loadedAccount.profile.$isLoaded).toBe(true);
        expect(loadedAccount.profile.name).toBe("Hermes Puggington");
      });

      test("for DiscriminatedUnion", async () => {
        const Person = co.map({
          name: co.plainText(),
        });
        const Dog = co.map({
          type: z.literal("dog"),
          name: co.plainText(),
          owner: Person,
        });
        const Cat = co.map({
          type: z.literal("cat"),
          name: co.plainText(),
        });
        const Pet = co.discriminatedUnion("type", [Dog, Cat]).resolved({
          name: true,
          owner: {
            name: true,
          },
        });

        const dog = Dog.create(
          {
            type: "dog",
            name: "Rex",
            owner: { name: "Lewis" },
          },
          publicGroup,
        );

        const loadedDiscriminatedUnion = await Pet.load(dog.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(loadedDiscriminatedUnion);
        expect(loadedDiscriminatedUnion.name.$isLoaded).toBe(true);
        expect(loadedDiscriminatedUnion.name.toUpperCase()).toBe("REX");

        assert(loadedDiscriminatedUnion.type === "dog");
        expect(loadedDiscriminatedUnion.owner.name.toUpperCase()).toBe("LEWIS");
      });
    });

    describe("on subscribe()", () => {
      test("for CoMap", async () => {
        const TestMap = co.map({ name: co.plainText() });

        const TestMapWithName = TestMap.resolved({ name: true });

        const map = TestMapWithName.create({ name: "Test" }, publicGroup);

        const updates: co.loaded<typeof TestMapWithName>[] = [];
        TestMapWithName.subscribe(
          map.$jazz.id,
          {
            loadAs: clientAccount,
          },
          (map) => {
            expectTypeOf<typeof map.name>().toEqualTypeOf<CoPlainText>();
            updates.push(map);
          },
        );

        await waitFor(() => expect(updates.length).toBe(1));
        expect(updates[0]?.name.toUpperCase()).toEqual("TEST");
      });

      test("for CoRecord", async () => {
        const TestRecord = co.record(z.string(), co.plainText());

        const TestRecordWithName = TestRecord.resolved({ name: true });

        const record = TestRecordWithName.create({ name: "Test" }, publicGroup);

        const updates: co.loaded<typeof TestRecordWithName>[] = [];
        TestRecordWithName.subscribe(
          record.$jazz.id,
          {
            loadAs: clientAccount,
          },
          (record) => {
            expectTypeOf<typeof record.name>().toEqualTypeOf<CoPlainText>();
            updates.push(record);
          },
        );

        await waitFor(() => expect(updates.length).toBe(1));
        expect(updates[0]?.name.toUpperCase()).toEqual("TEST");
      });

      test("for CoList", async () => {
        const TestList = co.list(co.plainText());

        const TestListWithItems = TestList.resolved({ $each: true });

        const list = TestListWithItems.create(["Test"], publicGroup);

        const updates: co.loaded<typeof TestListWithItems>[] = [];
        TestListWithItems.subscribe(
          list.$jazz.id,
          {
            loadAs: clientAccount,
          },
          (list) => {
            expectTypeOf<(typeof list)[0]>().toEqualTypeOf<CoPlainText>();
            updates.push(list);
          },
        );

        await waitFor(() => expect(updates.length).toBe(1));
        expect(updates[0]?.[0]?.toUpperCase()).toEqual("TEST");
      });

      // TODO fix - `$each` does not load nested CoValues when providing an explicit resolve query either
      test.skip("for CoFeed", async () => {
        const TestFeed = co.feed(co.plainText());

        const TestFeedWithItems = TestFeed.resolved({ $each: true });

        const feed = TestFeedWithItems.create(["Test"], publicGroup);

        const updates: co.loaded<typeof TestFeedWithItems>[] = [];
        TestFeedWithItems.subscribe(
          feed.$jazz.id,
          {
            loadAs: clientAccount,
          },
          (feed) => {
            updates.push(feed);
          },
        );

        await waitFor(() => expect(updates.length).toBe(1));
        expect(updates[0]?.inCurrentSession?.value.toUpperCase()).toEqual(
          "TEST",
        );
      });

      test("for Account", async () => {
        const AccountWithProfile = co.account().resolved({ profile: true });

        const account = await AccountWithProfile.createAs(serverAccount, {
          creationProps: { name: "Hermes Puggington" },
        });
        account.$jazz.set(
          "profile",
          co.profile().create({ name: "Hermes Puggington" }, publicGroup),
        );

        const updates: co.loaded<typeof AccountWithProfile>[] = [];
        AccountWithProfile.subscribe(
          account.$jazz.id,
          {
            loadAs: clientAccount,
          },
          (account) => {
            updates.push(account);
          },
        );

        await waitFor(() => expect(updates.length).toBe(1));
        expect(updates[0]?.profile.name).toBe("Hermes Puggington");
      });
    });

    describe("on merge()", () => {
      test("for CoMap", async () => {
        const TestMap = co.map({ name: co.plainText() });

        const TestMapWithName = TestMap.resolved({ name: true });

        const map = TestMap.create({ name: "Test" }, publicGroup);

        const branchMap = await TestMapWithName.load(map.$jazz.id, {
          unstable_branch: { name: "test-merge-coMap", owner: publicGroup },
          loadAs: clientAccount,
        });

        assertLoaded(branchMap);
        branchMap.name.insertAfter(branchMap.name.length, "!!");

        await TestMapWithName.unstable_merge(map.$jazz.id, {
          branch: { name: "test-merge-coMap", owner: publicGroup },
          loadAs: clientAccount,
        });

        const mergedMap = await TestMapWithName.load(map.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(mergedMap);
        expect(mergedMap.name.toUpperCase()).toEqual("TEST!!");
      });

      test("for CoRecord", async () => {
        const TestRecord = co.record(z.string(), co.plainText());

        const TestRecordWithName = TestRecord.resolved({ name: true });

        const record = TestRecordWithName.create({ name: "Test" }, publicGroup);

        const branchRecord = await TestRecordWithName.load(record.$jazz.id, {
          unstable_branch: { name: "test-merge-coRecord", owner: publicGroup },
          loadAs: clientAccount,
        });

        assertLoaded(branchRecord);
        branchRecord.name.insertAfter(branchRecord.name.length, "!!");

        await TestRecordWithName.unstable_merge(record.$jazz.id, {
          branch: { name: "test-merge-coRecord", owner: publicGroup },
          loadAs: clientAccount,
        });

        const mergedRecord = await TestRecordWithName.load(record.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(mergedRecord);
        expect(mergedRecord.name.toUpperCase()).toEqual("TEST!!");
      });

      test("for CoList", async () => {
        const TestList = co.list(co.plainText());

        const TestListWithItems = TestList.resolved({ $each: true });

        const list = TestListWithItems.create(["Test"], publicGroup);

        const branchList = await TestListWithItems.load(list.$jazz.id, {
          unstable_branch: { name: "test-merge-coList", owner: publicGroup },
          loadAs: clientAccount,
        });

        assertLoaded(branchList);
        branchList[0]?.insertAfter(branchList[0].length, "!!");

        await TestListWithItems.unstable_merge(list.$jazz.id, {
          branch: { name: "test-merge-coList", owner: publicGroup },
          loadAs: clientAccount,
        });

        const mergedList = await TestListWithItems.load(list.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(mergedList);
        expect(mergedList[0]?.toUpperCase()).toEqual("TEST!!");
      });

      // TODO fix - `$each` does not load nested CoValues when providing an explicit resolve query either
      test.skip("for CoFeed", async () => {
        const TestFeed = co.feed(co.plainText());

        const TestFeedWithItems = TestFeed.resolved({ $each: true });

        const feed = TestFeedWithItems.create(["Test"], publicGroup);

        const branchFeed = await TestFeedWithItems.load(feed.$jazz.id, {
          unstable_branch: { name: "test-merge-coFeed", owner: publicGroup },
          loadAs: clientAccount,
        });

        assertLoaded(branchFeed);
        branchFeed.inCurrentSession?.value.insertAfter(
          branchFeed.inCurrentSession.value.length,
          "!!",
        );

        await TestFeedWithItems.unstable_merge(feed.$jazz.id, {
          branch: { name: "test-merge-coFeed", owner: publicGroup },
          loadAs: clientAccount,
        });

        const mergedFeed = await TestFeedWithItems.load(feed.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(mergedFeed);
        expect(mergedFeed.inCurrentSession?.value.toUpperCase()).toEqual(
          "TEST!!",
        );
      });

      test("for Account", async () => {
        const TestAccount = co
          .account({
            profile: co.profile(),
            root: co.map({ text: co.plainText() }),
          })
          .resolved({ profile: true, root: { text: true } });
        const AccountList = co
          .list(TestAccount)
          .resolved({ $each: TestAccount.resolveQuery });

        const account = await TestAccount.createAs(serverAccount, {
          creationProps: { name: "Hermes Puggington" },
        });
        account.$jazz.set(
          "profile",
          TestAccount.shape.profile.create(
            { name: "Hermes Puggington" },
            publicGroup,
          ),
        );
        account.$jazz.set(
          "root",
          TestAccount.shape.root.create({ text: "Test" }, publicGroup),
        );
        const accountList = AccountList.create([account], publicGroup);

        const branchAccountList = await AccountList.load(accountList.$jazz.id, {
          unstable_branch: {
            name: "test-merge-account",
            owner: publicGroup,
          },
          loadAs: clientAccount,
        });

        assertLoaded(branchAccountList);
        branchAccountList[0]?.root.text.insertAfter(
          branchAccountList[0].root.text.length,
          "!!",
        );

        await TestAccount.unstable_merge(account.$jazz.id, {
          branch: { name: "test-merge-account", owner: publicGroup },
          loadAs: clientAccount,
        });

        const mergedAccount = await TestAccount.load(account.$jazz.id, {
          loadAs: clientAccount,
        });

        assertLoaded(mergedAccount);
        expect(mergedAccount.root.text.toUpperCase()).toEqual("TEST!!");
      });
    });

    describe("on upsertUnique()", () => {
      test("for CoMap", async () => {
        const TestMap = co.map({ name: co.plainText() });

        const TestMapWithName = TestMap.resolved({ name: true });

        const map = await TestMapWithName.upsertUnique({
          value: { name: "Test" },
          unique: "test-upsertUnique-coList",
          owner: publicGroup,
        });

        assertLoaded(map);
        expect(map.name.toUpperCase()).toEqual("TEST");
      });

      test("for CoRecord", async () => {
        const TestRecord = co.record(z.string(), co.plainText());

        const TestRecordWithName = TestRecord.resolved({ name: true });

        const record = await TestRecordWithName.upsertUnique({
          value: { name: "Test" },
          unique: "test-upsertUnique-coRecord",
          owner: publicGroup,
        });

        assertLoaded(record);
        expect(record.name?.toUpperCase()).toEqual("TEST");
      });

      test("for CoList", async () => {
        const TestList = co.list(co.plainText());

        const TestListWithItems = TestList.resolved({ $each: true });

        const list = await TestListWithItems.upsertUnique({
          value: ["Test"],
          unique: "test-upsertUnique-coList",
          owner: publicGroup,
        });

        assertLoaded(list);
        expect(list[0]?.toUpperCase()).toEqual("TEST");
      });
    });

    describe("on loadUnique()", () => {
      let group: Group;
      beforeAll(async () => {
        group = Group.create();
      });

      test("for CoMap", async () => {
        const TestMap = co.map({ name: co.plainText() });
        const TestMapWithName = TestMap.resolved({ name: true });

        const map = TestMapWithName.create(
          { name: "Test" },
          {
            unique: "test-loadUnique-coMap",
            owner: group,
          },
        );

        const loadedMap = await TestMapWithName.loadUnique(
          "test-loadUnique-coMap",
          group.$jazz.id,
        );

        assertLoaded(loadedMap);
        expect(loadedMap.name.toUpperCase()).toEqual("TEST");
      });

      test("for CoRecord", async () => {
        const TestRecord = co.record(z.string(), co.plainText());
        const TestRecordWithName = TestRecord.resolved({ name: true });

        const record = TestRecordWithName.create(
          { name: "Test" },
          {
            unique: "test-loadUnique-coRecord",
            owner: group,
          },
        );

        const loadedRecord = await TestRecordWithName.loadUnique(
          "test-loadUnique-coRecord",
          group.$jazz.id,
        );

        assertLoaded(loadedRecord);
        expect(loadedRecord.name?.toUpperCase()).toEqual("TEST");
      });

      test("for CoList", async () => {
        const TestList = co.list(co.plainText());
        const TestListWithItems = TestList.resolved({ $each: true });

        const list = TestListWithItems.create(["Test"], {
          unique: "test-loadUnique-coList",
          owner: group,
        });

        const loadedList = await TestListWithItems.loadUnique(
          "test-loadUnique-coList",
          group.$jazz.id,
        );

        assertLoaded(loadedList);
        expect(loadedList[0]?.toUpperCase()).toEqual("TEST");
      });
    });

    test("the default resolve query is overridden with provided resolve queries", async () => {
      const TestMap = co.map({ name: co.plainText() });

      const TestMapWithName = TestMap.resolved({ name: true });

      const map = TestMapWithName.create({ name: "Test" }, publicGroup);

      const loadedMap = await TestMapWithName.load(map.$jazz.id, {
        loadAs: clientAccount,
        resolve: true,
      });

      assertLoaded(loadedMap);
      expect(loadedMap.name.$jazz.loadingState).toEqual(
        CoValueLoadingState.LOADING,
      );
    });

    test("nested schemas' resolve queries are ignored when loading the parent schema", async () => {
      const TestMap = co.map({
        name: co.plainText(),
      });
      const TestMapWithName = TestMap.resolved({ name: true });
      const ParentMap = co.map({
        child: TestMapWithName,
      });
      const ParentMapWithChild = ParentMap.resolved({ child: true });

      const parentMap = ParentMapWithChild.create(
        { child: { name: "Test" } },
        publicGroup,
      );

      const loadedParentMap = await ParentMapWithChild.load(
        parentMap.$jazz.id,
        {
          loadAs: clientAccount,
        },
      );

      assertLoaded(loadedParentMap);
      expect(loadedParentMap.child.$isLoaded).toEqual(true);
      expect(loadedParentMap.child.name.$isLoaded).toEqual(false);
    });

    test("works with recursive schemas", async () => {
      const Person = co.map({
        get friends(): co.List<typeof Person, { $each: true }> {
          return Friends;
        },
      });
      const Friends = co.list(Person).resolved({ $each: true });

      const PersonWithFriends = Person.resolved({
        friends: { $each: true },
      });

      const person = Person.create(
        {
          friends: [{ friends: [] }],
        },
        publicGroup,
      );

      const loadedPerson = await PersonWithFriends.load(person.$jazz.id, {
        loadAs: clientAccount,
      });

      assertLoaded(loadedPerson);
      expect(loadedPerson.friends.$isLoaded).toBe(true);
      expect(loadedPerson.friends.length).toBe(1);
      expect(loadedPerson.friends[0]?.friends.$isLoaded).toBe(false);
    });
  });
});
