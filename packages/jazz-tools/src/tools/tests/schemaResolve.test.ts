import {
  assert,
  beforeAll,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
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

describe("Schema-level CoValue resolution", () => {
  beforeAll(async () => {
    await setupJazzTestSync();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
  });

  describe("defining resolve queries in schemas", () => {
    test("by default, schemas have no default resolve queries", () => {
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
        expect(Schema.defaultResolveQuery).toBe(false);
      }
    });

    describe("resolved() makes the schema eagerly-loaded", () => {
      test("CoPlainText", () => {
        const Text = co.plainText().resolved();

        expectTypeOf<typeof Text.defaultResolveQuery>().toEqualTypeOf(true);
        expect(Text.defaultResolveQuery).toBe(true);
      });

      test("CoRichText", () => {
        const Text = co.richText().resolved();

        expectTypeOf<typeof Text.defaultResolveQuery>().toEqualTypeOf(true);
        expect(Text.defaultResolveQuery).toBe(true);
      });

      test("FileStream", () => {
        const FileStream = co.fileStream().resolved();

        expectTypeOf<typeof FileStream.defaultResolveQuery>().toEqualTypeOf(
          true,
        );
        expect(FileStream.defaultResolveQuery).toBe(true);
      });

      test("Group", () => {
        const Group = co.group().resolved();

        expectTypeOf<typeof Group.defaultResolveQuery>().toEqualTypeOf(true);
        expect(Group.defaultResolveQuery).toBe(true);
      });

      describe("CoOption", () => {
        test("schemas inherit the default resolve query of their inner type", () => {
          const Text = co.plainText().resolved();
          const OptionalText = co.optional(Text).resolved();

          expectTypeOf<typeof OptionalText.defaultResolveQuery>().toEqualTypeOf(
            true,
          );
          expect(OptionalText.defaultResolveQuery).toBe(true);
        });

        test("schema becomes shallowly-loaded when its inner type is not eagerly-loaded", () => {
          const Text = co.plainText();
          const OptionalText = co.optional(Text).resolved();

          expectTypeOf<typeof OptionalText.defaultResolveQuery>().toEqualTypeOf(
            true,
          );
          expect(OptionalText.defaultResolveQuery).toBe(true);
        });
      });

      describe("CoDiscriminatedUnion", () => {
        test("schema can only be made shallowly-loaded", () => {
          const DiscriminatedUnion = co
            .discriminatedUnion("type", [
              co
                .map({
                  type: z.literal("a"),
                  fieldA: co.plainText().resolved(),
                })
                .resolved(),
              co
                .map({
                  type: z.literal("b"),
                  fieldB: co.plainText().resolved(),
                })
                .resolved(),
            ])
            .resolved();

          expectTypeOf<
            typeof DiscriminatedUnion.defaultResolveQuery
          >().toEqualTypeOf(true);
          expect(DiscriminatedUnion.defaultResolveQuery).toBe(true);
        });
      });

      describe("CoList", () => {
        test("schemas inherit the default resolve query of their element type", () => {
          const Text = co.plainText().resolved();
          const TestList = co.list(Text).resolved();

          expectTypeOf<typeof TestList.defaultResolveQuery>().toEqualTypeOf<{
            $each: true;
          }>();
          expect(TestList.defaultResolveQuery).toEqual({ $each: true });
        });

        test("schema becomes shallowly-loaded when its element type is not eagerly-loaded", () => {
          const Text = co.plainText();
          const TestList = co.list(Text).resolved();

          expectTypeOf<
            typeof TestList.defaultResolveQuery
          >().toEqualTypeOf<true>();
          expect(TestList.defaultResolveQuery).toBe(true);
        });
      });

      describe("CoFeed", () => {
        test("schemas inherit the default resolve query of their element type", () => {
          const Text = co.plainText().resolved();
          const TestFeed = co.feed(Text).resolved();

          expectTypeOf<typeof TestFeed.defaultResolveQuery>().toEqualTypeOf<{
            $each: true;
          }>();
          expect(TestFeed.defaultResolveQuery).toEqual({ $each: true });
        });

        test("schema becomes shallowly-loaded when its element type is not eagerly-loaded", () => {
          const Text = co.plainText();
          const TestFeed = co.feed(Text).resolved();

          expectTypeOf<
            typeof TestFeed.defaultResolveQuery
          >().toEqualTypeOf<true>();
          expect(TestFeed.defaultResolveQuery).toBe(true);
        });
      });

      describe("CoMap", () => {
        test("schemas inherit the default resolve query of their shape", () => {
          const Text = co.plainText().resolved();
          const TestMap = co.map({ text: Text }).resolved();

          expectTypeOf<typeof TestMap.defaultResolveQuery>().toEqualTypeOf<{
            text: true;
          }>();
          expect(TestMap.defaultResolveQuery).toEqual({ text: true });
        });

        test("schema becomes shallowly-loaded when its fields are not eagerly-loaded", () => {
          const Text = co.plainText();
          const TestMap = co.map({ text: Text }).resolved();

          expectTypeOf<
            typeof TestMap.defaultResolveQuery
          >().toEqualTypeOf<true>();
          expect(TestMap.defaultResolveQuery).toBe(true);
        });
      });

      describe("CoRecord", () => {
        test("schemas inherit the default resolve query of their fields", () => {
          const Text = co.plainText().resolved();
          const TestRecord = co.record(z.string(), Text).resolved();

          expectTypeOf<typeof TestRecord.defaultResolveQuery>().toEqualTypeOf<{
            $each: true;
          }>();
          expect(TestRecord.defaultResolveQuery).toEqual({ $each: true });
        });

        test("schema becomes shallowly-loaded when its fields are not eagerly-loaded", () => {
          const Text = co.plainText();
          const TestRecord = co.record(z.string(), Text).resolved();

          expectTypeOf<
            typeof TestRecord.defaultResolveQuery
          >().toEqualTypeOf<true>();
          expect(TestRecord.defaultResolveQuery).toBe(true);
        });
      });

      describe("Account", () => {
        test("schemas inherit the default resolve query of their fields", () => {
          const TestAccount = co
            .account({
              profile: co.profile().resolved(),
              root: co.map({}).resolved(),
            })
            .resolved();

          expectTypeOf<typeof TestAccount.defaultResolveQuery>().toEqualTypeOf<{
            profile: true;
            root: true;
          }>();
          expect(TestAccount.defaultResolveQuery).toEqual({
            profile: true,
            root: true,
          });
        });

        test("schema becomes shallowly-loaded when its fields are not eagerly-loaded", () => {
          const TestAccount = co.account().resolved();

          expectTypeOf<
            typeof TestAccount.defaultResolveQuery
          >().toEqualTypeOf<true>();
          expect(TestAccount.defaultResolveQuery).toBe(true);
        });
      });
    });
  });

  describe("using Schema-level resolution when loading CoValues", () => {
    let clientAccount: Account;
    let serverAccount: Account;
    let publicGroup: Group;

    beforeAll(async () => {
      ({ clientAccount, serverAccount } = await setupTwoNodes());
      publicGroup = Group.create(serverAccount).makePublic();
    });

    describe("the default resolve query is used if no resolve query is provided", () => {
      describe("on load()", () => {
        test("for CoList", async () => {
          const TestList = co.list(co.plainText().resolved()).resolved();

          const list = TestList.create(["Hello"], publicGroup);

          const loadedList = await TestList.load(list.$jazz.id, {
            loadAs: clientAccount,
          });

          assertLoaded(loadedList);
          assert(loadedList[0]);
          expect(loadedList[0].$jazz.loadingState).toBe(
            CoValueLoadingState.LOADED,
          );
          expect(loadedList[0].toUpperCase()).toEqual("HELLO");
        });

        test("for CoMap", async () => {
          const TestMap = co
            .map({ text: co.plainText().resolved() })
            .resolved();

          const map = TestMap.create({ text: "Hello" }, publicGroup);

          const loadedMap = await TestMap.load(map.$jazz.id, {
            loadAs: clientAccount,
          });

          assertLoaded(loadedMap);
          expect(loadedMap.text.$jazz.loadingState).toBe(
            CoValueLoadingState.LOADED,
          );
          expect(loadedMap.text.toUpperCase()).toEqual("HELLO");
        });

        test("for CoRecord", async () => {
          const TestRecord = co
            .record(z.string(), co.plainText().resolved())
            .resolved();

          const record = TestRecord.create(
            {
              key1: "Hello",
              key2: "World",
            },
            publicGroup,
          );

          const loadedRecord = await TestRecord.load(record.$jazz.id, {
            loadAs: clientAccount,
          });

          assertLoaded(loadedRecord);
          expect(loadedRecord.key1?.$jazz.loadingState).toBe(
            CoValueLoadingState.LOADED,
          );
          expect(loadedRecord.key1?.toUpperCase()).toEqual("HELLO");
          expect(loadedRecord.key2?.$jazz.loadingState).toBe(
            CoValueLoadingState.LOADED,
          );
          expect(loadedRecord.key2?.toUpperCase()).toEqual("WORLD");
        });

        test("for Account", async () => {
          const TestAccount = co
            .account({
              profile: co.profile().resolved(),
              root: co.map({ text: co.plainText().resolved() }).resolved(),
            })
            .resolved();
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
            TestAccount.shape.root.create({ text: "Hello" }, publicGroup),
          );

          const loadedAccount = await TestAccount.load(account.$jazz.id, {
            loadAs: clientAccount,
          });

          assertLoaded(loadedAccount);
          expect(loadedAccount.profile.name).toBe("Hermes Puggington");
          expect(loadedAccount.root.text.toUpperCase()).toEqual("HELLO");
        });

        // TODO fix - this is not working when providing an explicit resolve query:
        // TestFeed.load(feed.$jazz.id, {
        //   loadAs: clientAccount,
        //   resolve: {
        //     $each: true,
        //   },
        // })
        test.skip("for CoFeed", async () => {
          const TestFeed = co.feed(co.plainText().resolved()).resolved();

          const feed = TestFeed.create(["Hello"], publicGroup);

          const loadedFeed = await TestFeed.load(feed.$jazz.id, {
            loadAs: clientAccount,
            resolve: {
              $each: true,
            },
          });

          assertLoaded(loadedFeed);
          expect(loadedFeed.inCurrentSession?.value.$jazz.loadingState).toBe(
            CoValueLoadingState.LOADED,
          );
          expect(loadedFeed.inCurrentSession?.value.toUpperCase()).toEqual(
            "HELLO",
          );
        });
      });

      describe("on subscribe()", () => {
        test("for CoList", async () => {
          const TestList = co.list(co.plainText().resolved()).resolved();

          const list = TestList.create(["Hello"], publicGroup);

          const updates: co.loaded<typeof TestList, { $each: true }>[] = [];
          TestList.subscribe(
            list.$jazz.id,
            { loadAs: clientAccount },
            (list) => {
              expectTypeOf<(typeof list)[0]>().toEqualTypeOf<CoPlainText>();
              updates.push(list);
            },
          );

          await waitFor(() => expect(updates.length).toBe(1));
          expect(updates[0]?.[0]?.toUpperCase()).toEqual("HELLO");
        });

        test("for CoMap", async () => {
          const TestMap = co
            .map({ text: co.plainText().resolved() })
            .resolved();

          const map = TestMap.create({ text: "Hello" }, publicGroup);

          const updates: co.loaded<typeof TestMap, { text: true }>[] = [];
          TestMap.subscribe(
            map.$jazz.id,
            {
              loadAs: clientAccount,
            },
            (map) => {
              expectTypeOf<typeof map.text>().toEqualTypeOf<CoPlainText>();
              updates.push(map);
            },
          );

          await waitFor(() => expect(updates.length).toBe(1));
          expect(updates[0]?.text.toUpperCase()).toEqual("HELLO");
        });

        test("for CoRecord", async () => {
          const TestRecord = co
            .record(z.string(), co.plainText().resolved())
            .resolved();

          const record = TestRecord.create(
            { key1: "Hello", key2: "World" },
            publicGroup,
          );

          const updates: co.loaded<typeof TestRecord, { $each: true }>[] = [];
          TestRecord.subscribe(
            record.$jazz.id,
            { loadAs: clientAccount },
            (record) => {
              expectTypeOf<typeof record.key1>().toEqualTypeOf<
                CoPlainText | undefined
              >();
              expectTypeOf<typeof record.key2>().toEqualTypeOf<
                CoPlainText | undefined
              >();
              updates.push(record);
            },
          );

          await waitFor(() => expect(updates.length).toBe(1));
          expect(updates[0]?.key1?.toUpperCase()).toEqual("HELLO");
          expect(updates[0]?.key2?.toUpperCase()).toEqual("WORLD");
        });

        test("for Account", async () => {
          const TestAccount = co
            .account({
              profile: co.profile().resolved(),
              root: co.map({ text: co.plainText().resolved() }).resolved(),
            })
            .resolved();

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
            TestAccount.shape.root.create({ text: "Hello" }, publicGroup),
          );

          const updates: co.loaded<
            typeof TestAccount,
            { profile: true; root: { text: true } }
          >[] = [];
          TestAccount.subscribe(
            account.$jazz.id,
            { loadAs: clientAccount },
            (account) => {
              updates.push(account);
            },
          );

          await waitFor(() => expect(updates.length).toBe(1));
          expect(updates[0]?.profile.name).toBe("Hermes Puggington");
          expect(updates[0]?.root.text.toUpperCase()).toEqual("HELLO");
        });

        // TODO fix - this is not working when providing an explicit resolve query either
        test.skip("for CoFeed", async () => {
          const TestFeed = co.feed(co.plainText().resolved()).resolved();

          const feed = TestFeed.create(["Hello"], publicGroup);

          const updates: co.loaded<typeof TestFeed, { $each: true }>[] = [];
          TestFeed.subscribe(
            feed.$jazz.id,
            { loadAs: clientAccount },
            (feed) => {
              updates.push(feed);
            },
          );

          await waitFor(() => expect(updates.length).toBe(1));
          expect(updates[0]?.inCurrentSession?.value.toUpperCase()).toEqual(
            "HELLO",
          );
        });
      });

      describe("on upsertUnique()", () => {
        test("for CoList", async () => {
          const TestList = co.list(co.plainText().resolved()).resolved();

          const list = await TestList.upsertUnique({
            value: ["Hello"],
            unique: "test-upsertUnique-coList",
            owner: publicGroup,
          });

          assertLoaded(list);
          expect(list[0]?.toUpperCase()).toEqual("HELLO");
        });

        test("for CoMap", async () => {
          const TestMap = co
            .map({ text: co.plainText().resolved() })
            .resolved();

          const map = await TestMap.upsertUnique({
            value: { text: "Hello" },
            unique: "test-upsertUnique-coMap",
            owner: publicGroup,
          });

          assertLoaded(map);
          expect(map.text.toUpperCase()).toEqual("HELLO");
        });

        test("for CoRecord", async () => {
          const TestRecord = co
            .record(z.string(), co.plainText().resolved())
            .resolved();

          const record = await TestRecord.upsertUnique({
            value: { key1: "Hello", key2: "World" },
            unique: "test-upsertUnique-coRecord",
            owner: publicGroup,
          });

          assertLoaded(record);
          expect(record.key1?.toUpperCase()).toEqual("HELLO");
          expect(record.key2?.toUpperCase()).toEqual("WORLD");
        });
      });

      describe("on loadUnique()", () => {
        let group: Group;
        beforeAll(async () => {
          group = Group.create();
        });

        test("for CoList", async () => {
          const TestList = co.list(co.plainText().resolved()).resolved();

          const list = TestList.create(["Hello"], {
            unique: "test-loadUnique-coList",
            owner: group,
          });

          const loadedList = await TestList.loadUnique(
            "test-loadUnique-coList",
            group.$jazz.id,
          );

          assertLoaded(loadedList);
          expect(loadedList[0]?.toUpperCase()).toEqual("HELLO");
        });

        test("for CoMap", async () => {
          const TestMap = co
            .map({ text: co.plainText().resolved() })
            .resolved();

          const map = TestMap.create(
            { text: "Hello" },
            {
              unique: "test-loadUnique-coMap",
              owner: group,
            },
          );

          const loadedMap = await TestMap.loadUnique(
            "test-loadUnique-coMap",
            group.$jazz.id,
          );

          assertLoaded(loadedMap);
          expect(loadedMap.text.toUpperCase()).toEqual("HELLO");
        });

        test("for CoRecord", async () => {
          const TestRecord = co
            .record(z.string(), co.plainText().resolved())
            .resolved();

          const record = TestRecord.create(
            { key1: "Hello", key2: "World" },
            {
              unique: "test-loadUnique-coRecord",
              owner: group,
            },
          );

          const loadedRecord = await TestRecord.loadUnique(
            "test-loadUnique-coRecord",
            group.$jazz.id,
          );

          assertLoaded(loadedRecord);
          expect(loadedRecord.key1?.toUpperCase()).toEqual("HELLO");
          expect(loadedRecord.key2?.toUpperCase()).toEqual("WORLD");
        });
      });
    });

    // TODO merge default resolve query with provided resolve queries instead of overriding it
    describe("the default resolve query is overridden with provided resolve queries", () => {
      test("for CoMap", async () => {
        const TestMap = co.map({ text: co.plainText().resolved() }).resolved();

        const map = TestMap.create({ text: "Hello" }, publicGroup);

        const loadedMap = await TestMap.load(map.$jazz.id, {
          loadAs: clientAccount,
          resolve: true,
        });

        assertLoaded(loadedMap);
        expect(loadedMap.text.$jazz.loadingState).toEqual(
          CoValueLoadingState.LOADING,
        );
      });

      // TODO test other container schemas
    });
  });
});
