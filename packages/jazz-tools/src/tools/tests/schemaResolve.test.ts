import { assert, beforeAll, describe, expect, test } from "vitest";
import { Account, co, CoValueLoadingState, Group, z } from "../exports";
import { createJazzTestAccount, setupJazzTestSync } from "../testing";
import { assertLoaded, setupTwoNodes } from "./utils";

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

    test("can make a schema greedily-loaded by using resolved()", () => {
      const Text = co.plainText().resolved();

      expect(Text.defaultResolveQuery).toBe(true);
    });

    test("CoOption schemas inherit the default resolve query of their inner type", () => {
      const Text = co.plainText().resolved();
      const OptionalText = co.optional(Text);

      expect(OptionalText.defaultResolveQuery).toBe(true);
    });

    test("CoList schemas inherit the default resolve query of their element type", () => {
      const Text = co.plainText().resolved();
      const TestList = co.list(Text);

      expect(TestList.defaultResolveQuery).toEqual({ $each: true });
    });

    test("CoMap schemas inherit the default resolve query of their shape", () => {
      const Text = co.plainText().resolved();
      const TestMap = co.map({ text: Text });

      expect(TestMap.defaultResolveQuery).toEqual({ text: true });
    });

    test("Account schemas inherit the default resolve query of their shape", () => {
      const Account = co.account({
        root: co
          .map({
            text: co.plainText(),
          })
          .resolved(),
        profile: co.profile({
          name: z.string(),
        }),
      });

      expect(Account.defaultResolveQuery).toEqual({ root: true });
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
          const TestList = co.list(co.plainText().resolved());

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
          const TestMap = co.map({ text: co.plainText().resolved() });

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
          // TODO
        });

        test("for Account", async () => {
          // TODO
        });

        test("for CoFeed", async () => {
          // TODO
        });
      });

      describe("on subscribe()", () => {
        // TODO
      });

      describe("on merge()", () => {
        // TODO
      });

      describe("on upsertUnique()", () => {
        // TODO
      });

      describe("on loadUnique()", () => {
        // TODO
      });
    });

    test("the default resolve query is merged with provided resolve queries", async () => {
      // TODO
    });
  });
});
