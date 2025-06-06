import { beforeEach, describe, expect, test } from "vitest";
import { Account, CoMap, Group, co, z, zodSchemaToCoSchema } from "../exports";
import { createJazzTestAccount, setupJazzTestSync } from "../testing";

describe("Jazz Test Sync", () => {
  beforeEach(async () => {
    await setupJazzTestSync();
  });

  test("two test accounts can sync through sync server", async () => {
    const account1 = await createJazzTestAccount();
    const account2 = await createJazzTestAccount();

    // Create a test group in account1
    const group = Group.create(account1);
    group.addMember("everyone", "reader");

    const map = CoMap.create({}, group);
    map._raw.set("test", "value");

    // Verify account2 can see the group
    const loadedMap = await CoMap.load(map.id, { loadAs: account2 });
    expect(loadedMap).toBeDefined();
    expect(loadedMap?._raw.get("test")).toBe("value");
  });

  test("correctly set the globalMe before starting the migration", async () => {
    const MyRoot = co.map({
      value: z.string(),
    });

    const CustomAccount = co
      .account({
        root: MyRoot,
        profile: co.profile(),
      })
      .withMigration((account) => {
        if (account.root === undefined) {
          account.root = MyRoot.create({
            value: "ok",
          });
        }
      });

    const account1 = await createJazzTestAccount({
      AccountSchema: zodSchemaToCoSchema(CustomAccount),
      isCurrentActiveAccount: true,
    });

    expect(account1.root?.value).toBe("ok");
  });

  test("correctly manages the global me during the migrations", async () => {
    const MyRoot = co.map({
      value: z.string(),
    });

    const CustomAccount = co
      .account({
        root: MyRoot,
        profile: co.profile(),
      })
      .withMigration((account) => {
        if (account.root === undefined) {
          account.root = MyRoot.create({
            value: "ok",
          });
        }
      });

    const account1 = await createJazzTestAccount({
      AccountSchema: zodSchemaToCoSchema(CustomAccount),
      isCurrentActiveAccount: true,
    });

    const account2 = await createJazzTestAccount({
      AccountSchema: zodSchemaToCoSchema(CustomAccount),
      isCurrentActiveAccount: false,
    });

    expect(account1.root?.value).toBe("ok");
    expect(account2.root?.value).toBe("ok");

    expect(Account.getMe()).toBe(account1);
  });

  test("throws when running multiple migrations in parallel", async () => {
    const CustomAccount = co.account().withMigration(async (account) => {
      await new Promise((resolve) => setTimeout(resolve, 10));
    });

    const promise = Promise.all([
      createJazzTestAccount({
        AccountSchema: zodSchemaToCoSchema(CustomAccount),
        isCurrentActiveAccount: true,
      }),
      createJazzTestAccount({
        AccountSchema: zodSchemaToCoSchema(CustomAccount),
        isCurrentActiveAccount: true,
      }),
    ]);

    await expect(promise).rejects.toThrow(
      "It is not possible to create multiple accounts in parallel inside the test environment.",
    );
  });
});
