import { assert, beforeEach, expect, test } from "vitest";
import { Account, CoListSchema, Group, co, z } from "../exports.js";
import {
  createJazzTestAccount,
  linkAccounts,
  setActiveAccount,
  setupJazzTestSync,
} from "../testing.js";
import { setupTwoNodes } from "./utils.js";

beforeEach(async () => {
  await setupJazzTestSync();
});

test("waitForAllCoValuesSync should resolve when all the values are synced", async () => {
  const TestMap = co.map({
    name: z.string(),
  });

  const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

  const maps = Array.from({ length: 10 }).map(() =>
    TestMap.create({ name: "Alice" }, { owner: clientAccount }),
  );

  await clientAccount.waitForAllCoValuesSync({
    timeout: 1000,
  });

  // Killing the client node so the serverNode can't load the map from it
  clientNode.gracefulShutdown();

  for (const map of maps) {
    const loadedMap = await serverNode.load(map._raw.id);
    expect(loadedMap).not.toBe("unavailable");
  }
});

test("waitForSync should resolve when the value is uploaded", async () => {
  const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

  await clientAccount.waitForSync({ timeout: 1000 });

  // Killing the client node so the serverNode can't load the map from it
  clientNode.gracefulShutdown();

  const loadedAccount = await serverNode.load(clientAccount._raw.id);

  expect(loadedAccount).not.toBe("unavailable");
});

test("isMe gets updated correctly when switching accounts", async () => {
  const oldMe = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  expect(oldMe.isMe).toBe(true);

  const newMe = await createJazzTestAccount({
    isCurrentActiveAccount: false,
  });

  expect(newMe.isMe).toBe(false);
  expect(oldMe.isMe).toBe(true);

  setActiveAccount(newMe);

  expect(newMe.isMe).toBe(true);
  expect(oldMe.isMe).toBe(false);
});

test("Me gets updated correctly when creating a new account as active", async () => {
  const oldMe = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  expect(oldMe.isMe).toBe(true);

  const newMe = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  expect(newMe.isMe).toBe(true);
  expect(oldMe.isMe).toBe(false);
});

test("accounts should sync correctly", async () => {
  const account = await createJazzTestAccount({ isCurrentActiveAccount: true });
  account.profile!.name = "test 1";
  const otherAccount = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
  otherAccount.profile!.name = "test 2";

  await linkAccounts(account, otherAccount);

  const group = Group.create({ owner: account });

  group.addMember(otherAccount, "writer");

  expect(group.members[0]?.account.profile!.name).toBe("test 1");
  expect(group.members[1]?.account.profile!.name).toBe("test 2");
});

test("loading accounts should work", async () => {
  const account = await createJazzTestAccount({
    creationProps: {
      name: "test 1",
    },
  });

  const otherAccount = await createJazzTestAccount();

  const loadedAccount = await Account.load(account.id, {
    loadAs: otherAccount,
    resolve: {
      profile: true,
    },
  });

  assert(loadedAccount);
  expect(loadedAccount.profile.name).toBe("test 1");
});

test("loading raw accounts should work", async () => {
  const account = await createJazzTestAccount({
    creationProps: {
      name: "test 1",
    },
  });

  const loadedAccount = await Account.load(account.id, {
    loadAs: account,
  });

  assert(loadedAccount);
  expect(loadedAccount.profile!.name).toBe("test 1");
});

test("should support recursive props on co.profile", async () => {
  const User = co.profile({
    name: z.string(),
    email: z.optional(z.string()),
    image_url: z.optional(z.string()),
    created: z.date(),
    updated: z.date(),
    username: z.optional(z.string()),
    display_name: z.optional(z.string()),
    anonymous: z.boolean(),
    get following(): CoListSchema<typeof User> {
      return co.list(User);
    },
    get followers(): CoListSchema<typeof User> {
      return co.list(User);
    },
  });

  const MyAccount = co
    .account({
      profile: User,
      root: co.map({}),
    })
    .withMigration((me) => {
      if (me.profile === undefined) {
        const group = Group.create({ owner: me });
        group.addMember("everyone", "reader");
        me.profile = User.create(
          {
            name: "test 1",
            created: new Date(),
            updated: new Date(),
            anonymous: false,
            following: co.list(User).create([], group),
            followers: co.list(User).create([], group),
          },
          group,
        );
      }

      if (me.root === undefined) {
        me.root = co.map({}).create({});
      }
    });

  const account = await createJazzTestAccount({
    creationProps: {
      name: "test 1",
    },
    AccountSchema: MyAccount,
  });

  expect(account.profile.name).toBe("test 1");
  expect(account.root).toBeDefined();
  expect(account.profile.following.length).toBe(0);
  expect(account.profile.followers.length).toBe(0);
});
