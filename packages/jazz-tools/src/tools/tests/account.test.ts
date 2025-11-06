import { assert, beforeEach, describe, expect, test } from "vitest";
import { Account, Group, co, z } from "../exports.js";
import {
  createJazzTestAccount,
  linkAccounts,
  setActiveAccount,
  setupJazzTestSync,
} from "../testing.js";
import { assertLoaded, setupTwoNodes } from "./utils.js";
import { CoValueLoadingState } from "../internal.js";

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

  await clientAccount.$jazz.waitForAllCoValuesSync({
    timeout: 1000,
  });

  // Killing the client node so the serverNode can't load the map from it
  clientNode.gracefulShutdown();

  for (const map of maps) {
    const loadedMap = await serverNode.load(map.$jazz.raw.id);
    expect(loadedMap).not.toBe(CoValueLoadingState.UNAVAILABLE);
  }
});

test("waitForSync should resolve when the value is uploaded", async () => {
  const { clientNode, serverNode, clientAccount } = await setupTwoNodes();

  await clientAccount.$jazz.waitForSync({ timeout: 1000 });

  // Killing the client node so the serverNode can't load the map from it
  clientNode.gracefulShutdown();

  const loadedAccount = await serverNode.load(clientAccount.$jazz.raw.id);

  expect(loadedAccount).not.toBe(CoValueLoadingState.UNAVAILABLE);
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
  assertLoaded(account.profile);
  account.profile.$jazz.set("name", "test 1");
  const otherAccount = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
  assertLoaded(otherAccount.profile);
  otherAccount.profile.$jazz.set("name", "test 2");

  await linkAccounts(account, otherAccount);

  const group = Group.create({ owner: account });

  group.addMember(otherAccount, "writer");

  const accountMember = group.members[0]?.account;
  assert(accountMember);
  assertLoaded(accountMember.profile);
  expect(accountMember.profile.name).toBe("test 1");
  const otherAccountMember = group.members[1]?.account;
  assert(otherAccountMember);
  assertLoaded(otherAccountMember.profile);
  expect(otherAccountMember.profile.name).toBe("test 2");
});

test("loading accounts should work", async () => {
  const account = await createJazzTestAccount({
    creationProps: {
      name: "test 1",
    },
  });

  const otherAccount = await createJazzTestAccount();

  const loadedAccount = await Account.load(account.$jazz.id, {
    loadAs: otherAccount,
    resolve: {
      profile: true,
    },
  });

  assertLoaded(loadedAccount);
  expect(loadedAccount.profile.name).toBe("test 1");
});

test("loading raw accounts should work", async () => {
  const account = await createJazzTestAccount({
    creationProps: {
      name: "test 1",
    },
  });

  const loadedAccount = await Account.load(account.$jazz.id, {
    loadAs: account,
  });

  assertLoaded(loadedAccount);
  assertLoaded(loadedAccount.profile);
  expect(loadedAccount.profile.name).toBe("test 1");
});

describe("co.profile() schema", () => {
  test("co.profile() should throw an error if passed a CoValue schema", async () => {
    expect(() => co.profile(co.map({}))).toThrow(
      "co.profile() expects an object as its argument, not a CoValue schema",
    );
  });

  test("co.profile() should throw an error if its shape does not contain valid schemas", () => {
    expect(() =>
      co.profile({
        field: "a string is not a valid schema",
      }),
    ).toThrow("co.profile() supports only Zod v4 schemas and CoValue schemas");
  });
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
    get following(): co.List<typeof User> {
      return co.list(User);
    },
    get followers(): co.List<typeof User> {
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
        me.$jazz.set(
          "profile",
          User.create(
            {
              name: "test 1",
              created: new Date(),
              updated: new Date(),
              anonymous: false,
              following: co.list(User).create([], group),
              followers: co.list(User).create([], group),
            },
            group,
          ),
        );
      }

      if (me.root === undefined) {
        me.$jazz.set("root", {});
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

test("cannot update account profile properties directly", async () => {
  const account = await createJazzTestAccount({
    creationProps: {
      name: "test 1",
    },
  });

  assertLoaded(account.profile);

  // @ts-expect-error - cannot update profile properties directly
  expect(() => (account.profile.name = "test 2")).toThrow(
    "Cannot update a CoMap directly. Use `$jazz.set` instead.",
  );
  account.profile.$jazz.set("name", "test 3");
  expect(account.profile.name).toBe("test 3");
});

describe("root and profile", () => {
  test("root and profile should be trusting by default", async () => {
    const AccountSchema = co
      .account({
        profile: co.profile(),
        root: co.map({
          name: z.string(),
        }),
      })
      .withMigration((me, creationProps) => {
        const group = Group.create({ owner: me }).makePublic();

        if (me.profile === undefined) {
          me.$jazz.set(
            "profile",
            co.profile().create(
              {
                name: creationProps?.name ?? "Anonymous",
              },
              group,
            ),
          );
        }

        if (me.root === undefined) {
          me.$jazz.set(
            "root",
            co
              .map({
                name: z.string(),
              })
              .create(
                {
                  name: creationProps?.name ?? "Anonymous",
                },
                group,
              ),
          );
        }
      });

    const bob = await createJazzTestAccount({
      AccountSchema,
      creationProps: {
        name: "Bob",
      },
    });

    const alice = await createJazzTestAccount({
      AccountSchema,
      creationProps: {
        name: "Alice",
      },
    });

    const bobAccountLoadedFromAlice = await AccountSchema.load(bob.$jazz.id, {
      loadAs: alice,
      resolve: {
        profile: true,
        root: true,
      },
    });

    assertLoaded(bobAccountLoadedFromAlice);

    expect(bobAccountLoadedFromAlice.profile.name).toBe("Bob");
    expect(bobAccountLoadedFromAlice.root.name).toBe("Bob");
  });

  test("can be initialized using JSON objects", async () => {
    const Avatar = co.map({
      url: z.string(),
    });
    const CustomProfile = co.profile({
      avatar: Avatar,
    });
    const CustomRoot = co.map({
      name: z.string(),
    });
    const AccountSchema = co
      .account({
        profile: CustomProfile,
        root: CustomRoot,
      })
      .withMigration((me, creationProps) => {
        if (me.profile === undefined) {
          me.$jazz.set("profile", {
            name: creationProps?.name ?? "Anonymous",
            avatar: {
              url: "https://example.com/avatar.png",
            },
          });
        }

        if (me.root === undefined) {
          me.$jazz.set("root", { name: creationProps?.name ?? "Anonymous" });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
      creationProps: {
        name: "test 1",
      },
    });

    expect(account.profile.name).toBe("test 1");
    expect(account.profile.avatar.url).toBe("https://example.com/avatar.png");
    expect(account.root.name).toBe("test 1");

    // The account when setting the owner, the owner should not extend the account
    expect(account.root.$jazz.owner.getParentGroups()).toEqual([]);
    expect(account.profile.$jazz.owner.getParentGroups()).toEqual([]);
  });
});

describe("account.$jazz.has", () => {
  test("should return true if the key is defined", async () => {
    const account = await createJazzTestAccount({
      creationProps: { name: "John" },
    });

    expect(account.$jazz.has("profile")).toBe(true);
    expect(account.$jazz.has("root")).toBe(false);
  });

  test("should work as migration check", async () => {
    const CustomProfile = co.profile({
      name: z.string(),
      email: z.string().optional(),
    });

    const CustomRoot = co.map({
      settings: z.string(),
    });

    const CustomAccount = co
      .account({
        profile: CustomProfile,
        root: CustomRoot,
      })
      .withMigration((me, creationProps) => {
        if (!me.$jazz.has("profile")) {
          me.$jazz.set("profile", {
            name: creationProps?.name ?? "Anonymous",
            email: "test@example.com",
          });
        }

        if (!me.$jazz.has("root")) {
          me.$jazz.set("root", { settings: "default" });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema: CustomAccount,
      creationProps: { name: "Custom User" },
    });

    expect(account.$jazz.has("profile")).toBe(true);
    expect(account.$jazz.has("root")).toBe(true);

    expect(account.profile.email).toBe("test@example.com");
    expect(account.root.settings).toBe("default");
  });
});

describe("account.toJSON", () => {
  test("returns only the acccount's Jazz id", async () => {
    const account = await createJazzTestAccount({
      creationProps: { name: "John" },
    });

    expect(account.toJSON()).toEqual({
      $jazz: { id: account.$jazz.id },
    });
  });
});

describe("accepting invites", () => {
  test("accepting an invite to a Group", async () => {
    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
    const group = co.group().create();
    const invite = group.$jazz.createInvite("reader");
    const newAccount = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
    expect(group.getRoleOf(newAccount.$jazz.id)).toBeUndefined();
    await newAccount.acceptInvite(group.$jazz.id, invite);
    expect(group.getRoleOf(newAccount.$jazz.id)).toBe("reader");
  });
});
