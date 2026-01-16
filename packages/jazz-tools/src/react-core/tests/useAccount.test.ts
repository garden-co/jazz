// @vitest-environment happy-dom

import { CoValueLoadingState, Group, RefsToResolve, co, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { assert, beforeEach, describe, expect, it } from "vitest";
import { useAccount, useAgent, useJazzContext, useLogOut } from "../hooks.js";
import { useIsAuthenticated } from "../index.js";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing.js";
import { act, renderHook, waitFor } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();
});

describe("useAccount", () => {
  it("should return the correct value", async () => {
    const account = await createJazzTestAccount();

    const { result } = renderHook(() => useAccount(), {
      account,
    });

    expect(result.current).toEqual(account);
  });

  it("should load nested values if requested", async () => {
    const AccountRoot = co.map({
      value: z.string(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", { value: "123" });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useAccount(AccountSchema, {
          resolve: {
            root: true,
          },
        }),
      {
        account,
      },
    );

    assertLoaded(result.current);
    expect(result.current.root.value).toBe("123");
  });

  it("should return a 'deleted' value when a required resolved child is deleted", async () => {
    const AccountRoot = co.map({
      value: z.string(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", { value: "123" });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
      isCurrentActiveAccount: true,
    });

    const { result } = renderHook(
      () =>
        useAccount(AccountSchema, {
          resolve: {
            root: true,
          },
        }),
      {
        account,
      },
    );

    await waitFor(() => {
      assertLoaded(result.current);
      assertLoaded(result.current.root);
      expect(result.current.root.value).toBe("123");
    });

    assertLoaded(result.current);
    assertLoaded(result.current.root);
    // Capture the root reference before deletion; after deletion the account may become NotLoaded.
    const root = result.current.root;

    act(() => {
      root.$jazz.raw.core.deleteCoValue();
    });

    await waitFor(() => {
      expect(result.current.$jazz.loadingState).toBe(
        CoValueLoadingState.DELETED,
      );
    });
  });

  it("should be in sync with useIsAuthenticated when logOut is called", async () => {
    const account = await createJazzTestAccount({});

    const accounts: string[] = [];
    const updates: { isAuthenticated: boolean; accountIndex: number }[] = [];

    const { result } = renderHook(
      () => {
        const isAuthenticated = useIsAuthenticated();
        const account = useAccount();
        const logOut = useLogOut();

        if (account) {
          if (!accounts.includes(account.$jazz.id)) {
            accounts.push(account.$jazz.id);
          }

          updates.push({
            isAuthenticated,
            accountIndex: accounts.indexOf(account.$jazz.id),
          });
        }

        return { isAuthenticated, account, logOut };
      },
      {
        account,
        isAuthenticated: true,
      },
    );

    expect(result.current?.isAuthenticated).toBe(true);
    expect(result.current?.account).toBeDefined();

    const id = result.current?.account.$jazz.id;

    await act(async () => {
      await result.current?.logOut();
    });

    expect(result.current?.isAuthenticated).toBe(false);
    expect(result.current?.account?.$jazz.id).not.toBe(id);

    expect(updates).toMatchInlineSnapshot(`
      [
        {
          "accountIndex": 0,
          "isAuthenticated": true,
        },
        {
          "accountIndex": 0,
          "isAuthenticated": false,
        },
        {
          "accountIndex": 1,
          "isAuthenticated": false,
        },
      ]
    `);
  });

  it("should be in sync with useIsAuthenticated when authenticate is called", async () => {
    const accountToAuthenticate = await createJazzTestAccount({});
    const account = await createJazzTestAccount({});

    const accounts: string[] = [];
    const updates: { isAuthenticated: boolean; accountIndex: number }[] = [];

    const { result } = renderHook(
      () => {
        const isAuthenticated = useIsAuthenticated();
        const account = useAccount();
        const contextManager = useJazzContext();

        if (account) {
          if (!accounts.includes(account.$jazz.id)) {
            accounts.push(account.$jazz.id);
          }

          updates.push({
            isAuthenticated,
            accountIndex: accounts.indexOf(account.$jazz.id),
          });
        }

        return { isAuthenticated, account, contextManager };
      },
      {
        account,
        isAuthenticated: false,
      },
    );

    expect(result.current?.isAuthenticated).toBe(false);
    expect(result.current?.account).toBeDefined();

    const id = result.current?.account.$jazz.id;

    await act(async () => {
      await result.current?.contextManager?.authenticate({
        accountID: accountToAuthenticate.$jazz.id,
        accountSecret:
          accountToAuthenticate.$jazz.localNode.getCurrentAgent().agentSecret,
      });
    });

    expect(result.current?.isAuthenticated).toBe(true);
    expect(result.current?.account.$jazz.id).not.toBe(id);

    expect(updates).toMatchInlineSnapshot(`
      [
        {
          "accountIndex": 0,
          "isAuthenticated": false,
        },
        {
          "accountIndex": 1,
          "isAuthenticated": true,
        },
      ]
    `);
  });

  it("should not load nested values if the account is a guest", async () => {
    const AccountRoot = co.map({
      value: z.string(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", { value: "123" });
        }
      });

    const account = await createJazzTestGuest();

    const { result } = renderHook(
      () => {
        const me = useAccount(AccountSchema, {
          resolve: {
            root: true,
          },
        });
        const agent = useAgent();
        return { account: me, agent };
      },
      {
        account,
      },
    );

    expect(result.current.account.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAVAILABLE,
    );
    expect(result.current.agent).toBe(account.guest);
  });

  it("should use the schema's resolve query if no resolve query is provided", async () => {
    const AccountRoot = co.map({
      name: co.plainText(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", { name: "John Doe" });
        }
      })
      .resolved({
        root: { name: true },
      });

    const account = await createJazzTestAccount({
      AccountSchema,
      isCurrentActiveAccount: true,
    });

    const { result } = renderHook(() => useAccount(AccountSchema), {
      account,
    });

    await waitFor(() => {
      expect(result.current).not.toBeNull();
    });

    assertLoaded(result.current);
    expect(result.current.root.name.toUpperCase()).toBe("JOHN DOE");
  });

  it("should work with branches - create branch, edit and merge", async () => {
    const AccountRoot = co.map({
      name: z.string(),
      age: z.number(),
      email: z.string(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", {
            name: "John Doe",
            age: 30,
            email: "john@example.com",
          });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
      isCurrentActiveAccount: true,
    });

    const group = Group.create();
    group.addMember("everyone", "writer");
    // Use useAccount with the branch
    const { result } = renderHook(
      () => {
        const branchAccount = useAccount(AccountSchema, {
          resolve: {
            root: true,
          },
          unstable_branch: { name: "feature-branch" },
        });

        const mainAccount = useAccount(AccountSchema, {
          resolve: {
            root: true,
          },
        });

        return { branchAccount, mainAccount };
      },
      {
        account,
      },
    );

    await act(async () => {
      // Wait for the account to be loaded
      await new Promise((resolve) => setTimeout(resolve, 10));
    });

    expect(result.current).not.toBeNull();

    const branchAccount = result.current.branchAccount;
    const mainAccount = result.current.mainAccount;

    assertLoaded(branchAccount);
    assertLoaded(mainAccount);

    act(() => {
      branchAccount.root.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
        email: "john.smith@example.com",
      });
    });

    // Verify the branch has the changes
    expect(branchAccount.root.name).toBe("John Smith");
    expect(branchAccount.root.age).toBe(31);
    expect(branchAccount.root.email).toBe("john.smith@example.com");

    // Verify the original is unchanged
    expect(mainAccount.root.name).toBe("John Doe");
    expect(mainAccount.root.age).toBe(30);
    expect(mainAccount.root.email).toBe("john@example.com");

    // Merge the branch back
    branchAccount.root.$jazz.unstable_merge();

    // Verify the original now has the merged changes
    expect(mainAccount.root.name).toBe("John Smith");
    expect(mainAccount.root.age).toBe(31);
    expect(mainAccount.root.email).toBe("john.smith@example.com");
  });
});
