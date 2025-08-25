// @vitest-environment happy-dom

import { RefsToResolve, co, z } from "jazz-tools";
import { beforeEach, describe, expect, it } from "vitest";
import { useAccount, useJazzContextManager } from "../hooks.js";
import { useIsAuthenticated } from "../index.js";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing.js";
import { act, renderHook } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();
});

describe("useAccount", () => {
  it("should return the correct value", async () => {
    const account = await createJazzTestAccount();

    const { result } = renderHook(() => useAccount(), {
      account,
    });

    expect(result.current?.me).toEqual(account);
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
        useAccount<typeof AccountSchema, RefsToResolve<{ root: true }>>(
          AccountSchema,
          {
            resolve: {
              root: true,
            },
          },
        ),
      {
        account,
      },
    );

    expect(result.current?.me?.root?.value).toBe("123");
  });

  it("should be in sync with useIsAuthenticated when logOut is called", async () => {
    const account = await createJazzTestAccount({});

    const accounts: string[] = [];
    const updates: { isAuthenticated: boolean; accountIndex: number }[] = [];

    const { result } = renderHook(
      () => {
        const isAuthenticated = useIsAuthenticated();
        const account = useAccount();

        if (account.me) {
          if (!accounts.includes(account.me.$jazz.id)) {
            accounts.push(account.me.$jazz.id);
          }

          updates.push({
            isAuthenticated,
            accountIndex: accounts.indexOf(account.me.$jazz.id),
          });
        }

        return { isAuthenticated, account };
      },
      {
        account,
        isAuthenticated: true,
      },
    );

    expect(result.current?.isAuthenticated).toBe(true);
    expect(result.current?.account?.me).toBeDefined();

    const id = result.current?.account?.me?.$jazz.id;

    await act(async () => {
      await result.current?.account?.logOut();
    });

    expect(result.current?.isAuthenticated).toBe(false);
    expect(result.current?.account?.me?.$jazz.id).not.toBe(id);

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
        const contextManager = useJazzContextManager();

        if (account.me) {
          if (!accounts.includes(account.me.$jazz.id)) {
            accounts.push(account.me.$jazz.id);
          }

          updates.push({
            isAuthenticated,
            accountIndex: accounts.indexOf(account.me.$jazz.id),
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
    expect(result.current?.account?.me).toBeDefined();

    const id = result.current?.account?.me?.$jazz.id;

    await act(async () => {
      await result.current?.contextManager?.authenticate({
        accountID: accountToAuthenticate.$jazz.id,
        accountSecret:
          accountToAuthenticate.$jazz.localNode.getCurrentAgent().agentSecret,
      });
    });

    expect(result.current?.isAuthenticated).toBe(true);
    expect(result.current?.account?.me?.$jazz.id).not.toBe(id);

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

    expect(result.current.me).toBe(null);
    expect(result.current.agent).toBe(account.guest);
  });
});
