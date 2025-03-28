// @vitest-environment happy-dom

import { Account, CoMap, RefsToResolve, Resolved, co } from "jazz-tools";
import { beforeEach, describe, expect, it } from "vitest";
import { useAccount, useJazzContextManager } from "../hooks.js";
import { useIsAuthenticated } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
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
    class AccountRoot extends CoMap {
      value = co.string;
    }

    class AccountSchema extends Account {
      root = co.ref(AccountRoot);

      migrate() {
        if (!this._refs.root) {
          this.root = AccountRoot.create({ value: "123" }, { owner: this });
        }
      }
    }

    const account = await createJazzTestAccount({ AccountSchema });

    const { result } = renderHook(
      () =>
        useAccount<AccountSchema, RefsToResolve<{ root: true }>>({
          resolve: {
            root: true,
          },
        }),
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

        if (!accounts.includes(account.me.id)) {
          accounts.push(account.me.id);
        }

        updates.push({
          isAuthenticated,
          accountIndex: accounts.indexOf(account.me.id),
        });

        return { isAuthenticated, account };
      },
      {
        account,
        isAuthenticated: true,
      },
    );

    expect(result.current?.isAuthenticated).toBe(true);
    expect(result.current?.account?.me).toBeDefined();

    const id = result.current?.account?.me?.id;

    await act(async () => {
      await result.current?.account?.logOut();
    });

    expect(result.current?.isAuthenticated).toBe(false);
    expect(result.current?.account?.me.id).not.toBe(id);

    expect(updates).toMatchInlineSnapshot(`
      [
        {
          "accountIndex": 0,
          "isAuthenticated": true,
        },
        {
          "accountIndex": 0,
          "isAuthenticated": true,
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

        if (!accounts.includes(account.me.id)) {
          accounts.push(account.me.id);
        }

        updates.push({
          isAuthenticated,
          accountIndex: accounts.indexOf(account.me.id),
        });

        return { isAuthenticated, account, contextManager };
      },
      {
        account,
        isAuthenticated: false,
      },
    );

    expect(result.current?.isAuthenticated).toBe(false);
    expect(result.current?.account?.me).toBeDefined();

    const id = result.current?.account?.me?.id;

    await act(async () => {
      await result.current?.contextManager?.authenticate({
        accountID: accountToAuthenticate.id,
        accountSecret: accountToAuthenticate._raw.core.node.account.agentSecret,
      });
    });

    expect(result.current?.isAuthenticated).toBe(true);
    expect(result.current?.account?.me.id).not.toBe(id);

    expect(updates).toMatchInlineSnapshot(`
      [
        {
          "accountIndex": 0,
          "isAuthenticated": false,
        },
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
});
