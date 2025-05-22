// @vitest-environment happy-dom

import { RefsToResolve, co, z, zodSchemaToCoSchema } from "jazz-tools";
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
    const AccountRoot = co.map({
      value: z.string(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account._refs.root) {
          account.root = AccountRoot.create(
            { value: "123" },
            { owner: account },
          );
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema: zodSchemaToCoSchema(AccountSchema),
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

        if (account.me && !accounts.includes(account.me.id)) {
          accounts.push(account.me.id);
        }

        if (account.me) {
          updates.push({
            isAuthenticated,
            accountIndex: accounts.indexOf(account.me.id),
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

    const id = result.current?.account?.me?.id;

    await act(async () => {
      await result.current?.account?.logOut();
    });

    expect(result.current?.isAuthenticated).toBe(false);
    expect(result.current?.account?.me?.id).not.toBe(id);

    expect(updates).toMatchInlineSnapshot(`
      [
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

        if (account.me && !accounts.includes(account.me.id)) {
          accounts.push(account.me.id);
        }

        if (account.me) {
          updates.push({
            isAuthenticated,
            accountIndex: accounts.indexOf(account.me.id),
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

    const id = result.current?.account?.me?.id;

    await act(async () => {
      await result.current?.contextManager?.authenticate({
        accountID: accountToAuthenticate.id,
        accountSecret:
          accountToAuthenticate._raw.core.node.getCurrentAgent().agentSecret,
      });
    });

    expect(result.current?.isAuthenticated).toBe(true);
    expect(result.current?.account?.me?.id).not.toBe(id);

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
});
