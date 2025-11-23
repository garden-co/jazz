// @vitest-environment happy-dom

import { co, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { beforeEach, describe, expect, it } from "vitest";
import { useAccountRef } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { act, renderHook, useRenderCount, waitFor } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();
});

describe("useAccountRef", () => {
  it("should return a ref with the correct account value", async () => {
    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const {
      result: { current: accountRef },
    } = renderHook(() => useAccountRef(), {
      account,
    });

    assertLoaded(accountRef.current);
    expect(accountRef.current.$jazz.id).toBe(account.$jazz.id);
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

    const {
      result: { current: accountRef },
    } = renderHook(
      () =>
        useAccountRef(AccountSchema, {
          resolve: {
            root: true,
          },
        }),
      {
        account,
      },
    );

    assertLoaded(accountRef.current);
    expect(accountRef.current.root.value).toBe("123");
  });

  it("should update the ref when the account changes", async () => {
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
          account.$jazz.set("root", { value: "initial" });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const {
      result: { current: accountRef },
    } = renderHook(
      () =>
        useAccountRef(AccountSchema, {
          resolve: {
            root: true,
          },
        }),
      {
        account,
      },
    );

    assertLoaded(accountRef.current);
    expect(accountRef.current.root.value).toBe("initial");

    act(() => {
      account.root.$jazz.set("value", "updated");
    });

    await waitFor(() => {
      assertLoaded(accountRef.current);
      expect(accountRef.current.root.value).toBe("updated");
    });
  });

  it("should not cause re-renders when the account changes", async () => {
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
          account.$jazz.set("root", { value: "initial" });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const {
      result: {
        current: { renderCount, result: accountRef },
      },
    } = renderHook(
      () =>
        useRenderCount(() =>
          useAccountRef(AccountSchema, {
            resolve: {
              root: true,
            },
          }),
        ),
      {
        account,
      },
    );

    assertLoaded(accountRef.current);
    expect(accountRef.current.root.value).toBe("initial");
    expect(renderCount).toBe(1);

    act(() => {
      account.root.$jazz.set("value", "updated");
    });

    await waitFor(() => {
      assertLoaded(accountRef.current);
      expect(accountRef.current.root.value).toBe("updated");
    });

    expect(renderCount).toBe(1);
  });

  it("should allow editing the account through the ref without causing re-renders", async () => {
    const AccountRoot = co.map({
      value: z.string(),
      count: z.number(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", { value: "initial", count: 0 });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const {
      result: {
        current: { renderCount, result: accountRef },
      },
    } = renderHook(
      () =>
        useRenderCount(() =>
          useAccountRef(AccountSchema, {
            resolve: {
              root: true,
            },
          }),
        ),
      {
        account,
      },
    );

    assertLoaded(accountRef.current);
    expect(accountRef.current.root.value).toBe("initial");
    expect(accountRef.current.root.count).toBe(0);
    expect(renderCount).toBe(1);

    act(() => {
      const current = accountRef.current;
      if (current.$isLoaded) {
        current.root.$jazz.set("value", "updated");
        current.root.$jazz.set("count", 42);
      }
    });

    await waitFor(() => {
      assertLoaded(accountRef.current);
      expect(accountRef.current.root.value).toBe("updated");
      expect(accountRef.current.root.count).toBe(42);
    });

    expect(renderCount).toBe(1);

    expect(account.root.value).toBe("updated");
    expect(account.root.count).toBe(42);
  });

  it("should provide access to $jazz object for editing without re-renders", async () => {
    const AccountRoot = co.map({
      settings: co.map({
        theme: z.string(),
        notifications: z.boolean(),
      }),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", {
            settings: {
              theme: "light",
              notifications: true,
            },
          });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const {
      result: {
        current: { renderCount, result: accountRef },
      },
    } = renderHook(
      () =>
        useRenderCount(() =>
          useAccountRef(AccountSchema, {
            resolve: {
              root: {
                settings: true,
              },
            },
          }),
        ),
      {
        account,
      },
    );

    assertLoaded(accountRef.current);
    expect(accountRef.current.root.settings.theme).toBe("light");
    expect(renderCount).toBe(1);

    act(() => {
      const current = accountRef.current;
      if (current.$isLoaded) {
        current.root.settings.$jazz.set("theme", "dark");
        current.root.settings.$jazz.set("notifications", false);
      }
    });

    await waitFor(() => {
      assertLoaded(accountRef.current);
      expect(accountRef.current.root.settings.theme).toBe("dark");
      expect(accountRef.current.root.settings.notifications).toBe(false);
    });

    expect(renderCount).toBe(1);

    expect(account.root.settings.theme).toBe("dark");
    expect(account.root.settings.notifications).toBe(false);
  });
});
