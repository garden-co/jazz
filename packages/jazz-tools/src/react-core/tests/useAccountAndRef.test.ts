// @vitest-environment happy-dom

import { co, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { beforeEach, describe, expect, it } from "vitest";
import { useAccountAndRef } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { act, renderHook, useRenderCount, waitFor } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();
});

describe("useAccountAndRef", () => {
  it("should return state and ref with the correct account values", async () => {
    const AccountRoot = co.map({
      name: z.string(),
      count: z.number(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", { name: "test", count: 42 });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useAccountAndRef(AccountSchema, {
          resolve: {
            root: true,
          },
        }),
      {
        account,
      },
    );

    assertLoaded(result.current[0]);
    expect(result.current[0].root.name).toBe("test");
    expect(result.current[0].root.count).toBe(42);

    assertLoaded(result.current[1].current);
    expect(result.current[1].current.root.name).toBe("test");
    expect(result.current[1].current.root.count).toBe(42);
  });

  it("should only re-render when selected field changes", async () => {
    const AccountRoot = co.map({
      name: z.string(),
      loginCount: z.number(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", { name: "User", loginCount: 0 });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useAccountAndRef(AccountSchema, {
            resolve: {
              root: true,
            },
            select: (acc) => (acc.$isLoaded ? acc.root.name : undefined),
          }),
        ),
      {
        account,
      },
    );

    expect(result.current.renderCount).toBe(1);
    expect(result.current.result[0]).toBe("User");

    // Update field NOT in selector - should not re-render
    act(() => {
      account.root.$jazz.set("loginCount", 5);
    });

    await waitFor(() => {
      assertLoaded(result.current.result[1].current);
      expect(result.current.result[1].current.root.loginCount).toBe(5);
    });

    expect(result.current.renderCount).toBe(1);

    // Update field in selector - should re-render
    act(() => {
      account.root.$jazz.set("name", "Updated User");
    });

    await waitFor(() => {
      expect(result.current.result[0]).toBe("Updated User");
    });

    expect(result.current.renderCount).toBe(2);
  });

  it("should allow editing non-selected fields through ref without re-rendering", async () => {
    const AccountRoot = co.map({
      displayName: z.string(),
      lastLoginAt: z.string().optional(),
      loginCount: z.number(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", {
            displayName: "John",
            loginCount: 0,
          });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useAccountAndRef(AccountSchema, {
            resolve: {
              root: true,
            },
            select: (acc) => (acc.$isLoaded ? acc.root.displayName : undefined),
          }),
        ),
      {
        account,
      },
    );

    expect(result.current.renderCount).toBe(1);
    expect(result.current.result[0]).toBe("John");

    // Edit fields NOT in selector using ref.$jazz - no re-render
    act(() => {
      assertLoaded(result.current.result[1].current);
      result.current.result[1].current.root.$jazz.set("loginCount", 10);
      result.current.result[1].current.root.$jazz.set(
        "lastLoginAt",
        "2024-01-01",
      );
    });

    await waitFor(() => {
      assertLoaded(result.current.result[1].current);
      expect(result.current.result[1].current.root.loginCount).toBe(10);
      expect(result.current.result[1].current.root.lastLoginAt).toBe(
        "2024-01-01",
      );
    });

    expect(result.current.renderCount).toBe(1);
    expect(account.root.loginCount).toBe(10);
    expect(account.root.lastLoginAt).toBe("2024-01-01");
  });

  it("should support selecting account ID (which never changes)", async () => {
    const AccountRoot = co.map({
      settings: co.map({
        theme: z.string(),
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
            settings: { theme: "light" },
          });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useAccountAndRef(AccountSchema, {
            resolve: {
              root: {
                settings: true,
              },
            },
            select: (acc) => acc.$jazz.id,
          }),
        ),
      {
        account,
      },
    );

    expect(result.current.renderCount).toBe(1);
    expect(result.current.result[0]).toBe(account.$jazz.id);

    // Update settings - should not re-render since ID never changes
    act(() => {
      account.root.settings.$jazz.set("theme", "dark");
    });

    await waitFor(() => {
      assertLoaded(result.current.result[1].current);
      expect(result.current.result[1].current.root.settings.theme).toBe("dark");
    });

    expect(result.current.renderCount).toBe(1);
  });

  it("should allow using ref.$jazz for permissions without re-rendering", async () => {
    const AccountRoot = co.map({
      name: z.string(),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", { name: "User" });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useAccountAndRef(AccountSchema, {
            resolve: {
              root: true,
            },
            select: (acc) => (acc.$isLoaded ? acc.root.name : undefined),
          }),
        ),
      {
        account,
      },
    );

    expect(result.current.renderCount).toBe(1);
    expect(result.current.result[0]).toBe("User");

    // Access $jazz API through ref - no re-render
    act(() => {
      assertLoaded(result.current.result[1].current);
      expect(result.current.result[1].current.$jazz.id).toBeDefined();
    });

    expect(result.current.renderCount).toBe(1);
  });

  it("should support complex selector with nested data", async () => {
    const AccountRoot = co.map({
      profile: co.map({
        firstName: z.string(),
        lastName: z.string(),
      }),
      metadata: co.map({
        createdAt: z.string(),
        lastActive: z.string(),
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
            profile: {
              firstName: "John",
              lastName: "Doe",
            },
            metadata: {
              createdAt: "2024-01-01",
              lastActive: "2024-01-01",
            },
          });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useAccountAndRef(AccountSchema, {
            resolve: {
              root: {
                profile: true,
                metadata: true,
              },
            },
            select: (acc) =>
              acc.$isLoaded
                ? `${acc.root.profile.firstName} ${acc.root.profile.lastName}`
                : undefined,
          }),
        ),
      {
        account,
      },
    );

    expect(result.current.renderCount).toBe(1);
    expect(result.current.result[0]).toBe("John Doe");

    // Update metadata (not in selector) - no re-render
    act(() => {
      account.root.metadata.$jazz.set("lastActive", "2024-01-02");
    });

    await waitFor(() => {
      assertLoaded(result.current.result[1].current);
      expect(result.current.result[1].current.root.metadata.lastActive).toBe(
        "2024-01-02",
      );
    });

    expect(result.current.renderCount).toBe(1);

    // Update firstName (in selector) - should re-render
    act(() => {
      account.root.profile.$jazz.set("firstName", "Jane");
    });

    await waitFor(() => {
      expect(result.current.result[0]).toBe("Jane Doe");
    });

    expect(result.current.renderCount).toBe(2);
  });
});
