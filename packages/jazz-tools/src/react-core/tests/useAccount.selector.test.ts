// @vitest-environment happy-dom

import { Account, co, z, Group } from "jazz-tools";
import { assert, beforeEach, describe, expect, it } from "vitest";
import { useAccount } from "../hooks.js";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing.js";
import { act, renderHook } from "./testUtils.js";
import { useRef } from "react";

beforeEach(async () => {
  await setupJazzTestSync();
});

const useRenderCount = <T>(hook: () => T) => {
  const renderCountRef = useRef(0);
  const result = hook();
  renderCountRef.current = renderCountRef.current + 1;
  return {
    renderCount: renderCountRef.current,
    result,
  };
};

describe("useAccount", () => {
  it("should return the correct selected value", async () => {
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
          select: (account) => {
            if (!account.$isLoaded) {
              return "Loading...";
            }
            return account.root.value;
          },
        }),
      {
        account,
      },
    );

    expect(result.current).toBe("123");
  });

  it("should load nested values if requested", async () => {
    const AccountRoot = co.map({
      value: z.string(),
      nested: co.map({
        nestedValue: z.string(),
      }),
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          const root = AccountRoot.create({
            value: "123",
            nested: co
              .map({
                nestedValue: z.string(),
              })
              .create({
                nestedValue: "456",
              }),
          });
          account.$jazz.set("root", root);
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useAccount(AccountSchema, {
          resolve: {
            root: {
              nested: true,
            },
          },
          select: (account) => {
            if (!account.$isLoaded) {
              return "Loading...";
            }
            return account.root.nested.nestedValue;
          },
        }),
      {
        account,
      },
    );

    expect(result.current).toBe("456");
  });

  it("should not re-render when a nested coValue is updated and not selected", async () => {
    const AccountRoot = co.map({
      value: z.string(),
      get nested() {
        return co
          .map({
            nestedValue: z.string(),
          })
          .optional();
      },
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          const root = AccountRoot.create({
            value: "1",
            nested: co
              .map({
                nestedValue: z.string(),
              })
              .create({
                nestedValue: "1",
              }),
          });
          account.$jazz.set("root", root);
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useAccount(AccountSchema, {
            resolve: {
              root: {
                nested: true,
              },
            },
            select: (account) => {
              if (!account.$isLoaded) {
                return "Loading...";
              }
              return account.root.value;
            },
          }),
        ),
      {
        account,
      },
    );

    await act(async () => {
      // Update nested value that is not selected
      account.root.nested?.$jazz.set("nestedValue", "100");
      await account.$jazz.waitForAllCoValuesSync();
    });

    expect(result.current.result).toEqual("1");
    expect(result.current.renderCount).toEqual(1);
  });

  it("should re-render when a nested coValue is updated and selected", async () => {
    const AccountRoot = co.map({
      value: z.string(),
      get nested() {
        return co
          .map({
            nestedValue: z.string(),
          })
          .optional();
      },
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          const root = AccountRoot.create({
            value: "1",
            nested: co
              .map({
                nestedValue: z.string(),
              })
              .create({
                nestedValue: "1",
              }),
          });
          account.$jazz.set("root", root);
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useAccount(AccountSchema, {
            resolve: {
              root: {
                nested: true,
              },
            },
            select: (account) => {
              if (!account.$isLoaded) {
                return "Loading...";
              }
              return account.root.nested?.nestedValue ?? "Loading...";
            },
          }),
        ),
      {
        account,
      },
    );

    await act(async () => {
      // Update nested value that is selected
      account.root?.nested?.$jazz.set("nestedValue", "100");
      await account.$jazz.waitForAllCoValuesSync();
    });

    expect(result.current.result).toEqual("100");
    expect(result.current.renderCount).toEqual(2); // Initial render + update
  });

  it("should not re-render when equalityFn always returns true", async () => {
    const AccountRoot = co.map({
      value: z.string(),
      get nested() {
        return co
          .map({
            nestedValue: z.string(),
          })
          .optional();
      },
    });

    const AccountSchema = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.root) {
          const root = AccountRoot.create({
            value: "1",
            nested: co
              .map({
                nestedValue: z.string(),
              })
              .create({
                nestedValue: "1",
              }),
          });
          account.$jazz.set("root", root);
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useAccount(AccountSchema, {
            resolve: {
              root: {
                nested: true,
              },
            },
            select: (account) => {
              if (!account.$isLoaded) {
                return "Loading...";
              }
              return account.root.nested?.nestedValue ?? "Loading...";
            },
            equalityFn: () => true, // Always return true to prevent re-renders
          }),
        ),
      {
        account,
      },
    );

    await act(async () => {
      // Update nested value that is selected
      account.root?.nested?.$jazz.set("nestedValue", "100");
      await account.$jazz.waitForAllCoValuesSync();
    });

    expect(result.current.result).toEqual("1"); // Should still be "1" due to equalityFn
    expect(result.current.renderCount).toEqual(1); // Should not re-render
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
          select: (account) => {
            if (!account.$isLoaded) {
              return "Guest";
            }
            return account.root.value;
          },
        }),
      {
        account,
      },
    );

    expect(result.current).toBe("Guest");
  });

  it("should handle undefined account gracefully", async () => {
    const account = await createJazzTestGuest();

    const { result } = renderHook(
      () =>
        useAccount(Account, {
          select: (account) => {
            if (!account.$isLoaded) {
              return "No account";
            }
            return account.$jazz.id;
          },
        }),
      {
        account,
      },
    );

    expect(result.current).toBe("No account");
  });

  it("should re-render when selector result changes due to external prop changes", async () => {
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

    let externalProp = "suffix1";

    const { result, rerender } = renderHook(
      () =>
        useRenderCount(() =>
          useAccount(AccountSchema, {
            resolve: {
              root: true,
            },
            select: (account) => {
              let baseValue: string;
              if (!account.$isLoaded) {
                baseValue = "loading";
              } else {
                baseValue = account.root.value;
              }
              return `${baseValue}-${externalProp}`;
            },
          }),
        ),
      {
        account,
      },
    );

    expect(result.current.result).toEqual("initial-suffix1");
    expect(result.current.renderCount).toEqual(1);

    // Change external prop and rerender
    externalProp = "suffix2";
    rerender();

    expect(result.current.result).toEqual("initial-suffix2");
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
    const { result } = renderHook(
      () => {
        const branchAccountRoot = useAccount(AccountSchema, {
          resolve: {
            root: true,
          },
          select: (account) => {
            if (!account.$isLoaded) {
              return undefined;
            }
            return account.root;
          },
          unstable_branch: { name: "feature-branch" },
        });

        const mainAccountRoot = useAccount(AccountSchema, {
          resolve: {
            root: true,
          },
          select: (account) => {
            if (!account.$isLoaded) {
              return undefined;
            }
            return account.root;
          },
        });

        return {
          branchAccountRoot,
          mainAccountRoot,
        };
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

    const branchAccountRoot = result.current.branchAccountRoot;
    const mainAccountRoot = result.current.mainAccountRoot;

    assert(branchAccountRoot);
    assert(mainAccountRoot);

    act(() => {
      branchAccountRoot.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
        email: "john.smith@example.com",
      });
    });

    // Verify the branch has the changes
    expect(branchAccountRoot.name).toBe("John Smith");
    expect(branchAccountRoot.age).toBe(31);
    expect(branchAccountRoot.email).toBe("john.smith@example.com");

    // Verify the original is unchanged
    expect(mainAccountRoot.name).toBe("John Doe");
    expect(mainAccountRoot.age).toBe(30);
    expect(mainAccountRoot.email).toBe("john@example.com");

    // Merge the branch back
    branchAccountRoot.$jazz.unstable_merge();

    // Verify the original now has the merged changes
    expect(mainAccountRoot.name).toBe("John Smith");
    expect(mainAccountRoot.age).toBe(31);
    expect(mainAccountRoot.email).toBe("john.smith@example.com");
  });
});
