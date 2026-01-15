// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { Account, Group, Loaded, co, getJazzErrorType, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import React, { Suspense } from "react";
import { useSuspenseAccount, useLogOut } from "../hooks.js";
import {
  createJazzTestAccount,
  createJazzTestGuest,
  setupJazzTestSync,
} from "../testing.js";
import { act, render, renderHook, waitFor } from "./testUtils.js";
import { ErrorBoundary } from "react-error-boundary";

// Silence unhandled rejection errors coming from Suspense
process.on("unhandledRejection", () => {});

beforeEach(async () => {
  await setupJazzTestSync({
    asyncPeers: true,
  });

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

cojsonInternals.setCoValueLoadingRetryDelay(10);

function ErrorFallback(props: { error: Error }) {
  return <div>Error: {getJazzErrorType(props.error)}</div>;
}

describe("useSuspenseAccount", () => {
  it("should return loaded account without suspending when data is available", async () => {
    const AccountRoot = co.map({
      projects: co.list(
        co.map({
          name: z.string(),
          description: z.string(),
        }),
      ),
    });

    const MyAppAccount = co
      .account({
        profile: co.profile({
          name: z.string(),
        }),
        root: AccountRoot,
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.profile) {
          account.$jazz.set("profile", {
            name: creationProps?.name || "John Doe",
          });
        }
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", {
            projects: [],
          });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema: MyAppAccount,
      isCurrentActiveAccount: true,
      creationProps: {
        name: "John Doe",
      },
    });

    let suspenseTriggered = false;

    const SuspenseFallback = () => {
      suspenseTriggered = true;
      return <div>Loading...</div>;
    };

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<SuspenseFallback />}>{children}</Suspense>
    );

    const { result } = renderHook(
      () =>
        useSuspenseAccount(MyAppAccount, {
          resolve: {
            profile: true,
            root: {
              projects: true,
            },
          },
        }),
      {
        account,
        wrapper,
      },
    );

    // Wait for any async operations to complete
    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    // Verify Suspense was not triggered since data was immediately available
    expect(suspenseTriggered).toBe(false);

    // Verify the hook returns loaded data
    assertLoaded(result.current);
    expect(result.current.profile.name).toBe("John Doe");
    expect(result.current.root.projects).toEqual([]);
  });

  it("should have Loaded<A> return type", async () => {
    const AccountRoot = co.map({
      value: z.string(),
    });

    const MyAppAccount = co
      .account({
        profile: co.profile({
          name: z.string(),
        }),
        root: AccountRoot,
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.profile) {
          account.$jazz.set("profile", {
            name: creationProps?.name || "Test User",
          });
        }
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", {
            value: "test",
          });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema: MyAppAccount,
      isCurrentActiveAccount: true,
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>{children}</Suspense>
    );

    const { result } = renderHook(() => useSuspenseAccount(MyAppAccount), {
      account,
      wrapper,
    });

    await waitFor(() => {
      expect(result.current).toBeDefined();
    });

    // Verify the return type is Loaded<typeof MyAppAccount>
    expectTypeOf(result.current).toEqualTypeOf<Loaded<typeof MyAppAccount>>();
  });

  it("should suspend when account data is not immediately available", async () => {
    const Project = co.map({
      name: z.string(),
      description: z.string(),
    });

    const AccountRoot = co.map({
      projects: co.list(Project),
    });

    const root = AccountRoot.create(
      {
        projects: [
          {
            name: "My Project",
            description: "A test project",
          },
        ],
      },
      Group.create().makePublic(),
    );

    const MyAppAccount = co.account({
      profile: co.profile({
        name: z.string(),
      }),
      root: AccountRoot,
    });

    const account = await createJazzTestAccount({
      AccountSchema: MyAppAccount,
      isCurrentActiveAccount: true,
      creationProps: {
        name: "John Doe",
      },
    });

    account.$jazz.set("root", root);

    let suspenseTriggered = false;

    const SuspenseFallback = () => {
      suspenseTriggered = true;
      return <div>Loading...</div>;
    };

    const TestComponent = () => {
      const account = useSuspenseAccount(MyAppAccount, {
        resolve: {
          root: {
            projects: {
              $each: true,
            },
          },
        },
      });
      return <div>{account.root.projects[0]?.name || "No project"}</div>;
    };

    const { container } = await act(async () => {
      return render(
        <Suspense fallback={<SuspenseFallback />}>
          <TestComponent />
        </Suspense>,
        {
          account,
        },
      );
    });

    expect(suspenseTriggered).toBe(true);

    // Wait for data to load - the subscription should update and resolve
    await waitFor(() => {
      expect(container.textContent).toContain("My Project");
      expect(container.textContent).not.toContain("Loading...");
    });
  });

  it("should throw error when a required resolved child is deleted", async () => {
    const AccountRoot = co.map({
      value: z.string(),
    });

    const MyAppAccount = co
      .account({
        profile: co.profile({
          name: z.string(),
        }),
        root: AccountRoot,
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.profile) {
          account.$jazz.set("profile", {
            name: creationProps?.name || "John Doe",
          });
        }
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", {
            value: "123",
          });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema: MyAppAccount,
      isCurrentActiveAccount: true,
      creationProps: {
        name: "John Doe",
      },
    });

    // Ensure root exists, then delete it.
    const loaded = await account.$jazz.ensureLoaded({
      resolve: { root: true },
    });
    loaded.root.$jazz.raw.core.deleteCoValue();

    const TestComponent = () => {
      const account = useSuspenseAccount(MyAppAccount, {
        resolve: {
          root: true,
          profile: true,
        },
      });
      return <div>{account.profile.name}</div>;
    };

    const { container } = await act(async () => {
      return render(
        <ErrorBoundary FallbackComponent={ErrorFallback}>
          <Suspense fallback={<div>Loading...</div>}>
            <TestComponent />
          </Suspense>
        </ErrorBoundary>,
        { account },
      );
    });

    await waitFor(
      () => {
        expect(container.textContent).toContain("Error: deleted");
      },
      { timeout: 10_000 },
    );
  });

  it("should throw error for anonymous agent", async () => {
    const MyAppAccount = co.account({
      profile: co.profile({
        name: z.string(),
      }),
      root: co.map({
        value: z.string(),
      }),
    });

    const guestAccount = await createJazzTestGuest();

    const TestComponent = () => {
      useSuspenseAccount(MyAppAccount);
      return <div>Should not render</div>;
    };

    const { container } = await act(async () => {
      return render(
        <ErrorBoundary FallbackComponent={ErrorFallback}>
          <Suspense fallback={<div>Loading...</div>}>
            <TestComponent />
          </Suspense>
        </ErrorBoundary>,
        {
          account: guestAccount,
        },
      );
    });

    // Verify error is displayed in error boundary
    await waitFor(
      () => {
        expect(container.textContent).toContain("Error: unknown");
      },
      { timeout: 1000 },
    );
  });

  it("should handle account logout", async () => {
    const MyAppAccount = co
      .account({
        profile: co.profile({
          name: z.string(),
        }),
        root: co.map({
          value: z.string(),
        }),
      })
      .withMigration((account, creationProps) => {
        if (!account.$jazz.refs.profile) {
          account.$jazz.set("profile", {
            name: creationProps?.name || "John Doe",
          });
        }
        if (!account.$jazz.refs.root) {
          account.$jazz.set("root", {
            value: "test",
          });
        }
      });

    const account = await createJazzTestAccount({
      AccountSchema: MyAppAccount,
      isCurrentActiveAccount: true,
      creationProps: {
        name: "John Doe",
      },
    });

    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <Suspense fallback={<div>Loading...</div>}>
        <ErrorBoundary FallbackComponent={ErrorFallback}>
          {children}
        </ErrorBoundary>
      </Suspense>
    );

    const { result } = renderHook(
      () => {
        const account = useSuspenseAccount(MyAppAccount);
        const logOut = useLogOut();
        return { account, logOut };
      },
      {
        account,
        wrapper,
      },
    );

    // Wait for account to load
    await waitFor(() => {
      expect(result.current.account).toBeDefined();
    });

    // Verify initial account data
    assertLoaded(result.current.account);
    const initialAccountId = result.current.account.$jazz.id;

    // Logout should cause an error since useSuspenseAccount requires authentication
    await act(async () => {
      result.current.logOut();
    });

    expect(result.current.account.$jazz.id).not.toBe(initialAccountId);
  });
});
