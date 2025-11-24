// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { co, z, CoValueLoadingState, Account } from "jazz-tools";
import { beforeEach, describe, expect, it } from "vitest";
import { createAccountSubscriptionContext } from "../index.js";
import {
  createJazzTestAccount,
  JazzTestProvider,
  setupJazzTestSync,
  createJazzTestGuest,
} from "../testing.js";
import { act, render, renderHook, useRenderCount } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

cojsonInternals.setCoValueLoadingRetryDelay(300);

describe("createAccountSubscriptionContext", () => {
  describe("useSelector", () => {
    it("creates a custom provider and selector hook for a coValue schema", async () => {
      const account = await createJazzTestAccount();

      const { Provider, useSelector } = createAccountSubscriptionContext();

      const { result } = renderHook(
        () => {
          return useSelector({
            select: (account) => account.$jazz.id,
          });
        },
        {
          wrapper: ({ children }) => (
            <JazzTestProvider account={account} isAuthenticated>
              <Provider>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current).toBe(account.$jazz.id);
    });

    it("the selector hook updates when account deeply loaded values change", async () => {
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
      });

      const { Provider, useSelector } = createAccountSubscriptionContext(
        AccountSchema,
        {
          root: true,
        },
      );

      const { result } = renderHook(
        () => {
          return useSelector({
            select: (account) => account.root.value,
          });
        },
        {
          wrapper: ({ children }) => (
            <JazzTestProvider account={account} isAuthenticated>
              <Provider>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current).toBe("123");

      act(() => {
        account.root.$jazz.set("value", "456");
      });

      expect(result.current).toBe("456");
    });

    it("should not render children when not authenticated", async () => {
      const guest = await createJazzTestGuest();

      const { Provider } = createAccountSubscriptionContext();

      const { result } = renderHook(
        () => {
          return "hello";
        },
        {
          wrapper: ({ children }) => (
            <JazzTestProvider account={guest}>
              <Provider unavailableFallback={<div>Unavailable</div>}>
                {children}
              </Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current).toBe(null);
    });

    it("should render unavailable fallback when not authenticated", async () => {
      const guest = await createJazzTestGuest();

      const { Provider } = createAccountSubscriptionContext();

      const { container } = render(
        <Provider unavailableFallback={<div>Unavailable</div>}>
          <div>Children should not render</div>
        </Provider>,
        {
          account: guest,
        },
      );

      expect(container.textContent).toContain("Unavailable");
      expect(container.textContent).not.toContain("Children should not render");
    });

    it("should throw error when useSelector is used outside provider", () => {
      const { useSelector } = createAccountSubscriptionContext();

      expect(() => {
        renderHook(() => {
          return useSelector();
        });
      }).toThrow(
        "useSelector must be used within an account subscription Provider",
      );
    });

    describe("with passthroughNotLoaded = true", () => {
      it("should return the loaded CoValue when available", async () => {
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
        });

        const { Provider, useSelector } = createAccountSubscriptionContext(
          AccountSchema,
          { root: true },
          { passthroughNotLoaded: true },
        );

        const { result } = renderHook(
          () => {
            return useSelector({
              select: (account) =>
                account.$isLoaded ? account.root.value : undefined,
            });
          },
          {
            wrapper: ({ children }) => (
              <JazzTestProvider account={account} isAuthenticated>
                <Provider>{children}</Provider>
              </JazzTestProvider>
            ),
          },
        );

        expect(result.current).toBe("123");
      });

      it("should return unavailable state when not authenticated", async () => {
        const guest = await createJazzTestGuest();

        const { Provider, useSelector } = createAccountSubscriptionContext(
          Account,
          true,
          { passthroughNotLoaded: true },
        );

        const { result } = renderHook(
          () => {
            return useSelector({
              select: (account) => account.$jazz.loadingState,
            });
          },
          {
            wrapper: ({ children }) => (
              <JazzTestProvider account={guest}>
                <Provider>{children}</Provider>
              </JazzTestProvider>
            ),
          },
        );

        expect(result.current).toBe(CoValueLoadingState.UNAVAILABLE);
      });
    });
  });

  describe("useRef", () => {
    it("should return a ref with the correct account", async () => {
      const account = await createJazzTestAccount();

      const { Provider, useRef } = createAccountSubscriptionContext();

      const { result } = renderHook(
        () => {
          return useRef();
        },
        {
          wrapper: ({ children }) => (
            <JazzTestProvider account={account} isAuthenticated>
              <Provider>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.current.$jazz.id).toBe(account.$jazz.id);
    });

    it("should update ref when account data changes", async () => {
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
      });

      const { Provider, useRef } = createAccountSubscriptionContext(
        AccountSchema,
        {
          root: true,
        },
      );

      const { result } = renderHook(
        () => {
          return useRef();
        },
        {
          wrapper: ({ children }) => (
            <JazzTestProvider account={account} isAuthenticated>
              <Provider>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.current.root.value).toBe("123");

      // not running through act since updating a ref is not tied to the rendering pipeline
      account.root.$jazz.set("value", "456");

      expect(result.current.current.root.value).toBe("456");
    });

    it("should contain latest value when non-selected value changes", async () => {
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
            account.$jazz.set("root", { name: "stable", count: 0 });
          }
        });

      const account = await createJazzTestAccount({
        AccountSchema,
      });

      const { Provider, useSelector, useRef } =
        createAccountSubscriptionContext(AccountSchema, {
          root: true,
        });

      const { result } = renderHook(
        () =>
          useRenderCount(() => {
            useSelector({
              select: (acc) => acc.root.name,
            });

            return useRef();
          }),
        {
          wrapper: ({ children }) => (
            <JazzTestProvider account={account} isAuthenticated>
              <Provider>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.result.current.root.count).toBe(0);

      act(() => {
        account.root.$jazz.set("count", 42);
      });

      expect(result.current.result.current.root.count).toBe(42);

      // Should still only have rendered once
      expect(result.current.renderCount).toBe(1);
    });

    it("should not re-render when editing non-selected value through ref", async () => {
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
            account.$jazz.set("root", { name: "User", count: 0 });
          }
        });

      const account = await createJazzTestAccount({
        AccountSchema,
      });

      const { Provider, useRef, useSelector } =
        createAccountSubscriptionContext(AccountSchema, {
          root: true,
        });

      const { result } = renderHook(
        () =>
          useRenderCount(() => {
            return {
              state: useSelector({
                select: (acc) => acc.root.name,
              }),
              ref: useRef(),
            };
          }),
        {
          wrapper: ({ children }) => (
            <JazzTestProvider account={account} isAuthenticated>
              <Provider>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.result.ref.current.root.name).toBe("User");
      expect(result.current.result.ref.current.root.count).toBe(0);
      expect(result.current.renderCount).toBe(1);

      // Edit through the ref
      act(() => {
        result.current.result.ref.current.root.$jazz.set("count", 42);
      });

      expect(result.current.result.ref.current.root.count).toBe(42);

      // Should still only have rendered once
      expect(result.current.renderCount).toBe(1);
    });

    it("should re-render when editing selected value through ref", async () => {
      const account = await createJazzTestAccount({
        creationProps: {
          name: "Earl",
        },
      });

      const { Provider, useRef, useSelector } =
        createAccountSubscriptionContext(Account, {
          profile: true,
        });

      const { result } = renderHook(
        () =>
          useRenderCount(() => {
            return {
              state: useSelector({
                select: (acc) => acc.profile.name,
              }),
              ref: useRef(),
            };
          }),
        {
          wrapper: ({ children }) => (
            <JazzTestProvider account={account} isAuthenticated>
              <Provider>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.result.ref.current.profile.name).toBe("Earl");
      expect(result.current.renderCount).toBe(1);

      // Edit through the ref
      act(() => {
        result.current.result.ref.current.profile.$jazz.set("name", "Bob");
      });

      expect(result.current.result.ref.current.profile.name).toBe("Bob");
      expect(result.current.result.state).toBe("Bob");

      // should render one more time for the changed name
      expect(result.current.renderCount).toBe(2);
    });

    it("should throw error when useRef is used outside provider", () => {
      const { useRef } = createAccountSubscriptionContext();

      expect(() => {
        renderHook(() => {
          return useRef();
        });
      }).toThrow("useRef must be used within an account subscription Provider");
    });
  });
});
