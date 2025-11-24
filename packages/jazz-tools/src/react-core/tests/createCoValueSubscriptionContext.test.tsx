// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { co, z, CoValueLoadingState } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { beforeEach, describe, expect, it } from "vitest";
import { createCoValueSubscriptionContext } from "../index.js";
import {
  createJazzTestAccount,
  JazzTestProvider,
  setupJazzTestSync,
} from "../testing.js";
import {
  act,
  render,
  renderHook,
  useRenderCount,
  waitFor,
} from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

cojsonInternals.setCoValueLoadingRetryDelay(300);

describe("createCoValueSubscriptionContext", () => {
  describe("useSelector", () => {
    it("creates a custom provider and selector hook for a coValue schema", () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const map = TestMap.create({
        value: "123",
      });

      const { Provider, useSelector } =
        createCoValueSubscriptionContext(TestMap);

      const { result } = renderHook(
        () => {
          return useSelector();
        },
        {
          wrapper: ({ children }) => (
            <JazzTestProvider>
              <Provider id={map.$jazz.id}>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.value).toBe("123");
    });

    it("the selector hook updates when the coValue changes", () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const map = TestMap.create({
        value: "123",
      });

      const { Provider, useSelector } =
        createCoValueSubscriptionContext(TestMap);

      const { result } = renderHook(
        () => {
          return useSelector();
        },
        {
          wrapper: ({ children }) => (
            <JazzTestProvider>
              <Provider id={map.$jazz.id}>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.value).toBe("123");

      act(() => {
        map.$jazz.set("value", "456");
      });

      expect(result.current.value).toBe("456");
    });

    it("the selector hook can be narrowed down further with a select function", () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const map = TestMap.create({
        value: "123",
      });

      const { Provider, useSelector } =
        createCoValueSubscriptionContext(TestMap);

      const { result } = renderHook(
        () => {
          return useSelector({
            select: (v) => v.value,
          });
        },
        {
          wrapper: ({ children }) => (
            <JazzTestProvider>
              <Provider id={map.$jazz.id}>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current).toBe("123");
    });

    it("should not re-render when a non-selected field changes", () => {
      const TestMap = co.map({
        value: z.string(),
        other: z.string(),
      });

      const map = TestMap.create({
        value: "1",
        other: "1",
      });

      const { Provider, useSelector } =
        createCoValueSubscriptionContext(TestMap);

      const { result } = renderHook(
        () =>
          useRenderCount(() => {
            return useSelector({
              select: (v) => v.value,
            });
          }),
        {
          wrapper: ({ children }) => (
            <JazzTestProvider>
              <Provider id={map.$jazz.id}>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.result).toBe("1");
      expect(result.current.renderCount).toBe(1);

      act(() => {
        map.$jazz.set("other", "2");
      });

      expect(result.current.result).toBe("1");
      expect(result.current.renderCount).toBe(1);
    });

    it("the provider renders a loading fallback when loading the CoValue", async () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const { Provider } = createCoValueSubscriptionContext(TestMap);

      const { container } = render(
        <JazzTestProvider>
          <Provider id="co_test123" loadingFallback={<div>Loading...</div>}>
            <div>Children should not render</div>
          </Provider>
        </JazzTestProvider>,
      );

      // The loading fallback should be rendered
      expect(container.textContent).toContain("Loading...");
      // Children should not be rendered
      expect(container.textContent).not.toContain("Children should not render");
    });

    it("the provider accepts React components as fallbacks and renders them correctly", async () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const { Provider } = createCoValueSubscriptionContext(TestMap);

      const Loading = () => <div>Loading...</div>;
      const Unavailable = () => <div>Unavailable</div>;

      const { container } = render(
        <JazzTestProvider>
          <Provider
            id="co_test123"
            loadingFallback={Loading}
            unavailableFallback={Unavailable}
          >
            <div>Children should not render</div>
          </Provider>
        </JazzTestProvider>,
      );

      // The loading fallback should be rendered
      expect(container.textContent).toContain("Loading...");
      // Children should not be rendered
      expect(container.textContent).not.toContain("Children should not render");
    });

    it("the provider shows an unavailable fallback when the coValue is unavailable", async () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const { Provider } = createCoValueSubscriptionContext(TestMap);

      const { container } = render(
        <JazzTestProvider>
          <Provider
            id="invalid_id"
            loadingFallback={<div>Loading...</div>}
            unavailableFallback={<div>Unavailable</div>}
          >
            <div>Children should not render</div>
          </Provider>
        </JazzTestProvider>,
      );

      // Initially shows loading fallback
      expect(container.textContent).toContain("Loading...");

      // Should show unavailable fallback after CoValue load timeout
      await waitFor(() => {
        expect(container.textContent).toContain("Unavailable");
      });

      // Children should never be rendered
      expect(container.textContent).not.toContain("Children should not render");
    });

    it("should throw error when useSelector is used outside provider", () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const { useSelector } = createCoValueSubscriptionContext(TestMap);

      expect(() => {
        renderHook(() => {
          return useSelector();
        });
      }).toThrow(
        "useSelector must be used within a CoValue subscription Provider",
      );
    });

    describe("with passthroughNotLoaded = true", () => {
      it("should return the loaded CoValue when available", () => {
        const TestMap = co.map({
          value: z.string(),
        });

        const map = TestMap.create({
          value: "123",
        });

        const { Provider, useSelector } = createCoValueSubscriptionContext(
          TestMap,
          true,
          {
            passthroughNotLoaded: true,
          },
        );

        const { result } = renderHook(
          () => {
            return useSelector({
              select: (v) => (v.$isLoaded ? v.value : undefined),
            });
          },
          {
            wrapper: ({ children }) => (
              <JazzTestProvider>
                <Provider id={map.$jazz.id}>{children}</Provider>
              </JazzTestProvider>
            ),
          },
        );

        expect(result.current).toBe("123");
      });

      it("should return loading state when CoValue is loading", () => {
        const TestMap = co.map({
          value: z.string(),
        });

        const { Provider, useSelector } = createCoValueSubscriptionContext(
          TestMap,
          true,
          {
            passthroughNotLoaded: true,
          },
        );

        const { result } = renderHook(
          () => {
            return useSelector({
              select: (v) => v.$jazz.loadingState,
            });
          },
          {
            wrapper: ({ children }) => (
              <JazzTestProvider>
                <Provider id="co_test123">{children}</Provider>
              </JazzTestProvider>
            ),
          },
        );

        expect(result.current).toBe(CoValueLoadingState.LOADING);
      });

      it("should return unavailable state when CoValue is not found", async () => {
        const TestMap = co.map({
          value: z.string(),
        });

        const { Provider, useSelector } = createCoValueSubscriptionContext(
          TestMap,
          true,
          {
            passthroughNotLoaded: true,
          },
        );

        const { result } = renderHook(
          () => {
            return useSelector({
              select: (v) => {
                // @ts-expect-error - must narrow to loaded type to access `v.value`
                return v?.value ?? v.$jazz.loadingState;
              },
            });
          },
          {
            wrapper: ({ children }) => (
              <JazzTestProvider>
                <Provider id="invalid_id">{children}</Provider>
              </JazzTestProvider>
            ),
          },
        );

        await waitFor(() => {
          expect(result.current).toBe(CoValueLoadingState.UNAVAILABLE);
        });
      });
    });
  });

  describe("useRef", () => {
    it("should return a ref with the correct CoValue", () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const map = TestMap.create({
        value: "123",
      });

      const { Provider, useRef: useCoValueContextRef } =
        createCoValueSubscriptionContext(TestMap);

      const { result } = renderHook(
        () => {
          return useCoValueContextRef();
        },
        {
          wrapper: ({ children }) => (
            <JazzTestProvider>
              <Provider id={map.$jazz.id}>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.current.value).toBe("123");
    });

    it("should update ref when CoValue changes", async () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const map = TestMap.create({
        value: "123",
      });

      const { Provider, useRef: useCoValueContextRef } =
        createCoValueSubscriptionContext(TestMap);

      const { result } = renderHook(
        () => {
          return useCoValueContextRef();
        },
        {
          wrapper: ({ children }) => (
            <JazzTestProvider>
              <Provider id={map.$jazz.id}>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.current.value).toBe("123");

      map.$jazz.set("value", "456");

      expect(result.current.current.value).toBe("456");
    });

    it("should contain latest value when non-selected value changes remotely", async () => {
      const TestMap = co.map({
        name: z.string(),
        value: z.string(),
      });

      const map = TestMap.create({
        name: "stable",
        value: "123",
      });

      const { Provider, useSelector, useRef } =
        createCoValueSubscriptionContext(TestMap);

      const { result } = renderHook(
        () =>
          useRenderCount(() => {
            useSelector({
              select: (v) => v.name,
            });

            return useRef();
          }),
        {
          wrapper: ({ children }) => (
            <JazzTestProvider>
              <Provider id={map.$jazz.id}>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.result.current.value).toBe("123");

      map.$jazz.set("value", "456");

      expect(result.current.result.current.value).toBe("456");

      // Should still only have rendered once
      expect(result.current.renderCount).toBe(1);
    });

    it("should not re-render when editing non-selected value through ref", async () => {
      const TestMap = co.map({
        value: z.string(),
        count: z.number(),
      });

      const map = TestMap.create({
        value: "123",
        count: 0,
      });

      const { Provider, useRef, useSelector } =
        createCoValueSubscriptionContext(TestMap);

      const { result } = renderHook(
        () =>
          useRenderCount(() => {
            return {
              state: useSelector({
                select: (v) => (v.$isLoaded ? v.value : undefined),
              }),
              ref: useRef(),
            };
          }),
        {
          wrapper: ({ children }) => (
            <JazzTestProvider>
              <Provider id={map.$jazz.id}>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.result.ref.current.value).toBe("123");
      expect(result.current.result.ref.current.count).toBe(0);
      expect(result.current.renderCount).toBe(1);

      // Edit through the ref
      result.current.result.ref.current.$jazz.applyDiff({
        value: "updated",
        count: 42,
      });

      expect(result.current.result.ref.current.value).toBe("updated");
      expect(result.current.result.ref.current.count).toBe(42);

      // Should still only have rendered once
      expect(result.current.renderCount).toBe(1);
    });

    it("should contain latest value despite not re-rendering selected value in useSelector", async () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const map = TestMap.create({
        value: "123",
      });

      const { Provider, useRef, useSelector } =
        createCoValueSubscriptionContext(TestMap);

      const { result } = renderHook(
        () =>
          useRenderCount(() => {
            return {
              state: useSelector({
                select: (v) => v.value,
                equalityFn: () => true,
              }),
              ref: useRef(),
            };
          }),
        {
          wrapper: ({ children }) => (
            <JazzTestProvider>
              <Provider id={map.$jazz.id}>{children}</Provider>
            </JazzTestProvider>
          ),
        },
      );

      expect(result.current.result.ref.current.value).toBe("123");

      act(() => {
        map.$jazz.set("value", "456");
      });

      // contains latest value despite not re-rendering
      expect(result.current.result.ref.current.value).toBe("456");

      // state still stuck at old value due to equalityFn = () => true
      expect(result.current.result.state).toBe("123");

      // Should still only have rendered once
      expect(result.current.renderCount).toBe(1);
    });

    it("should throw error when useRef is used outside provider", () => {
      const TestMap = co.map({
        value: z.string(),
      });

      const { useRef } = createCoValueSubscriptionContext(TestMap);

      expect(() => {
        renderHook(() => {
          return useRef();
        });
      }).toThrow("useRef must be used within a CoValue subscription Provider");
    });

    describe("with passthroughNotLoaded = true", () => {
      it("should return the loaded CoValue when available", () => {
        const TestMap = co.map({
          value: z.string(),
        });

        const map = TestMap.create({
          value: "123",
        });

        const { Provider, useRef } = createCoValueSubscriptionContext(
          TestMap,
          true,
          {
            passthroughNotLoaded: true,
          },
        );

        const { result } = renderHook(
          () => {
            return useRef();
          },
          {
            wrapper: ({ children }) => (
              <JazzTestProvider>
                <Provider id={map.$jazz.id}>{children}</Provider>
              </JazzTestProvider>
            ),
          },
        );

        assertLoaded(result.current.current);
        expect(result.current.current.value).toBe("123");
      });

      it("should return loading state when CoValue is loading", () => {
        const TestMap = co.map({
          value: z.string(),
        });

        const { Provider, useRef } = createCoValueSubscriptionContext(
          TestMap,
          true,
          {
            passthroughNotLoaded: true,
          },
        );

        const { result } = renderHook(
          () => {
            return useRef();
          },
          {
            wrapper: ({ children }) => (
              <JazzTestProvider>
                <Provider id="co_test123">{children}</Provider>
              </JazzTestProvider>
            ),
          },
        );

        expect(result.current.current.$jazz.loadingState).toBe(
          CoValueLoadingState.LOADING,
        );
      });

      it("should return unavailable state when CoValue is not found", async () => {
        const TestMap = co.map({
          value: z.string(),
        });

        const { Provider, useRef } = createCoValueSubscriptionContext(
          TestMap,
          true,
          {
            passthroughNotLoaded: true,
          },
        );

        const { result } = renderHook(
          () => {
            return useRef();
          },
          {
            wrapper: ({ children }) => (
              <JazzTestProvider>
                <Provider id="invalid_id">{children}</Provider>
              </JazzTestProvider>
            ),
          },
        );

        await waitFor(() => {
          expect(result.current.current.$jazz.loadingState).toBe(
            CoValueLoadingState.UNAVAILABLE,
          );
        });
      });
    });
  });
});
