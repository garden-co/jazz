// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { co, z, CoValueLoadingState } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { useRef } from "react";
import { beforeEach, describe, expect, it } from "vitest";
import { createCoValueSubscriptionContext } from "../index.js";
import {
  createJazzTestAccount,
  JazzTestProvider,
  setupJazzTestSync,
} from "../testing.js";
import { act, render, renderHook, waitFor } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

cojsonInternals.setCoValueLoadingRetryDelay(300);

const useRenderCount = <T,>(hook: () => T) => {
  const renderCountRef = useRef(0);
  const result = hook();
  renderCountRef.current = renderCountRef.current + 1;
  return {
    renderCount: renderCountRef.current,
    result,
  };
};

describe("createCoValueSubscriptionContext", () => {
  it("creates a custom provider and selector hook for a coValue schema", () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "123",
    });

    const { Provider, useSelector } = createCoValueSubscriptionContext(TestMap);

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

    const { Provider, useSelector } = createCoValueSubscriptionContext(TestMap);

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

    const { Provider, useSelector } = createCoValueSubscriptionContext(TestMap);

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

    const { Provider, useSelector } = createCoValueSubscriptionContext(TestMap);

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

  it("the provider shows a loading fallback when loading the coValue", async () => {
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
      "useSelector must be used within a coValue subscription provider",
    );
  });
});
