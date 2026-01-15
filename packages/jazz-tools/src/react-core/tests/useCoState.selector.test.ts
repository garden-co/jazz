// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { Account, co, z } from "jazz-tools";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { useCoState, useCoStates, useSuspenseCoStates } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { act, renderHook, waitFor } from "./testUtils.js";
import React, { Suspense, useRef } from "react";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

cojsonInternals.setCoValueLoadingRetryDelay(300);

const useRenderCount = <T>(hook: () => T) => {
  const renderCountRef = useRef(0);
  const result = hook();
  renderCountRef.current = renderCountRef.current + 1;
  return {
    renderCount: renderCountRef.current,
    result,
  };
};

const TestMap = co.map({
  value: z.string(),
  get nested() {
    return TestMap.optional();
  },
});

describe("useCoState", () => {
  it("should not re-render when a nested coValue is updated and not selected", async () => {
    const map = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });

    const { result } = renderHook(() =>
      useRenderCount(() =>
        useCoState(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
          select: (v) => {
            if (!v.$isLoaded) {
              return undefined;
            }
            return v.value;
          },
        }),
      ),
    );

    await waitFor(() => {
      expect(result.current.result).not.toBeUndefined();
    });

    for (let i = 0; i < 100; i++) {
      map.nested!.$jazz.set("value", `${i}`);
      await Account.getMe().$jazz.waitForAllCoValuesSync();
    }

    expect(result.current.result).toEqual("1");
    expect(result.current.renderCount).toEqual(1);
  });

  it("should re-render when a nested coValue is updated and selected", async () => {
    const map = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });

    const { result } = renderHook(() =>
      useRenderCount(() =>
        useCoState(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
          select: (v) => {
            if (!v.$isLoaded) {
              return undefined;
            }
            return v.nested?.value;
          },
        }),
      ),
    );

    await waitFor(() => {
      expect(result.current.result).not.toBeUndefined();
    });

    for (let i = 1; i <= 100; i++) {
      map.nested!.$jazz.set("value", `${i}`);
      await Account.getMe().$jazz.waitForAllCoValuesSync();
    }

    expect(result.current.result).toEqual("100");

    // skips re-render on i = 1, only re-renders on i = [2,100], so initial render + 99 renders = 100
    expect(result.current.renderCount).toEqual(100);

    expectTypeOf(result.current.result).toEqualTypeOf<string | undefined>();
  });

  it("should not re-render when equalityFn always returns true", async () => {
    const map = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });

    const { result } = renderHook(() =>
      useRenderCount(() =>
        useCoState(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
          select: (v) => {
            if (!v.$isLoaded) {
              return undefined;
            }
            return v.nested?.value;
          },
          equalityFn: () => true,
        }),
      ),
    );

    for (let i = 1; i <= 100; i++) {
      map.nested!.$jazz.set("value", `${i}`);
      await Account.getMe().$jazz.waitForAllCoValuesSync();
    }

    expect(result.current.result).toEqual("1");
    expect(result.current.renderCount).toEqual(1);
  });
});

describe("useCoStates", () => {
  it("should not re-render when a nested coValue is updated and not selected", async () => {
    const TestMap = co.map({
      value: z.string(),
      get nested() {
        return TestMap.optional();
      },
    });

    const map1 = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });
    const map2 = TestMap.create({
      value: "2",
      nested: TestMap.create({
        value: "2",
      }),
    });

    const { result } = renderHook(() =>
      useRenderCount(() =>
        useCoStates(TestMap, [map1.$jazz.id, map2.$jazz.id], {
          resolve: {
            nested: true,
          },
          select: (v) => {
            if (!v.$isLoaded) {
              return undefined;
            }
            return v.value;
          },
        }),
      ),
    );

    await waitFor(() => {
      expect(result.current.result).not.toBeUndefined();
    });

    for (let i = 1; i <= 100; i++) {
      map1.nested!.$jazz.set("value", `${i}`);
      map2.nested!.$jazz.set("value", `${i}`);
      await Account.getMe().$jazz.waitForAllCoValuesSync();
    }

    expect(result.current.result).toEqual(["1", "2"]);
    expect(result.current.renderCount).toEqual(1);
  });

  it("should re-render when a nested coValue is updated and selected", async () => {
    const map1 = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });
    const map2 = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });

    const { result } = renderHook(() =>
      useRenderCount(() =>
        useCoStates(TestMap, [map1.$jazz.id, map2.$jazz.id], {
          resolve: {
            nested: true,
          },
          select: (v) => {
            if (!v.$isLoaded) {
              return undefined;
            }
            return v.nested?.value;
          },
        }),
      ),
    );

    await waitFor(() => {
      expect(result.current.result).not.toBeUndefined();
    });

    for (let i = 1; i <= 100; i++) {
      map1.nested!.$jazz.set("value", `${i}`);
      map2.nested!.$jazz.set("value", `${i}`);
      await Account.getMe().$jazz.waitForAllCoValuesSync();
    }

    expect(result.current.result).toEqual(["100", "100"]);

    // skips re-render on i = 1, only re-renders on i = [2,100], so initial render + 99 renders = 100
    expect(result.current.renderCount).toEqual(100);

    expectTypeOf(result.current.result).toEqualTypeOf<(string | undefined)[]>();
  });

  it("should not re-render when equalityFn always returns true", async () => {
    const map1 = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });
    const map2 = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });

    const { result } = renderHook(() =>
      useRenderCount(() =>
        useCoStates(TestMap, [map1.$jazz.id, map2.$jazz.id], {
          resolve: {
            nested: true,
          },
          select: (v) => {
            if (!v.$isLoaded) {
              return undefined;
            }
            return v.nested?.value;
          },
          equalityFn: () => true,
        }),
      ),
    );

    for (let i = 1; i <= 100; i++) {
      map1.nested!.$jazz.set("value", `${i}`);
      map2.nested!.$jazz.set("value", `${i}`);
      await Account.getMe().$jazz.waitForAllCoValuesSync();
    }

    expect(result.current.result).toEqual(["1", "1"]);
    expect(result.current.renderCount).toEqual(1);
  });
});

describe("useSuspenseCoStates", () => {
  it("should not re-render when a nested coValue is updated and not selected", async () => {
    const map1 = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });
    const map2 = TestMap.create({
      value: "2",
      nested: TestMap.create({
        value: "2",
      }),
    });

    const wrapper = ({ children }: { children: React.ReactNode }) =>
      React.createElement(
        Suspense,
        { fallback: React.createElement("div", null, "Loading...") },
        children,
      );

    const { result } = await act(async () => {
      return renderHook(
        () =>
          useRenderCount(() =>
            useSuspenseCoStates(TestMap, [map1.$jazz.id, map2.$jazz.id], {
              resolve: {
                nested: true,
              },
              select: (v) => v.value,
            }),
          ),
        {
          wrapper,
        },
      );
    });

    await waitFor(() => {
      expect(result.current.result).not.toBeUndefined();
    });

    for (let i = 1; i <= 100; i++) {
      map1.nested!.$jazz.set("value", `${i}`);
      map2.nested!.$jazz.set("value", `${i}`);
      await Account.getMe().$jazz.waitForAllCoValuesSync();
    }

    expect(result.current.result).toEqual(["1", "2"]);
    expect(result.current.renderCount).toEqual(1);
  });

  it("should re-render when a nested coValue is updated and selected", async () => {
    const map1 = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });
    const map2 = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });

    const wrapper = ({ children }: { children: React.ReactNode }) =>
      React.createElement(
        Suspense,
        { fallback: React.createElement("div", null, "Loading...") },
        children,
      );

    const { result } = await act(async () => {
      return renderHook(
        () =>
          useRenderCount(() =>
            useSuspenseCoStates(TestMap, [map1.$jazz.id, map2.$jazz.id], {
              resolve: {
                nested: true,
              },
              select: (v) => v.nested?.value,
            }),
          ),
        {
          wrapper,
        },
      );
    });

    await waitFor(() => {
      expect(result.current.result).not.toBeUndefined();
    });

    for (let i = 1; i <= 100; i++) {
      map1.nested!.$jazz.set("value", `${i}`);
      map2.nested!.$jazz.set("value", `${i}`);
      await Account.getMe().$jazz.waitForAllCoValuesSync();
    }

    expect(result.current.result).toEqual(["100", "100"]);

    // skips re-render on i = 1, only re-renders on i = [2,100], so initial render + 99 renders = 100
    expect(result.current.renderCount).toEqual(100);

    expectTypeOf(result.current.result).toEqualTypeOf<(string | undefined)[]>();
  });

  it("should not re-render when equalityFn always returns true", async () => {
    const map1 = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });
    const map2 = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });

    const wrapper = ({ children }: { children: React.ReactNode }) =>
      React.createElement(
        Suspense,
        { fallback: React.createElement("div", null, "Loading...") },
        children,
      );

    const { result } = await act(async () => {
      return renderHook(
        () =>
          useRenderCount(() =>
            useSuspenseCoStates(TestMap, [map1.$jazz.id, map2.$jazz.id], {
              resolve: {
                nested: true,
              },
              select: (v) => v.nested?.value,
              equalityFn: () => true,
            }),
          ),
        {
          wrapper,
        },
      );
    });

    for (let i = 1; i <= 100; i++) {
      map1.nested!.$jazz.set("value", `${i}`);
      map2.nested!.$jazz.set("value", `${i}`);
      await Account.getMe().$jazz.waitForAllCoValuesSync();
    }

    expect(result.current.result).toEqual(["1", "1"]);
    expect(result.current.renderCount).toEqual(1);
  });
});
