// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { Account, co, CoValueLoadingState, Loaded, z } from "jazz-tools";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { useCoStateWithSelector } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { renderHook, waitFor } from "./testUtils.js";
import { useRef } from "react";

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

describe("useCoStateWithSelector", () => {
  it("should not re-render when a nested coValue is updated and not selected", async () => {
    const TestMap = co.map({
      value: z.string(),
      get nested() {
        return TestMap.optional();
      },
    });

    const map = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });

    const { result } = renderHook(() =>
      useRenderCount(() =>
        useCoStateWithSelector(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
          select: (v) => {
            if (v.$jazzState !== CoValueLoadingState.LOADED) {
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
    const TestMap = co.map({
      value: z.string(),
      get nested() {
        return TestMap.optional();
      },
    });

    const map = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });

    const { result } = renderHook(() =>
      useRenderCount(() =>
        useCoStateWithSelector(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
          select: (v) => {
            if (v.$jazzState !== CoValueLoadingState.LOADED) {
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
    const TestMap = co.map({
      value: z.string(),
      get nested() {
        return TestMap.optional();
      },
    });

    const map = TestMap.create({
      value: "1",
      nested: TestMap.create({
        value: "1",
      }),
    });

    const { result } = renderHook(() =>
      useRenderCount(() =>
        useCoStateWithSelector(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
          select: (v) => {
            if (v.$jazzState !== CoValueLoadingState.LOADED) {
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
