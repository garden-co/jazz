// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { co, z } from "jazz-tools";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import {
  useCoValueSubscription,
  useAccountSubscription,
  useSubscriptionSelector,
} from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { act, renderHook, waitFor } from "./testUtils.js";
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

describe("useSubscriptionSelector", () => {
  it("should return coValue", () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(() => {
      const subscription = useCoValueSubscription(TestMap, map.$jazz.id);
      return useSubscriptionSelector(subscription);
    });

    expect(result.current?.value).toBe("123");
  });

  it("should resolve nested coValues", () => {
    const TestNestedMap = co.map({
      content: z.string(),
    });

    const TestMap = co.map({
      content: z.string(),
      nested: TestNestedMap,
    });

    const map = TestMap.create({
      content: "123",
      nested: {
        content: "456",
      },
    });

    const { result } = renderHook(() => {
      const subscription = useCoValueSubscription(TestMap, map.$jazz.id);
      return useSubscriptionSelector(subscription);
    });

    expect(result.current?.nested?.content).toBe("456");
  });

  it("should return null on invalid coValue id", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const { result } = renderHook(() => {
      const subscription = useCoValueSubscription(TestMap, "123");
      return useSubscriptionSelector(subscription);
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).toBeNull();
    });
  });

  it("should return coAccount", async () => {
    const account = await createJazzTestAccount();

    const { result } = renderHook(
      () => {
        const subscription = useAccountSubscription(co.account());
        return useSubscriptionSelector(subscription);
      },
      {
        account,
      },
    );

    expect(result.current?.$jazz.id).toBe(account.$jazz.id);
  });

  it("should return value from coValue with selector", () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(() => {
      const subscription = useCoValueSubscription(TestMap, map.$jazz.id);
      return useSubscriptionSelector(subscription, {
        select: (v) => v?.value,
      });
    });

    expect(result.current).toBe("123");
  });

  it("should return value from coAccount with selector", async () => {
    const account = await createJazzTestAccount({
      creationProps: { name: "test" },
    });

    const { result } = renderHook(
      () => {
        const subscription = useAccountSubscription(co.account());
        return useSubscriptionSelector(subscription, {
          select: (v) => v?.profile?.name,
        });
      },
      {
        account,
      },
    );

    expect(result.current).toBe("test");
  });

  it("should update value from coValue with selected value changes", () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(() => {
      const subscription = useCoValueSubscription(TestMap, map.$jazz.id);
      return useSubscriptionSelector(subscription, {
        select: (v) => v?.value,
      });
    });

    expect(result.current).toBe("123");

    act(() => {
      map.$jazz.set("value", "456");
    });

    expect(result.current).toBe("456");
  });

  it("should not re-render when a non-selected field is updated and not selected", () => {
    const TestMap = co.map({
      value: z.string(),
      other: z.string(),
    });

    const map = TestMap.create({
      value: "1",
      other: "1",
    });

    const { result } = renderHook(() =>
      useRenderCount(() => {
        const subscription = useCoValueSubscription(TestMap, map.$jazz.id);
        return useSubscriptionSelector(subscription, {
          select: (v) => v?.value,
        });
      }),
    );

    expect(result.current.result).toBe("1");
    expect(result.current.renderCount).toBe(1);

    act(() => {
      map.$jazz.set("other", "2");
    });

    expect(result.current.result).toBe("1");
    expect(result.current.renderCount).toBe(1);
  });

  it("should only re-render or load new value when equalityFn returns true", () => {
    const TestMap = co.map({
      value: z.number(),
      other: z.number(),
    });

    const map = TestMap.create({
      value: 1,
      other: 2,
    });

    const { result } = renderHook(() =>
      useRenderCount(() => {
        const subscription = useCoValueSubscription(TestMap, map.$jazz.id);
        return useSubscriptionSelector(subscription, {
          select: (v) =>
            v
              ? [Math.floor(v.value / 5), Math.floor(v.other / 5)].toSorted()
              : [],
          equalityFn: (a, b) => a.every((v, i) => v === b[i]),
        });
      }),
    );

    expect(result.current.result).toEqual([0, 0]);
    expect(result.current.renderCount).toBe(1);

    act(() => {
      map.$jazz.applyDiff({
        value: 3,
        other: 4,
      });
    });

    expect(result.current.result).toEqual([0, 0]);
    expect(result.current.renderCount).toBe(1);

    act(() => {
      map.$jazz.set("value", 5);
    });

    expect(result.current.result).toEqual([0, 1]);
    expect(result.current.renderCount).toBe(2);
  });
});
