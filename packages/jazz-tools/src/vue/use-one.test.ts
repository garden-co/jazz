import { beforeEach, describe, expect, it, vi } from "vitest";
import { effectScope } from "vue";

const mocks = vi.hoisted(() => {
  const unsubscribe = vi.fn();
  const subscribe = vi.fn(() => unsubscribe);
  const makeQueryKey = vi.fn(() => "test-key");
  const getCacheEntry = vi.fn(() => ({
    state: { status: "fulfilled", data: [] },
    subscribe,
  }));

  return {
    makeQueryKey,
    getCacheEntry,
    subscribe,
    unsubscribe,
    reset() {
      makeQueryKey.mockReset().mockReturnValue("test-key");
      getCacheEntry.mockReset().mockReturnValue({
        state: { status: "fulfilled", data: [] },
        subscribe: subscribe.mockReset().mockReturnValue(unsubscribe.mockReset()),
      });
    },
  };
});

vi.mock("./provider.js", () => ({
  useJazzClient: () => ({
    manager: {
      makeQueryKey: mocks.makeQueryKey,
      getCacheEntry: mocks.getCacheEntry,
    },
  }),
}));

import { useOne, useOneSuspense } from "./use-one.js";

function makeQuery(marker = "todos") {
  return { _build: () => `{"table":"${marker}"}`, _table: marker } as any;
}

describe("vue/useOne", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("subscribes with the query limited to one row", () => {
    const scope = effectScope();
    scope.run(() => useOne(makeQuery()));

    expect(mocks.makeQueryKey).toHaveBeenCalledTimes(1);
    const limitedQuery = (mocks.makeQueryKey.mock.calls[0] as any)[0];
    expect(JSON.parse(limitedQuery._build())).toEqual({ table: "todos", limit: 1 });
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    scope.stop();
  });

  it("forwards QueryOptions to makeQueryKey", () => {
    const scope = effectScope();
    scope.run(() => useOne(makeQuery(), { tier: "edge" }));
    expect(mocks.makeQueryKey).toHaveBeenCalledWith(expect.anything(), { tier: "edge" });
    scope.stop();
  });

  it("reflects the first matching row", () => {
    const alice = { id: "1", name: "Alice" };
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice] },
      subscribe: mocks.subscribe,
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useOne(makeQuery()))!;
    expect(result.data.value).toEqual(alice);
    expect(result.loading.value).toBe(false);
    scope.stop();
  });

  it("yields null once resolved with no match", () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [] },
      subscribe: mocks.subscribe,
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useOne(makeQuery()))!;
    expect(result.data.value).toBeNull();
    scope.stop();
  });

  it("yields undefined while loading (distinct from null)", () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "pending" as const },
      subscribe: mocks.subscribe,
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useOne(makeQuery()))!;
    expect(result.data.value).toBeUndefined();
    expect(result.loading.value).toBe(true);
    scope.stop();
  });

  it("surfaces subscription errors via the error ref", () => {
    let capturedOnError: ((err: unknown) => void) | undefined;
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "pending" as const },
      subscribe: (callbacks: any) => {
        capturedOnError = callbacks.onError;
        return mocks.unsubscribe;
      },
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useOne(makeQuery()))!;

    expect(result.error.value).toBeNull();

    capturedOnError!(new Error("network down"));

    expect(result.error.value).toBeInstanceOf(Error);
    expect((result.error.value as Error).message).toBe("network down");
    expect(result.data.value).toBeUndefined();
    expect(result.loading.value).toBe(false);

    scope.stop();
  });

  it("useOneSuspense resolves the single row and omits loading", async () => {
    const alice = { id: "1", name: "Alice" };
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice] },
      promise: Promise.resolve([alice]),
      subscribe: mocks.subscribe,
    } as any);

    const scope = effectScope();
    const result = await scope.run(() => useOneSuspense(makeQuery()))!;

    expect(result.data.value).toEqual(alice);
    expect("loading" in result).toBe(false);

    const limitedQuery = (mocks.makeQueryKey.mock.calls[0] as any)[0];
    expect(JSON.parse(limitedQuery._build())).toEqual({ table: "todos", limit: 1 });

    scope.stop();
  });

  it("useOneSuspense yields null when no row matches", async () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [] },
      promise: Promise.resolve([]),
      subscribe: mocks.subscribe,
    } as any);

    const scope = effectScope();
    const result = await scope.run(() => useOneSuspense(makeQuery()))!;

    expect(result.data.value).toBeNull();

    scope.stop();
  });
});
