import { beforeEach, describe, expect, it, vi } from "vitest";
import { effectScope, nextTick, ref } from "vue";

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

import { useAll, useAllSuspense } from "./use-all.js";

function makeQuery(marker = "todos") {
  return { _build: () => `{"table":"${marker}"}`, _table: marker } as any;
}

describe("vue/useAll", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("calls makeQueryKey without options when none provided", () => {
    const query = makeQuery();
    const scope = effectScope();
    scope.run(() => useAll(query));
    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, undefined);
    scope.stop();
  });

  it("forwards QueryOptions with tier to makeQueryKey", () => {
    const query = makeQuery();
    const scope = effectScope();
    scope.run(() => useAll(query, { tier: "edge" }));
    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, { tier: "edge" });
    scope.stop();
  });

  it("forwards full QueryOptions to makeQueryKey", () => {
    const query = makeQuery();
    const options = {
      tier: "local" as const,
      localUpdates: "deferred" as const,
      propagation: "local-only" as const,
    };
    const scope = effectScope();
    scope.run(() => useAll(query, options));
    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, options);
    scope.stop();
  });

  it("reactive options trigger re-subscription on change", async () => {
    const query = makeQuery();
    const options = ref<any>({ tier: "local" });

    mocks.makeQueryKey.mockReturnValueOnce("key-worker").mockReturnValueOnce("key-edge");
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled", data: [] },
      subscribe: mocks.subscribe,
    });

    const scope = effectScope();
    scope.run(() => useAll(query, options));

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, { tier: "local" });
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    options.value = { tier: "edge" };
    await nextTick();

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, { tier: "edge" });
    expect(mocks.unsubscribe).toHaveBeenCalledTimes(1);
    expect(mocks.subscribe).toHaveBeenCalledTimes(2);

    scope.stop();
  });

  it("returns data from cache entry state", () => {
    const alice = { id: "1", name: "Alice" };
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice] },
      subscribe: mocks.subscribe,
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useAll(makeQuery()));
    expect(result!.data.value).toEqual([alice]);
    scope.stop();
  });

  it("returns undefined when entry state is pending", () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "pending" as const },
      subscribe: mocks.subscribe,
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useAll(makeQuery()));
    expect(result!.data.value).toBeUndefined();
    scope.stop();
  });

  it("onDelta reconciles in-place, preserving object identity", () => {
    const alice = { id: "u1", name: "Alice", role: "admin" };
    let capturedOnDelta: ((delta: any) => void) | undefined;

    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice] },
      subscribe: (callbacks: any) => {
        capturedOnDelta = callbacks.onDelta;
        return vi.fn();
      },
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useAll(makeQuery()));

    // Initial state from cache
    expect(result!.data.value).toHaveLength(1);
    const originalRef = result!.data.value![0];

    // Simulate a delta — role changed, name unchanged
    capturedOnDelta!({
      all: [{ id: "u1", name: "Alice", role: "editor" }],
      delta: [{ kind: 2, id: "u1", index: 0, item: { id: "u1", name: "Alice", role: "editor" } }],
    });

    expect(result!.data.value).toHaveLength(1);
    expect(result!.data.value![0]).toBe(originalRef); // same object reference
    expect((result!.data.value![0] as any).role).toBe("editor"); // updated value
    expect((result!.data.value![0] as any).name).toBe("Alice"); // unchanged

    scope.stop();
  });

  it("batch delta with remove + add preserves correct items", () => {
    //  Before:  [alice, bob, carol]
    //  Delta:   remove alice (index 0), add dave (index 2)
    //  After:   [bob, carol, dave]
    const alice = { id: "u1", name: "Alice" };
    const bob = { id: "u2", name: "Bob" };
    const carol = { id: "u3", name: "Carol" };
    let capturedOnDelta: ((delta: any) => void) | undefined;

    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice, bob, carol] },
      subscribe: (callbacks: any) => {
        capturedOnDelta = callbacks.onDelta;
        return vi.fn();
      },
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useAll(makeQuery()));

    expect(result!.data.value).toHaveLength(3);
    const bobRef = result!.data.value![1];
    const carolRef = result!.data.value![2];

    // Batch: remove alice (was at index 0) + add dave (at index 2 in final state)
    capturedOnDelta!({
      all: [
        { id: "u2", name: "Bob" },
        { id: "u3", name: "Carol" },
        { id: "u4", name: "Dave" },
      ],
      delta: [
        { kind: 1, id: "u1", index: 0 },
        { kind: 0, id: "u4", index: 2, item: { id: "u4", name: "Dave" } },
      ],
    });

    expect(result!.data.value).toHaveLength(3);
    expect(result!.data.value![0]).toBe(bobRef); // bob preserved
    expect(result!.data.value![1]).toBe(carolRef); // carol preserved
    expect((result!.data.value![2] as any).name).toBe("Dave"); // dave added

    scope.stop();
  });

  it("batch delta with two removes preserves survivors", () => {
    //  Before:  [alice, bob, carol, dave]
    //  Delta:   remove alice (index 0), remove carol (index 2)
    //  After:   [bob, dave]
    const alice = { id: "u1", name: "Alice" };
    const bob = { id: "u2", name: "Bob" };
    const carol = { id: "u3", name: "Carol" };
    const dave = { id: "u4", name: "Dave" };
    let capturedOnDelta: ((delta: any) => void) | undefined;

    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice, bob, carol, dave] },
      subscribe: (callbacks: any) => {
        capturedOnDelta = callbacks.onDelta;
        return vi.fn();
      },
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useAll(makeQuery()));

    expect(result!.data.value).toHaveLength(4);
    const bobRef = result!.data.value![1];
    const daveRef = result!.data.value![3];

    // Batch: remove alice + remove carol
    capturedOnDelta!({
      all: [
        { id: "u2", name: "Bob" },
        { id: "u4", name: "Dave" },
      ],
      delta: [
        { kind: 1, id: "u1", index: 0 },
        { kind: 1, id: "u3", index: 2 },
      ],
    });

    expect(result!.data.value).toHaveLength(2);
    expect(result!.data.value![0]).toBe(bobRef); // bob preserved
    expect(result!.data.value![1]).toBe(daveRef); // dave preserved

    scope.stop();
  });

  it("updated item changes position, array reorders correctly", () => {
    //  Before: [alice, bob, carol]
    //  Delta:  alice updated and moved to end (e.g. sort order changed)
    //  After:  [bob, carol, alice']
    const alice = { id: "u1", name: "Alice", score: 10 };
    const bob = { id: "u2", name: "Bob", score: 20 };
    const carol = { id: "u3", name: "Carol", score: 30 };
    let capturedOnDelta: ((delta: any) => void) | undefined;

    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice, bob, carol] },
      subscribe: (callbacks: any) => {
        capturedOnDelta = callbacks.onDelta;
        return vi.fn();
      },
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useAll(makeQuery()));

    const aliceRef = result!.data.value![0];
    const bobRef = result!.data.value![1];
    const carolRef = result!.data.value![2];

    // Alice's score changed, causing her to sort to the end
    capturedOnDelta!({
      all: [
        { id: "u2", name: "Bob", score: 20 },
        { id: "u3", name: "Carol", score: 30 },
        { id: "u1", name: "Alice", score: 5 },
      ],
      delta: [{ kind: 2, id: "u1", index: 2, item: { id: "u1", name: "Alice", score: 5 } }],
    });

    expect(result!.data.value).toHaveLength(3);
    expect(result!.data.value![0]).toBe(bobRef); // bob kept position
    expect(result!.data.value![1]).toBe(carolRef); // carol kept position
    expect(result!.data.value![2]).toBe(aliceRef); // alice moved, identity preserved
    expect((result!.data.value![2] as any).score).toBe(5); // property updated

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
    const result = scope.run(() => useAll(makeQuery()))!;

    expect(result.error.value).toBeNull();
    expect(result.loading.value).toBe(true);

    capturedOnError!(new Error("network down"));

    expect(result.error.value).toBeInstanceOf(Error);
    expect((result.error.value as Error).message).toBe("network down");
    expect(result.data.value).toBeUndefined();
    expect(result.loading.value).toBe(false);

    scope.stop();
  });

  it("useAllSuspense exposes data and error but omits loading", async () => {
    const alice = { id: "1", name: "Alice" };
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice] },
      promise: Promise.resolve([alice]),
      subscribe: mocks.subscribe,
    } as any);

    const scope = effectScope();
    const result = await scope.run(() => useAllSuspense(makeQuery()))!;

    expect(result.data.value).toEqual([alice]);
    expect("loading" in result).toBe(false);

    scope.stop();
  });
});
