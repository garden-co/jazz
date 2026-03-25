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

import { useAll } from "./use-all.js";

function makeQuery(marker = "todos") {
  return { _build: () => `{"table":"${marker}"}`, _table: marker } as any;
}

describe("vue/useAll", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("VU-ALL-01: calls makeQueryKey without options when none provided", () => {
    const query = makeQuery();
    const scope = effectScope();
    scope.run(() => useAll(query));
    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, undefined);
    scope.stop();
  });

  it("VU-ALL-02: forwards QueryOptions with tier to makeQueryKey", () => {
    const query = makeQuery();
    const scope = effectScope();
    scope.run(() => useAll(query, { tier: "edge" }));
    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, { tier: "edge" });
    scope.stop();
  });

  it("VU-ALL-03: forwards full QueryOptions to makeQueryKey", () => {
    const query = makeQuery();
    const options = {
      tier: "worker" as const,
      localUpdates: "deferred" as const,
      propagation: "local-only" as const,
    };
    const scope = effectScope();
    scope.run(() => useAll(query, options));
    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, options);
    scope.stop();
  });

  it("VU-ALL-04: reactive options trigger re-subscription on change", async () => {
    const query = makeQuery();
    const options = ref<any>({ tier: "worker" });

    mocks.makeQueryKey.mockReturnValueOnce("key-worker").mockReturnValueOnce("key-edge");
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled", data: [] },
      subscribe: mocks.subscribe,
    });

    const scope = effectScope();
    scope.run(() => useAll(query, options));

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, { tier: "worker" });
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    options.value = { tier: "edge" };
    await nextTick();

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, { tier: "edge" });
    expect(mocks.unsubscribe).toHaveBeenCalledTimes(1);
    expect(mocks.subscribe).toHaveBeenCalledTimes(2);

    scope.stop();
  });

  it("VU-ALL-05: returns data from cache entry state", () => {
    const alice = { id: "1", name: "Alice" };
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice] },
      subscribe: mocks.subscribe,
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useAll(makeQuery()));
    expect(result!.value).toEqual([alice]);
    scope.stop();
  });

  it("VU-ALL-06: returns undefined when entry state is pending", () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "pending" as const },
      subscribe: mocks.subscribe,
    } as any);

    const scope = effectScope();
    const result = scope.run(() => useAll(makeQuery()));
    expect(result!.value).toBeUndefined();
    scope.stop();
  });

  it("VU-ALL-07: onDelta reconciles in-place, preserving object identity", () => {
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
    expect(result!.value).toHaveLength(1);
    const originalRef = result!.value![0];

    // Simulate a delta — role changed, name unchanged
    capturedOnDelta!({
      all: [{ id: "u1", name: "Alice", role: "editor" }],
      delta: [{ kind: 2, id: "u1", index: 0, item: { id: "u1", name: "Alice", role: "editor" } }],
    });

    expect(result!.value).toHaveLength(1);
    expect(result!.value![0]).toBe(originalRef); // same object reference
    expect((result!.value![0] as any).role).toBe("editor"); // updated value
    expect((result!.value![0] as any).name).toBe("Alice"); // unchanged

    scope.stop();
  });
});
