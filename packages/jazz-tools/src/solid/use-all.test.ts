import { createRoot, createSignal } from "solid-js";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { RowDelta, SubscriptionDelta } from "../runtime/subscription-manager.js";

const mocks = vi.hoisted(() => {
  const unsubscribe = vi.fn();
  const subscribe = vi.fn(() => unsubscribe);
  const makeQueryKey = vi.fn(() => "test-key");
  const getCacheEntry = vi.fn(() => ({
    state: { status: "fulfilled", data: [] },
    subscribe,
  }));

  return {
    unsubscribe,
    subscribe,
    makeQueryKey,
    getCacheEntry,
    reset() {
      unsubscribe.mockReset();
      subscribe.mockReset().mockReturnValue(unsubscribe);
      makeQueryKey.mockReset().mockReturnValue("test-key");
      getCacheEntry.mockReset().mockReturnValue({
        state: { status: "fulfilled", data: [] },
        subscribe,
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

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
}

function makeUpdatedDelta<T extends { id: string }>(item: T, index = 0): SubscriptionDelta<T> {
  const UPDATED_KIND: RowDelta<T>["kind"] = 2;
  const updated: RowDelta<T> = { kind: UPDATED_KIND, id: item.id, index, item };
  return { all: [item], delta: [updated] };
}

describe("solid/useAll", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("SD-ALL-01: builds cache key from query and options", async () => {
    const query = makeQuery();
    const options = {
      tier: "local" as const,
      localUpdates: "deferred" as const,
      propagation: "local-only" as const,
    };

    let dispose!: () => void;
    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        useAll(() => ({ query, options }));
        return undefined;
      });
      await flushMicrotasks();
      expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, options);
    } finally {
      dispose?.();
    }
  });

  it("SD-ALL-02: when query is undefined, stays idle and skips subscription", () => {
    let dispose!: () => void;
    let result!: ReturnType<typeof useAll<{ id: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useAll(() => ({ query: undefined }));
        return undefined;
      });

      expect(mocks.makeQueryKey).not.toHaveBeenCalled();
      expect(mocks.subscribe).not.toHaveBeenCalled();
      expect(result.data).toBeUndefined();
      expect(result.isLoading).toBe(false);
      expect(result.error).toBeNull();
    } finally {
      dispose?.();
    }
  });

  it("SD-ALL-03: pending entry sets loading true", async () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "pending" as const },
      subscribe: mocks.subscribe,
    } as any);

    let dispose!: () => void;
    let result!: ReturnType<typeof useAll<{ id: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useAll(() => ({ query: makeQuery() }));
        return undefined;
      });
      await flushMicrotasks();

      expect(result.data).toBeUndefined();
      expect(result.isLoading).toBe(true);
      expect(result.error).toBeNull();
    } finally {
      dispose?.();
    }
  });

  it("SD-ALL-04: rejected entry maps error and clears loading", async () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "rejected" as const, error: new Error("boom") },
      subscribe: mocks.subscribe,
    } as any);

    let dispose!: () => void;
    let result!: ReturnType<typeof useAll<{ id: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useAll(() => ({ query: makeQuery() }));
        return undefined;
      });
      await flushMicrotasks();

      expect(result.data).toBeUndefined();
      expect(result.isLoading).toBe(false);
      expect(result.error).toBeInstanceOf(Error);
      expect(result.error?.message).toBe("boom");
    } finally {
      dispose?.();
    }
  });

  it("SD-ALL-05: fulfilled entry exposes initial data", async () => {
    const alice = { id: "u1", name: "Alice" };
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice] },
      subscribe: mocks.subscribe,
    } as any);

    let dispose!: () => void;
    let result!: ReturnType<typeof useAll<{ id: string; name: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useAll(() => ({ query: makeQuery() }));
        return undefined;
      });
      await flushMicrotasks();

      expect(result.data).toEqual([alice]);
      expect(result.isLoading).toBe(false);
      expect(result.error).toBeNull();
    } finally {
      dispose?.();
    }
  });

  it("SD-ALL-06: re-subscribes when options change and cleans up previous subscription", async () => {
    const query = makeQuery();
    let dispose!: () => void;
    let setOptions!: (next: any) => void;

    const unsubscribeA = vi.fn();
    const unsubscribeB = vi.fn();

    mocks.makeQueryKey.mockReturnValueOnce("key-local").mockReturnValueOnce("key-edge");
    mocks.getCacheEntry
      .mockReturnValueOnce({
        state: { status: "fulfilled" as const, data: [] },
        subscribe: vi.fn(() => unsubscribeA),
      })
      .mockReturnValueOnce({
        state: { status: "fulfilled" as const, data: [] },
        subscribe: vi.fn(() => unsubscribeB),
      });

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        const [options, _setOptions] = createSignal<any>({ tier: "local" });
        setOptions = _setOptions;
        useAll(() => ({ query, options: options() }));
        return undefined;
      });
      await flushMicrotasks();

      expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, { tier: "local" });

      setOptions({ tier: "edge" });
      await flushMicrotasks();

      expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, { tier: "edge" });
      expect(unsubscribeA).toHaveBeenCalledTimes(1);
      dispose();
      expect(unsubscribeB).toHaveBeenCalledTimes(1);
    } finally {
      dispose?.();
    }
  });

  it("SD-ALL-07: applies delta in place and preserves row identity", async () => {
    const alice = { id: "u1", name: "Alice", role: "admin" };
    let capturedOnDelta:
      | ((delta: SubscriptionDelta<{ id: string; name: string; role: string }>) => void)
      | undefined;

    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice] },
      subscribe: (callbacks: any) => {
        capturedOnDelta = callbacks.onDelta;
        return vi.fn();
      },
    } as any);

    let dispose!: () => void;
    let result!: ReturnType<typeof useAll<{ id: string; name: string; role: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useAll(() => ({ query: makeQuery() }));
        return undefined;
      });
      await flushMicrotasks();

      const originalRef = result.data![0];
      capturedOnDelta!(makeUpdatedDelta({ id: "u1", name: "Alice", role: "editor" }));

      expect(result.data).toHaveLength(1);
      expect(result.data![0]).toBe(originalRef);
      expect(result.data![0]?.role).toBe("editor");
      expect(result.isLoading).toBe(false);
      expect(result.error).toBeNull();
    } finally {
      dispose?.();
    }
  });

  it("SD-ALL-08: on subscription error, clears data and normalizes error", async () => {
    let capturedOnError: ((error: unknown) => void) | undefined;

    mocks.getCacheEntry.mockReturnValue({
      state: {
        status: "fulfilled" as const,
        data: [{ id: "u1", name: "Alice" }],
      },
      subscribe: (callbacks: any) => {
        capturedOnError = callbacks.onError;
        return vi.fn();
      },
    } as any);

    let dispose!: () => void;
    let result!: ReturnType<typeof useAll<{ id: string; name: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useAll(() => ({ query: makeQuery() }));
        return undefined;
      });
      await flushMicrotasks();

      capturedOnError!("boom");

      expect(result.data).toBeUndefined();
      expect(result.isLoading).toBe(false);
      expect(result.error).toBeInstanceOf(Error);
      expect(result.error?.message).toBe("boom");
    } finally {
      dispose?.();
    }
  });

  it("SD-ALL-09: handles synchronous cache-entry setup errors by exposing error state", async () => {
    const syncFailure = new Error("sync manager failure");
    mocks.getCacheEntry.mockImplementation(() => {
      throw syncFailure;
    });

    let dispose!: () => void;
    let result!: ReturnType<typeof useAll<{ id: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useAll(() => ({ query: makeQuery() }));
        return undefined;
      });
      await flushMicrotasks();

      expect(result.data).toBeUndefined();
      expect(result.isLoading).toBe(false);
      expect(result.error).toBe(syncFailure);
    } finally {
      dispose?.();
    }
  });
});
