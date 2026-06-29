import { createRoot } from "solid-js";
import { beforeEach, describe, expect, it, vi } from "vitest";

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

import { useOne } from "./use-one.js";

function makeQuery(marker = "todos") {
  return { _build: () => `{"table":"${marker}"}`, _table: marker } as any;
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
}

describe("solid/useOne", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("SD-ONE-01: subscribes with the query limited to one row", async () => {
    const query = makeQuery();

    let dispose!: () => void;
    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        useOne(() => ({ query }));
        return undefined;
      });
      await flushMicrotasks();

      expect(mocks.makeQueryKey).toHaveBeenCalledTimes(1);
      const [builtQuery] = mocks.makeQueryKey.mock.calls[0] as unknown as [any, unknown];
      expect(JSON.parse(builtQuery._build())).toEqual({
        table: "todos",
        limit: 1,
      });
      expect(mocks.subscribe).toHaveBeenCalledTimes(1);
    } finally {
      dispose?.();
    }
  });

  it("SD-ONE-02: when query is undefined, stays idle and skips subscription", () => {
    let dispose!: () => void;
    let result!: ReturnType<typeof useOne<{ id: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useOne(() => ({ query: undefined }));
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

  it("SD-ONE-03: pending entry sets loading true and data undefined", async () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "pending" as const },
      subscribe: mocks.subscribe,
    } as any);

    let dispose!: () => void;
    let result!: ReturnType<typeof useOne<{ id: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useOne(() => ({ query: makeQuery() }));
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

  it("SD-ONE-04: fulfilled entry exposes the single matching row", async () => {
    const alice = { id: "u1", name: "Alice" };
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [alice] },
      subscribe: mocks.subscribe,
    } as any);

    let dispose!: () => void;
    let result!: ReturnType<typeof useOne<{ id: string; name: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useOne(() => ({ query: makeQuery() }));
        return undefined;
      });
      await flushMicrotasks();

      expect(result.data).toEqual(alice);
      expect(result.isLoading).toBe(false);
      expect(result.error).toBeNull();
    } finally {
      dispose?.();
    }
  });

  it("SD-ONE-05: empty fulfilled result yields null", async () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled" as const, data: [] },
      subscribe: mocks.subscribe,
    } as any);

    let dispose!: () => void;
    let result!: ReturnType<typeof useOne<{ id: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useOne(() => ({ query: makeQuery() }));
        return undefined;
      });
      await flushMicrotasks();

      expect(result.data).toBeNull();
      expect(result.isLoading).toBe(false);
      expect(result.error).toBeNull();
    } finally {
      dispose?.();
    }
  });

  it("SD-ONE-06: rejected entry maps error and clears loading", async () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "rejected" as const, error: new Error("boom") },
      subscribe: mocks.subscribe,
    } as any);

    let dispose!: () => void;
    let result!: ReturnType<typeof useOne<{ id: string }>>;

    try {
      createRoot((rootDispose) => {
        dispose = rootDispose;
        result = useOne(() => ({ query: makeQuery() }));
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
});
