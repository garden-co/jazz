import { createEffect, createRoot } from "solid-js";
import { beforeEach, describe, expect, expectTypeOf, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  makeQueryKey: vi.fn(() => "one-key"),
  getCacheEntry: vi.fn(),
}));

vi.mock("./provider.js", () => ({
  useJazzClient: () => ({ manager: mocks }),
}));

vi.mock("../subscription-store-internal.js", () => ({
  getSubscriptionStore: (client: any) => client.manager,
}));

import { useOne, type UseOneResult } from "./use-one.js";

type Todo = { id: string; title: string };

function makeQuery() {
  return {
    _table: "todos",
    _build: () => JSON.stringify({ table: "todos", limit: 5 }),
  } as any;
}

describe("solid/useOne", () => {
  beforeEach(() => {
    mocks.makeQueryKey.mockReset().mockReturnValue("one-key");
    mocks.getCacheEntry.mockReset();
  });

  it("exposes one nullable row and tracks later subscription results", async () => {
    let onDelta!: (delta: { all: Todo[]; delta: never[] }) => void;
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled", data: [] },
      subscribe: vi.fn((callbacks) => {
        onDelta = callbacks.onDelta;
        return vi.fn();
      }),
    });

    let dispose!: () => void;
    let result!: UseOneResult<Todo>;
    createRoot((rootDispose) => {
      dispose = rootDispose;
      result = useOne<Todo>(() => ({ query: makeQuery() }));
    });
    await Promise.resolve();

    expectTypeOf(result).toEqualTypeOf<UseOneResult<Todo>>();
    expect(result.data).toBeNull();
    const limitedQuery = (mocks.makeQueryKey.mock.calls as any[][])[0]![0];
    expect(JSON.parse(limitedQuery._build()).limit).toBe(1);

    onDelta({ all: [{ id: "1", title: "first" }], delta: [] });
    expect(result.data).toEqual({ id: "1", title: "first" });

    onDelta({ all: [], delta: [] });
    expect(result.data).toBeNull();
    dispose();
  });

  it("clears a prior session row atomically when its subscription resets", async () => {
    const priorSessionRow = { id: "prior", title: "prior session" };
    let onReset!: () => void;
    let onfulfilled!: (data: Todo[]) => void;
    const unsubscribe = vi.fn();
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled", data: [priorSessionRow] },
      subscribe: vi.fn((callbacks) => {
        onReset = callbacks.onReset;
        onfulfilled = callbacks.onfulfilled;
        return unsubscribe;
      }),
    });

    let dispose!: () => void;
    let result!: UseOneResult<Todo>;
    const observed: Array<Pick<UseOneResult<Todo>, "data" | "isLoading" | "error">> = [];
    createRoot((rootDispose) => {
      dispose = rootDispose;
      result = useOne<Todo>(() => ({ query: makeQuery() }));
      createEffect(() => {
        observed.push({
          data: result.data,
          isLoading: result.isLoading,
          error: result.error,
        });
      });
    });
    await Promise.resolve();

    expect(result).toMatchObject({ data: priorSessionRow, isLoading: false, error: null });

    onReset();

    expect(result).toMatchObject({ data: undefined, isLoading: true, error: null });
    expect(observed).toHaveLength(2);
    expect(observed.at(-1)).toMatchObject({ data: undefined, isLoading: true, error: null });

    // A reset must sever the old session's row before it can be mutated while
    // the replacement subscription is still loading.
    priorSessionRow.title = "must not leak";
    expect(result.data).toBeUndefined();

    const nextSessionRow = { id: "next", title: "next session" };
    onfulfilled([nextSessionRow]);
    expect(result).toMatchObject({ data: nextSessionRow, isLoading: false, error: null });

    dispose();
    expect(unsubscribe).toHaveBeenCalledOnce();
  });
});
