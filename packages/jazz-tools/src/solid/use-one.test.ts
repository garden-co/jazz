import { createRoot } from "solid-js";
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
});
