import { effectScope } from "vue";
import { beforeEach, describe, expect, expectTypeOf, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  makeQueryKey: vi.fn(() => "one-key"),
  getCacheEntry: vi.fn(),
}));

vi.mock("./provider.js", () => ({
  useJazzClient: () => ({ manager: mocks }),
}));

import { useOne, type UseOneResult } from "./use-one.js";

type Todo = { id: string; title: string };

function makeQuery() {
  return {
    _table: "todos",
    _build: () => JSON.stringify({ table: "todos", limit: 12 }),
  } as any;
}

describe("vue/useOne", () => {
  beforeEach(() => {
    mocks.makeQueryKey.mockReset().mockReturnValue("one-key");
    mocks.getCacheEntry.mockReset();
  });

  it("returns a nullable row ref and forces limit one", () => {
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "fulfilled", data: [] },
      subscribe: vi.fn(() => vi.fn()),
    });

    const scope = effectScope();
    const result = scope.run(() => useOne<Todo>(makeQuery()))!;

    expectTypeOf(result).toEqualTypeOf<UseOneResult<Todo>>();
    expect(result.data.value).toBeNull();
    const limitedQuery = (mocks.makeQueryKey.mock.calls as any[][])[0]![0];
    expect(JSON.parse(limitedQuery._build()).limit).toBe(1);
    scope.stop();
  });

  it("reacts when the first row appears and disappears", () => {
    let onDelta!: (delta: { all: Todo[]; delta: never[] }) => void;
    mocks.getCacheEntry.mockReturnValue({
      state: { status: "pending" },
      subscribe: vi.fn((callbacks) => {
        onDelta = callbacks.onDelta;
        return vi.fn();
      }),
    });

    const scope = effectScope();
    const result = scope.run(() => useOne<Todo>(makeQuery()))!;
    expect(result.data.value).toBeUndefined();

    onDelta({ all: [{ id: "1", title: "first" }], delta: [] });
    expect(result.data.value).toEqual({ id: "1", title: "first" });

    onDelta({ all: [], delta: [] });
    expect(result.data.value).toBeNull();
    scope.stop();
  });
});
