import * as React from "react";
import { act, cleanup, render } from "@testing-library/react";
import { afterEach, describe, expect, expectTypeOf, it, vi } from "vitest";
import type { DbDeltaSubscriptionCallbacks, QueryBuilder } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import { attachSubscriptionStore } from "../subscription-store-internal.js";
import { JazzClientProvider } from "./provider.js";
import { useOne, type UseOneResult } from "./use-one.js";

type Todo = { id: string; title: string };

function makeQuery(): QueryBuilder<Todo> {
  return {
    _table: "todos",
    _schema: {},
    _rowType: {} as Todo,
    _build: () =>
      JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [], limit: 20 }),
  } as unknown as QueryBuilder<Todo>;
}

function makeHarness() {
  let subscribedQuery: QueryBuilder<Todo> | undefined;
  let callback: ((delta: SubscriptionDelta<Todo>) => void) | undefined;
  const db = {
    getAuthState: () => ({ authMode: "local-first" as const, session: null }),
    onAuthChanged: () => () => {},
    updateAuthToken: () => {},
    subscribeDelta: (query: QueryBuilder<Todo>, callbacks: DbDeltaSubscriptionCallbacks<Todo>) => {
      subscribedQuery = query;
      callback = callbacks.onDelta;
      return vi.fn();
    },
  };
  const manager = new SubscriptionsOrchestrator({ appId: "react-use-one" }, db as never);
  return {
    client: attachSubscriptionStore({ db, session: null, shutdown: async () => {} }, manager),
    getSubscribedQuery: () => subscribedQuery,
    emit: (all: Todo[]) => callback!({ all, delta: [] }),
  };
}

afterEach(cleanup);

describe("react-core/useOne", () => {
  it("infers a nullable single-row result", () => {
    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      expectTypeOf(useOne(makeQuery())).toEqualTypeOf<UseOneResult<Todo>>();
    }
  });

  it("forces limit one and distinguishes loading, empty, and populated results", () => {
    const harness = makeHarness();
    let result!: UseOneResult<Todo>;

    function Probe() {
      result = useOne(makeQuery());
      return <span>{result.isLoading ? "loading" : (result.data?.title ?? "empty")}</span>;
    }

    const { container } = render(
      <JazzClientProvider client={harness.client}>
        <Probe />
      </JazzClientProvider>,
    );

    expect(container.textContent).toBe("loading");
    expect(JSON.parse(harness.getSubscribedQuery()!._build()).limit).toBe(1);

    act(() => harness.emit([]));
    expect(result.data).toBeNull();
    expect(container.textContent).toBe("empty");

    act(() => harness.emit([{ id: "1", title: "first" }]));
    expect(result.data).toEqual({ id: "1", title: "first" });
    expect(container.textContent).toBe("first");

    act(() => harness.emit([]));
    expect(result.data).toBeNull();
  });
});
