import * as React from "react";
import { Component, Suspense, useState, type ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { renderToStaticMarkup } from "react-dom/server";
import type { QueryBuilder } from "../runtime/db.js";
import { limitQueryToOne } from "../runtime/limit-query.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { SubscriptionsOrchestrator } from "../subscriptions-orchestrator.js";
import { JazzClientProvider } from "./provider.js";
import { useOne, useOneSuspense } from "./use-one.js";

type Todo = { id: string; title: string };

function makeQuery(table = "todos"): QueryBuilder<Todo> {
  return {
    _table: table,
    _schema: {},
    _rowType: {} as Todo,
    _build: () => JSON.stringify({ table, conditions: [], includes: {}, orderBy: [] }),
  } as unknown as QueryBuilder<Todo>;
}

function delta(all: Todo[]): SubscriptionDelta<Todo> {
  return { all, delta: [] };
}

function makeHarness(appId: string, options?: { throwOnSubscribe?: Error }) {
  const subscribeCalls: Array<{
    query: QueryBuilder<Todo>;
    callback: (d: SubscriptionDelta<Todo>) => void;
    unsubscribe: ReturnType<typeof vi.fn>;
  }> = [];

  const db = {
    getAuthState: () => ({ authMode: "local-first", session: null }),
    onAuthChanged: () => () => {},
    updateAuthToken: () => {},
    subscribeAll: (query: any, callback: (d: SubscriptionDelta<any>) => void) => {
      if (options?.throwOnSubscribe) {
        throw options.throwOnSubscribe;
      }
      const unsubscribe = vi.fn();
      subscribeCalls.push({
        query: query as QueryBuilder<Todo>,
        callback: callback as (d: SubscriptionDelta<Todo>) => void,
        unsubscribe,
      });
      return unsubscribe;
    },
  };

  const manager = new SubscriptionsOrchestrator({ appId }, db as any);
  const client = { db, manager, session: null, shutdown: async () => {} } as any;
  return { client, manager, subscribeCalls };
}

class ErrorBoundary extends Component<
  { fallback: ReactNode; children: ReactNode },
  { error: Error | null }
> {
  state: { error: Error | null } = { error: null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    return this.state.error ? this.props.fallback : this.props.children;
  }
}

afterEach(() => {
  cleanup();
});

describe("react-core/useOne", () => {
  it("subscribes with the query limited to one row", () => {
    const { client, subscribeCalls } = makeHarness("rc-one-00");

    function Probe() {
      useOne(makeQuery());
      return null;
    }

    render(
      <JazzClientProvider client={client}>
        <Probe />
      </JazzClientProvider>,
    );

    expect(subscribeCalls).toHaveLength(1);
    const built = JSON.parse(subscribeCalls[0]!.query._build());
    expect(built.limit).toBe(1);
  });

  it("an inline query does not resubscribe across re-renders", () => {
    const { client, subscribeCalls } = makeHarness("rc-one-01");
    let force!: (n: number) => void;

    function Probe() {
      const [, setN] = useState(0);
      force = setN;
      useOne(makeQuery());
      return null;
    }

    render(
      <JazzClientProvider client={client}>
        <Probe />
      </JazzClientProvider>,
    );

    act(() => force(1));
    act(() => force(2));

    expect(subscribeCalls).toHaveLength(1);
  });

  it("renders the row and reflects later deltas", () => {
    const { client, subscribeCalls } = makeHarness("rc-one-02");

    function Item() {
      const { data: todo } = useOne(makeQuery());
      return <span>{todo === undefined ? "pending" : (todo?.title ?? "none")}</span>;
    }

    const { container } = render(
      <JazzClientProvider client={client}>
        <Item />
      </JazzClientProvider>,
    );

    expect(container.textContent).toBe("pending");

    act(() => subscribeCalls[0]!.callback(delta([{ id: "1", title: "first" }])));
    expect(container.textContent).toBe("first");

    // A later delta with a different top row replaces the tracked row.
    act(() => subscribeCalls[0]!.callback(delta([{ id: "2", title: "second" }])));
    expect(container.textContent).toBe("second");

    // Resolving to no rows yields null, distinct from the pending undefined.
    act(() => subscribeCalls[0]!.callback(delta([])));
    expect(container.textContent).toBe("none");
  });

  it("StrictMode does not open a duplicate subscription", () => {
    const { client, subscribeCalls } = makeHarness("rc-one-03");

    function Item() {
      const { data: todo } = useOne(makeQuery());
      return <span>{todo?.title ?? ""}</span>;
    }

    render(
      <React.StrictMode>
        <JazzClientProvider client={client}>
          <Item />
        </JazzClientProvider>
      </React.StrictMode>,
    );

    expect(subscribeCalls).toHaveLength(1);
  });

  it("server render reads the seeded snapshot without subscribing", () => {
    const { client, manager, subscribeCalls } = makeHarness("rc-one-04");
    const query = makeQuery();
    // The hook subscribes to the limited query, so the seed must key off it too.
    manager.makeQueryKey(limitQueryToOne(query), undefined, [{ id: "1", title: "seeded" }]);

    function Item() {
      const { data: todo } = useOne(query);
      return <span>{todo?.title ?? "none"}</span>;
    }

    const html = renderToStaticMarkup(
      <JazzClientProvider client={client}>
        <Item />
      </JazzClientProvider>,
    );

    expect(html).toContain("seeded");
    expect(subscribeCalls).toHaveLength(0);
  });

  it("a failed subscription surfaces the error and leaves data undefined without throwing", () => {
    const { client } = makeHarness("rc-one-05", {
      throwOnSubscribe: new Error("subscribe failed"),
    });

    function Item() {
      const { data: todo, isLoading, error } = useOne(makeQuery());
      return (
        <span>
          {todo === undefined ? "no-data" : (todo?.title ?? "none")}/{String(isLoading)}/
          {error?.message ?? "no-error"}
        </span>
      );
    }

    const { container } = render(
      <JazzClientProvider client={client}>
        <Item />
      </JazzClientProvider>,
    );

    expect(container.textContent).toBe("no-data/false/subscribe failed");
  });

  it("useOneSuspense throws a failed subscription to the error boundary", () => {
    const { client } = makeHarness("rc-one-06", {
      throwOnSubscribe: new Error("subscribe failed"),
    });

    function Item() {
      const todo = useOneSuspense(makeQuery());
      return <span>{todo?.title ?? "none"}</span>;
    }

    const { container } = render(
      <JazzClientProvider client={client}>
        <ErrorBoundary fallback={<span>caught</span>}>
          <Suspense fallback={<span>loading</span>}>
            <Item />
          </Suspense>
        </ErrorBoundary>
      </JazzClientProvider>,
    );

    expect(container.textContent).toBe("caught");
  });

  // Pre-resolve the entry for the limited query so the suspense reader returns
  // synchronously on first render — exercising the success branch (`rows[0] ??
  // null`) deterministically, without relying on async suspense-retry timing.
  function resolveOne(manager: SubscriptionsOrchestrator, query: QueryBuilder<Todo>, rows: Todo[]) {
    const limited = limitQueryToOne(query);
    manager.makeQueryKey(limited, undefined, rows);
    manager.getCacheEntry<Todo>(manager.computeKey(limited, undefined));
  }

  it("useOneSuspense returns the matching row once the query resolves", () => {
    const { client, manager } = makeHarness("rc-one-07");
    const query = makeQuery();
    resolveOne(manager, query, [{ id: "1", title: "only" }]);

    function Item() {
      const todo = useOneSuspense(query);
      return <span>{todo?.title ?? "none"}</span>;
    }

    const { container } = render(
      <JazzClientProvider client={client}>
        <Suspense fallback={<span>loading</span>}>
          <Item />
        </Suspense>
      </JazzClientProvider>,
    );

    expect(container.textContent).toBe("only");
  });

  it("useOneSuspense returns null when the query resolves with no row", () => {
    const { client, manager } = makeHarness("rc-one-08");
    const query = makeQuery();
    resolveOne(manager, query, []);

    function Item() {
      const todo = useOneSuspense(query);
      return <span>{todo === null ? "null" : (todo?.title ?? "?")}</span>;
    }

    const { container } = render(
      <JazzClientProvider client={client}>
        <Suspense fallback={<span>loading</span>}>
          <Item />
        </Suspense>
      </JazzClientProvider>,
    );

    expect(container.textContent).toBe("null");
  });
});
